//! Base-branch movement and the branch-scoped work it still owes.
//!
//! Base advance is lazy. Moving a base (a Cairn landing, an origin import, a
//! placed batch) records nothing about the branches cut from it and touches
//! none of them: no sibling is enumerated, replayed, noted or resumed. A branch
//! is carried onto its current base only when work needs that base, at its
//! next commit, a requested check or `run branch:`, or its merge, by the
//! branch-scoped preparer in [`preparation`]; an owner's explicit `/rebase`
//! replay goes through the same preparer ([`replay_requests`]).
//!
//! What stays here is the work a base movement genuinely owns: importing an
//! advance into the store, base-branch checks, service and code-map
//! notifications, re-pointing children of a merged parent, and the shared
//! records and messages the preparer uses. The queue the previous, eager
//! design left behind is settled once by [`retirement`].

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use crate::execution::routing::{owning_db_for_job, owning_db_for_project};
use crate::jj::RECONCILE_TIMEOUT;
use crate::messages::delivery::{
    latest_run_for_job, queue_system_direct, queue_system_direct_once,
};
use crate::messages::queued::DeliveryUrgency;
use crate::models::ExecutionSnapshot;
use crate::orchestrator::conflict_session::{record_marker_state, MarkerState};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::executor_protocol::{ResidencyOperation, ResidencyResult};
use cairn_db::turso::params;

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod dismissal_tests;
pub(crate) mod preparation;
#[cfg(test)]
mod publication_tests;
#[cfg(test)]
mod recovery_tests;
pub(crate) mod regenerate;
#[cfg(test)]
mod regenerate_tests;
pub(crate) mod replay_cost;
mod replay_requests;
pub(crate) mod retirement;

pub(crate) use replay_requests::{
    on_branch_hold_released, on_branch_hold_released_for_run, request_branch_replay,
    serve_pending_replay_requests, REPEAT_REFUSED,
};

/// The one project whose store holds a branch, the database that owns its
/// records, and whether that project manages the branch.
pub(crate) struct BranchOwner {
    pub(crate) project_id: String,
    pub(crate) db: Arc<LocalDb>,
    /// Named by one of the project's jobs, and the default branch of no project
    /// on the store: the only kind of branch whose publication may rewrite
    /// origin's copy.
    managed: bool,
}

impl BranchOwner {
    /// The acceptance evidence a rewrite of origin's copy may use. `None` grants
    /// no sideways publication authority: integration defaults and unmanaged
    /// refs stay fast-forward-only, regardless of matching commit ids.
    async fn publication_claims(
        &self,
        store: &Path,
        branch: &str,
    ) -> Result<Option<Vec<crate::jj::AcceptedAdvanceDrops>>, String> {
        if !self.managed {
            return Ok(None);
        }
        let claims =
            preparation::accepted_drop_claims(&self.db, &self.project_id, store, branch).await?;
        Ok(Some(
            claims
                .into_iter()
                .map(
                    |(head_after, dropped_commit_ids)| crate::jj::AcceptedAdvanceDrops {
                        head_after,
                        dropped_commit_ids,
                    },
                )
                .collect(),
        ))
    }
}

/// Whether any job in project `?1` names branch `?2`. Runs per project on
/// every publication; its plan is pinned by
/// `live_job_lookups_by_branch_seek_project_and_branch` (migration
/// `0403_index_jobs_project_branch`).
const BRANCH_NAMED_BY_A_JOB_SQL: &str =
    "SELECT EXISTS(SELECT 1 FROM jobs j WHERE j.project_id = ?1 AND j.branch = ?2)";

/// The project that owns `branch` in `store`, matched on the store's canonical
/// identity, so any spelling of the store finds the same project.
///
/// Several projects on one store resolve to the one that names the branch (as
/// its default, or in one of its jobs); when that is not exactly one, the
/// answer is ambiguous and fails closed rather than pick a journal. `None` when
/// no project's store is this one.
pub(crate) async fn branch_owner(
    orch: &Orchestrator,
    store: &Path,
    branch: &str,
) -> Result<Option<BranchOwner>, String> {
    let store_key = preparation::journal_store_key(store)?;
    // (project, the branch is its default, one of its jobs names the branch)
    let mut candidates: Vec<(String, bool, bool)> = Vec::new();
    for db in orch.db.all_dbs().await {
        let projects: Vec<String> = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query(
                        "SELECT id FROM projects WHERE repo_path IS NOT NULL AND repo_path <> ''",
                        (),
                    ).await?;
                    let mut projects = Vec::new();
                    while let Some(row) = rows.next().await? {
                        projects.push(row.text(0)?);
                    }
                    Ok(projects)
                })
            })
            .await
            .map_err(|error| format!("resolve the project owning `{branch}`: {error}"))?;
        for project_id in projects {
            // A team project's synced path names another machine's checkout:
            // the store is derived from this machine's clone, when it has one.
            let Some(repo_path) = load_project_repo_path(&orch.db, &project_id).await? else {
                continue;
            };
            let candidate = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
            let candidate = match std::fs::canonicalize(&candidate) {
                Ok(path) => path.to_string_lossy().into_owned(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(format!(
                        "resolve publication store {}: {error}",
                        candidate.display()
                    ))
                }
            };
            if candidate != store_key {
                continue;
            }
            let is_default = load_project_default_branch(&db, &project_id)
                .await?
                .as_deref()
                == Some(branch);
            let named = db
                .query_one(
                    BRANCH_NAMED_BY_A_JOB_SQL,
                    (project_id.clone(), branch.to_string()),
                    |row| row.i64(0),
                )
                .await
                .map_err(|error| format!("resolve the owner of `{branch}`: {error}"))?
                != 0;
            candidates.push((project_id, is_default, named));
        }
    }
    let naming: BTreeSet<&str> = candidates
        .iter()
        .filter(|(_, is_default, named)| *is_default || *named)
        .map(|(id, ..)| id.as_str())
        .collect();
    let pool: BTreeSet<&str> = if naming.is_empty() {
        candidates.iter().map(|(id, ..)| id.as_str()).collect()
    } else {
        naming
    };
    let project_id = match pool.iter().collect::<Vec<_>>().as_slice() {
        [] => return Ok(None),
        [only] => only.to_string(),
        many => {
            return Err(format!(
                "`{branch}` is claimed by more than one project on the store {} ({}); its \
                 publication authority is ambiguous",
                store.display(),
                many.iter()
                    .map(|id| format!("`{id}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
    };
    let managed = !candidates.iter().any(|(_, is_default, _)| *is_default)
        && candidates
            .iter()
            .any(|(id, _, named)| *id == project_id && *named);
    let db = owning_db_for_project(&orch.db, &project_id)
        .await
        .map_err(|error| error.to_string())?;
    Ok(Some(BranchOwner {
        project_id,
        db,
        managed,
    }))
}

/// The acceptance evidence a managed branch may use to publish its own rewrite.
/// `None` grants no sideways publication authority: integration defaults and
/// unmanaged refs remain fast-forward-only, regardless of matching commit ids.
pub(crate) async fn managed_publication_claims(
    orch: &Orchestrator,
    store: &Path,
    branch: &str,
) -> Result<Option<Vec<crate::jj::AcceptedAdvanceDrops>>, String> {
    match branch_owner(orch, store, branch).await? {
        Some(owner) => owner.publication_claims(store, branch).await,
        None => Ok(None),
    }
}

/// Publish a managed branch's store head to origin: one push, leased on exactly
/// the copy of origin the store recorded, so origin moves only from that copy.
///
/// The branch is reserved first and stays reserved until the publication is
/// recorded, so nothing in Cairn moves it while the store lock is released.
/// Under the lock, the branch's write permit settles its acceptance journal and
/// refuses while an install is owed; the head and origin's recorded copy are
/// then read from one settled operation. The push and its read-back run with the
/// lock released. A push origin accepted is recorded under the lock again, as a
/// write of this branch's refs alone (no import), so an unrelated branch's
/// reservation never holds it up; an exact read-back of a managed branch also
/// checkpoints its acceptance journal.
///
/// A fast-forward always publishes. A sideways move is allowed only for a
/// branch its project manages, and only when the accepted installs prove that
/// origin loses nothing but what this store replaced or dropped. When origin's
/// copy is not the one the store recorded, its current copy is fetched once
/// into an observation ref and judged under the lock: a strict fast-forward of
/// the head is taken (inside the store-mutation bracket, since taking it
/// imports), and a copy the head contains, or for a managed branch a copy it may
/// prove it replaced, is recorded as origin's and leased on. A copy
/// holding work the store cannot account for is never overwritten. The
/// publication is refused, naming both commits.
///
/// Origin having accepted is the publication, whatever happens to its
/// read-back or record: a failure there is logged, nothing is rolled back, and
/// the next publication finds origin's copy and records it. A push whose
/// outcome is unknown is reported as such, and the open pull request's cached
/// verdict is downgraded; the store records nothing, and the next publication
/// re-sends the same head under the same lease, which origin already holding
/// it accepts.
pub(crate) async fn publish_managed_branch(
    orch: &Orchestrator,
    store: &Path,
    branch: &str,
) -> Result<(), String> {
    crate::orchestrator::base_branch_sync::refuse_if_behind_origin(orch, store, branch).await?;
    let reservation = orch
        .reserve_branches(
            store,
            &[branch],
            "publish managed branch",
            crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        )
        .await?;
    let owner = branch_owner(orch, store, branch).await?.ok_or_else(|| {
        format!(
            "`{branch}` was not published: no project owns the store {}, so there is no \
             acceptance journal to admit its publication against",
            store.display()
        )
    })?;
    let has_origin = load_project_repo_path(&orch.db, &owner.project_id)
        .await?
        .is_some_and(|repo| crate::mcp::git::has_remote(Path::new(&repo)));
    if !has_origin {
        log::info!("publish `{branch}`: no `origin` remote; nothing to publish");
        return Ok(());
    }
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let publisher = Publisher {
        orch,
        jj: &jj,
        store,
        branch,
        owner: &owner,
        reservation: &reservation,
    };
    let published = publisher.publish().await?;
    let observation = orch.base_branch_sync.observe();
    crate::orchestrator::base_branch_sync::record_publication(
        orch,
        store,
        branch,
        &published,
        observation,
    )
    .await;
    Ok(())
}

/// The database holding `store`'s recovery fences: the one whose transaction
/// recorded them, the database owning the project the store serves.
pub(crate) async fn recovery_fence_db(orch: &Orchestrator, store: &Path) -> Arc<LocalDb> {
    let path = store.to_string_lossy().into_owned();
    for db in orch.db.all_dbs().await {
        if db
            .query_opt_i64(
                "SELECT 1 FROM jj_branch_recoveries WHERE store_path = ?1 LIMIT 1",
                params![path.as_str()],
            )
            .await
            .ok()
            .flatten()
            .is_some()
        {
            return db;
        }
    }
    Arc::clone(&orch.db.local)
}

/// Where a publication starts: the branch's store head, and the store's
/// recorded copy of origin's branch, which the push leases on.
#[derive(Debug, Clone)]
struct PublicationPoint {
    head: String,
    origin: Option<String>,
}

/// How one recovery from a moved origin copy ended.
enum OriginMove {
    /// The store took origin's fast-forward of the head: origin already holds
    /// the branch's head, which is this commit.
    Taken(String),
    /// Publish again, leased on this observation of origin's copy.
    Lease(Option<String>),
}

/// One publication's fixed context. The reservation keeps the branch still
/// across every release of the store lock.
struct Publisher<'a> {
    orch: &'a Orchestrator,
    jj: &'a crate::jj::JjEnv,
    store: &'a Path,
    branch: &'a str,
    owner: &'a BranchOwner,
    reservation: &'a crate::orchestrator::BranchReservation,
}

impl Publisher<'_> {
    /// Push the head, recovering once when origin's copy is not the one the
    /// store recorded. Answers the commit origin holds for the branch.
    async fn publish(&self) -> Result<String, String> {
        let branch = self.branch;
        let mut point = self.read_point().await?;
        if point.origin.as_deref() == Some(point.head.as_str()) {
            return Ok(point.head);
        }
        let mut recovered = false;
        loop {
            let accepted = self.owner.publication_claims(self.store, branch).await?;
            match self.send(&point, accepted).await {
                Ok((heads, readback)) => {
                    self.record(heads, readback, &point.head).await;
                    return Ok(point.head);
                }
                Err(crate::jj::LeasedPushError::TargetMoved { observed, .. }) if !recovered => {
                    recovered = true;
                    log::warn!(
                        "publish `{branch}`: origin's copy is {}, not the {} the store recorded; \
                         fetching it and judging it once",
                        observed.as_deref().unwrap_or("<absent>"),
                        point.origin.as_deref().unwrap_or("<absent>")
                    );
                    match self.absorb_origin_move(&point.head).await? {
                        OriginMove::Taken(tip) => return Ok(tip),
                        OriginMove::Lease(origin) => point.origin = origin,
                    }
                }
                Err(crate::jj::LeasedPushError::Unknown(error)) => {
                    if let Err(mark) = mark_publication_unconfirmed(
                        self.orch,
                        &self.owner.db,
                        &self.owner.project_id,
                        branch,
                    )
                    .await
                    {
                        log::error!("downgrade the cached verdict of `{branch}`: {mark}");
                    }
                    return Err(format!(
                        "`{branch}` at {} may or may not have reached origin ({error}). The store \
                         recorded nothing; its next publication re-sends the same head under the \
                         same lease, and origin already holding it counts as accepted",
                        point.head
                    ));
                }
                Err(error) => return Err(format!("`{branch}` was not published: {error}")),
            }
        }
    }

    /// The store lock, with the branch admitted under it: reserved by this
    /// publication and owing no acceptance once its journal is settled.
    async fn admitted_guard(
        &self,
        purpose: String,
    ) -> Result<crate::orchestrator::JjStoreGuard, String> {
        let guard = self
            .orch
            .acquire_jj_store_lock(
                self.store,
                crate::orchestrator::StoreLockClass::Foreground,
                purpose,
            )
            .await;
        guard
            .branch_write_permit(
                &self.owner.db,
                self.reservation,
                &self.owner.project_id,
                self.branch,
                crate::orchestrator::BatchAdmission::None,
            )
            .await
            .map(drop)?;
        Ok(guard)
    }

    /// The head and origin's recorded copy, from one settled operation read
    /// under an admitted lock.
    async fn read_point(&self) -> Result<PublicationPoint, String> {
        let branch = self.branch;
        let guard = self
            .admitted_guard(format!("read the publication point of {branch}"))
            .await?;
        // Every managed push reads its head here. A fenced bookmark's live
        // position is not trusted, so it is never read and published.
        let fence_db = recovery_fence_db(self.orch, self.store).await;
        crate::orchestrator::branch_recovery::refuse_if_fenced(
            &fence_db,
            self.store,
            &[branch],
            &format!("Publishing `{branch}`"),
        )
        .await?;
        let (store, name) = (self.store.to_path_buf(), branch.to_string());
        let state =
            tokio::task::spawn_blocking(move || crate::jj::read_local_branch_state(&store, &name))
                .await
                .map_err(|error| format!("the branch read worker failed: {error}"))??;
        drop(guard);
        let head = state.bookmark.ok_or_else(|| {
            format!("`{branch}` was not published: the store has no bookmark of that name")
        })?;
        Ok(PublicationPoint {
            head,
            origin: state.origin,
        })
    }

    /// The leased push and its read-back, both network I/O, with the store
    /// lock released. The read-back answers whether origin holds exactly the
    /// accepted head (`true`) or a descendant carrying it (`false`). A worker
    /// that did not finish leaves the outcome unknown.
    async fn send(
        &self,
        point: &PublicationPoint,
        accepted: Option<Vec<crate::jj::AcceptedAdvanceDrops>>,
    ) -> Result<(crate::jj::AcceptedHeads, Result<bool, String>), crate::jj::LeasedPushError> {
        let (jj, store, branch, point) = (
            self.jj.clone(),
            self.store.to_path_buf(),
            self.branch.to_string(),
            point.clone(),
        );
        tokio::task::spawn_blocking(
            move || -> Result<
                (crate::jj::AcceptedHeads, Result<bool, String>),
                crate::jj::LeasedPushError,
            > {
                let heads = crate::jj::push_managed_branch_unrecorded(
                    &jj,
                    &store,
                    &branch,
                    point.origin.as_deref(),
                    &point.head,
                    accepted.as_deref(),
                )?;
                let readback = crate::jj::readback_leased_push_exact(&jj, &store, &heads);
                Ok((heads, readback))
            },
        )
        .await
        .unwrap_or_else(|error| {
            Err(crate::jj::LeasedPushError::Unknown(format!(
                "the push worker did not finish: {error}"
            )))
        })
    }

    /// Record a publication origin accepted as the store's copy of origin's
    /// branch: a write of this branch's refs alone, under the admitted lock,
    /// so another branch's reservation never holds it up. When origin was read
    /// back at exactly `head` and the record landed, a managed branch's
    /// journal checkpoints the publication. Nothing here un-publishes: a
    /// refused admission, a refused record or a read-back that did not confirm
    /// is logged, and the next publication finds origin's copy and records it.
    async fn record(
        &self,
        heads: crate::jj::AcceptedHeads,
        readback: Result<bool, String>,
        head: &str,
    ) {
        let branch = self.branch;
        let mut unconfirmed: Vec<String> = Vec::new();
        match self
            .admitted_guard(format!("record the publication of {branch}"))
            .await
        {
            Ok(guard) => {
                // In place, with no await until it returns: a caller that stops
                // awaiting cannot release the guard while the record still writes.
                let (jj, store) = (self.jj, self.store);
                let recorded = crate::jj::in_place_blocking(|| {
                    crate::jj::record_leased_push(jj, store, &heads)
                });
                let exact = match &readback {
                    Ok(exact) => *exact,
                    Err(error) => {
                        unconfirmed.push(error.clone());
                        false
                    }
                };
                match recorded {
                    Ok(()) if exact && self.owner.managed => {
                        if let Err(error) = preparation::record_publication_checkpoint(
                            &self.owner.db,
                            &guard,
                            self.reservation,
                            &self.owner.project_id,
                            self.store,
                            branch,
                            head,
                        )
                        .await
                        {
                            log::warn!(
                                "`{branch}` was published at {head}, but its journal checkpoint \
                                 was not recorded: {error}"
                            );
                        }
                    }
                    Ok(()) => {}
                    Err(error) => unconfirmed.push(error),
                }
                drop(guard);
            }
            Err(refusal) => {
                unconfirmed.extend(readback.err());
                unconfirmed.push(refusal);
            }
        }
        if !unconfirmed.is_empty() {
            log::warn!(
                "`{branch}` was published (origin accepted it), but the publication is not yet \
                 confirmed and recorded in the store: {}. Nothing is rolled back; the branch's \
                 next publication finds origin's copy and records it",
                unconfirmed.join("; ")
            );
        }
    }

    /// Origin's copy is not the one the store recorded. Fetch its current copy
    /// into the observation ref (network, lock released), then judge it under
    /// the lock with this branch's write permit. A strict fast-forward of
    /// `head` is taken; a copy that `head` contains, or one a managed branch
    /// may prove it replaced, is recorded as origin's copy (the local bookmark
    /// untouched) and leased on, and the retried push's own proof decides the
    /// rewrite. Both writes touch this branch's refs alone, so another
    /// branch's reservation never holds them up, and both run in place, with
    /// no await while they write. Anything else is refused, and origin's copy
    /// is left alone.
    async fn absorb_origin_move(&self, head: &str) -> Result<OriginMove, String> {
        let branch = self.branch;
        let (fetch_store, fetch_branch) = (self.store.to_path_buf(), branch.to_string());
        let observed =
            crate::orchestrator::base_branch_sync::with_fetch_lock(self.orch, self.store, || {
                tokio::task::spawn_blocking(move || {
                    crate::jj::fetch_origin_observation(&fetch_store, &fetch_branch)
                })
            })
            .await
            .map_err(|error| format!("the origin fetch worker failed: {error}"))??;
        let Some(observed) = observed else {
            return Ok(OriginMove::Lease(None));
        };
        enum Judged {
            FastForward(Result<crate::jj::OriginFastForward, String>),
            Unmanaged,
            Recorded(Result<(), String>),
        }
        let judged = {
            let guard = self
                .orch
                .acquire_jj_store_lock(
                    self.store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    format!("judge origin's copy of {branch}"),
                )
                .await;
            // The permit borrows the guard, so it ends with this block, before
            // the guard is released.
            let permit = guard
                .branch_write_permit(
                    &self.owner.db,
                    self.reservation,
                    &self.owner.project_id,
                    branch,
                    crate::orchestrator::BatchAdmission::None,
                )
                .await?;
            let ahead = observed != head && self.is_ancestor(head, &observed).await?;
            let (jj, store, seen) = (self.jj, self.store, observed.as_str());
            if ahead {
                Judged::FastForward(crate::jj::in_place_blocking(|| {
                    let _admitted = &permit;
                    crate::jj::fast_forward_branch_from_origin(jj, store, branch, seen)
                }))
            } else if !(observed == head || self.is_ancestor(&observed, head).await?)
                && !self.owner.managed
            {
                Judged::Unmanaged
            } else {
                Judged::Recorded(crate::jj::in_place_blocking(|| {
                    let _admitted = &permit;
                    crate::jj::record_origin_observation(jj, store, branch, seen, head)
                }))
            }
        };
        match judged {
            Judged::FastForward(taken) => match taken? {
                crate::jj::OriginFastForward::Advanced { to, .. } => Ok(OriginMove::Taken(to)),
                other => Err(format!(
                    "`{branch}` was not published: origin's copy {observed} descends from its \
                     head {head}, but taking it answered {other:?}"
                )),
            },
            Judged::Unmanaged => {
                forget_origin_observation(self.store, branch);
                Err(format!(
                    "`{branch}` was not published: origin's copy is {observed}, which its head \
                     {head} does not contain, and origin's copy is rewritten only for a branch a \
                     project manages"
                ))
            }
            Judged::Recorded(recorded) => {
                forget_origin_observation(self.store, branch);
                recorded?;
                Ok(OriginMove::Lease(Some(observed)))
            }
        }
    }

    /// Whether `ancestor` is `descendant` or one of its ancestors in the
    /// store's backing repository: a pure read of objects already present.
    async fn is_ancestor(&self, ancestor: &str, descendant: &str) -> Result<bool, String> {
        let (store, ancestor, descendant) = (
            self.store.to_path_buf(),
            ancestor.to_string(),
            descendant.to_string(),
        );
        tokio::task::spawn_blocking(move || {
            let git_dir = crate::jj::store_git_dir(&store)
                .ok_or_else(|| format!("resolve the backing repository of {}", store.display()))?;
            let output = cairn_platform::env::git()
                .args([
                    "--git-dir=.",
                    "merge-base",
                    "--is-ancestor",
                    ancestor.as_str(),
                    descendant.as_str(),
                ])
                .current_dir(&git_dir)
                .output()
                .map_err(|error| format!("git merge-base --is-ancestor: {error}"))?;
            match output.status.code() {
                Some(0) => Ok(true),
                Some(1) => Ok(false),
                _ => Err(format!(
                    "git merge-base --is-ancestor {ancestor} {descendant}: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )),
            }
        })
        .await
        .map_err(|error| format!("the ancestry worker failed: {error}"))?
    }
}

/// Delete the branch's observation ref once its fetch has been judged: its
/// lifecycle is one fetch-and-judge, and the judged commit is named in the
/// answer. Best effort; the next fetch overwrites a leftover.
fn forget_origin_observation(store: &Path, branch: &str) {
    let Some(git_dir) = crate::jj::store_git_dir(store) else {
        return;
    };
    let observation = crate::jj::origin_observation_ref(branch);
    let _ = cairn_platform::env::git()
        .args(["--git-dir=.", "update-ref", "-d", observation.as_str()])
        .current_dir(&git_dir)
        .output();
}

/// What a replay measures the branch's own work from, path by path.
///
/// The fork — the live merge-base of the tip and the destination — is right for
/// every path the branch simply edited. It is wrong for a path the branch
/// RESOLVED against a newer base: that resolution is a linear commit holding the
/// newer base's content without the newer base in its ancestry, so measured from
/// the fork the absorbed content reads as the branch's own. Such a path is
/// measured from the side it was resolved against instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayBaseline {
    pub(crate) fork: String,
    pub(crate) basis: crate::orchestrator::conflict_session::ResolutionBasis,
}

impl ReplayBaseline {
    pub(crate) fn fork_only(fork: &str) -> Self {
        Self {
            fork: fork.to_string(),
            basis: Default::default(),
        }
    }

    /// The side `path` was resolved against, when a resolution covered it.
    pub(crate) fn resolved_against(&self, path: &str) -> Option<&str> {
        self.basis.get(path).map(|side| side.theirs.as_str())
    }

    /// What the branch's work on `path` is measured from.
    pub(crate) fn baseline_for(&self, path: &str) -> &str {
        self.resolved_against(path).unwrap_or(&self.fork)
    }

    /// This baseline with the basis narrowed to the paths `tip` provably holds
    /// the resolved side of: restoring the tip's file whole over the basis's
    /// merge drops nothing from it.
    ///
    /// A session's basis is a claim about the branch's commits, and only an
    /// explicit replay has had that claim checked by the request guard. An
    /// automatic advance gets it checked here, per path, because measuring an
    /// unresolved file from the newer side would read the newer side's lines as
    /// deletions the branch made.
    ///
    /// An attested deletion never survives this: a deleted path is never
    /// assessed lossless. That is right, because an automatic advance carries
    /// no content, so a deletion lands only through an explicit replay.
    pub(crate) fn verified(self, jj: &crate::jj::JjEnv, store: &Path, tip: &str) -> Self {
        let mut by_side: BTreeMap<(&str, &str), Vec<String>> = BTreeMap::new();
        for (path, side) in &self.basis {
            by_side
                .entry((side.base.as_str(), side.theirs.as_str()))
                .or_default()
                .push(path.clone());
        }
        let held: BTreeSet<String> = by_side
            .into_iter()
            .flat_map(|((base, theirs), paths)| {
                crate::jj::assess_paths(jj, store, base, tip, theirs, &paths)
            })
            .filter(|assessment| assessment.verdict == crate::jj::RestoreVerdict::Lossless)
            .map(|assessment| assessment.path)
            .collect();
        Self {
            basis: self
                .basis
                .into_iter()
                .filter(|(path, _)| held.contains(path))
                .collect(),
            fork: self.fork,
        }
    }
}

struct StructuralMergePlan {
    tip: String,
    destination: String,
    paths: Vec<String>,
    edits: Vec<crate::orchestrator::landing::bind::FileEdit>,
}

/// Read immutable sides and prove candidate content without writer exclusion.
/// `regenerated` names paths settled by declared regeneration instead, which
/// the plan leaves alone.
fn prepare_structural_merge(
    jj: &crate::jj::JjEnv,
    store: &Path,
    diagnostic: &crate::jj::ConflictDiagnostic,
    baseline: &ReplayBaseline,
    rules: &crate::orchestrator::landing::MergeRules,
    regenerated: &dyn Fn(&str) -> bool,
) -> Result<Option<StructuralMergePlan>, String> {
    if diagnostic.condition == crate::jj::ConflictCondition::BaseDrift {
        return Ok(None);
    }
    let paths: Vec<String> = diagnostic
        .conflicting_paths()
        .into_iter()
        .filter(|path| !regenerated(path))
        .collect();
    if paths.is_empty() || paths.iter().any(|path| rules.kind_for(path).is_none()) {
        return Ok(None);
    }
    let (Some(fork), Some(ours), Some(theirs)) = (
        diagnostic.base.as_deref(),
        diagnostic.ours.as_deref(),
        diagnostic.theirs.as_deref(),
    ) else {
        return Ok(None);
    };
    let mut edits = Vec::new();
    for path in &paths {
        let base = baseline.resolved_against(path).unwrap_or(fork);
        for (path, sides) in crate::jj::read_sides_for_paths(
            jj,
            store,
            base,
            ours,
            theirs,
            std::slice::from_ref(path),
        ) {
            let sides = sides?;
            let (
                crate::jj::FileContent::Text(base_text),
                crate::jj::FileContent::Text(ours_text),
                crate::jj::FileContent::Text(theirs_text),
            ) = (&sides.base, &sides.ours, &sides.theirs)
            else {
                return Ok(None);
            };
            let Some(content) = rules.proven_merge(&path, base_text, ours_text, theirs_text) else {
                return Ok(None);
            };
            edits.push(crate::orchestrator::landing::bind::FileEdit::Rewrite { path, content });
        }
    }
    Ok(Some(StructuralMergePlan {
        tip: ours.to_string(),
        destination: theirs.to_string(),
        paths,
        edits,
    }))
}

fn text_at_revision(
    jj: &crate::jj::JjEnv,
    store: &Path,
    revision: &str,
    path: &str,
) -> Result<String, String> {
    crate::jj::text_at_revision(jj, store, revision, path)?
        .ok_or_else(|| format!("`{path}` is absent at revision `{revision}`"))
}

/// Where `pre_tip` left `dest`: their live merge-base, and nothing else.
///
/// `jobs.base_commit` is deliberately not consulted. It is an archival record
/// (see [`advance_sibling_durable_base`]), a branch can have several job rows
/// holding different generations of it, and a fork read from an older one puts
/// the base's own increments between fork and tip — which the resolver then
/// attributed to the branch. A merge-base that does not resolve to one commit
/// returns `None`, and the caller refuses before touching the graph.
fn branch_fork_point(
    jj: &crate::jj::JjEnv,
    store: &Path,
    pre_tip: &str,
    dest: &str,
) -> Option<String> {
    crate::jj::revset_commit(jj, store, &format!("heads(::{pre_tip} & ::{dest})"))
}

/// Whether replaying a branch from `before` to `after` onto `dest` kept the
/// branch's own change: its diff from its fork point is the same at both tips.
/// An unreadable comparison answers no, so doubt withdraws a running wave
/// exactly as every base advance did before the comparison existed.
fn replay_kept_patch(
    jj: &crate::jj::JjEnv,
    store: &Path,
    before: Option<&String>,
    after: Option<&String>,
    dest: &str,
) -> bool {
    let (Some(before), Some(after)) = (before, after) else {
        return false;
    };
    match (
        crate::jj::own_patch_against(jj, store, before, dest),
        crate::jj::own_patch_against(jj, store, after, dest),
    ) {
        (Ok(before), Ok(after)) => before.patch == after.patch,
        (Err(error), _) | (_, Err(error)) => {
            log::warn!("replayed patch comparison {before} -> {after} unreadable: {error}");
            false
        }
    }
}

/// A committed resolution carried onto one replay destination, and the paths
/// that could not be.
struct CarryPlan {
    carry: crate::jj::ResolutionCarry,
    refused: Vec<(String, String)>,
}

impl CarryPlan {
    fn describe_refusals(&self) -> String {
        let mut out = String::from(
            "Your committed resolution was not carried onto the new base for these file(s), so \
             they were left to the merge — which is why this replay did not land:\n",
        );
        for (path, why) in &self.refused {
            out.push_str(&format!("- `{path}`: {why}\n"));
        }
        out.push_str(
            "Resolve them against the new base (cairn:~/rebase now shows both sides) and request \
             the replay again. Every other conflicting file kept your resolution.",
        );
        out
    }
}

/// Carry a session's committed resolution onto `destination`, path by path.
///
/// Each conflicting path is merged three ways: the side it was resolved
/// against, the committed tip, and the destination. A destination that has not
/// touched the path since takes the committed file whole — today's restore,
/// exactly right. One that changed it elsewhere has those changes merged in.
/// One that changed the very region the agent resolved has produced a conflict
/// nobody has resolved, and is refused unless the destination declares a
/// structural merge for the file (`landing.merge`) that settles it into a file
/// that still parses. An incoming hunk is therefore always kept: merged in,
/// merged structurally, or the replay is refused.
///
/// The committed side is the tip the request was assessed at; the rebase
/// refuses if the branch has moved off it since.
fn plan_resolution_carry(
    jj: &crate::jj::JjEnv,
    store: &Path,
    session: &crate::orchestrator::conflict_session::ConflictSession,
    destination: &str,
    live_tip: &str,
) -> Result<CarryPlan, String> {
    use crate::jj::{Carried, CarriedContent};

    let committed = session
        .replay_requested_tip
        .clone()
        .unwrap_or_else(|| live_tip.to_string());
    let fork = branch_fork_point(jj, store, &committed, destination).ok_or_else(|| {
        format!("the fork point of `{committed}` and `{destination}` did not resolve")
    })?;
    let baseline = ReplayBaseline {
        fork,
        basis: session.resolution_basis_at(jj, store, &committed)?,
    };
    // Every path the replay has to place, not only the current conflicts: a
    // path an earlier replay carried and then rolled back is no longer
    // conflicting here, and leaving it out would let it conflict again.
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for path in session.replay_paths(
        &baseline.basis,
        crate::orchestrator::conflict_session::ReplayScope::Carry,
    ) {
        groups
            .entry(baseline.baseline_for(&path).to_string())
            .or_default()
            .push(path);
    }
    let rules = crate::orchestrator::landing::merge_rules_at(jj, store, destination);
    let mut carried = Vec::new();
    let mut refused = Vec::new();
    for (resolved_against, paths) in groups {
        for (path, outcome) in crate::jj::carry_paths(
            jj,
            store,
            &resolved_against,
            &committed,
            destination,
            &paths,
        ) {
            match outcome {
                Ok(Carried::Unchanged) => carried.push((path, CarriedContent::FromTip)),
                Ok(Carried::Merged(text)) => carried.push((path, CarriedContent::Text(text))),
                Ok(Carried::Conflicted { .. }) if rules.kind_for(&path).is_some() => {
                    let read = |revision: &str| text_at_revision(jj, store, revision, &path);
                    let merged = rules.proven_merge(
                        &path,
                        &read(&resolved_against)?,
                        &read(&committed)?,
                        &read(destination)?,
                    );
                    match merged {
                        Some(merged) => carried.push((path, CarriedContent::Text(merged))),
                        None => refused.push((
                            path,
                            format!(
                                "`{destination}` changed the region you resolved in a way its \
                                 declared structural merge could not settle"
                            ),
                        )),
                    }
                }
                Ok(Carried::Conflicted { .. }) => {
                    let why = match baseline.resolved_against(&path) {
                        Some(theirs) => format!(
                            "`{destination}` changed the region you resolved against `{theirs}`"
                        ),
                        None => "your branch has not resolved this file against any base yet"
                            .to_string(),
                    };
                    refused.push((path, why));
                }
                Ok(Carried::DeletionOutpaced) => {
                    let attested = baseline
                        .basis
                        .get(&path)
                        .and_then(|side| side.attested.clone());
                    let deletion = crate::orchestrator::conflict_session::OutpacedDeletion {
                        commits: crate::jj::commits_changing_path(
                            jj,
                            store,
                            &resolved_against,
                            destination,
                            &path,
                        ),
                        from: resolved_against.clone(),
                        attested,
                        to: destination.to_string(),
                    };
                    let why = format!(
                        "{}{}",
                        crate::orchestrator::conflict_session::describe_outpaced_deletion(
                            &deletion
                        ),
                        crate::orchestrator::conflict_session::deletion_remedy(destination)
                    );
                    refused.push((path, why));
                }
                Ok(Carried::Unjudged(why)) | Err(why) => refused.push((path, why)),
            }
        }
    }
    // An expired budget turns the remaining paths into refusals above; that
    // is a timeout, not a plan.
    jj.merge_deadline().check("resolution carry plan")?;
    Ok(CarryPlan {
        carry: crate::jj::ResolutionCarry {
            expected_pre_tip: committed,
            paths: carried,
        },
        refused,
    })
}

/// The [`ReplayBaseline`] of `pre_tip` onto `dest`: the live fork, and the open
/// session's resolution basis narrowed to the paths the tip provably holds.
///
/// A basis that cannot be read for any reason other than time is replaced by
/// the session's stored one, which `verified` then narrows, and says so. An
/// expired budget is an error: the item times out rather than replay on an
/// answer nobody computed.
fn replay_baseline_at(
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    session: Option<&crate::orchestrator::conflict_session::ConflictSession>,
    pre_tip: &str,
    dest: &str,
) -> Result<Option<ReplayBaseline>, String> {
    let ctx = format!("resolution basis assessment of `{branch}` at {pre_tip}");
    let fork = branch_fork_point(jj, store, pre_tip, dest);
    jj.merge_deadline().check(&ctx)?;
    let Some(fork) = fork else {
        return Ok(None);
    };
    let basis =
        match session.map(|session| (session, session.resolution_basis_at(jj, store, pre_tip))) {
            None => Default::default(),
            Some((_, Ok(basis))) => basis,
            Some((_, Err(error))) if crate::jj::is_jj_timeout_error(&error) => {
                return Err(format!("{ctx}: {error}"));
            }
            Some((session, Err(error))) => {
                log::warn!("{ctx} unreadable; measuring from the session's stored basis: {error}");
                session
                    .without_superseded(jj, store, session.resolution_basis.clone())
                    .map_err(|error| format!("{ctx}: {error}"))?
            }
        };
    let baseline = ReplayBaseline { fork, basis }.verified(jj, store, pre_tip);
    // `verified` reads an expired budget as "not provably held" and narrows.
    jj.merge_deadline().check(&ctx)?;
    Ok(Some(baseline))
}

const ROLLED_BACK_CONFLICT: &str = "rolled_back_conflict";

fn replay_is_explicit(label: &str) -> bool {
    label.starts_with("replay requested for ")
        || label.starts_with("resolved replay requested for ")
}

fn quarantine_blocks_automatic_replay(
    quarantine: &ReconcileQuarantine,
    current_tip: Option<&str>,
    label: &str,
) -> bool {
    if quarantine.failure_kind == RECONCILE_TIMEOUT {
        // A new destination does not make the unchanged stack any cheaper. An
        // owner's explicit replay runs under its own budget and is never held.
        return !replay_is_explicit(label)
            && quarantine.observed_tip() == current_tip.unwrap_or("missing")
            && chrono::Utc::now().timestamp() < quarantine.retry_at();
    }
    quarantine.failure_kind != "ambiguous_divergence"
        && quarantine.observed_tip() == current_tip.unwrap_or("missing")
        && ((quarantine.failure_kind != ROLLED_BACK_CONFLICT
            && quarantine.failure_kind != crate::orchestrator::catch_up::MERGE_CONFLICT_QUARANTINE)
            || !replay_is_explicit(label))
}

/// Invalidate every cached GitHub verdict for a branch that moved locally but
/// did not reach origin. The live artifact compares GitHub's head with the store
/// and names both commits; this cache downgrade closes the window before anyone
/// opens that artifact, so the desktop cannot continue presenting the old head's
/// green mergeability as current.
async fn mark_publication_unconfirmed(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "UPDATE merge_requests
         SET github_mergeable = 'UNKNOWN', github_fetched_at = NULL, updated_at = ?3
         WHERE project_id = ?1 AND source_branch = ?2
           AND status NOT IN ('merged', 'closed')
           AND (github_state IS NULL OR lower(github_state) NOT IN ('merged', 'closed'))",
        params![project_id, branch, now],
    )
    .await
    .map_err(|error| format!("invalidate stale pull-request verdict for `{branch}`: {error}"))?;
    // The row's own cached verdict has just been downgraded, so any snapshot
    // published from it describes a head the branch has moved past. Advance the
    // generation for every job whose PR this touched, by the same predicate the
    // update used, so the next read recomputes instead of serving the old head.
    for job_id in merge_request_jobs_for_branch(db, project_id, branch).await {
        orch.invalidate_pr_refresh(&job_id, "source-head-unpublished");
    }
    // Every open request on the branch moved, all within this one project.
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::merge_request_db_change_ids("update", None, None, None, project_id),
    );
    Ok(())
}

/// The jobs owning every unresolved merge request on a branch — the exact set a
/// branch-scoped `merge_requests` mutation moves.
async fn merge_request_jobs_for_branch(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Vec<String> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        let branch = branch.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT job_id FROM merge_requests
                      WHERE project_id = ?1 AND source_branch = ?2
                        AND status NOT IN ('merged', 'closed')",
                    params![project_id.as_str(), branch.as_str()],
                )
                .await?;
            let mut jobs = Vec::new();
            while let Some(row) = rows.next().await? {
                if let Some(job_id) = row.opt_text(0)? {
                    jobs.push(job_id);
                }
            }
            Ok(jobs)
        })
    })
    .await
    .unwrap_or_default()
}

/// A `run` batch whose durable suspension row is still open: the thing that
/// holds a branch at the base-advance mutation boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InflightRunBatch {
    /// The job whose turn the batch parked. Not necessarily the node reading
    /// about it: child tasks share their parent's branch, so a child's batch
    /// holds the parent's branch too.
    pub(crate) job_id: String,
    /// That job's home URI, when it resolves: the batch's cancel address
    /// (`<holder>/batches/<request_id>`) sits under it.
    pub(crate) holder: Option<String>,
    /// The batch's placement request, which is what cancellation addresses.
    pub(crate) request_id: String,
    pub(crate) label: Option<String>,
    /// When the batch parked its turn (Unix milliseconds). The batch itself
    /// started up to one synchronous window before that.
    pub(crate) parked_at_ms: i64,
    /// Where the batch stands now, when this runner holds its task: the item
    /// running and for how long, or what it queues for. `None` for a batch
    /// this runner does not hold, and wherever the reader has no fleet to ask
    /// ([`Self::with_live_progress`]).
    pub(crate) progress: Option<crate::mcp::handlers::run::detached::BatchProgress>,
}

impl InflightRunBatch {
    /// ``run batch `REQUEST` ("LABEL") on cairn://p/…, parked since 2026-10-18 04:28 PDT;
    /// now running item 3 of 5 (`cargo nextest run`) for 14m 2s``
    pub(crate) fn describe(&self) -> String {
        self.describe_at(chrono::Utc::now().timestamp_millis())
    }

    pub(crate) fn describe_at(&self, now_ms: i64) -> String {
        let label = self
            .label
            .as_deref()
            .map(|label| format!(" (\"{label}\")"))
            .unwrap_or_default();
        let owner = match self.holder.as_deref() {
            Some(uri) => format!(" on {uri}"),
            None => format!(" on job `{}`", self.job_id),
        };
        let parked = crate::clock::stamp_millis(self.parked_at_ms)
            .map(|stamp| format!(", parked since {stamp}"))
            .unwrap_or_default();
        let progress = self
            .progress
            .as_ref()
            .map(|progress| format!("; {}", progress.describe(now_ms)))
            .unwrap_or_default();
        format!(
            "run batch `{}`{label}{owner}{parked}{progress}",
            self.request_id
        )
    }

    /// The batch's own resource, whose `action:"cancel"` patch ends it. A
    /// task's holder already ends in `/task/<name>`.
    pub(crate) fn cancel_uri(&self) -> Option<String> {
        self.holder
            .as_deref()
            .map(|holder| format!("{holder}/batches/{}", self.request_id))
    }

    /// Fill in where the batch stands now, when this runner holds its task.
    pub(crate) fn with_live_progress(mut self, orch: &crate::orchestrator::Orchestrator) -> Self {
        self.progress = crate::mcp::handlers::run::detached::live_progress(orch, &self.request_id);
        self
    }

    /// Fill in the holder's home URI. Best effort: an unresolved URI falls
    /// back to the job id, which still identifies it.
    async fn resolve_holder(mut self, db: &LocalDb) -> Self {
        self.holder = crate::jobs::queries::home_uri_for_job(db, &self.job_id)
            .await
            .ok()
            .flatten();
        self
    }
}

/// The batch named by one `agent_waits` row, if the row is a run batch that
/// holds its branch ([`Condition::holds_branch`]). A host batch (a long
/// response item) holds no working tree and cannot commit, so it never holds
/// the boundary: deferring a rebase for minutes behind a model call would
/// protect nothing.
///
/// [`Condition::holds_branch`]: crate::mcp::handlers::durable_suspend::Condition::holds_branch
fn run_batch_from_wait(
    job_id: String,
    condition_json: &str,
    created_at: i64,
) -> Option<InflightRunBatch> {
    let condition: crate::mcp::handlers::durable_suspend::Condition =
        serde_json::from_str(condition_json).ok()?;
    if !condition.holds_branch() {
        return None;
    }
    match condition {
        crate::mcp::handlers::durable_suspend::Condition::RunBatch {
            request_id, label, ..
        } => Some(InflightRunBatch {
            job_id,
            holder: None,
            request_id,
            label,
            parked_at_ms: created_at,
            progress: None,
        }),
        _ => None,
    }
}

/// A batch that outlived its call and still runs, as the boundary names it.
fn run_batch_from_detached(
    row: &cairn_db::turso::Row,
) -> Result<InflightRunBatch, crate::storage::DbError> {
    Ok(InflightRunBatch {
        job_id: row.text(0)?,
        holder: None,
        request_id: row.text(1)?,
        label: row.opt_text(2)?,
        parked_at_ms: row.get::<i64>(3)?,
        progress: None,
    })
}

/// [`inflight_run_batch_except`] as job `job_id` sees it: within that job's
/// project, its own batch first, nothing exempt. Read by views of the hold.
pub(crate) async fn inflight_run_batch_on_branch(
    db: &LocalDb,
    job_id: &str,
    branch: &str,
) -> Result<Option<InflightRunBatch>, String> {
    let project_id = db
        .query_text(
            "SELECT project_id FROM jobs WHERE id = ?1",
            (job_id.to_string(),),
        )
        .await
        .map_err(|error| format!("inspect in-flight run batches on `{branch}`: {error}"))?;
    let Some(project_id) = project_id else {
        return Ok(None);
    };
    inflight_run_batch_except(db, &project_id, branch, Some(job_id), None).await
}

/// The open run batch that holds `branch` of `project_id` at the mutation
/// boundary, across every live job on it: a node-only lookup would report
/// "nothing holds it" to a parent whose child task, sharing the branch, still
/// has a batch open. This is the one reader every writer admission uses.
///
/// `exempt` is one publication's own `(job, sealed request)`, named only after
/// that request's immutable sealed delta was captured. Only that exact wait is
/// skipped and the scan continues, so a child task or a second batch still
/// prevents branch movement. `prefer_job`'s batch is reported first when
/// several are open, since that is the one its reader can end itself.
///
/// A batch holds the boundary for as long as a durable row for its request is
/// unsettled, and only settlement releases it (cairn/6145 and cairn/6205): an
/// open run-batch wait (`pending` or `resolving`), or a `run_batch_detached`
/// row still `running` for a batch that can disturb the branch. A wait
/// abandoned while its batch ran becomes such a detached row in the same
/// write, so the agent losing interest never ends the hold. The source is
/// these durable rows, not the in-memory fleet registry: the boundary
/// revalidates inside the store-lock bracket that moves the bookmark, where a
/// durable row can be read and the registry cannot, and the registry is
/// per-incarnation, so for minutes after a boot it reads "not in flight" for
/// remote batches that are running.
pub(crate) async fn inflight_run_batch_except(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    prefer_job: Option<&str>,
    exempt: Option<(&str, &str)>,
) -> Result<Option<InflightRunBatch>, String> {
    let exempt = exempt.map(|(job, request)| (job.to_string(), request.to_string()));
    let (project_id, branch) = (project_id.to_string(), branch.to_string());
    let prefer_job = prefer_job.unwrap_or_default().to_string();
    let sql = format!(
        "SELECT w.job_id, w.condition_json, w.created_at
           FROM agent_waits w
           JOIN jobs j ON j.id = w.job_id
          WHERE j.project_id = ?1
            AND j.branch = ?2
            AND w.state IN ('pending', 'resolving')
            AND {LIVE_JOB_PREDICATE}
          ORDER BY (w.job_id = ?3) DESC, w.created_at"
    );
    let detached_sql = format!(
        "SELECT d.job_id, d.request_id, d.label, d.created_ms
           FROM run_batch_detached d
           JOIN jobs j ON j.id = d.job_id
          WHERE j.project_id = ?1
            AND j.branch = ?2
            AND d.state = 'running' AND d.holds_branch = 1
            AND {LIVE_JOB_PREDICATE}
          ORDER BY (d.job_id = ?3) DESC, d.created_ms"
    );
    let found = db
        .read(|conn| {
            let (project_id, branch, prefer_job, sql, detached_sql) = (
                project_id.clone(),
                branch.clone(),
                prefer_job.clone(),
                sql.clone(),
                detached_sql.clone(),
            );
            let exempt = exempt.clone();
            let is_exempt = move |batch: &InflightRunBatch| {
                exempt.as_ref().is_some_and(|(job, request)| {
                    batch.job_id == *job && batch.request_id == *request
                })
            };
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        &sql,
                        params![project_id.as_str(), branch.as_str(), prefer_job.as_str()],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    if let Some(batch) =
                        run_batch_from_wait(row.text(0)?, &row.text(1)?, row.get::<i64>(2)?)
                    {
                        if is_exempt(&batch) {
                            continue;
                        }
                        return Ok(Some(batch));
                    }
                }
                drop(rows);
                let mut rows = conn
                    .query(
                        &detached_sql,
                        params![project_id.as_str(), branch.as_str(), prefer_job.as_str()],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    let batch = run_batch_from_detached(&row)?;
                    if is_exempt(&batch) {
                        continue;
                    }
                    return Ok(Some(batch));
                }
                Ok(None)
            })
        })
        .await
        .map_err(|error| format!("inspect in-flight run batches on `{branch}`: {error}"))?;
    Ok(match found {
        Some(batch) => Some(batch.resolve_holder(db).await),
        None => None,
    })
}

/// Whether `job_id` and `other` are both on one live branch of one project:
/// a node and the tasks that share its branch, which hold it together at the
/// mutation boundary ([`inflight_run_batch_on_branch`]).
pub(crate) async fn shares_live_branch(
    db: &LocalDb,
    job_id: &str,
    other: &str,
) -> Result<bool, String> {
    let sql = format!(
        "SELECT EXISTS(
           SELECT 1 FROM jobs j JOIN jobs o ON o.project_id = j.project_id AND o.branch = j.branch
            WHERE j.id = ?1 AND o.id = ?2 AND j.branch IS NOT NULL AND {LIVE_JOB_PREDICATE})"
    );
    db.query_one(sql, params![job_id.to_string(), other.to_string()], |row| {
        Ok(row.i64(0)? != 0)
    })
    .await
    .map_err(|error| format!("read whether two jobs share a branch: {error}"))
}

/// What a deferral at the mutation boundary is waiting on, read from the live
/// suspension rows of every job on the branch: the batch that holds it, that
/// nothing does any more, or that this could not be read. Shared by the rebase
/// page and the replay request's answer so the two never describe the same
/// hold differently.
pub(crate) fn mutation_boundary_hold(batch: &Result<Option<InflightRunBatch>, String>) -> String {
    match batch {
        Ok(Some(batch)) => format!(
            "It is held by {}: a branch is never rewritten under a batch that may commit to it. \
             The hold ends when that batch finishes; when it is cancelled, by its owning node or \
             the operator, with {}; or when the operator stops the node, which stops every run \
             under it. Ending a turn does not release it: a suspended batch outlives the turn \
             that started it. To cancel it and request the replay in one step: \
             `write({{changes:[{{target:\"cairn:~/rebase\",mode:\"patch\",payload:{{action:\"replay\",cancel_holding_batch:true}}}}]}})`. \
             Nothing revisits it on a timer: after that, the branch is carried onto its base at \
             its next commit, requested check or run, or merge, or when the replay is requested \
             again.",
            batch.describe(),
            cancel_action(batch)
        ),
        Ok(None) => "No run batch is in flight on this branch now, on this node or any other \
                     that shares it, so nothing holds the boundary any more: the branch is \
                     carried onto its base at its next commit, requested check or run, or \
                     merge, or when the replay is requested again."
            .to_string(),
        Err(error) => {
            format!("Whether a run batch on this branch still holds it is unknown: {error}.")
        }
    }
}

/// The copy-paste cancel for a holding batch, or where to find it when its
/// owner's home did not resolve.
pub(crate) fn cancel_action(batch: &InflightRunBatch) -> String {
    match batch.cancel_uri() {
        Some(uri) => format!(
            "`write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"cancel\"}}}}]}})`"
        ),
        None => format!(
            "an `action:\"cancel\"` patch on `batches/{}` under job `{}`'s home",
            batch.request_id, batch.job_id
        ),
    }
}

#[derive(Debug)]
struct MergedJob {
    id: String,
    project_id: String,
    /// The branch that just merged, and is about to be deleted. Anything cut
    /// from it is stranded the moment it goes.
    branch: Option<String>,
    base_branch: Option<String>,
}

#[derive(Debug, Clone)]
struct ReconcileQuarantine {
    failure_kind: String,
    fingerprint: String,
    strike_count: i64,
    updated_at: i64,
}

impl ReconcileQuarantine {
    /// A merge catch-up's quarantine names both coordinates, `tip:dest`: the
    /// same pair is never retried, and a newer destination is.
    fn is_merge_catch_up(&self) -> bool {
        self.failure_kind == crate::orchestrator::catch_up::MERGE_CONFLICT_QUARANTINE
            || self.failure_kind == crate::jj::MERGE_CATCH_UP_REFUSED
    }

    /// The destination a merge catch-up quarantine was recorded against.
    fn observed_dest(&self) -> Option<&str> {
        self.is_merge_catch_up()
            .then(|| self.fingerprint.split_once(':').map(|(_, dest)| dest))
            .flatten()
    }

    fn observed_tip(&self) -> &str {
        if self.failure_kind == RECONCILE_TIMEOUT || self.is_merge_catch_up() {
            // Both coordinates are immutable commit ids, which cannot contain ':'.
            self.fingerprint
                .split_once(':')
                .map_or(self.fingerprint.as_str(), |(tip, _)| tip)
        } else {
            &self.fingerprint
        }
    }

    fn retry_at(&self) -> i64 {
        self.updated_at
            .saturating_add(timeout_backoff_seconds(self.strike_count))
    }
}

/// Five minutes buys foreground work a useful window; doubling up to one hour
/// avoids hammering an expensive unchanged stack while still probing recovery.
fn timeout_backoff_seconds(strikes: i64) -> i64 {
    (300_i64 * (1_i64 << strikes.saturating_sub(1).clamp(0, 4))).min(3600)
}

async fn load_reconcile_quarantine(
    db: &LocalDb,
    project_id: &str,
    store_path: &Path,
    bookmark: &str,
) -> Result<Option<ReconcileQuarantine>, String> {
    let project_id = project_id.to_string();
    let store_path = store_path.to_string_lossy().into_owned();
    let bookmark = bookmark.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT failure_kind, fingerprint, last_diagnostic, strike_count, updated_at
                     FROM jj_reconcile_quarantines
                     WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3",
                    params![project_id, store_path, bookmark],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(ReconcileQuarantine {
                failure_kind: row.text(0)?,
                fingerprint: row.text(1)?,
                strike_count: row.i64(3)?,
                updated_at: row.i64(4)?,
            }))
        })
    })
    .await
    .map_err(|error| format!("load reconcile quarantine: {error}"))
}

/// Persist the circuit breaker and the owning issues' explanation together.
/// Notification delivery is not a precondition: an idle/missing run must not
/// turn an expensive timeout back into a retry on every advance.
async fn record_reconcile_timeout(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
    tip: &str,
    destination: &str,
    error: &str,
) -> Result<String, String> {
    let project_id = project_id.to_string();
    let store = store.to_string_lossy().into_owned();
    let branch = branch.to_string();
    let tip = tip.to_string();
    let destination = destination.to_string();
    let error = error.to_string();
    db.write(move |conn| {
        let project_id = project_id.clone();
        let store = store.clone();
        let branch = branch.clone();
        let tip = tip.clone();
        let destination = destination.clone();
        let error = error.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT failure_kind, fingerprint, strike_count FROM jj_reconcile_quarantines
                 WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3",
                params![project_id.as_str(), store.as_str(), branch.as_str()],
            ).await?;
            let strikes = match rows.next().await? {
                Some(row) if row.text(0)? == RECONCILE_TIMEOUT
                    && row.text(1)?.split_once(':').is_some_and(|(old_tip, _)| old_tip == tip) =>
                    row.i64(2)?.saturating_add(1),
                _ => 1,
            };
            drop(rows);
            let now = chrono::Utc::now().timestamp();
            let delay = timeout_backoff_seconds(strikes);
            let retry_at = now.saturating_add(delay);
            let fingerprint = format!("{tip}:{destination}");
            let diagnostic = format!(
                "{error}\nAutomatic reconcile timed out for branch `{branch}` at `{tip}` onto `{destination}`. \
                 This branch is outside automatic reconcile until it changes or the cooldown expires at \
                 Unix time {retry_at} ({delay}s; timeout strike {strikes}). A new main tip does not bypass \
                 this cooldown. Other branches continue. An owner-requested store-side replay bypasses \
                 this automatic cooldown and uses its own cumulative forward budget. Inspect the \
                 timeout/rollback diagnostic before requesting it: an incomplete rollback needs \
                 controlled store-side recovery with execution, not reset or force-push."
            );
            conn.execute(
                "INSERT INTO jj_reconcile_quarantines
                 (project_id, store_path, bookmark, failure_kind, fingerprint, last_diagnostic,
                  strike_count, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
                 ON CONFLICT(project_id, store_path, bookmark) DO UPDATE SET
                   failure_kind = excluded.failure_kind, fingerprint = excluded.fingerprint,
                   last_diagnostic = excluded.last_diagnostic, strike_count = excluded.strike_count,
                   updated_at = excluded.updated_at",
                params![project_id.as_str(), store.as_str(), branch.as_str(), RECONCILE_TIMEOUT,
                    fingerprint.as_str(), diagnostic.as_str(), strikes, now],
            ).await?;
            let mut rows = conn.query(
                "SELECT DISTINCT issue_id FROM jobs
                 WHERE project_id = ?1 AND branch = ?2 AND issue_id IS NOT NULL",
                params![project_id.as_str(), branch.as_str()],
            ).await?;
            let mut owners = Vec::new();
            while let Some(row) = rows.next().await? {
                owners.push(row.text(0)?);
            }
            drop(rows);
            for issue_id in owners {
                crate::issues::comments::insert_conn(
                    conn, &cairn_common::ids::mint_child(&issue_id), &issue_id,
                    &diagnostic, "agent", now,
                ).await?;
            }
            log::warn!("jj reconcile: {branch} timeout cooldown recorded: coordinates={fingerprint}, strikes={strikes}, retry_at={retry_at}");
            Ok(diagnostic)
        })
    }).await.map_err(|error| format!("record reconcile timeout cooldown: {error}"))
}

async fn release_reconcile_quarantine(
    db: &LocalDb,
    project_id: &str,
    store_path: &Path,
    bookmark: &str,
) -> Result<(), String> {
    db.execute(
        "DELETE FROM jj_reconcile_quarantines
         WHERE project_id = ?1 AND store_path = ?2 AND bookmark = ?3",
        params![project_id, store_path.to_string_lossy().as_ref(), bookmark],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("release reconcile quarantine: {error}"))
}

#[derive(Debug, Clone)]
pub(super) struct SiblingJob {
    pub(super) id: String,
    base_commit: Option<String>,
    /// Whether the job owns the branch rather than sharing it with the job
    /// that delegated it ([`OWNS_BRANCH`]).
    pub(super) owns_branch: bool,
}

/// The jobs on a branch that are told about it: its owners. A task working on
/// its delegating parent's branch shares the branch but is not the one to
/// resolve, carry or decide anything about it. When none of the live jobs owns
/// the branch (its owner ended while a task it delegated still runs), the
/// jobs sharing it stand in, as they do for
/// [`preparation::live_branch_owner`].
pub(super) fn branch_owners(jobs: &[SiblingJob]) -> Vec<&SiblingJob> {
    let owners: Vec<&SiblingJob> = jobs.iter().filter(|job| job.owns_branch).collect();
    if owners.is_empty() {
        jobs.iter().collect()
    } else {
        owners
    }
}

#[derive(Debug)]
pub(crate) struct DefaultReconcileProject {
    pub(crate) id: String,
    pub(crate) repo_path: String,
    pub(crate) default_branch: String,
}

/// Queue non-waking notifications for in-flight siblings whose changes overlap
/// a merged job that advanced their shared base branch.
pub(crate) async fn notify_downstream_of_base_advance(
    orch: &Orchestrator,
    merged_job_id: &str,
) -> Result<(), String> {
    let db = owning_db_for_job(&orch.db, merged_job_id)
        .await
        .map_err(|error| {
            log::warn!(
                "Skipping base advance notify for owner {merged_job_id}: failed to route owning database: {error}"
            );
            error.to_string()
        })?;
    let Some(merged_job) = load_merged_job_for_owner(&db, merged_job_id).await? else {
        log::debug!(
            "Skipping base advance notify: no implementation job found for owner {}",
            merged_job_id
        );
        return Ok(());
    };
    let Some(base_branch) = merged_job.base_branch.as_deref() else {
        log::debug!(
            "Skipping base advance notify for job {}: no base_branch",
            merged_job.id
        );
        return Ok(());
    };

    // What a landing owes the base it moved: the default's origin import,
    // base checks and service/code-map notices, and re-pointing work cut from
    // the merged branch. No branch cut from the base is touched here.
    let Some(repo_path) = load_project_repo_path(&orch.db, &merged_job.project_id).await? else {
        log::debug!(
            "Skipping base advance reconcile for job {}: no project repo_path",
            merged_job.id
        );
        return Ok(());
    };
    reconcile_jj_downstream(orch, &db, &merged_job, base_branch, &repo_path).await
}

/// Project conflict markers into every live checkout holding this branch, and
/// report back only what the executor CONFIRMED.
///
/// The return value is the marker state to persist. It is deliberately
/// pessimistic: with no live checkout there is nothing to mark, and an executor
/// that could not be reached leaves the session `Pending` so the retry pass picks
/// it up rather than a wake claiming markers exist.
async fn materialize_markers_for_branch(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    diagnostic: &crate::jj::ConflictDiagnostic,
) -> (MarkerState, Option<String>, Vec<(String, String)>) {
    let paths = diagnostic.conflicting_paths();
    if paths.is_empty() {
        return (
            MarkerState::NotMaterialized,
            Some("the rebase named no conflicting paths".to_string()),
            Vec::new(),
        );
    }
    let (Some(ours), Some(theirs)) = (diagnostic.ours.as_deref(), diagnostic.theirs.as_deref())
    else {
        return (
            MarkerState::NotMaterialized,
            Some("the conflict did not record both merge sides".to_string()),
            Vec::new(),
        );
    };
    let Ok(leases) = load_live_terminal_leases(db, project_id, branch).await else {
        return (
            MarkerState::Pending,
            Some("could not enumerate this branch's live checkouts".to_string()),
            Vec::new(),
        );
    };
    if leases.is_empty() {
        return (
            MarkerState::NotMaterialized,
            Some("this branch has no live checkout to project markers into".to_string()),
            Vec::new(),
        );
    }

    let mut confirmed: Vec<(String, String)> = Vec::new();
    let mut last_error = None;
    let mut confirmations = 0usize;
    let mut attempts = 0usize;
    let mut retryable = false;
    for (residency_holder, incarnation_id, cell_epoch, _job_id) in leases {
        attempts += 1;
        let Some(holder) =
            cairn_common::executor_protocol::ResidencyHolder::parse_storage_key(&residency_holder)
        else {
            last_error = Some(format!(
                "unreadable execution environment {residency_holder}"
            ));
            continue;
        };
        let result = orch
            .fleet
            .operate_residency(
                orch,
                ResidencyOperation::MaterializeConflict {
                    fence: cairn_common::executor_protocol::ResidencyFence {
                        holder,
                        incarnation_id,
                        cell_epoch,
                    },
                    request: cairn_common::executor_protocol::ConflictMaterializationRequest {
                        expected_head: ours.to_string(),
                        base_commit: diagnostic.base.clone(),
                        ours_commit: ours.to_string(),
                        theirs_commit: theirs.to_string(),
                        paths: paths.clone(),
                    },
                },
            )
            .await;
        match result {
            ResidencyResult::ConflictMaterialized { outcome, .. } => {
                confirmations += 1;
                for path in outcome.paths {
                    confirmed.push((path.path, path.disposition.as_str().to_string()));
                }
            }
            ResidencyResult::Failed {
                kind, diagnostic, ..
            } => {
                // An unreachable executor is a fact about the link, not about
                // this branch: keep the request alive for the retry pass.
                retryable |= matches!(
                    kind,
                    cairn_common::executor_protocol::ResidencyFailureKind::Unavailable
                );
                last_error = Some(diagnostic);
            }
            other => last_error = Some(format!("unexpected materialization reply: {other:?}")),
        }
    }

    // Marker state is stored once per branch, but materialization happens per
    // CHECKOUT. So a partial success cannot be recorded as `Materialized`: the
    // resource would then tell an agent whose checkout got nothing that markers
    // are present in it, which is precisely the false claim this whole design
    // exists to prevent. Only unanimity is confirmation; anything short of it
    // stays retryable and reads as "not confirmed".
    if confirmations == attempts && confirmations > 0 {
        (MarkerState::Materialized, last_error, confirmed)
    } else if confirmations > 0 {
        (
            MarkerState::Pending,
            Some(format!(
                "markers landed in {confirmations} of {attempts} live checkouts{}",
                last_error
                    .map(|error| format!("; last failure: {error}"))
                    .unwrap_or_default()
            )),
            confirmed,
        )
    } else if retryable {
        (MarkerState::Pending, last_error, Vec::new())
    } else {
        (MarkerState::Failed, last_error, Vec::new())
    }
}

async fn refresh_residencies_for_branch(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    new_tip: &str,
) -> usize {
    let rows = load_live_terminal_leases(db, project_id, branch).await;
    let Ok(leases) = rows else {
        log::error!("committed branch advance could not enumerate residencies: {rows:?}");
        return 1;
    };
    let mut failed = 0;
    for (residency_holder, incarnation_id, cell_epoch, job_id) in leases {
        let Some(holder) =
            cairn_common::executor_protocol::ResidencyHolder::parse_storage_key(&residency_holder)
        else {
            failed += 1;
            log::error!(
                "terminal row records an unreadable execution environment {residency_holder}"
            );
            continue;
        };
        let result = orch
            .fleet
            .operate_residency(
                orch,
                ResidencyOperation::RefreshCheckout {
                    fence: cairn_common::executor_protocol::ResidencyFence {
                        holder,
                        incarnation_id: incarnation_id.clone(),
                        cell_epoch,
                    },
                    base_commit: new_tip.to_string(),
                    require_clean: false,
                },
            )
            .await;
        if let ResidencyResult::Failed {
            kind, diagnostic, ..
        } = result
        {
            if kind == cairn_common::executor_protocol::ResidencyFailureKind::Unavailable {
                failed += 1;
                log::warn!(
                    "terminal lease {residency_holder} could not be refreshed while its executor is disconnected: {diagnostic}"
                );
                continue;
            }
            // A lease its executor no longer has, or has moved to a newer cell
            // epoch, holds none of the processes this branch's running
            // terminals were bound to, so there is no checkout of theirs left to
            // refresh. Those terminals are not this path's to settle: terminal
            // liveness reconciliation ends each one through the ordinary exit
            // with the reason its executor gave, which is what a waiting agent
            // needs, rather than a blocking note about a refresh that is moot.
            if matches!(
                kind,
                cairn_common::executor_protocol::ResidencyFailureKind::NotFound
                    | cairn_common::executor_protocol::ResidencyFailureKind::StaleEpoch
            ) {
                log::info!(
                    "terminal lease {residency_holder} at cell epoch {cell_epoch} is no longer current on its executor ({diagnostic}); terminal liveness reconciliation settles the terminals bound to it"
                );
                continue;
            }
            failed += 1;
            log::error!("committed branch advance could not refresh terminal lease {residency_holder}: {diagnostic}");
            if let Some(run_id) = latest_run_for_job(db, &job_id) {
                let note = format!(
                    "⛔ BLOCKING [Terminal head reconciliation] The branch commit succeeded, but Cairn could not advance terminal lease {residency_holder} to {new_tip}. The committed branch was not rolled back. Exact executor diagnostic: {diagnostic}"
                );
                if let Err(error) =
                    queue_system_direct(orch, &run_id, &note, DeliveryUrgency::Steer)
                {
                    log::error!(
                        "could not notify terminal owner {job_id} after refresh failure: {error}"
                    );
                }
            }
        }
    }
    failed += crate::dev_instances::sync_live_branch_instances(orch, project_id, branch, new_tip)
        .await
        .len();
    failed
}

async fn load_live_terminal_leases(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> crate::storage::DbResult<Vec<(String, String, u64, String)>> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT DISTINCT t.residency_holder, t.residency_incarnation_id, t.cell_epoch, t.job_id
             FROM job_terminals t JOIN jobs j ON j.id = t.job_id
             WHERE j.project_id = ?1 AND j.branch = ?2
               AND t.status = 'running' AND t.residency_holder IS NOT NULL",
                    (project_id.as_str(), branch.as_str()),
                )
                .await?;
            let mut leases = Vec::new();
            while let Some(row) = rows.next().await? {
                leases.push((
                    row.text(0)?,
                    row.text(1)?,
                    row.get::<i64>(2)? as u64,
                    row.text(3)?,
                ));
            }
            Ok(leases)
        })
    })
    .await
}

/// The base-movement follow-up of a landing: for a default base, import
/// origin's copy and start base checks and service and code-map notices; for
/// any base, re-point work cut from the merged branch onto the base it merged
/// into. Branches cut from the base keep their literal heads until their own
/// next commit, requested check or run, or merge.
async fn reconcile_jj_downstream(
    orch: &Orchestrator,
    db: &LocalDb,
    merged_job: &MergedJob,
    base_branch: &str,
    repo_path: &str,
) -> Result<(), String> {
    // The store owns the LOCAL fold, but it does not own what origin did with the
    // pull request. An operator squash-merging on GitHub mints a commit id the
    // store has never seen, so origin moves and the store's local default
    // bookmark does not — and this path used to neither fetch nor import, on the
    // reasoning that a Cairn-driven fold needs neither. That reasoning holds for
    // the fold and fails for the merge, which is why nearly every merge left the
    // bookmark to conflict. Reconcile before the sibling gate, and fetch first so
    // the commit origin holds is actually in the store to reconcile onto.
    if branch_is_project_default(db, &merged_job.project_id, base_branch).await {
        let repo = Path::new(repo_path);
        if project_has_origin(orch, repo) {
            let ctx = format!("merged job {}", merged_job.id);
            let record = crate::orchestrator::base_branch_sync::FetchRecord {
                project_id: &merged_job.project_id,
                source: crate::orchestrator::base_branch_sync::Source::Fetch,
            };
            match crate::orchestrator::base_branch_sync::fetch_origin_serialized(
                orch,
                repo,
                base_branch,
                Some(record),
            )
            .await
            {
                Ok(()) => {
                    // Logged inside; the merged-job path has no sync state to
                    // report it to, and the next poll re-checks the bookmark.
                    let _ = reconcile_store_default_bookmark(
                        orch,
                        db,
                        &merged_job.project_id,
                        repo,
                        base_branch,
                        &ctx,
                    )
                    .await;
                }
                Err(error) => log::warn!(
                    "{ctx}: git fetch origin failed; skipping default bookmark reconcile: {error}"
                ),
            }
        }
        crate::execution::checks_main::spawn(
            orch,
            merged_job.project_id.clone(),
            repo_path.to_string(),
            base_branch.to_string(),
            crate::execution::checks_main::Trigger::Merge,
        );
        // The code map projects the base tree, so this merge is exactly what
        // makes the stored one describe a tree that is no longer the base.
        // A service runs its package as it stands at the base tip, so this
        // merge may have changed what one of them should be running.
        crate::project_services::supervisor::note_base_advance(orch, &merged_job.project_id);
        crate::projects::codemap::note_base_advance(
            orch,
            db,
            &merged_job.project_id,
            repo_path,
            base_branch,
        )
        .await;
    }

    // Anything cut FROM the branch that just merged loses its base the moment
    // that branch is deleted. Re-point it before the sibling set is read, so
    // those children inherit this advance in the same pass instead of being
    // stranded by it.
    if let Some(merged_branch) = merged_job
        .branch
        .as_deref()
        .filter(|branch| !branch.is_empty() && *branch != base_branch)
    {
        let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
        let _reservation = orch
            .reserve_branches(
                &store,
                &[merged_branch],
                "retarget children after an observed merge",
                crate::mcp::vcs::STORE_LOCK_TIMEOUT,
            )
            .await?;
        let guard = orch
            .acquire_jj_store_lock_with_timeout(
                &store,
                crate::orchestrator::StoreLockClass::Foreground,
                format!("repoint children of merged branch {merged_branch}"),
                Some(crate::mcp::vcs::STORE_LOCK_TIMEOUT),
            )
            .await
            .map_err(|()| format!("store busy while repointing children of `{merged_branch}`; no relationship was changed"))?;
        if let Err(error) = repoint_children_of_merged_branch(
            db,
            &guard,
            &merged_job.project_id,
            merged_branch,
            base_branch,
        )
        .await
        {
            log::warn!(
                "merged job {}: could not re-point work cut from `{merged_branch}` onto \
                 `{base_branch}`: {error}",
                merged_job.id
            );
        }
    }

    Ok(())
}

/// Bring the store's local bookmark for the project's default branch back into
/// agreement with origin, under the per-store lock.
///
/// Every caller runs this BEFORE deciding whether any sibling needs rebasing.
/// That ordering is the whole point: the import and repair used to be reachable
/// only through store provisioning, downstream of an `if siblings.is_empty()`
/// return, so the overwhelmingly common shape — a PR merges with nothing else in
/// flight — reconciled nothing at all and left the tracked bookmark to conflict
/// on the next operation that touched it. Reconciliation is not a step the
/// sibling rebase needs; it is what keeps the store able to answer for the
/// default branch.
///
/// The backward repair is gated HERE rather than at each call site, so no caller
/// can reach it with a branch origin does not own. `branch` must be the
/// project's configured default branch; an integration or agent branch
/// legitimately holds sealed work origin has not seen and is left untouched.
///
/// Non-fatal end to end. A reconciliation failure is operator-facing diagnostic
/// output; it never becomes a message to an agent. The result is returned so
/// the default-branch sync can report a store whose `main` did not move as
/// stale and retry it, instead of reading `<default>@origin` as success.
async fn reconcile_store_default_bookmark(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    repo_path: &Path,
    branch: &str,
    ctx: &str,
) -> Result<(), String> {
    if !branch_is_project_default(db, project_id, branch).await {
        log::debug!(
            "{ctx}: `{branch}` is not project {project_id}'s default branch; not reconciling it onto origin"
        );
        return Ok(());
    }
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let reservation = orch
        .reserve_branches(
            &store,
            &[branch],
            "default bookmark reconcile",
            crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        )
        .await?;
    let guard = orch
        .acquire_jj_store_lock(
            &store,
            crate::orchestrator::StoreLockClass::Background,
            format!("default bookmark reconcile: {ctx}"),
        )
        .await;
    let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
    let _phase = guard.phase("default bookmark reconcile");
    orch.refuse_foreign_reservations(&guard, Some(&reservation))?;
    let preserved = crate::orchestrator::branch_recovery::preserved_import_bookmarks(
        orch,
        &store,
        &[branch],
        &format!("Reconciling `{branch}` onto origin"),
    )
    .await?;
    let jj = jj.preserving_import_bookmarks(preserved);
    if let Err(error) = crate::jj::ensure_store_initialized(&jj, &store, repo_path) {
        log::warn!("{ctx}: ensure jj store failed; skipping default bookmark reconcile: {error}");
        return Err(format!("ensure jj store failed: {error}"));
    }
    // The reconcile imports (which may move any bookmark) and then moves this
    // one: admit this branch's writer, and bracket the import for every other.
    let owning = owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    guard
        .branch_write_permit(
            &owning,
            &reservation,
            project_id,
            branch,
            crate::orchestrator::BatchAdmission::None,
        )
        .await?;
    let (reconcile_store, reconcile_branch) = (store.clone(), branch.to_string());
    let reconciled = orch
        .bracket_store_mutation(guard, Some(&reservation), owning, project_id, move || {
            crate::jj::reconcile_tracked_bookmark(&jj, &reconcile_store, &reconcile_branch)
        })
        .await
        .map(|(_guard, outcome)| outcome);
    match reconciled {
        Ok(outcome) => {
            log::debug!("{ctx}: default bookmark `{branch}` reconcile: {outcome:?}");
            Ok(())
        }
        Err(error) => {
            log::error!(
                "{ctx}: default bookmark `{branch}` did not reconcile against origin: {error}"
            );
            Err(format!(
                "default bookmark `{branch}` did not reconcile against origin: {error}"
            ))
        }
    }
}

/// Bring origin's advanced default branch into the shared store: the default
/// bookmark only. Branches cut from it are not touched; each is carried onto
/// the new tip at its own next commit, requested check or run, or merge.
///
/// Reached only through `base_branch_sync::sync`, which single-flights it per
/// project. Its triggers are the GitHub `push` webhook, the default-branch poll
/// floor, and Cairn's own GitHub merge path; startup catch-up has its own
/// before/after variant below.
///
/// Returns `Err` when the store could not be brought to origin (the fetch or the
/// import failed), so the caller can say the store is stale and why. Failures
/// past that point are logged and do not make the store's copy of origin wrong.
pub(crate) async fn reconcile_default_advance(
    orch: &Orchestrator,
    project_id: &str,
    default_branch: &str,
) -> Result<(), String> {
    let db = owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| {
            log::warn!(
                "Skipping external advance reconcile for project {project_id}: failed to route owning database: {error}"
            );
            error.to_string()
        })?;
    let Some(repo_path) = load_project_repo_path(&orch.db, project_id).await? else {
        log::debug!("Skipping external advance reconcile: no repo_path for project {project_id}");
        return Ok(());
    };
    // Arm independently of sibling reconciliation. Its own retry refreshes and
    // reconciles the canonical bookmark, so a transient failure below cannot
    // erase the observed main advance.
    crate::execution::checks_main::spawn(
        orch,
        project_id.to_string(),
        repo_path.clone(),
        default_branch.to_string(),
        crate::execution::checks_main::Trigger::Merge,
    );
    // Same reasoning as the Cairn-merge path: an advance the webhook observed
    // moves the base the code map projects, whether or not Cairn drove it.
    crate::projects::codemap::note_base_advance(orch, &db, project_id, &repo_path, default_branch)
        .await;
    crate::project_services::supervisor::note_base_advance(orch, project_id);
    // Bring the advanced tip into the shared store and reconcile the default
    // bookmark onto it — BEFORE looking at siblings, because this is owed whether
    // or not anything downstream needs rebasing.
    let repo_path_path = Path::new(&repo_path);
    // Transfer objects and update the ordinary Git repository's origin-tracking
    // refs before entering the jj critical section. Git object writes are
    // content-addressed and additive; jj observes the fetched refs only when the
    // locked import below runs, preserving the store's single-writer discipline.
    crate::orchestrator::base_branch_sync::fetch_origin_serialized(
        orch,
        repo_path_path,
        default_branch,
        Some(crate::orchestrator::base_branch_sync::FetchRecord {
            project_id,
            source: crate::orchestrator::base_branch_sync::Source::Fetch,
        }),
    )
    .await?;
    // Provision and import the store before anything downstream depends on it
    // resolving. A store that cannot be provisioned aborts the whole reconcile
    // rather than letting the sibling pass fail on an unresolvable dest.
    {
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, repo_path_path);
        let reservation = orch
            .reserve_branches(
                &store,
                &[default_branch],
                "external base import",
                crate::mcp::vcs::STORE_LOCK_TIMEOUT,
            )
            .await?;
        let store_guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                format!("external import on {default_branch}"),
            )
            .await;
        let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
        let _phase = store_guard.phase("git ref import");
        let preserved = crate::orchestrator::branch_recovery::preserved_import_bookmarks(
            orch,
            &store,
            &[default_branch],
            &format!("Importing origin's `{default_branch}`"),
        )
        .await?;
        let jj = jj.preserving_import_bookmarks(preserved);
        // Origin is the authority for the branch just fetched, so its import
        // observes it wherever it went, backwards included: admit that
        // branch's writer. Every other tracked branch stays guarded, and the
        // bracket marks any owed branch the import moved anyway.
        store_guard
            .branch_write_permit(
                &db,
                &reservation,
                project_id,
                default_branch,
                crate::orchestrator::BatchAdmission::None,
            )
            .await?;
        let (import_store, import_repo, import_branch) = (
            store.clone(),
            repo_path_path.to_path_buf(),
            default_branch.to_string(),
        );
        orch.bracket_store_mutation(
            store_guard,
            Some(&reservation),
            db.clone(),
            project_id,
            move || {
                crate::jj::ensure_store_initialized(&jj, &import_store, &import_repo)
                    .and_then(|()| {
                        crate::jj::import_git_with_remote_authority(
                            &jj,
                            &import_store,
                            &import_branch,
                        )
                    })
                    .map_err(|error| format!("importing origin into the store failed: {error}"))
            },
        )
        .await?;
    }
    // A bookmark that did not move is reported after the downstream work, which
    // rebases onto `<default>@origin` and does not need it.
    let bookmark = reconcile_store_default_bookmark(
        orch,
        &db,
        project_id,
        repo_path_path,
        default_branch,
        &format!("external advance on {default_branch}"),
    )
    .await;
    bookmark
}

/// Bring an advance made on origin to a STORE-OWNED base branch (an issue
/// parent's integration branch) into the store, and reconcile everything
/// downstream of it: the jobs whose own branch it is, and every live child cut
/// from it, which is replayed onto the new tip through the ordinary base-advance
/// path exactly as a Cairn-driven merge into the branch would replay it.
///
/// The store owns this branch, so origin's copy is taken only when it is a
/// pure fast-forward ([`crate::jj::fast_forward_branch_from_origin`]): a pull
/// request into the branch merged in GitHub's UI, GitHub's "update branch".
/// Anything else is reported, to the sync state and to the branch's own jobs,
/// and moves nothing.
///
/// Reached only through `base_branch_sync::sync`, which single-flights it per
/// branch. Returns `Err` when the store did not take origin's copy, so reads
/// and publication can say why the store is behind.
pub(crate) async fn reconcile_base_branch_advance(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
) -> Result<(), String> {
    let db = owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    let Some(repo_path) = load_project_repo_path(&orch.db, project_id).await? else {
        return Ok(());
    };
    let repo = Path::new(&repo_path);
    if !project_has_origin(orch, repo) {
        return Ok(());
    }
    let record = crate::orchestrator::base_branch_sync::FetchRecord {
        project_id,
        source: crate::orchestrator::base_branch_sync::Source::Fetch,
    };
    let Some(observed) =
        crate::orchestrator::base_branch_sync::fetch_origin_observation_serialized(
            orch, repo, branch, record,
        )
        .await?
    else {
        log::debug!("origin holds no `{branch}` for project {project_id}; nothing to take");
        return Ok(());
    };
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo);
    let reservation = orch
        .reserve_branches(
            &store,
            &[branch],
            "origin fast-forward",
            crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        )
        .await?;
    let outcome = {
        let guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                format!("origin fast-forward of {branch}"),
            )
            .await;
        let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
        let _phase = guard.phase("fast-forward from origin");
        let preserved = crate::orchestrator::branch_recovery::preserved_import_bookmarks(
            orch,
            &store,
            &[branch],
            &format!("Fast-forwarding `{branch}` from origin"),
        )
        .await?;
        let jj = jj.preserving_import_bookmarks(preserved);
        // The fast-forward is a refs-only take of this one branch: admit its
        // writer before it moves. The bracket around it is kept, though the
        // take no longer imports or moves any other bookmark.
        guard
            .branch_write_permit(
                &db,
                &reservation,
                project_id,
                branch,
                crate::orchestrator::BatchAdmission::None,
            )
            .await?;
        let (ff_store, ff_repo, ff_branch) =
            (store.clone(), repo.to_path_buf(), branch.to_string());
        let (_guard, outcome) = orch
            .bracket_store_mutation(
                guard,
                Some(&reservation),
                db.clone(),
                project_id,
                move || {
                    crate::jj::ensure_store_initialized(&jj, &ff_store, &ff_repo).and_then(|()| {
                        crate::jj::fast_forward_branch_from_origin(
                            &jj, &ff_store, &ff_branch, &observed,
                        )
                    })
                },
            )
            .await?;
        outcome
    };
    match outcome {
        crate::jj::OriginFastForward::Current { .. } | crate::jj::OriginFastForward::NoBookmark => {
            Ok(())
        }
        crate::jj::OriginFastForward::Superseded { local, remote } => {
            log::debug!(
                "origin's `{branch}` ({remote}) is the store's own publication, since rewritten \
                 to {local}; its next publication replaces it"
            );
            Ok(())
        }
        crate::jj::OriginFastForward::Diverged { local, remote } => {
            let reason = format!(
                "origin's `{branch}` ({remote}) and the store's ({local}) each hold commits the \
                 other lacks, so it is not a fast-forward and the store did not take it"
            );
            notify_on_branch_jobs(
                orch,
                &db,
                project_id,
                branch,
                &build_origin_divergence_note(branch, &local, &remote),
                DeliveryUrgency::Steer,
                &format!("origin-divergence:{project_id}:{branch}:{local}:{remote}"),
            )
            .await;
            Err(reason)
        }
        crate::jj::OriginFastForward::Advanced { from, to } => {
            notify_on_branch_jobs(
                orch,
                &db,
                project_id,
                branch,
                &build_origin_fast_forward_note(branch, &from, &to),
                DeliveryUrgency::Passive,
                &format!("origin-fast-forward:{project_id}:{branch}:{to}"),
            )
            .await;
            Ok(())
        }
    }
}

/// Tell the owners of `branch` ([`branch_owners`]) something about it, once
/// per `key`. A task sharing the branch neither publishes it nor decides how
/// a divergence from origin is combined.
async fn notify_on_branch_jobs(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    note: &str,
    urgency: DeliveryUrgency,
    key: &str,
) {
    let jobs = match load_on_branch_jobs(db, project_id, branch).await {
        Ok(jobs) => jobs,
        Err(error) => {
            log::warn!("could not load the jobs on `{branch}` to notify: {error}");
            return;
        }
    };
    for job in branch_owners(&jobs) {
        let Some(run_id) = latest_run_for_job(db, &job.id) else {
            continue;
        };
        if let Err(error) =
            queue_system_direct_once(orch, &run_id, note, urgency, &format!("{key}:{run_id}"))
        {
            log::warn!("could not notify job {} about `{branch}`: {error}", job.id);
        }
    }
}

fn build_origin_fast_forward_note(branch: &str, from: &str, to: &str) -> String {
    format!(
        "[Branch update] Your branch `{branch}` advanced on origin outside Cairn (a pull request \
         into it merged in GitHub, or a push), from `{from}` to `{to}`. The store fast-forwarded \
         to it. Work cut from this branch remains at its literal head until its next commit, \
         requested check/run capture, or merge prepares it. A clean advance is not a green build."
    )
}

fn build_origin_divergence_note(branch: &str, local: &str, remote: &str) -> String {
    format!(
        "⛔ [Branch update] Origin's `{branch}` is at `{remote}`, which the store's `{local}` \
         does not contain, and it does not contain the store's either: each side holds commits \
         the other lacks. The store will not move your branch sideways, so it stays at \
         `{local}`, and publishing it is refused until the two agree. Read `cairn:~/rebase` for \
         both commits; combining them is a decision for you or your thread, not something to \
         force-push."
    )
}

/// One-time startup catch-up for remote default-branch advances that landed while
/// Cairn was closed. This is intentionally not a sweep: no-remote projects are
/// skipped because nothing outside Cairn can advance them, and remote projects
/// only reconcile when fetching `origin` actually changes the stored remote
/// default tip. An unchanged base never reaches the sibling rebase path.
pub(crate) async fn reconcile_startup_remote_default_advances(orch: &Orchestrator) {
    let projects = match load_projects_for_default_reconcile(orch).await {
        Ok(projects) => projects,
        Err(error) => {
            log::warn!("startup default-advance catch-up: failed to load projects: {error}");
            return;
        }
    };
    for (db, project) in projects {
        if !project_has_origin(orch, Path::new(&project.repo_path)) {
            log::debug!(
                "startup default-advance catch-up: skipping project {} with no origin remote",
                project.id
            );
            continue;
        }
        if let Err(error) = reconcile_startup_remote_default_advance(orch, &db, &project).await {
            log::warn!(
                "startup default-advance catch-up for project {} failed: {error}",
                project.id
            );
        }
    }
}

async fn reconcile_startup_remote_default_advance(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    project: &DefaultReconcileProject,
) -> Result<(), String> {
    let repo_path = Path::new(&project.repo_path);
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let ctx = format!("startup remote advance on {}", project.default_branch);
    {
        let _store_guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                format!(
                    "startup remote advance snapshot on {}",
                    project.default_branch
                ),
            )
            .await;
        let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
        // The store import may move any bookmark; owed branches are bracketed.
        let (import_jj, import_store, import_repo) =
            (jj.clone(), store.clone(), repo_path.to_path_buf());
        let imported = orch
            .bracket_store_mutation(_store_guard, None, db.clone(), &project.id, move || {
                crate::jj::ensure_project_store(&import_jj, &import_store, &import_repo)
            })
            .await;
        if let Err(error) = imported {
            log::warn!("{ctx}: ensure store failed: {error}");
            return Ok(());
        }
    }

    if let Err(error) = crate::orchestrator::base_branch_sync::fetch_origin_serialized(
        orch,
        repo_path,
        &project.default_branch,
        Some(crate::orchestrator::base_branch_sync::FetchRecord {
            project_id: &project.id,
            source: crate::orchestrator::base_branch_sync::Source::Startup,
        }),
    )
    .await
    {
        log::warn!("{ctx}: git fetch origin failed: {error}");
        return Ok(());
    }

    // Reconcile the default bookmark unconditionally on the way through. A
    // restart is exactly when the store is most likely to be holding a bookmark
    // that conflicts with what origin did while Cairn was closed, and the old
    // shape — gated on in-flight siblings, gated again on the tip having moved —
    // meant a restart could not heal it either.
    // Logged inside; a bookmark left behind here is what the poll's local
    // bookmark check catches and re-syncs.
    let _ = reconcile_store_default_bookmark(
        orch,
        db,
        &project.id,
        repo_path,
        &project.default_branch,
        &ctx,
    )
    .await;
    Ok(())
}

fn project_has_origin(orch: &Orchestrator, repo_path: &Path) -> bool {
    orch.services
        .git
        .remote_get_url(repo_path)
        .ok()
        .is_some_and(|url| !url.trim().is_empty())
}

/// Run one pass of a periodic loop so that a panic inside it becomes a value
/// here instead of unwinding the caller.
///
/// A panic that escapes into a background loop retires that loop for the
/// lifetime of the process without a log line. Joining a child task converts
/// the panic into an `Err` the loop can report and continue past.
pub(crate) async fn supervised_sweep_pass<F>(pass: F) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(pass)
        .await
        .map_err(|error| format!("sweep pass did not complete: {error}"))
}

/// Re-attempt marker materializations left pending, typically because the
/// executor holding the checkout was unreachable at conflict time.
///
/// Pending is deliberately a state rather than a silent failure: the wake told
/// the agent markers were not confirmed, and this is what eventually makes that
/// sentence change. A session whose markers land here is not re-notified — the
/// agent already has the coordinates and the resource — so this only upgrades
/// what `cairn:~/rebase` reports.
pub(super) async fn retry_pending_materializations(orch: &Orchestrator, db: &LocalDb) {
    let pending = match load_pending_materializations(db).await {
        Ok(pending) => pending,
        Err(error) => {
            log::warn!("could not enumerate pending marker materializations: {error}");
            return;
        }
    };
    for (intent_id, bookmark) in pending {
        let session =
            match crate::orchestrator::conflict_session::load_active_session(db, &bookmark).await {
                Ok(Some(session)) if session.intent_id == intent_id => session,
                // The session closed or was superseded while this was queued. There
                // is nothing left to mark, and marking it would be scaffolding for a
                // merge nobody will perform.
                Ok(_) => continue,
                Err(error) => {
                    log::warn!("could not reload pending session for {bookmark}: {error}");
                    continue;
                }
            };
        let diagnostic = crate::jj::ConflictDiagnostic {
            base: session.base.clone(),
            ours: session.ours.clone(),
            theirs: session.theirs.clone(),
            conflicted_tip: session.conflicted_tip.clone(),
            condition: crate::jj::ConflictCondition::from_stored(&session.condition),
            validation: None,
            incoming: session
                .files
                .iter()
                .filter(|file| file.is_conflicting())
                .map(|file| crate::jj::IncomingFile {
                    path: file.path.clone(),
                    status: file.status.clone(),
                    classification: crate::jj::IncomingClassification::Conflicting,
                })
                .collect(),
            scaffolding: None,
        };
        let (state, marker_diagnostic, dispositions) =
            materialize_markers_for_branch(orch, db, &session.project_id, &bookmark, &diagnostic)
                .await;
        if let Err(error) = record_marker_state(
            db,
            &intent_id,
            &bookmark,
            state,
            marker_diagnostic.as_deref(),
            &dispositions,
        )
        .await
        {
            log::warn!("could not record retried marker state for {bookmark}: {error}");
        }
    }
}

async fn load_pending_materializations(db: &LocalDb) -> Result<Vec<(String, String)>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT intent_id, bookmark FROM jj_reconcile_items
                     WHERE marker_state = 'pending' AND resolution_state = 'open'",
                    (),
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
    .map_err(|error| format!("load pending materializations: {error}"))
}

struct ReconcileItemUpdate<'a> {
    intent_id: &'a str,
    bookmark: &'a str,
    observed_tip: Option<&'a str>,
    status: &'a str,
    failure_kind: Option<&'a str>,
    outcome_kind: Option<&'a str>,
    fingerprint: Option<&'a str>,
    diagnostic: Option<&'a str>,
}

async fn persist_reconcile_item(db: &LocalDb, item: ReconcileItemUpdate<'_>) -> Result<(), String> {
    db.execute(
        "INSERT INTO jj_reconcile_items
         (intent_id, bookmark, observed_tip, status, failure_kind,
          outcome_kind, suppression_fingerprint, last_diagnostic, attempt_count, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9)
         ON CONFLICT(intent_id, bookmark) DO UPDATE SET
           observed_tip = excluded.observed_tip,
           status = excluded.status,
           failure_kind = excluded.failure_kind,
           outcome_kind = excluded.outcome_kind,
           suppression_fingerprint = excluded.suppression_fingerprint,
           last_diagnostic = excluded.last_diagnostic,
           attempt_count = jj_reconcile_items.attempt_count + 1,
           updated_at = excluded.updated_at",
        params![
            item.intent_id,
            item.bookmark,
            item.observed_tip,
            item.status,
            item.failure_kind,
            item.outcome_kind,
            item.fingerprint,
            item.diagnostic,
            chrono::Utc::now().timestamp()
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("persist reconcile item progress: {error}"))
}

/// Filter a set of reconciled sibling branches down to those this reconcile
/// actually rewrote: a branch whose commit id changed between the before/after
/// snapshots. A double-fire reconcile at the same dest tip is a `jj rebase` no-op,
/// so the commit id is unchanged → the branch is filtered out and not re-notified
/// (conflicted or clean). When either snapshot is missing (an unexpected resolve
/// failure), notify conservatively rather than silently dropping a real change.
/// Record where a sibling's branch now sits after this reconcile rebased it.
///
/// `jobs.base_commit` is a **record of where a branch was last anchored**, not a
/// coordinate any surface resolves against: every reader that presents or acts
/// on a base coordinate derives it live from the store (CAIRN-3224). What is
/// left of the row is archival — a seed for `pack_anchor` and a degraded
/// fallback — and archival values do not need ordering.
///
/// This deliberately does not compare-and-swap, because the CAS it replaces
/// never provided ordering to begin with (CAIRN-3226). Its lost-race branch
/// re-read the row and then swapped against whatever it found, writing this
/// call's value unconditionally: two reconciles racing on one job always ended
/// with the later writer's value, exactly as a plain write does. The ceremony
/// bought nothing and cost two real things — a null row aborted the write with a
/// manufactured error, and a doubly-lost race produced an operator-facing
/// diagnostic for a non-event, the same shape that turned zero-delta planner
/// branches into ⛔ BLOCKING directs (CAIRN-3094 comment #4).
///
/// A write that matches no row means the job was deleted mid-reconcile. That is
/// the graph moving on, not a failure of this bookkeeping, so it is logged and
/// swallowed.
async fn advance_sibling_durable_base(
    db: &LocalDb,
    sibling: &SiblingJob,
    new_base: &str,
) -> Result<(), String> {
    if sibling.base_commit.as_deref() == Some(new_base) {
        return Ok(());
    }
    let changed = db
        .execute(
            "UPDATE jobs SET base_commit = ?2, updated_at = ?3 WHERE id = ?1",
            params![
                sibling.id.as_str(),
                new_base,
                chrono::Utc::now().timestamp()
            ],
        )
        .await
        .map_err(|error| {
            format!(
                "record durable base coordinate for job {}: {error}",
                sibling.id
            )
        })?;
    if changed == 0 {
        log::debug!(
            "jj reconcile: job {} no longer exists; its durable base record was not updated to \
             {new_base}",
            sibling.id
        );
    }
    Ok(())
}

/// How many incoming files a wake lists by name before summarizing the rest. A
/// base advance can legitimately carry hundreds; the point is to make the shape
/// of the incoming change obvious, not to paste a manifest into a message.
const MAX_LISTED_INCOMING: usize = 40;

/// Render one classification's file list, capped and counted.
fn render_incoming_group(label: &str, paths: &[String]) -> String {
    if paths.is_empty() {
        return String::new();
    }
    let listed: Vec<&str> = paths
        .iter()
        .take(MAX_LISTED_INCOMING)
        .map(String::as_str)
        .collect();
    let elided = paths.len().saturating_sub(listed.len());
    let more = if elided > 0 {
        format!(" (and {elided} more)")
    } else {
        String::new()
    };
    format!("\n{label} ({}): {}{more}", paths.len(), listed.join(", "))
}

/// Append the conflict diagnostic to a base-advance note: the incoming change's
/// whole file set, the immutable three-way coordinates, and guidance specific to
/// the condition that was actually found.
///
/// The two lists are the point. A merged PR is one coordinated change across
/// many files, and a report naming only the conflicting subset is how an agent
/// comes to resolve the file it was told about, stop compiling, and have no idea
/// why (CAIRN-3337). Naming the clean-on-retry siblings makes it visible that
/// the tree is mid-change.
///
/// No patch text goes in here. The coordinates are immutable objects, so both
/// sides of the merge are recomputable on demand and a wake stays a wake.
/// What the agent may be told about markers, given only what was confirmed.
///
/// The standing rule is that machinery never instructs an agent to act on state
/// it has not made true. A wake saying "resolve the markers" when none exist is a
/// defect, so every branch here is keyed to the executor's own answer.
/// The condition decides whether editing files is even the right instruction, so
/// it is threaded in: under base drift the two sides already agree, and telling
/// that agent to "write the merged result" is the exact wasted round CAIRN-3327
/// and CAIRN-3328 each burned.
fn marker_guidance(state: MarkerState, condition: crate::jj::ConflictCondition) -> &'static str {
    let drift = condition == crate::jj::ConflictCondition::BaseDrift;
    match state {
        MarkerState::Materialized if drift => {
            "\nAny marker-bearing file in your checkout is scaffolding from this diagnostic, not \
             work to do: the two sides already agree. Request the replay above."
        }
        MarkerState::Materialized => {
            "\nConflict markers have been written into your checkout for the conflicting files \
             above. Resolve them with ordinary file writes; a file still containing marker syntax \
             cannot be committed, so nothing half-resolved can reach history."
        }
        MarkerState::Pending => {
            "\nMarkers were requested but are NOT confirmed present yet, so do not go looking for \
             them. Read both sides of the merge through `cairn:~/rebase` instead; it says so when \
             they land."
        }
        _ if drift => {
            "\nThere are no conflict markers in your checkout, and there is nothing to merge into \
             one. `cairn:~/rebase` carries the coordinates if you want to confirm that for \
             yourself."
        }
        _ => {
            "\nThere are no conflict markers in your checkout. Read both sides of the merge \
             through `cairn:~/rebase` and write the merged result yourself."
        }
    }
}

/// The step that moves a conflicted branch onto its base. Committing merged
/// content does not: the branch's ancestry stays on the old base until a replay
/// is requested on the session. Every note that asks for content work first ends
/// on this one phrase, so no note can stop at "commit it" while another names
/// the replay.
const REQUEST_THE_REPLAY: &str = "then request the replay on `cairn:~/rebase`";

/// The guidance for a genuine content conflict, as its four steps in order:
/// open the session, write the merge, commit it, request the replay.
fn content_conflict_guidance() -> String {
    format!(
        "\nThis is a CONTENT CONFLICT: the two sides genuinely disagree, and your branch has NOT \
         moved onto the new base. Open the session at `cairn:~/rebase`: it lists each conflicting \
         file with both sides' commits, and `cairn:~/rebase?view=merged` shows the two sides \
         merged. Write the merged result of each conflicting file with ordinary edits and commit \
         it on your branch with a normal `commit_msg`, {REQUEST_THE_REPLAY} and follow the \
         resolution that page states; the commit alone does not move your branch. Request \
         `resolution:\"take-committed-tip\"` only when your committed version of every \
         conflicting file already contains the base's side; the page checks that by content and \
         names any file that falls short. Pull the clean-on-retry files across too if your \
         resolution depends on them — they are part of the same coordinated change and your \
         branch will not compile without them. Never rebase or force-push by hand."
    )
}

/// Guidance for a replay withheld because its attempted tree carried conflict
/// scaffolding, whether or not jj also recorded a native conflict.
///
/// It must not read as "nothing to resolve": the branch did NOT take the new
/// base, and a replay requested without new work meets the same tree. It names
/// the refused commit and the entries so the agent (or an operator reading over
/// its shoulder) can see exactly what was refused.
fn scaffolding_guidance(refusal: &crate::jj::ScaffoldingRefusal, ours: Option<&str>) -> String {
    let tip = ours.map(|ours| format!(" {ours}")).unwrap_or_default();
    format!(
        "\nThis replay was WITHHELD BEFORE PUBLICATION. The attempted commit {} carries jj \
         conflict scaffolding ({}), and a tree like that cannot be materialized on any machine. \
         It was never exported: your branch and its git ref stay on your last clean tip{tip}, \
         and a `/rebase` session is open for this conflict at `cairn:~/rebase`. Read both versions \
         of the affected files through that session, commit any \
         merged result your work needs with ordinary edits, {REQUEST_THE_REPLAY}. Never \
         rebase or force-push by hand. If a replay meets the same refusal, the branch needs an \
         operator to repair it in the store.",
        refusal.commit,
        refusal.entries.join(", ")
    )
}

fn append_conflict_diagnostic(
    note: &str,
    diagnostic: Option<&crate::jj::ConflictDiagnostic>,
    marker_state: MarkerState,
) -> String {
    let Some(diagnostic) = diagnostic else {
        return note.to_string();
    };
    let conflicting = diagnostic.conflicting_paths();
    let clean = diagnostic.clean_on_retry_paths();

    let coordinates = match (
        diagnostic.base.as_deref(),
        diagnostic.ours.as_deref(),
        diagnostic.theirs.as_deref(),
    ) {
        (Some(base), Some(ours), Some(theirs)) => format!(
            "\nThree-way coordinates — base {base}, yours {ours}, incoming {theirs}. These are \
             immutable commits: read any file as of either side with `?branch=<commit>` to see \
             both versions without reconstructing them by hand."
        ),
        _ => String::new(),
    };

    let guidance = match (&diagnostic.scaffolding, diagnostic.condition) {
        (Some(refusal), _) => scaffolding_guidance(refusal, diagnostic.ours.as_deref()),
        (None, crate::jj::ConflictCondition::ContentConflict) => content_conflict_guidance(),
        (None, crate::jj::ConflictCondition::BaseDrift) => {
            "\nThis is BASE DRIFT, not a content conflict: every conflicting file is already \
             byte-identical between your branch and the new base, so there is nothing to merge and \
             editing will not clear it. What is stale is your branch's ANCESTRY, and you cannot \
             repair that from here — your checkout is a plain git worktree whose refs are \
             downstream exports of the runner's private jj store, so no local ref move survives. \
             Ask the store to replay your branch instead: \
             write({changes:[{target:\"cairn:~/rebase\",mode:\"patch\",payload:{action:\"replay\"}}]})."
                .to_string()
        }
        (None, crate::jj::ConflictCondition::Unparseable) => {
            unparseable_guidance(diagnostic.validation.as_ref())
        }
    };
    let conflicting_label = if diagnostic.scaffolding.is_some() {
        "Affected files in the replay withheld for conflict scaffolding"
    } else if diagnostic.condition == crate::jj::ConflictCondition::Unparseable {
        "Changed on both sides, merged by jj into files that do not parse"
    } else {
        "Conflicting files, yours to merge"
    };

    format!(
        "{note}{}{}{coordinates}{guidance}{}\nThe complete session — both sides of the merge as \
         browsable patches, the whole incoming file set, and the sanctioned replay action — is at \
         `cairn:~/rebase`.",
        render_incoming_group(conflicting_label, &conflicting),
        render_incoming_group("Also arriving with this change, cleanly on retry", &clean),
        marker_guidance(marker_state, diagnostic.condition),
    )
}

/// What an agent is told when its replay merged "cleanly" into a file that
/// does not parse: the parser's first error per file, which is the whole
/// diagnosis, since there are no markers to find.
fn unparseable_guidance(refusal: Option<&crate::jj::ValidationRefusal>) -> String {
    let failures = refusal
        .map(|refusal| format!("\n{}", crate::jj::render_failures(&refusal.failures)))
        .unwrap_or_default();
    format!(
        "\nThis replay MERGED WITHOUT A RECORDED CONFLICT INTO FILES THAT DO NOT PARSE: each side \
         parsed on its own and the line merge of the two did not, so the replay was rolled back \
         and your branch is still on its old base. The parser's first error in each file:{failures}\n\
         Read both versions of each file, write the merged result with ordinary edits (usually a \
         list closed twice or an entry left outside its brackets), commit it on your branch, \
         {REQUEST_THE_REPLAY}; that replay is checked the same way. Never rebase or force-push by \
         hand."
    )
}

async fn load_merged_job_for_owner(
    db: &LocalDb,
    owner_id: &str,
) -> Result<Option<MergedJob>, String> {
    if let Some(job) = load_job_by_id(db, owner_id).await? {
        return Ok(Some(job));
    }

    let Some(action_run) = load_action_run_pr_owner(db, owner_id).await? else {
        return Ok(None);
    };

    if let Some(parent_job_id) = action_run.parent_job_id.as_deref() {
        if let Some(job) = load_job_by_id(db, parent_job_id).await? {
            if job.base_branch.is_some() {
                return Ok(Some(job));
            }
        }
    }

    if let Some(job) =
        find_context_source_job(db, &action_run.execution_id, &action_run.recipe_node_id).await?
    {
        return Ok(Some(job));
    }

    latest_complete_implementation_job(db, &action_run.execution_id).await
}

#[derive(Debug)]
struct ActionRunOwner {
    execution_id: String,
    recipe_node_id: String,
    parent_job_id: Option<String>,
}

async fn load_job_by_id(db: &LocalDb, job_id: &str) -> Result<Option<MergedJob>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { load_job_by_id_conn(conn, &job_id).await })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_job_by_id_conn(
    conn: &crate::storage::TrackedConnection,
    job_id: &str,
) -> DbResult<Option<MergedJob>> {
    let mut rows = conn
        .query(
            "SELECT id, project_id, issue_id, branch, base_branch
             FROM jobs
             WHERE id = ?1",
            params![job_id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| {
            Ok(MergedJob {
                id: row.text(0)?,
                project_id: row.text(1)?,
                branch: row.opt_text(3)?,
                base_branch: row.opt_text(4)?,
            })
        })
        .transpose()
}

async fn load_action_run_pr_owner(
    db: &LocalDb,
    owner_id: &str,
) -> Result<Option<ActionRunOwner>, String> {
    let owner_id = owner_id.to_string();
    db.read(|conn| {
        let owner_id = owner_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT execution_id, recipe_node_id, parent_job_id
                         FROM action_runs
                         WHERE id = ?1",
                    params![owner_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(ActionRunOwner {
                        execution_id: row.text(0)?,
                        recipe_node_id: row.text(1)?,
                        parent_job_id: row.opt_text(2)?,
                    })
                })
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn find_context_source_job(
    db: &LocalDb,
    execution_id: &str,
    pr_node_id: &str,
) -> Result<Option<MergedJob>, String> {
    let execution_id = execution_id.to_string();
    let pr_node_id = pr_node_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        let pr_node_id = pr_node_id.clone();
        Box::pin(async move {
            let snapshot = load_execution_snapshot_conn(conn, &execution_id).await?;
            for edge in snapshot.recipe.edges.iter().filter(|edge| {
                edge.edge_type.to_string() == "context" && edge.target_node_id == pr_node_id
            }) {
                let mut rows = conn
                    .query(
                        "SELECT id, project_id, issue_id, branch, base_branch
                             FROM jobs
                             WHERE execution_id = ?1
                               AND recipe_node_id = ?2
                               AND branch IS NOT NULL
                               AND status <> 'cancelled'
                             ORDER BY created_at DESC
                             LIMIT 1",
                        params![execution_id.as_str(), edge.source_node_id.as_str()],
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    return Ok(Some(MergedJob {
                        id: row.text(0)?,
                        project_id: row.text(1)?,
                        branch: row.opt_text(3)?,
                        base_branch: row.opt_text(4)?,
                    }));
                }
            }
            Ok(None)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn latest_complete_implementation_job(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Option<MergedJob>, String> {
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, project_id, issue_id, branch, base_branch
                         FROM jobs
                         WHERE execution_id = ?1
                           AND branch IS NOT NULL
                           AND status = 'complete'
                         ORDER BY completed_at DESC, updated_at DESC
                         LIMIT 1",
                    params![execution_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(MergedJob {
                        id: row.text(0)?,
                        project_id: row.text(1)?,
                        branch: row.opt_text(3)?,
                        base_branch: row.opt_text(4)?,
                    })
                })
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_execution_snapshot_conn(
    conn: &crate::storage::TrackedConnection,
    execution_id: &str,
) -> DbResult<ExecutionSnapshot> {
    let mut rows = conn
        .query(
            "SELECT snapshot FROM executions WHERE id = ?1",
            params![execution_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Err(DbError::Row("execution not found".to_string()));
    };
    let Some(snapshot_json) = row.opt_text(0)? else {
        return Err(DbError::Row("execution has no snapshot".to_string()));
    };
    crate::config::snapshot_migrate::load(&snapshot_json)
        .map_err(|error| DbError::Row(error.to_string()))
}

/// In-flight siblings on the same base that may need rebasing after a merge.
/// Beyond the status filter (still-running jobs), this also enumerates a
/// **completed** sibling that still has an **open** PR (`merge_requests.status`
/// not merged/closed): a child whose build job finished but whose PR is awaiting
/// merge is exactly the sibling that must auto-rebase onto the advanced base.
/// A job that can still act on the base it records: it is not on a resolved
/// issue, and it is either still running or still holding an open pull request.
///
/// Shared by the sibling selection and the child re-point, because the two have
/// to name one population. Every branch that inherits a base advance is a branch
/// whose recorded base must survive that advance, and a predicate written twice
/// is a population that eventually differs.
const LIVE_JOB_PREDICATE: &str = "NOT EXISTS (
              SELECT 1 FROM issues i
               WHERE i.id = j.issue_id AND i.status IN ('merged', 'closed')
            )
            AND ( j.status NOT IN ('complete', 'failed', 'cancelled')
                  OR EXISTS (
                    SELECT 1 FROM merge_requests mr
                     WHERE mr.source_branch = j.branch
                       AND mr.project_id = j.project_id
                       AND mr.status NOT IN ('merged', 'closed')
                  ) )";

/// Joins `delegator` to job `j` when `j` works on the branch of the job that
/// delegated it: a sub-agent task (or a workflow agent) on its parent's
/// branch. Read through [`OWNS_BRANCH`].
const DELEGATOR_ON_BRANCH_JOIN: &str = "LEFT JOIN jobs delegator
             ON delegator.id = j.parent_job_id
            AND delegator.project_id = j.project_id
            AND delegator.branch = j.branch";

/// Whether job `j` owns its branch, over [`DELEGATOR_ON_BRANCH_JOIN`]: it did
/// not inherit the branch from the job that delegated it. A task sharing its
/// parent's branch holds it at the mutation boundary like its owner does
/// ([`shares_live_branch`]), but the branch's declaration, its conflicts and
/// the notices about it belong to the owner. The one owner predicate: owner
/// resolution ([`preparation::live_branch_owner`]) and every notice about a
/// branch ([`branch_owners`]) read it, so the two cannot name different owners.
const OWNS_BRANCH: &str = "(delegator.id IS NULL)";

/// Re-point everything cut from a branch that has just merged.
///
/// A child issue's execution records its parent's integration branch in
/// `jobs.base_branch`, and merging the parent deletes that branch. From that
/// moment the record names nothing, and it is the name through which the child's
/// diff range, its pull request's target, the next base advance, and the replay
/// that would rescue it all resolve. What the parent merged INTO is where those
/// children were always going — it is what GitHub itself retargets their open
/// pull requests onto — and it is knowable at exactly this moment rather than
/// reconstructable later from a name that no longer resolves.
///
/// Both records that hold it are corrected, because both are read: the job's
/// base branch, and the target of any open pull request still naming the merged
/// branch. Cairn landings invoke this while still reserving the merged branch,
/// before another child can bind a landing onto it. Observed external merges
/// use the same operation on recovery. No child bookmark is replayed or moved.
pub(crate) async fn repoint_children_of_merged_branch(
    db: &LocalDb,
    _guard: &crate::orchestrator::JjStoreGuard,
    project_id: &str,
    merged_branch: &str,
    new_base: &str,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    let jobs_sql = format!(
        "UPDATE jobs SET base_branch = ?3, updated_at = ?4
          WHERE id IN (
            SELECT j.id FROM jobs j
             WHERE j.project_id = ?1 AND j.base_branch = ?2 AND {LIVE_JOB_PREDICATE}
          )"
    );
    let project = project_id.to_string();
    let merged = merged_branch.to_string();
    let base = new_base.to_string();
    // SQL only: LocalDb may retry this transaction. The store guard, held by
    // the caller, serializes the relationship with bookmark publication.
    let (jobs, pull_requests) = db
        .write(move |conn| {
            let (project, merged, base, jobs_sql) = (
                project.clone(),
                merged.clone(),
                base.clone(),
                jobs_sql.clone(),
            );
            Box::pin(async move {
                let jobs = conn
                    .execute(
                        &jobs_sql,
                        params![project.as_str(), merged.as_str(), base.as_str(), now],
                    )
                    .await?;
                let pull_requests = conn
                    .execute(
                        "UPDATE merge_requests SET target_branch = ?3, updated_at = ?4
                  WHERE project_id = ?1 AND target_branch = ?2
                    AND status NOT IN ('merged', 'closed')",
                        params![project.as_str(), merged.as_str(), base.as_str(), now],
                    )
                    .await?;
                Ok((jobs, pull_requests))
            })
        })
        .await
        .map_err(|error| format!("repoint work cut from `{merged_branch}`: {error}"))?;
    if jobs > 0 || pull_requests > 0 {
        log::info!(
            "merged branch `{merged_branch}` was folded into `{new_base}`: re-pointed {jobs} job(s) \
             and {pull_requests} open pull request(s) that were cut from it"
        );
    }
    Ok(())
}

/// The non-default branches of a project the base-branch sync tracks against
/// origin: every branch live work is cut from, and every branch that work was
/// ever cut from while the job owning it is still live.
///
/// The second half is not redundant. Once its children have merged, a parent's
/// integration branch has no live child, and it is exactly then that an
/// operator's UI merge of the last child leaves the store's copy behind origin
/// while the parent itself goes on to publish, open its own pull request and
/// cut its next child from it.
pub(crate) async fn load_tracked_base_branches(
    db: &LocalDb,
    project_id: &str,
    default_branch: &str,
) -> Result<Vec<String>, String> {
    let sql = format!(
        "SELECT j.base_branch FROM jobs j
          WHERE j.project_id = ?1 AND j.base_branch IS NOT NULL
            AND j.base_branch NOT IN ('', ?2) AND {LIVE_JOB_PREDICATE}
         UNION
         SELECT j.branch FROM jobs j
          WHERE j.project_id = ?1
            AND j.branch IN (
              SELECT b.base_branch FROM jobs b
               WHERE b.project_id = ?1 AND b.base_branch IS NOT NULL
                 AND b.base_branch NOT IN ('', ?2)
            )
            AND {LIVE_JOB_PREDICATE}"
    );
    let project_id = project_id.to_string();
    let default_branch = default_branch.to_string();
    db.read(|conn| {
        let sql = sql.clone();
        let project_id = project_id.clone();
        let default_branch = default_branch.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(&sql, params![project_id.as_str(), default_branch.as_str()])
                .await?;
            let mut branches = Vec::new();
            while let Some(row) = rows.next().await? {
                branches.push(row.text(0)?);
            }
            branches.sort();
            branches.dedup();
            Ok(branches)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Active jobs whose durable branch is the branch that just advanced. Every
/// one of them records the advanced base; only its [`branch_owners`] are told.
pub(super) async fn load_on_branch_jobs(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<SiblingJob>, String> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    let sql = format!(
        "SELECT j.id, j.base_commit, {OWNS_BRANCH}
           FROM jobs j {DELEGATOR_ON_BRANCH_JOIN}
          WHERE j.project_id = ?1
            AND j.branch = ?2
            AND {LIVE_JOB_PREDICATE}"
    );
    db.read(|conn| {
        let project_id = project_id.clone();
        let branch = branch.clone();
        let sql = sql.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(&sql, params![project_id.as_str(), branch.as_str()])
                .await?;
            let mut jobs = Vec::new();
            while let Some(row) = rows.next().await? {
                jobs.push(SiblingJob {
                    id: row.text(0)?,
                    base_commit: row.opt_text(1)?,
                    owns_branch: row.i64(2)? != 0,
                });
            }
            Ok(jobs)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Whether `branch` is the project's configured default branch, and so the one
/// branch for which origin is the sole authority.
///
/// The gate on the backward repair in `reconcile_tracked_bookmark`: a Coordinator
/// integration branch or an agent branch legitimately holds sealed work origin
/// has not seen, and must never be reset onto origin. An unreadable project row
/// answers `false` — declining to reconcile is always safe, resetting the wrong
/// branch is not.
async fn branch_is_project_default(db: &LocalDb, project_id: &str, branch: &str) -> bool {
    match load_project_default_branch(db, project_id).await {
        Ok(Some(default_branch)) => default_branch == branch,
        Ok(None) => false,
        Err(error) => {
            log::warn!(
                "could not read default_branch for project {project_id}; skipping default bookmark reconcile: {error}"
            );
            false
        }
    }
}

pub(crate) async fn load_project_default_branch(
    db: &LocalDb,
    project_id: &str,
) -> Result<Option<String>, String> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT repo_path, default_branch FROM projects WHERE id = ?1",
                    params![project_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => {
                    let repo_path = row.opt_text(0)?;
                    let stored = row.opt_text(1)?;
                    Ok(Some(
                        crate::config::project_settings::effective_default_branch(
                            repo_path.as_deref().map(Path::new),
                            stored.as_deref(),
                        ),
                    ))
                }
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The git-backed checkout path for a project (the source of the jj-managed
/// signal and the anchor for the shared jj store).
pub(crate) async fn load_project_repo_path(
    dbs: &crate::db::DbState,
    project_id: &str,
) -> Result<Option<String>, String> {
    crate::projects::crud::resolve_local_repo_path_and_key(dbs, project_id)
        .await
        .map(|(path, _)| path)
        .map_err(|error| error.to_string())
}

/// Projects eligible for startup default-advance catch-up: those with a local git
/// checkout (`repo_path`) and a known `default_branch`. Remote presence is checked
/// against live git config after this query so local-only projects stay cheap to
/// skip and cloud-only projects never enter the path.
pub(crate) async fn load_projects_for_default_reconcile(
    orch: &Orchestrator,
) -> Result<Vec<(Arc<LocalDb>, DefaultReconcileProject)>, String> {
    let mut all_projects = Vec::new();
    for db in orch.db.all_dbs().await {
        let mut projects = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT id, repo_path, default_branch FROM projects
                         WHERE repo_path IS NOT NULL AND repo_path != ''
                           AND default_branch IS NOT NULL AND default_branch != ''",
                            (),
                        )
                        .await?;
                    let mut projects = Vec::new();
                    while let Some(row) = rows.next().await? {
                        projects.push(DefaultReconcileProject {
                            id: row.text(0)?,
                            repo_path: row.text(1)?,
                            default_branch: row.text(2)?,
                        });
                    }
                    Ok(projects)
                })
            })
            .await
            .map_err(|error| error.to_string())?;
        for mut project in projects.drain(..) {
            if let Some(repo_path) = load_project_repo_path(&orch.db, &project.id).await? {
                project.repo_path = repo_path;
                all_projects.push((db.clone(), project));
            }
        }
    }
    Ok(all_projects)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::db::DbState;
    use crate::services::testing::{MockGitClient, TestServicesBuilder};
    use crate::storage::{LocalDb, SearchIndex};
    use std::path::PathBuf;
    use std::sync::Arc;

    #[tokio::test]
    async fn publication_authority_uses_the_configured_default_and_literal_store_identity() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let repository = tempfile::tempdir().unwrap();
        let config = repository.path().join(".cairn/config.yaml");
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        let settings = "defaultBranch: trunk\n";
        std::fs::write(&config, settings).unwrap();
        db.execute(
            "UPDATE projects SET repo_path = ?1 WHERE id = 'proj-1'",
            (repository.path().to_string_lossy().into_owned(),),
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE jobs SET branch = 'trunk' WHERE id = 'job-clean'",
            (),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());
        let store = crate::jj::project_store_dir(&orch.config_dir, repository.path());
        std::fs::create_dir_all(&store).unwrap();

        assert_eq!(
            load_project_default_branch(&orch.db.local, "proj-1")
                .await
                .unwrap()
                .as_deref(),
            Some("trunk")
        );
        assert!(
            managed_publication_claims(&orch, &store, "trunk")
                .await
                .unwrap()
                .is_none(),
            "a job naming the integration default cannot authorize rewriting it"
        );
        assert!(managed_publication_claims(&orch, &store, "unmanaged")
            .await
            .unwrap()
            .is_none());
        let managed = managed_publication_claims(&orch, &store.join("."), "agent/overlap")
            .await
            .unwrap();
        assert!(
            managed.is_some_and(|claims| claims.is_empty()),
            "the literal managed branch is authorized even before any accepted drops exist"
        );
        assert_eq!(std::fs::read_to_string(&config).unwrap(), settings);
        let stored = orch
            .db
            .local
            .query_one(
                "SELECT default_branch FROM projects WHERE id = 'proj-1'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(
            stored, "main",
            "resolving settings never rewrites the project row"
        );
    }

    /// A pass that panics must cost one cycle and not the loop. Unsupervised, a
    /// single panic retires a periodic loop for the lifetime of the process.
    #[tokio::test]
    async fn a_panicking_sweep_pass_does_not_retire_the_loop() {
        let passes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for attempt in 0..3 {
            let counter = passes.clone();
            let outcome = supervised_sweep_pass(async move {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if attempt == 0 {
                    panic!("dirty pages should be empty for read txn");
                }
            })
            .await;
            assert_eq!(
                outcome.is_err(),
                attempt == 0,
                "the panicking pass reports, and only it reports"
            );
        }
        assert_eq!(
            passes.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "every pass after the panic still ran"
        );
    }

    /// The refusal that used to greet every sessionless replay is gone, but the
    /// two payload keys that only mean something ALONGSIDE a session must still
    /// be refused, and each must say which artifact is missing rather than
    /// silently doing a different thing.
    ///
    /// `take-committed-tip` matters most: it restores a session's CONFLICTING
    /// paths from the branch tip, so with no session there is no such set, and
    /// accepting it would mean choosing some other set on the requester's
    /// behalf.
    #[tokio::test]
    async fn a_sessionless_replay_refuses_the_keys_that_need_a_session() {
        let db = Arc::new(migrated_db().await);
        seed_base_advance_fixture(&db).await;
        let root = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();

        let fingerprinted = request_branch_replay(
            &orch,
            &db,
            "job-clean",
            "agent/clean",
            Some("base:ours:theirs"),
            false,
            None,
        )
        .await
        .expect_err("a fingerprint pins session coordinates that do not exist");
        assert!(
            fingerprinted.contains("no open rebase session")
                && fingerprinted.contains("without one"),
            "the refusal names the missing artifact and the way forward: {fingerprinted}"
        );

        let restored =
            request_branch_replay(&orch, &db, "job-clean", "agent/clean", None, true, None)
                .await
                .expect_err("there is no set of conflicting paths to restore");
        assert!(
            restored.contains("CONFLICTING paths") && restored.contains("Request a plain"),
            "the refusal explains the scope it cannot resolve and offers the plain replay: \
             {restored}"
        );
    }

    /// Read one text column keyed by id.
    async fn text_field(db: &LocalDb, sql: &'static str, id: &str) -> Option<String> {
        let id = id.to_string();
        db.read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn.query(sql, params![id]).await?;
                match rows.next().await? {
                    Some(row) => row.opt_text(0),
                    None => Ok(None),
                }
            })
        })
        .await
        .unwrap()
    }

    const JOB_BASE: &str = "SELECT base_branch FROM jobs WHERE id = ?1";
    const MR_TARGET: &str = "SELECT target_branch FROM merge_requests WHERE id = ?1";

    #[tokio::test]
    async fn failed_publication_invalidates_the_open_pull_requests_cached_verdict() {
        let db = Arc::new(migrated_db().await);
        seed_base_advance_fixture(&db).await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj-2', 'default', 'Other', 'other', '/other', 'main', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-other', 'proj-2', 1, 'Other', 'active', 1, 1);
             INSERT INTO jobs (id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
               VALUES ('job-other', 'issue-other', 'proj-2', 'running', 'agent/clean', 'main', 1, 1);
             INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch,
                status, github_state, github_mergeable, github_fetched_at, opened_at, updated_at)
             VALUES
               ('mr-open', 'job-clean', 'proj-1', 'issue-3', 'Open',
                'agent/clean', 'main', 'open', 'OPEN', 'MERGEABLE', 100, 1, 1),
               ('mr-github-resolved', 'job-overlap', 'proj-1', 'issue-2', 'Resolved remotely',
                'agent/clean', 'main', 'open', 'MERGED', 'MERGEABLE', 100, 1, 1),
               ('mr-other-project', 'job-other', 'proj-2', 'issue-other', 'Other project',
                'agent/clean', 'main', 'open', 'OPEN', 'MERGEABLE', 100, 1, 1);",
        )
        .await
        .unwrap();
        let root = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root,
        )
        .build();

        mark_publication_unconfirmed(&orch, &db, "proj-1", "agent/clean")
            .await
            .unwrap();

        let open: (String, Option<i64>) = db
            .query_one(
                "SELECT github_mergeable, github_fetched_at FROM merge_requests WHERE id = 'mr-open'",
                (),
                |row| Ok((row.text(0)?, row.opt_i64(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(open, ("UNKNOWN".to_string(), None));
        for (id, reason) in [
            (
                "mr-github-resolved",
                "GitHub-resolved pull requests are historical records",
            ),
            (
                "mr-other-project",
                "the same branch name in another project is unrelated",
            ),
        ] {
            assert_eq!(
                text_field(
                    &db,
                    "SELECT github_mergeable FROM merge_requests WHERE id = ?1",
                    id,
                )
                .await
                .as_deref(),
                Some("MERGEABLE"),
                "{reason} and must not be rewritten"
            );
        }
    }

    /// A child issue's branch is cut from its parent's, and the parent's merge
    /// deletes that branch. Re-pointing is what keeps the child placeable
    /// afterwards, so it has to reach BOTH records that hold the dead name and
    /// leave the child in the same pass's sibling set — which is how it inherits
    /// the advance instead of being stranded by it.
    #[tokio::test]
    async fn merging_a_parent_re_points_the_work_cut_from_its_branch() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute_script(
            "
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
              VALUES ('issue-child', 'proj-1', 5, 'Child', 'active', 1, 1);
            INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES ('exec-5', 'recipe-default', 'issue-child', 'proj-1', 'running', 1, 1);
            INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
              VALUES ('job-child', 'exec-5', 'node', 'issue-child', 'proj-1', 'running', 'agent/child', 'agent/merged', 1, 1);
            INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
              VALUES ('mr-child', 'job-child', 'proj-1', 'issue-child', 'PR', 'agent/child', 'agent/merged', 'open', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
              VALUES ('issue-done', 'proj-1', 6, 'Done', 'merged', 1, 1);
            INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES ('exec-6', 'recipe-default', 'issue-done', 'proj-1', 'complete', 1, 1);
            INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
              VALUES ('job-done', 'exec-6', 'node', 'issue-done', 'proj-1', 'complete', 'agent/done', 'agent/merged', 1, 1);
            ",
        )
        .await
        .unwrap();

        let orch = test_orchestrator(migrated_db().await, MockGitClient::new());
        let guard = orch
            .acquire_jj_store_lock(
                Path::new("/fixture/store"),
                crate::orchestrator::StoreLockClass::Foreground,
                "test declared-base repoint",
            )
            .await;
        repoint_children_of_merged_branch(&db, &guard, "proj-1", "agent/merged", "integration")
            .await
            .unwrap();

        assert_eq!(
            text_field(&db, JOB_BASE, "job-child").await.as_deref(),
            Some("integration"),
            "the child is cut from what its parent merged into"
        );
        assert_eq!(
            text_field(&db, MR_TARGET, "mr-child").await.as_deref(),
            Some("integration"),
            "the open pull request follows, so nothing later tries to merge into a deleted branch"
        );
        assert_eq!(
            text_field(&db, JOB_BASE, "job-done").await.as_deref(),
            Some("agent/merged"),
            "work that resolved alongside its parent keeps the record of where it was cut from"
        );

        db.execute_script(
            "CREATE TRIGGER refuse_child_retarget BEFORE UPDATE OF target_branch ON merge_requests
             WHEN NEW.target_branch = 'refused-base'
             BEGIN SELECT RAISE(ABORT, 'retarget refused'); END;",
        )
        .await
        .unwrap();
        let error =
            repoint_children_of_merged_branch(&db, &guard, "proj-1", "integration", "refused-base")
                .await
                .unwrap_err();
        assert!(error.contains("retarget refused"), "{error}");
        assert_eq!(
            text_field(&db, JOB_BASE, "job-child").await.as_deref(),
            Some("integration"),
            "a refused PR retarget must roll back the job's relationship too"
        );
        assert_eq!(
            text_field(&db, MR_TARGET, "mr-child").await.as_deref(),
            Some("integration")
        );
    }

    /// End to end over a real store: a branch whose recorded base was deleted
    /// when its parent merged asks for a replay and gets an answer — the base
    /// it is actually measured against, and a corrected record — instead of the
    /// resolution error that used to be the end of the road.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn a_replay_after_its_parent_merged_lands_on_the_surviving_base() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            eprintln!(
                "skipping a_replay_after_its_parent_merged_lands_on_the_surviving_base: no jj"
            );
            return;
        };
        let home = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        crate::jj::tests::init_project(project.path());
        crate::jj::tests::git(project.path(), &["checkout", "-q", "-b", "agent/child"]);
        std::fs::write(project.path().join("child.rs"), "child\n").unwrap();
        crate::jj::tests::git(project.path(), &["add", "-A"]);
        crate::jj::tests::git(project.path(), &["commit", "-q", "-m", "child"]);
        crate::jj::tests::git(project.path(), &["checkout", "-q", "main"]);

        let db = Arc::new(migrated_db().await);
        db.execute_script(format!(
            "
            INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
              VALUES ('proj-1', 'default', 'Project', 'proj', '{repo}', 'main', 1, 1);
            INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
              VALUES ('issue-child', 'proj-1', 1, 'Child', 'active', 1, 1);
            INSERT INTO jobs (id, project_id, issue_id, status, branch, base_branch, created_at, updated_at)
              VALUES ('job-child', 'proj-1', 'issue-child', 'running', 'agent/child', 'agent/deleted-parent', 1, 1);
            ",
            repo = project.path().display()
        ))
        .await
        .unwrap();

        let root = home.path().to_path_buf();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root.clone(),
        )
        .jj_binary_path(bin.clone())
        .build();
        let jj = crate::jj::environment(&bin, &root);
        let store = crate::jj::project_store_dir(&root, project.path());
        crate::jj::ensure_project_store(&jj, &store, project.path()).unwrap();

        let summary =
            request_branch_replay(&orch, &db, "job-child", "agent/child", None, false, None)
                .await
                .expect("a deleted base is not a dead end");

        assert!(
            summary.contains("agent/deleted-parent")
                && summary.contains("no longer exists")
                && summary.contains("`main`"),
            "the answer names the base that vanished and the one it is measured against: {summary}"
        );
        assert_eq!(
            text_field(&db, JOB_BASE, "job-child").await.as_deref(),
            Some("main"),
            "the correction is durable, so the next surface to ask gets the same answer"
        );
    }

    fn held() -> Result<Option<InflightRunBatch>, String> {
        Ok(Some(InflightRunBatch {
            job_id: "job-builder".into(),
            holder: Some("cairn://p/cairn/5069/1/builder".into()),
            request_id: "ba0e0085".into(),
            label: Some("rust-tests on main".into()),
            parked_at_ms: 1_760_788_078_000,
            progress: None,
        }))
    }

    /// The 5222 specimen: the hold named the batch by its description and
    /// advised "stopping the turn", which an agent reads as ending its own
    /// turn, which releases nothing. The hold now names the item running and
    /// for how long, offers the batch's own cancel as a copy-paste action and
    /// the one-step replay, names the operator's node Stop, and says ending a
    /// turn does not release it.
    #[test]
    fn the_hold_names_the_running_item_and_offers_the_batch_cancel() {
        use crate::mcp::handlers::run::detached::BatchProgress;
        let started = 1_760_788_078_000;
        let mut batch = held().unwrap().unwrap();
        batch.progress = Some(BatchProgress::Running {
            index: 2,
            total: 5,
            header: "cargo nextest run -p cairn-core".into(),
            since_ms: started,
        });
        assert!(
            batch
                .describe_at(started + 14 * 60_000)
                .contains("now running item 3 of 5 (`cargo nextest run -p cairn-core`) for 14m 0s"),
            "{}",
            batch.describe_at(started + 14 * 60_000)
        );
        let said = mutation_boundary_hold(&Ok(Some(batch)));
        assert!(said.contains("now running item 3 of 5"), "{said}");
        assert!(
            said.contains(
                "target:\"cairn://p/cairn/5069/1/builder/batches/ba0e0085\",mode:\"patch\",payload:{action:\"cancel\"}"
            ),
            "offers the batch's own cancel: {said}"
        );
        assert!(said.contains("cancel_holding_batch:true"), "{said}");
        assert!(said.contains("operator stops the node"), "{said}");
        assert!(said.contains("Ending a turn does not release it"), "{said}");
        assert!(
            said.contains("next commit, requested check or run, or merge"),
            "{said}"
        );
        assert!(!said.contains("stopping the turn"), "{said}");
        assert!(!said.contains("or is stopped"), "{said}");

        let mut task = held().unwrap().unwrap();
        task.holder = Some("cairn://p/cairn/5069/1/builder/task/verify".into());
        assert_eq!(
            task.cancel_uri().as_deref(),
            Some("cairn://p/cairn/5069/1/builder/task/verify/batches/ba0e0085")
        );
        let mut unresolved = held().unwrap().unwrap();
        unresolved.holder = None;
        let said = mutation_boundary_hold(&Ok(Some(unresolved)));
        assert!(
            said.contains("`batches/ba0e0085` under job `job-builder`"),
            "{said}"
        );
    }

    /// A host batch parks on a run-batch row too, but it holds no tree and
    /// cannot commit, so it must not hold its branch at the boundary. A row
    /// written before the marker existed reads as a tree batch and still holds.
    #[test]
    fn a_host_run_batch_does_not_hold_the_branch() {
        let host = r#"{"kind":"run_batch","request_id":"r","commits":false,"host":true}"#;
        assert_eq!(run_batch_from_wait("job".into(), host, 1), None);
        let legacy = r#"{"kind":"run_batch","request_id":"r","commits":false}"#;
        assert_eq!(
            run_batch_from_wait("job".into(), legacy, 1).map(|batch| batch.request_id),
            Some("r".to_string())
        );
    }

    /// A child task shares its parent's branch, so the automatic guard defers
    /// the parent's branch on the CHILD's batch. The reported hold has to see
    /// the same thing: a node-only lookup told an idle parent that nothing held
    /// the boundary while every sweep kept deferring it.
    #[tokio::test]
    async fn the_reported_hold_spans_every_live_job_sharing_the_branch() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        for sql in [
            "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at) VALUES ('job-child','exec-2','node','issue-2','proj-1','running','agent/overlap','integration',1,1)",
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-child','job-child','open','backend',1,1)",
            "INSERT INTO runs (id,issue_id,project_id,job_id,status,session_id,created_at,updated_at) VALUES ('run-child','issue-2','proj-1','job-child','live','session-child',1,1)",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-child','session-child','run-child','job-child',1,'yielded','initial',1,1)",
            "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-child','job-child','run-child','session-child','turn-child','tool-child','{\"kind\":\"run_batch\",\"request_id\":\"request-child\",\"commits\":true,\"label\":\"child suite\"}','pending',1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }

        // The parent is idle; its child's batch is what holds the branch.
        let hold = inflight_run_batch_on_branch(&db, "job-overlap", "agent/overlap").await;
        let batch = hold
            .clone()
            .unwrap()
            .expect("the child's batch holds the shared branch");
        assert_eq!(batch.job_id, "job-child");
        assert_eq!(batch.request_id, "request-child");
        assert!(
            batch.holder.is_some() || batch.describe().contains("job `job-child`"),
            "names whose turn to stop: {}",
            batch.describe()
        );
        let said = mutation_boundary_hold(&hold);
        assert!(
            said.contains("`request-child`") && !said.contains("nothing holds"),
            "{said}"
        );

        assert!(
            inflight_run_batch_on_branch(&db, "job-clean", "agent/clean")
                .await
                .unwrap()
                .is_none(),
            "another branch's batch holds nothing here"
        );

        // With both open, the reader's own batch leads: it is the one the
        // reader can end itself.
        for sql in [
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-parent','job-overlap','open','backend',1,1)",
            "UPDATE runs SET session_id='session-parent' WHERE id='run-job-overlap'",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-parent','session-parent','run-job-overlap','job-overlap',1,'yielded','initial',1,1)",
            "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-parent','job-overlap','run-job-overlap','session-parent','turn-parent','tool-parent','{\"kind\":\"run_batch\",\"request_id\":\"request-parent\",\"commits\":false}','pending',5)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        let own = inflight_run_batch_on_branch(&db, "job-overlap", "agent/overlap")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(own.request_id, "request-parent");
        let child_view = inflight_run_batch_on_branch(&db, "job-child", "agent/overlap")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(child_view.request_id, "request-child");

        let publishing = inflight_run_batch_except(
            &db,
            "proj-1",
            "agent/overlap",
            Some("job-overlap"),
            Some(("job-overlap", "request-parent")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            publishing.request_id, "request-child",
            "excluding the publisher must continue scanning"
        );
        let not_own = inflight_run_batch_except(
            &db,
            "proj-1",
            "agent/overlap",
            Some("job-overlap"),
            Some(("job-overlap", "request-child")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            not_own.request_id, "request-parent",
            "another job's request cannot grant admission"
        );
        db.execute(
            "UPDATE agent_waits SET state = 'resolved' WHERE id = 'wait-child'",
            (),
        )
        .await
        .unwrap();
        assert!(
            inflight_run_batch_except(
                &db,
                "proj-1",
                "agent/overlap",
                Some("job-overlap"),
                Some(("job-overlap", "request-parent")),
            )
            .await
            .unwrap()
            .is_none(),
            "the publisher must not wait for its own suspension to resolve"
        );
        assert_eq!(
            inflight_run_batch_on_branch(&db, "job-overlap", "agent/overlap")
                .await
                .unwrap()
                .unwrap()
                .request_id,
            "request-parent",
            "ordinary callers still honor the publisher's wait",
        );
    }

    /// cairn/6145 and cairn/6205: a batch holds its branch while any durable row
    /// for it is unsettled. A batch running detached (no wait row at all, as a
    /// Codex carrier's batch or one whose wait was abandoned) holds it, whether
    /// or not this runner's registry knows it; only settlement releases it,
    /// and a verdict-only batch never holds.
    #[tokio::test]
    async fn a_batch_running_detached_holds_its_branch_until_it_settles() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        // The 6205 shape: the agent's wait was cancelled, the batch lives on.
        for sql in [
            "INSERT INTO run_batch_detached(request_id,job_id,run_id,label,commits,holds_branch,state,reason,created_ms) VALUES('req-detached','job-overlap','run-job-overlap','cargo test',1,1,'running','its wait was abandoned while the batch ran',7)",
            "INSERT INTO run_batch_detached(request_id,job_id,run_id,label,commits,holds_branch,state,reason,created_ms) VALUES('req-verdict','job-clean','run-job-clean','branch: main',0,0,'running','x',7)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        let waits = db
            .query_one("SELECT COUNT(*) FROM agent_waits", (), |row| row.i64(0))
            .await
            .unwrap();
        assert_eq!(waits, 0, "no wait row exists for either batch");

        let hold = inflight_run_batch_on_branch(&db, "job-overlap", "agent/overlap").await;
        assert_eq!(
            hold.clone().unwrap(),
            Some(InflightRunBatch {
                job_id: "job-overlap".into(),
                holder: hold.clone().unwrap().unwrap().holder,
                request_id: "req-detached".into(),
                label: Some("cargo test".into()),
                parked_at_ms: 7,
                progress: None,
            })
        );
        assert!(mutation_boundary_hold(&hold).contains("`req-detached`"));
        assert!(
            inflight_run_batch_except(&db, "proj-1", "agent/overlap", None, None)
                .await
                .unwrap()
                .is_some(),
            "every writer admission sees it too"
        );
        assert!(
            inflight_run_batch_except(
                &db,
                "proj-1",
                "agent/overlap",
                None,
                Some(("job-overlap", "req-detached")),
            )
            .await
            .unwrap()
            .is_none(),
            "a publication's own sealed request is exempt, detached or not"
        );
        assert!(
            inflight_run_batch_on_branch(&db, "job-clean", "agent/clean")
                .await
                .unwrap()
                .is_none(),
            "a verdict-only batch never holds its branch"
        );

        db.execute(
            "UPDATE run_batch_detached SET state='settled' WHERE request_id='req-detached'",
            (),
        )
        .await
        .unwrap();
        assert!(
            inflight_run_batch_on_branch(&db, "job-overlap", "agent/overlap")
                .await
                .unwrap()
                .is_none(),
            "settlement releases the hold"
        );
    }

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("base-advance-test.db").await
    }

    /// Both statements bind a job's project and branch. Without an index
    /// leading with both, Turso seeks `idx_jobs_project_base_branch` or walks
    /// the project's jobs: the owner lookup once per project on every
    /// publication, the preparer's owner lookup on every preparation. Results
    /// are identical either way, so only the plan witnesses the regression.
    #[tokio::test]
    async fn live_job_lookups_by_branch_seek_project_and_branch() {
        let db = migrated_db().await;
        for (name, sql) in [
            (
                "branch named by a job",
                BRANCH_NAMED_BY_A_JOB_SQL.to_string(),
            ),
            (
                "live owners on a branch",
                preparation::live_owners_on_branch_sql(),
            ),
        ] {
            let plan =
                crate::storage::query_plan(&db, &sql, vec!["p".into(), "agent/x".into()]).await;
            assert!(
                plan.lines().any(|step| step.starts_with("SEARCH j USING ")
                    && step.contains("INDEX idx_jobs_project_branch (project_id=? AND branch=?)")),
                "{name}: jobs must be sought on project and branch, got\n{plan}"
            );
            // `SCAN CONSTANT ROW` is the outer `SELECT EXISTS(...)` row, not a table.
            assert!(
                !plan
                    .lines()
                    .any(|step| step.starts_with("SCAN ") && step != "SCAN CONSTANT ROW"),
                "{name}: no table may be scanned, got\n{plan}"
            );
        }
    }

    fn test_orchestrator(db: LocalDb, git: MockGitClient) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = temp.keep();
        let index_path = config_dir.join("search-index.db");
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(index_path).unwrap()),
        ));
        let services = Arc::new(TestServicesBuilder::new().with_git(git).build());
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    async fn seed_base_advance_fixture(db: &LocalDb) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Project', 'proj', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                for (id, number) in [
                    ("issue-1", 1_i64),
                    ("issue-2", 2_i64),
                    ("issue-3", 3_i64),
                    ("issue-4", 4_i64),
                ] {
                    conn.execute(
                        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                         VALUES (?1, 'proj-1', ?2, 'Issue', 'active', 1, 1)",
                        params![id, number],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                         VALUES (?1, 'recipe-default', ?2, 'proj-1', 'running', 1, 1)",
                        params![format!("exec-{}", number).as_str(), id],
                    )
                    .await?;
                }
                for (job, exec, issue, status, branch) in [
                    ("job-merged", "exec-1", "issue-1", "complete", "agent/merged"),
                    ("job-overlap", "exec-2", "issue-2", "running", "agent/overlap"),
                    ("job-clean", "exec-3", "issue-3", "running", "agent/clean"),
                    ("job-complete", "exec-4", "issue-4", "complete", "agent/complete"),
                ] {
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
                         VALUES (?1, ?2, 'node', ?3, 'proj-1', ?4, ?5, 'integration', 1, 1)",
                        params![job, exec, issue, status, branch],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
                         VALUES (?1, ?2, 'proj-1', ?3, 'live', 1, 1)",
                        params![format!("run-{}", job).as_str(), issue, job],
                    )
                    .await?;
                }
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at, github_pr_number)
                     VALUES ('mr-1', 'job-merged', 'proj-1', 'issue-1', 'PR', 'feature', 'integration', 'merged', 1, 1, 42)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn timeout_cooldown_persists_coordinates_and_grows_across_main_advances() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let first_note = record_reconcile_timeout(
            &db,
            "proj-1",
            store,
            "agent/overlap",
            "tip-a",
            "main-a",
            "jj rebase timed out after 10s and was killed",
        )
        .await
        .unwrap();
        let first = load_reconcile_quarantine(&db, "proj-1", store, "agent/overlap")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.fingerprint, "tip-a:main-a");
        assert_eq!(first.strike_count, 1);
        assert_eq!(first.retry_at() - first.updated_at, 300);
        assert!(first_note.contains("new main tip does not bypass"));
        assert!(quarantine_blocks_automatic_replay(
            &first,
            Some("tip-a"),
            "external advance main-b"
        ));
        assert!(!quarantine_blocks_automatic_replay(
            &first,
            Some("tip-b"),
            "child merge"
        ));
        assert!(
            load_reconcile_quarantine(&db, "proj-1", store, "agent/clean")
                .await
                .unwrap()
                .is_none(),
            "one expensive branch does not park its peers"
        );
        let comments = crate::issues::comments::list(&db, "issue-2").await.unwrap();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].content, first_note);
        assert!(crate::issues::comments::list(&db, "issue-3")
            .await
            .unwrap()
            .is_empty());

        // The same tip against a different target grows, rather than resets,
        // the persisted penalty. Expiry makes it eligible without an edit.
        record_reconcile_timeout(
            &db,
            "proj-1",
            store,
            "agent/overlap",
            "tip-a",
            "main-b",
            "jj rebase timed out after 10s and was killed",
        )
        .await
        .unwrap();
        let mut second = load_reconcile_quarantine(&db, "proj-1", store, "agent/overlap")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.fingerprint, "tip-a:main-b");
        assert_eq!(second.strike_count, 2);
        assert_eq!(second.retry_at() - second.updated_at, 600);
        second.updated_at = chrono::Utc::now().timestamp() - 601;
        assert!(!quarantine_blocks_automatic_replay(
            &second,
            Some("tip-a"),
            "sweep"
        ));
        for (strikes, seconds) in [
            (1, 300),
            (2, 600),
            (3, 1200),
            (4, 2400),
            (5, 3600),
            (i64::MAX, 3600),
        ] {
            assert_eq!(timeout_backoff_seconds(strikes), seconds);
        }

        record_reconcile_timeout(
            &db,
            "proj-1",
            store,
            "agent/overlap",
            "tip-b",
            "main-c",
            "jj rebase timed out after 10s and was killed",
        )
        .await
        .unwrap();
        let moved = load_reconcile_quarantine(&db, "proj-1", store, "agent/overlap")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            moved.strike_count, 1,
            "a changed stack starts a new failure series"
        );
        assert_eq!(moved.observed_tip(), "tip-b");
    }

    #[test]
    fn rolled_back_conflict_parks_only_automatic_replays_of_the_same_tip() {
        let quarantine = ReconcileQuarantine {
            failure_kind: ROLLED_BACK_CONFLICT.to_string(),
            fingerprint: "tip-a".to_string(),
            strike_count: 1,
            updated_at: 0,
        };

        for advance in 0..5 {
            assert!(
                quarantine_blocks_automatic_replay(
                    &quarantine,
                    Some("tip-a"),
                    &format!("base advance {advance}")
                ),
                "every later automatic base advance stays parked"
            );
        }
        assert!(
            !quarantine_blocks_automatic_replay(
                &quarantine,
                Some("tip-a"),
                "replay requested for agent/test"
            ),
            "an explicit plain replay bypasses the automatic circuit breaker"
        );
        assert!(
            !quarantine_blocks_automatic_replay(
                &quarantine,
                Some("tip-a"),
                "resolved replay requested for agent/test"
            ),
            "an explicit resolved replay bypasses the automatic circuit breaker"
        );
        assert!(
            !quarantine_blocks_automatic_replay(
                &quarantine,
                Some("tip-with-resolution"),
                "base advance after resolution"
            ),
            "a new clean resolution commit changes the tip and resets eligibility"
        );
    }

    #[test]
    fn permanent_failure_quarantine_keeps_existing_automatic_behavior() {
        let quarantine = ReconcileQuarantine {
            failure_kind: "immutable_commit".to_string(),
            fingerprint: "tip-a".to_string(),
            strike_count: 1,
            updated_at: 0,
        };

        assert!(quarantine_blocks_automatic_replay(
            &quarantine,
            Some("tip-a"),
            "base advance"
        ));
        assert!(!quarantine_blocks_automatic_replay(
            &quarantine,
            Some("tip-b"),
            "base advance"
        ));
        assert!(
            quarantine_blocks_automatic_replay(
                &quarantine,
                Some("tip-a"),
                "replay requested for agent/test"
            ),
            "explicit replay authority is limited to rolled-back content conflicts"
        );
    }

    #[test]
    fn explicit_replay_bypasses_unchanged_automatic_timeout_cooldown() {
        let quarantine = ReconcileQuarantine {
            failure_kind: RECONCILE_TIMEOUT.to_string(),
            fingerprint: "tip-a:main-a".to_string(),
            strike_count: 1,
            updated_at: chrono::Utc::now().timestamp(),
        };
        assert!(quarantine_blocks_automatic_replay(
            &quarantine,
            Some("tip-a"),
            "external advance main-b"
        ));
        for label in [
            "replay requested for agent/replay",
            "resolved replay requested for agent/replay",
        ] {
            assert!(
                !quarantine_blocks_automatic_replay(&quarantine, Some("tip-a"), label),
                "an explicit request must bypass automatic cooldown: {label}"
            );
        }
        assert!(!quarantine_blocks_automatic_replay(
            &quarantine,
            Some("tip-b"),
            "external advance main-b"
        ));
    }

    /// The durable base record this reconcile leaves on a job.
    async fn recorded_durable_base(db: &LocalDb, job_id: &str) -> Option<String> {
        db.query_opt_text(
            "SELECT base_commit FROM jobs WHERE id = ?1",
            (job_id.to_string(),),
        )
        .await
        .unwrap()
    }

    /// A concurrent reconcile that wrote the same value is agreement.
    ///
    /// A caller's snapshot of the job's coordinate can be stale, so a write
    /// routinely finds a row another writer has already advanced to exactly
    /// this target. The row holds what the call
    /// wanted to put there; calling that a failure is what turned zero-delta
    /// planner branches into ⛔ BLOCKING directs that resumed deliberately
    /// parked sessions (CAIRN-3094 comment #4).
    #[tokio::test]
    async fn a_durable_base_already_at_the_target_is_agreement() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute(
            "UPDATE jobs SET base_commit = 'new-tip' WHERE id = 'job-overlap'",
            (),
        )
        .await
        .unwrap();
        let sibling = SiblingJob {
            id: "job-overlap".to_string(),
            base_commit: Some("stale-snapshot".to_string()),
            owns_branch: true,
        };

        advance_sibling_durable_base(&db, &sibling, "new-tip")
            .await
            .expect("a row already holding the target is not a lost race");

        assert_eq!(
            recorded_durable_base(&db, "job-overlap").await.as_deref(),
            Some("new-tip")
        );
    }

    /// A snapshot that lost to a writer heading somewhere ELSE still lands on
    /// this reconcile's target.
    ///
    /// This is the assertion the retired compare-and-swap also made, and making
    /// it identically against a plain write is the evidence that the CAS never
    /// ordered anything (CAIRN-3226): its retry swapped against the freshly read
    /// value and wrote this call's target regardless of what it found.
    #[tokio::test]
    async fn a_stale_snapshot_still_records_this_reconciles_target() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute(
            "UPDATE jobs SET base_commit = 'somebody-elses-tip' WHERE id = 'job-overlap'",
            (),
        )
        .await
        .unwrap();
        let sibling = SiblingJob {
            id: "job-overlap".to_string(),
            base_commit: Some("stale-snapshot".to_string()),
            owns_branch: true,
        };

        advance_sibling_durable_base(&db, &sibling, "new-tip")
            .await
            .expect("a stale snapshot does not block the record");

        assert_eq!(
            recorded_durable_base(&db, "job-overlap").await.as_deref(),
            Some("new-tip")
        );
    }

    /// A job with no recorded base is recorded, not refused.
    ///
    /// The retired CAS needed a non-null expected value and aborted with a
    /// manufactured error without one, so a job whose bookkeeping had never been
    /// written could never acquire it — substrate state failing a step that has
    /// nothing to fail about.
    #[tokio::test]
    async fn a_job_with_no_recorded_base_is_recorded_not_refused() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute(
            "UPDATE jobs SET base_commit = NULL WHERE id = 'job-overlap'",
            (),
        )
        .await
        .unwrap();
        let sibling = SiblingJob {
            id: "job-overlap".to_string(),
            base_commit: None,
            owns_branch: true,
        };

        advance_sibling_durable_base(&db, &sibling, "new-tip")
            .await
            .expect("an unrecorded base is not a reason to refuse the record");

        assert_eq!(
            recorded_durable_base(&db, "job-overlap").await.as_deref(),
            Some("new-tip")
        );
    }

    /// A job deleted mid-reconcile is the graph moving on, not a failure of this
    /// bookkeeping.
    #[tokio::test]
    async fn a_vanished_job_does_not_fail_the_record() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let sibling = SiblingJob {
            id: "job-that-was-deleted".to_string(),
            base_commit: Some("stale-snapshot".to_string()),
            owns_branch: true,
        };

        advance_sibling_durable_base(&db, &sibling, "new-tip")
            .await
            .expect("a missing job row is logged, not raised");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_lease_enumeration_includes_only_running_terminals() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute_script(
            "UPDATE jobs SET branch = 'agent/proj-2-builder-0' WHERE id = 'job-overlap';
             INSERT INTO job_terminals
                 (id, job_id, session_id, command, status, created_at, slug,
                  residency_holder, residency_incarnation_id, cell_epoch)
             VALUES
                 ('live-terminal', 'job-overlap', 'live-session', 'true', 'running', 1, 'live', 'live-lease', 'live-inc', 7),
                 ('exited-terminal', 'job-overlap', 'exited-session', 'true', 'exited', 1, 'exited', 'exited-lease', 'exited-inc', 8);",
        )
        .await
        .unwrap();

        let leases = load_live_terminal_leases(&db, "proj-1", "agent/proj-2-builder-0")
            .await
            .unwrap();
        assert_eq!(
            leases,
            vec![(
                "live-lease".to_string(),
                "live-inc".to_string(),
                7,
                "job-overlap".to_string(),
            )]
        );
    }

    /// Committing a resolution does not move a conflicted branch; the replay
    /// does. cairn/5253's builder followed a content-conflict note that ended at
    /// "commit it", and its branch never moved until it found the replay itself.
    /// So the content-conflict note must carry the replay step, in the same words
    /// the scaffolding note uses, and the replay must come after the commit.
    #[test]
    fn the_content_conflict_note_requests_the_replay_the_scaffolding_note_does() {
        let content = content_conflict_guidance();
        let scaffolding = scaffolding_guidance(
            &crate::jj::ScaffoldingRefusal {
                commit: "r".repeat(40),
                entries: vec!["JJ-CONFLICT-README".to_string()],
            },
            None,
        );
        let unparseable = unparseable_guidance(None);

        for note in [&content, &scaffolding, &unparseable] {
            assert!(
                note.contains(REQUEST_THE_REPLAY),
                "every note that asks for a merge names the replay that lands it: {note}"
            );
        }
        assert!(content.contains("cairn:~/rebase?view=merged"), "{content}");

        let step = |needle: &str| {
            content
                .find(needle)
                .unwrap_or_else(|| panic!("`{needle}` missing: {content}"))
        };
        let open = step("Open the session at `cairn:~/rebase`");
        let write = step("Write the merged result");
        let commit = step("`commit_msg`");
        let replay = step(REQUEST_THE_REPLAY);
        assert!(
            open < write && write < commit && commit < replay,
            "the steps run open, write, commit, replay: {content}"
        );
        assert!(
            content.contains("take-committed-tip")
                && content.contains("already contains the base's side"),
            "take-committed-tip is offered only with its precondition: {content}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_branch_query_selects_only_the_coordinator_on_the_branch() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        // The Coordinator: a running job whose BRANCH *is* the integration branch
        // (it sits ON it), branched FROM 'main'. The fixture's other jobs have
        // base_branch = 'integration' and a NULL branch (children branched FROM
        // it). The two queries must be disjoint.
        db.execute_script(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-coord', 'proj-1', 5, 'Coord', 'active', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-coord', 'recipe-default', 'issue-coord', 'proj-1', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
             VALUES ('job-coord', 'exec-coord', 'node', 'issue-coord', 'proj-1', 'running', 'integration', 'main', 1, 1);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());

        let on_branch = load_on_branch_jobs(&orch.db.local, "proj-1", "integration")
            .await
            .unwrap();
        let on_ids: std::collections::HashSet<&str> =
            on_branch.iter().map(|s| s.id.as_str()).collect();
        assert!(
            on_ids.contains("job-coord"),
            "the job ON the integration branch (the coordinator) is selected"
        );
        assert_eq!(
            on_ids.len(),
            1,
            "only the on-branch job; children branched FROM it are excluded"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_fetch_does_not_block_publication_lock_during_external_reconcile() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
        let mut git = MockGitClient::new();
        git.expect_fetch_origin_branch()
            .with(
                mockall::predicate::eq(PathBuf::from("/repo")),
                mockall::predicate::eq("integration".to_string()),
            )
            .return_once(move |_, _| {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                Ok(())
            });
        let orch = Arc::new(test_orchestrator(db, git));
        let reconcile_orch = orch.clone();
        let reconcile = tokio::spawn(async move {
            reconcile_default_advance(&reconcile_orch, "proj-1", "integration").await
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("production reconcile reached the injected fetch stall");

        let store = crate::jj::project_store_dir(&orch.config_dir, Path::new("/repo"));
        orch.acquire_jj_store_lock_with_timeout(
            &store,
            crate::orchestrator::StoreLockClass::Background,
            "test run commit barrier publication",
            Some(std::time::Duration::from_millis(100)),
        )
        .await
        .expect("publication acquires the canonical store lock while reconcile fetch is stalled");

        release_tx.send(()).unwrap();
        // `/repo` is not a real repository, so the import after the fetch
        // fails; what this test owns is that the fetch held no store lock.
        let _ = reconcile.await.unwrap();
    }

    /// THE REGRESSION. A merge lands on origin with NOTHING else in flight, which
    /// is the overwhelmingly common shape. Every base-advance path used to return
    /// early on `siblings.is_empty()` BEFORE it imported anything, so the store's
    /// default bookmark was never reconciled and was left holding a target that
    /// no longer agreed with origin — which the next operation to touch it
    /// reported as a conflicted name, killing every `main`-resolving verb.
    ///
    /// This asserts the store converges on origin with zero siblings. It fails on
    /// the code this change replaces, for the reason the whole slice exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn default_bookmark_reconciles_with_zero_in_flight_siblings() {
        let Some(_bin) = crate::jj::tests::jj_bin() else {
            eprintln!("skipping default_bookmark_reconciles_with_zero_siblings: jj not resolvable");
            return;
        };
        use crate::jj::tests::{git, git_stdout, init_project};

        let origin = tempfile::tempdir().unwrap();
        let proj = tempfile::tempdir().unwrap();
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        git(
            proj.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);
        let repo_path = proj.path().to_string_lossy().into_owned();

        let db = migrated_db().await;
        let seed_repo = repo_path.clone();
        db.write(|conn| {
            let seed_repo = seed_repo.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-jj', 'default', 'Project', 'PJJ', ?1, 'main', 1, 1)",
                    params![seed_repo.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let mut git_client = MockGitClient::new();
        git_client
            .expect_fetch_origin_branch()
            .returning(|repo, branch| {
                crate::services::GitClient::fetch_origin_branch(
                    &crate::services::RealGitClient,
                    repo,
                    branch,
                )
            });
        let orch = test_orchestrator(db, git_client);

        // The store is provisioned and tracking origin, exactly as a live project's is.
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
        crate::jj::ensure_project_store(&jj, &store, Path::new(&repo_path)).unwrap();

        // A PR merges on GitHub: origin's default branch advances onto a commit
        // the store has never seen. No sibling job exists anywhere.
        git(proj.path(), &["checkout", "-q", "--detach"]);
        std::fs::write(proj.path().join("merged.rs"), "landed\n").unwrap();
        git(proj.path(), &["add", "-A"]);
        git(proj.path(), &["commit", "-q", "-m", "squash merge (#1)"]);
        let landed = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
        git(proj.path(), &["push", "-q", "-f", "origin", "HEAD:main"]);

        reconcile_default_advance(&orch, "proj-jj", "main")
            .await
            .unwrap();

        assert_eq!(
            crate::jj::bookmark_commit(&jj, &store, "main").as_deref(),
            Some(landed.as_str()),
            "the store's default bookmark must equal origin after a merge, with no siblings in flight and no human in the loop"
        );
        assert_eq!(
            git_stdout(proj.path(), &["rev-parse", "refs/heads/main"]),
            landed,
            "the backing git ref must follow, or the next import re-conflicts the bookmark"
        );

        // A `?branch=main` read resolves the name and returns the merged content.
        // Resolving to the right commit id is not the same as being readable: the
        // conflicted-name state fails at resolution, so this is the assertion that
        // the branch is actually usable again.
        let content = crate::jj::file_show(&jj, &store, "main", "merged.rs")
            .expect("a read at `main` must resolve once the bookmark is reconciled");
        assert_eq!(String::from_utf8_lossy(&content), "landed\n");

        // And a fresh job provisions off it — the verb a conflicted `main` kills.
        let wts = tempfile::tempdir().unwrap();
        crate::jj::add_workspace(
            &jj,
            &store,
            &wts.path().join("job"),
            "agent/cairn-3192-builder-0",
            "main",
            None,
        )
        .expect("a fresh job must spawn off `main` with no human in the loop");
    }

    /// Base advance is lazy. An advance made on origin is imported into the
    /// store's default bookmark and touches nothing cut from it: three live
    /// siblings of `main` (one no pull request needs, one with its own open
    /// pull request, and an integration branch an open child pull request
    /// targets) keep their exact heads, locally and on origin. The PR-less
    /// branch still publishes when its pull request opens.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn an_origin_advance_moves_and_publishes_no_sibling() {
        use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
        let Some(bin) = jj_bin() else {
            eprintln!("skipping an_origin_advance_moves_and_publishes_no_sibling: no jj");
            return;
        };
        const LOCAL: &str = "agent/local";
        const OPEN: &str = "agent/open";
        const INTEGRATION: &str = "agent/integration";

        let home = tempfile::tempdir().unwrap();
        let origin = tempfile::tempdir().unwrap();
        let proj = tempfile::tempdir().unwrap();
        let workspaces = tempfile::tempdir().unwrap();
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(proj.path());
        git(
            proj.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(proj.path(), &["push", "-q", "origin", "main"]);

        let db = Arc::new(migrated_db().await);
        db.execute_script(format!(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj-1', 'default', 'Project', 'proj', '{repo}', 'main', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-1', 'proj-1', 1, 'Issue', 'active', 1, 1);
             INSERT INTO jobs (id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
               VALUES ('job-local', 'issue-1', 'proj-1', 'running', '{LOCAL}', 'main', 1, 1),
                      ('job-open', 'issue-1', 'proj-1', 'running', '{OPEN}', 'main', 1, 1),
                      ('job-coordinator', 'issue-1', 'proj-1', 'running', '{INTEGRATION}', 'main', 1, 1),
                      ('job-child', 'issue-1', 'proj-1', 'running', 'agent/child', '{INTEGRATION}', 1, 1);
             INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch,
                status, github_state, is_local, opened_at, updated_at)
             VALUES
               ('mr-open', 'job-open', 'proj-1', 'issue-1', 'Open', '{OPEN}', 'main',
                'open', 'OPEN', 0, 1, 1),
               ('mr-child', 'job-child', 'proj-1', 'issue-1', 'Child', 'agent/child', '{INTEGRATION}',
                'open', 'OPEN', 0, 1, 1);",
            repo = proj.path().to_string_lossy().replace('\'', "''"),
        ))
        .await
        .unwrap();

        let mut git_client = MockGitClient::new();
        git_client
            .expect_fetch_origin_branch()
            .returning(|repo, branch| {
                crate::services::GitClient::fetch_origin_branch(
                    &crate::services::RealGitClient,
                    repo,
                    branch,
                )
            });
        git_client.expect_remote_get_url().returning(|repo| {
            crate::services::GitClient::remote_get_url(&crate::services::RealGitClient, repo)
        });
        let root = home.path().to_path_buf();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().with_git(git_client).build()),
            root.clone(),
        )
        .jj_binary_path(bin.clone())
        .build();
        let jj = crate::jj::environment(&bin, &root);
        let store = crate::jj::project_store_dir(&root, proj.path());
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();

        for branch in [LOCAL, OPEN, INTEGRATION] {
            let workspace = workspaces.path().join(branch.replace('/', "-"));
            crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
            std::fs::write(
                workspace.join(format!("{}.rs", branch.replace('/', "-"))),
                "work\n",
            )
            .unwrap();
            crate::jj::seal(&jj, &workspace, &format!("work on {branch}"), None).unwrap();
            let tip = crate::jj::bookmark_commit(&jj, &store, branch).unwrap();
            crate::jj::export_git_verified(&jj, &store, true, "test commit", &[(branch, &tip)])
                .unwrap();
        }
        // What already needed origin before the advance is already there: the
        // open PR's head, and the base its child PR was opened against.
        for branch in [OPEN, INTEGRATION] {
            publish_managed_branch(&orch, &store, branch).await.unwrap();
        }
        let origin_head = |branch: &str| {
            git_stdout(
                origin.path(),
                &[
                    "for-each-ref",
                    "--format=%(objectname)",
                    &format!("refs/heads/{branch}"),
                ],
            )
        };
        assert_eq!(
            origin_head(LOCAL),
            "",
            "no commit has published the PR-less branch"
        );

        // A PR merges on GitHub: origin's default branch advances.
        git(proj.path(), &["checkout", "-q", "--detach"]);
        std::fs::write(proj.path().join("landed.rs"), "landed\n").unwrap();
        git(proj.path(), &["add", "-A"]);
        git(proj.path(), &["commit", "-q", "-m", "squash merge (#1)"]);
        let landed = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
        git(proj.path(), &["push", "-q", "-f", "origin", "HEAD:main"]);

        let store_tip = |branch: &str| crate::jj::bookmark_commit(&jj, &store, branch).unwrap();
        let before = [LOCAL, OPEN, INTEGRATION].map(store_tip);
        let published = [OPEN, INTEGRATION].map(origin_head);

        reconcile_default_advance(&orch, "proj-1", "main")
            .await
            .unwrap();

        assert_eq!(
            store_tip("main"),
            landed,
            "the advance is imported into the store"
        );
        assert_eq!(
            [LOCAL, OPEN, INTEGRATION].map(store_tip),
            before,
            "no sibling is replayed onto the advance"
        );
        for branch in [LOCAL, OPEN, INTEGRATION] {
            assert_eq!(
                crate::jj::revset_commit(&jj, &store, &format!("{landed} & ::{branch}")),
                None,
                "{branch} stays at its literal head until its own demand"
            );
        }
        assert_eq!(
            origin_head(LOCAL),
            "",
            "nothing is published for a PR-less branch"
        );
        for table in [
            "messages",
            "attention_pushes",
            "jj_reconcile_intents",
            "jj_reconcile_items",
        ] {
            assert_eq!(
                db.query_text(format!("SELECT CAST(COUNT(*) AS TEXT) FROM {table}"), ())
                    .await
                    .unwrap()
                    .as_deref(),
                Some("0"),
                "an origin advance writes no {table} row for any sibling"
            );
        }
        assert_eq!(
            [OPEN, INTEGRATION].map(origin_head),
            published,
            "published heads are not rewritten on origin"
        );

        // Opening a pull request publishes first (`PrVcs::publish`), through
        // this barrier.
        publish_managed_branch(&orch, &store, LOCAL)
            .await
            .expect("an untouched branch publishes when its pull request opens");
        assert_eq!(origin_head(LOCAL), store_tip(LOCAL));
    }

    /// A branch that is NOT the project default is never reset onto origin: a
    /// Coordinator integration branch and an agent branch legitimately hold sealed
    /// work origin has not seen. The gate lives in the reconciler so no call site
    /// can bypass it — the GitHub-merge caller passes a PR's target branch and
    /// only asserted in a comment that it is always the default.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_non_default_branch_is_never_reconciled_onto_origin() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;

        assert!(branch_is_project_default(&db, "proj-1", "main").await);
        assert!(!branch_is_project_default(&db, "proj-1", "integration").await);
        assert!(!branch_is_project_default(&db, "proj-1", "agent/cairn-1-builder-0").await);
        assert!(
            !branch_is_project_default(&db, "no-such-project", "main").await,
            "an unreadable project must decline to reconcile rather than guess"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn project_has_origin_uses_live_git_config() {
        let db = migrated_db().await;
        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(mockall::predicate::function(|path: &Path| {
                path == Path::new("/repo/remote")
            }))
            .returning(|_| Ok("https://github.com/acme/repo.git".to_string()));
        let orch = test_orchestrator(db, git);

        assert!(project_has_origin(&orch, Path::new("/repo/remote")));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn project_has_origin_skips_missing_or_empty_origin() {
        let db = migrated_db().await;
        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(mockall::predicate::function(|path: &Path| {
                path == Path::new("/repo/no-remote")
            }))
            .returning(|_| Err("No such remote 'origin'".to_string()));
        git.expect_remote_get_url()
            .with(mockall::predicate::function(|path: &Path| {
                path == Path::new("/repo/empty")
            }))
            .returning(|_| Ok("   ".to_string()));
        let orch = test_orchestrator(db, git);

        assert!(!project_has_origin(&orch, Path::new("/repo/no-remote")));
        assert!(!project_has_origin(&orch, Path::new("/repo/empty")));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn default_reconcile_projects_skip_cloud_only_and_branchless() {
        let db = migrated_db().await;
        // p-ok: a local checkout with a default branch — eligible. p-no-repo: no
        // local checkout (cloud-only) — nothing to advance. p-no-branch: no
        // default branch — nothing to reconcile onto.
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('p-ok', 'default', 'Ok', 'OK', '/repo/ok', 'main', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('p-no-repo', 'default', 'NoRepo', 'NR', '', 'main', 1, 1);
             INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES ('p-no-branch', 'default', 'NoBranch', 'NB', '/repo/nb', NULL, 1, 1);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());

        let projects = load_projects_for_default_reconcile(&orch).await.unwrap();
        let ids: std::collections::HashSet<&str> = projects
            .iter()
            .map(|(_, project)| project.id.as_str())
            .collect();

        assert!(
            ids.contains("p-ok"),
            "a project with a local checkout and default branch is eligible"
        );
        assert!(
            !ids.contains("p-no-repo"),
            "a cloud-only project with no local checkout is skipped"
        );
        assert!(
            !ids.contains("p-no-branch"),
            "a project with no default branch is skipped"
        );
        let ok = projects
            .iter()
            .find(|(_, project)| project.id == "p-ok")
            .unwrap();
        assert_eq!(
            ok.1.repo_path, "/repo/ok",
            "the repository path is returned for live remote detection"
        );
        assert_eq!(
            ok.1.default_branch, "main",
            "the default branch is returned alongside the id"
        );
    }
}

#[cfg(test)]
mod real_store_collision_tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    struct Fixture {
        // Held only for their lifetime: the jj environment and the store live
        // under `_home`, and the store is imported from the git repository in
        // `_repo`, so both must outlive every use of `jj` and `store`.
        _home: TempDir,
        _repo: TempDir,
        workspaces: TempDir,
        jj: crate::jj::JjEnv,
        store: PathBuf,
    }

    fn fixture(bin: &str) -> Fixture {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let workspaces = tempfile::tempdir().unwrap();
        let registry = repo
            .path()
            .join("src-tauri/os/cairn-db/src/storage/migrations.rs");
        std::fs::create_dir_all(registry.parent().unwrap()).unwrap();
        std::fs::create_dir_all(repo.path().join("src-tauri/turso_migrations")).unwrap();
        std::fs::create_dir_all(repo.path().join(".cairn")).unwrap();
        // The destination's declared landing rules, which every replay onto
        // `main` reads at `main`.
        std::fs::write(
            repo.path().join(".cairn/config.yaml"),
            "landing:\n  merge:\n    - paths: [src-tauri/os/cairn-db/src/storage/migrations.rs]\n      kind: ordered-set\n",
        )
        .unwrap();
        std::fs::write(
            registry,
            "fn migrations() {\n    vec![\n        migration!(\"0227_before\"),\n        migration!(\"0229_after\"),\n    ];\n}\n",
        ).unwrap();
        for stem in ["0227_before", "0229_after"] {
            std::fs::write(
                repo.path()
                    .join(format!("src-tauri/turso_migrations/{stem}.sql")),
                format!("-- {stem}\n"),
            )
            .unwrap();
        }
        let git = |args: &[&str]| {
            let out = crate::env::git()
                .args(args)
                .current_dir(repo.path())
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        git(&["init", "-q", "-b", "main"]);
        git(&["config", "user.email", "tests@cairn.local"]);
        git(&["config", "user.name", "Cairn tests"]);
        git(&["add", "-A"]);
        git(&["commit", "-q", "-m", "base"]);

        let jj = crate::jj::environment(bin, home.path());
        let store = crate::jj::project_store_dir(home.path(), repo.path());
        crate::jj::ensure_project_store(&jj, &store, repo.path()).unwrap();
        Fixture {
            _home: home,
            _repo: repo,
            workspaces,
            jj,
            store,
        }
    }

    fn add_migration(workspace: &Path, stem: &str) {
        std::fs::write(
            workspace.join(format!("src-tauri/turso_migrations/{stem}.sql")),
            format!("-- {stem}\n"),
        )
        .unwrap();
        let path = workspace.join("src-tauri/os/cairn-db/src/storage/migrations.rs");
        let registry = std::fs::read_to_string(&path).unwrap();
        std::fs::write(
            path,
            registry.replace(
                "        migration!(\"0229_after\"),",
                &format!("        migration!(\"{stem}\"),\n        migration!(\"0229_after\"),"),
            ),
        )
        .unwrap();
    }

    fn text_at(fx: &Fixture, revision: &str, path: &str) -> String {
        String::from_utf8(crate::jj::file_show(&fx.jj, &fx.store, revision, path).unwrap()).unwrap()
    }

    fn set_main(fx: &Fixture, revision: &str) {
        fx.jj
            .run(
                &fx.store,
                &[
                    "bookmark",
                    "set",
                    "main",
                    "-r",
                    revision,
                    "--ignore-working-copy",
                ],
                "advance main",
            )
            .unwrap();
    }

    #[test]
    #[serial_test::serial(jj)]
    fn sibling_insertions_into_one_registry_list_prove_a_clean_structural_merge() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let fx = fixture(&bin);
        let registry = "src-tauri/os/cairn-db/src/storage/migrations.rs";
        let workspace = |name: &str, bookmark: &str| {
            let path = fx.workspaces.path().join(name);
            crate::jj::add_workspace(&fx.jj, &fx.store, &path, bookmark, "main", None).unwrap();
            path
        };
        let land = |name: &str, stem: &str| {
            let bookmark = format!("agent/registry-{name}");
            let path = workspace(name, &bookmark);
            add_migration(&path, stem);
            crate::jj::seal(&fx.jj, &path, name, None).unwrap();
            set_main(
                &fx,
                &crate::jj::bookmark_commit(&fx.jj, &fx.store, &bookmark).unwrap(),
            );
        };
        let replay = |branch: &str| {
            let crate::jj::RebaseOutcome::Conflicted { diagnostic } =
                crate::jj::rebase_branch_onto(&fx.jj, &fx.store, branch, "main").unwrap()
            else {
                panic!("insertions at one place in one list must conflict line-wise")
            };
            let plan = prepare_structural_merge(
                &fx.jj,
                &fx.store,
                &diagnostic,
                &ReplayBaseline::fork_only(diagnostic.base.as_deref().unwrap()),
                &crate::orchestrator::landing::merge_rules_at(&fx.jj, &fx.store, "main"),
                &|_| false,
            )
            .unwrap()
            .expect("the declared list merge settles it");
            assert_eq!(plan.paths, vec![registry.to_string()]);
            let [crate::orchestrator::landing::bind::FileEdit::Rewrite { content, .. }] =
                plan.edits.as_slice()
            else {
                panic!("one proven rewrite of the registry")
            };
            content.clone()
        };
        let entries = |text: &str| -> Vec<String> {
            text.lines()
                .filter_map(|line| line.trim().strip_prefix("migration!(\""))
                .filter_map(|rest| rest.strip_suffix("\"),"))
                .map(ToOwned::to_owned)
                .collect()
        };

        let branch = "agent/registry-branch";
        let branch_ws = workspace("branch", branch);
        add_migration(&branch_ws, "0230_branch");
        crate::jj::seal(&fx.jj, &branch_ws, "branch migration", None).unwrap();

        land("first", "0228_first");
        let once = replay(branch);
        assert!(
            cairn_vcs::parse_gate::parse_failure(registry, &once).is_none(),
            "{once}"
        );
        assert_eq!(
            entries(&once),
            ["0227_before", "0228_first", "0230_branch", "0229_after"]
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn unregistered_real_store_conflict_is_declined_unchanged() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let fx = fixture(&bin);
        let destination_branch = "agent/outside-destination";
        let branch = "agent/outside-branch";
        let destination_ws = fx.workspaces.path().join("outside-destination");
        let branch_ws = fx.workspaces.path().join("outside-branch");
        crate::jj::add_workspace(
            &fx.jj,
            &fx.store,
            &destination_ws,
            destination_branch,
            "main",
            None,
        )
        .unwrap();
        crate::jj::add_workspace(&fx.jj, &fx.store, &branch_ws, branch, "main", None).unwrap();
        std::fs::write(destination_ws.join("outside.txt"), "destination\n").unwrap();
        crate::jj::seal(&fx.jj, &destination_ws, "destination outside edit", None).unwrap();
        std::fs::write(branch_ws.join("outside.txt"), "branch\n").unwrap();
        crate::jj::seal(&fx.jj, &branch_ws, "branch outside edit", None).unwrap();
        let pre_tip = crate::jj::bookmark_commit(&fx.jj, &fx.store, branch).unwrap();
        let destination =
            crate::jj::bookmark_commit(&fx.jj, &fx.store, destination_branch).unwrap();
        set_main(&fx, &destination);

        let crate::jj::RebaseOutcome::Conflicted { diagnostic } =
            crate::jj::rebase_branch_onto(&fx.jj, &fx.store, branch, "main").unwrap()
        else {
            panic!("outside path must conflict")
        };
        assert_eq!(diagnostic.conflicting_paths(), vec!["outside.txt"]);
        assert!(prepare_structural_merge(
            &fx.jj,
            &fx.store,
            &diagnostic,
            &ReplayBaseline::fork_only(diagnostic.base.as_deref().unwrap()),
            &crate::orchestrator::landing::merge_rules_at(&fx.jj, &fx.store, "main"),
            &|_| false,
        )
        .unwrap()
        .is_none());
        assert_eq!(
            crate::jj::bookmark_commit(&fx.jj, &fx.store, branch).as_deref(),
            Some(pre_tip.as_str())
        );
        assert_eq!(text_at(&fx, branch, "outside.txt"), "branch\n");
    }
}
