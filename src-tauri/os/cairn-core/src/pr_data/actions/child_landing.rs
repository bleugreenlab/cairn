//! Landing a child pull request on the integration branch it targets.
//!
//! The loop is the default landing's (`default_landing`), and the shape is the
//! one a child has always landed in: every commit preserved. The canonical
//! preparer carries the source onto exactly the integration branch's tip T, so
//! the prepared head H descends from T and the landing is a fast-forward of the
//! integration branch to H. The landing commit is H itself: nothing is folded,
//! bound, replayed or healed here, and nothing has to be set aside.
//!
//! 1. **Prepare**: for a project with a remote, origin's copy O of the
//!    integration branch is read off the lock and judged against the store's
//!    copy ([`origin_integration`]); then `store_merge::prepare_landing_source`
//!    carries the source onto the store's copy T. T may hold more than O (the
//!    integration branch's own commits reach origin only when a landing
//!    publishes them), so a child lands on T and the push fast-forwards origin
//!    from O over both. Or T may be the store's own rewrite of O (preparation
//!    carried the integration branch onto an advanced base without pushing
//!    it): then the push rewrites origin's copy, but only when the transport's
//!    supersession proof establishes, from the integration branch's accepted
//!    installs, that every commit origin would lose is one this store replaced
//!    or dropped. Anything else, such as a commit pushed there outside Cairn,
//!    refuses with nothing pushed.
//! 2. **Validate**, off the lock: every file the landing changes parses at H
//!    (the parse gate every fold applies), then the combined-tree checks judge
//!    H's tree against T.
//! 3. **Land if unchanged**, under the lock: the coordinate, the relationship,
//!    the pull request's target and that target's own landing state are
//!    revalidated (`store_merge::prepared_landing_holds`). Any movement
//!    prepares and validates again. A local project then moves the integration
//!    bookmark from exactly T to H. A project with a remote pins H, journals the
//!    publication's intent, and publishes source and target to H in one atomic
//!    push with the store lock released, each ref leased on its exact expected
//!    old value (`default_landing::publish_pinned`): the target on O, the source
//!    on the store's recorded copy of origin's source, so a push made to the
//!    pull request head outside Cairn refuses the landing rather than being
//!    overwritten. Only an accepted push moves the store, and its target only
//!    from exactly T.
//!
//! The source and target stay reserved from before preparation until the
//! caller has recorded the merge: the reservation travels in the returned
//! [`DefaultLanding`].

use std::path::Path;
use std::sync::Arc;

use crate::orchestrator::base_advance::preparation::PreparedBranch;
use crate::orchestrator::landing::bind;
use crate::orchestrator::{BranchReservation, JjStoreGuard, Orchestrator};
use crate::storage::LocalDb;

use super::context::MergeMrContext;
use super::default_landing::{
    admit_landing_writers, advance_landing_bookmarks, coordinate_moved, judge_origin_target,
    origin_target_refusal, pin_landing, publish_pinned, repoint_landed_children,
    reservation_covers, store_of, DefaultLanding, Landed, PublishRefs, Store,
    LANDING_RESERVATION_WAIT, MAX_LANDING_ATTEMPTS,
};
use super::landing_record::{Publication, PublicationShape};
use super::merge::MergeAttemptJournal;
use super::store_merge::{prepare_landing_source, prepared_landing_holds, ProspectiveMerge};

/// Land `merge_context`'s branch on the integration branch its pull request
/// targets. `journal` is the merge call's attempt; each publication try to
/// origin journals under its own attempt id derived from it.
pub(super) async fn land_on_integration(
    orch: &Orchestrator,
    job_id: &str,
    db: &Arc<LocalDb>,
    merge_context: &MergeMrContext,
    journal: &MergeAttemptJournal<'_>,
) -> Result<DefaultLanding, String> {
    let source = merge_context.source_branch.as_str();
    let target = merge_context.target_branch.as_str();
    let store = store_of(orch, merge_context);
    // Held from preparation until the merge is recorded: neither branch moves
    // while the store lock is free for the network push, and a landing of the
    // integration branch itself cannot be publishing from it meanwhile.
    // Preparation inherits it, so it installs the source's advance under this
    // reservation rather than reserving the source again from this task.
    let reservation = Arc::new(
        orch.reserve_branches(
            &store.path,
            &[source, target],
            &format!("landing {source} on {target}"),
            LANDING_RESERVATION_WAIT,
        )
        .await
        .map_err(|error| {
            format!("Refusing to merge `{source}` into `{target}`: {error}. Nothing changed.")
        })?,
    );
    for attempt in 0..MAX_LANDING_ATTEMPTS {
        super::merge_metrics::landing_attempt();
        reservation_covers(&reservation, &store, merge_context)?;
        let origin_target = if merge_context.mr.is_local {
            None
        } else {
            Some(origin_integration(orch, &store, merge_context, &reservation).await?)
        };
        let prepared = prepare_landing_source(orch, job_id, merge_context, &reservation).await?;
        // Maintenance reclaims nothing this attempt reads, validates or
        // publishes until it settles. It does not outlive the process: the
        // landing pin is what recovery keeps after an unknown outcome.
        let _objects = crate::jj::StoreObjectLease::for_store(&store.path).map_err(|error| {
            format!(
                "Refusing to merge `{source}` into `{target}`: the store's objects could not be \
                 protected from maintenance ({error}). Nothing changed; merge again."
            )
        })?;
        let prospect = {
            let wait = super::merge_metrics::time(super::merge_metrics::Phase::LockWait);
            let guard = orch
                .acquire_jj_store_lock(
                    &store.path,
                    crate::orchestrator::StoreLockClass::Foreground,
                    format!("child landing prospect for {source}"),
                )
                .await;
            drop(wait);
            if let Some(moved) = coordinate_moved(&store, merge_context, &prepared) {
                log::info!(
                    "landing[{job_id}]: the prepared coordinate of `{source}` moved before its \
                     landing was read (attempt {}): {moved}",
                    attempt + 1
                );
                continue;
            }
            guard.blocking_phase("read the child landing", || {
                fast_forward_prospect(&store, merge_context, &prepared)
            })?
        };

        validate(orch, db, &store, merge_context, job_id, &prospect).await?;

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
        let landed = land_if_unchanged(
            orch,
            merge_context,
            &store,
            &prepared,
            &prospect,
            origin_target.as_ref(),
            guard,
            &reservation,
            &publication_journal,
        )
        .await?;
        match landed {
            Some(Landed { commit, published }) => {
                return Ok(DefaultLanding {
                    landed_commit: commit,
                    plan: bind::BindPlan::default(),
                    config_notice: None,
                    published,
                    reservation: Some(reservation),
                })
            }
            None => log::info!(
                "landing[{job_id}]: `{source}` or `{target}` moved while the landing was \
                 validated (attempt {}); preparing and validating again",
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

/// Origin's copy O of the integration branch: the target ref's lease. The
/// store's copy T may be exactly O, or descend from it (the store holds the
/// integration branch's own commits origin has not been sent; a landing lands
/// on T and its push fast-forwards origin from O over both), or have been
/// behind it (origin took a merge made outside Cairn, which the store now
/// holds too). Any other shape is refused before anything moves. That covers
/// a copy that diverged, and a copy origin holds that the store has since
/// rewritten, or one that holds commits the store lacks: the hidden-commit
/// test that tells those apart is not the authority here, since a dormant
/// child legitimately keeps an old version visible. Both are left to the
/// transport's supersession proof, which rewrites origin's copy only when
/// every commit it would lose is proven the store's own replacement or drop,
/// and otherwise refuses before anything is sent.
async fn origin_integration(
    orch: &Orchestrator,
    store: &Store,
    merge_context: &MergeMrContext,
    reservation: &crate::orchestrator::BranchReservation,
) -> Result<OriginIntegration, String> {
    let taken = judge_origin_target(orch, store, merge_context, reservation).await?;
    let target = merge_context.target_branch.as_str();
    match taken.judged {
        crate::jj::OriginFastForward::Current { .. }
        | crate::jj::OriginFastForward::Advanced { .. } => Ok(OriginIntegration {
            origin: taken.origin,
            rewrites: false,
        }),
        crate::jj::OriginFastForward::Superseded { .. }
        | crate::jj::OriginFastForward::Diverged { .. } => Ok(OriginIntegration {
            origin: taken.origin,
            rewrites: true,
        }),
        crate::jj::OriginFastForward::NoBookmark => Err(origin_target_refusal(
            merge_context,
            format!("the store has no `{target}`"),
        )),
    }
}

/// Origin's copy of the integration branch, and whether publishing the
/// landing would rewrite it rather than fast-forward it.
struct OriginIntegration {
    origin: String,
    rewrites: bool,
}

/// The evidence that lets a landing rewrite origin's copy of its managed
/// integration branch: the branch's accepted installs' drop claims. A project
/// default, or a branch with no live owner, is never rewritten.
async fn rewrite_evidence(
    orch: &Orchestrator,
    store: &Store,
    merge_context: &MergeMrContext,
) -> Result<Vec<crate::jj::AcceptedAdvanceDrops>, String> {
    let (project, target) = (
        merge_context.project_id.as_str(),
        merge_context.target_branch.as_str(),
    );
    let refuse = |why: String| origin_target_refusal(merge_context, why);
    if target == merge_context.default_branch {
        return Err(refuse(format!(
            "origin's `{target}` is not an ancestor of the landing, and the project default is \
             never rewritten"
        )));
    }
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project)
        .await
        .map_err(|error| refuse(format!("the project's database is unavailable: {error}")))?;
    if crate::orchestrator::base_advance::preparation::live_branch_owner(&db, project, target)
        .await
        .map_err(&refuse)?
        .is_none()
    {
        return Err(refuse(format!(
            "origin's `{target}` is not an ancestor of the landing, and `{target}` has no live \
             owner, so nothing proves its rewrite is the store's own"
        )));
    }
    let claims = crate::orchestrator::base_advance::preparation::accepted_drop_claims(
        &db,
        project,
        &store.path,
        target,
    )
    .await
    .map_err(&refuse)?;
    Ok(claims
        .into_iter()
        .map(
            |(head_after, dropped_commit_ids)| crate::jj::AcceptedAdvanceDrops {
                head_after,
                dropped_commit_ids,
            },
        )
        .collect())
}

/// The landing a fast-forward of the integration branch to the prepared head
/// would be: the head itself, judged against the prepared target. Read-only;
/// the caller holds the store lock and has checked both bookmarks still name
/// the prepared coordinate.
fn fast_forward_prospect(
    store: &Store,
    merge_context: &MergeMrContext,
    prepared: &PreparedBranch,
) -> Result<ProspectiveMerge, String> {
    let (head, base) = (prepared.head_after.as_str(), prepared.base_commit.as_str());
    let descends =
        crate::jj::revset_commit(&store.jj, &store.path, &format!("{base} & ::{head}")).is_some();
    if !descends {
        return Err(format!(
            "Refusing to merge `{}`: its prepared head {head} does not descend from `{}` at \
             {base}, so it cannot land on it as a fast-forward. Nothing changed.",
            merge_context.source_branch, merge_context.target_branch
        ));
    }
    let patch = store.jj.run(
        &store.path,
        &[
            "diff",
            "--ignore-working-copy",
            "--git",
            "--from",
            base,
            "--to",
            head,
        ],
        "jj diff child landing",
    )?;
    Ok(ProspectiveMerge {
        source_tip: head.to_string(),
        target_tip: base.to_string(),
        commit_id: head.to_string(),
        tree_hash: crate::jj::sealed_tree_hash_via_git(&store.jj, &store.path, head)?,
        tree_entries: crate::jj::tree_entries(&store.jj, &store.path, head)?,
        changed_files: crate::jj::parse_git_diff(&patch),
    })
}

/// Judge the landing off the store lock: every changed file parses at the
/// head, and the combined-tree checks pass on its tree.
async fn validate(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    store: &Store,
    merge_context: &MergeMrContext,
    job_id: &str,
    prospect: &ProspectiveMerge,
) -> Result<(), String> {
    let _validate = super::merge_metrics::time(super::merge_metrics::Phase::Validate);
    let (head, tip) = (prospect.commit_id.clone(), prospect.target_tip.clone());
    // Off the store lock, so every read is pinned: an unpinned jj read would
    // merge a split operation log behind the writer.
    let jj = store
        .jj
        .pin_immutable_read(&store.path, &[head.as_str(), tip.as_str()])?;
    let path = store.path.clone();
    let (target, source) = (
        merge_context.target_branch.clone(),
        merge_context.source_branch.clone(),
    );
    tokio::task::spawn_blocking(move || {
        bind::validate_landing(&jj, &path, &head, &tip, &target, &source, &[])
    })
    .await
    .map_err(|error| format!("landing validation did not finish: {error}"))??;

    let repo_path = merge_context.mr.repo_path.as_str();
    match crate::execution::checks::verify_review_tree(
        orch,
        db.clone(),
        &merge_context.project_id,
        repo_path,
        Path::new(repo_path),
        &prospect.commit_id,
        &prospect.target_tip,
        &prospect.tree_hash,
        &prospect.tree_entries,
        &prospect.changed_files,
        job_id,
        crate::fleet::CellPriority::ReviewCheck,
    )
    .await
    {
        crate::execution::checks::ReviewTreeGateResult::Green => Ok(()),
        crate::execution::checks::ReviewTreeGateResult::CheckFailed { name, detail } => {
            Err(format!("Combined-tree check '{name}' failed: {detail}"))
        }
        crate::execution::checks::ReviewTreeGateResult::InfrastructureFailure(error) => Err(
            format!("Combined-tree verification infrastructure failure: {error}. Retry the merge."),
        ),
        crate::execution::checks::ReviewTreeGateResult::Cancelled(detail) => Err(format!(
            "Combined-tree verification was cancelled ({detail}); retry the merge."
        )),
    }
}

/// Land the validated head if its prepared coordinate still holds: `Some` with
/// what landed, `None` when the source, the target or the relationship moved,
/// or origin's target was no longer `origin_target`, which restarts
/// preparation. `origin_target` is origin's copy O for a project with a remote
/// (`None` without one). `guard` is the store lock for the final check; for a
/// publication it is released for the network push and re-taken to record it,
/// while `reservation` keeps the source and target still.
#[allow(clippy::too_many_arguments)]
async fn land_if_unchanged(
    orch: &Orchestrator,
    merge_context: &MergeMrContext,
    store: &Store,
    prepared: &PreparedBranch,
    prospect: &ProspectiveMerge,
    origin_target: Option<&OriginIntegration>,
    guard: JjStoreGuard,
    reservation: &BranchReservation,
    journal: &MergeAttemptJournal<'_>,
) -> Result<Option<Landed>, String> {
    let (source, target) = (
        merge_context.source_branch.as_str(),
        merge_context.target_branch.as_str(),
    );
    reservation_covers(reservation, store, merge_context)?;
    if let Some(moved) = coordinate_moved(store, merge_context, prepared) {
        log::info!("landing: {moved}; preparing again");
        return Ok(None);
    }
    if !prepared_landing_holds(orch, merge_context, prepared, &guard).await? {
        return Ok(None);
    }
    // Both writers are admitted under this guard before either bookmark moves
    // or anything is pinned and published.
    admit_landing_writers(orch, &guard, reservation, merge_context)
        .await
        .map_err(|refusal| {
            format!("Refusing to merge `{source}` into `{target}`: {refusal}. Nothing changed.")
        })?;
    let (head, base) = (prepared.head_after.as_str(), prepared.base_commit.as_str());

    let Some(origin_target) = origin_target else {
        // The store is the only copy: the integration branch moves to the
        // prepared head, and only from the prepared target.
        guard.blocking_phase("land child on integration branch", || {
            advance_landing_bookmarks(store, &[(target, base, head)])
        })?;
        if let Err(error) = repoint_landed_children(orch, merge_context, &guard).await {
            log::warn!("landing: {error}");
        }
        return Ok(Some(Landed {
            commit: Some(head.to_string()),
            published: None,
        }));
    };

    // What the store last recorded of origin's source is the source ref's
    // lease: the landing replaces exactly that, and refuses if anyone pushed
    // to the pull request head since.
    let source_origin = guard.blocking_phase("read the recorded source on origin", || {
        crate::jj::revset_commit_checked(
            &store.jj,
            &store.path,
            &format!("remote_bookmarks(exact:{source:?}, remote=exact:\"origin\")"),
        )
    })?;
    let remote_rewrite = if origin_target.rewrites {
        Some(rewrite_evidence(orch, store, merge_context).await?)
    } else {
        None
    };
    // The head is referenced by the source bookmark, but only the pin keeps it
    // alive for recovery if the outcome is unknown and the branch moves on.
    pin_landing(&merge_context.mr.repo_path, journal.attempt_id, head)?;
    let publication = Publication {
        target_branch: target.to_string(),
        expected_old: origin_target.origin.clone(),
        target_before: base.to_string(),
        landing_commit: head.to_string(),
        landing_tree: prospect.tree_hash.clone(),
        source_branch: source.to_string(),
        source_before: head.to_string(),
        source_after: head.to_string(),
        shape: PublicationShape::FastForward,
    };
    publish_pinned(
        orch,
        store,
        merge_context,
        guard,
        reservation,
        journal,
        publication,
        PublishRefs::SourceAndTarget {
            source_origin,
            remote_rewrite,
        },
    )
    .await
}
