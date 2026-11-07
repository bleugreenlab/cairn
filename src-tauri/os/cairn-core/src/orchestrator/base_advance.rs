//! Proactive base-branch advance notifications for downstream jobs.
//!
//! When a base branch advances (a Cairn PR merges, or a remote default branch
//! moves externally), every in-flight sibling branched from that base is
//! auto-rebased onto the new tip over the shared jj store. Each rebased sibling
//! is then told its branch moved, split by outcome:
//!
//! - A sibling whose rebase recorded a **conflict** gets a **Steer** system direct
//!   (naming the conflicting files) so an idle agent wakes and an active agent sees
//!   it at the next tool boundary without having its current tool call cancelled.
//!   A conflicted commit can neither push nor merge, so this is stop-the-line work,
//!   but it should steer the agent rather than interrupt the active turn.
//! - A sibling that rebased **cleanly** gets a **passive** (non-waking) note that
//!   rides along into its next natural run — its work moved underneath it but
//!   there is nothing to resolve, so it is never mechanically resumed.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::execution::routing::{owning_db_for_job, owning_db_for_project};
use crate::jj::RECONCILE_TIMEOUT;
use crate::merge_requests::queries::{
    publication_requirement_for_managed_branch, PublicationRequirement,
};
use crate::messages::delivery::{
    latest_run_for_job, queue_system_direct, queue_system_direct_once,
    queue_system_direct_once_confirmed, system_direct_once_message_id, DirectQueueDisposition,
};
use crate::messages::queued::DeliveryUrgency;
use crate::models::ExecutionSnapshot;
use crate::orchestrator::conflict_session::{
    close_open_sessions_for_branch, record_conflict_session, record_marker_state,
    supersede_stale_sessions, IncomingIdentity, MarkerState, ReplayDecision,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::executor_protocol::{ResidencyOperation, ResidencyResult};
use cairn_db::turso::params;

/// Publish a managed store-authoritative branch, recovering exactly once when jj
/// proves its record of origin is wrong: stale (origin moved unexpectedly) or
/// untracked (origin's copy was imported rather than pushed, so jj will not push
/// over it). Both take the same recovery: fetch, converge (which ends with
/// origin's copy tracked, or refuses a copy with foreign history), retry.
/// Network transfer occurs without the store mutex; import and graph
/// convergence occur under it; the verified retry is again outside it. This
/// boundary prevents slow network I/O from blocking store writers while
/// ensuring no jj mutation can fork the operation log.
pub(crate) async fn publish_managed_branch(
    orch: &Orchestrator,
    store: &Path,
    branch: &str,
) -> Result<(), String> {
    crate::orchestrator::base_branch_sync::refuse_if_behind_origin(orch, store, branch).await?;
    // Every managed push enters here. A fenced bookmark's live position is not
    // trusted, so it is never read and published. This is an admission check,
    // not a lease: a publisher already past it is cairn/6201's.
    let db = recovery_fence_db(orch, store).await;
    crate::orchestrator::branch_recovery::refuse_if_fenced(
        &db,
        store,
        &[branch],
        &format!("Publishing `{branch}`"),
    )
    .await?;
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    push_managed_branch(orch, &jj, store, branch).await?;
    let observation = orch.base_branch_sync.observe();
    if let Some(tip) = crate::jj::bookmark_commit(&jj, store, branch) {
        crate::orchestrator::base_branch_sync::record_publication(
            orch,
            store,
            branch,
            &tip,
            observation,
        )
        .await;
    }
    Ok(())
}

/// The database holding `store`'s recovery fences: the one whose reconcile
/// worker recorded them, the database owning the project the store serves.
pub(crate) async fn recovery_fence_db(orch: &Orchestrator, store: &Path) -> Arc<LocalDb> {
    for db in orch.db.all_dbs().await {
        let path = store.to_string_lossy().into_owned();
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

async fn push_managed_branch(
    orch: &Orchestrator,
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
) -> Result<(), String> {
    match crate::jj::publish_branch_to_origin(jj, store, branch).await {
        Ok(()) => return Ok(()),
        Err(crate::jj::StoreBookmarkPushError::Failed(error)) => return Err(error),
        Err(crate::jj::StoreBookmarkPushError::StaleRemote(error)) => {
            log::warn!("publish `{branch}` found stale origin state; fetching and converging once: {error}");
        }
        Err(crate::jj::StoreBookmarkPushError::UntrackedRemote(error)) => {
            log::warn!("publish `{branch}` found an untracked origin copy; fetching and converging once: {error}");
        }
    }

    let fetch_store = store.to_path_buf();
    let fetch_branch = branch.to_string();
    crate::orchestrator::base_branch_sync::with_fetch_lock(orch, store, || {
        tokio::task::spawn_blocking(move || {
            crate::jj::fetch_remote_branch_via_git(&fetch_store, "origin", &fetch_branch)
        })
    })
    .await
    .map_err(|error| format!("stale publication fetch worker failed: {error}"))??;

    {
        let guard = orch
            .acquire_jj_store_lock(
                store,
                crate::orchestrator::StoreLockClass::Background,
                format!("stale publication recovery: {branch}"),
            )
            .await;
        let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
        let _phase = guard.phase(format!("import and converge branch={branch}"));
        crate::jj::converge_managed_branch_after_remote_rewrite(&jj, store, branch)?;
    }

    crate::jj::publish_branch_to_origin(jj, store, branch)
        .await
        .map_err(|error| format!("stale publication retry failed: {error}"))
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

/// Settle a replay's conflict by the structural merges the destination declares
/// (`landing.merge` in its `.cairn/config.yaml`). Every conflicting path needs a
/// declared merge that settles it into a file that still parses, and the result
/// is proven lossless before and after it is committed; otherwise nothing is
/// touched and the conflict stays the agent's.
///
/// Each path merges from its [`ReplayBaseline`]. Merged from the stale fork, a
/// registry the branch already resolved against an earlier base would see that
/// base's entries on both sides and keep them twice.
#[cfg(test)]
type StructuralPreparedHook = Box<dyn FnOnce() + Send>;
#[cfg(test)]
static STRUCTURAL_PREPARED_HOOKS: std::sync::OnceLock<
    std::sync::Mutex<HashMap<std::path::PathBuf, StructuralPreparedHook>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn structural_prepared_test_hook(store: &Path) {
    let hook = STRUCTURAL_PREPARED_HOOKS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(store);
    if let Some(hook) = hook {
        hook();
    }
}

struct StructuralMergePlan {
    source_cut: Option<String>,
    preserve_bookmarks: BTreeSet<String>,
    tip: String,
    destination: String,
    paths: Vec<String>,
    edits: Vec<crate::orchestrator::landing::bind::FileEdit>,
}

/// Read immutable sides and prove candidate content without writer exclusion.
fn prepare_structural_merge(
    jj: &crate::jj::JjEnv,
    store: &Path,
    diagnostic: &crate::jj::ConflictDiagnostic,
    baseline: &ReplayBaseline,
    rules: &crate::orchestrator::landing::MergeRules,
) -> Result<Option<StructuralMergePlan>, String> {
    if diagnostic.condition == crate::jj::ConflictCondition::BaseDrift {
        return Ok(None);
    }
    let paths = diagnostic.conflicting_paths();
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
        source_cut: None,
        preserve_bookmarks: BTreeSet::new(),
        tip: ours.to_string(),
        destination: theirs.to_string(),
        paths,
        edits,
    }))
}

/// Apply a pre-proven candidate as one rollback window. Caller holds the store
/// lock, and neither the original tip nor the named destination may have moved.
fn apply_structural_merge(
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    destination: &str,
    plan: &StructuralMergePlan,
) -> Result<Option<crate::jj::ReconcileReport>, String> {
    if crate::jj::bookmark_commit_checked(jj, store, branch)?.as_deref() != Some(plan.tip.as_str())
        || crate::jj::revset_commit_checked(jj, store, destination)?.as_deref()
            != Some(plan.destination.as_str())
    {
        return Err(
            "structural replay inputs moved while its candidate was prepared; nothing was changed"
                .to_string(),
        );
    }
    let pre_op = crate::jj::operation_id(jj, store)?;
    let result = (|| {
        crate::orchestrator::landing::bind::seal_edits(
            jj,
            store,
            branch,
            &plan.tip,
            &plan.edits,
            "Merge declared registry lists element-wise after base advance",
        )?;
        let committed = crate::jj::bookmark_commit_checked(jj, store, branch)?
            .ok_or_else(|| format!("structural merge commit left `{branch}` unresolved"))?;
        // proven_merge already proved that this exact candidate parses and holds
        // both sides. Equality transfers that proof to the sealed bytes without
        // rerunning the potentially quadratic whole-file proof under the lock.
        for edit in &plan.edits {
            let crate::orchestrator::landing::bind::FileEdit::Rewrite { path, content } = edit
            else {
                return Err("structural replay contained an unproven rename".to_string());
            };
            if text_at_revision(jj, store, &committed, path)? != *content {
                return Err(format!(
                    "committed structural candidate differs at `{path}`"
                ));
            }
        }
        let report = crate::jj::reconcile_siblings_with_options(
            jj,
            store,
            &plan.destination,
            &[branch.to_string()],
            &crate::jj::ReconcileOptions {
                resolution: Some((
                    branch.to_string(),
                    crate::jj::ResolutionCarry::from_tip(&committed, &plan.paths),
                )),
                preserve_bookmarks: plan.preserve_bookmarks.clone(),
                source_cut: plan
                    .source_cut
                    .as_ref()
                    .map(|cut| (branch.to_string(), cut.clone())),
                ..Default::default()
            },
        )?;
        if let Some(failure) = report
            .failed
            .iter()
            .find(|failure| failure.branch == branch)
        {
            return Err(failure.error.clone());
        }
        if !report.rebased_clean.iter().any(|clean| clean == branch) {
            return Ok(None);
        }
        Ok(Some(report))
    })();
    if matches!(&result, Ok(Some(_))) {
        return result;
    }
    let cleanup = jj.cleanup_env();
    let rollback = crate::jj::restore_operation(&cleanup, store, &pre_op).and_then(|()| {
        if crate::jj::bookmark_commit_checked(&cleanup, store, branch)?.as_deref()
            == Some(plan.tip.as_str())
        {
            Ok(())
        } else {
            Err(format!(
                "restored structural replay did not recover `{branch}` at {}",
                plan.tip
            ))
        }
    });
    if let Err(rollback) = rollback {
        let cause = result
            .as_ref()
            .err()
            .map(String::as_str)
            .unwrap_or("structural replay remained conflicted");
        return Err(format!(
            "{} automatic structural merge failed ({cause}); restoring {pre_op} also failed: {rollback}",
            crate::jj::ROLLBACK_INCOMPLETE,
        ));
    }
    result.map_err(|error| {
        // The outer snapshot supersedes an inner failed undo: the entire attempt
        // has now been restored and exported, not merely its nested rebase.
        error.replace(
            crate::jj::ROLLBACK_INCOMPLETE,
            "inner rollback failed, outer rollback completed:",
        )
    })
}

#[cfg(test)]
fn try_structural_merge(
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    destination: &str,
    diagnostic: &crate::jj::ConflictDiagnostic,
    baseline: &ReplayBaseline,
    rules: &crate::orchestrator::landing::MergeRules,
) -> Result<Option<crate::jj::ReconcileReport>, String> {
    match prepare_structural_merge(jj, store, diagnostic, baseline, rules)? {
        Some(plan) => apply_structural_merge(jj, store, branch, destination, &plan),
        None => Ok(None),
    }
}

/// Replay `branch` onto `target` for a landing, by the base advance's own
/// means: jj's rebase with its parse gate, and on a conflict the structural
/// merges `target` declares. A conflict nothing settles is refused, leaving the
/// branch where it was; the landing never resolves a conflict an agent has not.
/// The caller holds the store lock.
pub(crate) fn replay_for_landing(
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    target: &str,
    target_name: &str,
    source_cut: Option<&str>,
    preserve_bookmarks: &BTreeSet<String>,
) -> Result<BTreeMap<String, crate::jj::ConflictDiagnostic>, String> {
    let outcome = crate::jj::rebase_branch_onto_with_options(
        jj,
        store,
        branch,
        target,
        &crate::jj::RebaseOptions {
            source_cut: source_cut
                .filter(|_| !crate::jj::revset_descends_from(jj, store, branch, target))
                .map(str::to_string),
            preserve_bookmarks: preserve_bookmarks.clone(),
        },
    )?;
    let diagnostic = match outcome {
        crate::jj::RebaseOutcome::Rebased { held_descendants }
        | crate::jj::RebaseOutcome::RebasedOverConflictedAncestry {
            held_descendants, ..
        } => return Ok(held_descendants),
        crate::jj::RebaseOutcome::Conflicted { diagnostic } => diagnostic,
    };
    let rules = crate::orchestrator::landing::merge_rules_at(jj, store, target);
    let baseline = ReplayBaseline::fork_only(diagnostic.base.as_deref().unwrap_or_default());
    let result = match prepare_structural_merge(jj, store, &diagnostic, &baseline, &rules) {
        Ok(Some(mut plan)) => {
            plan.source_cut = source_cut.map(str::to_string);
            plan.preserve_bookmarks = preserve_bookmarks.clone();
            apply_structural_merge(jj, store, branch, target, &plan)
        }
        Ok(None) => Ok(None),
        Err(error) => Err(error),
    };
    match result {
        Ok(Some(report)) if report.rebased_clean.iter().any(|clean| clean == branch) => Ok(report
            .conflict_diagnostics
            .into_iter()
            .filter(|(name, _)| name != branch)
            .collect()),
        Ok(_) => Err(crate::jj::base_conflict_refusal(
            target_name,
            branch,
            &diagnostic.conflicting_paths(),
        )),
        Err(error) => Err(format!(
            "{} (the declared structural merge declined: {error})",
            crate::jj::base_conflict_refusal(target_name, branch, &diagnostic.conflicting_paths())
        )),
    }
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
/// The committed side is the assessed tip. Deferred requests re-assess the
/// current tip before constructing this plan; the rebase still pins that tip.
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
    for path in session.replay_paths(&baseline.basis) {
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
    Ok(CarryPlan {
        carry: crate::jj::ResolutionCarry {
            expected_pre_tip: committed,
            paths: carried,
        },
        refused,
    })
}

/// A deferred ask authorizes a new assessment, not reuse of an old tip's proof.
/// Run under the mutation guard so the assessed tip is the one carried.
fn plan_current_requested_resolution(
    jj: &crate::jj::JjEnv,
    store: &Path,
    session: &crate::orchestrator::conflict_session::ConflictSession,
    destination: &str,
    live_tip: &str,
    drop_incoming_reason: Option<&str>,
) -> Result<
    (
        CarryPlan,
        crate::orchestrator::conflict_session::ResolutionBasis,
    ),
    String,
> {
    use crate::orchestrator::conflict_session::{ReplayDecision, RestoreProof};
    let proof = crate::orchestrator::conflict_session::assess_session_tip_exhaustively(
        jj,
        store,
        session,
        drop_incoming_reason,
    );
    match crate::orchestrator::conflict_session::decide_replay(&proof, drop_incoming_reason) {
        ReplayDecision::Refuse(refusal) => return Err(refusal),
        ReplayDecision::Proceed => {}
        ReplayDecision::ProceedOnStatedReason(caveat) => {
            log::warn!(
                "requested replay of `{}` proceeds on its retained statement: {caveat}",
                session.bookmark
            );
        }
    }
    let mut current = session.clone();
    if let RestoreProof::Assessed(assessment) = proof {
        if assessment.tip != live_tip {
            return Err(
                "the branch moved during its resolution assessment; no resolution was carried"
                    .into(),
            );
        }
        current.resolution_basis.extend(assessment.attested);
    }
    current.replay_requested_tip = Some(live_tip.to_string());
    let basis = current.resolution_basis_at(jj, store, live_tip)?;
    plan_resolution_carry(jj, store, &current, destination, live_tip).map(|plan| (plan, basis))
}

const ROLLED_BACK_CONFLICT: &str = "rolled_back_conflict";

/// One cumulative allowance for owner-requested resolution carry, rebase,
/// export and ancestry healing, rather than renewing the old command limit.
const EXPLICIT_REPLAY_BUDGET: std::time::Duration = std::time::Duration::from_secs(60);

fn replay_is_explicit_for(label: &str, branch: &str) -> bool {
    label.strip_prefix("replay requested for ") == Some(branch)
        || label.strip_prefix("resolved replay requested for ") == Some(branch)
}

#[cfg(test)]
static EXPLICIT_REPLAY_TEST_BUDGETS: std::sync::OnceLock<
    std::sync::Mutex<HashMap<std::path::PathBuf, std::time::Duration>>,
> = std::sync::OnceLock::new();

/// Test-only automatic budgets by (store, branch). A fixture's own I/O must
/// not spend the production budget of a sibling whose progress the test
/// asserts, while the branch whose timeout is under test keeps the real one.
#[cfg(test)]
static AUTOMATIC_REPLAY_TEST_BUDGETS: std::sync::OnceLock<
    std::sync::Mutex<HashMap<(std::path::PathBuf, String), std::time::Duration>>,
> = std::sync::OnceLock::new();

fn automatic_replay_budget_for(_store: &Path, _branch: &str) -> std::time::Duration {
    #[cfg(test)]
    if let Some(budget) = AUTOMATIC_REPLAY_TEST_BUDGETS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .get(&(_store.to_path_buf(), _branch.to_string()))
        .copied()
    {
        return budget;
    }
    replay_budget(false)
}

fn explicit_replay_budget_for_store(_store: &Path) -> std::time::Duration {
    #[cfg(test)]
    if let Some(budget) = EXPLICIT_REPLAY_TEST_BUDGETS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .get(_store)
        .copied()
    {
        return budget;
    }
    EXPLICIT_REPLAY_BUDGET
}

fn replay_budget(explicit: bool) -> std::time::Duration {
    if explicit {
        EXPLICIT_REPLAY_BUDGET
    } else {
        crate::jj::BACKGROUND_RECONCILE_BUDGET
    }
}

fn replay_lock_operation(
    label: &str,
    branch: &str,
    phase: &str,
    budget: std::time::Duration,
) -> String {
    if replay_is_explicit_for(label, branch) {
        format!(
            "explicit replay {phase} (budget={}s; {label}): {branch}",
            budget.as_secs_f64()
        )
    } else {
        format!("{phase} ({label}): {branch}")
    }
}

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
        // A new destination does not make the unchanged stack any cheaper.
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

async fn release_reminted_claim_after_failure(
    db: &LocalDb,
    original_intent_id: &str,
    effective_claim: &ReconcileClaim,
) {
    if effective_claim.id != original_intent_id {
        release_reconcile_claim(db, effective_claim).await;
    }
}

async fn refresh_stale_durable_intent(
    db: &LocalDb,
    store: &Path,
    target_branch: &str,
    stored_destination: &str,
    current_destination: &str,
    refresh_source: &str,
    claim: ReconcileClaim,
) -> Result<Option<ReconcileClaim>, String> {
    if stored_destination == current_destination {
        return Ok(Some(claim));
    }

    let diagnostic = format!(
        "superseded stale destination pin {stored_destination} with current destination {current_destination}"
    );
    let changed = db
        .execute(
            "UPDATE jj_reconcile_intents
             SET status = 'superseded', lease_owner = NULL, lease_expires_at = NULL,
                 last_diagnostic = ?3, updated_at = ?4
             WHERE id = ?1 AND lease_owner = ?2 AND status = 'running'",
            params![
                claim.id.as_str(),
                claim.owner.as_str(),
                diagnostic.as_str(),
                chrono::Utc::now().timestamp()
            ],
        )
        .await
        .map_err(|error| format!("supersede stale reconcile intent: {error}"))?;
    if changed == 0 {
        return Err(format!(
            "durable reconcile intent {} lost its lease while refreshing destination",
            claim.id
        ));
    }
    log::info!(
        "jj reconcile worker superseded durable intent {}: {diagnostic}",
        claim.id
    );

    let reminted = claim_reconcile_intent(
        db,
        &claim.project_id,
        store,
        target_branch,
        current_destination,
        refresh_source,
    )
    .await?;
    if reminted.is_none() {
        log::info!(
            "jj reconcile worker reaped durable intent {}: current destination {} is already owned or completed",
            claim.id,
            current_destination
        );
    }
    Ok(reminted)
}

/// A base advance must not move a job's branch while one of its process batches
/// is still executing off-turn. The suspension row is the durable execution
/// bracket: it is created before the provider turn yields and resolved only
/// after the executor result has been published. Moving the branch inside that
/// bracket can leave the resolver publishing against a coordinate that no
/// longer names the running batch.
///
/// Returns the batch rather than a yes/no, because the deferral it causes is
/// only actionable by someone who can name it: the cure is that batch ending,
/// and an agent told merely "a batch" waits on a lane it should cancel.
async fn inflight_run_batch(
    db: &LocalDb,
    siblings: &[SiblingJob],
) -> Result<Option<InflightRunBatch>, String> {
    for sibling in siblings {
        if let Some(batch) = inflight_run_batch_for_job(db, &sibling.id).await? {
            return Ok(Some(batch));
        }
    }
    Ok(None)
}

/// A `run` batch whose durable suspension row is still open: the thing that
/// holds a branch at the base-advance mutation boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InflightRunBatch {
    /// The job whose turn the batch parked. Not necessarily the node reading
    /// about it: child tasks share their parent's branch, so a child's batch
    /// holds the parent's branch too.
    pub(crate) job_id: String,
    /// That job's home URI, when it resolves: the node whose turn to stop.
    pub(crate) holder: Option<String>,
    /// The batch's placement request, which is what cancellation addresses.
    pub(crate) request_id: String,
    pub(crate) label: Option<String>,
    /// When the batch parked its turn (Unix milliseconds). The batch itself
    /// started up to one synchronous window before that.
    pub(crate) parked_at_ms: i64,
}

impl InflightRunBatch {
    /// ``run batch `REQUEST` ("LABEL") on cairn://p/…, parked since 2026-10-18 04:28 PDT``
    pub(crate) fn describe(&self) -> String {
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
        format!("run batch `{}`{label}{owner}{parked}", self.request_id)
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
/// can write to its branch. A host batch (a long response item) holds no
/// working tree and cannot commit, so it never holds the boundary: deferring
/// a rebase for minutes behind a model call would protect nothing.
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
    })
}

/// The unsettled batch rows that hold one job's branch.
///
/// A batch holds the boundary for as long as a durable row for its request is
/// unsettled, and only settlement releases it (cairn/6145 and cairn/6205,
/// agreed with the execution thread 11-05): an open run-batch wait
/// (`pending` or `resolving`), or a `run_batch_detached` row still `running`
/// for a batch that can disturb the branch. A wait abandoned while its batch
/// ran becomes such a detached row in the same write, so the agent losing
/// interest never ends the hold. The source is these durable rows, not the
/// in-memory fleet registry, for two reasons: the boundary revalidates inside
/// the store-lock bracket that moves the bookmark, where a durable row can be
/// read and the registry cannot; and the registry is per-incarnation, so for
/// minutes after a boot it reads "not in flight" for remote batches that are
/// running (tenets 309 and 311).
async fn inflight_run_batch_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<InflightRunBatch>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT condition_json, created_at FROM agent_waits
                     WHERE job_id = ?1 AND state IN ('pending', 'resolving')
                     ORDER BY created_at",
                    params![job_id.as_str()],
                )
                .await?;
            while let Some(row) = rows.next().await? {
                if let Some(batch) =
                    run_batch_from_wait(job_id.clone(), &row.text(0)?, row.get::<i64>(1)?)
                {
                    return Ok(Some(batch));
                }
            }
            drop(rows);
            let mut rows = conn
                .query(
                    "SELECT job_id, request_id, label, created_ms FROM run_batch_detached
                     WHERE job_id = ?1 AND state = 'running' AND holds_branch = 1
                     ORDER BY created_ms LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(run_batch_from_detached(&row)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|error| format!("inspect in-flight run batches: {error}"))
}

/// The open run batch that holds `branch` at the mutation boundary, across
/// every live job on it — the same owner set the automatic advance and the
/// durable sweep guard, which reach it as `siblings_for_branch` over the live
/// jobs cut from the base. A node-only lookup would report "nothing holds it"
/// to a parent whose child task, sharing the branch, still has a batch open.
///
/// `job_id` names the reader: it scopes the lookup to that job's project, and
/// the reader's own batch is preferred when several are open, since that is
/// the one it can end itself.
pub(crate) async fn inflight_run_batch_on_branch(
    db: &LocalDb,
    job_id: &str,
    branch: &str,
) -> Result<Option<InflightRunBatch>, String> {
    let (job_id, branch) = (job_id.to_string(), branch.to_string());
    let sql = format!(
        "SELECT w.job_id, w.condition_json, w.created_at
           FROM agent_waits w
           JOIN jobs j ON j.id = w.job_id
          WHERE j.project_id = (SELECT project_id FROM jobs WHERE id = ?1)
            AND j.branch = ?2
            AND w.state IN ('pending', 'resolving')
            AND {LIVE_JOB_PREDICATE}
          ORDER BY (w.job_id = ?1) DESC, w.created_at"
    );
    // The same owner set for batches that outlived their calls; see
    // [`inflight_run_batch_for_job`] for why these rows decide existence.
    let detached_sql = format!(
        "SELECT d.job_id, d.request_id, d.label, d.created_ms
           FROM run_batch_detached d
           JOIN jobs j ON j.id = d.job_id
          WHERE j.project_id = (SELECT project_id FROM jobs WHERE id = ?1)
            AND j.branch = ?2
            AND d.state = 'running' AND d.holds_branch = 1
            AND {LIVE_JOB_PREDICATE}
          ORDER BY (d.job_id = ?1) DESC, d.created_ms
          LIMIT 1"
    );
    let found = db
        .read(|conn| {
            let (job_id, branch, sql, detached_sql) = (
                job_id.clone(),
                branch.clone(),
                sql.clone(),
                detached_sql.clone(),
            );
            Box::pin(async move {
                let mut rows = conn
                    .query(&sql, params![job_id.as_str(), branch.as_str()])
                    .await?;
                while let Some(row) = rows.next().await? {
                    if let Some(batch) =
                        run_batch_from_wait(row.text(0)?, &row.text(1)?, row.get::<i64>(2)?)
                    {
                        return Ok(Some(batch));
                    }
                }
                drop(rows);
                let mut rows = conn
                    .query(&detached_sql, params![job_id.as_str(), branch.as_str()])
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some(run_batch_from_detached(&row)?)),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|error| format!("inspect in-flight run batches on `{branch}`: {error}"))?;
    Ok(match found {
        Some(batch) => Some(batch.resolve_holder(db).await),
        None => None,
    })
}

/// What a deferral at the mutation boundary is waiting on, read from the live
/// suspension rows of every job on the branch: the batch that holds it, that
/// nothing does any more, or that this could not be read. Shared by the rebase
/// page and the replay request's answer so the two never describe the same
/// hold differently.
pub(crate) fn mutation_boundary_hold(batch: &Result<Option<InflightRunBatch>, String>) -> String {
    let sweep = RECONCILE_SWEEP_INTERVAL.as_secs();
    match batch {
        Ok(Some(batch)) => format!(
            "It is held by {}: a base advance never rewrites a branch under a batch that may \
             commit to it. The hold ends when that batch finishes or is stopped (stopping the \
             turn that owns it cancels it); the durable sweep, which revisits deferred work \
             every {sweep} seconds, then performs the rebase with no further request.",
            batch.describe()
        ),
        Ok(None) => format!(
            "No run batch is in flight on this branch now, on this node or any other that shares \
             it, so nothing holds the boundary any more: the durable sweep performs the rebase on \
             its next pass (every {sweep} seconds) with no further request."
        ),
        Err(error) => {
            format!("Whether a run batch on this branch still holds it is unknown: {error}.")
        }
    }
}

#[derive(Debug)]
struct MergedJob {
    id: String,
    project_id: String,
    issue_id: Option<String>,
    /// The branch that just merged, and is about to be deleted. Anything cut
    /// from it is stranded the moment it goes.
    branch: Option<String>,
    base_branch: Option<String>,
}

struct DurableReconcileWork {
    claim: ReconcileClaim,
    store: std::path::PathBuf,
    target_branch: String,
    destination_commit: String,
    sources: Vec<String>,
}

fn on_branch_ambiguous_delivery_key(
    project_id: &str,
    branch: &str,
    fingerprint: &str,
    run_id: &str,
) -> String {
    format!("on-branch:{project_id}:{branch}:{fingerprint}:{run_id}:ambiguous")
}

async fn activate_notified_quarantines(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    pending: &[PendingReconcileQuarantine],
    notified: &[String],
) -> Result<(), String> {
    for quarantine in pending {
        if !notified.contains(&quarantine.bookmark) {
            continue;
        }
        upsert_reconcile_quarantine(
            db,
            project_id,
            store,
            &quarantine.bookmark,
            &quarantine.failure_kind,
            &quarantine.fingerprint,
            quarantine.diagnostic.as_deref(),
        )
        .await?;
    }

    Ok(())
}

struct PendingReconcileQuarantine {
    bookmark: String,
    failure_kind: String,
    fingerprint: String,
    diagnostic: Option<String>,
}

fn reconcile_has_transient_failures(failed: &[crate::jj::ReconcileFailure]) -> bool {
    failed.iter().any(|failure| {
        !crate::jj::reconcile_failure_is_permanent(crate::jj::reconcile_failure_kind(
            &failure.error,
        ))
    })
}

#[derive(Debug, Clone)]
struct ReconcileQuarantine {
    failure_kind: String,
    fingerprint: String,
    last_diagnostic: Option<String>,
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
                last_diagnostic: row.opt_text(2)?,
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

/// The caller retains its store guard until the breaker is durable.
async fn persist_report_timeouts(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
    tip: &str,
    destination: &str,
    report: &mut crate::jj::ReconcileReport,
) -> Result<bool, String> {
    let mut recorded = false;
    for failure in &mut report.failed {
        if failure.branch == branch
            && crate::jj::reconcile_failure_kind(&failure.error) == RECONCILE_TIMEOUT
        {
            failure.error = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                record_reconcile_timeout(
                    db,
                    project_id,
                    store,
                    branch,
                    tip,
                    destination,
                    &failure.error,
                ),
            )
            .await
            .map_err(|_| {
                "timed out persisting reconcile cooldown; releasing store lock".to_string()
            })??;
            recorded = true;
        }
    }
    Ok(recorded)
}

async fn defer_timeout_reconcile(
    db: &LocalDb,
    intent_id: &str,
    branch: &str,
    tip: Option<&str>,
    quarantine: &ReconcileQuarantine,
    label: &str,
) -> Result<(), String> {
    persist_reconcile_item(
        db,
        ReconcileItemUpdate {
            intent_id,
            bookmark: branch,
            observed_tip: tip,
            status: "pending",
            failure_kind: Some(RECONCILE_TIMEOUT),
            outcome_kind: Some("deferred"),
            fingerprint: Some(&quarantine.fingerprint),
            diagnostic: quarantine.last_diagnostic.as_deref(),
        },
    )
    .await?;
    log::info!(
        "jj reconcile ({label}): timeout cooldown skips unchanged {branch} at {}; failed coordinates={}, retry_at={} (a branch change releases it; a main advance does not)",
        tip.unwrap_or("missing"), quarantine.fingerprint, quarantine.retry_at()
    );
    Ok(())
}

async fn upsert_reconcile_quarantine(
    db: &LocalDb,
    project_id: &str,
    store_path: &Path,
    bookmark: &str,
    failure_kind: &str,
    fingerprint: &str,
    diagnostic: Option<&str>,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO jj_reconcile_quarantines
         (project_id, store_path, bookmark, failure_kind, fingerprint,
          last_diagnostic, strike_count, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?7)
         ON CONFLICT(project_id, store_path, bookmark) DO UPDATE SET
           failure_kind = excluded.failure_kind,
           fingerprint = excluded.fingerprint,
           last_diagnostic = excluded.last_diagnostic,
           strike_count = jj_reconcile_quarantines.strike_count + 1,
           updated_at = excluded.updated_at",
        params![
            project_id,
            store_path.to_string_lossy().as_ref(),
            bookmark,
            failure_kind,
            fingerprint,
            diagnostic,
            now
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("persist reconcile quarantine: {error}"))
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

fn divergence_fingerprint(twins: &[String]) -> String {
    let mut twins = twins.to_vec();
    twins.sort();
    twins.join("+")
}

async fn heartbeat_reconcile_intent(db: &LocalDb, claim: &ReconcileClaim) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp();
    let changed = db
        .execute(
            "UPDATE jj_reconcile_intents SET lease_expires_at = ?3, updated_at = ?4
         WHERE id = ?1 AND lease_owner = ?2 AND status = 'running'",
            params![
                claim.id.as_str(),
                claim.owner.as_str(),
                now + RECONCILE_LEASE_SECONDS,
                now
            ],
        )
        .await
        .map_err(|error| format!("heartbeat reconcile intent: {error}"))?;
    if changed == 0 {
        return Err("reconcile intent lease ownership was lost".into());
    }

    Ok(())
}

async fn mark_reconcile_delivered(db: &LocalDb, intent_id: &str) -> Result<(), String> {
    db.execute(
        "UPDATE jj_reconcile_items
         SET status = CASE WHEN status = 'graph_moved' THEN 'completed' ELSE status END,
             notification_sent = 1, updated_at = ?2
         WHERE intent_id = ?1 AND status IN ('graph_moved', 'suppressed')",
        params![intent_id, chrono::Utc::now().timestamp()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("persist reconcile delivery: {error}"))
}

#[derive(Debug, Clone)]
struct SiblingJob {
    id: String,
    branch: Option<String>,
    base_commit: Option<String>,
}

#[derive(Debug)]
struct MergeRequestInfo {
    pr_number: Option<i64>,
}

#[derive(Debug)]
struct IssueInfo {
    project_key: String,
    number: i64,
}

#[derive(Clone)]
struct BaseAdvanceNotes {
    conflict: String,
    clean: String,
    /// What advanced the base, carried through to the durable resolution session
    /// so `cairn:~/rebase` can name the incoming change instead of describing an
    /// anonymous "the base moved".
    incoming: IncomingIdentity,
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

    // jj is the only substrate: a base advance is reconciled by a non-blocking
    // auto-rebase of in-flight siblings over the shared store. The advance
    // propagates through the commit graph itself; conflicts are recorded (not
    // blocking) and no sibling rebase/force-push is required.
    let Some(repo_path) = load_project_repo_path(&orch.db, &merged_job.project_id).await? else {
        log::debug!(
            "Skipping base advance reconcile for job {}: no project repo_path",
            merged_job.id
        );
        return Ok(());
    };
    reconcile_jj_downstream(
        orch,
        &db,
        merged_job_id,
        &merged_job,
        base_branch,
        &repo_path,
    )
    .await
}

/// Sentinel for sibling selection when no merged job should be excluded.
const EXCLUDE_NONE: &str = "";

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct BranchAdvanceOutcome {
    eligible: usize,
    rebased_clean: usize,
    conflicted: usize,
    failed: usize,
    coalesced_destination: Option<String>,
    /// The intent a coalesced call folded into, so a requester can be told
    /// which already-running work absorbed it rather than that it was queued.
    coalesced_intent: Option<String>,
    /// The completed explicit pass's failure, also persisted for the rebase page.
    replay_diagnostic: Option<String>,
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

/// Reconcile in-flight siblings of a merged jj job by auto-rebasing each onto
/// the locally-advanced integration tip over the shared store and pushing the
/// cleanly-rebased ones so their PR heads advance. Non-blocking: conflicts are
/// recorded for the agent to resolve, and a conflicted sibling is woken (via a
/// `Steer` system direct) to resolve and re-seal so its PR can advance.
/// Cleanly-rebased siblings get a passive (non-waking) note that their branch
/// moved.
async fn reconcile_jj_downstream(
    orch: &Orchestrator,
    db: &LocalDb,
    merged_job_id: &str,
    merged_job: &MergedJob,
    base_branch: &str,
    repo_path: &str,
) -> Result<(), String> {
    // Serialize each bounded jj mutation on the project store. The on-branch
    // advance is one transaction; downstream reconciliation later reacquires per
    // sibling and yields between them. Merge, webhook, and startup triggers can
    // overlap, so the durable intent lease coalesces them while the shared mutex
    // remains the sole jj/ref/history writer.
    // The refresh holds the store only while capturing the advanced coordinate;
    // database notification and executor work do not require writer exclusion.
    refresh_advanced_branch_cells(orch, db, &merged_job.project_id, base_branch, repo_path).await;

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
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        let _guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Foreground,
                "retarget held children after parent landing",
            )
            .await;
        let retarget = match crate::jj::bookmark_commit_checked(&jj, &store, base_branch) {
            Ok(Some(destination)) => {
                repoint_children_of_merged_branch(
                    db,
                    &merged_job.project_id,
                    merged_branch,
                    base_branch,
                    &store,
                    &destination,
                )
                .await
            }
            Ok(None) => Err(format!(
                "retarget destination `{base_branch}` did not resolve"
            )),
            Err(error) => Err(error),
        };
        if let Err(error) = retarget {
            log::warn!(
                "merged job {}: could not re-point work cut from `{merged_branch}` onto \
                 `{base_branch}`: {error}",
                merged_job.id
            );
        }
    }

    let siblings =
        load_sibling_jobs(db, &merged_job.project_id, base_branch, &merged_job.id).await?;
    if siblings.is_empty() {
        log::debug!(
            "jj base advance for merged job {}: no in-flight siblings to reconcile",
            merged_job.id
        );
        return Ok(());
    }

    // The store's local bookmark for the base now holds the merge: a local fold
    // wrote it directly, and for a default-branch PR that GitHub merged, the
    // origin reconcile above imported it (when its fetch succeeded). So the
    // rebase dest is the bare local bookmark.
    let issue_info = match merged_job.issue_id.as_deref() {
        Some(issue_id) => load_issue_info(db, issue_id).await?,
        None => None,
    };
    let pr_number = load_merge_request_info(db, merged_job_id, &merged_job.id)
        .await?
        .and_then(|info| info.pr_number);
    let notes = BaseAdvanceNotes {
        conflict: build_jj_conflict_note(base_branch, pr_number, issue_info.as_ref()),
        clean: build_jj_clean_note(base_branch, pr_number, issue_info.as_ref()),
        incoming: IncomingIdentity {
            base_branch: base_branch.to_string(),
            pr_number,
            issue: issue_info
                .as_ref()
                .map(|issue| format!("{}/{}", issue.project_key, issue.number)),
        },
    };
    reconcile_base_advance(
        orch,
        db,
        &merged_job.project_id,
        &format!("merged job {}", merged_job.id),
        repo_path,
        base_branch,
        base_branch,
        siblings,
        notes,
    )
    .await
    .map(|_| ())
}

fn remote_default_revset(default_branch: &str) -> String {
    format!("{default_branch}@origin")
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
    let guard = orch
        .acquire_jj_store_lock(
            &store,
            crate::orchestrator::StoreLockClass::Background,
            format!("default bookmark reconcile: {ctx}"),
        )
        .await;
    let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
    let _phase = guard.phase("default bookmark reconcile");
    crate::orchestrator::branch_recovery::refuse_import_if_fenced(
        db,
        &store,
        &[branch],
        &format!("Reconciling `{branch}` onto origin"),
    )
    .await?;
    if let Err(error) = crate::jj::ensure_store_initialized(&jj, &store, repo_path) {
        log::warn!("{ctx}: ensure jj store failed; skipping default bookmark reconcile: {error}");
        return Err(format!("ensure jj store failed: {error}"));
    }
    match crate::jj::reconcile_tracked_bookmark(&jj, &store, branch) {
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

/// Bring origin's advanced default branch into the shared store and reconcile
/// everything downstream of it: the default bookmark and every in-flight
/// sibling, which is auto-rebased onto the new
/// tip — cleanly-rebased ones pushed when a pull request needs them, conflicts
/// recorded non-blocking, and each
/// sibling this reconcile actually rewrote notified (a waking `Steer` note when
/// conflicted, a passive ride-along note when clean; the before/after commit-id
/// guard in `reconcile_base_advance` gates both).
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
        let store_guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                format!("external import on {default_branch}"),
            )
            .await;
        let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
        let _phase = store_guard.phase("git ref import");
        crate::orchestrator::branch_recovery::refuse_import_if_fenced(
            &db,
            &store,
            &[default_branch],
            &format!("Importing origin's `{default_branch}`"),
        )
        .await?;
        // Origin is the authority for the branch just fetched, so its import
        // observes it wherever it went, backwards included. Every other
        // tracked branch stays guarded.
        crate::jj::ensure_store_initialized(&jj, &store, repo_path_path)
            .and_then(|()| crate::jj::import_git_with_remote_authority(&jj, &store, default_branch))
            .map_err(|error| format!("importing origin into the store failed: {error}"))?;
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
    let siblings = load_sibling_jobs(&db, project_id, default_branch, EXCLUDE_NONE).await?;
    if siblings.is_empty() {
        log::debug!("external advance on {default_branch}: no in-flight siblings to reconcile");
        return bookmark;
    }

    let notes = BaseAdvanceNotes {
        conflict: build_external_advance_conflict_note(default_branch),
        clean: build_external_advance_clean_note(default_branch),
        incoming: IncomingIdentity {
            base_branch: default_branch.to_string(),
            ..IncomingIdentity::default()
        },
    };
    reconcile_base_advance(
        orch,
        &db,
        project_id,
        &format!("external advance on {default_branch}"),
        &repo_path,
        default_branch,
        &remote_default_revset(default_branch),
        siblings,
        notes,
    )
    .await
    .map(|_| ())
    .and(bookmark)
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
        let fence_db = recovery_fence_db(orch, &store).await;
        crate::orchestrator::branch_recovery::refuse_import_if_fenced(
            &fence_db,
            &store,
            &[branch],
            &format!("Fast-forwarding `{branch}` from origin"),
        )
        .await?;
        crate::jj::ensure_store_initialized(&jj, &store, repo).and_then(|()| {
            crate::jj::fast_forward_branch_from_origin(&jj, &store, branch, &observed)
        })?
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
            refresh_advanced_branch_cells(orch, &db, project_id, branch, &repo_path).await;
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
            let siblings = load_sibling_jobs(&db, project_id, branch, EXCLUDE_NONE).await?;
            if siblings.is_empty() {
                log::debug!("origin advance of {branch}: no live work cut from it to replay");
                return Ok(());
            }
            let notes = BaseAdvanceNotes {
                conflict: build_external_advance_conflict_note(branch),
                clean: build_external_advance_clean_note(branch),
                incoming: IncomingIdentity {
                    base_branch: branch.to_string(),
                    ..IncomingIdentity::default()
                },
            };
            reconcile_base_advance(
                orch,
                &db,
                project_id,
                &format!("origin advance of {branch}"),
                &repo_path,
                branch,
                branch,
                siblings,
                notes,
            )
            .await
            .map(|_| ())
        }
    }
}

/// Tell every live job whose own branch is `branch` something about it, once
/// per `key`.
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
    for job in jobs {
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
         to it, so your reads, runs and next publication carry it, and work cut from your branch \
         is being replayed onto it. Nothing to do."
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
    db: &LocalDb,
    project: &DefaultReconcileProject,
) -> Result<(), String> {
    let repo_path = Path::new(&project.repo_path);
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, repo_path);
    let remote_default = remote_default_revset(&project.default_branch);
    let ctx = format!("startup remote advance on {}", project.default_branch);
    let before = {
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
        if let Err(error) = crate::jj::ensure_project_store(&jj, &store, repo_path) {
            log::warn!("{ctx}: ensure store failed: {error}");
            return Ok(());
        }
        crate::jj::revset_commit(&jj, &store, &remote_default)
    };

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

    let after = {
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
        crate::jj::revset_commit(&jj, &store, &remote_default)
    };
    if before == after {
        log::debug!("{ctx}: origin tip unchanged; skipping sibling reconcile");
        return Ok(());
    }
    if after.is_none() {
        log::debug!("{ctx}: origin tip did not resolve after fetch; skipping");
        return Ok(());
    }

    let siblings =
        load_sibling_jobs(db, &project.id, &project.default_branch, EXCLUDE_NONE).await?;
    if siblings.is_empty() {
        log::debug!("{ctx}: no in-flight siblings to reconcile");
        return Ok(());
    }

    let notes = BaseAdvanceNotes {
        conflict: build_external_advance_conflict_note(&project.default_branch),
        clean: build_external_advance_clean_note(&project.default_branch),
        incoming: IncomingIdentity {
            base_branch: project.default_branch.clone(),
            ..IncomingIdentity::default()
        },
    };
    reconcile_base_advance(
        orch,
        db,
        &project.id,
        &format!("startup external advance on {}", project.default_branch),
        &project.repo_path,
        &project.default_branch,
        &remote_default,
        siblings,
        notes,
    )
    .await
    .map(|_| ())
}

fn project_has_origin(orch: &Orchestrator, repo_path: &Path) -> bool {
    orch.services
        .git
        .remote_get_url(repo_path)
        .ok()
        .is_some_and(|url| !url.trim().is_empty())
}

/// Shared reconcile body for both base-advance paths (Cairn merge and external
/// default-branch advance): build the branch specs, snapshot each
/// sibling's pre-reconcile commit id, run the non-blocking auto-rebase onto
/// `rebase_dest`, then notify each sibling this reconcile actually rewrote — a
/// **waking** `Steer` note for a conflicted sibling (resolve the markers), a
/// **passive** ride-along note for a cleanly-rebased one (its branch moved, with
/// nothing to resolve).
///
/// The before/after commit-id guard makes both paths idempotent against their
/// double-fires and applies to both outcomes: a Cairn merge into the default
/// branch fires the merge path AND a GitHub `push` webhook for the same advance,
/// and a second reconcile at the same dest tip is a `jj rebase` no-op (the commit
/// id is unchanged), so `after == before` → no redundant notification, conflicted
/// or clean.
struct ReconcileClaim {
    id: String,
    owner: String,
    project_id: String,
}

const RECONCILE_LEASE_SECONDS: i64 = 600;

/// How often the durable sweep revisits queued reconcile work.
pub(crate) const RECONCILE_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

async fn release_reconcile_claim(db: &LocalDb, claim: &ReconcileClaim) {
    if let Err(error) = db
        .execute(
            "UPDATE jj_reconcile_intents
             SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL, updated_at = ?3
             WHERE id = ?1 AND lease_owner = ?2",
            params![
                claim.id.as_str(),
                claim.owner.as_str(),
                chrono::Utc::now().timestamp()
            ],
        )
        .await
    {
        log::warn!(
            "failed to release reconcile claim {} for retry: {error}",
            claim.id
        );
    }
}

async fn claim_next_reconcile_intent(db: &LocalDb) -> Result<Option<DurableReconcileWork>, String> {
    db.write(|conn| {
        Box::pin(async move {
            let now = chrono::Utc::now().timestamp();
            let mut rows = conn
                .query(
                    "SELECT id, project_id, store_path, target_branch, destination_commit,
                            trigger_sources_json
                     FROM jj_reconcile_intents
                     WHERE status = 'pending'
                        OR (status = 'running' AND COALESCE(lease_expires_at, 0) <= ?1)
                        OR (status IN ('completed', 'superseded') AND EXISTS (
                            SELECT 1 FROM jj_reconcile_items i
                            WHERE i.intent_id = jj_reconcile_intents.id
                              AND i.replay_request_id IS NOT NULL
                              AND i.replay_served_at IS NULL
                              AND i.replay_requested_at IS NOT NULL
                        ))
                     ORDER BY updated_at ASC LIMIT 1",
                    (now,),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let id = row.text(0)?;
            let project_id = row.text(1)?;
            let store = std::path::PathBuf::from(row.text(2)?);
            let target_branch = row.text(3)?;
            let destination_commit = row.text(4)?;
            let sources = serde_json::from_str(&row.text(5)?).unwrap_or_default();
            let owner = uuid::Uuid::new_v4().to_string();
            let changed = conn
                .execute(
                    "UPDATE jj_reconcile_intents
                     SET status = 'running', lease_owner = ?2, lease_expires_at = ?3,
                         updated_at = ?4
                     WHERE id = ?1 AND (status = 'pending'
                        OR (status = 'running' AND COALESCE(lease_expires_at, 0) <= ?4)
                        OR (status IN ('completed', 'superseded') AND EXISTS (
                            SELECT 1 FROM jj_reconcile_items i
                            WHERE i.intent_id = jj_reconcile_intents.id
                              AND i.replay_request_id IS NOT NULL
                              AND i.replay_served_at IS NULL
                              AND i.replay_requested_at IS NOT NULL
                        )))",
                    params![
                        id.as_str(),
                        owner.as_str(),
                        now + RECONCILE_LEASE_SECONDS,
                        now
                    ],
                )
                .await?;
            if changed == 0 {
                return Ok(None);
            }
            Ok(Some(DurableReconcileWork {
                claim: ReconcileClaim {
                    id,
                    owner,
                    project_id,
                },
                store,
                target_branch,
                destination_commit,
                sources,
            }))
        })
    })
    .await
    .map_err(|error| format!("claim pending reconcile intent: {error}"))
}

async fn execute_durable_reconcile_work(
    orch: &Orchestrator,
    db: &LocalDb,
    work: DurableReconcileWork,
) -> Result<(), String> {
    let project_id = work.claim.project_id.clone();
    let original_intent_id = work.claim.id.clone();
    let (repo_path, _) =
        crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, &project_id)
            .await
            .map_err(|error| error.to_string())?;
    let repo_path =
        repo_path.ok_or_else(|| format!("project {project_id} has no local repository path"))?;
    let effective_store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    let repository = crate::jj::repository_directory(&effective_store)
        .ok_or_else(|| format!("project {project_id} local repository did not resolve"))?;
    if crate::jj::repository_directory(&work.store).as_ref() != Some(&repository) {
        return Err(format!(
            "durable reconcile intent {} no longer matches project {project_id}'s local repository",
            work.claim.id
        ));
    }
    let siblings = load_sibling_jobs(db, &project_id, &work.target_branch, EXCLUDE_NONE).await?;
    let specs = siblings
        .iter()
        .filter_map(sibling_branch)
        .collect::<Vec<_>>();
    if specs.is_empty() {
        log::info!(
            "jj reconcile worker reaped durable intent {}: no live sibling bookmarks remain for {}",
            work.claim.id,
            work.target_branch
        );
        finish_reconcile_intent(db, &work.claim.id, &work.claim.owner, false).await?;
        return Ok(());
    }
    let external = work
        .sources
        .iter()
        .any(|source| source.contains("external advance"));
    let rebase_dest = if external {
        remote_default_revset(&work.target_branch)
    } else {
        work.target_branch.clone()
    };
    let refresh_source = if external {
        "external advance durable destination refresh"
    } else {
        "durable destination refresh"
    };
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let (pinned_dest, existing_bookmarks) = {
        let _guard = orch
            .acquire_jj_store_lock(
                &work.store,
                crate::orchestrator::StoreLockClass::Background,
                format!(
                    "durable reconcile destination refresh ({})",
                    work.target_branch
                ),
            )
            .await;
        let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
        let current_dest =
            crate::jj::revset_commit(&jj, &work.store, &rebase_dest).ok_or_else(|| {
                format!("durable reconcile destination `{rebase_dest}` did not resolve")
            })?;
        let existing_bookmarks = crate::jj::query_local_bookmarks(&jj, &work.store, &specs).ok();
        (current_dest, existing_bookmarks)
    };
    // Retargeting must not coalesce an unserved ask into a pass that already
    // completed before it arrived. This is the same destination reopen as a
    // fresh request, without changing the sweep's ordering or admission policy.
    for branch in &specs {
        if crate::orchestrator::conflict_session::load_pending_replay_request(
            db,
            &work.store,
            branch,
        )
        .await?
        .is_some()
        {
            reopen_reconcile_intent(
                db,
                &project_id,
                &work.store,
                &work.target_branch,
                &pinned_dest,
                branch,
            )
            .await?;
        }
    }
    // This is database bookkeeping, not a local-view mutation. The worker
    // revalidates the destination under its mutation guard before acting.
    let Some(claim) = refresh_stale_durable_intent(
        db,
        &work.store,
        &work.target_branch,
        &work.destination_commit,
        &pinned_dest,
        refresh_source,
        work.claim,
    )
    .await?
    else {
        return Ok(());
    };
    let notes = BaseAdvanceNotes {
        conflict: build_jj_conflict_note(&work.target_branch, None, None),
        clean: build_jj_clean_note(&work.target_branch, None, None),
        // A durable retry resumes work whose original trigger is no longer in
        // hand, so the session records the base it is landing on and nothing it
        // cannot vouch for.
        incoming: IncomingIdentity {
            base_branch: work.target_branch.clone(),
            ..IncomingIdentity::default()
        },
    };
    let label = format!("durable retry on {}", work.target_branch);
    let effective_claim = ReconcileClaim {
        id: claim.id.clone(),
        owner: claim.owner.clone(),
        project_id: claim.project_id.clone(),
    };
    let result = execute_reconcile_claim(
        orch,
        db,
        &project_id,
        &label,
        &repo_path,
        &rebase_dest,
        siblings,
        notes,
        specs,
        existing_bookmarks,
        pinned_dest,
        claim,
    )
    .await
    .map(|_| ());
    if result.is_err() {
        release_reminted_claim_after_failure(db, &original_intent_id, &effective_claim).await;
    }
    result
}

/// Return a completed intent to the sweep once a branch it deferred at the
/// mutation boundary is free to move.
///
/// A pass whose only unfinished branches are held by in-flight run batches
/// completes instead of staying open to re-defer them. Re-deferring a held branch
/// every sweep interval cannot succeed until its batch ends, and it keeps the
/// intent `running` for the batch's whole life, which turns away every explicit
/// replay at that destination. The rebase the branch owes is not dropped: its
/// item stays `pending`/`deferred`, and this reopens the intent when the batch
/// has ended, with no further base advance. The hold is judged by the reader the
/// pass itself uses, over the same live-sibling set, so the sweep reopens an
/// intent exactly when its pass would replay the branch rather than defer it
/// again.
///
/// Only the newest intent that visited a branch on a base owns its debt; an
/// older one is superseded by it and is not revisited.
async fn reopen_released_deferrals(db: &LocalDb) -> Result<(), String> {
    let sql = format!(
        "SELECT intent.id, intent.project_id, intent.target_branch, item.bookmark
           FROM jj_reconcile_intents intent
           JOIN jj_reconcile_items item ON item.intent_id = intent.id
          WHERE intent.status = 'completed'
            AND item.status = 'pending' AND item.outcome_kind = 'deferred'
            AND item.failure_kind IS NULL
            AND EXISTS (
              SELECT 1 FROM jobs j
               WHERE j.project_id = intent.project_id AND j.branch = item.bookmark
                 AND j.base_branch = intent.target_branch AND {LIVE_JOB_PREDICATE})
            AND NOT EXISTS (
              SELECT 1 FROM jj_reconcile_intents newer
                JOIN jj_reconcile_items newer_item ON newer_item.intent_id = newer.id
               WHERE newer.store_path = intent.store_path
                 AND newer.target_branch = intent.target_branch
                 AND newer.created_at > intent.created_at
                 AND newer_item.bookmark = item.bookmark)"
    );
    let candidates = db
        .read(|conn| {
            let sql = sql.clone();
            Box::pin(async move {
                let mut rows = conn.query(&sql, ()).await?;
                let mut candidates = Vec::new();
                while let Some(row) = rows.next().await? {
                    candidates.push((row.text(0)?, row.text(1)?, row.text(2)?, row.text(3)?));
                }
                Ok(candidates)
            })
        })
        .await
        .map_err(|error| format!("list deferred reconcile items: {error}"))?;
    let mut siblings_by_base: HashMap<(String, String), Vec<SiblingJob>> = HashMap::new();
    let mut reopened: HashSet<String> = HashSet::new();
    for (intent_id, project_id, target_branch, bookmark) in candidates {
        if reopened.contains(&intent_id) {
            continue;
        }
        let key = (project_id, target_branch);
        if !siblings_by_base.contains_key(&key) {
            let siblings = load_sibling_jobs(db, &key.0, &key.1, EXCLUDE_NONE).await?;
            siblings_by_base.insert(key.clone(), siblings);
        }
        let branch_siblings = siblings_for_branch(&siblings_by_base[&key], &bookmark);
        if branch_siblings.is_empty() || inflight_run_batch(db, &branch_siblings).await?.is_some() {
            continue;
        }
        let changed = db
            .execute(
                "UPDATE jj_reconcile_intents SET status = 'pending', updated_at = ?2
                  WHERE id = ?1 AND status = 'completed'",
                params![intent_id.as_str(), chrono::Utc::now().timestamp()],
            )
            .await
            .map_err(|error| format!("reopen released deferral: {error}"))?;
        if changed > 0 {
            log::info!(
                "jj reconcile worker reopened intent {intent_id}: {bookmark} is no longer held at the mutation boundary"
            );
            reopened.insert(intent_id);
        }
    }
    Ok(())
}

fn first_claim_this_sweep(claimed: &mut HashSet<String>, intent_id: &str) -> bool {
    claimed.insert(intent_id.to_owned())
}

/// Run one sweep pass so that a panic inside it becomes a value here instead of
/// unwinding the caller.
///
/// The sweep loop is the only thing that ever revisits deferred reconcile work,
/// so its liveness is the liveness of the whole recovery path, and a panic that
/// escapes into the loop retires that path for the lifetime of the process
/// without a log line. Joining a child task converts the panic into an `Err` the
/// loop can report and continue past.
pub(crate) async fn supervised_sweep_pass<F>(pass: F) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(pass)
        .await
        .map_err(|error| format!("sweep pass did not complete: {error}"))
}

pub(crate) async fn sweep_reconcile_intents(orch: &Orchestrator) {
    for db in orch.db.all_dbs().await {
        // Recovery first: a due repair or validation runs on its persisted
        // backoff from startup and every sweep, with no operator in the loop.
        crate::orchestrator::branch_recovery::run_due_recoveries(orch, &db).await;
        if let Err(error) = reopen_released_deferrals(&db).await {
            log::warn!("jj reconcile worker could not reopen released deferrals: {error}");
        }
        let mut claimed_this_pass = HashSet::new();
        loop {
            let work = match claim_next_reconcile_intent(&db).await {
                Ok(Some(work)) => work,
                Ok(None) => break,
                Err(error) => {
                    log::warn!("jj reconcile worker failed to claim durable work: {error}");
                    break;
                }
            };
            if !first_claim_this_sweep(&mut claimed_this_pass, &work.claim.id) {
                release_reconcile_claim(&db, &work.claim).await;
                break;
            }
            let claim = ReconcileClaim {
                id: work.claim.id.clone(),
                owner: work.claim.owner.clone(),
                project_id: work.claim.project_id.clone(),
            };
            if let Err(error) = execute_durable_reconcile_work(orch, &db, work).await {
                log::warn!(
                    "jj reconcile worker failed durable intent {}: {error}",
                    claim.id
                );
                release_reconcile_claim(&db, &claim).await;
                break;
            }
        }
        retry_pending_materializations(orch, &db).await;
    }
}

/// Re-attempt marker materializations left pending, typically because the
/// executor holding the checkout was unreachable at conflict time.
///
/// Pending is deliberately a state rather than a silent failure: the wake told
/// the agent markers were not confirmed, and this is what eventually makes that
/// sentence change. A session whose markers land here is not re-notified — the
/// agent already has the coordinates and the resource — so this only upgrades
/// what `cairn:~/rebase` reports.
async fn retry_pending_materializations(orch: &Orchestrator, db: &LocalDb) {
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

async fn claim_reconcile_intent(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    target_branch: &str,
    destination: &str,
    source: &str,
) -> Result<Option<ReconcileClaim>, String> {
    let project_id = project_id.to_string();
    let store = store.to_string_lossy().into_owned();
    let target_branch = target_branch.to_string();
    let destination = destination.to_string();
    let source = source.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let store = store.clone();
        let target_branch = target_branch.clone();
        let destination = destination.clone();
        let source = source.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, trigger_sources_json, status, lease_expires_at
                     FROM jj_reconcile_intents
                     WHERE project_id = ?1 AND store_path = ?2
                       AND target_branch = ?3 AND destination_commit = ?4",
                    params![
                        project_id.as_str(),
                        store.as_str(),
                        target_branch.as_str(),
                        destination.as_str()
                    ],
                )
                .await?;
            let now = chrono::Utc::now().timestamp();
            if let Some(row) = rows.next().await? {
                let id = row.text(0)?;
                let mut sources: Vec<String> =
                    serde_json::from_str(&row.text(1)?).unwrap_or_default();
                if !sources.contains(&source) {
                    sources.push(source);
                }
                let status = row.text(2)?;
                let lease_expires_at = row.get::<Option<i64>>(3)?.unwrap_or(0);
                let sources = serde_json::to_string(&sources).unwrap_or_else(|_| "[]".into());
                conn.execute(
                    "UPDATE jj_reconcile_intents
                     SET trigger_sources_json = ?2, updated_at = ?3
                     WHERE id = ?1",
                    params![id.as_str(), sources.as_str(), now],
                )
                .await?;
                if status == "completed" || (status == "running" && lease_expires_at > now) {
                    return Ok(None);
                }
                let owner = uuid::Uuid::new_v4().to_string();
                let claimed = conn.execute(
                    "UPDATE jj_reconcile_intents
                     SET status = 'running', lease_owner = ?2, lease_expires_at = ?3,
                         updated_at = ?4 WHERE id = ?1 AND (status != 'running' OR lease_expires_at <= ?4)",
                    params![id.as_str(), owner.as_str(), now + RECONCILE_LEASE_SECONDS, now],
                )
                .await?;
                if claimed == 0 {
                    return Ok(None);
                }
                return Ok(Some(ReconcileClaim {
                    id,
                    owner,
                    project_id,
                }));
            }

            let id = uuid::Uuid::new_v4().to_string();
            let owner = uuid::Uuid::new_v4().to_string();
            let sources = serde_json::to_string(&vec![source]).unwrap();
            conn.execute(
                "INSERT INTO jj_reconcile_intents
                 (id, project_id, store_path, target_branch, destination_commit,
                  trigger_sources_json, status, lease_owner, lease_expires_at, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7, ?8, ?9, ?9)",
                params![
                    id.as_str(),
                    project_id.as_str(),
                    store.as_str(),
                    target_branch.as_str(),
                    destination.as_str(),
                    sources.as_str(),
                    owner.as_str(),
                    now + RECONCILE_LEASE_SECONDS,
                    now
                ],
            )
            .await?;
            Ok(Some(ReconcileClaim {
                id,
                owner,
                project_id,
            }))
        })
    })
    .await
    .map_err(|error| format!("claim reconcile intent: {error}"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HeldReplaySource {
    pub(crate) intent_id: String,
    pub(crate) destination: String,
    pub(crate) base_branch: String,
    pub(crate) cut: Option<String>,
    pub(crate) tip: String,
}

/// The outstanding receipt is independent of the session's open/resolved state.
/// A NULL cut with a recorded tip means held-but-unsafe, never ordinary replay.
pub(crate) async fn held_replay_source(
    db: &LocalDb,
    store: &Path,
    branch: &str,
) -> Result<Option<HeldReplaySource>, String> {
    let path = store.to_string_lossy().into_owned();
    let branch = branch.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT n.target_branch, i.held_replay_source_cut, i.held_replay_source_tip, n.id, n.destination_commit
             FROM jj_reconcile_items i JOIN jj_reconcile_intents n ON n.id = i.intent_id
             WHERE n.store_path = ?1 AND i.bookmark = ?2 AND i.held_replay_source_tip IS NOT NULL",
                    params![path, branch],
                )
                .await?;
            let mut sources = BTreeSet::new();
            while let Some(row) = rows.next().await? {
                sources.insert((row.text(0)?, row.opt_text(1)?, row.text(2)?, row.text(3)?, row.text(4)?));
            }
            if sources.len() > 1 {
                return Err(DbError::Row(
                    "held replay has competing source receipts".to_string(),
                ));
            }
            Ok(sources
                .into_iter()
                .next()
                .map(|(base_branch, cut, tip, intent_id, destination)| HeldReplaySource {
                    intent_id,
                    destination,
                    base_branch,
                    cut,
                    tip,
                }))
        })
    })
    .await
    .map_err(|error| format!("read held replay source: {error}"))
}

/// Snapshot all outstanding branch obligations while holding the writer guard.
/// Implicit descendant rewrites cannot consume another branch's original cut.
pub(crate) async fn held_replay_bookmarks(
    db: &LocalDb,
    store: &Path,
) -> Result<BTreeSet<String>, String> {
    let store = store.to_string_lossy().into_owned();
    db.read(move |conn| {
        let store = store.clone();
        Box::pin(async move {
            let mut rows = conn.query("SELECT DISTINCT i.bookmark FROM jj_reconcile_items i JOIN jj_reconcile_intents n ON n.id=i.intent_id WHERE n.store_path=?1 AND i.held_replay_source_tip IS NOT NULL", params![store]).await?;
            let mut names = BTreeSet::new();
            while let Some(row) = rows.next().await? { names.insert(row.text(0)?); }
            Ok(names)
        })
    }).await.map_err(|error| format!("read outstanding held bookmarks: {error}"))
}

/// Physical workspace barriers carry a database facade rather than a logical
/// branch coordinate. Match canonical repository identity, not mutable markers
/// or a project name guessed from the workspace path.
pub(crate) async fn held_replay_bookmarks_for_repository(
    dbs: &crate::db::DbState,
    worktree: &Path,
) -> Result<BTreeSet<String>, String> {
    let repository = crate::jj::repository_directory(worktree)
        .ok_or_else(|| "cannot identify ancestry-heal repository".to_string())?;
    let mut held = BTreeSet::new();
    for db in dbs.all_dbs().await {
        let stores = db.read(|conn| Box::pin(async move {
            let mut rows = conn.query("SELECT DISTINCT n.store_path FROM jj_reconcile_intents n JOIN jj_reconcile_items i ON i.intent_id=n.id WHERE i.held_replay_source_tip IS NOT NULL", ()).await?;
            let mut stores = Vec::new();
            while let Some(row) = rows.next().await? { stores.push(row.text(0)?); }
            Ok(stores)
        })).await.map_err(|error| error.to_string())?;
        for store in stores {
            let store = Path::new(&store);
            if crate::jj::repository_directory(store).as_ref() == Some(&repository) {
                held.extend(held_replay_bookmarks(&db, store).await?);
            }
        }
    }
    Ok(held)
}

pub(crate) async fn held_replay_source_cut(
    orch: &Orchestrator,
    db: &LocalDb,
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    base_branch: &str,
) -> Result<Option<String>, String> {
    let Some(source) = held_replay_source(db, store, branch).await? else {
        return Ok(None);
    };
    if source.base_branch != base_branch {
        return Err(format!("held replay of `{branch}` records immediate parent `{}`, not `{base_branch}`; no replay was attempted", source.base_branch));
    }
    let repository = crate::jj::repository_directory(store)
        .ok_or_else(|| "held replay repository did not resolve".to_string())?;
    let dbs = orch.db.all_dbs().await;
    let mut databases = vec![db];
    databases.extend(
        dbs.iter()
            .map(AsRef::as_ref)
            .filter(|candidate| !std::ptr::eq(*candidate, db)),
    );
    for owner_db in databases {
        if let Some(parent) =
            crate::orchestrator::conflict_session::load_active_session(owner_db, base_branch)
                .await?
        {
            if crate::jj::repository_directory(Path::new(&parent.store_path)).as_ref()
                == Some(&repository)
            {
                return Err(format!("held replay of `{branch}` waits for immediate parent `{base_branch}` to resolve its session"));
            }
        }
    }
    let cut = source.cut.ok_or_else(|| format!("held replay of `{branch}` has no proven original source cut; repair its ownership coordinates before replaying"))?;
    // A bound landing may already have replayed inside its still-speculative
    // transaction. No further replay is needed, but only its accepting caller
    // may consume the durable receipt.
    if crate::jj::revset_descends_from(jj, store, branch, base_branch) {
        return Ok(Some(cut));
    }
    if !crate::jj::revset_descends_from(jj, store, branch, &source.tip)
        || !crate::jj::revset_descends_from(jj, store, &source.tip, &cut)
    {
        return Err(format!("held replay of `{branch}` no longer descends from its recorded tip and source cut; no replay was attempted"));
    }
    Ok(Some(cut))
}

/// Successful accepted replay is the only consumer. Preview rollback and a
/// no-op on the unchanged original parent leave the receipt outstanding.
pub(crate) async fn clear_held_replay_source(
    db: &LocalDb,
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    destination: &str,
    accepted_head: &str,
    expected: Option<&HeldReplaySource>,
) -> Result<(), String> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let Some(cut) = expected.cut.as_deref() else {
        return Ok(());
    };
    if cut == destination {
        return Ok(());
    }
    if !crate::jj::revset_descends_from(jj, store, accepted_head, destination) {
        return Err(format!("cannot consume held replay of `{branch}`: accepted head does not carry its destination"));
    }
    // The caller retains the writer guard from capture through acceptance. The
    // immutable tuple is also a compare-and-swap fence against another receipt
    // generation; installing an obligation never makes an older one consumable.
    let (store, branch, expected) = (store.to_path_buf(), branch.to_string(), expected.clone());
    db.write(|conn| {
        let (store, branch, expected) = (store.clone(), branch.clone(), expected.clone());
        Box::pin(
            async move { consume_held_replay_source_conn(conn, &store, &branch, &expected).await },
        )
    })
    .await
    .map_err(|error| format!("consume held replay source: {error}"))?;
    Ok(())
}

/// Compare-and-swap only this captured receipt inside an accepting transaction.
/// Callers prove acceptance authority before entering the database transaction:
/// destination coverage for replay, or original immutable-tip binding for rebuild.
/// Explicit rebuild may also retire a held-but-unsafe receipt with no source cut.
pub(crate) async fn consume_held_replay_source_conn(
    conn: &crate::storage::TrackedConnection,
    store: &Path,
    branch: &str,
    expected: &HeldReplaySource,
) -> DbResult<u64> {
    Ok(conn.execute(
        "UPDATE jj_reconcile_items SET held_replay_source_cut = NULL, held_replay_source_tip = NULL
         WHERE bookmark = ?1 AND held_replay_source_cut IS ?3 AND held_replay_source_tip = ?4
           AND intent_id = ?6 AND intent_id IN (SELECT id FROM jj_reconcile_intents WHERE store_path = ?2 AND target_branch = ?5 AND destination_commit = ?7)",
        params![branch, store.to_string_lossy().as_ref(), expected.cut.as_deref(), expected.tip.as_str(), expected.base_branch.as_str(), expected.intent_id.as_str(), expected.destination.as_str()],
    ).await?)
}

/// A new bookmark must not erase an outstanding held-lineage obligation.
/// Existing-branch restart keeps the receipt; a successor waits for recovery on
/// the original branch instead of inventing a cut from today's parent tip.
pub(crate) async fn refuse_held_continuity(
    db: &LocalDb,
    jj: &crate::jj::JjEnv,
    store: &Path,
    adopt_head: &str,
) -> Result<(), String> {
    let path = store.to_string_lossy().into_owned();
    let rows = db
        .read(move |conn| {
            Box::pin(async move {
                let mut rows = conn.query(
            "SELECT DISTINCT i.bookmark, i.held_replay_source_tip FROM jj_reconcile_items i
             JOIN jj_reconcile_intents n ON n.id=i.intent_id
             WHERE n.store_path=?1 AND i.held_replay_source_tip IS NOT NULL",
            params![path],
        ).await?;
                let mut tips = Vec::new();
                while let Some(row) = rows.next().await? {
                    tips.push((row.text(0)?, row.text(1)?));
                }
                Ok(tips)
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    for (branch, tip) in rows {
        if crate::jj::revset_descends_from(jj, store, adopt_head, &tip) {
            return Err(format!("continuity from held branch `{branch}` is refused: recover its exact source-cut replay before minting a successor bookmark"));
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum BranchOwnerKind {
    Mint,
    Observer,
    Unknown,
}

/// Mint authority and cut consensus use the same frozen behavior. Missing
/// standalone workflow metadata is not evidence of either ownership or inherit.
async fn branch_owner_kind(
    orch: &Orchestrator,
    db: &LocalDb,
    owner: &SiblingJob,
    branch: &str,
) -> Result<BranchOwnerKind, String> {
    let metadata = db.query_one(
        "SELECT p.branch,j.execution_id,j.recipe_node_id,j.project_id FROM jobs j LEFT JOIN jobs p ON p.id=j.parent_job_id WHERE j.id=?1",
        params![owner.id.as_str()],
        |row| Ok((row.opt_text(0)?,row.opt_text(1)?,row.opt_text(2)?,row.text(3)?)),
    ).await.map_err(|error| error.to_string())?;
    if metadata.0.as_deref() == Some(branch) {
        return Ok(BranchOwnerKind::Observer);
    }
    if let (Some(execution), Some(node_id)) = (metadata.1.as_ref(), metadata.2.as_ref()) {
        let snapshot = match crate::jobs::queries::load_execution_snapshot(db, execution).await {
            Ok(snapshot) => snapshot,
            Err(_) => return Ok(BranchOwnerKind::Unknown),
        };
        let Some(node) = snapshot
            .recipe
            .nodes
            .iter()
            .find(|node| &node.id == node_id)
        else {
            return Ok(BranchOwnerKind::Unknown);
        };
        let node = crate::execution::dag::recipe_node_to_db(node, &snapshot.recipe.id);
        return Ok(
            if crate::execution::step_behavior::resolve_node_behavior(&node).mints_branch {
                BranchOwnerKind::Mint
            } else {
                BranchOwnerKind::Observer
            },
        );
    }
    if metadata.2.is_none() && orch.db.local.query_opt_i64(
        "SELECT 1 FROM workflow_run WHERE job_id=?1 AND project_id=?2 AND anchor_branch=?3 AND branch_mode='inherit' AND execution_id IS ?4 LIMIT 1",
        params![owner.id.as_str(),metadata.3.as_str(),branch,metadata.1.as_deref()],
    ).await.map_err(|error| error.to_string())?.is_some() {
        return Ok(BranchOwnerKind::Observer);
    }
    Ok(BranchOwnerKind::Unknown)
}

async fn original_held_source_cut(
    orch: &Orchestrator,
    db: &LocalDb,
    store: &Path,
    branch: &str,
    base_branch: &str,
    tip: &str,
    owners: &[SiblingJob],
) -> Result<Option<String>, String> {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    if let Some(source) = held_replay_source(db, store, branch).await? {
        return Ok(
            if source.base_branch == base_branch
                && crate::jj::revset_descends_from(&jj, store, tip, &source.tip)
            {
                source.cut
            } else {
                None
            },
        );
    }
    let mut cuts = BTreeSet::new();
    let mut uncertain = false;
    for owner in owners {
        match branch_owner_kind(orch, db, owner, branch).await? {
            BranchOwnerKind::Observer => continue,
            BranchOwnerKind::Unknown => {
                uncertain = true;
                continue;
            }
            BranchOwnerKind::Mint => {}
        }
        match owner.base_commit.as_ref() {
            Some(cut) if crate::jj::revset_descends_from(&jj, store, tip, cut) => {
                cuts.insert(cut.clone());
            }
            _ => uncertain = true,
        }
    }
    Ok(if !uncertain && cuts.len() == 1 {
        cuts.into_iter().next()
    } else {
        None
    })
}

/// Record held bookmarks against their own immediate destination. The VCS
/// operation knows graph descendants, not job ownership: a failed replay of a
/// parent also flags its descendants, which must wait rather than resolve onto
/// the selected ancestor independently. Caller holds the replay's store writer
/// guard through installation, marker projection, and notification: otherwise a
/// queued writer could install a newer conflict before this one. Database and
/// executor work here extends the guard beyond the jj-only replay budget.
pub(crate) async fn persist_held_descendant_sessions(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    held: &BTreeMap<String, crate::jj::ConflictDiagnostic>,
) -> Result<(), String> {
    let repository = crate::jj::repository_directory(store)
        .ok_or_else(|| "held descendant repository did not resolve".to_string())?;
    let dbs = orch.db.all_dbs().await;
    let mut databases = vec![db];
    databases.extend(
        dbs.iter()
            .map(AsRef::as_ref)
            .filter(|candidate| !std::ptr::eq(*candidate, db)),
    );
    for branch in held.keys() {
        let mut routes = Vec::new();
        for owning_db in &databases {
            let query_branch = branch.clone();
            let candidates = owning_db.read(move |conn| {
                let branch = query_branch.clone();
                Box::pin(async move {
                    let mut rows = conn.query(&format!("SELECT DISTINCT j.project_id,p.repo_path,p.key FROM jobs j JOIN projects p ON p.id=j.project_id WHERE j.branch=?1 AND {LIVE_JOB_PREDICATE}"), params![branch]).await?;
                    let mut candidates = Vec::new();
                    while let Some(row) = rows.next().await? { candidates.push((row.text(0)?,row.text(1)?,row.text(2)?)); }
                    Ok(candidates)
                })
            }).await.map_err(|error| error.to_string())?;
            for (project, synced_repo, key) in candidates {
                let Some(repo) = crate::projects::crud::resolve_local_repo_path(
                    &orch.db,
                    &project,
                    &key,
                    &synced_repo,
                )
                .await
                .map_err(|error| error.to_string())?
                else {
                    continue;
                };
                let owner_store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo));
                if crate::jj::repository_directory(&owner_store).as_ref() == Some(&repository) {
                    let owners = load_jobs_on_branch(owning_db, &project, branch).await?;
                    let mut possible_mint = false;
                    for owner in owners {
                        if branch_owner_kind(orch, owning_db, &owner, branch).await?
                            != BranchOwnerKind::Observer
                        {
                            possible_mint = true;
                        }
                    }
                    if possible_mint {
                        routes.push((*owning_db, project));
                    }
                }
            }
        }
        if routes.len() > 1 {
            return Err(format!("held descendant `{branch}` has ambiguous project/database ownership in its physical repository"));
        }
        if let Some((owner_db, owner_project)) = routes.pop() {
            persist_held_descendant_sessions_for_owner(
                orch,
                owner_db,
                &owner_project,
                store,
                held,
                branch,
            )
            .await?;
        } else {
            log::warn!("held descendant `{branch}` from project {project_id} has no live owner in its physical repository");
        }
    }
    Ok(())
}

async fn persist_held_descendant_sessions_for_owner(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    held: &BTreeMap<String, crate::jj::ConflictDiagnostic>,
    selected_branch: &str,
) -> Result<(), String> {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    for (branch, raw) in held
        .iter()
        .filter(|(name, _)| name.as_str() == selected_branch)
    {
        let owners = load_jobs_on_branch(db, project_id, branch).await?;
        let mut bases = BTreeSet::new();
        for owner in &owners {
            if let Some(base) =
                crate::orchestrator::replay_base::load_base_candidates_for_job(db, &owner.id)
                    .await
                    .map_err(|error| error.to_string())?
                    .recorded
            {
                bases.insert(base);
            }
        }
        if owners.is_empty() {
            continue;
        }
        if bases.len() != 1 {
            return Err(format!(
                "held descendant `{branch}` has no unambiguous recorded immediate base"
            ));
        }
        let base = bases.into_iter().next().unwrap();
        let blocked = held.contains_key(&base);
        let held_tip = raw
            .ours
            .as_deref()
            .ok_or_else(|| format!("held branch `{branch}` has no original tip"))?;
        let source_cut =
            original_held_source_cut(orch, db, store, branch, &base, held_tip, &owners).await?;
        let destination = crate::jj::bookmark_commit(&jj, store, &base)
            .ok_or_else(|| format!("held descendant `{branch}` base `{base}` did not resolve"))?;
        let mut diagnostic = crate::jj::capture_conflict_diagnostic(
            &jj,
            store,
            branch,
            &destination,
            raw.ours.as_deref(),
            raw.conflicting_paths(),
        );
        diagnostic.conflicted_tip = raw.conflicted_tip.clone();
        diagnostic.scaffolding = raw.scaffolding.clone();
        diagnostic.validation = raw.validation.clone();
        let now = chrono::Utc::now().timestamp();
        let candidate = uuid::Uuid::new_v4().to_string();
        let store_path = store.to_string_lossy();
        db.execute(
            "INSERT INTO jj_reconcile_intents
             (id, project_id, store_path, target_branch, destination_commit,
              trigger_sources_json, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, '[\"held_descendant\"]', 'pending', ?6, ?6)
             ON CONFLICT(project_id, store_path, target_branch, destination_commit) DO NOTHING",
            params![
                candidate.as_str(),
                project_id,
                store_path.as_ref(),
                base.as_str(),
                destination.as_str(),
                now
            ],
        )
        .await
        .map_err(|error| format!("record held descendant intent: {error}"))?;
        let intent_id = reconcile_intent_id(db, project_id, store, &base, &destination)
            .await
            .ok_or_else(|| format!("held descendant `{branch}` intent was not persisted"))?;
        persist_reconcile_item(db, ReconcileItemUpdate {
            intent_id: &intent_id, bookmark: branch, observed_tip: Some(held_tip),
            status: "completed", failure_kind: None, outcome_kind: Some("held_parent"),
            fingerprint: None, diagnostic: Some("Held for immediate-parent recovery; original ownership cut is recorded separately from conflict coordinates."),
        }).await?;
        let path = store.to_string_lossy().into_owned();
        let bookmark = branch.clone();
        let intent = intent_id.clone();
        let tip = held_tip.to_string();
        db.write(move |conn| {
            let (path, bookmark, intent, tip, cut) = (path.clone(), bookmark.clone(), intent.clone(), tip.clone(), source_cut.clone());
            Box::pin(async move {
                conn.execute("UPDATE jj_reconcile_items SET held_replay_source_cut = NULL, held_replay_source_tip = NULL
                    WHERE bookmark = ?1 AND intent_id IN (SELECT id FROM jj_reconcile_intents WHERE store_path = ?2)",
                    params![bookmark.as_str(), path.as_str()]).await?;
                conn.execute("UPDATE jj_reconcile_items SET held_replay_source_cut = ?3, held_replay_source_tip = ?4
                    WHERE intent_id = ?1 AND bookmark = ?2",
                    params![intent.as_str(), bookmark.as_str(), cut.as_deref(), tip.as_str()]).await?;
                Ok(())
            })
        }).await.map_err(|error| format!("freeze held replay source: {error}"))?;
        if blocked
            || (raw.conflicting_paths().is_empty()
                && raw.scaffolding.is_none()
                && raw.validation.is_none())
        {
            continue;
        }
        let previous =
            crate::orchestrator::conflict_session::load_active_session(db, branch).await?;
        let basis = crate::orchestrator::conflict_session::inherited_basis(
            &jj,
            store,
            previous.as_ref(),
            diagnostic.ours.as_deref(),
        );
        let incoming = IncomingIdentity {
            base_branch: base,
            ..IncomingIdentity::default()
        };
        persist_reconcile_item(db, ReconcileItemUpdate {
            intent_id: &intent_id,
            bookmark: branch,
            observed_tip: diagnostic.ours.as_deref(),
            status: "completed",
            failure_kind: Some(ROLLED_BACK_CONFLICT),
            outcome_kind: Some("conflicted"),
            fingerprint: None,
            diagnostic: Some("The immediate parent advanced; this branch remains at its pre-replay commit because its own edits conflict."),
        }).await?;
        record_conflict_session(db, &intent_id, branch, &diagnostic, &incoming, &basis).await?;
        supersede_stale_sessions(db, branch, &intent_id).await?;
        withdraw_pending_clean_notes(db, branch).await?;
        let (state, marker_diagnostic, dispositions) =
            materialize_markers_for_branch(orch, db, project_id, branch, &diagnostic).await;
        record_marker_state(
            db,
            &intent_id,
            branch,
            state,
            marker_diagnostic.as_deref(),
            &dispositions,
        )
        .await?;
        let evidence = ConflictEvidence {
            diagnostics: HashMap::from([(branch.clone(), diagnostic)]),
            marker_states: HashMap::from([(branch.clone(), state)]),
            carry_notes: HashMap::new(),
        };
        notify_conflicted_siblings(
            orch, db, &owners, std::slice::from_ref(branch),
            "Your immediate parent branch advanced, but replaying your own edits conflicted. Your branch remains at its previous clean commit; resolve your rebase session.",
            &evidence, &intent_id,
        )?;
    }
    Ok(())
}

#[derive(Debug)]
struct ReconcileItemProgress {
    status: String,
    observed_tip: Option<String>,
    fingerprint: Option<String>,
    failure_kind: Option<String>,
    outcome_kind: Option<String>,
    notification_sent: bool,
}

async fn reconcile_item_status(
    db: &LocalDb,
    intent_id: &str,
    bookmark: &str,
) -> Result<Option<ReconcileItemProgress>, String> {
    let intent_id = intent_id.to_string();
    let bookmark = bookmark.to_string();
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn.query(
            "SELECT status, observed_tip, suppression_fingerprint, failure_kind, outcome_kind, notification_sent
             FROM jj_reconcile_items WHERE intent_id = ?1 AND bookmark = ?2",
            params![intent_id.as_str(), bookmark.as_str()],
        ).await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(ReconcileItemProgress {
                status: row.text(0)?,
                observed_tip: row.get::<Option<String>>(1)?,
                fingerprint: row.get::<Option<String>>(2)?,
                failure_kind: row.get::<Option<String>>(3)?,
                outcome_kind: row.get::<Option<String>>(4)?,
                notification_sent: row.get::<i64>(5)? != 0,
            }))
        })
    })
    .await
    .map_err(|error| format!("load reconcile item progress: {error}"))
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

async fn finish_reconcile_intent(
    db: &LocalDb,
    intent_id: &str,
    owner: &str,
    retry_transient: bool,
) -> Result<(), String> {
    let status = if retry_transient {
        "pending"
    } else {
        "completed"
    };
    db.execute(
        format!("UPDATE jj_reconcile_intents
         SET status = CASE WHEN ?3='completed' AND EXISTS (
             SELECT 1 FROM jj_reconcile_items item JOIN jj_reconcile_intents n ON n.id=item.intent_id
             JOIN jobs j ON j.project_id=n.project_id AND j.branch=item.bookmark AND j.base_branch=n.target_branch
             WHERE item.intent_id=?1 AND item.status='pending' AND item.outcome_kind='held_retarget' AND {LIVE_JOB_PREDICATE}
         ) THEN 'pending' ELSE ?3 END,
         lease_owner = NULL, lease_expires_at = NULL, updated_at = ?4
         WHERE id = ?1 AND lease_owner = ?2"),
        params![intent_id, owner, status, chrono::Utc::now().timestamp()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("complete reconcile intent: {error}"))
}

/// The id of the intent that owns one destination, for naming it in a summary.
/// Best effort: a read failure costs the name, never the request.
async fn reconcile_intent_id(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    target_branch: &str,
    destination: &str,
) -> Option<String> {
    let store = store.to_string_lossy().into_owned();
    db.query_text(
        "SELECT id FROM jj_reconcile_intents
         WHERE store_path = ?1 AND target_branch = ?2 AND destination_commit = ?3 AND project_id = ?4",
        params![store.as_str(), target_branch, destination, project_id],
    )
    .await
    .ok()
    .flatten()
}

async fn wait_for_reconcile_slot(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    target_branch: &str,
    destination: &str,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    let store = store.to_string_lossy().into_owned();
    loop {
        let running = db
            .query_text(
                "SELECT status FROM jj_reconcile_intents
                 WHERE store_path = ?1 AND target_branch = ?2 AND destination_commit = ?3 AND project_id = ?4",
                params![store.as_str(), target_branch, destination, project_id],
            )
            .await
            .map_err(|error| format!("inspect reconcile intent: {error}"))?
            .as_deref()
            == Some("running");
        if !running {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "The reconcile already running for `{target_branch}` at `{destination}` did not \
                 yield a follow-on slot before its lease deadline. No replay was silently accepted; \
                 request it again after the running reconcile is recovered."
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_base_advance(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    label: &str,
    repo_path: &str,
    sibling_base_branch: &str,
    rebase_dest: &str,
    siblings: Vec<SiblingJob>,
    notes: BaseAdvanceNotes,
) -> Result<BranchAdvanceOutcome, String> {
    let specs: Vec<String> = siblings.iter().filter_map(sibling_branch).collect();
    if specs.is_empty() {
        log::debug!("jj base advance ({label}): no in-flight siblings with a branch to reconcile");
        return Ok(BranchAdvanceOutcome::default());
    }

    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    let (pinned_dest, existing_bookmarks) = {
        let guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                format!("sibling reconcile preparation ({label})"),
            )
            .await;
        let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
        let _phase = guard.phase(format!(
            "bookmark listing and destination resolution candidate_count={} queried_count={}",
            specs.len(),
            specs.len()
        ));
        let pinned_dest = crate::jj::revset_commit(&jj, &store, rebase_dest).ok_or_else(|| {
            format!("jj base advance ({label}): destination `{rebase_dest}` did not resolve")
        })?;
        let candidate_names = specs.to_vec();
        // Failure deliberately falls back to processing every candidate so a
        // read optimization can never become a liveness gate.
        let bookmarks = crate::jj::query_local_bookmarks(&jj, &store, &candidate_names).ok();
        (pinned_dest, bookmarks)
    };

    let Some(claim) = claim_reconcile_intent(
        db,
        project_id,
        &store,
        sibling_base_branch,
        &pinned_dest,
        label,
    )
    .await?
    else {
        let intent =
            reconcile_intent_id(db, project_id, &store, sibling_base_branch, &pinned_dest).await;
        log::debug!(
            "jj base advance ({label}): coalesced with existing intent {} for {pinned_dest}",
            intent.as_deref().unwrap_or("(unread)")
        );
        return Ok(BranchAdvanceOutcome {
            coalesced_destination: Some(pinned_dest),
            coalesced_intent: intent,
            ..BranchAdvanceOutcome::default()
        });
    };
    let eligible = siblings.len();
    let worker_orch = orch.clone();
    let worker_project_id = project_id.to_string();
    let worker_label = label.to_string();
    let worker_repo_path = repo_path.to_string();
    let worker_rebase_dest = rebase_dest.to_string();
    let explicit = specs
        .iter()
        .any(|branch| replay_is_explicit_for(label, branch));
    let worker = tokio::spawn(async move {
        let worker_db = match owning_db_for_project(&worker_orch.db, &worker_project_id).await {
            Ok(db) => db,
            Err(error) => {
                log::warn!("jj reconcile worker ({worker_label}) could not reopen its owning database: {error}");
                return Err(error.to_string());
            }
        };
        let retry_claim = ReconcileClaim {
            id: claim.id.clone(),
            owner: claim.owner.clone(),
            project_id: claim.project_id.clone(),
        };
        let result = execute_reconcile_claim(
            &worker_orch,
            worker_db.as_ref(),
            &worker_project_id,
            &worker_label,
            &worker_repo_path,
            &worker_rebase_dest,
            siblings,
            notes,
            specs,
            existing_bookmarks,
            pinned_dest,
            claim,
        )
        .await;
        if let Err(error) = &result {
            log::warn!("jj reconcile worker ({worker_label}) failed: {error}");
            release_reconcile_claim(worker_db.as_ref(), &retry_claim).await;
        }
        result
    });

    // Awaiting a JoinHandle does not cancel the durable worker if the caller
    // disconnects. Explicit callers receive the actual verdict, not just "queued".
    if explicit {
        return worker
            .await
            .map_err(|error| format!("explicit replay worker failed: {error}"))?;
    }

    Ok(BranchAdvanceOutcome {
        eligible,
        ..BranchAdvanceOutcome::default()
    })
}

/// The sanctioned store-side replay: ask the shared jj store to move one
/// branch's ancestry onto the base it is supposed to sit on.
///
/// This is the only surface that can do it. An agent's slot is a plain git
/// worktree whose refs are downstream EXPORTS of the runner's private jj store,
/// so no ref move made there survives — which is why the request is enqueued as
/// durable reconcile work rather than run in the caller's slot. It reuses the
/// whole existing path: per-store lock, pinned-destination validation, intent
/// lease, rollback on conflict, verified export, residency refresh.
///
/// It is needed after EVERY content conflict, not only under a base-drift
/// classification. Resolving the conflicting files with ordinary writes fixes the
/// branch's content; its ancestry is still rooted at the old base, because the
/// rebase was rolled back and nothing replays it afterwards.
///
/// An open conflict session is NOT a precondition. A session is the artifact of
/// an advance that was attempted and hit a conflict, so requiring one made the
/// remedy reachable only in the case where the automatic path had already got
/// far enough to describe the problem — and unreachable in the case that needs
/// it most, where the advance never ran at all and the branch sits silently
/// behind its base with an unmergeable PR. Without a session this replays the
/// branch onto its base as recorded on the job; if that replay conflicts, the
/// reconcile worker opens the session on the way through, which is how a branch
/// the automatic path skipped gets one.
pub(crate) async fn request_branch_replay(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    branch: &str,
    expected_fingerprint: Option<&str>,
    take_committed_tip: bool,
    drop_incoming_reason: Option<&str>,
) -> Result<String, String> {
    let session = crate::orchestrator::conflict_session::load_active_session(db, branch).await?;

    // Both of these name a session's own artifacts. A request carrying one
    // without a session describes a world that does not exist, so it is answered
    // with which artifact is missing rather than accepted and quietly
    // reinterpreted as something else.
    if session.is_none() {
        if expected_fingerprint.is_some() {
            return Err(format!(
                "`{branch}` has no open rebase session, so there are no session coordinates for a \
                 fingerprint to pin. Request the replay without one."
            ));
        }
        if take_committed_tip {
            return Err(format!(
                "`resolution:\"take-committed-tip\"` restores an open session's CONFLICTING paths \
                 from your branch's committed tip, and `{branch}` has no open session, so there \
                 is no such set of paths. Request a plain `{{action:\"replay\"}}`: it moves your \
                 branch onto the current base without taking content from either side."
            ));
        }
    }

    // A request quoting coordinates that have since moved was composed against a
    // view of the world that no longer holds. Refuse and hand back the current
    // one rather than acting on the stale intent.
    if let (Some(session), Some(expected)) = (session.as_ref(), expected_fingerprint) {
        let current = session.fingerprint();
        if expected != current {
            return Err(format!(
                "This rebase session has moved on: you named fingerprint `{expected}`, but it is \
                 now `{current}`. Re-read cairn:~/rebase and request the replay again."
            ));
        }
    }

    // `take-committed-tip` restores each conflicting path WHOLE from the
    // branch's committed tip, so every incoming hunk in such a file that lives
    // OUTSIDE the region the agent resolved is discarded along with it. Refuse
    // rather than warn: this request is asynchronous and it lands on branch
    // ancestry, so a warning is read after a compiler finds the damage rather
    // than before it is done. The refusal is not a dead end — it hands back the
    // exact content that makes the same request correct.
    let mut caveats: Vec<String> = Vec::new();
    // The tip the request guard judged. It is recorded for the page; a deferred
    // worker re-assesses the current tip under its mutation guard before carry.
    let mut request_tip: Option<String> = None;
    if let Some(session) = session.as_ref().filter(|_| take_committed_tip) {
        // Read-only, and deliberately outside the store lock: every object it
        // reads is immutable, and the one mutable thing (the bookmark) is
        // re-resolved by the replay itself. Holding the lock across dozens of
        // subprocess reads would stall the reconcile worker to no purpose.
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        let store = Path::new(&session.store_path);
        // EXHAUSTIVE, not the read path's capped form. The restore covers every
        // conflicting path, so the decision has to as well; assessing a prefix
        // and accepting on its silence is the same silent loss wearing a guard's
        // clothes.
        //
        // The decision is a total match on a typed outcome rather than an
        // `if let`. Every way of NOT having a proof — base drift aside — has to
        // reach a refusal, and an `Option` here is what let "could not check"
        // fall through as "nothing to report" twice over.
        //
        // A stated reason is also the resolution of every file the tip deletes:
        // no content can show an absent file holding the incoming side, so the
        // statement is entered into the basis before the assessment runs.
        let proof = crate::orchestrator::conflict_session::assess_session_tip_exhaustively(
            &jj,
            store,
            session,
            drop_incoming_reason,
        );
        request_tip = match &proof {
            crate::orchestrator::conflict_session::RestoreProof::Assessed(assessment) => {
                Some(assessment.tip.clone())
            }
            _ => crate::jj::bookmark_commit(&jj, store, branch),
        };
        match crate::orchestrator::conflict_session::decide_replay(&proof, drop_incoming_reason) {
            ReplayDecision::Proceed => {}
            ReplayDecision::Refuse(refusal) => return Err(refusal),
            ReplayDecision::ProceedOnStatedReason(caveat) => {
                log::warn!(
                    "replay request for `{branch}`: proceeding with an unproven \
                     take-committed-tip restore on the requester's stated reason. {caveat}"
                );
                caveats.push(caveat);
            }
        }
        // Only an accepted request records its statement, and before the
        // worker is queued: the worker reloads the session from the database.
        if let crate::orchestrator::conflict_session::RestoreProof::Assessed(assessment) = &proof {
            if !assessment.newly_attested.is_empty() {
                let mut stored = session.resolution_basis.clone();
                stored.extend(assessment.attested.clone());
                crate::orchestrator::conflict_session::record_attested_basis(
                    db,
                    &session.intent_id,
                    branch,
                    &stored,
                )
                .await?;
                let paths: Vec<String> = assessment
                    .newly_attested
                    .iter()
                    .map(|path| format!("`{path}`"))
                    .collect();
                caveats.push(format!(
                    "Recorded your statement (\"{}\") as the resolution of {} deleted file(s) \
                     against `{}`: {}. A replay carries each deletion as long as the base has \
                     not changed the file since; cairn:~/rebase lists them.",
                    drop_incoming_reason.unwrap_or_default(),
                    paths.len(),
                    assessment.recorded.theirs,
                    paths.join(", ")
                ));
            }
        }
    }

    let mut project = load_replay_project(db, job_id).await?;
    let (local_repo, _) =
        crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, &project.id)
            .await
            .map_err(|error| error.to_string())?;
    project.repo_path = local_repo.ok_or_else(|| {
        "Clone this team project's repository before requesting replay".to_string()
    })?;
    // An explicit replay is not repair. A fenced branch is refused here with the
    // recovery's coordinates and route, and again at the locked boundary.
    crate::orchestrator::branch_recovery::refuse_if_fenced(
        db,
        &crate::jj::project_store_dir(&orch.config_dir, Path::new(&project.repo_path)),
        &[branch],
        "Replay",
    )
    .await?;
    let siblings = vec![SiblingJob {
        id: job_id.to_string(),
        branch: Some(branch.to_string()),
        base_commit: project.base_commit.clone(),
    }];

    // With a session, the base it recorded; without one, the base the job was
    // cut from; and if that name is gone from the store, whatever this work is
    // still going to merge into. Resolve the CURRENT head of it, not the
    // destination this session was opened against: the request is to land on the
    // base as it is now.
    let session_base = session.as_ref().map(|session| {
        if session.incoming.base_branch.is_empty() {
            session.target_branch.as_str()
        } else {
            session.incoming.base_branch.as_str()
        }
    });
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&project.repo_path));
    let (resolved, retargeted_held_session) = {
        let guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                format!("replay request for {branch}"),
            )
            .await;
        let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
        let _phase = guard.phase(format!("resolve replay destination branch={branch}"));
        let candidates =
            crate::orchestrator::replay_base::load_base_candidates_for_job(db, job_id).await?;
        let held = held_replay_source(db, &store, branch).await?;
        if let Some(held) = held.as_ref() {
            if candidates.recorded.as_deref() != Some(held.base_branch.as_str()) {
                return Err(format!("held replay of `{branch}` has no matching recorded destination authority; no replay was queued"));
            }
        }
        // A sanctioned retarget moves the receipt and job together. A historical
        // session still supplies resolution evidence, never authority to undo
        // that transition by routing the request back to its old parent.
        let current_session =
            crate::orchestrator::conflict_session::load_active_session(db, branch).await?;
        let current_session_base = current_session.as_ref().map(|session| {
            if session.incoming.base_branch.is_empty() {
                session.target_branch.as_str()
            } else {
                session.incoming.base_branch.as_str()
            }
        });
        let candidates = candidates.with_session_fallback(current_session_base);
        let resolved =
            crate::orchestrator::replay_base::resolve_base(&jj, &store, branch, &candidates)
                .map_err(|error| error.to_string())?;
        if held
            .as_ref()
            .is_some_and(|held| held.base_branch != resolved.branch)
        {
            return Err(format!("held replay of `{branch}` requires its sanctioned destination transition before falling back from a missing parent"));
        }
        let retargeted = session_base.is_some_and(|historical| historical != resolved.branch);
        (resolved, retargeted)
    };
    let base_branch = resolved.branch.clone();
    let destination = resolved.commit.clone();

    // The recorded name has been proven gone, so it is corrected here rather
    // than at whatever surface next trips over it — and before the
    // already-carried early return, because a branch that needs no replay still
    // needs a base it can be diffed and merged against. The preamble goes out
    // with EVERY answer for the same reason: an agent told only "nothing to
    // replay" learns nothing about the base that just changed underneath it.
    let mut preamble = String::new();
    if let Some(superseded) = resolved.superseded.as_deref() {
        crate::orchestrator::replay_base::repoint_recorded_base(db, job_id, &base_branch).await;
        preamble = format!(
            "The base `{superseded}` this branch recorded no longer exists in the store — the \
             usual cause is that its parent merged and the branch was deleted with it. So it is \
             measured against `{base_branch}`, {}, and the recorded base has been re-pointed \
             there so the surfaces that read it agree.\n\n",
            resolved.source.describe()
        );
    }

    // Only for a sessionless request. A session exists precisely because a
    // rebase was attempted and rolled back, so its branch is behind by
    // construction; a bare request is the one that can arrive with nothing to
    // do, and saying so beats queueing a no-op the requester then waits on.
    if session.is_none() && crate::jj::branch_carries_commit(&jj, &store, branch, &destination) {
        return Ok(format!(
            "{preamble}`{branch}` already carries `{base_branch}` at `{destination}` in its \
             ancestry, so there is nothing to replay. If its pull request still reports a \
             conflict, that is a different problem from a stale base — file it rather than \
             replaying again."
        ));
    }

    let incoming = match session.as_ref() {
        Some(session) if !retargeted_held_session => session.incoming.clone(),
        // A bare request vouches for the base it is landing on and nothing else:
        // no PR and no issue carried the advance, because as far as this branch
        // is concerned no advance was ever announced.
        _ => IncomingIdentity {
            base_branch: base_branch.clone(),
            ..IncomingIdentity::default()
        },
    };
    let notes = BaseAdvanceNotes {
        conflict: build_jj_conflict_note(&base_branch, incoming.pr_number, None),
        clean: build_jj_clean_note(&base_branch, incoming.pr_number, None),
        incoming,
    };
    let label = if take_committed_tip {
        format!("resolved replay requested for {branch}")
    } else {
        format!("replay requested for {branch}")
    };

    // Candidate resolution happens before the destination intent is claimed, so
    // an already-running intent has a frozen work list and cannot absorb this
    // request. Wait for that pass, reopen the completed coordinate, and retry as
    // a follow-on pass. Returning success is reserved for a pass that actually
    // accepted this branch; timeout and database failures remain observable to
    // the requester. One deadline spans every attempt, including attempts made
    // after the base moves to a new destination.
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs((RECONCILE_LEASE_SECONDS + 30) as u64);
    // Project the accepted ask onto an existing session before queueing its
    // authoritative token. Queueing also creates an item for a sessionless ask.
    if let Some(session) = session.as_ref() {
        crate::orchestrator::conflict_session::record_replay_request(
            db,
            &session.intent_id,
            branch,
            request_tip.as_deref(),
            &destination,
        )
        .await;
    }
    let replay_request = crate::orchestrator::conflict_session::queue_replay_request(
        db,
        &project.id,
        &store,
        &base_branch,
        &destination,
        branch,
        request_tip.as_deref(),
        take_committed_tip,
        drop_incoming_reason,
    )
    .await?;
    #[cfg(test)]
    replay_request_test_hook(&store, "queued").await;
    let mut reopen_destination = destination.clone();
    // Every destination this request was re-aimed at, so the summary can name a
    // race instead of leaving it to look like a failed resolution.
    let mut retargets: Vec<(String, String)> = Vec::new();
    // The first already-running intent this request folded into, and the
    // destination it was running for. Reported, because a request absorbed by
    // work already under way is not the fresh action "queued" describes.
    let mut coalesced: Option<(Option<String>, String)> = None;
    let replay_outcome = loop {
        if let Some((outcome, served_destination)) =
            completed_replay_request(db, &replay_request).await?
        {
            if served_destination != reopen_destination {
                retargets.push((reopen_destination, served_destination.clone()));
                reopen_destination = served_destination;
            }
            break outcome;
        }
        #[cfg(test)]
        replay_request_test_hook(&store, "pending_observed").await;
        let attempted_destination = reopen_destination.clone();
        if !crate::orchestrator::conflict_session::prepare_pending_replay_request(
            db,
            &replay_request,
            &reopen_destination,
        )
        .await?
        {
            continue;
        }
        let outcome = reconcile_base_advance(
            orch,
            db,
            &project.id,
            &label,
            &project.repo_path,
            &base_branch,
            &base_branch,
            siblings.clone(),
            notes.clone(),
        )
        .await?;
        if let Some((served, served_destination)) =
            completed_replay_request(db, &replay_request).await?
        {
            if served_destination != reopen_destination {
                retargets.push((reopen_destination, served_destination.clone()));
                reopen_destination = served_destination;
            }
            break served;
        }
        if outcome.eligible > 0 {
            break outcome;
        }
        if coalesced.is_none() {
            if let Some(destination) = outcome.coalesced_destination.clone() {
                coalesced = Some((outcome.coalesced_intent.clone(), destination));
            }
        }
        reopen_destination = outcome.coalesced_destination.ok_or_else(|| {
            "Replay reconciliation returned no accepted branch and no coalesced destination."
                .to_string()
        })?;
        if reopen_destination != attempted_destination {
            // Another merge landed on the base while this request sat in the
            // queue, so it was re-aimed. Named explicitly because the resulting
            // page churn reads exactly like a failed resolution otherwise, and
            // an agent who cannot tell them apart re-does content work that was
            // already correct.
            retargets.push((attempted_destination, reopen_destination.clone()));
        }
        wait_for_reconcile_slot(
            db,
            &project.id,
            &store,
            &base_branch,
            &reopen_destination,
            deadline,
        )
        .await?;
    };

    // Re-aimed at a newer base while queued: the page names where it went.
    if let Some(session) = session
        .as_ref()
        .filter(|_| reopen_destination != destination)
    {
        crate::orchestrator::conflict_session::record_replay_request(
            db,
            &session.intent_id,
            branch,
            request_tip.as_deref(),
            &reopen_destination,
        )
        .await;
    }

    let mut summary = preamble;
    if let (Some(first), Some(last)) = (retargets.first(), retargets.last()) {
        let (from, to) = (&first.0, &last.1);
        summary.push_str(&format!(
            "The replay target advanced while your request was queued, from `{from}` to `{to}`, so \
             it was re-aimed at the current base. This is a race with another merge landing, not a \
             failed resolution — your content resolution still stands.\n\n"
        ));
    }
    if let Some(diagnostic) = replay_outcome.replay_diagnostic {
        summary.push_str(&format!(
            "Explicit replay of `{branch}` failed.\n\n{diagnostic}"
        ));
    } else if replay_outcome.conflicted > 0 {
        summary.push_str(&format!(
            "Explicit replay of `{branch}` onto `{base_branch}` still conflicts. \
             The rebase was rolled back; cairn:~/rebase records the fresh conflict coordinates."
        ));
    } else if replay_outcome.rebased_clean > 0 {
        summary.push_str(&format!(
            "Explicit replay of `{branch}` onto `{base_branch}` completed. \
             The replay and verified export landed, and the rebase session is closed."
        ));
    } else {
        // A pass held at an unsafe mutation boundary remains queued work.
        let hold = inflight_run_batch_on_branch(db, job_id, branch).await;
        summary.push_str(&replay_queued_summary(
            branch,
            &base_branch,
            &reopen_destination,
            coalesced.as_ref(),
            job_id,
            &hold,
        ));
    }
    for caveat in &caveats {
        summary.push_str("\n\n⚠️ ");
        summary.push_str(caveat);
    }
    Ok(summary)
}

#[cfg(test)]
type ReplayRequestHook =
    Box<dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send>;
#[cfg(test)]
static REPLAY_REQUEST_HOOKS: std::sync::OnceLock<
    std::sync::Mutex<HashMap<(std::path::PathBuf, &'static str), ReplayRequestHook>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
async fn replay_request_test_hook(store: &Path, stage: &'static str) {
    let hook = REPLAY_REQUEST_HOOKS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&(store.to_path_buf(), stage));
    if let Some(hook) = hook {
        hook().await;
    }
}

async fn completed_replay_request(
    db: &LocalDb,
    request: &crate::orchestrator::conflict_session::PendingReplayRequest,
) -> Result<Option<(BranchAdvanceOutcome, String)>, String> {
    use crate::orchestrator::conflict_session::ReplayRequestState;
    match crate::orchestrator::conflict_session::replay_request_state(db, request).await? {
        ReplayRequestState::Pending => Ok(None),
        ReplayRequestState::Superseded => Err(format!(
            "Replay request `{}` was superseded by a newer request. No further replay was attempted for this request.",
            request.id
        )),
        ReplayRequestState::Served { pass, destination } => {
            let mut outcome = BranchAdvanceOutcome { eligible: 1, ..Default::default() };
            match pass.outcome_kind.as_deref() {
                Some("rebased_clean" | "silent") => outcome.rebased_clean = 1,
                Some("conflicted") => outcome.conflicted = 1,
                _ => {
                    outcome.failed = 1;
                    outcome.replay_diagnostic = Some(pass.diagnostic.unwrap_or_else(|| format!(
                        "The completed replay recorded {} ({}); inspect cairn:~/rebase.",
                        pass.outcome_kind.as_deref().unwrap_or("no outcome"),
                        pass.failure_kind.as_deref().unwrap_or("no failure kind"),
                    )));
                }
            }
            Ok(Some((outcome, destination)))
        }
    }
}

/// What an accepted replay request did and what it is about to meet.
///
/// Pure, so each wording is provable without a store. A request that folded
/// into already-running work says so and names it; a request whose branch has a
/// run batch in flight says the pass will be deferred, by what, and what ends
/// it — otherwise "queued" reads as imminent while the branch cannot move.
fn replay_queued_summary(
    branch: &str,
    base_branch: &str,
    destination: &str,
    coalesced: Option<&(Option<String>, String)>,
    requester_job_id: &str,
    hold: &Result<Option<InflightRunBatch>, String>,
) -> String {
    let mut out = String::new();
    if let Some((intent, running_for)) = coalesced {
        let intent = intent
            .as_deref()
            .map(|id| format!("reconcile intent `{id}`"))
            .unwrap_or_else(|| "a reconcile intent (its id could not be read)".to_string());
        out.push_str(&format!(
            "Coalesced into {intent}, already running for `{base_branch}` at `{running_for}`. \
             This request waited for that pass and was then queued as a follow-on pass of \
             `{branch}` onto `{destination}`; it is not a separate replay.\n\n"
        ));
    } else {
        out.push_str(&format!(
            "Queued a store-side replay of `{branch}` onto `{base_branch}` at `{destination}`. "
        ));
    }
    out.push_str(
        "The durable reconcile worker performs it under the store lock; nothing runs in your \
         slot. A clean replay publishes your branch, refreshes your checkout, and closes this \
         session. A replay that still conflicts leaves your branch untouched and refreshes \
         cairn:~/rebase with fresh coordinates.",
    );
    match hold {
        Ok(None) => {}
        Ok(Some(batch)) if batch.job_id == requester_job_id => out.push_str(&format!(
            "\n\n⚠️ **The branch is deferred, not replayed, while its run batch is in flight.** \
             A pass that reaches it before the batch ends stops at the mutation boundary without \
             attempting the rebase. {} Requesting a replay before then only queues another pass \
             that defers the same way.",
            mutation_boundary_hold(hold)
        )),
        // An explicit replay guards only the requesting job's own batches
        // (cairn/5098), so another node's batch on the shared branch does not
        // hold this pass back, though it does hold every automatic one.
        Ok(Some(batch)) => out.push_str(&format!(
            "\n\n⚠️ **Another node's {} is in flight on this branch.** Automatic base advances \
             and the durable sweep defer the branch while it is, but this explicit replay pass \
             guards only the requesting node's own batches (cairn/5098), so it may rewrite the \
             branch under that batch. If that batch may commit, stop it or let it finish before \
             relying on this replay.",
            batch.describe()
        )),
        Err(_) => out.push_str(&format!(
            "\n\nWhether this pass will be deferred at the mutation boundary is unknown. {}",
            mutation_boundary_hold(hold)
        )),
    }
    out
}

struct ReplayProject {
    id: String,
    repo_path: String,
    base_commit: Option<String>,
}

async fn load_replay_project(db: &LocalDb, job_id: &str) -> Result<ReplayProject, String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.id, p.repo_path, j.base_commit
                     FROM jobs j JOIN projects p ON j.project_id = p.id
                     WHERE j.id = ?1 LIMIT 1",
                    params![job_id],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(ReplayProject {
                id: row.text(0)?,
                repo_path: row.text(1)?,
                base_commit: row.opt_text(2)?,
            }))
        })
    })
    .await
    .map_err(|error| format!("load replay project: {error}"))?
    .ok_or_else(|| "This node has no project repository to replay against.".to_string())
}

/// Re-open an intent an earlier pass already finished, so an explicitly
/// requested replay is not swallowed as a duplicate.
///
/// Coalescing exists to stop AUTOMATIC work piling up at one destination. An
/// agent asking for a replay is not that: it is a new fact about the branch —
/// usually that a content conflict has just been resolved — and landing on the
/// same destination is exactly the point.
async fn reopen_reconcile_intent(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    target_branch: &str,
    destination: &str,
    bookmark: &str,
) -> Result<(), String> {
    let store = store.to_string_lossy().into_owned();
    db.execute(
        "UPDATE jj_reconcile_intents
         SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
         WHERE store_path = ?1 AND target_branch = ?2 AND destination_commit = ?3 AND project_id = ?4
           AND status = 'completed'",
        params![store.as_str(), target_branch, destination, project_id],
    )
    .await
    .map_err(|error| format!("reopen reconcile intent: {error}"))?;
    db.execute(
        "UPDATE jj_reconcile_items SET status = 'pending', notification_sent = 0
         WHERE bookmark = ?1 AND intent_id IN (
             SELECT id FROM jj_reconcile_intents
             WHERE store_path = ?2 AND target_branch = ?3 AND destination_commit = ?4 AND project_id = ?5
         )",
        params![bookmark, store.as_str(), target_branch, destination, project_id],
    )
    .await
    .map_err(|error| format!("reopen reconcile item: {error}"))?;
    Ok(())
}

/// Whether a replayed branch must reach origin now, asked of the one rule every
/// managed-branch advance consults, once per live job on the branch: any job's
/// open pull request requires it. A branch with no job here is still judged by
/// the pull requests that name it.
async fn branch_publication_requirement(
    db: &LocalDb,
    project_id: &str,
    siblings: &[SiblingJob],
    branch: &str,
) -> PublicationRequirement {
    let owners = siblings_for_branch(siblings, branch);
    let job_ids: Vec<&str> = if owners.is_empty() {
        vec![""]
    } else {
        owners.iter().map(|owner| owner.id.as_str()).collect()
    };
    for job_id in job_ids {
        if publication_requirement_for_managed_branch(db, job_id, project_id, branch).await
            == PublicationRequirement::RequiredForOpenPr
        {
            return PublicationRequirement::RequiredForOpenPr;
        }
    }
    PublicationRequirement::DeferredUntilPublication
}

fn siblings_for_branch(siblings: &[SiblingJob], branch: &str) -> Vec<SiblingJob> {
    siblings
        .iter()
        .filter(|sibling| sibling.branch.as_deref() == Some(branch))
        .cloned()
        .collect()
}

/// Whether a durable recovery fence refuses mutating `branch`: the branch
/// itself, or any bookmark riding its own lineage that the rebase would move.
/// Checked under the store lock, so a fence the previous holder recorded is
/// seen. Fails closed on an unreadable fence table.
async fn reconcile_fence_refusal(
    db: &LocalDb,
    jj: &crate::jj::JjEnv,
    store: &Path,
    branch: &str,
    dest: &str,
    action: &str,
) -> Option<String> {
    use crate::orchestrator::branch_recovery;
    let fences = match branch_recovery::fences(db, store).await {
        Ok(fences) => fences,
        Err(error) => {
            return Some(format!(
                "{action} refused: the branch recovery fences could not be read ({error})"
            ))
        }
    };
    let mut bookmarks = vec![branch.to_string()];
    if !fences.is_empty() {
        bookmarks.extend(crate::jj::local_bookmarks_in_range(
            jj,
            store,
            &format!("({dest})..bookmarks(exact:{branch:?})"),
            branch,
        ));
    }
    let bookmarks: Vec<&str> = bookmarks.iter().map(String::as_str).collect();
    branch_recovery::refuse_if_fenced(db, store, &bookmarks, action)
        .await
        .err()
}

/// Persist what this transaction's rollbacks could not verify, while the
/// caller still holds the store guard, and make the item's report say so.
/// Returns whether anything was recorded (or failed to record).
#[allow(clippy::too_many_arguments)]
async fn persist_transaction_recoveries(
    db: &LocalDb,
    project_id: &str,
    store: &Path,
    branch: &str,
    label: &str,
    operation: &str,
    jj: &crate::jj::JjEnv,
    report: &mut crate::jj::ReconcileReport,
) -> bool {
    use crate::orchestrator::branch_recovery;
    let recoveries = jj.take_recoveries();
    if recoveries.is_empty() {
        return false;
    }
    let class = if replay_is_explicit(label) {
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
    // A branch whose transaction left a fence is neither clean nor a conflict.
    report.rebased_clean.retain(|name| name != branch);
    report.conflicted.retain(|name| name != branch);
    report.conflict_diagnostics.remove(branch);
    match report
        .failed
        .iter_mut()
        .find(|failure| failure.branch == branch)
    {
        Some(failure) => {
            // A nested rollback may have reported itself complete while another
            // in the same transaction was not verified; with a fence standing,
            // the branch is not known to be unchanged.
            failure.error = failure.error.replace(
                ", so the branch is unchanged.",
                "; another rollback in the same transaction could not be verified, so the \
                 branch is fenced, not known to be unchanged.",
            );
            if !failure.error.contains(crate::jj::ROLLBACK_INCOMPLETE) {
                failure.error = format!("{} {}", crate::jj::ROLLBACK_INCOMPLETE, failure.error);
            }
            failure.error = format!("{}\n{note}", failure.error);
        }
        None => report.failed.push(crate::jj::ReconcileFailure {
            branch: branch.to_string(),
            error: format!("{} {note}", crate::jj::ROLLBACK_INCOMPLETE),
        }),
    }
    true
}

fn failed_reconcile_report(branch: &str, error: String) -> crate::jj::ReconcileReport {
    let mut report = crate::jj::ReconcileReport::default();
    report.failed.push(crate::jj::ReconcileFailure {
        branch: branch.to_string(),
        error,
    });
    report
}

struct ExplicitReplayTrace {
    branch: String,
    environment: crate::jj::JjEnv,
    request: Option<crate::orchestrator::conflict_session::PendingReplayRequest>,
}

async fn defer_run_batch_reconcile(
    db: &LocalDb,
    intent_id: &str,
    branch: &str,
    observed_tip: Option<&str>,
    batch: InflightRunBatch,
    label: &str,
) -> Result<(), String> {
    let held_by = batch.resolve_holder(db).await.describe();
    log::info!(
        "jj base advance ({label}): deferred {branch} at the mutation boundary for an in-flight {held_by}"
    );
    let diagnostic = format!(
        "deferred at the mutation boundary: {held_by} was in flight on this branch, \
         so its rebase was not attempted. It stays queued for the durable sweep, \
         which performs it once that batch finishes or is stopped."
    );
    persist_reconcile_item(
        db,
        ReconcileItemUpdate {
            intent_id,
            bookmark: branch,
            observed_tip,
            status: "pending",
            failure_kind: None,
            outcome_kind: Some("deferred"),
            fingerprint: None,
            diagnostic: Some(&diagnostic),
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_reconcile_claim(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    label: &str,
    repo_path: &str,
    rebase_dest: &str,
    siblings: Vec<SiblingJob>,
    notes: BaseAdvanceNotes,
    specs: Vec<String>,
    existing_bookmarks: Option<std::collections::HashSet<String>>,
    pinned_dest: String,
    claim: ReconcileClaim,
) -> Result<BranchAdvanceOutcome, String> {
    let intent_id = claim.id.clone();
    let owner = claim.owner.clone();
    let candidate_count = specs.len();
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    let mut explicit_trace = None;
    let started = std::time::Instant::now();
    let result = execute_reconcile_claim_body(
        orch,
        db,
        project_id,
        label,
        repo_path,
        rebase_dest,
        siblings,
        notes,
        specs,
        existing_bookmarks,
        pinned_dest,
        claim,
        &mut explicit_trace,
    )
    .await;
    // A timeout before a report exists is still a completed explicit attempt.
    // These exits are at read/admission boundaries: no mutation is in flight,
    // and an earlier conflicting rebase has already restored its transaction.
    if let (Some(attempt), Err(error)) = (explicit_trace, &result) {
        let branch = attempt.branch;
        if crate::jj::is_jj_timeout_error(error)
            || error.contains("reconcile timed out")
            || error.contains("replay timed out")
        {
            let trace = attempt.environment.operation_phase_summary();
            let diagnostic = format!(
                "Explicit replay preflight timed out: operation budget exhausted. {error}\n\
                 Explicit replay timed out under its {:.3}s cumulative forward budget \
                 ({:.3}s elapsed including admission). No automatic cooldown was recorded. \
                 No replay mutation was left in flight at this boundary; a preceding conflicting \
                 rebase, if any, was already rolled back. A new explicit request may run immediately.\n{trace}",
                explicit_replay_budget_for_store(&store).as_secs_f64(),
                started.elapsed().as_secs_f64(),
            );
            persist_reconcile_item(
                db,
                ReconcileItemUpdate {
                    intent_id: &intent_id,
                    bookmark: &branch,
                    observed_tip: None,
                    status: "completed",
                    failure_kind: Some(RECONCILE_TIMEOUT),
                    outcome_kind: Some("failed"),
                    fingerprint: None,
                    diagnostic: Some(&diagnostic),
                },
            )
            .await?;
            if let Some(request) = attempt.request.as_ref() {
                crate::orchestrator::conflict_session::mark_replay_request_served(
                    db, request, &intent_id,
                )
                .await?;
            }
            // Other siblings may still be owed work. The completed failed
            // item is skipped on that retry, rather than becoming automatic.
            finish_reconcile_intent(db, &intent_id, &owner, candidate_count > 1).await?;
            return Ok(BranchAdvanceOutcome {
                eligible: 1,
                failed: 1,
                replay_diagnostic: Some(diagnostic),
                ..BranchAdvanceOutcome::default()
            });
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn execute_reconcile_claim_body(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    label: &str,
    repo_path: &str,
    rebase_dest: &str,
    siblings: Vec<SiblingJob>,
    notes: BaseAdvanceNotes,
    specs: Vec<String>,
    existing_bookmarks: Option<std::collections::HashSet<String>>,
    pinned_dest: String,
    claim: ReconcileClaim,
    explicit_trace: &mut Option<ExplicitReplayTrace>,
) -> Result<BranchAdvanceOutcome, String> {
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    let intent_id = claim.id.as_str();

    // One pass visits each branch once. Two jobs on one branch (a node's
    // successive executions) would otherwise replay it twice, and the second
    // visit meets the quarantine the first just wrote and rewrites the item as
    // parked, erasing the conflict it recorded.
    let specs = unique_branches(specs);
    // List bookmarks once, then transact only live siblings.
    let (specs, skipped_missing) = retain_present_siblings(specs, existing_bookmarks.as_ref());
    if skipped_missing > 0 {
        log::info!(
                "jj base advance ({label}): skipped {skipped_missing} sibling(s) with missing bookmarks before reconcile"
            );
    }
    if specs.is_empty() {
        finish_reconcile_intent(db, intent_id, &claim.owner, false).await?;
        return Ok(BranchAdvanceOutcome::default());
    }

    let mut ambiguous: Vec<AmbiguousDivergence> = Vec::new();
    let mut pending_quarantines: Vec<PendingReconcileQuarantine> = Vec::new();
    let mut before: HashMap<String, String> = HashMap::new();
    let mut after: HashMap<String, String> = HashMap::new();
    let mut carry_notes: HashMap<String, String> = HashMap::new();
    let mut report = crate::jj::ReconcileReport::default();
    let mut any_branch_deferred = false;
    let mut explicit_branches = HashSet::new();
    // Set once a branch's rewrite gave up on `packed-refs.lock`. Every rewrite
    // waits out the lock while holding the store lock, so a long contention
    // spell must not cost that wait once per branch; the branches after it are
    // left for the sweep, which retries the set shortly.
    let mut ref_lock_deferral: Option<String> = None;
    // Set when a transaction recorded a recovery fence, so its notices go out
    // once the store guard is released.
    let mut recovery_recorded = false;

    // The branches of this store that catch up by one merge commit and are
    // never replayed. Loaded once per pass; every other branch is untouched.
    let merge_mode =
        crate::orchestrator::catch_up::merge_mode_bookmarks(db, &claim.project_id, &store).await?;

    for branch in &specs {
        *explicit_trace = None;
        heartbeat_reconcile_intent(db, &claim).await?;
        let pending_request =
            crate::orchestrator::conflict_session::load_pending_replay_request(db, &store, branch)
                .await?;
        let progress = reconcile_item_status(db, intent_id, branch).await?;
        // Recording an implicit hold completed its bookkeeping, not the owed
        // replay. The same destination can later be retried automatically once
        // the immediate parent has recovered.
        let owes_held_replay = progress
            .as_ref()
            .is_some_and(|progress| progress.outcome_kind.as_deref() == Some("held_parent"))
            && held_replay_source(db, &store, branch).await?.is_some();
        if pending_request.is_none()
            && !owes_held_replay
            && progress.as_ref().is_some_and(|progress| {
                progress.status == "completed"
                    || (progress.status == "graph_moved" && progress.notification_sent)
            })
        {
            log::debug!("jj reconcile ({label}): resumed past completed {branch}");
            continue;
        }
        let requested_label = pending_request.as_ref().map(|request| {
            if request.take_committed_tip {
                format!("resolved replay requested for {branch}")
            } else {
                format!("replay requested for {branch}")
            }
        });
        let label = requested_label.as_deref().unwrap_or(label);
        if pending_request.is_none() && replay_is_explicit_for(label, branch) {
            // A different worker already served or superseded this caller's ask.
            // Its label cannot grant another mutation or renew its allowance.
            continue;
        }
        let explicit = pending_request.is_some();
        if explicit {
            explicit_branches.insert(branch.clone());
        }
        // An intent may contain several siblings, but only the branch named by
        // this request receives its authority and foreground allowance.
        let automatic_label = "automatic reconcile";
        let item_label = if explicit { label } else { automatic_label };
        let budget = if explicit {
            explicit_replay_budget_for_store(&store)
        } else {
            automatic_replay_budget_for(&store, branch)
        };
        let mut replay_phases = Vec::new();
        // This read needs no store lock. In particular, do not inspect a held
        // bookmark or prepare its resolution basis just to decide not to replay
        // it. Admission is rechecked under each mutation guard below.
        let branch_siblings = siblings_for_branch(&siblings, branch);
        if pending_request.is_some() || progress.as_ref().is_none_or(|p| !resumes_past_mutation(p))
        {
            if let Some(batch) = inflight_run_batch(db, &branch_siblings).await? {
                defer_run_batch_reconcile(
                    db,
                    intent_id,
                    branch,
                    progress.as_ref().and_then(|p| p.observed_tip.as_deref()),
                    batch,
                    label,
                )
                .await?;
                continue;
            }
        }
        let (current_tip, branch_jj, explicit_deadline) = {
            let guard = orch
                .acquire_jj_store_lock(
                    &store,
                    crate::orchestrator::StoreLockClass::Background,
                    replay_lock_operation(label, branch, "sibling reconcile inspect", budget),
                )
                .await;
            let scoped = jj.with_operation_budget(budget);
            if explicit {
                *explicit_trace = Some(ExplicitReplayTrace {
                    branch: branch.clone(),
                    environment: scoped.clone(),
                    request: pending_request.clone(),
                });
            }
            let deadline = tokio::time::Instant::now() + budget;
            let _phase = guard.phase(format!("bookmark suppression probe branch={branch}"));
            let tip = crate::jj::bookmark_commit_checked(&scoped, &store, branch);
            // A read that timed out and left its process unproven gone still
            // fences the store before this guard is released.
            let mut unused = crate::jj::ReconcileReport::default();
            if persist_transaction_recoveries(
                db,
                &claim.project_id,
                &store,
                branch,
                label,
                &format!("sibling reconcile inspect ({label})"),
                &scoped,
                &mut unused,
            )
            .await
            {
                recovery_recorded = true;
            }
            (tip, scoped, deadline)
        };
        let jj = &branch_jj;
        let current_tip = match current_tip {
            Ok(tip) => tip,
            Err(error) => {
                if explicit && crate::jj::is_jj_timeout_error(&error) {
                    return Err(error);
                }
                log::warn!("jj reconcile ({label}): deferring {branch}; tip unreadable, keeping any timeout cooldown: {error}");
                any_branch_deferred = true;
                continue;
            }
        };
        let mut quarantine =
            load_reconcile_quarantine(db, &claim.project_id, &store, branch).await?;
        // A merge catch-up parked against an older destination is attempted
        // again: one merge, so a newer base is worth one more try.
        if quarantine
            .as_ref()
            .and_then(ReconcileQuarantine::observed_dest)
            .is_some_and(|dest| dest != pinned_dest)
        {
            release_reconcile_quarantine(db, &claim.project_id, &store, branch).await?;
            quarantine = None;
        }
        let parked_unannounced = match quarantine.as_ref() {
            Some(existing)
                if (existing.failure_kind == ROLLED_BACK_CONFLICT
                    || existing.failure_kind
                        == crate::orchestrator::catch_up::MERGE_CONFLICT_QUARANTINE)
                    && quarantine_blocks_automatic_replay(
                        existing,
                        current_tip.as_deref(),
                        label,
                    ) =>
            {
                !conflict_wake_queued(db, branch).await?
            }
            _ => false,
        };
        if parked_unannounced {
            log::info!(
                "jj reconcile ({label}): {branch} is parked on a conflict whose blocking note was \
                 never queued; replaying it so the session is re-recorded and its agent woken"
            );
            release_reconcile_quarantine(db, &claim.project_id, &store, branch).await?;
            quarantine = None;
        }
        if let Some(existing) = quarantine.as_ref() {
            if quarantine_blocks_automatic_replay(existing, current_tip.as_deref(), item_label) {
                if existing.failure_kind == RECONCILE_TIMEOUT {
                    defer_timeout_reconcile(
                        db,
                        intent_id,
                        branch,
                        current_tip.as_deref(),
                        existing,
                        label,
                    )
                    .await?;
                    any_branch_deferred = true;
                    continue;
                }
                let diagnostic = format!(
                    "quarantined: {}",
                    existing
                        .last_diagnostic
                        .as_deref()
                        .unwrap_or("permanent reconcile failure")
                );
                persist_reconcile_item(
                    db,
                    ReconcileItemUpdate {
                        intent_id,
                        bookmark: branch,
                        observed_tip: current_tip.as_deref(),
                        status: "suppressed",
                        failure_kind: Some(&existing.failure_kind),
                        outcome_kind: Some("quarantined"),
                        fingerprint: Some(&existing.fingerprint),
                        diagnostic: Some(&diagnostic),
                    },
                )
                .await?;
                log::debug!(
                    "jj reconcile ({label}): skipped quarantined unchanged bookmark {branch}"
                );
                if explicit {
                    report.failed.push(crate::jj::ReconcileFailure {
                        branch: branch.clone(),
                        error: diagnostic,
                    });
                    if let Some(request) = pending_request.as_ref() {
                        crate::orchestrator::conflict_session::mark_replay_request_served(
                            db, request, intent_id,
                        )
                        .await?;
                    }
                }
                continue;
            }
            if existing.failure_kind != "ambiguous_divergence"
                && existing.observed_tip() != current_tip.as_deref().unwrap_or("missing")
            {
                release_reconcile_quarantine(db, &claim.project_id, &store, branch).await?;
                quarantine = None;
            }
        }
        if let Some(progress) = progress.as_ref() {
            if progress.status == "suppressed" && progress.notification_sent {
                let prefix = format!(
                    "{pinned_dest}:{branch}:{}:",
                    current_tip.as_deref().unwrap_or("missing")
                );
                if progress
                    .fingerprint
                    .as_deref()
                    .is_some_and(|value| value.starts_with(&prefix))
                {
                    log::debug!("jj reconcile ({label}): suppressed unchanged {branch}");
                    continue;
                }
            }
        }
        // After the quarantine and suppression skips, so a parked branch keeps
        // its parked state rather than being rewritten as owed work.
        if let Some(deferred) = ref_lock_deferral.as_deref() {
            persist_reconcile_item(
                db,
                ReconcileItemUpdate {
                    intent_id,
                    bookmark: branch,
                    observed_tip: current_tip.as_deref(),
                    status: "pending",
                    failure_kind: Some(crate::jj::reconcile_failure_kind(deferred)),
                    outcome_kind: Some("deferred"),
                    fingerprint: None,
                    diagnostic: Some(deferred),
                },
            )
            .await?;
            log::debug!("jj reconcile ({label}): deferred {branch} behind the ref-store lock");
            any_branch_deferred = true;
            continue;
        }

        // The open session, if any, is what knows which base the branch's
        // committed tip was resolved against. Automatic and explicit replays
        // both measure from it; only an explicit one may carry content.
        let open_session =
            crate::orchestrator::conflict_session::load_active_session(db, branch).await?;
        if let (Some(request), Some(session)) = (pending_request.as_ref(), open_session.as_ref()) {
            crate::orchestrator::conflict_session::project_pending_replay_request(
                db,
                request,
                &session.intent_id,
                &pinned_dest,
                current_tip.as_deref(),
            )
            .await?;
        }
        // What a structural merge measures each path from if this replay
        // conflicts: the live fork, or the side the open session's resolution
        // was made against.
        let mut replay_baseline = current_tip.as_deref().and_then(|pre_tip| {
            let fork = branch_fork_point(jj, &store, pre_tip, &pinned_dest)?;
            Some(
                ReplayBaseline {
                    fork,
                    basis: open_session
                        .as_ref()
                        .map(|session| {
                            session
                                .resolution_basis_at(jj, &store, pre_tip)
                                .unwrap_or_else(|_| session.resolution_basis.clone())
                        })
                        .unwrap_or_default(),
                }
                .verified(jj, &store, pre_tip),
            )
        });

        // A requested replay is the explicit resolution operation: take the
        // branch's committed tip bytes for this session's conflicting paths.
        // Automatic base-advance reconciliation never receives this authority
        // and therefore retains the fail-closed rollback. A previous failed retry
        // may already name the resolved tip as `ours`, so tip inequality cannot
        // identify resolution intent reliably (the live CAIRN-3412 shape).
        let resolved_request = explicit && label.starts_with("resolved replay requested for ");
        let resolution_session = if resolved_request {
            open_session.clone()
        } else {
            None
        };

        // A conflict rollback stops this environment. Retain the immutable
        // resolution evidence before mutation rather than re-reading afterward.
        let mut resolution_basis_for_conflict = None;
        let mut ambiguous_item = None;
        let mut divergence_resolved = false;
        let mut held_cut = None;
        let mut expected_held_source = None;
        let mut item_report = if let Some(progress) = progress
            .as_ref()
            .filter(|p| resumes_past_mutation(p) && pending_request.is_none())
        {
            let mut resumed = crate::jj::ReconcileReport::default();
            match progress.outcome_kind.as_deref() {
                Some("conflicted") => resumed.conflicted.push(branch.clone()),
                Some("rebased_clean") => resumed.rebased_clean.push(branch.clone()),
                Some("silent") => resumed.silent.push(branch.clone()),
                Some("failed") => resumed.failed.push(crate::jj::ReconcileFailure {
                    branch: branch.clone(),
                    error: progress
                        .failure_kind
                        .clone()
                        .unwrap_or_else(|| "resumed reconcile failure".into()),
                }),
                _ => {}
            }
            if let Some(tip) = progress.observed_tip.clone() {
                after.insert(branch.clone(), tip);
            }
            resumed
        } else {
            let guard = orch
                .acquire_jj_store_lock(
                    &store,
                    crate::orchestrator::StoreLockClass::Background,
                    replay_lock_operation(label, branch, "sibling reconcile", budget),
                )
                .await;
            let lock_deadline = if explicit {
                explicit_deadline
            } else {
                tokio::time::Instant::now() + budget
            };
            let jj = if explicit {
                jj.clone()
            } else {
                jj.with_operation_budget(budget)
            };
            // RunBatch suspension admission takes this same store lock while
            // inserting its durable row. Revalidating under the guard makes the
            // row check and bookmark mutation one exclusive bracket.
            let branch_siblings = siblings_for_branch(&siblings, branch);
            let admission_check = guard.phase("in-flight run batch check");
            let inflight =
                tokio::time::timeout_at(lock_deadline, inflight_run_batch(db, &branch_siblings))
                    .await
                    .map_err(|_| {
                        "background reconcile timed out checking in-flight run batches".to_string()
                    })??;
            drop(admission_check);
            if let Some(batch) = inflight {
                drop(guard);
                defer_run_batch_reconcile(
                    db,
                    intent_id,
                    branch,
                    current_tip.as_deref(),
                    batch,
                    label,
                )
                .await?;
                continue;
            }

            // A durable recovery fence outranks every cooldown exemption, an
            // explicit replay's included: normal replay is not repair.
            let fence_check = guard.phase("recovery fence check");
            let fenced = reconcile_fence_refusal(
                db,
                &jj,
                &store,
                branch,
                &pinned_dest,
                "Replaying this branch",
            )
            .await;
            drop(fence_check);
            if let Some(refusal) = fenced {
                drop(guard);
                log::warn!("jj base advance ({label}): {branch} not replayed: {refusal}");
                persist_reconcile_item(
                    db,
                    ReconcileItemUpdate {
                        intent_id,
                        bookmark: branch,
                        observed_tip: current_tip.as_deref(),
                        status: "pending",
                        failure_kind: Some(crate::jj::ROLLBACK_FAILED),
                        outcome_kind: Some("deferred"),
                        fingerprint: None,
                        diagnostic: Some(&refusal),
                    },
                )
                .await?;
                any_branch_deferred = true;
                continue;
            }

            // One in-process read answers the destination, the name, the
            // divergence, and the tip. Each repair below runs its CLI probe only
            // when this read could not answer or found something to repair; a
            // failed read falls back to all of them.
            let precheck = {
                let _phase = guard.phase("bookmark precheck");
                // This read-only optimization can be cancelled; unlike a writer,
                // it cannot outlive the guard with an uncommitted mutation.
                match tokio::time::timeout_at(
                    lock_deadline,
                    cairn_vcs::bookmark_precheck(&store, rebase_dest, branch),
                )
                .await
                {
                    Ok(Ok(precheck)) => Some(precheck),
                    result => {
                        log::debug!(
                            "jj base advance ({label}): in-process precheck of {branch} did not answer, \
                             probing with budgeted jj instead: {result:?}"
                        );
                        None
                    }
                }
            };
            // Another destination's worker may have timed out while this
            // worker waited for admission. Recheck the durable breaker and the
            // actual tip at the mutation boundary, not just before queueing.
            let locked_tip = match precheck.as_ref().and_then(|precheck| precheck.tip.clone()) {
                Some(tip) => Some(tip),
                None => crate::jj::bookmark_commit_checked(&jj, &store, branch)?,
            };
            let locked_quarantine = tokio::time::timeout_at(
                lock_deadline,
                load_reconcile_quarantine(db, &claim.project_id, &store, branch),
            )
            .await
            .map_err(|_| {
                "background reconcile timed out rechecking timeout cooldown".to_string()
            })??;
            if let Some(existing) = locked_quarantine.as_ref().filter(|existing| {
                existing.failure_kind == RECONCILE_TIMEOUT
                    && quarantine_blocks_automatic_replay(
                        existing,
                        locked_tip.as_deref(),
                        item_label,
                    )
            }) {
                drop(guard);
                defer_timeout_reconcile(
                    db,
                    intent_id,
                    branch,
                    locked_tip.as_deref(),
                    existing,
                    label,
                )
                .await?;
                any_branch_deferred = true;
                continue;
            }
            if let Some(request) = pending_request.as_ref() {
                let _phase = guard.phase("revalidate explicit replay request");
                let current_request = tokio::time::timeout_at(
                    lock_deadline,
                    crate::orchestrator::conflict_session::load_pending_replay_request(
                        db, &store, branch,
                    ),
                )
                .await
                .map_err(|_| "explicit replay timed out revalidating its request".to_string())??;
                if current_request.as_ref().map(|current| current.id.as_str())
                    != Some(request.id.as_str())
                {
                    drop(_phase);
                    drop(guard);
                    any_branch_deferred = true;
                    continue;
                }
            }
            let settled = precheck.as_ref().filter(|precheck| precheck.is_settled());

            // Revalidate that the named destination still identifies the pinned
            // commit before mutating this bookmark.
            let observed_dest = match precheck
                .as_ref()
                .and_then(|precheck| precheck.destination.clone())
            {
                Some(destination) => Some(destination),
                None => guard.blocking_phase("revalidate destination", || {
                    crate::jj::revset_commit(&jj, &store, rebase_dest)
                }),
            };
            if observed_dest.as_deref() != Some(pinned_dest.as_str()) {
                return Err(format!(
                        "jj base advance ({label}): destination moved while intent was running (pinned {pinned_dest}, observed {observed_dest:?})"
                    ));
            }

            // A conflicted name fails the rebase below outright, and used to be
            // retried as `conflicted_bookmark` forever without anything trying
            // to settle it. Settle it here, under the same guard, so healing
            // does not wait for someone to read the branch.
            let name_repair = if precheck
                .as_ref()
                .is_some_and(|precheck| !precheck.name_conflicted)
            {
                Ok(None)
            } else {
                guard.blocking_phase("name repair", || {
                    crate::jj::repair_conflicted_branch_name(
                        &jj,
                        &store,
                        branch,
                        crate::jj::BranchAuthority::Store,
                    )
                })
            };
            let name_refusal = match name_repair {
                Ok(Some(settled)) => {
                    log::info!(
                        "jj base advance ({label}): settled `{branch}`'s conflicted name on {settled} before replaying it"
                    );
                    None
                }
                Ok(None) => None,
                Err(error) => {
                    log::warn!(
                        "jj base advance ({label}): `{branch}`'s conflicted name was not settled: {error}"
                    );
                    Some(match error.refusal() {
                        Some(_) => format!(
                            "{error}. This branch's rebase page (cairn:~/rebase) lists every \
                             version and the commands that combine them."
                        ),
                        None => error.to_string(),
                    })
                }
            };

            // A repaired name may have moved the tip, so only a read that found
            // nothing to repair can stand in for the collapse probe.
            let collapse = if settled.is_some() {
                Ok(crate::jj::CollapseOutcome::NotDivergent)
            } else {
                guard.blocking_phase("divergence collapse", || {
                    crate::jj::collapse_divergent_bookmark(&jj, &store, branch)
                })
            };
            match collapse {
                Ok(crate::jj::CollapseOutcome::NotDivergent) => {
                    divergence_resolved = true;
                }
                Ok(crate::jj::CollapseOutcome::Collapsed { kept, abandoned }) => {
                    divergence_resolved = true;
                    log::info!(
                        "jj collapse ({label}): sibling {branch} converged to {kept}; abandoned {}",
                        abandoned.join(", ")
                    );
                }
                Ok(crate::jj::CollapseOutcome::Ambiguous { change_id, twins }) => {
                    ambiguous_item = Some(AmbiguousDivergence {
                        branch: branch.clone(),
                        change_id,
                        twins,
                    });
                }
                Err(error) => {
                    log::warn!("jj collapse ({label}): sibling {branch} failed: {error}");
                }
            }

            if ambiguous_item.is_none() {
                let tip = match settled {
                    Some(precheck) => precheck.tip.clone(),
                    None => guard.blocking_phase("read tip before", || {
                        crate::jj::bookmark_commit(&jj, &store, branch)
                    }),
                };
                if let Some(commit) = tip {
                    before.insert(branch.clone(), commit);
                }
                if let Some(tip) = before.get(branch) {
                    resolution_basis_for_conflict =
                        Some(guard.blocking_phase("capture resolution inheritance", || {
                            crate::orchestrator::conflict_session::inherited_basis(
                                &jj,
                                &store,
                                open_session.as_ref(),
                                Some(tip),
                            )
                        }));
                }
            }
            // Carry the committed resolution onto THIS destination, under the
            // same lock as the rebase it feeds, so nothing can move between
            // the plan and its use.
            let already_landed = resolved_request
                && resolution_session.is_none()
                && crate::jj::branch_carries_commit(&jj, &store, branch, &pinned_dest);
            let merge_mode_branch = merge_mode.contains(branch);
            // A merge-mode resolution is the owner's committed tip on top of the
            // tip that conflicted: each session path is taken whole from it into
            // a merge against the destination it was resolved against.
            let merge_resolution = (merge_mode_branch && resolved_request && !already_landed)
                .then(|| match (resolution_session.as_ref(), before.get(branch)) {
                    (Some(session), Some(live_tip)) => {
                        let paths: Vec<String> =
                            session.conflicting().map(|file| file.path.clone()).collect();
                        Ok((
                            crate::jj::ResolutionCarry::from_tip(live_tip, &paths),
                            session.theirs.clone(),
                        ))
                    }
                    (None, _) => Err("the requested take-committed-tip merge no longer has an open conflict session; re-read cairn:~/rebase".to_string()),
                    (_, None) => Err("the current committed tip could not be read; no resolution was carried".to_string()),
                });
            let merge_options =
                |resolution: Option<(crate::jj::ResolutionCarry, Option<String>)>| {
                    let (resolution, resolution_dest) = match resolution {
                        Some((carry, dest)) => (Some((branch.clone(), carry)), dest),
                        None => (None, None),
                    };
                    crate::jj::ReconcileOptions {
                        merge_mode: [branch.clone()].into_iter().collect(),
                        target_name: Some(rebase_dest.to_string()),
                        resolution,
                        resolution_dest,
                        ..crate::jj::ReconcileOptions::default()
                    }
                };
            let carry_plan = if resolved_request && !already_landed && !merge_mode_branch {
                Some(match (resolution_session.as_ref(), before.get(branch)) {
                    (Some(session), Some(live_tip)) => guard.blocking_phase("plan resolution carry", || {
                        plan_current_requested_resolution(
                            &jj, &store, session, &pinned_dest, live_tip,
                            pending_request.as_ref().and_then(|request| request.drop_incoming_reason.as_deref()),
                        ).map(|(plan, basis)| {
                            resolution_basis_for_conflict = Some(basis);
                            plan
                        })
                    }),
                    (None, _) => Err("the requested take-committed-tip replay no longer has an open conflict session; re-read cairn:~/rebase".to_string()),
                    (_, None) => Err("the current committed tip could not be read; no resolution was carried".to_string()),
                })
            } else {
                None
            };
            let preserve_bookmarks = held_replay_bookmarks_for_repository(&orch.db, &store).await?;
            expected_held_source = held_replay_source(db, &store, branch).await?;
            let source =
                held_replay_source_cut(orch, db, &jj, &store, branch, &notes.incoming.base_branch)
                    .await;
            held_cut = source.as_ref().ok().cloned().flatten();
            if let (Some(cut), Some(baseline)) = (held_cut.as_ref(), replay_baseline.as_mut()) {
                baseline.fork = cut.clone();
            }
            let mut item_report = if let Err(error) = source {
                failed_reconcile_report(branch, error)
            } else if ambiguous_item.is_some() {
                crate::jj::ReconcileReport::default()
            } else if let Some(error) = name_refusal {
                let mut refused = crate::jj::ReconcileReport::default();
                refused.failed.push(crate::jj::ReconcileFailure {
                    branch: branch.clone(),
                    error,
                });
                refused
            } else if let Some(merge_resolution) = merge_resolution {
                match merge_resolution {
                    Ok(resolution) => {
                        let item = vec![branch.clone()];
                        let options = merge_options(Some(resolution));
                        guard
                            .blocking_phase("resolved catch-up merge and export", || {
                                crate::jj::reconcile_siblings_with_options(
                                    &jj,
                                    &store,
                                    &pinned_dest,
                                    &item,
                                    &options,
                                )
                            })
                            .unwrap_or_else(|error| failed_reconcile_report(branch, error))
                    }
                    Err(error) => {
                        let mut refused = crate::jj::ReconcileReport::default();
                        refused.failed.push(crate::jj::ReconcileFailure {
                            branch: branch.clone(),
                            error: format!(
                                "your committed resolution could not be merged with \
                                 `{pinned_dest}`, so the branch was left untouched: {error}"
                            ),
                        });
                        refused
                    }
                }
            } else if merge_mode_branch {
                let item = vec![branch.clone()];
                let options = merge_options(None);
                guard
                    .blocking_phase("catch-up merge and export", || {
                        crate::jj::reconcile_siblings_with_options(
                            &jj,
                            &store,
                            &pinned_dest,
                            &item,
                            &options,
                        )
                    })
                    .unwrap_or_else(|error| failed_reconcile_report(branch, error))
            } else if let Some(carry_plan) = carry_plan {
                match carry_plan {
                    Ok(plan) => {
                        if !plan.refused.is_empty() {
                            let note = plan.describe_refusals();
                            log::info!("jj reconcile ({label}): `{branch}` {note}");
                            carry_notes.insert(branch.clone(), note);
                        }
                        guard
                            .blocking_phase("rebase and export", || {
                                crate::jj::reconcile_siblings_with_options(
                                    &jj,
                                    &store,
                                    &pinned_dest,
                                    std::slice::from_ref(branch),
                                    &crate::jj::ReconcileOptions {
                                        resolution: Some((branch.clone(), plan.carry.clone())),
                                        preserve_bookmarks: preserve_bookmarks.clone(),
                                        source_cut: held_cut
                                            .as_ref()
                                            .map(|cut| (branch.clone(), cut.clone())),
                                        ..Default::default()
                                    },
                                )
                            })
                            .unwrap_or_else(|error| failed_reconcile_report(branch, error))
                    }
                    Err(error) => {
                        let mut refused = crate::jj::ReconcileReport::default();
                        refused.failed.push(crate::jj::ReconcileFailure {
                            branch: branch.clone(),
                            error: format!(
                                "your committed resolution could not be carried onto \
                                 `{pinned_dest}`, so the branch was left untouched: {error}"
                            ),
                        });
                        refused
                    }
                }
            } else {
                let item = vec![branch.clone()];
                guard
                    .blocking_phase("rebase and export", || {
                        crate::jj::reconcile_siblings_with_options(
                            &jj,
                            &store,
                            &pinned_dest,
                            &item,
                            &crate::jj::ReconcileOptions {
                                preserve_bookmarks: preserve_bookmarks.clone(),
                                source_cut: held_cut
                                    .as_ref()
                                    .map(|cut| (branch.clone(), cut.clone())),
                                ..Default::default()
                            },
                        )
                    })
                    .unwrap_or_else(|error| failed_reconcile_report(branch, error))
            };
            if ambiguous_item.is_none() {
                if let Some(commit) = guard.blocking_phase("read tip after", || {
                    crate::jj::bookmark_commit(&jj, &store, branch)
                }) {
                    after.insert(branch.clone(), commit);
                }
            }
            if item_report.rebased_clean.contains(branch) {
                clear_held_replay_source(
                    db,
                    &jj,
                    &store,
                    branch,
                    &pinned_dest,
                    branch,
                    expected_held_source.as_ref(),
                )
                .await?;
            }
            // The recovery fence first, and before releasing this guard: no
            // queued writer may act on a store whose rollback is unverified.
            {
                let _phase = guard.phase("recovery fence persistence");
                if persist_transaction_recoveries(
                    db,
                    &claim.project_id,
                    &store,
                    branch,
                    label,
                    &format!("sibling reconcile ({label})"),
                    &jj,
                    &mut item_report,
                )
                .await
                {
                    recovery_recorded = true;
                }
            }
            // Persist before releasing this guard: otherwise a second intent
            // already queued on the store could retry before seeing the breaker.
            let timeout_persistence = guard.phase("timeout cooldown persistence");
            let tip = before
                .get(branch)
                .or(locked_tip.as_ref())
                .map_or("missing", String::as_str);
            if !explicit
                && persist_report_timeouts(
                    db,
                    &claim.project_id,
                    &store,
                    branch,
                    tip,
                    &pinned_dest,
                    &mut item_report,
                )
                .await?
            {
                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table":"comments", "action":"insert"}),
                );
            }
            drop(timeout_persistence);
            let held = item_report
                .conflict_diagnostics
                .iter()
                .filter(|(name, _)| *name != branch)
                .map(|(name, diagnostic)| (name.clone(), diagnostic.clone()))
                .collect();
            persist_held_descendant_sessions(orch, db, project_id, &store, &held).await?;
            if explicit {
                replay_phases.push(guard.phase_summary());
            }
            if after.contains_key(branch) && before.get(branch) != after.get(branch) {
                if let Some(sibling) = siblings
                    .iter()
                    .find(|sibling| sibling.branch.as_deref() == Some(branch.as_str()))
                {
                    orch.invalidate_node_check_status(&sibling.id, "base-or-fork-point-advance");
                    // A replay that kept the branch's own patch leaves its
                    // running wave verifying the change the branch still
                    // claims, so the wave finishes and records against the head
                    // it ran on. A replay that altered the patch (a conflict, a
                    // structural merge, an edit inside a hunk's context) leaves
                    // the wave answering a question nobody can ask any more:
                    // cancelling it releases the slot, and the release arms
                    // one for the new tree (`arm_turn_end_successor`). A sibling with no wave in flight is
                    // untouched, and pays for no comparison: dormant branches
                    // are armed lazily, when someone waits on their checks.
                    if orch.turn_end_checks_in_flight(&sibling.id) {
                        // A clean catch-up merge leaves the branch's own patch
                        // against its base exactly as it was, so a running wave
                        // finishes; a merge that carried a resolution changed it.
                        let merged = item_report.merged_clean.get(branch);
                        let patch_unchanged = match merged {
                            Some(merged) => !merged.resolved,
                            None => guard.blocking_phase("replayed patch comparison", || {
                                replay_kept_patch(
                                    &jj,
                                    &store,
                                    before.get(branch),
                                    after.get(branch),
                                    &pinned_dest,
                                )
                            }),
                        };
                        drop(guard);
                        crate::execution::checks::cancel_stale_review_on_branch_advance(
                            orch,
                            &sibling.id,
                            crate::execution::checks::BranchAdvance::BaseReplay { patch_unchanged },
                        )
                        .await;
                    }
                }
            }
            item_report
        };

        // A merge-mode conflict is the owner's: the structural repair below
        // replays the branch, which this mode never does.
        if item_report.conflicted.contains(branch)
            && item_report.failed.is_empty()
            && !merge_mode.contains(branch)
        {
            if let Some(diagnostic) = item_report.conflict_diagnostics.get(branch).cloned() {
                // A reported conflict proves the first transaction was restored.
                // Structural repair is a new transaction, but NOT a new forward deadline.
                let structural_jj = if explicit {
                    jj.next_transaction_after_restored_conflict()?
                } else {
                    jj.clone()
                };
                // These immutable-side reads and CPU-heavy proofs must not hold
                // the writer lock. The application revalidates both coordinates.
                let prepare_jj = if explicit {
                    structural_jj.clone()
                } else {
                    structural_jj.with_operation_budget(budget)
                };
                let prepare_store = store.clone();
                let prepare_dest = pinned_dest.clone();
                let baseline = replay_baseline.clone().unwrap_or_else(|| {
                    ReplayBaseline::fork_only(diagnostic.base.as_deref().unwrap_or_default())
                });
                let prepared = tokio::task::spawn_blocking(move || {
                    let jj = prepare_jj;
                    let rules = crate::orchestrator::landing::merge_rules_at(
                        &jj,
                        &prepare_store,
                        &prepare_dest,
                    );
                    prepare_structural_merge(&jj, &prepare_store, &diagnostic, &baseline, &rules)
                })
                .await
                .map_err(|error| format!("structural candidate worker failed: {error}"))?;
                #[cfg(test)]
                structural_prepared_test_hook(&store);
                if !matches!(&prepared, Ok(None)) {
                    let guard = orch
                        .acquire_jj_store_lock(
                            &store,
                            crate::orchestrator::StoreLockClass::Background,
                            replay_lock_operation(label, branch, "structural merge", budget),
                        )
                        .await;
                    let jj = if explicit {
                        structural_jj.clone()
                    } else {
                        structural_jj.with_operation_budget(budget)
                    };
                    let deadline = if explicit {
                        explicit_deadline
                    } else {
                        tokio::time::Instant::now() + budget
                    };

                    // A run may have been admitted while the pure candidate was
                    // prepared. Revalidate at this second mutation boundary.
                    let branch_siblings = siblings_for_branch(&siblings, branch);
                    let inflight =
                        tokio::time::timeout_at(deadline, inflight_run_batch(db, &branch_siblings))
                            .await
                            .map_err(|_| {
                                "structural replay timed out checking in-flight batches".to_string()
                            })??;
                    if let Some(batch) = inflight {
                        drop(guard);
                        defer_run_batch_reconcile(
                            db,
                            intent_id,
                            branch,
                            current_tip.as_deref(),
                            batch,
                            label,
                        )
                        .await?;
                        continue;
                    }
                    let tip = crate::jj::bookmark_commit_checked(&jj, &store, branch)?;
                    let quarantine = tokio::time::timeout_at(
                        deadline,
                        load_reconcile_quarantine(db, &claim.project_id, &store, branch),
                    )
                    .await
                    .map_err(|_| "structural replay timed out checking cooldown".to_string())??;
                    if let Some(existing) = quarantine.as_ref().filter(|existing| {
                        existing.failure_kind == RECONCILE_TIMEOUT
                            && quarantine_blocks_automatic_replay(
                                existing,
                                tip.as_deref(),
                                item_label,
                            )
                    }) {
                        drop(guard);
                        defer_timeout_reconcile(
                            db,
                            intent_id,
                            branch,
                            tip.as_deref(),
                            existing,
                            label,
                        )
                        .await?;
                        any_branch_deferred = true;
                        continue;
                    }
                    if let Some(refusal) = reconcile_fence_refusal(
                        db,
                        &jj,
                        &store,
                        branch,
                        &pinned_dest,
                        "Structural replay of this branch",
                    )
                    .await
                    {
                        drop(guard);
                        log::warn!("jj reconcile ({label}): {branch} structural replay not applied: {refusal}");
                        persist_reconcile_item(
                            db,
                            ReconcileItemUpdate {
                                intent_id,
                                bookmark: branch,
                                observed_tip: tip.as_deref(),
                                status: "pending",
                                failure_kind: Some(crate::jj::ROLLBACK_FAILED),
                                outcome_kind: Some("deferred"),
                                fingerprint: None,
                                diagnostic: Some(&refusal),
                            },
                        )
                        .await?;
                        any_branch_deferred = true;
                        continue;
                    }
                    let result = match prepared {
                        Ok(Some(mut plan)) => {
                            plan.source_cut = held_cut.clone();
                            plan.preserve_bookmarks =
                                held_replay_bookmarks_for_repository(&orch.db, &store).await?;
                            let _phase = guard.phase(format!(
                                "apply pre-proven structural replay branch={branch}"
                            ));
                            apply_structural_merge(&jj, &store, branch, rebase_dest, &plan)
                        }
                        Ok(None) => unreachable!("an absent candidate never takes the store lock"),
                        Err(error) => Err(error),
                    };
                    match result {
                        Ok(Some(resolved)) => {
                            item_report = resolved;
                            if let Some(commit) = crate::jj::bookmark_commit(&jj, &store, branch) {
                                after.insert(branch.clone(), commit);
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            log::warn!("jj reconcile ({label}): structural replay of {branch} failed: {error}");
                            item_report = failed_reconcile_report(branch, error);
                        }
                    }
                    if item_report.rebased_clean.contains(branch) {
                        clear_held_replay_source(
                            db,
                            &jj,
                            &store,
                            branch,
                            &pinned_dest,
                            branch,
                            expected_held_source.as_ref(),
                        )
                        .await?;
                    }
                    let held = item_report
                        .conflict_diagnostics
                        .iter()
                        .filter(|(name, _)| *name != branch)
                        .map(|(name, diagnostic)| (name.clone(), diagnostic.clone()))
                        .collect();
                    persist_held_descendant_sessions(orch, db, project_id, &store, &held).await?;
                    {
                        let _phase = guard.phase("structural recovery fence persistence");
                        if persist_transaction_recoveries(
                            db,
                            &claim.project_id,
                            &store,
                            branch,
                            label,
                            &format!("structural merge ({label})"),
                            &jj,
                            &mut item_report,
                        )
                        .await
                        {
                            recovery_recorded = true;
                        }
                    }
                    let _phase = guard.phase("structural timeout cooldown persistence");
                    if !explicit
                        && persist_report_timeouts(
                            db,
                            &claim.project_id,
                            &store,
                            branch,
                            tip.as_deref().unwrap_or("missing"),
                            &pinned_dest,
                            &mut item_report,
                        )
                        .await?
                    {
                        let _ = orch.services.emitter.emit(
                            "db-change",
                            serde_json::json!({"table":"comments", "action":"insert"}),
                        );
                    }
                    if explicit {
                        replay_phases.push(guard.phase_summary());
                    }
                }
            }
        }

        if explicit {
            for failure in &mut item_report.failed {
                if crate::jj::is_jj_timeout_error(&failure.error) {
                    let retry = if crate::jj::reconcile_failure_kind(&failure.error)
                        == crate::jj::ROLLBACK_FAILED
                    {
                        "Rollback did not complete. Do not publish this branch; recover the store before retrying."
                    } else {
                        "A new explicit request may run immediately."
                    };
                    failure.error = format!(
                        "{}\nExplicit replay timed out under its {:.3}s cumulative forward budget. \
                         No automatic cooldown was recorded. {retry}\nStore-lock phases: {}\n{}",
                        failure.error, budget.as_secs_f64(), replay_phases.join("; "),
                        branch_jj.operation_phase_summary()
                    );
                }
            }
        }

        if divergence_resolved
            && quarantine
                .as_ref()
                .is_some_and(|existing| existing.failure_kind == "ambiguous_divergence")
        {
            release_reconcile_quarantine(db, &claim.project_id, &store, branch).await?;
            quarantine = None;
        }

        if let Some(item) = ambiguous_item {
            let fingerprint = divergence_fingerprint(&item.twins);
            let already_quarantined = quarantine.as_ref().is_some_and(|existing| {
                existing.failure_kind == "ambiguous_divergence"
                    && existing.fingerprint == fingerprint
            });
            let diagnostic = "bookmark divergence has no unique canonical tip";
            persist_reconcile_item(
                db,
                ReconcileItemUpdate {
                    intent_id,
                    bookmark: branch,
                    observed_tip: current_tip.as_deref(),
                    status: "suppressed",
                    failure_kind: Some("ambiguous_divergence"),
                    outcome_kind: Some("ambiguous"),
                    fingerprint: Some(&fingerprint),
                    diagnostic: Some(diagnostic),
                },
            )
            .await?;
            if !already_quarantined {
                pending_quarantines.push(PendingReconcileQuarantine {
                    bookmark: branch.clone(),
                    failure_kind: "ambiguous_divergence".to_string(),
                    fingerprint,
                    diagnostic: Some(diagnostic.to_string()),
                });
                ambiguous.push(item);
            }
            if explicit {
                report.failed.push(crate::jj::ReconcileFailure {
                    branch: branch.clone(),
                    error: diagnostic.to_string(),
                });
                if let Some(request) = pending_request.as_ref() {
                    crate::orchestrator::conflict_session::mark_replay_request_served(
                        db, request, intent_id,
                    )
                    .await?;
                }
            }
            continue;
        }

        if item_report.conflicted.contains(branch) {
            let tip = after
                .get(branch)
                .or_else(|| before.get(branch))
                .map_or("missing", String::as_str);
            if merge_mode.contains(branch) {
                upsert_reconcile_quarantine(
                    db,
                    &claim.project_id,
                    &store,
                    branch,
                    crate::orchestrator::catch_up::MERGE_CONFLICT_QUARANTINE,
                    &format!("{tip}:{pinned_dest}"),
                    Some(
                        "automatic catch-up merge conflicted and was undone; this tip and destination are not merged again until the tip changes, the base advances, or an explicit replay is requested",
                    ),
                )
                .await?;
            } else {
                upsert_reconcile_quarantine(
                    db,
                    &claim.project_id,
                    &store,
                    branch,
                    ROLLED_BACK_CONFLICT,
                    tip,
                    Some(
                        "automatic base-advance replay conflicted and was rolled back; unchanged tips are parked until an explicit replay is requested or the branch tip changes",
                    ),
                )
                .await?;
            }
        }

        // Origin transfer and durable lineage persistence are deliberately
        // outside the jj mutex. A clean replay reaches origin only when the
        // branch's publication rule requires it; otherwise it stays local, as a
        // commit would, until opening its pull request publishes it.
        let publish = item_report.rebased_clean.contains(branch)
            && !item_report.silent.contains(branch)
            && branch_publication_requirement(db, &claim.project_id, &siblings, branch).await
                == PublicationRequirement::RequiredForOpenPr;
        if publish {
            if let Err(error) = publish_managed_branch(orch, &store, branch).await {
                item_report
                    .rebased_clean
                    .retain(|candidate| candidate != branch);
                let cache_diagnostic =
                    mark_publication_unconfirmed(orch, db, &claim.project_id, branch)
                        .await
                        .err()
                        .map(|cache_error| format!("; additionally, {cache_error}"))
                        .unwrap_or_default();
                item_report.failed.push(crate::jj::ReconcileFailure {
                    branch: branch.clone(),
                    error: format!(
                        "origin push failed: {error}. GitHub still holds the previous branch head; the pull-request artifact has been downgraded to UNKNOWN until publication recovers{cache_diagnostic}"
                    ),
                });
            }
        }
        // Bookkeeping that could not be persisted does not change what the
        // graph did. The branch keeps its real classification and its agent
        // keeps whatever notification that classification earned; the
        // unpersisted coordinate is recorded on the reconcile item and logged
        // for the operator, and reaches nobody else.
        let mut internal_diagnostic: Option<String> = None;
        let touched =
            item_report.rebased_clean.contains(branch) || item_report.conflicted.contains(branch);
        if touched {
            if let Some(sibling) = siblings
                .iter()
                .find(|candidate| sibling_branch(candidate).as_deref() == Some(branch.as_str()))
            {
                if let Err(error) = advance_sibling_durable_base(db, sibling, &pinned_dest).await {
                    log::warn!(
                        "jj reconcile ({label}): durable base advancement for {branch} did not \
                         persist: {error}"
                    );
                    internal_diagnostic = Some(format!("durable base advancement failed: {error}"));
                }
            }
        }

        let reported_failure = item_report
            .failed
            .iter()
            .find(|failure| failure.branch == *branch)
            .map(|failure| failure.error.as_str());
        let item_diagnostic = reported_failure.or(internal_diagnostic.as_deref());
        let failure_kind = item_diagnostic.map(crate::jj::reconcile_failure_kind);
        let permanent = failure_kind.is_some_and(crate::jj::reconcile_failure_is_permanent);
        let quarantine_fingerprint = after
            .get(branch)
            .or_else(|| before.get(branch))
            .map_or("missing", String::as_str);
        if permanent {
            let fingerprint = if failure_kind == Some(crate::jj::MERGE_CATCH_UP_REFUSED) {
                format!("{quarantine_fingerprint}:{pinned_dest}")
            } else {
                quarantine_fingerprint.to_string()
            };
            pending_quarantines.push(PendingReconcileQuarantine {
                bookmark: branch.clone(),
                failure_kind: failure_kind.unwrap_or("unknown").to_string(),
                fingerprint,
                diagnostic: item_diagnostic.map(str::to_string),
            });
        }
        let suppression_fingerprint = permanent.then(|| {
            format!(
                "{}:{}:{}:{}",
                pinned_dest,
                branch,
                after
                    .get(branch)
                    .or_else(|| before.get(branch))
                    .map_or("missing", String::as_str),
                failure_kind.unwrap_or("unknown")
            )
        });
        let outcome_kind = if reported_failure.is_some() {
            "failed"
        } else if item_report.conflicted.contains(branch) {
            "conflicted"
        } else if item_report.rebased_clean.contains(branch) {
            "rebased_clean"
        } else if item_report.silent.contains(branch) {
            "silent"
        } else {
            "unchanged"
        };
        persist_reconcile_item(
            db,
            ReconcileItemUpdate {
                intent_id,
                bookmark: branch,
                observed_tip: after
                    .get(branch)
                    .or_else(|| before.get(branch))
                    .map(String::as_str),
                status: if permanent {
                    "suppressed"
                } else if explicit && reported_failure.is_some() {
                    "completed"
                } else if item_diagnostic.is_some() {
                    "pending"
                } else {
                    "graph_moved"
                },
                failure_kind,
                outcome_kind: Some(outcome_kind),
                fingerprint: suppression_fingerprint.as_deref(),
                diagnostic: item_diagnostic,
            },
        )
        .await?;
        if ref_lock_deferral.is_none() {
            ref_lock_deferral = reported_failure.and_then(crate::jj::ref_lock_deferral);
            // The cause once, with the holder the give-up named, rather than
            // once per deferred branch; each deferred row also carries it.
            if let Some(diagnosis) = reported_failure.and_then(crate::jj::ref_lock_diagnosis) {
                log::warn!(
                    "jj reconcile ({label}): {branch} gave up on the ref-store lock; deferring the \
                     rest of this pass to the sweep. {diagnosis}"
                );
            }
        }

        // A conflicted branch opens (or refreshes) a durable resolution session,
        // then — and only then — markers are projected into its checkout. The
        // order matters: the session row must exist before the diagnostic can
        // land on it, and the marker state must be recorded from what the
        // executor confirmed before any wake is composed.
        let mut marker_state: Option<MarkerState> = None;
        if let Some(diagnostic) = item_report.conflict_diagnostics.get(branch) {
            // Inherited from the session open before this pass: which paths of
            // the tip that just failed were resolved against what.
            let basis = resolution_basis_for_conflict.clone().unwrap_or_else(|| {
                crate::orchestrator::conflict_session::inherited_basis(
                    jj,
                    &store,
                    open_session.as_ref(),
                    diagnostic.ours.as_deref(),
                )
            });
            record_conflict_session(db, intent_id, branch, diagnostic, &notes.incoming, &basis)
                .await?;
            supersede_stale_sessions(db, branch, intent_id).await?;
            withdraw_pending_clean_notes(db, branch).await?;
            let (state, marker_diagnostic, dispositions) =
                materialize_markers_for_branch(orch, db, &claim.project_id, branch, diagnostic)
                    .await;
            record_marker_state(
                db,
                intent_id,
                branch,
                state,
                marker_diagnostic.as_deref(),
                &dispositions,
            )
            .await?;
            marker_state = Some(state);
        } else if item_report.rebased_clean.contains(branch) {
            // The branch absorbed the incoming change, so whatever session it had
            // is genuinely finished. This is the only truthful close: an ordinary
            // commit resolves CONTENT, but a branch is not reconciled until its
            // ancestry actually moves, which is exactly what just happened.
            close_open_sessions_for_branch(db, branch).await?;
            if quarantine.as_ref().is_some_and(|existing| {
                matches!(
                    existing.failure_kind.as_str(),
                    ROLLED_BACK_CONFLICT | RECONCILE_TIMEOUT
                ) || existing.is_merge_catch_up()
            }) {
                release_reconcile_quarantine(db, &claim.project_id, &store, branch).await?;
            }
        }

        // A wait is not service. Only a pass that produced a terminal replay
        // verdict consumes this request; the compare token protects newer asks.
        if item_report.rebased_clean.contains(branch)
            || item_report.conflicted.contains(branch)
            || item_report.silent.contains(branch)
            || item_report
                .failed
                .iter()
                .any(|failure| failure.branch == *branch)
        {
            if let Some(request) = pending_request.as_ref() {
                crate::orchestrator::conflict_session::mark_replay_request_served(
                    db, request, intent_id,
                )
                .await?;
            }
        }
        // The blocking note goes out with the session that justifies it, not
        // after the loop: a pass that dies on a later branch (the base
        // routinely moves again mid-pass) must not take this one's note with
        // it. It is sent without the moved-commit filter clean notes use,
        // because a conflict is rolled back and so never moves the commit;
        // redundancy is the delivery key's to suppress, and the key carries the
        // diagnostic's fingerprint so a genuinely new advance still gets through.
        if item_report.conflicted.contains(branch) {
            let only = |value| HashMap::from([(branch.clone(), value)]);
            let evidence = ConflictEvidence {
                diagnostics: item_report
                    .conflict_diagnostics
                    .get(branch)
                    .cloned()
                    .map(only)
                    .unwrap_or_default(),
                marker_states: marker_state
                    .map(|state| HashMap::from([(branch.clone(), state)]))
                    .unwrap_or_default(),
                carry_notes: carry_notes
                    .remove(branch)
                    .map(|note| HashMap::from([(branch.clone(), note)]))
                    .unwrap_or_default(),
            };
            notify_conflicted_siblings(
                orch,
                db,
                &siblings,
                std::slice::from_ref(branch),
                &notes.conflict,
                &evidence,
                intent_id,
            )?;
        }

        // A merge-mode branch's outcome goes to its audit trail, and a clean
        // merge carries its own wake (or passive note) naming what it brought
        // in, in place of the generic clean note below.
        if merge_mode.contains(branch) {
            crate::orchestrator::catch_up::announce_reconcile_outcome(
                orch,
                db,
                &claim.project_id,
                &store,
                branch,
                rebase_dest,
                &item_report,
            )
            .await;
        }
        report
            .merged_clean
            .extend(std::mem::take(&mut item_report.merged_clean));
        report
            .merge_refused
            .extend(std::mem::take(&mut item_report.merge_refused));
        report.rebased_clean.append(&mut item_report.rebased_clean);
        report.conflicted.append(&mut item_report.conflicted);
        report.silent.append(&mut item_report.silent);
        report.held.append(&mut item_report.held);
        report.failed.append(&mut item_report.failed);
        report
            .conflict_diagnostics
            .extend(std::mem::take(&mut item_report.conflict_diagnostics));
        heartbeat_reconcile_intent(db, &claim).await?;
        tokio::task::yield_now().await;
    }

    let ambiguous_notified = if ambiguous.is_empty() {
        Vec::new()
    } else {
        notify_ambiguous_divergence(orch, db, &siblings, &ambiguous, intent_id)?
    };

    log::info!(
        "jj reconcile ({label}): {} rebased clean, {} recorded a conflict, {} failed",
        report.rebased_clean.len(),
        report.conflicted.len(),
        report.failed.len()
    );

    let failed_notified = if report.failed.is_empty() {
        Vec::new()
    } else {
        notify_failed_siblings(orch, db, &siblings, &report.failed, label, intent_id)?
    };
    let notified: Vec<String> = ambiguous_notified
        .into_iter()
        .chain(failed_notified)
        .collect();
    activate_notified_quarantines(
        db,
        &claim.project_id,
        &store,
        &pending_quarantines,
        &notified,
    )
    .await?;
    if recovery_recorded {
        // Every store guard of this pass is released by now; the fence itself
        // was durable before each was.
        crate::orchestrator::branch_recovery::deliver_pending_notices(orch, db).await;
    }

    // Re-read each cleanly-rebased sibling's commit id AFTER the rebase, so we
    // notify only the ones whose commit actually changed (a no-op double-fire
    // leaves it equal). Conflicted siblings are deliberately absent: their rebase
    // was rolled back, so their commit is unchanged BY DESIGN and the same test
    // would mean the opposite thing.
    let after: HashMap<String, String> = report
        .rebased_clean
        .iter()
        .filter_map(|branch| {
            crate::jj::bookmark_commit(&jj, &store, branch).map(|commit| (branch.clone(), commit))
        })
        .collect();

    // Cleanly-rebased siblings: nothing to resolve, but their branch moved — a
    // passive (non-waking) note rides along into their next natural run.
    let mut clean_rewritten = siblings_rewritten(&report.rebased_clean, &before, &after);
    clean_rewritten.retain(|branch| !report.silent.contains(branch));
    // A catch-up merge already announced itself with what it brought in.
    let merge_announced: Vec<String> = clean_rewritten
        .iter()
        .filter(|branch| report.merged_clean.contains_key(*branch))
        .cloned()
        .collect();
    // A clean replay closes its branch's session on the way through, so a
    // session open now was recorded since, and "nothing to resolve" would
    // contradict the blocking note that session sent.
    let mut clean_announced = Vec::with_capacity(clean_rewritten.len());
    for branch in &clean_rewritten {
        if merge_announced.contains(branch) {
            continue;
        }
        if crate::orchestrator::conflict_session::load_active_session(db, branch)
            .await?
            .is_none()
        {
            clean_announced.push(branch.clone());
        }
    }
    if clean_announced.is_empty() {
        log::debug!("jj reconcile ({label}): clean rebases unchanged since a prior reconcile, or now in conflict; no clean note");
    } else {
        notify_clean_siblings(
            orch,
            db,
            &siblings,
            &clean_announced,
            &notes.clean,
            intent_id,
        )?;
    }

    // Delivery is a separate durable step from graph movement. A restart before
    // this write resumes graph_moved items without replaying their jj mutation.
    mark_reconcile_delivered(db, intent_id).await?;

    // A branch held by an in-flight run batch does not keep the intent open:
    // its item stays pending/deferred, and `reopen_released_deferrals` returns
    // the intent to the sweep once the batch ends. The other deferrals (an
    // unreadable tip, a timeout cooldown, the ref-store lock) still retry.
    let retry_transient = any_branch_deferred
        || report.failed.iter().any(|failure| {
            !explicit_branches.contains(&failure.branch)
                && reconcile_has_transient_failures(std::slice::from_ref(failure))
        });

    // Fan a lifetime-cell refresh out to every sibling this reconcile actually
    // rewrote. A running cell on a rebased job branch must follow the logical
    // coordinate to the new tip or it keeps serving pre-rebase source. This is the
    // sibling analogue of the advanced-branch fan-out, and reaches every caller of
    // this shared body — including the external and startup default-advance paths
    // that previously skipped it. The store lock is released by now.
    // Only cleanly-rebased siblings moved; a conflicted sibling was rolled back to
    // the coordinate its cells already hold, so there is nothing to follow.
    let mut terminal_failed = 0;
    for branch in clean_rewritten.iter() {
        if let Some(new_tip) = after.get(branch) {
            terminal_failed +=
                refresh_residencies_for_branch(orch, db, project_id, branch, new_tip).await;
        }
    }

    finish_reconcile_intent(db, intent_id, &claim.owner, retry_transient).await?;
    Ok(BranchAdvanceOutcome {
        eligible: siblings.len(),
        rebased_clean: clean_rewritten.len(),
        conflicted: report.conflicted.len(),
        failed: report.failed.len() + terminal_failed,
        coalesced_destination: None,
        coalesced_intent: None,
        replay_diagnostic: report
            .failed
            .iter()
            .find(|failure| explicit_branches.contains(&failure.branch))
            .map(|failure| failure.error.clone()),
    })
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

fn siblings_rewritten(
    branches: &[String],
    before: &HashMap<String, String>,
    after: &HashMap<String, String>,
) -> Vec<String> {
    branches
        .iter()
        .filter(
            |branch| match (before.get(branch.as_str()), after.get(branch.as_str())) {
                (Some(before_commit), Some(after_commit)) => before_commit != after_commit,
                _ => true,
            },
        )
        .cloned()
        .collect()
}

/// Drop siblings whose branch bookmark no longer exists in `existing`, returning
/// the retained specs and how many were dropped (for one summary log line). This
/// is the store-truth guard on the DB-sourced sibling set: `load_sibling_jobs`
/// yields stale rows for long-dead `agent/…` branches, and filtering them here —
/// before the divergence-collapse and before-snapshot loops — keeps a base advance
/// from spawning a `jj` subprocess per dead sibling. `None` (the store-wide
/// bookmark list failed) disables the filter: proceed with all, liveness over
/// strictness.
fn retain_present_siblings(
    specs: Vec<String>,
    existing: Option<&std::collections::HashSet<String>>,
) -> (Vec<String>, usize) {
    let Some(existing) = existing else {
        return (specs, 0);
    };
    let total = specs.len();
    let retained: Vec<_> = specs
        .into_iter()
        .filter(|branch| existing.contains(branch))
        .collect();
    let dropped = total - retained.len();
    (retained, dropped)
}

/// The sibling's runner-owned logical bookmark.
fn sibling_branch(sibling: &SiblingJob) -> Option<String> {
    sibling.branch.clone()
}

/// Each branch once, in first-seen order.
fn unique_branches(specs: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    specs
        .into_iter()
        .filter(|branch| seen.insert(branch.clone()))
        .collect()
}

/// Whether a re-claimed pass takes this item up after its graph mutation
/// instead of replaying it. A conflict never qualifies: its rebase was rolled
/// back, so the graph did not move, and replaying is what rebuilds the
/// diagnostic its blocking note is composed from.
fn resumes_past_mutation(progress: &ReconcileItemProgress) -> bool {
    progress.status == "graph_moved" && progress.outcome_kind.as_deref() != Some("conflicted")
}

/// The delivery key of a branch's conflict note, without the diagnostic
/// fingerprint `notify_conflicted_siblings` appends when it has one. Every
/// note for one session shares this prefix.
fn conflict_wake_key(delivery_scope: &str, branch: &str) -> String {
    format!("{delivery_scope}:{branch}:conflicted")
}

/// The delivery key of a branch's clean-rebase note.
fn clean_note_key(delivery_scope: &str, branch: &str) -> String {
    format!("{delivery_scope}:{branch}:clean")
}

/// Whether the branch's open conflict session has put its blocking note in an
/// agent's queue.
///
/// A rolled-back conflict parks its branch, and a parked branch is not replayed
/// again until its tip changes, so the park is only safe once the agent has
/// been told. The note is the fact to check, not the order things were written
/// in: a pass can record the session and quarantine and then die before
/// notifying (the base moving again mid-pass ends one every few minutes), and
/// the intent it belonged to is then superseded and never revisited.
async fn conflict_wake_queued(db: &LocalDb, branch: &str) -> Result<bool, String> {
    let Some(session) =
        crate::orchestrator::conflict_session::load_active_session(db, branch).await?
    else {
        return Ok(false);
    };
    let prefix = system_direct_once_message_id(&conflict_wake_key(&session.intent_id, branch));
    let queued = db
        .query_text(
            "SELECT id FROM messages WHERE substr(id, 1, length(?1)) = ?1 LIMIT 1",
            params![prefix.as_str()],
        )
        .await
        .map_err(|error| format!("read conflict wake for {branch}: {error}"))?;
    Ok(queued.is_some())
}

/// Withdraw every clean-rebase note still waiting in a queue for `branch`.
///
/// Each was true when it was queued and is false once the branch has an open
/// conflict session: read after the conflict, "nothing to resolve" is how an
/// agent learns to discount the one note that matters. Only pending pushes are
/// dismissed; one already delivered stays delivered, and the message row stays
/// as the record of what was said.
async fn withdraw_pending_clean_notes(db: &LocalDb, branch: &str) -> Result<(), String> {
    let prefix = crate::orchestrator::attention_push::direct_push_key(
        &system_direct_once_message_id(""),
        None,
    );
    let suffix = clean_note_key("", branch);
    let ids = db
        .read(move |conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id FROM attention_pushes
                          WHERE delivered_event_id IS NULL AND retired_at IS NULL
                            AND substr(key, 1, length(?1)) = ?1
                            AND substr(key, length(key) - length(?2) + 1) = ?2",
                        params![prefix.as_str(), suffix.as_str()],
                    )
                    .await?;
                let mut ids = Vec::new();
                while let Some(row) = rows.next().await? {
                    ids.push(row.text(0)?);
                }
                Ok(ids)
            })
        })
        .await
        .map_err(|error| format!("list pending clean notes for {branch}: {error}"))?;
    for id in &ids {
        crate::orchestrator::attention_push::delete_pending_by_id(db, id)
            .await
            .map_err(|error| format!("withdraw clean note {id}: {error}"))?;
    }
    if !ids.is_empty() {
        log::info!(
            "jj reconcile: withdrew {} pending clean-rebase note(s) for {branch}, which now has an open conflict session",
            ids.len()
        );
    }
    Ok(())
}

/// The IDENTITY half of the note for a sibling whose auto-rebase could not be
/// applied cleanly: which base advanced, which PR and issue carried it, and the
/// fact that the rebase was rolled back.
///
/// It deliberately stops short of telling the agent what to do. What to do
/// depends on the condition the rebase actually found — a content conflict and
/// base drift call for opposite actions — and that is
/// [`append_conflict_diagnostic`]'s to say, from evidence, per branch. This half
/// is the part that is true regardless.
///
/// The branch was NOT rebased: a conflicting rebase is rolled back before it can
/// reach git, so the branch still sits on its own content at its own base, and no
/// conflict markers exist anywhere — not in the store, not in any cell. That is
/// what makes this actionable, and it is why the note never asks the agent to
/// "resolve markers" that were never materialized. It is still STOP-THE-LINE:
/// until the branch carries the new
/// base's content, its merge will keep being refused. Delivered via a `Steer`
/// system direct that wakes idle agents and lands at the next tool boundary
/// without stopping an active turn (see `notify_conflicted_siblings`).
fn build_jj_conflict_note(
    base_branch: &str,
    pr_number: Option<i64>,
    issue_info: Option<&IssueInfo>,
) -> String {
    let pr_fragment = pr_number
        .map(|number| format!("PR #{} merged", number))
        .unwrap_or_else(|| "A PR merged".to_string());
    let issue_fragment = issue_info
        .map(|issue| format!(" (cairn://p/{}/{})", issue.project_key, issue.number))
        .unwrap_or_default();
    format!(
        "⛔ BLOCKING [Base branch update] Your base branch `{base_branch}` advanced — {pr_fragment}{issue_fragment}. The automatic rebase could not be applied cleanly and did not land the new base. Your branch remains on its pre-advance content, but replay machinery may rewrite its store commit identity; use the current coordinates in `cairn:~/rebase`. Your work cannot merge until it carries the new base."
    )
}

/// The note for a sibling whose auto-rebase landed cleanly — its branch moved onto
/// the advanced base with no conflict, so there is nothing to resolve. Delivered
/// passively (a non-waking `queue_system_direct`) so it rides along into the
/// agent's next natural run rather than mechanically resuming an idle agent (see
/// `notify_clean_siblings`).
fn build_jj_clean_note(
    base_branch: &str,
    pr_number: Option<i64>,
    issue_info: Option<&IssueInfo>,
) -> String {
    let pr_fragment = pr_number
        .map(|number| format!("PR #{} merged", number))
        .unwrap_or_else(|| "A PR merged".to_string());
    let issue_fragment = issue_info
        .map(|issue| format!(" (cairn://p/{}/{})", issue.project_key, issue.number))
        .unwrap_or_default();
    format!(
        "[Base branch update] Your base branch `{base_branch}` advanced — {pr_fragment}{issue_fragment}. Your work was auto-rebased cleanly onto the new tip; nothing to resolve. No manual rebase or force-push is needed."
    )
}

/// Validate the runner-owned bookmark that advanced and refresh matching
/// held cells. Agent jobs have no follower workspace to update.
async fn refresh_advanced_branch_cells(
    orch: &Orchestrator,
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    repo_path: &str,
) {
    let on_branch = match load_on_branch_jobs(db, project_id, branch).await {
        Ok(jobs) => jobs,
        Err(error) => {
            log::warn!("on-branch advance: failed to load jobs on {branch}: {error}");
            return;
        }
    };
    if on_branch.is_empty() {
        log::debug!("on-branch advance on {branch}: no in-flight job uses the branch");
        return;
    }

    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));

    // Keep the writer exclusion through collapse and coordinate capture, not
    // through notification delivery or remote executor refresh.
    let (collapse, dest) = {
        let guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                format!("on-branch coordinate advance: {branch}"),
            )
            .await;
        let jj = jj.with_operation_budget(crate::jj::BACKGROUND_RECONCILE_BUDGET);
        let _phase = guard.phase("collapse and capture advanced coordinate");
        let collapse = crate::jj::collapse_divergent_bookmark(&jj, &store, branch);
        let dest = crate::jj::bookmark_commit(&jj, &store, branch);
        (collapse, dest)
    };
    match collapse {
        Ok(crate::jj::CollapseOutcome::NotDivergent) => {
            if let Err(error) = release_reconcile_quarantine(db, project_id, &store, branch).await {
                log::warn!("on-branch advance: failed to release quarantine for {branch}: {error}");
            }
        }
        Ok(crate::jj::CollapseOutcome::Collapsed { kept, abandoned }) => {
            if let Err(error) = release_reconcile_quarantine(db, project_id, &store, branch).await {
                log::warn!("on-branch advance: failed to release quarantine for {branch}: {error}");
            }
            log::info!(
                "jj collapse (on-branch {branch}): converged to {kept}; abandoned {}",
                abandoned.join(", ")
            );
        }
        Ok(crate::jj::CollapseOutcome::Ambiguous { change_id, twins }) => {
            let fingerprint = divergence_fingerprint(&twins);
            let already_quarantined =
                match load_reconcile_quarantine(db, project_id, &store, branch).await {
                    Ok(Some(existing)) => {
                        existing.failure_kind == "ambiguous_divergence"
                            && existing.fingerprint == fingerprint
                    }
                    Ok(None) => false,
                    Err(error) => {
                        log::warn!(
                            "on-branch advance: failed to inspect quarantine for {branch}: {error}"
                        );
                        false
                    }
                };
            if already_quarantined {
                log::debug!(
                    "jj collapse (on-branch {branch}): unchanged ambiguous divergence remains quarantined"
                );
                return;
            }
            log::warn!(
                "jj collapse (on-branch {branch}): divergent change {change_id} is ambiguous (twins {}); interrupting on-branch jobs and skipping the refresh",
                twins.join(", ")
            );
            let mut all_notified = true;
            for job in &on_branch {
                let Some(run_id) = latest_run_for_job(db, &job.id) else {
                    all_notified = false;
                    continue;
                };
                let message = build_ambiguous_divergence_note(branch, &change_id, &twins);
                let key =
                    on_branch_ambiguous_delivery_key(project_id, branch, &fingerprint, &run_id);
                match queue_system_direct_once_confirmed(
                    orch,
                    &run_id,
                    &message,
                    DeliveryUrgency::Interrupt,
                    &key,
                ) {
                    Ok(DirectQueueDisposition::QueuedOrPresent) => {}
                    Ok(DirectQueueDisposition::Undeliverable) => all_notified = false,
                    Err(error) => {
                        all_notified = false;
                        log::warn!(
                            "on-branch advance: failed to interrupt {} for ambiguous divergence: {error}",
                            job.id
                        );
                    }
                }
            }
            if all_notified {
                if let Err(error) = upsert_reconcile_quarantine(
                    db,
                    project_id,
                    &store,
                    branch,
                    "ambiguous_divergence",
                    &fingerprint,
                    Some("bookmark divergence has no unique canonical tip"),
                )
                .await
                {
                    log::warn!("on-branch advance: failed to quarantine {branch}: {error}");
                }
            }
            return;
        }
        Err(error) => log::warn!("jj collapse (on-branch {branch}): failed: {error}"),
    }

    let Some(dest) = dest else {
        log::debug!("on-branch advance: bookmark {branch} did not resolve in store; skipping");
        return;
    };

    let refresh_failures =
        refresh_residencies_for_branch(orch, db, project_id, branch, &dest).await;
    if refresh_failures > 0 {
        log::warn!(
            "on-branch advance: {refresh_failures} held cell(s) failed to refresh for {branch}"
        );
    }
}

/// The note for a sibling whose auto-rebase recorded a conflict after the default
/// branch advanced **outside Cairn** (a non-Cairn merge or direct push detected
/// via the GitHub `push` webhook). Same shape as `build_jj_conflict_note` but
/// carries no PR number — there is no Cairn-tracked owner for the advance.
fn build_external_advance_conflict_note(default_branch: &str) -> String {
    format!(
        "⛔ BLOCKING [Base branch update] Your base branch `{default_branch}` advanced (changes landed outside Cairn). The automatic rebase could not be applied cleanly and did not land the new base. Your branch remains on its pre-advance content, but replay machinery may rewrite its store commit identity; use the current coordinates in `cairn:~/rebase`. Your work cannot merge until it carries the new base."
    )
}

/// The clean-rebase counterpart to `build_external_advance_conflict_note`: the
/// default branch advanced outside Cairn and the sibling's auto-rebase landed
/// cleanly. Carries no PR number (no Cairn-tracked owner) and needs no action;
/// delivered passively.
fn build_external_advance_clean_note(default_branch: &str) -> String {
    format!(
        "[Base branch update] Your base branch `{default_branch}` advanced (changes landed outside Cairn). Your work was auto-rebased cleanly onto the new tip; nothing to resolve. No manual rebase or force-push is needed."
    )
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

/// Steer every sibling whose auto-rebase recorded a conflict: a conflicted
/// sibling's PR can never advance (jj refuses to push a conflicted commit), so the
/// branch is wedged until the agent resolves the materialized markers and
/// re-seals. `queue_system_direct` enqueues a `Steer` delivery — it wakes an idle
/// recipient and lands at an active recipient's next tool boundary without
/// cancelling the tool call in progress.
/// `files_by_branch` supplies the conflicting file paths per branch, appended to
/// the note so the agent knows exactly where to look. Cleanly-rebased siblings
/// are not in `conflicted`; they receive a passive note via `notify_clean_siblings`.
fn notify_failed_siblings(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    failed: &[crate::jj::ReconcileFailure],
    label: &str,
    delivery_scope: &str,
) -> Result<Vec<String>, String> {
    notify_failed_siblings_with(
        orch,
        db,
        siblings,
        failed,
        label,
        delivery_scope,
        queue_system_direct_once_confirmed,
    )
}

fn notify_failed_siblings_with<F>(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    failed: &[crate::jj::ReconcileFailure],
    label: &str,
    delivery_scope: &str,
    enqueue: F,
) -> Result<Vec<String>, String>
where
    F: Fn(
        &Orchestrator,
        &str,
        &str,
        DeliveryUrgency,
        &str,
    ) -> Result<DirectQueueDisposition, String>,
{
    let mut notified = Vec::new();
    for failure in failed {
        let Some(sibling) = siblings
            .iter()
            .find(|sibling| sibling_branch(sibling).as_deref() == Some(failure.branch.as_str()))
        else {
            continue;
        };
        // Runner-internal bookkeeping never addresses an agent. The
        // `persistence` kind covers the durable-base compare-and-swap and
        // genuine database persist errors; an agent can act on neither. The CAS
        // path already declines to report these as failures at all — this is the
        // backstop that keeps any future internal error from reaching a builder.
        let failure_kind = crate::jj::reconcile_failure_kind(&failure.error);
        if failure_kind == "persistence" {
            log::warn!(
                "jj reconcile: internal {failure_kind} failure on {} withheld from job {}: {}",
                failure.branch,
                sibling.id,
                failure.error
            );
            continue;
        }
        // A lost `packed-refs.lock` is the ref store's state, not the branch's:
        // the worker retries it and cairn:~/rebase reports it, and "needs
        // repair" would send an agent after something only the machine can
        // clear. A stale lock nobody could remove is the operator's to see.
        if crate::jj::reconcile_failure_is_ref_lock(failure_kind) {
            if failure_kind == crate::jj::REF_LOCK_STALE {
                log::error!(
                    "jj reconcile: {} is blocked by a stale packed-refs lock that patience \
                     could not remove; retried automatically, withheld from job {}: {}",
                    failure.branch,
                    sibling.id,
                    failure.error
                );
            } else {
                log::info!(
                    "jj reconcile: packed-refs lock contention on {} withheld from job {}; \
                     retried automatically",
                    failure.branch,
                    sibling.id
                );
            }
            continue;
        }
        let Some(run_id) = latest_run_for_job(db, &sibling.id) else {
            log::debug!(
                "jj reconcile: no run for failed sibling {} to steer",
                sibling.id
            );
            continue;
        };
        let quarantine_note = if crate::jj::reconcile_failure_is_permanent(failure_kind) {
            let guidance = match failure_kind {
                "immutable_commit" => "jj refused to rewrite some of this branch's commits. The diagnostic above says why when Cairn could tell: if they are already on the default branch, the work landed and the PR and issue can close; if origin holds them under a remote bookmark this store does not track, nothing landed and that copy needs reconciling, so report the diagnostic.",
                "missing_bookmark" => "This branch no longer exists and needs to be re-created; report the diagnostic above.",
                _ => "Report the diagnostic above.",
            };
            format!(
                "\nFuture base-branch updates will skip this branch until it changes. {guidance}"
            )
        } else {
            String::new()
        };
        let note = format!(
            "⛔ BLOCKING [Base branch update] Cairn could not update branch `{}` after the base branch advanced ({label}).\nDiagnostic:\n{}\nYour commits are intact. Do not force-push or reset; this branch needs repair before it can move.{quarantine_note}",
            failure.branch,
            failure.error
        );
        let key = format!("{delivery_scope}:{}:failed", failure.branch);
        if enqueue(orch, &run_id, &note, DeliveryUrgency::Steer, &key)?
            == DirectQueueDisposition::Undeliverable
        {
            continue;
        }
        notified.push(failure.branch.clone());
        log::info!(
            "Steered jj sibling job {} after automatic reconcile failed",
            sibling.id
        );
    }
    Ok(notified)
}

/// What the reconcile pass learned about each conflicted branch: the diagnostic
/// captured before its rollback, and the marker state an executor confirmed.
/// They travel together because a wake is only truthful when composed from both.
#[derive(Default)]
struct ConflictEvidence {
    diagnostics: HashMap<String, crate::jj::ConflictDiagnostic>,
    marker_states: HashMap<String, MarkerState>,
    /// For an explicit replay, which committed resolutions could not be
    /// carried onto the destination, and why.
    carry_notes: HashMap<String, String>,
}

fn notify_conflicted_siblings(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    conflicted: &[String],
    note: &str,
    evidence: &ConflictEvidence,
    delivery_scope: &str,
) -> Result<(), String> {
    for sibling in siblings {
        let Some(branch) = sibling_branch(sibling) else {
            continue;
        };
        if !conflicted.contains(&branch) {
            continue;
        }
        let Some(run_id) = latest_run_for_job(db, &sibling.id) else {
            log::debug!(
                "jj reconcile: no run for conflicted sibling {} to steer",
                sibling.id
            );
            continue;
        };
        let diagnostic = evidence.diagnostics.get(&branch);
        let marker_state = evidence
            .marker_states
            .get(&branch)
            .copied()
            .unwrap_or_default();
        let mut message = append_conflict_diagnostic(note, diagnostic, marker_state);
        if let Some(carry_note) = evidence.carry_notes.get(&branch) {
            message.push_str("\n\n");
            message.push_str(carry_note);
        }
        // The delivery key carries the diagnostic's fingerprint, so a resumed or
        // double-fired reconcile at the SAME base cannot re-wake the agent, while
        // a genuinely new base advance — different coordinates — correctly does.
        // Without it, a second advance arriving inside one intent would be
        // silently swallowed as a duplicate of the first.
        let key = match diagnostic {
            Some(diagnostic) => format!(
                "{}:{}",
                conflict_wake_key(delivery_scope, &branch),
                diagnostic.fingerprint()
            ),
            None => conflict_wake_key(delivery_scope, &branch),
        };
        queue_system_direct_once(orch, &run_id, &message, DeliveryUrgency::Steer, &key)?;
        log::info!(
            "Steered jj sibling job {} to resolve a recorded conflict",
            sibling.id
        );
    }
    Ok(())
}

/// One bookmark whose divergent change the collapse step refused to resolve
/// automatically, carried from the collapse loop to the interrupt layer.
struct AmbiguousDivergence {
    branch: String,
    change_id: String,
    twins: Vec<String>,
}

/// The stop-the-line note for a bookmark carrying an AMBIGUOUS divergent change
/// Cairn declined to collapse (every twin conflicts, or more than one carries
/// edits — picking one automatically could lose work). Names the bookmark, the
/// change-id, and the twin commit ids, and instructs MANUAL resolution +
/// escalation, never a force-push: Cairn owns the deterministic collapse, and a
/// genuinely ambiguous tangle is a human's call, not something the agent papers
/// over by pushing a hand-picked twin.
fn build_ambiguous_divergence_note(branch: &str, change_id: &str, twins: &[String]) -> String {
    format!(
        "⛔ BLOCKING [Divergent history] Your branch `{branch}` has more than one version of the same change ({}) that Cairn could not reconcile automatically — either every copy still conflicts or more than one carries edits, so choosing for you could lose work. Sort it out by hand (keep the correct commit, discard the rest), then verify the build and tests. Do NOT force-push; if you cannot resolve it cleanly, escalate to a human. Change id: `{change_id}`.",
        twins.join(", ")
    )
}

/// Interrupt every sibling whose bookmark carries an ambiguous divergent change
/// the collapse step refused to resolve. Mirror of `notify_conflicted_siblings`:
/// map each ambiguous branch -> its sibling job -> latest run -> a stop-the-line
/// `Interrupt`. The store was left untouched, so the message names the divergent
/// twins and asks for manual resolution + escalation (never a force-push).
fn notify_ambiguous_divergence(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    ambiguous: &[AmbiguousDivergence],
    delivery_scope: &str,
) -> Result<Vec<String>, String> {
    let mut notified = Vec::new();
    for divergence in ambiguous {
        let Some(sibling) = siblings
            .iter()
            .find(|sibling| sibling_branch(sibling).as_deref() == Some(divergence.branch.as_str()))
        else {
            continue;
        };
        let Some(run_id) = latest_run_for_job(db, &sibling.id) else {
            log::debug!(
                "jj collapse: no run for ambiguous sibling {} to interrupt",
                sibling.id
            );
            continue;
        };
        let message = build_ambiguous_divergence_note(
            &divergence.branch,
            &divergence.change_id,
            &divergence.twins,
        );
        let key = format!("{delivery_scope}:{}:ambiguous", divergence.branch);
        if queue_system_direct_once_confirmed(
            orch,
            &run_id,
            &message,
            DeliveryUrgency::Interrupt,
            &key,
        )? == DirectQueueDisposition::Undeliverable
        {
            continue;
        }
        notified.push(divergence.branch.clone());
        log::info!(
            "Interrupted jj sibling job {} for an ambiguous divergent change on {}",
            sibling.id,
            divergence.branch
        );
    }
    Ok(notified)
}

/// Notify every sibling whose auto-rebase landed cleanly that its branch moved.
/// Unlike the conflict path this needs no action from the agent: the rebase is
/// done and the cleanly-rebased PR head already advanced. `queue_system_direct`
/// enqueues a `Passive` delivery, which never wakes an idle recipient — the note
/// rides along into the agent's next natural run so the silent rebase is on the
/// record without mechanically resuming an idle agent.
fn notify_clean_siblings(
    orch: &Orchestrator,
    db: &LocalDb,
    siblings: &[SiblingJob],
    clean: &[String],
    note: &str,
    delivery_scope: &str,
) -> Result<(), String> {
    for sibling in siblings {
        let Some(branch) = sibling_branch(sibling) else {
            continue;
        };
        if !clean.contains(&branch) {
            continue;
        }
        let Some(run_id) = latest_run_for_job(db, &sibling.id) else {
            log::debug!(
                "jj reconcile: no run for cleanly-rebased sibling {} to notify",
                sibling.id
            );
            continue;
        };
        let key = clean_note_key(delivery_scope, &branch);
        queue_system_direct_once(orch, &run_id, note, DeliveryUrgency::Passive, &key)?;
        log::info!(
            "Passively notified jj sibling job {} of a clean base-advance rebase",
            sibling.id
        );
    }
    Ok(())
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
                issue_id: row.opt_text(2)?,
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
                        issue_id: row.opt_text(2)?,
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
                        issue_id: row.opt_text(2)?,
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
/// branch. The jobs re-pointed here land in the same pass's sibling set, so they
/// inherit this advance rather than being stranded by it.
async fn repoint_children_of_merged_branch(
    db: &LocalDb,
    project_id: &str,
    merged_branch: &str,
    new_base: &str,
    store: &Path,
    destination: &str,
) -> Result<(), String> {
    let project = project_id.to_string();
    let merged = merged_branch.to_string();
    let base = new_base.to_string();
    let path = store.to_string_lossy().into_owned();
    let destination = destination.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(move |conn| {
        let (project, merged, base, path, destination) = (project.clone(), merged.clone(), base.clone(), path.clone(), destination.clone());
        Box::pin(async move {
            let mut rows = conn.query(&format!("SELECT DISTINCT j.branch FROM jobs j WHERE j.project_id=?1 AND j.base_branch=?2 AND j.branch IS NOT NULL AND {LIVE_JOB_PREDICATE}"), params![project.as_str(),merged.as_str()]).await?;
            let mut branches = Vec::new();
            while let Some(row) = rows.next().await? { branches.push(row.text(0)?); }
            drop(rows);
            for branch in branches {
                let mut rows = conn.query(
                    "SELECT n.id,n.destination_commit,n.target_branch,i.held_replay_source_cut,i.held_replay_source_tip
                     FROM jj_reconcile_items i JOIN jj_reconcile_intents n ON n.id=i.intent_id
                     WHERE n.project_id=?1 AND n.store_path=?2 AND i.bookmark=?3 AND i.held_replay_source_tip IS NOT NULL",
                    params![project.as_str(),path.as_str(),branch.as_str()]).await?;
                let mut receipts = Vec::new();
                while let Some(row) = rows.next().await? {
                    receipts.push(HeldReplaySource { intent_id:row.text(0)?,destination:row.text(1)?,base_branch:row.text(2)?,cut:row.opt_text(3)?,tip:row.text(4)? });
                }
                drop(rows);
                if receipts.is_empty() { continue; }
                if receipts.len()!=1 || receipts[0].base_branch!=merged {
                    return Err(DbError::Row(format!("retarget of `{branch}` refuses competing held authority")));
                }
                let expected = &receipts[0];
                let candidate = uuid::Uuid::new_v4().to_string();
                conn.execute("INSERT INTO jj_reconcile_intents (id,project_id,store_path,target_branch,destination_commit,trigger_sources_json,status,created_at,updated_at)
                    VALUES (?1,?2,?3,?4,?5,'[\"held_retarget\"]','pending',?6,?6)
                    ON CONFLICT(project_id,store_path,target_branch,destination_commit) DO NOTHING",
                    params![candidate,project.as_str(),path.as_str(),base.as_str(),destination.as_str(),now]).await?;
                let mut rows = conn.query("SELECT id FROM jj_reconcile_intents WHERE project_id=?1 AND store_path=?2 AND target_branch=?3 AND destination_commit=?4",
                    params![project.as_str(),path.as_str(),base.as_str(),destination.as_str()]).await?;
                let intent = rows.next().await?.ok_or_else(|| DbError::Row("retarget intent missing".into()))?.text(0)?;
                drop(rows);
                let mut rows = conn.query("SELECT 1 FROM jj_reconcile_items WHERE intent_id=?1 AND bookmark=?2 AND (held_replay_source_tip IS NOT NULL OR resolution_state='open' OR (replay_request_id IS NOT NULL AND replay_served_at IS NULL))",
                    params![intent.as_str(),branch.as_str()]).await?;
                if rows.next().await?.is_some() { return Err(DbError::Row(format!("retarget of `{branch}` refuses newer destination authority"))); }
                drop(rows);
                if consume_held_replay_source_conn(conn,Path::new(&path),&branch,expected).await? != 1 {
                    return Err(DbError::Row("retarget held-source generation changed".into()));
                }
                // Only the obligation moves. The old open session and request
                // retain their real coordinates and per-path resolution basis.
                conn.execute("INSERT INTO jj_reconcile_items (intent_id,bookmark,status,observed_tip,outcome_kind,updated_at,held_replay_source_cut,held_replay_source_tip)
                    VALUES (?1,?2,'pending',?4,'held_retarget',?5,?3,?4)
                    ON CONFLICT(intent_id,bookmark) DO UPDATE SET status='pending',outcome_kind='held_retarget',observed_tip=excluded.observed_tip,updated_at=excluded.updated_at,held_replay_source_cut=excluded.held_replay_source_cut,held_replay_source_tip=excluded.held_replay_source_tip",
                    params![intent.as_str(),branch.as_str(),expected.cut.as_deref(),expected.tip.as_str(),now]).await?;
                // A running lease keeps its owner. Its finisher observes the
                // pending follow-up; a completed destination becomes claimable.
                conn.execute("UPDATE jj_reconcile_intents SET status='pending',updated_at=?2 WHERE id=?1 AND status IN ('completed','superseded')",
                    params![intent.as_str(),now]).await?;
            }
            conn.execute(&format!("UPDATE jobs SET base_branch=?3,updated_at=?4 WHERE id IN (SELECT j.id FROM jobs j WHERE j.project_id=?1 AND j.base_branch=?2 AND {LIVE_JOB_PREDICATE})"),
                params![project.as_str(),merged.as_str(),base.as_str(),now]).await?;
            conn.execute("UPDATE merge_requests SET target_branch=?3,updated_at=?4 WHERE project_id=?1 AND target_branch=?2 AND status NOT IN ('merged','closed')",
                params![project.as_str(),merged.as_str(),base.as_str(),now]).await?;
            Ok(())
        })
    }).await.map_err(|error| format!("retarget children of `{merged_branch}`: {error}"))
}

/// The live jobs cut from base `?2` in project `?1`, other than job `?3`.
///
/// Runs on every base advance, so its plan is pinned by
/// `sibling_job_statement_seeks_both_lookups`: both the jobs selection and the
/// open-PR probe inside [`LIVE_JOB_PREDICATE`] must seek (migration 0267). The
/// probe scanning a project's merge requests once per settled job cost ~60 s
/// per call on production data.
fn sibling_jobs_sql() -> String {
    format!(
        "SELECT j.id, j.branch, j.base_commit
           FROM jobs j
          WHERE j.project_id = ?1
            AND j.base_branch = ?2
            AND j.id != ?3
            AND {LIVE_JOB_PREDICATE}"
    )
}

async fn load_jobs_on_branch(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<SiblingJob>, String> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    let sql = format!(
        "SELECT j.id, j.branch, j.base_commit FROM jobs j
         WHERE j.project_id = ?1 AND j.branch = ?2 AND {LIVE_JOB_PREDICATE}"
    );
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(&sql, params![project_id.as_str(), branch.as_str()])
                .await?;
            let mut jobs = Vec::new();
            while let Some(row) = rows.next().await? {
                jobs.push(SiblingJob {
                    id: row.text(0)?,
                    branch: row.opt_text(1)?,
                    base_commit: row.opt_text(2)?,
                });
            }
            Ok(jobs)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_sibling_jobs(
    db: &LocalDb,
    project_id: &str,
    base_branch: &str,
    merged_job_id: &str,
) -> Result<Vec<SiblingJob>, String> {
    let project_id = project_id.to_string();
    let base_branch = base_branch.to_string();
    let merged_job_id = merged_job_id.to_string();
    let sql = sibling_jobs_sql();
    db.read(|conn| {
        let project_id = project_id.clone();
        let base_branch = base_branch.clone();
        let merged_job_id = merged_job_id.clone();
        let sql = sql.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &sql,
                    params![
                        project_id.as_str(),
                        base_branch.as_str(),
                        merged_job_id.as_str()
                    ],
                )
                .await?;
            let mut siblings = Vec::new();
            while let Some(row) = rows.next().await? {
                siblings.push(SiblingJob {
                    id: row.text(0)?,
                    branch: row.opt_text(1)?,
                    base_commit: row.opt_text(2)?,
                });
            }
            Ok(siblings)
        })
    })
    .await
    .map_err(|error| error.to_string())
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

/// Active jobs whose durable branch is the branch that just advanced.
async fn load_on_branch_jobs(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<SiblingJob>, String> {
    let project_id = project_id.to_string();
    let branch = branch.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        let branch = branch.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.id, j.branch, j.base_commit
                         FROM jobs j
                         WHERE j.project_id = ?1
                           AND j.branch = ?2
                           AND NOT EXISTS (
                             SELECT 1 FROM issues i
                             WHERE i.id = j.issue_id AND i.status IN ('merged', 'closed')
                           )
                           AND ( j.status NOT IN ('complete', 'failed', 'cancelled')
                                 OR EXISTS (
                                   SELECT 1 FROM merge_requests mr
                                   WHERE mr.source_branch = j.branch
                                     AND mr.project_id = j.project_id
                                     AND mr.status NOT IN ('merged', 'closed')
                                 ) )",
                    params![project_id.as_str(), branch.as_str()],
                )
                .await?;
            let mut jobs = Vec::new();
            while let Some(row) = rows.next().await? {
                jobs.push(SiblingJob {
                    id: row.text(0)?,
                    branch: row.opt_text(1)?,
                    base_commit: row.opt_text(2)?,
                });
            }
            Ok(jobs)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_merge_request_info(
    db: &LocalDb,
    owner_id: &str,
    implementation_job_id: &str,
) -> Result<Option<MergeRequestInfo>, String> {
    let owner_id = owner_id.to_string();
    let implementation_job_id = implementation_job_id.to_string();
    db.read(|conn| {
        let owner_id = owner_id.clone();
        let implementation_job_id = implementation_job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT github_pr_number
                         FROM merge_requests
                         WHERE job_id = ?1 OR job_id = ?2
                         ORDER BY CASE WHEN job_id = ?1 THEN 0 ELSE 1 END
                         LIMIT 1",
                    params![owner_id.as_str(), implementation_job_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(MergeRequestInfo {
                        pr_number: row.opt_i64(0)?,
                    })
                })
                .transpose()
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
                    "SELECT default_branch FROM projects WHERE id = ?1",
                    params![project_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.get::<Option<String>>(0)?.filter(|s| !s.is_empty())),
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

async fn load_issue_info(db: &LocalDb, issue_id: &str) -> Result<Option<IssueInfo>, String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.key, i.number
                         FROM issues i
                         JOIN projects p ON p.id = i.project_id
                         WHERE i.id = ?1",
                    params![issue_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    Ok(IssueInfo {
                        project_key: row.text(0)?,
                        number: row.i64(1)?,
                    })
                })
                .transpose()
        })
    })
    .await
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The store-truth sibling filter drops branches whose bookmark is gone (the
    /// stale-`jobs`-row case) and counts them, but a `None` bookmark set (the
    /// store-wide list failed) disables the filter and proceeds with all.
    #[test]
    fn retain_present_siblings_drops_missing_and_honors_none() {
        let live = "agent/cairn-1-builder-0".to_string();
        let ghost = "agent/cairn-1-ghost-0".to_string();
        let existing: std::collections::HashSet<String> = [live.clone()].into_iter().collect();

        let (retained, dropped) =
            retain_present_siblings(vec![live.clone(), ghost.clone()], Some(&existing));
        assert_eq!(
            retained,
            vec![live.clone()],
            "the missing-bookmark sibling is dropped before any per-sibling jj work"
        );
        assert_eq!(dropped, 1);

        // A failed store list (None) disables the filter: proceed with all.
        let (all, none_dropped) = retain_present_siblings(vec![live.clone(), ghost.clone()], None);
        assert_eq!(all, vec![live, ghost]);
        assert_eq!(none_dropped, 0);
    }
    use crate::db::DbState;
    use crate::services::testing::{MockGitClient, TestServicesBuilder};
    use crate::storage::{LocalDb, SearchIndex};
    use std::path::PathBuf;
    use std::sync::Arc;

    #[tokio::test]
    async fn base_advance_defers_while_a_sibling_run_batch_is_in_flight() {
        let db = Arc::new(migrated_db().await);
        seed_base_advance_fixture(&db).await;
        for sql in [
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-batch','job-overlap','open','backend',1,1)",
            "UPDATE runs SET session_id='session-batch' WHERE id='run-job-overlap'",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-batch','session-batch','run-job-overlap','job-overlap',1,'yielded','initial',1,1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
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
        let sibling = SiblingJob {
            id: "job-overlap".into(),
            branch: Some("agent/overlap".into()),
            base_commit: None,
        };

        // Reconcile wins the store lock first and reaches its final mutation
        // boundary. A batch that starts between the initial claim query and this
        // point cannot insert its bracket row underneath that mutation.
        let store = PathBuf::from("/store");
        let mutation_guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                "test reconcile mutation",
            )
            .await;
        let admission_orch = orch.clone();
        let admission_db = db.clone();
        let admission_store = store.clone();
        let mut admission = tokio::spawn(async move {
            let _guard = admission_orch
                .acquire_jj_store_lock(
                    &admission_store,
                    crate::orchestrator::StoreLockClass::Background,
                    "test run batch admission",
                )
                .await;
            admission_db
                .execute(
                    "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-batch','job-overlap','run-job-overlap','session-batch','turn-batch','tool-batch','{\"kind\":\"run_batch\",\"request_id\":\"request-1\",\"commits\":false,\"label\":\"focused test\"}','pending',1)",
                    (),
                )
                .await
                .unwrap();
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut admission)
                .await
                .is_err(),
            "run-batch admission must wait while bookmark mutation owns the store lock"
        );
        assert!(inflight_run_batch(&db, std::slice::from_ref(&sibling))
            .await
            .unwrap()
            .is_none());

        drop(mutation_guard);
        admission.await.unwrap();
        let _revalidation_guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Background,
                "test reconcile revalidation",
            )
            .await;
        assert!(inflight_run_batch(&db, &[sibling]).await.unwrap().is_some());
    }

    /// The sweep is the only mechanism that revisits a deferred base advance, so
    /// a pass that panics must cost one cycle and not the loop. Before this was
    /// supervised, a single panic retired the sweep for the lifetime of the
    /// process: work queued for retry was never claimed again, and the branches
    /// waiting on it sat silently behind their base.
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

    /// A branch deferred at the mutation boundary owes a rebase nobody has
    /// attempted. That is neither "done" nor "never advanced", and the resume
    /// and delivery passes must both leave it alone so the next sweep picks it
    /// up rather than treating it as serviced.
    #[tokio::test]
    async fn a_deferred_sibling_stays_queued_rather_than_reading_as_serviced() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let claim =
            claim_reconcile_intent(&db, "proj-1", Path::new("/store"), "main", "dest-a", "test")
                .await
                .unwrap()
                .unwrap();

        persist_reconcile_item(
            &db,
            ReconcileItemUpdate {
                intent_id: &claim.id,
                bookmark: "agent/overlap",
                observed_tip: Some("tip-a"),
                status: "pending",
                failure_kind: None,
                outcome_kind: Some("deferred"),
                fingerprint: None,
                diagnostic: Some("deferred at the mutation boundary"),
            },
        )
        .await
        .unwrap();

        let progress = reconcile_item_status(&db, &claim.id, "agent/overlap")
            .await
            .unwrap()
            .expect("the deferral is recorded rather than left invisible");
        assert_eq!(progress.status, "pending");
        assert_eq!(progress.outcome_kind.as_deref(), Some("deferred"));
        assert!(
            !progress.notification_sent,
            "nothing was delivered, because nothing was attempted"
        );

        // Delivery only closes out graph movement and suppression. A deferral is
        // neither, so it must survive untouched.
        mark_reconcile_delivered(&db, &claim.id).await.unwrap();
        let after = reconcile_item_status(&db, &claim.id, "agent/overlap")
            .await
            .unwrap()
            .expect("the deferral survives the delivery pass");
        assert_eq!(after.status, "pending");
        assert!(!after.notification_sent);
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

    /// A job with no base branch has nothing to be replayed onto, and that is a
    /// different answer from "no session" — it must not fall through to a
    /// destination resolved from an empty string.
    #[tokio::test]
    async fn a_sessionless_replay_needs_a_base_branch_to_land_on() {
        let db = Arc::new(migrated_db().await);
        seed_base_advance_fixture(&db).await;
        db.execute(
            "UPDATE jobs SET base_branch = NULL WHERE id = 'job-clean'",
            (),
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

        let error =
            request_branch_replay(&orch, &db, "job-clean", "agent/clean", None, false, None)
                .await
                .expect_err("a branch with no base has no destination");
        assert!(
            error.contains("no base branch recorded"),
            "the refusal names the missing base rather than a resolution failure: {error}"
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

        repoint_children_of_merged_branch(
            &db,
            "proj-1",
            "agent/merged",
            "integration",
            Path::new("/store"),
            "integration-tip",
        )
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

        let siblings = load_sibling_jobs(&db, "proj-1", "integration", "job-merged")
            .await
            .unwrap();
        assert!(
            siblings.iter().any(|sibling| sibling.id == "job-child"),
            "the re-pointed child joins this same advance rather than waiting for the next one"
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

    /// cairn/4810 end to end: a PR branch left with an UNTRACKED origin copy of
    /// its own history (a first push forgotten by a whole-store rollback, then
    /// re-imported by a fetch) used to fail every publication with "Non-tracking
    /// remote bookmark". The managed publish recovers once, and afterwards the
    /// branch keeps publishing: commit, publish, origin advances.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn publish_managed_branch_heals_an_untracked_origin_copy_and_keeps_publishing() {
        use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
        let Some(bin) = jj_bin() else {
            eprintln!("skipping publish_managed_branch_heals_an_untracked_origin_copy: no jj");
            return;
        };
        let home = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        let origin = tempfile::tempdir().unwrap();
        let workspaces = tempfile::tempdir().unwrap();
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(project.path());
        git(
            project.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(project.path(), &["push", "-q", "origin", "main"]);

        let root = home.path().to_path_buf();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                Arc::new(migrated_db().await),
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

        let branch = "agent/cairn-4804-builder-5";
        let workspace = workspaces.path().join("builder");
        crate::jj::add_workspace(&jj, &store, &workspace, branch, "main", None).unwrap();
        let commit = |file: &str| {
            std::fs::write(workspace.join(file), format!("{file}\n")).unwrap();
            crate::jj::seal(&jj, &workspace, &format!("edit {file}"), None).unwrap();
            let tip = crate::jj::bookmark_commit(&jj, &store, branch).unwrap();
            crate::jj::export_git_verified(&jj, &store, true, "test commit", &[(branch, &tip)])
                .unwrap();
            tip
        };
        let origin_tip = || {
            git_stdout(
                origin.path(),
                &["rev-parse", &format!("refs/heads/{branch}")],
            )
        };

        // The incident: the PR's first push lands while a lock holder's snapshot
        // is outstanding, the old whole-store rollback forgets it, and a later
        // fetch re-imports origin's copy untracked.
        commit("first.rs");
        let snapshot = crate::jj::operation_id(&jj, &store).unwrap();
        crate::jj::push_store_bookmark(&jj, &store, branch).unwrap();
        jj.run(
            &store,
            &["op", "restore", &snapshot, "--ignore-working-copy"],
            "pre-fix whole-store rollback",
        )
        .unwrap();
        crate::jj::fetch_remote_branch_via_git(&store, "origin", branch).unwrap();
        crate::jj::import_git(&jj, &store).unwrap();

        let second = commit("second.rs");
        publish_managed_branch(&orch, &store, branch)
            .await
            .expect("an untracked copy of the branch's own history is healed, not fatal");
        assert_eq!(origin_tip(), second);

        let third = commit("third.rs");
        publish_managed_branch(&orch, &store, branch)
            .await
            .expect("once healed, the branch publishes like any other");
        assert_eq!(origin_tip(), third, "a later commit advances origin");
    }

    #[tokio::test]
    async fn reconcile_sweep_bounds_requeued_work_to_one_claim_per_pass() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let claim =
            claim_reconcile_intent(&db, "proj-1", Path::new("/store"), "main", "dest-a", "test")
                .await
                .unwrap()
                .unwrap();
        let mut claimed = HashSet::new();

        assert!(first_claim_this_sweep(&mut claimed, &claim.id));
        release_reconcile_claim(&db, &claim).await;
        let retried = claim_next_reconcile_intent(&db)
            .await
            .unwrap()
            .expect("released work remains retryable");
        assert_eq!(retried.claim.id, claim.id);
        assert_ne!(retried.claim.owner, claim.owner);
        assert!(
            !first_claim_this_sweep(&mut claimed, &retried.claim.id),
            "the retry is deferred to the next sweep instead of spinning in this one"
        );
        release_reconcile_claim(&db, &retried.claim).await;
        assert!(
            claim_next_reconcile_intent(&db).await.unwrap().is_some(),
            "bounding the pass must not consume the retry"
        );
    }

    #[tokio::test]
    async fn durable_retry_supersedes_a_fossilized_pin_and_remints_current_destination() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let stale = claim_reconcile_intent(
            &db,
            "proj-1",
            store,
            "main",
            "destination-a",
            "pre-sweep trigger",
        )
        .await
        .unwrap()
        .unwrap();

        let current = refresh_stale_durable_intent(
            &db,
            store,
            "main",
            "destination-a",
            "destination-b",
            "durable destination refresh",
            stale,
        )
        .await
        .unwrap()
        .expect("the stale intent is reminted against the live destination");

        let stale_state: (String, String) = db
            .query_one(
                "SELECT status, last_diagnostic FROM jj_reconcile_intents
                 WHERE destination_commit = 'destination-a'",
                (),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        assert_eq!(stale_state.0, "superseded");
        assert!(stale_state.1.contains("destination-b"));
        assert_eq!(
            db.query_text(
                "SELECT destination_commit FROM jj_reconcile_intents WHERE id = ?1",
                (current.id.clone(),),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("destination-b")
        );

        release_reminted_claim_after_failure(&db, "fossil-intent", &current).await;
        assert_eq!(
            db.query_text(
                "SELECT status FROM jj_reconcile_intents WHERE id = ?1",
                (current.id.clone(),),
            )
            .await
            .unwrap()
            .as_deref(),
            Some("pending"),
            "a failed execution releases the reminted destination immediately"
        );
        let retried = claim_next_reconcile_intent(&db)
            .await
            .unwrap()
            .expect("the replacement is retryable without waiting for lease expiry");
        finish_reconcile_intent(&db, &retried.claim.id, &retried.claim.owner, false)
            .await
            .unwrap();
        assert!(
            claim_next_reconcile_intent(&db).await.unwrap().is_none(),
            "one sweep can leave both the fossil and its replacement terminal"
        );
    }

    #[tokio::test]
    async fn run_batch_deferral_is_scoped_to_the_owning_branch() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        for sql in [
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-local','job-overlap','open','backend',1,1)",
            "UPDATE runs SET session_id='session-local' WHERE id='run-job-overlap'",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-local','session-local','run-job-overlap','job-overlap',1,'yielded','initial',1,1)",
            "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-local','job-overlap','run-job-overlap','session-local','turn-local','tool-local','{\"kind\":\"run_batch\",\"request_id\":\"request-local\",\"commits\":false,\"label\":\"branch-local test\"}','pending',1)",
        ] {
            db.execute(sql, ()).await.unwrap();
        }
        let siblings = vec![
            SiblingJob {
                id: "job-overlap".into(),
                branch: Some("agent/overlap".into()),
                base_commit: None,
            },
            SiblingJob {
                id: "job-clean".into(),
                branch: Some("agent/clean".into()),
                base_commit: None,
            },
        ];
        let blocked = siblings_for_branch(&siblings, "agent/overlap");
        let clear = siblings_for_branch(&siblings, "agent/clean");

        assert_eq!(
            blocked.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
            vec!["job-overlap"]
        );
        assert_eq!(
            clear.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
            vec!["job-clean"]
        );
        assert_eq!(
            inflight_run_batch(&db, &blocked).await.unwrap(),
            Some(InflightRunBatch {
                job_id: "job-overlap".into(),
                holder: None,
                request_id: "request-local".into(),
                label: Some("branch-local test".into()),
                parked_at_ms: 1,
            }),
            "the hold names the batch, which is what ends it"
        );
        assert!(
            inflight_run_batch(&db, &clear).await.unwrap().is_none(),
            "a run batch on one branch must not defer its clear sibling"
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

        // The parent's replay pass guards only its own batches (cairn/5098),
        // and its answer says so rather than promising a deferral.
        let answer = replay_queued_summary(
            "agent/overlap",
            "integration",
            "dest",
            None,
            "job-overlap",
            &hold,
        );
        assert!(
            answer.contains("Another node's run batch `request-child`")
                && answer.contains("cairn/5098")
                && !answer.contains("deferred, not replayed"),
            "{answer}"
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
        assert_eq!(
            count_waits(&db).await,
            0,
            "no wait row exists for either batch"
        );

        let hold = inflight_run_batch_on_branch(&db, "job-overlap", "agent/overlap").await;
        assert_eq!(
            hold.clone().unwrap(),
            Some(InflightRunBatch {
                job_id: "job-overlap".into(),
                holder: hold.clone().unwrap().unwrap().holder,
                request_id: "req-detached".into(),
                label: Some("cargo test".into()),
                parked_at_ms: 7,
            })
        );
        assert!(mutation_boundary_hold(&hold).contains("`req-detached`"));
        assert!(
            inflight_run_batch_for_job(&db, "job-overlap")
                .await
                .unwrap()
                .is_some(),
            "the automatic reconcile's guard sees it too"
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

    async fn count_waits(db: &LocalDb) -> i64 {
        db.query_one("SELECT COUNT(*) FROM agent_waits", (), |row| row.i64(0))
            .await
            .unwrap()
    }

    fn held() -> Result<Option<InflightRunBatch>, String> {
        Ok(Some(InflightRunBatch {
            job_id: "job-builder".into(),
            holder: Some("cairn://p/cairn/5069/1/builder".into()),
            request_id: "ba0e0085".into(),
            label: Some("rust-tests on main".into()),
            parked_at_ms: 1_760_788_078_000,
        }))
    }

    /// The 5069 specimen: a replay request that folded into a running intent
    /// was answered "Queued a store-side replay" while the branch's own run
    /// batch guaranteed the pass would defer. The answer names the intent it
    /// joined, the batch that holds the branch, and what releases it.
    #[test]
    fn a_coalesced_replay_names_the_intent_and_the_batch_that_defers_it() {
        let coalesced = (Some("intent-542c".to_string()), "279ddbcf".to_string());
        let summary = replay_queued_summary(
            "agent/b",
            "main",
            "279ddbcf",
            Some(&coalesced),
            "job-builder",
            &held(),
        );
        assert!(
            summary.contains("Coalesced into reconcile intent `intent-542c`")
                && summary.contains("`main` at `279ddbcf`"),
            "{summary}"
        );
        assert!(!summary.contains("Queued a store-side replay"), "{summary}");
        assert!(
            summary.contains("deferred, not replayed")
                && summary.contains("run batch `ba0e0085` (\"rust-tests on main\")")
                && summary.contains("on cairn://p/cairn/5069/1/builder")
                && summary.contains("parked since"),
            "{summary}"
        );
        assert!(
            summary.contains("finishes or is stopped") && summary.contains("durable sweep"),
            "says what releases it: {summary}"
        );

        let fresh = replay_queued_summary(
            "agent/b",
            "main",
            "279ddbcf",
            None,
            "job-builder",
            &Ok(None),
        );
        assert!(fresh.starts_with("Queued a store-side replay"), "{fresh}");
        assert!(
            !fresh.contains("deferred") && !fresh.contains("unknown"),
            "nothing holds it, so nothing is said about a hold: {fresh}"
        );

        let unread = replay_queued_summary(
            "agent/b",
            "main",
            "279ddbcf",
            None,
            "job-builder",
            &Err("database is locked".into()),
        );
        assert!(
            unread.contains("unknown") && unread.contains("database is locked"),
            "an unreadable hold says unknown rather than nothing: {unread}"
        );
    }

    /// The diagnostic exists only inside the rebase, between the recorded
    /// conflict and the rollback. If it does not survive that window in storage,
    /// nothing downstream can ever show it: the branch is clean again and a later
    /// probe finds nothing. So the round-trip is the whole feature.
    #[tokio::test]
    async fn a_conflict_diagnostic_outlives_the_rollback_that_erased_it() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let claim = claim_reconcile_intent(&db, "proj-1", store, "main", "dest-a", "local_merge")
            .await
            .unwrap()
            .unwrap();
        let branch = "agent/test";
        persist_reconcile_item(
            &db,
            ReconcileItemUpdate {
                intent_id: &claim.id,
                bookmark: branch,
                observed_tip: Some("tip"),
                status: "graph_moved",
                failure_kind: None,
                outcome_kind: Some("conflicted"),
                fingerprint: None,
                diagnostic: None,
            },
        )
        .await
        .unwrap();

        let diagnostic = crate::jj::ConflictDiagnostic {
            validation: None,
            base: Some("b".repeat(40)),
            ours: Some("o".repeat(40)),
            theirs: Some("t".repeat(40)),
            conflicted_tip: Some("c".repeat(40)),
            condition: crate::jj::ConflictCondition::ContentConflict,
            incoming: vec![
                crate::jj::IncomingFile {
                    path: "shared.rs".to_string(),
                    status: "M".to_string(),
                    classification: crate::jj::IncomingClassification::Conflicting,
                },
                crate::jj::IncomingFile {
                    path: "storage.rs".to_string(),
                    status: "D".to_string(),
                    classification: crate::jj::IncomingClassification::CleanOnRetry,
                },
            ],
            scaffolding: None,
        };
        let incoming = IncomingIdentity {
            base_branch: "main".to_string(),
            pr_number: Some(2893),
            issue: Some("cairn-3337".to_string()),
        };
        record_conflict_session(
            &db,
            &claim.id,
            branch,
            &diagnostic,
            &incoming,
            &Default::default(),
        )
        .await
        .unwrap();

        let session = crate::orchestrator::conflict_session::load_active_session(&db, branch)
            .await
            .unwrap()
            .expect("the session survives the pass that recorded it");

        assert_eq!(session.base.as_deref(), Some("b".repeat(40).as_str()));
        assert_eq!(session.theirs.as_deref(), Some("t".repeat(40).as_str()));
        assert_eq!(
            session.fingerprint(),
            diagnostic.fingerprint(),
            "the stored fingerprint is the one the wake deduplicated on"
        );
        assert_eq!(session.incoming.pr_number, Some(2893));
        assert_eq!(session.incoming.issue.as_deref(), Some("cairn-3337"));

        // The cross-file trap: a report naming only the conflicting path lets a
        // coordinated change land half-applied, so the clean-on-retry sibling
        // must be stored too, distinguishably.
        let conflicting: Vec<&str> = session.conflicting().map(|f| f.path.as_str()).collect();
        let clean: Vec<&str> = session.clean_on_retry().map(|f| f.path.as_str()).collect();
        assert_eq!(conflicting, vec!["shared.rs"]);
        assert_eq!(clean, vec!["storage.rs"]);
        assert_eq!(
            session
                .files
                .iter()
                .find(|f| f.path == "storage.rs")
                .unwrap()
                .status,
            "D",
            "an add/delete/rename is distinguishable, not flattened to 'changed'"
        );

        // Markers start unclaimed. Nothing may say otherwise until an executor does.
        assert_eq!(session.marker_state, MarkerState::NotMaterialized);
    }

    /// A base that advances twice leaves the first session describing a merge
    /// nobody will perform again. Exactly one session is active, and closing on a
    /// clean rebase is what ends it.
    #[tokio::test]
    async fn a_newer_session_supersedes_the_old_and_a_clean_rebase_closes_it() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let branch = "agent/test";

        let mut fingerprints = Vec::new();
        for dest in ["dest-a", "dest-b"] {
            let claim = claim_reconcile_intent(&db, "proj-1", store, "main", dest, "local_merge")
                .await
                .unwrap()
                .unwrap();
            persist_reconcile_item(
                &db,
                ReconcileItemUpdate {
                    intent_id: &claim.id,
                    bookmark: branch,
                    observed_tip: Some("tip"),
                    status: "graph_moved",
                    failure_kind: None,
                    outcome_kind: Some("conflicted"),
                    fingerprint: None,
                    diagnostic: None,
                },
            )
            .await
            .unwrap();
            let diagnostic = crate::jj::ConflictDiagnostic {
                validation: None,
                base: Some("b".repeat(40)),
                ours: Some("o".repeat(40)),
                theirs: Some(dest.to_string()),
                conflicted_tip: None,
                condition: crate::jj::ConflictCondition::ContentConflict,
                incoming: Vec::new(),
                scaffolding: None,
            };
            fingerprints.push(diagnostic.fingerprint());
            record_conflict_session(
                &db,
                &claim.id,
                branch,
                &diagnostic,
                &IncomingIdentity::default(),
                &Default::default(),
            )
            .await
            .unwrap();
            supersede_stale_sessions(&db, branch, &claim.id)
                .await
                .unwrap();
            // Mark it claimed so the next destination is not coalesced away.
            db.execute(
                "UPDATE jj_reconcile_intents SET status = 'completed' WHERE id = ?1",
                (claim.id.as_str(),),
            )
            .await
            .unwrap();
        }

        let session = crate::orchestrator::conflict_session::load_active_session(&db, branch)
            .await
            .unwrap()
            .expect("one session is active");
        assert_eq!(
            session.fingerprint(),
            fingerprints[1],
            "the active session describes the CURRENT merge, not the superseded one"
        );

        close_open_sessions_for_branch(&db, branch).await.unwrap();
        assert!(
            crate::orchestrator::conflict_session::load_active_session(&db, branch)
                .await
                .unwrap()
                .is_none(),
            "absorbing the base closes the session"
        );
    }

    fn list_conflict(ours: &str, theirs: &str) -> crate::jj::ConflictDiagnostic {
        crate::jj::ConflictDiagnostic {
            validation: None,
            base: Some("fork".to_string()),
            ours: Some(ours.to_string()),
            theirs: Some(theirs.to_string()),
            conflicted_tip: None,
            condition: crate::jj::ConflictCondition::ContentConflict,
            incoming: vec![crate::jj::IncomingFile {
                path: "list.txt".to_string(),
                status: "M".to_string(),
                classification: crate::jj::IncomingClassification::Conflicting,
            }],
            scaffolding: None,
        }
    }

    /// Record a conflict the way the worker does, inheriting from the session
    /// open before it. `changed` stands in for the store's answer to which
    /// conflicting paths changed since that session's `ours`.
    async fn record_on_intent(
        db: &LocalDb,
        intent_id: &str,
        ours: &str,
        theirs: &str,
        changed: &[&str],
    ) {
        let previous = crate::orchestrator::conflict_session::load_active_session(db, "agent/test")
            .await
            .unwrap();
        let basis = previous
            .map(|previous| {
                previous.basis_after(
                    &changed.iter().map(|path| path.to_string()).collect(),
                    &std::collections::BTreeSet::new(),
                )
            })
            .unwrap_or_default();
        persist_reconcile_item(
            db,
            ReconcileItemUpdate {
                intent_id,
                bookmark: "agent/test",
                observed_tip: Some(ours),
                status: "graph_moved",
                failure_kind: None,
                outcome_kind: Some("conflicted"),
                fingerprint: None,
                diagnostic: None,
            },
        )
        .await
        .unwrap();
        record_conflict_session(
            db,
            intent_id,
            "agent/test",
            &list_conflict(ours, theirs),
            &IncomingIdentity::default(),
            &basis,
        )
        .await
        .unwrap();
        supersede_stale_sessions(db, "agent/test", intent_id)
            .await
            .unwrap();
    }

    /// The resolution basis survives every way a session is re-recorded: the
    /// agent committing during a session (including the same-intent re-detect,
    /// whose previous session is the row being overwritten), and a later
    /// advance recording over the unmoved resolution.
    #[tokio::test]
    async fn a_recorded_session_inherits_the_basis_its_tip_was_resolved_against() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let load = || async {
            crate::orchestrator::conflict_session::load_active_session(&db, "agent/test")
                .await
                .unwrap()
                .expect("an open session")
        };

        let d1 = claim_reconcile_intent(&db, "proj-1", store, "main", "d1", "local_merge")
            .await
            .unwrap()
            .unwrap();
        let resolved_against =
            |session: &crate::orchestrator::conflict_session::ConflictSession| {
                session
                    .resolution_basis
                    .get("list.txt")
                    .map(|side| (side.base.clone(), side.theirs.clone()))
            };
        record_on_intent(&db, &d1.id, "c1", "d1", &[]).await;
        let first = load().await;
        assert_eq!(resolved_against(&first), None, "nothing resolved yet");

        // Re-detected under the same intent, now with the resolution as `ours`.
        record_on_intent(&db, &d1.id, "resolved", "d1", &["list.txt"]).await;
        let redetected = load().await;
        assert_eq!(redetected.ours.as_deref(), Some("resolved"));
        assert_eq!(
            resolved_against(&redetected),
            Some(("fork".to_string(), "d1".to_string()))
        );
        db.execute(
            "UPDATE jj_reconcile_intents SET status = 'completed' WHERE id = ?1",
            (d1.id.as_str(),),
        )
        .await
        .unwrap();

        // A newer advance records over the unmoved resolution.
        let d2 = claim_reconcile_intent(&db, "proj-1", store, "main", "d2", "local_merge")
            .await
            .unwrap()
            .unwrap();
        record_on_intent(&db, &d2.id, "resolved", "d2", &[]).await;
        let newer = load().await;
        assert_eq!(newer.theirs.as_deref(), Some("d2"));
        assert_eq!(
            resolved_against(&newer),
            Some(("fork".to_string(), "d1".to_string())),
            "still resolved against D1, never its own D2"
        );
        assert!(newer.replay_requested_tip.is_none());
    }

    #[tokio::test]
    async fn replay_waits_for_resolved_intent_then_claims_follow_on_pass() {
        let db = Arc::new(migrated_db().await);
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let first = claim_reconcile_intent(
            &db,
            "proj-1",
            store,
            "main",
            "dest-a",
            "resolved replay requested for agent/first",
        )
        .await
        .unwrap()
        .expect("the first resolved candidate list owns the intent");

        assert!(
            claim_reconcile_intent(
                &db,
                "proj-1",
                store,
                "main",
                "dest-a",
                "resolved replay requested for agent/second",
            )
            .await
            .unwrap()
            .is_none(),
            "the second request initially encounters the live frozen intent"
        );

        let waiting_db = db.clone();
        let waiter = tokio::spawn(async move {
            wait_for_reconcile_slot(
                &waiting_db,
                "proj-1",
                Path::new("/store"),
                "main",
                "dest-a",
                tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "the second request waits for the first pass"
        );

        finish_reconcile_intent(&db, &first.id, &first.owner, false)
            .await
            .unwrap();
        waiter.await.unwrap().unwrap();
        reopen_reconcile_intent(&db, "proj-1", store, "main", "dest-a", "agent/second")
            .await
            .unwrap();
        let second = claim_reconcile_intent(
            &db,
            "proj-1",
            store,
            "main",
            "dest-a",
            "resolved replay requested for agent/second",
        )
        .await
        .unwrap()
        .expect("the second request claims a follow-on pass instead of disappearing");
        assert_ne!(second.owner, first.owner);

        // If the base advances between attempts, the failed claim reports the
        // newly pinned destination. Waiting on that coordinate must not be
        // satisfied by the older pass releasing its different destination.
        let moved = claim_reconcile_intent(
            &db,
            "proj-1",
            store,
            "main",
            "dest-b",
            "resolved replay requested for agent/other",
        )
        .await
        .unwrap()
        .expect("the moved base owns a distinct intent");
        let moved_waiting_db = Arc::clone(&db);
        let moved_waiter = tokio::spawn(async move {
            wait_for_reconcile_slot(
                &moved_waiting_db,
                "proj-1",
                Path::new("/store"),
                "main",
                "dest-b",
                tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            )
            .await
        });
        tokio::task::yield_now().await;
        finish_reconcile_intent(&db, &second.id, &second.owner, false)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert!(
            !moved_waiter.is_finished(),
            "releasing the stale destination does not satisfy the moved-base waiter"
        );
        finish_reconcile_intent(&db, &moved.id, &moved.owner, false)
            .await
            .unwrap();
        moved_waiter.await.unwrap().unwrap();
        reopen_reconcile_intent(&db, "proj-1", store, "main", "dest-b", "agent/second")
            .await
            .unwrap();
        let moved_follow_on = claim_reconcile_intent(
            &db,
            "proj-1",
            store,
            "main",
            "dest-b",
            "resolved replay requested for agent/second",
        )
        .await
        .unwrap()
        .expect("the moved destination is reopened and claimed after its owner finishes");
        assert_ne!(moved_follow_on.owner, moved.owner);
    }

    #[tokio::test]
    async fn reconcile_coordinates_are_scoped_to_the_known_project() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute("INSERT INTO projects (id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
            VALUES ('proj-2','default','Second','second','/repo','main',1,1)",()).await.unwrap();
        let store = Path::new("/store");
        let first = claim_reconcile_intent(&db, "proj-1", store, "main", "dest", "first")
            .await
            .unwrap()
            .unwrap();
        let second = claim_reconcile_intent(&db, "proj-2", store, "main", "dest", "second")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first.id, second.id);
        assert_eq!(second.project_id, "proj-2");
        assert_eq!(
            reconcile_intent_id(&db, "proj-1", store, "main", "dest")
                .await
                .as_deref(),
            Some(first.id.as_str())
        );
        assert_eq!(
            reconcile_intent_id(&db, "proj-2", store, "main", "dest")
                .await
                .as_deref(),
            Some(second.id.as_str())
        );
        finish_reconcile_intent(&db, &second.id, &second.owner, false)
            .await
            .unwrap();
        wait_for_reconcile_slot(
            &db,
            "proj-2",
            store,
            "main",
            "dest",
            tokio::time::Instant::now(),
        )
        .await
        .unwrap();
        assert_eq!(
            db.query_text(
                "SELECT status FROM jj_reconcile_intents WHERE id=?1",
                (first.id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
            Some("running")
        );
        finish_reconcile_intent(&db, &first.id, &first.owner, false)
            .await
            .unwrap();
        reopen_reconcile_intent(&db, "proj-2", store, "main", "dest", "agent/second")
            .await
            .unwrap();
        assert_eq!(
            db.query_text(
                "SELECT status FROM jj_reconcile_intents WHERE id=?1",
                (first.id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
            Some("completed")
        );
        assert_eq!(
            db.query_text(
                "SELECT status FROM jj_reconcile_intents WHERE id=?1",
                (second.id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
            Some("pending")
        );
    }

    #[tokio::test]
    async fn reconcile_intents_coalesce_and_stale_claims_resume() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");

        let claim = claim_reconcile_intent(&db, "proj-1", store, "main", "dest-a", "local_merge")
            .await
            .unwrap()
            .expect("first trigger claims the pinned intent");
        assert!(
            claim_reconcile_intent(&db, "proj-1", store, "main", "dest-a", "webhook")
                .await
                .unwrap()
                .is_none(),
            "duplicate delivery coalesces while the worker owns the intent"
        );

        db.execute(
            "UPDATE jj_reconcile_intents SET lease_expires_at = 0 WHERE id = ?1",
            (claim.id.as_str(),),
        )
        .await
        .unwrap();
        let durable_work = claim_next_reconcile_intent(&db)
            .await
            .unwrap()
            .expect("the runner worker reclaims an expired lease without a duplicate trigger");
        let resumed = durable_work.claim;
        assert_eq!(resumed.id, claim.id);
        assert_ne!(resumed.owner, claim.owner);
        assert_eq!(durable_work.target_branch, "main");
        assert_eq!(durable_work.destination_commit, "dest-a");

        persist_reconcile_item(
            &db,
            ReconcileItemUpdate {
                intent_id: &resumed.id,
                bookmark: "agent/test",
                observed_tip: Some("tip"),
                status: "graph_moved",
                failure_kind: None,
                outcome_kind: Some("unchanged"),
                fingerprint: None,
                diagnostic: None,
            },
        )
        .await
        .unwrap();
        let moved = reconcile_item_status(&db, &resumed.id, "agent/test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(moved.status, "graph_moved");
        assert!(!moved.notification_sent);

        // A stale owner cannot complete a lease reclaimed by another worker.
        finish_reconcile_intent(&db, &claim.id, &claim.owner, false)
            .await
            .unwrap();
        assert_eq!(
            db.query_text(
                "SELECT status FROM jj_reconcile_intents WHERE id = ?1",
                (resumed.id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
            Some("running")
        );

        mark_reconcile_delivered(&db, &resumed.id).await.unwrap();
        let delivered = reconcile_item_status(&db, &resumed.id, "agent/test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivered.status, "completed");
        assert!(delivered.notification_sent);

        finish_reconcile_intent(&db, &resumed.id, &resumed.owner, false)
            .await
            .unwrap();
        assert!(
            claim_reconcile_intent(&db, "proj-1", store, "main", "dest-a", "webhook")
                .await
                .unwrap()
                .is_none(),
            "completed duplicate delivery remains acknowledged"
        );
        assert!(
            claim_reconcile_intent(&db, "proj-1", store, "main", "dest-b", "webhook")
                .await
                .unwrap()
                .is_some(),
            "a new pinned destination creates new work"
        );
    }

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("base-advance-test.db").await
    }

    /// Every base advance runs this statement, and both of its lookups must
    /// seek. Results are identical either way, so only the plan can witness a
    /// regression: the open-PR probe falling back to `idx_mr_project_status`
    /// reads every merge request in the project once per settled job, which
    /// was ~60 s per call on production data before migration 0267.
    #[tokio::test]
    async fn sibling_job_statement_seeks_both_lookups() {
        let db = migrated_db().await;
        let sql = format!("EXPLAIN QUERY PLAN {}", sibling_jobs_sql());
        let plan: Vec<String> = db
            .read(|conn| {
                let sql = sql.clone();
                Box::pin(async move {
                    let mut rows = conn.query(&sql, params!["p", "main", "j"]).await?;
                    let mut steps = Vec::new();
                    while let Some(row) = rows.next().await? {
                        steps.push(row.text(3)?);
                    }
                    Ok(steps)
                })
            })
            .await
            .unwrap();

        assert!(
            plan.iter().any(|step| step
                == "SEARCH mr USING INDEX idx_mr_project_source_branch \
                    (project_id=? AND source_branch=?)"),
            "the open-PR probe must seek on project and branch together, got {plan:?}"
        );
        assert!(
            plan.iter().any(|step| step.starts_with(
                "SEARCH j USING INDEX idx_jobs_project_base_branch \
                 (project_id=? AND base_branch=?)"
            )),
            "the sibling selection must seek jobs on project and base branch, got {plan:?}"
        );
        assert!(
            !plan.iter().any(|step| step.starts_with("SCAN ")),
            "no table in the sibling lookup may be scanned, got {plan:?}"
        );
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
    async fn reconcile_quarantine_upsert_load_and_release() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");

        upsert_reconcile_quarantine(
            &db,
            "proj-1",
            store,
            "agent/test",
            "immutable_commit",
            "tip-a",
            Some("immutable commit"),
        )
        .await
        .unwrap();
        let first = load_reconcile_quarantine(&db, "proj-1", store, "agent/test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.failure_kind, "immutable_commit");
        assert_eq!(first.fingerprint, "tip-a");

        upsert_reconcile_quarantine(
            &db,
            "proj-1",
            store,
            "agent/test",
            "conflicted_bookmark",
            "tip-b",
            Some("name is conflicted"),
        )
        .await
        .unwrap();
        assert_eq!(db.query_opt_i64(
            "SELECT strike_count FROM jj_reconcile_quarantines WHERE project_id = 'proj-1' AND store_path = '/store' AND bookmark = 'agent/test'",
            (),
        ).await.unwrap(), Some(2));
        release_reconcile_quarantine(&db, "proj-1", store, "agent/test")
            .await
            .unwrap();
        assert!(
            load_reconcile_quarantine(&db, "proj-1", store, "agent/test")
                .await
                .unwrap()
                .is_none()
        );
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
            last_diagnostic: Some("rolled back".to_string()),
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
            last_diagnostic: None,
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

    #[tokio::test]
    async fn failed_notification_does_not_activate_quarantine_and_retry_can_activate_once() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let store = Path::new("/store");
        let pending = vec![PendingReconcileQuarantine {
            bookmark: "agent/test".to_string(),
            failure_kind: "immutable_commit".to_string(),
            fingerprint: "tip-a".to_string(),
            diagnostic: Some("immutable commit".to_string()),
        }];
        let orch = test_orchestrator(db, MockGitClient::new());
        let siblings = vec![SiblingJob {
            id: "job-overlap".to_string(),
            branch: Some("agent/test".to_string()),
            base_commit: None,
        }];
        let failures = vec![crate::jj::ReconcileFailure {
            branch: "agent/test".to_string(),
            error: "commit is immutable".to_string(),
        }];

        let first_notified = notify_failed_siblings_with(
            &orch,
            &orch.db.local,
            &siblings,
            &failures,
            "test",
            "intent-1",
            |_, _, _, _, _| Ok(DirectQueueDisposition::Undeliverable),
        )
        .unwrap();
        activate_notified_quarantines(&orch.db.local, "proj-1", store, &pending, &first_notified)
            .await
            .unwrap();
        assert!(
            load_reconcile_quarantine(&orch.db.local, "proj-1", store, "agent/test")
                .await
                .unwrap()
                .is_none(),
            "an undeliverable notification must leave the branch eligible for retry"
        );

        let retried_notified = notify_failed_siblings_with(
            &orch,
            &orch.db.local,
            &siblings,
            &failures,
            "test",
            "intent-2",
            |_, _, _, _, _| Ok(DirectQueueDisposition::QueuedOrPresent),
        )
        .unwrap();
        activate_notified_quarantines(&orch.db.local, "proj-1", store, &pending, &retried_notified)
            .await
            .unwrap();
        assert_eq!(
            orch.db
                .local
                .query_opt_i64(
                    "SELECT strike_count FROM jj_reconcile_quarantines
                 WHERE project_id = 'proj-1' AND store_path = '/store'
                   AND bookmark = 'agent/test'",
                    (),
                )
                .await
                .unwrap(),
            Some(1),
            "the successful retry activates quarantine exactly once"
        );
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
    /// `load_sibling_jobs` snapshots every sibling's coordinate once at the top
    /// of a reconcile, so a later branch routinely finds a row another reconcile
    /// has already advanced to exactly this target. The row holds what the call
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
            branch: Some("agent/overlap".to_string()),
            base_commit: Some("stale-snapshot".to_string()),
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
            branch: Some("agent/overlap".to_string()),
            base_commit: Some("stale-snapshot".to_string()),
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
            branch: Some("agent/overlap".to_string()),
            base_commit: None,
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
            branch: Some("agent/gone".to_string()),
            base_commit: Some("stale-snapshot".to_string()),
        };

        advance_sibling_durable_base(&db, &sibling, "new-tip")
            .await
            .expect("a missing job row is logged, not raised");
    }

    /// Runner-internal bookkeeping never addresses an agent, and the control
    /// half proves the filter is narrow: a genuine reconcile failure still
    /// steers the branch's job.
    #[tokio::test]
    async fn a_persistence_failure_is_withheld_while_a_real_one_still_steers() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let siblings = vec![SiblingJob {
            id: "job-overlap".to_string(),
            branch: Some("agent/overlap".to_string()),
            base_commit: None,
        }];
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let enqueue = |_: &Orchestrator, _: &str, _: &str, _: DeliveryUrgency, _: &str| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(DirectQueueDisposition::QueuedOrPresent)
        };

        let internal = vec![crate::jj::ReconcileFailure {
            branch: "agent/overlap".to_string(),
            error: "durable base advancement failed: durable base coordinate for job \
                    job-overlap changed concurrently twice (last observed abc); refused to \
                    overwrite it"
                .to_string(),
        }];
        let notified = notify_failed_siblings_with(
            &orch,
            &orch.db.local,
            &siblings,
            &internal,
            "test",
            "intent-1",
            enqueue,
        )
        .unwrap();
        assert!(
            notified.is_empty(),
            "an unpersisted coordinate is not something an agent can act on"
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "and nothing may even be enqueued for it"
        );

        let genuine = vec![crate::jj::ReconcileFailure {
            branch: "agent/overlap".to_string(),
            error: "origin push failed: connection reset".to_string(),
        }];
        let notified = notify_failed_siblings_with(
            &orch,
            &orch.db.local,
            &siblings,
            &genuine,
            "test",
            "intent-2",
            enqueue,
        )
        .unwrap();
        assert_eq!(
            notified,
            vec!["agent/overlap".to_string()],
            "a branch that genuinely cannot move still wakes its agent"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// A lost `packed-refs.lock` is the machine's ref store, not the branch:
    /// neither contention nor a stale lock may send an agent a "needs repair"
    /// steer it cannot act on. The worker retries, and cairn:~/rebase says so.
    #[tokio::test]
    async fn a_lost_ref_lock_is_withheld_from_the_repair_steer() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let siblings = vec![SiblingJob {
            id: "job-overlap".to_string(),
            branch: Some("agent/overlap".to_string()),
            base_commit: None,
        }];
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let enqueue = |_: &Orchestrator, _: &str, _: &str, _: DeliveryUrgency, _: &str| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(DirectQueueDisposition::QueuedOrPresent)
        };
        let lost_locks = [
            "jj rebase failed: The lock for the packed-ref file could not be obtained\n\
             packed-refs lock: contention — held by live process 77 (git) after 30s",
            "jj rebase failed: The lock for the packed-ref file could not be obtained\n\
             packed-refs lock: stale — older than 30s with no live holder, not removed: lsof \
             exited 2",
            "deferred without an attempt: the ref store stayed locked past patience earlier in \
             this pass, and is retried automatically.\n\
             packed-refs lock: contention — no lock at give-up; lost the race to a burst of \
             writers for 30s",
        ];
        for (index, error) in lost_locks.iter().enumerate() {
            let failed = vec![crate::jj::ReconcileFailure {
                branch: "agent/overlap".to_string(),
                error: error.to_string(),
            }];
            let notified = notify_failed_siblings_with(
                &orch,
                &orch.db.local,
                &siblings,
                &failed,
                "test",
                &format!("intent-lock-{index}"),
                enqueue,
            )
            .unwrap();
            assert!(notified.is_empty(), "withheld: {error}");
        }
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "nothing may even be enqueued for a lost ref lock"
        );

        // A lock that caused a failure whose rollback then did not complete is
        // NOT routine: the branch may not be where the pass found it.
        let unsafe_rollback = vec![crate::jj::ReconcileFailure {
            branch: "agent/overlap".to_string(),
            error: "jj rebase of `agent/overlap` moved the bookmark, and exporting it to git \
                    failed: The lock for the packed-ref file could not be obtained\n\
                    packed-refs lock: contention — held by live process 77 (git) after 30s\n\
                    rollback incomplete: rolling the rebase back to operation op1 also failed"
                .to_string(),
        }];
        let notified = notify_failed_siblings_with(
            &orch,
            &orch.db.local,
            &siblings,
            &unsafe_rollback,
            "test",
            "intent-rollback",
            enqueue,
        )
        .unwrap();
        assert_eq!(
            notified,
            vec!["agent/overlap".to_string()],
            "an incomplete rollback still reaches the agent"
        );
    }

    #[test]
    fn divergence_quarantine_fingerprint_is_order_independent() {
        assert_eq!(
            divergence_fingerprint(&["bbb".to_string(), "aaa".to_string()]),
            "aaa+bbb"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_branch_ambiguous_delivery_is_idempotent_per_recipient() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let branch = "agent/proj-integration";
        let fingerprint = "aaa+bbb";
        let message = "ambiguous divergence";
        let recipients = ["run-job-overlap", "run-job-clean"];

        for run_id in recipients {
            let key = on_branch_ambiguous_delivery_key("proj-1", branch, fingerprint, run_id);
            assert_eq!(
                queue_system_direct_once_confirmed(
                    &orch,
                    run_id,
                    message,
                    DeliveryUrgency::Interrupt,
                    &key,
                )
                .unwrap(),
                DirectQueueDisposition::QueuedOrPresent
            );
        }
        let count_messages = || async {
            orch.db
                .local
                .query_opt_i64(
                    "SELECT COUNT(*) FROM messages
                     WHERE recipient_run_id IN ('run-job-overlap', 'run-job-clean')
                       AND content = 'ambiguous divergence'",
                    (),
                )
                .await
                .unwrap()
        };
        assert_eq!(
            count_messages().await,
            Some(2),
            "each on-branch job receives its own direct"
        );

        for run_id in recipients {
            let key = on_branch_ambiguous_delivery_key("proj-1", branch, fingerprint, run_id);
            queue_system_direct_once_confirmed(
                &orch,
                run_id,
                message,
                DeliveryUrgency::Interrupt,
                &key,
            )
            .unwrap();
        }
        assert_eq!(
            count_messages().await,
            Some(2),
            "retrying the same twin fingerprint does not duplicate either direct"
        );
    }

    #[test]
    fn only_transient_failures_keep_an_intent_pending() {
        let failure = |error: &str| crate::jj::ReconcileFailure {
            branch: "agent/test".to_string(),
            error: error.to_string(),
        };
        assert!(!reconcile_has_transient_failures(&[failure(
            "commit is immutable"
        )]));
        // A conflicted bookmark is now TRANSIENT, and deliberately so: it is a
        // reconciliation TODO the next base advance repairs, not a terminal
        // state. Holding it as permanent meant it was never retried and never
        // repaired, which is the agent-unreachable dead end this arc forbids.
        assert!(reconcile_has_transient_failures(&[failure(
            "bookmark name is conflicted"
        )]));
        assert!(reconcile_has_transient_failures(&[failure(
            "process exited unexpectedly"
        )]));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn merged_issue_jobs_are_excluded_but_null_issue_jobs_remain() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        db.execute_script(
            "UPDATE issues SET status = 'merged' WHERE id = 'issue-2';
             INSERT INTO jobs
               (id, execution_id, recipe_node_id, issue_id, project_id, status,
                branch, base_branch, created_at, updated_at)
             VALUES
               ('job-no-issue', NULL, 'node', NULL, 'proj-1', 'running',
                'agent/proj-null-builder-0', 'integration', 1, 1);",
        )
        .await
        .unwrap();

        let siblings = load_sibling_jobs(&db, "proj-1", "integration", "job-merged")
            .await
            .unwrap();
        let ids: std::collections::HashSet<&str> =
            siblings.iter().map(|job| job.id.as_str()).collect();
        assert!(!ids.contains("job-overlap"));
        assert!(ids.contains("job-no-issue"));
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

    async fn migrated_team_db(path: &Path) -> Arc<LocalDb> {
        let db = Arc::new(LocalDb::open(path).await.unwrap());
        crate::storage::MigrationRunner::new(crate::storage::TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_team_base_advance_notification_fixture(
        db: &LocalDb,
        team_id: &str,
        job_id: &str,
        run_id: &str,
    ) {
        let project_id = format!("{team_id}~00000000-0000-4000-8000-200000000001");
        let issue_id = format!("{team_id}~00000000-0000-4000-8000-200000000002");
        let execution_id = format!("{team_id}~00000000-0000-4000-8000-200000000003");
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
             VALUES (?1, 'default', 'Team Project', 'TEAM', '/repo/team', 'main', 1, 1)",
            params![project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
             VALUES (?1, ?2, 9, 'Team Issue', 'active', 1, 1)",
            params![issue_id.as_str(), project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES (?1, 'recipe-default', ?2, ?3, 'running', 1, 1)",
            params![
                execution_id.as_str(),
                issue_id.as_str(),
                project_id.as_str()
            ],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, uri_segment, created_at, updated_at)
             VALUES (?1, ?2, 'node', ?3, ?4, 'running', 'agent/TEAM-9-builder-0', 'integration', 'builder', 1, 1)",
            params![job_id, execution_id.as_str(), issue_id.as_str(), project_id.as_str()],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'live', 1, 1)",
            params![run_id, issue_id.as_str(), project_id.as_str(), job_id],
        )
        .await
        .unwrap();
    }

    async fn message_count(db: &LocalDb) -> i64 {
        db.query_one("SELECT COUNT(*) FROM messages", (), |row| row.i64(0))
            .await
            .unwrap()
    }

    /// A replay refused at export for conflict scaffolding tells the agent what
    /// was refused and where the session is, and never that there is nothing to
    /// resolve.
    #[test]
    fn a_scaffolding_refusal_names_the_commit_the_entries_and_the_open_session() {
        let note = build_jj_conflict_note("main", Some(42), None);
        let refused = "r".repeat(40);
        let message = append_conflict_diagnostic(
            &note,
            Some(&crate::jj::ConflictDiagnostic {
                validation: None,
                base: Some("b".repeat(40)),
                ours: Some("o".repeat(40)),
                theirs: Some("t".repeat(40)),
                conflicted_tip: Some(refused.clone()),
                condition: crate::jj::ConflictCondition::ContentConflict,
                incoming: vec![crate::jj::IncomingFile {
                    path: "shared.rs".to_string(),
                    status: "M".to_string(),
                    classification: crate::jj::IncomingClassification::Conflicting,
                }],
                scaffolding: Some(crate::jj::ScaffoldingRefusal {
                    commit: refused.clone(),
                    entries: vec![
                        ".jjconflict-base-0".to_string(),
                        "JJ-CONFLICT-README".to_string(),
                    ],
                }),
            }),
            MarkerState::NotMaterialized,
        );
        assert!(message.contains(&refused), "{message}");
        assert!(
            message.contains(".jjconflict-base-0, JJ-CONFLICT-README"),
            "{message}"
        );
        assert!(message.contains("a `/rebase` session is open"), "{message}");
        assert!(message.contains("shared.rs"), "{message}");
        assert!(message.contains(&"o".repeat(40)), "{message}");
        assert!(!message.contains("nothing to resolve"), "{message}");
        assert!(!message.contains("nothing to merge"), "{message}");
        assert!(!message.contains("not by jj"), "{message}");
        assert!(!message.contains("jj reported"), "{message}");
        assert!(
            !message.contains("without a recorded conflict"),
            "{message}"
        );
        assert!(
            !message.contains("the two sides genuinely disagree"),
            "scaffolding alone does not prove native disagreement: {message}"
        );
    }

    /// The two conditions must give OPPOSITE advice, and the base-drift branch
    /// must not send an agent to edit files that already agree — the exact round
    /// CAIRN-3327 and CAIRN-3328 each burned.
    #[test]
    fn base_drift_and_content_conflict_give_opposite_guidance() {
        let note = build_jj_conflict_note("main", Some(42), None);
        let incoming = vec![crate::jj::IncomingFile {
            path: "shared.rs".to_string(),
            status: "M".to_string(),
            classification: crate::jj::IncomingClassification::Conflicting,
        }];

        let drift = append_conflict_diagnostic(
            &note,
            Some(&crate::jj::ConflictDiagnostic {
                validation: None,
                base: Some("b".repeat(40)),
                ours: Some("o".repeat(40)),
                theirs: Some("t".repeat(40)),
                conflicted_tip: None,
                condition: crate::jj::ConflictCondition::BaseDrift,
                incoming: incoming.clone(),
                scaffolding: None,
            }),
            MarkerState::NotMaterialized,
        );
        assert!(drift.contains("BASE DRIFT"), "{drift}");
        assert!(
            drift.contains("nothing to merge"),
            "drift must say the content work may already be done: {drift}"
        );
        assert!(
            drift.contains("downstream exports"),
            "drift must explain why a local ref move cannot stick: {drift}"
        );
        assert!(
            !drift.contains("write the merged result"),
            "drift must NOT ask for content edits: {drift}"
        );

        let content = append_conflict_diagnostic(
            &note,
            Some(&crate::jj::ConflictDiagnostic {
                validation: None,
                base: Some("b".repeat(40)),
                ours: Some("o".repeat(40)),
                theirs: Some("t".repeat(40)),
                conflicted_tip: None,
                condition: crate::jj::ConflictCondition::ContentConflict,
                incoming,
                scaffolding: None,
            }),
            MarkerState::NotMaterialized,
        );
        assert!(content.contains("CONTENT CONFLICT"), "{content}");
        assert!(content.contains("write the merged result"), "{content}");
        assert!(
            !content.contains("BASE DRIFT"),
            "the two conditions never both appear: {content}"
        );

        // Neither may ever authorize the one action that cannot be undone, and
        // each must name the route it DOES sanction — the drift branch by handing
        // over the replay request verbatim, the content branch by forbidding a
        // hand rebase while pointing at the session.
        for message in [&drift, &content] {
            assert!(
                message.contains("cairn:~/rebase"),
                "each condition routes to the session: {message}"
            );
            assert!(!message.contains("git rebase"), "{message}");
        }
        assert!(
            drift.contains("action:\"replay\""),
            "drift hands over the exact request rather than describing it: {drift}"
        );
        assert!(
            content.contains("force-push"),
            "the content branch still forbids hand-rebasing: {content}"
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

    /// The standing rule: machinery never instructs an agent to act on state it
    /// has not made true. Only a CONFIRMED materialization may put "resolve the
    /// markers" in front of an agent; every other state has to say plainly that
    /// there is nothing on disk to resolve, and point at the session instead.
    #[test]
    fn only_a_confirmed_materialization_tells_the_agent_to_resolve_markers() {
        let note = build_jj_conflict_note("main", Some(42), None);
        let diagnostic = crate::jj::ConflictDiagnostic {
            validation: None,
            base: Some("b".repeat(40)),
            ours: Some("o".repeat(40)),
            theirs: Some("t".repeat(40)),
            conflicted_tip: None,
            condition: crate::jj::ConflictCondition::ContentConflict,
            incoming: vec![crate::jj::IncomingFile {
                path: "shared.rs".to_string(),
                status: "M".to_string(),
                classification: crate::jj::IncomingClassification::Conflicting,
            }],
            scaffolding: None,
        };

        let confirmed =
            append_conflict_diagnostic(&note, Some(&diagnostic), MarkerState::Materialized);
        assert!(
            confirmed.contains("Conflict markers have been written into your checkout"),
            "{confirmed}"
        );

        for state in [
            MarkerState::NotMaterialized,
            MarkerState::Pending,
            MarkerState::Failed,
        ] {
            let message = append_conflict_diagnostic(&note, Some(&diagnostic), state);
            assert!(
                !message.contains("Conflict markers have been written"),
                "{state:?} must not claim markers exist: {message}"
            );
            assert!(
                message.contains("cairn:~/rebase"),
                "{state:?} must route the agent to both sides of the merge: {message}"
            );
        }

        // Pending is specifically NOT the same as absent: an agent told markers
        // are absent would stop looking, while the retry may still land them.
        let pending = append_conflict_diagnostic(&note, Some(&diagnostic), MarkerState::Pending);
        assert!(pending.contains("NOT confirmed present"), "{pending}");
        let absent =
            append_conflict_diagnostic(&note, Some(&diagnostic), MarkerState::NotMaterialized);
        assert!(
            absent.contains("no conflict markers in your checkout"),
            "{absent}"
        );

        // Under base drift the two sides already agree, so no marker state may
        // ever produce "write the merged result" — including the confirmed one,
        // where any marker on disk is scaffolding rather than work to do.
        let drift = crate::jj::ConflictDiagnostic {
            condition: crate::jj::ConflictCondition::BaseDrift,
            ..diagnostic.clone()
        };
        for state in [
            MarkerState::NotMaterialized,
            MarkerState::Pending,
            MarkerState::Materialized,
            MarkerState::Failed,
        ] {
            let message = append_conflict_diagnostic(&note, Some(&drift), state);
            assert!(
                !message.contains("write the merged result"),
                "{state:?} must not send a drifted branch to edit agreeing files: {message}"
            );
        }
    }

    /// A wake that carries no diagnostic (nothing enumerated) degrades to the
    /// identity half rather than rendering an empty file table.
    #[test]
    fn a_wake_without_a_diagnostic_is_left_unchanged() {
        let note = build_jj_conflict_note("main", Some(42), None);
        assert_eq!(
            append_conflict_diagnostic(&note, None, MarkerState::NotMaterialized),
            note
        );
    }

    #[test]
    fn jj_conflict_note_carries_no_rebase_commands() {
        let issue = IssueInfo {
            project_key: "proj".to_string(),
            number: 7,
        };
        let note = build_jj_conflict_note("agent/cairn-1940-coordinator-0", Some(42), Some(&issue));
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("PR #42 merged"));
        assert!(note.contains("cairn://p/proj/7"));
        // Stop-the-line: the note names the conflict as blocking, not optional.
        assert!(note.contains("BLOCKING"));
        // Content remains on the pre-advance side, but replay machinery may
        // rewrite the store identity even when the new base does not land.
        assert!(note.contains("pre-advance content"), "{note}");
        assert!(note.contains("store commit identity"), "{note}");
        assert!(note.contains("cairn:~/rebase"), "{note}");
        // Agents hold detached git worktrees, so conflict markers are never
        // materialized anywhere. A note that points at them is unanswerable.
        assert!(!note.to_lowercase().contains("marker"), "{note}");
        assert!(!note.contains("resolve them"), "{note}");
        // The note must not instruct a manual rebase/force-push.
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
    }

    #[test]
    fn jj_clean_note_describes_clean_rebase_with_no_action() {
        let issue = IssueInfo {
            project_key: "proj".to_string(),
            number: 7,
        };
        let note = build_jj_clean_note("agent/cairn-1940-coordinator-0", Some(42), Some(&issue));
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("agent/cairn-1940-coordinator-0"));
        assert!(note.contains("PR #42 merged"));
        assert!(note.contains("cairn://p/proj/7"));
        assert!(note.contains("cleanly"));
        assert!(note.contains("nothing to resolve"));
        // A clean rebase needs no manual git work.
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
        assert!(note.contains("No manual rebase or force-push is needed"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn enumerates_completed_sibling_with_open_pr() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        // The completed sibling has an OPEN merge request on the same base — the
        // live-bug case where reconcile previously found zero siblings because the
        // status filter excluded a `complete` job awaiting merge.
        db.execute_script(
            "UPDATE jobs SET branch = 'agent/proj-4-builder-0' WHERE id = 'job-complete';
             INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-complete', 'job-complete', 'proj-1', 'issue-4', 'PR', 'agent/proj-4-builder-0', 'integration', 'open', 1, 1);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db, MockGitClient::new());

        let siblings = load_sibling_jobs(&orch.db.local, "proj-1", "integration", "job-merged")
            .await
            .unwrap();
        let ids: std::collections::HashSet<&str> = siblings.iter().map(|s| s.id.as_str()).collect();

        assert!(
            ids.contains("job-complete"),
            "a completed sibling with an open PR must be enumerated for rebase"
        );
        assert!(
            ids.contains("job-overlap"),
            "an in-flight sibling is still enumerated"
        );
        assert!(
            ids.contains("job-clean"),
            "an in-flight sibling is still enumerated"
        );
        assert!(
            !ids.contains("job-merged"),
            "the merged job itself is excluded"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_branch_query_selects_coordinator_distinct_from_siblings() {
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

        // The sibling query (branches based ON integration) must NOT include it.
        let siblings = load_sibling_jobs(&orch.db.local, "proj-1", "integration", "job-merged")
            .await
            .unwrap();
        let sib_ids: std::collections::HashSet<&str> =
            siblings.iter().map(|s| s.id.as_str()).collect();
        assert!(
            !sib_ids.contains("job-coord"),
            "the coordinator is not a sibling of itself"
        );
        assert!(
            sib_ids.contains("job-overlap"),
            "siblings are still the children branched from integration"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn excludes_completed_sibling_without_open_pr() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let siblings = load_sibling_jobs(&orch.db.local, "proj-1", "integration", "job-merged")
            .await
            .unwrap();
        let ids: std::collections::HashSet<&str> = siblings.iter().map(|s| s.id.as_str()).collect();

        // job-complete is `complete` with no MR: still excluded.
        assert!(!ids.contains("job-complete"));
        assert!(ids.contains("job-overlap"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn external_advance_enumerates_all_in_flight_siblings_with_no_exclusion() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        // An external default-branch advance has no merged job to exclude, so the
        // sentinel excludes nothing. Every in-flight sibling on the branch is a
        // reconcile candidate; a completed job without an open PR is still out.
        let siblings = load_sibling_jobs(&orch.db.local, "proj-1", "integration", EXCLUDE_NONE)
            .await
            .unwrap();
        let ids: std::collections::HashSet<&str> = siblings.iter().map(|s| s.id.as_str()).collect();

        assert!(
            ids.contains("job-overlap"),
            "in-flight sibling is enumerated"
        );
        assert!(ids.contains("job-clean"), "in-flight sibling is enumerated");
        // job-merged is `complete` with a MERGED merge request; job-complete is
        // `complete` with no MR. Both are excluded even with no job to exclude.
        assert!(
            !ids.contains("job-merged"),
            "a completed job whose PR already merged is not a reconcile candidate"
        );
        assert!(
            !ids.contains("job-complete"),
            "a completed job without an open PR is not a reconcile candidate"
        );
    }
    #[test]
    fn external_advance_note_carries_no_pr_or_rebase_commands() {
        let note = build_external_advance_conflict_note("main");
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("`main`"));
        assert!(note.contains("outside Cairn"));
        // Stop-the-line: the note names the conflict as blocking, not optional.
        assert!(note.contains("BLOCKING"));
        // Same contract as the Cairn-owned advance: content remains while the
        // store identity may move, and there are no markers to point at.
        assert!(note.contains("pre-advance content"), "{note}");
        assert!(note.contains("store commit identity"), "{note}");
        assert!(note.contains("cairn:~/rebase"), "{note}");
        assert!(!note.to_lowercase().contains("marker"), "{note}");
        // No Cairn-tracked owner: the note must not reference a PR number.
        assert!(!note.contains("PR #"));
        // The note must not instruct a manual rebase/force-push/fetch.
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
    }

    #[test]
    fn external_advance_clean_note_carries_no_pr_or_rebase_commands() {
        let note = build_external_advance_clean_note("main");
        assert!(note.contains("[Base branch update]"));
        assert!(note.contains("`main`"));
        assert!(note.contains("outside Cairn"));
        assert!(note.contains("cleanly"));
        assert!(note.contains("nothing to resolve"));
        // No Cairn-tracked owner: no PR number.
        assert!(!note.contains("PR #"));
        assert!(!note.contains("git rebase"));
        assert!(!note.contains("git fetch"));
        assert!(note.contains("No manual rebase or force-push is needed"));
    }

    #[test]
    fn siblings_rewritten_skips_unchanged_commits() {
        let branches = vec![
            "agent/rewritten".to_string(),
            "agent/unchanged".to_string(),
            "agent/missing-after".to_string(),
        ];
        let before: HashMap<String, String> = [
            ("agent/rewritten".to_string(), "commit-a".to_string()),
            ("agent/unchanged".to_string(), "commit-b".to_string()),
            ("agent/missing-after".to_string(), "commit-c".to_string()),
        ]
        .into_iter()
        .collect();
        // `rewritten` moved (this reconcile rewrote it), `unchanged` is a
        // double-fire no-op, `missing-after` failed to resolve post-rebase.
        let after: HashMap<String, String> = [
            ("agent/rewritten".to_string(), "commit-a2".to_string()),
            ("agent/unchanged".to_string(), "commit-b".to_string()),
        ]
        .into_iter()
        .collect();

        let rewritten = siblings_rewritten(&branches, &before, &after);

        assert!(
            rewritten.contains(&"agent/rewritten".to_string()),
            "a sibling this reconcile actually rewrote is notified"
        );
        assert!(
            !rewritten.contains(&"agent/unchanged".to_string()),
            "a double-fire no-op at the same tip is not re-notified"
        );
        assert!(
            rewritten.contains(&"agent/missing-after".to_string()),
            "an unresolved snapshot notifies conservatively rather than dropping a change"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_reconcile_queues_waking_steer_note() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let siblings = vec![SiblingJob {
            id: "job-overlap".to_string(),
            branch: Some("agent/proj-2-builder-0".to_string()),
            base_commit: None,
        }];
        let failed = vec![crate::jj::ReconcileFailure {
            branch: "agent/proj-2-builder-0".to_string(),
            error: "jj bookmark rebase failed: exact stderr".to_string(),
        }];

        notify_failed_siblings(
            &orch,
            &orch.db.local,
            &siblings,
            &failed,
            "external advance on main",
            "test-failed",
        )
        .unwrap();

        let (content, wake): (String, String) = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut messages = conn
                        .query("SELECT content FROM messages WHERE recipient_run_id = 'run-job-overlap'", ())
                        .await?;
                    let content = messages.next().await?.expect("failed sibling message").text(0)?;
                    let mut pushes = conn
                        .query("SELECT wake FROM attention_pushes WHERE recipient = 'job-overlap'", ())
                        .await?;
                    let wake = pushes.next().await?.expect("failed sibling attention push").text(0)?;
                    Ok::<_, DbError>((content, wake))
                })
            })
            .await
            .unwrap();
        // The agent is told its work is safe and what not to do; the exact
        // reconciliation stderr rides along for the human who repairs it.
        assert!(content.contains("Your commits are intact"));
        assert!(content.contains("jj bookmark rebase failed: exact stderr"));
        assert!(!content.to_lowercase().contains("retry"));
        assert!(content.to_lowercase().contains("do not force-push"));
        assert_eq!(wake, "wake");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wakes_only_conflicted_jj_siblings() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let siblings = vec![
            SiblingJob {
                id: "job-overlap".to_string(),
                branch: Some("agent/proj-2-builder-0".to_string()),
                base_commit: None,
            },
            SiblingJob {
                id: "job-clean".to_string(),
                branch: Some("agent/proj-3-builder-0".to_string()),
                base_commit: None,
            },
        ];
        let conflicted = vec!["agent/proj-2-builder-0".to_string()];
        let note = build_jj_conflict_note("integration", Some(42), None);
        // The cross-file trap this diagnostic exists for: the incoming change
        // conflicts on two files and ALSO carries a third that arrives cleanly.
        // An agent told only about the first two resolves them, stops compiling,
        // and has no idea why (CAIRN-3337).
        let mut diagnostics: HashMap<String, crate::jj::ConflictDiagnostic> = HashMap::new();
        diagnostics.insert(
            "agent/proj-2-builder-0".to_string(),
            crate::jj::ConflictDiagnostic {
                validation: None,
                base: Some("b".repeat(40)),
                ours: Some("o".repeat(40)),
                theirs: Some("t".repeat(40)),
                conflicted_tip: None,
                condition: crate::jj::ConflictCondition::ContentConflict,
                incoming: vec![
                    crate::jj::IncomingFile {
                        path: "shared.rs".to_string(),
                        status: "M".to_string(),
                        classification: crate::jj::IncomingClassification::Conflicting,
                    },
                    crate::jj::IncomingFile {
                        path: "lib.rs".to_string(),
                        status: "M".to_string(),
                        classification: crate::jj::IncomingClassification::Conflicting,
                    },
                    crate::jj::IncomingFile {
                        path: "storage.rs".to_string(),
                        status: "M".to_string(),
                        classification: crate::jj::IncomingClassification::CleanOnRetry,
                    },
                ],
                scaffolding: None,
            },
        );

        notify_conflicted_siblings(
            &orch,
            &orch.db.local,
            &siblings,
            &conflicted,
            &note,
            &ConflictEvidence {
                diagnostics,
                ..ConflictEvidence::default()
            },
            "test-conflicted",
        )
        .unwrap();

        // Only the conflicted sibling receives a message, and that message names
        // the conflicting files threaded through from `files_by_branch`.
        let messages: Vec<(String, String)> = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT recipient_run_id, content FROM messages ORDER BY created_at",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(row) = rows.next().await? {
                        v.push((row.text(0)?, row.text(1)?));
                    }
                    Ok::<_, DbError>(v)
                })
            })
            .await
            .unwrap();

        assert_eq!(
            messages.len(),
            1,
            "only the conflicted sibling is messaged; the cleanly-rebased one is not"
        );
        assert_eq!(messages[0].0, "run-job-overlap");
        let wake = &messages[0].1;
        assert!(
            wake.contains("Conflicting files, yours to merge (2): shared.rs, lib.rs"),
            "the note names the conflicting files: {wake}"
        );
        // The load-bearing half: the sibling file that is NOT conflicting still
        // has to be named, or the agent resolves two files and ships a branch
        // that does not compile.
        assert!(
            wake.contains("cleanly on retry (1): storage.rs"),
            "the note names the incoming change's clean-on-retry siblings: {wake}"
        );
        assert!(
            wake.contains("CONTENT CONFLICT"),
            "the note names which condition this is: {wake}"
        );
        assert!(
            wake.contains(&"t".repeat(40)) && wake.contains(&"b".repeat(40)),
            "the immutable three-way coordinates ride out, so both sides are recomputable: {wake}"
        );
        // Markers were not confirmed for this branch, so the wake must say they
        // are absent rather than tell the agent to resolve state nothing made
        // true. It may still MENTION markers — saying "there are none" is the
        // honest thing — but never in the imperative.
        assert!(
            wake.contains("no conflict markers in your checkout"),
            "an unconfirmed materialization must say markers are absent: {wake}"
        );
        assert!(
            !wake.contains("Conflict markers have been written"),
            "the wake must never claim markers exist without confirmation: {wake}"
        );
        assert!(
            wake.contains("cairn:~/rebase"),
            "the wake links the session that carries both sides of the merge: {wake}"
        );

        // The push is waking but non-interrupting: it steers the agent at the next
        // boundary instead of cancelling an active tool call, and remains distinct
        // from the `passive` clean-rebase note asserted in
        // `notify_clean_siblings_passively`.
        let wake: String = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT wake FROM attention_pushes WHERE recipient = 'job-overlap'",
                            (),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("no push for job-overlap".to_string()))?;
                    row.text(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            wake, "wake",
            "a base-advance conflict wakes or steers the agent without cancelling the active turn"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn team_conflicted_sibling_notification_lands_in_team_replica() {
        let private = migrated_db().await;
        let orch = test_orchestrator(private, MockGitClient::new());
        let team_temp = tempfile::tempdir().unwrap();
        let team_id = "teambase";
        let team_db = migrated_team_db(&team_temp.path().join("team-base-test.db")).await;
        let job_id = "teambase~00000000-0000-4000-8000-200000000010";
        let run_id = "teambase~00000000-0000-4000-8000-200000000011";
        seed_team_base_advance_notification_fixture(&team_db, team_id, job_id, run_id).await;
        orch.db
            .insert_team_db_for_test(team_id, team_db.clone())
            .await;

        let siblings = vec![SiblingJob {
            id: job_id.to_string(),
            branch: Some("agent/TEAM-9-builder-0".to_string()),
            base_commit: None,
        }];
        let conflicted = vec!["agent/TEAM-9-builder-0".to_string()];
        let note = build_jj_conflict_note("integration", Some(42), None);
        let mut diagnostics: HashMap<String, crate::jj::ConflictDiagnostic> = HashMap::new();
        diagnostics.insert(
            "agent/TEAM-9-builder-0".to_string(),
            crate::jj::ConflictDiagnostic::from_paths(vec!["team.rs".to_string()]),
        );

        notify_conflicted_siblings(
            &orch,
            &team_db,
            &siblings,
            &conflicted,
            &note,
            &ConflictEvidence {
                diagnostics,
                ..ConflictEvidence::default()
            },
            "test-team-conflicted",
        )
        .unwrap();

        assert_eq!(
            message_count(&team_db).await,
            1,
            "the base-advance direct message must be written to the team replica"
        );
        assert_eq!(
            message_count(&orch.db.local).await,
            0,
            "a team base-advance notification must not fall back to the private database"
        );
        let wake: String = team_db
            .query_one(
                "SELECT wake FROM attention_pushes WHERE recipient = ?1",
                params![job_id],
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(
            wake, "wake",
            "team base-advance conflicts steer without interrupting active turns"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ambiguous_divergence_interrupts_only_the_affected_sibling() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let siblings = vec![
            SiblingJob {
                id: "job-overlap".to_string(),
                branch: Some("agent/proj-2-builder-0".to_string()),
                base_commit: None,
            },
            SiblingJob {
                id: "job-clean".to_string(),
                branch: Some("agent/proj-3-builder-0".to_string()),
                base_commit: None,
            },
        ];
        let ambiguous = vec![AmbiguousDivergence {
            branch: "agent/proj-2-builder-0".to_string(),
            change_id: "qpvuntsmxyzw".to_string(),
            twins: vec!["aaaa1111".to_string(), "bbbb2222".to_string()],
        }];

        notify_ambiguous_divergence(
            &orch,
            &orch.db.local,
            &siblings,
            &ambiguous,
            "test-ambiguous",
        )
        .unwrap();

        // Only the ambiguous sibling is messaged, and the note names the
        // change-id, both twin commit ids, and the no-force-push instruction.
        let messages: Vec<(String, String)> = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT recipient_run_id, content FROM messages ORDER BY created_at",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(row) = rows.next().await? {
                        v.push((row.text(0)?, row.text(1)?));
                    }
                    Ok::<_, DbError>(v)
                })
            })
            .await
            .unwrap();

        assert_eq!(
            messages.len(),
            1,
            "only the ambiguous sibling is interrupted; the healthy one is not"
        );
        assert_eq!(messages[0].0, "run-job-overlap");
        assert!(
            messages[0].1.contains("qpvuntsmxyzw"),
            "names the change-id: {}",
            messages[0].1
        );
        assert!(messages[0].1.contains("aaaa1111") && messages[0].1.contains("bbbb2222"));
        assert!(messages[0].1.contains("Do NOT force-push"));

        // Delivered as a stop-the-line interrupt (a divergent tangle wedges the
        // branch the same way a recorded conflict does).
        let wake: String = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT wake FROM attention_pushes WHERE recipient = 'job-overlap'",
                            (),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("no push for job-overlap".to_string()))?;
                    row.text(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            wake, "interrupt",
            "an ambiguous divergence interrupts the agent — stop-the-line"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn notify_clean_siblings_passively() {
        let db = migrated_db().await;
        seed_base_advance_fixture(&db).await;
        let orch = test_orchestrator(db, MockGitClient::new());

        let siblings = vec![SiblingJob {
            id: "job-clean".to_string(),
            branch: Some("agent/proj-3-builder-0".to_string()),
            base_commit: None,
        }];
        let clean = vec!["agent/proj-3-builder-0".to_string()];
        let note = build_jj_clean_note("integration", Some(42), None);

        notify_clean_siblings(
            &orch,
            &orch.db.local,
            &siblings,
            &clean,
            &note,
            "test-clean",
        )
        .unwrap();
        // A crash after enqueue but before the reconcile checkpoint retries the
        // same deterministic delivery key. It must not append a second message.
        notify_clean_siblings(
            &orch,
            &orch.db.local,
            &siblings,
            &clean,
            &note,
            "test-clean",
        )
        .unwrap();

        // (a) the cleanly-rebased sibling receives a direct note.
        let recipients: Vec<String> = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT recipient_run_id FROM messages ORDER BY created_at",
                            (),
                        )
                        .await?;
                    let mut v = Vec::new();
                    while let Some(row) = rows.next().await? {
                        v.push(row.text(0)?);
                    }
                    Ok::<_, DbError>(v)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            recipients,
            vec!["run-job-clean".to_string()],
            "the cleanly-rebased sibling receives a direct note"
        );

        // (b) its attention push is non-waking (`passive`), so an idle agent is
        // never resumed by it — the note rides along on the next natural run.
        let wake: String = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT wake FROM attention_pushes WHERE recipient = 'job-clean'",
                            (),
                        )
                        .await?;
                    let row = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("no push for job-clean".to_string()))?;
                    row.text(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            wake, "passive",
            "a clean base-advance note is delivered passively and never wakes an idle agent"
        );
    }

    #[test]
    fn remote_default_revset_targets_origin() {
        assert_eq!(remote_default_revset("main"), "main@origin");
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

    /// Base advance publishes a replayed branch by the rule the commit verbs
    /// consult. Three live siblings of `main` are replayed onto an advance made
    /// on origin: one no pull request needs, one with its own open pull request,
    /// and an integration branch an open child pull request targets. Only the
    /// latter two reach origin. The first stays a local rebased head until
    /// opening its pull request crosses the publication barrier, which pushes it.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn base_advance_publishes_only_branches_a_pull_request_needs() {
        use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
        let Some(bin) = jj_bin() else {
            eprintln!("skipping base_advance_publishes_only_branches_a_pull_request_needs: no jj");
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

        reconcile_default_advance(&orch, "proj-1", "main")
            .await
            .unwrap();
        super::real_store_collision_tests::wait_for_reconcile_workers(&db).await;

        let store_tip = |branch: &str| crate::jj::bookmark_commit(&jj, &store, branch).unwrap();
        for branch in [LOCAL, OPEN, INTEGRATION] {
            assert_eq!(
                crate::jj::revset_commit(&jj, &store, &format!("{landed} & ::{branch}")).as_deref(),
                Some(landed.as_str()),
                "{branch} is replayed onto the advance whether or not it is published"
            );
        }
        assert_eq!(
            origin_head(LOCAL),
            "",
            "a branch no pull request needs keeps its rebased head local"
        );
        assert_eq!(
            origin_head(OPEN),
            store_tip(OPEN),
            "an open pull request's head follows its replay"
        );
        assert_eq!(
            origin_head(INTEGRATION),
            store_tip(INTEGRATION),
            "the base child pull requests target stays current on origin"
        );

        // Opening a pull request publishes first (`PrVcs::publish`), through
        // this barrier, so the head base advance left local reaches origin then.
        publish_managed_branch(&orch, &store, LOCAL)
            .await
            .expect("a branch base advance left local publishes when its pull request opens");
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
        _home: TempDir,
        _repo: TempDir,
        home: PathBuf,
        repo: PathBuf,
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
        // Where an orchestrator configured at `home` looks for this repo's
        // store, so the end-to-end replay tests drive the same one.
        let store = crate::jj::project_store_dir(home.path(), repo.path());
        crate::jj::ensure_project_store(&jj, &store, repo.path()).unwrap();
        Fixture {
            home: home.path().to_path_buf(),
            repo: repo.path().to_path_buf(),
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
    fn sibling_insertions_into_one_registry_list_replay_clean_across_advances() {
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
            let report = try_structural_merge(
                &fx.jj,
                &fx.store,
                branch,
                "main",
                &diagnostic,
                &ReplayBaseline::fork_only(diagnostic.base.as_deref().unwrap()),
                &crate::orchestrator::landing::merge_rules_at(&fx.jj, &fx.store, "main"),
            )
            .unwrap()
            .expect("the declared list merge settles it");
            assert_eq!(report.rebased_clean, vec![branch.to_string()]);
            let tip = crate::jj::bookmark_commit(&fx.jj, &fx.store, branch).unwrap();
            text_at(&fx, &tip, registry)
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

        land("second", "0228_second");
        let twice = replay(branch);
        assert!(
            cairn_vcs::parse_gate::parse_failure(registry, &twice).is_none(),
            "{twice}"
        );
        assert_eq!(
            entries(&twice),
            [
                "0227_before",
                "0228_first",
                "0228_second",
                "0230_branch",
                "0229_after"
            ],
            "every entry once, the landed ones first"
        );
    }

    fn main_line(fx: &Fixture) -> PathBuf {
        let workspace = fx.workspaces.path().join("main-line");
        crate::jj::add_workspace(
            &fx.jj,
            &fx.store,
            &workspace,
            "agent/main-line",
            "main",
            None,
        )
        .unwrap();
        workspace
    }

    // ── End to end: request_branch_replay through the durable worker ────────
    //
    // cairn/4987's acceptance criterion. Each scenario drives the real request
    // path and the real reconcile worker against a real store and database:
    // automatic advances record sessions, the agent commits a linear
    // resolution, the base advances again, and the replay must never land a
    // tree missing what the newer base added.

    const LIST: &str = "list.txt";
    const MIGRATIONS: &str = "src-tauri/os/cairn-db/src/storage/migrations.rs";
    const JOB: &str = "job-replay";
    const BRANCH: &str = "agent/replay";
    const MAIN_LINE: &str = "agent/main-line";

    struct Replay {
        fx: Fixture,
        db: Arc<LocalDb>,
        orch: Orchestrator,
        main_ws: PathBuf,
        branch_ws: PathBuf,
    }

    /// A store, a database holding one job on `BRANCH`, and an orchestrator
    /// pointed at both. `base_files` are committed to main before the branch
    /// is cut.
    async fn replay_harness(bin: &str, name: &str, base_files: &[(&str, &str)]) -> Replay {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        let fx = fixture(bin);
        let main_ws = main_line(&fx);
        if !base_files.is_empty() {
            let base = commit(&fx, &main_ws, MAIN_LINE, base_files, "base files");
            set_main(&fx, &base);
        }
        let branch_ws = fx.workspaces.path().join("replay");
        crate::jj::add_workspace(&fx.jj, &fx.store, &branch_ws, BRANCH, "main", None).unwrap();

        let db = Arc::new(crate::storage::migrated_test_db(name).await);
        let repo = fx.repo.to_string_lossy().replace('\'', "''");
        db.execute_script(format!(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj-1', 'default', 'Project', 'proj', '{repo}', 'main', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-1', 'proj-1', 1, 'Issue', 'active', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
               VALUES ('{JOB}', 'exec-1', 'node', 'issue-1', 'proj-1', 'running', '{BRANCH}', 'main', 1, 1);
             INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
               VALUES ('run-replay', 'issue-1', 'proj-1', '{JOB}', 'live', 1, 1);"
        ))
        .await
        .unwrap();
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(fx.home.join("search")).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            fx.home.clone(),
        )
        .jj_binary_path(bin)
        .build();
        Replay {
            fx,
            db,
            orch,
            main_ws,
            branch_ws,
        }
    }

    /// Write `files` in a workspace, seal them, and return the bookmark's new
    /// commit.
    fn commit(
        fx: &Fixture,
        workspace: &Path,
        bookmark: &str,
        files: &[(&str, &str)],
        message: &str,
    ) -> String {
        // A store-side rebase that was rolled back still advanced the op log
        // under this workspace.
        let _ = crate::jj::update_stale(&fx.jj, workspace);
        for (path, content) in files {
            let target = workspace.join(path);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            std::fs::write(target, content).unwrap();
        }
        crate::jj::seal(&fx.jj, workspace, message, None).unwrap();
        crate::jj::bookmark_commit(&fx.jj, &fx.store, bookmark).unwrap()
    }

    /// Delete `path` in a workspace, seal, and return the bookmark's new
    /// commit.
    fn delete(fx: &Fixture, workspace: &Path, bookmark: &str, path: &str, message: &str) -> String {
        let _ = crate::jj::update_stale(&fx.jj, workspace);
        std::fs::remove_file(workspace.join(path)).unwrap();
        crate::jj::seal(&fx.jj, workspace, message, None).unwrap();
        crate::jj::bookmark_commit(&fx.jj, &fx.store, bookmark).unwrap()
    }

    fn branch_tip(r: &Replay) -> String {
        crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, BRANCH).unwrap()
    }

    /// Wait until no reconcile intent is held by a worker.
    ///
    /// The bound only turns a wedged worker into a failure instead of a hang.
    /// It is generous because a pass is a few dozen jj subprocesses, and on a
    /// loaded machine running the whole suite in parallel one pass has been
    /// measured past two minutes.
    async fn wait_for_worker(r: &Replay) {
        wait_for_reconcile_workers(&r.db).await;
        let diagnostic =
            r.db.query_text(
                "SELECT json_object(
                    'items', (SELECT json_group_array(json_object(
                        'intent', intent_id, 'status', status, 'failure', failure_kind,
                        'outcome', outcome_kind, 'diagnostic', last_diagnostic))
                        FROM jj_reconcile_items),
                    'comments', (SELECT json_group_array(content) FROM comments),
                    'messages', (SELECT json_group_array(content) FROM messages))",
                (),
            )
            .await
            .unwrap();
        eprintln!("reconcile worker outcomes: {diagnostic:?}");
    }

    pub(super) async fn wait_for_reconcile_workers(db: &LocalDb) {
        let started = std::time::Instant::now();
        while started.elapsed() < std::time::Duration::from_secs(600) {
            let running = db
                .query_text(
                    "SELECT CAST(COUNT(*) AS TEXT) FROM jj_reconcile_intents WHERE status = 'running'",
                    (),
                )
                .await
                .unwrap();
            if running.as_deref() == Some("0") {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        panic!(
            "the reconcile worker never released its intent within {:?}",
            started.elapsed()
        );
    }

    /// The automatic base advance a merge on main triggers.
    async fn automatic_advance(r: &Replay) {
        advance_siblings(
            r,
            vec![SiblingJob {
                id: JOB.to_string(),
                branch: Some(BRANCH.to_string()),
                base_commit: None,
            }],
        )
        .await;
    }

    /// An automatic advance over an explicit sibling set.
    async fn advance_siblings(r: &Replay, siblings: Vec<SiblingJob>) {
        let eligible = siblings.len();
        let notes = BaseAdvanceNotes {
            conflict: build_jj_conflict_note("main", None, None),
            clean: build_jj_clean_note("main", None, None),
            incoming: IncomingIdentity {
                base_branch: "main".to_string(),
                ..IncomingIdentity::default()
            },
        };
        let outcome = reconcile_base_advance(
            &r.orch,
            &r.db,
            "proj-1",
            "merged job (test)",
            &r.fx.repo.to_string_lossy(),
            "main",
            "main",
            siblings,
            notes,
        )
        .await
        .unwrap();
        assert_eq!(outcome.eligible, eligible, "the advance claimed the branch");
        wait_for_worker(r).await;
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn held_workspace_lookup_matches_repository_identity_not_branch_name() {
        let bin = crate::jj::tests::jj_bin().expect("real jj required for held workspace lookup");
        let r = replay_harness(&bin, "held-workspace-identity.db", &[(LIST, "base\n")]).await;
        let foreign = replay_harness(&bin, "foreign-held-workspace.db", &[(LIST, "base\n")]).await;
        let tip = branch_tip(&r);
        for (id, store, bookmark) in [
            ("owned-held", &r.fx.store, "agent/owned-held"),
            ("foreign-held", &foreign.fx.store, "agent/foreign-held"),
        ] {
            r.db.execute("INSERT INTO jj_reconcile_intents (id,project_id,store_path,target_branch,destination_commit,status,created_at,updated_at) VALUES (?1,'proj-1',?2,'main',?3,'completed',1,1)",
                params![id,store.to_string_lossy().as_ref(),tip.as_str()]).await.unwrap();
            r.db.execute("INSERT INTO jj_reconcile_items (intent_id,bookmark,status,updated_at,held_replay_source_cut,held_replay_source_tip) VALUES (?1,?2,'completed',1,?3,?3)",
                params![id,bookmark,tip.as_str()]).await.unwrap();
        }
        let held = held_replay_bookmarks_for_repository(&r.orch.db, &r.branch_ws)
            .await
            .unwrap();
        assert_eq!(held, ["agent/owned-held".to_string()].into_iter().collect());
        assert_eq!(
            crate::jj::repository_directory(&r.branch_ws),
            crate::jj::repository_directory(&r.fx.store)
        );
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn held_child_retargets_after_parent_merge_and_recovers_after_parent_deletion() {
        recover_retargeted_held_child(true).await;
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn held_child_retargets_while_the_merged_parent_bookmark_still_exists() {
        recover_retargeted_held_child(false).await;
    }

    async fn recover_retargeted_held_child(delete_parent: bool) {
        let bin = crate::jj::tests::jj_bin().expect("real jj required for held parent retarget");
        let r = replay_harness(&bin, "held-retarget.db", &[(LIST, "base\n")]).await;
        held_owner_snapshot(&r).await;
        let cut = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("parent.txt", "old parent\n")],
            "parent work",
        );
        let child = "agent/retarget-child";
        let child_ws = r.fx.workspaces.path().join("retarget-child");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &child_ws, child, BRANCH, None).unwrap();
        let held_tip = commit(
            &r.fx,
            &child_ws,
            child,
            &[(LIST, "child\n"), ("own.txt", "child own work\n")],
            "child work",
        );
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, child).unwrap();
        r.db.execute("INSERT INTO jobs (id,execution_id,recipe_node_id,issue_id,project_id,status,branch,base_branch,base_commit,created_at,updated_at)
            VALUES ('retarget-child','exec-1','child','issue-1','proj-1','running',?1,?2,?3,1,1)",
            params![child,BRANCH,cut.as_str()]).await.unwrap();
        let main = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "incoming\n")],
            "main changes child path",
        );
        set_main(&r.fx, &main);
        automatic_advance(&r).await;
        let original = held_replay_source(&r.db, &r.fx.store, child)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.cut.as_deref(), Some(cut.as_str()));
        assert_eq!(original.tip, held_tip);
        let old_session = crate::orchestrator::conflict_session::load_active_session(&r.db, child)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old_session.target_branch, BRANCH);
        assert_eq!(old_session.ours.as_deref(), Some(held_tip.as_str()));

        let child_ws = r.fx.workspaces.path().join("retarget-child-resolution");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &child_ws, child, child, None).unwrap();
        let resolution_tip = commit(
            &r.fx,
            &child_ws,
            child,
            &[(LIST, "incoming\n")],
            "commit child resolution",
        );
        let resolution_basis = old_session
            .resolution_basis_at(&r.fx.jj, &r.fx.store, &resolution_tip)
            .unwrap();
        assert!(
            !resolution_basis.is_empty(),
            "the open session carries actual committed-resolution evidence"
        );
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, child).unwrap();
        crate::jj::merge_into_bookmark(&r.fx.jj, &r.fx.store, "main", BRANCH).unwrap();
        let folded_main = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, "main").unwrap();
        let accepted_ws = r.fx.workspaces.path().join("accepted-main-landing");
        let accepted_branch = "agent/accepted-main-landing";
        crate::jj::add_workspace(
            &r.fx.jj,
            &r.fx.store,
            &accepted_ws,
            accepted_branch,
            &folded_main,
            None,
        )
        .unwrap();
        let destination = commit(
            &r.fx,
            &accepted_ws,
            accepted_branch,
            &[
                ("parent.txt", "accepted parent\n"),
                ("after-merge.txt", "new main work\n"),
            ],
            "accepted parent landing",
        );
        set_main(&r.fx, &destination);
        assert!(
            !crate::jj::revset_descends_from(&r.fx.jj, &r.fx.store, &destination, &cut),
            "the destination does not carry obsolete parent commit identity"
        );
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, BRANCH).unwrap();
        if delete_parent {
            let deleted = crate::env::command(&bin)
                .current_dir(&r.fx.store)
                .env("JJ_CONFIG", r.fx.home.join("jj/config.toml"))
                .args(["bookmark", "forget", BRANCH, "--ignore-working-copy"])
                .output()
                .unwrap();
            assert!(
                deleted.status.success(),
                "{}",
                String::from_utf8_lossy(&deleted.stderr)
            );
            let deleted_git = crate::env::git()
                .current_dir(&r.fx.repo)
                .args(["update-ref", "-d", &format!("refs/heads/{BRANCH}")])
                .output()
                .unwrap();
            assert!(deleted_git.status.success());
            assert!(crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, BRANCH).is_none());
        } else {
            assert_eq!(
                crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, BRANCH).as_deref(),
                Some(folded_main.as_str())
            );
        }

        let in_flight = claim_reconcile_intent(
            &r.db,
            "proj-1",
            &r.fx.store,
            "main",
            &destination,
            "existing main pass",
        )
        .await
        .unwrap()
        .unwrap();
        // A newer active destination row must not be overwritten, and refusal
        // rolls back the jobs and original obligation together.
        r.db.execute("INSERT INTO jj_reconcile_items (intent_id,bookmark,status,updated_at,resolution_state) VALUES (?1,?2,'completed',1,'open')",
            params![in_flight.id.as_str(),child]).await.unwrap();
        {
            let _guard = r
                .orch
                .acquire_jj_store_lock(
                    &r.fx.store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    "test held child retarget",
                )
                .await;
            assert!(repoint_children_of_merged_branch(
                &r.db,
                "proj-1",
                BRANCH,
                "main",
                &r.fx.store,
                &destination
            )
            .await
            .unwrap_err()
            .contains("newer destination authority"));
        }
        assert_eq!(
            held_replay_source(&r.db, &r.fx.store, child)
                .await
                .unwrap()
                .as_ref(),
            Some(&original)
        );
        assert_eq!(
            r.db.query_text("SELECT base_branch FROM jobs WHERE id='retarget-child'", ())
                .await
                .unwrap()
                .as_deref(),
            Some(BRANCH)
        );
        r.db.execute(
            "DELETE FROM jj_reconcile_items WHERE intent_id=?1 AND bookmark=?2",
            params![in_flight.id.as_str(), child],
        )
        .await
        .unwrap();
        {
            let _guard = r
                .orch
                .acquire_jj_store_lock(
                    &r.fx.store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    "test accepted parent retarget",
                )
                .await;
            repoint_children_of_merged_branch(
                &r.db,
                "proj-1",
                BRANCH,
                "main",
                &r.fx.store,
                &destination,
            )
            .await
            .unwrap();
        }
        let transported = held_replay_source(&r.db, &r.fx.store, child)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(transported.base_branch, "main");
        assert_eq!(transported.destination, destination);
        assert_eq!(transported.cut, original.cut);
        assert_eq!(transported.tip, original.tip);
        let historical = crate::orchestrator::conflict_session::load_active_session(&r.db, child)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(historical.intent_id, old_session.intent_id);
        assert_eq!(historical.target_branch, old_session.target_branch);
        assert_eq!(
            historical.destination_commit,
            old_session.destination_commit
        );
        assert_eq!(historical.theirs, old_session.theirs);
        assert_eq!(historical.resolution_basis, old_session.resolution_basis);
        assert_eq!(
            historical
                .resolution_basis_at(&r.fx.jj, &r.fx.store, &resolution_tip)
                .unwrap(),
            resolution_basis
        );
        assert_eq!(
            r.db.query_text(
                "SELECT lease_owner FROM jj_reconcile_intents WHERE id=?1",
                (in_flight.id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
            Some(in_flight.owner.as_str())
        );
        finish_reconcile_intent(&r.db, &in_flight.id, &in_flight.owner, false)
            .await
            .unwrap();
        assert_eq!(
            r.db.query_text(
                "SELECT status FROM jj_reconcile_intents WHERE id=?1",
                (in_flight.id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
            Some("pending"),
            "a pass that captured its siblings before retarget must leave the new item claimable"
        );

        request_branch_replay(&r.orch, &r.db, "retarget-child", child, None, true, None)
            .await
            .unwrap();
        wait_for_worker(&r).await;
        assert!(crate::jj::revset_descends_from(
            &r.fx.jj,
            &r.fx.store,
            child,
            &destination
        ));
        assert!(held_replay_source(&r.db, &r.fx.store, child)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            crate::jj::text_at_revision(&r.fx.jj, &r.fx.store, child, "own.txt")
                .unwrap()
                .as_deref(),
            Some("child own work\n")
        );
        assert_eq!(
            crate::jj::text_at_revision(&r.fx.jj, &r.fx.store, child, "parent.txt")
                .unwrap()
                .as_deref(),
            Some("accepted parent\n")
        );
        assert_eq!(
            crate::jj::text_at_revision(&r.fx.jj, &r.fx.store, child, "after-merge.txt")
                .unwrap()
                .as_deref(),
            Some("new main work\n")
        );
        // Simulate the consume-before-close window seen by a delayed request.
        r.db.execute("UPDATE jj_reconcile_items SET resolution_state='open' WHERE intent_id=?1 AND bookmark=?2",params![old_session.intent_id.as_str(),child]).await.unwrap();
        assert!(held_replay_source(&r.db, &r.fx.store, child)
            .await
            .unwrap()
            .is_none());
        let accepted_tip = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, child).unwrap();
        request_branch_replay(&r.orch, &r.db, "retarget-child", child, None, false, None)
            .await
            .unwrap();
        assert_eq!(
            crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, child).as_deref(),
            Some(accepted_tip.as_str())
        );
        assert_eq!(
            r.db.query_text("SELECT base_commit FROM jobs WHERE id='retarget-child'", ())
                .await
                .unwrap()
                .as_deref(),
            Some(destination.as_str()),
            "a historical session cannot regain routing authority after receipt discharge"
        );
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn held_ownership_routes_across_databases_sharing_a_physical_store() {
        cross_project_held_ownership(true).await;
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn held_ownership_routes_across_projects_in_one_database() {
        cross_project_held_ownership(false).await;
    }

    async fn cross_project_held_ownership(separate_database: bool) {
        let bin =
            crate::jj::tests::jj_bin().expect("real jj required for physical-store ownership");
        let r = replay_harness(&bin, "physical-owner-a.db", &[(LIST, "base\n")]).await;
        let other = replay_harness(&bin, "physical-owner-b.db", &[(LIST, "base\n")]).await;
        held_owner_snapshot(&r).await;
        let scoped = |suffix: &str| {
            if separate_database {
                format!("teamABC123~00000000-0000-4000-8000-{suffix}")
            } else {
                format!("local-{suffix}")
            }
        };
        let b_project = scoped("000000000001");
        let b_issue = scoped("000000000002");
        let b_execution = scoped("000000000003");
        let b_job = scoped("000000000004");
        let g_job = scoped("000000000005");
        let b_db = if separate_database {
            other.db.clone()
        } else {
            r.db.clone()
        };
        if separate_database {
            r.orch
                .db
                .register_team_db_for_test("teamABC123".to_string(), b_db.clone())
                .await;
            r.orch
                .db
                .set_route("project-b", Some("teamABC123".into()))
                .await;
        }
        if separate_database {
            r.db.execute("INSERT INTO teams (id,name,sync_url,replica_path,created_at) VALUES ('teamABC123','Held team','http://sync','/tmp/held-team.db',1)",()).await.unwrap();
            r.db.execute("INSERT INTO project_routes (project_key,team_id,local_repo_path,created_at) VALUES ('project-b','teamABC123',?1,1)",params![r.fx.repo.to_string_lossy().as_ref()]).await.unwrap();
        }
        let synced_repo = if separate_database {
            "/creator/machine/repository".to_string()
        } else {
            r.fx.repo.to_string_lossy().into_owned()
        };
        b_db.execute("INSERT INTO projects (id,workspace_id,name,key,repo_path,default_branch,created_at,updated_at)
            VALUES (?1,'default','Other owner','project-b',?2,?3,1,1)",params![b_project.as_str(),synced_repo.as_str(),BRANCH]).await.unwrap();
        b_db.execute(
            "INSERT INTO issues (id,project_id,number,title,status,created_at,updated_at)
            VALUES (?1,?2,1,'Other owner','active',1,1)",
            params![b_issue.as_str(), b_project.as_str()],
        )
        .await
        .unwrap();
        let mut snapshot: crate::models::ExecutionSnapshot = serde_json::from_str(
            &crate::test_support::shipping_execution_snapshot_json(&b_issue, &b_project),
        )
        .unwrap();
        let mut prototype = snapshot
            .recipe
            .nodes
            .iter()
            .find(|node| node.agent_config.is_some())
            .unwrap()
            .clone();
        prototype.agent_config.as_mut().unwrap().git_config = Some(crate::models::AgentGitConfig {
            branch_mode: crate::models::BranchMode::Isolate,
            ..Default::default()
        });
        snapshot.recipe.nodes = ["independent-b", "grandchild-b"]
            .into_iter()
            .map(|id| {
                let mut node = prototype.clone();
                node.id = id.to_string();
                node
            })
            .collect();
        b_db.execute("INSERT INTO executions (id,recipe_id,issue_id,project_id,status,started_at,seq,snapshot)
            VALUES (?1,'recipe-default',?2,?3,'running',1,1,?4)",params![b_execution.as_str(),b_issue.as_str(),b_project.as_str(),serde_json::to_string(&snapshot).unwrap()]).await.unwrap();

        let parent_cut = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("parent.txt", "parent\n")],
            "parent work",
        );
        let c = "agent/physical-owner-c";
        let c_ws = r.fx.workspaces.path().join("physical-owner-c");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &c_ws, c, BRANCH, None).unwrap();
        let c_cut = commit(&r.fx, &c_ws, c, &[(LIST, "C own conflict\n")], "C work");
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, c).unwrap();
        r.db.execute("INSERT INTO jobs (id,execution_id,recipe_node_id,issue_id,project_id,status,branch,base_branch,base_commit,created_at,updated_at)
            VALUES ('physical-c','exec-1','child','issue-1','proj-1','running',?1,?2,?3,1,1)",params![c,BRANCH,c_cut.as_str()]).await.unwrap();
        // C's mint cut is the original parent, not its own sealed tip.
        r.db.execute(
            "UPDATE jobs SET base_commit=?1 WHERE id='physical-c'",
            params![parent_cut.as_str()],
        )
        .await
        .unwrap();
        let observer = scoped("000000000008");
        b_db.execute("INSERT INTO jobs (id,project_id,status,branch,base_branch,base_commit,created_at,updated_at) VALUES (?1,?2,'running',?3,?3,?4,1,1)",
            params![observer.as_str(),b_project.as_str(),c,c_cut.as_str()]).await.unwrap();
        r.db.execute("INSERT INTO workflow_run (run_id,job_id,workflow_id,project_id,anchor_branch,branch_mode,package_path,script_path,args_json,output_name,created_at) VALUES (?1,?2,'observer',?3,?4,'inherit','/workflow','/workflow/main.ts','{}','result',1)",
            params![scoped("000000000009"),observer.as_str(),b_project.as_str(),c]).await.unwrap();
        let independent = "agent/physical-owner-b";
        let b_ws = r.fx.workspaces.path().join("physical-owner-b");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &b_ws, independent, BRANCH, None).unwrap();
        let b_tip = commit(
            &r.fx,
            &b_ws,
            independent,
            &[(LIST, "B own conflict\n")],
            "B work",
        );
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, independent).unwrap();
        let grandchild = "agent/physical-owner-g";
        let g_ws = r.fx.workspaces.path().join("physical-owner-g");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &g_ws, grandchild, c, None).unwrap();
        let g_tip = commit(
            &r.fx,
            &g_ws,
            grandchild,
            &[("grandchild.txt", "G own work\n")],
            "G work",
        );
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, grandchild).unwrap();
        for (id, node, branch, base, cut) in [
            (
                b_job.as_str(),
                "independent-b",
                independent,
                BRANCH,
                parent_cut.as_str(),
            ),
            (
                g_job.as_str(),
                "grandchild-b",
                grandchild,
                c,
                c_cut.as_str(),
            ),
        ] {
            b_db.execute("INSERT INTO jobs (id,execution_id,recipe_node_id,issue_id,project_id,status,branch,base_branch,base_commit,created_at,updated_at)
                VALUES (?1,?6,?2,?7,?8,'running',?3,?4,?5,1,1)",params![id,node,branch,base,cut,b_execution.as_str(),b_issue.as_str(),b_project.as_str()]).await.unwrap();
            b_db.execute(
                "INSERT INTO runs (id,issue_id,project_id,job_id,status,created_at,updated_at)
                VALUES (?1,?3,?4,?2,'live',1,1)",
                params![
                    scoped(if id == b_job {
                        "000000000006"
                    } else {
                        "000000000007"
                    }),
                    id,
                    b_issue.as_str(),
                    b_project.as_str()
                ],
            )
            .await
            .unwrap();
        }
        let main = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "incoming\n")],
            "main advance",
        );
        set_main(&r.fx, &main);
        automatic_advance(&r).await;
        let b_source = held_replay_source(&b_db, &r.fx.store, independent)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b_source.cut.as_deref(), Some(parent_cut.as_str()));
        assert_eq!(b_source.tip, b_tip);
        let b_session =
            crate::orchestrator::conflict_session::load_active_session(&b_db, independent)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(b_session.target_branch, BRANCH);
        assert_eq!(
            b_db.query_text(
                "SELECT project_id FROM jj_reconcile_intents WHERE id=?1",
                (b_session.intent_id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
            Some(b_project.as_str())
        );
        assert_eq!(r.db.query_text("SELECT CAST(COUNT(*) AS TEXT) FROM jj_reconcile_items i JOIN jj_reconcile_intents n ON n.id=i.intent_id WHERE n.project_id='proj-1' AND i.bookmark=?1",(independent,)).await.unwrap().as_deref(),Some("0"));
        if separate_database {
            assert!(held_replay_source(&r.db, &r.fx.store, independent)
                .await
                .unwrap()
                .is_none());
        }
        let g_source = held_replay_source(&b_db, &r.fx.store, grandchild)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            b_db.query_text(
                "SELECT project_id FROM jj_reconcile_intents WHERE id=?1",
                (g_source.intent_id.clone(),)
            )
            .await
            .unwrap()
            .as_deref(),
            Some(b_project.as_str())
        );
        assert_eq!(g_source.cut.as_deref(), Some(c_cut.as_str()));
        assert!(
            held_replay_source_cut(&r.orch, &b_db, &r.fx.jj, &r.fx.store, grandchild, c)
                .await
                .unwrap_err()
                .contains("waits for immediate parent"),
            "a parent session in another database is still authoritative in this physical store"
        );
        assert!(
            crate::orchestrator::conflict_session::load_active_session(&b_db, grandchild)
                .await
                .unwrap()
                .is_none(),
            "the full held map blocks G behind C across ownership boundaries"
        );
        let foreign_tip = branch_tip(&other);
        other.db.execute("INSERT INTO jj_reconcile_intents (id,project_id,store_path,target_branch,destination_commit,status,created_at,updated_at) VALUES ('unrelated-held','proj-1',?1,'main',?2,'completed',1,1)",params![other.fx.store.to_string_lossy().as_ref(),foreign_tip.as_str()]).await.unwrap();
        other.db.execute("INSERT INTO jj_reconcile_items (intent_id,bookmark,status,updated_at,held_replay_source_cut,held_replay_source_tip) VALUES ('unrelated-held','agent/unrelated-held','completed',1,?1,?1)",params![foreign_tip.as_str()]).await.unwrap();
        let union = held_replay_bookmarks_for_repository(&r.orch.db, &r.fx.store)
            .await
            .unwrap();
        assert!(union.contains(independent) && union.contains(grandchild));
        assert!(
            !union.contains("agent/unrelated-held"),
            "another database's unrelated physical store is excluded"
        );

        let recovery_ws = r.fx.workspaces.path().join("physical-c-resolution");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &recovery_ws, c, c, None).unwrap();
        commit(
            &r.fx,
            &recovery_ws,
            c,
            &[
                (LIST, "incoming\n"),
                ("after-resolution.txt", "C later work\n"),
            ],
            "resolve C and add work",
        );
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, c).unwrap();
        request_branch_replay(&r.orch, &r.db, "physical-c", c, None, true, None)
            .await
            .unwrap();
        wait_for_worker(&r).await;
        let c_after = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, c).unwrap();
        assert!(crate::jj::revset_descends_from(
            &r.fx.jj,
            &r.fx.store,
            c,
            &main
        ));
        assert_eq!(
            crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, grandchild).as_deref(),
            Some(g_tip.as_str()),
            "A's clean implicit rewrite must preserve B's original cut"
        );
        assert_eq!(
            held_replay_source(&b_db, &r.fx.store, grandchild)
                .await
                .unwrap()
                .unwrap()
                .cut
                .as_deref(),
            Some(c_cut.as_str())
        );
        // The same team overlay must route automatic publication, not only an
        // explicit request. A completed merge into C advances its live child G.
        let merged_job = scoped("000000000010");
        b_db.execute("INSERT INTO jobs (id,project_id,status,base_branch,created_at,updated_at) VALUES (?1,?2,'complete',?3,1,1)",params![merged_job.as_str(),b_project.as_str(),c]).await.unwrap();
        notify_downstream_of_base_advance(&r.orch, &merged_job)
            .await
            .unwrap();
        wait_for_reconcile_workers(&b_db).await;
        assert!(
            crate::jj::revset_descends_from(&r.fx.jj, &r.fx.store, grandchild, &c_after),
            "automatic publication must use the machine-local team repository"
        );
        assert!(held_replay_source(&b_db, &r.fx.store, grandchild)
            .await
            .unwrap()
            .is_none());
        request_branch_replay(&r.orch, &b_db, &g_job, grandchild, None, false, None)
            .await
            .unwrap();
        assert_eq!(b_db.query_text("SELECT project_id FROM jj_reconcile_intents WHERE store_path=?1 AND target_branch=?2 AND destination_commit=?3 AND project_id=?4",params![r.fx.store.to_string_lossy().as_ref(),c,c_after.as_str(),b_project.as_str()]).await.unwrap().as_deref(),Some(b_project.as_str()));
        assert!(crate::jj::revset_descends_from(
            &r.fx.jj,
            &r.fx.store,
            grandchild,
            &c_after
        ));
        assert!(held_replay_source(&b_db, &r.fx.store, grandchild)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            crate::jj::text_at_revision(&r.fx.jj, &r.fx.store, grandchild, "grandchild.txt")
                .unwrap()
                .as_deref(),
            Some("G own work\n")
        );
        assert_eq!(
            crate::jj::text_at_revision(&r.fx.jj, &r.fx.store, grandchild, "after-resolution.txt")
                .unwrap()
                .as_deref(),
            Some("C later work\n")
        );
    }

    async fn held_owner_snapshot(r: &Replay) {
        let mut snapshot: crate::models::ExecutionSnapshot = serde_json::from_str(
            &crate::test_support::shipping_execution_snapshot_json("issue-1", "proj-1"),
        )
        .unwrap();
        let mut prototype = snapshot
            .recipe
            .nodes
            .iter()
            .find(|node| node.agent_config.is_some())
            .unwrap()
            .clone();
        prototype.agent_config.as_mut().unwrap().git_config = Some(crate::models::AgentGitConfig {
            branch_mode: crate::models::BranchMode::Isolate,
            ..Default::default()
        });
        snapshot.recipe.nodes = ["node", "child", "grandchild"]
            .into_iter()
            .map(|id| {
                let mut node = prototype.clone();
                node.id = id.to_string();
                node
            })
            .collect();
        r.db.execute(
            "UPDATE executions SET snapshot=?1 WHERE id='exec-1'",
            (serde_json::to_string(&snapshot).unwrap(),),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn clean_parent_advance_opens_only_the_held_childs_session() {
        let bin =
            crate::jj::tests::jj_bin().expect("real jj required for held-child session regression");
        let r = replay_harness(&bin, "held-child-session.db", &[(LIST, "base\n")]).await;
        held_owner_snapshot(&r).await;
        let parent_before = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("parent.txt", "parent\n")],
            "parent work",
        );
        let child = "agent/child";
        let child_ws = r.fx.workspaces.path().join("child");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &child_ws, child, BRANCH, None).unwrap();
        let child_before = commit(&r.fx, &child_ws, child, &[(LIST, "child\n")], "child work");
        let grandchild = "agent/grandchild";
        let grandchild_ws = r.fx.workspaces.path().join("grandchild");
        crate::jj::add_workspace(
            &r.fx.jj,
            &r.fx.store,
            &grandchild_ws,
            grandchild,
            child,
            None,
        )
        .unwrap();
        let grandchild_before = commit(
            &r.fx,
            &grandchild_ws,
            grandchild,
            &[("grandchild.txt", "grandchild\n")],
            "grandchild work",
        );
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, grandchild).unwrap();
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, child).unwrap();
        r.db.execute_script(format!(
            "INSERT INTO jobs
             (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
             VALUES ('job-child', 'exec-1', 'child', 'issue-1', 'proj-1', 'running', '{child}', '{BRANCH}', 1, 1);
             INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
             VALUES ('run-child', 'issue-1', 'proj-1', 'job-child', 'live', 1, 1);
             INSERT INTO jobs
             (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
             VALUES ('job-grandchild', 'exec-1', 'grandchild', 'issue-1', 'proj-1', 'running', '{grandchild}', '{child}', 1, 1);
             INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
             VALUES ('run-grandchild', 'issue-1', 'proj-1', 'job-grandchild', 'live', 1, 1);"
        )).await.unwrap();
        r.db.execute(
            "UPDATE jobs SET base_commit=?1 WHERE id='job-child'",
            (parent_before.as_str(),),
        )
        .await
        .unwrap();
        r.db.execute(
            "UPDATE jobs SET base_commit=?1 WHERE id='job-grandchild'",
            (child_before.as_str(),),
        )
        .await
        .unwrap();
        r.db.execute(
            "INSERT INTO jobs (id, project_id, parent_job_id, status, branch, base_branch, base_commit, created_at, updated_at)
             VALUES ('inherited-observer','proj-1','job-grandchild','running',?1,?2,?3,1,1)",
            params![grandchild, child, parent_before.as_str()],
        ).await.unwrap();
        let main = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "incoming\n")],
            "main changes child path",
        );
        set_main(&r.fx, &main);
        automatic_advance(&r).await;

        let parent_after = branch_tip(&r);
        let source = held_replay_source(&r.db, &r.fx.store, grandchild)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            source.cut.as_deref(),
            Some(child_before.as_str()),
            "inherited observer is not a cut owner"
        );
        assert_eq!(source.tip, grandchild_before);
        assert!(
            refuse_held_continuity(&r.db, &r.fx.jj, &r.fx.store, &grandchild_before)
                .await
                .unwrap_err()
                .contains("held branch")
        );
        assert!(crate::jj::revset_descends_from(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &main
        ));
        assert_eq!(
            crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, child).as_deref(),
            Some(child_before.as_str())
        );
        assert!(!crate::jj::branch_has_conflict(&r.fx.jj, &r.fx.store, child).unwrap());
        assert!(
            crate::orchestrator::conflict_session::load_active_session(&r.db, BRANCH)
                .await
                .unwrap()
                .is_none()
        );
        let session = crate::orchestrator::conflict_session::load_active_session(&r.db, child)
            .await
            .unwrap()
            .expect("the held child owns an independent session");
        assert_eq!(session.ours.as_deref(), Some(child_before.as_str()));
        assert_eq!(session.theirs.as_deref(), Some(parent_after.as_str()));
        assert_eq!(session.incoming.base_branch, BRANCH);
        assert_eq!(session.target_branch, BRANCH);
        assert_eq!(session.destination_commit, parent_after);
        // Storing one held child is not a verdict for the whole destination:
        // the same parent advance must still service another sibling.
        let sibling = "agent/same-destination";
        let sibling_ws = r.fx.workspaces.path().join("same-destination");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &sibling_ws, sibling, BRANCH, None)
            .unwrap();
        commit(
            &r.fx,
            &sibling_ws,
            sibling,
            &[("sibling.txt", "sibling\n")],
            "sibling work",
        );
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, sibling).unwrap();
        r.db.execute(
            "INSERT INTO jobs (id, project_id, status, branch, base_branch, created_at, updated_at)
             VALUES ('job-same-destination', 'proj-1', 'running', ?1, ?2, 1, 1)",
            params![sibling, BRANCH],
        )
        .await
        .unwrap();
        let serviced = reconcile_base_advance(
            &r.orch,
            &r.db,
            "proj-1",
            "same destination",
            &r.fx.repo.to_string_lossy(),
            BRANCH,
            BRANCH,
            vec![SiblingJob {
                id: "job-same-destination".to_string(),
                branch: Some(sibling.to_string()),
                base_commit: None,
            }],
            BaseAdvanceNotes {
                conflict: build_jj_conflict_note(BRANCH, None, None),
                clean: build_jj_clean_note(BRANCH, None, None),
                incoming: IncomingIdentity {
                    base_branch: BRANCH.to_string(),
                    ..IncomingIdentity::default()
                },
            },
        )
        .await
        .unwrap();
        assert_eq!(
            serviced.eligible, 1,
            "the session-only intent does not coalesce away a sibling pass"
        );
        wait_for_worker(&r).await;
        assert_eq!(
            r.db.query_opt_text(
                "SELECT base_commit FROM jobs WHERE id = 'job-same-destination'",
                ()
            )
            .await
            .unwrap()
            .as_deref(),
            Some(parent_after.as_str())
        );
        assert!(
            crate::orchestrator::conflict_session::load_active_session(&r.db, grandchild)
                .await
                .unwrap()
                .is_none(),
            "a blocked grandchild must not resolve onto the ancestor"
        );
        assert_eq!(
            crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, grandchild).as_deref(),
            Some(grandchild_before.as_str())
        );
        // Resolve the child on its held clean tip, then add work that did not
        // exist at the ancestor's advance. Recovery must inherit both.
        let recovery_ws = r.fx.workspaces.path().join("child-recovery");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &recovery_ws, child, child, None).unwrap();
        commit(
            &r.fx,
            &recovery_ws,
            child,
            &[(LIST, "incoming\n")],
            "resolve child",
        );
        commit(
            &r.fx,
            &recovery_ws,
            child,
            &[("after-resolution.txt", "child later work\n")],
            "child later work",
        );
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, child).unwrap();
        request_branch_replay(&r.orch, &r.db, "job-child", child, None, true, None)
            .await
            .unwrap();
        wait_for_worker(&r).await;
        let child_after = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, child).unwrap();
        assert!(crate::jj::revset_descends_from(
            &r.fx.jj,
            &r.fx.store,
            child,
            &parent_after
        ));
        let expected_source = held_replay_source(&r.db, &r.fx.store, grandchild)
            .await
            .unwrap()
            .unwrap();
        // Closing the owning intent cannot erase an unconsumed branch obligation.
        r.db.execute("UPDATE jj_reconcile_intents SET status='completed' WHERE id IN (SELECT intent_id FROM jj_reconcile_items WHERE bookmark=?1)", (grandchild,)).await.unwrap();
        assert_eq!(
            held_replay_source(&r.db, &r.fx.store, grandchild)
                .await
                .unwrap()
                .as_ref(),
            Some(&expected_source)
        );
        r.db.execute("UPDATE jj_reconcile_items SET held_replay_source_cut=NULL WHERE bookmark=?1 AND held_replay_source_tip IS NOT NULL", (grandchild,)).await.unwrap();
        assert!(
            held_replay_source_cut(&r.orch, &r.db, &r.fx.jj, &r.fx.store, grandchild, child)
                .await
                .unwrap_err()
                .contains("no proven")
        );
        assert!(held_replay_source(&r.db, &r.fx.store, grandchild)
            .await
            .unwrap()
            .unwrap()
            .cut
            .is_none());
        r.db.execute("UPDATE jj_reconcile_items SET held_replay_source_cut=?1 WHERE bookmark=?2 AND held_replay_source_tip IS NOT NULL", params![child_before.as_str(),grandchild]).await.unwrap();

        let preview_op = crate::jj::operation_id(&r.fx.jj, &r.fx.store).unwrap();
        crate::jj::rebase_branch_onto_from_cut(
            &r.fx.jj,
            &r.fx.store,
            grandchild,
            &child_after,
            &child_before,
        )
        .unwrap();
        assert_eq!(
            held_replay_source(&r.db, &r.fx.store, grandchild)
                .await
                .unwrap()
                .as_ref(),
            Some(&expected_source),
            "preview preserves receipt values"
        );
        let mut stale = expected_source.clone();
        stale.tip = parent_before.clone();
        clear_held_replay_source(
            &r.db,
            &r.fx.jj,
            &r.fx.store,
            grandchild,
            &child_after,
            grandchild,
            Some(&stale),
        )
        .await
        .unwrap();
        assert_eq!(
            held_replay_source(&r.db, &r.fx.store, grandchild)
                .await
                .unwrap()
                .as_ref(),
            Some(&expected_source),
            "stale generation cannot consume a newer receipt"
        );
        crate::jj::restore_operation(&r.fx.jj, &r.fx.store, &preview_op).unwrap();
        request_branch_replay(
            &r.orch,
            &r.db,
            "job-grandchild",
            grandchild,
            None,
            false,
            None,
        )
        .await
        .unwrap();
        wait_for_worker(&r).await;
        assert!(
            crate::jj::revset_descends_from(&r.fx.jj, &r.fx.store, grandchild, &child_after),
            "grandchild recovery follows the resolved immediate parent"
        );
        assert!(
            held_replay_source(&r.db, &r.fx.store, grandchild)
                .await
                .unwrap()
                .is_none(),
            "accepted exact replay consumes the outstanding generation"
        );
        let grandchild_after =
            crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, grandchild).unwrap();
        assert_eq!(
            crate::jj::text_at_revision(
                &r.fx.jj,
                &r.fx.store,
                &grandchild_after,
                "after-resolution.txt"
            )
            .unwrap()
            .as_deref(),
            Some("child later work\n")
        );
        assert_eq!(
            crate::jj::text_at_revision(&r.fx.jj, &r.fx.store, &grandchild_after, "grandchild.txt")
                .unwrap()
                .as_deref(),
            Some("grandchild\n")
        );
    }

    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn held_session_installation_precedes_queued_parent_writer() {
        let bin = crate::jj::tests::jj_bin()
            .expect("real jj required for pinned held-session coordinates");
        let r = replay_harness(&bin, "held-pinned-parent.db", &[(LIST, "base\n")]).await;
        held_owner_snapshot(&r).await;
        let parent_before = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("parent.txt", "parent\n")],
            "parent work",
        );
        let child = "agent/clean-child";
        let child_ws = r.fx.workspaces.path().join("clean-child");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &child_ws, child, BRANCH, None).unwrap();
        let child_before = commit(
            &r.fx,
            &child_ws,
            child,
            &[("child.txt", "child\n")],
            "child work",
        );
        let grandchild = "agent/held-grandchild";
        let grandchild_ws = r.fx.workspaces.path().join("held-grandchild");
        crate::jj::add_workspace(
            &r.fx.jj,
            &r.fx.store,
            &grandchild_ws,
            grandchild,
            child,
            None,
        )
        .unwrap();
        let ours = commit(
            &r.fx,
            &grandchild_ws,
            grandchild,
            &[(LIST, "grandchild\n")],
            "grandchild conflict",
        );
        crate::jj::forget_workspace(&r.fx.jj, &r.fx.store, grandchild).unwrap();
        for (id, branch, base) in [
            ("clean-child-job", child, BRANCH),
            ("held-grandchild-job", grandchild, child),
        ] {
            r.db.execute(
                "INSERT INTO jobs (id, project_id, status, branch, base_branch, created_at, updated_at)
                 VALUES (?1, 'proj-1', 'running', ?2, ?3, 1, 1)",
                params![id, branch, base],
            ).await.unwrap();
        }
        r.db.execute("UPDATE jobs SET execution_id='exec-1', recipe_node_id='child', base_commit=?1 WHERE id='clean-child-job'",
            (parent_before.as_str(),)).await.unwrap();
        r.db.execute("UPDATE jobs SET execution_id='exec-1', recipe_node_id='grandchild', base_commit=?1 WHERE id='held-grandchild-job'",
            (child_before.as_str(),)).await.unwrap();
        let main = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "incoming\n")],
            "incoming conflict",
        );
        set_main(&r.fx, &main);
        let guard = r
            .orch
            .acquire_jj_store_lock(
                &r.fx.store,
                crate::orchestrator::StoreLockClass::Foreground,
                "pin held coordinates",
            )
            .await;
        let held =
            match crate::jj::rebase_branch_onto(&r.fx.jj, &r.fx.store, BRANCH, "main").unwrap() {
                crate::jj::RebaseOutcome::Rebased { held_descendants } => held_descendants,
                other => panic!("parent must advance cleanly: {other:?}"),
            };
        assert!(held.contains_key(grandchild));
        assert!(
            !held.contains_key(child),
            "the immediate parent advanced cleanly"
        );
        let failed_tip = held[grandchild].conflicted_tip.clone();
        let child_at_replay = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, child).unwrap();
        // Poll the queued writer before installation. It cannot acquire this
        // guard until the owning replay has established the held session.
        let writer = async {
            let writer_guard = r
                .orch
                .acquire_jj_store_lock(
                    &r.fx.store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    "queued parent writer",
                )
                .await;
            let pinned =
                crate::orchestrator::conflict_session::load_active_session(&r.db, grandchild)
                    .await
                    .unwrap()
                    .expect("a queued writer must observe the held session");
            assert_eq!(pinned.target_branch, child);
            assert_eq!(pinned.incoming.base_branch, child);
            assert_eq!(pinned.destination_commit, child_at_replay);
            assert_eq!(pinned.theirs.as_deref(), Some(child_at_replay.as_str()));
            assert_eq!(pinned.ours.as_deref(), Some(ours.as_str()));
            assert_eq!(pinned.conflicted_tip, failed_tip);
            let later = commit(
                &r.fx,
                &child_ws,
                child,
                &[("later.txt", "new destination work\n")],
                "parent advances again",
            );
            drop(writer_guard);
            later
        };
        let install = async {
            tokio::task::yield_now().await;
            persist_held_descendant_sessions(&r.orch, &r.db, "proj-1", &r.fx.store, &held)
                .await
                .unwrap();
            drop(guard);
        };
        let (child_later, ()) = tokio::join!(writer, install);
        assert_ne!(child_at_replay, child_later);
        let pinned = crate::orchestrator::conflict_session::load_active_session(&r.db, grandchild)
            .await
            .unwrap()
            .expect("parent-only movement preserves the recovery obligation");
        assert_eq!(pinned.destination_commit, child_at_replay);
        assert_eq!(pinned.conflicted_tip, failed_tip);
        request_branch_replay(
            &r.orch,
            &r.db,
            "held-grandchild-job",
            grandchild,
            None,
            false,
            None,
        )
        .await
        .unwrap();
        wait_for_worker(&r).await;
        let current = crate::orchestrator::conflict_session::load_active_session(&r.db, grandchild)
            .await
            .unwrap()
            .expect("the fresh replay records the remaining real conflict");
        assert_eq!(current.target_branch, child);
        assert_eq!(current.destination_commit, child_later);
        assert_eq!(current.theirs.as_deref(), Some(child_later.as_str()));
    }

    /// Conflict notes queued for `BRANCH`, as (message id, wake level).
    async fn conflict_wakes(r: &Replay) -> Vec<(String, String)> {
        let pattern = format!("reconcile:%:{BRANCH}:conflicted%");
        r.db.read(move |conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT m.id, p.wake FROM messages m
                           JOIN attention_pushes p ON p.key = 'direct:' || m.id
                          WHERE m.id LIKE ?1 ORDER BY m.created_at, m.id",
                        params![pattern.as_str()],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((row.text(0)?, row.text(1)?));
                }
                Ok(out)
            })
        })
        .await
        .unwrap()
    }

    async fn pending_clean_notes(r: &Replay) -> i64 {
        r.db.query_one(
            "SELECT COUNT(*) FROM attention_pushes
              WHERE delivered_event_id IS NULL AND retired_at IS NULL AND key LIKE ?1",
            params![format!("direct:reconcile:%:{BRANCH}:clean")],
            |row| row.i64(0),
        )
        .await
        .unwrap()
    }

    /// Erase every conflict note for `BRANCH`: the state a pass leaves behind
    /// when it records the session and quarantine and then dies before notifying.
    async fn lose_conflict_wakes(r: &Replay) {
        let pattern = format!("reconcile:%:{BRANCH}:conflicted%");
        r.db.execute(
            "DELETE FROM attention_pushes WHERE key LIKE 'direct:' || ?1",
            params![pattern.as_str()],
        )
        .await
        .unwrap();
        r.db.execute(
            "DELETE FROM messages WHERE id LIKE ?1",
            params![pattern.as_str()],
        )
        .await
        .unwrap();
    }

    /// A parked conflict is silent only once its agent was told. With the note
    /// lost, the next advance replays the branch, re-records the session and
    /// wakes the agent; with the note queued, the advance after that parks it.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn an_open_conflict_session_wakes_its_agent_even_when_its_note_was_lost() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let r = replay_harness(&bin, "conflict-wake.db", &[(LIST, "head\n")]).await;
        let tip = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(LIST, "head\nX\n")],
            "branch appends X",
        );
        let d1 = commit(&r.fx, &r.main_ws, MAIN_LINE, &[(LIST, "head\nA\n")], "D1");
        set_main(&r.fx, &d1);
        automatic_advance(&r).await;

        assert!(open_session(&r).await.is_some(), "D1 conflicts");
        let first = conflict_wakes(&r).await;
        assert_eq!(first.len(), 1, "the session's note is queued: {first:?}");
        assert_ne!(first[0].1, "passive", "a conflict note wakes its agent");

        lose_conflict_wakes(&r).await;
        let d2 = commit(&r.fx, &r.main_ws, MAIN_LINE, &[("other.txt", "o\n")], "D2");
        set_main(&r.fx, &d2);
        automatic_advance(&r).await;

        assert_eq!(branch_tip(&r), tip, "the replay conflicted and rolled back");
        let session = open_session(&r).await.expect("the session is re-recorded");
        assert_eq!(session.theirs.as_deref(), Some(d2.as_str()));
        let resent = conflict_wakes(&r).await;
        assert_eq!(
            resent.len(),
            1,
            "a parked branch whose note was lost is woken again: {resent:?}"
        );
        assert!(
            resent[0].0.contains(&session.intent_id),
            "the note belongs to the session now open"
        );

        let d3 = commit(&r.fx, &r.main_ws, MAIN_LINE, &[("third.txt", "t\n")], "D3");
        set_main(&r.fx, &d3);
        automatic_advance(&r).await;
        assert_eq!(
            conflict_wakes(&r).await,
            resent,
            "once its agent was told, an unchanged conflicted tip stays parked"
        );
        assert_eq!(
            open_session(&r).await.map(|session| session.intent_id),
            Some(session.intent_id),
            "a parked branch is not replayed again"
        );
    }

    /// A branch two jobs share is replayed once per pass, its conflict stays
    /// recorded as a conflict, and the clean note an earlier advance left
    /// pending is withdrawn before it can contradict the blocking one.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_conflict_withdraws_pending_clean_notes_and_survives_a_shared_branch() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let r = replay_harness(&bin, "conflict-shared.db", &[(LIST, "head\n")]).await;
        let both = || {
            [JOB, "job-replay-earlier"]
                .into_iter()
                .map(|id| SiblingJob {
                    id: id.to_string(),
                    branch: Some(BRANCH.to_string()),
                    base_commit: None,
                })
                .collect::<Vec<_>>()
        };
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(LIST, "head\nX\n")],
            "branch appends X",
        );
        let d1 = commit(&r.fx, &r.main_ws, MAIN_LINE, &[("other.txt", "o\n")], "D1");
        set_main(&r.fx, &d1);
        advance_siblings(&r, both()).await;
        assert_eq!(pending_clean_notes(&r).await, 1, "D1 replays cleanly");

        let d2 = commit(&r.fx, &r.main_ws, MAIN_LINE, &[(LIST, "head\nA\n")], "D2");
        set_main(&r.fx, &d2);
        advance_siblings(&r, both()).await;

        let session = open_session(&r).await.expect("D2 conflicts");
        let outcome = r
            .db
            .query_text(
                "SELECT outcome_kind FROM jj_reconcile_items WHERE intent_id = ?1 AND bookmark = ?2",
                params![session.intent_id.as_str(), BRANCH],
            )
            .await
            .unwrap();
        assert_eq!(
            outcome.as_deref(),
            Some("conflicted"),
            "a second visit in the same pass must not rewrite the conflict as parked"
        );
        assert_eq!(conflict_wakes(&r).await.len(), 1, "the agent is woken");
        assert_eq!(
            pending_clean_notes(&r).await,
            0,
            "no clean note is left to deliver while the session is open"
        );
    }

    async fn request_take_committed_tip(r: &Replay) -> String {
        let answer = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
            .await
            .unwrap_or_else(|refusal| panic!("the request was refused: {refusal}"));
        wait_for_worker(r).await;
        answer
    }

    async fn open_session(
        r: &Replay,
    ) -> Option<crate::orchestrator::conflict_session::ConflictSession> {
        crate::orchestrator::conflict_session::load_active_session(&r.db, BRANCH)
            .await
            .unwrap()
    }

    /// Scenario 1's shared start: the branch appends X, D1 appends A at the
    /// same spot, the advance records a session, and the agent commits the
    /// resolution A-then-X as an ordinary linear commit on the old fork.
    async fn same_spot_resolution(bin: &str, name: &str) -> (Replay, String, String) {
        let r = replay_harness(bin, name, &[(LIST, "head\n")]).await;
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(LIST, "head\nX\n")],
            "branch appends X",
        );
        let d1 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "head\nA\n")],
            "D1 appends A",
        );
        set_main(&r.fx, &d1);
        // Setup's conflicting replay must reach its stage, a recorded session,
        // whatever fixture I/O costs: under a loaded full suite its production
        // 10 s was spent in the rollback's export verification and no session
        // was recorded (59042442). The non-starving budget covers only this
        // setup replay and is removed before the test body, which keeps the
        // production budget (and its explicit override) for what it tests.
        let setup_budget = (r.fx.store.clone(), BRANCH.to_string());
        AUTOMATIC_REPLAY_TEST_BUDGETS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(setup_budget.clone(), std::time::Duration::from_secs(120));
        automatic_advance(&r).await;
        AUTOMATIC_REPLAY_TEST_BUDGETS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .remove(&setup_budget);
        let session = open_session(&r)
            .await
            .expect("D1 conflicts and records a session");
        assert_eq!(
            session
                .conflicting()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec![LIST]
        );
        assert_eq!(session.theirs.as_deref(), Some(d1.as_str()));
        let resolved = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(LIST, "head\nA\nX\n")],
            "resolve against D1",
        );
        (r, d1, resolved)
    }

    /// The refusal shape scenario 1 requires when D2 appends at the spot the
    /// agent resolved: the branch is untouched, D2 is not in its ancestry, and
    /// the fresh session against D2 still knows the tip was resolved against
    /// D1, so the agent's next resolution is judged correctly.
    async fn assert_refused_with_fresh_session(r: &Replay, d1: &str, d2: &str, tip: &str) {
        assert_eq!(
            branch_tip(r),
            tip,
            "a refused replay leaves the branch untouched"
        );
        assert!(
            !crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, d2),
            "no tree landed on D2"
        );
        let session = open_session(r).await.expect("a fresh session against D2");
        assert_eq!(session.theirs.as_deref(), Some(d2));
        assert_eq!(session.ours.as_deref(), Some(tip));
        assert_eq!(resolved_against(&session, LIST).as_deref(), Some(d1));
    }

    fn resolved_against(
        session: &crate::orchestrator::conflict_session::ConflictSession,
        path: &str,
    ) -> Option<String> {
        session
            .resolution_basis
            .get(path)
            .map(|side| side.theirs.clone())
    }

    /// Scenario 1a, the specimen path: the session recorded against D1 is still
    /// open when the request lands, and main is already at D2.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_resolution_against_d1_is_not_restored_over_d2s_same_spot_append() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let (r, d1, resolved) = same_spot_resolution(&bin, "replay-1a.db").await;
        let d2 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "head\nA\nB\n")],
            "D2 appends B",
        );
        set_main(&r.fx, &d2);

        request_take_committed_tip(&r).await;
        assert_refused_with_fresh_session(&r, &d1, &d2, &resolved).await;

        // The agent resolves the new pair of sides and the replay lands both.
        let rebased_resolution = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(LIST, "head\nA\nB\nX\n")],
            "resolve against D2",
        );
        assert_ne!(rebased_resolution, resolved);
        request_take_committed_tip(&r).await;
        assert!(crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &d2
        ));
        assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nB\nX\n");
        assert!(
            open_session(&r).await.is_none(),
            "the landed replay closes the session"
        );
    }

    /// Scenario 1b: an automatic advance onto D2 records a session whose `ours`
    /// IS the resolution, before the request. The guard must still judge the
    /// tip against D1, the side it was resolved against.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_session_recorded_over_the_resolution_still_knows_it_was_made_against_d1() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let (r, d1, resolved) = same_spot_resolution(&bin, "replay-1b.db").await;
        let d2 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "head\nA\nB\n")],
            "D2 appends B",
        );
        set_main(&r.fx, &d2);
        automatic_advance(&r).await;
        let session = open_session(&r)
            .await
            .expect("D2 conflicts with the resolution");
        assert_eq!(session.ours.as_deref(), Some(resolved.as_str()));
        assert_eq!(resolved_against(&session, LIST), Some(d1.clone()));

        request_take_committed_tip(&r).await;
        assert_refused_with_fresh_session(&r, &d1, &d2, &resolved).await;
    }

    /// Review regression on 1b: after the session is re-recorded over the
    /// resolution, the agent commits only an UNRELATED file. That moves the tip
    /// but says nothing about `list.txt`, which must stay resolved against D1 —
    /// promoted to D2, the guard would certify `head A X` against D2 and the
    /// carry would restore it whole, dropping B.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_commit_to_an_unrelated_file_does_not_promote_a_resolution_to_d2() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let (r, d1, _resolved) = same_spot_resolution(&bin, "replay-1b-unrelated.db").await;
        let d2 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "head\nA\nB\n")],
            "D2 appends B",
        );
        set_main(&r.fx, &d2);
        automatic_advance(&r).await;
        assert!(open_session(&r).await.is_some());
        let unrelated = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("other.txt", "unrelated\n")],
            "an unrelated change",
        );

        request_take_committed_tip(&r).await;
        assert_refused_with_fresh_session(&r, &d1, &d2, &unrelated).await;
        assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
    }

    /// Review regression: two resolved files, and D2 changes the resolved
    /// region of only one. The replay carries `f`, refuses `g`, and rolls back
    /// both, so the next session conflicts on `g` alone. When the agent
    /// re-resolves `g`, the replay must still place `f` — or it conflicts on
    /// `f` again and the two files trade places forever.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_carried_file_rolled_back_with_its_neighbour_lands_on_the_next_retry() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let r = replay_harness(
            &bin,
            "replay-two-file.db",
            &[("f.txt", "head\n"), ("g.txt", "head\n")],
        )
        .await;
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("f.txt", "head\nX\n"), ("g.txt", "head\nX\n")],
            "branch appends X to both",
        );
        let d1 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[("f.txt", "head\nA\n"), ("g.txt", "head\nA\n")],
            "D1 appends A to both",
        );
        set_main(&r.fx, &d1);
        automatic_advance(&r).await;
        let resolved = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("f.txt", "head\nA\nX\n"), ("g.txt", "head\nA\nX\n")],
            "resolve both against D1",
        );
        let d2 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[("g.txt", "head\nA\nB\n")],
            "D2 appends B to g only",
        );
        set_main(&r.fx, &d2);

        request_take_committed_tip(&r).await;
        assert_eq!(branch_tip(&r), resolved, "g's refusal rolls f back too");
        let retried = open_session(&r).await.expect("a session against D2");
        assert!(retried.conflicting().any(|file| file.path == "g.txt"));
        assert_eq!(resolved_against(&retried, "f.txt"), Some(d1.clone()));
        assert_eq!(resolved_against(&retried, "g.txt"), Some(d1.clone()));

        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("g.txt", "head\nA\nB\nX\n")],
            "resolve g against D2",
        );
        request_take_committed_tip(&r).await;
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &d2),
            "the retry landed: {:?}",
            open_session(&r).await.map(|session| session
                .conflicting()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>())
        );
        assert_eq!(text_at(&r.fx, BRANCH, "f.txt"), "head\nA\nX\n");
        assert_eq!(text_at(&r.fx, BRANCH, "g.txt"), "head\nA\nB\nX\n");
        assert!(open_session(&r).await.is_none());
    }

    /// Scenario 1c: D2 changes the same file somewhere the resolution did not
    /// touch, so the resolution is carried onto it and all three changes land.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_resolution_is_merged_with_d2s_change_elsewhere_in_the_file() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let (r, _d1, _resolved) = same_spot_resolution(&bin, "replay-1c.db").await;
        let d2 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "B\nhead\nA\n")],
            "D2 prepends B",
        );
        set_main(&r.fx, &d2);

        request_take_committed_tip(&r).await;
        assert!(crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &d2
        ));
        assert_eq!(text_at(&r.fx, BRANCH, LIST), "B\nhead\nA\nX\n");
        assert!(open_session(&r).await.is_none());
    }

    /// cairn/5222 and cairn/5328: the branch commits D1's line beside its own
    /// BEFORE the advance records a session. jj's line merge still conflicts at
    /// the shared insertion point, so the session is recorded with that
    /// resolution as `ours` and no basis, and later commits touch only other
    /// files. The tip already holds both sides, so the request is accepted and
    /// the replay lands it — no cosmetic edit to the file is needed.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_resolution_committed_before_the_session_was_recorded_is_taken() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let r = replay_harness(&bin, "replay-pre-resolved.db", &[(LIST, "head\n")]).await;
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(LIST, "head\nX\n")],
            "branch appends X",
        );
        let d1 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "head\nA\n")],
            "D1 appends A",
        );
        set_main(&r.fx, &d1);
        let resolved = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(LIST, "head\nA\nX\n")],
            "carry D1's line ahead of the advance",
        );
        automatic_advance(&r).await;
        let session = open_session(&r)
            .await
            .expect("the line merge still conflicts at the shared insertion point");
        assert_eq!(session.ours.as_deref(), Some(resolved.as_str()));
        assert_eq!(
            resolved_against(&session, LIST),
            None,
            "no session existed when the resolution was committed, so none is recorded"
        );
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("other.txt", "unrelated\n")],
            "an unrelated follow-up",
        );

        request_take_committed_tip(&r).await;
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &d1),
            "the replay landed: {:?}",
            open_session(&r).await.map(|session| session.fingerprint())
        );
        assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
        assert_eq!(text_at(&r.fx, BRANCH, "other.txt"), "unrelated\n");
        assert!(open_session(&r).await.is_none());
    }

    /// The arm the fall-through must not open: a tip whose conflicting file is
    /// still only the branch's side is refused, by name, even after commits to
    /// other files have moved it.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_one_sided_tip_is_still_refused_by_name() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let r = replay_harness(&bin, "replay-one-sided.db", &[(LIST, "head\n")]).await;
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(LIST, "head\nX\n")],
            "branch appends X",
        );
        let d1 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "head\nA\n")],
            "D1 appends A",
        );
        set_main(&r.fx, &d1);
        automatic_advance(&r).await;
        let session = open_session(&r).await.expect("D1 conflicts");
        let ours = session.ours.clone().expect("the session records ours");
        let moved = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("other.txt", "unrelated\n")],
            "an unrelated change",
        );

        let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
            .await
            .expect_err("a one-sided tip has no resolution to take");
        assert!(refusal.contains("resolves none"), "{refusal}");
        assert!(refusal.contains(&format!("- `{LIST}`")), "{refusal}");
        assert!(
            refusal.contains(&format!("changes it on top of `{ours}`")),
            "{refusal}"
        );
        let refused_with_reason =
            request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some("trust me"))
                .await
                .expect_err("a stated reason cannot supply a resolution");
        assert!(
            refused_with_reason.contains("resolves none"),
            "{refused_with_reason}"
        );
        assert_eq!(branch_tip(&r), moved, "a refused request moves nothing");
    }

    /// cairn/5051, bug-sink #372: both sides insert a module declaration above
    /// the same line, and D1 also appends a function further down. The agent
    /// commits every incoming hunk the old checklist listed — the function,
    /// not D1's declaration — which touches the file, so the session counts it
    /// resolved. The guard must still refuse, quoting the missing declaration,
    /// and once the agent commits the merged view's candidate the replay lands
    /// both declarations.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_same_spot_insertion_the_tip_lacks_is_refused_by_name_then_lands_with_both() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        const MODS: &str = "mod.rs";
        let base = "mod policy;\nmod rules;\n\npub fn run() {}\n";
        let r = replay_harness(&bin, "replay-same-spot-insert.db", &[(MODS, base)]).await;
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(
                MODS,
                "pub mod evidence;\nmod policy;\nmod rules;\n\npub fn run() {}\n",
            )],
            "branch declares evidence",
        );
        let incoming =
            "pub(crate) mod filing;\nmod policy;\nmod rules;\n\npub fn run() {}\n\npub fn answer_and_wake() {}\n";
        let d1 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(MODS, incoming)],
            "D1 declares filing and adds answer_and_wake",
        );
        set_main(&r.fx, &d1);
        automatic_advance(&r).await;
        let session = open_session(&r)
            .await
            .expect("the shared insertion point conflicts");
        assert_eq!(
            session
                .conflicting()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec![MODS]
        );

        let partial = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(
                MODS,
                "pub mod evidence;\nmod policy;\nmod rules;\n\npub fn run() {}\n\npub fn answer_and_wake() {}\n",
            )],
            "take the listed hunks",
        );
        let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
            .await
            .expect_err("a tip missing D1's declaration would drop it");
        assert!(
            refusal.contains("+pub(crate) mod filing;"),
            "the refusal quotes the dropped line: {refusal}"
        );
        assert_eq!(branch_tip(&r), partial, "a refused request moves nothing");

        let both =
            "pub mod evidence;\npub(crate) mod filing;\nmod policy;\nmod rules;\n\npub fn run() {}\n\npub fn answer_and_wake() {}\n";
        assert_eq!(
            crate::jj::completion_candidate(base, &text_at(&r.fx, BRANCH, MODS), incoming),
            both,
            "the merged view's candidate is the union"
        );
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(MODS, both)],
            "commit the candidate",
        );
        request_take_committed_tip(&r).await;
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &d1),
            "the replay landed: {:?}",
            open_session(&r).await.map(|session| session.fingerprint())
        );
        assert_eq!(text_at(&r.fx, BRANCH, MODS), both);
        assert!(open_session(&r).await.is_none());
    }

    // ── cairn/5473: a branch's deletion of a file the base edited ──────────────

    const FENCE: &str = "src/fence.rs";
    const FENCE_BASE: &str = "fn fence() {}\n";

    /// The shared start: the branch retires `FENCE` before any session exists,
    /// D1 edits it (and `LIST`, which the branch never touches), and the
    /// automatic advance records a session conflicting on the deletion alone,
    /// with nothing in the basis for it. Returns the harness, D1, and the tip.
    async fn deletion_before_the_session(bin: &str, name: &str) -> (Replay, String, String) {
        let r = replay_harness(bin, name, &[(FENCE, FENCE_BASE), (LIST, "head\n")]).await;
        let deleted = delete(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            FENCE,
            "branch retires fence.rs",
        );
        let d1 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[
                (FENCE, "fn fence() {}\nfn grant() {}\n"),
                (LIST, "head\nA\n"),
            ],
            "D1 tightens the fence",
        );
        set_main(&r.fx, &d1);
        automatic_advance(&r).await;
        let session = open_session(&r)
            .await
            .expect("the deletion conflicts with D1's edit");
        assert_eq!(
            session
                .conflicting()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec![FENCE]
        );
        assert_eq!(resolved_against(&session, FENCE), None);
        (r, d1, deleted)
    }

    fn fence_is_absent_at(r: &Replay, revision: &str) -> bool {
        crate::jj::paths_present_at(&r.fx.jj, &r.fx.store, revision, &[FENCE.to_string()])
            .unwrap()
            .is_empty()
    }

    /// Requirement (i): a deletion made before any session opened, with no
    /// later base edit, is refused by the commit that edited the file until
    /// the requester states where that edit went, and then it carries.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_deletion_made_before_the_session_carries_once_attested() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let (r, d1, deleted) = deletion_before_the_session(&bin, "replay-deletion-1.db").await;

        let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
            .await
            .expect_err("nothing covers what D1 did to the deleted file");
        assert!(
            refusal.contains(&d1[..8]) && refusal.contains("D1 tightens the fence"),
            "the refusal names the incoming commit: {refusal}"
        );
        assert!(!refusal.contains("resolves none"), "{refusal}");
        assert!(
            !refusal.contains("since that resolution was made"),
            "{refusal}"
        );
        assert_eq!(branch_tip(&r), deleted, "a refused request moves nothing");

        let reason = "fence.rs's grant check moved into the policy module";
        let answer = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some(reason))
            .await
            .unwrap_or_else(|refusal| panic!("the attested request was refused: {refusal}"));
        assert!(answer.contains("Recorded your statement"), "{answer}");
        wait_for_worker(&r).await;
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &d1),
            "the replay landed: {:?}",
            open_session(&r).await.map(|session| session.fingerprint())
        );
        assert!(fence_is_absent_at(&r, BRANCH), "the deletion carried");
        assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\n");
        assert!(open_session(&r).await.is_none());
    }

    /// Requirements (ii) and (iii): a base edit after the attested side is
    /// refused by name — by the carry, and then by the guard of the fresh
    /// session — never as a stale resolution, the statement survives the
    /// rollback on the side it covered, and a new statement lands it.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_base_edit_after_the_attestation_is_refused_by_name() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let (r, d1, deleted) = deletion_before_the_session(&bin, "replay-deletion-2.db").await;
        let d2 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(FENCE, "fn fence() {}\nfn grant() {}\nfn audit() {}\n")],
            "D2 audits the fence",
        );
        set_main(&r.fx, &d2);

        let first = "grant moved into policy::grant";
        let answer = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some(first))
            .await
            .unwrap_or_else(|refusal| panic!("the statement covers D1: {refusal}"));
        assert!(answer.contains(first), "{answer}");
        wait_for_worker(&r).await;
        assert_eq!(
            branch_tip(&r),
            deleted,
            "the carry refused, so nothing moved"
        );
        assert!(!crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &d2
        ));

        let fresh = open_session(&r).await.expect("a fresh session against D2");
        assert_eq!(fresh.theirs.as_deref(), Some(d2.as_str()));
        let kept = fresh
            .resolution_basis
            .get(FENCE)
            .expect("the statement survives");
        assert_eq!(kept.theirs, d1, "on the side it covered");
        assert_eq!(kept.attested.as_deref(), Some(first));

        // The carry's own refusal, as the worker wrote it.
        let plan = plan_resolution_carry(&r.fx.jj, &r.fx.store, &fresh, &d2, &deleted).unwrap();
        let (path, why) = plan
            .refused
            .first()
            .expect("the carry refuses the deletion");
        assert_eq!(path, FENCE);
        assert!(
            why.contains(&d2[..8]) && why.contains("D2 audits the fence"),
            "{why}"
        );
        assert!(
            why.contains(first),
            "it quotes the statement it outran: {why}"
        );
        assert!(!why.contains("since that resolution was made"), "{why}");

        // The page reads the same assessment: the statement, and what outran it.
        let page = crate::orchestrator::conflict_session::assess_session_tip(
            &r.fx.jj,
            &r.fx.store,
            &fresh,
        )
        .expect("the page assesses the tip");
        assert_eq!(
            page.attested
                .get(FENCE)
                .and_then(|side| side.attested.as_deref()),
            Some(first)
        );
        assert!(matches!(
            page.deletions.get(FENCE),
            Some(crate::orchestrator::conflict_session::DeletionStanding::Outpaced(_))
        ));

        let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
            .await
            .expect_err("the statement covered D1, not D2");
        assert!(
            refusal.contains(&d2[..8]) && refusal.contains("D2 audits the fence"),
            "{refusal}"
        );
        assert!(
            !refusal.contains("D1 tightens the fence"),
            "only what came after the statement: {refusal}"
        );

        let second = "audit moved into policy::audit too";
        let answer = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some(second))
            .await
            .unwrap_or_else(|refusal| panic!("the new statement covers D2: {refusal}"));
        assert!(answer.contains(second), "{answer}");
        wait_for_worker(&r).await;
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &d2),
            "the replay landed: {:?}",
            open_session(&r).await.map(|session| session.fingerprint())
        );
        assert!(fence_is_absent_at(&r, BRANCH));
        assert!(open_session(&r).await.is_none());
    }

    /// Review regression: the deletion is made WHILE the session is open, so
    /// it reads as a content change since `ours` on every recomputation of the
    /// basis. The statement must survive those rereads — the page's, the
    /// worker's, and the rollback's inheritance onto a D2 session — so the next
    /// refusal names only what D2 did, and quotes the statement it outran.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_deletion_attested_during_the_session_survives_a_rollback_onto_d2() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let r = replay_harness(
            &bin,
            "replay-deletion-in-session.db",
            &[(FENCE, FENCE_BASE)],
        )
        .await;
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(FENCE, "fn fence() {}\nfn branch() {}\n")],
            "branch extends the fence",
        );
        let d1 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(FENCE, "fn fence() {}\nfn grant() {}\n")],
            "D1 tightens the fence",
        );
        set_main(&r.fx, &d1);
        automatic_advance(&r).await;
        assert!(open_session(&r)
            .await
            .expect("both sides extend the fence")
            .conflicting()
            .any(|file| file.path == FENCE));
        let deleted = delete(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            FENCE,
            "retire fence.rs while the conflict is open",
        );
        let d2 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(FENCE, "fn fence() {}\nfn grant() {}\nfn audit() {}\n")],
            "D2 audits the fence",
        );
        set_main(&r.fx, &d2);

        let first = "grant moved into policy::grant";
        request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some(first))
            .await
            .unwrap_or_else(|refusal| panic!("the statement covers D1: {refusal}"));
        wait_for_worker(&r).await;
        assert_eq!(branch_tip(&r), deleted, "D2 outran the statement");

        let fresh = open_session(&r).await.expect("a fresh session against D2");
        assert_eq!(fresh.theirs.as_deref(), Some(d2.as_str()));
        let kept = fresh
            .resolution_basis
            .get(FENCE)
            .expect("the rollback's inheritance kept the statement");
        assert_eq!(kept.theirs, d1);
        assert_eq!(kept.attested.as_deref(), Some(first));
        let reread = fresh
            .resolution_basis_at(&r.fx.jj, &r.fx.store, &deleted)
            .unwrap();
        assert_eq!(
            reread.get(FENCE).and_then(|side| side.attested.as_deref()),
            Some(first),
            "and a reread keeps it"
        );

        let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
            .await
            .expect_err("the statement covered D1, not D2");
        assert!(
            refusal.contains(&d2[..8]) && refusal.contains("D2 audits the fence"),
            "{refusal}"
        );
        assert!(
            refusal.contains(first) && !refusal.contains("D1 tightens the fence"),
            "measured from the statement, not the fork: {refusal}"
        );

        let second = "audit moved into policy::audit too";
        request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some(second))
            .await
            .unwrap_or_else(|refusal| panic!("the new statement covers D2: {refusal}"));
        wait_for_worker(&r).await;
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &d2),
            "the replay landed: {:?}",
            open_session(&r).await.map(|session| session.fingerprint())
        );
        assert!(fence_is_absent_at(&r, BRANCH));
        assert!(open_session(&r).await.is_none());
    }

    /// An automatic advance that conflicts and rolls back records its fresh
    /// session's basis from the tip it actually found, so a requester's
    /// statement for a deleted file survives intact. A narrower structural
    /// proof subset must not stand in for that inheritance and erase it.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn automatic_conflict_rollback_preserves_an_attested_deletion() {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let r = replay_harness(
            &bin,
            "replay-automatic-attested-deletion.db",
            &[(LIST, "head\n"), (FENCE, FENCE_BASE)],
        )
        .await;
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(LIST, "head\nX\n")],
            "branch appends X",
        );
        let d1 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[
                (LIST, "head\nA\n"),
                (FENCE, "fn fence() {}\nfn grant() {}\n"),
            ],
            "D1 appends A and tightens the fence",
        );
        set_main(&r.fx, &d1);
        automatic_advance(&r).await;
        let session = open_session(&r)
            .await
            .expect("D1 conflicts on the list and records a session");
        assert_eq!(session.theirs.as_deref(), Some(d1.as_str()));
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[(LIST, "head\nA\nX\n")],
            "resolve the list against D1",
        );
        let deleted = delete(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            FENCE,
            "retire fence.rs; grant moved into policy",
        );

        let reason = "grant moved into policy::grant";
        let attested = crate::orchestrator::conflict_session::ResolvedSide {
            base: session
                .base
                .clone()
                .expect("the D1 session records its merge base"),
            theirs: d1.clone(),
            attested: Some(reason.to_string()),
        };
        let mut basis = session.resolution_basis.clone();
        basis.insert(FENCE.to_string(), attested.clone());
        crate::orchestrator::conflict_session::record_attested_basis(
            &r.db,
            &session.intent_id,
            BRANCH,
            &basis,
        )
        .await
        .unwrap();
        assert_eq!(
            open_session(&r).await.unwrap().resolution_basis.get(FENCE),
            Some(&attested),
            "the statement is seeded on the open session"
        );

        let d2 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[(LIST, "head\nA\nB\n")],
            "D2 appends B at the resolved spot",
        );
        set_main(&r.fx, &d2);
        automatic_advance(&r).await;

        assert_eq!(
            branch_tip(&r),
            deleted,
            "the conflicting advance rolled back"
        );
        assert!(!crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &d2
        ));
        assert!(fence_is_absent_at(&r, BRANCH));
        let fresh = open_session(&r)
            .await
            .expect("the automatic conflict records a fresh session against D2");
        assert_eq!(fresh.theirs.as_deref(), Some(d2.as_str()));
        assert_eq!(fresh.ours.as_deref(), Some(deleted.as_str()));
        assert_eq!(
            fresh.resolution_basis.get(FENCE),
            Some(&attested),
            "the exact statement and the side it covered survive the rollback"
        );
    }

    /// Scenario 2, the specimen's own file: the branch resolved D1's migration
    /// registration and renumbered its own, then D2 registered a migration at
    /// the number the branch took. The replayed registry holds every entry
    /// once. The replay does not renumber the collision: numbering is the
    /// landing's (`landing::bind`), which rebinds the branch's file against the
    /// tip it lands on.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial(jj)]
    async fn a_registry_resolution_against_d1_keeps_d2s_registration_and_leaves_numbers_to_the_landing(
    ) {
        let Some(bin) = crate::jj::tests::jj_bin() else {
            return;
        };
        let r = replay_harness(&bin, "replay-2.db", &[("other.txt", "one\n")]).await;
        add_migration(&r.branch_ws, "0228_branch");
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("other.txt", "one\nbranch\n")],
            "branch",
        );
        add_migration(&r.main_ws, "0228_destination");
        let d1 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[("other.txt", "one\nmain\n")],
            "D1",
        );
        set_main(&r.fx, &d1);
        automatic_advance(&r).await;
        let session = open_session(&r)
            .await
            .expect("a non-registry path keeps it a session");
        assert_eq!(
            session
                .conflicting()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            vec!["other.txt", MIGRATIONS]
        );

        // The agent's resolution, the specimen's 39ecbb51: D1's registration
        // kept, its own renumbered past it, as one linear commit.
        let _ = crate::jj::update_stale(&r.fx.jj, &r.branch_ws);
        std::fs::rename(
            r.branch_ws
                .join("src-tauri/turso_migrations/0228_branch.sql"),
            r.branch_ws
                .join("src-tauri/turso_migrations/0230_branch.sql"),
        )
        .unwrap();
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[
                ("other.txt", "one\nmain\nbranch\n"),
                (
                    MIGRATIONS,
                    "fn migrations() {\n    vec![\n        migration!(\"0227_before\"),\n        migration!(\"0228_destination\"),\n        migration!(\"0230_branch\"),\n        migration!(\"0229_after\"),\n    ];\n}\n",
                ),
            ],
            "resolve against D1",
        );
        add_migration(&r.main_ws, "0230_d2");
        let d2 = commit(&r.fx, &r.main_ws, MAIN_LINE, &[], "D2 registers 0230");
        set_main(&r.fx, &d2);

        request_take_committed_tip(&r).await;
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &d2),
            "the replay landed"
        );
        let registry = text_at(&r.fx, BRANCH, MIGRATIONS);
        for (stem, expected) in [
            ("0228_destination", 1),
            ("0230_d2", 1),
            ("0230_branch", 1),
            ("0228_branch", 0),
        ] {
            assert_eq!(
                registry.matches(&format!("\"{stem}\"")).count(),
                expected,
                "{stem} in:\n{registry}"
            );
        }
        let files =
            crate::jj::paths_under_at(&r.fx.jj, &r.fx.store, BRANCH, "src-tauri/turso_migrations")
                .unwrap();
        for file in ["0230_branch.sql", "0230_d2.sql"] {
            assert!(
                files.contains(&format!("src-tauri/turso_migrations/{file}")),
                "the replay moves no number: {files:?}"
            );
        }
        assert_eq!(text_at(&r.fx, BRANCH, "other.txt"), "one\nmain\nbranch\n");
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
        assert!(try_structural_merge(
            &fx.jj,
            &fx.store,
            branch,
            "main",
            &diagnostic,
            &ReplayBaseline::fork_only(diagnostic.base.as_deref().unwrap()),
            &crate::orchestrator::landing::merge_rules_at(&fx.jj, &fx.store, "main"),
        )
        .unwrap()
        .is_none());
        assert_eq!(
            crate::jj::bookmark_commit(&fx.jj, &fx.store, branch).as_deref(),
            Some(pre_tip.as_str())
        );
        assert_eq!(text_at(&fx, branch, "outside.txt"), "branch\n");
    }

    #[test]
    fn explicit_replay_bypasses_unchanged_automatic_timeout_cooldown() {
        let quarantine = ReconcileQuarantine {
            failure_kind: RECONCILE_TIMEOUT.to_string(),
            fingerprint: "tip-a:main-a".to_string(),
            last_diagnostic: Some("rebase timed out".to_string()),
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

    #[test]
    fn explicit_replay_budget_and_hold_name_select_only_the_exact_requested_branch() {
        for prefix in ["replay requested for ", "resolved replay requested for "] {
            let label = format!("{prefix}{BRANCH}");
            for branch in [BRANCH, "agent/replay-child", "agent/repla", "agent/other"] {
                let explicit = replay_is_explicit_for(&label, branch);
                assert_eq!(explicit, branch == BRANCH);
                assert_eq!(
                    replay_budget(explicit),
                    if branch == BRANCH {
                        EXPLICIT_REPLAY_BUDGET
                    } else {
                        crate::jj::BACKGROUND_RECONCILE_BUDGET
                    }
                );
                let hold = replay_lock_operation(
                    &label,
                    branch,
                    "rebase and export",
                    replay_budget(explicit),
                );
                assert_eq!(
                    hold.starts_with("explicit replay "),
                    branch == BRANCH,
                    "{hold}"
                );
                if branch == BRANCH {
                    assert!(
                        hold.contains(&format!("budget={}s", EXPLICIT_REPLAY_BUDGET.as_secs())),
                        "{hold}"
                    );
                    assert!(hold.ends_with(BRANCH), "{hold}");
                } else {
                    assert!(!hold.contains("budget="), "{hold}");
                }
            }
        }
        assert!(!replay_is_explicit_for("merged job (test)", BRANCH));
        assert!(EXPLICIT_REPLAY_BUDGET > crate::jj::BACKGROUND_RECONCILE_BUDGET);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn explicit_replay_carries_and_heals_past_automatic_budget_despite_cooldown() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::{Duration, Instant};

        const STORE_LOCK_TARGET: &str = "cairn_core::orchestrator::store_lock";
        crate::test_support::capture_log(STORE_LOCK_TARGET);

        let bin =
            crate::jj::tests::jj_bin().expect("real jj required for explicit replay regression");
        let home = tempfile::tempdir().unwrap();
        let wrapper = home.path().join("jj-explicit-delay");
        let gate = home.path().join("enabled");
        let attempts = home.path().join("attempts");
        let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
        std::fs::write(&wrapper, format!(
            "#!/bin/sh\nif test -f {}; then\n case \" $* \" in\n *' rebase -b {BRANCH} '*) echo rebase >> {}; sleep 11 ;;\n *' restore --from '*) echo restore >> {} ;;\n esac\nfi\nexec {} \"$@\"\n",
            quote(&gate.to_string_lossy()), quote(&attempts.to_string_lossy()),
            quote(&attempts.to_string_lossy()), quote(&bin),
        )).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let (r, d1, resolved) =
            same_spot_resolution(wrapper.to_str().unwrap(), "explicit-past-background.db").await;
        // The capture sink is shared by the whole test binary; only this
        // fixture's store attributes a hold line to this test.
        let store_marker = format!("store={}, ", r.fx.store.to_string_lossy());
        let hold_lines = || -> Vec<String> {
            crate::test_support::captured_log(STORE_LOCK_TARGET)
                .into_iter()
                .filter(|line| {
                    line.starts_with("jj store lock hold exceeded ")
                        && line.contains(&store_marker)
                        && line.contains(BRANCH)
                })
                .collect()
        };
        let phases_listed = |line: &str| line.contains("phases=[") && !line.contains("phases=[]");
        std::fs::write(&gate, "delay").unwrap();
        reopen_reconcile_intent(&r.db, "proj-1", &r.fx.store, "main", &d1, BRANCH)
            .await
            .unwrap();

        automatic_advance(&r).await;
        let automatic_holds = hold_lines();
        assert!(
            !automatic_holds.is_empty(),
            "the automatic attempt past its budget logs its store hold"
        );
        for hold in &automatic_holds {
            assert!(!hold.contains("operation=explicit replay "), "{hold}");
            assert!(!hold.contains("budget="), "{hold}");
            assert!(phases_listed(hold), "{hold}");
        }
        let cooldown = load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
            .await
            .unwrap()
            .expect("automatic timeout records cooldown");
        assert_eq!(cooldown.failure_kind, RECONCILE_TIMEOUT);
        assert_eq!(cooldown.fingerprint, format!("{resolved}:{d1}"));
        assert_eq!(branch_tip(&r), resolved);
        let attempts_before = std::fs::read_to_string(&attempts).unwrap();
        assert_eq!(
            attempts_before.lines().filter(|s| *s == "rebase").count(),
            1
        );
        automatic_advance(&r).await;
        assert_eq!(
            std::fs::read_to_string(&attempts).unwrap(),
            attempts_before,
            "automatic cooldown must suppress unchanged work"
        );

        let started = Instant::now();
        let answer = request_take_committed_tip(&r).await;
        assert!(started.elapsed() > crate::jj::BACKGROUND_RECONCILE_BUDGET);
        assert!(started.elapsed() < EXPLICIT_REPLAY_BUDGET);
        assert!(!answer.contains("exhausted"), "{answer}");
        assert_ne!(branch_tip(&r), resolved);
        assert!(crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &d1
        ));
        assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
        assert!(!crate::jj::branch_has_conflict(&r.fx.jj, &r.fx.store, BRANCH).unwrap());
        assert!(
            r.fx.jj
                .run(
                    &r.fx.store,
                    &[
                        "log",
                        "-r",
                        &format!("(({d1})..bookmarks(exact:{BRANCH:?})) & conflicts()"),
                        "--no-graph",
                        "-T",
                        "commit_id"
                    ],
                    "verify replay healed intermediate history",
                )
                .unwrap()
                .trim()
                .is_empty(),
            "the committed resolution also heals conflicted intermediate history"
        );
        let attempts_after = std::fs::read_to_string(&attempts).unwrap();
        assert_eq!(attempts_after.lines().filter(|s| *s == "rebase").count(), 2);
        assert!(
            attempts_after.contains("restore"),
            "resolution carry/heal was exercised"
        );
        assert!(
            load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
                .await
                .unwrap()
                .is_none(),
            "successful explicit replay clears automatic cooldown"
        );
        let git_tip = crate::jj::tests::git_stdout(
            &r.fx.repo,
            &["rev-parse", &format!("refs/heads/{BRANCH}")],
        );
        assert_eq!(git_tip.trim(), branch_tip(&r));
        let all_holds = hold_lines();
        let explicit_holds = &all_holds[automatic_holds.len()..];
        assert!(
            !explicit_holds.is_empty(),
            "the explicit replay past the automatic budget logs its store hold"
        );
        let explicit_budget = format!("(budget={}s; ", EXPLICIT_REPLAY_BUDGET.as_secs());
        for hold in explicit_holds {
            assert!(hold.contains("operation=explicit replay "), "{hold}");
            assert!(hold.contains(&explicit_budget), "{hold}");
            assert!(phases_listed(hold), "{hold}");
        }
        for hold in &automatic_holds {
            eprintln!("automatic store hold: {hold}");
        }
        for hold in explicit_holds {
            eprintln!("explicit store hold: {hold}");
        }
        eprintln!(
            "explicit replay completed after {:?}: {answer}",
            started.elapsed()
        );
        assert!(started.elapsed() >= Duration::from_secs(11));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn explicit_replay_budget_overrun_restores_refs_reports_phases_and_retries_immediately() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;

        let bin =
            crate::jj::tests::jj_bin().expect("real jj required for explicit timeout regression");
        let home = tempfile::tempdir().unwrap();
        let wrapper = home.path().join("jj-explicit-overrun");
        let gate = home.path().join("enabled");
        let attempts = home.path().join("attempts");
        let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
        std::fs::write(&wrapper, format!(
            "#!/bin/sh\nif test -f {}; then\n case \" $* \" in\n *' rebase -b {BRANCH} '*) echo rebase >> {} ;;\n *' restore --from '*) echo restore >> {}; exec sleep 30 ;;\n esac\nfi\nexec {} \"$@\"\n",
            quote(&gate.to_string_lossy()), quote(&attempts.to_string_lossy()),
            quote(&attempts.to_string_lossy()), quote(&bin),
        )).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let (r, d1, resolved) =
            same_spot_resolution(wrapper.to_str().unwrap(), "explicit-overrun.db").await;
        let refs_before = crate::jj::tests::git_stdout(
            &r.fx.repo,
            &[
                "for-each-ref",
                "--format=%(objectname) %(refname)",
                "refs/heads",
                "refs/remotes",
                "refs/tags",
            ],
        );
        // The deterministic element is the wrapper's hanging `restore --from`;
        // the window only has to be wide enough that the hang, not the real
        // work before it, is what expires. The explicit deadline is absolute
        // from branch admission, and the ~40 jj commands ahead of the rebase
        // take ~2.75 s nominal, with a single `jj log` observed stalling 2.25 s
        // under a loaded full suite. 20 s leaves the hang as the only plausible
        // expiry. cairn/6173's injected-deadline discipline is the structural
        // replacement for sizing this window by hand.
        EXPLICIT_REPLAY_TEST_BUDGETS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(r.fx.store.clone(), Duration::from_secs(20));
        assert_eq!(
            explicit_replay_budget_for_store(&r.fx.store),
            Duration::from_secs(20)
        );
        std::fs::write(&gate, "delay").unwrap();

        let answer = request_take_committed_tip(&r).await;
        assert_eq!(
            branch_tip(&r),
            resolved,
            "the entire replay, not only the failed carry, is undone"
        );
        assert_eq!(
            crate::jj::tests::git_stdout(
                &r.fx.repo,
                &[
                    "for-each-ref",
                    "--format=%(objectname) %(refname)",
                    "refs/heads",
                    "refs/remotes",
                    "refs/tags"
                ]
            ),
            refs_before,
            "rollback restores every exported Git ref"
        );
        assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
        assert!(!crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &d1
        ));
        assert!(
            load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
                .await
                .unwrap()
                .is_none(),
            "explicit failure never records automatic cooldown"
        );
        let commands = std::fs::read_to_string(&attempts).unwrap();
        assert!(
            commands.contains("rebase") && commands.contains("restore"),
            "{commands}"
        );
        let diagnostic = r.db.query_text(
            "SELECT last_diagnostic FROM jj_reconcile_items WHERE bookmark = 'agent/replay' AND failure_kind = 'reconcile_timeout' ORDER BY updated_at DESC LIMIT 1", ()
        ).await.unwrap().expect("durable rebase-page diagnostic");
        for rendered in [&answer, &diagnostic] {
            assert!(
                rendered.contains("20.000s cumulative forward budget"),
                "{rendered}"
            );
            assert!(
                rendered.contains("jj restore (committed conflict resolution) timed out after"),
                "the budget must cut the injected restore hang, not the work before it: {rendered}"
            );
            assert!(rendered.contains("No automatic cooldown"), "{rendered}");
            assert!(
                rendered.contains("restore")
                    && rendered.contains("ms:")
                    && rendered.contains("forward")
                    && rendered.contains("cleanup"),
                "{rendered}"
            );
        }
        std::fs::remove_file(&gate).unwrap();
        EXPLICIT_REPLAY_TEST_BUDGETS
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(&r.fx.store);
        let retry = request_take_committed_tip(&r).await;
        assert_ne!(
            branch_tip(&r),
            resolved,
            "the next request runs without waiting for cooldown"
        );
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &d1),
            "{retry}"
        );
        assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
        assert_eq!(
            crate::jj::tests::git_stdout(
                &r.fx.repo,
                &["rev-parse", &format!("refs/heads/{BRANCH}")]
            )
            .trim(),
            branch_tip(&r)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn explicit_replay_plain_request_applies_structural_fallback_after_conflict_rollback() {
        let bin =
            crate::jj::tests::jj_bin().expect("real jj required for structural replay regression");
        let r = replay_harness(&bin, "explicit-structural-fallback.db", &[]).await;
        add_migration(&r.branch_ws, "0230_branch");
        let original = commit(&r.fx, &r.branch_ws, BRANCH, &[], "branch registry");
        add_migration(&r.main_ws, "0228_main");
        let destination = commit(&r.fx, &r.main_ws, MAIN_LINE, &[], "main registry");
        set_main(&r.fx, &destination);
        let answer = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, false, None)
            .await
            .unwrap();
        wait_for_worker(&r).await;
        assert_ne!(branch_tip(&r), original, "{answer}");
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &destination),
            "{answer}"
        );
        let registry = text_at(&r.fx, BRANCH, MIGRATIONS);
        for stem in ["0227_before", "0228_main", "0230_branch", "0229_after"] {
            assert_eq!(
                registry.matches(&format!("migration!(\"{stem}\")")).count(),
                1,
                "{registry}"
            );
        }
        assert!(!crate::jj::branch_has_conflict(&r.fx.jj, &r.fx.store, BRANCH).unwrap());
        assert!(open_session(&r).await.is_none());
        assert!(answer.contains("completed"), "{answer}");
        assert_eq!(
            crate::jj::tests::git_stdout(
                &r.fx.repo,
                &["rev-parse", &format!("refs/heads/{BRANCH}")]
            )
            .trim(),
            branch_tip(&r)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn explicit_replay_preflight_expiry_is_terminal_diagnosed_and_immediately_retryable() {
        use std::time::Duration;
        let bin =
            crate::jj::tests::jj_bin().expect("real jj required for preflight replay regression");
        let (r, destination, original) =
            same_spot_resolution(&bin, "explicit-preflight-expiry.db").await;
        let refs_before = crate::jj::tests::git_stdout(
            &r.fx.repo,
            &[
                "for-each-ref",
                "--format=%(objectname) %(refname)",
                "refs/heads",
                "refs/remotes",
                "refs/tags",
            ],
        );
        EXPLICIT_REPLAY_TEST_BUDGETS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(r.fx.store.clone(), Duration::ZERO);
        let answer = request_take_committed_tip(&r).await;
        assert_eq!(branch_tip(&r), original);
        assert_eq!(
            crate::jj::tests::git_stdout(
                &r.fx.repo,
                &[
                    "for-each-ref",
                    "--format=%(objectname) %(refname)",
                    "refs/heads",
                    "refs/remotes",
                    "refs/tags"
                ]
            ),
            refs_before
        );
        assert!(
            load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
                .await
                .unwrap()
                .is_none_or(|quarantine| quarantine.failure_kind != RECONCILE_TIMEOUT),
            "preflight expiry does not create an automatic timeout cooldown"
        );
        assert_eq!(r.db.query_text(
            "SELECT status FROM jj_reconcile_intents WHERE store_path = ?1 ORDER BY updated_at DESC LIMIT 1",
            [r.fx.store.to_string_lossy().to_string()],
        ).await.unwrap().as_deref(), Some("completed"), "a failed explicit request must not become an automatic retry");
        let diagnostic = r.db.query_text(
            "SELECT last_diagnostic FROM jj_reconcile_items WHERE bookmark = 'agent/replay' AND failure_kind = 'reconcile_timeout' ORDER BY updated_at DESC LIMIT 1",
            (),
        ).await.unwrap().expect("preflight failure is durable for the rebase page");
        for rendered in [&answer, &diagnostic] {
            assert!(rendered.contains("cumulative forward budget"), "{rendered}");
            assert!(rendered.contains("No automatic cooldown"), "{rendered}");
            assert!(rendered.contains("ms:"), "{rendered}");
        }
        EXPLICIT_REPLAY_TEST_BUDGETS
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(&r.fx.store);
        let retry = request_take_committed_tip(&r).await;
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &destination),
            "{retry}"
        );
        assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn explicit_replay_deferred_request_retains_budget_carry_and_consumes_exact_marker() {
        use crate::orchestrator::conflict_session::load_pending_replay_request;
        use std::os::unix::fs::PermissionsExt;
        use std::time::{Duration, Instant};

        let bin =
            crate::jj::tests::jj_bin().expect("real jj required for deferred replay regression");
        for mode in ["success", "timeout", "invalid"] {
            let home = tempfile::tempdir().unwrap();
            let wrapper = home.path().join("jj-deferred-replay");
            let gate = home.path().join("enabled");
            let attempts = home.path().join("attempts");
            let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
            let delay = if mode == "timeout" { 30 } else { 11 };
            std::fs::write(&wrapper, format!(
                "#!/bin/sh\nif test -f {}; then\n case \" $* \" in\n *' rebase -b {BRANCH} '*) echo rebase >> {}; sleep {delay} ;;\n esac\nfi\nexec {} \"$@\"\n",
                quote(&gate.to_string_lossy()), quote(&attempts.to_string_lossy()), quote(&bin),
            )).unwrap();
            std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
            let (r, destination, resolved) = same_spot_resolution(
                wrapper.to_str().unwrap(),
                &format!("explicit-deferred-{mode}.db"),
            )
            .await;
            for sql in [
                "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-deferred','job-replay','open','backend',1,1)",
                "UPDATE runs SET session_id='session-deferred' WHERE id='run-replay'",
                "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-deferred','session-deferred','run-replay','job-replay',1,'yielded','initial',1,1)",
                "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-deferred','job-replay','run-replay','session-deferred','turn-deferred','tool-deferred','{\"kind\":\"run_batch\",\"request_id\":\"deferred-batch\",\"commits\":true}','pending',1)",
            ] {
                r.db.execute(sql, ()).await.unwrap();
            }
            let answer = request_take_committed_tip(&r).await;
            assert!(answer.contains("deferred"), "{answer}");
            assert_eq!(branch_tip(&r), resolved);
            let pending = load_pending_replay_request(&r.db, &r.fx.store, BRANCH)
                .await
                .unwrap()
                .expect("own run batch leaves typed explicit authority pending");
            assert!(pending.take_committed_tip);
            assert_eq!(pending.requested_tip.as_deref(), Some(resolved.as_str()));

            let files: Vec<(&str, &str)> = if mode == "invalid" {
                vec![(LIST, "head\nX\n"), ("later.txt", "new work\n")]
            } else {
                vec![("later.txt", "new work\n")]
            };
            let latest = commit(
                &r.fx,
                &r.branch_ws,
                BRANCH,
                &files,
                "new commit after deferred request",
            );
            assert_ne!(latest, resolved);
            let refs_before = crate::jj::tests::git_stdout(
                &r.fx.repo,
                &[
                    "for-each-ref",
                    "--format=%(objectname) %(refname)",
                    "refs/heads",
                    "refs/remotes",
                    "refs/tags",
                ],
            );
            if mode == "success" {
                r.db.execute(
                    "UPDATE jj_reconcile_items SET status = 'graph_moved', outcome_kind = 'rebased_clean', notification_sent = 0 WHERE intent_id = ?1 AND bookmark = ?2",
                    params![pending.intent_id.as_str(), BRANCH],
                ).await.unwrap();
            }
            r.db.execute("DELETE FROM agent_waits WHERE id='wait-deferred'", ())
                .await
                .unwrap();
            if mode == "timeout" {
                EXPLICIT_REPLAY_TEST_BUDGETS
                    .get_or_init(Default::default)
                    .lock()
                    .unwrap()
                    .insert(r.fx.store.clone(), Duration::from_secs(5));
            }
            std::fs::write(&gate, "delay").unwrap();
            let started = Instant::now();
            let work = claim_next_reconcile_intent(&r.db)
                .await
                .unwrap()
                .expect("deferred intent remains durable work");
            execute_durable_reconcile_work(&r.orch, &r.db, work)
                .await
                .unwrap();
            wait_for_worker(&r).await;
            assert!(
                load_pending_replay_request(&r.db, &r.fx.store, BRANCH)
                    .await
                    .unwrap()
                    .is_none(),
                "serving pass consumes the exact request for {mode}"
            );
            assert_eq!(r.db.query_opt_i64(
                "SELECT COUNT(*) FROM jj_reconcile_items WHERE replay_request_id = ?1 AND replay_served_at IS NOT NULL",
                [pending.id.clone()],
            ).await.unwrap(), Some(1));
            assert!(
                load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
                    .await
                    .unwrap()
                    .is_none_or(|q| q.failure_kind != RECONCILE_TIMEOUT),
                "retained explicit authority must not record automatic timeout cooldown for {mode}"
            );
            assert_eq!(text_at(&r.fx, BRANCH, "later.txt"), "new work\n");

            if mode == "success" {
                assert!(started.elapsed() > crate::jj::BACKGROUND_RECONCILE_BUDGET);
                assert!(started.elapsed() < EXPLICIT_REPLAY_BUDGET);
                assert!(crate::jj::branch_carries_commit(
                    &r.fx.jj,
                    &r.fx.store,
                    BRANCH,
                    &destination
                ));
                assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
                assert!(std::fs::read_to_string(&attempts)
                    .unwrap()
                    .contains("rebase"));
            } else {
                assert_eq!(
                    branch_tip(&r),
                    latest,
                    "failure reassesses and preserves the current tip"
                );
                assert_eq!(
                    crate::jj::tests::git_stdout(
                        &r.fx.repo,
                        &[
                            "for-each-ref",
                            "--format=%(objectname) %(refname)",
                            "refs/heads",
                            "refs/remotes",
                            "refs/tags"
                        ]
                    ),
                    refs_before
                );
                assert!(!crate::jj::branch_carries_commit(
                    &r.fx.jj,
                    &r.fx.store,
                    BRANCH,
                    &destination
                ));
                let diagnostic = r.db.query_text(
                    "SELECT last_diagnostic FROM jj_reconcile_items WHERE bookmark = 'agent/replay' AND outcome_kind = 'failed' ORDER BY updated_at DESC LIMIT 1", ()
                ).await.unwrap().expect("retained explicit request failure is diagnosed");
                if mode == "timeout" {
                    assert!(
                        diagnostic.contains("5.000s cumulative forward budget"),
                        "{diagnostic}"
                    );
                    assert!(diagnostic.contains("No automatic cooldown"), "{diagnostic}");
                    EXPLICIT_REPLAY_TEST_BUDGETS
                        .get()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .remove(&r.fx.store);
                } else {
                    assert!(
                        !attempts.exists(),
                        "invalid current resolution is refused before rebase"
                    );
                    assert!(
                        diagnostic.contains("resolution") || diagnostic.contains("one-sided"),
                        "{diagnostic}"
                    );
                }
            }
        }
    }

    /// A jj wrapper that, while `gate` exists, logs every `rebase -b BRANCH`
    /// and every `restore --from` to `attempts`, and holds each restore for
    /// 30s when `stall_restore` is set so an explicit 5s budget expires after
    /// the rebase has mutated the store.
    #[cfg(unix)]
    fn logging_jj_wrapper(
        bin: &str,
        home: &Path,
        stall_restore: bool,
    ) -> (PathBuf, PathBuf, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let wrapper = home.join("jj-request-race");
        let gate = home.join("enabled");
        let attempts = home.join("attempts");
        let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
        let stall = if stall_restore { "; exec sleep 30" } else { "" };
        std::fs::write(&wrapper, format!(
            "#!/bin/sh\nif test -f {}; then\n case \" $* \" in\n *' rebase -b {BRANCH} '*) echo rebase >> {} ;;\n *' restore --from '*) echo restore >> {}{stall} ;;\n esac\nfi\nexec {} \"$@\"\n",
            quote(&gate.to_string_lossy()), quote(&attempts.to_string_lossy()),
            quote(&attempts.to_string_lossy()), quote(bin),
        )).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        (wrapper, gate, attempts)
    }

    /// The refs a replay exports and its rollback must restore. jj's internal
    /// `refs/jj/keep/*` pins are excluded: they retain every attempted object
    /// by design and grow across a rolled-back attempt.
    fn exported_refs(r: &Replay) -> String {
        crate::jj::tests::git_stdout(
            &r.fx.repo,
            &["for-each-ref", "refs/heads", "refs/remotes", "refs/tags"],
        )
    }

    fn logged(attempts: &Path, command: &str) -> usize {
        std::fs::read_to_string(attempts)
            .unwrap_or_default()
            .lines()
            .filter(|line| *line == command)
            .count()
    }

    fn install_replay_request_hook<F>(store: &Path, stage: &'static str, hook: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        REPLAY_REQUEST_HOOKS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(
                (store.to_path_buf(), stage),
                Box::new(
                    move || -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                        Box::pin(hook)
                    },
                ),
            );
    }

    fn replay_request_hook_installed(store: &Path, stage: &'static str) -> bool {
        REPLAY_REQUEST_HOOKS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .contains_key(&(store.to_path_buf(), stage))
    }

    /// A worker that serves the caller's queued request before the caller
    /// claims it — right after the queue, or between the caller's Pending read
    /// and its atomic prepare — decides the request. The caller returns that
    /// worker's first terminal timeout verdict and never makes a second
    /// attempt on its stale explicit label.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn explicit_replay_request_served_by_another_worker_returns_its_first_timeout_verdict() {
        use crate::orchestrator::conflict_session::{
            load_pending_replay_request, replay_request_state, PendingReplayRequest,
            ReplayRequestState,
        };
        use std::time::Duration;

        let bin = crate::jj::tests::jj_bin().expect("real jj required for request race regression");
        for stage in ["queued", "pending_observed"] {
            let home = tempfile::tempdir().unwrap();
            let (wrapper, gate, attempts) = logging_jj_wrapper(&bin, home.path(), true);
            let (r, d1, resolved) = same_spot_resolution(
                wrapper.to_str().unwrap(),
                &format!("request-race-served-{stage}.db"),
            )
            .await;
            let r = Arc::new(r);
            let refs_before = exported_refs(&r);
            EXPLICIT_REPLAY_TEST_BUDGETS
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .insert(r.fx.store.clone(), Duration::from_secs(5));
            std::fs::write(&gate, "delay").unwrap();

            let seen: Arc<std::sync::Mutex<Option<PendingReplayRequest>>> = Default::default();
            let (worker, record) = (r.clone(), seen.clone());
            install_replay_request_hook(&r.fx.store, stage, async move {
                let request = load_pending_replay_request(&worker.db, &worker.fx.store, BRANCH)
                    .await
                    .unwrap()
                    .expect("the caller's request is durably queued before the hook");
                let work = claim_next_reconcile_intent(&worker.db)
                    .await
                    .unwrap()
                    .expect("the queued request is claimable durable work");
                execute_durable_reconcile_work(&worker.orch, &worker.db, work)
                    .await
                    .unwrap();
                wait_for_worker(&worker).await;
                *record.lock().unwrap() = Some(request);
            });

            let answer = request_take_committed_tip(&r).await;
            let request =
                seen.lock().unwrap().take().unwrap_or_else(|| {
                    panic!("the competing worker never ran at {stage}: {answer}")
                });
            assert!(!replay_request_hook_installed(&r.fx.store, stage));

            let ReplayRequestState::Served { pass, destination } =
                replay_request_state(&r.db, &request).await.unwrap()
            else {
                panic!("the competing worker must have served the request at {stage}")
            };
            assert_eq!(destination, d1, "{stage}");
            assert_eq!(pass.outcome_kind.as_deref(), Some("failed"), "{stage}");
            assert_eq!(
                pass.failure_kind.as_deref(),
                Some(RECONCILE_TIMEOUT),
                "{stage}"
            );
            let first = pass
                .diagnostic
                .expect("the served snapshot carries the first diagnostic");
            assert!(
                first.contains("5.000s cumulative forward budget"),
                "{first}"
            );
            assert!(first.contains("No automatic cooldown"), "{first}");
            assert!(
                answer.contains(&format!("Explicit replay of `{BRANCH}` failed.")),
                "{stage}: {answer}"
            );
            assert!(
                answer.contains(&first),
                "the caller returns the competing worker's first verdict at {stage}: {answer}"
            );

            assert_eq!(
                logged(&attempts, "rebase"),
                1,
                "no second attempt at {stage}"
            );
            assert_eq!(
                logged(&attempts, "restore"),
                1,
                "no second attempt at {stage}"
            );
            assert_eq!(
                branch_tip(&r),
                resolved,
                "the timed-out attempt rolled back"
            );
            assert_eq!(exported_refs(&r), refs_before);
            assert!(!crate::jj::branch_carries_commit(
                &r.fx.jj,
                &r.fx.store,
                BRANCH,
                &d1
            ));
            assert!(
                load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
                    .await
                    .unwrap()
                    .is_none_or(|q| q.failure_kind != RECONCILE_TIMEOUT),
                "an explicit timeout records no automatic cooldown at {stage}"
            );
            assert!(
                load_pending_replay_request(&r.db, &r.fx.store, BRANCH)
                    .await
                    .unwrap()
                    .is_none(),
                "no fresh request marker is left pending at {stage}"
            );
            assert_eq!(
                r.db.query_opt_i64(
                    "SELECT COUNT(*) FROM jj_reconcile_items
                     WHERE bookmark = ?1 AND replay_request_id IS NOT NULL",
                    [BRANCH.to_string()],
                )
                .await
                .unwrap(),
                Some(1),
                "only the caller's one request was ever queued at {stage}"
            );
            assert_eq!(
                r.db.query_opt_i64(
                    "SELECT COUNT(*) FROM jj_reconcile_intents WHERE status IN ('pending', 'running')",
                    (),
                )
                .await
                .unwrap(),
                Some(0),
                "a served explicit failure leaves no queued retry at {stage}"
            );

            std::fs::remove_file(&gate).unwrap();
            EXPLICIT_REPLAY_TEST_BUDGETS
                .get()
                .unwrap()
                .lock()
                .unwrap()
                .remove(&r.fx.store);
        }
    }

    /// The label is never authority: a pass carrying the exact explicit label
    /// for a branch with no pending typed request makes no replay mutation.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn explicit_replay_label_without_a_pending_request_grants_no_mutation() {
        use crate::orchestrator::conflict_session::load_pending_replay_request;

        let bin =
            crate::jj::tests::jj_bin().expect("real jj required for label authority regression");
        let home = tempfile::tempdir().unwrap();
        let (wrapper, gate, attempts) = logging_jj_wrapper(&bin, home.path(), false);
        let (r, _d1, resolved) =
            same_spot_resolution(wrapper.to_str().unwrap(), "label-without-request.db").await;
        assert!(load_pending_replay_request(&r.db, &r.fx.store, BRANCH)
            .await
            .unwrap()
            .is_none());
        // A fresh destination, so the labelled call claims a new intent and
        // reaches the per-branch body rather than coalescing.
        let d2 = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[("other.txt", "unrelated\n")],
            "D2 touches another file",
        );
        set_main(&r.fx, &d2);
        let refs_before = exported_refs(&r);
        let quarantine = |q: Option<ReconcileQuarantine>| {
            q.map(|q| {
                (
                    q.failure_kind.clone(),
                    q.fingerprint.clone(),
                    q.strike_count,
                    q.updated_at,
                )
            })
        };
        let cooldown_before = quarantine(
            load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
                .await
                .unwrap(),
        );
        std::fs::write(&gate, "log").unwrap();

        let label = format!("resolved replay requested for {BRANCH}");
        assert!(replay_is_explicit_for(&label, BRANCH));
        let outcome = reconcile_base_advance(
            &r.orch,
            &r.db,
            "proj-1",
            &label,
            &r.fx.repo.to_string_lossy(),
            "main",
            "main",
            vec![SiblingJob {
                id: JOB.to_string(),
                branch: Some(BRANCH.to_string()),
                base_commit: None,
            }],
            BaseAdvanceNotes {
                conflict: build_jj_conflict_note("main", None, None),
                clean: build_jj_clean_note("main", None, None),
                incoming: IncomingIdentity {
                    base_branch: "main".to_string(),
                    ..IncomingIdentity::default()
                },
            },
        )
        .await
        .unwrap();
        wait_for_worker(&r).await;

        assert!(
            outcome.coalesced_destination.is_none(),
            "the labelled call claimed a fresh intent and ran its body"
        );
        assert_eq!(
            (outcome.rebased_clean, outcome.conflicted, outcome.failed),
            (0, 0, 0),
            "no attempt was made on label alone"
        );
        assert!(outcome.replay_diagnostic.is_none());
        assert_eq!(logged(&attempts, "rebase"), 0);
        assert_eq!(logged(&attempts, "restore"), 0);
        assert_eq!(branch_tip(&r), resolved);
        assert_eq!(exported_refs(&r), refs_before);
        assert!(!crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &d2
        ));
        assert_eq!(
            r.db.query_opt_i64(
                "SELECT COUNT(*) FROM jj_reconcile_items i
                 JOIN jj_reconcile_intents n ON n.id = i.intent_id
                 WHERE n.destination_commit = ?1 AND i.bookmark = ?2
                   AND i.outcome_kind IS NOT NULL",
                params![d2.clone(), BRANCH.to_string()],
            )
            .await
            .unwrap(),
            Some(0),
            "no verdict was recorded for a pass that had no authority"
        );
        assert!(load_pending_replay_request(&r.db, &r.fx.store, BRANCH)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            quarantine(
                load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
                    .await
                    .unwrap()
            ),
            cooldown_before
        );
    }

    /// A newer request queued while the original caller waits supersedes it.
    /// The old caller reports that, makes no attempt, and leaves the newer
    /// request pending for the worker that serves it.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn explicit_replay_superseded_request_reports_superseded_and_leaves_the_newer_pending() {
        use crate::orchestrator::conflict_session::{
            load_pending_replay_request, queue_replay_request, replay_request_state,
            PendingReplayRequest, ReplayRequestState,
        };

        let bin = crate::jj::tests::jj_bin().expect("real jj required for supersede regression");
        for stage in ["queued", "pending_observed"] {
            let home = tempfile::tempdir().unwrap();
            let (wrapper, gate, attempts) = logging_jj_wrapper(&bin, home.path(), false);
            let (r, d1, resolved) = same_spot_resolution(
                wrapper.to_str().unwrap(),
                &format!("request-race-superseded-{stage}.db"),
            )
            .await;
            let r = Arc::new(r);
            let refs_before = exported_refs(&r);
            std::fs::write(&gate, "log").unwrap();

            let seen: Arc<std::sync::Mutex<Option<(PendingReplayRequest, PendingReplayRequest)>>> =
                Default::default();
            let (worker, record, tip, destination) =
                (r.clone(), seen.clone(), resolved.clone(), d1.clone());
            install_replay_request_hook(&r.fx.store, stage, async move {
                let old = load_pending_replay_request(&worker.db, &worker.fx.store, BRANCH)
                    .await
                    .unwrap()
                    .expect("the caller's request is durably queued before the hook");
                let newer = queue_replay_request(
                    &worker.db,
                    "proj-1",
                    &worker.fx.store,
                    "main",
                    &destination,
                    BRANCH,
                    Some(&tip),
                    true,
                    None,
                )
                .await
                .unwrap();
                assert_ne!(old.id, newer.id);
                *record.lock().unwrap() = Some((old, newer));
            });

            let result = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None).await;
            wait_for_worker(&r).await;
            let (old, newer) =
                seen.lock().unwrap().take().unwrap_or_else(|| {
                    panic!("the superseding request was never queued at {stage}")
                });
            let error = result.expect_err("a superseded caller must not report a verdict");
            assert!(
                error.contains("superseded") && error.contains(&old.id),
                "{stage}: {error}"
            );
            assert_eq!(
                replay_request_state(&r.db, &old).await.unwrap(),
                ReplayRequestState::Superseded
            );
            assert_eq!(
                replay_request_state(&r.db, &newer).await.unwrap(),
                ReplayRequestState::Pending,
                "the old caller neither served nor consumed the newer request at {stage}"
            );
            assert_eq!(
                load_pending_replay_request(&r.db, &r.fx.store, BRANCH)
                    .await
                    .unwrap()
                    .map(|request| request.id),
                Some(newer.id.clone())
            );
            assert_eq!(logged(&attempts, "rebase"), 0, "no attempt at {stage}");
            assert_eq!(branch_tip(&r), resolved);
            assert_eq!(exported_refs(&r), refs_before);

            // The newer request keeps its authority for the next worker.
            let work = claim_next_reconcile_intent(&r.db)
                .await
                .unwrap()
                .expect("the newer request remains durable work");
            execute_durable_reconcile_work(&r.orch, &r.db, work)
                .await
                .unwrap();
            wait_for_worker(&r).await;
            assert!(
                matches!(
                    replay_request_state(&r.db, &newer).await.unwrap(),
                    ReplayRequestState::Served { .. }
                ),
                "{stage}"
            );
            assert_eq!(logged(&attempts, "rebase"), 1, "{stage}");
            assert!(crate::jj::branch_carries_commit(
                &r.fx.jj,
                &r.fx.store,
                BRANCH,
                &d1
            ));
            assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
        }
    }

    #[cfg(unix)]
    async fn timeout_worker_advance(r: &Replay, clean_branch: &str) {
        let outcome = reconcile_base_advance(
            &r.orch,
            &r.db,
            "proj-1",
            "merged job (timeout regression)",
            &r.fx.repo.to_string_lossy(),
            "main",
            "main",
            vec![
                SiblingJob {
                    id: JOB.to_string(),
                    branch: Some(BRANCH.to_string()),
                    base_commit: None,
                },
                SiblingJob {
                    id: "job-timeout-clean".to_string(),
                    branch: Some(clean_branch.to_string()),
                    base_commit: None,
                },
            ],
            BaseAdvanceNotes {
                conflict: build_jj_conflict_note("main", None, None),
                clean: build_jj_clean_note("main", None, None),
                incoming: IncomingIdentity {
                    base_branch: "main".to_string(),
                    ..IncomingIdentity::default()
                },
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome.eligible, 2);
        wait_for_worker(r).await;
    }

    /// A main advance cannot bypass a timed-out stack's cooldown. A branch
    /// commit can, but the new attempt still gets the background budget rather
    /// than the foreground command timeout.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn real_worker_timeout_cooldown_and_changed_tip_retry_stay_bounded() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::{Duration, Instant};

        let bin = crate::jj::tests::jj_bin().expect("real jj required for worker regression");
        let wrapper_home = tempfile::tempdir().unwrap();
        let wrapper = wrapper_home.path().join("jj-delayed");
        let attempts = wrapper_home.path().join("attempts");
        let fail_tip = wrapper_home.path().join("fail-tip");
        let failed_probes = wrapper_home.path().join("failed-probes");
        let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
        std::fs::write(
            &wrapper,
            format!(
                "#!/bin/sh\nif test -f {}; then\n  for arg do\n    if test \"$arg\" = 'bookmarks(exact:\"{BRANCH}\")'; then\n      echo probe >> {}\n      echo forced-branch-tip-probe-failure >&2\n      exit 1\n    fi\n  done\nfi\ncase \" $* \" in\n  *' rebase -b {BRANCH} '*)\n    echo $$ >> {}\n    exec sleep 30\n    ;;\nesac\nexec {} \"$@\"\n",
                quote(&fail_tip.to_string_lossy()),
                quote(&failed_probes.to_string_lossy()),
                quote(&attempts.to_string_lossy()),
                quote(&bin),
            ),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let r = replay_harness(
            wrapper.to_str().unwrap(),
            "reconcile-worker-timeout.db",
            &[("base.txt", "base\n")],
        )
        .await;
        let original = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("branch.txt", "own work\n")],
            "branch work",
        );
        let clean_branch = "agent/timeout-clean";
        // The clean neighbour's progress is asserted, not its budget: under a
        // suite wave its production 10 s was spent before its first `jj op id`
        // (717ab57c, :13850). It gets a generous test budget; `BRANCH` keeps
        // the real automatic one, whose wiring is what this test proves.
        AUTOMATIC_REPLAY_TEST_BUDGETS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(
                (r.fx.store.clone(), clean_branch.to_string()),
                Duration::from_secs(120),
            );
        let clean_ws = r.fx.workspaces.path().join("timeout-clean");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &clean_ws, clean_branch, "main", None)
            .unwrap();
        commit(
            &r.fx,
            &clean_ws,
            clean_branch,
            &[("clean.txt", "sibling work\n")],
            "clean sibling work",
        );
        r.db.execute_script(
            "INSERT INTO jobs
             (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
             VALUES ('job-timeout-clean', 'exec-1', 'node-clean', 'issue-1', 'proj-1',
                     'running', 'agent/timeout-clean', 'main', 1, 1);",
        ).await.unwrap();
        let first_main = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[("incoming.txt", "first\n")],
            "first main advance",
        );
        set_main(&r.fx, &first_main);

        let attempt_pids = || {
            std::fs::read_to_string(&attempts)
                .unwrap_or_default()
                .lines()
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        let admission = async {
            tokio::time::timeout(Duration::from_secs(20), async {
                while attempt_pids().is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("worker must reach delayed rebase");
            let waiting = Instant::now();
            // A hang watchdog, not the assertion: what this proves is that the
            // writer is dead by the time the lock is handed over (below).
            let guard = tokio::time::timeout(
                crate::jj::BACKGROUND_RECONCILE_BUDGET
                    + crate::jj::RECONCILE_CLEANUP_BUDGET
                    + Duration::from_secs(60),
                r.orch.acquire_jj_store_lock(
                    &r.fx.store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    "foreground timeout regression",
                ),
            )
            .await
            .expect("foreground admission hung past budget, cleanup and a minute of slack");
            let pid = attempt_pids().pop().unwrap();
            let alive = std::process::Command::new("kill")
                .args(["-0", &pid])
                .output()
                .unwrap()
                .status
                .success();
            assert!(
                !alive,
                "timed-out writer {pid} must be reaped before lock admission"
            );
            r.fx.jj
                .run(
                    &r.fx.store,
                    &[
                        "bookmark",
                        "set",
                        "foreground-admitted",
                        "-r",
                        BRANCH,
                        "--ignore-working-copy",
                    ],
                    "foreground write after timeout",
                )
                .unwrap();
            eprintln!(
                "foreground admission after delayed rebase: {:?}",
                waiting.elapsed()
            );
            drop(guard);
        };
        let first_started = Instant::now();
        tokio::join!(timeout_worker_advance(&r, clean_branch), admission);
        // Duration is reported, not asserted: on a loaded host it measures the
        // host. The bound is proven by the killed writer and the timeout record.
        eprintln!("first timed-out pass: {:?}", first_started.elapsed());
        assert_eq!(attempt_pids().len(), 1);
        assert_eq!(branch_tip(&r), original, "timeout leaves bookmark intact");
        assert_eq!(text_at(&r.fx, BRANCH, "branch.txt"), "own work\n");
        assert!(!crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &first_main
        ));
        assert!(crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            clean_branch,
            &first_main
        ));
        let first = load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
            .await
            .unwrap()
            .expect("worker persists timeout cooldown");
        assert_eq!(first.failure_kind, RECONCILE_TIMEOUT);
        assert_eq!(first.fingerprint, format!("{original}:{first_main}"));
        assert_eq!(first.strike_count, 1);
        assert_eq!(first.retry_at() - first.updated_at, 300);
        let comment = r.db.query_text(
            "SELECT content FROM comments WHERE issue_id = 'issue-1' AND content LIKE '%timeout strike 1%' ORDER BY created_at DESC LIMIT 1",
            (),
        ).await.unwrap().expect("owning issue receives timeout diagnostic");
        assert!(
            comment.contains(&original) && comment.contains(&first_main),
            "{comment}"
        );

        let probe_main = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[("incoming.txt", "probe failure\n")],
            "main advances while branch tip cannot be read",
        );
        set_main(&r.fx, &probe_main);
        std::fs::write(&fail_tip, "fail exact branch probes\n").unwrap();
        timeout_worker_advance(&r, clean_branch).await;
        assert!(!std::fs::read_to_string(&failed_probes).unwrap().is_empty());
        assert_eq!(
            attempt_pids().len(),
            1,
            "unknown tip must not reopen timed-out work"
        );
        let unknown = load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
            .await
            .unwrap()
            .expect("failed tip probe preserves circuit breaker");
        assert_eq!(unknown.fingerprint, first.fingerprint);
        assert_eq!(unknown.strike_count, first.strike_count);
        assert_eq!(unknown.updated_at, first.updated_at);
        assert!(crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            clean_branch,
            &probe_main
        ));
        std::fs::remove_file(&fail_tip).unwrap();

        let second_main = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[("incoming.txt", "second\n")],
            "second main advance",
        );
        set_main(&r.fx, &second_main);
        timeout_worker_advance(&r, clean_branch).await;
        assert_eq!(
            attempt_pids().len(),
            1,
            "a main advance must not retry unchanged branch"
        );
        assert_eq!(branch_tip(&r), original);
        assert!(crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            clean_branch,
            &second_main
        ));
        let unchanged = load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unchanged.fingerprint, first.fingerprint);
        assert_eq!(unchanged.updated_at, first.updated_at);
        assert_eq!(unchanged.strike_count, 1);

        let changed = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("child.txt", "new branch commit\n")],
            "branch changes during cooldown",
        );
        assert_ne!(changed, original);
        let retry_started = Instant::now();
        // A hang watchdog only. Under the 300s default the 30s sleep would
        // exit cleanly and record no timeout; the timeout record below is what
        // proves the retry ran under the 10s background budget.
        tokio::time::timeout(
            Duration::from_secs(120),
            timeout_worker_advance(&r, clean_branch),
        )
        .await
        .expect("changed-tip retry hung");
        eprintln!("changed-tip worker retry: {:?}", retry_started.elapsed());
        assert_eq!(
            attempt_pids().len(),
            2,
            "exactly one retry after branch change"
        );
        let retried_pid = attempt_pids().pop().unwrap();
        assert!(
            !std::process::Command::new("kill")
                .args(["-0", &retried_pid])
                .output()
                .unwrap()
                .status
                .success(),
            "the retry's delayed writer {retried_pid} was killed and reaped"
        );
        assert_eq!(
            branch_tip(&r),
            changed,
            "retry timeout leaves new bookmark intact"
        );
        assert_eq!(text_at(&r.fx, BRANCH, "child.txt"), "new branch commit\n");
        assert!(!crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &second_main
        ));
        let retried = load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
            .await
            .unwrap()
            .expect("retry records fresh cooldown");
        assert_eq!(retried.failure_kind, RECONCILE_TIMEOUT);
        assert_eq!(retried.fingerprint, format!("{changed}:{second_main}"));
        assert_eq!(
            retried.strike_count, 1,
            "new branch coordinates reset strikes"
        );
        assert_eq!(retried.retry_at() - retried.updated_at, 300);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn structural_timeout_restores_the_entire_attempt_and_persists_backoff() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;

        let bin =
            crate::jj::tests::jj_bin().expect("real jj required for structural timeout regression");
        let r = replay_harness(&bin, "structural-timeout.db", &[]).await;
        add_migration(&r.branch_ws, "0230_branch");
        crate::jj::seal(&r.fx.jj, &r.branch_ws, "branch registry", None).unwrap();
        let original = branch_tip(&r);
        add_migration(&r.main_ws, "0228_main");
        crate::jj::seal(&r.fx.jj, &r.main_ws, "main registry", None).unwrap();
        let destination = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, MAIN_LINE).unwrap();
        set_main(&r.fx, &destination);
        let crate::jj::RebaseOutcome::Conflicted { diagnostic } =
            crate::jj::rebase_branch_onto(&r.fx.jj, &r.fx.store, BRANCH, "main").unwrap()
        else {
            panic!("registry insertions must exercise structural repair")
        };
        let rules =
            crate::orchestrator::landing::merge_rules_at(&r.fx.jj, &r.fx.store, &destination);
        let plan = prepare_structural_merge(
            &r.fx.jj,
            &r.fx.store,
            &diagnostic,
            &ReplayBaseline::fork_only(diagnostic.base.as_deref().unwrap()),
            &rules,
        )
        .unwrap()
        .expect("declared registry produces a lossless candidate");
        let visible = || {
            r.fx.jj
                .run(
                    &r.fx.store,
                    &[
                        "log",
                        "-r",
                        "all()",
                        "--no-graph",
                        "-T",
                        "commit_id ++ \"\\n\"",
                        "--ignore-working-copy",
                    ],
                    "visible graph around structural timeout",
                )
                .unwrap()
                .lines()
                .map(str::to_string)
                .collect::<BTreeSet<_>>()
        };
        let before = visible();
        let home = tempfile::tempdir().unwrap();
        let wrapper = home.path().join("structural-delay");
        let mode = home.path().join("mode");
        let reached = home.path().join("reached");
        let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
        std::fs::write(&wrapper, format!(
            "#!/bin/sh\nmode=$(cat {})\nif [ \"$1\" = commit ]; then\n  {} \"$@\" || exit $?\n  if [ \"$mode\" = seal ]; then\n    echo seal > {}\n    exec sleep 300\n  fi\n  exit 0\nfi\nif [ \"$1\" = rebase ] && [ \"$mode\" = replay ]; then\n  echo replay > {}\n  exec sleep 300\nfi\nexec {} \"$@\"\n",
            quote(&mode.to_string_lossy()), quote(&bin), quote(&reached.to_string_lossy()),
            quote(&reached.to_string_lossy()), quote(&bin),
        )).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let delayed = crate::jj::JjEnv::with_binary(wrapper.to_string_lossy(), &r.fx.home);
        for (index, stage) in ["seal", "replay"].into_iter().enumerate() {
            std::fs::write(&mode, stage).unwrap();
            std::fs::write(&reached, "").unwrap();
            let guard = r
                .orch
                .acquire_jj_store_lock(
                    &r.fx.store,
                    crate::orchestrator::StoreLockClass::Background,
                    format!("structural timeout at {stage}"),
                )
                .await;
            // The deadline must expire AT the stage, never in the setup before
            // it: a 5 s budget let a loaded host spend it before `rebase` was
            // reached, and the attempt timed out with an empty stage marker
            // (a2785171). So the budget sits far above any setup cost and the
            // wrapper parks far past it once the stage is entered; the timeout
            // can then only fire there.
            const STAGE_BUDGET: Duration = Duration::from_secs(30);
            let budgeted = delayed.with_operation_budget(STAGE_BUDGET);
            let error = match apply_structural_merge(&budgeted, &r.fx.store, BRANCH, "main", &plan)
            {
                Err(error) => error,
                _ => panic!("delayed structural {stage} must time out"),
            };
            assert_eq!(
                std::fs::read_to_string(&reached).unwrap().trim(),
                stage,
                "the {stage} stage was not entered before the {}s budget expired, so setup \
                 outran the budget: {error}",
                STAGE_BUDGET.as_secs()
            );
            assert_eq!(
                crate::jj::reconcile_failure_kind(&error),
                RECONCILE_TIMEOUT,
                "{error}"
            );
            assert!(!error.contains(crate::jj::ROLLBACK_INCOMPLETE), "{error}");
            let mut report = failed_reconcile_report(BRANCH, error);
            assert!(persist_report_timeouts(
                &r.db,
                "proj-1",
                &r.fx.store,
                BRANCH,
                &original,
                &destination,
                &mut report,
            )
            .await
            .unwrap());
            drop(guard);
            assert_eq!(
                branch_tip(&r),
                original,
                "entire structural attempt was undone"
            );
            assert_eq!(
                visible(),
                before,
                "no synthetic or divergent commit remains visible"
            );
            let cooldown = load_reconcile_quarantine(&r.db, "proj-1", &r.fx.store, BRANCH)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(cooldown.fingerprint, format!("{original}:{destination}"));
            assert_eq!(cooldown.strike_count, index as i64 + 1);
            assert!(quarantine_blocks_automatic_replay(
                &cooldown,
                Some(&original),
                "next main advance"
            ));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn structural_replay_rechecks_batches_admitted_during_preparation() {
        use std::time::Duration;
        let bin = crate::jj::tests::jj_bin().expect("real jj required for structural admission");
        let r = replay_harness(&bin, "structural-admission.db", &[]).await;
        // This test is about admission, not the budget: under deliberate load
        // (sp1, I/O PSI full ~26%) the durable retry's production 10 s was spent
        // in classification and the run ended fenced instead of carried. The
        // retry gets a non-starving test budget so the recheck is what is tested.
        AUTOMATIC_REPLAY_TEST_BUDGETS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(
                (r.fx.store.clone(), BRANCH.to_string()),
                Duration::from_secs(120),
            );
        add_migration(&r.branch_ws, "0230_branch");
        crate::jj::seal(&r.fx.jj, &r.branch_ws, "branch registry", None).unwrap();
        let original = branch_tip(&r);
        add_migration(&r.main_ws, "0228_main");
        crate::jj::seal(&r.fx.jj, &r.main_ws, "main registry", None).unwrap();
        let destination = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, MAIN_LINE).unwrap();
        set_main(&r.fx, &destination);

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        STRUCTURAL_PREPARED_HOOKS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(
                r.fx.store.clone(),
                Box::new(move || {
                    let _ = entered_tx.send(());
                    resume_rx.recv_timeout(Duration::from_secs(20)).unwrap();
                }),
            );
        let admit = async {
            tokio::time::timeout(Duration::from_secs(20), entered_rx)
                .await
                .expect("worker prepares structural candidate")
                .unwrap();
            let guard = r
                .orch
                .acquire_jj_store_lock(
                    &r.fx.store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    "admit run batch while structural proof is prepared",
                )
                .await;
            for sql in [
                "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-structural','job-replay','open','backend',1,1)",
                "UPDATE runs SET session_id='session-structural' WHERE id='run-replay'",
                "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-structural','session-structural','run-replay','job-replay',1,'yielded','initial',1,1)",
                "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-structural','job-replay','run-replay','session-structural','turn-structural','tool-structural','{\"kind\":\"run_batch\",\"request_id\":\"structural-batch\",\"commits\":true}','pending',1)",
            ] {
                r.db.execute(sql, ()).await.unwrap();
            }
            drop(guard);
            resume_tx.send(()).unwrap();
        };
        tokio::join!(automatic_advance(&r), admit);
        assert_eq!(
            branch_tip(&r),
            original,
            "structural phase must not seal under an admitted batch"
        );
        assert!(!crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &destination
        ));
        let item = r.db.query_text(
            "SELECT outcome_kind FROM jj_reconcile_items WHERE bookmark = ?1 ORDER BY updated_at DESC LIMIT 1",
            (BRANCH.to_string(),),
        ).await.unwrap();
        assert_eq!(item.as_deref(), Some("deferred"));

        r.db.execute("DELETE FROM agent_waits WHERE id='wait-structural'", ())
            .await
            .unwrap();
        sweep_reconcile_intents(&r.orch).await;
        wait_for_worker(&r).await;
        // A proven end state, and only one of two. Either the retry finished
        // and carries the batch's destination, or its rollback could not be
        // verified and the branch is fenced with exact coordinates, the owner
        // told and publication refused. Nothing in between passes.
        if crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &destination) {
            return;
        }
        let fences = crate::orchestrator::branch_recovery::fences(&r.db, &r.fx.store)
            .await
            .unwrap();
        let fence = fences
            .iter()
            .find(|fence| fence.affected.iter().any(|name| name == BRANCH))
            .unwrap_or_else(|| {
                panic!(
                    "durable retry neither finished nor left a recovery fence on {BRANCH}: \
                     {fences:?}"
                )
            });
        assert_eq!(fence.phase, "export_unverified", "{fence:?}");
        let recorded = fence
            .coordinates
            .bookmarks
            .iter()
            .find(|bookmark| bookmark.bookmark == BRANCH)
            .expect("the fence names the branch's coordinates");
        assert_eq!(
            recorded.pre,
            cairn_vcs::BookmarkState::normal(&original),
            "the fence records the exact pre-attempt target"
        );
        assert!(fence.coordinates.pre_op.is_some(), "{fence:?}");
        assert!(
            r.db.query_text(
                "SELECT content FROM comments WHERE issue_id = 'issue-1' AND content LIKE ?1 LIMIT 1",
                (format!("%{}%", fence.id),),
            )
            .await
            .unwrap()
            .is_some(),
            "the owning issue is told about the fence"
        );
        let refused = publish_managed_branch(&r.orch, &r.fx.store, BRANCH)
            .await
            .unwrap_err();
        assert!(
            refused.contains(&fence.id),
            "publication is refused: {refused}"
        );
        eprintln!("structural admission retry ended fenced: {}", fence.id);
    }

    // ── Recovery fences (cairn/6173) ─────────────────────────────────────────

    fn git_head(r: &Replay, branch: &str) -> String {
        let out = crate::env::git()
            .args([
                "rev-parse",
                "--verify",
                "-q",
                &format!("refs/heads/{branch}"),
            ])
            .current_dir(&r.fx.repo)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// The c43a1f1e shape end to end: a budgeted rollback whose export after
    /// `op restore` cannot be verified records a branch fence. The fence
    /// refuses an explicit replay of that branch with its coordinates and
    /// route, leaves an unrelated branch admitted, survives a fresh read of the
    /// table, and is cleared only by the scheduled targeted repair, which puts
    /// the Git head back and tells the owning issue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn an_unverified_rollback_fences_its_branch_until_the_targeted_repair_verifies_it() {
        use crate::orchestrator::branch_recovery;
        let bin = crate::jj::tests::jj_bin().expect("real jj required for a recovery fence");
        let r = replay_harness(&bin, "recovery-fence.db", &[]).await;
        let own = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("own.txt", "own\n")],
            "own work",
        );
        crate::jj::export_git_verified(&r.fx.jj, &r.fx.store, true, "fixture export", &[]).unwrap();
        assert_eq!(git_head(&r, BRANCH), own);
        let main = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, "main").unwrap();

        // The forward mutation of a budgeted transaction, then a rollback
        // whose export Git refuses because the ref is locked.
        let jj =
            r.fx.jj
                .with_operation_budget(std::time::Duration::from_secs(60));
        let op = crate::jj::operation_id(&jj, &r.fx.store).unwrap();
        jj.run(
            &r.fx.store,
            &[
                "bookmark",
                "set",
                BRANCH,
                "-r",
                &main,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "forward mutation",
        )
        .unwrap();
        crate::jj::export_git_verified(&jj, &r.fx.store, true, "forward export", &[]).unwrap();
        let lock =
            r.fx.repo
                .join(".git/refs/heads")
                .join(format!("{BRANCH}.lock"));
        std::fs::write(&lock, "").unwrap();
        let error = crate::jj::restore_operation(&jj.cleanup_env(), &r.fx.store, &op).unwrap_err();
        assert!(error.contains(crate::jj::ROLLBACK_INCOMPLETE), "{error}");

        let mut report = failed_reconcile_report(BRANCH, error);
        assert!(
            persist_transaction_recoveries(
                &r.db,
                "proj-1",
                &r.fx.store,
                BRANCH,
                "merged job (test)",
                "sibling reconcile (test)",
                &jj,
                &mut report,
            )
            .await
        );
        let fences = branch_recovery::fences(&r.db, &r.fx.store).await.unwrap();
        assert_eq!(fences.len(), 1, "{fences:?}");
        let fence = &fences[0];
        assert_eq!(fence.scope, crate::jj::RecoveryScope::Branch);
        assert_eq!(fence.affected, vec![BRANCH.to_string()]);
        assert_eq!(fence.budget_class, branch_recovery::BUDGET_AUTOMATIC);
        assert_eq!(fence.coordinates.pre_op.as_deref(), Some(op.as_str()));
        assert!(
            report.failed[0].error.contains(&fence.id),
            "{:?}",
            report.failed
        );

        // An explicit replay is refused by name, with the route.
        let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, false, None)
            .await
            .unwrap_err();
        assert!(refusal.contains(&fence.id), "{refusal}");
        assert!(
            refusal.contains(BRANCH) && refusal.contains("automatic"),
            "{refusal}"
        );
        assert!(refusal.contains("{action:\"repair\"}"), "{refusal}");
        // An unrelated branch is not fenced.
        branch_recovery::refuse_if_fenced(&r.db, &r.fx.store, &["agent/unrelated"], "Probe")
            .await
            .unwrap();
        // Tip movement, a new base or a cooldown do not clear it: only repair.
        let reread = branch_recovery::fences(&r.db, &r.fx.store).await.unwrap();
        assert_eq!(reread.len(), 1);

        // The repair is not due yet; once due, a still-locked ref keeps the fence.
        r.db.execute("UPDATE jj_branch_recoveries SET next_repair_at = 0", ())
            .await
            .unwrap();
        branch_recovery::run_due_recoveries(&r.orch, &r.db).await;
        let kept = branch_recovery::fences(&r.db, &r.fx.store).await.unwrap();
        assert_eq!(kept.len(), 1, "a refused repair keeps the fence");
        assert_eq!(kept[0].repair_attempts, 1);
        assert!(kept[0].next_repair_at >= chrono::Utc::now().timestamp() + 250);

        std::fs::remove_file(&lock).unwrap();
        r.db.execute("UPDATE jj_branch_recoveries SET next_repair_at = 0", ())
            .await
            .unwrap();
        branch_recovery::run_due_recoveries(&r.orch, &r.db).await;
        assert!(branch_recovery::fences(&r.db, &r.fx.store)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            git_head(&r, BRANCH),
            own,
            "the repair put the Git head back"
        );
        assert_eq!(branch_tip(&r), own);
        let notice =
            r.db.query_text(
                "SELECT content FROM comments WHERE issue_id = 'issue-1' AND content LIKE ?1
                 ORDER BY created_at DESC LIMIT 1",
                (format!("%{}%repaired and verified%", fence.id),),
            )
            .await
            .unwrap()
            .expect("the owning issue hears the repair outcome");
        assert!(notice.contains(BRANCH), "{notice}");
    }

    /// Lay a branch fence on `BRANCH` the way an unverified rollback does:
    /// forward move to `main`, then an `op restore` whose export Git refuses.
    /// Returns the branch's own tip and the held ref lock, to remove to let the
    /// repair export.
    async fn lay_branch_fence(r: &Replay) -> (String, PathBuf) {
        let own = commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("own.txt", "own\n")],
            "own work",
        );
        crate::jj::export_git_verified(&r.fx.jj, &r.fx.store, true, "fixture export", &[]).unwrap();
        let main = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, "main").unwrap();
        let jj =
            r.fx.jj
                .with_operation_budget(std::time::Duration::from_secs(60));
        let op = crate::jj::operation_id(&jj, &r.fx.store).unwrap();
        jj.run(
            &r.fx.store,
            &[
                "bookmark",
                "set",
                BRANCH,
                "-r",
                &main,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "forward mutation",
        )
        .unwrap();
        crate::jj::export_git_verified(&jj, &r.fx.store, true, "forward export", &[]).unwrap();
        let lock =
            r.fx.repo
                .join(".git/refs/heads")
                .join(format!("{BRANCH}.lock"));
        std::fs::write(&lock, "").unwrap();
        let error = crate::jj::restore_operation(&jj.cleanup_env(), &r.fx.store, &op).unwrap_err();
        let mut report = failed_reconcile_report(BRANCH, error);
        assert!(
            persist_transaction_recoveries(
                &r.db,
                "proj-1",
                &r.fx.store,
                BRANCH,
                "merged job (test)",
                "sibling reconcile (test)",
                &jj,
                &mut report,
            )
            .await
        );
        (own, lock)
    }

    /// Review finding 1: a repair that outlives its allowance must neither
    /// hold the store lock until it finishes nor be left writing unfenced. The
    /// worker is paused after it is admitted to write. The attempt escalates
    /// the record to a store fence and releases the lock; while the worker
    /// lives, a second attempt changes nothing and an unrelated write is
    /// refused. Once it finishes, the next attempt validates and clears.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn a_repair_that_outlives_its_allowance_fences_the_store_until_its_worker_ends() {
        use crate::orchestrator::branch_recovery;
        let bin = crate::jj::tests::jj_bin().expect("real jj required for a recovery fence");
        let r = replay_harness(&bin, "recovery-rendezvous.db", &[]).await;
        let (own, lock) = lay_branch_fence(&r).await;
        std::fs::remove_file(&lock).unwrap();
        let id = branch_recovery::fences(&r.db, &r.fx.store).await.unwrap()[0]
            .id
            .clone();

        let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let reached_tx = std::sync::Mutex::new(reached_tx);
        let release_rx = std::sync::Mutex::new(release_rx);
        let _hook = branch_recovery::test_support::pause_repairs_after_write_admission(
            std::time::Duration::from_millis(500),
            move || {
                let _ = reached_tx.lock().unwrap().send(());
                let _ = release_rx.lock().unwrap().recv();
            },
        );
        r.db.execute("UPDATE jj_branch_recoveries SET next_repair_at = 0", ())
            .await
            .unwrap();
        // The attempt returns, bounded, with the worker still paused.
        tokio::time::timeout(
            std::time::Duration::from_secs(60),
            branch_recovery::run_due_recoveries(&r.orch, &r.db),
        )
        .await
        .expect("the attempt is bounded by its allowance, not by its worker");
        reached_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the worker reached write admission");
        let fence = branch_recovery::fences(&r.db, &r.fx.store).await.unwrap();
        assert_eq!(fence.len(), 1);
        assert_eq!(fence[0].id, id);
        assert_eq!(fence[0].scope, crate::jj::RecoveryScope::Store);
        assert_eq!(fence[0].phase, "repair_interrupted");
        // The store lock is free, and exclusion is held by the fence instead.
        let guard = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            r.orch.acquire_jj_store_lock(
                &r.fx.store,
                crate::orchestrator::StoreLockClass::Foreground,
                "probe while the worker lives",
            ),
        )
        .await
        .expect("the store lock was released at the allowance");
        let refused = branch_recovery::refuse_if_fenced(
            &r.db,
            &r.fx.store,
            &["agent/unrelated"],
            "This write",
        )
        .await
        .unwrap_err();
        assert!(refused.contains(&id), "{refused}");
        drop(guard);
        // A second attempt while the worker lives clears nothing.
        r.db.execute("UPDATE jj_branch_recoveries SET next_repair_at = 0", ())
            .await
            .unwrap();
        branch_recovery::run_due_recoveries(&r.orch, &r.db).await;
        let kept = branch_recovery::fences(&r.db, &r.fx.store).await.unwrap();
        assert_eq!(
            kept.len(),
            1,
            "nothing clears a fence while its worker may commit"
        );
        assert!(
            kept[0]
                .last_repair_outcome
                .as_deref()
                .is_some_and(|outcome| outcome.contains("still running")),
            "{:?}",
            kept[0].last_repair_outcome
        );

        // Let the first worker finish its write, then validate.
        release_tx.send(()).unwrap();
        let finished = std::time::Instant::now();
        loop {
            r.db.execute("UPDATE jj_branch_recoveries SET next_repair_at = 0", ())
                .await
                .unwrap();
            branch_recovery::run_due_recoveries(&r.orch, &r.db).await;
            if branch_recovery::fences(&r.db, &r.fx.store)
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            assert!(
                finished.elapsed() < std::time::Duration::from_secs(60),
                "the fence never cleared: {:?}",
                branch_recovery::fences(&r.db, &r.fx.store).await.unwrap()
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(branch_tip(&r), own);
        assert_eq!(git_head(&r, BRANCH), own);
    }

    /// Review finding 3: managed origin imports honour fences. A store fence
    /// refuses any import; a branch fence refuses one that names the fenced
    /// bookmark or would carry it off its position, and admits the rest. The
    /// scheduled default-bookmark reconcile is refused at its own boundary.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn origin_imports_honour_branch_and_store_fences() {
        use crate::orchestrator::branch_recovery;
        let bin = crate::jj::tests::jj_bin().expect("real jj required for a recovery fence");
        let r = replay_harness(&bin, "recovery-imports.db", &[]).await;
        let (_own, lock) = lay_branch_fence(&r).await;
        std::fs::remove_file(&lock).unwrap();

        let refused = branch_recovery::refuse_import_if_fenced(
            &r.db,
            &r.fx.store,
            &[BRANCH],
            "Fast-forwarding",
        )
        .await
        .unwrap_err();
        assert!(refused.contains(BRANCH), "{refused}");
        branch_recovery::refuse_import_if_fenced(&r.db, &r.fx.store, &["main"], "Importing main")
            .await
            .expect("an import that leaves the fenced bookmark alone is admitted");

        // Git moves the fenced ref behind jj: an import would follow it.
        let main = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, "main").unwrap();
        let git_out = |args: &[&str]| {
            let out = crate::env::git()
                .args(args)
                .current_dir(&r.fx.repo)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8(out.stdout).unwrap().trim().to_string()
        };
        let before = git_head(&r, BRANCH);
        let foreign = git_out(&[
            "commit-tree",
            "-p",
            &main,
            "-m",
            "moved behind jj",
            &format!("{main}^{{tree}}"),
        ]);
        git_out(&["update-ref", &format!("refs/heads/{BRANCH}"), &foreign]);
        let refused = branch_recovery::refuse_import_if_fenced(
            &r.db,
            &r.fx.store,
            &["main"],
            "Importing main",
        )
        .await
        .unwrap_err();
        assert!(refused.contains("behind jj"), "{refused}");
        git_out(&["update-ref", &format!("refs/heads/{BRANCH}"), &before]);

        // A store fence refuses every import, the scheduled default-bookmark
        // reconcile included, at its own locked boundary.
        r.db.execute("UPDATE jj_branch_recoveries SET scope = 'store'", ())
            .await
            .unwrap();
        let refused = reconcile_store_default_bookmark(
            &r.orch,
            &r.db,
            "proj-1",
            &r.fx.repo,
            "main",
            "test advance",
        )
        .await
        .unwrap_err();
        assert!(
            refused.contains("Reconciling `main` onto origin refused"),
            "{refused}"
        );
        assert!(refused.contains("store scope"), "{refused}");
    }

    /// Review finding 4: a recovery the database refuses survives a restart.
    /// The fallback marker refuses admission after this process forgets the
    /// failure, stays until the sweep has recorded it, and only then leaves.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn an_unrecorded_recovery_survives_a_restart_until_the_sweep_records_it() {
        use crate::orchestrator::branch_recovery;
        let bin = crate::jj::tests::jj_bin().expect("real jj required");
        let r = replay_harness(&bin, "recovery-marker.db", &[]).await;
        r.db.execute(
            "ALTER TABLE jj_branch_recoveries RENAME TO jj_branch_recoveries_away",
            (),
        )
        .await
        .unwrap();
        let coordinates = crate::jj::RecoveryCoordinates {
            scope: crate::jj::RecoveryScope::Store,
            phase: crate::jj::RollbackFailurePhase::RestoreOutcomeUnknown,
            pre_op: Some("0".repeat(40)),
            pre_view: None,
            pre_git: None,
            failed_view: None,
            bookmarks: Vec::new(),
            survivors: Vec::new(),
            evidence: vec!["test".into()],
        };
        let error = branch_recovery::record_recoveries(
            &r.db,
            "proj-1",
            &r.fx.store,
            Some(BRANCH),
            branch_recovery::BUDGET_AUTOMATIC,
            "test",
            vec![coordinates],
        )
        .await
        .unwrap_err();
        assert!(error.contains("until the sweep records it"), "{error}");
        let marker = r.fx.store.join(".jj/cairn-unrecorded-recoveries.json");
        assert!(marker.exists());

        // A restart: the process-lifetime refusal is gone, the marker is not.
        branch_recovery::forget_process_state_for_test();
        let refused =
            branch_recovery::refuse_if_fenced(&r.db, &r.fx.store, &["agent/other"], "Probe")
                .await
                .unwrap_err();
        assert!(
            refused.contains("could not be written to the database"),
            "{refused}"
        );

        // While the table is still unwritable, the sweep keeps the marker.
        branch_recovery::run_due_recoveries(&r.orch, &r.db).await;
        assert!(
            marker.exists(),
            "the marker is removed only after a recorded insert"
        );

        r.db.execute(
            "ALTER TABLE jj_branch_recoveries_away RENAME TO jj_branch_recoveries",
            (),
        )
        .await
        .unwrap();
        branch_recovery::run_due_recoveries(&r.orch, &r.db).await;
        assert!(!marker.exists(), "recorded, so the marker is gone");
        let fences = branch_recovery::fences(&r.db, &r.fx.store).await.unwrap();
        assert_eq!(fences.len(), 1, "the recovery now stands in the table");
        assert_eq!(fences[0].scope, crate::jj::RecoveryScope::Store);
        branch_recovery::refuse_if_fenced(&r.db, &r.fx.store, &["agent/other"], "Probe")
            .await
            .unwrap_err();

        // An unreadable marker refuses rather than reading as absent.
        std::fs::write(&marker, "not json").unwrap();
        let refused =
            branch_recovery::refuse_if_fenced(&r.db, &r.fx.store, &["agent/other"], "Probe")
                .await
                .unwrap_err();
        assert!(refused.contains("unreadable"), "{refused}");
        std::fs::remove_file(&marker).unwrap();
    }

    /// A recovery that cannot be recorded keeps the store refused in process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn a_recovery_that_cannot_be_persisted_keeps_the_store_refused() {
        use crate::orchestrator::branch_recovery;
        let bin = crate::jj::tests::jj_bin().expect("real jj required");
        let r = replay_harness(&bin, "recovery-unpersisted.db", &[]).await;
        r.db.execute("DROP TABLE jj_branch_recoveries", ())
            .await
            .unwrap();
        let coordinates = crate::jj::RecoveryCoordinates {
            scope: crate::jj::RecoveryScope::Store,
            phase: crate::jj::RollbackFailurePhase::RestoreOutcomeUnknown,
            pre_op: Some("0".repeat(40)),
            pre_view: None,
            pre_git: None,
            failed_view: None,
            bookmarks: Vec::new(),
            survivors: Vec::new(),
            evidence: vec!["test".into()],
        };
        let error = branch_recovery::record_recoveries(
            &r.db,
            "proj-1",
            &r.fx.store,
            Some(BRANCH),
            branch_recovery::BUDGET_AUTOMATIC,
            "test",
            vec![coordinates],
        )
        .await
        .unwrap_err();
        assert!(
            error.contains("recording its recovery fence in the database failed"),
            "{error}"
        );
        let refused =
            branch_recovery::refuse_if_fenced(&r.db, &r.fx.store, &["agent/other"], "Probe")
                .await
                .unwrap_err();
        assert!(refused.contains("Probe refused"), "{refused}");
    }

    // ── A pass whose only unfinished branches are held at the boundary ──────
    //
    // cairn/6175. `BRANCH` (job `JOB`) is held by a run batch that stays in
    // flight; `HEALTHY` is an ordinary sibling on the same base.

    const HEALTHY_JOB: &str = "job-healthy";
    const HEALTHY: &str = "agent/healthy";

    /// The harness plus a second live job on `HEALTHY`, both cut from `main`,
    /// each carrying one commit of its own, and `main` advanced past both.
    /// Returns the advanced main.
    async fn held_and_healthy(r: &Replay) -> String {
        let healthy_ws = r.fx.workspaces.path().join("healthy");
        crate::jj::add_workspace(&r.fx.jj, &r.fx.store, &healthy_ws, HEALTHY, "main", None)
            .unwrap();
        r.db.execute_script(format!(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-2', 'proj-1', 2, 'Healthy', 'active', 1, 1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('exec-2', 'recipe-default', 'issue-2', 'proj-1', 'running', 1, 1);
             INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
               VALUES ('{HEALTHY_JOB}', 'exec-2', 'node', 'issue-2', 'proj-1', 'running', '{HEALTHY}', 'main', 1, 1);"
        ))
        .await
        .unwrap();
        commit(
            &r.fx,
            &r.branch_ws,
            BRANCH,
            &[("held.txt", "held\n")],
            "held work",
        );
        commit(
            &r.fx,
            &healthy_ws,
            HEALTHY,
            &[("healthy.txt", "healthy\n")],
            "healthy work",
        );
        let destination = commit(
            &r.fx,
            &r.main_ws,
            MAIN_LINE,
            &[("main.txt", "main\n")],
            "main advances",
        );
        set_main(&r.fx, &destination);
        destination
    }

    /// Park a run batch on `JOB`, which holds `BRANCH` at the mutation boundary.
    async fn hold_branch(r: &Replay) {
        for sql in [
            "INSERT INTO sessions (id,job_id,status,backend_id,created_at,updated_at) VALUES ('session-held','job-replay','open','backend',1,1)",
            "UPDATE runs SET session_id='session-held' WHERE id='run-replay'",
            "INSERT INTO turns (id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES ('turn-held','session-held','run-replay','job-replay',1,'yielded','initial',1,1)",
            "INSERT INTO agent_waits (id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at) VALUES ('wait-held','job-replay','run-replay','session-held','turn-held','tool-held','{\"kind\":\"run_batch\",\"request_id\":\"held-batch\",\"commits\":true,\"label\":\"long verification\"}','pending',1)",
        ] {
            r.db.execute(sql, ()).await.unwrap();
        }
    }

    async fn intent_status(r: &Replay, destination: &str) -> String {
        r.db.query_text(
            "SELECT status FROM jj_reconcile_intents WHERE destination_commit = ?1",
            (destination.to_string(),),
        )
        .await
        .unwrap()
        .expect("the advance recorded an intent")
    }

    /// `(status, outcome_kind, attempt_count)` of one branch's item.
    async fn held_item(r: &Replay, destination: &str) -> (String, String, i64) {
        r.db.query_one(
            "SELECT item.status, COALESCE(item.outcome_kind, ''), item.attempt_count
               FROM jj_reconcile_items item
               JOIN jj_reconcile_intents intent ON intent.id = item.intent_id
              WHERE intent.destination_commit = ?1 AND item.bookmark = ?2",
            (destination.to_string(), BRANCH.to_string()),
            |row| Ok((row.text(0)?, row.text(1)?, row.get::<i64>(2)?)),
        )
        .await
        .unwrap()
    }

    /// A held sibling used to keep its intent alive for as long as its batch
    /// ran: every pass re-deferred it, returned the intent to `pending`, and the
    /// sweep claimed it again, so the intent read `running` for tens of minutes
    /// and refused every explicit replay at its destination. The pass now ends
    /// when the healthy siblings are done, the held branch keeps its debt, and
    /// the sweep replays it once the batch ends, with no further base advance.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn a_pass_left_with_only_held_branches_completes_and_replays_them_once_released() {
        let bin = crate::jj::tests::jj_bin().expect("real jj required for a held-branch pass");
        let r = replay_harness(&bin, "held-pass-completes.db", &[]).await;
        let destination = held_and_healthy(&r).await;
        hold_branch(&r).await;

        let outcome = reconcile_base_advance(
            &r.orch,
            &r.db,
            "proj-1",
            "merged job (test)",
            &r.fx.repo.to_string_lossy(),
            "main",
            "main",
            load_sibling_jobs(&r.db, "proj-1", "main", EXCLUDE_NONE)
                .await
                .unwrap(),
            BaseAdvanceNotes {
                conflict: build_jj_conflict_note("main", None, None),
                clean: build_jj_clean_note("main", None, None),
                incoming: IncomingIdentity {
                    base_branch: "main".to_string(),
                    ..IncomingIdentity::default()
                },
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome.eligible, 2, "the advance claimed both siblings");
        wait_for_worker(&r).await;

        assert!(crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            HEALTHY,
            &destination
        ));
        assert!(!crate::jj::branch_carries_commit(
            &r.fx.jj,
            &r.fx.store,
            BRANCH,
            &destination
        ));
        assert_eq!(
            intent_status(&r, &destination).await,
            "completed",
            "a pass with only held branches left is finished, not waiting on them"
        );
        let (status, outcome_kind, attempts) = held_item(&r, &destination).await;
        assert_eq!(
            (status.as_str(), outcome_kind.as_str()),
            ("pending", "deferred")
        );

        // While the batch runs, the sweep neither reopens the intent nor
        // re-defers the branch across revisits.
        for _ in 0..2 {
            sweep_reconcile_intents(&r.orch).await;
            wait_for_worker(&r).await;
        }
        assert_eq!(intent_status(&r, &destination).await, "completed");
        assert_eq!(
            held_item(&r, &destination).await,
            ("pending".to_string(), "deferred".to_string(), attempts),
            "a held branch is not revisited while its batch is in flight"
        );

        r.db.execute("DELETE FROM agent_waits WHERE id = 'wait-held'", ())
            .await
            .unwrap();
        sweep_reconcile_intents(&r.orch).await;
        wait_for_worker(&r).await;
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &destination),
            "the deferred branch is replayed once its batch ends, without a new advance"
        );
        assert_eq!(intent_status(&r, &destination).await, "completed");
        assert_ne!(held_item(&r, &destination).await.0, "pending");
    }

    /// An explicit replay requested while a pass runs waits for that pass and is
    /// served by the follow-on on the same intent as soon as it completes. The
    /// requester is told it was accepted, not handed the lease refusal.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(jj)]
    async fn an_explicit_replay_waiting_on_a_pass_is_accepted_when_it_completes() {
        use std::time::Duration;
        let bin = crate::jj::tests::jj_bin().expect("real jj required for a follow-on pass");
        let r = replay_harness(&bin, "explicit-follow-on.db", &[]).await;
        let destination = held_and_healthy(&r).await;
        hold_branch(&r).await;

        // The pass in flight at this destination, its remaining work held.
        let pass = claim_reconcile_intent(
            &r.db,
            "proj-1",
            &r.fx.store,
            "main",
            &destination,
            "merged job (test)",
        )
        .await
        .unwrap()
        .expect("the pass owns the destination");

        let orch = r.orch.clone();
        let db = Arc::clone(&r.db);
        let mut request = tokio::spawn(async move {
            request_branch_replay(&orch, &db, HEALTHY_JOB, HEALTHY, None, false, None).await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(500), &mut request)
                .await
                .is_err(),
            "the request waits for the running pass"
        );
        sweep_reconcile_intents(&r.orch).await;
        assert!(
            !request.is_finished(),
            "the sweep does not take a live pass"
        );

        finish_reconcile_intent(&r.db, &pass.id, &pass.owner, false)
            .await
            .unwrap();
        let answer = tokio::time::timeout(Duration::from_secs(120), request)
            .await
            .expect("the request is served once the pass completes")
            .unwrap()
            .unwrap_or_else(|refusal| panic!("the request was refused: {refusal}"));
        assert!(!answer.contains("did not yield"), "{answer}");
        wait_for_worker(&r).await;
        assert!(
            crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, HEALTHY, &destination),
            "the follow-on pass replayed the requested branch"
        );
        assert_eq!(intent_status(&r, &destination).await, "completed");
        assert!(
            !crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, &destination),
            "serving the request did not move the held branch"
        );
    }
}
