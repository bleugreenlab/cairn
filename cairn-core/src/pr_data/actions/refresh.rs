//! Read-side PR presentation: GitHub / local cache refresh, the live `/pr`
//! markdown section, and PR close.

use crate::execution::teardown::{cleanup_issue_jobs, TeardownReason, TeardownScope};
use crate::github::api;
use crate::github::api::PrFile;
use crate::github::credentials::get_owner_repo;
use crate::models::{Check, CheckState, MergeableState, PrCache, PrState};
use crate::orchestrator::Orchestrator;
use crate::pr_data::helpers::{
    compute_checks_status, compute_local_mergeable, fetch_checks_via_api, fetch_pr_via_api,
    local_pr_files, ParsedPrDetails,
};
use crate::pr_data::publication::{
    probe_unbound_publication, publication_summary, DiscoveredPr, HeadDivergence, Publication,
    UNVERIFIED_VERDICT_NOTE,
};
use crate::security::broker::github::installation_authority;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::conflict::{
    conflict_recovery_hint, format_conflicted_commits, source_conflict_report, SourceConflictReport,
};
use super::context::{
    db_error, load_mr_branches, load_mr_issue_id, resolve_mr_context_for_job,
    try_resolve_mr_context_for_job, MrContext, PrNodeResolution,
};
use super::resolution::resolve_pr_node;

/// Mergeability override for a jj PR read path: `Conflicting` only when the source
/// bookmark's TIP carries a recorded conflict (a hard block), else `None` (keep
/// the GitHub value). A clean-tip / conflicted-intermediate branch is
/// auto-recoverable via the merge-time flatten, so it is NOT surfaced as a hard
/// `Conflicting` that disables the merge button.
fn source_tip_is_conflicted(
    orch: &Orchestrator,
    repo_path: &str,
    source_branch: &str,
    target_branch: &str,
) -> bool {
    source_conflict_report(
        &orch.jj_binary_path,
        &orch.config_dir,
        repo_path,
        source_branch,
        Some(target_branch),
    )
    .is_some_and(|report| report.tip_conflicted)
}

/// The mergeability an artifact may publish for a live pull request.
///
/// The precedence here is the whole point, because the three inputs do not all
/// have the same subject. GitHub's bit is about the head the pull request holds.
/// The conflict probe is about the commit the branch holds now.
///
/// When those are the same commit, both describe the change under review, and
/// the local probe wins: GitHub reports a jj-conflicted commit as mergeable, and
/// it is not.
///
/// When they are NOT the same commit, the two answers have different subjects
/// and neither is a verdict on this pull request. The answer is then UNKNOWN,
/// and the artifact states both facts in prose rather than compressing two
/// subjects into one word — attributing a property of the branch's current tree
/// to a pull request that does not contain it is the same error as the green
/// signal this whole path exists to stop.
///
/// UNKNOWN is not a weaker block than CONFLICTING. The merge boundary
/// (`merge_pr_for_job`) refuses a conflicted source from the store itself, and
/// the desktop enables its merge button only on MERGEABLE.
fn published_mergeable(
    github: MergeableState,
    diverged: bool,
    tip_conflicted: bool,
) -> MergeableState {
    match (diverged, tip_conflicted) {
        (true, _) => MergeableState::Unknown,
        (false, true) => MergeableState::Conflicting,
        (false, false) => github,
    }
}

/// The persisted PR projection a refresh can move.
///
/// Comparing this before writing is what makes an unchanged refresh silent. The
/// `merge_requests` row also carries `github_fetched_at`/`updated_at`, and
/// writing only those is a timestamp-only refresh: it changes no fact anyone
/// renders, but it still emits a `db-change` that invalidates every mounted PR
/// query, which re-enters this path. That loop is the self-excitation, so a
/// no-op refresh must not write and must not emit.
#[derive(Debug, PartialEq, Eq)]
struct PersistedProjection {
    title: Option<String>,
    body: Option<String>,
    additions: Option<i32>,
    deletions: Option<i32>,
    checks_status: Option<String>,
    checks_json: Option<String>,
    github_state: Option<String>,
    github_review: Option<String>,
    github_mergeable: Option<String>,
    /// A row that has never been fetched must be written even when every other
    /// field matches, because `fetched_at == 0` is itself what the desktop reads
    /// as "this cache has never been populated" and re-fetches on.
    fetched: bool,
}

/// The persisted projection plus the timestamp of the fetch that produced it.
/// The timestamp is deliberately outside the compared struct: it moves on every
/// refresh and comparing it would make every refresh look like a change.
async fn load_persisted_projection(
    db: &LocalDb,
    mr_id: &str,
) -> Option<(PersistedProjection, i64)> {
    let mr_id = mr_id.to_string();
    db.read(|conn| {
        let mr_id = mr_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT title, body, additions, deletions, checks_status, checks_json,
                            github_state, github_review, github_mergeable, github_fetched_at
                       FROM merge_requests WHERE id = ?1 LIMIT 1",
                    params![mr_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let fetched_at = row.opt_i64(9)?.unwrap_or_default();
            Ok(Some((
                PersistedProjection {
                    title: row.opt_text(0)?,
                    body: row.opt_text(1)?,
                    additions: row.opt_i64(2)?.map(|v| v as i32),
                    deletions: row.opt_i64(3)?.map(|v| v as i32),
                    checks_status: row.opt_text(4)?,
                    checks_json: row.opt_text(5)?,
                    github_state: row.opt_text(6)?,
                    github_review: row.opt_text(7)?,
                    github_mergeable: row.opt_text(8)?,
                    fetched: fetched_at > 0,
                },
                fetched_at,
            )))
        })
    })
    .await
    .ok()
    .flatten()
}

/// What one persist attempt did: whether the row moved, and the fetch timestamp
/// the projection should now carry (the new one on a write, the stored one when
/// nothing moved).
struct CacheWrite {
    changed: bool,
    fetched_at: i64,
}

/// Persist a freshly fetched projection, returning whether anything moved.
///
/// `false` means the row already said exactly this, so no write happened and the
/// caller must not emit.
async fn update_merge_request_github_cache(
    db: &LocalDb,
    mr_id: &str,
    pr_details: &ParsedPrDetails,
    checks: &[Check],
    checks_status: &Option<crate::models::ChecksStatus>,
    now: i64,
) -> Result<CacheWrite, String> {
    let incoming = PersistedProjection {
        title: Some(
            pr_details
                .title
                .clone()
                .unwrap_or_else(|| "Untitled".to_string()),
        ),
        body: pr_details.body.clone(),
        additions: pr_details.additions,
        deletions: pr_details.deletions,
        checks_status: checks_status.as_ref().map(|status| status.to_string()),
        checks_json: Some(serde_json::to_string(checks).unwrap_or_default()),
        github_state: Some(pr_details.state.to_string()),
        github_review: pr_details
            .review_decision
            .as_ref()
            .map(|decision| decision.to_string()),
        github_mergeable: Some(pr_details.mergeable.to_string()),
        fetched: true,
    };
    if let Some((persisted, fetched_at)) = load_persisted_projection(db, mr_id).await {
        if persisted == incoming {
            return Ok(CacheWrite {
                changed: false,
                fetched_at,
            });
        }
    }

    let mr_id = mr_id.to_string();
    let title = pr_details.title.clone();
    let body = pr_details.body.clone();
    let additions = pr_details.additions;
    let deletions = pr_details.deletions;
    let checks_json = serde_json::to_string(checks).unwrap_or_default();
    let state = pr_details.state.to_string();
    let review_decision = pr_details
        .review_decision
        .as_ref()
        .map(|decision| decision.to_string());
    let mergeable = pr_details.mergeable.to_string();
    let checks_status = checks_status.as_ref().map(|status| status.to_string());

    db.write(|conn| {
        let mr_id = mr_id.clone();
        let title = title.clone();
        let body = body.clone();
        let checks_json = checks_json.clone();
        let state = state.clone();
        let review_decision = review_decision.clone();
        let mergeable = mergeable.clone();
        let checks_status = checks_status.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests
                 SET title = ?1, body = ?2, additions = ?3, deletions = ?4,
                     checks_status = ?5, checks_json = ?6, github_state = ?7,
                     github_review = ?8, github_mergeable = ?9,
                     github_fetched_at = ?10, updated_at = ?10
                 WHERE id = ?11",
                params![
                    title.as_deref().unwrap_or("Untitled"),
                    body.as_deref(),
                    additions,
                    deletions,
                    checks_status.as_deref(),
                    checks_json.as_str(),
                    state.as_str(),
                    review_decision.as_deref(),
                    mergeable.as_str(),
                    now,
                    mr_id.as_str()
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to update merge request", e))?;
    Ok(CacheWrite {
        changed: true,
        fetched_at: now,
    })
}

/// The `db-change` a `merge_requests` mutation emits, carrying the scope every
/// consumer needs to decide whether it is affected.
///
/// An unscoped emit forces the frontend to invalidate the whole PR family, and
/// the whole project's status rollups with it, for a change to one row. Scope is
/// available at every mutation boundary here, so there is no reason to make
/// every reader recompute.
fn emit_merge_request_change(orch: &Orchestrator, mr_context: &MrContext, action: &str) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({
            "table": "merge_requests",
            "action": action,
            "mergeRequestId": mr_context.mr_id,
            "jobId": mr_context.job_id,
            "projectId": mr_context.project_id,
            "issueId": mr_context.issue_id,
        }),
    );
}

/// The commit the branch store holds for `branch`, or `None` when the store
/// cannot answer.
///
/// The store is the branch authority, so this is the commit every claim about
/// "the current version of this change" is measured against. `None` is a real
/// answer meaning the version is unknown — never a licence to measure something
/// else instead.
fn store_branch_commit(orch: &Orchestrator, repo_path: &str, branch: &str) -> Option<String> {
    if branch.is_empty() {
        return None;
    }
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    crate::jj::bookmark_commit(&jj, &store, branch)
}

/// Compare the commit the pull request describes against the commit the branch
/// actually holds.
///
/// A pull request whose head lags its branch reports mergeability, checks and a
/// diffstat for the head it holds. Every one of those signals is then a verdict
/// on a version nobody has reviewed — the shape in which a pull request read
/// green while the entire merge-blocking fix sat in commits it did not contain.
fn head_divergence(
    orch: &Orchestrator,
    repo_path: &str,
    source_branch: &str,
    pr_head_sha: &str,
) -> Option<HeadDivergence> {
    if pr_head_sha.is_empty() {
        return None;
    }
    let branch_head = store_branch_commit(orch, repo_path, source_branch)?;
    if branch_head == pr_head_sha {
        return None;
    }
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(repo_path));
    let pr_is_behind = crate::jj::revset_descends_from(&jj, &store, &branch_head, pr_head_sha);
    Some(HeadDivergence {
        pr_head: pr_head_sha.to_string(),
        branch_head,
        pr_is_behind,
    })
}

/// Record a pull request discovered by head branch onto the merge request that
/// had lost its binding.
///
/// The repair half of "a refresh can bind a PR that was opened outside Cairn":
/// once the number is back on the row, every downstream path that trusts the
/// record — the artifact, the merge action, the issue's resolution guard —
/// follows the real pull request again instead of a stranded row.
async fn bind_discovered_pr(
    orch: &Orchestrator,
    db: &LocalDb,
    mr_context: &MrContext,
    discovered: &DiscoveredPr,
) -> Result<(), String> {
    let mr_id = mr_context.mr_id.to_string();
    let url = discovered.url.clone();
    let state = discovered.state.clone();
    let number = i64::from(discovered.number);
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let mr_id = mr_id.clone();
        let url = url.clone();
        let state = state.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests
                 SET github_pr_number = ?1, github_pr_url = ?2, github_state = ?3, updated_at = ?4
                 WHERE id = ?5",
                params![number, url.as_str(), state.as_str(), now, mr_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to bind the discovered pull request", e))?;
    emit_merge_request_change(orch, mr_context, "update");
    Ok(())
}

/// Bind the pull request an unbound refresh discovered, returning its number and
/// url so the caller can continue on the live-PR path.
async fn adopt_discovered_binding(
    orch: &Orchestrator,
    db: &LocalDb,
    mr_context: &MrContext,
    publication: Option<&Publication>,
) -> Option<(i32, String)> {
    let Some(Publication::Bound(discovered)) = publication else {
        return None;
    };
    let mr_id = &mr_context.mr_id;
    match bind_discovered_pr(orch, db, mr_context, discovered).await {
        Ok(()) => {
            log::info!(
                "Re-bound merge request {mr_id} to pull request #{} ({}) found by head branch",
                discovered.number,
                discovered.url
            );
            // The row's binding is what every downstream projection is keyed on,
            // so the snapshot computed under the old (unbound) generation must
            // not be published for the now-bound change.
            orch.invalidate_pr_refresh(&mr_context.job_id, "pull-request-bound");
            Some((discovered.number, discovered.url.clone()))
        }
        Err(error) => {
            log::error!("Failed to re-bind merge request {mr_id}: {error}");
            None
        }
    }
}

/// The publication facts an unbound change renders, carried on the snapshot so
/// the renderer states them instead of re-probing for them.
pub(crate) struct UnboundFacts {
    /// What one probe of the world established, or `None` for a change that is
    /// already merged or closed and has nothing left to publish.
    pub(crate) publication: Option<Publication>,
    pub(crate) source_branch: String,
    /// Why the most recent attempt to open a pull request failed, when it did.
    pub(crate) failure: Option<String>,
}

/// The complete PR projection every surface renders, materialized once per
/// generation.
///
/// One structure for all of it is the point. Before this, the desktop's refresh
/// and the `/pr` artifact each fetched GitHub state and ran their own `jj`
/// probes to answer the same questions, so two readers of one pull request paid
/// for it twice and could disagree about the answer.
pub(crate) struct PrSnapshot {
    pub(crate) cache: PrCache,
    /// Present only for a change with no pull request bound to it.
    pub(crate) unbound: Option<UnboundFacts>,
    pub(crate) branches: Option<(String, String)>,
    pub(crate) divergence: Option<HeadDivergence>,
    pub(crate) conflict: Option<SourceConflictReport>,
    pub(crate) files: Vec<PrFile>,
    /// When this snapshot stops describing the live world on its own, or `None`
    /// for a settled change that only an exact invalidation can move.
    expires_at: Option<i64>,
}

/// A change whose remote signals are still resolving — GitHub computes
/// mergeability asynchronously, and a check run in flight has no verdict yet —
/// is genuinely live, so it re-reads on a short window.
const UNRESOLVED_FRESHNESS_SECS: i64 = 15;
/// An open change whose signals HAVE resolved rides a long window. Every real
/// transition (a push, a review, a check completing, a merge) advances the
/// generation exactly, so this window is a backstop for a workspace with no
/// webhook delivery, not the mechanism liveness depends on.
const RESOLVED_FRESHNESS_SECS: i64 = 300;

impl PrSnapshot {
    /// A merged or closed change is settled: no further remote fact about it can
    /// change without a transition that invalidates this snapshot outright.
    fn expiry(cache: &PrCache, now: i64) -> Option<i64> {
        match cache.state {
            PrState::Merged | PrState::Closed => None,
            PrState::Open | PrState::Unpublished => {
                let unresolved = matches!(cache.mergeable, MergeableState::Unknown)
                    || matches!(
                        cache.checks_status,
                        Some(crate::models::ChecksStatus::Pending)
                    );
                Some(
                    now + if unresolved {
                        UNRESOLVED_FRESHNESS_SECS
                    } else {
                        RESOLVED_FRESHNESS_SECS
                    },
                )
            }
        }
    }

    fn is_expired(&self, now: i64) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }
}

/// The one place a PR projection is produced.
///
/// Every surface — the desktop panel, the `/pr` artifact, the invoke command —
/// reads through here, so concurrent readers of one pull request share exactly
/// one GitHub fetch and one bounded set of `jj` probes, and a read of unchanged
/// state performs neither.
///
/// `live` is explicit operator refresh: it advances the generation once and then
/// joins the single flight that advance created, so a burst of refresh presses
/// still costs one refresh.
pub(crate) async fn pr_snapshot_for_job(
    orch: &Orchestrator,
    job_id: &str,
    live: bool,
) -> Result<Arc<PrSnapshot>, String> {
    let cache = &orch.pr_refresh_cache;
    if live {
        // Replace what is published, or join the refresh already in flight — a
        // burst of refresh presses is one refresh. Invalidating unconditionally
        // would advance once per caller, and every advance rejects the readers
        // already computing, running the GitHub fetch and `jj` probes N times.
        cache.invalidate_published(job_id, "explicit-refresh");
    } else {
        // Every mounted reader observes the same expired snapshot at the moment
        // a freshness window elapses, so the decision to advance has to be made
        // under the same lock as the advance itself.
        let now = chrono::Utc::now().timestamp();
        cache.invalidate_stale(job_id, "freshness-window-elapsed", |snapshot| {
            snapshot.is_expired(now)
        });
    }

    // A computation that fails is not a fact about the pull request, so it is
    // never published. Its error still has to reach the caller that ran it; a
    // reader that merely joined someone else's failed flight gets the generic
    // message, because the specific one belongs to the reader that produced it.
    let failure: Mutex<Option<String>> = Mutex::new(None);
    let snapshot = cache
        .get_or_compute(job_id, || async {
            match compute_pr_snapshot(orch, job_id).await {
                Ok(snapshot) => Some(Arc::new(snapshot)),
                Err(error) => {
                    if let Ok(mut slot) = failure.lock() {
                        *slot = Some(error);
                    }
                    None
                }
            }
        })
        .await;

    match snapshot {
        Some(snapshot) => Ok(snapshot),
        None => Err(failure
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
            .unwrap_or_else(|| "Failed to refresh the pull request".to_string())),
    }
}

/// The canonical, uncached PR computation: fetch remote PR and check state,
/// probe local head divergence and source conflicts, persist the projection.
///
/// Nothing calls this directly — it runs inside the coordinator's single flight,
/// so "one refresh" is enforced here rather than trusted at each call site.
async fn compute_pr_snapshot(orch: &Orchestrator, job_id: &str) -> Result<PrSnapshot, String> {
    // Route to the database that owns this job — the team replica for a team
    // execution, the private DB for a local one. The `merge_requests` row and its
    // producing job live wholly in that database; reading `orch.db.local` for a
    // team job would miss the row. GitHub credentials stay on the private DB
    // below: `github_credentials` is a private, un-synced table.
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;
    let mr_context = resolve_mr_context_for_job(&db, job_id).await?;
    let mr_id = mr_context.mr_id.clone();
    let mut pr_url = mr_context.pr_url.clone();
    let repo_path = mr_context.repo_path.clone();

    let pr_number = match mr_context.github_pr_number {
        Some(number) => number,
        None => {
            // Nothing is bound. The single probe that establishes the honest
            // unpublished state also finds a pull request opened outside Cairn,
            // or one whose binding a failed open never recorded — which is what
            // turns a stranded artifact back into a usable one.
            let unbound = refresh_unbound_pr_for_job(orch, &db, job_id, &mr_context).await?;
            match adopt_discovered_binding(orch, &db, &mr_context, unbound.publication.as_ref())
                .await
            {
                Some((number, url)) => {
                    pr_url = url;
                    number
                }
                None => {
                    let now = chrono::Utc::now().timestamp();
                    let branches = unbound
                        .cache
                        .source_branch
                        .clone()
                        .zip(unbound.cache.target_branch.clone());
                    return Ok(PrSnapshot {
                        expires_at: PrSnapshot::expiry(&unbound.cache, now),
                        branches,
                        cache: unbound.cache,
                        unbound: Some(UnboundFacts {
                            publication: unbound.publication,
                            source_branch: unbound.source_branch,
                            failure: unbound.failure,
                        }),
                        divergence: None,
                        conflict: None,
                        files: Vec::new(),
                    });
                }
            }
        }
    };

    let (owner, repo) = get_owner_repo(&repo_path)?;
    let auth = installation_authority(&orch.db.local, &owner).await?;

    let http = &*orch.services.http;
    let mut pr_details = fetch_pr_via_api(http, &auth, &owner, &repo, pr_number).await?;
    let branches = load_mr_branches(&db, &mr_id).await.ok().flatten();
    // A green signal on a head the branch has moved past is a verdict on a tree
    // nobody validated; both surfaces render which commit each side holds. This
    // and the conflict probe feed one decision, so the two cannot overwrite each
    // other — see `published_mergeable`.
    let divergence = branches
        .as_ref()
        .filter(|_| matches!(pr_details.state, PrState::Open))
        .and_then(|(source_branch, _)| {
            head_divergence(orch, &repo_path, source_branch, &pr_details.head_sha)
        });
    if let (Some(divergence), Some((source_branch, _))) = (divergence.as_ref(), branches.as_ref()) {
        log::warn!(
            "Pull request #{pr_number} describes {} while `{source_branch}` holds {}; its \
             mergeability and checks are not verdicts on the current change",
            divergence.pr_head,
            divergence.branch_head
        );
    }
    let conflict = branches
        .as_ref()
        .and_then(|(source_branch, target_branch)| {
            source_conflict_report(
                &orch.jj_binary_path,
                &orch.config_dir,
                &repo_path,
                source_branch,
                Some(target_branch),
            )
        });
    if branches.is_some() {
        pr_details.mergeable = published_mergeable(
            pr_details.mergeable.clone(),
            divergence.is_some(),
            conflict
                .as_ref()
                .is_some_and(|report| report.tip_conflicted),
        );
    }
    let checks = fetch_checks_via_api(http, &auth, &owner, &repo, &pr_details.head_sha)
        .await
        .unwrap_or_default();
    let checks_status = compute_checks_status(&checks);
    // Files ride on the same snapshot as everything else the artifact renders, so
    // the `/pr` read no longer needs a fetch path of its own.
    let files = api::fetch_pr_files(http, &auth, &owner, &repo, pr_number)
        .await
        .unwrap_or_default();

    let now = chrono::Utc::now().timestamp();
    let write =
        update_merge_request_github_cache(&db, &mr_id, &pr_details, &checks, &checks_status, now)
            .await?;
    if write.changed {
        emit_merge_request_change(orch, &mr_context, "update");
    }

    let cache = PrCache {
        id: mr_id.clone(),
        job_id: None,
        pr_number: Some(pr_number),
        pr_url: pr_url.clone(),
        title: pr_details.title.clone(),
        body: pr_details.body.clone(),
        state: pr_details.state.clone(),
        is_draft: pr_details.is_draft,
        review_decision: pr_details.review_decision.clone(),
        mergeable: pr_details.mergeable.clone(),
        additions: pr_details.additions,
        deletions: pr_details.deletions,
        checks_status: checks_status.clone(),
        checks: checks.clone(),
        fetched_at: write.fetched_at,
        updated_at: write.fetched_at,
        is_local: mr_context.is_local,
        source_branch: branches.as_ref().map(|(source, _)| source.clone()),
        target_branch: branches.as_ref().map(|(_, target)| target.clone()),
    };

    Ok(PrSnapshot {
        expires_at: PrSnapshot::expiry(&cache, now),
        cache,
        unbound: None,
        branches,
        divergence,
        conflict,
        files,
    })
}

/// The PR projection for a job, served from the generation-fenced snapshot.
pub async fn refresh_pr_for_job(orch: &Orchestrator, job_id: &str) -> Result<PrCache, String> {
    Ok(pr_snapshot_for_job(orch, job_id, false)
        .await?
        .cache
        .clone())
}

/// Explicit operator refresh: advance the generation once, then join the single
/// flight that advance created.
pub async fn refresh_pr_for_job_live(orch: &Orchestrator, job_id: &str) -> Result<PrCache, String> {
    Ok(pr_snapshot_for_job(orch, job_id, true).await?.cache.clone())
}

/// The publication facts behind an artifact with no pull request bound to it,
/// carried beside the cache row so the renderer states them instead of
/// re-deriving them.
struct UnboundPr {
    cache: PrCache,
    /// What one probe of the world established, or `None` for a change that is
    /// already merged or closed and has nothing left to publish.
    publication: Option<Publication>,
    source_branch: String,
    /// Why the most recent attempt to open a pull request failed, when it did.
    failure: Option<String>,
}

/// A change's size and mergeability, or the honest absence of both.
struct Measurement {
    additions: Option<i32>,
    deletions: Option<i32>,
    mergeable: MergeableState,
}

impl Measurement {
    /// Nothing measured. `None` change counts render as "unknown"; a `0` would
    /// render as "this change is empty", which is a claim.
    fn unknown() -> Self {
        Self {
            additions: None,
            deletions: None,
            mergeable: MergeableState::Unknown,
        }
    }
}

/// Measure a change between two resolved commits.
///
/// Commits, not branch names: the branch store is the authority for what the
/// branch holds, and naming the commit makes the measurement reproducible and
/// makes the two paths (store bookmark, git ref) cross-check rather than share
/// one point of failure.
fn measure_change(
    git: &dyn crate::services::GitClient,
    repo_path: &str,
    target_commit: &str,
    source_commit: &str,
) -> Measurement {
    let repo = Path::new(repo_path);
    match local_pr_files(git, repo, target_commit, source_commit) {
        Ok(files) => Measurement {
            additions: Some(files.iter().map(|file| file.additions).sum()),
            deletions: Some(files.iter().map(|file| file.deletions).sum()),
            mergeable: compute_local_mergeable(git, repo, target_commit, source_commit),
        },
        Err(error) => {
            log::warn!(
                "Could not measure the change from {target_commit} to {source_commit}: {error}"
            );
            Measurement::unknown()
        }
    }
}

/// Whether a change size and a mergeability verdict computed here would describe
/// the change the world can see.
///
/// This is the whole of the honesty rule in one predicate. Measuring is fine;
/// measuring *something other than what is under review* and presenting the
/// result as the pull request's is not.
fn verdict_is_licensed(
    publication: &Publication,
    origin_tip: Option<&str>,
    source_commit: Option<&str>,
) -> bool {
    match publication {
        // No remote at all: this machine's branch is the whole world's view of
        // the change, so measuring it measures exactly what is under review.
        Publication::LocalOnly => true,
        // On the remote but with no pull request: measurable only while the
        // published branch IS the branch being measured.
        Publication::NoPullRequest => match (origin_tip, source_commit) {
            (Some(origin), Some(store)) => origin == store,
            _ => false,
        },
        // Not on the remote, unreachable remote, or a live pull request whose
        // own head is the thing to compare against.
        Publication::BranchAbsent | Publication::Unknown { .. } | Publication::Bound(_) => false,
    }
}

/// The error text of the most recent failed attempt to open a pull request for
/// this change.
///
/// The attempt is an action run and its failure is already recorded there; what
/// was missing is that the artifact — the surface a person actually reads — never
/// showed it, so a failed open was indistinguishable from a slow one.
async fn last_publication_failure(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    db.query_opt_text(
        "SELECT error_message FROM action_runs
         WHERE parent_job_id = ?1
           AND status = 'failed'
           AND action_config_id IN ('builtin:pr', 'builtin:create_pr')
         ORDER BY completed_at DESC, created_at DESC
         LIMIT 1",
        params![job_id.as_str()],
    )
    .await
    .ok()
    .flatten()
    .filter(|text| !text.trim().is_empty())
}

/// Refresh a merge request that carries no pull-request number.
///
/// This is the path that rendered "PR #0 — OPEN, MERGEABLE, +2,126,840 −30" for
/// a pull request that had never been opened. Every claim came from the row:
/// `status` said open so the artifact said OPEN; a local `git merge-tree`
/// against whatever the checkout happened to hold said MERGEABLE; a local
/// `git diff --numstat` against that same ref supplied the counts, and its
/// failures were swallowed into zeros.
///
/// Now the row supplies only prose (title, body) and lifecycle (merged/closed).
/// Whether a pull request exists at all, and whether the change being measured
/// is the change the world can see, are established by probe — and a question
/// that cannot be answered renders as unknown rather than as a number.
async fn refresh_unbound_pr_for_job(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    mr_context: &MrContext,
) -> Result<UnboundPr, String> {
    let mr_id = mr_context.mr_id.clone();
    let repo_path = mr_context.repo_path.clone();
    let (title, body, status, source_branch, target_branch, additions, deletions, updated_at) = db
        .read(|conn| {
            let mr_id = mr_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT title, body, status, source_branch, target_branch, additions, deletions, updated_at
                         FROM merge_requests WHERE id = ?1 LIMIT 1",
                        params![mr_id.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| DbError::internal("merge request not found"))?;
                Ok((
                    row.opt_text(0)?,
                    row.opt_text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                    row.opt_i64(5)?.map(|v| v as i32),
                    row.opt_i64(6)?.map(|v| v as i32),
                    row.i64(7)?,
                ))
            })
        })
        .await
        .map_err(|e| db_error("Failed to load local PR", e))?;

    let git = &*orch.services.git;
    let open = status == "open";

    // A merged or closed change has nothing left to publish, so it is not probed:
    // the network call would serve a question nobody is asking.
    let probe = open.then(|| {
        probe_unbound_publication(
            git,
            Path::new(&repo_path),
            &source_branch,
            !mr_context.is_local,
        )
    });

    // Both sides of the measurement come from the branch store, the branch
    // authority. An open change whose branch the store cannot resolve is one
    // whose current version is unknown — which is an answer, not a licence to
    // measure whatever the checkout happens to hold.
    let source_commit = store_branch_commit(orch, &repo_path, &source_branch);
    let target_commit = store_branch_commit(orch, &repo_path, &target_branch);
    let licensed = probe.as_ref().is_some_and(|probe| {
        verdict_is_licensed(
            &probe.publication,
            probe.origin_tip.as_deref(),
            source_commit.as_deref(),
        )
    });
    let measured = match (licensed, source_commit.as_deref(), target_commit.as_deref()) {
        (true, Some(source), Some(target)) => measure_change(git, &repo_path, target, source),
        _ => Measurement::unknown(),
    };

    // An open change's counts are recomputed on every refresh so a rebased or
    // flattened branch self-corrects; freezing the first value would pin a stale,
    // possibly pre-conflict-resolution diff forever. A merged or closed change is
    // not re-measured, so its stored counts stand.
    let (additions, deletions) = if open {
        (measured.additions, measured.deletions)
    } else {
        (additions, deletions)
    };
    let mergeable = measured.mergeable;
    let now = chrono::Utc::now().timestamp();
    let mergeable_str = mergeable.to_string();
    // Same no-op rule as the bound path: an unbound refresh that re-derives the
    // values already on the row must not write and must not emit, or a mounted
    // `/pr` surface re-enters this probe (and its `jj` fan-out) on its own event.
    let unchanged = load_persisted_projection(db, &mr_id)
        .await
        .is_some_and(|(persisted, _)| {
            persisted.fetched
                && persisted.github_mergeable.as_deref() == Some(mergeable_str.as_str())
                && persisted.additions == additions
                && persisted.deletions == deletions
        });
    if !unchanged {
        db.write(|conn| {
            let mr_id = mr_id.clone();
            let mergeable_str = mergeable_str.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE merge_requests
                     SET github_mergeable = ?1, github_fetched_at = ?2, updated_at = ?2,
                         additions = ?3, deletions = ?4
                     WHERE id = ?5",
                    params![
                        mergeable_str.as_str(),
                        now,
                        additions,
                        deletions,
                        mr_id.as_str()
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| db_error("Failed to update local PR cache", e))?;

        emit_merge_request_change(orch, mr_context, "update");
    }

    let publication = probe.map(|probe| probe.publication);
    let state = match status.as_str() {
        "merged" => PrState::Merged,
        "closed" => PrState::Closed,
        // An open row is OPEN only when there is something open to point at. A
        // local-only change is open by construction; a change that has a remote
        // and no bound pull request has nothing that could be open, and saying
        // OPEN would assert a pull request exists.
        _ => match publication {
            Some(Publication::LocalOnly) => PrState::Open,
            _ => PrState::Unpublished,
        },
    };
    let failure = if matches!(state, PrState::Unpublished) {
        last_publication_failure(db, &mr_context.job_id).await
    } else {
        None
    };

    Ok(UnboundPr {
        cache: PrCache {
            id: mr_id,
            job_id: Some(job_id.to_string()),
            pr_number: None,
            pr_url: String::new(),
            title,
            body,
            state,
            is_draft: false,
            review_decision: None,
            mergeable,
            additions,
            deletions,
            checks_status: None,
            checks: Vec::new(),
            fetched_at: now,
            updated_at,
            is_local: mr_context.is_local,
            source_branch: Some(source_branch.clone()),
            target_branch: Some(target_branch),
        },
        publication,
        source_branch,
        failure,
    })
}

/// Close a PR without merging, mark the `merge_requests` row closed, and tear
/// down the issue's worktrees.
pub async fn close_pr_for_job(
    orch: &Orchestrator,
    job_id: &str,
    attribution: Option<super::PrResolutionAttribution>,
) -> Result<String, String> {
    // Route to the owning database (team replica or private DB); the PR's
    // `merge_requests` row lives there. GitHub credentials stay on the private DB.
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;
    let mr_context = resolve_mr_context_for_job(&db, job_id).await?;
    let mr_id = mr_context.mr_id.clone();
    let repo_path = mr_context.repo_path.clone();

    if let Some(pr_number) = mr_context.github_pr_number {
        let (owner, repo) = get_owner_repo(&repo_path)?;
        let auth = installation_authority(&orch.db.local, &owner).await?;

        let http = &*orch.services.http;
        api::close_pr(http, &auth, &owner, &repo, pr_number).await?;
    }

    resolve_pr_node(orch, job_id, PrNodeResolution::Close, attribution).await?;

    // Release runtime resources and apply the established branch cleanup policy.
    // This runs after the cascade above has stopped the issue's agents: killing
    // a live agent's terminals only invites it to open new ones.
    if let Some(issue_id) = load_mr_issue_id(&db, &mr_id).await? {
        let orch_inner = orch.clone();
        tokio::spawn(async move {
            if let Err(e) = cleanup_issue_jobs(
                &orch_inner,
                TeardownScope::Issue(issue_id),
                TeardownReason::Discarded,
            )
            .await
            {
                log::warn!("Worktree teardown after PR close failed: {}", e);
            }
        });
    }

    // Refresh PR details to get closed state.
    let _ = refresh_pr_for_job(orch, job_id).await;

    Ok("PR closed successfully".to_string())
}

/// The artifact section for a change with no pull request bound to it.
///
/// It states what is true, why it is true, and — when an attempt was made and
/// failed — what the failure said. It renders no pull-request number, and no
/// mergeability verdict or change counts unless the change measured is the
/// change the world can see.
fn render_unbound_section(cache: &PrCache, unbound: &UnboundFacts, artifact_uri: &str) -> String {
    let local_only = matches!(unbound.publication, Some(Publication::LocalOnly));
    let mut out = if local_only {
        String::from("## Local PR\n\n")
    } else {
        String::from("## Pull Request\n\n")
    };
    out.push_str(cache.title.as_deref().unwrap_or("Untitled"));
    out.push('\n');
    out.push_str(&format!("State: {}\n", cache.state));
    if let Some(publication) = unbound.publication.as_ref() {
        out.push_str(&publication_summary(publication, &unbound.source_branch));
        out.push('\n');
    }
    match (cache.additions, cache.deletions) {
        (Some(additions), Some(deletions)) => {
            out.push_str(&format!("Mergeable: {}\n", cache.mergeable));
            out.push_str(&format!("Changes: +{} -{}\n", additions, deletions));
        }
        _ if matches!(cache.state, PrState::Open | PrState::Unpublished) => {
            out.push_str(UNVERIFIED_VERDICT_NOTE);
            out.push('\n');
        }
        _ => {}
    }
    if let Some(failure) = unbound.failure.as_deref() {
        out.push_str(&format!(
            "\n### The last attempt to open this pull request failed\n\n```\n{}\n```\n",
            failure.trim()
        ));
    }
    if let Some(body) = cache.body.as_deref().filter(|b| !b.is_empty()) {
        out.push_str("\n### Description\n\n");
        out.push_str(body);
        out.push('\n');
    }
    match cache.state {
        // A local-only change really is an open PR: it merges through the branch
        // store, so the full action set applies.
        PrState::Open => out.push_str(&format!(
            "\n## actions\n- [merge]({uri}): patch with action:\"merge\" (optional method, default squash).\n- [close]({uri}): patch with action:\"close\".\n- [refresh]({uri}): patch with action:\"refresh\".",
            uri = artifact_uri
        )),
        // Nothing is open, so there is nothing to merge. `refresh` is the repair:
        // it re-probes and binds the pull request if one now exists.
        PrState::Unpublished => out.push_str(&format!(
            "\n## actions\n- [refresh]({uri}): patch with action:\"refresh\" to re-check GitHub and bind the pull request if one now exists.\n- [close]({uri}): patch with action:\"close\" to abandon this change.",
            uri = artifact_uri
        )),
        PrState::Merged | PrState::Closed => {}
    }
    out
}

fn check_icon(state: &CheckState) -> &'static str {
    match state {
        CheckState::Success => "✓",
        CheckState::Failure => "✗",
        CheckState::Pending => "◐",
        CheckState::Skipped => "⊘",
        CheckState::Cancelled => "⊗",
    }
}

/// Render the live-PR markdown section for a node `/pr` artifact whose job owns
/// a `merge_requests` row. Returns `None` when the job has no PR, so non-PR
/// artifacts (e.g. `plan`) are unaffected.
///
/// The section renders the generation-fenced snapshot: one reader materializes
/// the GitHub state and the `jj` probes behind it, and every other reader of the
/// same unchanged pull request — including this one — gets it for free. When the
/// PR is open, an `## actions` block advertising merge/close/refresh is appended.
/// `artifact_uri` is the `/pr` URI used in the action examples; `diff_full`
/// inlines the full patch text per file.
pub async fn render_live_pr_section(
    orch: &Orchestrator,
    job_id: &str,
    artifact_uri: &str,
    diff_full: bool,
) -> Option<String> {
    // Route to the owning database (team replica or private DB). A team node's
    // `merge_requests` row lives in its replica; a closed replica yields no PR
    // section rather than a wrong read against the private DB. This resolve is a
    // single indexed row read and answers "is there a PR here at all", which has
    // to be decided before entering the coordinator.
    let db = match crate::execution::routing::routing_db_for_id(&orch.db, job_id).await {
        Ok(db) => db,
        Err(_) => return None,
    };
    let mr_context = match try_resolve_mr_context_for_job(&db, job_id).await {
        Ok(Some(ctx)) => ctx,
        Ok(None) => return None,
        Err(e) => return Some(format!("## Pull Request\n\n(failed to resolve PR: {e})\n")),
    };

    let snapshot = match pr_snapshot_for_job(orch, job_id, false).await {
        Ok(snapshot) => snapshot,
        Err(e) => return Some(format!("## Pull Request\n\n(failed to refresh: {e})\n")),
    };
    let cache = &snapshot.cache;
    if let Some(unbound) = snapshot.unbound.as_ref() {
        return Some(render_unbound_section(cache, unbound, artifact_uri));
    }
    let pr_number = cache.pr_number.unwrap_or_default();
    let header = format!("## Pull Request\n\nPR #{}: {}\n", pr_number, cache.pr_url);

    let source_branches = snapshot.branches.clone();
    let divergence = snapshot.divergence.as_ref();
    let source_conflict = snapshot.conflict.as_ref();
    let checks = &cache.checks;
    let checks_status = &cache.checks_status;
    let files = &snapshot.files;

    let mut out = header;
    out.push_str(&format!(
        "State: {}{}\n",
        cache.state,
        if cache.is_draft { " (draft)" } else { "" }
    ));
    let expected_action = match cache.state {
        PrState::Merged => Some("merge"),
        PrState::Closed => Some("close"),
        _ => None,
    };
    let mut attribution = super::latest_resolution_attribution(&db, &mr_context.mr_id).await;
    if matches!(cache.state, PrState::Merged)
        && attribution
            .as_ref()
            .is_none_or(|event| event.action != "merge")
    {
        log::error!(
            "merge performed by an unjournaled path: merge_request={}",
            mr_context.mr_id
        );
        if let Err(error) =
            super::ensure_unjournaled_merge_observation(orch, &db, &mr_context.mr_id, job_id).await
        {
            log::error!("{error}");
        }
        attribution = super::latest_resolution_attribution(&db, &mr_context.mr_id).await;
    }
    if let Some(attribution) =
        attribution.filter(|event| expected_action.is_some_and(|expected| event.action == expected))
    {
        let actor = match (
            attribution.actor_kind.as_str(),
            attribution.actor_identity.as_deref(),
        ) {
            ("operator-ui", _) => "operator (UI)".to_string(),
            ("operator-cli", _) => "operator (CLI)".to_string(),
            ("agent", Some(uri)) => uri.to_string(),
            ("unjournaled", _) => "merge performed by an unjournaled path".to_string(),
            (_, Some(identity)) => identity.to_string(),
            _ => attribution.surface.clone(),
        };
        let verb = if attribution.action == "close" {
            "Closed"
        } else {
            "Merged"
        };
        out.push_str(&format!("{verb} by {actor} at {}\n\n<details><summary>Resolution provenance</summary>\n\nSurface: `{}`\n\nLane snapshot: `{}`\n\n</details>\n", chrono::DateTime::from_timestamp(attribution.created_at, 0).map(|v| v.to_rfc3339()).unwrap_or_else(|| attribution.created_at.to_string()), attribution.surface, attribution.lane_snapshot));
    }
    if let Some(review) = &cache.review_decision {
        out.push_str(&format!("Review: {}\n", review));
    }
    out.push_str(&format!("Mergeable: {}\n", cache.mergeable));
    if let Some(status) = checks_status {
        out.push_str(&format!("Checks: {}\n", status));
    }
    if let (Some(divergence), Some((src, _))) = (divergence, &source_branches) {
        out.push('\n');
        out.push_str(&divergence.note(src));
        out.push('\n');
    }
    if source_conflict.as_ref().is_some_and(|r| r.tip_conflicted) {
        // A conflicted TIP inflates the diff GitHub reports; flag it so the
        // number can't read as a clean, mergeable change.
        out.push_str(&format!(
            "Changes: +{} -{} (stale — branch tip carries conflicts; resolve before trusting)\n",
            cache.additions.unwrap_or(0),
            cache.deletions.unwrap_or(0)
        ));
    } else {
        out.push_str(&format!(
            "Changes: +{} -{}\n",
            cache.additions.unwrap_or(0),
            cache.deletions.unwrap_or(0)
        ));
    }
    if let (Some(report), Some((src, tgt))) = (source_conflict, &source_branches) {
        if report.tip_conflicted {
            out.push_str("\n⛔ Conflicted history — cannot merge:\n");
            out.push_str(&format_conflicted_commits(&report.commits));
            out.push('\n');
            out.push_str(&conflict_recovery_hint(src.as_str(), Some(tgt.as_str())));
            out.push('\n');
        } else {
            // Clean tip, conflicted intermediates: the merge is not blocked — the
            // guarded flatten collapses these away automatically at merge time.
            out.push_str(
                "\n♻️ Auto-recoverable history — the branch tip is clean; these conflicted intermediate commits are flattened automatically at merge:\n",
            );
            out.push_str(&format_conflicted_commits(&report.commits));
            out.push('\n');
        }
    }

    if let Some(body) = cache.body.as_deref().filter(|b| !b.is_empty()) {
        out.push_str("\n### Description\n\n");
        out.push_str(body);
        out.push('\n');
    }

    if !checks.is_empty() {
        out.push_str("\n### Checks\n\n");
        for c in checks {
            out.push_str(&format!("- [{}] {}\n", check_icon(&c.state), c.name));
        }
    }

    // Turn-end (when:idle/when:review) project checks: live log tail while a suite
    // is in flight, else the cached per-check verdicts for this node's sealed tree.
    if let Some(section) =
        crate::execution::checks_turn_end::render_turn_end_checks_section(orch, job_id).await
    {
        out.push_str(&section);
    }

    if !files.is_empty() {
        out.push_str("\n### Files\n\n");
        for f in files {
            out.push_str(&format!(
                "- {} (+{} -{}) {}\n",
                f.filename, f.additions, f.deletions, f.status
            ));
        }
    }

    if diff_full {
        out.push_str("\n### Diff\n\n");
        for f in files {
            if let Some(patch) = f.patch.as_deref() {
                out.push_str(&format!(
                    "#### {}\n\n```diff\n{}\n```\n\n",
                    f.filename, patch
                ));
            }
        }
    } else if !files.is_empty() {
        out.push_str("\nFull patch: append `?diff=full` to this URI.\n");
    }

    // Actions are valid only while the PR is open.
    if matches!(cache.state, PrState::Open) {
        out.push_str(&format!(
            "\n## actions\n- [merge]({uri}): patch with action:\"merge\" (optional method, default squash). e.g. write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"merge\",method:\"squash\"}}}}]}})\n- [close]({uri}): patch with action:\"close\". e.g. write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"close\"}}}}]}})\n- [refresh]({uri}): patch with action:\"refresh\" to re-fetch live PR state. e.g. write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"refresh\"}}}}]}})",
            uri = artifact_uri
        ));
    }

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::PrState;
    use crate::pr_data::actions::test_support::{
        migrated_db, test_orchestrator, test_orchestrator_with_emitter,
    };
    use crate::pr_data::publication::DiscoveredPr;
    use crate::services::testing::MockGitClient;
    use crate::services::GitOutput;
    use cairn_db::turso::params;
    use std::sync::Arc;

    /// An open merge request against a project WITH a GitHub remote that carries
    /// no bound pull-request number — the shape a failed open leaves behind, and
    /// the shape that rendered "PR #0 — OPEN, MERGEABLE".
    ///
    /// `recorded_number` seeds `github_pr_number` directly so the phantom `0`
    /// binding can be reproduced as it was found in the field.
    async fn seed_open_unbound_mr(db: &LocalDb, job_id: &str, recorded_number: Option<i64>) {
        let job_id = job_id.to_string();
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-u', 'default', 'P', 'proj', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-u', 'proj-u', 1, 'Issue', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('exec-u', 'recipe-1', 'issue-u', 'proj-u', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, status, issue_id, project_id, created_at, updated_at)
                     VALUES (?1, 'exec-u', 'builder', 'complete', 'issue-u', 'proj-u', 1, 1)",
                    params![job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, body, source_branch, target_branch, status, opened_at, updated_at, is_local, github_pr_number, additions, deletions)
                     VALUES ('mr-u', ?1, 'proj-u', 'issue-u', 'A change', 'Body', 'agent/proj-1-builder', 'main', 'open', 1, 1, 0, ?2, 2126840, 30)",
                    params![job_id.as_str(), recorded_number],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// A git client whose only answer is "origin does not have that branch":
    /// `ls-remote` reports absence as a zero exit with empty output.
    fn git_without_the_branch_on_origin() -> MockGitClient {
        let mut git = MockGitClient::new();
        git.expect_run().returning(|_, _| {
            Ok(GitOutput {
                success: true,
                stdout: String::new(),
                stderr: String::new(),
            })
        });
        git
    }

    /// The CAIRN-3189 regression. A row that says `open` over a branch origin has
    /// never seen must not render as an open pull request, must carry no
    /// pull-request number, and must publish no mergeability verdict or change
    /// counts — the stored `+2,126,840 −30` (conflict wreckage measured locally)
    /// is precisely what must not survive a refresh.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unreached_branch_is_not_an_open_pull_request() {
        let orch = test_orchestrator(migrated_db().await, git_without_the_branch_on_origin());
        seed_open_unbound_mr(&orch.db.local, "job-u", None).await;

        let cache = refresh_pr_for_job(&orch, "job-u").await.unwrap();

        assert_eq!(cache.state, PrState::Unpublished);
        assert_eq!(cache.pr_number, None);
        assert_eq!(cache.mergeable, MergeableState::Unknown);
        assert_eq!(
            cache.additions, None,
            "a fabricated diffstat must not survive"
        );
        assert_eq!(cache.deletions, None);

        // And it is cleared from the row, not merely hidden by this read: the
        // desktop's cached path reads those columns directly, so a stale value
        // left behind would keep rendering the wreckage.
        let stored: (Option<i64>, Option<i64>) = orch
            .db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT additions, deletions FROM merge_requests WHERE id = 'mr-u'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    Ok((row.opt_i64(0)?, row.opt_i64(1)?))
                })
            })
            .await
            .unwrap();
        assert_eq!(stored, (None, None));
    }

    /// A recorded `0` is not a binding. Left intact it survives every refresh and
    /// makes the row outrank reality — which is how an issue could not be marked
    /// merged because a pull request that never existed was not merged.
    #[tokio::test(flavor = "current_thread")]
    async fn a_zero_binding_is_treated_as_no_pull_request() {
        let orch = test_orchestrator(migrated_db().await, git_without_the_branch_on_origin());
        seed_open_unbound_mr(&orch.db.local, "job-u", Some(0)).await;

        let context = try_resolve_mr_context_for_job(&orch.db.local, "job-u")
            .await
            .unwrap()
            .expect("the row resolves");
        assert_eq!(
            context.github_pr_number, None,
            "a zero must not present itself as pull request #0"
        );

        // And the refresh takes the unbound path rather than asking GitHub about
        // pull request number zero.
        let cache = refresh_pr_for_job(&orch, "job-u").await.unwrap();
        assert_eq!(cache.pr_number, None);
        assert_eq!(cache.state, PrState::Unpublished);
    }

    /// The artifact text itself: it names the state in the vocabulary of the
    /// work, renders no `#` number, and offers `refresh` (the repair) rather than
    /// `merge`.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unpublished_change_renders_no_number_and_no_verdict() {
        let orch = test_orchestrator(migrated_db().await, git_without_the_branch_on_origin());
        seed_open_unbound_mr(&orch.db.local, "job-u", None).await;

        let section = render_live_pr_section(&orch, "job-u", "cairn://x/pr", false)
            .await
            .expect("a job with a merge request yields a section");

        assert!(section.contains("State: UNPUBLISHED"), "{section}");
        assert!(
            section.contains("has not reached GitHub yet"),
            "names the actual condition: {section}"
        );
        assert!(
            !section.contains("PR #"),
            "renders no pull-request number: {section}"
        );
        assert!(
            !section.contains("MERGEABLE") && !section.contains("Changes: +"),
            "publishes no verdict it has not verified: {section}"
        );
        assert!(
            section.contains("action:\"refresh\"") && !section.contains("action:\"merge\""),
            "offers the repair, not a merge: {section}"
        );
    }

    /// A failed `gh pr create` records its error on the action run that ran it.
    /// The artifact is where a person looks, so the failure has to be visible
    /// there — otherwise a failed open is indistinguishable from a slow one.
    #[tokio::test(flavor = "current_thread")]
    async fn a_failed_open_shows_its_failure_on_the_artifact() {
        let orch = test_orchestrator(migrated_db().await, git_without_the_branch_on_origin());
        seed_open_unbound_mr(&orch.db.local, "job-u", None).await;
        orch.db
            .local
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO action_runs (id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, error_message, completed_at, created_at, parent_job_id)
                         VALUES ('ar-u', 'exec-u', 'pr', 'builtin:pr', 'issue-u', 'proj-u', 'failed', 'gh pr create failed: No commits between main and agent/proj-1-builder', 5, 2, 'job-u')",
                        (),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let section = render_live_pr_section(&orch, "job-u", "cairn://x/pr", false)
            .await
            .expect("a job with a merge request yields a section");

        assert!(
            section.contains("last attempt to open this pull request failed"),
            "{section}"
        );
        assert!(section.contains("No commits between"), "{section}");
    }

    /// A project with no GitHub remote is not "unpublished": its branch IS the
    /// change under review, so it is open by construction and keeps the full
    /// action set.
    #[tokio::test(flavor = "current_thread")]
    async fn a_local_only_change_stays_open() {
        let mut git = MockGitClient::new();
        git.expect_run().never();
        let orch = test_orchestrator(migrated_db().await, git);
        orch.db
            .local
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                         VALUES ('proj-l', 'default', 'P', 'proj', '/repo', 'main', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                         VALUES ('issue-l', 'proj-l', 1, 'Issue', 'active', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at, is_local)
                         VALUES ('mr-l', 'job-l', 'proj-l', 'issue-l', 'A change', 'agent/proj-1-builder', 'main', 'open', 1, 1, 1)",
                        (),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let cache = refresh_pr_for_job(&orch, "job-l").await.unwrap();
        assert_eq!(cache.state, PrState::Open);
        assert_eq!(cache.pr_number, None, "a local change has no PR number");
    }

    /// The repair, persisted: binding a pull request found by head branch makes
    /// every downstream reader — artifact, merge action, issue-resolution guard —
    /// follow the real pull request instead of the stranded row.
    #[tokio::test(flavor = "current_thread")]
    async fn a_discovered_pull_request_is_bound_to_the_row() {
        let orch = test_orchestrator(migrated_db().await, MockGitClient::new());
        seed_open_unbound_mr(&orch.db.local, "job-u", Some(0)).await;

        let mr_context = try_resolve_mr_context_for_job(&orch.db.local, "job-u")
            .await
            .unwrap()
            .expect("the row resolves");
        let bound = adopt_discovered_binding(
            &orch,
            &orch.db.local,
            &mr_context,
            Some(&Publication::Bound(DiscoveredPr {
                number: 2797,
                url: "https://github.com/octo/widget/pull/2797".to_string(),
                state: "OPEN".to_string(),
            })),
        )
        .await;
        assert_eq!(
            bound,
            Some((2797, "https://github.com/octo/widget/pull/2797".to_string()))
        );

        let context = try_resolve_mr_context_for_job(&orch.db.local, "job-u")
            .await
            .unwrap()
            .expect("the row resolves");
        assert_eq!(context.github_pr_number, Some(2797));
    }

    /// The precedence among three answers with two different subjects.
    ///
    /// The middle case is the one a review caught: with a divergent head AND a
    /// conflicted current tip, the local conflict must not overwrite the
    /// divergence downgrade. CONFLICTING would be a claim about the branch's
    /// current tree stated as a property of a pull request that does not contain
    /// it — the same category of error as the green signal.
    #[test]
    fn a_divergent_head_outranks_the_local_conflict_probe() {
        // Same commit on both sides: both answers describe the change under
        // review, and the local probe wins over GitHub's bit, which reports a
        // jj-conflicted commit as mergeable.
        assert_eq!(
            published_mergeable(MergeableState::Mergeable, false, true),
            MergeableState::Conflicting
        );
        // Divergent head, conflicted current tip: two subjects, no verdict.
        assert_eq!(
            published_mergeable(MergeableState::Mergeable, true, true),
            MergeableState::Unknown
        );
        // Divergent head alone.
        assert_eq!(
            published_mergeable(MergeableState::Mergeable, true, false),
            MergeableState::Unknown
        );
        // Agreed head, clean tip: GitHub is describing the change under review.
        assert_eq!(
            published_mergeable(MergeableState::Mergeable, false, false),
            MergeableState::Mergeable
        );
        assert_eq!(
            published_mergeable(MergeableState::Conflicting, false, false),
            MergeableState::Conflicting
        );
    }

    /// The honesty rule as a predicate. A measurement is publishable only when
    /// the thing measured is the thing the world can see.
    #[test]
    fn a_verdict_needs_the_version_the_world_can_see() {
        // No remote: this machine's branch is the whole world's view of it.
        assert!(verdict_is_licensed(
            &Publication::LocalOnly,
            None,
            Some("aaaa")
        ));
        // Published and identical: measurable.
        assert!(verdict_is_licensed(
            &Publication::NoPullRequest,
            Some("aaaa"),
            Some("aaaa")
        ));
        // Published but the local branch has moved on: measuring it would
        // describe a version nobody else holds.
        assert!(!verdict_is_licensed(
            &Publication::NoPullRequest,
            Some("aaaa"),
            Some("bbbb")
        ));
        // Never reached the remote, or the probe could not tell.
        assert!(!verdict_is_licensed(
            &Publication::BranchAbsent,
            None,
            Some("aaaa")
        ));
        assert!(!verdict_is_licensed(
            &Publication::Unknown {
                reason: "offline".to_string()
            },
            None,
            Some("aaaa")
        ));
    }

    /// A closed, local (no-GitHub) merge request keyed by `job_id`. Closed status
    /// keeps `refresh_local_pr_for_job` off the git client, so the test isolates
    /// the database routing.
    async fn seed_closed_local_mr(db: &LocalDb, job_id: &str) {
        let job_id = job_id.to_string();
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-r', 'default', 'P', 'proj', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-r', 'proj-r', 1, 'Issue', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
                     VALUES ('mr-r', ?1, 'proj-r', 'issue-r', 'Team PR', 'agent/proj-1-builder', 'main', 'closed', 1, 1)",
                    params![job_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// A team-prefixed job routes to its injected replica: the PR resolves from
    /// the team DB even though the private DB is empty. Reading `orch.db.local`
    /// (the pre-fix behavior) would miss the row and error.
    #[tokio::test(flavor = "current_thread")]
    async fn refresh_pr_for_job_routes_to_owning_team_replica() {
        let orch = test_orchestrator(migrated_db().await, MockGitClient::new());
        let team = Arc::new(migrated_db().await);
        orch.db.insert_team_db_for_test("teamx", team.clone()).await;
        let job_id = "teamx~00000000-0000-4000-8000-000000000001";
        seed_closed_local_mr(&team, job_id).await;

        let cache = refresh_pr_for_job(&orch, job_id)
            .await
            .expect("the team-owned PR resolves from the injected replica");
        assert_eq!(cache.state, PrState::Closed);
        assert_eq!(cache.title.as_deref(), Some("Team PR"));
    }

    /// Fail-closed: a team-prefixed job whose replica is not open errors rather
    /// than silently falling back to the private DB (the CAIRN-2170 split-brain
    /// class).
    #[tokio::test(flavor = "current_thread")]
    async fn refresh_pr_for_job_fails_closed_without_open_replica() {
        let orch = test_orchestrator(migrated_db().await, MockGitClient::new());
        let job_id = "teamx~00000000-0000-4000-8000-000000000001";
        let err = refresh_pr_for_job(&orch, job_id)
            .await
            .expect_err("a team id with no open replica must fail closed");
        assert!(
            err.contains("fail-closed") || err.contains("replica"),
            "{err}"
        );
    }

    /// A local-only open change: every read of it runs the branch-store probes
    /// (`store_branch_commit` twice, the publication probe, the local diff) with
    /// no GitHub involved, so it is the cleanest way to count what a repeat read
    /// actually costs.
    async fn seed_local_open_mr(db: &LocalDb, job_id: &str) {
        let job_id = job_id.to_string();
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-c', 'default', 'P', 'projc', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-c', 'proj-c', 1, 'Issue', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at, is_local)
                     VALUES ('mr-c', ?1, 'proj-c', 'issue-c', 'A change', 'agent/proj-1-builder', 'main', 'open', 1, 1, 1)",
                    params![job_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    fn merge_request_events(emitter: &crate::services::testing::CapturingEmitter) -> usize {
        emitter
            .events_named("db-change")
            .into_iter()
            .filter(|payload| {
                payload.get("table").and_then(|t| t.as_str()) == Some("merge_requests")
            })
            .count()
    }

    /// The idle case the runner was burning cores on. Once one read has
    /// materialized the projection, every repeat read of unchanged state does no
    /// probe work at all — and, just as importantly, emits nothing, because an
    /// emit is what sends every mounted PR query back around this loop.
    #[tokio::test(flavor = "current_thread")]
    async fn a_repeat_read_of_unchanged_state_probes_nothing_and_emits_nothing() {
        let (orch, emitter) =
            test_orchestrator_with_emitter(migrated_db().await, MockGitClient::new());
        seed_local_open_mr(&orch.db.local, "job-c").await;

        let first = refresh_pr_for_job(&orch, "job-c").await.unwrap();
        assert_eq!(first.state, PrState::Open);
        let after_first = merge_request_events(&emitter);

        for _ in 0..8 {
            let repeat = refresh_pr_for_job(&orch, "job-c").await.unwrap();
            assert_eq!(repeat.state, first.state);
            assert_eq!(repeat.mergeable, first.mergeable);
        }

        assert_eq!(
            merge_request_events(&emitter),
            after_first,
            "a warm read must not emit a merge_requests change"
        );
        assert_eq!(
            orch.pr_refresh_cache.counters().misses,
            1,
            "one computation served every read"
        );
        assert_eq!(orch.pr_refresh_cache.counters().hits, 8);
    }

    /// Sixteen readers arriving together — the desktop panel, the artifact, the
    /// sidebar — share one computation rather than each running their own.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reads_of_one_pull_request_compute_once() {
        let orch =
            std::sync::Arc::new(test_orchestrator(migrated_db().await, MockGitClient::new()));
        seed_local_open_mr(&orch.db.local, "job-c").await;

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(16));
        let mut readers = Vec::new();
        for _ in 0..16 {
            let orch = orch.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                refresh_pr_for_job(&orch, "job-c").await.unwrap().state
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), PrState::Open);
        }

        assert_eq!(
            orch.pr_refresh_cache.counters().misses,
            1,
            "sixteen concurrent readers must produce one refresh"
        );
    }

    /// An explicit operator refresh is the one caller that demands new state, and
    /// it gets exactly one recomputation — not one per press of the button, and
    /// not one per mounted surface.
    #[tokio::test(flavor = "current_thread")]
    async fn an_explicit_refresh_recomputes_once() {
        let orch = test_orchestrator(migrated_db().await, MockGitClient::new());
        seed_local_open_mr(&orch.db.local, "job-c").await;

        refresh_pr_for_job(&orch, "job-c").await.unwrap();
        assert_eq!(orch.pr_refresh_cache.counters().misses, 1);

        refresh_pr_for_job_live(&orch, "job-c").await.unwrap();
        assert_eq!(orch.pr_refresh_cache.counters().misses, 2);

        refresh_pr_for_job(&orch, "job-c").await.unwrap();
        assert_eq!(
            orch.pr_refresh_cache.counters().misses,
            2,
            "the read after an explicit refresh is warm again"
        );
    }

    /// A burst of refresh presses is still one refresh.
    ///
    /// Advancing the generation once per caller would be worse than not
    /// coalescing at all: each advance rejects the readers already computing, so
    /// N simultaneous demands would run the GitHub fetch and the `jj` probes N
    /// times over.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_explicit_refreshes_compute_once() {
        let orch =
            std::sync::Arc::new(test_orchestrator(migrated_db().await, MockGitClient::new()));
        seed_local_open_mr(&orch.db.local, "job-c").await;

        // Warm it first, so the burst is a demand to replace a published value.
        refresh_pr_for_job(&orch, "job-c").await.unwrap();
        let warm = orch.pr_refresh_cache.counters().misses;

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(16));
        let mut readers = Vec::new();
        for _ in 0..16 {
            let orch = orch.clone();
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                refresh_pr_for_job_live(&orch, "job-c").await.unwrap().state
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap(), PrState::Open);
        }

        assert_eq!(
            orch.pr_refresh_cache.counters().misses,
            warm + 1,
            "sixteen simultaneous refresh demands must produce one refresh"
        );
    }

    /// One job's transition leaves every other job's projection warm. Without
    /// this, a single PR moving would re-probe every open pull request.
    #[tokio::test(flavor = "current_thread")]
    async fn invalidating_one_job_leaves_another_warm() {
        let orch = test_orchestrator(migrated_db().await, MockGitClient::new());
        seed_local_open_mr(&orch.db.local, "job-c").await;
        seed_closed_local_mr(&orch.db.local, "job-r").await;

        refresh_pr_for_job(&orch, "job-c").await.unwrap();
        refresh_pr_for_job(&orch, "job-r").await.unwrap();
        assert_eq!(orch.pr_refresh_cache.counters().misses, 2);

        orch.invalidate_pr_refresh("job-c", "test");
        refresh_pr_for_job(&orch, "job-r").await.unwrap();
        assert_eq!(
            orch.pr_refresh_cache.counters().misses,
            2,
            "the untouched job stayed warm"
        );
        refresh_pr_for_job(&orch, "job-c").await.unwrap();
        assert_eq!(orch.pr_refresh_cache.counters().misses, 3);
    }

    /// A merged or closed change is settled: it has no freshness window, so it
    /// stays warm until an exact invalidation rather than re-reading on a timer.
    #[tokio::test(flavor = "current_thread")]
    async fn a_resolved_change_settles_and_an_open_one_stays_live() {
        let now = chrono::Utc::now().timestamp();
        let settled = PrCache {
            id: "mr".into(),
            job_id: None,
            pr_number: Some(1),
            pr_url: String::new(),
            title: None,
            body: None,
            state: PrState::Merged,
            is_draft: false,
            review_decision: None,
            mergeable: MergeableState::Unknown,
            additions: None,
            deletions: None,
            checks_status: None,
            checks: Vec::new(),
            fetched_at: now,
            updated_at: now,
            is_local: false,
            source_branch: None,
            target_branch: None,
        };
        assert_eq!(PrSnapshot::expiry(&settled, now), None);

        let unresolved = PrCache {
            state: PrState::Open,
            ..settled.clone()
        };
        assert_eq!(
            PrSnapshot::expiry(&unresolved, now),
            Some(now + UNRESOLVED_FRESHNESS_SECS),
            "a pull request GitHub has not finished judging stays live"
        );

        let resolved_open = PrCache {
            state: PrState::Open,
            mergeable: MergeableState::Mergeable,
            checks_status: Some(crate::models::ChecksStatus::Success),
            ..settled
        };
        assert_eq!(
            PrSnapshot::expiry(&resolved_open, now),
            Some(now + RESOLVED_FRESHNESS_SECS)
        );
    }

    /// Local no-op: a bare (non-team) job still routes to the private DB, so
    /// local-only installs are byte-for-byte unchanged.
    #[tokio::test(flavor = "current_thread")]
    async fn refresh_pr_for_job_local_is_unchanged() {
        let orch = test_orchestrator(migrated_db().await, MockGitClient::new());
        let job_id = "00000000-0000-4000-8000-000000000009";
        seed_closed_local_mr(&orch.db.local, job_id).await;

        let cache = refresh_pr_for_job(&orch, job_id)
            .await
            .expect("a local PR still resolves against the private DB");
        assert_eq!(cache.state, PrState::Closed);
    }
}
