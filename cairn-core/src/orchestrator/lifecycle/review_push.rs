//! Review-push creation across both trigger edges (work-turn idle and PR-open),
//! plus the turn-end attention/checks emitters shared by finalize and warm
//! transition. Sliced verbatim from the former `lifecycle.rs`.

use crate::orchestrator::attention_push::{Boundary, Wake};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbError, RowExt};

use super::common::*;

/// Synchronously read `(issue_id, IssueAttentionContext)` from a job_id.
/// Returns None when the job has no issue (project-level jobs).
pub(crate) fn issue_for_attention_by_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Option<(
    String,
    crate::orchestrator::attention::IssueAttentionContext,
)> {
    let issue_id = blocking_text_lookup(
        orch,
        job_id,
        "SELECT issue_id FROM jobs WHERE id = ?1",
        TextColumn::Optional,
    )?;
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        let ctx = crate::orchestrator::attention::read_issue_for_attention(&db, &issue_id).await?;
        Ok(Some((issue_id, ctx)))
    })
    .ok()
    .flatten()
}

/// Emit the typed attention event for a turn that just terminalized.
/// - Issue reached a terminal status → `Resolved`
/// - Issue still needs the driver (attention != None) → `AgentIdleWithWork`
/// - Issue has an open PR work product while attention is None → `AgentIdleWithWork`
///   pointing at the producing builder's `/pr` (the freshly-opened-PR case the
///   attention projection deliberately leaves silent)
/// - Otherwise: no emit (the turn ended cleanly with no work left).
///
/// Returns `true` when an actionable fact was emitted, `false` when the turn
/// ended with nothing for the driver to act on. The warm-transition caller uses
/// this to gate the desktop "completed" toast so it fires only on a real
/// idle-with-work, not on every intermediate turn-end (CAIRN-1625).
///
/// Fact construction is shared with the boundary wake (`wake_for_issue`) via
/// [`attention::idle_fact_for_issue`] so the two paths cannot diverge. The
/// terminalized `job_id` biases the open-PR lookup toward this builder's `/pr`.
pub(super) fn emit_for_turn_end(orch: &Orchestrator, job_id: &str) -> bool {
    let Some((issue_id, ctx)) = issue_for_attention_by_job(orch, job_id) else {
        return false;
    };
    let issue_uri = ctx.issue_uri();
    // CAIRN-1882: the single creator of a review push. At this work-turn idle
    // edge, push a review to the issue's watchers when there is reviewable
    // output. Independent of the idle fact computed below (which still drives the
    // desktop "completed" toast and `cairn watch`): only the review-to-watcher
    // wake moved to the push queue.
    create_review_push_on_turn_end(orch, job_id, &issue_id, &ctx);
    // Resolve the fact (and any detail URI) synchronously via the shared helper.
    let dbs = orch.db.clone();
    let issue_id_for_fact = issue_id.clone();
    let ctx_for_fact = ctx.clone();
    let job_id_owned = job_id.to_string();
    let idle = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id_owned)
            .await
            .map_err(|e| e.to_string())?;
        Ok::<_, String>(
            crate::orchestrator::attention::idle_fact_for_issue(
                &db,
                &issue_id_for_fact,
                &ctx_for_fact,
                Some(&job_id_owned),
            )
            .await,
        )
    })
    .ok()
    .flatten();
    let Some(idle) = idle else {
        return false;
    };
    orch.emit_attention_event(crate::orchestrator::AttentionEvent {
        issue_id,
        issue_uri,
        fact: idle.fact,
        attention: ctx.attention,
        status: ctx.status,
        updated_at: idle.updated_at,
    });
    true
}

/// Fire the turn-end (`when:review`) project checks for a job that just idled,
/// detached onto a background task so the minutes-long suite never
/// blocks the turn from ending. Skipped for a trailing memory-review turn (not a
/// work turn) and when a run is already in flight for the job (single-flight).
/// Runs UNSANDBOXED in the background; on any check failure it resumes the idle
/// builder with the failure inlined. Invoked at BOTH turn-end callers
/// (`finalize_run` and `transition_to_warm_state`) so the two stay mirrored.
pub(super) fn spawn_turn_end_checks(orch: &Orchestrator, job_id: &str) {
    let short = &job_id[..job_id.len().min(8)];
    if latest_turn_is_memory_review(orch, job_id) {
        log::debug!(
            "turn-end checks for job {short}: skipped, latest turn is a memory-review turn (not a work turn)"
        );
        return;
    }
    // Claim the single-flight slot; a concurrent run for this job means skip.
    if !orch.try_begin_turn_end_checks(job_id) {
        log::debug!("turn-end checks for job {short}: skipped, a run is already in flight");
        return;
    }
    let orch_clone = orch.clone();
    let job_id_owned = job_id.to_string();
    let orch_for_release = orch.clone();
    let job_id_for_release = job_id.to_string();
    detach_onto_runtime(
        async move {
            crate::execution::checks_turn_end::run_turn_end_checks(orch_clone, job_id_owned).await;
        },
        move || {
            // Runtime construction failed, so the future above never reached
            // `run_turn_end_checks` (which releases the single-flight slot itself
            // on every path). This is the ONLY path where the slot would leak, so
            // release it here to let a later turn-end retry.
            orch_for_release.end_turn_end_checks(&job_id_for_release);
        },
    );
}

/// Detach `fut` onto a runtime that is guaranteed to run it, without ever
/// blocking the caller — which at turn-end is an agent backend's plain
/// `std::thread` stdout loop, not a Tokio worker.
///
/// With an ambient Tokio runtime this is a plain `tokio::spawn`. Without one —
/// the COMMON turn-end case, since the two turn-end hooks fire from the backends'
/// non-runtime stdout threads — spawn a detached `std::thread` that builds its
/// own current-thread runtime and `block_on`s the future. Unlike the sibling
/// detach helpers (`execution::dispatch::run_dispatch_db`,
/// `execution::triggers::block_on_trigger_db`) this thread is NOT joined: the
/// turn-end suite can run for many minutes and the caller is the turn-end path,
/// which must return immediately so the turn can end.
///
/// If the current-thread runtime cannot be built (OS resource exhaustion) the
/// future never runs; `on_spawn_failure` is invoked so the caller can compensate
/// (e.g. release a single-flight slot the future would otherwise have released).
pub(crate) fn detach_onto_runtime(
    fut: impl std::future::Future<Output = ()> + Send + 'static,
    on_spawn_failure: impl FnOnce() + Send + 'static,
) {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::spawn(fut);
        return;
    }
    std::thread::spawn(move || {
        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime.block_on(fut),
            Err(error) => {
                log::error!("detach_onto_runtime: failed to build current-thread runtime: {error}");
                on_spawn_failure();
            }
        }
    });
}

/// Create the review push at this work-turn idle edge (CAIRN-1882).
///
/// The node-idle edge of the review push (the second edge — when a PR opens
/// after the node already idled — is [`create_review_push_for_pr_open`]): when a
/// producing node's turn ends and it is idle, push a `review:{issue}` to each
/// watcher of the node's issue if
///
/// 1. the just-ended turn was a *work* turn (`start_reason != 'memory_review'`),
///    and
/// 2. the issue has reviewable output — either an open, unmerged `merge_requests`
///    row, or a create-pr/unconfirmed-plan artifact for the producing job.
///
/// `content_ref` is the producing node's `/pr` URI when a PR exists, else its
/// artifact URI — the resolvable thing the watcher reviews. Supersede-by-key
/// collapses repeats; the delivery layer (CAIRN-1881) drains, lazy-resolves, and
/// stamps the push. The producing node is never a recipient of its own review.
pub(crate) fn create_review_push_on_turn_end(
    orch: &Orchestrator,
    job_id: &str,
    issue_id: &str,
    ctx: &crate::orchestrator::attention::IssueAttentionContext,
) {
    // Gate: only a WORK-turn idle creates a review push. A trailing memory-review
    // turn end (start_reason = 'memory_review') must not.
    if latest_turn_is_memory_review(orch, job_id) {
        return;
    }
    let dbs = orch.db.clone();
    let issue_id_owned = issue_id.to_string();
    let job_id_owned = job_id.to_string();
    let ctx_owned = ctx.clone();
    let result = run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id_owned)
            .await
            .map_err(|e| e.to_string())?;
        create_review_push_rows(&db, &job_id_owned, &issue_id_owned, &ctx_owned).await
    });
    match result {
        Ok(recipients) => {
            orch.notifier.emit_change("attention_pushes");
            wake_review_recipients(orch, &recipients);
        }
        Err(e) => log::warn!(
            "review push creation for job {} failed: {}",
            &job_id[..job_id.len().min(8)],
            e
        ),
    }
}

/// The PR-open edge of the review push (CAIRN-1891) — the second of the two edges
/// at which a review fires. The model: a review fires when (producing node
/// quiescent) AND (reviewable work exists), at the moment the *later* of the two
/// becomes true. [`create_review_push_on_turn_end`] is the edge where idle is the
/// later event; this is the edge where the PR opening is.
///
/// A builder that writes a `create-pr` artifact and idles cannot fire the review
/// at its idle edge: the PR is not open yet (branch push + webhook lag), and the
/// create-pr artifact auto-confirms on write (the PR lifecycle is the gate,
/// CAIRN-1219), so neither reviewable arm matches at that instant. Slice B
/// (CAIRN-1882) demoted the PR webhook to state-only, so without this edge the
/// review wake is lost entirely. The webhook is the moment that observes the PR
/// opening, so it fires the review here — gated on the producing node being
/// quiescent (not running a *work* turn — a trailing memory-review turn still
/// counts as quiescent — and not self-suspended on its own work) so a mid-work
/// `synchronize` does not fire, and fingerprint-deduped by the shared row creator
/// so a mergeability-only settle (unchanged diffstat) does not re-wake.
///
/// `issue_id` and `source_branch` come from the merge_request the webhook just
/// updated. The producing builder is resolved by `source_branch` so webhook
/// handling works even for old rows whose `merge_requests.job_id` was not a jobs
/// id. That builder is the work-producing job to quiescence-gate; the separate
/// pr-action node is blocked while the PR is open. Shares
/// [`create_review_push_rows`] and [`wake_review_recipients`] with the node-idle
/// edge — one implementation of the fingerprint/dedup/push/wake logic, two
/// trigger edges.
///
/// Live callers are the two IN-PROCESS PR-opening paths, both via
/// `execution::actions::fire_pr_open_review`: `handle_create_pr` (the legacy
/// `builtin:create_pr` action) and `handle_pr_node` (the modern first-class PR
/// node, wired in under CAIRN-2410 — before that this edge never fired on the
/// PR-node path and idle coordinators lost the child-PR review wake). The
/// desktop GitHub webhook handler (`src-tauri/src/github/`) that once also called
/// this is dead code after the runner split (the `github` module is not declared
/// in `src-tauri/src/lib.rs`); its removal is tracked under CAIRN-2408. There is
/// therefore no live webhook-driven caller today — the in-process edges are the
/// only ones that fire.
pub async fn create_review_push_for_pr_open(
    orch: &Orchestrator,
    issue_id: &str,
    source_branch: &str,
) {
    let owning = match crate::issues::crud::owning_db_for_issue(&orch.db, issue_id).await {
        Ok(db) => db,
        Err(e) => {
            log::warn!("PR-open review push: issue db resolve failed: {e}");
            return;
        }
    };
    let db = &owning;
    // Resolve the producing BUILDER by the shared branch — not the pr-action node
    // that owns the merge_request. Without a builder there is nothing to gate on,
    // so skip rather than fire blind.
    let builder_job_id = match find_producing_builder_job(db, issue_id, source_branch).await {
        Ok(Some(job_id)) => job_id,
        Ok(None) => {
            log::warn!(
                "PR-open review push: no producing builder on branch {} for issue {}",
                source_branch,
                &issue_id[..issue_id.len().min(8)]
            );
            return;
        }
        Err(e) => {
            log::warn!("PR-open review push: builder lookup failed: {e}");
            return;
        }
    };
    // Quiescence gate on the BUILDER. A builder running a *work* turn is still
    // committing work (the diffstat is in flux), so the review is premature. But a
    // running *memory-review* turn is quiescent-for-review: the work turn already
    // ended and the PR is the reviewable output, and the node-idle edge gates out
    // memory-review turn ends — so on the common build path (create-pr → trailing
    // memory-review turn → `opened` webhook lands while it runs) this edge is the
    // ONLY one that can wake the watcher. Treating a running memory-review turn as
    // busy would lose that wake permanently. A builder self-suspended on its own
    // sub-work is not done either. Fail closed (a lookup error reads as
    // non-quiescent) so a transient read error never fires a spurious wake.
    let active = crate::messages::delivery::head_turn_active(db, &builder_job_id)
        .await
        .unwrap_or(true);
    let memory_review = latest_turn_is_memory_review(orch, &builder_job_id);
    let self_suspended = crate::messages::delivery::head_turn_self_suspended(db, &builder_job_id)
        .await
        .unwrap_or(true);
    log::debug!(
        "PR-open review: issue={} branch={} builder={} active={} memory_review={} self_suspended={}",
        &issue_id[..issue_id.len().min(8)],
        source_branch,
        &builder_job_id[..builder_job_id.len().min(8)],
        active,
        memory_review,
        self_suspended,
    );
    if (active && !memory_review) || self_suspended {
        return;
    }
    let ctx = match crate::orchestrator::attention::read_issue_for_attention(db, issue_id).await {
        Ok(ctx) => ctx,
        Err(e) => {
            log::warn!(
                "PR-open review push: issue context for {} failed: {}",
                &issue_id[..issue_id.len().min(8)],
                e
            );
            return;
        }
    };
    match create_review_push_rows(db, &builder_job_id, issue_id, &ctx).await {
        Ok(recipients) => {
            log::debug!(
                "PR-open review: pushed to {} watcher(s) for issue {}",
                recipients.len(),
                &issue_id[..issue_id.len().min(8)]
            );
            orch.notifier.emit_change("attention_pushes");
            wake_review_recipients(orch, &recipients);
        }
        Err(e) => log::warn!(
            "PR-open review push creation for job {} failed: {}",
            &builder_job_id[..builder_job_id.len().min(8)],
            e
        ),
    }
}

/// The work-producing builder job for a PR, resolved by the shared branch
/// (CAIRN-1891). The PR may be opened by a separate pr-action node, which is
/// blocked-while-open with a pending turn and so never reads as quiescent; gating
/// on it silently loses the wake. Among jobs on `source_branch`, select the node
/// that **produced the reviewable artifact** (a create-pr or plan). `None` when no
/// job on the branch exists.
async fn find_producing_builder_job(
    db: &crate::storage::LocalDb,
    issue_id: &str,
    source_branch: &str,
) -> Result<Option<String>, String> {
    let issue_id = issue_id.to_string();
    let source_branch = source_branch.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        let source_branch = source_branch.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.id FROM jobs j
                     WHERE j.issue_id = ?1
                       AND j.branch = ?2
                     ORDER BY
                       -- The builder is the node that produced the reviewable
                       -- artifact (create-pr/plan); the pr-action node writes
                       -- none, so this excludes it even when it shares the branch
                       -- and carries a blocked turn.
                       CASE WHEN EXISTS (
                         SELECT 1 FROM artifacts a
                         WHERE a.job_id = j.id
                           AND a.artifact_type IN ('create-pr', 'plan')
                       ) THEN 0 ELSE 1 END,
                       -- Then a node with a worktree that ran agent work turns,
                       -- as a secondary guard against pure action nodes.
                       CASE WHEN j.worktree_path IS NOT NULL THEN 0 ELSE 1 END,
                       CASE WHEN EXISTS (SELECT 1 FROM turns t WHERE t.job_id = j.id)
                            THEN 0 ELSE 1 END,
                       j.created_at DESC
                     LIMIT 1",
                    (issue_id.as_str(), source_branch.as_str()),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok::<_, DbError>(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Create the review push rows for an issue's watchers — the single shared
/// implementation behind both review edges (node-idle and PR-open). Resolves the
/// reviewable `content_ref` + change fingerprint, then pushes a `review:{issue}`
/// to each watcher except the producing node, skipping any watcher whose latest
/// review push already carries this fingerprint (CAIRN-1889 change-trigger;
/// supersede-by-key collapses an undelivered older-fingerprint row). Returns the
/// recipients that received a fresh push so the caller can wake them.
///
/// DB-only and async: the node-idle edge runs it inside `run_db_blocking`; the
/// PR-open webhook edge awaits it directly.
async fn create_review_push_rows(
    db: &crate::storage::LocalDb,
    producing_job_id: &str,
    issue_id: &str,
    ctx: &crate::orchestrator::attention::IssueAttentionContext,
) -> Result<Vec<String>, String> {
    let issue_uri = ctx.issue_uri();
    // Reviewable predicate (disjunction), resolved straight into the content_ref
    // the watcher follows plus a change fingerprint:
    //   arm 1 — an open unmerged PR -> the producing node's `/pr` URI;
    //   arm 2 — a create-pr artifact or unconfirmed plan artifact -> its artifact URI.
    // The second arm is load-bearing at the node-idle edge: at the create-pr idle
    // the PR may not be open yet, but the artifact write is already observable.
    // Neither arm -> no push.
    let Some((content_ref, fingerprint)) = reviewable_ref_and_fingerprint(
        db,
        &ctx.project_key,
        ctx.number,
        issue_id,
        producing_job_id,
    )
    .await?
    else {
        log::debug!(
            "review push: no reviewable ref (job={} issue={})",
            &producing_job_id[..producing_job_id.len().min(8)],
            issue_uri
        );
        return Ok(Vec::new());
    };
    let key = format!("review:{issue_uri}");
    let watchers =
        crate::orchestrator::attention_delivery::subscriber_jobs_for_issue(db, &issue_uri).await?;
    log::debug!(
        "review push: job={} content_ref={} fp={} watchers={}",
        &producing_job_id[..producing_job_id.len().min(8)],
        content_ref,
        fingerprint,
        watchers.len()
    );
    let mut pushed = Vec::new();
    for recipient in watchers {
        // Never push to the producing node itself.
        if recipient == producing_job_id {
            continue;
        }
        // CAIRN-1889 change-trigger: skip when the latest review push to this
        // recipient (delivered OR undelivered) already carries this fingerprint —
        // the reviewable state is unchanged, so re-firing would spuriously
        // re-wake. A changed fingerprint creates a new push; supersede-by-key
        // still collapses an undelivered older-fingerprint row to the newest.
        if let Some(Some(prev)) =
            crate::orchestrator::attention_push::latest_push_fingerprint(db, &recipient, &key)
                .await
                .map_err(|e| e.to_string())?
        {
            if prev == fingerprint {
                log::debug!(
                    "review push: recipient {} deduped (fingerprint unchanged)",
                    &recipient[..recipient.len().min(8)]
                );
                continue;
            }
        }
        let (_, effective) = crate::orchestrator::attention_push::push_with_fingerprint(
            db,
            &recipient,
            &content_ref,
            Wake::Wake,
            Boundary::Event,
            &key,
            Some(&fingerprint),
        )
        .await
        .map_err(|e| e.to_string())?;
        // A watcher that muted this issue gets a `Passive` review row that rides
        // along on its next run; it is not handed back to `wake_review_recipients`
        // so an idle muted watcher is never woken (CAIRN-1900).
        if effective.wakes_idle() {
            pushed.push(recipient);
        }
    }
    Ok(pushed)
}

/// Wake each watcher that received a fresh review push so an already-idle one
/// wakes now instead of only seeing the review ride along with an unrelated later
/// wake (CAIRN-1889). `nudge_job_for_urgency` is the shared resume-ladder
/// primitive: an idle recipient resumes; a busy one (a mid-turn agent OR a
/// self-suspended one, both of which read as active) is left alone for the
/// event-boundary push drain or its own-work resume to deliver the push. Steer
/// wakes idle and never stops a busy turn.
fn wake_review_recipients(orch: &Orchestrator, recipients: &[String]) {
    for recipient in recipients {
        if let Err(e) = crate::messages::delivery::nudge_job_for_urgency(
            orch,
            recipient,
            crate::messages::queued::DeliveryUrgency::Steer,
        ) {
            log::warn!(
                "review push wake for {} failed: {}",
                &recipient[..recipient.len().min(8)],
                e
            );
        }
    }
}

/// Whether the job's most recent turn was a memory-review turn. The work-turn
/// idle gate for the review push: a trailing `memory_review` turn end is not a
/// work turn and must not create a review push (CAIRN-1882).
fn latest_turn_is_memory_review(orch: &Orchestrator, job_id: &str) -> bool {
    blocking_text_lookup(
        orch,
        job_id,
        "SELECT CASE WHEN start_reason = 'memory_review' THEN '1' ELSE '0' END
         FROM turns WHERE job_id = ?1
         ORDER BY created_at DESC, sequence DESC LIMIT 1",
        TextColumn::Optional,
    )
    .as_deref()
        == Some("1")
}

/// The reviewable content_ref + change fingerprint at a work-turn idle, or `None`
/// when nothing is reviewable (CAIRN-1889). The fingerprint is the change key the
/// creator compares against the latest review push to decide whether to re-fire.
///
/// - Arm 1 (open unmerged PR), branch-precise: scoped to the producing builder's
///   own branch (`jobs.branch == merge_requests.source_branch`) so an unrelated
///   open PR on another branch for the same issue can't drive this node's review.
///   The producing node's `/pr` URI and a head-SHA-or-diffstat fingerprint (see
///   [`open_pr_review_arm`]).
/// - Arm 2 (create-pr artifact or unconfirmed plan artifact): the artifact URI
///   and an `artifact:{version}:{updated_at}` fingerprint.
async fn reviewable_ref_and_fingerprint(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
    job_id: &str,
) -> Result<Option<(String, String)>, String> {
    // Arm 1 is scoped to this builder's own branch. Both edges pass the builder
    // job, whose `jobs.branch` is the PR's `source_branch`, so the open-PR lookup
    // is unambiguous even when an issue carries more than one branch/PR.
    if let Some(source_branch) = job_branch(db, job_id).await? {
        if let Some(arm1) =
            open_pr_review_arm(db, project_key, number, issue_id, &source_branch, job_id).await?
        {
            return Ok(Some(arm1));
        }
    }
    review_artifact_ref(db, project_key, number, job_id).await
}

/// The producing builder's branch (`jobs.branch`), or `None` when unset (a
/// plan-only node has no worktree branch, in which case arm 1 never applies).
async fn job_branch(db: &crate::storage::LocalDb, job_id: &str) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT branch FROM jobs WHERE id = ?1", (job_id.as_str(),))
                .await?;
            match rows.next().await? {
                Some(row) => Ok::<_, DbError>(row.opt_text(0)?),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Arm 1: an open PR on `source_branch` for the issue, resolved to a content_ref
/// the watcher reviews plus a change fingerprint.
///
/// Reviewability is driven by the `merge_requests` row ALONE — "an open PR on
/// this branch." The content URI is built from the `builder_job_id` we already
/// resolved (cleanly joinable to its execution), NOT from `mr.job_id`: that is
/// the pr-action node, whose execution may not join, and an inner join through it
/// dropped the whole row so a real open PR read as unreviewable (the live
/// CAIRN-1891 failure). The builder node is the right review target anyway. If
/// the builder's node coordinates don't resolve, the content_ref falls back to
/// the issue URI, which still resolves for the drain/render path.
///
/// The fingerprint prefers the PR head commit SHA (`pr:{mr}:sha:{sha}`), which a
/// real new commit always changes and a mergeability-only settle never does; it
/// falls back to the diffstat (`pr:{mr}:{additions}:{deletions}`) when no head
/// SHA has been recorded yet. `None` when no open PR exists on that branch.
async fn open_pr_review_arm(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    issue_id: &str,
    source_branch: &str,
    builder_job_id: &str,
) -> Result<Option<(String, String)>, String> {
    let issue_id = issue_id.to_string();
    let source_branch = source_branch.to_string();
    let builder_job_id = builder_job_id.to_string();
    #[allow(clippy::type_complexity)]
    let resolved: Option<(
        String,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
    )> = db
        .read(|conn| {
            let issue_id = issue_id.clone();
            let source_branch = source_branch.clone();
            let builder_job_id = builder_job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT mr.id, mr.additions, mr.deletions, mr.head_sha,
                                e.seq, j.uri_segment, parent.uri_segment
                         FROM merge_requests mr
                         LEFT JOIN jobs j ON j.id = ?3
                         LEFT JOIN executions e ON j.execution_id = e.id
                         LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                         WHERE mr.issue_id = ?1 AND mr.source_branch = ?2
                           AND mr.status = 'open'
                         ORDER BY mr.updated_at DESC
                         LIMIT 1",
                        (
                            issue_id.as_str(),
                            source_branch.as_str(),
                            builder_job_id.as_str(),
                        ),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some((
                        row.text(0)?,
                        row.opt_i64(1)?,
                        row.opt_i64(2)?,
                        row.opt_text(3)?,
                        row.opt_i64(4)?,
                        row.opt_text(5)?,
                        row.opt_text(6)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    Ok(resolved.map(
        |(mr_id, additions, deletions, head_sha, seq, uri_segment, parent_segment)| {
            let content_ref = match (seq, uri_segment) {
                (Some(seq), Some(node)) => match parent_segment {
                    Some(parent) => cairn_common::uri::build_task_artifact_uri_named(
                        project_key,
                        number,
                        seq as i32,
                        &parent,
                        &node,
                        None,
                    ),
                    None => cairn_common::uri::build_node_artifact_uri_named(
                        project_key,
                        number,
                        seq as i32,
                        &node,
                        None,
                    ),
                },
                _ => format!("cairn://p/{project_key}/{number}"),
            };
            let fingerprint = match head_sha {
                Some(sha) => format!("pr:{mr_id}:sha:{sha}"),
                None => {
                    let fmt =
                        |n: Option<i64>| n.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
                    format!("pr:{mr_id}:{}:{}", fmt(additions), fmt(deletions))
                }
            };
            (content_ref, fingerprint)
        },
    ))
}

/// The producing job's create-pr or unconfirmed plan artifact, resolved to its
/// node-artifact URI plus an `artifact:{version}:{updated_at}` change fingerprint
/// (CAIRN-1889), or `None` when the job has no such artifact. Arm 2 of the
/// review-push reviewable predicate. `create-pr` remains reviewable even when it
/// is already confirmed because the PR lifecycle auto-confirms that artifact
/// before every deployment shape has observed the PR-open edge.
pub(crate) async fn review_artifact_ref(
    db: &crate::storage::LocalDb,
    project_key: &str,
    number: i32,
    job_id: &str,
) -> Result<Option<(String, String)>, String> {
    let job_id = job_id.to_string();
    #[allow(clippy::type_complexity)]
    let resolved: Option<(i64, String, Option<String>, Option<String>, i64, i64)> = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT e.seq, j.uri_segment, parent.uri_segment, a.output_name, a.version, a.updated_at
                         FROM artifacts a
                         JOIN jobs j ON a.job_id = j.id
                         JOIN executions e ON j.execution_id = e.id
                         LEFT JOIN jobs parent ON j.parent_job_id = parent.id
                         WHERE a.job_id = ?1
                           AND (a.artifact_type = 'create-pr'
                                OR (a.artifact_type = 'plan' AND a.confirmed = 0))
                         ORDER BY a.version DESC
                         LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some((
                        row.i64(0)?,
                        row.text(1)?,
                        row.opt_text(2)?,
                        row.opt_text(3)?,
                        row.i64(4)?,
                        row.i64(5)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    // A sub-agent task job nests its artifact under the parent node
    // (`.../{parent}/task/{task}/...`); a top-level node uses the flat node URI.
    // The flat form does not resolve for a sub-task (issue #143).
    Ok(resolved.map(
        |(seq, node, parent_segment, output_name, version, updated_at)| {
            let uri = match parent_segment {
                Some(parent) => cairn_common::uri::build_task_artifact_uri_named(
                    project_key,
                    number,
                    seq as i32,
                    &parent,
                    &node,
                    output_name.as_deref(),
                ),
                None => cairn_common::uri::build_node_artifact_uri_named(
                    project_key,
                    number,
                    seq as i32,
                    &node,
                    output_name.as_deref(),
                ),
            };
            (uri, format!("artifact:{version}:{updated_at}"))
        },
    ))
}
