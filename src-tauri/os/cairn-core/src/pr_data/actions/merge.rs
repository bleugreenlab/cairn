//! Merge orchestration: the conflict gate, default-branch vs child landing,
//! merge-method resolution, and post-merge reconciliation. Every landing is a
//! store fold; a remote one is published by a leased fast-forward of origin's
//! target, never through GitHub's merge API.

use crate::execution::teardown::{cleanup_issue_jobs, TeardownReason, TeardownScope};
use crate::github::api;
use crate::github::credentials::get_owner_repo_for_pr;
use crate::orchestrator::Orchestrator;
use crate::pr_data::helpers::{
    assert_main_checkout_clean_for_default_merge, local_pr_files,
    reconcile_main_checkout_after_merge,
};

/// What the provably-human base-merge policy decides for one merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BaseMergeGate {
    /// The merge does not cross into the base branch.
    NotApplicable,
    Disabled,
    /// The stored policy cannot be read, so the merge is refused.
    Unavailable(String),
    /// The merge lands only on the selected phone's approval.
    RequiresHumanApproval {
        enrollment_id: String,
    },
}

impl BaseMergeGate {
    /// The gate decision's journal state and detail (sequence 0).
    fn journal(&self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::NotApplicable => ("not_applicable", None),
            Self::Disabled => ("policy_disabled", None),
            Self::Unavailable(_) => ("policy_unavailable", Some("stored policy unavailable")),
            Self::RequiresHumanApproval { .. } => ("approval_required", None),
        }
    }
}

fn enforce_human_base_merge_policy(
    policy: &HumanBaseMergePolicyReadState,
    crosses_base: bool,
) -> BaseMergeGate {
    if !crosses_base {
        return BaseMergeGate::NotApplicable;
    }
    match policy {
        HumanBaseMergePolicyReadState::Available { policy }
            if policy.state == HumanBaseMergePolicyState::Disabled =>
        {
            BaseMergeGate::Disabled
        }
        HumanBaseMergePolicyReadState::Available { policy } => {
            match policy.approval_enrollment_id.as_deref() {
                Some(enrollment_id) => BaseMergeGate::RequiresHumanApproval {
                    enrollment_id: enrollment_id.to_string(),
                },
                // `parse` refuses this shape; kept total so the gate fails closed.
                None => BaseMergeGate::Unavailable(
                    "Refusing to merge into the project base branch: provably-human approval is \
                     required, but no approval phone is selected. Choose a phone in project \
                     settings."
                        .to_string(),
                ),
            }
        }
        HumanBaseMergePolicyReadState::Unavailable { reason } => BaseMergeGate::Unavailable(format!(
            "Refusing to merge into the project base branch: the provably-human merge policy is unavailable ({reason:?})."
        )),
    }
}

async fn journal_policy_gate_event(
    db: &LocalDb,
    merge_context: &MergeMrContext,
    job_id: &str,
    state: &str,
    stored_policy: &str,
    attribution: Option<&super::PrResolutionAttribution>,
    detail: Option<&str>,
) -> Result<String, String> {
    let authority_decision = attribution.map(|value| {
        serde_json::json!({
            "legacyActorKind": value.actor_kind,
            "legacyActorIdentity": value.actor_identity,
            "surface": value.surface,
        })
        .to_string()
    });
    let event_id = Uuid::new_v4().to_string();
    db.execute(
        "INSERT INTO merge_attempt_events (
            event_id, attempt_id, sequence, project_id, merge_request_id, job_id,
            state, target_ref, source_ref, policy_json, authority_decision_json,
            detail, created_at
         ) VALUES (?1, ?1, 0, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            event_id.as_str(),
            merge_context.project_id.as_str(),
            merge_context.mr.mr_id.as_str(),
            job_id,
            state,
            merge_context.target_branch.as_str(),
            merge_context.source_branch.as_str(),
            stored_policy,
            authority_decision.as_deref(),
            detail,
            chrono::Utc::now().timestamp(),
        ],
    )
    .await
    .map_err(|error| format!("Failed to journal merge policy decision: {error}"))?;
    Ok(event_id)
}

/// A gate a caller asks the merge to prove before landing. A precondition only
/// ever adds a refusal; it never relaxes another gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergePrecondition {
    /// The change is exactly an upstream tool-pin bump the base revision's
    /// `pinBumps` config describes, verified at the head being landed.
    PinBump,
}

impl std::str::FromStr for MergePrecondition {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pinBump" => Ok(Self::PinBump),
            other => Err(format!(
                "unknown merge precondition {other:?}; the only precondition is \"pinBump\""
            )),
        }
    }
}

/// Read `payload.precondition` for a PR action. Absent is `None`; anything
/// present must be exactly the string `"pinBump"` on a merge, because a
/// misspelled or mistyped precondition that was silently ignored would land
/// the merge without the gate its caller asked for.
pub fn parse_merge_precondition(
    precondition: Option<&serde_json::Value>,
    action: &str,
) -> Result<Option<MergePrecondition>, String> {
    let Some(value) = precondition else {
        return Ok(None);
    };
    if action != "merge" {
        return Err("payload.precondition is only accepted with action='merge'".to_string());
    }
    let Some(text) = value.as_str() else {
        return Err(format!(
            "payload.precondition must be the string \"pinBump\", not {value}"
        ));
    };
    text.parse()
        .map(Some)
        .map_err(|error| format!("payload.precondition: {error}"))
}

/// The identity every event of one merge attempt repeats.
#[derive(Debug, Clone, Copy)]
pub(super) struct MergeAttemptJournal<'a> {
    pub attempt_id: &'a str,
    pub project_id: &'a str,
    pub merge_request_id: &'a str,
    pub job_id: &'a str,
    pub target_ref: &'a str,
    pub source_ref: &'a str,
    pub policy_json: &'a str,
}

/// The optional columns of one merge attempt event: what was asked, of
/// which challenge, and the Human evidence the approval carried.
#[derive(Debug, Clone, Default)]
pub(super) struct JournalEntry {
    pub detail: Option<String>,
    pub intent_digest_sha256: Option<String>,
    pub challenge_id: Option<String>,
    pub approval_id: Option<String>,
    pub human_principal_json: Option<String>,
    pub human_appearance_json: Option<String>,
    pub verification_json: Option<String>,
}

/// Append one event to a merge attempt's immutable journal.
pub(super) async fn journal_merge_attempt_event(
    db: &LocalDb,
    attempt: &MergeAttemptJournal<'_>,
    sequence: i64,
    state: &str,
    entry: &JournalEntry,
) -> Result<(), String> {
    db.execute(
        "INSERT INTO merge_attempt_events (
            event_id, attempt_id, sequence, project_id, merge_request_id, job_id,
            state, target_ref, source_ref, intent_digest_sha256, challenge_id, approval_id,
            policy_json, human_principal_json, human_appearance_json, verification_json,
            detail, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
        params![
            Uuid::new_v4().to_string(),
            attempt.attempt_id,
            sequence,
            attempt.project_id,
            attempt.merge_request_id,
            attempt.job_id,
            state,
            attempt.target_ref,
            attempt.source_ref,
            entry.intent_digest_sha256.as_deref(),
            entry.challenge_id.as_deref(),
            entry.approval_id.as_deref(),
            attempt.policy_json,
            entry.human_principal_json.as_deref(),
            entry.human_appearance_json.as_deref(),
            entry.verification_json.as_deref(),
            entry.detail.as_deref(),
            chrono::Utc::now().timestamp(),
        ],
    )
    .await
    .map_err(|error| format!("Failed to journal merge attempt event: {error}"))?;
    Ok(())
}

/// Append an ordinary (ungated) attempt's terminal event, sequence 1.
async fn journal_merge_attempt_outcome(
    db: &LocalDb,
    attempt: MergeAttemptJournal<'_>,
    result: &Result<String, String>,
    success_detail: Option<&str>,
) -> Result<(), String> {
    let (state, detail) = match result {
        Ok(_) => ("succeeded", success_detail),
        Err(error) => ("failed", Some(error.as_str())),
    };
    journal_merge_attempt_terminal(db, attempt, state, detail).await
}

async fn journal_merge_attempt_terminal(
    db: &LocalDb,
    attempt: MergeAttemptJournal<'_>,
    state: &str,
    detail: Option<&str>,
) -> Result<(), String> {
    journal_merge_attempt_event(
        db,
        &attempt,
        1,
        state,
        &JournalEntry {
            detail: detail.map(str::to_string),
            ..JournalEntry::default()
        },
    )
    .await
}
use crate::security::broker::github::installation_authority;
use crate::storage::LocalDb;
use cairn_db::models::{HumanBaseMergePolicyReadState, HumanBaseMergePolicyState};
use cairn_db::turso::params;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

use super::context::{db_error, resolve_merge_mr_context_for_job, MergeMrContext};
use super::default_landing::PublishedLanding;
use super::landing_record::journal_accepted_publication;
use super::refresh::refresh_pr_for_job;
use super::resolution::{resolve_pr_node_landed, Landed};

/// Resolve a PR owner id to the `jobs.id` used by `file_changes.job_id`.
async fn resolve_file_change_job_id(
    db: &LocalDb,
    owner_id: &str,
) -> Result<Option<String>, String> {
    let owner_id = owner_id.to_string();
    db.query_opt_text(
        "SELECT id
         FROM (
             SELECT j.id AS id, 0 AS priority
             FROM jobs j
             WHERE j.id = ?1
             UNION ALL
             SELECT ar.parent_job_id AS id, 1 AS priority
             FROM action_runs ar
             JOIN jobs parent ON parent.id = ar.parent_job_id
             WHERE ar.id = ?1 AND ar.parent_job_id IS NOT NULL
         )
         ORDER BY priority
         LIMIT 1",
        params![owner_id.as_str()],
    )
    .await
    .map_err(|e| db_error("Failed to resolve file-change job owner", e))
}

/// Store file changes from a merged PR for issue history tracking.
async fn store_file_changes(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    files: &[api::PrFile],
) -> Result<(), String> {
    let job_id = job_id.to_string();
    let files = files.to_vec();
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let job_id = job_id.clone();
        let files = files.clone();
        Box::pin(async move {
            for file in files {
                let mut rows = conn
                    .query(
                        "SELECT id
                         FROM file_changes
                         WHERE job_id = ?1 AND file_path = ?2
                         LIMIT 1",
                        params![job_id.as_str(), file.filename.as_str()],
                    )
                    .await?;
                let existing_id = crate::storage::next_text(&mut rows, 0).await?;
                drop(rows);

                if let Some(existing_id) = existing_id {
                    conn.execute(
                        "UPDATE file_changes
                         SET status = ?1, additions = ?2, deletions = ?3, previous_path = ?4
                         WHERE id = ?5",
                        params![
                            file.status.as_str(),
                            file.additions,
                            file.deletions,
                            file.previous_filename.as_deref(),
                            existing_id.as_str()
                        ],
                    )
                    .await?;
                } else {
                    let id = Uuid::new_v4().to_string();
                    conn.execute(
                        "INSERT INTO file_changes (
                            id, job_id, file_path, status, additions, deletions,
                            previous_path, created_at
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            id.as_str(),
                            job_id.as_str(),
                            file.filename.as_str(),
                            file.status.as_str(),
                            file.additions,
                            file.deletions,
                            file.previous_filename.as_deref(),
                            now
                        ],
                    )
                    .await?;
                }
            }

            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to store file changes", e))?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "file_changes", "action": "insert"}),
    );

    Ok(())
}

/// Background post-merge reconciliation shared by the in-app merge
/// (`merge_pr_for_job`) and the GitHub merge path.
///
/// Runs the side effects that follow a PR landing, in order:
/// 1. capture the merged file list for issue history,
/// 2. reconcile the user's main checkout only when the merge advanced the project
///    default branch,
/// 3. tear down the merged issue's worktrees,
/// 4. refresh the cached PR row.
///
/// Every step is non-fatal: each logs and continues on failure so one broken
/// step never strands the rest. The PR-merged state transition itself
/// (`resolve_pr_node`, which also fires base-advance notifications, plus the
/// caller's attention emit) has already happened by the time this runs. Takes
/// owned values so callers can spawn it detached via `tokio::spawn`.
/// Reconciliation follows the landing lifecycle: a GitHub landing pulls because
/// no local fold advanced the checkout, while a local fold repairs the checkout
/// from the ref it already advanced without an optional network operation.
fn should_reconcile_main_checkout_after_merge(
    target_branch: &str,
    resolved_default_branch: &str,
) -> bool {
    target_branch == resolved_default_branch
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckoutReconciliation {
    PullRemoteLanding,
    RepairLocalFold,
}

impl CheckoutReconciliation {
    fn pulls_remote(self) -> bool {
        matches!(self, Self::PullRemoteLanding)
    }
}

pub async fn reconcile_after_merge(
    orch: Orchestrator,
    db: Arc<LocalDb>,
    ctx: MergeMrContext,
    checkout_reconciliation: CheckoutReconciliation,
) {
    let owner_id = ctx.mr.job_id.clone();
    let repo_path = ctx.mr.repo_path.clone();
    let pr_number = ctx.mr.github_pr_number;
    let default_branch = ctx.default_branch.clone();
    let resolved_default_branch = resolve_project_default_branch(&repo_path, &default_branch);
    let target_branch = ctx.target_branch.clone();
    let issue_id = ctx.issue_id.clone();

    log::info!(
        "Starting post-merge reconciliation for owner {} (target branch '{}')",
        owner_id,
        target_branch
    );

    // 1. Reconcile the user's project checkout FIRST, before the network file
    //    capture below, so the detach window is bounded by store work rather than
    //    stretched behind a GitHub round-trip. With the export wrapper repairing
    //    HEAD synchronously at each export this is belt-and-braces, but it also
    //    bounds any export path the wrapper's pre/post attribution cannot cover.
    //    Only a real default-branch advance detaches or moves the main checkout;
    //    a child→integration merge never does, and resetting there only races
    //    unrelated work.
    if should_reconcile_main_checkout_after_merge(&target_branch, &resolved_default_branch) {
        let git = &*orch.services.git;
        let pull = checkout_reconciliation.pulls_remote();
        if let Err(e) =
            reconcile_main_checkout_after_merge(git, &repo_path, &resolved_default_branch, pull)
        {
            log::warn!("Failed to reconcile main checkout after merge: {}", e);
        }
    } else {
        log::debug!(
            "Skipping main checkout reconcile after merge into non-default target '{}' (resolved default '{}')",
            target_branch,
            resolved_default_branch
        );
    }

    let file_change_job_id = match resolve_file_change_job_id(&db, &owner_id).await {
        Ok(job_id) => job_id,
        Err(e) => {
            log::warn!(
                "Skipping post-merge file capture for owner {}: {}",
                owner_id,
                e
            );
            None
        }
    };
    if file_change_job_id.is_none() {
        log::warn!(
            "Skipping post-merge file capture for owner {}: no jobs.id owner found",
            owner_id
        );
    }

    // 2. Capture file changes for issue history.
    if let Some(pr_number) = pr_number {
        match get_owner_repo_for_pr(&ctx.mr.pr_url, &repo_path) {
            Ok((owner, repo)) => match installation_authority(&orch.db.local, &owner).await {
                Ok(auth) => {
                    let http = &*orch.services.http;
                    match api::fetch_pr_files(http, &auth, &owner, &repo, pr_number).await {
                        Ok(files) => {
                            if let Some(file_change_job_id) = file_change_job_id.as_deref() {
                                let count = files.len();
                                if let Err(e) =
                                    store_file_changes(&orch, &db, file_change_job_id, &files).await
                                {
                                    log::warn!("Failed to store file changes: {}", e);
                                } else {
                                    log::info!(
                                        "Stored {} file changes for owner {} under job {}",
                                        count,
                                        owner_id,
                                        file_change_job_id
                                    );
                                }
                            }
                        }
                        Err(e) => log::warn!("Failed to fetch PR files for history: {}", e),
                    }
                }
                Err(e) => log::warn!("Skipping post-merge file capture (credentials): {}", e),
            },
            Err(e) => log::warn!("Skipping post-merge file capture (owner/repo): {}", e),
        }
    } else {
        match local_pr_files(
            &*orch.services.git,
            Path::new(&repo_path),
            &ctx.target_branch,
            &ctx.source_branch,
        ) {
            Ok(files) => {
                if let Some(file_change_job_id) = file_change_job_id.as_deref() {
                    if let Err(e) = store_file_changes(&orch, &db, file_change_job_id, &files).await
                    {
                        log::warn!("Failed to store local file changes: {}", e);
                    } else {
                        log::info!(
                            "Stored {} local file changes for owner {} under job {}",
                            files.len(),
                            owner_id,
                            file_change_job_id
                        );
                    }
                }
            }
            Err(e) => log::warn!("Failed to capture local file changes: {}", e),
        }
    }

    // 3. Downstream-workspace reconciliation is spawned (not awaited) inside
    //    `resolve_pr_node`, so it runs in the background off the user-facing merge
    //    path, in `base_advance::reconcile_jj_downstream`. It does two asymmetric
    //    things over the shared store: in-flight SIBLINGS (branched FROM the
    //    integration branch) auto-rebase onto the advanced tip, and the workspace
    //    whose branch IS the integration branch (a Coordinator) has its `@`
    //    re-parented onto the folded tip via `crate::jj::advance_workspace_onto` —
    //    the jj-native restoration of the old git "post-merge fast-forward of
    //    active worktrees". So there is no git worktree fast-forward step here.

    // 4. Release runtime resources and apply the established branch cleanup policy.
    //    Scoped to the merged PR's issue, so a Coordinator worktree on a
    //    different issue survives and the step-3 fast-forward sticks.
    if let Some(issue_id) = issue_id.as_deref() {
        // A merge that reaches here already ran a verified fold (the merge
        // postcondition) or an out-of-band GitHub squash — the content is
        // incorporated, so branches are cleaned up unconditionally. The
        // preserve-unlanded guard belongs on the record-only status path, not
        // here, where an ancestor test would misfire on a legitimate squash.
        if let Err(e) = cleanup_issue_jobs(
            &orch,
            TeardownScope::Issue(issue_id.to_string()),
            TeardownReason::Discarded,
        )
        .await
        {
            log::warn!("Worktree teardown after merge failed: {}", e);
        }
    }

    // 5. Refresh PR details.
    let _ = refresh_pr_for_job(&orch, &owner_id).await;

    log::info!("Post-merge reconciliation completed for owner {}", owner_id);
}

/// Resolve the effective merge method for a PR. Squash is the default shape for
/// a normal PR landing on the default branch (one commit per PR), but workspace
/// PRs deliberately preserve their individual commits, so they always force
/// `"merge"` regardless of the requested method.
fn effective_merge_method(merge_context: &MergeMrContext, merge_method: Option<String>) -> String {
    let force_merge = merge_context.is_workspace;
    if force_merge {
        "merge".to_string()
    } else {
        merge_method.unwrap_or_else(|| "squash".to_string())
    }
}

/// The merge method a DEFAULT-BRANCH landing uses. Only meaningful when the
/// target IS the default branch.
///
/// A remote pull request's landing is published as ONE commit whose only parent
/// is origin's expected old target, by a leased fast-forward, and is journaled
/// and recovered as that shape (`default_landing`, `PublicationShape::Squash`).
/// It therefore takes only `squash`: any other requested method is refused by
/// name, never silently squashed or published under a shape the journal does
/// not describe. A local fold applies the workspace forcing
/// (`effective_merge_method`).
fn default_landing_method(
    ctx: &MergeMrContext,
    resolved_default: &str,
    merge_method: Option<String>,
) -> Result<String, String> {
    if !is_remote_default_landing(ctx, resolved_default) {
        return Ok(effective_merge_method(ctx, merge_method));
    }
    match merge_method.as_deref() {
        None | Some("squash") => Ok("squash".to_string()),
        Some(other) => Err(format!(
            "Refusing to merge `{source}` into `{target}` with method `{other}`: a pull \
             request lands on the default branch as one squash commit, published by a \
             fast-forward of origin's `{target}`, so only `squash` is available. \
             `{target}` was not advanced.",
            source = ctx.source_branch,
            target = ctx.target_branch,
        )),
    }
}

/// Resolve a project's effective (config-aware) default branch from its stored
/// value. Mirrors how worktree creation resolves the default, so the merge
/// routing and the post-merge checkout reconcile can never disagree with where
/// worktrees were based — stopping short of trusting the raw DB column the merge
/// used to read directly.
fn resolve_project_default_branch(repo_path: &str, stored_default: &str) -> String {
    crate::config::project_settings::effective_default_branch(
        Some(Path::new(repo_path)),
        Some(stored_default),
    )
}

/// Whether this merge is a remote pull request's landing on the default branch,
/// which publishes one squash commit to origin, rather than a local fold. Every
/// default landing is a store fold (`default_landing`); this decides only the
/// method it may use (`default_landing_method`). Pure so it is unit-testable
/// without an `Orchestrator`: `resolved_default` is the config-aware default
/// resolved by the caller.
///
/// True only for a real remote PR (`github_pr_number` present, not a local-only
/// project) that lands on the default branch and is not a workspace PR. A
/// Coordinator child→integration PR (`target_branch` is the integration branch,
/// not the default) and a local-only project are local folds; workspace PRs
/// force the keep-every-commit `merge` method and are excluded too.
fn is_remote_default_landing(ctx: &MergeMrContext, resolved_default: &str) -> bool {
    ctx.mr.github_pr_number.is_some()
        && !ctx.mr.is_local
        && !ctx.is_workspace
        && ctx.target_branch == resolved_default
}

/// Merge a PR, mark the issue merged, and run post-merge reconciliation (file
/// capture, optional main-repo pull, active-worktree fast-forward, teardown, PR
/// refresh) in the background via `reconcile_after_merge`.
///
/// A merge is a commit barrier on the PR's source branch: commits earlier
/// batches sealed there but could not publish are republished first, and the
/// merge is refused while any is still held, because the pull request would
/// ship without work its author was told is committed.
pub async fn merge_pr_for_job(
    orch: &Orchestrator,
    job_id: &str,
    merge_method: Option<String>,
    attribution: Option<super::PrResolutionAttribution>,
    precondition: Option<MergePrecondition>,
) -> Result<String, String> {
    // One measured record per call, refusals included (`merge_metrics`).
    let attempt = super::merge_metrics::MergeAttempt::begin(job_id);
    let result = super::merge_metrics::measured(
        Arc::clone(&attempt),
        merge_pr_for_job_measured(orch, job_id, merge_method, attribution, precondition),
    )
    .await;
    attempt.finish(&result);
    result
}

async fn merge_pr_for_job_measured(
    orch: &Orchestrator,
    job_id: &str,
    merge_method: Option<String>,
    attribution: Option<super::PrResolutionAttribution>,
    precondition: Option<MergePrecondition>,
) -> Result<String, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;
    let merge_context = resolve_merge_mr_context_for_job(&db, job_id).await?;
    super::merge_metrics::identify(
        &merge_context.mr.mr_id,
        merge_context.mr.github_pr_number,
        &merge_context.source_branch,
        &merge_context.target_branch,
    );
    let barrier = crate::mcp::handlers::run::held::pr_commit_barrier(
        orch,
        &merge_context.project_id,
        &merge_context.source_branch,
    )
    .await;
    barrier.refuse_merge()?;
    merge_pr_for_job_past_barrier(orch, job_id, merge_method, attribution, precondition)
        .await
        .map(|reply| barrier.prefix(reply))
        .map_err(|error| barrier.prefix(error))
}

async fn merge_pr_for_job_past_barrier(
    orch: &Orchestrator,
    job_id: &str,
    merge_method: Option<String>,
    attribution: Option<super::PrResolutionAttribution>,
    precondition: Option<MergePrecondition>,
) -> Result<String, String> {
    // Route to the owning database (team replica or private DB). The merge
    // request, its issue, and the producing job for a team execution all live in
    // the team replica; GitHub credentials stay on the private DB.
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;
    let merge_context = resolve_merge_mr_context_for_job(&db, job_id).await?;
    let repo_path = merge_context.mr.repo_path.clone();

    // Per-phase timing for the synchronous merge path. The downstream sibling
    // reconcile no longer runs here (it is spawned inside `resolve_pr_node`), so
    // these spans bound only the work the "merging" button actually waits on:
    // the conflict gate, the store fold + origin round-trips, and the DB/
    // execution-recompute resolution. Logged at `info` so a live merge reports
    // real numbers from the user's own instance.
    let merge_started = std::time::Instant::now();

    // Merge boundary: refuse a jj-conflicted source bookmark before the store
    // fold. Fails closed regardless of cached/rendered mergeable state — a
    // conflicted child must never be folded into integration. Under
    // store-owns-merge the old staleness window largely dissolves: a
    // cleanly-rebased sibling is pushed immediately, so origin's PR head tracks
    // the local rebased tip rather than lagging a stale pre-rebase commit.
    let gate_started = std::time::Instant::now();
    let resolved_default =
        resolve_project_default_branch(&repo_path, &merge_context.default_branch);
    let stored_policy = db
        .query_opt_text(
            "SELECT human_base_merge_policy FROM projects WHERE id = ?1",
            (merge_context.project_id.clone(),),
        )
        .await
        .map_err(|error| format!("Failed to load base-merge policy: {error}"))?
        .ok_or_else(|| {
            "Refusing to merge: the project's provably-human merge policy is unavailable."
                .to_string()
        })?;
    let policy = HumanBaseMergePolicyReadState::from_stored(&stored_policy);
    let crosses_base = merge_context.target_branch == resolved_default;
    let gate = enforce_human_base_merge_policy(&policy, crosses_base);
    let (journal_state, detail) = gate.journal();
    let attempt_id = journal_policy_gate_event(
        &db,
        &merge_context,
        job_id,
        journal_state,
        &stored_policy,
        attribution.as_ref(),
        detail,
    )
    .await?;
    super::merge_metrics::journaled(&attempt_id);

    // Evaluated ahead of the ordinary policy refusal so a pin bump that cannot
    // land names its own condition rather than the generic policy message.
    let pin_bump = match precondition {
        None => None,
        Some(MergePrecondition::PinBump) => {
            let inputs = super::pin_bump::collect_inputs(
                orch,
                &db,
                &merge_context,
                &resolved_default,
                policy.clone(),
            )
            .await;
            match super::pin_bump::evaluate(&inputs) {
                Ok(evidence) => Some(evidence),
                Err(refusal) => {
                    journal_merge_attempt_terminal(
                        &db,
                        MergeAttemptJournal {
                            attempt_id: &attempt_id,
                            project_id: &merge_context.project_id,
                            merge_request_id: &merge_context.mr.mr_id,
                            job_id,
                            target_ref: &merge_context.target_branch,
                            source_ref: &merge_context.source_branch,
                            policy_json: &stored_policy,
                        },
                        "violation",
                        Some(&format!("pinBump: {refusal}")),
                    )
                    .await?;
                    return Err(format!(
                        "Refusing to merge `{}` as a pin bump: {refusal}",
                        merge_context.source_branch
                    ));
                }
            }
        }
    };
    // The landing is bound to the commit the bump was verified at: GitHub is
    // told the expected head, and the local fold re-reads the source under the
    // store lock.
    let verified_head = pin_bump
        .as_ref()
        .map(|evidence| evidence.head_commit.clone());
    let pin_bump_detail = pin_bump.as_ref().map(ToString::to_string);
    let approval_enrollment = match gate {
        BaseMergeGate::Unavailable(refusal) => return Err(refusal),
        BaseMergeGate::RequiresHumanApproval { enrollment_id } => Some(enrollment_id),
        BaseMergeGate::NotApplicable | BaseMergeGate::Disabled => None,
    };
    let mr_description = if approval_enrollment.is_some() {
        db.query_opt_text(
            "SELECT body FROM merge_requests WHERE id = ?1",
            (merge_context.mr.mr_id.clone(),),
        )
        .await
        .ok()
        .flatten()
    } else {
        None
    };

    let journal_project_id = merge_context.project_id.clone();
    let journal_merge_request_id = merge_context.mr.mr_id.clone();
    let journal_target_ref = merge_context.target_branch.clone();
    let journal_source_ref = merge_context.source_branch.clone();
    // An earlier approved merge of this pull request whose outcome is still
    // being reconciled owns whatever lands next; a second one would make that
    // landing impossible to attribute.
    if approval_enrollment.is_some() {
        if let Some(previous) =
            super::human_approval::unsettled_attempt_for(&db, &merge_context.mr.mr_id).await?
        {
            let refusal = format!(
                "Refusing to merge: the outcome of the previous approved merge of this pull \
                 request ({previous}) is still being reconciled. Check the pull request, then \
                 merge again once that settles."
            );
            journal_merge_attempt_terminal(
                &db,
                MergeAttemptJournal {
                    attempt_id: &attempt_id,
                    project_id: &journal_project_id,
                    merge_request_id: &journal_merge_request_id,
                    job_id,
                    target_ref: &journal_target_ref,
                    source_ref: &journal_source_ref,
                    policy_json: &stored_policy,
                },
                "failed",
                Some(&refusal),
            )
            .await?;
            return Err(refusal);
        }
    }
    let mut config_notice: Option<String> = None;
    // A landing origin accepted, whose journal outcome is written once the
    // merge is recorded.
    let mut published: Option<PublishedLanding> = None;
    let mut settles_github = false;
    // Once the ceremony starts it journals the attempt's outcome itself; a
    // gated attempt refused before then is journaled here like any other.
    let mut ceremony_started = false;
    let merge_result = async {
        let db = db.clone();
        // A conflicted source history is the shared owner-merge preparation's
        // to heal or refuse (opening the conflict session), and the landing
        // then validates the exact combined tree. The default landing judges
        // the configuration it would land on that same exact tree.
    if merge_context.target_branch == resolved_default {
        assert_main_checkout_clean_for_default_merge(&*orch.services.git, &repo_path)?;
    }
    let fold_started = std::time::Instant::now();
    // Serialize every fold behind the per-store mutex so a merge fold and a
    // base-advance reconcile on the same shared store never run jj ops
    // concurrently (which would mint divergent conflicted copies). The detached
    // downstream reconcile spawned later inside `resolve_pr_node` acquires the
    // same lock, after this guard drops — no nested acquisition.
    let merge_store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    super::merge_metrics::route(
        match (
            merge_context.target_branch == resolved_default,
            merge_context.mr.is_local,
            approval_enrollment.is_some(),
        ) {
            (false, _, _) => "child",
            (true, true, false) => "store",
            (true, true, true) => "store-approval",
            (true, false, false) => "origin",
            (true, false, true) => "origin-approval",
        },
    );
    let (merged_commit, landed) = if merge_context.target_branch == resolved_default {
        // The default branch takes only a merge result prepared onto its
        // exact target, bound and validated there, and only if the source, the
        // target and the relationship are unchanged since (`default_landing`).
        // It lands by the store fold, published to origin (when there is one)
        // by a leased fast-forward of the target alone.
        let method =
            default_landing_method(&merge_context, &resolved_default, merge_method.clone())?;
        let landed = match approval_enrollment.as_deref() {
            Some(enrollment_id) => {
                ceremony_started = true;
                let (landed, notice) = super::default_landing::land_on_default_with_approval(
                    orch,
                    job_id,
                    &merge_context,
                    &method,
                    super::default_landing::ApprovalGate {
                        db: &db,
                        enrollment_id,
                        description: mr_description.clone(),
                        attempt: MergeAttemptJournal {
                            attempt_id: &attempt_id,
                            project_id: &journal_project_id,
                            merge_request_id: &journal_merge_request_id,
                            job_id,
                            target_ref: &journal_target_ref,
                            source_ref: &journal_source_ref,
                            policy_json: &stored_policy,
                        },
                    },
                )
                .await?;
                if let Some(notice) = notice {
                    config_notice = merge_journal_detail(&[config_notice.as_deref(), Some(&notice)]);
                }
                landed
            }
            None => {
                super::default_landing::land_on_default(
                    orch,
                    job_id,
                    &merge_context,
                    &method,
                    verified_head.as_deref(),
                    &MergeAttemptJournal {
                        attempt_id: &attempt_id,
                        project_id: &journal_project_id,
                        merge_request_id: &journal_merge_request_id,
                        job_id,
                        target_ref: &journal_target_ref,
                        source_ref: &journal_source_ref,
                        policy_json: &stored_policy,
                    },
                )
                .await?
            }
        };
        if let Some(notice) = landed.config_notice.as_deref() {
            config_notice = merge_journal_detail(&[config_notice.as_deref(), Some(notice)]);
        }
        log::info!(
            "merge_pr_for_job[{job_id}]: merge gates and the default-branch landing took {:?}",
            gate_started.elapsed()
        );
        published = landed.published.clone();
        settles_github = !merge_context.mr.is_local;
        let commit = landed
            .landed_commit
            .clone()
            .ok_or_else(|| "the store fold reported no landed commit".to_string())?;
        (commit, landed)
    } else {
        log::info!(
            "merge_pr_for_job[{job_id}]: merge gates took {:?}",
            gate_started.elapsed()
        );
        // A child lands on its integration branch by the same reserved,
        // two-phase protocol, as a fast-forward of the integration branch to
        // the prepared source head (`child_landing`).
        let landed = super::child_landing::land_on_integration(
            orch,
            job_id,
            &db,
            &merge_context,
            &MergeAttemptJournal {
                attempt_id: &attempt_id,
                project_id: &journal_project_id,
                merge_request_id: &journal_merge_request_id,
                job_id,
                target_ref: &journal_target_ref,
                source_ref: &journal_source_ref,
                policy_json: &stored_policy,
            },
        )
        .await?;
        published = landed.published.clone();
        settles_github = !merge_context.mr.is_local;
        let commit = landed
            .landed_commit
            .clone()
            .ok_or_else(|| "the child landing reported no landed commit".to_string())?;
        (commit, landed)
    };
    // Held until the merge is recorded: it carries the landing's reservation.
    let mut landing = Some(landed);
    log::info!(
        "merge_pr_for_job[{job_id}]: the landing (preparation, validation and publication) took \
         {:?}",
        fold_started.elapsed()
    );
    let _bookkeeping = super::merge_metrics::time(super::merge_metrics::Phase::Bookkeeping);

    // Merge postcondition: the fold must have advanced the target bookmark to
    // contain the source tip. Verify it BEFORE `resolve_pr_node` marks the PR
    // merged, resolves the issue, or tears anything down — so a fold that
    // silently no-ops (a future regression) converts to a loud, recoverable
    // error instead of the silent data loss CAIRN-2287 traces. No commits are
    // lost on failure: nothing downstream has run, and the source bookmark is
    // untouched. `store_merge_child` FFs the target onto the (possibly squashed)
    // source, so on every success path the source is an ancestor of the target.
    {
        let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
        if !crate::jj::bookmark_landed_in(
            &jj,
            &merge_store,
            &merge_context.source_branch,
            &merge_context.target_branch,
        ) {
            return Err(format!(
                "Merge postcondition failed: after folding `{source}` into `{target}`, the target bookmark does not contain the source tip. Refusing to mark the PR merged — no commits were lost and the source branch is intact; retry the merge.",
                source = merge_context.source_branch,
                target = merge_context.target_branch,
            ));
        }
    }
    super::merge_metrics::landed(&merged_commit);
    if let Some(landed) = landing.as_ref() {
        super::default_landing::record_allocations(orch, &db, &merge_context, landed).await;
    }

    match crate::orchestrator::catch_up::mode_for_project(
        orch, &merge_context.project_id, &merge_store, &merge_context.source_branch,
    ).await {
        Ok(Some(mode)) => crate::orchestrator::catch_up::record_landing_merged(
            orch, &mode, &merge_store, &merge_context.target_branch, Some(&merged_commit),
        ).await,
        Ok(None) => {},
        Err(error) => log::warn!("landing accepted but catch-up event lookup failed: {error}"),
    }
    crate::orchestrator::catch_up::end_on_landing(
        orch, &db, &merge_context.project_id, &repo_path, &merge_context.source_branch,
    ).await;
    let resolve_started = std::time::Instant::now();
    // Every landing moves the source and the target to the same landing
    // commit (a default landing's squash, a child's prepared head), so that
    // commit is the source's exact merged identity.
    let merged_source = landing.as_ref().map(|_| merged_commit.as_str());
    resolve_pr_node_landed(
        orch,
        job_id,
        attribution,
        Landed {
            commit: Some(merged_commit.as_str()),
            source: merged_source,
        },
    )
    .await
    .map_err(|error| {
        log::error!(
                "Merged git branch but failed to mark merge request merged for job/action {}: {}",
                job_id,
                error
            );
            error
        })?;
    // The merge is recorded: the landing's source reads merged, so the final
    // check refuses any child still naming it, and its reservation can go.
    if let Some(landed) = landing.as_mut() {
        landed.reservation = None;
    }
    log::info!(
        "merge_pr_for_job[{job_id}]: resolve_pr_node took {:?}",
        resolve_started.elapsed()
    );

    // Background reconciliation, shared with the GitHub webhook merge path. The
    // local fold already advanced the checkout ref, so reconciliation repairs the
    // checkout without an unnecessary network pull.
    tokio::spawn(reconcile_after_merge(
        orch.clone(),
        db,
        merge_context,
        CheckoutReconciliation::RepairLocalFold,
    ));

    log::info!(
        "merge_pr_for_job[{job_id}]: synchronous merge path took {:?} (downstream reconcile deferred)",
        merge_started.elapsed()
    );
        Ok(with_merge_notice(
            "PR merged successfully".to_string(),
            config_notice.as_deref(),
        ))
    }
    .await;

    if merge_result.is_ok() && settles_github {
        let _settle = super::merge_metrics::time(super::merge_metrics::Phase::Settle);
        super::landing_record::settle_github_best_effort(orch, job_id).await;
    }
    let _journal = super::merge_metrics::time(super::merge_metrics::Phase::Bookkeeping);
    journal_merge_outcomes(
        &db,
        MergeAttemptJournal {
            attempt_id: &attempt_id,
            project_id: &journal_project_id,
            merge_request_id: &journal_merge_request_id,
            job_id,
            target_ref: &journal_target_ref,
            source_ref: &journal_source_ref,
            policy_json: &stored_policy,
        },
        published,
        merge_result,
        merge_journal_detail(&[pin_bump_detail.as_deref(), config_notice.as_deref()]),
        // An approval-gated attempt's ceremony journals its own outcome,
        // which only a proven landing makes `succeeded`.
        approval_enrollment.is_none() || !ceremony_started,
    )
    .await
}

/// Journal how a merge ended, then answer with its result.
///
/// A publication origin accepted journals its own outcome under its own
/// attempt, and only once the merge is recorded; until then its intent is what
/// recovery records it from, and `published` keeps it in flight so recovery
/// leaves it to this call. The base attempt's terminal outcome is written
/// whatever the publication's journal said: a merge that landed always has
/// one. Only a publication outcome that contradicts the landing is an error.
async fn journal_merge_outcomes(
    db: &LocalDb,
    attempt: MergeAttemptJournal<'_>,
    published: Option<PublishedLanding>,
    merge_result: Result<String, String>,
    success_detail: Option<String>,
    journals_base: bool,
) -> Result<String, String> {
    let publication = match (&published, &merge_result) {
        (Some(published), Ok(_)) => {
            journal_accepted_publication(
                db,
                &MergeAttemptJournal {
                    attempt_id: &published.attempt_id,
                    ..attempt
                },
                &published.publication,
                success_detail.clone(),
            )
            .await
        }
        _ => Ok(()),
    };
    // Settled, or left unsettled for recovery: no longer driven here.
    drop(published);
    if journals_base {
        journal_merge_attempt_outcome(db, attempt, &merge_result, success_detail.as_deref())
            .await?;
    }
    publication?;
    merge_result
}

/// What the successful landing of a merge request recorded beyond an ordinary
/// one (a pin bump's evidence, a config notice), for the merged PR's
/// rendering.
pub(super) async fn landing_record(db: &LocalDb, merge_request_id: &str) -> Option<String> {
    db.query_opt_text(
        "SELECT detail FROM merge_attempt_events
          WHERE merge_request_id = ?1 AND state = 'succeeded'
            AND challenge_id IS NULL AND detail IS NOT NULL
          ORDER BY created_at DESC, rowid DESC
          LIMIT 1",
        params![merge_request_id],
    )
    .await
    .ok()
    .flatten()
}

/// What the merge journal's outcome event records beyond an ordinary landing:
/// a pin bump's evidence, a config notice, the empty commits dropped.
fn merge_journal_detail(details: &[Option<&str>]) -> Option<String> {
    let details: Vec<&str> = details.iter().flatten().copied().collect();
    (!details.is_empty()).then(|| details.join("; "))
}

/// A merge's reply with a notice the person who merged must read, set apart
/// as a `⚠️` paragraph: the PR view raises each such paragraph on its own.
fn with_merge_notice(reply: String, notice: Option<&str>) -> String {
    match notice {
        Some(notice) => format!("{reply}\n\n⚠️ {notice}"),
        None => reply,
    }
}

#[cfg(test)]
mod tests {
    use super::super::context::MrContext;
    use super::*;
    use crate::storage::RowExt;

    async fn merge_journal_test_db() -> LocalDb {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(dir.keep().join("merge-journal.db"))
            .await
            .unwrap();
        db.execute(
            "INSERT INTO workspaces (id, name, created_at, updated_at)
             VALUES ('workspace', 'Workspace', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO projects (
                id, workspace_id, name, key, repo_path, human_base_merge_policy,
                created_at, updated_at
             ) VALUES (
                'project', 'workspace', 'Project', 'project', '/tmp/repo',
                '{\"version\":1,\"state\":\"disabled\"}', 1, 1
             )",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO merge_requests (
                id, job_id, project_id, title, source_branch, target_branch,
                opened_at, updated_at
             ) VALUES ('mr', 'job', 'project', 'PR', 'feature', 'main', 1, 1)",
            (),
        )
        .await
        .unwrap();
        db
    }

    async fn journal_rows(
        db: &LocalDb,
        attempt_id: &str,
    ) -> Vec<(i64, String, String, Option<String>)> {
        db.read(|conn| {
            let attempt_id = attempt_id.to_string();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT sequence, event_id, state, detail
                         FROM merge_attempt_events
                         WHERE attempt_id = ?1
                         ORDER BY sequence",
                        (attempt_id,),
                    )
                    .await?;
                let mut events = Vec::new();
                while let Some(row) = rows.next().await? {
                    events.push((row.i64(0)?, row.text(1)?, row.text(2)?, row.opt_text(3)?));
                }
                Ok(events)
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn allowed_merge_attempts_append_terminal_outcomes_with_the_same_attempt_id() {
        let db = merge_journal_test_db().await;
        let context = merge_context(false, false);
        let policy = r#"{"version":1,"state":"disabled"}"#;

        for result in [
            Ok("merged".to_string()),
            Err("merge failed at the landing boundary".to_string()),
        ] {
            let attempt_id = journal_policy_gate_event(
                &db,
                &context,
                "job",
                "policy_disabled",
                policy,
                None,
                None,
            )
            .await
            .unwrap();
            journal_merge_attempt_outcome(
                &db,
                MergeAttemptJournal {
                    attempt_id: &attempt_id,
                    project_id: "project",
                    merge_request_id: "mr",
                    job_id: "job",
                    target_ref: "main",
                    source_ref: "feature",
                    policy_json: policy,
                },
                &result,
                None,
            )
            .await
            .unwrap();

            let rows = journal_rows(&db, &attempt_id).await;
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].0, 0);
            assert_eq!(rows[0].2, "policy_disabled");
            assert_eq!(rows[1].0, 1);
            assert_ne!(rows[0].1, rows[1].1, "events have immutable identities");
            match &result {
                Ok(_) => {
                    assert_eq!(rows[1].2, "succeeded");
                    assert_eq!(rows[1].3, None);
                }
                Err(error) => {
                    assert_eq!(rows[1].2, "failed");
                    assert_eq!(rows[1].3.as_deref(), Some(error.as_str()));
                }
            }

            let update = db
                .execute(
                    "UPDATE merge_attempt_events SET state = 'failed'
                     WHERE attempt_id = ?1 AND sequence = 0",
                    (attempt_id.as_str(),),
                )
                .await;
            assert!(
                update.is_err(),
                "the journal must reject mutation of an appended event"
            );
            assert_eq!(journal_rows(&db, &attempt_id).await, rows);
        }
    }

    const DISABLED: &str = r#"{"version":1,"state":"disabled"}"#;

    fn attempt_journal(attempt_id: &str) -> MergeAttemptJournal<'_> {
        MergeAttemptJournal {
            attempt_id,
            project_id: "project",
            merge_request_id: "mr",
            job_id: "job",
            target_ref: "main",
            source_ref: "feature",
            policy_json: DISABLED,
        }
    }

    /// An ordinary merge attempt whose landing origin accepted as `landing`:
    /// its gate event, and its publication's intent, still driven.
    async fn landed_attempt(db: &LocalDb, landing: &str) -> (String, PublishedLanding) {
        let attempt_id = journal_policy_gate_event(
            db,
            &merge_context(false, false),
            "job",
            "policy_disabled",
            DISABLED,
            None,
            None,
        )
        .await
        .unwrap();
        let publication = super::super::landing_record::Publication {
            target_branch: "main".into(),
            expected_old: "t".repeat(40),
            target_before: "t".repeat(40),
            landing_commit: landing.to_string(),
            landing_tree: "e".repeat(40),
            source_branch: "feature".into(),
            source_before: "h".repeat(40),
            source_after: landing.to_string(),
            shape: super::super::landing_record::PublicationShape::Squash,
        };
        let publication_id = format!("{attempt_id}-publication-1");
        super::super::landing_record::journal_publication_intent(
            db,
            &attempt_journal(&publication_id),
            &publication,
        )
        .await
        .unwrap();
        let published = PublishedLanding {
            publication,
            _driving: super::super::default_landing::PublicationInFlight::enter(&publication_id)
                .unwrap(),
            attempt_id: publication_id,
        };
        (attempt_id, published)
    }

    /// The outcome a landing-recovery pass journals for a publication it
    /// finds landed.
    async fn settle_by_recovery(db: &LocalDb, publication_id: &str, landed: &str) {
        journal_merge_attempt_event(
            db,
            &attempt_journal(publication_id),
            2,
            "succeeded",
            &JournalEntry {
                detail: Some(
                    serde_json::json!({"reconciled": true, "landedCommit": landed}).to_string(),
                ),
                ..JournalEntry::default()
            },
        )
        .await
        .unwrap();
    }

    fn states(rows: &[(i64, String, String, Option<String>)]) -> Vec<(i64, &str)> {
        rows.iter()
            .map(|(sequence, _, state, _)| (*sequence, state.as_str()))
            .collect()
    }

    #[tokio::test]
    async fn a_landed_merge_journals_its_publication_then_its_attempt() {
        let db = merge_journal_test_db().await;
        let landing = "l".repeat(40);
        let (attempt_id, published) = landed_attempt(&db, &landing).await;
        let publication_id = published.attempt_id.clone();

        let reply = journal_merge_outcomes(
            &db,
            attempt_journal(&attempt_id),
            Some(published),
            Ok("PR merged successfully".to_string()),
            None,
            true,
        )
        .await;

        assert_eq!(reply, Ok("PR merged successfully".to_string()));
        let publication_rows = journal_rows(&db, &publication_id).await;
        assert_eq!(
            states(&publication_rows),
            [(1, "executing"), (2, "succeeded")]
        );
        assert_eq!(publication_rows[1].3, None);
        assert_eq!(
            states(&journal_rows(&db, &attempt_id).await),
            [(0, "policy_disabled"), (1, "succeeded")]
        );
        assert!(
            super::super::default_landing::PublicationInFlight::enter(&publication_id).is_ok(),
            "the publication is no longer driven once its outcome is journaled"
        );
    }

    /// Recovery settled the publication as the same landing before the
    /// command journaled it: one fact, recorded once. The merge still answers
    /// success and its attempt still gets its terminal outcome.
    #[tokio::test]
    async fn a_publication_recovery_already_settled_as_this_landing_does_not_fail_the_merge() {
        let db = merge_journal_test_db().await;
        let landing = "l".repeat(40);
        let (attempt_id, published) = landed_attempt(&db, &landing).await;
        let publication_id = published.attempt_id.clone();
        settle_by_recovery(&db, &publication_id, &landing).await;
        let settled = journal_rows(&db, &publication_id).await;

        let reply = journal_merge_outcomes(
            &db,
            attempt_journal(&attempt_id),
            Some(published),
            Ok("PR merged successfully".to_string()),
            None,
            true,
        )
        .await;

        assert_eq!(reply, Ok("PR merged successfully".to_string()));
        assert_eq!(
            journal_rows(&db, &publication_id).await,
            settled,
            "recovery's outcome stands"
        );
        assert_eq!(
            states(&journal_rows(&db, &attempt_id).await),
            [(0, "policy_disabled"), (1, "succeeded")]
        );
    }

    /// A settlement naming another commit contradicts what this merge landed:
    /// it is returned, naming both, after the attempt's own outcome is written.
    #[tokio::test]
    async fn a_publication_settled_as_a_different_landing_is_surfaced() {
        let db = merge_journal_test_db().await;
        let (landing, other) = ("l".repeat(40), "m".repeat(40));
        let (attempt_id, published) = landed_attempt(&db, &landing).await;
        settle_by_recovery(&db, &published.attempt_id, &other).await;

        let error = journal_merge_outcomes(
            &db,
            attempt_journal(&attempt_id),
            Some(published),
            Ok("PR merged successfully".to_string()),
            None,
            true,
        )
        .await
        .unwrap_err();

        assert!(error.contains(&landing), "{error}");
        assert!(error.contains(&other), "{error}");
        assert_eq!(
            states(&journal_rows(&db, &attempt_id).await),
            [(0, "policy_disabled"), (1, "succeeded")]
        );
    }

    #[tokio::test]
    async fn policy_refusal_is_a_truthful_terminal_sequence_zero_event() {
        let db = merge_journal_test_db().await;
        let context = merge_context(false, false);
        let policy = r#"{"version":2,"state":"enabled"}"#;
        let detail = "stored policy unavailable";

        let attempt_id = journal_policy_gate_event(
            &db,
            &context,
            "job",
            "policy_unavailable",
            policy,
            None,
            Some(detail),
        )
        .await
        .unwrap();

        let rows = journal_rows(&db, &attempt_id).await;
        assert_eq!(
            rows,
            vec![(
                0,
                attempt_id,
                "policy_unavailable".to_string(),
                Some(detail.to_string()),
            )],
            "a refusal is terminal at the gate and must not claim execution"
        );
    }

    #[test]
    fn human_base_merge_policy_gates_only_the_actual_base_crossing() {
        let disabled =
            HumanBaseMergePolicyReadState::from_stored(r#"{"version":1,"state":"disabled"}"#);
        assert_eq!(
            enforce_human_base_merge_policy(&disabled, true),
            BaseMergeGate::Disabled
        );

        let enabled = HumanBaseMergePolicyReadState::from_stored(
            r#"{"version":1,"state":"enabled","approval_enrollment_id":"cae_1"}"#,
        );
        assert_eq!(
            enforce_human_base_merge_policy(&enabled, true),
            BaseMergeGate::RequiresHumanApproval {
                enrollment_id: "cae_1".to_string()
            }
        );
        assert_eq!(
            enforce_human_base_merge_policy(&enabled, false),
            BaseMergeGate::NotApplicable
        );

        // Enabled without a phone, and an unknown version, both fail closed.
        for stored in [
            r#"{"version":1,"state":"enabled"}"#,
            r#"{"version":2,"state":"enabled"}"#,
        ] {
            let unavailable = HumanBaseMergePolicyReadState::from_stored(stored);
            let BaseMergeGate::Unavailable(refusal) =
                enforce_human_base_merge_policy(&unavailable, true)
            else {
                panic!("{stored} must refuse");
            };
            assert!(refusal.contains("policy is unavailable"), "{refusal}");
            assert_eq!(
                enforce_human_base_merge_policy(&unavailable, false),
                BaseMergeGate::NotApplicable
            );
        }
    }

    fn merge_context(is_workspace: bool, has_triage_batch: bool) -> MergeMrContext {
        MergeMrContext {
            mr: MrContext {
                mr_id: "mr".to_string(),
                pr_url: String::new(),
                github_pr_number: None,
                repo_path: "/tmp/repo".to_string(),
                job_id: "job".to_string(),
                is_local: false,
                project_id: "project".to_string(),
                issue_id: None,
            },
            issue_id: Some("issue".to_string()),
            default_branch: "main".to_string(),
            project_id: "project".to_string(),
            target_branch: "main".to_string(),
            source_branch: "feature".to_string(),
            title: "PR".to_string(),
            is_workspace,
            has_triage_batch,
        }
    }

    /// Squash is the default landing shape; only workspace PRs force `merge` so
    /// their individual commits survive, even when the caller asks for another
    /// method.
    #[test]
    fn effective_merge_method_defaults_squash_and_only_forces_merge_for_workspace() {
        // Workspace PR forces merge regardless of the requested method.
        assert_eq!(
            effective_merge_method(&merge_context(true, false), Some("squash".to_string())),
            "merge"
        );
        // Memory-triage-batch PRs now honor the requested method.
        assert_eq!(
            effective_merge_method(&merge_context(false, true), Some("rebase".to_string())),
            "rebase"
        );
        // Memory-triage-batch PRs default to squash like normal PRs.
        assert_eq!(
            effective_merge_method(&merge_context(false, true), None),
            "squash"
        );
        // A normal PR with no requested method defaults to squash.
        assert_eq!(
            effective_merge_method(&merge_context(false, false), None),
            "squash"
        );
        // An explicit method on a normal PR is honored.
        assert_eq!(
            effective_merge_method(&merge_context(false, false), Some("merge".to_string())),
            "merge"
        );
    }

    /// A remote PR's default landing is one squash commit, so it takes only
    /// `squash` and refuses any other method by name instead of silently
    /// squashing it; a local fold keeps the workspace forcing.
    #[test]
    fn a_remote_default_landing_takes_only_squash_and_refuses_other_methods_by_name() {
        let mut remote = merge_context(false, false);
        remote.mr.github_pr_number = Some(7);
        assert_eq!(
            default_landing_method(&remote, "main", None).unwrap(),
            "squash"
        );
        assert_eq!(
            default_landing_method(&remote, "main", Some("squash".to_string())).unwrap(),
            "squash"
        );
        for method in ["merge", "rebase"] {
            let refusal = default_landing_method(&remote, "main", Some(method.to_string()))
                .expect_err("a non-squash remote default landing is refused");
            assert!(
                refusal.contains(&format!("with method `{method}`"))
                    && refusal.contains("only `squash` is available")
                    && refusal.contains(&format!("`{}` was not advanced", remote.target_branch)),
                "the refusal names the method, the one available, and that nothing moved: {refusal}"
            );
        }

        // Local-only default PR (a local fold): a normal PR defaults to squash,
        // a workspace PR is forced to merge (preserve).
        let mut local = merge_context(false, false);
        local.mr.is_local = true;
        assert_eq!(
            default_landing_method(&local, "main", None).unwrap(),
            "squash"
        );
        let mut local_ws = merge_context(true, false);
        local_ws.mr.is_local = true;
        assert_eq!(
            default_landing_method(&local_ws, "main", Some("squash".to_string())).unwrap(),
            "merge",
            "a workspace PR forces a preserve landing even when squash is requested"
        );

        let mut local_triage = merge_context(false, true);
        local_triage.mr.is_local = true;
        assert_eq!(
            default_landing_method(&local_triage, "main", None).unwrap(),
            "squash",
            "a local memory-triage PR defaults to squash like a normal PR"
        );
    }

    /// A remote PR landing on the default branch is a remote default landing;
    /// everything else (local-only, child→integration, workspace) is a local fold.
    #[test]
    fn is_remote_default_landing_only_for_remote_default_branch_pr() {
        // Base case: a real remote PR on the default branch.
        let mut ctx = merge_context(false, false);
        ctx.mr.github_pr_number = Some(7);
        assert!(is_remote_default_landing(&ctx, "main"));

        // Local-only project (no GitHub PR) is a local fold.
        let mut local = ctx.clone();
        local.mr.github_pr_number = None;
        assert!(!is_remote_default_landing(&local, "main"));
        let mut is_local = ctx.clone();
        is_local.mr.is_local = true;
        assert!(!is_remote_default_landing(&is_local, "main"));

        // Coordinator child→integration PR (target ≠ default) is a local fold.
        let mut child = ctx.clone();
        child.target_branch = "agent/cairn-1-coordinator-0".to_string();
        assert!(!is_remote_default_landing(&child, "main"));

        // Workspace PRs (keep-every-commit) are excluded; memory-triage PRs land
        // like normal PRs.
        let mut workspace = ctx.clone();
        workspace.is_workspace = true;
        assert!(!is_remote_default_landing(&workspace, "main"));
        let mut triage = ctx.clone();
        triage.has_triage_batch = true;
        assert!(is_remote_default_landing(&triage, "main"));

        // A stale stored default that disagrees with the real base is judged by
        // the PR's actual base: target "staging" against resolved "staging" is a
        // remote default landing, against a stale "main" it is not.
        let mut staging = ctx.clone();
        staging.target_branch = "staging".to_string();
        assert!(is_remote_default_landing(&staging, "staging"));
        assert!(!is_remote_default_landing(&staging, "main"));
    }

    #[test]
    fn main_checkout_dirty_gate_allows_only_regenerable_lockfile_churn() {
        use crate::services::testing::MockGitClient;

        let mut clean = MockGitClient::new();
        clean.expect_status().returning(|_| Ok(String::new()));
        assert!(assert_main_checkout_clean_for_default_merge(&clean, "/repo").is_ok());

        let mut lockfile_only = MockGitClient::new();
        lockfile_only
            .expect_status()
            .returning(|_| Ok(" M src-tauri/Cargo.lock".to_string()));
        assert!(assert_main_checkout_clean_for_default_merge(&lockfile_only, "/repo").is_ok());

        let mut real_edit = MockGitClient::new();
        real_edit
            .expect_status()
            .returning(|_| Ok(" M src-tauri/Cargo.lock\n M src/lib.rs".to_string()));
        let error = assert_main_checkout_clean_for_default_merge(&real_edit, "/repo").unwrap_err();
        assert!(error.contains("Refusing to merge"), "{error}");
        assert!(error.contains("/repo"), "{error}");
        assert!(error.contains("src/lib.rs"), "{error}");
        assert!(!error.contains("src-tauri/Cargo.lock"), "{error}");
    }

    #[test]
    fn reconciliation_is_driven_by_landing_lifecycle() {
        assert!(CheckoutReconciliation::PullRemoteLanding.pulls_remote());
        assert!(!CheckoutReconciliation::RepairLocalFold.pulls_remote());
    }

    #[test]
    fn main_checkout_reconcile_runs_only_for_default_branch_advances() {
        assert!(should_reconcile_main_checkout_after_merge("main", "main"));
        assert!(!should_reconcile_main_checkout_after_merge(
            "agent/cairn-1-coordinator-0",
            "main"
        ));
    }
}
