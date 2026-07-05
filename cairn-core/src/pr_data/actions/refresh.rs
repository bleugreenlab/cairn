//! Read-side PR presentation: GitHub / local cache refresh, the live `/pr`
//! markdown section, and PR close.

use crate::execution::teardown::{teardown_worktrees, TeardownReason, TeardownScope};
use crate::github::api;
use crate::github::credentials::{get_credentials_for_owner, get_owner_repo};
use crate::models::{Check, CheckState, MergeableState, PrCache, PrState};
use crate::orchestrator::Orchestrator;
use crate::pr_data::helpers::{
    compute_checks_status, compute_local_mergeable, fetch_checks_via_api, fetch_pr_via_api,
    local_pr_files, ParsedPrDetails,
};
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
use std::path::Path;

use super::conflict::{conflict_recovery_hint, format_conflicted_commits, source_conflict_report};
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
async fn jj_conflict_mergeable_override(
    orch: &Orchestrator,
    repo_path: &str,
    mr_id: &str,
) -> Option<MergeableState> {
    let (source_branch, target_branch) = load_mr_branches(&orch.db.local, mr_id).await.ok()??;
    source_conflict_report(
        &orch.jj_binary_path,
        &orch.config_dir,
        repo_path,
        &source_branch,
        Some(&target_branch),
    )
    .filter(|report| report.tip_conflicted)
    .map(|_| MergeableState::Conflicting)
}

async fn update_merge_request_github_cache(
    db: &LocalDb,
    mr_id: &str,
    pr_details: &ParsedPrDetails,
    checks: &[Check],
    checks_status: &Option<crate::models::ChecksStatus>,
    now: i64,
) -> Result<(), String> {
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
    .map_err(|e| db_error("Failed to update merge request", e))
}

pub async fn refresh_pr_for_job(orch: &Orchestrator, job_id: &str) -> Result<PrCache, String> {
    let mr_context = resolve_mr_context_for_job(&orch.db.local, job_id).await?;
    let mr_id = mr_context.mr_id.clone();
    let pr_url = mr_context.pr_url.clone();
    let repo_path = mr_context.repo_path.clone();

    if mr_context.github_pr_number.is_none() {
        return refresh_local_pr_for_job(orch, job_id, &mr_context).await;
    }
    let pr_number = mr_context.github_pr_number.expect("checked above");

    let (owner, repo) = get_owner_repo(&repo_path)?;
    let creds = get_credentials_for_owner(&orch.db.local, &owner).await?;

    let http = &*orch.services.http;
    let mut pr_details = fetch_pr_via_api(http, &creds, &owner, &repo, pr_number).await?;
    if let Some(mergeable) = jj_conflict_mergeable_override(orch, &repo_path, &mr_id).await {
        pr_details.mergeable = mergeable;
    }
    let checks = fetch_checks_via_api(http, &creds, &owner, &repo, &pr_details.head_sha)
        .await
        .unwrap_or_default();
    let checks_status = compute_checks_status(&checks);

    let now = chrono::Utc::now().timestamp();

    let pr_cache_result = PrCache {
        id: mr_id.clone(),
        job_id: None,
        pr_number,
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
        fetched_at: now,
        updated_at: now,
        is_local: mr_context.is_local,
        source_branch: None,
        target_branch: None,
    };

    update_merge_request_github_cache(
        &orch.db.local,
        &mr_id,
        &pr_details,
        &checks,
        &checks_status,
        now,
    )
    .await?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );

    Ok(pr_cache_result)
}

async fn refresh_local_pr_for_job(
    orch: &Orchestrator,
    job_id: &str,
    mr_context: &MrContext,
) -> Result<PrCache, String> {
    let mr_id = mr_context.mr_id.clone();
    let repo_path = mr_context.repo_path.clone();
    let (title, body, status, source_branch, target_branch, additions, deletions, updated_at) = orch
        .db
        .local
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
    let local_files = if status == "open" {
        local_pr_files(git, Path::new(&repo_path), &target_branch, &source_branch)
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    // An open local PR's diff stat must track its live source branch: recompute
    // it from the fresh `git diff --numstat` on every refresh so a rebased or
    // flattened source branch self-corrects. Freezing the first value (via
    // `.or_else`/`COALESCE`) would pin a stale — possibly pre-conflict-resolution
    // — diff forever. For merged/closed PRs `local_files` is empty, so keep the
    // stored stat rather than zeroing it.
    let (additions, deletions) = if status == "open" {
        (
            Some(local_files.iter().map(|file| file.additions).sum()),
            Some(local_files.iter().map(|file| file.deletions).sum()),
        )
    } else {
        (additions, deletions)
    };
    let mergeable = if status == "open" {
        compute_local_mergeable(git, Path::new(&repo_path), &target_branch, &source_branch)
    } else {
        MergeableState::Unknown
    };
    let now = chrono::Utc::now().timestamp();
    let mergeable_str = mergeable.to_string();
    orch.db
        .local
        .write(|conn| {
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

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );

    Ok(PrCache {
        id: mr_id,
        job_id: Some(job_id.to_string()),
        pr_number: 0,
        pr_url: String::new(),
        title,
        body,
        state: match status.as_str() {
            "merged" => PrState::Merged,
            "closed" => PrState::Closed,
            _ => PrState::Open,
        },
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
        source_branch: Some(source_branch),
        target_branch: Some(target_branch),
    })
}

/// Close a PR without merging, mark the `merge_requests` row closed, and tear
/// down the issue's worktrees.
pub async fn close_pr_for_job(orch: &Orchestrator, job_id: &str) -> Result<String, String> {
    let mr_context = resolve_mr_context_for_job(&orch.db.local, job_id).await?;
    let mr_id = mr_context.mr_id.clone();
    let repo_path = mr_context.repo_path.clone();

    if let Some(pr_number) = mr_context.github_pr_number {
        let (owner, repo) = get_owner_repo(&repo_path)?;
        let creds = get_credentials_for_owner(&orch.db.local, &owner).await?;

        let http = &*orch.services.http;
        api::close_pr(http, &creds, &owner, &repo, pr_number).await?;
    }

    let closed_sessions = resolve_pr_node(orch, job_id, PrNodeResolution::Close).await?;
    for session_id in &closed_sessions {
        orch.process_state.remove_by_session(session_id);
    }

    // Tear down worktrees and branches for the issue (issue-wide, unconditional).
    if let Some(issue_id) = load_mr_issue_id(&orch.db.local, &mr_id).await? {
        let orch_inner = orch.clone();
        tokio::spawn(async move {
            if let Err(e) = teardown_worktrees(
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
/// Fetching live data refreshes the cached row as a side effect
/// (refresh-on-read). When the PR is open, an `## actions` block advertising
/// merge/close/refresh is appended. `artifact_uri` is the `/pr` URI used in the
/// action examples; `diff_full` inlines the full patch text per file.
pub async fn render_live_pr_section(
    orch: &Orchestrator,
    job_id: &str,
    artifact_uri: &str,
    diff_full: bool,
) -> Option<String> {
    let mr_context = match try_resolve_mr_context_for_job(&orch.db.local, job_id).await {
        Ok(Some(ctx)) => ctx,
        Ok(None) => return None,
        Err(e) => return Some(format!("## Pull Request\n\n(failed to resolve PR: {e})\n")),
    };
    if mr_context.github_pr_number.is_none() {
        let cache = match refresh_local_pr_for_job(orch, job_id, &mr_context).await {
            Ok(cache) => cache,
            Err(e) => {
                return Some(format!(
                    "## Local PR\n\n(failed to refresh local PR: {e})\n"
                ))
            }
        };
        let mut out = format!(
            "## Local PR\n\n{}\nState: {}\nMergeable: {}\n",
            cache.title.as_deref().unwrap_or("Untitled"),
            cache.state,
            cache.mergeable
        );
        if let (Some(additions), Some(deletions)) = (cache.additions, cache.deletions) {
            out.push_str(&format!("Changes: +{} -{}\n", additions, deletions));
        }
        if let Some(body) = cache.body.as_deref().filter(|b| !b.is_empty()) {
            out.push_str("\n### Description\n\n");
            out.push_str(body);
            out.push('\n');
        }
        if matches!(cache.state, PrState::Open) {
            out.push_str(&format!(
                "\n## actions\n- [merge]({uri}): patch with action:\"merge\" (optional method, default squash).\n- [close]({uri}): patch with action:\"close\".\n- [refresh]({uri}): patch with action:\"refresh\".",
                uri = artifact_uri
            ));
        }
        return Some(out);
    }

    let pr_number = mr_context.github_pr_number.expect("checked above");
    let header = format!(
        "## Pull Request\n\nPR #{}: {}\n",
        pr_number, mr_context.pr_url
    );

    let (owner, repo) = match get_owner_repo(&mr_context.repo_path) {
        Ok(v) => v,
        Err(e) => return Some(format!("{header}\n(failed to resolve repo: {e})\n")),
    };
    let creds = match get_credentials_for_owner(&orch.db.local, &owner).await {
        Ok(c) => c,
        Err(e) => return Some(format!("{header}\n(failed to resolve credentials: {e})\n")),
    };

    let http = &*orch.services.http;
    let mut pr_details = match fetch_pr_via_api(http, &creds, &owner, &repo, pr_number).await {
        Ok(d) => d,
        Err(e) => return Some(format!("{header}\n(failed to fetch live PR: {e})\n")),
    };
    // Same jj conflict gate as the cache refresh: a jj-conflicted source bookmark
    // is rendered (and re-cached) as Conflicting, not GitHub's false mergeable.
    // Keep the full report to enumerate the offending commits/files below so the
    // live artifact and the node summary tell one consistent story.
    let source_branches = load_mr_branches(&orch.db.local, &mr_context.mr_id)
        .await
        .ok()
        .flatten();
    let source_conflict = source_branches.as_ref().and_then(|(src, tgt)| {
        source_conflict_report(
            &orch.jj_binary_path,
            &orch.config_dir,
            &mr_context.repo_path,
            src,
            Some(tgt),
        )
    });
    if source_conflict.as_ref().is_some_and(|r| r.tip_conflicted) {
        pr_details.mergeable = MergeableState::Conflicting;
    }
    let checks = fetch_checks_via_api(http, &creds, &owner, &repo, &pr_details.head_sha)
        .await
        .unwrap_or_default();
    let checks_status = compute_checks_status(&checks);
    let files = api::fetch_pr_files(http, &creds, &owner, &repo, pr_number)
        .await
        .unwrap_or_default();

    // Refresh-on-read: persist freshly fetched details to the cache.
    let now = chrono::Utc::now().timestamp();
    if let Err(e) = update_merge_request_github_cache(
        &orch.db.local,
        &mr_context.mr_id,
        &pr_details,
        &checks,
        &checks_status,
        now,
    )
    .await
    {
        log::warn!("Failed to refresh PR cache on read: {}", e);
    } else {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "merge_requests", "action": "update"}),
        );
    }

    let mut out = header;
    out.push_str(&format!(
        "State: {}{}\n",
        pr_details.state,
        if pr_details.is_draft { " (draft)" } else { "" }
    ));
    if let Some(review) = &pr_details.review_decision {
        out.push_str(&format!("Review: {}\n", review));
    }
    out.push_str(&format!("Mergeable: {}\n", pr_details.mergeable));
    if let Some(status) = &checks_status {
        out.push_str(&format!("Checks: {}\n", status));
    }
    if source_conflict.as_ref().is_some_and(|r| r.tip_conflicted) {
        // A conflicted TIP inflates the diff GitHub reports; flag it so the
        // number can't read as a clean, mergeable change.
        out.push_str(&format!(
            "Changes: +{} -{} (stale — branch tip carries conflicts; resolve before trusting)\n",
            pr_details.additions.unwrap_or(0),
            pr_details.deletions.unwrap_or(0)
        ));
    } else {
        out.push_str(&format!(
            "Changes: +{} -{}\n",
            pr_details.additions.unwrap_or(0),
            pr_details.deletions.unwrap_or(0)
        ));
    }
    if let (Some(report), Some((src, tgt))) = (&source_conflict, &source_branches) {
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

    if let Some(body) = pr_details.body.as_deref().filter(|b| !b.is_empty()) {
        out.push_str("\n### Description\n\n");
        out.push_str(body);
        out.push('\n');
    }

    if !checks.is_empty() {
        out.push_str("\n### Checks\n\n");
        for c in &checks {
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
        for f in &files {
            out.push_str(&format!(
                "- {} (+{} -{}) {}\n",
                f.filename, f.additions, f.deletions, f.status
            ));
        }
    }

    if diff_full {
        out.push_str("\n### Diff\n\n");
        for f in &files {
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
    if matches!(pr_details.state, PrState::Open) {
        out.push_str(&format!(
            "\n## actions\n- [merge]({uri}): patch with action:\"merge\" (optional method, default squash). e.g. write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"merge\",method:\"squash\"}}}}]}})\n- [close]({uri}): patch with action:\"close\". e.g. write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"close\"}}}}]}})\n- [refresh]({uri}): patch with action:\"refresh\" to re-fetch live PR state. e.g. write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{action:\"refresh\"}}}}]}})",
            uri = artifact_uri
        ));
    }

    Some(out)
}
