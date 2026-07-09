use crate::issues::crud;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;
use crate::transitions::Resolution;
use cairn_db::turso::params;

/// Who is driving a status resolution. Gates the fail-closed checks: a person
/// acting through the UI is a deliberate override (the menu confirms first), so
/// `User` skips them; an agent/recipe writing through MCP is held to them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionActor {
    /// A person acting through the UI — stop active work and resolve, no refusals.
    User,
    /// An agent/recipe via the MCP write path — refuse a `merged` resolution
    /// while a PR is open, and refuse any terminal resolution that would strand
    /// active dependent work (CAIRN-2287).
    Agent,
}

pub async fn update_status(
    orch: &Orchestrator,
    id: &str,
    status: &str,
    actor: ResolutionActor,
) -> Result<(), String> {
    let is_terminal_state = status == "merged" || status == "closed";

    // Agent/recipe-initiated terminal resolutions fail closed. The user UI path
    // is a deliberate override and skips these guards.
    if is_terminal_state && matches!(actor, ResolutionActor::Agent) {
        // `merged` while a PR is still OPEN would record a resolution WITHOUT
        // merging — stranding the branch's commits and auto-closing the PR
        // unmerged. Refuse and name the real merge lever (CAIRN-2287). Merging
        // through the PR resolves the issue itself, so an agent never needs the
        // status patch to merge.
        if status == "merged" {
            if let Some((project_key, number)) = open_merge_request_for_issue(orch, id).await? {
                return Err(format!(
                    "Refusing to mark {project_key}-{number} merged: it still has an OPEN pull request. Setting status=merged records a resolution WITHOUT merging the PR — it strands the branch's commits and auto-closes the PR unmerged. Merge through the PR instead:\n  write({{changes:[{{target:\"cairn://p/{project_key}/{number}/1/builder/create-pr\",mode:\"patch\",payload:{{action:\"merge\"}}}}]}})\nThat merge resolves this issue for you. If the PR was already merged externally, refresh it instead with payload:{{action:\"refresh\"}}."
                ));
            }
        }
        // Never resolve an issue out from under its own still-running work (e.g. a
        // reviewer on a child a coordinator is about to merge).
        let blockers = terminal_resolution_blockers(orch, id).await?;
        if !blockers.is_empty() {
            return Err(format!(
                "Refusing to mark issue {status} while it still has {}; finish or stop the running work first (or resolve it from the UI, a deliberate override).",
                blockers.join(", ")
            ));
        }
    }

    if is_terminal_state {
        // A user marking an issue merged/closed from the UI is a deliberate
        // override (the menu confirms it before calling here). Stop the issue's
        // active work so the resolve + worktree teardown below can clean up
        // without stranding a live agent on a soon-to-be-deleted worktree,
        // rather than refusing the action while agents are running.
        // content→execution boundary (CAIRN-2181): stop_active_runs queries
        // runs/jobs, which stay private until CAIRN-2182.
        stop_active_runs_for_terminal_issue(orch, id).await;
    }

    // The issue row lives in its owning database — the private DB for a local
    // project, the team replica for a team project (CAIRN-2181). resolve/unresolve
    // mutate that row (and close sessions that, for a team project, are empty in
    // the replica), so they must target the same DB the row is read from.
    let owning_db = crud::owning_db_for_issue(&orch.db, id)
        .await
        .map_err(|e| e.to_string())?;

    let closed_sessions = match status {
        "merged" => crud::resolve(&owning_db, &*orch.services.clock, id, Resolution::Merged)
            .await
            .map_err(|e| e.to_string())?,
        "closed" => crud::resolve(&owning_db, &*orch.services.clock, id, Resolution::Closed)
            .await
            .map_err(|e| e.to_string())?,
        "backlog" => {
            crud::unresolve(&owning_db, &*orch.services.clock, id)
                .await
                .map_err(|e| e.to_string())?;
            Vec::new()
        }
        _ => {
            return Err(format!(
                "Cannot manually set status to '{}' because it is derived from execution state",
                status
            ))
        }
    };

    if is_terminal_state {
        crate::execution::advancement::release_dependent_executions(orch, id).await?;
    }

    if is_terminal_state {
        // The issue is resolved — quit any in-flight turn-end review suite so it
        // does not keep running against a merged/closed issue (CAIRN-2648).
        crate::execution::checks_turn_end::cancel_turn_end_checks_for_issue(orch, &owning_db, id)
            .await;
    }

    for session_id in &closed_sessions {
        if let Some(run_id) = orch.process_state.remove_by_session(session_id) {
            log::info!(
                "Evicted warm process {} for closed session {}",
                &run_id[..run_id.len().min(8)],
                &session_id[..session_id.len().min(8)]
            );
        }
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    );

    orch.wake_for_issue(id).await;

    if is_terminal_state {
        let orch_clone = orch.clone();
        let issue_id = id.to_string();
        let status_label = status.to_string();
        // A merged resolution recorded here did NOT run a fold (that is the PR
        // action's job), so teardown must preserve any branch whose commits have
        // not landed — the KMCP data-loss guard (CAIRN-2287). A closed resolution
        // is an explicit discard and deletes branches as before.
        let reason = if status == "merged" {
            crate::execution::teardown::TeardownReason::Merged
        } else {
            crate::execution::teardown::TeardownReason::Discarded
        };

        tokio::spawn(async move {
            log::info!(
                "Starting background worktree teardown for issue {} (status: {})",
                issue_id,
                status_label
            );

            if let Err(error) = crate::execution::teardown::teardown_worktrees(
                &orch_clone,
                crate::execution::teardown::TeardownScope::Issue(issue_id),
                reason,
            )
            .await
            {
                log::warn!("Worktree teardown for issue failed: {}", error);
            }
        });
    }

    Ok(())
}

/// The `(project_key, issue_number)` of an issue's OPEN merge request, or `None`
/// when it has no unresolved PR. Used to refuse an agent `status:"merged"` and
/// point it at the real merge lever. Reads `orch.db.local`, where all
/// `merge_requests` access lives.
async fn open_merge_request_for_issue(
    orch: &Orchestrator,
    issue_id: &str,
) -> Result<Option<(String, i32)>, String> {
    let issue_id = issue_id.to_string();
    orch.db
        .local
        .read(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.key, i.number
                         FROM merge_requests mr
                         JOIN issues i ON mr.issue_id = i.id
                         JOIN projects p ON i.project_id = p.id
                         WHERE mr.issue_id = ?1
                           AND mr.status NOT IN ('merged', 'closed')
                         LIMIT 1",
                        params![issue_id.as_str()],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok(Some((row.text(0)?, row.i64(1)? as i32))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| format!("Failed to check for an open merge request: {e}"))
}

/// Stop every live agent run owned by an issue heading into a terminal state.
///
/// Covers the issue's own jobs and any child jobs based on this issue's branch
/// (the same scope the old guard refused on). `stop_session` interrupts the
/// run's turn and cascades to child runs, so this drains the coordinator and its
/// reviewers before the resolve + worktree teardown removes their worktrees and
/// branches. Best-effort: a failed stop logs a warning and never blocks the
/// terminal transition the user asked for.
async fn stop_active_runs_for_terminal_issue(orch: &Orchestrator, issue_id: &str) {
    let issue_id_owned = issue_id.to_string();
    let active_runs = orch
        .db
        .local
        .read(|conn| {
            let issue_id = issue_id_owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT r.id
                         FROM runs r
                         JOIN jobs j ON r.job_id = j.id
                         WHERE r.status IN ('starting', 'live')
                           AND (
                               j.issue_id = ?1
                               OR (
                                   j.issue_id IS NOT NULL
                                   AND j.issue_id <> ?1
                                   AND j.base_branch IN (
                                       SELECT DISTINCT parent.branch
                                       FROM jobs parent
                                       WHERE parent.issue_id = ?1
                                         AND parent.branch IS NOT NULL
                                   )
                               )
                           )",
                        params![issue_id.as_str()],
                    )
                    .await?;
                let mut run_ids = Vec::new();
                while let Some(row) = rows.next().await? {
                    run_ids.push(row.text(0)?);
                }
                Ok(run_ids)
            })
        })
        .await;

    let active_runs = match active_runs {
        Ok(run_ids) => run_ids,
        Err(e) => {
            log::warn!(
                "Failed to enumerate active runs for terminal issue {issue_id}: {e}; \
                 proceeding with teardown"
            );
            return;
        }
    };

    for run_id in &active_runs {
        if let Err(e) = crate::orchestrator::lifecycle::stop_session(orch, run_id) {
            log::warn!("Failed to stop run {run_id} for terminal issue {issue_id}: {e}");
        }
    }
}

/// Active work that should block an *automated* (agent/recipe-initiated)
/// terminal resolution of `issue_id`: the issue's own non-terminal jobs and
/// child jobs based on its branch (reviewers, follow-ups). Returns
/// human-readable blocker descriptions; an empty vec means the issue is safe to
/// resolve automatically.
///
/// This is deliberately NOT consulted by the user-driven UI path
/// (`update_status`): a person marking an issue terminal is an explicit
/// override that stops the active work instead. It guards the recipe merge/close
/// actions so a coordinator does not resolve a child issue out from under a
/// reviewer that is still running. Recipe action nodes are not job rows and the
/// implementation job is `complete` by merge time, so the action performing the
/// resolution never counts itself here.
pub async fn terminal_resolution_blockers(
    orch: &Orchestrator,
    issue_id: &str,
) -> Result<Vec<String>, String> {
    let issue_id = issue_id.to_string();
    let (active_jobs, active_child_jobs) = orch
        .db
        .local
        .read(|conn| {
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*)
                         FROM jobs
                         WHERE issue_id = ?1 AND status NOT IN ('complete', 'failed')",
                        params![issue_id.as_str()],
                    )
                    .await?;
                let active_jobs = rows
                    .next()
                    .await?
                    .map(|row| row.i64(0))
                    .transpose()?
                    .unwrap_or(0);

                let mut rows = conn
                    .query(
                        "SELECT COUNT(*)
                         FROM jobs child
                         WHERE child.issue_id IS NOT NULL
                           AND child.issue_id <> ?1
                           AND child.status NOT IN ('complete', 'failed')
                           AND child.base_branch IN (
                               SELECT DISTINCT parent.branch
                               FROM jobs parent
                               WHERE parent.issue_id = ?1 AND parent.branch IS NOT NULL
                           )",
                        params![issue_id.as_str()],
                    )
                    .await?;
                let active_child_jobs = rows
                    .next()
                    .await?
                    .map(|row| row.i64(0))
                    .transpose()?
                    .unwrap_or(0);

                Ok((active_jobs, active_child_jobs))
            })
        })
        .await
        .map_err(|e| format!("Failed to check active work for issue: {e}"))?;

    let mut blockers = Vec::new();
    if active_jobs > 0 {
        blockers.push(format!("{active_jobs} pending/running/blocked job(s)"));
    }
    if active_child_jobs > 0 {
        blockers.push(format!(
            "{active_child_jobs} active child job(s) based on this issue's branch"
        ));
    }
    Ok(blockers)
}
