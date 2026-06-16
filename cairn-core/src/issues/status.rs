use crate::issues::crud;
use crate::models::Issue;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;
use crate::sync::SyncMessage;
use crate::transitions::Resolution;
use turso::params;

pub async fn update_status(orch: &Orchestrator, id: &str, status: &str) -> Result<(), String> {
    let is_terminal_state = status == "merged" || status == "closed";

    if is_terminal_state {
        // A user marking an issue merged/closed from the UI is a deliberate
        // override (the menu confirms it before calling here). Stop the issue's
        // active work so the resolve + worktree teardown below can clean up
        // without stranding a live agent on a soon-to-be-deleted worktree,
        // rather than refusing the action while agents are running.
        stop_active_runs_for_terminal_issue(orch, id).await;
    }

    let closed_sessions = match status {
        "merged" => crud::resolve(
            &orch.db.local,
            &*orch.services.clock,
            id,
            Resolution::Merged,
        )
        .await
        .map_err(|e| e.to_string())?,
        "closed" => crud::resolve(
            &orch.db.local,
            &*orch.services.clock,
            id,
            Resolution::Closed,
        )
        .await
        .map_err(|e| e.to_string())?,
        "backlog" => {
            crud::unresolve(&orch.db.local, &*orch.services.clock, id)
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

    for session_id in &closed_sessions {
        if let Some(run_id) = orch.process_state.remove_by_session(session_id) {
            log::info!(
                "Evicted warm process {} for closed session {}",
                &run_id[..run_id.len().min(8)],
                &session_id[..session_id.len().min(8)]
            );
        }
    }

    if let Ok(Some(issue)) = crud::get(&orch.db.local, id).await {
        sync_issue(orch, &issue);
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

        tokio::spawn(async move {
            log::info!(
                "Starting background worktree teardown for issue {} (status: {})",
                issue_id,
                status_label
            );

            if let Err(error) = crate::execution::teardown::teardown_worktrees(
                &orch_clone,
                crate::execution::teardown::TeardownScope::Issue(issue_id),
            )
            .await
            {
                log::warn!("Worktree teardown for issue failed: {}", error);
            }
        });
    }

    Ok(())
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

fn sync_issue(orch: &Orchestrator, issue: &Issue) {
    orch.sync(SyncMessage::Issue(issue.into()));
}
