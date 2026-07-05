//! PR-node resolution: the merge / close lifecycle (`resolve_pr_node`), the
//! `merge_requests` status transitions, and downstream execution advancement.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use crate::transitions::Resolution;
use cairn_db::turso::params;

use super::context::{db_error, resolve_merge_mr_context_for_job, PrNodeResolution};

async fn mark_merge_request_closed_and_resolve_issue(
    db: &LocalDb,
    mr_id: &str,
    issue_id: Option<&str>,
    now: i64,
) -> Result<Vec<String>, String> {
    let mr_id = mr_id.to_string();
    db.write(|conn| {
        let mr_id = mr_id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests
                 SET status = 'closed', closed_at = ?1, updated_at = ?1
                 WHERE id = ?2",
                params![now, mr_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to update merge request", e))?;

    let Some(issue_id) = issue_id else {
        return Ok(Vec::new());
    };
    crate::issues::crud::resolve(
        db,
        &crate::services::RealClock,
        issue_id,
        Resolution::Closed,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Resolve a `pr` node by its owner id (`merge_requests.job_id`): the producing
/// `pr` action_run (CAIRN-1220) or a legacy `create_pr` producing job.
pub async fn resolve_pr_node(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
) -> Result<Vec<String>, String> {
    let merge_context = resolve_merge_mr_context_for_job(&orch.db.local, owner_id).await?;
    let mr_id = merge_context.mr.mr_id.clone();
    let now = chrono::Utc::now().timestamp();
    let closed_sessions = match resolution {
        PrNodeResolution::Merge => {
            mark_merge_request_merged_and_resolve_issue(
                &orch.db.local,
                &mr_id,
                merge_context.issue_id.as_deref(),
                None,
                now,
            )
            .await?
        }
        PrNodeResolution::Close => {
            mark_merge_request_closed_and_resolve_issue(
                &orch.db.local,
                &mr_id,
                merge_context.issue_id.as_deref(),
                now,
            )
            .await?
        }
    };

    if let Some(issue_id) = merge_context.issue_id.as_deref() {
        if merge_context.has_triage_batch {
            let result = match resolution {
                PrNodeResolution::Merge => {
                    crate::memories::db::resolve_triage_batch_on_merge(&orch.db.local, issue_id)
                        .await
                }
                PrNodeResolution::Close => {
                    crate::memories::db::revert_triage_batch_on_close(&orch.db.local, issue_id)
                        .await
                }
            };
            match result {
                Ok(ids) if !ids.is_empty() => log::info!(
                    "Resolved {} canon memory triage row(s) for issue {} via {:?}",
                    ids.len(),
                    issue_id,
                    resolution
                ),
                Ok(_) => {}
                Err(error) => log::warn!(
                    "Memory triage {:?} reconciliation failed for issue {}: {}",
                    resolution,
                    issue_id,
                    error
                ),
            }
        }

        if matches!(resolution, PrNodeResolution::Close) {
            match crate::memories::db::discard_draft_memories_for_closed_issue(
                &orch.db.local,
                issue_id,
            )
            .await
            {
                Ok(ids) if !ids.is_empty() => log::info!(
                    "Discarded {} draft memory row(s) for closed issue {}",
                    ids.len(),
                    issue_id
                ),
                Ok(_) => {}
                Err(error) => log::warn!(
                    "Failed to discard draft memories for closed issue {}: {}",
                    issue_id,
                    error
                ),
            }
        }
    }

    if let Some(issue_id) = merge_context.issue_id.as_deref() {
        crate::execution::advancement::release_dependent_executions(orch, issue_id).await?;
    }

    let port = match resolution {
        PrNodeResolution::Merge => "merge",
        PrNodeResolution::Close => "close",
    };
    crate::pr_data::ports::fire_pr_node_port_for_owner(&orch.db.local, owner_id, port).await?;
    // A first-class `pr` action_run was `Blocked` while the PR was open;
    // resolution completes it so the execution can finish. The durable MR owner
    // is the producing job, so the action_run is found by parent_job_id. Legacy
    // `create_pr` PRs have no action_run to flip; a single-job recompute covers
    // that path. See CAIRN-1220.
    match complete_pr_action_run_if_owner(&orch.db.local, owner_id, now).await? {
        Some(execution_id) => {
            crate::execution::advancement::recompute_execution_jobs(orch, &execution_id)?
        }
        None => crate::execution::advancement::recompute_job(orch, owner_id)?,
    }
    if let Err(e) = advance_producing_execution_after_pr_resolution(orch, &mr_id).await {
        log::warn!(
            "Failed to advance producing execution after PR resolution: {}",
            e
        );
    }

    if matches!(resolution, PrNodeResolution::Merge) {
        if let Some(issue_id) = merge_context.issue_id.clone() {
            let orch_for_memories = orch.clone();
            tokio::spawn(async move {
                let started = std::time::Instant::now();
                match crate::memories::commands::confirm_and_spawn_drafts_for_merged_issue(
                    orch_for_memories,
                    &issue_id,
                )
                .await
                {
                    Ok(spawned) if !spawned.is_empty() => log::info!(
                        "Confirmed draft memories for merged issue {} and spawned {} triage issue(s) in {:?}",
                        issue_id,
                        spawned.len(),
                        started.elapsed()
                    ),
                    Ok(_) => log::info!(
                        "Confirmed draft memories for merged issue {} with no triage issue spawned in {:?}",
                        issue_id,
                        started.elapsed()
                    ),
                    Err(error) => log::warn!(
                        "Failed to confirm draft memories for merged issue {}: {}",
                        issue_id,
                        error
                    ),
                }
            });
        }

        // Downstream sibling/coordinator reconciliation advances *other* in-flight
        // branches (siblings auto-rebase onto the advanced tip; a Coordinator on
        // the integration branch has its `@` re-parented). It has no bearing on
        // whether THIS PR is merged, is best-effort end to end, and is re-fired
        // idempotently by the GitHub `push` webhook (guarded by the before/after
        // commit-id snapshot in `reconcile_base_advance`). So spawn it off the
        // synchronous merge path: the resolution — and the "merging" button —
        // returns immediately, and the reconcile lands in the background.
        let orch = orch.clone();
        let owner_id = owner_id.to_string();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            if let Err(e) = crate::orchestrator::base_advance::notify_downstream_of_base_advance(
                &orch, &owner_id,
            )
            .await
            {
                log::warn!("Failed to notify downstream jobs of base advance: {}", e);
            }
            log::info!(
                "Deferred downstream base-advance reconcile for owner {} took {:?}",
                owner_id,
                started.elapsed()
            );
        });
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "merge_requests", "action": "update"}),
    );
    if merge_context.issue_id.is_some() {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "issues", "action": "update"}),
        );
    }

    Ok(closed_sessions)
}

/// If `owner_id` is a producing job with a blocked `pr` action_run child, mark
/// that action_run `complete` and return its execution_id. Also accepts an
/// action_run id for unrepaired historical rows. Returns `None` for legacy
/// `create_pr` jobs with no first-class PR action.
async fn complete_pr_action_run_if_owner(
    db: &LocalDb,
    owner_id: &str,
    now: i64,
) -> Result<Option<String>, String> {
    let owner_id = owner_id.to_string();
    db.write(|conn| {
        let owner_id = owner_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, execution_id
                     FROM (
                         SELECT id, execution_id, 0 AS priority
                         FROM action_runs
                         WHERE parent_job_id = ?1
                         UNION ALL
                         SELECT id, execution_id, 1 AS priority
                         FROM action_runs
                         WHERE id = ?1
                     )
                     ORDER BY priority
                     LIMIT 1",
                    params![owner_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let action_run_id = row.text(0)?;
            let execution_id = row.text(1)?;
            drop(rows);
            conn.execute(
                "UPDATE action_runs SET status = 'complete', completed_at = ?1 WHERE id = ?2",
                params![now, action_run_id.as_str()],
            )
            .await?;
            Ok(Some(execution_id))
        })
    })
    .await
    .map_err(|e| db_error("Failed to complete PR action_run", e))
}

/// After a `merge_requests` row transitions to merged or closed, enqueue an
/// `AdvanceDag` for the execution that produced the PR so any downstream node
/// gated on PR-merged-ness wakes. No-op when the row has no linked job or the
/// job has no `execution_id` (manually-attached or non-recipe PRs).
///
/// Idempotency: callers may invoke this more than once for the same `mr_id`
/// without harm. Each call enqueues a fresh outbox entry and sends a follow-on
/// `AdvanceDag`; `reduce_dag` is idempotent, so a duplicate advancement is
/// cheap and strictly safer than dropping one.
///
/// Errors are swallowed at the call sites — webhook handlers must not return
/// errors that GitHub will retry, and the durable outbox row is the recovery
/// mechanism if the in-memory send is lost.
pub async fn advance_producing_execution_after_pr_resolution(
    orch: &Orchestrator,
    mr_id: &str,
) -> Result<(), String> {
    let mr_id_owned = mr_id.to_string();
    let execution_id = match orch
        .db
        .local
        .query_opt_text(
            // The PR's owner (`merge_requests.job_id`) is either a producing
            // job (legacy `create_pr`) or a producing `pr` action_run
            // (CAIRN-1220); resolve the execution from whichever it is.
            "SELECT COALESCE(j.execution_id, ar.execution_id)
             FROM merge_requests mr
             LEFT JOIN jobs j ON mr.job_id = j.id
             LEFT JOIN action_runs ar ON mr.job_id = ar.id
             WHERE mr.id = ?1
             LIMIT 1",
            params![mr_id_owned.as_str()],
        )
        .await
    {
        Ok(value) => value,
        Err(e) => {
            log::warn!(
                "Failed to look up producing execution for mr {}: {}",
                mr_id,
                e
            );
            return Ok(());
        }
    };

    let Some(execution_id) = execution_id else {
        log::debug!(
            "No producing execution for mr {} — skipping DAG advance",
            mr_id
        );
        return Ok(());
    };

    match crate::effects::outbox::insert_pending_with_payload_async(
        &orch.db.local,
        "advance_dag",
        &execution_id,
        "{}",
    )
    .await
    {
        Ok(entry_id) => {
            if let Some(ref tx) = orch.effect_tx {
                let _ = tx.send(crate::effects::types::WorkflowEffect::AdvanceDag {
                    execution_id: execution_id.clone(),
                    outbox_entry_id: Some(entry_id),
                });
            } else {
                log::debug!(
                    "No effect_tx configured — relying on outbox replay for advance_dag of execution {}",
                    &execution_id[..execution_id.len().min(8)]
                );
            }
        }
        Err(e) => log::warn!(
            "Failed to enqueue advance_dag outbox entry for execution {}: {}",
            execution_id,
            e
        ),
    }

    Ok(())
}

pub(super) async fn persist_merged_commit(
    db: &LocalDb,
    mr_id: &str,
    merged_commit: &str,
) -> Result<(), String> {
    let mr_id = mr_id.to_string();
    let merged_commit = merged_commit.to_string();
    db.write(|conn| {
        let mr_id = mr_id.clone();
        let merged_commit = merged_commit.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests SET merged_commit = ?1 WHERE id = ?2",
                params![merged_commit.as_str(), mr_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to persist merged commit", e))
}

async fn mark_merge_request_merged_and_resolve_issue(
    db: &LocalDb,
    mr_id: &str,
    issue_id: Option<&str>,
    merged_commit: Option<&str>,
    now: i64,
) -> Result<Vec<String>, String> {
    let mr_id = mr_id.to_string();
    let _source_branch = db
        .read(|conn| {
            let mr_id = mr_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT source_branch FROM merge_requests WHERE id = ?1 LIMIT 1",
                        params![mr_id.as_str()],
                    )
                    .await?;
                crate::storage::next_text(&mut rows, 0).await
            })
        })
        .await
        .map_err(|e| db_error("Failed to query merge request source branch", e))?;

    db.write(|conn| {
        let mr_id = mr_id.clone();
        let merged_commit = merged_commit.map(ToOwned::to_owned);
        Box::pin(async move {
            conn.execute(
                "UPDATE merge_requests
                 SET status = 'merged', merged_at = ?1, updated_at = ?1,
                     merged_commit = COALESCE(?2, merged_commit)
                 WHERE id = ?3",
                params![now, merged_commit.as_deref(), mr_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| db_error("Failed to update merge request", e))?;

    let Some(issue_id) = issue_id else {
        return Ok(Vec::new());
    };
    crate::issues::crud::resolve(
        db,
        &crate::services::RealClock,
        issue_id,
        Resolution::Merged,
    )
    .await
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pr_data::actions::test_support::{
        migrated_db, seed_pr_node_merge_request_for_artifact_job,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn pr_resolution_completes_pr_action_run_for_producing_job_owner() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;

        let execution_id = complete_pr_action_run_if_owner(&db, "builder-job", 42)
            .await
            .unwrap()
            .expect("builder job resolves its pr action_run child");

        assert_eq!(execution_id, "exec-pr-node");
        let status = db
            .query_text(
                "SELECT status FROM action_runs WHERE id = 'pr-action-run'",
                (),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, "complete");
    }
}
