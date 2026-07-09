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
    // Route to the database that owns this PR's producing job (team replica or
    // private DB). Every row this resolution reads or writes — the merge_requests
    // status transition, the issue resolution, the pr action_run completion, and
    // the producing-execution advance — lives in that database. This runs AFTER
    // the merge/close already hit GitHub or the local jj fold, so reading the
    // private DB for a team job would error with “No merge request found” and
    // strand the team replica's PR (and its blocked action run) unresolved.
    let db = crate::execution::routing::routing_db_for_id(&orch.db, owner_id)
        .await
        .map_err(|e| e.to_string())?;
    let merge_context = resolve_merge_mr_context_for_job(&db, owner_id).await?;
    let mr_id = merge_context.mr.mr_id.clone();
    let now = chrono::Utc::now().timestamp();
    let closed_sessions = match resolution {
        PrNodeResolution::Merge => {
            mark_merge_request_merged_and_resolve_issue(
                &db,
                &mr_id,
                merge_context.issue_id.as_deref(),
                None,
                now,
            )
            .await?
        }
        PrNodeResolution::Close => {
            mark_merge_request_closed_and_resolve_issue(
                &db,
                &mr_id,
                merge_context.issue_id.as_deref(),
                now,
            )
            .await?
        }
    };

    // The PR is now merged/closed — quit any in-flight turn-end review suite for
    // this issue so a minutes-long run does not keep burning CPU validating a tree
    // that will never be reviewed again (CAIRN-2648).
    if let Some(issue_id) = merge_context.issue_id.as_deref() {
        crate::execution::checks_turn_end::cancel_turn_end_checks_for_issue(orch, &db, issue_id)
            .await;
    }

    if let Some(issue_id) = merge_context.issue_id.as_deref() {
        if merge_context.has_triage_batch {
            let result = match resolution {
                PrNodeResolution::Merge => {
                    crate::memories::db::resolve_triage_batch_on_merge(&db, issue_id).await
                }
                PrNodeResolution::Close => {
                    crate::memories::db::revert_triage_batch_on_close(&db, issue_id).await
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
            match crate::memories::db::discard_draft_memories_for_closed_issue(&db, issue_id).await
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
    crate::pr_data::ports::fire_pr_node_port_for_owner(&db, owner_id, port).await?;
    // A first-class `pr` action_run was `Blocked` while the PR was open;
    // resolution completes it so the execution can finish. The durable MR owner
    // is the producing job, so the action_run is found by parent_job_id. Legacy
    // `create_pr` PRs have no action_run to flip; a single-job recompute covers
    // that path. See CAIRN-1220.
    match complete_pr_action_run_if_owner(&db, owner_id, now).await? {
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
    // Route to the owning database (team replica or private DB); the PR's
    // producing job/action_run and execution live there. Best-effort: a routing
    // failure logs and no-ops, matching the swallow-errors contract below.
    let db = match crate::execution::routing::routing_db_for_id(&orch.db, mr_id).await {
        Ok(db) => db,
        Err(e) => {
            log::warn!(
                "Failed to route producing-execution lookup for mr {}: {}",
                mr_id,
                e
            );
            return Ok(());
        }
    };
    let execution_id = match db
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
        &db,
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

    /// The merge/close lifecycle transition itself must land in the owning replica.
    /// A team-prefixed producing job resolves (Close) against its injected replica
    /// — marking the team MR closed and never touching the empty private DB — the
    /// state-transition step that runs AFTER the irreversible GitHub/local work.
    #[tokio::test(flavor = "current_thread")]
    async fn resolve_pr_node_close_routes_to_owning_team_replica() {
        use crate::models::{
            ExecutionSnapshot, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
        };
        use crate::pr_data::actions::test_support::test_orchestrator;
        use crate::services::testing::MockGitClient;
        use std::collections::HashMap;
        use std::sync::Arc;

        let orch = test_orchestrator(migrated_db().await, MockGitClient::new());
        let team = Arc::new(migrated_db().await);
        orch.db.insert_team_db_for_test("team1", team.clone()).await;

        let exec_id = "team1~00000000-0000-4000-8000-0000000000e0";
        let job_id = "team1~00000000-0000-4000-8000-0000000000e1";
        let issue_id = "team1~00000000-0000-4000-8000-0000000000e2";
        let mr_id = "team1~00000000-0000-4000-8000-0000000000e3";
        let snapshot = ExecutionSnapshot::new(
            RecipeSnapshot {
                id: "recipe-1".into(),
                name: "R".into(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: Vec::new(),
                edges: Vec::new(),
            },
            HashMap::new(),
            HashMap::new(),
            TriggerContext {
                issue_id: Some(issue_id.to_string()),
                project_id: "proj-t".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        )
        .to_json()
        .unwrap();

        // Seed the full close fixture in the TEAM replica ONLY; the private DB
        // stays empty, so a resolution that read `orch.db.local` would error.
        {
            let exec_id = exec_id.to_string();
            let job_id = job_id.to_string();
            let issue_id = issue_id.to_string();
            let mr_id = mr_id.to_string();
            team.write(|conn| {
                let exec_id = exec_id.clone();
                let job_id = job_id.clone();
                let issue_id = issue_id.clone();
                let mr_id = mr_id.clone();
                let snapshot = snapshot.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                         VALUES ('proj-t', 'default', 'P', 'PROJ', '/repo', 'main', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                         VALUES (?1, 'proj-t', 1, 'Issue', 'active', 1, 1)",
                        params![issue_id.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, snapshot, started_at, seq)
                         VALUES (?1, 'recipe-1', ?2, 'proj-t', 'running', ?3, 1, 1)",
                        params![exec_id.as_str(), issue_id.as_str(), snapshot.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, branch, created_at, updated_at)
                         VALUES (?1, ?2, ?3, 'proj-t', 'complete', 'feature', 1, 1)",
                        params![job_id.as_str(), exec_id.as_str(), issue_id.as_str()],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
                         VALUES (?1, ?2, 'proj-t', ?3, 'Team PR', 'feature', 'main', 'open', 1, 1)",
                        params![mr_id.as_str(), job_id.as_str(), issue_id.as_str()],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }

        resolve_pr_node(&orch, job_id, PrNodeResolution::Close)
            .await
            .expect("a team PR resolves its close transition against the injected replica");

        // The transition landed in the TEAM replica, not the (empty) private DB.
        let team_status = team
            .query_text(
                "SELECT status FROM merge_requests WHERE id = ?1",
                params![mr_id],
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(team_status, "closed");
        let private_status = orch
            .db
            .local
            .query_opt_text(
                "SELECT status FROM merge_requests WHERE id = ?1",
                params![mr_id],
            )
            .await
            .unwrap();
        assert!(
            private_status.is_none(),
            "the private DB must never receive the team PR's row"
        );
    }
}
