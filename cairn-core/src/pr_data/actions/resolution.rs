//! PR-node resolution: the merge / close lifecycle (`resolve_pr_node`), the
//! `merge_requests` status transitions, and downstream execution advancement.

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use crate::transitions::Resolution;
use cairn_db::turso::params;

use super::context::{db_error, resolve_merge_mr_context_for_job, PrNodeResolution};

async fn mark_merge_request_closed_and_resolve_issue(
    orch: &Orchestrator,
    db: &LocalDb,
    mr_id: &str,
    issue_id: Option<&str>,
    now: i64,
) -> Result<(), String> {
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
        return Ok(());
    };
    resolve_issue_for_pr(orch, db, issue_id, Resolution::Closed).await
}

/// Resolve the PR's issue through the one terminal cascade.
///
/// The PR has already been merged or closed for real by the time this runs — on
/// GitHub, or as a fold landed in the shared store — so the resolution cannot be
/// refused back out; [`StopFailure::Escalates`] says so. Everything else is the
/// same cascade the status-patch path runs: the issue's live work is stopped
/// through the canonical node stop before the rows move, so no session is closed
/// over a running turn and no batch outlives the issue that owns it.
async fn resolve_issue_for_pr(
    orch: &Orchestrator,
    db: &LocalDb,
    issue_id: &str,
    resolution: Resolution,
) -> Result<(), String> {
    crate::issues::status::resolve_terminal(
        orch,
        db,
        issue_id,
        resolution,
        crate::issues::status::StopFailure::Escalates,
    )
    .await
    .map_err(|refusal| refusal.to_string())
}

/// Resolve a `pr` node by its owner id (`merge_requests.job_id`): the producing
/// `pr` action_run (CAIRN-1220) or a legacy `create_pr` producing job.
pub async fn resolve_pr_node(
    orch: &Orchestrator,
    owner_id: &str,
    resolution: PrNodeResolution,
) -> Result<(), String> {
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
    // Marking the PR resolved resolves its issue, and resolving an issue runs
    // the terminal cascade: the issue's live work is stopped, its queued work is
    // cancelled, its sessions close, and any in-flight turn-end review suite is
    // quit (CAIRN-2648, CAIRN-3253).
    match resolution {
        PrNodeResolution::Merge => {
            mark_merge_request_merged_and_resolve_issue(
                orch,
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
                orch,
                &db,
                &mr_id,
                merge_context.issue_id.as_deref(),
                now,
            )
            .await?
        }
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
    if let Some(issue_id) = merge_context.issue_id.as_deref() {
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::issue_db_change_ids("update", issue_id, Some(&merge_context.project_id)),
        );
    }

    Ok(())
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
    orch: &Orchestrator,
    db: &LocalDb,
    mr_id: &str,
    issue_id: Option<&str>,
    merged_commit: Option<&str>,
    now: i64,
) -> Result<(), String> {
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
        return Ok(());
    };
    resolve_issue_for_pr(orch, db, issue_id, Resolution::Merged).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pr_data::actions::test_support::{
        migrated_db, seed_pr_node_merge_request_for_artifact_job, test_orchestrator,
    };
    use crate::services::testing::MockGitClient;

    /// Another job on the PR node fixture's issue. `recipe_node_id` stays NULL so
    /// it is extra work on the issue rather than a second claimant of a snapshot
    /// node.
    async fn seed_issue_job(db: &LocalDb, job_id: &str, node_name: &str, status: &str) {
        let job_id = job_id.to_string();
        let node_name = node_name.to_string();
        let status = status.to_string();
        db.execute(
            "INSERT INTO jobs (id, execution_id, node_name, status, issue_id, project_id, created_at, updated_at)
             VALUES (?1, 'exec-pr-node', ?2, ?3, 'issue-pr-node', 'proj-pr-node', 2, 2)",
            params![job_id.as_str(), node_name.as_str(), status.as_str()],
        )
        .await
        .unwrap();
    }

    /// Give an existing job the session, run, and running turn a live agent holds
    /// — the shape the canonical stop acts on.
    async fn attach_live_session(db: &LocalDb, job_id: &str) {
        let job_id = job_id.to_string();
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let session_id = format!("session-{job_id}");
                let run_id = format!("run-{job_id}");
                let turn_id = format!("turn-{job_id}");
                conn.execute(
                    "INSERT INTO sessions (id, job_id, status, created_at, updated_at)
                     VALUES (?1, ?2, 'open', 1, 1)",
                    params![session_id.as_str(), job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs (id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
                     VALUES (?1, 'proj-pr-node', ?2, NULL, 'live', ?3, 1, 1, 'resume')",
                    params![run_id.as_str(), job_id.as_str(), session_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, 1, 'running', 1, 1)",
                    params![
                        turn_id.as_str(),
                        session_id.as_str(),
                        run_id.as_str(),
                        job_id.as_str()
                    ],
                )
                .await?;
                conn.execute(
                    "UPDATE jobs SET current_session_id = ?1, current_turn_id = ?2 WHERE id = ?3",
                    params![session_id.as_str(), turn_id.as_str(), job_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn scalar(db: &LocalDb, sql: &str, id: &str) -> Option<String> {
        db.query_opt_text(sql, params![id]).await.unwrap()
    }

    async fn live_run_count(db: &LocalDb) -> i64 {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT COUNT(*) FROM runs WHERE status IN ('starting', 'live')",
                        (),
                    )
                    .await?;
                rows.next().await?.expect("count row").i64(0)
            })
        })
        .await
        .unwrap()
    }

    /// The PR merge is the door CAIRN-3241 came through: the PR merged, the issue
    /// row flipped, and the builder went on running test suites against a merged
    /// issue — because this path resolved the issue directly instead of running
    /// the cascade the confirmed-close path already had. It runs the same one now.
    #[tokio::test(flavor = "current_thread")]
    async fn merging_a_pr_stops_the_issues_live_work() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        seed_issue_job(&db, "live-builder", "builder", "running").await;
        attach_live_session(&db, "live-builder").await;
        seed_issue_job(&db, "queued-reviewer", "reviewer", "pending").await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        resolve_pr_node(&orch, "builder-job", PrNodeResolution::Merge)
            .await
            .expect("the merge resolution lands");

        assert_eq!(
            scalar(
                db,
                "SELECT status FROM issues WHERE id = ?1",
                "issue-pr-node"
            )
            .await,
            Some("merged".to_string())
        );

        // The running builder went through the canonical node stop: its turn is
        // interrupted and its run settled as a stop, not left executing.
        assert_eq!(
            scalar(
                db,
                "SELECT state FROM turns WHERE id = ?1",
                "turn-live-builder"
            )
            .await,
            Some("interrupted".to_string())
        );
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM runs WHERE id = ?1",
                "run-live-builder"
            )
            .await,
            Some("exited".to_string())
        );
        assert_eq!(
            scalar(
                db,
                "SELECT exit_reason FROM runs WHERE id = ?1",
                "run-live-builder"
            )
            .await,
            Some("user_stop".to_string())
        );

        // Work that never started is cancelled rather than left queued against a
        // merged issue.
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM jobs WHERE id = ?1",
                "queued-reviewer"
            )
            .await,
            Some("cancelled".to_string())
        );

        // And the session closes over stopped work, not running work.
        assert_eq!(
            scalar(
                db,
                "SELECT status FROM sessions WHERE id = ?1",
                "session-live-builder"
            )
            .await,
            Some("closed".to_string())
        );
        assert_eq!(
            live_run_count(db).await,
            0,
            "no run keeps executing against a merged issue"
        );
    }

    /// The half-dead state must be unrepresentable: a session that refuses to
    /// continue while its own runs keep going.
    ///
    /// The cascade's enumeration reads the `jobs` table, so a job that is not
    /// itself live work slips past it — here one already marked `complete` while
    /// still holding an open session and a running turn, which is also what a
    /// turn started between the enumeration and the close looks like. The
    /// postcondition is what closes that gap.
    #[tokio::test(flavor = "current_thread")]
    async fn a_closed_session_is_never_left_with_a_turn_in_flight() {
        let db = migrated_db().await;
        seed_pr_node_merge_request_for_artifact_job(&db).await;
        // `builder-job` is `complete`, so it is not enumerated as live work.
        attach_live_session(&db, "builder-job").await;
        let orch = test_orchestrator(db, MockGitClient::new());
        let db = &orch.db.local;

        resolve_pr_node(&orch, "builder-job", PrNodeResolution::Merge)
            .await
            .expect("the merge resolution lands");

        assert_eq!(
            scalar(
                db,
                "SELECT status FROM sessions WHERE id = ?1",
                "session-builder-job"
            )
            .await,
            Some("closed".to_string()),
            "the resolution closes the session"
        );
        assert_eq!(
            scalar(
                db,
                "SELECT state FROM turns WHERE id = ?1",
                "turn-builder-job"
            )
            .await,
            Some("interrupted".to_string()),
            "and nothing it closed is left with a turn in flight"
        );
        assert_eq!(live_run_count(db).await, 0);
    }

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
