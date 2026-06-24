use super::*;

/// Advance execution DAG and handle action/condition/checkpoint nodes.
///
/// Returns agent jobs that are ready to start. All operations flow through
/// the typed effects system: `reduce_dag` produces effects, `execute_effects`
/// processes them through the core effect loop. Actions, checkpoints,
/// conditions, and worktree creation are all handled inline by the loop.
///
/// This is the canonical entry point for DAG advancement. All callers —
/// event listeners, direct async calls, scheduler — go through this function,
/// which means all paths use `reduce_dag` as the single source of truth.
pub async fn advance_execution_with_actions(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Vec<Job>, String> {
    use crate::effects::dag::reduce_dag;
    use crate::effects::run::execute_effects;
    use crate::effects::types::WorkflowEffect;

    // Step 1: Synchronous DAG reduction — produces typed effects
    let (mut agent_jobs, effects) = reduce_dag(orch, execution_id)?;

    // Step 2: Separate StartAgentJobs from other effects (we accumulate them)
    let mut remaining_effects = Vec::new();
    for effect in effects {
        match effect {
            WorkflowEffect::StartAgentJobs(jobs) => {
                agent_jobs.extend(jobs);
            }
            other => remaining_effects.push(other),
        }
    }

    // Step 3: Execute remaining effects through the core effect loop.
    // Cascading StartAgentJobs are handled by the executor directly.
    if !remaining_effects.is_empty() {
        let report = execute_effects(orch, remaining_effects).await?;

        log::debug!(
            "Effect loop completed: {} effects executed for execution {}",
            report.effects_executed,
            &execution_id[..execution_id.len().min(8)]
        );
    }

    Ok(agent_jobs)
}
pub fn block_job(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    // Ensure the blockable job has an (unconfirmed) checkpoint artifact so the
    // confirmed gate has a row to flip, then let the projection derive Blocked.
    let db = orch.db.local.clone();
    let job_id_owned = job_id.to_string();
    run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move {
                crate::execution::checkpoints::ensure_checkpoint_artifact_conn(conn, &job_id).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;
    recompute_job(orch, job_id)
}

pub fn mark_job_failed(orch: &Orchestrator, job_id: &str, error: &str) -> Result<(), String> {
    // Record the failure as a turn fact; the projection derives Failed and cascades.
    let fail_db = orch.db.local.clone();
    let fail_job_id = job_id.to_string();
    run_advancement_db(async move {
        fail_db
            .write(|conn| {
                let job_id = fail_job_id.clone();
                Box::pin(async move { force_fail_job_turn_conn(conn, &job_id).await })
            })
            .await
            .map_err(|e| e.to_string())
    })?;

    let db = orch.db.local.clone();
    let job_id_owned = job_id.to_string();
    let run_id = run_advancement_db(async move {
        db.query_text(
            "SELECT id
             FROM runs
             WHERE job_id = ?1
             ORDER BY created_at DESC
             LIMIT 1",
            (job_id_owned,),
        )
        .await
        .map_err(|e| format!("Failed to load latest run: {e}"))
    })?;

    log::error!("Job {} failed: {}", job_id, error);

    if let Some(run_id) = run_id {
        crate::orchestrator::session::insert_error_event(orch, &run_id, None, error);
    }

    recompute_job(orch, job_id)?;
    Ok(())
}

pub fn create_action_run(
    orch: &Orchestrator,
    execution_id: &str,
    node: &DbRecipeNode,
) -> Result<String, String> {
    let node_config: crate::models::ActionNodeConfig = node
        .config
        .as_ref()
        .and_then(|c| serde_json::from_str(c).ok())
        .ok_or("Action node has no valid config")?;
    // A `pr` node carries no `action` string (its config is just the inherited
    // input schema); its action vehicle is the fixed `builtin:pr` handler, not a
    // row in `action_configs`. See CAIRN-1220.
    let action_config_id = node_config.action_config_id.unwrap_or_else(|| {
        if node.node_type == "pr" {
            "builtin:pr".to_string()
        } else {
            format!("builtin:{}", node_config.action)
        }
    });

    let now = chrono::Utc::now().timestamp() as i32;
    let action_run_id = Uuid::new_v4().to_string();
    let db = orch.db.local.clone();
    let execution_id_owned = execution_id.to_string();
    let node_id = node.id.clone();
    let node_name = node.name.clone();
    let action_run_id_for_db = action_run_id.clone();
    run_advancement_db(async move {
        db.write(|conn| {
            let execution_id = execution_id_owned.clone();
            let node_id = node_id.clone();
            let node_name = node_name.clone();
            let action_config_id = action_config_id.clone();
            let action_run_id = action_run_id_for_db.clone();
            Box::pin(async move {
                let mut execution_rows = conn
                    .query(
                        "SELECT issue_id, project_id
                         FROM executions
                         WHERE id = ?1",
                        (execution_id.as_str(),),
                    )
                    .await?;
                let execution_row = execution_rows
                    .next()
                    .await?
                    .ok_or_else(|| db_internal(format!("Execution not found: {execution_id}")))?;
                let issue_id = execution_row.opt_text(0)?;
                let project_id = match execution_row.opt_text(1)? {
                    Some(project_id) => project_id,
                    None => {
                        let Some(issue_id) = issue_id.as_deref() else {
                            return Err(db_internal("No project_id found for action_run"));
                        };
                        let mut issue_rows = conn
                            .query(
                                "SELECT project_id FROM issues WHERE id = ?1",
                                (issue_id,),
                            )
                            .await?;
                        issue_rows
                            .next()
                            .await?
                            .map(|row| row.text(0))
                            .transpose()?
                            .ok_or_else(|| db_internal("Failed to get issue project"))?
                    }
                };
                let parent_job_id = find_parent_job_id_conn(conn, &execution_id, &node_id).await?;

                // Allocate the addressable URI segment from the node name
                // (e.g. name "PR" -> "pr"), deduped across both jobs and
                // action_runs for this execution so an agent and an action
                // can't both claim the same segment. This makes the action node
                // resolvable at cairn://p/PROJ/N/EXEC/<segment> (CAIRN-1222).
                let segment_base = crate::node_segments::node_segment_base(
                    Some(node_name.as_str()),
                    None,
                    Some(node_id.as_str()),
                );
                let uri_segment = if let Some(issue_id) = issue_id.as_deref() {
                    crate::node_segments::allocate_top_level_segment(
                        conn,
                        issue_id,
                        &execution_id,
                        &segment_base,
                    )
                    .await?
                } else {
                    segment_base
                };

                conn.execute(
                    "INSERT INTO action_runs(
                        id, execution_id, recipe_node_id, action_config_id, issue_id,
                        project_id, status, inputs, output, error_message, started_at,
                        completed_at, created_at, parent_job_id, uri_segment
                     )
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', NULL, NULL, NULL, ?7, NULL, ?7, ?8, ?9)",
                    params![
                        action_run_id.as_str(),
                        execution_id.as_str(),
                        node_id.as_str(),
                        action_config_id.as_str(),
                        issue_id.as_deref(),
                        project_id.as_str(),
                        now,
                        parent_job_id.as_deref(),
                        uri_segment.as_str(),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to insert action_run: {e}"))
    })?;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "action_runs", "action": "insert"}),
    );

    Ok(action_run_id)
}

async fn find_parent_job_id_conn(
    conn: &turso::Connection,
    execution_id: &str,
    recipe_node_id: &str,
) -> DbResult<Option<String>> {
    let snapshot = load_execution_snapshot_conn(conn, execution_id).await?;
    let edges = snapshot_edges_to_db(&snapshot);

    let control_edge = edges
        .iter()
        .find(|e| e.edge_type == "control" && e.target_node_id == recipe_node_id);

    if let Some(edge) = control_edge {
        let parent_job = crate::db_records::load_live_job_by_execution_node_conn(
            conn,
            execution_id,
            &edge.source_node_id,
        )
        .await?;
        return Ok(parent_job.map(|j| j.id));
    }

    Ok(None)
}

// ── Command-checkpoint auto-fix loop ──────────────────────────────────────
//
// A failed command checkpoint is no longer a dead end. `block_job` halts it
// (resumable), then `wake_upstream_after_checkpoint_failure` resumes the
// upstream agent with the captured failure. When that agent commits a fix and
// goes idle, `rearm_blocked_checkpoints` (a pre-pass in `reduce_dag`) re-pends
// the checkpoint so it re-runs against the new worktree HEAD. The loop is
// bounded by `CHECKPOINT_MAX_ATTEMPTS` and the SHA-change progress gate.

use crate::execution::checkpoint_runs::{
    build_checkpoint_failure_message, checkpoint_attempt_count, is_rearm_eligible,
    latest_checkpoint_run, reset_checkpoint_runs, CHECKPOINT_MAX_ATTEMPTS,
};

/// Resume the upstream agent that owns a failed checkpoint's worktree, with the
/// captured command output. Best-effort: if the attempt cap is exhausted, or the
/// checkpoint has no agent upstream (no session to resume), the checkpoint stays
/// Blocked for manual resolution. Reads the failure detail from the latest
/// recorded `checkpoint_runs` row.
pub fn wake_upstream_after_checkpoint_failure(
    orch: &Orchestrator,
    checkpoint_job_id: &str,
) -> Result<(), String> {
    let attempts = checkpoint_attempt_count(orch, checkpoint_job_id)?;
    if attempts >= CHECKPOINT_MAX_ATTEMPTS {
        log::warn!(
            "Checkpoint {} hit the auto-fix cap ({} attempts); staying Blocked for manual resolution",
            &checkpoint_job_id[..checkpoint_job_id.len().min(8)],
            CHECKPOINT_MAX_ATTEMPTS
        );
        return Ok(());
    }

    let Some(run) = latest_checkpoint_run(orch, checkpoint_job_id)? else {
        return Ok(());
    };

    let (parent_job_id, node_name) = load_checkpoint_parent_and_name(orch, checkpoint_job_id)?;
    let Some(parent_job_id) = parent_job_id else {
        log::info!(
            "Checkpoint {} has no agent upstream to wake; staying Blocked",
            &checkpoint_job_id[..checkpoint_job_id.len().min(8)]
        );
        return Ok(());
    };

    if !job_has_session(orch, &parent_job_id)? {
        log::info!(
            "Upstream job {} has no session to resume; checkpoint stays Blocked",
            &parent_job_id[..parent_job_id.len().min(8)]
        );
        return Ok(());
    }

    let message = build_checkpoint_failure_message(
        node_name.as_deref().unwrap_or("checkpoint"),
        run.command.as_deref().unwrap_or("the checkpoint command"),
        run.exit_code,
        run.stdout_tail.as_deref().unwrap_or(""),
        run.stderr_tail.as_deref().unwrap_or(""),
    );

    crate::execution::jobs::continue_job_impl(orch, &parent_job_id, Some(&message), None, None)?;
    log::info!(
        "Woke upstream agent {} after checkpoint {} failed (attempt {})",
        &parent_job_id[..parent_job_id.len().min(8)],
        &checkpoint_job_id[..checkpoint_job_id.len().min(8)],
        attempts
    );
    Ok(())
}

fn load_checkpoint_parent_and_name(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<(Option<String>, Option<String>), String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT parent_job_id, node_name FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                Ok(rows
                    .next()
                    .await?
                    .map(|row| Ok::<_, DbError>((row.opt_text(0)?, row.opt_text(1)?)))
                    .transpose()?
                    .unwrap_or((None, None)))
            })
        })
        .await
        .map_err(|e| format!("Failed to load checkpoint job: {e}"))
    })
}

fn job_has_session(orch: &Orchestrator, job_id: &str) -> Result<bool, String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT current_session_id FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                Ok(rows
                    .next()
                    .await?
                    .map(|row| row.opt_text(0))
                    .transpose()?
                    .flatten()
                    .is_some())
            })
        })
        .await
        .map_err(|e| format!("Failed to load job session: {e}"))
    })
}

/// Re-arm pre-pass for `reduce_dag`: for every Blocked command-checkpoint job in
/// the execution that is eligible (upstream committed new work, upstream not
/// running, under the attempt cap), delete its checkpoint artifact and set it
/// Pending. The subsequent pending-scan claims it Running and re-runs the
/// command against the new worktree HEAD. A cheap no-op when nothing is eligible.
pub fn rearm_blocked_checkpoints(orch: &Orchestrator, execution_id: &str) -> Result<(), String> {
    // Map checkpoint node ids from the execution snapshot.
    let nodes = load_nodes_from_execution(orch.db.local.clone(), execution_id)?;
    let checkpoint_node_ids: HashSet<String> = nodes
        .iter()
        .filter(|n| n.node_type == "checkpoint")
        .map(|n| n.id.clone())
        .collect();
    if checkpoint_node_ids.is_empty() {
        return Ok(());
    }

    let blocked = load_blocked_checkpoint_jobs(orch, execution_id, &checkpoint_node_ids)?;
    for job in blocked {
        let worktree =
            resolve_checkpoint_worktree(orch, execution_id, job.parent_job_id.as_deref())?;
        let head_sha = worktree
            .as_deref()
            .and_then(|w| crate::execution::cache::get_current_head_sha(orch, w).ok());
        let last_run = latest_checkpoint_run(orch, &job.id)?;
        let last_run_sha = last_run.as_ref().and_then(|r| r.commit_sha.clone());
        let attempts = checkpoint_attempt_count(orch, &job.id)?;
        let upstream_running = match job.parent_job_id.as_deref() {
            Some(parent) => job_is_running(orch, parent)?,
            None => false,
        };

        if is_rearm_eligible(
            head_sha.as_deref(),
            last_run_sha.as_deref(),
            upstream_running,
            attempts,
            CHECKPOINT_MAX_ATTEMPTS,
        ) {
            reset_checkpoint_job_to_pending(orch, &job.id)?;
            log::info!(
                "Re-armed blocked checkpoint {} (new HEAD {}, attempt {})",
                &job.id[..job.id.len().min(8)],
                head_sha.as_deref().unwrap_or("?"),
                attempts
            );
        }
    }
    Ok(())
}

/// Manual Re-run entry for a Blocked command checkpoint. Validates the job is a
/// Blocked, sessionless (command) checkpoint, clears its run history for a fresh
/// attempt cycle, re-pends it (bypassing the SHA gate — explicit user intent),
/// and advances the DAG so it re-runs. Returns any newly ready agent jobs.
pub async fn rerun_checkpoint_job(orch: &Orchestrator, job_id: &str) -> Result<Vec<Job>, String> {
    let (status, session_id, recipe_node_id, execution_id) = load_rerun_job_fields(orch, job_id)?;
    if status != "blocked" {
        return Err(format!(
            "Checkpoint can only be re-run while Blocked (current status: {status})"
        ));
    }
    if session_id.is_some() {
        return Err(
            "This job ran an agent (it has a session) and is not a command checkpoint. Re-run only applies to command checkpoints."
                .to_string(),
        );
    }
    let execution_id = execution_id.ok_or_else(|| "Checkpoint job has no execution".to_string())?;
    let node_id = recipe_node_id.ok_or_else(|| "Checkpoint job has no recipe node".to_string())?;

    let nodes = load_nodes_from_execution(orch.db.local.clone(), &execution_id)?;
    let is_checkpoint = nodes
        .iter()
        .any(|n| n.id == node_id && n.node_type == "checkpoint");
    if !is_checkpoint {
        return Err("This job is not a command checkpoint.".to_string());
    }

    reset_checkpoint_runs(orch, job_id)?;
    reset_checkpoint_job_to_pending(orch, job_id)?;
    advance_execution_with_actions(orch, &execution_id).await
}

struct BlockedCheckpointJob {
    id: String,
    parent_job_id: Option<String>,
}

fn load_blocked_checkpoint_jobs(
    orch: &Orchestrator,
    execution_id: &str,
    checkpoint_node_ids: &HashSet<String>,
) -> Result<Vec<BlockedCheckpointJob>, String> {
    let db = orch.db.local.clone();
    let execution_id = execution_id.to_string();
    let node_ids: HashSet<String> = checkpoint_node_ids.clone();
    run_advancement_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            let node_ids = node_ids.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, recipe_node_id, parent_job_id
                         FROM jobs
                         WHERE execution_id = ?1 AND status = 'blocked'",
                        (execution_id.as_str(),),
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    let id = row.text(0)?;
                    let recipe_node_id = row.opt_text(1)?;
                    let parent_job_id = row.opt_text(2)?;
                    if recipe_node_id
                        .as_deref()
                        .map(|n| node_ids.contains(n))
                        .unwrap_or(false)
                    {
                        out.push(BlockedCheckpointJob { id, parent_job_id });
                    }
                }
                Ok(out)
            })
        })
        .await
        .map_err(|e| format!("Failed to load blocked checkpoint jobs: {e}"))
    })
}

fn resolve_checkpoint_worktree(
    orch: &Orchestrator,
    execution_id: &str,
    parent_job_id: Option<&str>,
) -> Result<Option<String>, String> {
    let db = orch.db.local.clone();
    let execution_id = execution_id.to_string();
    let parent_job_id = parent_job_id.map(str::to_string);
    run_advancement_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            let parent_job_id = parent_job_id.clone();
            Box::pin(async move {
                if let Some(parent) = parent_job_id.as_deref() {
                    let mut rows = conn
                        .query(
                            "SELECT worktree_path FROM jobs WHERE id = ?1 AND worktree_path IS NOT NULL",
                            (parent,),
                        )
                        .await?;
                    if let Some(row) = rows.next().await? {
                        return Ok(Some(row.text(0)?));
                    }
                }
                let mut rows = conn
                    .query(
                        "SELECT worktree_path FROM jobs
                         WHERE execution_id = ?1 AND worktree_path IS NOT NULL
                         LIMIT 1",
                        (execution_id.as_str(),),
                    )
                    .await?;
                crate::storage::next_text(&mut rows, 0).await
            })
        })
        .await
        .map_err(|e| format!("Failed to resolve checkpoint worktree: {e}"))
    })
}

fn job_is_running(orch: &Orchestrator, job_id: &str) -> Result<bool, String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT status FROM jobs WHERE id = ?1", (job_id.as_str(),))
                    .await?;
                Ok(rows
                    .next()
                    .await?
                    .map(|row| row.text(0))
                    .transpose()?
                    .map(|s| s == "running")
                    .unwrap_or(false))
            })
        })
        .await
        .map_err(|e| format!("Failed to load job status: {e}"))
    })
}

fn reset_checkpoint_job_to_pending(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let db = orch.db.local.clone();
    let job_id_for_emit = job_id.to_string();
    let job_id = job_id_for_emit.clone();
    run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                // Delete the seeded checkpoint artifact so `artifact_present`
                // resets and the projection falls through to Pending; the
                // pending-scan then re-claims and re-runs the command.
                conn.execute(
                    "DELETE FROM artifacts WHERE job_id = ?1",
                    (job_id.as_str(),),
                )
                .await?;
                let now = chrono::Utc::now().timestamp() as i32;
                conn.execute(
                    "UPDATE jobs SET status = 'pending', updated_at = ?1, completed_at = NULL
                     WHERE id = ?2",
                    params![now, job_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to reset checkpoint job: {e}"))
    })?;
    let job = run_advancement_db({
        let db = orch.db.local.clone();
        let job_id = job_id_for_emit.clone();
        async move {
            crate::jobs::queries::get_job(&db, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })?;
    let _ = orch
        .services
        .emitter
        .emit("db-change", crate::notify::job_db_change(&job, "update"));
    Ok(())
}

#[allow(clippy::type_complexity)]
fn load_rerun_job_fields(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<(String, Option<String>, Option<String>, Option<String>), String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status, current_session_id, recipe_node_id, execution_id
                         FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::internal(format!("Job not found: {job_id}")))?;
                Ok((
                    row.text(0)?,
                    row.opt_text(1)?,
                    row.opt_text(2)?,
                    row.opt_text(3)?,
                ))
            })
        })
        .await
        .map_err(|e| format!("Failed to load job for re-run: {e}"))
    })
}

pub fn mark_action_run_failed(
    orch: &Orchestrator,
    action_run_id: &str,
    error: &str,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let db = orch.db.local.clone();
    let action_run_id = action_run_id.to_string();
    let error = error.to_string();
    let log_action_run_id = action_run_id.clone();
    let log_error = error.clone();
    run_advancement_db(async move {
        db.execute(
            "UPDATE action_runs
             SET status = 'failed',
                 error_message = ?1,
                 completed_at = ?2
             WHERE id = ?3",
            params![error.as_str(), now, action_run_id.as_str()],
        )
        .await
        .map_err(|e| format!("Failed to update action_run: {e}"))
    })?;

    log::error!("Action run {} failed: {}", log_action_run_id, log_error);

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "action_runs", "action": "update"}),
    );

    Ok(())
}
