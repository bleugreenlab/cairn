use super::*;

/// Advance execution DAG and handle action/condition/checkpoint nodes.
///
/// Returns agent jobs that are ready to start. All operations flow through
/// the typed effects system: `reduce_dag` produces effects, `execute_effects`
/// processes them through the core effect loop. Actions, checkpoints,
/// conditions, and branch-coordinate preparation are all handled inline by the loop.
///
/// This is the canonical entry point for DAG advancement. All callers —
/// event listeners, direct async calls, scheduler — go through this function,
/// which means all paths use `reduce_dag` as the single source of truth.
pub async fn advance_execution_with_actions(
    runtime: &WorkflowRuntime,
    execution_id: &str,
) -> Result<Vec<Job>, String> {
    use crate::effects::dag::reduce_dag;
    use crate::effects::run::execute_effects;
    use crate::effects::types::WorkflowEffect;

    // Step 1: Synchronous DAG reduction — produces typed effects
    let (mut agent_jobs, effects) = reduce_dag(runtime, execution_id)?;

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
        let report = execute_effects(runtime, remaining_effects).await?;

        log::debug!(
            "Effect loop completed: {} effects executed for execution {}",
            report.effects_executed,
            &execution_id[..execution_id.len().min(8)]
        );
    }

    Ok(agent_jobs)
}
pub fn block_job(runtime: &WorkflowRuntime, job_id: &str) -> Result<(), String> {
    // Ensure the blockable job has an (unconfirmed) checkpoint artifact so the
    // confirmed gate has a row to flip, then let the projection derive Blocked.
    let job_id_owned = job_id.to_string();
    let db = runtime.database(WorkflowOwner::Job(&job_id_owned))?;
    run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move {
                crate::checkpoints::ensure_checkpoint_artifact_conn(conn, &job_id).await
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;
    recompute_job(runtime, job_id)
}

pub fn mark_job_failed(runtime: &WorkflowRuntime, job_id: &str, error: &str) -> Result<(), String> {
    // Record the failure as a turn fact; the projection derives Failed and cascades.
    let owning_db = runtime.database(WorkflowOwner::Job(job_id))?;
    let fail_db = owning_db.clone();
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

    let db = owning_db.clone();
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
        runtime.host.run_error(&run_id, error);
    }

    recompute_job(runtime, job_id)?;
    Ok(())
}

pub fn create_action_run(
    runtime: &WorkflowRuntime,
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
    let action_run_id = ids::mint_child(execution_id);
    let initial_pr_attempt = if node.node_type == "pr" {
        Some(crate::pr_publish_attempts::prepare_initial_publish_attempt(
            runtime,
            &action_run_id,
            execution_id,
            &node.id,
        )?)
    } else {
        None
    };
    let execution_id_owned = execution_id.to_string();
    let db = runtime.database(WorkflowOwner::Execution(&execution_id_owned))?;
    let node_id = node.id.clone();
    let node_name = node.name.clone();
    let action_run_id_for_db = action_run_id.clone();
    let initial_intent = initial_pr_attempt.as_ref().map(|a| a.id.clone());
    let inserted = run_advancement_db(async move {
        db.write(|conn| {
            let execution_id = execution_id_owned.clone();
            let node_id = node_id.clone();
            let node_name = node_name.clone();
            let action_config_id = action_config_id.clone();
            let action_run_id = action_run_id_for_db.clone();
            let initial_pr_attempt = initial_pr_attempt.clone();
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
                // resolvable at cairn://p/proj/N/EXEC/<segment> (CAIRN-1222).
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
                if let Some(attempt) = initial_pr_attempt {
                    crate::pr_publish_attempts::insert_attempt(conn,&attempt).await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| format!("Failed to insert action_run: {e}"))
    });
    if let Err(error) = inserted {
        if let Some(intent) = initial_intent {
            let runtime = runtime.clone();
            if let Err(ack_error) = run_advancement_db(async move {
                crate::pr_publish_attempts::finish_dispatch_intent(&runtime, &intent).await
            }) {
                log::warn!("Could not acknowledge unclaimed initial PR intent: {ack_error}");
            }
        }
        return Err(error);
    }

    let _ = runtime.emitter.emit(
        "db-change",
        serde_json::json!({"table": "action_runs", "action": "insert"}),
    );

    if node.node_type == "pr" {
        crate::pr_publish_attempts::capture_initial_publish_revision(runtime, &action_run_id);
    }
    Ok(action_run_id)
}

async fn find_parent_job_id_conn(
    conn: &cairn_db::turso::Connection,
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
// the checkpoint so it re-runs against the new logical head. The loop is
// bounded by `CHECKPOINT_MAX_ATTEMPTS` and the SHA-change progress gate.

use crate::checkpoint_runs::{
    build_checkpoint_failure_message, checkpoint_attempt_count, is_rearm_eligible,
    latest_checkpoint_run, reset_checkpoint_runs, CHECKPOINT_MAX_ATTEMPTS,
};

/// Resume the upstream agent that owns a failed checkpoint's branch, with the
/// captured command output. Best-effort: if the attempt cap is exhausted, or the
/// checkpoint has no agent upstream (no session to resume), the checkpoint stays
/// Blocked for manual resolution. Reads the failure detail from the latest
/// recorded `checkpoint_runs` row.
pub fn wake_upstream_after_checkpoint_failure(
    runtime: &WorkflowRuntime,
    checkpoint_job_id: &str,
) -> Result<(), String> {
    let attempts = checkpoint_attempt_count(runtime, checkpoint_job_id)?;
    if attempts >= CHECKPOINT_MAX_ATTEMPTS {
        log::warn!(
            "Checkpoint {} hit the auto-fix cap ({} attempts); staying Blocked for manual resolution",
            &checkpoint_job_id[..checkpoint_job_id.len().min(8)],
            CHECKPOINT_MAX_ATTEMPTS
        );
        return Ok(());
    }

    let Some(run) = latest_checkpoint_run(runtime, checkpoint_job_id)? else {
        return Ok(());
    };

    let (parent_job_id, node_name) = load_checkpoint_parent_and_name(runtime, checkpoint_job_id)?;
    let Some(parent_job_id) = parent_job_id else {
        log::info!(
            "Checkpoint {} has no agent upstream to wake; staying Blocked",
            &checkpoint_job_id[..checkpoint_job_id.len().min(8)]
        );
        return Ok(());
    };

    if !job_has_session(runtime, &parent_job_id)? {
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

    runtime.host.continue_job(&parent_job_id, &message)?;
    log::info!(
        "Woke upstream agent {} after checkpoint {} failed (attempt {})",
        &parent_job_id[..parent_job_id.len().min(8)],
        &checkpoint_job_id[..checkpoint_job_id.len().min(8)],
        attempts
    );
    Ok(())
}

fn load_checkpoint_parent_and_name(
    runtime: &WorkflowRuntime,
    job_id: &str,
) -> Result<(Option<String>, Option<String>), String> {
    let job_id = job_id.to_string();
    let db = runtime.database(WorkflowOwner::Job(&job_id))?;
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

fn job_has_session(runtime: &WorkflowRuntime, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    let db = runtime.database(WorkflowOwner::Job(&job_id))?;
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
/// command against the new logical head. A cheap no-op when nothing is eligible.
pub fn rearm_blocked_checkpoints(
    runtime: &WorkflowRuntime,
    execution_id: &str,
) -> Result<(), String> {
    // Map checkpoint node ids from the execution snapshot.
    let exec_db = runtime.database(WorkflowOwner::Execution(execution_id))?;
    let nodes = load_nodes_from_execution(exec_db, execution_id)?;
    let checkpoint_node_ids: HashSet<String> = nodes
        .iter()
        .filter(|n| n.node_type == "checkpoint")
        .map(|n| n.id.clone())
        .collect();
    if checkpoint_node_ids.is_empty() {
        return Ok(());
    }

    let blocked = load_blocked_checkpoint_jobs(runtime, execution_id, &checkpoint_node_ids)?;
    for job in blocked {
        let head_sha = job
            .parent_job_id
            .as_deref()
            .and_then(|parent| runtime.host.job_head(parent).ok());
        let last_run = latest_checkpoint_run(runtime, &job.id)?;
        let last_run_sha = last_run.as_ref().and_then(|r| r.commit_sha.clone());
        let attempts = checkpoint_attempt_count(runtime, &job.id)?;
        let upstream_running = match job.parent_job_id.as_deref() {
            Some(parent) => job_is_running(runtime, parent)?,
            None => false,
        };

        if is_rearm_eligible(
            head_sha.as_deref(),
            last_run_sha.as_deref(),
            upstream_running,
            attempts,
            CHECKPOINT_MAX_ATTEMPTS,
        ) {
            reset_checkpoint_job_to_pending(runtime, &job.id)?;
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
pub async fn rerun_checkpoint_job(
    runtime: &WorkflowRuntime,
    job_id: &str,
) -> Result<Vec<Job>, String> {
    let (status, session_id, recipe_node_id, execution_id) =
        load_rerun_job_fields(runtime, job_id)?;
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

    let exec_db = runtime.database(WorkflowOwner::Execution(&execution_id))?;
    let nodes = load_nodes_from_execution(exec_db, &execution_id)?;
    let is_checkpoint = nodes
        .iter()
        .any(|n| n.id == node_id && n.node_type == "checkpoint");
    if !is_checkpoint {
        return Err("This job is not a command checkpoint.".to_string());
    }

    reset_checkpoint_runs(runtime, job_id)?;
    reset_checkpoint_job_to_pending(runtime, job_id)?;
    advance_execution_with_actions(runtime, &execution_id).await
}

/// Re-evaluate a condition that blocked and routed nowhere.
///
/// Only a blocked condition of a live execution qualifies: one that already
/// routed has decided, one never evaluated runs on its own once its inputs are
/// ready, and a superseded or resolved execution must not start work. The new
/// evaluation is stored as a fresh row, routed or blocked again, beside the old
/// blocked row, which stays as history. A routed result advances the DAG.
///
/// Eligibility is read before the model call, so it can go stale while the
/// call is in flight. The store guard rechecks it atomically: a route recorded
/// meanwhile (a concurrent re-evaluation), or a supersession or resolution that
/// landed meanwhile, discards this result instead of firing a second branch.
pub async fn reevaluate_condition(
    runtime: &WorkflowRuntime,
    execution_id: &str,
    node_id: &str,
) -> Result<crate::conditions::ConditionVerdict, String> {
    use crate::effects::run::{record_condition_verdict, RecordedVerdict};
    use crate::effects::types::WorkflowEffect;

    let name = reevaluable_condition(runtime, execution_id, node_id).await?;
    let verdict = crate::effects::run::evaluate_condition(runtime, execution_id, node_id, &name)
        .await
        .ok_or_else(|| {
            format!(
                "Condition '{name}' could not be re-evaluated: its execution's database is unavailable"
            )
        })?;
    match record_condition_verdict(runtime, execution_id, node_id, &verdict).await? {
        RecordedVerdict::Routed => {
            crate::effects::run::execute_effects(
                runtime,
                vec![WorkflowEffect::AdvanceDag {
                    execution_id: execution_id.to_string(),
                    outbox_entry_id: None,
                }],
            )
            .await?;
            Ok(verdict)
        }
        RecordedVerdict::Blocked => Ok(verdict),
        RecordedVerdict::Refused => {
            let why = match reevaluable_condition(runtime, execution_id, node_id).await {
                Err(reason) => reason,
                Ok(_) => "its execution changed while it was evaluating".to_string(),
            };
            Err(format!(
                "Condition '{name}' re-evaluation was discarded: {why}"
            ))
        }
    }
}

/// Check that `node_id` is a blocked condition of this execution, returning its
/// name. The refusals are the ones `reevaluate_condition` gives.
pub async fn reevaluable_condition(
    runtime: &WorkflowRuntime,
    execution_id: &str,
    node_id: &str,
) -> Result<String, String> {
    let db = runtime.database(WorkflowOwner::Execution(execution_id))?;
    let node = crate::conditions::load_condition_node(&db, execution_id, node_id).await?;
    if node.node_type != "condition" && node.node_type != "decision" {
        return Err(format!(
            "Node '{}' ({node_id}) is a {} node, not a condition or decision",
            node.name, node.node_type
        ));
    }

    let rows = load_condition_outcomes(&db, execution_id, node_id).await?;
    if let Some((_, port)) = rows.iter().find(|(outcome, _)| outcome == "routed") {
        return Err(format!(
            "Condition '{}' already routed to '{port}'; only a blocked condition can be re-evaluated",
            node.name
        ));
    }
    if rows.is_empty() {
        return Err(format!(
            "Condition '{}' has not been evaluated yet; it runs when its inputs are ready",
            node.name
        ));
    }
    if !execution_is_live(&db, execution_id).await? {
        return Err(format!(
            "Condition '{}' can't be re-evaluated: its execution was superseded by a newer attempt or its issue is resolved",
            node.name
        ));
    }
    Ok(node.name)
}

/// Whether the execution can still act: the same predicate the condition store
/// guard and the waiting projections use.
async fn execution_is_live(db: &LocalDb, execution_id: &str) -> Result<bool, String> {
    let execution_id = execution_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    concat!(
                        "SELECT COUNT(*) FROM executions e WHERE e.id = ?1 AND ",
                        cairn_db::live_execution_sql!()
                    ),
                    (execution_id.as_str(),),
                )
                .await?;
            Ok(match rows.next().await? {
                Some(row) => row.i64(0)? > 0,
                None => false,
            })
        })
    })
    .await
    .map_err(|e| format!("Failed to read execution liveness: {e}"))
}

/// `(outcome, result_port)` for every evaluation row of one condition node.
async fn load_condition_outcomes(
    db: &LocalDb,
    execution_id: &str,
    node_id: &str,
) -> Result<Vec<(String, String)>, String> {
    let execution_id = execution_id.to_string();
    let node_id = node_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        let node_id = node_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT outcome, result_port
                     FROM condition_evaluations
                     WHERE execution_id = ?1 AND recipe_node_id = ?2",
                    (execution_id.as_str(), node_id.as_str()),
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.text(0)?, row.text(1)?));
            }
            Ok(out)
        })
    })
    .await
    .map_err(|e| format!("Failed to load condition evaluations: {e}"))
}

struct BlockedCheckpointJob {
    id: String,
    parent_job_id: Option<String>,
}

fn load_blocked_checkpoint_jobs(
    runtime: &WorkflowRuntime,
    execution_id: &str,
    checkpoint_node_ids: &HashSet<String>,
) -> Result<Vec<BlockedCheckpointJob>, String> {
    let execution_id = execution_id.to_string();
    let db = runtime.database(WorkflowOwner::Execution(&execution_id))?;
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

fn job_is_running(runtime: &WorkflowRuntime, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    let db = runtime.database(WorkflowOwner::Job(&job_id))?;
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

fn reset_checkpoint_job_to_pending(runtime: &WorkflowRuntime, job_id: &str) -> Result<(), String> {
    let owning_db = runtime.database(WorkflowOwner::Job(job_id))?;
    let db = owning_db.clone();
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
    let job = Job::try_from(
        load_job(owning_db, &job_id_for_emit)?
            .ok_or_else(|| format!("Job not found: {job_id_for_emit}"))?,
    )?;
    let _ = runtime.emitter.emit(
        "db-change",
        cairn_db::changes::job_db_change(&job, "update"),
    );
    Ok(())
}

#[allow(clippy::type_complexity)]
fn load_rerun_job_fields(
    runtime: &WorkflowRuntime,
    job_id: &str,
) -> Result<(String, Option<String>, Option<String>, Option<String>), String> {
    let job_id = job_id.to_string();
    let db = runtime.database(WorkflowOwner::Job(&job_id))?;
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
    runtime: &WorkflowRuntime,
    action_run_id: &str,
    error: &str,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let action_run_id = action_run_id.to_string();
    let db = runtime.database(WorkflowOwner::ActionRun(&action_run_id))?;
    let error = error.to_string();
    let log_action_run_id = action_run_id.clone();
    let log_error = error.clone();
    run_advancement_db(async move {
        db.write(|conn| {
            let (id,error) = (action_run_id.clone(), error.clone());
            Box::pin(async move {
                conn.execute(
                    "UPDATE action_runs SET status='failed',error_message=?1,completed_at=?2 WHERE id=?3",
                    params![error.as_str(),now,id.as_str()],
                ).await?;
                crate::pr_publish_attempts::settle_publish_attempt_conn(conn,&id,"failed",&error).await?;
                Ok(())
            })
        }).await.map_err(|e|format!("Failed to update action_run: {e}"))
    })?;

    let runtime_for_ack = runtime.clone();
    let id_for_ack = log_action_run_id.clone();
    if let Err(error) = run_advancement_db(async move {
        crate::pr_publish_attempts::acknowledge_settled_dispatch(&runtime_for_ack, &id_for_ack)
            .await
    }) {
        log::warn!("Could not acknowledge settled PR dispatch: {error}");
    }
    log::error!("Action run {} failed: {}", log_action_run_id, log_error);

    let _ = runtime.emitter.emit(
        "db-change",
        serde_json::json!({"table": "action_runs", "action": "update"}),
    );

    Ok(())
}
