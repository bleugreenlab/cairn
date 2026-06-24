//! Effect execution loop.
//!
//! Drives the reduce→execute→reduce cycle until quiescent. Core-internal
//! effects are handled inline. Host-crossing effects (`StartAgentJobs`,
//! `ExecuteAction`) are dispatched to the `EffectExecutor` on the
//! Orchestrator. `EffectResult`s from the executor feed back into
//! `reduce_effect_result` which may produce more effects.

use super::reduce::reduce_effect_result;
use super::types::{EffectResult, WorkflowEffect};
use crate::orchestrator::Orchestrator;

/// Summary of what happened during effect execution.
#[derive(Debug, Default)]
pub struct EffectLoopReport {
    pub effects_executed: usize,
}

/// Drive the reduce→execute→reduce loop until quiescent.
///
/// All effects are handled inline using core functions. `EffectResult`s
/// feed back into `reduce_effect_result` which may produce more effects.
pub async fn execute_effects(
    orch: &Orchestrator,
    mut effects: Vec<WorkflowEffect>,
) -> Result<EffectLoopReport, String> {
    let mut report = EffectLoopReport::default();

    while !effects.is_empty() {
        let batch = std::mem::take(&mut effects);
        for effect in batch {
            log::debug!("Executing effect: {:?}", effect);
            match effect {
                // ── DAG advancement ──
                WorkflowEffect::AdvanceDag {
                    execution_id,
                    outbox_entry_id,
                } => {
                    let (agent_jobs, new_effects) =
                        crate::effects::dag::reduce_dag(orch, &execution_id)?;
                    if !agent_jobs.is_empty() {
                        effects.push(WorkflowEffect::StartAgentJobs(agent_jobs));
                    }
                    effects.extend(new_effects);

                    // Mark the specific outbox entry as done (row-level acknowledgement).
                    // Only root AdvanceDag effects from apply_step_outcome carry an
                    // outbox_entry_id. Follow-on effects from reduce_dag have None.
                    if let Some(ref entry_id) = outbox_entry_id {
                        if let Err(error) = mark_outbox_done(orch, entry_id).await {
                            log::warn!("Failed to mark outbox entry done: {}", error);
                        }
                    }
                }

                // ── Lifecycle messaging ──
                WorkflowEffect::EmitLifecycleMessage {
                    job_id,
                    run_id,
                    event,
                } => {
                    crate::messages::system::emit_job_event(
                        orch,
                        &job_id,
                        run_id.as_deref(),
                        event,
                    );
                }

                // ── Condition evaluation ──
                WorkflowEffect::StoreConditionEvaluation {
                    execution_id,
                    node_id,
                    port,
                    error_msg,
                } => {
                    crate::execution::conditions::store_condition_evaluation(
                        &orch.db.local,
                        &execution_id,
                        &node_id,
                        &port,
                        error_msg.as_deref(),
                        &*orch.services.emitter,
                    )
                    .await?;
                    let condition_ref = format!("{execution_id}/{node_id}");
                    if let Err(error) = crate::orchestrator::wakes::route_condition_event(
                        orch,
                        &condition_ref,
                        "evaluated",
                        None,
                    ) {
                        log::warn!("failed to route condition wake for {condition_ref}: {error}");
                    }
                    effects.push(WorkflowEffect::AdvanceDag {
                        execution_id,
                        outbox_entry_id: None,
                    });
                }

                WorkflowEffect::EvaluateCondition {
                    execution_id,
                    node_id,
                    node_name,
                    ..
                } => {
                    let result =
                        evaluate_condition(orch, &execution_id, &node_id, &node_name).await;
                    if let Some(effect_result) = result {
                        effects.extend(reduce_effect_result(effect_result));
                    }
                }

                // ── Checkpoint handling ──
                WorkflowEffect::ApplyCheckpointApproval { job_id } => {
                    let new_effects = crate::effects::checkpoint::approve_job_pure(orch, &job_id)?;
                    effects.extend(new_effects);
                }

                WorkflowEffect::BlockCheckpointJob { job_id } => {
                    // Command checkpoint failed: block the job (seed unconfirmed
                    // artifact + recompute -> Blocked). Resumable, not terminal.
                    crate::execution::advancement::block_job(orch, &job_id)?;
                    // Then wake the upstream agent with the captured failure so it
                    // can fix and commit; the checkpoint re-runs automatically when
                    // the agent goes idle. Best-effort — a failed wake leaves the
                    // checkpoint Blocked for manual resolution.
                    if let Err(e) =
                        crate::execution::advancement::wake_upstream_after_checkpoint_failure(
                            orch, &job_id,
                        )
                    {
                        log::warn!(
                            "Failed to wake upstream after checkpoint {} failed: {}",
                            &job_id[..job_id.len().min(8)],
                            e
                        );
                    }
                }

                WorkflowEffect::RunCheckpointCommand {
                    job_id,
                    node_name,
                    command,
                    worktree_path,
                    cached_pass,
                    ..
                } => {
                    let result = run_checkpoint(
                        orch,
                        &job_id,
                        &node_name,
                        &command,
                        &worktree_path,
                        cached_pass,
                    )
                    .await;
                    effects.extend(reduce_effect_result(result));
                }

                // ── Failure marking ──
                WorkflowEffect::MarkJobFailed { job_id, error } => {
                    crate::execution::advancement::mark_job_failed(orch, &job_id, &error)?;
                }

                WorkflowEffect::MarkActionRunFailed {
                    action_run_id,
                    error,
                } => {
                    crate::execution::advancement::mark_action_run_failed(
                        orch,
                        &action_run_id,
                        &error,
                    )?;
                }

                // ── Agent jobs: dispatch to host executor ──
                WorkflowEffect::StartAgentJobs(jobs) => {
                    if let Some(executor) = orch.executor.get() {
                        executor
                            .execute(orch, WorkflowEffect::StartAgentJobs(jobs))
                            .await?;
                    } else {
                        log::error!(
                            "No executor configured — cannot start {} agent jobs",
                            jobs.len()
                        );
                    }
                }

                // ── Action execution: dispatch to host executor ──
                WorkflowEffect::ExecuteAction {
                    action_run_id,
                    execution_id,
                    node_id,
                    ctx,
                } => {
                    if let Some(executor) = orch.executor.get() {
                        let result = executor
                            .execute(
                                orch,
                                WorkflowEffect::ExecuteAction {
                                    action_run_id,
                                    execution_id,
                                    node_id,
                                    ctx,
                                },
                            )
                            .await?;
                        if let Some(r) = result {
                            effects.extend(reduce_effect_result(r));
                        }
                    } else {
                        log::error!(
                            "No executor configured — cannot execute action {}",
                            action_run_id
                        );
                    }
                }
            }
            report.effects_executed += 1;
        }
    }

    Ok(report)
}

// ── Helper functions for effect handling ──────────────────────────────────

/// Run a programmatic checkpoint command. Every actual run (pass or fail) is
/// recorded in `checkpoint_runs` — the durable history that drives the auto-fix
/// loop (attempt count, last-run SHA, and the failure output shown on wake).
async fn run_checkpoint(
    orch: &Orchestrator,
    job_id: &str,
    node_name: &str,
    command: &str,
    worktree_path: &std::path::Path,
    cached_pass: bool,
) -> EffectResult {
    if cached_pass {
        log::info!(
            "Programmatic checkpoint '{}' using cached pass, auto-approving",
            node_name
        );
        return EffectResult::CheckpointComplete {
            job_id: job_id.to_string(),
            passed: true,
            error: None,
        };
    }

    let worktree_str = worktree_path.to_string_lossy().to_string();
    let commit_sha = crate::execution::cache::get_current_head_sha(orch, &worktree_str).ok();
    let output =
        match crate::execution::conditions::execute_programmatic_checkpoint(&worktree_str, command)
            .await
        {
            Ok(output) => output,
            Err(e) => {
                // The command could not even be spawned. Record it as a failed run
                // (exit -1) so the loop treats it like any other failure.
                log::error!("Programmatic checkpoint '{}' error: {}", node_name, e);
                crate::execution::conditions::CheckpointRunOutput {
                    passed: false,
                    exit_code: -1,
                    stdout: String::new(),
                    stderr: e,
                }
            }
        };

    if let Err(e) = crate::execution::checkpoint_runs::record_checkpoint_run(
        orch,
        job_id,
        command,
        commit_sha.as_deref(),
        &output,
    ) {
        log::warn!("Failed to record checkpoint run for '{}': {}", node_name, e);
    }

    if output.passed {
        log::info!(
            "Programmatic checkpoint '{}' passed, auto-approving",
            node_name
        );
    } else {
        log::info!(
            "Programmatic checkpoint '{}' failed (exit {}), blocking",
            node_name,
            output.exit_code
        );
    }

    EffectResult::CheckpointComplete {
        job_id: job_id.to_string(),
        passed: output.passed,
        error: if output.passed {
            None
        } else {
            Some(format!(
                "Programmatic check failed (exit {}): {}",
                output.exit_code, command
            ))
        },
    }
}

/// Evaluate a condition node.
async fn evaluate_condition(
    orch: &Orchestrator,
    execution_id: &str,
    node_id: &str,
    node_name: &str,
) -> Option<EffectResult> {
    let node = match crate::execution::conditions::load_condition_node(
        &orch.db.local,
        execution_id,
        node_id,
    )
    .await
    {
        Ok(node) => node,
        Err(error) => {
            log::error!("Failed to load condition node '{}': {}", node_name, error);
            return None;
        }
    };

    let context = match crate::execution::conditions::gather_condition_context(
        &orch.db.local,
        execution_id,
        node_id,
    )
    .await
    {
        Ok(context) => context,
        Err(error) => {
            log::error!(
                "Failed to gather context for condition node '{}': {}",
                node_name,
                error
            );
            return None;
        }
    };

    match crate::execution::conditions::evaluate_condition_node(
        orch.services.completion.as_ref(),
        &node,
        &context,
    ) {
        Ok((port, error_msg)) => {
            log::info!(
                "Condition node '{}' evaluated to port '{}'",
                node_name,
                port
            );
            Some(EffectResult::ConditionEvaluated {
                execution_id: execution_id.to_string(),
                node_id: node_id.to_string(),
                port,
                error_msg,
            })
        }
        Err(e) => {
            log::error!("Condition node '{}' failed: {}", node_name, e);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS};
    use std::path::PathBuf;
    use std::sync::Arc;

    async fn test_orch() -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.keep().join("run-checkpoint-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let config = tempfile::tempdir().unwrap().keep();
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(config.join("idx")).unwrap()),
        ));
        let services = Arc::new(TestServicesBuilder::new().build());
        Orchestrator::builder(db_state, services, config).build()
    }

    /// Insert a project + job so a recorded checkpoint run satisfies the
    /// `checkpoint_runs.job_id -> jobs(id)` foreign key.
    async fn seed_job(orch: &Orchestrator, job_id: &str) {
        let job_id = job_id.to_string();
        orch.db
            .local
            .write(|conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT OR IGNORE INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                         VALUES ('p-run', 'default', 'P', 'PRUN', '/tmp/p', 1, 1)",
                        (),
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO jobs (id, project_id, status, created_at, updated_at)
                         VALUES (?1, 'p-run', 'running', 1, 1)",
                        (job_id.as_str(),),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    async fn recorded_runs(orch: &Orchestrator, job_id: &str) -> i64 {
        let job_id = job_id.to_string();
        orch.db
            .local
            .read(|conn| {
                let job_id = job_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*) FROM checkpoint_runs WHERE job_id = ?1",
                            (job_id.as_str(),),
                        )
                        .await?;
                    rows.next().await?.unwrap().i64(0)
                })
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn run_checkpoint_cached_pass_returns_complete_true() {
        let orch = test_orch().await;
        let result = run_checkpoint(
            &orch,
            "job-1",
            "ci-check",
            "bun run test",
            &PathBuf::from("/tmp/test"),
            true,
        )
        .await;

        assert!(matches!(
            result,
            EffectResult::CheckpointComplete { ref job_id, passed, ref error }
                if job_id == "job-1" && passed && error.is_none()
        ));
        // A cached pass does not run the command, so nothing is recorded.
        assert_eq!(recorded_runs(&orch, "job-1").await, 0);
    }

    #[tokio::test]
    async fn run_checkpoint_not_cached_runs_command_and_records() {
        let orch = test_orch().await;
        seed_job(&orch, "job-2").await;
        // "exit 0" should pass on any system.
        let result = run_checkpoint(
            &orch,
            "job-2",
            "exit-check",
            "exit 0",
            &PathBuf::from("/tmp"),
            false,
        )
        .await;

        assert!(matches!(
            result,
            EffectResult::CheckpointComplete { ref job_id, passed, ref error }
                if job_id == "job-2" && passed && error.is_none()
        ));
        assert_eq!(recorded_runs(&orch, "job-2").await, 1);
    }

    #[tokio::test]
    async fn run_checkpoint_command_failure_returns_false_and_records() {
        let orch = test_orch().await;
        seed_job(&orch, "job-3").await;
        let result = run_checkpoint(
            &orch,
            "job-3",
            "fail-check",
            "exit 1",
            &PathBuf::from("/tmp"),
            false,
        )
        .await;

        assert!(matches!(
            result,
            EffectResult::CheckpointComplete { ref job_id, passed, .. }
                if job_id == "job-3" && !passed
        ));
        assert_eq!(recorded_runs(&orch, "job-3").await, 1);
    }
}

async fn mark_outbox_done(orch: &Orchestrator, id: &str) -> Result<(), String> {
    let id = id.to_string();
    let now = chrono::Utc::now().timestamp() as i32;
    orch.db
        .local
        .write(|conn| {
            let id = id.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE effect_outbox
                     SET state = 'done',
                         updated_at = ?1
                     WHERE id = ?2",
                    (now, id.as_str()),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|error| format!("Failed to mark outbox entry done: {error}"))
}
