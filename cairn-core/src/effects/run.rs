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
                        if let Ok(mut conn) = orch.db.conn.lock() {
                            crate::effects::outbox::mark_done(&mut conn, entry_id);
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

                WorkflowEffect::WakeManager { job_id } => {
                    crate::orchestrator::lifecycle::wake_manager_on_failure(orch, &job_id);
                }

                // ── Condition evaluation ──
                WorkflowEffect::StoreConditionEvaluation {
                    execution_id,
                    node_id,
                    port,
                    error_msg,
                } => {
                    let mut conn = orch
                        .db
                        .conn
                        .lock()
                        .map_err(|e| format!("Failed to lock database: {}", e))?;
                    crate::execution::conditions::store_condition_evaluation(
                        &mut conn,
                        &execution_id,
                        &node_id,
                        &port,
                        error_msg.as_deref(),
                        &*orch.services.emitter,
                    )?;
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
                    let result = evaluate_condition(orch, &execution_id, &node_id, &node_name);
                    if let Some(effect_result) = result {
                        effects.extend(reduce_effect_result(effect_result));
                    }
                }

                // ── Checkpoint handling ──
                WorkflowEffect::ApplyCheckpointApproval { job_id } => {
                    let new_effects = crate::effects::checkpoint::approve_job_pure(orch, &job_id)?;
                    effects.extend(new_effects);
                }

                WorkflowEffect::ApplyCheckpointRejection { job_id, reason } => {
                    let new_effects = crate::effects::checkpoint::reject_job_pure(
                        orch,
                        &job_id,
                        reason.as_deref(),
                    )?;
                    effects.extend(new_effects);
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

                // ── Worktree management ──
                WorkflowEffect::CleanupWorktrees { execution_id } => {
                    let _ = crate::execution::advancement::cleanup_non_issue_worktrees_if_complete(
                        orch,
                        &execution_id,
                    );
                }

                WorkflowEffect::CreateExecutorWorktree {
                    job_id,
                    execution_id,
                    ..
                } => {
                    let result = create_worktree(orch, &job_id, &execution_id);
                    effects.extend(reduce_effect_result(result));
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

/// Run a programmatic checkpoint command.
async fn run_checkpoint(
    _orch: &Orchestrator,
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
    match crate::execution::conditions::execute_programmatic_checkpoint(&worktree_str, command)
        .await
    {
        Ok(passed) => {
            if passed {
                log::info!(
                    "Programmatic checkpoint '{}' passed, auto-approving",
                    node_name
                );
            } else {
                log::info!(
                    "Programmatic checkpoint '{}' failed, auto-rejecting",
                    node_name
                );
            }
            EffectResult::CheckpointComplete {
                job_id: job_id.to_string(),
                passed,
                error: if passed {
                    None
                } else {
                    Some(format!("Programmatic check failed: {}", command))
                },
            }
        }
        Err(e) => {
            log::error!("Programmatic checkpoint '{}' error: {}", node_name, e);
            EffectResult::CheckpointComplete {
                job_id: job_id.to_string(),
                passed: false,
                error: Some(e),
            }
        }
    }
}

/// Evaluate a condition node.
fn evaluate_condition(
    orch: &Orchestrator,
    execution_id: &str,
    node_id: &str,
    node_name: &str,
) -> Option<EffectResult> {
    let node = {
        let mut conn = orch.db.conn.lock().ok()?;
        let nodes =
            crate::execution::dag::load_nodes_from_execution(&mut conn, execution_id).ok()?;
        nodes.into_iter().find(|n| n.id == node_id)?
    };

    let recipe_id = {
        use crate::schema::executions;
        use diesel::prelude::*;
        let mut conn = orch.db.conn.lock().ok()?;
        executions::table
            .find(execution_id)
            .select(executions::recipe_id)
            .first::<String>(&mut *conn)
            .ok()?
    };

    let context = {
        let mut conn = orch.db.conn.lock().ok()?;
        crate::execution::conditions::gather_condition_context(
            &mut conn,
            execution_id,
            node_id,
            &recipe_id,
        )
        .ok()?
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
    use crate::test_utils::test_diesel_conn;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    fn test_orchestrator() -> Orchestrator {
        let conn = test_diesel_conn();
        let db = Arc::new(DbState {
            conn: Mutex::new(conn),
        });
        let services = Arc::new(TestServicesBuilder::new().build());
        let account_manager = Arc::new(crate::orchestrator::AccountManager::new(
            db.clone(),
            services.emitter.clone(),
        ));
        let sync_tx = Arc::new(Mutex::new(None));
        Orchestrator {
            db,
            services: services.clone(),
            process_state: Arc::new(crate::agent_process::process::AgentProcessState::default()),
            mcp_auth: Arc::new(crate::mcp::McpAuthState::new(PathBuf::from("/tmp"))),
            warm_gc: None,
            pty_state: Arc::new(crate::services::PtyState::default()),
            permission_responses: tokio::sync::broadcast::channel(16).0,
            run_completions: tokio::sync::broadcast::channel(64).0,
            prompt_responses: tokio::sync::broadcast::channel(16).0,
            trigger_events: tokio::sync::broadcast::channel(256).0,
            session_allowed_tools: Arc::new(Mutex::new(std::collections::HashSet::new())),
            identity_store: Arc::new(Mutex::new(None)),
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir: PathBuf::from("/tmp"),
            schema_dir: None,
            mcp_callback_port: 3847,
            embedding_engine: None,
            vibe_state: None,
            account_manager,
            sync_tx: sync_tx.clone(),
            notifier: crate::notify::Notifier::new(sync_tx, services.emitter.clone()),
            api_config: crate::api::ApiConfig::default(),
            effect_tx: None,
            model_catalog: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            provider_usage_snapshots: Default::default(),
            executor: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    #[tokio::test]
    async fn run_checkpoint_cached_pass_returns_complete_true() {
        let orch = test_orchestrator();
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
    }

    #[tokio::test]
    async fn run_checkpoint_not_cached_runs_command() {
        let orch = test_orchestrator();
        // "exit 0" should pass on any system
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
    }

    #[tokio::test]
    async fn run_checkpoint_command_failure_returns_false() {
        let orch = test_orchestrator();
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
    }
}

/// Create a worktree for an executor node.
fn create_worktree(orch: &Orchestrator, job_id: &str, execution_id: &str) -> EffectResult {
    let db_job = {
        use crate::schema::jobs;
        use diesel::prelude::*;
        let conn_result = orch.db.conn.lock();
        match conn_result {
            Ok(mut conn) => match jobs::table
                .find(job_id)
                .first::<crate::diesel_models::DbJob>(&mut *conn)
            {
                Ok(j) => j,
                Err(e) => {
                    return EffectResult::WorktreeFailed {
                        job_id: job_id.to_string(),
                        error: format!("Job not found: {}", e),
                    };
                }
            },
            Err(e) => {
                return EffectResult::WorktreeFailed {
                    job_id: job_id.to_string(),
                    error: format!("DB lock failed: {}", e),
                };
            }
        }
    };

    match crate::execution::advancement::create_executor_worktree(orch, &db_job) {
        Ok(()) => EffectResult::WorktreeCreated {
            job_id: job_id.to_string(),
            execution_id: execution_id.to_string(),
        },
        Err(e) => EffectResult::WorktreeFailed {
            job_id: job_id.to_string(),
            error: e,
        },
    }
}
