//! Drives workflow effects to quiescence. Each batch finishes before its
//! results are reduced into the next batch. External operations use the host;
//! workflow state changes and outbox acknowledgement remain in the engine.

use super::reduce::reduce_effect_result;
use super::types::{EffectResult, WorkflowEffect};
use crate::runtime::{WorkflowOwner, WorkflowRuntime, Workload};

/// Summary of what happened during effect execution.
#[derive(Debug, Default)]
pub struct EffectLoopReport {
    pub effects_executed: usize,
}

/// Drive the reduce→execute→reduce loop until quiescent.
///
/// The engine handles workflow mutations and dispatches external workloads. `EffectResult`s
/// feed back into `reduce_effect_result` which may produce more effects.
#[tracing::instrument(
    target = "profiler",
    name = "effect_loop",
    skip_all,
    fields(effects = effects.len())
)]
pub async fn execute_effects(
    runtime: &WorkflowRuntime,
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
                        crate::effects::dag::reduce_dag(runtime, &execution_id)?;
                    if !agent_jobs.is_empty() {
                        effects.push(WorkflowEffect::StartAgentJobs(agent_jobs));
                    }
                    effects.extend(new_effects);

                    // Mark the specific outbox entry as done (row-level acknowledgement).
                    // Only root AdvanceDag effects from apply_step_outcome carry an
                    // outbox_entry_id. Follow-on effects from reduce_dag have None.
                    if let Some(ref entry_id) = outbox_entry_id {
                        if let Err(error) = mark_outbox_done(runtime, entry_id).await {
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
                    runtime
                        .host
                        .lifecycle_message(&job_id, run_id.as_deref(), event);
                }

                // ── Condition evaluation ──
                WorkflowEffect::StoreConditionEvaluation {
                    execution_id,
                    node_id,
                    verdict,
                } => {
                    if record_condition_verdict(runtime, &execution_id, &node_id, &verdict)
                        .await?
                        .advances()
                    {
                        effects.push(WorkflowEffect::AdvanceDag {
                            execution_id,
                            outbox_entry_id: None,
                        });
                    }
                }

                WorkflowEffect::EvaluateCondition {
                    execution_id,
                    node_id,
                    node_name,
                    ..
                } => {
                    if let Some(verdict) =
                        evaluate_condition(runtime, &execution_id, &node_id, &node_name).await
                    {
                        effects.extend(reduce_effect_result(EffectResult::ConditionEvaluated {
                            execution_id,
                            node_id,
                            verdict,
                        }));
                    }
                }

                // ── Checkpoint handling ──
                WorkflowEffect::ApplyCheckpointApproval { job_id } => {
                    let new_effects =
                        crate::effects::checkpoint::approve_job_pure(runtime, &job_id)?;
                    effects.extend(new_effects);
                }

                WorkflowEffect::BlockCheckpointJob { job_id } => {
                    // Command checkpoint failed: block the job (seed unconfirmed
                    // artifact + recompute -> Blocked). Resumable, not terminal.
                    crate::advancement::block_job(runtime, &job_id)?;
                    // Then wake the upstream agent with the captured failure so it
                    // can fix and commit; the checkpoint re-runs automatically when
                    // the agent goes idle. Best-effort — a failed wake leaves the
                    // checkpoint Blocked for manual resolution.
                    if let Err(e) =
                        crate::advancement::wake_upstream_after_checkpoint_failure(runtime, &job_id)
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
                    cached_pass,
                    ..
                } => {
                    let result =
                        run_checkpoint(runtime, &job_id, &node_name, &command, cached_pass).await;
                    effects.extend(reduce_effect_result(result));
                }

                // ── Failure marking ──
                WorkflowEffect::MarkJobFailed { job_id, error } => {
                    crate::advancement::mark_job_failed(runtime, &job_id, &error)?;
                }

                WorkflowEffect::MarkActionRunFailed {
                    action_run_id,
                    error,
                } => {
                    crate::advancement::mark_action_run_failed(runtime, &action_run_id, &error)?;
                }

                // Workload execution is the host's responsibility; only the
                // workflow engine turns its result into subsequent effects.
                WorkflowEffect::StartAgentJobs(jobs) => {
                    runtime.host.execute(Workload::AgentJobs(jobs)).await?;
                }
                WorkflowEffect::ExecuteAction {
                    action_run_id,
                    execution_id,
                    node_id,
                    ctx,
                } => {
                    if let Some(result) = runtime
                        .host
                        .execute(Workload::Action {
                            action_run_id,
                            execution_id,
                            node_id,
                            ctx,
                        })
                        .await?
                    {
                        effects.extend(reduce_effect_result(result));
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
pub async fn run_checkpoint(
    runtime: &WorkflowRuntime,
    job_id: &str,
    node_name: &str,
    command: &str,
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

    let (commit_sha, output) = match runtime.host.checkpoint(job_id, node_name, command).await {
        Ok(result) => (Some(result.coordinate), result.output),
        Err(e) => {
            // The command could not even be spawned. Record it as a failed run
            // (exit -1) so the loop treats it like any other failure.
            log::error!("Programmatic checkpoint '{}' error: {}", node_name, e);
            (
                None,
                crate::conditions::CheckpointRunOutput {
                    passed: false,
                    exit_code: -1,
                    stdout: String::new(),
                    stderr: e,
                },
            )
        }
    };

    if let Err(e) = crate::checkpoint_runs::record_checkpoint_run(
        runtime,
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

/// What recording a condition verdict did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordedVerdict {
    /// Stored, and routed: downstream edges on its port may fire.
    Routed,
    /// Stored, and blocked: the node parks until it is re-evaluated.
    Blocked,
    /// Refused by the store guard (already routed, or the execution is no
    /// longer live). Nothing was written.
    Refused,
}

impl RecordedVerdict {
    pub(crate) fn advances(self) -> bool {
        self == RecordedVerdict::Routed
    }
}

/// Store a condition verdict in the execution's owning database and tell the
/// host. The single path both the automatic evaluation and an explicit
/// re-evaluation record through; the caller advances only on `Routed`.
pub(crate) async fn record_condition_verdict(
    runtime: &WorkflowRuntime,
    execution_id: &str,
    node_id: &str,
    verdict: &crate::conditions::ConditionVerdict,
) -> Result<RecordedVerdict, String> {
    // The condition evaluation row lands in the execution's owning database
    // (CAIRN-2197) — the synced replica for a team execution, the private DB
    // otherwise.
    let db = runtime.database(WorkflowOwner::Execution(execution_id))?;
    let stored = crate::conditions::store_condition_evaluation(
        &db,
        execution_id,
        node_id,
        verdict,
        &*runtime.emitter,
    )
    .await?;
    if !stored.accepted {
        log::info!(
            "Condition node '{node_id}' verdict not recorded: already routed, or execution {execution_id} is no longer live"
        );
        return Ok(RecordedVerdict::Refused);
    }
    runtime.host.condition_evaluated(
        execution_id,
        node_id,
        stored.issue_id.as_deref(),
        verdict.outcome(),
    );
    Ok(match verdict {
        crate::conditions::ConditionVerdict::Routed { .. } => RecordedVerdict::Routed,
        crate::conditions::ConditionVerdict::Blocked { .. } => RecordedVerdict::Blocked,
    })
}

/// Evaluate a condition node to a routed or blocked verdict.
///
/// `None` only when the owning database cannot be resolved, because then there
/// is nowhere to record a row. Every other failure is a blocked verdict.
pub(crate) async fn evaluate_condition(
    runtime: &WorkflowRuntime,
    execution_id: &str,
    node_id: &str,
    node_name: &str,
) -> Option<crate::conditions::ConditionVerdict> {
    use crate::conditions::ConditionVerdict;

    // Condition nodes read the execution snapshot + upstream artifacts; for a
    // team execution those rows live in the synced replica, so resolve the
    // owning database rather than reading the private DB (CAIRN-2197).
    let db = match runtime.database(WorkflowOwner::Execution(execution_id)) {
        Ok(db) => db,
        Err(error) => {
            log::error!(
                "Failed to resolve owning database for condition node '{}': {}",
                node_name,
                error
            );
            return None;
        }
    };

    let verdict = match crate::conditions::load_condition_node(&db, execution_id, node_id).await {
        Err(error) => ConditionVerdict::Blocked {
            reason: format!("Failed to load condition node: {error}"),
            decision: None,
        },
        Ok(node) => {
            match crate::conditions::gather_condition_context(&db, execution_id, node_id).await {
                Err(error) => ConditionVerdict::Blocked {
                    reason: format!("Failed to gather condition context: {error}"),
                    decision: None,
                },
                Ok(context) => {
                    crate::conditions::evaluate_condition_node(
                        runtime.host.as_ref(),
                        execution_id,
                        &node,
                        &context,
                    )
                    .await
                }
            }
        }
    };

    match &verdict {
        ConditionVerdict::Routed { port, .. } => {
            log::info!("Condition node '{node_name}' evaluated to port '{port}'");
        }
        ConditionVerdict::Blocked { reason, .. } => {
            log::warn!("Condition node '{node_name}' blocked, routed nowhere: {reason}");
        }
    }
    Some(verdict)
}

async fn mark_outbox_done(runtime: &WorkflowRuntime, id: &str) -> Result<(), String> {
    let id = id.to_string();
    let now = chrono::Utc::now().timestamp() as i32;
    runtime
        .private_db
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
