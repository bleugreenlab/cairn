//! Application services supplied to the durable workflow engine.
use crate::execution::advancement::run_advancement_db;
use crate::{
    models::{JobStatus, TriggerEvent},
    orchestrator::Orchestrator,
    storage::LocalDb,
};
use async_trait::async_trait;
use cairn_workflow::{
    advancement::RecomputeOutcome,
    conditions::ExecutedCheckpoint,
    effects::types::{EffectResult, JobEvent, WorkflowEffect},
    runtime::{DecisionRequest, WorkflowHost, WorkflowOwner, WorkflowRuntime, Workload},
};
use std::{collections::HashSet, path::Path, sync::Arc};

pub(crate) fn runtime(orch: &Orchestrator) -> WorkflowRuntime {
    WorkflowRuntime::new(
        Arc::new(CoreWorkflowHost(orch.clone())),
        orch.db.local.clone(),
        orch.services.emitter.clone(),
        orch.config_dir.clone(),
        orch.effect_tx.clone(),
    )
}

struct CoreWorkflowHost(Orchestrator);

/// The project an execution belongs to, so its decision resolves offers in
/// that project's scope (its private providers and tier overrides).
async fn execution_project(
    orch: &Orchestrator,
    execution_id: &str,
) -> Result<Option<String>, String> {
    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id)
        .await
        .map_err(|error| error.to_string())?;
    db.query_opt_text(
        "SELECT project_id FROM executions WHERE id = ?1",
        (execution_id.to_string(),),
    )
    .await
    .map_err(|error| format!("failed to read execution {execution_id}: {error}"))
}

#[async_trait]
impl WorkflowHost for CoreWorkflowHost {
    fn database(&self, owner: WorkflowOwner<'_>) -> Result<Arc<LocalDb>, String> {
        crate::storage::run_db_blocking(|| async {
            match owner {
                WorkflowOwner::Execution(id) => {
                    crate::execution::routing::owning_db_for_execution(&self.0.db, id).await
                }
                WorkflowOwner::Job(id) => {
                    crate::execution::routing::owning_db_for_job(&self.0.db, id).await
                }
                WorkflowOwner::ActionRun(id) => {
                    crate::execution::routing::owning_db_for_action_run(&self.0.db, id).await
                }
                WorkflowOwner::Issue(id) => {
                    crate::issues::crud::owning_db_for_issue(&self.0.db, id).await
                }
            }
            .map_err(|error| error.to_string())
        })
    }
    async fn databases(&self) -> Vec<Arc<LocalDb>> {
        self.0.db.all_dbs().await
    }
    fn claimed_launch_job_ids(&self) -> HashSet<String> {
        self.0.claimed_launch_job_ids()
    }
    fn resolve_coordinate(&self, repository: &Path, branch: &str) -> Result<String, String> {
        let jj_binary_path = self.0.jj_binary_path.clone();
        let config_dir = self.0.config_dir.clone();
        let project_repo = repository.to_path_buf();
        let coordinate_repository = run_advancement_db(async move {
            tokio::task::spawn_blocking(move || {
                let jj = crate::jj::environment(&jj_binary_path, &config_dir);
                crate::jj::coordinate_repository(&jj, &config_dir, &project_repo)
            })
            .await
            .map_err(|error| format!("checkpoint coordinate repository task failed: {error}"))?
        })?;
        let branch = branch.to_owned();
        run_advancement_db(async move {
            cairn_vcs::resolve_coordinate(&coordinate_repository, &branch)
                .await
                .map_err(|error| format!("Checkpoint branch '{branch}' is unresolvable: {error}"))
        })
    }
    fn job_head(&self, job_id: &str) -> Result<String, String> {
        crate::execution::cache::get_job_logical_head_sha(&self.0, job_id)
    }
    fn pr_publish_input_revision(&self, action_run_id: &str) -> Result<Option<String>, String> {
        let orch = self.0.clone();
        let id = action_run_id.to_owned();
        run_advancement_db(async move {
            let action =
                crate::execution::actions::get_action_run_for_orchestrator(&orch, &id).await?;
            crate::execution::actions::pr_publish_input_revision(&orch, &action).await
        })
    }
    async fn decide(
        &self,
        request: DecisionRequest,
    ) -> Result<crate::models::decision::Decision, String> {
        let project_id = execution_project(&self.0, &request.execution_id).await?;
        self.0
            .decide(project_id.as_deref(), &request.decision, request.input)
            .await
            .map_err(|error| {
                format!(
                    "Decision node '{}' could not be answered: {error}",
                    request.node_name
                )
            })
    }
    fn lifecycle_message(&self, job_id: &str, run_id: Option<&str>, event: JobEvent) {
        crate::messages::system::emit_job_event(&self.0, job_id, run_id, event);
    }
    fn projection_changed(&self, execution_id: &str, outcome: &RecomputeOutcome) {
        publish_projection(&self.0, execution_id, outcome);
    }
    fn condition_evaluated(
        &self,
        execution_id: &str,
        node_id: &str,
        issue_id: Option<&str>,
        outcome: crate::models::ConditionOutcome,
    ) {
        // A blocked condition is a gate on the user exactly like a blocked
        // checkpoint, but no job turns or lifecycle message announces it, so
        // the attention item is emitted here at the boundary.
        if outcome == crate::models::ConditionOutcome::Blocked {
            if let (Some(issue_id), Ok(handle)) = (issue_id, tokio::runtime::Handle::try_current())
            {
                let orch = self.0.clone();
                let issue_id = issue_id.to_string();
                handle.spawn(async move { orch.wake_for_issue(&issue_id).await });
            }
        }
        let condition_ref = format!("{execution_id}/{node_id}");
        let fact_kind = match outcome {
            crate::models::ConditionOutcome::Routed => "evaluated",
            crate::models::ConditionOutcome::Blocked => "blocked",
        };
        if let Err(error) = crate::orchestrator::wakes::route_condition_event(
            &self.0,
            &condition_ref,
            fact_kind,
            None,
        ) {
            log::warn!("failed to route condition wake for {condition_ref}: {error}");
        }
    }
    fn continue_job(&self, job_id: &str, message: &str) -> Result<(), String> {
        crate::execution::jobs::continue_job_impl(&self.0, job_id, Some(message), None, None)
            .map(|_| ())
    }
    fn resume_parent(&self, job_id: &str) -> Result<(), String> {
        crate::execution::delegation::resume_suspended_parent_after_task_completion(&self.0, job_id)
            .map(|_| ())
    }
    fn run_error(&self, run_id: &str, error: &str) {
        crate::orchestrator::session::insert_error_event(&self.0, run_id, None, error);
    }
    async fn checkpoint(
        &self,
        job_id: &str,
        node_name: &str,
        command: &str,
    ) -> Result<ExecutedCheckpoint, String> {
        crate::execution::conditions::execute_programmatic_checkpoint(
            &self.0, job_id, node_name, command,
        )
        .await
    }
    async fn execute(&self, workload: Workload) -> Result<Option<EffectResult>, String> {
        let effect = match workload {
            Workload::AgentJobs(jobs) => WorkflowEffect::StartAgentJobs(jobs),
            Workload::Action {
                action_run_id,
                execution_id,
                node_id,
                ctx,
            } => WorkflowEffect::ExecuteAction {
                action_run_id,
                execution_id,
                node_id,
                ctx,
            },
        };
        if let Some(executor) = self.0.executor.get() {
            executor.execute(&self.0, effect).await
        } else {
            log::error!("No executor configured — cannot execute {effect:?}");
            Ok(None)
        }
    }
}
fn publish_projection(orch: &Orchestrator, execution_id: &str, outcome: &RecomputeOutcome) {
    // One scoped jobs event per changed job; the frontend's invalidation batch
    // dedupes them into the minimal scoped set. Executions/issues stay broad.
    for change in &outcome.changes {
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::job_db_change_ids(
                "update",
                &change.job_id,
                change.issue_id.as_deref(),
                None,
                Some(execution_id),
                change.parent_job_id.as_deref(),
                change.parent_tool_use_id.as_deref(),
                &change.project_id,
            ),
        );
    }
    for table in ["executions", "issues"] {
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": table, "action": "update"}),
        );
    }

    // Everything below is an effect of a job's own facts moving. A job settling
    // onto a merged or closed issue moved for no such reason — the resolution
    // already announced itself — so it emits only the db-change above: no
    // lifecycle message, wake, JobEnded, or review re-evaluation. Without this
    // the boot repair would announce a completion for every job it settles.
    let changes: Vec<&cairn_workflow::advancement::JobStatusChange> = outcome
        .changes
        .iter()
        .filter(|change| change.is_announced())
        .collect();

    // Recompute is a pure projection-write; it no longer broadcasts a coarse
    // "something changed" poke. The fact-driven emit sites (artifact write,
    // turn-end, webhook PR state, manual issue close) carry their own typed
    // events. The one thing recompute is still responsible for here is
    // surfacing a *terminal* status flip that wasn't already signaled by the
    // calling fact site — e.g. a cascade-failure that closes the issue. Emit
    // one `Resolved` event for each issue this sweep moved to a terminal
    // status; non-terminal flips stay silent.
    // Only the transaction that crossed the terminal boundary owns this
    // announcement. Recovery can touch old terminal issues without resolving
    // them again (CAIRN-4756).
    for issue_id in &outcome.newly_terminal_issue_ids {
        let dbs = orch.db.clone();
        let issue_id_owned = issue_id.clone();
        let lookup = run_advancement_db(async move {
            let db = crate::issues::crud::owning_db_for_issue(&dbs, &issue_id_owned)
                .await
                .map_err(|e| e.to_string())?;
            crate::orchestrator::attention::read_issue_for_attention(&db, &issue_id_owned).await
        });
        if let Ok(ctx) = lookup {
            // The issue may have reopened since the transaction committed.
            if ctx.status.is_terminal() {
                orch.emit_attention_event(crate::orchestrator::AttentionEvent {
                    issue_id: issue_id.clone(),
                    issue_uri: ctx.issue_uri(),
                    fact: crate::orchestrator::AttentionFact::Resolved {
                        final_status: ctx.status.clone(),
                    },
                    attention: ctx.attention,
                    status: ctx.status,
                    updated_at: ctx.updated_at,
                    route_provenance: None,
                });
            }
        }
    }

    for change in &changes {
        match change.to {
            JobStatus::Complete => {
                match crate::memories::commands::confirm_drafts_for_completed_job_without_artifact_review(
                    orch,
                    &change.job_id,
                ) {
                    Ok(completion) if completion.confirmed_count > 0 => {
                        log::info!(
                            "Confirmed {} draft memor{} for completed no-artifact job {}",
                            completion.confirmed_count,
                            if completion.confirmed_count == 1 { "y" } else { "ies" },
                            &change.job_id[..change.job_id.len().min(8)]
                        );
                        let triage_orch = orch.clone();
                        let confirmed_scopes = completion.confirmed_scopes.clone();
                        tokio::spawn(async move {
                            if let Err(error) = crate::memories::triage::maybe_spawn_triage(
                                triage_orch,
                                confirmed_scopes,
                            )
                            .await
                            {
                                log::warn!("memory triage check after no-artifact confirmation failed: {error}");
                            }
                        });
                    }
                    Ok(_) => {}
                    Err(error) => log::warn!(
                        "Failed to run no-artifact draft-memory safety net for completed job {}: {error}",
                        &change.job_id[..change.job_id.len().min(8)]
                    ),
                }
                crate::messages::system::emit_job_event(
                    orch,
                    &change.job_id,
                    None,
                    crate::messages::system::JobEvent::Completed,
                );
            }
            JobStatus::Failed => {
                crate::messages::system::emit_job_event(
                    orch,
                    &change.job_id,
                    None,
                    crate::messages::system::JobEvent::Failed,
                );
            }
            JobStatus::Blocked => {
                if let Some(reason) = &change.blocked_reason {
                    log::info!(
                        "Job {} blocked without lifecycle message: {}",
                        &change.job_id[..change.job_id.len().min(8)],
                        reason
                    );
                }
                if change.emits_blocked_lifecycle_message() {
                    crate::messages::system::emit_job_event(
                        orch,
                        &change.job_id,
                        None,
                        crate::messages::system::JobEvent::Blocked { reason: None },
                    );
                }
            }
            _ => {}
        }
        if change.fires_job_ended() {
            let _ = orch.trigger_events.send(TriggerEvent::JobEnded {
                job_id: change.job_id.clone(),
                status: change.to.to_string(),
                execution_id: Some(execution_id.to_string()),
                issue_id: change.issue_id.clone(),
                project_id: change.project_id.clone(),
            });
        }
    }

    // CAIRN-2483 review-readiness hook: when a job of an issue stops working, the
    // whole child issue may have just gone quiescent. Evaluate review readiness for
    // each such issue — this is the edge that usually fires LAST for a planbuild
    // child (the review agent completing), and it generalizes to any recipe shape
    // (e.g. a build-only child settling after its builder terminalizes and the pr
    // action completes). The evaluator is fully gated and deduped, so
    // re-evaluation is a cheap idempotent no-op when nothing settled.
    //
    // This hook is the COVERING edge, and it has to be: every other edge covers
    // strictly less. The PR-open edge needs a PR, which a plan gate does not have,
    // and the turn-end check cadence runs only for branches that ship one
    // (CAIRN-3334), which a planner's does not. When `settles_for_review` omitted `Blocked`, a planbuild
    // child's plan gate matched no edge at all and its coordinator was never told
    // (CAIRN-3347).
    let review_issue_ids: std::collections::HashSet<String> = changes
        .iter()
        .filter(|change| change.settles_for_review())
        .filter_map(|change| change.issue_id.clone())
        .collect();
    for issue_id in review_issue_ids {
        let orch_for_review = orch.clone();
        crate::orchestrator::lifecycle::detach_onto_runtime(
            async move {
                crate::orchestrator::lifecycle::evaluate_review_readiness(
                    &orch_for_review,
                    &issue_id,
                )
                .await;
            },
            || {},
        );
    }
}
