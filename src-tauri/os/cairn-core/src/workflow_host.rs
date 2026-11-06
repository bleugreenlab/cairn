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
    runtime::{
        DecisionRequest, ParentResume, WorkflowHost, WorkflowOwner, WorkflowRuntime, Workload,
    },
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

/// Replay missed failed-resolution notices in every currently mounted owning
/// database. The normal attention creator rechecks identity under its push write
/// transaction, so startup can race safely with live resolution emitters.
pub async fn rearm_failed_resolutions_on_startup(orch: &Orchestrator) -> usize {
    let attention = crate::attention_host::runtime(orch);
    let mut created = 0;
    for db in orch.db.all_dbs().await {
        match cairn_attention::delivery::rearm_failed_resolutions(&attention, &db).await {
            Ok(count) => created += count,
            Err(error) => log::warn!("failed resolution startup re-arm: {error}"),
        }
    }
    created
}

struct CoreWorkflowHost(Orchestrator);

/// The project an execution belongs to, so its decision resolves offers in
/// that project's scope (its private providers and model pools).
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
    async fn pr_publish_input_revision(
        &self,
        action_run_id: &str,
    ) -> Result<Option<String>, String> {
        let action =
            crate::execution::actions::get_action_run_for_orchestrator(&self.0, action_run_id)
                .await?;
        crate::execution::actions::pr_publish_input_revision(&self.0, &action).await
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
    fn resume_parent(&self, job_id: &str, mode: ParentResume) -> Result<(), String> {
        match mode {
            ParentResume::Live => {
                crate::execution::delegation::resume_suspended_parent_after_task_completion(
                    &self.0, job_id,
                )
            }
            ParentResume::Repair => {
                crate::execution::delegation::resume_parked_parent_after_repair(&self.0, job_id)
            }
        }
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
    async fn execution_snapshot_guard(&self, execution_id: &str) -> Box<dyn std::any::Any + Send> {
        Box::new(self.0.execution_lock(execution_id).lock_owned().await)
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
    let project_id = outcome.project_id.as_deref().or_else(|| {
        outcome
            .changes
            .first()
            .map(|change| change.project_id.as_str())
    });
    match project_id {
        Some(project_id) => {
            let issue_id = outcome.issue_id.as_deref();
            let _ = orch.services.emitter.emit(
                "db-change",
                crate::notify::execution_db_change_ids(
                    "update",
                    execution_id,
                    issue_id,
                    project_id,
                ),
            );
            if let Some(issue_id) = issue_id {
                let _ = orch.services.emitter.emit(
                    "db-change",
                    crate::notify::issue_db_change_ids("update", issue_id, Some(project_id)),
                );
            }
        }
        // Unreachable for a real row: a change names its job's project, and a
        // terminal edge names an issue, which always has one.
        None => log::warn!(
            "execution {execution_id} recomputed with no project placement; not announced"
        ),
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
        orch.spawn_detached(async move {
            crate::orchestrator::lifecycle::evaluate_review_readiness(&orch_for_review, &issue_id)
                .await;
        });
    }
}

#[cfg(test)]
mod failed_resolution_integration_tests {
    use super::*;
    use crate::models::{
        ExecutionSnapshot, NodePosition, RecipeEdge, RecipeEdgeType, RecipeNode, RecipeNodeType,
        RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
    };
    use crate::storage::{RowExt, SearchIndex};

    fn node(id: &str, node_type: RecipeNodeType) -> RecipeNode {
        RecipeNode {
            id: id.into(),
            name: id.into(),
            node_type,
            position: NodePosition { x: 0.0, y: 0.0 },
            parent_id: None,
            trigger_config: None,
            agent_config: None,
            action_config: None,
            checkpoint_config: None,
            artifact_config: None,
            condition_config: None,
            decision_config: None,
            context_config: None,
        }
    }

    fn snapshot() -> String {
        let edges = [
            ("planner", "output", "builder"),
            ("builder", "output", "pr"),
            ("pr", "open", "review"),
        ]
        .into_iter()
        .map(|(from, handle, to)| RecipeEdge {
            id: format!("{from}-{to}"),
            edge_type: RecipeEdgeType::Control,
            source_node_id: from.into(),
            source_handle: handle.into(),
            target_node_id: to.into(),
            target_handle: "input".into(),
        })
        .collect();
        ExecutionSnapshot::new(
            RecipeSnapshot {
                id: "r".into(),
                name: "Failure recovery".into(),
                description: None,
                trigger: RecipeTrigger::Manual,
                nodes: vec![
                    node("planner", RecipeNodeType::Agent),
                    node("builder", RecipeNodeType::Agent),
                    node("pr", RecipeNodeType::Pr),
                    node("review", RecipeNodeType::Agent),
                ],
                edges,
            },
            Default::default(),
            Default::default(),
            TriggerContext {
                issue_id: Some("child".into()),
                project_id: "p".into(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        )
        .to_json()
        .unwrap()
    }

    async fn orchestrator() -> Orchestrator {
        let db = crate::storage::migrated_test_db("resolved-composed").await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('p','w','P','proj','/tmp',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
             VALUES('parent','p',1,'Parent','active','active','none',1,1),
                   ('child','p',2,'Child','active','active','none',1,1);
             INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
             VALUES('exec','r','child','p','running',1,1);
             INSERT INTO jobs(id,project_id,issue_id,execution_id,recipe_node_id,uri_segment,status,created_at,updated_at)
             VALUES('planner','p','child','exec','planner','planner','complete',1,1),
                   ('builder','p','child','exec','builder','builder','running',1,1),
                   ('review','p','child','exec','review','review','pending',1,1);
             UPDATE jobs SET parent_job_id='planner' WHERE id='builder';
             INSERT INTO jobs(id,project_id,issue_id,uri_segment,status,created_at,updated_at)
             VALUES('watcher','p','parent','watcher','running',1,1);
             UPDATE issues SET parent_issue_id='parent',parent_job_id='watcher' WHERE id='child';
             INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at)
             VALUES('planner-turn','planner-session','planner',1,'complete','initial',1,1),
                   ('builder-turn','builder-session','builder',1,'running','initial',1,1),
                   ('watcher-turn','watcher-session','watcher',1,'running','initial',1,1);
             INSERT INTO wake_subscriptions(id,job_id,source_kind,source_ref,state,created_by,created_at,updated_at)
             VALUES('watch','watcher','issue','cairn://p/proj/2','active','agent',1,1);"
        ).await.unwrap();
        db.execute(
            "UPDATE executions SET snapshot=?1 WHERE id='exec'",
            (snapshot(),),
        )
        .await
        .unwrap();
        orchestrator_for_db(db)
    }

    fn orchestrator_for_db(db: LocalDb) -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let state = Arc::new(crate::db::DbState::new(Arc::new(db), search));
        let services = Arc::new(crate::services::testing::TestServicesBuilder::new().build());
        crate::orchestrator::OrchestratorBuilder::new(state, services, config_dir).build()
    }

    async fn restart_before_projection(orch: Orchestrator) -> Orchestrator {
        let path = orch.db.local.path().to_path_buf();
        drop(orch);
        orchestrator_for_db(LocalDb::open(path).await.unwrap())
    }

    async fn project_failure(orch: &Orchestrator) -> RecomputeOutcome {
        orch.db
            .local
            .write(|conn| {
                Box::pin(async move {
                    cairn_workflow::advancement::recompute_execution_jobs_conn(
                        conn,
                        "exec",
                        &HashSet::new(),
                    )
                    .await
                })
            })
            .await
            .unwrap()
    }

    async fn assert_live_gap_notice(mixed_action: bool) {
        for restart in [false, true] {
            for heal_first in [false, true] {
                let mut orch = orchestrator().await;
                // The turn writer commits before the separate job projection.
                // A failed node-less task independently makes this execution a heal candidate.
                orch.db.local.execute_script(
                    "UPDATE turns SET state='failed', failure_reason='Provider failed before projection'
                     WHERE id='builder-turn';
                     INSERT INTO jobs(id,project_id,issue_id,execution_id,parent_job_id,uri_segment,status,created_at,updated_at)
                     VALUES('delegated','p','child','exec','builder','delegated','failed',1,1);"
                ).await.unwrap();
                if mixed_action {
                    orch.db.local.execute_script(
                        "INSERT INTO action_runs(id,execution_id,recipe_node_id,action_config_id,issue_id,project_id,status,error_message,created_at)
                         VALUES('historical-pr','exec','pr','builtin:pr','child','p','failed','Old publication failure',1);"
                    ).await.unwrap();
                }
                if restart {
                    orch = restart_before_projection(orch).await;
                }
                assert_eq!(orch.db.local.query_one(
                    "SELECT j.status,t.state FROM jobs j JOIN turns t ON t.job_id=j.id WHERE j.id='builder'",
                    (), |row| Ok((row.text(0)?, row.text(1)?)),
                ).await.unwrap(), ("running".into(), "failed".into()));
                assert_eq!(
                    cairn_db::storage::failed_dependency_candidates(&orch.db.local, "", 32,)
                        .await
                        .unwrap(),
                    ["exec"]
                );

                if heal_first {
                    // Production boot heals before ordinary re-projection and re-arm.
                    cairn_workflow::advancement::recompute::heal_failed_dependencies(&runtime(
                        &orch,
                    ))
                    .await;
                }
                let outcome = project_failure(&orch).await;
                if !heal_first {
                    assert_eq!(outcome.newly_terminal_issue_ids, ["child"]);
                }
                let state = orch.db.local.query_one(
                    "SELECT status,resolution_generation,quiet_failure_generation FROM issues WHERE id='child'",
                    (), |row| Ok((row.text(0)?, row.i64(1)?, row.opt_i64(2)?)),
                ).await.unwrap();
                assert_eq!(
                    state,
                    ("failed".into(), 0, None),
                    "restart={restart}, heal_first={heal_first}, mixed_action={mixed_action}"
                );
                for id in ["builder", "review"] {
                    assert_eq!(
                        orch.db
                            .local
                            .query_opt_text("SELECT status FROM jobs WHERE id=?1", (id,),)
                            .await
                            .unwrap()
                            .as_deref(),
                        Some("failed")
                    );
                }
                if restart {
                    // Startup re-arm may beat the normal post-commit emitter.
                    assert_eq!(
                        rearm_failed_resolutions_on_startup(&orch).await,
                        usize::from(!heal_first)
                    );
                }
                publish_projection(&orch, "exec", &outcome);
                cairn_workflow::advancement::recompute::heal_failed_dependencies(&runtime(&orch))
                    .await;
                assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
                let pending = cairn_attention::push::list_pending(&orch.db.local, "watcher")
                    .await
                    .unwrap();
                assert_eq!(pending.len(), 1);
                assert_eq!(pending[0].key, "resolved:cairn://p/proj/2");
                assert_eq!(pending[0].wake, cairn_attention::push::Wake::Wake);
                let rendered = cairn_attention::delivery::render_push_resolved(
                    &crate::attention_host::runtime(&orch),
                    &pending[0],
                )
                .await;
                assert!(
                    rendered.contains("Provider failed before projection"),
                    "{rendered}"
                );
                assert!(
                    rendered.contains("cairn://p/proj/2/1/builder"),
                    "{rendered}"
                );
                cairn_attention::push::stamp_delivered(
                    &orch.db.local,
                    &[pending[0].id.clone()],
                    "gap-carrier",
                )
                .await
                .unwrap();
                publish_projection(&orch, "exec", &outcome);
                let replay = project_failure(&orch).await;
                publish_projection(&orch, "exec", &replay);
                cairn_workflow::advancement::recompute::heal_failed_dependencies(&runtime(&orch))
                    .await;
                assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
                assert!(
                    cairn_attention::push::list_pending(&orch.db.local, "watcher")
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert_eq!(orch.db.local.query_one(
                    "SELECT COUNT(*) FROM attention_pushes WHERE recipient='watcher' AND key='resolved:cairn://p/proj/2'",
                    (), |row| row.i64(0),
                ).await.unwrap(), 1);
            }
        }
    }

    #[tokio::test]
    async fn unprojected_failed_turn_heal_and_restart_deliver_one_resolved() {
        assert_live_gap_notice(false).await;
    }

    #[tokio::test]
    async fn mixed_historical_action_and_unprojected_turn_deliver_one_resolved() {
        assert_live_gap_notice(true).await;
    }

    #[tokio::test]
    async fn historical_action_only_heals_quietly_across_restart_and_rearm() {
        let orch = orchestrator().await;
        orch.db.local.execute_script(
            "UPDATE turns SET state='complete' WHERE id='builder-turn';
             UPDATE jobs SET status='complete' WHERE id='builder';
             INSERT INTO jobs(id,project_id,issue_id,execution_id,parent_job_id,uri_segment,status,created_at,updated_at)
             VALUES('delegated','p','child','exec','builder','delegated','failed',1,1);
             INSERT INTO action_runs(id,execution_id,recipe_node_id,action_config_id,issue_id,project_id,status,error_message,created_at)
             VALUES('historical-pr','exec','pr','builtin:pr','child','p','failed','Old publication failure',1);"
        ).await.unwrap();
        let orch = restart_before_projection(orch).await;
        assert_eq!(
            cairn_db::storage::failed_dependency_candidates(&orch.db.local, "", 32,)
                .await
                .unwrap(),
            ["exec"]
        );
        cairn_workflow::advancement::recompute::heal_failed_dependencies(&runtime(&orch)).await;
        let state = orch.db.local.query_one(
            "SELECT status,resolution_generation,quiet_failure_generation FROM issues WHERE id='child'",
            (), |row| Ok((row.text(0)?, row.i64(1)?, row.opt_i64(2)?)),
        ).await.unwrap();
        assert_eq!(state, ("failed".into(), 0, Some(0)));
        assert_eq!(
            orch.db
                .local
                .query_opt_text("SELECT status FROM jobs WHERE id='review'", (),)
                .await
                .unwrap()
                .as_deref(),
            Some("failed")
        );
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
        let outcome = project_failure(&orch).await;
        assert!(outcome.newly_terminal_issue_ids.is_empty());
        publish_projection(&orch, "exec", &outcome);
        let orch = restart_before_projection(orch).await;
        cairn_workflow::advancement::recompute::heal_failed_dependencies(&runtime(&orch)).await;
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
        assert_eq!(orch.db.local.query_one(
            "SELECT COUNT(*) FROM attention_pushes WHERE recipient='watcher' AND key='resolved:cairn://p/proj/2'",
            (), |row| row.i64(0),
        ).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn failure_transaction_to_parent_resolved_replays_once_across_delivery() {
        for startup_first in [false, true] {
            let orch = orchestrator().await;
            let (execution, outcome) = orch
                .db
                .local
                .write(|conn| {
                    Box::pin(async move {
                        let projection = cairn_workflow::advancement::record_job_failure_conn(
                            conn,
                            "builder",
                            "Provider launch rejected: executable unavailable",
                            &HashSet::new(),
                        )
                        .await?
                        .expect("a bound job has an execution");
                        // Assert the whole invariant BEFORE returning from the transaction.
                        for (table, id) in [
                            ("jobs", "builder"),
                            ("jobs", "review"),
                            ("executions", "exec"),
                            ("issues", "child"),
                        ] {
                            let mut rows = conn
                                .query(format!("SELECT status FROM {table} WHERE id=?1"), (id,))
                                .await?;
                            assert_eq!(
                                rows.next().await?.unwrap().text(0)?,
                                "failed",
                                "{table}/{id}"
                            );
                        }
                        Ok(projection)
                    })
                })
                .await
                .unwrap();
            assert_eq!(outcome.newly_terminal_issue_ids, vec!["child".to_string()]);
            assert!(
                cairn_attention::push::list_pending(&orch.db.local, "watcher")
                    .await
                    .unwrap()
                    .is_empty(),
                "settlement committed without its post-commit emitter"
            );
            if startup_first {
                assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 1);
            }
            publish_projection(&orch, &execution, &outcome);
            assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
            let pending = cairn_attention::push::list_pending(&orch.db.local, "watcher")
                .await
                .unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].key, "resolved:cairn://p/proj/2");
            assert_eq!(pending[0].wake, cairn_attention::push::Wake::Wake);
            let rendered = cairn_attention::delivery::render_push_resolved(
                &crate::attention_host::runtime(&orch),
                &pending[0],
            )
            .await;
            assert!(
                rendered.contains("cairn://p/proj/2/1/builder"),
                "{rendered}"
            );
            assert!(
                rendered.contains("Provider launch rejected: executable unavailable"),
                "{rendered}"
            );
            assert!(!rendered.contains("/review"), "{rendered}");
            cairn_attention::push::stamp_delivered(
                &orch.db.local,
                &[pending[0].id.clone()],
                "carrying-event",
            )
            .await
            .unwrap();
            publish_projection(&orch, &execution, &outcome);
            assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
            assert!(
                cairn_attention::push::list_pending(&orch.db.local, "watcher")
                    .await
                    .unwrap()
                    .is_empty()
            );
            let count = orch.db.local.read(|conn| Box::pin(async move {
                let mut rows = conn.query(
                    "SELECT COUNT(*) FROM attention_pushes WHERE recipient='watcher' AND key='resolved:cairn://p/proj/2'", ()
                ).await?;
                rows.next().await?.unwrap().i64(0)
            })).await.unwrap();
            assert_eq!(count, 1);
        }
    }

    #[tokio::test]
    async fn quiet_failure_live_retry_and_rearm_create_exactly_one_fresh_fact() {
        let orch = orchestrator().await;
        orch.db
            .local
            .execute_script(
                "UPDATE turns SET state='failed' WHERE id='builder-turn';
             UPDATE jobs SET status='failed' WHERE id='builder';",
            )
            .await
            .unwrap();
        let quiet = orch
            .db
            .local
            .write(|conn| {
                Box::pin(async move {
                    cairn_workflow::advancement::recompute::heal_failed_dependencies_conn(
                        conn,
                        "exec",
                        &HashSet::new(),
                    )
                    .await
                })
            })
            .await
            .unwrap();
        assert!(quiet.newly_terminal_issue_ids.is_empty());
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
        orch.db.local.execute_script(
            "INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at)
             VALUES('retry','builder-session','builder',2,'running','retry',4,4);"
        ).await.unwrap();
        orch.db
            .local
            .write(|conn| {
                Box::pin(async move {
                    cairn_workflow::advancement::recompute_execution_jobs_conn(
                        conn,
                        "exec",
                        &HashSet::new(),
                    )
                    .await
                })
            })
            .await
            .unwrap();
        let reopened = orch.db.local.query_one(
            "SELECT resolution_generation, completed_at, quiet_failure_generation FROM issues WHERE id='child'",
            (), |row| Ok((row.i64(0)?, row.opt_i64(1)?, row.opt_i64(2)?)),
        ).await.unwrap();
        assert_eq!(reopened, (1, None, Some(0)));
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
        let (_, outcome) = orch
            .db
            .local
            .write(|conn| {
                Box::pin(async move {
                    cairn_workflow::advancement::record_job_failure_conn(
                        conn,
                        "builder",
                        "Retry independently failed",
                        &HashSet::new(),
                    )
                    .await
                })
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outcome.newly_terminal_issue_ids, ["child"]);
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 1);
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
        let pending = cairn_attention::push::list_pending(&orch.db.local, "watcher")
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].key, "resolved:cairn://p/proj/2");
        cairn_attention::push::stamp_delivered(
            &orch.db.local,
            &[pending[0].id.clone()],
            "retry-carrier",
        )
        .await
        .unwrap();
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
        let total = orch.db.local.query_one(
            "SELECT COUNT(*) FROM attention_pushes WHERE recipient='watcher' AND key='resolved:cairn://p/proj/2'",
            (), |row| row.i64(0),
        ).await.unwrap();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn explicit_failed_unresolve_advances_resolution_identity_exactly_once() {
        let orch = orchestrator().await;
        orch.db
            .local
            .execute_script(
                "UPDATE issues SET status='failed', progress='failed', completed_at=42,
             quiet_failure_generation=0 WHERE id='child';",
            )
            .await
            .unwrap();
        crate::issues::crud::unresolve(&orch.db.local, &crate::services::RealClock, "child")
            .await
            .unwrap();
        let state = orch.db.local.query_one(
            "SELECT status, resolution_generation, completed_at, quiet_failure_generation FROM issues WHERE id='child'",
            (), |row| Ok((row.text(0)?, row.i64(1)?, row.opt_i64(2)?, row.opt_i64(3)?)),
        ).await.unwrap();
        assert_eq!(state, ("active".into(), 1, None, Some(0)));
        orch.db
            .local
            .write(|conn| {
                Box::pin(async move {
                    cairn_workflow::transitions::outcome::recompute_issue_status_conn(conn, "child")
                        .await
                })
            })
            .await
            .unwrap();
        let generation = orch
            .db
            .local
            .query_one(
                "SELECT resolution_generation FROM issues WHERE id='child'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(generation, 1);
    }

    #[tokio::test]
    async fn explicit_unresolve_lifting_cancelled_parallel_work_advances_identity_once() {
        let orch = orchestrator().await;
        let mut independent: serde_json::Value = serde_json::from_str(&snapshot()).unwrap();
        // Review is an independent branch, not a descendant of the failed builder.
        independent["recipe"]["edges"]
            .as_array_mut()
            .unwrap()
            .retain(|edge| edge["targetNodeId"] != "review");
        orch.db
            .local
            .execute(
                "UPDATE executions SET snapshot=?1, status='failed' WHERE id='exec'",
                (independent.to_string(),),
            )
            .await
            .unwrap();
        orch.db
            .local
            .execute_script(
                "UPDATE turns SET state='failed' WHERE id='builder-turn';
             UPDATE jobs SET status='failed' WHERE id='builder';
             UPDATE jobs SET status='cancelled' WHERE id='review';
             UPDATE issues SET status='closed', progress='closed', closed_at=42,
             completed_at=42, quiet_failure_generation=0 WHERE id='child';",
            )
            .await
            .unwrap();
        crate::issues::crud::unresolve(&orch.db.local, &crate::services::RealClock, "child")
            .await
            .unwrap();
        let state = orch.db.local.query_one(
            "SELECT status, resolution_generation, completed_at, quiet_failure_generation FROM issues WHERE id='child'",
            (), |row| Ok((row.text(0)?, row.i64(1)?, row.opt_i64(2)?, row.opt_i64(3)?)),
        ).await.unwrap();
        assert_eq!(state, ("active".into(), 1, None, Some(0)));
        assert_eq!(
            orch.db
                .local
                .query_opt_text("SELECT status FROM jobs WHERE id='review'", (),)
                .await
                .unwrap()
                .as_deref(),
            Some("pending")
        );
        let (_, failed) = orch
            .db
            .local
            .write(|conn| {
                Box::pin(async move {
                    cairn_workflow::advancement::record_job_failure_conn(
                        conn,
                        "review",
                        "Independent reopened branch failed",
                        &HashSet::new(),
                    )
                    .await
                })
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(failed.newly_terminal_issue_ids, ["child"]);
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 1);
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
    }

    #[tokio::test]
    async fn startup_failed_resolution_rearm_visits_the_mounted_owning_database() {
        let orch = orchestrator().await;
        let team = Arc::new(crate::storage::migrated_test_db("resolved-team").await);
        team.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('teamA~00000000-0000-4000-8000-000000000001','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
             VALUES('teamA~00000000-0000-4000-8000-000000000002','teamA~00000000-0000-4000-8000-000000000001','P','shared','/tmp',1,1);
             INSERT INTO issues(id,project_id,number,title,status,progress,attention,completed_at,created_at,updated_at)
             VALUES('teamA~00000000-0000-4000-8000-000000000003','teamA~00000000-0000-4000-8000-000000000002',1,'Parent','active','active','none',NULL,1,1),
                   ('teamA~00000000-0000-4000-8000-000000000004','teamA~00000000-0000-4000-8000-000000000002',2,'Child','failed','complete','none',10,1,10);
             INSERT INTO jobs(id,project_id,issue_id,status,created_at,updated_at)
             VALUES('teamA~00000000-0000-4000-8000-000000000005','teamA~00000000-0000-4000-8000-000000000002','teamA~00000000-0000-4000-8000-000000000003','running',1,1);
             INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at)
             VALUES('teamA~00000000-0000-4000-8000-000000000006','teamA~00000000-0000-4000-8000-000000000007','teamA~00000000-0000-4000-8000-000000000005',1,'running','initial',1,1);
             INSERT INTO wake_subscriptions(id,job_id,source_kind,source_ref,state,created_by,created_at,updated_at)
             VALUES('teamA~00000000-0000-4000-8000-000000000008','teamA~00000000-0000-4000-8000-000000000005','issue','cairn://p/shared/2','active','agent',1,1);"
        ).await.unwrap();
        orch.db
            .register_team_db_for_test("teamA".into(), team.clone())
            .await;
        orch.db.set_route("shared", Some("teamA".into())).await;
        let owning = crate::issues::crud::owning_db_for_issue(
            &orch.db,
            "teamA~00000000-0000-4000-8000-000000000004",
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&owning, &team));
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 1);
        assert_eq!(
            cairn_attention::push::list_pending(
                &team,
                "teamA~00000000-0000-4000-8000-000000000005"
            )
            .await
            .unwrap()
            .len(),
            1
        );
        assert!(
            cairn_attention::push::list_pending(
                &orch.db.local,
                "teamA~00000000-0000-4000-8000-000000000005"
            )
            .await
            .unwrap()
            .is_empty(),
            "the notice belongs to the mounted owning database, never the private fallback"
        );
        assert_eq!(rearm_failed_resolutions_on_startup(&orch).await, 0);
    }
}
