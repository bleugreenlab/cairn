//! Unified step outcome reducer.
//!
//! Single authority for job terminal/blocked transitions + follow-on effects.
//! All callers that determine a job's outcome funnel through `apply_step_outcome`
//! to ensure consistent follow-on effects.
//!
//! `apply_step_outcome` returns `(StepOutcomeResult, Vec<WorkflowEffect>)`:
//! 1. `transition_job` (state machine, cascade, recompute exec/issue, db-change, TriggerEvent)
//! 2. `EmitLifecycleMessage` effect (system message for frontend)
//! 3. `WakeManager` effect (on failure)
//! 4. `AdvanceDag` effect (on complete with execution)
//!
//! Effects are collected but **not executed** — callers use
//! `emit_outcome_effects()` which dispatches through the effect_tx channel.

use crate::effects::types::WorkflowEffect;
use crate::models::JobStatus;
use crate::orchestrator::Orchestrator;
use crate::schema::jobs;
use diesel::prelude::*;
use diesel::Connection;

/// What happened to the step.
#[derive(Debug, Clone, PartialEq)]
pub enum StepOutcome {
    Complete,
    Failed,
    Blocked,
}

/// Who/what produced the outcome.
#[derive(Debug, Clone)]
pub enum OutcomeSource {
    /// Agent called the return tool
    Return,
    /// Backend process exited normally
    ProcessExit,
    /// Backend process crashed
    ProcessCrash,
    /// Checkpoint was approved by user
    CheckpointApproved,
    /// Checkpoint was rejected by user (no recovery edge)
    CheckpointRejected,
}

/// Context from the caller — extensible without reopening the signature.
#[derive(Debug)]
pub struct OutcomeContext<'a> {
    pub run_id: Option<&'a str>,
    pub source: OutcomeSource,
}

/// Result of applying an outcome.
#[derive(Debug)]
pub enum StepOutcomeResult {
    /// Outcome applied — state transitioned, effects collected
    Applied {
        previous_status: JobStatus,
        /// If Some, caller should advance this execution's DAG.
        /// Also represented in the returned effects vec as `AdvanceDag`.
        advance_execution_id: Option<String>,
    },
    /// Job was already terminal/blocked — no-op, no effects fired
    AlreadySettled { current_status: JobStatus },
}

/// Single authority for **applying** a decided step outcome's follow-on effects.
///
/// This unifies the effects that follow an outcome (transition, message, wake, DAG signal)
/// but does NOT decide the outcome itself — that stays with callers (`handle_return`
/// evaluates checkpoints, `finalize_run` checks checkpoint slots, etc.).
///
/// ## Idempotency
///
/// If the job is already Complete, Failed, or already in the target state, returns
/// `AlreadySettled` — no events emitted, no state changes. This handles the race where
/// `handle_return` settles the job and `finalize_run` arrives later for the same run.
///
/// **Settled job + later crash:** When `handle_return` completes a job and the process
/// subsequently crashes, `finalize_run` will call this with `StepOutcome::Failed` but get
/// `AlreadySettled` because the job is already Complete. This is intentional — the `return`
/// tool is the semantic source of truth for the job outcome. The crash is a runtime detail
/// handled by run/turn finalization in `finalize_run`, which runs before this function and
/// correctly records the run as Crashed and the turn as Failed.
///
/// ## When Applied
///
/// 1. `transition_job` (state machine, cascade, recompute exec/issue, db-change, TriggerEvent)
/// 2. System message — exactly once (lifecycle event for frontend)
/// 3. Manager wake on failure — exactly once
/// 4. Returns `advance_execution_id` — caller handles DAG advancement
///
/// `AlreadySettled` emits **nothing**: no lifecycle message, no manager wake, no DAG signal.
pub fn apply_step_outcome(
    orch: &Orchestrator,
    job_id: &str,
    ctx: &OutcomeContext,
    outcome: StepOutcome,
) -> Result<(StepOutcomeResult, Vec<WorkflowEffect>), String> {
    // Map outcome to target status
    let target = match outcome {
        StepOutcome::Complete => JobStatus::Complete,
        StepOutcome::Failed => JobStatus::Failed,
        StepOutcome::Blocked => JobStatus::Blocked,
    };

    // Single critical section: read current status, check idempotency, transition.
    // This prevents the TOCTOU race where two callers both observe "running",
    // one transitions to Complete, and the other hits an invalid-transition error.
    let (previous_status, execution_id, outbox_entry_id) = {
        let mut conn = orch
            .db
            .conn
            .lock()
            .map_err(|e| format!("Failed to lock database: {}", e))?;

        let (current_status_str, execution_id): (String, Option<String>) = jobs::table
            .find(job_id)
            .select((jobs::status, jobs::execution_id))
            .first(&mut *conn)
            .map_err(|e| format!("Job not found: {}", e))?;

        let current_status: JobStatus = current_status_str
            .parse()
            .map_err(|_| format!("Unparseable job status: {}", current_status_str))?;

        // Already in target state or terminal — no-op (prevents duplicate effects from racing callers).
        // Note: Blocked is NOT unconditionally settled because checkpoint approval/rejection
        // transitions from Blocked to Complete/Failed. But Blocked→Blocked would be a no-op.
        if current_status == target
            || matches!(current_status, JobStatus::Complete | JobStatus::Failed)
        {
            return Ok((StepOutcomeResult::AlreadySettled { current_status }, vec![]));
        }

        // Wrap transition + outbox write in an explicit transaction so both
        // commit or roll back together. This prevents the job completing
        // without a durable outbox entry (the exact crash gap this fixes).
        let (previous_status, outbox_entry_id) = conn
            .transaction::<_, diesel::result::Error, _>(|conn| {
                // 1. Transition job (validates state machine, cascades, recomputes,
                //    emits db-change + TriggerEvent)
                let previous_status = crate::transitions::transition_job(
                    conn,
                    &*orch.services.emitter,
                    job_id,
                    target.clone(),
                    &orch.trigger_events,
                )
                .map_err(|e| diesel::result::Error::QueryBuilderError(e.to_string().into()))?;

                // 1b. Persist outbox entry for AdvanceDag (crash-safe replay).
                let outbox_entry_id = if target == JobStatus::Complete {
                    if let Some(ref exec_id) = execution_id {
                        Some(crate::effects::outbox::insert_pending(
                            conn,
                            "advance_dag",
                            exec_id,
                        )?)
                    } else {
                        None
                    }
                } else {
                    None
                };

                Ok((previous_status, outbox_entry_id))
            })
            .map_err(|e| e.to_string())?;

        (previous_status, execution_id, outbox_entry_id)
    };
    // DB lock released — effects are collected but not executed.

    let mut effects = Vec::new();

    // 2. Lifecycle message (ordering: emits BEFORE DAG advancement)
    let event = match &target {
        JobStatus::Complete => Some(crate::messages::system::JobEvent::Completed),
        JobStatus::Failed => Some(crate::messages::system::JobEvent::Failed),
        JobStatus::Blocked => Some(crate::messages::system::JobEvent::Blocked),
        _ => None,
    };
    if let Some(event) = event {
        effects.push(WorkflowEffect::EmitLifecycleMessage {
            job_id: job_id.to_string(),
            run_id: ctx.run_id.map(|s| s.to_string()),
            event,
        });
    }

    // 3. Wake manager on failure (ordering: AFTER failure message)
    if target == JobStatus::Failed {
        effects.push(WorkflowEffect::WakeManager {
            job_id: job_id.to_string(),
        });
    }

    // 4. Signal DAG advancement for complete outcomes
    let advance_execution_id = if target == JobStatus::Complete {
        execution_id.clone()
    } else {
        None
    };

    if let Some(ref exec_id) = advance_execution_id {
        effects.push(WorkflowEffect::AdvanceDag {
            execution_id: exec_id.clone(),
            outbox_entry_id: outbox_entry_id.clone(),
        });
    }

    Ok((
        StepOutcomeResult::Applied {
            previous_status,
            advance_execution_id,
        },
        effects,
    ))
}

/// Emit effects from `apply_step_outcome`.
///
/// Lifecycle messages and manager wake are executed inline (sync-safe).
/// DAG advancement is sent through the `effect_tx` channel for the
/// async drainer task to handle.
pub fn emit_outcome_effects(orch: &Orchestrator, effects: Vec<WorkflowEffect>) {
    for effect in effects {
        match effect {
            WorkflowEffect::EmitLifecycleMessage {
                job_id,
                run_id,
                event,
            } => {
                crate::messages::system::emit_job_event(orch, &job_id, run_id.as_deref(), event);
            }
            WorkflowEffect::WakeManager { job_id } => {
                crate::orchestrator::lifecycle::wake_manager_on_failure(orch, &job_id);
            }
            WorkflowEffect::AdvanceDag {
                execution_id,
                outbox_entry_id,
            } => {
                if let Some(ref tx) = orch.effect_tx {
                    let _ = tx.send(WorkflowEffect::AdvanceDag {
                        execution_id: execution_id.clone(),
                        outbox_entry_id,
                    });
                } else {
                    log::error!(
                        "No effect_tx configured — cannot advance DAG for execution {}",
                        &execution_id[..execution_id.len().min(8)]
                    );
                }
            }
            _ => {
                log::warn!("Unexpected effect in emit_outcome_effects: {:?}", effect);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::NewJob;
    use crate::orchestrator::Orchestrator;
    use crate::schema::jobs;
    use crate::services::testing::{CapturingEmitter, TestServicesBuilder};
    use crate::test_utils::{create_test_issue, create_test_project, test_diesel_conn};
    use std::sync::{Arc, Mutex};

    fn test_orchestrator(conn: diesel::sqlite::SqliteConnection) -> Orchestrator {
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
            mcp_auth: Arc::new(crate::mcp::McpAuthState::new(std::path::PathBuf::from(
                "/tmp",
            ))),
            warm_gc: None,
            pty_state: Arc::new(crate::services::PtyState::default()),
            permission_responses: tokio::sync::broadcast::channel(16).0,
            run_completions: tokio::sync::broadcast::channel(64).0,
            prompt_responses: tokio::sync::broadcast::channel(16).0,
            trigger_events: tokio::sync::broadcast::channel(256).0,
            session_allowed_tools: Arc::new(Mutex::new(std::collections::HashSet::new())),
            identity_store: Arc::new(Mutex::new(None)),
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir: std::path::PathBuf::from("/tmp"),
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

    fn insert_job(
        conn: &mut diesel::sqlite::SqliteConnection,
        id: &str,
        status: &str,
        project_id: &str,
        issue_id: Option<&str>,
        execution_id: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_job = NewJob {
            id,
            execution_id,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status,
            agent_config_id: None,
            issue_id,
            project_id,
            task_description: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: if status == "running" { Some(now) } else { None },
            model: None,
            node_name: Some("Builder"),
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(conn)
            .unwrap();
    }

    fn make_ctx<'a>(run_id: Option<&'a str>, source: OutcomeSource) -> OutcomeContext<'a> {
        OutcomeContext { run_id, source }
    }

    // === Basic outcomes ===

    #[test]
    fn test_complete_outcome() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OC1");
        insert_job(&mut conn, "job-1", "running", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(Some("run-1"), OutcomeSource::Return);
        let (result, effects) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();

        match result {
            StepOutcomeResult::Applied {
                previous_status, ..
            } => assert_eq!(previous_status, JobStatus::Running),
            _ => panic!("Expected Applied"),
        }

        // Should produce a lifecycle message effect
        assert!(effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::EmitLifecycleMessage { .. })));

        let status: String = {
            let mut c = orch.db.conn.lock().unwrap();
            jobs::table
                .find("job-1")
                .select(jobs::status)
                .first(&mut *c)
                .unwrap()
        };
        assert_eq!(status, "complete");
    }

    #[test]
    fn test_failed_outcome() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OC2");
        insert_job(&mut conn, "job-1", "running", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::ProcessCrash);
        let (result, effects) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Failed).unwrap();

        match result {
            StepOutcomeResult::Applied {
                previous_status,
                advance_execution_id,
            } => {
                assert_eq!(previous_status, JobStatus::Running);
                assert!(advance_execution_id.is_none());
            }
            _ => panic!("Expected Applied"),
        }

        // Failed produces lifecycle + wake manager effects
        assert!(effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::EmitLifecycleMessage { .. })));
        assert!(effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::WakeManager { .. })));
        // No AdvanceDag for failed outcomes
        assert!(!effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::AdvanceDag { .. })));

        let status: String = {
            let mut c = orch.db.conn.lock().unwrap();
            jobs::table
                .find("job-1")
                .select(jobs::status)
                .first(&mut *c)
                .unwrap()
        };
        assert_eq!(status, "failed");
    }

    #[test]
    fn test_blocked_outcome() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OC3");
        insert_job(&mut conn, "job-1", "running", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::Return);
        let (result, effects) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Blocked).unwrap();

        match result {
            StepOutcomeResult::Applied {
                previous_status,
                advance_execution_id,
            } => {
                assert_eq!(previous_status, JobStatus::Running);
                assert!(advance_execution_id.is_none());
            }
            _ => panic!("Expected Applied"),
        }

        // Blocked produces lifecycle message but no AdvanceDag
        assert!(effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::EmitLifecycleMessage { .. })));
        assert!(!effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::AdvanceDag { .. })));

        let status: String = {
            let mut c = orch.db.conn.lock().unwrap();
            jobs::table
                .find("job-1")
                .select(jobs::status)
                .first(&mut *c)
                .unwrap()
        };
        assert_eq!(status, "blocked");
    }

    // === Idempotency / race tests ===

    #[test]
    fn test_already_complete_is_noop() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OC4");
        insert_job(&mut conn, "job-1", "complete", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::ProcessExit);
        let (result, effects) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();

        match result {
            StepOutcomeResult::AlreadySettled { current_status } => {
                assert_eq!(current_status, JobStatus::Complete);
            }
            _ => panic!("Expected AlreadySettled"),
        }
        assert!(
            effects.is_empty(),
            "AlreadySettled should produce no effects"
        );
    }

    #[test]
    fn test_handle_return_then_finalize_run() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OC5");
        insert_job(&mut conn, "job-1", "running", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx1 = make_ctx(Some("run-1"), OutcomeSource::Return);
        let (result1, _) =
            apply_step_outcome(&orch, "job-1", &ctx1, StepOutcome::Complete).unwrap();
        assert!(matches!(result1, StepOutcomeResult::Applied { .. }));

        let ctx2 = make_ctx(Some("run-1"), OutcomeSource::ProcessExit);
        let (result2, effects2) =
            apply_step_outcome(&orch, "job-1", &ctx2, StepOutcome::Complete).unwrap();
        assert!(matches!(result2, StepOutcomeResult::AlreadySettled { .. }));
        assert!(effects2.is_empty());
    }

    #[test]
    fn test_double_finalize_run() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OC6");
        insert_job(&mut conn, "job-1", "running", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(Some("run-1"), OutcomeSource::ProcessExit);
        let (result1, _) = apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();
        assert!(matches!(result1, StepOutcomeResult::Applied { .. }));

        let (result2, effects2) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();
        assert!(matches!(result2, StepOutcomeResult::AlreadySettled { .. }));
        assert!(effects2.is_empty());
    }

    #[test]
    fn test_checkpoint_approval_after_complete() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OC7");
        insert_job(&mut conn, "job-1", "complete", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::CheckpointApproved);
        let (result, effects) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();
        assert!(matches!(result, StepOutcomeResult::AlreadySettled { .. }));
        assert!(effects.is_empty());
    }

    #[test]
    fn test_return_complete_then_crash_stays_settled() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OCA");
        insert_job(&mut conn, "job-1", "running", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx1 = make_ctx(Some("run-1"), OutcomeSource::Return);
        let (result1, _) =
            apply_step_outcome(&orch, "job-1", &ctx1, StepOutcome::Complete).unwrap();
        assert!(matches!(result1, StepOutcomeResult::Applied { .. }));

        let ctx2 = make_ctx(Some("run-1"), OutcomeSource::ProcessCrash);
        let (result2, _) = apply_step_outcome(&orch, "job-1", &ctx2, StepOutcome::Failed).unwrap();
        match result2 {
            StepOutcomeResult::AlreadySettled { current_status } => {
                assert_eq!(current_status, JobStatus::Complete);
            }
            _ => panic!("Expected AlreadySettled, got Applied — crash would have overwritten the return outcome"),
        }

        let status: String = {
            let mut c = orch.db.conn.lock().unwrap();
            jobs::table
                .find("job-1")
                .select(jobs::status)
                .first(&mut *c)
                .unwrap()
        };
        assert_eq!(status, "complete");
    }

    #[test]
    fn test_blocked_then_blocked_is_noop() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OCB");
        insert_job(&mut conn, "job-1", "running", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx1 = make_ctx(Some("run-1"), OutcomeSource::Return);
        let (result1, _) = apply_step_outcome(&orch, "job-1", &ctx1, StepOutcome::Blocked).unwrap();
        assert!(matches!(result1, StepOutcomeResult::Applied { .. }));

        let ctx2 = make_ctx(Some("run-1"), OutcomeSource::ProcessExit);
        let (result2, effects2) =
            apply_step_outcome(&orch, "job-1", &ctx2, StepOutcome::Blocked).unwrap();
        match result2 {
            StepOutcomeResult::AlreadySettled { current_status } => {
                assert_eq!(current_status, JobStatus::Blocked);
            }
            _ => panic!("Expected AlreadySettled for duplicate Blocked"),
        }
        assert!(effects2.is_empty());
    }

    #[test]
    fn test_blocked_to_complete_from_checkpoint() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OCC");
        insert_job(&mut conn, "job-1", "blocked", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::CheckpointApproved);
        let (result, effects) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();
        match result {
            StepOutcomeResult::Applied {
                previous_status, ..
            } => {
                assert_eq!(previous_status, JobStatus::Blocked);
            }
            _ => panic!("Expected Applied — Blocked→Complete should not be treated as settled"),
        }
        assert!(!effects.is_empty());

        let status: String = {
            let mut c = orch.db.conn.lock().unwrap();
            jobs::table
                .find("job-1")
                .select(jobs::status)
                .first(&mut *c)
                .unwrap()
        };
        assert_eq!(status, "complete");
    }

    #[test]
    fn test_blocked_to_failed_from_rejection() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OCD");
        insert_job(&mut conn, "job-1", "blocked", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::CheckpointRejected);
        let (result, _) = apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Failed).unwrap();
        match result {
            StepOutcomeResult::Applied {
                previous_status, ..
            } => {
                assert_eq!(previous_status, JobStatus::Blocked);
            }
            _ => panic!("Expected Applied — Blocked→Failed should not be treated as settled"),
        }

        let status: String = {
            let mut c = orch.db.conn.lock().unwrap();
            jobs::table
                .find("job-1")
                .select(jobs::status)
                .first(&mut *c)
                .unwrap()
        };
        assert_eq!(status, "failed");
    }

    // === Edge cases ===

    #[test]
    fn test_failed_with_issue_transitions_correctly() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OC8");
        let issue_id = create_test_issue(&mut conn, &project_id, "Task");
        insert_job(
            &mut conn,
            "job-1",
            "running",
            &project_id,
            Some(&issue_id),
            None,
        );
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::CheckpointRejected);
        let (result, _) = apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Failed).unwrap();
        assert!(matches!(result, StepOutcomeResult::Applied { .. }));

        let status: String = {
            let mut c = orch.db.conn.lock().unwrap();
            jobs::table
                .find("job-1")
                .select(jobs::status)
                .first(&mut *c)
                .unwrap()
        };
        assert_eq!(status, "failed");
    }

    #[test]
    fn test_complete_with_execution_returns_advance_id() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OC9");
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::sql_query(format!(
            "INSERT INTO executions (id, recipe_id, status, started_at, seq) \
             VALUES ('exec-1', 'recipe-1', 'running', {}, 1)",
            now
        ))
        .execute(&mut conn)
        .unwrap();

        insert_job(
            &mut conn,
            "job-1",
            "running",
            &project_id,
            None,
            Some("exec-1"),
        );
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(Some("run-1"), OutcomeSource::Return);
        let (result, effects) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();

        match result {
            StepOutcomeResult::Applied {
                advance_execution_id,
                ..
            } => {
                assert_eq!(advance_execution_id.as_deref(), Some("exec-1"));
            }
            _ => panic!("Expected Applied"),
        }

        // Effects should include AdvanceDag with the execution_id
        assert!(effects.iter().any(|e| matches!(
            e,
            WorkflowEffect::AdvanceDag { execution_id, .. } if execution_id == "exec-1"
        )));
    }

    // === Effect ordering tests ===

    #[test]
    fn test_lifecycle_message_before_advance_dag() {
        // Ordering invariant: lifecycle message appears BEFORE AdvanceDag in effects vec
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "ORD");
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::sql_query(format!(
            "INSERT INTO executions (id, recipe_id, status, started_at, seq) \
             VALUES ('exec-ord', 'recipe-1', 'running', {}, 1)",
            now
        ))
        .execute(&mut conn)
        .unwrap();

        insert_job(
            &mut conn,
            "job-1",
            "running",
            &project_id,
            None,
            Some("exec-ord"),
        );
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::Return);
        let (_, effects) = apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();

        let lifecycle_pos = effects
            .iter()
            .position(|e| matches!(e, WorkflowEffect::EmitLifecycleMessage { .. }));
        let advance_pos = effects
            .iter()
            .position(|e| matches!(e, WorkflowEffect::AdvanceDag { .. }));

        assert!(
            lifecycle_pos < advance_pos,
            "Lifecycle message must appear before AdvanceDag"
        );
    }

    #[test]
    fn test_wake_manager_after_failure_message() {
        // Ordering invariant: WakeManager appears AFTER EmitLifecycleMessage for failures
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "WMO");
        insert_job(&mut conn, "job-1", "running", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::ProcessCrash);
        let (_, effects) = apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Failed).unwrap();

        let lifecycle_pos = effects
            .iter()
            .position(|e| matches!(e, WorkflowEffect::EmitLifecycleMessage { .. }));
        let wake_pos = effects
            .iter()
            .position(|e| matches!(e, WorkflowEffect::WakeManager { .. }));

        assert!(lifecycle_pos.is_some());
        assert!(wake_pos.is_some());
        assert!(
            lifecycle_pos < wake_pos,
            "WakeManager must appear after EmitLifecycleMessage"
        );
    }

    // === System message emission ===

    /// Helper: count system messages in the messages table.
    fn count_system_messages(conn: &mut diesel::sqlite::SqliteConnection) -> i64 {
        use crate::schema::messages;
        messages::table
            .filter(messages::sender_name.eq("system"))
            .count()
            .get_result(conn)
            .unwrap_or(0)
    }

    #[test]
    fn test_system_message_not_emitted_on_apply_only() {
        // apply_step_outcome no longer emits system messages directly —
        // it returns them as effects. Verify no messages are written.
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OCE");
        let issue_id = create_test_issue(&mut conn, &project_id, "Msg test");
        insert_job(
            &mut conn,
            "job-1",
            "running",
            &project_id,
            Some(&issue_id),
            None,
        );
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(Some("run-1"), OutcomeSource::Return);
        let (result, effects) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();
        assert!(matches!(result, StepOutcomeResult::Applied { .. }));

        // No system message yet — it's in effects, not executed
        let msg_count = {
            let mut c = orch.db.conn.lock().unwrap();
            count_system_messages(&mut c)
        };
        assert_eq!(
            msg_count, 0,
            "apply_step_outcome should not emit messages directly"
        );

        // But the effect exists
        assert!(effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::EmitLifecycleMessage { .. })));

        // Execute effects to actually emit the message
        emit_outcome_effects(&orch, effects);

        let msg_count_after = {
            let mut c = orch.db.conn.lock().unwrap();
            count_system_messages(&mut c)
        };
        assert_eq!(
            msg_count_after, 1,
            "emit_outcome_effects should emit the message"
        );
    }

    #[test]
    fn test_no_effects_on_already_settled() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OCF");
        let issue_id = create_test_issue(&mut conn, &project_id, "Settled test");
        insert_job(
            &mut conn,
            "job-1",
            "running",
            &project_id,
            Some(&issue_id),
            None,
        );
        let orch = test_orchestrator(conn);

        // First call: Applied
        let ctx1 = make_ctx(Some("run-1"), OutcomeSource::Return);
        let (_, effects1) =
            apply_step_outcome(&orch, "job-1", &ctx1, StepOutcome::Complete).unwrap();
        emit_outcome_effects(&orch, effects1);

        // Second call: AlreadySettled — must produce NO effects
        let ctx2 = make_ctx(Some("run-1"), OutcomeSource::ProcessExit);
        let (result, effects2) =
            apply_step_outcome(&orch, "job-1", &ctx2, StepOutcome::Complete).unwrap();
        assert!(matches!(result, StepOutcomeResult::AlreadySettled { .. }));
        assert!(
            effects2.is_empty(),
            "AlreadySettled must produce no effects"
        );
    }

    #[test]
    fn test_already_failed_then_complete_is_settled() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OCG");
        insert_job(&mut conn, "job-1", "failed", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::CheckpointApproved);
        let (result, effects) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();
        match result {
            StepOutcomeResult::AlreadySettled { current_status } => {
                assert_eq!(current_status, JobStatus::Failed);
            }
            _ => panic!("Expected AlreadySettled — Failed is terminal"),
        }
        assert!(effects.is_empty());
    }

    // === emit_outcome_effects with effect_tx ===

    #[test]
    fn test_emit_outcome_effects_uses_effect_tx_when_available() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "ETX");
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::sql_query(format!(
            "INSERT INTO executions (id, recipe_id, status, started_at, seq) \
             VALUES ('exec-tx', 'recipe-1', 'running', {}, 1)",
            now
        ))
        .execute(&mut conn)
        .unwrap();

        insert_job(
            &mut conn,
            "job-1",
            "running",
            &project_id,
            None,
            Some("exec-tx"),
        );

        // Create orchestrator with an effect_tx channel
        let mut orch = test_orchestrator(conn);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        orch.effect_tx = Some(tx);

        // Apply outcome to get effects with AdvanceDag
        let ctx = make_ctx(Some("run-1"), OutcomeSource::Return);
        let (_, effects) = apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();

        // Verify AdvanceDag is in the effects
        assert!(effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::AdvanceDag { .. })));

        // Emit effects — AdvanceDag should go through effect_tx, not the event emitter
        emit_outcome_effects(&orch, effects);

        // The channel should have received the AdvanceDag effect
        let received = rx.try_recv();
        assert!(received.is_ok(), "effect_tx should receive AdvanceDag");
        assert!(matches!(
            received.unwrap(),
            WorkflowEffect::AdvanceDag { execution_id, .. } if execution_id == "exec-tx"
        ));
    }

    #[test]
    fn test_emit_outcome_effects_logs_error_without_tx() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "ENT");
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::sql_query(format!(
            "INSERT INTO executions (id, recipe_id, status, started_at, seq) \
             VALUES ('exec-ev', 'recipe-1', 'running', {}, 1)",
            now
        ))
        .execute(&mut conn)
        .unwrap();

        insert_job(
            &mut conn,
            "job-1",
            "running",
            &project_id,
            None,
            Some("exec-ev"),
        );
        let orch = test_orchestrator(conn);
        assert!(orch.effect_tx.is_none());

        let ctx = make_ctx(Some("run-1"), OutcomeSource::Return);
        let (_, effects) = apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();

        // Verify AdvanceDag is in the effects
        assert!(effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::AdvanceDag { .. })));

        // Should not panic when effect_tx is None — logs error instead
        emit_outcome_effects(&orch, effects);
    }

    // === Outbox integration tests ===

    #[test]
    fn test_outbox_entry_created_on_complete_with_execution() {
        use crate::schema::effect_outbox;

        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OBX");
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::sql_query(format!(
            "INSERT INTO executions (id, recipe_id, status, started_at, seq) \
             VALUES ('exec-obx', 'recipe-1', 'running', {}, 1)",
            now
        ))
        .execute(&mut conn)
        .unwrap();

        insert_job(
            &mut conn,
            "job-1",
            "running",
            &project_id,
            None,
            Some("exec-obx"),
        );
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(Some("run-1"), OutcomeSource::Return);
        let (result, effects) =
            apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();
        assert!(matches!(result, StepOutcomeResult::Applied { .. }));

        // Verify outbox entry was created
        let entries: Vec<(String, String)> = {
            let mut c = orch.db.conn.lock().unwrap();
            effect_outbox::table
                .select((effect_outbox::kind, effect_outbox::dedupe_key))
                .load(&mut *c)
                .unwrap()
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "advance_dag");
        assert_eq!(entries[0].1, "exec-obx");

        // Verify AdvanceDag effect carries the outbox_entry_id for row-level ack
        let advance = effects
            .iter()
            .find(|e| matches!(e, WorkflowEffect::AdvanceDag { .. }));
        assert!(advance.is_some());
        if let Some(WorkflowEffect::AdvanceDag {
            outbox_entry_id, ..
        }) = advance
        {
            assert!(
                outbox_entry_id.is_some(),
                "Root AdvanceDag must carry outbox_entry_id"
            );
        }
    }

    #[test]
    fn test_no_outbox_entry_on_complete_without_execution() {
        use crate::schema::effect_outbox;

        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OBN");
        insert_job(&mut conn, "job-1", "running", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(Some("run-1"), OutcomeSource::Return);
        let _ = apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();

        let count: i64 = {
            let mut c = orch.db.conn.lock().unwrap();
            effect_outbox::table.count().get_result(&mut *c).unwrap()
        };
        assert_eq!(count, 0, "No outbox entry for job without execution_id");
    }

    #[test]
    fn test_no_outbox_entry_on_failure() {
        use crate::schema::effect_outbox;

        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OBF");
        insert_job(&mut conn, "job-1", "running", &project_id, None, None);
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::ProcessCrash);
        let _ = apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Failed).unwrap();

        let count: i64 = {
            let mut c = orch.db.conn.lock().unwrap();
            effect_outbox::table.count().get_result(&mut *c).unwrap()
        };
        assert_eq!(count, 0, "No outbox entry for failed jobs");
    }

    #[test]
    fn test_no_outbox_entry_on_already_settled() {
        use crate::schema::effect_outbox;

        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "OBS");
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::sql_query(format!(
            "INSERT INTO executions (id, recipe_id, status, started_at, seq) \
             VALUES ('exec-obs', 'recipe-1', 'running', {}, 1)",
            now
        ))
        .execute(&mut conn)
        .unwrap();

        insert_job(
            &mut conn,
            "job-1",
            "complete",
            &project_id,
            None,
            Some("exec-obs"),
        );
        let orch = test_orchestrator(conn);

        let ctx = make_ctx(None, OutcomeSource::ProcessExit);
        let (result, _) = apply_step_outcome(&orch, "job-1", &ctx, StepOutcome::Complete).unwrap();
        assert!(matches!(result, StepOutcomeResult::AlreadySettled { .. }));

        let count: i64 = {
            let mut c = orch.db.conn.lock().unwrap();
            effect_outbox::table.count().get_result(&mut *c).unwrap()
        };
        assert_eq!(count, 0, "No outbox entry for already-settled jobs");
    }
}
