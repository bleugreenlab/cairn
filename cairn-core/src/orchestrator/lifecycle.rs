//! Session lifecycle management
//!
//! This module handles cleanup, finalization, and stopping of backend sessions,
//! including cascading stops to child runs.
//!
//! ## Warm Process Retention
//!
//! When a turn completes successfully, processes can be transitioned to "warm" state
//! instead of being terminated. Warm processes:
//! - Keep their stdin open for follow-up messages
//! - Retain their MCP authentication token
//! - Preserve Claude's conversation cache
//! - Can be reused for continuation without spawning a new process

use crate::agent_process::checkpoints::has_approval_checkpoint_slot;
use crate::mcp::handlers::{emit_attention, AttentionEvent};
use crate::models::RunStatus;
use crate::schema::{executions, issues, job_terminals, jobs, projects, runs, turns};
use diesel::prelude::*;

use super::Orchestrator;

/// Transition a run to warm state after successful turn completion.
///
/// The Run stays `Live` in the DB — no durable status change.
/// Completes the current Turn and transitions process occupancy to Idle.
///
/// Returns true if the process was successfully transitioned to warm.
pub fn transition_to_warm_state(orch: &Orchestrator, run_id: &str) -> bool {
    // Complete the current turn before transitioning occupancy
    if let Some(turn_id) = orch.process_state.get_current_turn_id(run_id) {
        if let Ok(mut conn) = orch.db.conn.lock() {
            let _ = crate::transitions::apply_turn_outcome(
                &mut conn,
                &turn_id,
                crate::models::TurnState::Complete,
                &*orch.services.emitter,
            );
        }
    }

    if orch.process_state.transition_to_warm(run_id) {
        // Emit turn db-change so frontend sees the turn completion
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "turns", "action": "update"}),
        );

        log::info!(
            "Run {} transitioned to warm state (process retained for potential follow-up)",
            &run_id[..run_id.len().min(8)]
        );

        true
    } else {
        log::warn!(
            "Failed to transition run {} to warm state (process not found)",
            &run_id[..run_id.len().min(8)]
        );
        false
    }
}

/// Park a run for durable wait without tearing down its process.
///
/// Durable waits are not crashes. We interrupt the current turn, clean up any
/// foreground inline commands, and leave the process warm so it can resume when
/// the awaited dependency resolves.
pub fn suspend_run_for_durable_wait(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
) -> Result<(), String> {
    let _ = exit_reason;
    stop_session_internal(orch, run_id)?;
    Ok(())
}

pub fn finalize_run(orch: &Orchestrator, run_id: &str, status: RunStatus) {
    // Clean up system prompt temp file
    super::session::cleanup_prompt_file(run_id);

    // Finalize the current turn based on run outcome.
    // Primary: in-memory process state. Fallback: job's current_turn_id in DB
    // (covers crashes where the process was never registered or already deregistered).
    let turn_id = orch.process_state.get_current_turn_id(run_id).or_else(|| {
        let mut conn = orch.db.conn.lock().ok()?;
        runs::table
            .find(run_id)
            .inner_join(jobs::table.on(runs::job_id.assume_not_null().eq(jobs::id)))
            .select(jobs::current_turn_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten()
    });
    let had_active_turn = turn_id.is_some();
    if let Some(turn_id) = turn_id {
        if let Ok(mut conn) = orch.db.conn.lock() {
            let turn_state: Option<String> = turns::table
                .find(&turn_id)
                .select(turns::state)
                .first::<String>(&mut *conn)
                .optional()
                .unwrap_or_default();

            let result = if status == RunStatus::Exited {
                crate::transitions::apply_turn_outcome(
                    &mut conn,
                    &turn_id,
                    crate::models::TurnState::Complete,
                    &*orch.services.emitter,
                )
            } else if turn_state.as_deref() == Some("running") {
                crate::transitions::interrupt_turn(&mut conn, &turn_id, &*orch.services.emitter)
            } else {
                crate::transitions::apply_turn_outcome(
                    &mut conn,
                    &turn_id,
                    crate::models::TurnState::Failed,
                    &*orch.services.emitter,
                )
            };

            if let Err(e) = result {
                log::warn!(
                    "Failed to finalize turn {} for run {}: {}",
                    turn_id,
                    run_id,
                    e
                );
            }
        }
    }

    // Transition run via transition_run (validates state machine, emits db-change).
    // Also get the job_id for subsequent job lifecycle.
    let job_id: Option<String> = if let Ok(mut conn) = orch.db.conn.lock() {
        // Check current status - don't overwrite terminal states
        let current_status: Option<String> = runs::table
            .find(run_id)
            .select(runs::status)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten();

        if matches!(current_status.as_deref(), Some("exited") | Some("crashed")) {
            log::info!(
                "Run {} already finalized as {:?}, skipping re-finalization as {:?}",
                &run_id[..run_id.len().min(8)],
                current_status,
                status
            );
            // Still emit run-completed so task handlers waiting on this event are unblocked.
            let _ = orch
                .services
                .emitter
                .emit("run-completed", serde_json::json!(run_id));
            let _ = orch.run_completions.send(run_id.to_string());
            return;
        }

        // Use transition_run for validated status change + db-change emission
        match crate::transitions::transition_run(
            &mut conn,
            run_id,
            status.clone(),
            &*orch.services.emitter,
        ) {
            Ok(_) => {}
            Err(e) => {
                log::error!("Failed to transition run {}: {}", run_id, e);
            }
        }

        // Sync the finalized run
        if let Ok(db_run) = runs::table
            .find(run_id)
            .first::<crate::diesel_models::DbRun>(&mut *conn)
        {
            orch.sync(crate::sync::SyncMessage::Run(
                (&crate::runs::queries::db_run_to_run(db_run)).into(),
            ));
        }

        // Get job_id
        runs::table
            .inner_join(jobs::table.on(runs::job_id.assume_not_null().eq(jobs::id)))
            .filter(runs::id.eq(run_id))
            .filter(runs::job_id.is_not_null())
            .select(runs::job_id.assume_not_null())
            .first::<String>(&mut *conn)
            .ok()
    } else {
        None
    };

    if had_active_turn {
        // Finalize todos: mark any in_progress as completed
        if let Some(ref job_id) = job_id {
            if let Ok(mut conn) = orch.db.conn.lock() {
                let _ = crate::todos::finalize_todos(&mut conn, job_id);
            }
        }

        // Apply step outcome via unified reducer (transition + system message +
        // manager wake + DAG advance signal). Idempotent: if handle_return already
        // transitioned the job, the reducer returns AlreadySettled — no duplicate effects.
        if let Some(job_id) = job_id.clone() {
            use crate::transitions::outcome::{
                apply_step_outcome, emit_outcome_effects, OutcomeContext, OutcomeSource,
                StepOutcome,
            };

            let outcome = if status == RunStatus::Exited {
                if let Ok(mut conn) = orch.db.conn.lock() {
                    if has_approval_checkpoint_slot(&mut conn, &job_id) {
                        log::info!("Job {} has checkpoint slot, blocking for approval", job_id);
                        StepOutcome::Blocked
                    } else {
                        StepOutcome::Complete
                    }
                } else {
                    StepOutcome::Complete
                }
            } else {
                StepOutcome::Failed
            };
            let ctx = OutcomeContext {
                run_id: Some(run_id),
                source: if status == RunStatus::Exited {
                    OutcomeSource::ProcessExit
                } else {
                    OutcomeSource::ProcessCrash
                },
            };

            match apply_step_outcome(orch, &job_id, &ctx, outcome) {
                Ok((_result, effects)) => emit_outcome_effects(orch, effects),
                Err(e) => log::error!("Failed to apply step outcome for {}: {}", job_id, e),
            }
        }
    } else if let Some(ref job_id) = job_id {
        log::info!(
            "Run {} exited without an active turn; skipping job lifecycle reduction for {}",
            &run_id[..run_id.len().min(8)],
            &job_id[..job_id.len().min(8)]
        );
    }

    if status == RunStatus::Exited {
        if let Some(ref job_id) = job_id {
            if let Err(e) =
                crate::mcp::handlers::agents::resume_suspended_parent_after_task_completion(
                    orch, job_id,
                )
            {
                log::warn!(
                    "Failed to resume suspended delegated parent after job {} completed: {}",
                    job_id,
                    e
                );
            }
        }
    }

    // Emit run completed event (frontend)
    let _ = orch
        .services
        .emitter
        .emit("run-completed", serde_json::json!(run_id));

    // Signal run_completions broadcast (unblocks handle_task waiters)
    let _ = orch.run_completions.send(run_id.to_string());

    // Log session context for crash observability
    if status == RunStatus::Crashed {
        log_session_crash_context(orch, run_id);
    }

    // Emit agent-attention only when this exit actually finalized an in-flight turn.
    if had_active_turn {
        emit_attention_for_run(orch, run_id, status);
    }
}

/// Log session context when a run crashes. If this was a resume attempt, the session
/// may be invalid — log a warning so operators can investigate.
fn log_session_crash_context(orch: &Orchestrator, run_id: &str) {
    let Ok(mut conn) = orch.db.conn.lock() else {
        return;
    };

    let run_info: Option<(Option<String>, Option<String>)> = runs::table
        .find(run_id)
        .select((runs::session_id, runs::start_mode))
        .first(&mut *conn)
        .ok();

    if let Some((Some(session_id), start_mode)) = run_info {
        if start_mode.as_deref() == Some("resume") {
            log::warn!(
                "Resume run {} crashed for session {} — \
                 session may be invalid. If this repeats, session rotation may be needed.",
                &run_id[..run_id.len().min(8)],
                &session_id[..session_id.len().min(8)]
            );
        }
    }
}

/// Emit agent-attention for a completed/failed run, but only if it's a top-level job.
fn emit_attention_for_run(orch: &Orchestrator, run_id: &str, status: RunStatus) {
    let Ok(mut conn) = orch.db.conn.lock() else {
        return;
    };

    // Query top-level job data (parent_job_id IS NULL filters out sub-tasks)
    type AttentionRow = (
        String,
        Option<i32>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let row: Option<AttentionRow> = runs::table
        .inner_join(jobs::table.on(runs::job_id.assume_not_null().eq(jobs::id)))
        .inner_join(projects::table.on(jobs::project_id.eq(projects::id)))
        .left_join(issues::table.on(jobs::issue_id.eq(issues::id.nullable())))
        .filter(runs::id.eq(run_id))
        .filter(jobs::parent_job_id.is_null())
        .select((
            projects::key,
            issues::number.nullable(),
            issues::title.nullable(),
            jobs::node_name,
            jobs::execution_id,
        ))
        .first(&mut *conn)
        .ok();

    let Some((project_key, issue_number, issue_title, node_name, execution_id)) = row else {
        return;
    };

    let exec_seq = execution_id.as_deref().and_then(|eid| {
        executions::table
            .find(eid)
            .select(executions::seq)
            .first::<Option<i32>>(&mut *conn)
            .ok()
            .flatten()
    });

    let attention_type = if status == RunStatus::Exited {
        "completed"
    } else {
        "failed"
    };

    emit_attention(
        &*orch.services.emitter,
        &AttentionEvent {
            attention_type,
            project_key: &project_key,
            issue_number,
            issue_title: issue_title.as_deref(),
            node_name: node_name.as_deref(),
            exec_seq,
            tool_name: None,
        },
    );
}

/// Find all descendant job IDs for a given job (children, grandchildren, etc.)
fn find_descendant_job_ids(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
) -> Vec<String> {
    let mut all_descendants = Vec::new();
    let mut current_parents = vec![job_id.to_string()];

    while !current_parents.is_empty() {
        let children: Vec<String> = jobs::table
            .filter(jobs::parent_job_id.eq_any(&current_parents))
            .select(jobs::id)
            .load(conn)
            .unwrap_or_default();

        if children.is_empty() {
            break;
        }

        all_descendants.extend(children.clone());
        current_parents = children;
    }

    all_descendants
}

/// Get all running run IDs for a set of job IDs
fn get_running_runs_for_jobs(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_ids: &[String],
) -> Vec<String> {
    runs::table
        .filter(runs::job_id.eq_any(job_ids))
        .filter(runs::status.eq_any(&["starting", "live"]))
        .select(runs::id)
        .load(conn)
        .unwrap_or_default()
}

/// Stop a running backend session, cascading to child runs
pub fn stop_session(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    // First, collect child runs to stop
    let child_run_ids: Vec<String> = if let Ok(mut conn) = orch.db.conn.lock() {
        let job_id: Option<String> = runs::table
            .filter(runs::id.eq(run_id))
            .select(runs::job_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten();

        if let Some(job_id) = job_id {
            let descendant_job_ids = find_descendant_job_ids(&mut conn, &job_id);
            if !descendant_job_ids.is_empty() {
                get_running_runs_for_jobs(&mut conn, &descendant_job_ids)
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Stop child runs first
    for child_run_id in &child_run_ids {
        log::info!(
            "Stopping child run {} (parent run {} stopped)",
            child_run_id,
            run_id
        );
        let _ = stop_session_internal(orch, child_run_id);
    }

    // Stop the requested run
    stop_session_internal(orch, run_id)
}

/// Internal stop without cascading (used by cascading stop)
///
/// Sends an interrupt control request via stdin and transitions the process
/// to warm state. The process is NOT killed - it stays available for follow-up
/// messages.
fn stop_session_internal(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    // Interrupt the current turn
    if let Some(turn_id) = orch.process_state.get_current_turn_id(run_id) {
        if let Ok(mut conn) = orch.db.conn.lock() {
            let _ =
                crate::transitions::interrupt_turn(&mut conn, &turn_id, &*orch.services.emitter);
        }
    }

    // Send interrupt via backend-aware stdin handler
    if let Err(e) = crate::backends::stdin::send_interrupt(&orch.process_state, run_id) {
        log::warn!("Failed to send interrupt to run {}: {}", run_id, e);
    }

    // Only kill foreground bash processes — background terminals survive the interrupt
    cleanup_inline_commands(orch, run_id);

    // Transition to warm state instead of killing
    if orch.process_state.transition_to_warm(run_id) {
        log::info!(
            "Run {} interrupted and transitioned to warm state",
            &run_id[..run_id.len().min(8)]
        );
    } else {
        log::warn!(
            "Run {} not found in process map after interrupt",
            &run_id[..run_id.len().min(8)]
        );
    }

    Ok(())
}

/// Kill only foreground (inline) bash processes for a run.
///
/// Background terminals are intentionally left alive — they should survive
/// an interrupt so the agent can resume and still interact with them.
fn cleanup_inline_commands(orch: &Orchestrator, run_id: &str) {
    for child in orch.pty_state.take_inline_commands(run_id) {
        if let Ok(mut child) = child.lock() {
            let _ = child.kill();
            let _ = child.try_wait();
        }
    }
}

/// Kill background terminals associated with a run's job.
///
/// Called during hard kill (not interrupt) to clean up PTY sessions
/// and their database records.
fn cleanup_job_terminals(orch: &Orchestrator, run_id: &str) {
    let job_id = {
        let Ok(mut conn) = orch.db.conn.lock() else {
            return;
        };

        runs::table
            .find(run_id)
            .select(runs::job_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten()
    };

    let Some(job_id) = job_id else {
        return;
    };

    let running_terminals = {
        let Ok(mut conn) = orch.db.conn.lock() else {
            return;
        };

        job_terminals::table
            .filter(job_terminals::job_id.eq(&job_id))
            .filter(job_terminals::status.eq("running"))
            .select((job_terminals::id, job_terminals::session_id))
            .load::<(String, String)>(&mut *conn)
            .ok()
            .unwrap_or_default()
    };

    if running_terminals.is_empty() {
        return;
    }

    for (terminal_id, session_id) in running_terminals {
        {
            let Ok(mut sessions) = orch.pty_state.sessions.lock() else {
                continue;
            };

            if let Some(session_arc) = sessions.remove(&session_id) {
                if let Ok(mut session) = session_arc.lock() {
                    let _ = session.child.kill();
                    let _ = session.child.wait();
                }
            }
        }

        if let Ok(mut conn) = orch.db.conn.lock() {
            let _ = diesel::delete(job_terminals::table.find(&terminal_id)).execute(&mut *conn);
        }
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "job_terminals", "action": "delete"}),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::{NewJobTerminal, NewRun, NewTurn};
    use crate::models::CreateManager;
    use crate::schema::job_terminals;
    use crate::services::testing::{MockChildProcess, MockClock, TestServicesBuilder};
    use crate::services::PtySession;
    use crate::test_utils::{
        create_test_issue, create_test_job, create_test_project, test_diesel_conn,
    };
    use portable_pty::{CommandBuilder, PtySize};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    /// Create a manager and return its ID.
    fn create_manager(conn: &mut diesel::sqlite::SqliteConnection, project_id: &str) -> String {
        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700000000);
        let mgr = crate::managers::crud::create(
            conn,
            &clock,
            CreateManager {
                project_id: project_id.to_string(),
                home_project_id: None,
                scope_kind: None,
                name: "Test Manager".to_string(),
                branch: "mgr/test".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();
        mgr.id
    }

    /// Set manager_id on an issue.
    fn set_manager_id(
        conn: &mut diesel::sqlite::SqliteConnection,
        issue_id: &str,
        manager_id: &str,
    ) {
        diesel::update(issues::table.find(issue_id))
            .set(issues::manager_id.eq(Some(manager_id)))
            .execute(conn)
            .unwrap();
    }

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

    fn insert_test_run(
        conn: &mut diesel::sqlite::SqliteConnection,
        run_id: &str,
        issue_id: &str,
        project_id: &str,
        job_id: &str,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(runs::table)
            .values(&NewRun {
                id: run_id,
                issue_id: Some(issue_id),
                project_id: Some(project_id),
                job_id: Some(job_id),
                status: Some("live"),
                session_id: None,
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: None,
                chat_id: None,
            })
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn wake_info_returns_manager_for_managed_issue_top_level_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let manager_id = create_manager(&mut conn, &project_id);
        let issue_id = create_test_issue(&mut conn, &project_id, "Managed task");
        set_manager_id(&mut conn, &issue_id, &manager_id);

        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "failed", None);

        let result = get_manager_wake_info(&mut conn, &job_id);
        assert!(result.is_some());
        let (mgr_id, number, title) = result.unwrap();
        assert_eq!(mgr_id, manager_id);
        assert_eq!(number, 1);
        assert_eq!(title, "Managed task");
    }

    #[test]
    fn wake_info_returns_none_for_unmanaged_issue() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Regular task");
        // No manager_id set

        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "failed", None);

        let result = get_manager_wake_info(&mut conn, &job_id);
        assert!(result.is_none());
    }

    #[test]
    fn wake_info_returns_manager_for_sub_task_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let manager_id = create_manager(&mut conn, &project_id);
        let issue_id = create_test_issue(&mut conn, &project_id, "Managed task");
        set_manager_id(&mut conn, &issue_id, &manager_id);

        // Create parent job
        let parent_job_id =
            create_test_job(&mut conn, &issue_id, &project_id, "build", "running", None);
        // Create sub-task job
        let child_job_id = create_test_job(
            &mut conn,
            &issue_id,
            &project_id,
            "build",
            "failed",
            Some(&parent_job_id),
        );

        // Sub-task failure should still wake the manager for the managed issue
        let result = get_manager_wake_info(&mut conn, &child_job_id);
        assert!(result.is_some());

        // Parent job should also return wake info
        let result = get_manager_wake_info(&mut conn, &parent_job_id);
        assert!(result.is_some());
    }

    #[test]
    fn wake_info_returns_none_for_nonexistent_job() {
        let mut conn = test_diesel_conn();
        let result = get_manager_wake_info(&mut conn, "nonexistent-job-id");
        assert!(result.is_none());
    }

    #[test]
    fn stop_session_cleans_up_inline_commands() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Task");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "running", None);
        insert_test_run(&mut conn, "run-inline", &issue_id, &project_id, &job_id);
        let orch = test_orchestrator(conn);

        let child = Arc::new(Mutex::new(
            Box::new(MockChildProcess::with_stdout(42, vec![]))
                as Box<dyn crate::services::ChildProcess>,
        ));
        orch.pty_state.register_inline_command(
            "run-inline".to_string(),
            "cmd-1".to_string(),
            child.clone(),
        );

        stop_session_internal(&orch, "run-inline").unwrap();

        assert!(orch.pty_state.take_inline_commands("run-inline").is_empty());
        assert!(child.lock().unwrap().try_wait().unwrap().is_some());
    }

    #[test]
    fn stop_session_preserves_background_terminals() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Task");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "running", None);
        insert_test_run(&mut conn, "run-terminal", &issue_id, &project_id, &job_id);

        let session_id = "session-1".to_string();
        let terminal_id = "terminal-1".to_string();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(job_terminals::table)
            .values(&NewJobTerminal {
                id: &terminal_id,
                job_id: Some(&job_id),
                project_id: None,
                run_id: Some("run-terminal"),
                session_id: &session_id,
                command: "sleep 30",
                title: None,
                description: None,
                status: "running",
                exit_code: None,
                created_at: now,
                exited_at: None,
                slug: Some("sleep"),
            })
            .execute(&mut conn)
            .unwrap();

        let orch = test_orchestrator(conn);

        let pty_system = portable_pty::native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut cmd = CommandBuilder::new("sleep");
        cmd.arg("30");
        let child = pair.slave.spawn_command(cmd).unwrap();
        let writer = pair.master.take_writer().unwrap();
        let session = PtySession {
            master: pair.master,
            writer,
            child,
            output_buffer: Some(Arc::new(Mutex::new(VecDeque::new()))),
            is_agent_spawned: true,
        };
        orch.pty_state
            .sessions
            .lock()
            .unwrap()
            .insert(session_id.clone(), Arc::new(Mutex::new(session)));

        // Interrupt (stop) should NOT kill background terminals
        stop_session_internal(&orch, "run-terminal").unwrap();

        // Background terminal session should still be alive
        assert!(orch
            .pty_state
            .sessions
            .lock()
            .unwrap()
            .contains_key(&session_id));
        // Terminal DB record should still exist
        let mut conn = orch.db.conn.lock().unwrap();
        let remaining: i64 = job_terminals::table.count().get_result(&mut *conn).unwrap();
        assert_eq!(remaining, 1);
    }

    #[test]
    fn cleanup_job_terminals_kills_background_terminals() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Task");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "running", None);
        insert_test_run(&mut conn, "run-terminal", &issue_id, &project_id, &job_id);

        let session_id = "session-1".to_string();
        let terminal_id = "terminal-1".to_string();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(job_terminals::table)
            .values(&NewJobTerminal {
                id: &terminal_id,
                job_id: Some(&job_id),
                project_id: None,
                run_id: Some("run-terminal"),
                session_id: &session_id,
                command: "sleep 30",
                title: None,
                description: None,
                status: "running",
                exit_code: None,
                created_at: now,
                exited_at: None,
                slug: Some("sleep"),
            })
            .execute(&mut conn)
            .unwrap();

        let orch = test_orchestrator(conn);

        let pty_system = portable_pty::native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut cmd = CommandBuilder::new("sleep");
        cmd.arg("30");
        let child = pair.slave.spawn_command(cmd).unwrap();
        let writer = pair.master.take_writer().unwrap();
        let session = PtySession {
            master: pair.master,
            writer,
            child,
            output_buffer: Some(Arc::new(Mutex::new(VecDeque::new()))),
            is_agent_spawned: true,
        };
        orch.pty_state
            .sessions
            .lock()
            .unwrap()
            .insert(session_id.clone(), Arc::new(Mutex::new(session)));

        // Direct call to cleanup_job_terminals (used by kill_session path)
        super::cleanup_job_terminals(&orch, "run-terminal");

        // Background terminal session should be removed
        assert!(!orch
            .pty_state
            .sessions
            .lock()
            .unwrap()
            .contains_key(&session_id));
        // Terminal DB record should be deleted
        let mut conn = orch.db.conn.lock().unwrap();
        let remaining: i64 = job_terminals::table.count().get_result(&mut *conn).unwrap();
        assert_eq!(remaining, 0);
    }

    #[test]
    fn finalize_run_marks_active_running_turn_interrupted_on_crash() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = create_test_issue(&mut conn, &project_id, "Task");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "running", None);
        insert_test_run(&mut conn, "run-crash", &issue_id, &project_id, &job_id);

        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(turns::table)
            .values(&NewTurn {
                id: "turn-crash",
                session_id: "session-crash",
                run_id: Some("run-crash"),
                job_id: Some(&job_id),
                manager_id: None,
                sequence: 1,
                predecessor_id: None,
                state: "running",
                yield_reason: None,
                start_reason: "initial",
                created_at: now,
                started_at: Some(now),
                ended_at: None,
                updated_at: now,
            })
            .execute(&mut conn)
            .unwrap();
        diesel::update(jobs::table.find(&job_id))
            .set(jobs::current_turn_id.eq(Some("turn-crash")))
            .execute(&mut conn)
            .unwrap();

        let orch = test_orchestrator(conn);
        finalize_run(&orch, "run-crash", RunStatus::Crashed);

        let mut conn = orch.db.conn.lock().unwrap();
        let turn_state: String = turns::table
            .find("turn-crash")
            .select(turns::state)
            .first(&mut *conn)
            .unwrap();
        assert_eq!(turn_state, "interrupted");
    }
}
/// Query whether a failed job belongs to a managed issue.
///
/// Returns `(manager_id, issue_number, issue_title)` if the job is linked to an
/// issue with a `manager_id`.
///
/// We deliberately wake on any failed job under a managed issue. Manager mailbox
/// batching absorbs duplicate failures, and in practice nested job failures still
/// represent manager-relevant state transitions for the issue.
#[cfg_attr(test, allow(dead_code))]
pub(crate) fn get_manager_wake_info(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
) -> Option<(String, i32, String)> {
    jobs::table
        .inner_join(issues::table.on(jobs::issue_id.eq(issues::id.nullable())))
        .filter(jobs::id.eq(job_id))
        .filter(issues::manager_id.is_not_null())
        .select((
            issues::manager_id.assume_not_null(),
            issues::number,
            issues::title,
        ))
        .first(conn)
        .ok()
}

/// Check if a failed job belongs to a managed issue and wake the manager.
///
/// This is called from `finalize_run` when a job transitions to Failed.
/// The failing job is on the *managed issue*, not the manager's own job.
pub fn wake_manager_on_failure(orch: &Orchestrator, job_id: &str) {
    let wake_info: Option<(String, i32, String)> = {
        let Ok(mut conn) = orch.db.conn.lock() else {
            return;
        };
        get_manager_wake_info(&mut conn, job_id)
    };

    let Some((manager_id, issue_number, issue_title)) = wake_info else {
        return;
    };

    log::info!(
        "Managed issue #{} failed, waking manager {}",
        issue_number,
        manager_id
    );

    // wake_manager handles first-wake vs subsequent-wake.
    // For FirstWake, we can only log + start_agent_session here since we're in cairn-core.
    // But this is fine — wake_manager returns WakeResult which the caller handles.
    // In this case we're in cairn-core so we need to handle FirstWake inline.
    use crate::managers::wake::{
        acknowledge_prepared_manager_wake, release_prepared_manager_wake, wake_manager, WakeResult,
        WakeTrigger,
    };

    let trigger = WakeTrigger::IssueFailed {
        issue_number,
        issue_title,
        error: None,
    };

    match wake_manager(orch, &manager_id, trigger) {
        Ok(WakeResult::FirstWake(prepared)) => {
            let run = &prepared.prepared_job;
            if let Err(e) = crate::orchestrator::session::start_agent_session(
                orch,
                &run.run_id,
                &run.prompt,
                &run.worktree_path,
                crate::backends::SessionStart::New {
                    session_id: run.session_id.clone(),
                },
                run.job_model.clone(),
                None,
                run.agent_config.as_ref(),
                run.artifact_schema_info.as_ref(),
                false,
                run.execution_id.as_deref(),
                None,
            ) {
                let _ = release_prepared_manager_wake(orch, &prepared);
                log::error!("Failed to start manager session on issue failure: {}", e);
            } else {
                if let Err(e) = acknowledge_prepared_manager_wake(orch, &prepared) {
                    log::error!("Failed to acknowledge prepared manager wake: {}", e);
                }
                // Wire turn lifecycle
                orch.process_state
                    .set_current_turn_id(&run.run_id, Some(&run.turn_id));
                if let Ok(mut conn) = orch.db.conn.lock() {
                    let _ = crate::transitions::start_turn(
                        &mut conn,
                        &run.turn_id,
                        &run.run_id,
                        &*orch.services.emitter,
                    );
                }
            }
        }
        Ok(WakeResult::Resumed(run)) => {
            log::info!(
                "Manager {} resumed on issue failure, run {}",
                manager_id,
                &run.id[..run.id.len().min(8)]
            );
        }
        Ok(WakeResult::AlreadyRunning) => {
            log::info!(
                "Manager {} already running, failure trigger delivered inline",
                manager_id
            );
        }
        Ok(WakeResult::Inactive) => {}
        Err(e) => {
            log::error!(
                "Failed to wake manager {} on issue failure: {}",
                manager_id,
                e
            );
        }
    }
}

/// Forcefully kill a backend session and finalize.
///
/// Use this when truly terminating a session (e.g., closing an issue,
/// GC eviction, or cleanup). Unlike `stop_session`, this actually kills
/// the process and cannot be resumed.
pub fn kill_session(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    kill_session_with_reason(orch, run_id, "user_stop")
}

/// Kill a session with a specific exit reason.
pub fn kill_session_with_reason(
    orch: &Orchestrator,
    run_id: &str,
    exit_reason: &str,
) -> Result<(), String> {
    // Only send interrupt to non-idle processes
    let is_idle = orch
        .process_state
        .get_occupancy(run_id)
        .map(|o| matches!(o, crate::agent_process::process::RunOccupancy::Idle))
        .unwrap_or(false);

    if !is_idle {
        let _ = crate::backends::stdin::send_interrupt(&orch.process_state, run_id);

        // Brief wait for graceful handling
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Clean up all tool processes — both foreground and background
    cleanup_inline_commands(orch, run_id);
    cleanup_job_terminals(orch, run_id);

    // Kill the process
    let mut processes = orch
        .process_state
        .processes
        .lock()
        .map_err(|e| e.to_string())?;

    if let Some(handle) = processes.remove(run_id) {
        if let Ok(mut child_guard) = handle.child.lock() {
            if let Some(mut child) = child_guard.take() {
                crate::agent_process::process::graceful_stop(&mut *child);
                log::info!("Killed process for run {}", &run_id[..run_id.len().min(8)]);
            }
        }
    }

    // Drop the lock before calling finalize_run (which also locks)
    drop(processes);

    // Set exit reason and finalize as Exited (clean kill) or Crashed
    let final_status = if exit_reason == "crash" {
        RunStatus::Crashed
    } else {
        RunStatus::Exited
    };

    if let Ok(mut conn) = orch.db.conn.lock() {
        let _ = crate::transitions::set_exit_reason(&mut conn, run_id, exit_reason);
    }

    finalize_run(orch, run_id, final_status);

    Ok(())
}

#[cfg(test)]
mod warm_exit_tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::NewRun;
    use crate::schema::{jobs, runs};
    use crate::services::testing::{CapturingEmitter, MockChildProcess, TestServicesBuilder};
    use crate::test_utils::{
        create_test_issue, create_test_job, create_test_project, test_diesel_conn,
    };
    use std::sync::{Arc, Mutex};

    fn test_orchestrator_with_emitter(
        conn: diesel::sqlite::SqliteConnection,
    ) -> (Orchestrator, Arc<CapturingEmitter>) {
        let db = Arc::new(DbState {
            conn: Mutex::new(conn),
        });
        let emitter = Arc::new(CapturingEmitter::new());
        let mut services = TestServicesBuilder::new().build();
        services.emitter = emitter.clone();
        let services = Arc::new(services);
        let account_manager = Arc::new(crate::orchestrator::AccountManager::new(
            db.clone(),
            services.emitter.clone(),
        ));
        let sync_tx = Arc::new(Mutex::new(None));
        let orch = Orchestrator {
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
            executor: Arc::new(std::sync::OnceLock::new()),
        };
        (orch, emitter)
    }

    fn insert_test_run(
        conn: &mut diesel::sqlite::SqliteConnection,
        run_id: &str,
        issue_id: &str,
        project_id: &str,
        job_id: &str,
        session_id: &str,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(runs::table)
            .values(&NewRun {
                id: run_id,
                issue_id: Some(issue_id),
                project_id: Some(project_id),
                job_id: Some(job_id),
                status: Some("live"),
                session_id: Some(session_id),
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: None,
                chat_id: None,
            })
            .execute(conn)
            .unwrap();
    }

    fn register_warm_process(orch: &Orchestrator, run_id: &str, session_id: &str, job_id: &str) {
        let child = Arc::new(Mutex::new(Some(Box::new(MockChildProcess::with_stdout(
            999_999,
            vec![],
        ))
            as Box<dyn crate::services::ChildProcess>)));
        let stdin = Arc::new(Mutex::new(None));
        let mut handle = crate::agent_process::process::RunHandle::new(
            child,
            stdin,
            Some(session_id.to_string()),
            Some(job_id.to_string()),
        );
        handle.transition_to_warm();
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register(run_id.to_string(), handle);
    }

    #[test]
    fn warm_evict_does_not_emit_duplicate_completion_attention() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "WARM");
        let issue_id = create_test_issue(&mut conn, &project_id, "Warm issue");
        let job_id = create_test_job(
            &mut conn,
            &issue_id,
            &project_id,
            "planner",
            "complete",
            None,
        );
        insert_test_run(
            &mut conn,
            "run-warm",
            &issue_id,
            &project_id,
            &job_id,
            "session-1",
        );

        let (orch, emitter) = test_orchestrator_with_emitter(conn);
        register_warm_process(&orch, "run-warm", "session-1", &job_id);

        kill_session_with_reason(&orch, "run-warm", "warm_evict").unwrap();

        let mut conn = orch.db.conn.lock().unwrap();
        let job_status: String = jobs::table
            .find(&job_id)
            .select(jobs::status)
            .first(&mut *conn)
            .unwrap();
        let run_status: Option<String> = runs::table
            .find("run-warm")
            .select(runs::status)
            .first(&mut *conn)
            .unwrap();
        drop(conn);

        assert_eq!(job_status, "complete");
        assert_eq!(run_status.as_deref(), Some("exited"));
        assert!(
            emitter.events_named("agent-attention").is_empty(),
            "warm eviction should not emit a new completion notification"
        );
    }

    #[test]
    fn warm_evict_preserves_blocked_job_status() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "WARM");
        let issue_id = create_test_issue(&mut conn, &project_id, "Blocked issue");
        let job_id = create_test_job(
            &mut conn,
            &issue_id,
            &project_id,
            "builder",
            "blocked",
            None,
        );
        insert_test_run(
            &mut conn,
            "run-blocked",
            &issue_id,
            &project_id,
            &job_id,
            "session-2",
        );

        let (orch, emitter) = test_orchestrator_with_emitter(conn);
        register_warm_process(&orch, "run-blocked", "session-2", &job_id);

        kill_session_with_reason(&orch, "run-blocked", "warm_evict").unwrap();

        let mut conn = orch.db.conn.lock().unwrap();
        let job_status: String = jobs::table
            .find(&job_id)
            .select(jobs::status)
            .first(&mut *conn)
            .unwrap();
        let run_status: Option<String> = runs::table
            .find("run-blocked")
            .select(runs::status)
            .first(&mut *conn)
            .unwrap();
        drop(conn);

        assert_eq!(job_status, "blocked");
        assert_eq!(run_status.as_deref(), Some("exited"));
        assert!(
            emitter.events_named("agent-attention").is_empty(),
            "warm eviction should not surface a new attention event for blocked jobs"
        );
    }
}
