//! Pure checkpoint approval/rejection for the effect loop.
//!
//! These functions apply the DB transition for checkpoint approval/rejection
//! and return follow-on effects instead of executing them inline.

use crate::orchestrator::Orchestrator;
use crate::transitions::outcome::{apply_step_outcome, OutcomeContext, OutcomeSource, StepOutcome};

use super::types::WorkflowEffect;

/// Apply checkpoint approval and return follow-on effects.
///
/// Equivalent to `approve_job_inner` but returns effects instead of calling
/// `advance_dag` directly. Annotations are not handled here — they should
/// be stored before the effect is queued.
pub fn approve_job_pure(orch: &Orchestrator, job_id: &str) -> Result<Vec<WorkflowEffect>, String> {
    let ctx = OutcomeContext {
        run_id: None,
        source: OutcomeSource::CheckpointApproved,
    };
    let (_result, effects) = apply_step_outcome(orch, job_id, &ctx, StepOutcome::Complete)?;

    Ok(effects)
}

/// Apply checkpoint rejection (no recovery edge) and return follow-on effects.
///
/// Only handles the "mark as failed" path. The recovery-edge path (reset
/// checkpoint + continue upstream) is more complex and stays in the
/// existing `reject_job_inner` for now.
pub fn reject_job_pure(
    orch: &Orchestrator,
    job_id: &str,
    _reason: Option<&str>,
) -> Result<Vec<WorkflowEffect>, String> {
    let ctx = OutcomeContext {
        run_id: None,
        source: OutcomeSource::CheckpointRejected,
    };
    let (_result, effects) = apply_step_outcome(orch, job_id, &ctx, StepOutcome::Failed)?;

    Ok(effects)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::NewJob;
    use crate::schema::jobs;
    use crate::services::testing::TestServicesBuilder;
    use crate::test_utils::{create_test_project, test_diesel_conn};
    use diesel::prelude::*;
    use std::path::PathBuf;
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

    fn insert_blocked_job(conn: &mut diesel::sqlite::SqliteConnection, id: &str, project_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_job = NewJob {
            id,
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "blocked",
            agent_config_id: None,
            issue_id: None,
            project_id,
            task_description: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: None,
            node_name: Some("Checkpoint"),
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn approve_job_pure_transitions_to_complete_with_lifecycle_effect() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "AP1");
        insert_blocked_job(&mut conn, "job-1", &project_id);
        let orch = test_orchestrator(conn);

        let effects = approve_job_pure(&orch, "job-1").unwrap();

        // Job should now be complete
        let status: String = {
            let mut c = orch.db.conn.lock().unwrap();
            jobs::table
                .find("job-1")
                .select(jobs::status)
                .first(&mut *c)
                .unwrap()
        };
        assert_eq!(status, "complete");

        // Effects should include lifecycle message
        assert!(effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::EmitLifecycleMessage { .. })));
        // Should NOT include WakeManager (approval is not a failure)
        assert!(!effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::WakeManager { .. })));
    }

    #[test]
    fn reject_job_pure_transitions_to_failed_with_lifecycle_and_wake() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "RJ1");
        insert_blocked_job(&mut conn, "job-2", &project_id);
        let orch = test_orchestrator(conn);

        let effects = reject_job_pure(&orch, "job-2", Some("tests failed")).unwrap();

        // Job should now be failed
        let status: String = {
            let mut c = orch.db.conn.lock().unwrap();
            jobs::table
                .find("job-2")
                .select(jobs::status)
                .first(&mut *c)
                .unwrap()
        };
        assert_eq!(status, "failed");

        // Effects should include lifecycle message AND wake manager
        assert!(effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::EmitLifecycleMessage { .. })));
        assert!(effects
            .iter()
            .any(|e| matches!(e, WorkflowEffect::WakeManager { .. })));
    }

    #[test]
    fn approve_already_complete_returns_empty_effects() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "AP2");
        let now = chrono::Utc::now().timestamp() as i32;
        let new_job = NewJob {
            id: "job-3",
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: None,
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "complete",
            agent_config_id: None,
            issue_id: None,
            project_id: &project_id,
            task_description: None,
            created_at: now,
            updated_at: now,
            completed_at: Some(now),
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: None,
            node_name: Some("Checkpoint"),
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(&mut conn)
            .unwrap();
        let orch = test_orchestrator(conn);

        let effects = approve_job_pure(&orch, "job-3").unwrap();
        assert!(
            effects.is_empty(),
            "Already-complete job should produce no effects"
        );
    }
}
