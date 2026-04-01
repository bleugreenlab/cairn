//! Issue-related MCP handlers.
//!
//! Handles: create_issue, update_issue

use crate::orchestrator::Orchestrator;
use diesel::prelude::*;

use super::{lookup_project_by_key, lookup_project_context, parse_issue_identifier};
use crate::mcp::types::{CreateIssuePayload, McpCallbackRequest, UpdateIssuePayload};
use crate::models::{CreateIssue, UpdateIssue};
use crate::schema::issues as issues_table;
use crate::services::RealClock;

// ============================================================================
// Handlers
// ============================================================================

/// Handle create_issue tool call
pub async fn handle_create_issue(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: CreateIssuePayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!("create_issue: {}", payload.title);

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    let diesel_conn = &mut *conn;

    // Determine project context: use explicit project key if provided, else current project
    let ctx = if let Some(ref key) = payload.project {
        match lookup_project_by_key(diesel_conn, key) {
            Ok(c) => c,
            Err(e) => return e,
        }
    } else {
        match lookup_project_context(diesel_conn, request) {
            Ok(c) => c,
            Err(e) => return e,
        }
    };

    // Check if this run belongs to a manager (auto-link created issues)
    let manager_id = request
        .run_id
        .as_ref()
        .and_then(|_| super::lookup_run(diesel_conn, request).ok())
        .and_then(|run_ctx| {
            crate::managers::identity::lookup_manager_actor_by_run_context(diesel_conn, &run_ctx)
                .ok()
                .flatten()
                .map(|m| m.id)
        });

    let input = CreateIssue {
        project_id: ctx.project_id.clone(),
        title: payload.title.clone(),
        description: payload.description,
        backend_override: None,
        manager_id,
    };

    match crate::issues::crud::create(diesel_conn, &RealClock, input) {
        Ok(issue) => {
            orch.sync(crate::sync::SyncMessage::Issue((&issue).into()));
            if let Err(e) = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "issues", "action": "update"}),
            ) {
                log::error!("Failed to emit db-change event: {}", e);
            }
            format!(
                "Created issue {}-{}: \"{}\"",
                ctx.project_key, issue.number, issue.title
            )
        }
        Err(e) => format!("Failed to create issue: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::{NewJob, NewManager, NewRun};
    use crate::mcp::types::McpCallbackRequest;
    use crate::orchestrator::Orchestrator;
    use crate::schema::{managers, runs};
    use crate::services::testing::TestServicesBuilder;
    use crate::test_utils::create_test_project;
    use std::sync::{Arc, Mutex};

    /// Build a minimal Orchestrator backed by an in-memory database.
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

    /// Insert a job + run + manager linked together.
    /// Returns (manager_id, job_id, run_id).
    fn insert_manager_run(
        conn: &mut diesel::sqlite::SqliteConnection,
        project_id: &str,
        branch: &str,
    ) -> (String, String, String) {
        let now = chrono::Utc::now().timestamp() as i32;
        let job_id = uuid::Uuid::new_v4().to_string();
        let manager_id = uuid::Uuid::new_v4().to_string();
        let run_id = uuid::Uuid::new_v4().to_string();

        // Job (must exist before manager due to FK)
        let new_job = NewJob {
            id: &job_id,
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: Some("/tmp/manager-worktree"),
            branch: Some(branch),
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "running",
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
            node_name: Some("Manager"),
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(crate::schema::jobs::table)
            .values(&new_job)
            .execute(conn)
            .expect("insert job");

        // Manager linked to the job
        let new_manager = NewManager {
            id: &manager_id,
            project_id,
            home_project_id: None,
            scope_kind: "branch",
            name: "Test Manager",
            description: "",
            branch: Some(branch),
            job_id: Some(&job_id),
            status: "active",
            current_session_id: None,
            current_turn_id: None,
            last_wake_at: None,
            last_turn_completed_at: None,
            last_error: None,
            agent_config_id: None,
            model: None,
            parent_manager_id: None,
            created_at: now,
            updated_at: now,
            execution_id: None,
        };
        diesel::insert_into(managers::table)
            .values(&new_manager)
            .execute(conn)
            .expect("insert manager");

        // Run linked to the job
        let new_run = NewRun {
            id: &run_id,
            issue_id: None,
            project_id: Some(project_id),
            job_id: Some(&job_id),
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
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(conn)
            .expect("insert run");

        (manager_id, job_id, run_id)
    }

    /// Insert a non-manager job + run (plain agent). Returns run_id.
    fn insert_plain_run(
        conn: &mut diesel::sqlite::SqliteConnection,
        project_id: &str,
        issue_id: &str,
    ) -> String {
        let now = chrono::Utc::now().timestamp() as i32;
        let job_id = uuid::Uuid::new_v4().to_string();
        let run_id = uuid::Uuid::new_v4().to_string();

        let new_job = NewJob {
            id: &job_id,
            execution_id: None,
            manager_id: None,
            recipe_node_id: None,
            parent_job_id: None,
            worktree_path: Some("/tmp/agent-worktree"),
            branch: None,
            base_commit: None,
            current_session_id: None,
            resume_session_id: None,
            status: "running",
            agent_config_id: None,
            issue_id: Some(issue_id),
            project_id,
            task_description: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            parent_tool_use_id: None,
            task_index: None,
            started_at: Some(now),
            model: None,
            node_name: Some("Builder"),
            base_branch: None,
            current_turn_id: None,
        };
        diesel::insert_into(crate::schema::jobs::table)
            .values(&new_job)
            .execute(conn)
            .expect("insert job");

        let new_run = NewRun {
            id: &run_id,
            issue_id: Some(issue_id),
            project_id: Some(project_id),
            job_id: Some(&job_id),
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
        };
        diesel::insert_into(runs::table)
            .values(&new_run)
            .execute(conn)
            .expect("insert run");

        run_id
    }

    #[tokio::test]
    async fn create_issue_sets_manager_id_when_called_by_manager() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let (manager_id, _job_id, run_id) =
            insert_manager_run(&mut conn, &project_id, "feature/test");

        let orch = test_orchestrator(conn);
        let request = McpCallbackRequest {
            cwd: "/tmp/manager-worktree".to_string(),
            run_id: Some(run_id),
            tool: "create_issue".to_string(),
            payload: serde_json::json!({
                "title": "Test issue from manager"
            }),
            tool_use_id: None,
        };

        let result = handle_create_issue(&orch, &request).await;
        assert!(
            result.starts_with("Created issue TST-"),
            "Expected success, got: {}",
            result
        );

        // Verify manager_id was set on the created issue
        let mut conn = orch.db.conn.lock().unwrap();
        let actual_manager_id: Option<String> = issues_table::table
            .select(issues_table::manager_id)
            .order(issues_table::created_at.desc())
            .first(&mut *conn)
            .expect("issue should exist");

        assert_eq!(actual_manager_id, Some(manager_id));
    }

    #[tokio::test]
    async fn create_issue_leaves_manager_id_none_for_non_manager() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = crate::test_utils::create_test_issue(&mut conn, &project_id, "Existing");
        let run_id = insert_plain_run(&mut conn, &project_id, &issue_id);

        let orch = test_orchestrator(conn);
        let request = McpCallbackRequest {
            cwd: "/tmp/agent-worktree".to_string(),
            run_id: Some(run_id),
            tool: "create_issue".to_string(),
            payload: serde_json::json!({
                "title": "Test issue from plain agent"
            }),
            tool_use_id: None,
        };

        let result = handle_create_issue(&orch, &request).await;
        assert!(
            result.starts_with("Created issue TST-"),
            "Expected success, got: {}",
            result
        );

        // Verify manager_id is None
        let mut conn = orch.db.conn.lock().unwrap();
        let actual_manager_id: Option<String> = issues_table::table
            .filter(issues_table::title.eq("Test issue from plain agent"))
            .select(issues_table::manager_id)
            .first(&mut *conn)
            .expect("issue should exist");

        assert_eq!(actual_manager_id, None);
    }

    #[tokio::test]
    async fn create_issue_leaves_manager_id_none_without_run_id() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");

        // Create a manager but don't link the request to it
        insert_manager_run(&mut conn, &project_id, "feature/test");

        let orch = test_orchestrator(conn);
        let request = McpCallbackRequest {
            cwd: "/tmp/test-repo".to_string(), // matches project repo_path
            run_id: None,
            tool: "create_issue".to_string(),
            payload: serde_json::json!({
                "title": "Test issue without run"
            }),
            tool_use_id: None,
        };

        let result = handle_create_issue(&orch, &request).await;
        // Without run_id, falls back to cwd lookup which goes to project repo_path
        assert!(
            result.starts_with("Created issue TST-"),
            "Expected success, got: {}",
            result
        );

        let mut conn = orch.db.conn.lock().unwrap();
        let actual_manager_id: Option<String> = issues_table::table
            .filter(issues_table::title.eq("Test issue without run"))
            .select(issues_table::manager_id)
            .first(&mut *conn)
            .expect("issue should exist");

        assert_eq!(actual_manager_id, None);
    }
}

/// Handle update_issue tool call
pub async fn handle_update_issue(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: UpdateIssuePayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    // Parse identifier to get optional project key and issue number
    let (project_key_opt, issue_num) = match parse_issue_identifier(&payload.issue_number) {
        Some(parsed) => parsed,
        None => {
            return format!(
                "Invalid issue number: '{}'. Use formats like: 37, #37, or CAIRN-37",
                payload.issue_number
            )
        }
    };

    log::info!("update_issue: #{}", issue_num);

    if payload.title.is_none() && payload.description.is_none() {
        return "No fields to update".to_string();
    }

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    let diesel_conn = &mut *conn;

    // Determine project context: use explicit project key if provided, else current project
    let ctx = if let Some(ref key) = project_key_opt {
        match lookup_project_by_key(diesel_conn, key) {
            Ok(c) => c,
            Err(e) => return e,
        }
    } else {
        match lookup_project_context(diesel_conn, request) {
            Ok(c) => c,
            Err(e) => return e,
        }
    };

    let issue_id: Result<String, _> = issues_table::table
        .filter(issues_table::number.eq(issue_num))
        .filter(issues_table::project_id.eq(&ctx.project_id))
        .select(issues_table::id)
        .first(diesel_conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", ctx.project_key, issue_num),
    };

    let input = UpdateIssue {
        id: issue_id,
        title: payload.title,
        description: payload.description,
        backend_override: None,
    };

    match crate::issues::crud::update(diesel_conn, &RealClock, input) {
        Ok(issue) => {
            orch.sync(crate::sync::SyncMessage::Issue((&issue).into()));
            if let Err(e) = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "issues", "action": "update"}),
            ) {
                log::error!("Failed to emit db-change event: {}", e);
            }
            format!("Updated issue {}-{}", ctx.project_key, issue.number)
        }
        Err(e) => format!("Failed to update issue: {}", e),
    }
}
