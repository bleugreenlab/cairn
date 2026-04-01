//! Memory-related MCP handlers.
//!
//! Handles: list_memories, create_memory, update_memory, deactivate_memory

use serde::Deserialize;

use super::lookup_project_context;
use crate::mcp::types::McpCallbackRequest;
use crate::memories::db as memory_db;
use crate::orchestrator::Orchestrator;

// ============================================================================
// Payload Types
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListMemoriesPayload {
    #[serde(default = "default_true")]
    pub active_only: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerCondition {
    pub trigger_index: i32,
    pub json_path: String,
    pub pattern: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMemoryPayload {
    pub content: String,
    pub confidence: Option<String>,
    pub source_issue: Option<String>,
    #[serde(default)]
    pub triggers: Vec<TriggerCondition>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub keywords: Option<Vec<String>>,
    #[serde(default)]
    pub source_run_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMemoryPayload {
    pub id: String,
    pub content: Option<String>,
    pub confidence: Option<String>,
    pub active: Option<bool>,
    pub triggers: Option<Vec<TriggerCondition>>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub keywords: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeactivateMemoryPayload {
    pub id: String,
}

// ============================================================================
// Handlers
// ============================================================================

/// Handle list_memories tool call
pub async fn handle_list_memories(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: ListMemoriesPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    // Get project context
    let project_id = match lookup_project_context(&mut conn, request) {
        Ok(ctx) => Some(ctx.project_id),
        Err(_) => None, // Fall back to global-only memories
    };

    let memories = if payload.active_only {
        memory_db::load_active_memories(&mut conn, project_id.as_deref())
    } else {
        memory_db::load_all_memories(&mut conn, project_id.as_deref())
    };

    match memories {
        Ok(memories) => {
            if memories.is_empty() {
                "No memories found.".to_string()
            } else {
                let mut output = format!("Found {} memory(s):\n\n", memories.len());
                for memory in &memories {
                    let project_scope = match &memory.project_id {
                        Some(_) => "project",
                        None => "global",
                    };
                    let status = if memory.active { "" } else { " [inactive]" };
                    let scope_info = if memory.scope != "project" {
                        format!(" | Scope: {}", memory.scope)
                    } else {
                        String::new()
                    };
                    let keywords_info = if !memory.keywords.is_empty() {
                        format!(" | Keywords: {}", memory.keywords.join(", "))
                    } else {
                        String::new()
                    };
                    output.push_str(&format!(
                        "- **{}** ({}{}): {}\n  Confidence: {} | Surfaced: {} times | Triggers: {}{}{}\n\n",
                        memory.id,
                        project_scope,
                        status,
                        memory.content,
                        memory.confidence,
                        memory.surfaced_count,
                        memory.triggers.len(),
                        scope_info,
                        keywords_info,
                    ));
                }
                output
            }
        }
        Err(e) => format!("Failed to list memories: {}", e),
    }
}

/// Handle create_memory tool call
pub async fn handle_create_memory(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: CreateMemoryPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    if payload.content.is_empty() {
        return "Memory content cannot be empty".to_string();
    }

    // Validate scope format
    if let Some(ref scope) = payload.scope {
        if scope != "project" && scope != "workspace" {
            if let Some(branch) = scope.strip_prefix("branch:") {
                if branch.is_empty() {
                    return "Branch scope requires a non-empty branch name".to_string();
                }
            } else {
                return format!(
                    "Invalid scope: '{}'. Must be 'project', 'workspace', or 'branch:<name>'",
                    scope
                );
            }
        }
    }

    // Validate regex patterns
    for trigger in &payload.triggers {
        if regex::Regex::new(&trigger.pattern).is_err() {
            return format!("Invalid regex pattern: {}", trigger.pattern);
        }
    }

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    // Get project context for scoping
    let project_id = match lookup_project_context(&mut conn, request) {
        Ok(ctx) => Some(ctx.project_id),
        Err(_) => None,
    };

    let id = uuid::Uuid::new_v4().to_string();
    let confidence = payload.confidence.as_deref().unwrap_or("tentative");

    // Default scope follows the caller's manager actor scope when available.
    let default_scope = if payload.scope.is_none() {
        request
            .run_id
            .as_ref()
            .and_then(|_| super::lookup_run(&mut conn, request).ok())
            .and_then(|run_ctx| {
                crate::managers::identity::lookup_manager_actor_by_run_context(&mut conn, &run_ctx)
                    .ok()
                    .flatten()
                    .map(|manager| match manager.scope_kind {
                        crate::models::ManagerScopeKind::Branch => {
                            format!("branch:{}", manager.branch)
                        }
                        crate::models::ManagerScopeKind::Project => "project".to_string(),
                        crate::models::ManagerScopeKind::Workspace => "workspace".to_string(),
                    })
            })
    } else {
        None
    };
    let scope = payload
        .scope
        .as_deref()
        .or(default_scope.as_deref())
        .unwrap_or("project");
    let keywords_json = payload
        .keywords
        .as_ref()
        .filter(|k| !k.is_empty())
        .map(|k| serde_json::to_string(k).unwrap_or_default());

    let triggers: Vec<(i32, &str, &str)> = payload
        .triggers
        .iter()
        .map(|t| (t.trigger_index, t.json_path.as_str(), t.pattern.as_str()))
        .collect();

    // Auto-populate source_run_id from request context if not explicitly provided
    let source_run_id = payload
        .source_run_id
        .as_deref()
        .or(request.run_id.as_deref());

    match memory_db::create_memory(
        &mut conn,
        &id,
        &payload.content,
        project_id.as_deref(),
        confidence,
        payload.source_issue.as_deref(),
        &triggers,
        scope,
        keywords_json.as_deref(),
        source_run_id,
    ) {
        Ok(memory) => {
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "memories", "action": "insert"}),
            );
            format!(
                "Created memory {} ({}, {} triggers, {} keywords): {}",
                memory.id,
                memory.confidence,
                memory.triggers.len(),
                memory.keywords.len(),
                memory.content
            )
        }
        Err(e) => format!("Failed to create memory: {}", e),
    }
}

/// Handle update_memory tool call
pub async fn handle_update_memory(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: UpdateMemoryPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    if payload.content.is_none()
        && payload.confidence.is_none()
        && payload.active.is_none()
        && payload.triggers.is_none()
        && payload.scope.is_none()
        && payload.keywords.is_none()
    {
        return "No fields to update".to_string();
    }

    // Validate scope format if provided
    if let Some(ref scope) = payload.scope {
        if scope != "project" {
            if let Some(branch) = scope.strip_prefix("branch:") {
                if branch.is_empty() {
                    return "Branch scope requires a non-empty branch name".to_string();
                }
            } else {
                return format!(
                    "Invalid scope: '{}'. Must be 'project' or 'branch:<name>'",
                    scope
                );
            }
        }
    }

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    // Update scalar fields if any provided
    let has_scalar = payload.content.is_some()
        || payload.confidence.is_some()
        || payload.active.is_some()
        || payload.scope.is_some()
        || payload.keywords.is_some();

    if has_scalar {
        let keywords_json = payload
            .keywords
            .as_ref()
            .map(|k| serde_json::to_string(k).unwrap_or_default());

        if let Err(e) = memory_db::update_memory(
            &mut conn,
            &payload.id,
            payload.content.as_deref(),
            payload.confidence.as_deref(),
            payload.active,
            payload.scope.as_deref(),
            keywords_json.as_ref().map(|k| Some(k.as_str())).or(None),
        ) {
            return format!("Failed to update memory: {}", e);
        }
    }

    // Replace triggers if provided
    if let Some(triggers) = &payload.triggers {
        // Validate regex patterns
        for trigger in triggers {
            if regex::Regex::new(&trigger.pattern).is_err() {
                return format!("Invalid regex pattern: {}", trigger.pattern);
            }
        }

        let trigger_tuples: Vec<(i32, &str, &str)> = triggers
            .iter()
            .map(|t| (t.trigger_index, t.json_path.as_str(), t.pattern.as_str()))
            .collect();
        if let Err(e) = memory_db::replace_triggers(&mut conn, &payload.id, &trigger_tuples) {
            return format!("Failed to update triggers: {}", e);
        }
    }

    match memory_db::load_memory(&mut conn, &payload.id) {
        Ok(memory) => {
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "memories", "action": "update"}),
            );
            format!("Updated memory {}: {}", memory.id, memory.content)
        }
        Err(e) => format!("Failed to load updated memory: {}", e),
    }
}

/// Handle deactivate_memory tool call
pub async fn handle_deactivate_memory(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: DeactivateMemoryPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    match memory_db::update_memory(&mut conn, &payload.id, None, None, Some(false), None, None) {
        Ok(memory) => {
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "memories", "action": "update"}),
            );
            format!("Deactivated memory {}: {}", memory.id, memory.content)
        }
        Err(e) => format!("Failed to deactivate memory: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::{NewJob, NewManager, NewRun};
    use crate::mcp::types::McpCallbackRequest;
    use crate::orchestrator::Orchestrator;
    use crate::schema::{managers, memories, runs};
    use crate::services::testing::TestServicesBuilder;
    use crate::test_utils::create_test_project;
    use diesel::prelude::*;
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

    /// Insert a job + run + manager. Returns (manager_id, run_id).
    fn insert_manager_run(
        conn: &mut diesel::sqlite::SqliteConnection,
        project_id: &str,
        branch: &str,
    ) -> (String, String) {
        let now = chrono::Utc::now().timestamp() as i32;
        let job_id = uuid::Uuid::new_v4().to_string();
        let manager_id = uuid::Uuid::new_v4().to_string();
        let run_id = uuid::Uuid::new_v4().to_string();

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

        (manager_id, run_id)
    }

    /// Insert a non-manager job + run. Returns run_id.
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

    fn memory_payload(triggers: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "content": "Test memory content",
            "triggers": triggers,
        })
    }

    #[tokio::test]
    async fn create_memory_defaults_to_branch_scope_for_manager() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let (_manager_id, run_id) = insert_manager_run(&mut conn, &project_id, "feature/cool-work");

        let orch = test_orchestrator(conn);
        let request = McpCallbackRequest {
            cwd: "/tmp/manager-worktree".to_string(),
            run_id: Some(run_id),
            tool: "create_memory".to_string(),
            payload: memory_payload(serde_json::json!([
                {"triggerIndex": 0, "jsonPath": "$.tool_name", "pattern": "write"}
            ])),
            tool_use_id: None,
        };

        let result = handle_create_memory(&orch, &request).await;
        assert!(
            result.starts_with("Created memory"),
            "Expected success, got: {}",
            result
        );

        // Verify scope was defaulted to branch:<manager.branch>
        let mut conn = orch.db.conn.lock().unwrap();
        let scope: String = memories::table
            .select(memories::scope)
            .order(memories::created_at.desc())
            .first(&mut *conn)
            .expect("memory should exist");

        assert_eq!(scope, "branch:feature/cool-work");
    }

    #[tokio::test]
    async fn create_memory_respects_explicit_scope_from_manager() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let (_manager_id, run_id) = insert_manager_run(&mut conn, &project_id, "feature/cool-work");

        let orch = test_orchestrator(conn);
        let request = McpCallbackRequest {
            cwd: "/tmp/manager-worktree".to_string(),
            run_id: Some(run_id),
            tool: "create_memory".to_string(),
            payload: serde_json::json!({
                "content": "Explicit project scope",
                "scope": "project",
                "triggers": [
                    {"triggerIndex": 0, "jsonPath": "$.tool_name", "pattern": "read"}
                ],
            }),
            tool_use_id: None,
        };

        let result = handle_create_memory(&orch, &request).await;
        assert!(
            result.starts_with("Created memory"),
            "Expected success, got: {}",
            result
        );

        let mut conn = orch.db.conn.lock().unwrap();
        let scope: String = memories::table
            .select(memories::scope)
            .order(memories::created_at.desc())
            .first(&mut *conn)
            .expect("memory should exist");

        assert_eq!(scope, "project");
    }

    #[tokio::test]
    async fn create_memory_defaults_to_project_scope_for_non_manager() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = crate::test_utils::create_test_issue(&mut conn, &project_id, "Existing");
        let run_id = insert_plain_run(&mut conn, &project_id, &issue_id);

        let orch = test_orchestrator(conn);
        let request = McpCallbackRequest {
            cwd: "/tmp/agent-worktree".to_string(),
            run_id: Some(run_id),
            tool: "create_memory".to_string(),
            payload: memory_payload(serde_json::json!([
                {"triggerIndex": 0, "jsonPath": "$.tool_name", "pattern": "write"}
            ])),
            tool_use_id: None,
        };

        let result = handle_create_memory(&orch, &request).await;
        assert!(
            result.starts_with("Created memory"),
            "Expected success, got: {}",
            result
        );

        let mut conn = orch.db.conn.lock().unwrap();
        let scope: String = memories::table
            .select(memories::scope)
            .order(memories::created_at.desc())
            .first(&mut *conn)
            .expect("memory should exist");

        assert_eq!(scope, "project");
    }

    #[tokio::test]
    async fn create_memory_auto_populates_source_run_id() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = crate::test_utils::create_test_issue(&mut conn, &project_id, "Issue");
        let run_id = insert_plain_run(&mut conn, &project_id, &issue_id);

        let orch = test_orchestrator(conn);
        let request = McpCallbackRequest {
            cwd: "/tmp/agent-worktree".to_string(),
            run_id: Some(run_id.clone()),
            tool: "create_memory".to_string(),
            payload: memory_payload(serde_json::json!([
                {"triggerIndex": 0, "jsonPath": "$.tool_name", "pattern": "write"}
            ])),
            tool_use_id: None,
        };

        let result = handle_create_memory(&orch, &request).await;
        assert!(
            result.starts_with("Created memory"),
            "Expected success, got: {}",
            result
        );

        // source_run_id should be auto-populated from request.run_id
        let mut conn = orch.db.conn.lock().unwrap();
        let source_run_id: Option<String> = memories::table
            .select(memories::source_run_id)
            .order(memories::created_at.desc())
            .first(&mut *conn)
            .expect("memory should exist");

        assert_eq!(source_run_id, Some(run_id));
    }

    #[tokio::test]
    async fn create_memory_explicit_source_run_id_overrides_request() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = crate::test_utils::create_test_issue(&mut conn, &project_id, "Issue");
        let run_id = insert_plain_run(&mut conn, &project_id, &issue_id);

        let orch = test_orchestrator(conn);
        let request = McpCallbackRequest {
            cwd: "/tmp/agent-worktree".to_string(),
            run_id: Some(run_id),
            tool: "create_memory".to_string(),
            payload: serde_json::json!({
                "content": "Memory with explicit source",
                "sourceRunId": "explicit-run-999",
                "triggers": [
                    {"triggerIndex": 0, "jsonPath": "$.tool_name", "pattern": "read"}
                ],
            }),
            tool_use_id: None,
        };

        let result = handle_create_memory(&orch, &request).await;
        assert!(
            result.starts_with("Created memory"),
            "Expected success, got: {}",
            result
        );

        // Explicit source_run_id should take precedence over request.run_id
        let mut conn = orch.db.conn.lock().unwrap();
        let source_run_id: Option<String> = memories::table
            .select(memories::source_run_id)
            .order(memories::created_at.desc())
            .first(&mut *conn)
            .expect("memory should exist");

        assert_eq!(source_run_id, Some("explicit-run-999".to_string()));
    }

    #[tokio::test]
    async fn create_memory_without_triggers_succeeds() {
        let mut conn = crate::test_utils::test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let issue_id = crate::test_utils::create_test_issue(&mut conn, &project_id, "Issue");
        let run_id = insert_plain_run(&mut conn, &project_id, &issue_id);

        let orch = test_orchestrator(conn);
        let request = McpCallbackRequest {
            cwd: "/tmp/agent-worktree".to_string(),
            run_id: Some(run_id),
            tool: "create_memory".to_string(),
            // No triggers, no keywords — previously would have been rejected
            payload: serde_json::json!({
                "content": "Triggerless memory",
            }),
            tool_use_id: None,
        };

        let result = handle_create_memory(&orch, &request).await;
        assert!(
            result.starts_with("Created memory"),
            "Expected success, got: {}",
            result
        );

        // Verify it was actually stored
        let mut conn = orch.db.conn.lock().unwrap();
        let count: i64 = memories::table.count().get_result(&mut *conn).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn create_memory_payload_deserializes_without_triggers() {
        // Verify #[serde(default)] on triggers field
        let json = serde_json::json!({
            "content": "No triggers provided",
        });
        let payload: CreateMemoryPayload = serde_json::from_value(json).unwrap();
        assert!(payload.triggers.is_empty());
        assert!(payload.source_run_id.is_none());
    }

    #[test]
    fn create_memory_payload_deserializes_with_source_run_id() {
        // Struct uses #[serde(rename_all = "camelCase")] so JSON key is "sourceRunId"
        let json = serde_json::json!({
            "content": "With source",
            "triggers": [],
            "sourceRunId": "run-xyz",
        });
        let payload: CreateMemoryPayload = serde_json::from_value(json).unwrap();
        assert_eq!(payload.source_run_id, Some("run-xyz".to_string()));
        assert!(payload.triggers.is_empty());
    }
}
