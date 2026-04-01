//! External MCP handlers - work without an active run/session.
//!
//! Project resolution uses the explicit `project` param (key or issue prefix).
//! CWD-based project lookup is not used — it silently resolves to wrong projects
//! when CWD is a worktree or doesn't match any project's repo_path.

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use crate::diesel_models::NewComment;
use crate::mcp::handlers::resources::ResourceInfo;
use crate::mcp::handlers::{lookup_project_by_key, parse_issue_identifier};
use crate::mcp::types::{CreateIssuePayload, McpCallbackRequest, UpdateIssuePayload};
use crate::models::{CreateIssue, UpdateIssue};
use crate::orchestrator::Orchestrator;
use crate::schema::{comments, issues as issues_table, projects};
use crate::services::RealClock;
use cairn_common::uri::{parse_uri, CairnResource};

/// Query available projects and format as "KEY (Name), ..." for error messages.
fn format_available_projects(conn: &mut SqliteConnection) -> String {
    let rows: Vec<(String, String)> = projects::table
        .select((projects::key, projects::name))
        .order(projects::key.asc())
        .load(conn)
        .unwrap_or_default();
    if rows.is_empty() {
        return String::new();
    }
    rows.iter()
        .map(|(key, name)| format!("{} ({})", key, name))
        .collect::<Vec<_>>()
        .join(", ")
}

// ============================================================================
// Issue Handlers (External)
// ============================================================================

/// Handle create_issue for external mode
pub async fn handle_create_issue_external(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> String {
    let payload: CreateIssuePayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!("create_issue_external: {}", payload.title);

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    let diesel_conn = &mut *conn;

    // Determine project context: explicit project key required
    let ctx = match payload.project {
        Some(ref key) => match lookup_project_by_key(diesel_conn, key) {
            Ok(c) => c,
            Err(e) => return e,
        },
        None => {
            let available = format_available_projects(diesel_conn);
            return if available.is_empty() {
                "project param required, but no projects are configured".to_string()
            } else {
                format!("project param required. Available projects: {}", available)
            };
        }
    };

    let input = CreateIssue {
        project_id: ctx.project_id.clone(),
        title: payload.title.clone(),
        description: payload.description,
        backend_override: None,
        manager_id: None,
    };

    match crate::issues::crud::create(diesel_conn, &RealClock, input) {
        Ok(issue) => {
            orch.sync(crate::sync::SyncMessage::Issue((&issue).into()));
            if let Err(e) = services.emitter.emit(
                "db-change",
                serde_json::json!({ "table": "issues", "action": "insert" }),
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

/// Handle update_issue for external mode
pub async fn handle_update_issue_external(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> String {
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

    log::info!("update_issue_external: #{}", issue_num);

    if payload.title.is_none() && payload.description.is_none() {
        return "No fields to update".to_string();
    }

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    let diesel_conn = &mut *conn;

    // Determine project context: explicit project field, then issue identifier prefix
    let ctx = if let Some(ref key) = payload.project {
        match lookup_project_by_key(diesel_conn, key) {
            Ok(c) => c,
            Err(e) => return e,
        }
    } else if let Some(ref key) = project_key_opt {
        match lookup_project_by_key(diesel_conn, key) {
            Ok(c) => c,
            Err(e) => return e,
        }
    } else {
        let available = format_available_projects(diesel_conn);
        return if available.is_empty() {
            "project param required, but no projects are configured".to_string()
        } else {
            format!("project param required: use the 'project' field or prefix the issue number. Available projects: {}", available)
        };
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
                serde_json::json!({ "table": "issues", "action": "update" }),
            ) {
                log::error!("Failed to emit db-change event: {}", e);
            }
            format!("Updated issue {}-{}", ctx.project_key, issue.number)
        }
        Err(e) => format!("Failed to update issue: {}", e),
    }
}

/// Handle add_comment for external mode - requires issue_number in payload
pub async fn handle_add_comment_external(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> String {
    // For external mode, we need issue_number in the payload
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct AddCommentExternalPayload {
        content: String,
        issue_number: Option<String>,
        /// Explicit project key (e.g., "CAIRN"). Falls back to CWD-based lookup.
        project: Option<String>,
    }

    let payload: AddCommentExternalPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    // Parse identifier to get optional project key and issue number
    let (project_key_opt, issue_num) = match payload.issue_number {
        Some(ref num) => match parse_issue_identifier(num) {
            Some(parsed) => parsed,
            None => {
                return format!(
                    "Invalid issue number: '{}'. Use formats like: 37, #37, or CAIRN-37",
                    num
                )
            }
        },
        None => return "issue_number is required for external mode".to_string(),
    };

    log::info!(
        "add_comment_external for issue #{}, {} chars",
        issue_num,
        payload.content.len()
    );

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    let diesel_conn = &mut *conn;

    // Determine project context: explicit project field, then issue identifier prefix
    let ctx = if let Some(ref key) = payload.project {
        match lookup_project_by_key(diesel_conn, key) {
            Ok(c) => c,
            Err(e) => return e,
        }
    } else if let Some(ref key) = project_key_opt {
        match lookup_project_by_key(diesel_conn, key) {
            Ok(c) => c,
            Err(e) => return e,
        }
    } else {
        let available = format_available_projects(diesel_conn);
        return if available.is_empty() {
            "project param required, but no projects are configured".to_string()
        } else {
            format!("project param required: use the 'project' field or prefix the issue number. Available projects: {}", available)
        };
    };

    // Look up issue ID by number
    let issue_id: Result<String, _> = issues_table::table
        .filter(issues_table::number.eq(issue_num))
        .filter(issues_table::project_id.eq(&ctx.project_id))
        .select(issues_table::id)
        .first(diesel_conn);

    let issue_id = match issue_id {
        Ok(id) => id,
        Err(_) => return format!("Issue {}-{} not found", ctx.project_key, issue_num),
    };

    // Insert the comment
    let now = chrono::Utc::now().timestamp() as i32;
    let comment_id = uuid::Uuid::new_v4().to_string();

    let new_comment = NewComment {
        id: &comment_id,
        issue_id: &issue_id,
        content: &payload.content,
        source: "agent",
        created_at: now,
    };

    if let Err(e) = diesel::insert_into(comments::table)
        .values(&new_comment)
        .execute(diesel_conn)
    {
        return format!("Failed to insert comment: {}", e);
    }

    orch.sync(crate::sync::SyncMessage::Comment(
        crate::sync::SyncComment {
            id: comment_id.clone(),
            issue_id: issue_id.clone(),
            content: payload.content.clone(),
            source: Some("agent".to_string()),
            created_at: Some(now as i64),
        },
    ));

    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "comments", "action": "insert"}),
    );

    format!("Comment added to issue {}-{}.", ctx.project_key, issue_num)
}

// ============================================================================
// Resource Handlers (External)
// ============================================================================

/// Handle list_resources for external mode - returns recent issues for current project.
/// No terminal resources (external clients don't have terminal access).
pub async fn handle_list_resources_external(
    orch: &Orchestrator,
    _request: &McpCallbackRequest,
) -> String {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Database error: {}", e),
    };

    use crate::schema::projects;

    // List all registered projects
    let all_projects: Vec<(String, String)> = projects::table
        .select((projects::id, projects::key))
        .load(&mut *conn)
        .unwrap_or_default();

    let mut resources: Vec<ResourceInfo> = Vec::new();

    for (project_id, project_key) in all_projects {
        // Project overview resource
        resources.push(ResourceInfo {
            uri: format!("cairn://{}", project_key),
            name: project_key.clone(),
            description: Some("Project overview".to_string()),
        });

        // Recent issues for this project (active/waiting first, then by updated_at)
        let issue_rows: Vec<(i32, String, String)> = issues_table::table
            .filter(issues_table::project_id.eq(&project_id))
            .order((
                diesel::dsl::sql::<diesel::sql_types::Integer>(
                    "CASE status WHEN 'Active' THEN 0 WHEN 'Waiting' THEN 1 ELSE 2 END",
                ),
                issues_table::updated_at.desc(),
            ))
            .limit(20)
            .select((
                issues_table::number,
                issues_table::title,
                issues_table::status,
            ))
            .load(&mut *conn)
            .unwrap_or_default();

        resources.extend(
            issue_rows
                .into_iter()
                .map(|(number, title, status)| ResourceInfo {
                    uri: format!("cairn://{}/{}", project_key, number),
                    name: format!("{}-{}", project_key, number),
                    description: Some(format!("[{}] {}", status, title)),
                }),
        );
    }

    serde_json::to_string(&resources).unwrap_or_else(|_| "[]".to_string())
}

/// Handle read_issue_resource for external mode - reads cairn:// URIs.
/// Rejects terminal URIs (not meaningful in external mode).
pub async fn handle_read_issue_resource_external(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> String {
    // Parse the URI from the payload
    #[derive(serde::Deserialize)]
    struct ReadPayload {
        uri: String,
    }

    let payload: ReadPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    // Validate it's a cairn:// URI
    let resource = match parse_uri(&payload.uri) {
        Some(r) => r,
        None => return format!("Invalid cairn resource URI: {}", payload.uri),
    };

    // Reject terminal URIs in external mode
    if matches!(
        resource,
        CairnResource::NodeTerminal { .. } | CairnResource::ProjectTerminal { .. }
    ) {
        return "Terminal resources are not available in external mode".to_string();
    }

    // Delegate to the existing issue resource handler
    super::issue_resources::handle_read_issue_resource(orch, request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::mcp::types::McpCallbackRequest;
    use crate::orchestrator::Orchestrator;
    use crate::services::testing::TestServicesBuilder;
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

    fn make_request(cwd: &str, tool: &str, payload: serde_json::Value) -> McpCallbackRequest {
        McpCallbackRequest {
            cwd: cwd.to_string(),
            run_id: None,
            tool: tool.to_string(),
            payload,
            tool_use_id: None,
        }
    }

    // =========================================================================
    // handle_update_issue_external: project field priority
    // =========================================================================

    #[tokio::test]
    async fn update_issue_uses_explicit_project_field() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Alpha", "AAA");
        let _other_id = create_test_project(&mut conn, "Beta", "BBB");
        let _issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        let orch = test_orchestrator(conn);

        // CWD doesn't match anything, but explicit project field resolves it
        let request = make_request(
            "/nonexistent",
            "update_issue",
            serde_json::json!({
                "issueNumber": "1",
                "title": "Updated title",
                "project": "AAA",
            }),
        );

        let result = handle_update_issue_external(&orch, &request).await;
        assert!(
            result.contains("Updated issue AAA-1"),
            "Expected success with project AAA, got: {}",
            result,
        );
    }

    #[tokio::test]
    async fn update_issue_project_field_overrides_identifier_prefix() {
        let mut conn = test_diesel_conn();
        let _alpha_id = create_test_project(&mut conn, "Alpha", "AAA");
        let beta_id = create_test_project(&mut conn, "Beta", "BBB");
        let _issue_id = create_test_issue(&mut conn, &beta_id, "Beta issue");

        let orch = test_orchestrator(conn);

        // issue_number has AAA prefix, but project field says BBB — project field wins
        let request = make_request(
            "/nonexistent",
            "update_issue",
            serde_json::json!({
                "issueNumber": "AAA-1",
                "title": "Updated title",
                "project": "BBB",
            }),
        );

        let result = handle_update_issue_external(&orch, &request).await;
        assert!(
            result.contains("Updated issue BBB-1"),
            "Expected project field to override identifier prefix, got: {}",
            result,
        );
    }

    // =========================================================================
    // handle_add_comment_external: project field priority
    // =========================================================================

    #[tokio::test]
    async fn add_comment_uses_explicit_project_field() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Alpha", "AAA");
        let _other_id = create_test_project(&mut conn, "Beta", "BBB");
        let _issue_id = create_test_issue(&mut conn, &project_id, "Test issue");

        let orch = test_orchestrator(conn);

        let request = make_request(
            "/nonexistent",
            "add_comment",
            serde_json::json!({
                "issueNumber": "1",
                "content": "Hello",
                "project": "AAA",
            }),
        );

        let result = handle_add_comment_external(&orch, &request).await;
        assert!(
            result.contains("Comment added to issue AAA-1"),
            "Expected success with project AAA, got: {}",
            result,
        );
    }

    #[tokio::test]
    async fn add_comment_project_field_overrides_identifier_prefix() {
        let mut conn = test_diesel_conn();
        let _alpha_id = create_test_project(&mut conn, "Alpha", "AAA");
        let beta_id = create_test_project(&mut conn, "Beta", "BBB");
        let _issue_id = create_test_issue(&mut conn, &beta_id, "Beta issue");

        let orch = test_orchestrator(conn);

        // issue_number has AAA prefix, but project field says BBB — project field wins
        let request = make_request(
            "/nonexistent",
            "add_comment",
            serde_json::json!({
                "issueNumber": "AAA-1",
                "content": "Hello",
                "project": "BBB",
            }),
        );

        let result = handle_add_comment_external(&orch, &request).await;
        assert!(
            result.contains("Comment added to issue BBB-1"),
            "Expected project field to override identifier prefix, got: {}",
            result,
        );
    }

    // =========================================================================
    // CWD fallback: handlers resolve project from CWD when no explicit project
    // =========================================================================

    #[tokio::test]
    async fn create_issue_external_requires_project_param() {
        let mut conn = test_diesel_conn();
        create_test_project(&mut conn, "Test", "TST");

        let orch = test_orchestrator(conn);

        // No project field — should require explicit project key
        let request = make_request(
            "/tmp/test-repo",
            "create_issue",
            serde_json::json!({
                "title": "Missing project",
            }),
        );

        let result = handle_create_issue_external(&orch, &request).await;
        assert!(
            result.contains("project param required"),
            "Expected project required error, got: {}",
            result,
        );
        assert!(
            result.contains("TST"),
            "Expected available projects to include TST, got: {}",
            result,
        );
        assert!(
            result.contains("Test"),
            "Expected available projects to include project name, got: {}",
            result,
        );
    }

    #[tokio::test]
    async fn create_issue_external_works_with_explicit_project() {
        let mut conn = test_diesel_conn();
        create_test_project(&mut conn, "Test", "TST");

        let orch = test_orchestrator(conn);

        let request = make_request(
            "/tmp/test-repo",
            "create_issue",
            serde_json::json!({
                "title": "Explicit project",
                "project": "TST",
            }),
        );

        let result = handle_create_issue_external(&orch, &request).await;
        assert!(
            result.contains("Created issue TST-"),
            "Expected issue creation, got: {}",
            result,
        );
    }

    // =========================================================================
    // Resource handlers (external)
    // =========================================================================

    // =========================================================================
    // Missing-project error messages with available projects listing
    // =========================================================================

    #[tokio::test]
    async fn create_issue_external_no_projects_configured() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);

        let request = make_request(
            "/tmp/test-repo",
            "create_issue",
            serde_json::json!({ "title": "No projects" }),
        );

        let result = handle_create_issue_external(&orch, &request).await;
        assert_eq!(
            result,
            "project param required, but no projects are configured",
        );
    }

    #[tokio::test]
    async fn update_issue_external_requires_project_shows_available() {
        let mut conn = test_diesel_conn();
        create_test_project(&mut conn, "Alpha", "AAA");
        create_test_project(&mut conn, "Beta", "BBB");

        let orch = test_orchestrator(conn);

        let request = make_request(
            "/nonexistent",
            "update_issue",
            serde_json::json!({
                "issueNumber": "1",
                "title": "Updated",
            }),
        );

        let result = handle_update_issue_external(&orch, &request).await;
        assert!(
            result.contains("project param required"),
            "Expected project required error, got: {}",
            result,
        );
        assert!(
            result.contains("AAA (Alpha)"),
            "Expected AAA listed, got: {}",
            result,
        );
        assert!(
            result.contains("BBB (Beta)"),
            "Expected BBB listed, got: {}",
            result,
        );
    }

    #[tokio::test]
    async fn update_issue_external_no_projects_configured() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);

        let request = make_request(
            "/nonexistent",
            "update_issue",
            serde_json::json!({
                "issueNumber": "1",
                "title": "Updated",
            }),
        );

        let result = handle_update_issue_external(&orch, &request).await;
        assert_eq!(
            result,
            "project param required, but no projects are configured",
        );
    }

    #[tokio::test]
    async fn add_comment_external_requires_project_shows_available() {
        let mut conn = test_diesel_conn();
        create_test_project(&mut conn, "Alpha", "AAA");
        create_test_project(&mut conn, "Beta", "BBB");

        let orch = test_orchestrator(conn);

        let request = make_request(
            "/nonexistent",
            "add_comment",
            serde_json::json!({
                "issueNumber": "1",
                "content": "Hello",
            }),
        );

        let result = handle_add_comment_external(&orch, &request).await;
        assert!(
            result.contains("project param required"),
            "Expected project required error, got: {}",
            result,
        );
        assert!(
            result.contains("AAA (Alpha)"),
            "Expected AAA listed, got: {}",
            result,
        );
        assert!(
            result.contains("BBB (Beta)"),
            "Expected BBB listed, got: {}",
            result,
        );
    }

    #[tokio::test]
    async fn add_comment_external_no_projects_configured() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);

        let request = make_request(
            "/nonexistent",
            "add_comment",
            serde_json::json!({
                "issueNumber": "1",
                "content": "Hello",
            }),
        );

        let result = handle_add_comment_external(&orch, &request).await;
        assert_eq!(
            result,
            "project param required, but no projects are configured",
        );
    }

    #[tokio::test]
    async fn create_issue_external_lists_multiple_projects_alphabetically() {
        let mut conn = test_diesel_conn();
        // Insert in reverse order to verify alphabetical sorting
        create_test_project(&mut conn, "Zebra", "ZZZ");
        create_test_project(&mut conn, "Alpha", "AAA");

        let orch = test_orchestrator(conn);

        let request = make_request(
            "/tmp/test-repo",
            "create_issue",
            serde_json::json!({ "title": "Test" }),
        );

        let result = handle_create_issue_external(&orch, &request).await;
        // AAA should appear before ZZZ regardless of insertion order
        let aaa_pos = result.find("AAA").expect("AAA should be in result");
        let zzz_pos = result.find("ZZZ").expect("ZZZ should be in result");
        assert!(
            aaa_pos < zzz_pos,
            "Expected AAA before ZZZ (alphabetical), got: {}",
            result,
        );
    }

    // =========================================================================
    // Resource handlers (external)
    // =========================================================================

    #[tokio::test]
    async fn list_resources_external_returns_issues() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        create_test_issue(&mut conn, &project_id, "First issue");
        create_test_issue(&mut conn, &project_id, "Second issue");

        let orch = test_orchestrator(conn);
        let request = make_request("/tmp/test-repo", "list_resources", serde_json::json!({}));

        let result = handle_list_resources_external(&orch, &request).await;
        let resources: Vec<ResourceInfo> = serde_json::from_str(&result).unwrap();

        assert_eq!(resources.len(), 3);
        // First resource is the project
        assert_eq!(resources[0].uri, "cairn://TST");
        assert_eq!(resources[0].name, "TST");
        // Then issues
        assert_eq!(resources[1].uri, "cairn://TST/1");
        assert_eq!(resources[1].name, "TST-1");
        assert!(resources[1]
            .description
            .as_ref()
            .unwrap()
            .contains("First issue"));
        assert_eq!(resources[2].uri, "cairn://TST/2");
    }

    #[tokio::test]
    async fn list_resources_external_empty_when_no_projects() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);
        let request = make_request("/nonexistent/path", "list_resources", serde_json::json!({}));

        let result = handle_list_resources_external(&orch, &request).await;
        let resources: Vec<ResourceInfo> = serde_json::from_str(&result).unwrap();
        assert!(resources.is_empty());
    }

    #[tokio::test]
    async fn read_issue_resource_external_rejects_terminal_uris() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);

        let request = make_request(
            "/tmp/test-repo",
            "read_issue_resource",
            serde_json::json!({ "uri": "cairn://TST/123/1/builder-1/terminal/dev-server" }),
        );

        let result = handle_read_issue_resource_external(&orch, &request).await;
        assert!(result.contains("Terminal resources are not available"));
    }

    #[tokio::test]
    async fn read_issue_resource_external_rejects_project_terminal_uris() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);

        let request = make_request(
            "/tmp/test-repo",
            "read_issue_resource",
            serde_json::json!({ "uri": "cairn://TST/terminal/dev-server" }),
        );

        let result = handle_read_issue_resource_external(&orch, &request).await;
        assert!(result.contains("Terminal resources are not available"));
    }

    #[tokio::test]
    async fn read_issue_resource_external_rejects_invalid_uri() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);

        let request = make_request(
            "/tmp/test-repo",
            "read_issue_resource",
            serde_json::json!({ "uri": "not-a-cairn-uri" }),
        );

        let result = handle_read_issue_resource_external(&orch, &request).await;
        assert!(result.contains("Invalid cairn resource URI"));
    }

    #[tokio::test]
    async fn list_resources_external_orders_active_and_waiting_first() {
        use crate::diesel_models::NewIssue;
        use crate::schema::issues;

        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        let now = chrono::Utc::now().timestamp() as i32;

        // Insert issues with different statuses and timestamps.
        // We'll give Backlog the most recent updated_at to prove status ordering
        // takes priority over recency.
        let test_issues = vec![
            ("id-1", 1, "Backlog issue", "Backlog", now + 3), // most recent, but low priority status
            ("id-2", 2, "Active issue", "Active", now + 1),   // older, but high priority status
            ("id-3", 3, "Waiting issue", "Waiting", now + 2), // mid recency, mid priority status
            ("id-4", 4, "Closed issue", "Closed", now + 4),   // most recent overall, low priority
        ];

        for (id, number, title, status, updated_at) in &test_issues {
            diesel::insert_into(issues::table)
                .values(NewIssue {
                    id,
                    project_id: &project_id,
                    number: *number,
                    title,
                    description: None,
                    status,
                    progress: match *status {
                        "Backlog" => "backlog",
                        "Active" => "active",
                        "Waiting" => "active",
                        "Closed" => "closed",
                        _ => "backlog",
                    },
                    attention: if *status == "Waiting" {
                        "needs_input"
                    } else {
                        "none"
                    },
                    priority: Some(0),
                    created_at: now,
                    updated_at: *updated_at,
                    model: None,
                    manager_id: None,
                })
                .execute(&mut conn)
                .unwrap();
        }

        // Update next_issue_number so the project state is consistent
        diesel::update(crate::schema::projects::table.find(&project_id))
            .set(crate::schema::projects::next_issue_number.eq(Some(5)))
            .execute(&mut conn)
            .unwrap();

        let orch = test_orchestrator(conn);
        let request = make_request("/tmp/test-repo", "list_resources", serde_json::json!({}));

        let result = handle_list_resources_external(&orch, &request).await;
        let resources: Vec<ResourceInfo> = serde_json::from_str(&result).unwrap();

        assert_eq!(resources.len(), 5);
        // Project resource first
        assert_eq!(resources[0].uri, "cairn://TST");
        // Active first, then Waiting, then others by updated_at desc
        assert_eq!(resources[1].name, "TST-2", "Active should be first");
        assert_eq!(resources[2].name, "TST-3", "Waiting should be second");
        // Remaining (Backlog, Closed) ordered by updated_at desc
        assert_eq!(
            resources[3].name, "TST-4",
            "Closed (most recent updated_at) third"
        );
        assert_eq!(
            resources[4].name, "TST-1",
            "Backlog (older updated_at) fourth"
        );
    }

    #[tokio::test]
    async fn list_resources_external_limits_to_20() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");

        // Create 25 issues
        for _ in 0..25 {
            create_test_issue(&mut conn, &project_id, "Issue");
        }

        let orch = test_orchestrator(conn);
        let request = make_request("/tmp/test-repo", "list_resources", serde_json::json!({}));

        let result = handle_list_resources_external(&orch, &request).await;
        let resources: Vec<ResourceInfo> = serde_json::from_str(&result).unwrap();

        assert_eq!(resources.len(), 21, "Should be 1 project + 20 issues");
    }

    #[tokio::test]
    async fn list_resources_external_description_includes_status_bracket() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        create_test_issue(&mut conn, &project_id, "My issue");

        let orch = test_orchestrator(conn);
        let request = make_request("/tmp/test-repo", "list_resources", serde_json::json!({}));

        let result = handle_list_resources_external(&orch, &request).await;
        let resources: Vec<ResourceInfo> = serde_json::from_str(&result).unwrap();

        // First resource is project, second is the issue
        // Description should be "[status] title"
        let desc = resources[1].description.as_ref().unwrap();
        assert_eq!(desc, "[backlog] My issue");
    }

    #[tokio::test]
    async fn list_resources_external_includes_project_resource() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        create_test_issue(&mut conn, &project_id, "An issue");

        let orch = test_orchestrator(conn);
        let request = make_request("/tmp/test-repo", "list_resources", serde_json::json!({}));

        let result = handle_list_resources_external(&orch, &request).await;
        let resources: Vec<ResourceInfo> = serde_json::from_str(&result).unwrap();

        assert_eq!(resources[0].uri, "cairn://TST");
        assert_eq!(resources[0].name, "TST");
        assert_eq!(
            resources[0].description.as_deref(),
            Some("Project overview")
        );
    }

    #[tokio::test]
    async fn read_issue_resource_external_rejects_missing_uri_field() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);

        let request = make_request(
            "/tmp/test-repo",
            "read_issue_resource",
            serde_json::json!({ "wrong_field": "cairn://TST/1" }),
        );

        let result = handle_read_issue_resource_external(&orch, &request).await;
        assert!(
            result.contains("Invalid payload"),
            "Expected payload error, got: {}",
            result
        );
    }

    #[tokio::test]
    async fn read_issue_resource_external_reads_issue() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TST");
        create_test_issue(&mut conn, &project_id, "Test issue");

        let orch = test_orchestrator(conn);
        let request = make_request(
            "/tmp/test-repo",
            "read_issue_resource",
            serde_json::json!({ "uri": "cairn://TST/1" }),
        );

        let result = handle_read_issue_resource_external(&orch, &request).await;
        assert!(
            result.contains("Test issue"),
            "Expected issue title in result: {}",
            result
        );
    }
}
