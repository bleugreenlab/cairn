//! External MCP handlers - work without an active run/session.
//!
//! These handlers use lookup_project_by_cwd instead of lookup_run_by_cwd,
//! allowing them to work from Claude Code outside of the Cairn app.

use diesel::prelude::*;

use crate::diesel_models::NewComment;
use crate::mcp::handlers::{lookup_project_by_cwd, lookup_project_by_key, parse_issue_identifier};
use crate::mcp::types::{CreateIssuePayload, McpCallbackRequest, UpdateIssuePayload};
use crate::models::{CreateIssue, UpdateIssue};
use crate::orchestrator::Orchestrator;
use crate::schema::{comments, issues as issues_table};
use crate::services::RealClock;

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

    // Determine project context: use explicit project key if provided, else cwd-based lookup
    let ctx = if let Some(ref key) = payload.project {
        match lookup_project_by_key(diesel_conn, key) {
            Ok(c) => c,
            Err(e) => return e,
        }
    } else {
        match lookup_project_by_cwd(diesel_conn, &request.cwd) {
            Ok(c) => c,
            Err(e) => return e,
        }
    };

    let input = CreateIssue {
        project_id: ctx.project_id.clone(),
        title: payload.title.clone(),
        description: payload.description,
        model: None,
        skills: payload.skills,
    };

    match crate::issues::crud::create(diesel_conn, &RealClock, input) {
        Ok(issue) => {
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

    if payload.title.is_none() && payload.description.is_none() && payload.skills.is_none() {
        return "No fields to update".to_string();
    }

    let services = &orch.services;

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    let diesel_conn = &mut *conn;

    // Determine project context
    let ctx = if let Some(ref key) = project_key_opt {
        // Cross-project lookup by key
        match lookup_project_by_key(diesel_conn, key) {
            Ok(c) => c,
            Err(e) => return e,
        }
    } else {
        // Fall back to cwd-based lookup
        match lookup_project_by_cwd(diesel_conn, &request.cwd) {
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
        model: None,
        skills: payload.skills,
    };

    match crate::issues::crud::update(diesel_conn, &RealClock, input) {
        Ok(issue) => {
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

    // Determine project context
    let ctx = if let Some(ref key) = project_key_opt {
        // Cross-project lookup by key
        match lookup_project_by_key(diesel_conn, key) {
            Ok(c) => c,
            Err(e) => return e,
        }
    } else {
        // Fall back to cwd-based lookup
        match lookup_project_by_cwd(diesel_conn, &request.cwd) {
            Ok(c) => c,
            Err(e) => return e,
        }
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

    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "comments", "action": "insert"}),
    );

    format!("Comment added to issue {}-{}.", ctx.project_key, issue_num)
}
