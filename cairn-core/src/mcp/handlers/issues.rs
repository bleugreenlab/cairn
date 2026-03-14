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

    if payload.title.is_none() && payload.description.is_none() && payload.skills.is_none() {
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
        model: None,
        skills: payload.skills,
    };

    match crate::issues::crud::update(diesel_conn, &RealClock, input) {
        Ok(issue) => {
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
