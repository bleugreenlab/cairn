//! Message-related MCP handlers.
//!
//! Handles: message (send a message to a channel or direct to an agent)

use diesel::prelude::*;
use serde::Deserialize;

use crate::execution::jobs::continue_job_impl;
use crate::mcp::types::McpCallbackRequest;
use crate::messages::{db as msg_db, delivery};
use crate::models::ChannelType;
use crate::orchestrator::Orchestrator;
use crate::schema::{executions, issues, jobs, projects, runs};

use super::lookup_run;

// ============================================================================
// Payload Types
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagePayload {
    /// Message content
    pub content: String,
    /// Target cairn:// URI. Determines scope:
    /// cairn://PROJECT → project, cairn://PROJECT/NUMBER → issue,
    /// cairn://PROJECT/NUMBER/EXEC/NODE → direct.
    /// Omit for project channel.
    pub to: Option<String>,
}

/// Parsed target from a cairn:// URI.
enum MessageTarget {
    /// Project channel: channel_type=project, channel_id=project_key
    Project { project_key: String },
    /// Issue channel: channel_type=issue, channel_id=KEY/NUMBER (e.g. "CRN/40")
    Issue { issue_key: String },
    /// Direct to a specific agent (identified by run_id lookup)
    Direct {
        project_key: String,
        issue_number: i32,
        _exec_seq: String,
        node_name: String,
    },
}

// ============================================================================
// Handler
// ============================================================================

/// Handle message tool call
pub async fn handle_message(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: MessagePayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    if payload.content.is_empty() {
        return "Message content cannot be empty".to_string();
    }

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    // Get sender context
    let run_ctx = match lookup_run(&mut conn, request) {
        Ok(ctx) => ctx,
        Err(e) => return format!("No active run: {}", e),
    };

    // Build sender URI: cairn://PROJECT/NUMBER/SEQ/NODE for issue agents,
    // or just the node name for project-level agents
    let node_name = run_ctx.job_name.as_deref().unwrap_or("agent");
    let sender_name = if let Some(issue_number) = run_ctx.issue_number {
        let exec_seq = run_ctx.execution_id.as_deref().and_then(|eid| {
            executions::table
                .find(eid)
                .select(executions::seq)
                .first::<Option<i32>>(&mut *conn)
                .ok()
                .flatten()
        });
        format!(
            "cairn://{}/{}/{}/{}",
            run_ctx.project_key,
            issue_number,
            exec_seq.unwrap_or(1),
            node_name
        )
    } else {
        node_name.to_string()
    };

    // Resolve target
    let target = match &payload.to {
        None => {
            // Default: project channel
            MessageTarget::Project {
                project_key: run_ctx.project_key.clone(),
            }
        }
        Some(uri) => match parse_message_target(uri) {
            Ok(t) => t,
            Err(e) => return format!("Invalid target URI: {}", e),
        },
    };

    // Route based on target
    match target {
        MessageTarget::Project { project_key } => {
            let msg = match msg_db::insert_message(
                &mut conn,
                &ChannelType::Project,
                Some(&project_key),
                Some(&run_ctx.run_id),
                &sender_name,
                None,
                &payload.content,
            ) {
                Ok(m) => m,
                Err(e) => return format!("Failed to send message: {}", e),
            };

            // Emit db-change event
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "messages", "action": "insert"}),
            );

            // Deliver to inboxes (drop conn first)
            drop(conn);
            delivery::deliver(orch, &msg);

            "Sent to project channel".to_string()
        }
        MessageTarget::Issue { issue_key } => {
            let msg = match msg_db::insert_message(
                &mut conn,
                &ChannelType::Issue,
                Some(&issue_key),
                Some(&run_ctx.run_id),
                &sender_name,
                None,
                &payload.content,
            ) {
                Ok(m) => m,
                Err(e) => return format!("Failed to send message: {}", e),
            };

            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "messages", "action": "insert"}),
            );

            drop(conn);
            delivery::deliver(orch, &msg);

            "Sent to issue channel".to_string()
        }
        MessageTarget::Direct {
            project_key,
            issue_number,
            node_name,
            ..
        } => {
            // Look up job + latest run for the recipient
            let (job_id, recipient_run_id) =
                match find_recipient_job(&mut conn, &project_key, issue_number, &node_name) {
                    Some(ids) => ids,
                    None => {
                        return format!(
                            "No agent found: {}/{}/{}",
                            project_key, issue_number, node_name
                        )
                    }
                };

            let msg = match msg_db::insert_message(
                &mut conn,
                &ChannelType::Direct,
                None,
                Some(&run_ctx.run_id),
                &sender_name,
                Some(&recipient_run_id),
                &payload.content,
            ) {
                Ok(m) => m,
                Err(e) => return format!("Failed to send message: {}", e),
            };

            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "messages", "action": "insert"}),
            );

            // Drop conn before continue_job_impl (it manages its own locks)
            drop(conn);

            // Resume the recipient agent the same way a user follow-up would.
            // This handles warm processes (stdin push) and cold processes (new session).
            let formatted = format!(
                "[Direct message from {}]\n{}\n\nTo reply, use the message tool with to: \"{}\"",
                msg.sender_name, msg.content, msg.sender_name
            );
            match continue_job_impl(orch, &job_id, Some(&formatted)) {
                Ok(_run) => format!("Sent direct message to {}", node_name),
                Err(e) => {
                    log::warn!("Direct message stored but resume failed: {}", e);
                    format!("Message stored but agent resume failed: {}", e)
                }
            }
        }
    }
}

// ============================================================================
// URI Parsing
// ============================================================================

/// Parse a cairn:// URI into a message target.
///
/// - `cairn://PROJECT` → project channel
/// - `cairn://PROJECT/NUMBER` → issue channel
/// - `cairn://PROJECT/NUMBER/EXEC/NODE` → direct to agent
fn parse_message_target(uri: &str) -> Result<MessageTarget, String> {
    let path = uri
        .strip_prefix("cairn://")
        .ok_or_else(|| format!("URI must start with cairn://, got: {}", uri))?;

    let parts: Vec<&str> = path.split('/').collect();

    match parts.as_slice() {
        [project_key] => Ok(MessageTarget::Project {
            project_key: project_key.to_string(),
        }),
        [_project_key, number] => {
            let issue_number: i32 = number
                .parse()
                .map_err(|_| format!("Invalid issue number: {}", number))?;
            // Store as issue_id placeholder — handler resolves
            Ok(MessageTarget::Issue {
                issue_key: format!("{}/{}", parts[0], issue_number),
            })
        }
        [project_key, number, exec_seq, node_name] => {
            let issue_number: i32 = number
                .parse()
                .map_err(|_| format!("Invalid issue number: {}", number))?;
            Ok(MessageTarget::Direct {
                project_key: project_key.to_string(),
                issue_number,
                _exec_seq: exec_seq.to_string(),
                node_name: node_name.to_string(),
            })
        }
        _ => Err(format!("Unrecognized URI format: {}", uri)),
    }
}

/// Find the job and latest run for a recipient agent from URI components.
/// Returns (job_id, run_id).
fn find_recipient_job(
    conn: &mut diesel::sqlite::SqliteConnection,
    project_key: &str,
    issue_number: i32,
    node_name: &str,
) -> Option<(String, String)> {
    // Find issue_id
    let issue_id: String = issues::table
        .inner_join(projects::table)
        .filter(projects::key.eq(project_key.to_uppercase()))
        .filter(issues::number.eq(issue_number))
        .select(issues::id)
        .first(conn)
        .ok()?;

    // Find the job by issue + node_name, get its latest run
    let (job_id, run_id): (String, String) = runs::table
        .inner_join(jobs::table.on(runs::job_id.eq(jobs::id.nullable())))
        .filter(jobs::issue_id.eq(&issue_id))
        .filter(jobs::node_name.eq(node_name))
        .order(runs::created_at.desc())
        .select((jobs::id, runs::id))
        .first(conn)
        .ok()?;

    Some((job_id, run_id))
}

/// Public helper: resolve a "to" URI into (channel_type, channel_id, recipient_run_id).
/// Used by the handler to convert URI-based addressing to DB-level addressing.
///
/// For project/issue targets, resolves project keys and issue numbers to IDs.
pub fn resolve_target(
    conn: &mut diesel::sqlite::SqliteConnection,
    uri: &str,
) -> Result<(ChannelType, Option<String>, Option<String>), String> {
    let target = parse_message_target(uri)?;

    match target {
        MessageTarget::Project { project_key } => {
            Ok((ChannelType::Project, Some(project_key), None))
        }
        MessageTarget::Issue { issue_key } => Ok((ChannelType::Issue, Some(issue_key), None)),
        MessageTarget::Direct {
            project_key,
            issue_number,
            node_name,
            ..
        } => {
            let (_job_id, run_id) =
                find_recipient_job(conn, &project_key, issue_number, &node_name)
                    .ok_or_else(|| format!("No agent found: {}", node_name))?;
            Ok((ChannelType::Direct, None, Some(run_id)))
        }
    }
}
