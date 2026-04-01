//! Message-related MCP handlers.
//!
//! Handles: message (send a message to a channel or direct to an agent)

use diesel::prelude::*;
use serde::Deserialize;

use crate::execution::jobs::continue_job_impl;
use crate::mcp::types::McpCallbackRequest;
use crate::messages::{db as msg_db, delivery};
use crate::models::ChannelType;
use crate::node_segments::{matches_node_uri_segment, visible_node_segment_or_name};
use crate::orchestrator::Orchestrator;
use crate::schema::{issues, jobs, projects, runs};

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
        exec_seq: i32,
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
    let node_name = match run_ctx.job_name.as_deref() {
        Some(name) => name,
        None => {
            log::warn!(
                "No job_name for run_id={}, job_id={} — sender URI will use 'unknown'",
                run_ctx.run_id,
                run_ctx.job_id
            );
            "unknown"
        }
    };
    let sender_name = if let Some(issue_number) = run_ctx.issue_number {
        let recipe_node_id = jobs::table
            .find(&run_ctx.job_id)
            .select(jobs::recipe_node_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten();
        format!(
            "cairn://{}/{}/{}/{}",
            run_ctx.project_key,
            issue_number,
            run_ctx.exec_seq.unwrap_or(1),
            visible_node_segment_or_name(recipe_node_id.as_deref(), node_name)
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
            exec_seq,
            node_name,
        } => {
            // Look up job + latest run for the recipient
            let (job_id, recipient_run_id) = match find_recipient_job(
                &mut *conn,
                &project_key,
                issue_number,
                exec_seq,
                &node_name,
            ) {
                Some(ids) => ids,
                None => {
                    return format!(
                        "No agent found: {}/{}/{}",
                        project_key, issue_number, node_name
                    );
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
            match continue_job_impl(orch, &job_id, Some(&formatted), None) {
                Ok(_run) => format!("Sent direct message to {}", node_name),
                Err(e) => {
                    log::warn!("Direct message stored but resume failed: {}", e);
                    format!("Message stored but agent resume failed: {}", e)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_target;
    use crate::diesel_models::{NewExecution, NewJob, NewRun};
    use crate::mcp::handlers::messages::find_recipient_job;
    use crate::models::ChannelType;
    use crate::schema::{executions, jobs, runs};
    use crate::test_utils::{create_test_issue, create_test_project, test_diesel_conn};
    use diesel::prelude::*;
    use uuid::Uuid;

    fn insert_execution(conn: &mut diesel::sqlite::SqliteConnection, issue_id: &str) -> String {
        let execution_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(executions::table)
            .values(&NewExecution {
                id: &execution_id,
                recipe_id: "recipe-1",
                issue_id: Some(issue_id),
                project_id: None,
                status: "running",
                started_at: now,
                completed_at: None,
                snapshot: None,
                seq: Some(1),
                initiator_sub: None,
                initiator_auth_mode: None,
                initiator_org_id: None,
                triggered_by: "manual",
            })
            .execute(conn)
            .unwrap();
        execution_id
    }

    fn insert_job_and_run(
        conn: &mut diesel::sqlite::SqliteConnection,
        issue_id: &str,
        project_id: &str,
        execution_id: &str,
        recipe_node_id: &str,
        node_name: &str,
    ) -> (String, String) {
        let job_id = Uuid::new_v4().to_string();
        let run_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::insert_into(jobs::table)
            .values(&NewJob {
                id: &job_id,
                execution_id: Some(execution_id),
                manager_id: None,
                recipe_node_id: Some(recipe_node_id),
                parent_job_id: None,
                worktree_path: None,
                branch: None,
                base_commit: None,
                current_session_id: None,
                resume_session_id: None,
                status: "running",
                agent_config_id: Some("Build"),
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
                node_name: Some(node_name),
                base_branch: None,
                current_turn_id: None,
            })
            .execute(conn)
            .unwrap();

        diesel::insert_into(runs::table)
            .values(&NewRun {
                id: &run_id,
                issue_id: Some(issue_id),
                project_id: None,
                job_id: Some(&job_id),
                status: Some("live"),
                session_id: None,
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                chat_id: None,
                backend: None,
                exit_reason: None,
                start_mode: None,
            })
            .execute(conn)
            .unwrap();

        (job_id, run_id)
    }

    #[test]
    fn resolve_target_accepts_slugified_node_uri() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CAIRN");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");
        let execution_id = insert_execution(&mut conn, &issue_id);
        let (_job_id, run_id) = insert_job_and_run(
            &mut conn,
            &issue_id,
            &project_id,
            &execution_id,
            "54e54f2d-4ff1-45c5-ad0e-5e5f5846ea67",
            "Builder",
        );

        let resolved = resolve_target(&mut conn, "cairn://CAIRN/1/1/builder").unwrap();
        assert_eq!(resolved.0, ChannelType::Direct);
        assert_eq!(resolved.2.as_deref(), Some(run_id.as_str()));
    }

    #[test]
    fn resolve_target_accepts_exact_stored_node_name() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CAIRN");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");
        let execution_id = insert_execution(&mut conn, &issue_id);
        let (_job_id, run_id) = insert_job_and_run(
            &mut conn,
            &issue_id,
            &project_id,
            &execution_id,
            "54e54f2d-4ff1-45c5-ad0e-5e5f5846ea67",
            "Builder",
        );

        let resolved = resolve_target(&mut conn, "cairn://CAIRN/1/1/Builder").unwrap();
        assert_eq!(resolved.2.as_deref(), Some(run_id.as_str()));
    }

    #[test]
    fn resolve_target_accepts_legacy_recipe_node_id_uri() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CAIRN");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");
        let execution_id = insert_execution(&mut conn, &issue_id);
        let recipe_node_id = "54e54f2d-4ff1-45c5-ad0e-5e5f5846ea67";
        let (_job_id, run_id) = insert_job_and_run(
            &mut conn,
            &issue_id,
            &project_id,
            &execution_id,
            recipe_node_id,
            "Builder",
        );

        let slug_result = find_recipient_job(&mut conn, "CAIRN", 1, 1, "builder").unwrap();
        let legacy_result = find_recipient_job(&mut conn, "CAIRN", 1, 1, recipe_node_id).unwrap();

        assert_eq!(slug_result.1, run_id);
        assert_eq!(legacy_result.1, run_id);
        assert_eq!(slug_result, legacy_result);
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
            let exec_seq: i32 = exec_seq
                .parse()
                .map_err(|_| format!("Invalid execution sequence: {}", exec_seq))?;
            Ok(MessageTarget::Direct {
                project_key: project_key.to_string(),
                issue_number,
                exec_seq,
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
    exec_seq: i32,
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

    let execution_id: String = crate::schema::executions::table
        .filter(crate::schema::executions::issue_id.eq(&issue_id))
        .filter(crate::schema::executions::seq.eq(exec_seq))
        .select(crate::schema::executions::id)
        .first(conn)
        .ok()?;

    let candidates: Vec<(String, Option<String>, Option<String>, String)> = runs::table
        .inner_join(jobs::table.on(runs::job_id.eq(jobs::id.nullable())))
        .filter(jobs::issue_id.eq(&issue_id))
        .filter(jobs::execution_id.eq(&execution_id))
        .order(runs::created_at.desc())
        .select((jobs::id, jobs::node_name, jobs::recipe_node_id, runs::id))
        .load(conn)
        .ok()?;

    candidates
        .into_iter()
        .find_map(|(job_id, stored_node_name, recipe_node_id, run_id)| {
            let stored_node_name = stored_node_name?;
            if matches_node_uri_segment(
                node_name,
                recipe_node_id.as_deref(),
                Some(&stored_node_name),
            ) {
                Some((job_id, run_id))
            } else {
                None
            }
        })
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
            exec_seq,
            node_name,
            ..
        } => {
            let (_job_id, run_id) =
                find_recipient_job(conn, &project_key, issue_number, exec_seq, &node_name)
                    .ok_or_else(|| format!("No agent found: {}", node_name))?;
            Ok((ChannelType::Direct, None, Some(run_id)))
        }
    }
}
