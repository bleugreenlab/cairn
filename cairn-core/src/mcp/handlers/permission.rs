//! Permission prompt handling for Claude CLI --permission-prompt-tool
//!
//! Handles: permission_prompt
//!
//! When Claude wants to use a tool not in --allowedTools, it calls this MCP tool.
//! We store the request in the database, emit an event for the frontend, and
//! wait (blocking) for the user to approve or deny via the Orchestrator's
//! permission_responses broadcast channel.

use crate::diesel_models::NewPermissionRequest;
use crate::mcp::types::McpCallbackRequest;
use crate::models::ApprovalPolicy;
use crate::orchestrator::Orchestrator;
use crate::schema::{executions, jobs, permission_requests, runs};
use diesel::prelude::*;

use super::{emit_attention, get_issue_title, lookup_run, AttentionEvent};

/// Tools that are always allowed without user confirmation.
/// These tools are safe to auto-approve because they are non-destructive
/// or are standard Claude Code utilities (e.g., built-in /bug reporting).
const ALWAYS_ALLOWED_TOOLS: &[&str] = &["mcp__cairn__bug_report"];

/// Payload for permission_prompt tool
/// Note: Fields are snake_case to match what cairn-mcp sends
#[derive(Debug, serde::Deserialize)]
pub struct PermissionPromptPayload {
    pub tool_use_id: String,
    pub tool_name: String,
    pub input: serde_json::Value,
}

/// Handle permission_prompt tool call
/// Stores request in DB, emits event, waits for user response via broadcast channel
pub async fn handle_permission_prompt(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let services = &orch.services;
    let payload: PermissionPromptPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            return deny_response(&format!("Invalid payload: {}", e));
        }
    };

    log::info!(
        "Permission request: tool={}, tool_use_id={}",
        payload.tool_name,
        payload.tool_use_id
    );

    // Auto-allow tools that don't need user confirmation
    if ALWAYS_ALLOWED_TOOLS.contains(&payload.tool_name.as_str()) {
        log::info!("Auto-allowing tool: {}", payload.tool_name);
        return allow_response(&payload.input);
    }

    // Auto-allow tools previously approved via "Allow for Session"
    if let Ok(allowed) = orch.session_allowed_tools.lock() {
        if allowed.contains(&payload.tool_name) {
            log::info!("Auto-allowing session-approved tool: {}", payload.tool_name);
            return allow_response(&payload.input);
        }
    }

    // Short-circuit for RejectAll: auto-deny without DB insert or UI prompt
    if let Some(policy) = approval_policy_for_run(orch, request.run_id.as_deref()) {
        if policy == ApprovalPolicy::RejectAll {
            log::info!(
                "Auto-denying tool {} (approval policy: RejectAll)",
                payload.tool_name
            );
            return deny_response("Denied by agent approval policy (rejectAll)");
        }
    }

    // Get run_id - use request's run_id directly, or generate one for project chat
    let run_id = request
        .run_id
        .clone()
        .unwrap_or_else(|| format!("project-chat-{}", uuid::Uuid::new_v4()));

    // Store permission request
    let request_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    // Get current turn ID for this run
    let current_turn_id = orch.process_state.get_current_turn_id(&run_id);

    {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => return deny_response(&format!("DB lock failed: {}", e)),
        };

        let tool_input_json = serde_json::to_string(&payload.input).unwrap_or_default();

        let new_request = NewPermissionRequest {
            id: &request_id,
            run_id: &run_id,
            tool_use_id: &payload.tool_use_id,
            tool_name: &payload.tool_name,
            tool_input: &tool_input_json,
            status: "pending",
            created_at: now,
            turn_id: current_turn_id.as_deref(),
        };

        if let Err(e) = diesel::insert_into(permission_requests::table)
            .values(&new_request)
            .execute(&mut *conn)
        {
            return deny_response(&format!("Failed to store request: {}", e));
        }

        // Yield the current turn for permission
        if let Some(ref turn_id) = current_turn_id {
            let _ = crate::transitions::yield_turn(
                &mut conn,
                turn_id,
                crate::models::TurnYieldReason::Permission,
                &*orch.services.emitter,
            );
        }

        // Recompute issue status — may transition to Waiting (permission)
        let issue_id: Option<String> = runs::table
            .find(&run_id)
            .select(runs::issue_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten();
        if let Some(ref issue_id) = issue_id {
            crate::transitions::recompute_issue_status(&mut conn, issue_id);
        }
    }

    // Mark process as awaiting host (NOT GC-safe while holding continuation)
    if let Some(ref turn_id) = current_turn_id {
        orch.process_state.yield_for_host(&run_id, turn_id);
    }

    // Emit event for frontend
    let _ = services.emitter.emit(
        "permission-request",
        serde_json::json!({
            "requestId": request_id,
            "runId": run_id,
            "toolUseId": payload.tool_use_id,
            "toolName": payload.tool_name,
            "input": payload.input,
        }),
    );
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "permission_requests", "action": "insert"}),
    );

    // Emit attention event for toast notification
    if let Ok(mut conn) = orch.db.conn.lock() {
        if let Ok(ctx) = lookup_run(&mut conn, request) {
            let issue_title = ctx
                .issue_id
                .as_deref()
                .and_then(|id| get_issue_title(&mut conn, id));
            emit_attention(
                &*services.emitter,
                &AttentionEvent {
                    attention_type: "permission",
                    project_key: &ctx.project_key,
                    issue_number: ctx.issue_number,
                    issue_title: issue_title.as_deref(),
                    node_name: ctx.job_name.as_deref(),
                    exec_seq: ctx.exec_seq,
                    tool_name: Some(&payload.tool_name),
                },
            );
        }
    }

    // Subscribe to permission responses broadcast channel
    let mut rx = orch.permission_responses.subscribe();
    let request_id_clone = request_id.clone();

    // Wait for matching response with 5 minute timeout
    let result = tokio::time::timeout(std::time::Duration::from_secs(300), async {
        loop {
            match rx.recv().await {
                Ok((resp_request_id, response_json)) => {
                    if resp_request_id == request_id_clone {
                        return Ok(response_json);
                    }
                    // Not our request, keep waiting
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err("Channel closed");
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Missed some messages, keep going
                    continue;
                }
            }
        }
    })
    .await;

    match result {
        Ok(Ok(response)) => {
            // Create successor turn for the permission response
            if let Some(ref pred_turn_id) = current_turn_id {
                if let Ok(mut conn) = orch.db.conn.lock() {
                    let job_id: Option<String> = crate::schema::runs::table
                        .find(&run_id)
                        .select(crate::schema::runs::job_id)
                        .first::<Option<String>>(&mut *conn)
                        .ok()
                        .flatten();
                    let session_id: Option<String> = crate::schema::runs::table
                        .find(&run_id)
                        .select(crate::schema::runs::session_id)
                        .first::<Option<String>>(&mut *conn)
                        .ok()
                        .flatten();
                    if let (Some(job_id), Some(session_id)) = (job_id, session_id) {
                        let new_turn_id = uuid::Uuid::new_v4().to_string();
                        match crate::turns::queries::ensure_successor_turn(
                            &mut conn,
                            &new_turn_id,
                            &session_id,
                            &job_id,
                            pred_turn_id,
                            crate::models::TurnStartReason::PermissionResponse,
                            &*orch.services.emitter,
                        ) {
                            Ok(successor) => {
                                let _ = crate::transitions::start_turn(
                                    &mut conn,
                                    &successor.id,
                                    &run_id,
                                    &*orch.services.emitter,
                                );
                                orch.process_state
                                    .set_current_turn_id(&run_id, Some(&successor.id));
                            }
                            Err(e) => log::warn!("Failed to ensure successor turn: {}", e),
                        }
                    }
                }
            }
            response
        }
        Ok(Err(msg)) => deny_response(msg),
        Err(_) => {
            // Timeout - update the request status to denied
            if let Ok(mut conn) = orch.db.conn.lock() {
                let now = chrono::Utc::now().timestamp() as i32;
                let _ = diesel::update(permission_requests::table.find(&request_id))
                    .set((
                        permission_requests::status.eq("denied"),
                        permission_requests::response.eq(Some(
                            r#"{"behavior":"deny","message":"Request timed out after 5 minutes"}"#,
                        )),
                        permission_requests::responded_at.eq(Some(now)),
                    ))
                    .execute(&mut *conn);
            }
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "permission_requests", "action": "update"}),
            );
            deny_response("Permission request timed out after 5 minutes")
        }
    }
}

pub fn allow_response(original_input: &serde_json::Value) -> String {
    serde_json::json!({
        "behavior": "allow",
        "updatedInput": original_input
    })
    .to_string()
}

pub fn deny_response(message: &str) -> String {
    serde_json::json!({
        "behavior": "deny",
        "message": message
    })
    .to_string()
}

/// Look up the effective approval policy for a run.
///
/// Path: run → job → execution → snapshot → agent.approval_policy.
/// Returns None if any link is missing (e.g., project chat with no execution).
fn approval_policy_for_run(orch: &Orchestrator, run_id: Option<&str>) -> Option<ApprovalPolicy> {
    let run_id = run_id?;
    let mut conn = orch.db.conn.lock().ok()?;

    // run → job_id, then job → agent_config_id + execution_id
    let (job_id, agent_config_id, execution_id): (String, Option<String>, Option<String>) = {
        let jid: String = runs::table
            .find(run_id)
            .select(runs::job_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten()?;
        let (acid, eid) = jobs::table
            .find(&jid)
            .select((jobs::agent_config_id, jobs::execution_id))
            .first::<(Option<String>, Option<String>)>(&mut *conn)
            .ok()?;
        (jid, acid, eid)
    };
    let _ = job_id; // used only for the query chain

    // Look up the agent snapshot from the execution
    let execution_id = execution_id?;
    let agent_config_id = agent_config_id?;

    let snapshot_json: String = executions::table
        .find(&execution_id)
        .select(executions::snapshot)
        .first::<Option<String>>(&mut *conn)
        .ok()
        .flatten()?;

    let snapshot: crate::models::ExecutionSnapshot = serde_json::from_str(&snapshot_json).ok()?;
    let agent = snapshot.agents.get(&agent_config_id)?;
    agent.approval_policy
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::{NewExecution, NewRun, NewTurn};
    use crate::orchestrator::Orchestrator;
    use crate::schema::{executions, issues, jobs, permission_requests, turns};
    use crate::services::testing::TestServicesBuilder;
    use crate::test_utils::{
        create_test_issue, create_test_job, create_test_project, test_diesel_conn,
    };
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

    #[test]
    fn always_allowed_tools_uses_mcp_prefixed_names() {
        // Claude CLI sends tool names with the mcp__cairn__ prefix.
        // The constant must use prefixed names to match, not bare names like "bug_report".
        assert!(ALWAYS_ALLOWED_TOOLS.contains(&"mcp__cairn__bug_report"));
        assert!(!ALWAYS_ALLOWED_TOOLS.contains(&"bug_report"));
    }

    #[tokio::test]
    async fn permission_wait_recomputes_issue_to_waiting_on_yield() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "PERM");
        let issue_id = create_test_issue(&mut conn, &project_id, "Permission wait");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "running", None);
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::insert_into(executions::table)
            .values(&NewExecution {
                id: "exec-perm",
                recipe_id: "recipe-1",
                issue_id: Some(&issue_id),
                project_id: Some(&project_id),
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
            .execute(&mut conn)
            .unwrap();
        diesel::update(jobs::table.find(&job_id))
            .set(jobs::execution_id.eq(Some("exec-perm")))
            .execute(&mut conn)
            .unwrap();

        diesel::insert_into(crate::schema::runs::table)
            .values(&NewRun {
                id: "run-perm",
                issue_id: Some(&issue_id),
                project_id: Some(&project_id),
                job_id: Some(&job_id),
                chat_id: None,
                status: Some("live"),
                session_id: Some("session-perm"),
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: Some("fresh"),
            })
            .execute(&mut conn)
            .unwrap();
        diesel::insert_into(turns::table)
            .values(&NewTurn {
                id: "turn-perm",
                session_id: "session-perm",
                run_id: Some("run-perm"),
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

        let orch = test_orchestrator(conn);
        orch.process_state
            .set_current_turn_id("run-perm", Some("turn-perm"));

        let response_tx = orch.permission_responses.clone();
        let db = orch.db.clone();
        tokio::spawn(async move {
            for _ in 0..50 {
                if let Ok(mut conn) = db.conn.lock() {
                    let maybe_request_id: Option<String> = permission_requests::table
                        .select(permission_requests::id)
                        .first(&mut *conn)
                        .optional()
                        .ok()
                        .flatten();
                    if let Some(request_id) = maybe_request_id {
                        let _ =
                            response_tx.send((request_id, allow_response(&serde_json::json!({}))));
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            panic!("permission request was not inserted in time");
        });

        let request = crate::mcp::types::McpCallbackRequest {
            cwd: "/tmp/test-repo".to_string(),
            run_id: Some("run-perm".to_string()),
            tool: "permission_prompt".to_string(),
            payload: serde_json::json!({
                "tool_use_id": "tool-1",
                "tool_name": "bash",
                "input": {}
            }),
            tool_use_id: Some("tool-1".to_string()),
        };

        let response = handle_permission_prompt(&orch, &request).await;
        assert!(response.contains("behavior"));

        let mut conn = orch.db.conn.lock().unwrap();
        let issue_projection: (String, String, String) = issues::table
            .find(&issue_id)
            .select((issues::status, issues::progress, issues::attention))
            .first(&mut *conn)
            .unwrap();
        assert_eq!(issue_projection.0, "waiting");
        assert_eq!(issue_projection.1, "active");
        assert_eq!(issue_projection.2, "needs_authorization");
    }
}
