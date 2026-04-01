//! Communication-related MCP handlers.
//!
//! Handles: ask_user

use crate::diesel_models::NewPrompt;
use crate::mcp::types::{AskUserPayload, McpCallbackRequest};

use crate::orchestrator::Orchestrator;
use crate::schema::{prompts, runs};
use diesel::prelude::*;

use super::{emit_attention, get_issue_title, lookup_run, AttentionEvent};

const INLINE_PROMPT_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(45);

// ============================================================================
// Handlers
// ============================================================================

/// Handle ask_user tool call
/// Stores prompt in DB, transitions run to Idle, blocks waiting for user response
pub async fn handle_ask_user(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: AskUserPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!(
        "Prompting user for cwd={} with {} questions",
        request.cwd,
        payload.questions.len()
    );

    let services = &orch.services;

    // Look up the run and store prompt in DB
    let (
        run_id,
        prompt_id,
        project_key,
        issue_number,
        issue_title,
        node_name,
        exec_seq,
        current_turn_id,
    ) = {
        let db_state = &orch.db;

        let mut conn = match db_state.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Failed to lock database: {}", e),
        };

        let diesel_conn = &mut *conn;

        let ctx = match lookup_run(diesel_conn, request) {
            Ok(ctx) => ctx,
            Err(e) => return e,
        };

        let now = chrono::Utc::now().timestamp() as i32;
        let prompt_id = uuid::Uuid::new_v4().to_string();
        let questions_json = serde_json::to_string(&payload.questions).unwrap_or_default();

        // Insert prompt
        // Get current turn ID for this run
        let current_turn_id = orch.process_state.get_current_turn_id(&ctx.run_id);

        let new_prompt = NewPrompt {
            id: &prompt_id,
            run_id: &ctx.run_id,
            questions: &questions_json,
            response: None,
            created_at: now,
            answered_at: None,
            turn_id: current_turn_id.as_deref(),
        };

        if let Err(e) = diesel::insert_into(prompts::table)
            .values(&new_prompt)
            .execute(diesel_conn)
        {
            log::error!("Failed to insert prompt: {}", e);
            return format!("Failed to store prompt: {}", e);
        }

        // Yield the current turn for user input
        if let Some(ref turn_id) = current_turn_id {
            let _ = crate::transitions::yield_turn(
                diesel_conn,
                turn_id,
                crate::models::TurnYieldReason::UserInput,
                &*orch.services.emitter,
            );
        }

        // Yield the process for host interaction (run stays Live, occupancy → AwaitingHost)
        if let Some(ref turn_id) = current_turn_id {
            orch.process_state.yield_for_host(&ctx.run_id, turn_id);
        }

        // Recompute issue status — may transition to Waiting (ask_user)
        if let Some(ref iid) = ctx.issue_id {
            crate::transitions::recompute_issue_status(diesel_conn, iid);
        }

        let issue_title = ctx
            .issue_id
            .as_deref()
            .and_then(|id| get_issue_title(diesel_conn, id));

        (
            ctx.run_id,
            prompt_id,
            ctx.project_key,
            ctx.issue_number,
            issue_title,
            ctx.job_name,
            ctx.exec_seq,
            current_turn_id,
        )
    };

    // Emit events for frontend
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "prompts", "action": "insert"}),
    );
    let _ = services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "update"}),
    );

    // Emit run-paused event for frontend to show prompt UI
    let _ = services
        .emitter
        .emit("run-paused", serde_json::json!(&run_id));

    // Emit attention event for toast notification
    emit_attention(
        &*services.emitter,
        &AttentionEvent {
            attention_type: "prompt",
            project_key: &project_key,
            issue_number,
            issue_title: issue_title.as_deref(),
            node_name: node_name.as_deref(),
            exec_seq,
            tool_name: None,
        },
    );

    // Block waiting for user response (like permission_prompt does)
    let mut rx = orch.prompt_responses.subscribe();
    let prompt_id_clone = prompt_id.clone();

    // Keep a short inline fast-path, then durably suspend the run before the
    // surrounding callback/session budget expires.
    let result = tokio::time::timeout(INLINE_PROMPT_WAIT_BUDGET, async {
        loop {
            match rx.recv().await {
                Ok((resp_prompt_id, response_text)) => {
                    if resp_prompt_id == prompt_id_clone {
                        return Ok(response_text);
                    }
                    // Not our prompt, keep waiting
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
            // Create successor turn for the prompt response and transition run back to Running
            if let Ok(mut conn) = orch.db.conn.lock() {
                // Create successor turn linked to the yielded turn
                if let Some(ref pred_turn_id) = current_turn_id {
                    let job_id: Option<String> = runs::table
                        .find(&run_id)
                        .select(runs::job_id)
                        .first::<Option<String>>(&mut *conn)
                        .ok()
                        .flatten();
                    let session_id: Option<String> = runs::table
                        .find(&run_id)
                        .select(runs::session_id)
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
                            crate::models::TurnStartReason::PromptResponse,
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

                // Run stays Live — no status change needed on prompt response.
                // The successor turn handles the semantic state change.

                // Look up the issue_id for this run and recompute status
                let issue_id: Option<String> = runs::table
                    .find(&run_id)
                    .select(runs::issue_id)
                    .first::<Option<String>>(&mut *conn)
                    .ok()
                    .flatten();
                if let Some(ref iid) = issue_id {
                    crate::transitions::recompute_issue_status(&mut conn, iid);
                }
            }
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "runs", "action": "update"}),
            );
            let _ = services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "issues", "action": "update"}),
            );

            response
        }
        Ok(Err(msg)) => {
            log::warn!("Prompt wait channel ended for run {}: {}", run_id, msg);
            let _ = crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
                orch,
                &run_id,
                "prompt_wait_suspended",
            );
            "Prompt suspended; resume will continue from the real response.".to_string()
        }
        Err(_) => {
            let _ = crate::orchestrator::lifecycle::suspend_run_for_durable_wait(
                orch,
                &run_id,
                "prompt_wait_suspended",
            );
            "Prompt suspended; resume will continue from the real response.".to_string()
        }
    }
}
