use super::app_server::AppServerClient;
use super::{CodexBackend, CODEX_BACKEND_NAME};
use crate::backends::run_state::run_backend_db;
use crate::backends::AgentBackend;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use turso::params;

pub(super) fn codex_mcp_elicitation_accept_payload() -> Value {
    serde_json::json!({
        "action": "accept",
        "content": Value::Null,
        "_meta": Value::Null
    })
}

pub fn extract_codex_mcp_elicitation_cairn_tool_name(params: &Value) -> Option<String> {
    if params.get("serverName").and_then(|v| v.as_str()) != Some("cairn") {
        return None;
    }

    let tool_name = params
        .get("toolName")
        .and_then(|v| v.as_str())
        .or_else(|| {
            params
                .get("_meta")
                .and_then(|v| v.as_object())
                .and_then(|meta| meta.get("toolName").or_else(|| meta.get("tool_name")))
                .and_then(|v| v.as_str())
        })
        .or_else(|| {
            params
                .get("message")
                .and_then(|v| v.as_str())
                .and_then(extract_codex_mcp_tool_name_from_message)
        })?;

    Some(format!("mcp__cairn__{tool_name}"))
}

pub(super) fn extract_codex_mcp_tool_name_from_message(message: &str) -> Option<&str> {
    let marker = "run tool \"";
    let start = message.find(marker)? + marker.len();
    let tail = &message[start..];
    let end = tail.find('"')?;
    Some(&tail[..end])
}

pub(super) fn codex_mcp_elicitation_preflight_response(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    params: &Value,
    id_value: &Value,
) -> Result<Option<Value>, String> {
    let Some(tool_name) = extract_codex_mcp_elicitation_cairn_tool_name(params) else {
        return Ok(None);
    };

    if codex_mcp_tool_is_allowed(orch, run_db, run_id, &tool_name)? {
        log::info!(
            "Auto-accepting Codex MCP elicitation for already-allowed tool {}",
            tool_name
        );
        return Ok(Some(codex_mcp_elicitation_accept_payload()));
    }

    let tool_use_id = codex_mcp_elicitation_tool_use_id(id_value);
    let response =
        request_codex_permission(orch, run_db, run_id, &tool_use_id, &tool_name, params, true)?;
    Ok(Some(response))
}

pub(super) fn codex_mcp_tool_is_allowed(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    tool_name: &str,
) -> Result<bool, String> {
    if let Ok(allowed) = orch.session_allowed_tools.lock() {
        if allowed.contains(tool_name) {
            return Ok(true);
        }
    }

    Ok(load_codex_allowed_tools_for_run(orch, run_db, run_id)?.contains(tool_name))
}

pub(super) fn load_codex_allowed_tools_for_run(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
) -> Result<HashSet<String>, String> {
    use crate::config::agents as config_agents;

    let db = run_db.clone();
    let run_id = run_id.to_string();
    let lookup = run_backend_db(CODEX_BACKEND_NAME, async move {
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT job_id, project_id
                         FROM runs
                         WHERE id = ?1",
                        params![run_id.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    DbError::internal(format!(
                        "Failed to load run for Codex MCP approval: {}",
                        run_id
                    ))
                })?;
                let job_id = row.opt_text(0)?;
                let project_id = row.opt_text(1)?;
                let (Some(job_id), Some(project_id)) = (job_id, project_id) else {
                    return Ok::<_, DbError>(None);
                };

                let mut rows = conn
                    .query(
                        "SELECT agent_config_id, execution_id
                         FROM jobs
                         WHERE id = ?1",
                        params![job_id.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.ok_or_else(|| {
                    DbError::internal(format!(
                        "Failed to load job for Codex MCP approval: {}",
                        job_id
                    ))
                })?;
                let agent_config_id = row.opt_text(0)?;
                let execution_id = row.opt_text(1)?;
                let Some(agent_config_id) = agent_config_id else {
                    return Ok::<_, DbError>(None);
                };

                let mut rows = conn
                    .query(
                        "SELECT repo_path
                         FROM projects
                         WHERE id = ?1",
                        params![project_id.as_str()],
                    )
                    .await?;
                let project_path = rows
                    .next()
                    .await?
                    .map(|row| row.text(0))
                    .transpose()?
                    .map(std::path::PathBuf::from);

                let snapshot_json = if let Some(execution_id) = execution_id {
                    let mut rows = conn
                        .query(
                            "SELECT snapshot
                             FROM executions
                             WHERE id = ?1",
                            params![execution_id.as_str()],
                        )
                        .await?;
                    rows.next()
                        .await?
                        .map(|row| row.opt_text(0))
                        .transpose()?
                        .flatten()
                } else {
                    None
                };

                Ok::<_, DbError>(Some((agent_config_id, project_path, snapshot_json)))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;

    let Some((agent_config_id, project_path, snapshot_json)) = lookup else {
        return Ok(HashSet::new());
    };

    let tool_config = if let Some(snapshot_json) = snapshot_json.as_deref() {
        load_codex_tool_config_from_snapshot(snapshot_json, &agent_config_id)?
    } else {
        None
    }
    .or_else(|| {
        config_agents::get_agent(&orch.config_dir, &agent_config_id, project_path.as_deref())
            .ok()
            .flatten()
            .map(|agent| (agent.tools, agent.disallowed_tools.unwrap_or_default()))
    });

    let Some((tools, disallowed_tools)) = tool_config else {
        return Ok(HashSet::new());
    };

    let resolved = CodexBackend.resolve_tools(&tools, &disallowed_tools);
    Ok(resolved.allowed.into_iter().collect())
}

type CodexToolConfig = (Vec<String>, Vec<String>);

pub(super) fn load_codex_tool_config_from_snapshot(
    snapshot_json: &str,
    agent_config_id: &str,
) -> Result<Option<CodexToolConfig>, String> {
    let snapshot: crate::models::ExecutionSnapshot =
        serde_json::from_str(snapshot_json).map_err(|e| {
            format!(
                "Failed to parse execution snapshot for Codex approval: {}",
                e
            )
        })?;

    Ok(snapshot.agents.get(agent_config_id).map(|agent| {
        (
            agent.tools.clone(),
            agent.disallowed_tools.clone().unwrap_or_default(),
        )
    }))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_codex_approval_request(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    tool_name: &str,
    params: Value,
    client: &AppServerClient,
    id_value: &Value,
    allow_accept_for_session: bool,
) -> Result<(), String> {
    let item_id = params
        .get("itemId")
        .and_then(|v| v.as_str())
        .unwrap_or("codex-approval");
    let response = request_codex_permission(
        orch,
        run_db,
        run_id,
        item_id,
        tool_name,
        &params,
        allow_accept_for_session,
    )?;
    client.respond(id_value, response)
}

pub(super) fn handle_codex_mcp_server_elicitation_request(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    params: Value,
    client: &AppServerClient,
    id_value: &Value,
) -> Result<(), String> {
    if is_codex_mcp_tool_approval_elicitation(&params) {
        if let Some(response) =
            codex_mcp_elicitation_preflight_response(orch, run_db, run_id, &params, id_value)?
        {
            return client.respond(id_value, response);
        }
    }

    log::warn!(
        "Unsupported Codex MCP elicitation request from server={} mode={}; declining",
        params
            .get("serverName")
            .and_then(|v| v.as_str())
            .unwrap_or("<unknown>"),
        params
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("<unknown>")
    );
    client.respond(id_value, codex_mcp_elicitation_decline_payload())
}

pub(super) fn decline_codex_native_file_change(
    client: &AppServerClient,
    id_value: &Value,
) -> Result<(), String> {
    client.respond(id_value, native_file_change_decline_payload())
}

pub(super) fn native_edit_decline_message() -> &'static str {
    "Native Codex file edits and apply_patch are disabled in Cairn. Use mcp__cairn__write instead."
}

pub(super) fn native_file_change_decline_payload() -> Value {
    serde_json::json!({
        "decision": "decline",
        "message": native_edit_decline_message()
    })
}

pub(super) fn is_empty_object_schema(value: &Value) -> bool {
    value.as_object().is_some_and(|schema| {
        schema.get("type").and_then(|v| v.as_str()) == Some("object")
            && schema
                .get("properties")
                .and_then(|v| v.as_object())
                .is_some_and(serde_json::Map::is_empty)
    })
}

pub(super) fn is_codex_mcp_tool_approval_elicitation(params: &Value) -> bool {
    if params.get("mode").and_then(|v| v.as_str()) != Some("form") {
        return false;
    }

    let is_tool_approval = params
        .get("_meta")
        .and_then(|v| v.as_object())
        .and_then(|meta| meta.get("codex_approval_kind"))
        .and_then(|v| v.as_str())
        == Some("mcp_tool_call");

    if !is_tool_approval {
        return false;
    }

    match params.get("requestedSchema") {
        None | Some(Value::Null) => true,
        Some(schema) => is_empty_object_schema(schema),
    }
}

pub(super) fn codex_mcp_elicitation_decline_payload() -> Value {
    serde_json::json!({
        "action": "decline",
        "content": Value::Null,
        "_meta": Value::Null
    })
}

pub(super) fn codex_mcp_elicitation_tool_use_id(id_value: &Value) -> String {
    let request_id = match id_value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        other => serde_json::to_string(other).unwrap_or_else(|_| "unknown".to_string()),
    };
    format!("codex-mcp-elicitation:{request_id}")
}

#[allow(clippy::too_many_arguments)]
pub(super) fn request_codex_permission(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    tool_use_id: &str,
    tool_name: &str,
    tool_input: &Value,
    allow_accept_for_session: bool,
) -> Result<Value, String> {
    // Intrinsic prefixing (CAIRN-2210): inherit the owning run's scope so a team
    // codex permission_request routes back to the replica via routing_db_for_id.
    let request_id = cairn_common::ids::mint_child(run_id);
    let now = chrono::Utc::now().timestamp() as i32;
    let tool_input_json = serde_json::to_string(tool_input).unwrap_or_default();

    {
        let db = run_db.clone();
        let request_id_for_db = request_id.clone();
        let run_id_for_db = run_id.to_string();
        let tool_use_id_for_db = tool_use_id.to_string();
        let tool_name_for_db = tool_name.to_string();
        let tool_input_json_for_db = tool_input_json.clone();
        let current_turn_id = orch.process_state.get_current_turn_id(run_id);
        run_backend_db(CODEX_BACKEND_NAME, async move {
            db.write(|conn| {
                let request_id = request_id_for_db.clone();
                let run_id = run_id_for_db.clone();
                let tool_use_id = tool_use_id_for_db.clone();
                let tool_name = tool_name_for_db.clone();
                let tool_input_json = tool_input_json_for_db.clone();
                let current_turn_id = current_turn_id.clone();
                Box::pin(async move {
                    let job_id = {
                        let mut rows = conn
                            .query(
                                "SELECT job_id FROM runs WHERE id = ?1 LIMIT 1",
                                params![run_id.as_str()],
                            )
                            .await?;
                        match rows.next().await? {
                            Some(row) => row.opt_text(0)?,
                            None => None,
                        }
                    };

                    let uri_segment = if job_id.is_some() {
                        let mut count_rows = conn
                            .query(
                                "SELECT COUNT(*) FROM permission_requests pr \
                                 JOIN runs r ON pr.run_id = r.id \
                                 WHERE r.job_id = (SELECT job_id FROM runs WHERE id = ?1)",
                                params![run_id.as_str()],
                            )
                            .await?;
                        let ordinal = count_rows
                            .next()
                            .await?
                            .and_then(|row| row.i64(0).ok())
                            .unwrap_or(0)
                            + 1;
                        Some(format!("perm-{}", ordinal))
                    } else {
                        None
                    };

                    conn.execute(
                        "INSERT INTO permission_requests(
                             id, run_id, job_id, tool_use_id, tool_name, tool_input,
                             status, created_at, turn_id, uri_segment
                         )
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7, ?8, ?9)",
                        params![
                            request_id.as_str(),
                            run_id.as_str(),
                            job_id.as_deref(),
                            tool_use_id.as_str(),
                            tool_name.as_str(),
                            tool_input_json.as_str(),
                            now,
                            current_turn_id.as_deref(),
                            uri_segment.as_deref()
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .map_err(|e| format!("Failed to store Codex approval request: {}", e))
        })?;
    }

    let _ = orch.services.emitter.emit(
        "permission-request",
        serde_json::json!({
            "requestId": request_id,
            "runId": run_id,
            "toolUseId": tool_use_id,
            "toolName": tool_name,
            "input": tool_input,
        }),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "permission_requests", "action": "insert"}),
    );

    let mut rx = orch.permission_responses.subscribe();

    loop {
        match rx.try_recv() {
            Ok((resp_request_id, response_json)) => {
                if resp_request_id != request_id {
                    continue;
                }

                let mut parsed: Value = serde_json::from_str(&response_json)
                    .map_err(|e| format!("Invalid permission response payload: {}", e))?;
                if !allow_accept_for_session
                    && parsed.get("decision").and_then(|v| v.as_str()) == Some("acceptForSession")
                {
                    parsed["decision"] = serde_json::json!("accept");
                }
                return Ok(parsed);
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                return Err("Permission response channel closed".to_string());
            }
        }
    }
}
