//! Session management (configuration resolution, agent dispatch).
//!
//! This module handles resolving session configuration (tools, model, prompt,
//! MCP config) from agent configs and database state, then delegates to an
//! `AgentBackend` for process spawning and event streaming.
//!
//! All functions take `&Orchestrator` instead of framework-specific handles.

use crate::agent_process::stream::{ClaudeEvent, TranscriptEvent};
use crate::backends::{self, SessionConfig, SessionStart};
use crate::config::presets::{load_effective_presets, resolve_runtime_selection, PresetsConfig};
use crate::diesel_models::*;
use crate::models::Model;
use crate::node_segments::visible_node_segment;
use crate::schema::*;
use diesel::prelude::*;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;

use super::Orchestrator;

/// Insert a synthetic system:error event for display in the transcript
pub fn insert_error_event(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    error_message: &str,
) {
    let Ok(mut conn) = orch.db.conn.lock() else {
        return;
    };

    // Get next sequence number
    let sequence: i32 = events::table
        .filter(events::run_id.eq(run_id))
        .select(diesel::dsl::max(events::sequence))
        .first::<Option<i32>>(&mut *conn)
        .unwrap_or(None)
        .unwrap_or(-1)
        + 1;

    let now = chrono::Utc::now().timestamp() as i32;
    let event_id = Uuid::new_v4().to_string();

    let transcript_event = TranscriptEvent {
        event_type: "system:error".to_string(),
        session_id: session_id.map(|s| s.to_string()),
        parent_tool_use_id: None,
        content: Some(error_message.to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: true,
        usage: None,
        raw: Some(serde_json::json!({"error": error_message})),
    };

    let data = serde_json::to_string(&transcript_event).unwrap_or_default();

    let new_event = NewEvent {
        id: &event_id,
        run_id,
        session_id,
        sequence,
        timestamp: now,
        event_type: "system:error",
        data: &data,
        parent_tool_use_id: None,
        created_at: now,
        input_tokens: None,
        cache_read_tokens: None,
        cache_create_tokens: None,
        output_tokens: None,
        turn_id: None,
    };

    let _ = diesel::insert_into(events::table)
        .values(&new_event)
        .execute(&mut *conn);

    // Sync event to cloud
    orch.sync(crate::sync::SyncMessage::Event(crate::sync::SyncEvent {
        id: event_id.clone(),
        run_id: run_id.to_string(),
        session_id: session_id.map(|s| s.to_string()),
        sequence: Some(sequence),
        event_type: "system:error".to_string(),
        data: Some(data.clone()),
        input_tokens: None,
        output_tokens: None,
        cache_read_tokens: None,
        created_at: Some(now as i64),
        turn_id: None,
    }));

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "events", "action": "insert"}),
    );
}

/// Find the claude binary path, caching the result
pub fn get_claude_path(
    state: &crate::agent_process::process::AgentProcessState,
) -> Result<String, String> {
    log::debug!("get_claude_path: starting");

    // Check cache first
    {
        let cached = state.cli_binary_path.lock().map_err(|e| e.to_string())?;
        if let Some(path) = cached.as_ref() {
            log::debug!("get_claude_path: using cached path: {}", path);
            return Ok(path.clone());
        }
    }

    log::debug!("get_claude_path: no cache, resolving...");
    let path = crate::env::find_binary("claude").map_err(|e| {
        log::debug!("get_claude_path: {}", e);
        e
    })?;

    log::debug!("get_claude_path: found claude at: {}", path);

    // Cache and return
    {
        let mut cached = state.cli_binary_path.lock().map_err(|e| e.to_string())?;
        *cached = Some(path.clone());
    }

    log::info!("Resolved claude path: {}", path);
    Ok(path)
}

/// Extract session ID from a ClaudeEvent.
#[allow(dead_code)]
pub fn extract_session_id(event: &ClaudeEvent) -> Option<String> {
    match event {
        ClaudeEvent::System { session_id, .. } => Some(session_id.clone()),
        ClaudeEvent::User { session_id, .. } => Some(session_id.clone()),
        ClaudeEvent::Assistant { session_id, .. } => Some(session_id.clone()),
        ClaudeEvent::Result { session_id, .. } => Some(session_id.clone()),
        ClaudeEvent::StreamEvent { session_id, .. } => Some(session_id.clone()),
        ClaudeEvent::ControlResponse { .. } => None,
    }
}

/// Get the tmp directory for system prompt files
fn get_prompt_tmp_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".cairn")
        .join("tmp")
}

/// Write combined system prompt (bundled + agent) to a temp file
/// Returns the path to the temp file
pub fn write_system_prompt_file(
    run_id: &str,
    agent_prompt: Option<&str>,
) -> Result<PathBuf, String> {
    let tmp_dir = get_prompt_tmp_dir();
    fs::create_dir_all(&tmp_dir).map_err(|e| format!("Failed to create tmp dir: {}", e))?;

    let file_path = tmp_dir.join(format!("prompt-{}.md", run_id));

    // Combine bundled system prompt with agent-specific content
    let content = if let Some(agent_content) = agent_prompt {
        format!(
            "{}\n\n{}",
            crate::system_prompt::CAIRN_SYSTEM_PROMPT,
            agent_content
        )
    } else {
        crate::system_prompt::CAIRN_SYSTEM_PROMPT.to_string()
    };

    fs::write(&file_path, &content)
        .map_err(|e| format!("Failed to write system prompt file: {}", e))?;

    log::debug!(
        "Wrote system prompt to {:?} ({} bytes)",
        file_path,
        content.len()
    );

    Ok(file_path)
}

/// Clean up a specific prompt file after a run completes
pub fn cleanup_prompt_file(run_id: &str) {
    let file_path = get_prompt_tmp_dir().join(format!("prompt-{}.md", run_id));
    if file_path.exists() {
        if let Err(e) = fs::remove_file(&file_path) {
            log::warn!("Failed to remove prompt file {:?}: {}", file_path, e);
        } else {
            log::debug!("Cleaned up prompt file {:?}", file_path);
        }
    }
}

/// Clean up stale prompt files (older than 24 hours)
/// Called on startup to remove orphaned files from crashed runs
pub fn cleanup_stale_prompt_files() {
    let tmp_dir = get_prompt_tmp_dir();
    if !tmp_dir.exists() {
        return;
    }

    let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(24 * 60 * 60);

    let entries = match fs::read_dir(&tmp_dir) {
        Ok(entries) => entries,
        Err(e) => {
            log::warn!("Failed to read tmp dir {:?}: {}", tmp_dir, e);
            return;
        }
    };

    let mut cleaned = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("prompt-") && n.ends_with(".md"))
        {
            if let Ok(metadata) = entry.metadata() {
                if let Ok(modified) = metadata.modified() {
                    if modified < cutoff {
                        if let Err(e) = fs::remove_file(&path) {
                            log::warn!("Failed to remove stale prompt file {:?}: {}", path, e);
                        } else {
                            cleaned += 1;
                        }
                    }
                }
            }
        }
    }

    if cleaned > 0 {
        log::info!("Cleaned up {} stale prompt files", cleaned);
    }
}

/// Start an agent session (Claude or Codex).
///
/// Session ID handling:
/// - `session_id`: Cairn's internal session ID (always set). Used for event storage.
/// - `backend_id`: Backend conversation ID for resume. Claude session ID or Codex thread ID.
///   None on first run; set from Session.backend_id on subsequent runs.
///
/// Callers are responsible for creating run/job/chat/event records with the correct
/// session_id BEFORE calling this function.
#[allow(clippy::too_many_arguments)]
pub fn start_agent_session(
    orch: &Orchestrator,
    run_id: &str,
    prompt: &str,
    working_dir: &str,
    session_start: SessionStart,
    model: Option<Model>,
    _initial_user_message: Option<&str>,
    agent_config: Option<&crate::models::AgentConfig>,
    output_schema: Option<&crate::models::OutputSchemaInfo>,
    _is_job_level: bool,
    execution_id: Option<&str>,
    identity_override: Option<crate::identity::UserIdentity>,
) -> Result<(), String> {
    log::debug!("start_agent_session: entered");
    let start_time = std::time::Instant::now();
    log::info!("[PROFILE] start_agent_session begin");

    // Resolve output schema from provided info (if any)
    let (schema_temp_path, resolved_tool_name, resolved_tool_description) = {
        let resolved_info = output_schema.cloned();

        let tool_name = resolved_info
            .as_ref()
            .and_then(|info| info.tool_name.clone());
        let tool_description = resolved_info
            .as_ref()
            .and_then(|info| info.description.clone());

        let temp_path = if let Some(ref info) = resolved_info {
            log::debug!("start_agent_session: resolving output schema");
            let schema_value = crate::output_schemas::resolve_output_schema(
                orch.schema_dir.as_deref(),
                &info.schema,
            )
            .map_err(|e| format!("Failed to resolve output schema: {}", e))?;
            let temp_path = crate::output_schemas::write_schema_to_temp_file(&schema_value)
                .map_err(|e| format!("Failed to write schema to temp file: {}", e))?;
            log::debug!("start_agent_session: schema written to {:?}", temp_path);
            Some(temp_path)
        } else {
            None
        };

        (temp_path, tool_name, tool_description)
    };

    // Ensure MCP config file exists and get its path
    log::debug!("start_agent_session: ensuring MCP config");
    let schema_path_str = schema_temp_path
        .as_ref()
        .map(|p| p.to_string_lossy().to_string());

    // Serialize available agents, skills, and tools for MCP config
    let (agents_json, skills_json, tools_json, _session_project_path, session_project_id) = {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;

        // Get project_id from run
        let project_id: Option<String> = {
            let diesel_conn = &mut *conn;
            let run: crate::diesel_models::DbRun = runs::table
                .find(run_id)
                .first(diesel_conn)
                .map_err(|e| format!("Failed to get run: {}", e))?;

            if let Some(pid) = run.project_id {
                Some(pid)
            } else if let Some(iid) = run.issue_id {
                issues::table
                    .find(&iid)
                    .select(issues::project_id)
                    .first::<String>(diesel_conn)
                    .ok()
            } else {
                None
            }
        };

        // Get project path for file-based config lookup
        let project_path: Option<std::path::PathBuf> = project_id.as_ref().and_then(|pid| {
            projects::table
                .find(pid)
                .select(projects::repo_path)
                .first::<String>(&mut *conn)
                .ok()
                .map(std::path::PathBuf::from)
        });

        // Get available agents from files
        let agents = {
            use crate::config::{agents as config_agents, ConfigResult};

            let file_agents = config_agents::list_agents(&orch.config_dir, project_path.as_deref())
                .unwrap_or_default();

            let mut agent_infos: Vec<serde_json::Value> = file_agents
                .into_iter()
                .filter_map(|r| match r {
                    ConfigResult::Ok(agent) => Some(
                        serde_json::json!({"name": agent.name, "description": agent.description}),
                    ),
                    ConfigResult::Err { .. } => None,
                })
                .collect();

            agent_infos.sort_by(|a, b| {
                let a_name = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let b_name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                a_name.cmp(b_name)
            });

            if !agent_infos.is_empty() {
                serde_json::to_string(&agent_infos).ok()
            } else {
                None
            }
        };

        // Get available skills from files
        let skills = {
            use crate::config::{skills as config_skills, ConfigResult};

            let file_skills = config_skills::list_skills(&orch.config_dir, project_path.as_deref())
                .unwrap_or_default();

            let mut skill_infos: Vec<serde_json::Value> = file_skills
                .into_iter()
                .filter_map(|r| match r {
                    ConfigResult::Ok(skill) => Some(serde_json::json!({
                        "id": skill.id,
                        "name": skill.name,
                        "description": skill.description
                    })),
                    ConfigResult::Err { .. } => None,
                })
                .collect();

            skill_infos.sort_by(|a, b| {
                let a_name = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let b_name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                a_name.cmp(b_name)
            });

            if !skill_infos.is_empty() {
                serde_json::to_string(&skill_infos).ok()
            } else {
                None
            }
        };

        // Get available custom tools from files
        let tools = {
            use crate::config::{tools as config_tools, ConfigResult};

            let file_tools = config_tools::list_tools(&orch.config_dir, project_path.as_deref())
                .unwrap_or_default();

            let mut tool_infos: Vec<serde_json::Value> = file_tools
                .into_iter()
                .filter_map(|r| match r {
                    ConfigResult::Ok(tool) => Some(serde_json::json!({
                        "id": tool.id,
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.input_schema
                    })),
                    ConfigResult::Err { .. } => None,
                })
                .collect();

            tool_infos.sort_by(|a, b| {
                let a_name = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let b_name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                a_name.cmp(b_name)
            });

            if !tool_infos.is_empty() {
                serde_json::to_string(&tool_infos).ok()
            } else {
                None
            }
        };

        (agents, skills, tools, project_path, project_id)
    };

    let mcp_config_path = match crate::config::mcp_setup::ensure_mcp_config(
        &orch.config_dir,
        &orch.mcp_binary_path,
        orch.mcp_callback_port,
        schema_path_str.as_deref(),
        resolved_tool_name.as_deref(),
        resolved_tool_description.as_deref(),
        agents_json.as_deref(),
        skills_json.as_deref(),
        tools_json.as_deref(),
    ) {
        Ok(path) => {
            log::debug!("start_agent_session: MCP config done, path={:?}", path);
            path
        }
        Err(e) => {
            log::debug!("start_agent_session: MCP config FAILED: {}", e);
            return Err(e);
        }
    };
    log::info!("[PROFILE] MCP config done: {:?}", start_time.elapsed());

    let workspace_settings = crate::config::settings::load_settings(&orch.config_dir);

    // Use provided agent config (agents are now always explicitly passed)
    let agent_config = agent_config.cloned();

    // Resolve tools, model, prompt, permissions, and select backend.
    // All operations that need agent_config + DB access are grouped here.
    let (
        allowed_tools,
        disallowed_tools,
        effective_model,
        final_prompt,
        system_prompt_content,
        backend,
        permissions,
        max_thinking_tokens,
        reasoning_effort,
    ) = {
        use crate::config::{agents as config_agents, skills as config_skills, ConfigResult};
        use crate::diesel_models::DbRun;
        use crate::schema::{issues, jobs, runs};
        use diesel::prelude::*;

        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;

        // Get run context for URI (project_key, issue_number, node_name)
        // Also capture issue_id for messaging context injection
        let (current_location_uri, run_issue_id): (Option<String>, Option<String>) = {
            let diesel_conn = &mut *conn;
            let run: DbRun = runs::table
                .find(run_id)
                .first(diesel_conn)
                .map_err(|e| format!("Failed to get run: {}", e))?;

            let issue_id_clone = run.issue_id.clone();

            let project_key = run
                .project_id
                .as_ref()
                .and_then(|pid| {
                    projects::table
                        .find(pid)
                        .select(projects::key)
                        .first::<String>(diesel_conn)
                        .ok()
                })
                .or_else(|| {
                    run.issue_id.as_ref().and_then(|iid| {
                        issues::table
                            .inner_join(projects::table)
                            .filter(issues::id.eq(iid))
                            .select(projects::key)
                            .first::<String>(diesel_conn)
                            .ok()
                    })
                });

            let issue_number = run.issue_id.as_ref().and_then(|iid| {
                issues::table
                    .find(iid)
                    .select(issues::number)
                    .first::<i32>(diesel_conn)
                    .ok()
            });

            // Get node identifiers, execution_id, and parent context from job
            let (recipe_node_id, node_name, job_exec_id, parent_job_id) = run
                .job_id
                .as_ref()
                .and_then(|jid| {
                    jobs::table
                        .find(jid)
                        .select((
                            jobs::recipe_node_id,
                            jobs::node_name,
                            jobs::execution_id,
                            jobs::parent_job_id,
                        ))
                        .first::<(
                            Option<String>,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )>(diesel_conn)
                        .ok()
                })
                .unwrap_or((None, None, None, None));

            // Get exec_seq from the execution
            let exec_seq = job_exec_id.as_deref().and_then(|eid| {
                executions::table
                    .find(eid)
                    .select(executions::seq)
                    .first::<Option<i32>>(diesel_conn)
                    .ok()
                    .flatten()
            });

            // If this is a task (has parent_job_id), get parent's identifiers
            let (parent_recipe_node_id, parent_node_name) = parent_job_id
                .as_deref()
                .and_then(|pid| {
                    jobs::table
                        .find(pid)
                        .select((jobs::recipe_node_id, jobs::node_name))
                        .first::<(Option<String>, Option<String>)>(diesel_conn)
                        .ok()
                })
                .unwrap_or((None, None));

            let uri = project_key.map(|proj| {
                build_current_location_uri(
                    &proj,
                    issue_number,
                    exec_seq,
                    parent_recipe_node_id.as_deref(),
                    parent_node_name.as_deref(),
                    recipe_node_id.as_deref(),
                    node_name.as_deref(),
                )
            });

            (uri, issue_id_clone)
        };

        // Get project_id from run (either directly or via issue)
        let project_id = {
            let diesel_conn = &mut *conn;
            let run: DbRun = runs::table
                .find(run_id)
                .first(diesel_conn)
                .map_err(|e| format!("Failed to get run: {}", e))?;

            if let Some(pid) = run.project_id {
                Some(pid)
            } else if let Some(iid) = run.issue_id {
                issues::table
                    .find(&iid)
                    .select(issues::project_id)
                    .first::<String>(diesel_conn)
                    .ok()
            } else {
                None
            }
        };

        // Get project path for file-based config lookup
        let project_path_for_prompt: Option<std::path::PathBuf> =
            project_id.as_ref().and_then(|pid| {
                projects::table
                    .find(pid)
                    .select(projects::repo_path)
                    .first::<String>(&mut *conn)
                    .ok()
                    .map(std::path::PathBuf::from)
            });

        let (max_thinking_tokens, reasoning_effort) = {
            let presets = execution_id
                .and_then(|eid| {
                    executions::table
                        .find(eid)
                        .select(executions::snapshot)
                        .first::<Option<String>>(&mut *conn)
                        .ok()
                        .flatten()
                        .and_then(|json| {
                            serde_json::from_str::<crate::models::ExecutionSnapshot>(&json)
                                .ok()
                                .and_then(|snapshot| {
                                    snapshot.presets.as_ref().map(PresetsConfig::from)
                                })
                        })
                })
                .unwrap_or_else(|| {
                    load_effective_presets(&orch.config_dir, project_path_for_prompt.as_deref())
                });
            let authored_tier = agent_config
                .as_ref()
                .and_then(|ac| ac.tier.as_ref())
                .or(model.as_ref())
                .map(Model::as_str);
            let authored_backend = agent_config
                .as_ref()
                .and_then(|ac| ac.backend_preference.as_deref());
            let extras = resolve_runtime_selection(authored_tier, authored_backend, &presets)
                .map(|(_, _, extras)| extras)
                .unwrap_or_default();
            (
                extras
                    .max_thinking_tokens
                    .or(workspace_settings.max_thinking_tokens),
                extras.reasoning_effort.clone(),
            )
        };

        // Get list of available agents from files
        let available_agents: Vec<(String, String, String)> = {
            let agents =
                config_agents::list_agents(&orch.config_dir, project_path_for_prompt.as_deref())
                    .unwrap_or_default();
            let mut result: Vec<(String, String, String)> = agents
                .into_iter()
                .filter_map(|r| match r {
                    ConfigResult::Ok(agent) => Some((agent.id, agent.name, agent.description)),
                    ConfigResult::Err { .. } => None,
                })
                .collect();
            result.sort_by(|a, b| a.1.cmp(&b.1));
            result
        };

        // Get list of available skills from files
        let available_skills_for_prompt: Vec<(String, String, String)> = {
            let skills =
                config_skills::list_skills(&orch.config_dir, project_path_for_prompt.as_deref())
                    .unwrap_or_default();
            let mut result: Vec<(String, String, String)> = skills
                .into_iter()
                .filter_map(|r| match r {
                    ConfigResult::Ok(skill) => Some((skill.id, skill.name, skill.description)),
                    ConfigResult::Err { .. } => None,
                })
                .collect();
            result.sort_by(|a, b| a.1.cmp(&b.1));
            result
        };

        // ================================================================
        // Backend selection (moved before tool resolution so the backend
        // can control which tools are allowed/disallowed).
        // ================================================================

        let agent_backend_name = agent_config
            .as_ref()
            .and_then(|ac| ac.backend_preference.clone());

        // Runtime model should already be resolved before session start.
        let resolved_model = model.clone();

        let effective_backend_name = agent_backend_name.clone().or_else(|| {
            resolved_model
                .as_ref()
                .and_then(|m| backends::backend_for_model(m.as_str()))
                .map(|s| s.to_string())
        });

        let backend = backends::backend_for_name(effective_backend_name.as_deref());

        // Build canonical permissions from agent config fields
        let permissions = {
            let (ap, fs) = agent_config
                .as_ref()
                .map(|ac| (ac.approval_policy, ac.filesystem_scope))
                .unwrap_or_default();
            backends::AgentPermissions::new(ap.unwrap_or_default(), fs.unwrap_or_default())
        };

        // ================================================================
        // Tool resolution via backend adapter
        // ================================================================

        let agent_tools: Vec<String> = agent_config
            .as_ref()
            .map(|a| a.tools.clone())
            .unwrap_or_default();

        let agent_disallowed: Vec<String> = agent_config
            .as_ref()
            .and_then(|a| a.disallowed_tools.clone())
            .unwrap_or_default();

        let resolved = backend.resolve_tools(&agent_tools, &agent_disallowed);
        let mut allowed = resolved.allowed;
        let disallowed = resolved.disallowed;

        // Strip file mutation tools when filesystem scope is ReadOnly
        if permissions.filesystem == crate::models::FilesystemScope::ReadOnly {
            allowed.retain(|t| {
                !matches!(
                    t.as_str(),
                    "mcp__cairn__write" | "mcp__cairn__edit" | "mcp__cairn__filechange"
                )
            });
        }

        // Add custom submission tool name if an output schema defines one
        let submission_tool = resolved_tool_name
            .as_ref()
            .map(|n| format!("mcp__cairn__{}", n))
            .unwrap_or_else(|| "mcp__cairn__return".to_string());

        if !allowed.contains(&submission_tool) {
            allowed.push(submission_tool.clone());
        }

        // Build system prompt content from agent prompt + context
        let system_prompt_content = {
            let mut content = agent_config
                .as_ref()
                .map(|a| a.prompt.clone())
                .unwrap_or_default();

            // Append available agents list if task tool is available
            if allowed.contains(&"mcp__cairn__task".to_string()) && !available_agents.is_empty() {
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str("## Available Agents\n\n");
                content.push_str("You can spawn these agents using the task tool:\n\n");
                for (_id, name, description) in &available_agents {
                    content.push_str(&format!("- **{}**: {}\n", name, description));
                }
            }

            // Append available skills list if skill tool is available
            if allowed.contains(&"mcp__cairn__skill".to_string())
                && !available_skills_for_prompt.is_empty()
            {
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str("## Available Skills\n\n");
                content.push_str("You can retrieve skill instructions using the skill tool:\n\n");
                for (id, name, description) in &available_skills_for_prompt {
                    content.push_str(&format!("- **{}** (`{}`): {}\n", name, id, description));
                }
            }

            // Inject project resources section
            if let Some(ref project_path) = project_path_for_prompt {
                let proj_config =
                    crate::config::project_settings::load_project_settings(project_path);
                if let Some(ref resources) = proj_config.resources {
                    if !resources.is_empty() {
                        let resources_section =
                            crate::resources::build_resources_prompt(&orch.config_dir, resources);
                        if !resources_section.is_empty() {
                            if !content.is_empty() {
                                content.push_str("\n\n");
                            }
                            content.push_str(&resources_section);
                        }
                    }
                }
            }

            // Inject messaging context (peers + recent history)
            if let Some(ref issue_id) = run_issue_id {
                // Look up project key for channel queries
                let msg_project_key: Option<String> = project_id.as_ref().and_then(|pid| {
                    projects::table
                        .find(pid)
                        .select(projects::key)
                        .first::<String>(&mut *conn)
                        .ok()
                });
                let messaging_section = crate::messages::prompt::build_messaging_context(
                    &mut conn,
                    msg_project_key.as_deref().unwrap_or(""),
                    issue_id,
                    run_id,
                );
                if !messaging_section.is_empty() {
                    if !content.is_empty() {
                        content.push_str("\n\n");
                    }
                    content.push_str(&messaging_section);
                }
            }

            // Add current location URI if available
            if let Some(ref uri) = current_location_uri {
                if !content.is_empty() {
                    content.push_str("\n\n");
                }
                content.push_str(&format!("Current Location: `{}`", uri));
            }

            if content.is_empty() {
                None
            } else {
                // Wrap in <agent_role> tags to distinguish from MCP instructions
                Some(format!("<agent_role>\n{}\n</agent_role>", content))
            }
        };

        // Base prompt stays as user message
        let resolved_prompt = prompt.to_string();

        (
            allowed,
            disallowed,
            resolved_model,
            resolved_prompt,
            system_prompt_content,
            backend,
            permissions,
            max_thinking_tokens,
            reasoning_effort,
        )
    };

    // Resolve identity: explicit override (server) > ambient identity store (desktop)
    let resolved_identity = identity_override.or_else(|| {
        let project_overrides = session_project_id.as_ref().and_then(|pid| {
            orch.get_identity_store()
                .and_then(|store| store.project_overrides.get(pid).cloned())
        });
        orch.resolve_identity_for_project(project_overrides.as_ref())
    });

    let session_config = SessionConfig {
        run_id: run_id.to_string(),
        working_dir: working_dir.to_string(),
        prompt: final_prompt,
        system_prompt_content,
        model: effective_model,
        session_start,
        allowed_tools,
        disallowed_tools,
        mcp_config_path,
        max_thinking_tokens,
        reasoning_effort,
        permissions,
        bidirectional: true,
        identity: resolved_identity,
    };

    backend.start_session(session_config, orch)
}

/// Build the `current_location_uri` for an agent session.
///
/// Produces URIs matching the cairn:// scheme:
/// - Job node: `cairn://PROJECT/NUMBER/EXEC/NODE`
/// - Issue fallback: `cairn://PROJECT/NUMBER`
/// - Project fallback: `cairn://PROJECT`
pub(crate) fn build_current_location_uri(
    project_key: &str,
    issue_number: Option<i32>,
    exec_seq: Option<i32>,
    parent_recipe_node_id: Option<&str>,
    parent_node_name: Option<&str>,
    recipe_node_id: Option<&str>,
    node_name: Option<&str>,
) -> String {
    let _ = (parent_recipe_node_id, parent_node_name);
    match (issue_number, exec_seq) {
        (Some(num), Some(seq)) => {
            if let Some(node_segment) = visible_node_segment(recipe_node_id, node_name) {
                return format!("cairn://{}/{}/{}/{}", project_key, num, seq, node_segment);
            }

            format!("cairn://{}/{}", project_key, num)
        }
        // Fallbacks
        (Some(num), _) => format!("cairn://{}/{}", project_key, num),
        _ => format!("cairn://{}", project_key),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::stream::{MessageContent, MessageContentInner};
    use crate::services::SpawnConfig;

    fn make_system_event(session_id: &str) -> ClaudeEvent {
        ClaudeEvent::System {
            subtype: "init".to_string(),
            session_id: session_id.to_string(),
            data: serde_json::json!({}),
        }
    }

    fn make_user_event(session_id: &str) -> ClaudeEvent {
        ClaudeEvent::User {
            uuid: "user-uuid".to_string(),
            session_id: session_id.to_string(),
            message: MessageContent {
                role: "user".to_string(),
                content: MessageContentInner::Text("hello".to_string()),
            },
            parent_tool_use_id: None,
        }
    }

    fn make_assistant_event(session_id: &str) -> ClaudeEvent {
        ClaudeEvent::Assistant {
            uuid: "asst-uuid".to_string(),
            session_id: session_id.to_string(),
            message: MessageContent {
                role: "assistant".to_string(),
                content: MessageContentInner::Text("hi".to_string()),
            },
            parent_tool_use_id: None,
        }
    }

    fn make_result_event(session_id: &str) -> ClaudeEvent {
        ClaudeEvent::Result {
            subtype: "success".to_string(),
            session_id: session_id.to_string(),
            is_error: false,
            duration_ms: Some(1000),
            num_turns: Some(1),
            total_cost_usd: Some(0.01),
            result: None,
            usage: None,
            data: serde_json::json!({}),
        }
    }

    #[test]
    fn extract_session_id_from_system_event() {
        let event = make_system_event("session-123");
        let result = extract_session_id(&event);
        assert_eq!(result, Some("session-123".to_string()));
    }

    #[test]
    fn extract_session_id_from_user_event() {
        let event = make_user_event("session-456");
        let result = extract_session_id(&event);
        assert_eq!(result, Some("session-456".to_string()));
    }

    #[test]
    fn extract_session_id_from_assistant_event() {
        let event = make_assistant_event("session-789");
        let result = extract_session_id(&event);
        assert_eq!(result, Some("session-789".to_string()));
    }

    #[test]
    fn extract_session_id_from_result_event() {
        let event = make_result_event("session-abc");
        let result = extract_session_id(&event);
        assert_eq!(result, Some("session-abc".to_string()));
    }

    #[test]
    fn test_spawn_config_from_claude_args() {
        use crate::agent_process::args::{build_claude_args, ClaudeArgsConfig};
        use crate::backends::SessionStart;
        use crate::models::Model;

        let args_config = ClaudeArgsConfig {
            mcp_config_path: "/path/to/mcp.json".to_string(),
            skip_permissions: false,
            permission_prompt_tool: None,
            model: Some(Model::new(Model::OPUS)),
            session_start: SessionStart::New {
                session_id: "session-1".to_string(),
            },
            prompt: "Test prompt".to_string(),
            max_thinking_tokens: Some(31999),
            allowed_tools: vec!["Read".to_string()],
            disallowed_tools: vec![],
            append_system_prompt_file: None,
            settings_path: None,
            bidirectional: true,
        };

        let args = build_claude_args(&args_config);

        let spawn_config = SpawnConfig::new("claude")
            .args(&args)
            .cwd("/some/path")
            .stdin(true);

        assert_eq!(spawn_config.program, "claude");
        assert_eq!(spawn_config.cwd, Some("/some/path".to_string()));
        assert!(spawn_config.args.contains(&"--model".to_string()));
        assert!(spawn_config.args.contains(&"opus[1m]".to_string()));
        assert!(spawn_config.args.contains(&"--input-format".to_string()));
        assert!(spawn_config.capture_stdin);
    }

    // =========================================================================
    // build_current_location_uri
    // =========================================================================

    #[test]
    fn uri_for_task_agent() {
        let uri = build_current_location_uri(
            "CAIRN",
            Some(831),
            Some(1),
            None,
            Some("Builder"),
            None,
            Some("Explore"),
        );
        assert_eq!(uri, "cairn://CAIRN/831/1/explore");
    }

    #[test]
    fn uri_for_task_agent_with_suffix() {
        let uri = build_current_location_uri(
            "CAIRN",
            Some(831),
            Some(1),
            None,
            Some("Builder"),
            None,
            Some("Explore-2"),
        );
        assert_eq!(uri, "cairn://CAIRN/831/1/explore-2");
    }

    #[test]
    fn uri_for_recipe_node() {
        let uri = build_current_location_uri(
            "CAIRN",
            Some(831),
            Some(1),
            None,
            None,
            None,
            Some("builder-1"),
        );
        assert_eq!(uri, "cairn://CAIRN/831/1/builder-1");
    }

    #[test]
    fn uri_prefers_slugified_node_name_over_recipe_node_id() {
        let uri = build_current_location_uri(
            "CAIRN",
            Some(831),
            Some(1),
            None,
            None,
            Some("54e54f2d-4ff1-45c5-ad0e-5e5f5846ea67"),
            Some("Builder"),
        );
        assert_eq!(uri, "cairn://CAIRN/831/1/builder");
    }

    #[test]
    fn uri_falls_back_to_recipe_node_id_when_name_missing() {
        let uri = build_current_location_uri(
            "CAIRN",
            Some(831),
            Some(1),
            None,
            None,
            Some("54e54f2d-4ff1-45c5-ad0e-5e5f5846ea67"),
            None,
        );
        assert_eq!(
            uri,
            "cairn://CAIRN/831/1/54e54f2d-4ff1-45c5-ad0e-5e5f5846ea67"
        );
    }

    #[test]
    fn uri_fallback_issue_only() {
        // No exec_seq — can't build full path
        let uri =
            build_current_location_uri("CAIRN", Some(831), None, None, None, None, Some("Builder"));
        assert_eq!(uri, "cairn://CAIRN/831");
    }

    #[test]
    fn uri_fallback_issue_no_node() {
        let uri = build_current_location_uri("CAIRN", Some(831), Some(1), None, None, None, None);
        assert_eq!(uri, "cairn://CAIRN/831");
    }

    #[test]
    fn uri_fallback_project_only() {
        let uri = build_current_location_uri("CAIRN", None, None, None, None, None, None);
        assert_eq!(uri, "cairn://CAIRN");
    }

    #[test]
    fn uri_task_requires_all_four_components() {
        // Has parent but no node_name — falls back to issue
        let uri = build_current_location_uri(
            "CAIRN",
            Some(831),
            Some(1),
            None,
            Some("Builder"),
            None,
            None,
        );
        assert_eq!(uri, "cairn://CAIRN/831");
    }

    // =========================================================================
    // insert_error_event
    // =========================================================================

    use crate::db::DbState;
    use crate::diesel_models::{DbEvent, NewRun};
    use crate::orchestrator::Orchestrator;
    use crate::schema::events;
    use crate::services::testing::TestServicesBuilder;
    use crate::test_utils::test_diesel_conn;
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

    fn create_test_run(
        conn: &mut diesel::sqlite::SqliteConnection,
        run_id: &str,
        job_id: Option<&str>,
    ) {
        let now = chrono::Utc::now().timestamp() as i32;
        let new_run = NewRun {
            id: run_id,
            issue_id: None,
            project_id: None,
            job_id,
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
            .expect("Failed to create test run");
    }

    #[test]
    fn insert_error_event_creates_system_error_event() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);

        // Create a run first (events have FK to runs)
        {
            let mut conn = orch.db.conn.lock().unwrap();
            create_test_run(&mut conn, "run-1", None);
        }

        insert_error_event(&orch, "run-1", None, "Something went wrong");

        let mut conn = orch.db.conn.lock().unwrap();
        let events: Vec<DbEvent> = events::table
            .filter(events::run_id.eq("run-1"))
            .load(&mut *conn)
            .unwrap();

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.event_type, "system:error");
        assert_eq!(event.run_id, "run-1");
        assert_eq!(event.session_id, None);
        assert_eq!(event.sequence, 0);

        // Verify the data payload contains the error message (camelCase due to serde rename)
        let data: serde_json::Value = serde_json::from_str(&event.data).unwrap();
        assert_eq!(data["content"], "Something went wrong");
        assert_eq!(data["isError"], true);
        assert_eq!(data["eventType"], "system:error");
    }

    #[test]
    fn insert_error_event_includes_session_id() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);

        {
            let mut conn = orch.db.conn.lock().unwrap();
            create_test_run(&mut conn, "run-2", None);
        }

        insert_error_event(&orch, "run-2", Some("session-abc"), "Config error");

        let mut conn = orch.db.conn.lock().unwrap();
        let event: DbEvent = events::table
            .filter(events::run_id.eq("run-2"))
            .first(&mut *conn)
            .unwrap();

        assert_eq!(event.session_id, Some("session-abc".to_string()));

        let data: serde_json::Value = serde_json::from_str(&event.data).unwrap();
        assert_eq!(data["sessionId"], "session-abc");
    }

    #[test]
    fn insert_error_event_increments_sequence() {
        let conn = test_diesel_conn();
        let orch = test_orchestrator(conn);

        {
            let mut conn = orch.db.conn.lock().unwrap();
            create_test_run(&mut conn, "run-3", None);
        }

        // Insert two error events — second should get sequence 1
        insert_error_event(&orch, "run-3", None, "First error");
        insert_error_event(&orch, "run-3", None, "Second error");

        let mut conn = orch.db.conn.lock().unwrap();
        let events: Vec<DbEvent> = events::table
            .filter(events::run_id.eq("run-3"))
            .order(events::sequence.asc())
            .load(&mut *conn)
            .unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].sequence, 1);
    }
}
