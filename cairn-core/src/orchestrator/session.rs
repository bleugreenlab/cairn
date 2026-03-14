//! Claude session management (spawning, event streaming, process lifecycle).
//!
//! This module handles starting Claude sessions, resolving configuration,
//! managing event streams, and storing session state.
//!
//! All functions take `&Orchestrator` instead of framework-specific handles.

use crate::claude::args::{build_claude_args, ClaudeArgsConfig};
use crate::claude::stream::{
    parse_event, ClaudeEvent, DeltaContent, StreamEventInner, TranscriptEvent,
};
use crate::claude::turn_boundary::TurnBoundaryChecker;
use crate::debug::debug_log;
use crate::diesel_models::*;
use crate::models::{Model, RunStatus};
use crate::schema::*;
use crate::services::SpawnConfig;
use diesel::prelude::*;
use std::fs;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use uuid::Uuid;

use super::Orchestrator;

/// Insert a synthetic system:error event for display in the transcript
fn insert_error_event(
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
    };

    let _ = diesel::insert_into(events::table)
        .values(&new_event)
        .execute(&mut *conn);

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "events", "action": "insert"}),
    );
}

/// Finalize an `assistant:streaming` placeholder to `assistant` using accumulated content/thinking.
/// Acquires its own DB lock — only call when no lock is held.
fn finalize_streaming_placeholder(
    orch: &Orchestrator,
    streaming_state: &mut Option<StreamingState>,
    session_id: Option<&str>,
) {
    let Some(state) = streaming_state.take() else {
        return;
    };
    let Ok(mut conn) = orch.db.conn.lock() else {
        return;
    };

    let final_event = TranscriptEvent {
        event_type: "assistant".to_string(),
        session_id: session_id.map(|s| s.to_string()),
        parent_tool_use_id: None,
        content: if state.content.is_empty() {
            None
        } else {
            Some(state.content)
        },
        thinking: if state.thinking.is_empty() {
            None
        } else {
            Some(state.thinking)
        },
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        usage: None,
        raw: None,
    };
    let data = serde_json::to_string(&final_event).unwrap_or_default();
    let _ = diesel::update(events::table.find(&state.event_id))
        .set((events::event_type.eq("assistant"), events::data.eq(data)))
        .execute(&mut *conn);
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "events", "action": "update"}),
    );
}

/// State for tracking a streaming message being accumulated
#[derive(Debug)]
struct StreamingState {
    /// Database event ID for the placeholder
    event_id: String,
    /// Accumulated text content
    content: String,
    /// Accumulated thinking content
    thinking: String,
}

impl StreamingState {
    fn new(event_id: String) -> Self {
        Self {
            event_id,
            content: String::new(),
            thinking: String::new(),
        }
    }
}

/// Find the claude binary path, caching the result
pub fn get_claude_path(
    state: &crate::claude::process::ClaudeProcessState,
) -> Result<String, String> {
    debug_log("get_claude_path: starting");

    // Check cache first
    {
        let cached = state.claude_path.lock().map_err(|e| e.to_string())?;
        if let Some(path) = cached.as_ref() {
            debug_log(&format!("get_claude_path: using cached path: {}", path));
            return Ok(path.clone());
        }
    }

    debug_log("get_claude_path: no cache, resolving...");
    let path = crate::env::find_binary("claude").map_err(|e| {
        debug_log(&format!("get_claude_path: {}", e));
        e
    })?;

    debug_log(&format!("get_claude_path: found claude at: {}", path));

    // Cache and return
    {
        let mut cached = state.claude_path.lock().map_err(|e| e.to_string())?;
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
        format!("{}\n\n{}", crate::claude::SYSTEM_PROMPT, agent_content)
    } else {
        crate::claude::SYSTEM_PROMPT.to_string()
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

/// Start a Claude CLI session with stream-json output
///
/// Session ID handling:
/// - For new sessions: caller provides `session_id` (pre-generated UUID)
/// - For resume sessions: caller provides `resume_session_id` (existing session to continue)
///
/// Callers are responsible for creating run/job/chat/event records with the correct
/// session_id BEFORE calling this function. This function only spawns Claude and
/// passes the session_id via CLI flags.
#[allow(clippy::too_many_arguments)]
pub fn start_claude_session(
    orch: &Orchestrator,
    run_id: &str,
    prompt: &str,
    working_dir: &str,
    session_id: Option<&str>,
    resume_session_id: Option<&str>,
    model: Option<Model>,
    initial_user_message: Option<&str>,
    agent_config: Option<&crate::models::AgentConfig>,
    output_schema: Option<&crate::models::OutputSchemaInfo>,
    _is_job_level: bool,
    execution_id: Option<&str>,
) -> Result<(), String> {
    debug_log("start_claude_session: entered");
    let start_time = std::time::Instant::now();
    log::info!("[PROFILE] start_claude_session begin");

    // Session ID for the reader thread (either new or resume)
    let effective_session_id = session_id.or(resume_session_id).map(|s| s.to_string());

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
            debug_log("start_claude_session: resolving output schema");
            let schema_value =
                crate::schemas::resolve_output_schema(orch.schema_dir.as_deref(), &info.schema)
                    .map_err(|e| format!("Failed to resolve output schema: {}", e))?;
            let temp_path = crate::schemas::write_schema_to_temp_file(&schema_value)
                .map_err(|e| format!("Failed to write schema to temp file: {}", e))?;
            debug_log(&format!(
                "start_claude_session: schema written to {:?}",
                temp_path
            ));
            Some(temp_path)
        } else {
            None
        };

        (temp_path, tool_name, tool_description)
    };

    // Ensure MCP config file exists and get its path
    debug_log("start_claude_session: ensuring MCP config");
    let schema_path_str = schema_temp_path
        .as_ref()
        .map(|p| p.to_string_lossy().to_string());

    // Serialize available agents, skills, and tools for MCP config
    let (agents_json, skills_json, tools_json) = {
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

        (agents, skills, tools)
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
            debug_log(&format!(
                "start_claude_session: MCP config done, path={:?}",
                path
            ));
            path
        }
        Err(e) => {
            debug_log(&format!("start_claude_session: MCP config FAILED: {}", e));
            return Err(e);
        }
    };
    log::info!("[PROFILE] MCP config done: {:?}", start_time.elapsed());

    // Get cached claude path (resolves once on first use)
    debug_log("start_claude_session: getting claude path");
    let claude_path = get_claude_path(&orch.process_state)?;
    debug_log(&format!(
        "start_claude_session: claude path = {}",
        claude_path
    ));
    log::info!("[PROFILE] Claude path resolved: {:?}", start_time.elapsed());

    // Get max_thinking_tokens from settings
    let max_thinking_tokens =
        crate::config::settings::load_settings(&orch.config_dir).max_thinking_tokens;

    // Use provided agent config (agents are now always explicitly passed)
    let agent_config = agent_config.cloned();

    // Resolve tools from agent config with provider preferences
    let (allowed_tools, disallowed_tools, effective_model, final_prompt, system_prompt_content) = {
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

            let node_name = run.job_id.as_ref().and_then(|jid| {
                jobs::table
                    .find(jid)
                    .select(jobs::node_name)
                    .first::<Option<String>>(diesel_conn)
                    .ok()
                    .flatten()
            });

            let uri = project_key.map(|proj| match (issue_number, node_name) {
                (Some(num), Some(node)) => format!("cairn://{}/{}/{}", proj, num, node),
                (Some(num), None) => format!("cairn://{}/{}", proj, num),
                _ => format!("cairn://{}", proj),
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

        // Get agent tools (empty if no agent config)
        let agent_tools: Vec<String> = agent_config
            .as_ref()
            .map(|a| a.tools.clone())
            .unwrap_or_default();

        // Resolve tools: map overlapping tools to Cairn equivalents
        let (mut allowed, force_disallowed) = crate::claude::toolkits::resolve_tools(&agent_tools);

        // Add submission tool (custom name or default "return")
        let submission_tool = resolved_tool_name
            .as_ref()
            .map(|n| format!("mcp__cairn__{}", n))
            .unwrap_or_else(|| "mcp__cairn__return".to_string());

        if !allowed.contains(&submission_tool) {
            allowed.push(submission_tool.clone());
        }

        // Auto-add skill tool when skills are available
        if !available_skills_for_prompt.is_empty()
            && !allowed.contains(&"mcp__cairn__skill".to_string())
        {
            allowed.push("mcp__cairn__skill".to_string());
        }

        // Build disallowed: everything not allowed
        let allowed_set: std::collections::HashSet<_> = allowed.iter().cloned().collect();
        let all_known = crate::claude::toolkits::get_all_known_tools();

        let mut disallowed: Vec<String> = all_known
            .into_iter()
            .filter(|t| !allowed_set.contains(t))
            .collect();

        // Add unchosen provider versions
        disallowed.extend(force_disallowed);

        // Always disallow planning mode tools (managed by Cairn)
        for tool in crate::models::ALWAYS_DISALLOWED_TOOLS {
            if !disallowed.contains(&tool.to_string()) {
                disallowed.push(tool.to_string());
            }
        }

        // Add agent-specific disallowed tools
        if let Some(ref agent) = agent_config {
            if let Some(ref agent_disallowed) = agent.disallowed_tools {
                for tool in agent_disallowed {
                    if !disallowed.contains(tool) {
                        disallowed.push(tool.clone());
                    }
                }
            }
        }

        // Remove any always-disallowed tools from allowed (in case agent config has them)
        allowed.retain(|t| !crate::models::ALWAYS_DISALLOWED_TOOLS.contains(&t.as_str()));

        disallowed.sort();
        disallowed.dedup();

        // Resolve model: job.model (passed as model param) > agent_config.model > workspace default
        let resolved_model = model
            .clone()
            .or_else(|| agent_config.as_ref().and_then(|a| a.model.clone()));

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

            // Inject skill content for agent-configured skills
            let mut injected_skill_ids: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            if let Some(ref agent) = agent_config {
                if let Some(ref skill_ids) = agent.skills {
                    // Load from snapshot if available, otherwise from files
                    if let Some(exec_id) = execution_id {
                        use crate::jobs::queries::load_execution_snapshot;

                        if let Ok(snapshot) = load_execution_snapshot(&mut conn, exec_id) {
                            for skill_id in skill_ids {
                                if let Some(skill) = snapshot.skills.get(skill_id) {
                                    if !content.is_empty() {
                                        content.push_str("\n\n");
                                    }
                                    content.push_str(&skill.prompt);
                                    injected_skill_ids.insert(skill_id.clone());
                                }
                            }
                        }
                    } else {
                        // Fallback to files (non-execution runs)
                        for skill_id in skill_ids {
                            if let Ok(Some(skill)) = config_skills::get_skill(
                                &orch.config_dir,
                                skill_id,
                                project_path_for_prompt.as_deref(),
                            ) {
                                if !content.is_empty() {
                                    content.push_str("\n\n");
                                }
                                content.push_str(&skill.prompt);
                                injected_skill_ids.insert(skill_id.clone());
                            }
                        }
                    }
                }
            }

            // Inject issue-level skills (skip if already injected via agent)
            if let Some(exec_id) = execution_id {
                use crate::jobs::queries::load_execution_snapshot;

                if let Ok(snapshot) = load_execution_snapshot(&mut conn, exec_id) {
                    for skill_id in &snapshot.trigger_context.issue_skills {
                        if injected_skill_ids.contains(skill_id) {
                            continue;
                        }
                        if let Some(skill) = snapshot.skills.get(skill_id) {
                            if !content.is_empty() {
                                content.push_str("\n\n");
                            }
                            content.push_str(&skill.prompt);
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
        )
    };

    // Build Claude arguments using the pure function
    let agent_permission_mode = agent_config.and_then(|ac| ac.permission_mode.clone());
    let (use_skip_permissions, permission_prompt_tool) = match agent_permission_mode.as_deref() {
        Some("bypassPermissions") => (true, None),
        _ => (false, Some("mcp__cairn__permission_prompt".to_string())),
    };

    // Write combined system prompt (bundled + agent) to temp file
    let prompt_file_path = write_system_prompt_file(run_id, system_prompt_content.as_deref())?;

    // Write hook settings file for memory surfacing (passed via --settings)
    let hook_settings_path =
        crate::memories::hooks::write_hook_settings_file(orch.mcp_callback_port).ok();

    let args_config = ClaudeArgsConfig {
        mcp_config_path: mcp_config_path.to_string_lossy().to_string(),
        skip_permissions: use_skip_permissions,
        permission_prompt_tool,
        model: effective_model,
        session_id: session_id.map(|s| s.to_string()),
        resume_session_id: resume_session_id.map(|s| s.to_string()),
        prompt: final_prompt.clone(),
        max_thinking_tokens,
        allowed_tools,
        disallowed_tools,
        append_system_prompt_file: Some(prompt_file_path),
        settings_path: hook_settings_path,
        bidirectional: true,
    };
    let claude_args = build_claude_args(&args_config);

    debug_log(&format!(
        "start_claude_session: command built, claude_path={}",
        claude_path
    ));
    debug_log(&format!("start_claude_session: args={:?}", claude_args));
    debug_log(&format!(
        "start_claude_session: working_dir={}",
        working_dir
    ));

    log::info!("[PROFILE] Command built: {:?}", start_time.elapsed());
    log::info!("Spawning claude: {} {:?}", claude_path, claude_args);

    // Get MCP authentication secret (shared secret for TOTP-style passcodes)
    let mcp_secret = orch
        .mcp_auth
        .get_secret_for_mcp()
        .map_err(|e| format!("Failed to get MCP auth secret: {}", e))?;
    log::info!("Using MCP auth secret for run {}", run_id);

    // Build spawn config and spawn using ProcessSpawner service
    let spawn_config = SpawnConfig::new(&claude_path)
        .args(&claude_args)
        .cwd(working_dir)
        .env("CAIRN_RUN_ID", run_id)
        .env("CAIRN_MCP_SECRET", &mcp_secret)
        .env("ENABLE_TOOL_SEARCH", "false")
        .stdin(true);

    // Check if we need to evict a warm process to make room
    orch.collect_warm_if_needed();

    debug_log("start_claude_session: about to spawn");
    let mut child = orch.services.process.spawn(spawn_config).map_err(|e| {
        debug_log(&format!("start_claude_session: spawn failed: {}", e));
        insert_error_event(
            orch,
            run_id,
            effective_session_id.as_deref(),
            &format!("Failed to start Claude: {}", e),
        );
        e
    })?;
    debug_log(&format!(
        "start_claude_session: spawned, pid={}",
        child.id()
    ));
    log::info!("[PROFILE] Process spawned: {:?}", start_time.elapsed());

    // Update run and node status to running AFTER successful spawn
    debug_log("start_claude_session: updating status to running");
    let now = chrono::Utc::now().timestamp() as i32;
    {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        diesel::update(runs::table.find(&run_id))
            .set((
                runs::status.eq("running"),
                runs::started_at.eq(Some(now)),
                runs::updated_at.eq(now),
            ))
            .execute(&mut *conn)
            .map_err(|e| e.to_string())?;

        if let Ok(Some(job_id)) = runs::table
            .find(&run_id)
            .select(runs::job_id)
            .first::<Option<String>>(&mut *conn)
        {
            let _ = diesel::update(jobs::table.find(&job_id))
                .set((jobs::status.eq("running"), jobs::updated_at.eq(now)))
                .execute(&mut *conn);
        }
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "runs", "action": "update"}),
    );
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );
    debug_log("start_claude_session: status updated to running");

    let stdout = child.take_stdout().ok_or("Failed to capture stdout")?;
    let stderr = child.take_stderr();
    let stdin = child.take_stdin();

    // Spawn thread to log stderr
    if let Some(stderr) = stderr {
        thread::spawn(move || {
            debug_log("stderr_thread: started");
            for line in stderr.lines().map_while(Result::ok) {
                debug_log(&format!("claude stderr: {}", line));
                log::error!("claude stderr: {}", line);
            }
            debug_log("stderr_thread: ended");
        });
    }

    // Store the process handle with stdin for bidirectional communication
    let child_arc = Arc::new(Mutex::new(Some(child)));
    let stdin_arc = Arc::new(Mutex::new(stdin));

    // Get job_id for warm process tracking
    let process_job_id: Option<String> = {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
        runs::table
            .find(&run_id)
            .select(runs::job_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten()
    };

    {
        let mut processes = orch
            .process_state
            .processes
            .lock()
            .map_err(|e| e.to_string())?;
        let active_process = crate::claude::process::ActiveProcess::new(
            child_arc.clone(),
            stdin_arc.clone(),
            effective_session_id.clone(),
            process_job_id,
        );
        processes.insert(run_id.to_string(), active_process);
    }

    // In bidirectional mode, send the initial prompt via stdin
    if args_config.bidirectional {
        let mut stdin_guard = stdin_arc.lock().map_err(|e| e.to_string())?;
        if let Some(ref mut stdin_writer) = *stdin_guard {
            let content =
                crate::claude::stdin::build_message_content(&final_prompt, Some(working_dir), None);

            let initial_message = serde_json::json!({
                "type": "user",
                "message": {
                    "role": "user",
                    "content": content
                }
            });
            writeln!(stdin_writer, "{}", initial_message)
                .map_err(|e| format!("Failed to send initial prompt via stdin: {}", e))?;
            stdin_writer
                .flush()
                .map_err(|e| format!("Failed to flush stdin: {}", e))?;
            log::info!(
                "Sent initial prompt via stdin ({} chars)",
                final_prompt.len()
            );
        }
    }

    // Clone what we need for the thread
    let run_id = run_id.to_string();
    let orch = orch.clone();
    let emitter = orch.services.emitter.clone();
    let _initial_user_message = initial_user_message;

    let thread_session_id = effective_session_id;

    // Spawn thread to read stdout and emit events
    thread::spawn(move || {
        debug_log("reader_thread: started");
        let thread_start = std::time::Instant::now();
        log::info!("[PROFILE] Reader thread started");
        let mut sequence = 0;
        let session_id: Option<String> = thread_session_id;
        let mut first_event_logged = false;
        let mut boundary_checker = TurnBoundaryChecker::new();
        let mut streaming_state: Option<StreamingState> = None;

        debug_log("reader_thread: about to read lines");
        for line_result in stdout.lines() {
            let line = match line_result {
                Ok(l) => {
                    if !l.contains("\"type\":\"stream_event\"") {
                        debug_log(&format!(
                            "reader_thread: line {}: {}",
                            sequence,
                            &l[..l.len().min(100)]
                        ));
                    }
                    l
                }
                Err(e) => {
                    debug_log(&format!("reader_thread: error reading line: {}", e));
                    log::error!("Error reading line: {}", e);
                    continue;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            match parse_event(&line) {
                Ok((event, raw)) => {
                    // Handle control responses
                    if let ClaudeEvent::ControlResponse {
                        request_id,
                        response,
                    } = &event
                    {
                        use crate::claude::stream::ControlResponseInner;
                        match response {
                            ControlResponseInner::Success { .. } => {
                                log::info!(
                                    "Control request {} succeeded for run {}",
                                    &request_id[..request_id.len().min(8)],
                                    &run_id[..run_id.len().min(8)]
                                );
                            }
                            ControlResponseInner::Error { message } => {
                                log::warn!(
                                    "Control request {} failed for run {}: {:?}",
                                    &request_id[..request_id.len().min(8)],
                                    &run_id[..run_id.len().min(8)],
                                    message
                                );
                            }
                        }
                        continue;
                    }

                    if !first_event_logged {
                        log::info!(
                            "[PROFILE] First event received: {:?}",
                            thread_start.elapsed()
                        );
                        first_event_logged = true;
                    }

                    // Handle streaming events
                    if let ClaudeEvent::StreamEvent { inner, .. } = &event {
                        if let Ok(mut conn) = orch.db.conn.lock() {
                            match inner {
                                StreamEventInner::MessageStart { .. } => {
                                    // Defensive: finalize any orphaned streaming placeholder
                                    if let Some(orphan) = streaming_state.take() {
                                        log::warn!(
                                            "New MessageStart while streaming_state active for event {}",
                                            &orphan.event_id[..orphan.event_id.len().min(8)]
                                        );
                                        let orphan_event = TranscriptEvent {
                                            event_type: "assistant".to_string(),
                                            session_id: session_id.clone(),
                                            parent_tool_use_id: None,
                                            content: if orphan.content.is_empty() {
                                                None
                                            } else {
                                                Some(orphan.content)
                                            },
                                            thinking: if orphan.thinking.is_empty() {
                                                None
                                            } else {
                                                Some(orphan.thinking)
                                            },
                                            tool_name: None,
                                            tool_input: None,
                                            tool_uses: None,
                                            tool_use_id: None,
                                            tool_result: None,
                                            is_error: false,
                                            usage: None,
                                            raw: None,
                                        };
                                        let orphan_data = serde_json::to_string(&orphan_event)
                                            .unwrap_or_default();
                                        let _ =
                                            diesel::update(events::table.find(&orphan.event_id))
                                                .set((
                                                    events::event_type.eq("assistant"),
                                                    events::data.eq(orphan_data),
                                                ))
                                                .execute(&mut *conn);
                                        let _ = emitter.emit(
                                            "db-change",
                                            serde_json::json!({"table": "events", "action": "update"}),
                                        );
                                        sequence += 1;
                                    }
                                    // Create a placeholder event for streaming
                                    let event_id = Uuid::new_v4().to_string();
                                    let now = chrono::Utc::now().timestamp() as i32;
                                    let placeholder = TranscriptEvent {
                                        event_type: "assistant:streaming".to_string(),
                                        session_id: session_id.clone(),
                                        parent_tool_use_id: None,
                                        content: Some(String::new()),
                                        thinking: None,
                                        tool_name: None,
                                        tool_input: None,
                                        tool_uses: None,
                                        tool_use_id: None,
                                        tool_result: None,
                                        is_error: false,
                                        usage: None,
                                        raw: None,
                                    };
                                    let data =
                                        serde_json::to_string(&placeholder).unwrap_or_default();
                                    let new_event = NewEvent {
                                        id: &event_id,
                                        run_id: &run_id,
                                        session_id: session_id.as_deref(),
                                        sequence,
                                        timestamp: now,
                                        event_type: "assistant:streaming",
                                        data: &data,
                                        parent_tool_use_id: None,
                                        created_at: now,
                                        input_tokens: None,
                                        cache_read_tokens: None,
                                        cache_create_tokens: None,
                                        output_tokens: None,
                                    };
                                    let _ = diesel::insert_into(events::table)
                                        .values(&new_event)
                                        .execute(&mut *conn);
                                    streaming_state = Some(StreamingState::new(event_id));
                                    let _ = emitter.emit(
                                        "db-change",
                                        serde_json::json!({"table": "events", "action": "insert"}),
                                    );
                                }
                                StreamEventInner::ContentBlockDelta { delta, .. } => {
                                    if let Some(ref mut state) = streaming_state {
                                        match delta {
                                            DeltaContent::TextDelta { text } => {
                                                state.content.push_str(text);
                                            }
                                            DeltaContent::ThinkingDelta { thinking } => {
                                                state.thinking.push_str(thinking);
                                            }
                                            DeltaContent::Unknown => {}
                                        }
                                        let updated = TranscriptEvent {
                                            event_type: "assistant:streaming".to_string(),
                                            session_id: session_id.clone(),
                                            parent_tool_use_id: None,
                                            content: if state.content.is_empty() {
                                                None
                                            } else {
                                                Some(state.content.clone())
                                            },
                                            thinking: if state.thinking.is_empty() {
                                                None
                                            } else {
                                                Some(state.thinking.clone())
                                            },
                                            tool_name: None,
                                            tool_input: None,
                                            tool_uses: None,
                                            tool_use_id: None,
                                            tool_result: None,
                                            is_error: false,
                                            usage: None,
                                            raw: None,
                                        };
                                        let data =
                                            serde_json::to_string(&updated).unwrap_or_default();
                                        let _ = diesel::update(events::table.find(&state.event_id))
                                            .set(events::data.eq(&data))
                                            .execute(&mut *conn);
                                        let _ = emitter.emit(
                                            "streaming-update",
                                            serde_json::json!({
                                                "run_id": run_id,
                                                "event_id": state.event_id,
                                                "content": state.content,
                                                "thinking": state.thinking,
                                            }),
                                        );
                                    }
                                }
                                _ => {}
                            }
                        }
                        continue;
                    }

                    // Finalize streaming placeholder before Result event
                    if matches!(&event, ClaudeEvent::Result { .. }) && streaming_state.is_some() {
                        finalize_streaming_placeholder(
                            &orch,
                            &mut streaming_state,
                            session_id.as_deref(),
                        );
                        sequence += 1;
                    }

                    let transcript_event = TranscriptEvent::from_claude_event(&event, raw.clone());

                    // Handle Assistant events during streaming
                    let is_assistant = matches!(&event, ClaudeEvent::Assistant { .. });
                    let has_content =
                        transcript_event.content.is_some() || transcript_event.tool_uses.is_some();
                    let has_thinking = transcript_event.thinking.is_some();

                    // Partial Assistant event (thinking complete, no content yet)
                    if streaming_state.is_some() && is_assistant && has_thinking && !has_content {
                        if let Ok(mut conn) = orch.db.conn.lock() {
                            if let Some(ref mut state) = streaming_state {
                                state.thinking =
                                    transcript_event.thinking.clone().unwrap_or_default();
                                let updated = TranscriptEvent {
                                    event_type: "assistant:streaming".to_string(),
                                    session_id: session_id.clone(),
                                    parent_tool_use_id: None,
                                    content: if state.content.is_empty() {
                                        None
                                    } else {
                                        Some(state.content.clone())
                                    },
                                    thinking: Some(state.thinking.clone()),
                                    tool_name: None,
                                    tool_input: None,
                                    tool_uses: None,
                                    tool_use_id: None,
                                    tool_result: None,
                                    is_error: false,
                                    usage: None,
                                    raw: None,
                                };
                                let data = serde_json::to_string(&updated).unwrap_or_default();
                                let _ = diesel::update(events::table.find(&state.event_id))
                                    .set(events::data.eq(&data))
                                    .execute(&mut *conn);
                                let _ = emitter.emit(
                                    "db-change",
                                    serde_json::json!({"table": "events", "action": "update"}),
                                );
                            }
                        }
                        continue;
                    }

                    // Complete Assistant event (has content or tool_uses) - finalize placeholder
                    if streaming_state.is_some() && is_assistant && has_content {
                        if let Ok(mut conn) = orch.db.conn.lock() {
                            let state = streaming_state.take().unwrap();
                            let mut final_event = transcript_event.clone();
                            if final_event.thinking.is_none() && !state.thinking.is_empty() {
                                final_event.thinking = Some(state.thinking);
                            }
                            let data = serde_json::to_string(&final_event).unwrap_or_default();
                            let _ = diesel::update(events::table.find(&state.event_id))
                                .set((
                                    events::event_type.eq(&final_event.event_type),
                                    events::data.eq(&data),
                                ))
                                .execute(&mut *conn);
                            let _ = emitter.emit(
                                "db-change",
                                serde_json::json!({"table": "events", "action": "update"}),
                            );
                        }
                        sequence += 1;
                        continue;
                    }

                    // Store event in database
                    if let Ok(mut conn) = orch.db.conn.lock() {
                        let now = chrono::Utc::now().timestamp() as i32;
                        let event_id = Uuid::new_v4().to_string();
                        let event_type = &transcript_event.event_type;
                        let parent_tool_use_id = &transcript_event.parent_tool_use_id;
                        let data = serde_json::to_string(&transcript_event).unwrap_or_default();

                        let (input_tokens, cache_read_tokens, cache_create_tokens, output_tokens) =
                            if let Some(ref usage) = transcript_event.usage {
                                (
                                    Some(usage.input_tokens as i32),
                                    usage.cache_read_input_tokens.map(|t| t as i32),
                                    usage.cache_creation_input_tokens.map(|t| t as i32),
                                    Some(usage.output_tokens as i32),
                                )
                            } else {
                                (None, None, None, None)
                            };

                        let new_event = NewEvent {
                            id: &event_id,
                            run_id: &run_id,
                            session_id: session_id.as_deref(),
                            sequence,
                            timestamp: now,
                            event_type,
                            data: &data,
                            parent_tool_use_id: parent_tool_use_id.as_deref(),
                            created_at: now,
                            input_tokens,
                            cache_read_tokens,
                            cache_create_tokens,
                            output_tokens,
                        };

                        let _ = diesel::insert_into(events::table)
                            .values(&new_event)
                            .execute(&mut *conn);

                        let _ = emitter.emit(
                            "db-change",
                            serde_json::json!({"table": "events", "action": "insert"}),
                        );

                        // If this is a TodoWrite event, update run's todos column
                        if let Some(tool_uses) = &transcript_event.tool_uses {
                            for tool_use in tool_uses {
                                if tool_use.name == "TodoWrite" {
                                    if let Some(todos_val) = tool_use.input.get("todos") {
                                        let todos_json =
                                            serde_json::to_string(todos_val).unwrap_or_default();
                                        let _ = diesel::update(runs::table.find(&run_id))
                                            .set(runs::todos.eq(Some(&todos_json)))
                                            .execute(&mut *conn);
                                        let _ = emitter.emit(
                                            "db-change",
                                            serde_json::json!({"table": "runs", "action": "update"}),
                                        );
                                    }
                                }
                            }
                        } else if transcript_event.tool_name.as_deref() == Some("TodoWrite") {
                            if let Some(input) = &transcript_event.tool_input {
                                if let Some(todos_val) = input.get("todos") {
                                    let todos_json =
                                        serde_json::to_string(todos_val).unwrap_or_default();
                                    let _ = diesel::update(runs::table.find(&run_id))
                                        .set(runs::todos.eq(Some(&todos_json)))
                                        .execute(&mut *conn);
                                    let _ = emitter.emit(
                                        "db-change",
                                        serde_json::json!({"table": "runs", "action": "update"}),
                                    );
                                }
                            }
                        }
                    }

                    // Check for turn completion
                    if let ClaudeEvent::Result { is_error, .. } = &event {
                        if *is_error {
                            super::lifecycle::finalize_run(&orch, &run_id, RunStatus::Failed);
                        } else {
                            // Check if this is a task-spawned run (has parent_job_id)
                            let is_task_spawned = if let Ok(mut conn) = orch.db.conn.lock() {
                                runs::table
                                    .find(&run_id)
                                    .select(runs::job_id)
                                    .first::<Option<String>>(&mut *conn)
                                    .ok()
                                    .flatten()
                                    .and_then(|job_id| {
                                        jobs::table
                                            .find(&job_id)
                                            .select(jobs::parent_job_id)
                                            .first::<Option<String>>(&mut *conn)
                                            .ok()
                                            .flatten()
                                    })
                                    .is_some()
                            } else {
                                false
                            };

                            if is_task_spawned {
                                super::lifecycle::finalize_run(
                                    &orch,
                                    &run_id,
                                    RunStatus::Completed,
                                );
                                orch.process_state.transition_to_warm(&run_id);
                                log::info!(
                                    "Task-spawned run {} completed and finalized",
                                    &run_id[..run_id.len().min(8)]
                                );
                            } else {
                                super::lifecycle::transition_to_warm_state(&orch, &run_id);

                                let _ = emitter.emit(
                                    "run-turn-completed",
                                    serde_json::json!({
                                        "run_id": run_id,
                                        "is_warm": true,
                                    }),
                                );

                                log::info!(
                                    "Turn completed for run {}, process now warm",
                                    &run_id[..run_id.len().min(8)]
                                );
                            }
                        }
                    }

                    boundary_checker.update(&transcript_event);

                    sequence += 1;
                }
                Err(e) => {
                    log::warn!("Failed to parse event: {} - line: {}", e, line);
                }
            }
        }

        // Finalize any remaining streaming placeholder on EOF
        finalize_streaming_placeholder(&orch, &mut streaming_state, session_id.as_deref());

        debug_log(&format!(
            "reader_thread: loop ended after {} lines",
            sequence
        ));

        // Stdout closed - process has terminated
        let was_warm = orch
            .process_state
            .get_process_state(&run_id)
            .is_some_and(|s| s == crate::claude::process::ProcessState::Warm);

        if was_warm {
            log::info!(
                "Warm process {} terminated, finalizing as completed",
                &run_id[..run_id.len().min(8)]
            );
            super::lifecycle::finalize_run(&orch, &run_id, RunStatus::Completed);
        } else if let Ok(mut conn) = orch.db.conn.lock() {
            let status: Option<Option<String>> = runs::table
                .find(&run_id)
                .select(runs::status)
                .first(&mut *conn)
                .ok();
            if status.flatten() == Some("running".to_string()) {
                log::warn!(
                    "Process {} terminated without result event, marking as failed",
                    &run_id[..run_id.len().min(8)]
                );

                drop(conn);
                insert_error_event(
                    &orch,
                    &run_id,
                    session_id.as_deref(),
                    "Process terminated unexpectedly without completing",
                );

                super::lifecycle::finalize_run(&orch, &run_id, RunStatus::Failed);
            }
        }

        // Cleanup process handle
        if let Ok(mut processes) = orch.process_state.processes.lock() {
            processes.remove(&run_id);
            log::debug!(
                "Removed process {} from process map",
                &run_id[..run_id.len().min(8)]
            );
        }
    });

    log::info!(
        "[PROFILE] start_claude_session returning: {:?}",
        start_time.elapsed()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claude::stream::{MessageContent, MessageContentInner};
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
        use crate::claude::args::{build_claude_args, ClaudeArgsConfig};
        use crate::models::Model;

        let args_config = ClaudeArgsConfig {
            mcp_config_path: "/path/to/mcp.json".to_string(),
            skip_permissions: false,
            permission_prompt_tool: None,
            model: Some(Model::Opus),
            session_id: None,
            resume_session_id: None,
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
        assert!(spawn_config.args.contains(&"opus".to_string()));
        assert!(spawn_config.args.contains(&"--input-format".to_string()));
        assert!(spawn_config.capture_stdin);
    }
}
