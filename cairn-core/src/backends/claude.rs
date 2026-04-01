//! Claude CLI backend implementation.
//!
//! Handles spawning the Claude CLI process, managing stdin/stdout communication,
//! and reading the stream-json event stream into the database.

use crate::agent_process::args::{build_claude_args, ClaudeArgsConfig};
use crate::agent_process::stream::{
    parse_event, ClaudeEvent, DeltaContent, StreamEventInner, TranscriptEvent,
};
use crate::agent_process::turn_boundary::TurnBoundaryChecker;

use crate::diesel_models::*;
use crate::models::RunStatus;
use crate::orchestrator::session::{get_claude_path, insert_error_event, write_system_prompt_file};
use crate::orchestrator::Orchestrator;
use crate::schema::*;
use crate::services::SpawnConfig;
use crate::transcripts::stream_store::{
    abort_stream, append_chunks, finalize_stream, open_stream, process_post_commit_outbox,
    read_active_stream, ActiveMessageStream, StreamChunkInput,
};
use diesel::prelude::*;
use std::io::{BufRead, Write};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use uuid::Uuid;

use super::{AgentBackend, DiscoveredModel, ResolvedTools, SessionConfig};
use crate::models::ApprovalPolicy;

/// State for tracking a durable streaming message.
#[derive(Debug)]
struct StreamingState {
    stream_id: String,
    version: i32,
}

#[cfg(test)]
mod tests {
    use super::should_persist_backend_id;
    use crate::backends::SessionStart;

    #[test]
    fn claude_persists_backend_id_for_new_sessions() {
        assert!(should_persist_backend_id(&SessionStart::New {
            session_id: "session-new".to_string(),
        }));
    }

    #[test]
    fn claude_does_not_persist_backend_id_for_resumed_sessions() {
        assert!(!should_persist_backend_id(&SessionStart::Resume {
            session_id: "session-resume".to_string(),
            backend_id: "backend-existing".to_string(),
        }));
    }

    #[test]
    fn claude_persists_backend_id_for_forked_sessions() {
        assert!(should_persist_backend_id(&SessionStart::Fork {
            session_id: "session-fork".to_string(),
            source_backend_id: "backend-source".to_string(),
        }));
    }
}

impl StreamingState {
    fn new(stream: &ActiveMessageStream) -> Self {
        Self {
            stream_id: stream.stream.id.clone(),
            version: stream.stream.version,
        }
    }
}

fn emit_streaming_update(orch: &Orchestrator, run_id: &str, active: &ActiveMessageStream) {
    let _ = orch.services.emitter.emit(
        "streaming-update",
        serde_json::json!({
            "run_id": run_id,
            "event_id": active.stream.id,
            "content": active.content,
            "thinking": active.thinking,
        }),
    );
}

fn is_task_spawned_run(orch: &Orchestrator, run_id: &str) -> bool {
    if let Ok(mut conn) = orch.db.conn.lock() {
        return runs::table
            .find(run_id)
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
            .is_some();
    }

    false
}

fn should_finalize_task_run_on_terminal_tool_eof(
    terminal_tool_called: bool,
    run_status: Option<&str>,
    is_task_spawned: bool,
) -> bool {
    terminal_tool_called && run_status == Some("running") && is_task_spawned
}

fn finalize_streaming_message(
    orch: &Orchestrator,
    run_id: &str,
    streaming_state: &mut Option<StreamingState>,
    final_event: Option<TranscriptEvent>,
) -> bool {
    let Some(state) = streaming_state.take() else {
        return false;
    };
    let Ok(mut conn) = orch.db.conn.lock() else {
        return false;
    };
    match finalize_stream(&mut conn, &state.stream_id, state.version, final_event) {
        Ok(finalized) => {
            drop(conn);
            let _ = orch.services.emitter.emit(
                "db-change",
                serde_json::json!({"table": "events", "action": "insert"}),
            );
            process_post_commit_outbox(orch, &finalized.outbox_entries);
            true
        }
        Err(error) => {
            log::warn!(
                "Failed to finalize stream {} for run {}: {}",
                state.stream_id,
                run_id,
                error
            );
            false
        }
    }
}

/// Claude CLI agent backend.
pub struct ClaudeBackend;

fn should_persist_backend_id(session_start: &crate::backends::SessionStart) -> bool {
    matches!(
        session_start,
        crate::backends::SessionStart::New { .. } | crate::backends::SessionStart::Fork { .. }
    )
}

impl AgentBackend for ClaudeBackend {
    fn name(&self) -> &str {
        "Claude"
    }

    fn is_available(&self) -> Result<(), String> {
        crate::env::find_binary("claude").map(|_| ())
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        discover_claude_models()
    }

    fn resolve_tools(&self, agent_tools: &[String], agent_disallowed: &[String]) -> ResolvedTools {
        use crate::agent_process::toolkits;

        // Map agent tool names to preferred versions (Cairn vs native)
        let (mut allowed, force_disallowed) = toolkits::resolve_tools(agent_tools);

        // Auto-add submission tool
        if !allowed.contains(&"mcp__cairn__return".to_string()) {
            allowed.push("mcp__cairn__return".to_string());
        }

        // Auto-add skill tool (always available for Claude)
        if !allowed.contains(&"mcp__cairn__skill".to_string()) {
            allowed.push("mcp__cairn__skill".to_string());
        }

        // Always allow Glob and Grep (read-only search tools)
        for tool in &["mcp__cairn__glob", "mcp__cairn__grep"] {
            if !allowed.contains(&tool.to_string()) {
                allowed.push(tool.to_string());
            }
        }

        // Build disallowed: everything not allowed
        let allowed_set: std::collections::HashSet<_> = allowed.iter().cloned().collect();
        let all_known = toolkits::get_all_known_tools();

        let mut disallowed: Vec<String> = all_known
            .into_iter()
            .filter(|t| !allowed_set.contains(t))
            .collect();

        // Add unchosen provider versions
        disallowed.extend(force_disallowed);

        // Always disallow planning mode tools
        for tool in crate::models::ALWAYS_DISALLOWED_TOOLS {
            if !disallowed.contains(&tool.to_string()) {
                disallowed.push(tool.to_string());
            }
        }

        // Add agent-specific disallowed tools
        for tool in agent_disallowed {
            if !disallowed.contains(tool) {
                disallowed.push(tool.clone());
            }
        }

        // Remove always-disallowed from allowed (in case agent config has them)
        allowed.retain(|t| !crate::models::ALWAYS_DISALLOWED_TOOLS.contains(&t.as_str()));

        disallowed.sort();
        disallowed.dedup();

        ResolvedTools {
            allowed,
            disallowed,
        }
    }

    fn start_session(&self, config: SessionConfig, orch: &Orchestrator) -> Result<(), String> {
        let start_time = std::time::Instant::now();

        let session_id = Some(config.session_start.session_id().to_string());

        // Translate canonical permissions to Claude CLI flags
        let (use_skip_permissions, permission_prompt_tool) = match config.permissions.approval {
            ApprovalPolicy::AcceptAll => (true, None),
            _ => (false, Some("mcp__cairn__permission_prompt".to_string())),
        };

        // Write combined system prompt (bundled + agent) to temp file
        let prompt_file_path =
            write_system_prompt_file(&config.run_id, config.system_prompt_content.as_deref())?;

        // Write hook settings file for memory surfacing (passed via --settings)
        let hook_settings_path =
            crate::memories::hooks::write_hook_settings_file(orch.mcp_callback_port).ok();

        // Build Claude CLI arguments
        let args_config = ClaudeArgsConfig {
            mcp_config_path: config.mcp_config_path.to_string_lossy().to_string(),
            skip_permissions: use_skip_permissions,
            permission_prompt_tool,
            model: config.model,
            session_start: config.session_start.clone(),
            prompt: config.prompt.clone(),
            max_thinking_tokens: config.max_thinking_tokens,
            allowed_tools: config.allowed_tools,
            disallowed_tools: config.disallowed_tools,
            append_system_prompt_file: Some(prompt_file_path),
            settings_path: hook_settings_path,
            bidirectional: config.bidirectional,
        };
        let claude_args = build_claude_args(&args_config);

        // Get cached claude path (resolves once on first use)
        let claude_path = get_claude_path(&orch.process_state)?;

        log::debug!("ClaudeBackend: command built, claude_path={}", claude_path);
        log::debug!("ClaudeBackend: args={:?}", claude_args);
        log::debug!("ClaudeBackend: working_dir={}", config.working_dir);

        log::info!("[PROFILE] Command built: {:?}", start_time.elapsed());
        log::info!("Spawning claude: {} {:?}", claude_path, claude_args);

        // Get MCP authentication secret (shared secret for TOTP-style passcodes)
        let mcp_secret = orch
            .mcp_auth
            .get_secret_for_mcp()
            .map_err(|e| format!("Failed to get MCP auth secret: {}", e))?;
        log::info!("Using MCP auth secret for run {}", config.run_id);

        // Build spawn config and spawn using ProcessSpawner service
        let mut spawn_config = SpawnConfig::new(&claude_path)
            .args(&claude_args)
            .cwd(&config.working_dir)
            .env("CAIRN_RUN_ID", &config.run_id)
            .env("CAIRN_MCP_SECRET", &mcp_secret)
            .env("ENABLE_TOOL_SEARCH", "false")
            .stdin(true);

        // Inject user identity into Claude process environment
        // Prefer pre-resolved identity from SessionConfig (includes project overrides)
        if let Some(user) = config
            .identity
            .as_ref()
            .cloned()
            .or_else(|| orch.get_identity())
        {
            spawn_config = spawn_config
                .env("GIT_AUTHOR_NAME", &user.name)
                .env("GIT_AUTHOR_EMAIL", &user.email)
                .env("GIT_COMMITTER_NAME", &user.name)
                .env("GIT_COMMITTER_EMAIL", &user.email);

            // Forward Claude auth token for remote/headless sessions
            match &user.claude_auth {
                Some(crate::identity::ClaudeAuth::OAuthToken(token)) => {
                    log::info!("Setting CLAUDE_CODE_OAUTH_TOKEN (len={})", token.len());
                    spawn_config = spawn_config.env("CLAUDE_CODE_OAUTH_TOKEN", token);
                }
                Some(crate::identity::ClaudeAuth::ApiKey(key)) => {
                    log::info!("Setting ANTHROPIC_API_KEY (len={})", key.len());
                    spawn_config = spawn_config.env("ANTHROPIC_API_KEY", key);
                }
                None => {} // Use ambient auth (local claude login)
            }

            log::info!(
                "Injected user identity into session: {} <{}>",
                user.name,
                user.email
            );
        }

        // Check if we need to evict a warm process to make room
        orch.collect_warm_if_needed();

        log::debug!("ClaudeBackend: about to spawn");
        let mut child = orch.services.process.spawn(spawn_config).map_err(|e| {
            log::debug!("ClaudeBackend: spawn failed: {}", e);
            insert_error_event(
                orch,
                &config.run_id,
                session_id.as_deref(),
                &format!("Failed to start Claude: {}", e),
            );
            e
        })?;
        log::debug!("ClaudeBackend: spawned, pid={}", child.id());
        log::info!("[PROFILE] Process spawned: {:?}", start_time.elapsed());

        // Transition run to Running AFTER successful spawn (sets started_at accurately)
        log::debug!("ClaudeBackend: transitioning run to running");
        {
            let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
            if let Err(e) = crate::transitions::transition_run(
                &mut conn,
                &config.run_id,
                crate::models::RunStatus::Live,
                &*orch.services.emitter,
            ) {
                log::warn!("Failed to transition run to running: {}", e);
            }
            // Job is already Running from start_job's transition_job call — no write needed
        }
        log::debug!("ClaudeBackend: run transitioned to running");

        let stdout = child.take_stdout().ok_or("Failed to capture stdout")?;
        let stderr = child.take_stderr();
        let stdin = child
            .take_stdin()
            .map(|w| crate::agent_process::process::wrap_plain_stdin(w));

        // Spawn thread to log stderr
        if let Some(stderr) = stderr {
            thread::spawn(move || {
                log::debug!("stderr_thread: started");
                for line in stderr.lines().map_while(Result::ok) {
                    log::debug!("claude stderr: {}", line);
                    log::error!("claude stderr: {}", line);
                }
                log::debug!("stderr_thread: ended");
            });
        }

        // Store the process handle with stdin for bidirectional communication
        let child_arc = Arc::new(Mutex::new(Some(child)));
        let stdin_arc = Arc::new(Mutex::new(stdin));

        // Get job_id for warm process tracking
        let process_job_id: Option<String> = {
            let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;
            runs::table
                .find(&config.run_id)
                .select(runs::job_id)
                .first::<Option<String>>(&mut *conn)
                .ok()
                .flatten()
        };

        // For Claude, backend_id = session_id on fresh sessions. Forks and resumes keep their own source handle.
        if let Some(ref sid) = session_id {
            if should_persist_backend_id(&config.session_start) {
                if let Ok(mut conn) = orch.db.conn.lock() {
                    let _ = crate::sessions::queries::set_backend_id(&mut conn, sid, sid);
                }
            }
        }

        {
            let mut processes = orch
                .process_state
                .processes
                .lock()
                .map_err(|e| e.to_string())?;
            let active_process = crate::agent_process::process::ActiveProcess::new(
                child_arc.clone(),
                stdin_arc.clone(),
                session_id.clone(),
                process_job_id,
            );
            processes.register(config.run_id.clone(), active_process);
        }

        // In bidirectional mode, send the initial prompt via stdin
        if args_config.bidirectional {
            let mut stdin_guard = stdin_arc.lock().map_err(|e| e.to_string())?;
            if let Some(ref mut stdin_writer) = *stdin_guard {
                let content = crate::agent_process::stdin::build_message_content(
                    &config.prompt,
                    Some(&config.working_dir),
                    None,
                );

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
                    config.prompt.len()
                );
            }
        }

        // Clone what we need for the reader thread
        let run_id = config.run_id.clone();
        let orch = orch.clone();
        let emitter = orch.services.emitter.clone();

        let thread_session_id = session_id;

        // Spawn thread to read stdout and emit events
        thread::spawn(move || {
            Self::reader_thread(&orch, &emitter, &run_id, thread_session_id, stdout);
        });

        log::info!(
            "[PROFILE] ClaudeBackend::start_session returning: {:?}",
            start_time.elapsed()
        );
        Ok(())
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn supports_warm_processes(&self) -> bool {
        true
    }

    fn send_user_message(
        &self,
        stdin: &mut dyn crate::agent_process::process::BackendStdin,
        content: &str,
        session_id: &str,
        parent_tool_use_id: Option<&str>,
        working_dir: Option<&str>,
    ) -> Result<(), String> {
        crate::agent_process::stdin::send_user_message_with_images(
            stdin,
            session_id,
            content,
            parent_tool_use_id,
            working_dir,
            None,
        )
    }

    fn send_interrupt(
        &self,
        stdin: &mut dyn crate::agent_process::process::BackendStdin,
    ) -> Result<(), String> {
        let request_id = uuid::Uuid::new_v4().to_string();
        crate::agent_process::stdin::send_interrupt_request(stdin, &request_id)
    }

    fn send_set_model(
        &self,
        stdin: &mut dyn crate::agent_process::process::BackendStdin,
        model: &str,
    ) -> Result<(), String> {
        let request_id = uuid::Uuid::new_v4().to_string();
        crate::agent_process::stdin::send_set_model_request(stdin, &request_id, model)
    }

    fn send_set_permission_mode(
        &self,
        stdin: &mut dyn crate::agent_process::process::BackendStdin,
        mode: &str,
    ) -> Result<(), String> {
        let request_id = uuid::Uuid::new_v4().to_string();
        crate::agent_process::stdin::send_set_permission_mode_request(stdin, &request_id, mode)
    }
}

fn discover_claude_models() -> Result<Vec<DiscoveredModel>, String> {
    let claude_path = crate::env::find_binary("claude")?;
    let status = Command::new(&claude_path)
        .args(["auth", "status", "--json"])
        .output()
        .map_err(|e| format!("Failed to run Claude auth status: {}", e))?;

    if !status.status.success() {
        let stderr = String::from_utf8_lossy(&status.stderr).trim().to_string();
        return Err(if stderr.is_empty() {
            format!("Claude auth status exited with {}", status.status)
        } else {
            format!("Claude auth status failed: {}", stderr)
        });
    }

    let auth_json: serde_json::Value = serde_json::from_slice(&status.stdout)
        .map_err(|e| format!("Failed to parse Claude auth status JSON: {}", e))?;
    let logged_in = auth_json
        .get("loggedIn")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !logged_in {
        return Err("Claude CLI is not authenticated".to_string());
    }

    Ok(vec![
        DiscoveredModel {
            id: "haiku".to_string(),
            model: "haiku".to_string(),
            display_name: "haiku".to_string(),
            description: Some("Fast Claude alias".to_string()),
            hidden: false,
            is_default: false,
            default_reasoning_effort: None,
            supported_reasoning_efforts: vec![],
        },
        DiscoveredModel {
            id: "sonnet".to_string(),
            model: "sonnet".to_string(),
            display_name: "sonnet".to_string(),
            description: Some("Balanced Claude alias".to_string()),
            hidden: false,
            is_default: true,
            default_reasoning_effort: None,
            supported_reasoning_efforts: vec![],
        },
        DiscoveredModel {
            id: "opus".to_string(),
            model: "opus".to_string(),
            display_name: "opus".to_string(),
            description: Some("High-capability Claude alias".to_string()),
            hidden: false,
            is_default: false,
            default_reasoning_effort: None,
            supported_reasoning_efforts: vec![],
        },
    ])
}

impl ClaudeBackend {
    /// Reader thread: reads Claude's stream-json stdout and stores events in the database.
    fn reader_thread(
        orch: &Orchestrator,
        emitter: &Arc<dyn crate::services::EventEmitter>,
        run_id: &str,
        session_id: Option<String>,
        stdout: Box<dyn std::io::BufRead + Send>,
    ) {
        log::debug!("reader_thread: started");
        let thread_start = std::time::Instant::now();
        log::debug!("[PROFILE] Reader thread started");
        let mut sequence = 0;
        let mut first_event_logged = false;
        let mut boundary_checker = TurnBoundaryChecker::new();
        let mut streaming_state: Option<StreamingState> = None;

        // Grab the terminal_tool_called flag for this process.
        // When set, we stop storing events — the session is complete.
        let terminal_tool_flag = orch
            .process_state
            .processes
            .lock()
            .ok()
            .and_then(|p| p.get(run_id).map(|proc| proc.terminal_tool_called.clone()))
            .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));

        log::trace!("reader_thread: about to read lines");
        for line_result in stdout.lines() {
            let line = match line_result {
                Ok(l) => {
                    if !l.contains("\"type\":\"stream_event\"") {
                        log::trace!(
                            "reader_thread: line {}: {}",
                            sequence,
                            &l[..l.len().min(100)]
                        );
                    }
                    l
                }
                Err(e) => {
                    log::debug!("reader_thread: error reading line: {}", e);
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
                        use crate::agent_process::stream::ControlResponseInner;
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

                    // Handle streaming events (skip if session ended via terminal tool)
                    if let ClaudeEvent::StreamEvent { inner, .. } = &event {
                        if terminal_tool_flag.load(std::sync::atomic::Ordering::Acquire) {
                            continue;
                        }
                        if let Ok(mut conn) = orch.db.conn.lock() {
                            match inner {
                                StreamEventInner::MessageStart { .. } => {
                                    drop(conn);
                                    if streaming_state.is_some() {
                                        log::warn!(
                                            "New MessageStart while a stream is still active"
                                        );
                                        if finalize_streaming_message(
                                            orch,
                                            run_id,
                                            &mut streaming_state,
                                            None,
                                        ) {
                                            sequence += 1;
                                        }
                                    }
                                    if let Ok(mut conn) = orch.db.conn.lock() {
                                        let current_turn =
                                            orch.process_state.get_current_turn_id(run_id);
                                        match open_stream(
                                            &mut conn,
                                            run_id,
                                            session_id.as_deref(),
                                            current_turn.as_deref(),
                                            "claude",
                                            Some(sequence),
                                        ) {
                                            Ok(stream) => {
                                                streaming_state =
                                                    Some(StreamingState::new(&stream));
                                                let _ = emitter.emit(
                                                    "db-change",
                                                    serde_json::json!({"table": "events", "action": "insert"}),
                                                );
                                            }
                                            Err(error) => {
                                                log::warn!(
                                                    "Failed to open Claude stream for {}: {}",
                                                    run_id,
                                                    error
                                                );
                                            }
                                        }
                                    }
                                }
                                StreamEventInner::ContentBlockDelta { delta, .. } => {
                                    if let Some(ref mut state) = streaming_state {
                                        let chunk = match delta {
                                            DeltaContent::TextDelta { text } => {
                                                orch.sync(crate::sync::SyncMessage::StreamDelta(
                                                    crate::sync::StreamDelta {
                                                        run_id: run_id.to_string(),
                                                        event_id: state.stream_id.clone(),
                                                        tokens: text.to_string(),
                                                    },
                                                ));
                                                Some(StreamChunkInput::content(text.to_string()))
                                            }
                                            DeltaContent::ThinkingDelta { thinking } => Some(
                                                StreamChunkInput::thinking(thinking.to_string()),
                                            ),
                                            DeltaContent::Unknown => None,
                                        };
                                        if let Some(chunk) = chunk {
                                            match append_chunks(
                                                &mut *conn,
                                                &state.stream_id,
                                                state.version,
                                                &[chunk],
                                            ) {
                                                Ok(active) => {
                                                    state.version = active.version();
                                                    emit_streaming_update(orch, run_id, &active);
                                                }
                                                Err(error) => {
                                                    log::warn!(
                                                        "Failed to append Claude stream chunk for {}: {}",
                                                        run_id,
                                                        error
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        continue;
                    }

                    // After a terminal tool (return/create_pr/etc.), stop storing events.
                    // The session is complete — only process Result for lifecycle.
                    if terminal_tool_flag.load(std::sync::atomic::Ordering::Acquire) {
                        // Delete any streaming placeholder that started after the flag
                        if let Some(state) = streaming_state.take() {
                            if let Ok(mut conn) = orch.db.conn.lock() {
                                let _ = abort_stream(
                                    &mut conn,
                                    &state.stream_id,
                                    state.version,
                                    "terminal_tool",
                                );
                                let _ = emitter.emit(
                                    "db-change",
                                    serde_json::json!({"table": "events", "action": "update"}),
                                );
                            }
                        }

                        // Still process Result for lifecycle (warm transition, finalization).
                        // After a successful turn-ending tool like `return`, Claude can emit a
                        // terminal Result with `is_error=true` because we intentionally interrupt
                        // the session. At that point the tool side effects are already committed,
                        // so the Result is only transport cleanup and must not overwrite success.
                        if let ClaudeEvent::Result { is_error, .. } = &event {
                            if *is_error {
                                log::info!(
                                    "Ignoring Result.is_error for run {} because a terminal tool already completed the turn",
                                    &run_id[..run_id.len().min(8)]
                                );
                            }

                            let is_task_spawned = is_task_spawned_run(orch, run_id);

                            if is_task_spawned {
                                crate::orchestrator::lifecycle::finalize_run(
                                    orch,
                                    run_id,
                                    RunStatus::Exited,
                                );
                                orch.process_state.transition_to_warm(run_id);
                            } else {
                                crate::orchestrator::lifecycle::transition_to_warm_state(
                                    orch, run_id,
                                );
                                let _ = emitter.emit(
                                    "run-turn-completed",
                                    serde_json::json!({
                                        "run_id": run_id,
                                        "is_warm": true,
                                    }),
                                );
                            }
                        }
                        sequence += 1;
                        continue;
                    }

                    // Finalize streaming placeholder before Result event
                    if matches!(&event, ClaudeEvent::Result { .. })
                        && finalize_streaming_message(orch, run_id, &mut streaming_state, None)
                    {
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
                        if let Some(ref state) = streaming_state {
                            if let Ok(mut conn) = orch.db.conn.lock() {
                                if let Ok(Some(active)) =
                                    read_active_stream(&mut conn, &state.stream_id)
                                {
                                    emit_streaming_update(orch, run_id, &active);
                                }
                            }
                        }
                        continue;
                    }

                    // Complete Assistant event (has content or tool_uses) - finalize placeholder
                    if streaming_state.is_some() && is_assistant && has_content {
                        if finalize_streaming_message(
                            orch,
                            run_id,
                            &mut streaming_state,
                            Some(transcript_event.clone()),
                        ) {
                            sequence += 1;
                        }
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

                        let current_turn = orch.process_state.get_current_turn_id(run_id);
                        let new_event = NewEvent {
                            id: &event_id,
                            run_id,
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
                            turn_id: current_turn.as_deref(),
                        };

                        let _ = diesel::insert_into(events::table)
                            .values(&new_event)
                            .execute(&mut *conn);

                        // Sync event to cloud
                        orch.sync(crate::sync::SyncMessage::Event(crate::sync::SyncEvent {
                            id: event_id.clone(),
                            run_id: run_id.to_string(),
                            session_id: session_id.clone(),
                            sequence: Some(sequence),
                            event_type: event_type.to_string(),
                            data: Some(data.clone()),
                            input_tokens,
                            output_tokens,
                            cache_read_tokens,
                            created_at: Some(now as i64),
                            turn_id: current_turn.clone(),
                        }));

                        let _ = emitter.emit(
                            "db-change",
                            serde_json::json!({"table": "events", "action": "insert"}),
                        );

                        // Embed assistant events inline (~5ms)
                        if event_type == "assistant" {
                            if let Some(ref engine) = orch.embedding_engine {
                                crate::embeddings::embed_event_inline(
                                    engine, &mut conn, &event_id, &data,
                                );
                            }
                        }

                        // TodoWrite events are now handled via MCP tool (todo_write)
                        // and stored directly in the todos table — no event sniffing needed.
                    }

                    // Check for turn completion
                    if let ClaudeEvent::Result { is_error, .. } = &event {
                        if *is_error {
                            crate::orchestrator::lifecycle::finalize_run(
                                orch,
                                run_id,
                                RunStatus::Crashed,
                            );
                        } else {
                            // Check if this is a task-spawned run (has parent_job_id)
                            let is_task_spawned = is_task_spawned_run(orch, run_id);

                            if is_task_spawned {
                                crate::orchestrator::lifecycle::finalize_run(
                                    orch,
                                    run_id,
                                    RunStatus::Exited,
                                );
                                orch.process_state.transition_to_warm(run_id);
                                log::info!(
                                    "Task-spawned run {} completed and finalized",
                                    &run_id[..run_id.len().min(8)]
                                );
                            } else {
                                crate::orchestrator::lifecycle::transition_to_warm_state(
                                    orch, run_id,
                                );

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

        // Finalize any remaining durable stream on EOF
        let _ = finalize_streaming_message(orch, run_id, &mut streaming_state, None);

        log::debug!("reader_thread: loop ended after {} lines", sequence);

        // Stdout closed - process has terminated
        let was_warm = orch
            .process_state
            .get_occupancy(run_id)
            .map(|o| matches!(o, crate::agent_process::process::RunOccupancy::Idle))
            .unwrap_or(false);

        if was_warm {
            log::info!(
                "Warm process {} terminated, finalizing as completed",
                &run_id[..run_id.len().min(8)]
            );
            crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Exited);
        } else if let Ok(mut conn) = orch.db.conn.lock() {
            let status: Option<Option<String>> = runs::table
                .find(run_id)
                .select(runs::status)
                .first(&mut *conn)
                .ok();
            let run_status = status.flatten();
            let terminal_tool_called =
                terminal_tool_flag.load(std::sync::atomic::Ordering::Acquire);
            if should_finalize_task_run_on_terminal_tool_eof(
                terminal_tool_called,
                run_status.as_deref(),
                is_task_spawned_run(orch, run_id),
            ) {
                log::info!(
                    "Task-spawned process {} reached EOF after terminal tool without result event; finalizing as completed",
                    &run_id[..run_id.len().min(8)]
                );
                drop(conn);
                crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Exited);
            } else if run_status.as_deref() == Some("running") {
                log::warn!(
                    "Process {} terminated without result event, marking as failed",
                    &run_id[..run_id.len().min(8)]
                );

                drop(conn);
                insert_error_event(
                    orch,
                    run_id,
                    session_id.as_deref(),
                    "Process terminated unexpectedly without completing",
                );

                crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Crashed);
            }
        }

        // Cleanup process handle
        if let Ok(mut processes) = orch.process_state.processes.lock() {
            processes.remove(run_id);
            log::debug!(
                "Removed process {} from process map",
                &run_id[..run_id.len().min(8)]
            );
        }
    }
}

#[cfg(test)]
mod terminal_tool_tests {
    use super::should_finalize_task_run_on_terminal_tool_eof;

    #[test]
    fn terminal_tool_task_eof_is_treated_as_completed() {
        assert!(should_finalize_task_run_on_terminal_tool_eof(
            true,
            Some("running"),
            true
        ));
        assert!(!should_finalize_task_run_on_terminal_tool_eof(
            false,
            Some("running"),
            true
        ));
        assert!(!should_finalize_task_run_on_terminal_tool_eof(
            true,
            Some("exited"),
            true
        ));
        assert!(!should_finalize_task_run_on_terminal_tool_eof(
            true,
            Some("running"),
            false
        ));
    }
}
