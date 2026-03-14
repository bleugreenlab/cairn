//! Bash command MCP handler.
//!
//! Routes bash commands to either:
//! - Inline execution with streaming output (quick commands)
//! - PTY terminal spawning (long-running/background commands)

use super::{
    get_current_head_sha, get_job_checkpoint_command, is_worktree_dirty, lookup_run,
    lookup_run_by_cwd, normalize_command,
};
use crate::services::{get_default_shell, PtySession};
use portable_pty::{CommandBuilder, PtySize};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

use crate::diesel_models::{NewCheckpointCommandCache, NewJobTerminal};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::schema::{checkpoint_command_cache, job_terminals, jobs};
use diesel::prelude::*;

/// PTY data event payload (mirrors Tauri-side PtyDataPayload)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyDataPayload {
    pub session_id: String,
    pub data: String,
}

/// PTY exit event payload (mirrors Tauri-side PtyExitPayload)
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PtyExitPayload {
    pub session_id: String,
    pub exit_code: Option<i32>,
}

/// Payload for kill_shell tool from MCP server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KillShellPayloadInput {
    /// Terminal to kill - can be a slug or full URI
    pub terminal: String,
}

/// Payload for bash tool from MCP server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BashPayload {
    pub command: String,
    pub description: Option<String>,
    pub timeout: Option<u32>,
    pub run_in_background: Option<bool>,
    pub commit_msg: Option<String>,
    /// Terminal slug/name for background commands. If provided, runs in background and returns a URI.
    pub terminal: Option<String>,
}

/// Event payload for real-time bash output streaming
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BashOutputPayload {
    pub run_id: String,
    pub tool_use_id: String,
    pub chunk: String,
    pub stream: String, // "stdout" or "stderr"
}

/// Event payload for bash command completion
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BashCompletePayload {
    pub run_id: String,
    pub tool_use_id: String,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
}

/// Event payload when agent spawns a background terminal
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentTerminalCreatedPayload {
    pub run_id: String,
    pub session_id: String,
    pub command: String,
    pub description: Option<String>,
}

/// Maximum output buffer size (64KB)
const MAX_BUFFER_SIZE: usize = 64 * 1024;

// Re-export slug utilities from shared module
use super::slug::{build_terminal_uri, generate_terminal_slug};

/// Check if a command includes a git commit operation
fn is_git_commit_command(command: &str) -> bool {
    // Match "git commit" but not "git commit --help" or similar non-committing variants
    // Handles: git commit, git commit -m, git commit -am, git commit --amend, etc.
    let cmd_lower = command.to_lowercase();

    // Split by common command separators to check each part
    for part in cmd_lower.split(['&', '|', ';', '\n']) {
        let trimmed = part.trim();
        // Remove leading shell constructs like "cd foo &&"
        let git_part = trimmed
            .rsplit("&&")
            .next()
            .or_else(|| trimmed.rsplit("||").next())
            .unwrap_or(trimmed)
            .trim();

        if git_part.starts_with("git commit") || git_part.starts_with("git  commit") {
            // Exclude help commands (--help or -h as a flag)
            if git_part.contains("--help") {
                continue;
            }
            // Check for -h flag: either "-h " with trailing space or "-h" at end of string
            if git_part.contains("-h ") || git_part.ends_with("-h") {
                continue;
            }
            return true;
        }
    }
    false
}

/// Handle bash tool call - routes to inline or background execution
pub async fn handle_bash(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: BashPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!(
        "bash command: {} (cwd={}, bg={:?})",
        &payload.command[..payload.command.len().min(100)],
        request.cwd,
        payload.run_in_background
    );

    let run_in_background = payload.run_in_background.unwrap_or(false);

    if run_in_background {
        handle_background_bash(orch, &request.cwd, &payload).await
    } else {
        handle_inline_bash(orch, &request.cwd, &payload).await
    }
}

/// Handle inline bash execution with streaming output
async fn handle_inline_bash(orch: &Orchestrator, cwd: &str, payload: &BashPayload) -> String {
    let services = &orch.services;
    let timeout_ms = payload.timeout.unwrap_or(120_000).min(600_000);

    // Use platform-appropriate shell
    let (shell, flag) = if cfg!(windows) {
        ("cmd.exe", "/c")
    } else {
        ("bash", "-c")
    };

    // Spawn the process
    let mut child = match crate::env::command(shell)
        .arg(flag)
        .arg(&payload.command)
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return format!("Failed to spawn command: {}", e),
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // Collect output (we'll stream it if we have a run context)
    let stdout_content = Arc::new(Mutex::new(String::new()));
    let stderr_content = Arc::new(Mutex::new(String::new()));

    // Try to get run context for streaming (optional - works even without it)
    let run_context = {
        let mut conn = orch.db.conn.lock();
        match conn {
            Ok(ref mut c) => lookup_run_by_cwd(c, cwd).ok(),
            Err(_) => None,
        }
    };

    // Generate a tool_use_id for correlating stream events
    let tool_use_id = Uuid::new_v4().to_string();

    // Read stdout in a thread
    let stdout_handle = {
        let content = stdout_content.clone();
        let emitter = services.emitter.clone();
        let run_id = run_context.as_ref().map(|r| r.run_id.clone());
        let tool_id = tool_use_id.clone();

        thread::spawn(move || {
            if let Some(stdout) = stdout {
                let reader = BufReader::new(stdout);
                for line in reader.lines().map_while(Result::ok) {
                    {
                        let mut c = content.lock().unwrap();
                        if !c.is_empty() {
                            c.push('\n');
                        }
                        c.push_str(&line);
                    }

                    // Stream to frontend if we have run context
                    if let Some(ref rid) = run_id {
                        let _ = emitter.emit(
                            "bash-output",
                            serde_json::to_value(BashOutputPayload {
                                run_id: rid.clone(),
                                tool_use_id: tool_id.clone(),
                                chunk: format!("{}\n", line),
                                stream: "stdout".to_string(),
                            })
                            .unwrap_or_default(),
                        );
                    }
                }
            }
        })
    };

    // Read stderr in a thread
    let stderr_handle = {
        let content = stderr_content.clone();
        let emitter = services.emitter.clone();
        let run_id = run_context.as_ref().map(|r| r.run_id.clone());
        let tool_id = tool_use_id.clone();

        thread::spawn(move || {
            if let Some(stderr) = stderr {
                let reader = BufReader::new(stderr);
                for line in reader.lines().map_while(Result::ok) {
                    {
                        let mut c = content.lock().unwrap();
                        if !c.is_empty() {
                            c.push('\n');
                        }
                        c.push_str(&line);
                    }

                    // Stream to frontend if we have run context
                    if let Some(ref rid) = run_id {
                        let _ = emitter.emit(
                            "bash-output",
                            serde_json::to_value(BashOutputPayload {
                                run_id: rid.clone(),
                                tool_use_id: tool_id.clone(),
                                chunk: format!("{}\n", line),
                                stream: "stderr".to_string(),
                            })
                            .unwrap_or_default(),
                        );
                    }
                }
            }
        })
    };

    // Wait for process with timeout
    let start = std::time::Instant::now();
    let timeout = Duration::from_millis(timeout_ms as u64);
    let mut timed_out = false;
    let mut exit_code = None;

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                exit_code = status.code();
                break;
            }
            Ok(None) => {
                if start.elapsed() > timeout {
                    timed_out = true;
                    let _ = child.kill();
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => {
                return format!("Error waiting for process: {}", e);
            }
        }
    }

    // Wait for output threads to finish
    let _ = stdout_handle.join();
    let _ = stderr_handle.join();

    // Emit completion event
    if let Some(ref ctx) = run_context {
        let _ = services.emitter.emit(
            "bash-complete",
            serde_json::to_value(BashCompletePayload {
                run_id: ctx.run_id.clone(),
                tool_use_id,
                exit_code,
                timed_out,
            })
            .unwrap_or_default(),
        );
    }

    // Cache checkpoint command result if this command matches the job's checkpoint command
    if !timed_out {
        if let Some(ref ctx) = run_context {
            cache_checkpoint_result(orch, &ctx.job_id, &payload.command, cwd, exit_code);
        }
    }

    // Build result string
    let stdout_str = stdout_content.lock().unwrap().clone();
    let stderr_str = stderr_content.lock().unwrap().clone();

    let mut result = String::new();

    if timed_out {
        result.push_str(&format!("Command timed out after {}ms\n\n", timeout_ms));
    }

    if !stdout_str.is_empty() {
        result.push_str(&stdout_str);
    }

    if !stderr_str.is_empty() {
        if !result.is_empty() {
            result.push_str("\n\n");
        }
        result.push_str("stderr:\n");
        result.push_str(&stderr_str);
    }

    if let Some(code) = exit_code {
        if code != 0 {
            if !result.is_empty() {
                result.push_str("\n\n");
            }
            result.push_str(&format!("Exit code: {}", code));
        }
    }

    // Handle git commit if requested
    if let Some(ref commit_msg) = payload.commit_msg {
        // Only commit if command succeeded (exit code 0 or None)
        let command_succeeded = exit_code.is_none_or(|code| code == 0);

        if command_succeeded && !timed_out {
            let worktree_path = std::path::Path::new(cwd);
            match crate::mcp::git::git_commit_all(worktree_path, commit_msg) {
                Ok(commit_result) => {
                    // Emit worktree-changed event for diff tab updates
                    let _ = services.emitter.emit(
                        "worktree-changed",
                        serde_json::json!({"worktree_path": cwd}),
                    );

                    if !result.is_empty() {
                        result.push_str("\n\n");
                    }
                    let pr_suffix = commit_result
                        .pr_number
                        .map(|pr| format!(" updated PR#{}", pr))
                        .unwrap_or_default();
                    result.push_str(&format!(
                        "✓ Committed changes ({}){}",
                        commit_result.sha, pr_suffix
                    ));
                }
                Err(e) => {
                    // Don't fail the whole operation, just report the commit error
                    if !result.is_empty() {
                        result.push_str("\n\n");
                    }
                    result.push_str(&format!("⚠️ Failed to commit: {}", e));
                }
            }
        }
    } else {
        // Check if the command was a git commit and push if successful
        let worktree_path = std::path::Path::new(cwd);
        if is_git_commit_command(&payload.command) {
            let command_succeeded = exit_code.is_none_or(|code| code == 0);
            if command_succeeded && !timed_out {
                crate::mcp::git::push_to_origin(worktree_path);

                // Emit worktree-changed event for diff tab updates
                let _ = services.emitter.emit(
                    "worktree-changed",
                    serde_json::json!({"worktree_path": cwd}),
                );
            }
        }
    }

    if result.is_empty() {
        "(no output)".to_string()
    } else {
        // Truncate if too long
        if result.len() > 30000 {
            format!(
                "{}...\n\n(output truncated, {} total chars)",
                &result[..30000],
                result.len()
            )
        } else {
            result
        }
    }
}

/// Handle background bash execution via PTY terminal
async fn handle_background_bash(orch: &Orchestrator, cwd: &str, payload: &BashPayload) -> String {
    let services = &orch.services;

    // Look up run context (required for background commands)
    let run_context = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Database error: {}", e),
        };
        match lookup_run_by_cwd(&mut conn, cwd) {
            Ok(ctx) => ctx,
            Err(e) => return format!("Cannot run background command: {}", e),
        }
    };

    let pty_state = &orch.pty_state;

    let size = PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    };

    let pair = match services.pty_factory.create_pty(size) {
        Ok(p) => p,
        Err(e) => return format!("Failed to create PTY: {}", e),
    };

    // Get shell from env or default
    let shell_path = get_default_shell();

    let mut cmd = CommandBuilder::new(&shell_path);
    cmd.cwd(cwd);

    // Pass through environment
    for (key, value) in std::env::vars() {
        cmd.env(key, value);
    }

    // Override PATH with user's login shell PATH
    cmd.env("PATH", crate::env::get_user_path());

    // Spawn and get all components
    let components = match pair.spawn_and_split(cmd) {
        Ok(c) => c,
        Err(e) => return format!("Failed to spawn shell: {}", e),
    };

    let mut reader = components.reader;

    let session_id = Uuid::new_v4().to_string();

    // Create session with output buffer for late attachment
    let output_buffer: Arc<Mutex<VecDeque<u8>>> = Arc::new(Mutex::new(VecDeque::new()));
    let session = PtySession {
        master: components.master,
        writer: components.writer,
        child: components.child,
        output_buffer: Some(output_buffer.clone()),
        is_agent_spawned: true,
    };

    let session_arc = Arc::new(Mutex::new(session));

    // Store session
    {
        let mut sessions = match pty_state.sessions.lock() {
            Ok(s) => s,
            Err(e) => return format!("Failed to store session: {}", e),
        };
        sessions.insert(session_id.clone(), session_arc.clone());
    }

    // Generate slug and store in job_terminals table
    let slug = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Database error: {}", e),
        };

        let slug = generate_terminal_slug(
            &mut conn,
            Some(&run_context.job_id),
            None, // project_id - use job scope
            payload.terminal.as_deref(),
            payload.description.as_deref(),
            &payload.command,
        );

        let now = chrono::Utc::now().timestamp() as i32;
        let id = Uuid::new_v4().to_string();

        let new_terminal = NewJobTerminal {
            id: &id,
            job_id: Some(&run_context.job_id),
            project_id: None,
            run_id: Some(&run_context.run_id),
            session_id: &session_id,
            command: &payload.command,
            title: None, // Agent terminals use description, not title
            description: payload.description.as_deref(),
            status: "running",
            exit_code: None,
            created_at: now,
            exited_at: None,
            slug: Some(&slug),
        };

        let _ = diesel::insert_into(job_terminals::table)
            .values(&new_terminal)
            .execute(&mut *conn);

        let _ = services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "job_terminals", "action": "update"}),
        );

        slug
    };

    // Build the terminal URI using cairn:// scheme
    let uri = build_terminal_uri(
        &run_context.project_key,
        run_context.issue_number,
        run_context.job_name.as_deref(),
        &slug,
    );

    // Spawn reader thread
    let sid = session_id.clone();
    let orch_t = orch.clone();
    let emitter = services.emitter.clone();
    let buffer = output_buffer;
    let job_id = run_context.job_id.clone();
    let command_for_thread = payload.command.clone();

    thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => {
                    // EOF - process exited, try to get exit code
                    let exit_code = get_exit_code_from_session(&orch_t.pty_state, &sid);

                    let _ = emitter.emit(
                        "pty-exit",
                        serde_json::to_value(PtyExitPayload {
                            session_id: sid.clone(),
                            exit_code: exit_code.map(|c| c as i32),
                        })
                        .unwrap_or_default(),
                    );

                    // Collect info needed for injection while holding db lock
                    let injection_info: Option<(String, i32)> = {
                        if let Ok(mut conn) = orch_t.db.conn.lock() {
                            // Delete terminal record
                            let _ = diesel::delete(
                                job_terminals::table.filter(job_terminals::session_id.eq(&sid)),
                            )
                            .execute(&mut *conn);
                            let _ = emitter.emit(
                                "db-change",
                                serde_json::json!({"table": "job_terminals", "action": "delete"}),
                            );

                            // If non-zero exit, get claude_session_id for injection
                            if let Some(code) = exit_code {
                                if code != 0 {
                                    let claude_session_id: Option<String> = jobs::table
                                        .find(&job_id)
                                        .select(jobs::claude_session_id)
                                        .first::<Option<String>>(&mut *conn)
                                        .ok()
                                        .flatten();
                                    claude_session_id.map(|s| (s, code as i32))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    };

                    // Send terminal context OUTSIDE db lock to avoid deadlock
                    if let Some((claude_session_id, code)) = injection_info {
                        send_terminal_exit_context(
                            &orch_t,
                            &claude_session_id,
                            &sid,
                            code,
                            &buffer,
                            &command_for_thread,
                        );
                    }
                    break;
                }
                Ok(n) => {
                    // Emit event for frontend
                    let data = String::from_utf8_lossy(&buf[..n]).to_string();
                    let _ = emitter.emit(
                        "pty-data",
                        serde_json::to_value(PtyDataPayload {
                            session_id: sid.clone(),
                            data,
                        })
                        .unwrap_or_default(),
                    );

                    // Buffer for late attachment
                    {
                        let mut buf_guard = buffer.lock().unwrap();
                        buf_guard.extend(&buf[..n]);
                        // Keep buffer bounded
                        while buf_guard.len() > MAX_BUFFER_SIZE {
                            buf_guard.pop_front();
                        }
                    }
                }
                Err(e) => {
                    log::error!("Agent PTY read error: {}", e);
                    let _ = emitter.emit(
                        "pty-exit",
                        serde_json::to_value(PtyExitPayload {
                            session_id: sid.clone(),
                            exit_code: None,
                        })
                        .unwrap_or_default(),
                    );

                    // Delete terminal record
                    if let Ok(mut conn) = orch_t.db.conn.lock() {
                        let _ = diesel::delete(
                            job_terminals::table.filter(job_terminals::session_id.eq(&sid)),
                        )
                        .execute(&mut *conn);
                        let _ = emitter.emit(
                            "db-change",
                            serde_json::json!({"table": "job_terminals", "action": "delete"}),
                        );
                    }
                    break;
                }
            }
        }
    });

    // Send the command to the shell
    {
        let sessions = pty_state.sessions.lock().unwrap();
        if let Some(session) = sessions.get(&session_id) {
            if let Ok(mut session) = session.lock() {
                let cmd_with_newline = format!("{}\n", payload.command);
                let _ = session.writer.write_all(cmd_with_newline.as_bytes());
                let _ = session.writer.flush();
            }
        }
    }

    // Emit event for frontend to create terminal tab
    let _ = services.emitter.emit(
        "agent-terminal-created",
        serde_json::to_value(AgentTerminalCreatedPayload {
            run_id: run_context.run_id.clone(),
            session_id: session_id.clone(),
            command: payload.command.clone(),
            description: payload.description.clone(),
        })
        .unwrap_or_default(),
    );

    // Return immediately with URI for resource access
    serde_json::json!({
        "uri": uri,
        "message": format!("Background terminal started: {}", slug)
    })
    .to_string()
}

/// Get exit code from PTY session using non-blocking try_wait.
fn get_exit_code_from_session(
    pty_state: &crate::services::PtyState,
    session_id: &str,
) -> Option<u32> {
    let sessions = pty_state.sessions.lock().ok()?;
    let session_arc = sessions.get(session_id)?;
    let mut session = session_arc.lock().ok()?;

    // Use try_wait to avoid blocking - process should have exited if we got EOF
    match session.child.try_wait() {
        Ok(Some(status)) => Some(status.exit_code()),
        _ => None, // Not exited yet or error
    }
}

/// Send terminal context to Claude via stdin when a background process exits with error.
fn send_terminal_exit_context(
    orch: &Orchestrator,
    claude_session_id: &str,
    _terminal_session_id: &str,
    exit_code: i32,
    buffer: &Arc<Mutex<VecDeque<u8>>>,
    command: &str,
) {
    // Get recent output from buffer
    let output = {
        let buf_guard = buffer.lock().unwrap();
        let bytes: Vec<u8> = buf_guard.iter().copied().collect();
        String::from_utf8_lossy(&bytes).to_string()
    };

    // Truncate for display
    let cmd_display: String = command.chars().take(100).collect();
    let truncated_output: String = output.chars().take(2000).collect();

    let content = format!(
        "[Terminal Update] Background command '{}' exited with code {}:\n```\n{}\n```",
        cmd_display, exit_code, truncated_output
    );

    // Find the process and send via stdin
    let process_state = orch.process_state.clone();

    let run_id = match process_state.find_process_by_session(claude_session_id) {
        Some(rid) => rid,
        None => {
            log::info!(
                "No active process for session {}, skipping terminal context",
                &claude_session_id[..8.min(claude_session_id.len())]
            );
            return;
        }
    };

    let stdin_handle = match process_state.get_stdin_handle(&run_id) {
        Some(h) => h,
        None => {
            log::warn!("No stdin handle for run {}", &run_id[..8.min(run_id.len())]);
            return;
        }
    };

    let Ok(mut stdin_guard) = stdin_handle.lock() else {
        return;
    };

    if let Some(ref mut stdin) = *stdin_guard {
        // No image cache available in cairn-core context (Tauri-specific)
        match crate::claude::stdin::send_user_message_with_images(
            stdin,
            claude_session_id,
            &content,
            None,
            None,
            None,
        ) {
            Ok(()) => {
                log::info!(
                    "Sent terminal context to session {}: command='{}' exit_code={}",
                    &claude_session_id[..8.min(claude_session_id.len())],
                    cmd_display,
                    exit_code
                );
            }
            Err(e) => {
                log::warn!("Failed to send terminal context: {}", e);
            }
        }
    }
}

/// Cache a checkpoint command result if the executed command matches the job's checkpoint command.
/// This enables the checkpoint lookback optimization: when the programmatic checkpoint runs,
/// it can use this cached result instead of re-running the command.
fn cache_checkpoint_result(
    orch: &Orchestrator,
    job_id: &str,
    command: &str,
    cwd: &str,
    exit_code: Option<i32>,
) {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(_) => return,
    };

    // Check if this command matches the job's checkpoint command
    let checkpoint_cmd = match get_job_checkpoint_command(&mut conn, job_id) {
        Some(cmd) => cmd,
        None => return, // No checkpoint command configured for this job
    };

    let normalized_checkpoint = normalize_command(&checkpoint_cmd);
    let normalized_executed = normalize_command(command);

    if normalized_checkpoint != normalized_executed {
        return; // Command doesn't match checkpoint command
    }

    // Get git state for cache validity checking
    let commit_sha = match get_current_head_sha(cwd) {
        Ok(sha) => sha,
        Err(_) => return,
    };
    let is_dirty = is_worktree_dirty(cwd).unwrap_or(true);
    let now = chrono::Utc::now().timestamp() as i32;

    // Delete any existing cache for this job (only keep most recent)
    let _ = diesel::delete(
        checkpoint_command_cache::table.filter(checkpoint_command_cache::job_id.eq(job_id)),
    )
    .execute(&mut *conn);

    // Insert new cache entry
    let cache_entry = NewCheckpointCommandCache {
        id: &Uuid::new_v4().to_string(),
        job_id,
        command,
        normalized_command: &normalized_executed,
        exit_code: exit_code.unwrap_or(-1),
        commit_sha: &commit_sha,
        is_dirty: if is_dirty { 1 } else { 0 },
        ran_at: now,
        created_at: now,
    };

    if let Err(e) = diesel::insert_into(checkpoint_command_cache::table)
        .values(&cache_entry)
        .execute(&mut *conn)
    {
        log::warn!("Failed to cache checkpoint result: {}", e);
        return;
    }

    log::info!(
        "Cached checkpoint command result for job {}: exit={}, sha={}, dirty={}",
        &job_id[..8.min(job_id.len())],
        exit_code.unwrap_or(-1),
        &commit_sha[..7.min(commit_sha.len())],
        is_dirty
    );
}

/// Handle kill_shell tool call - kills a background terminal by slug or URI
pub async fn handle_kill_shell(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let services = &orch.services;
    let payload: KillShellPayloadInput = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            log::warn!("Invalid kill_shell payload: {}", e);
            return format!("Invalid payload: {}", e);
        }
    };

    log::info!("Killing terminal: {}", payload.terminal);

    // Extract slug from terminal input (could be slug or URI like "cairn://PROJ/ISSUE/slug")
    let slug = if payload.terminal.starts_with("cairn://") {
        // Parse URI to get slug (last component)
        payload
            .terminal
            .strip_prefix("cairn://")
            .and_then(|rest| rest.split('/').next_back())
            .map(|s| s.to_string())
            .unwrap_or_else(|| payload.terminal.clone())
    } else {
        payload.terminal.clone()
    };

    // Look up the run context to get job_id
    let run_context = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Database error: {}", e),
        };
        match lookup_run(&mut conn, request) {
            Ok(ctx) => ctx,
            Err(e) => return format!("No run context: {}", e),
        }
    };

    // Look up terminal by slug to get session_id
    let session_id = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Database error: {}", e),
        };

        match job_terminals::table
            .filter(job_terminals::job_id.eq(&run_context.job_id))
            .filter(job_terminals::slug.eq(&slug))
            .select(job_terminals::session_id)
            .first::<String>(&mut *conn)
        {
            Ok(sid) => sid,
            Err(_) => return format!("Terminal not found: {}", slug),
        }
    };

    log::info!("Found session {} for terminal {}", &session_id[..8], slug);

    // Remove session from in-memory state and kill process
    let mut sessions = match orch.pty_state.sessions.lock() {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to lock sessions: {}", e);
            return format!("Failed to access sessions: {}", e);
        }
    };

    if let Some(session_arc) = sessions.remove(&session_id) {
        // Kill the child process
        if let Ok(mut session) = session_arc.lock() {
            match session.child.kill() {
                Ok(_) => {
                    log::info!("Sent kill signal to terminal {}", slug);
                    let _ = session.child.wait(); // Wait to avoid zombies
                }
                Err(e) => {
                    log::warn!("Failed to kill process for terminal {}: {}", slug, e);
                }
            }
        }

        // Delete terminal record from database
        if let Ok(mut conn) = orch.db.conn.lock() {
            let delete_result = diesel::delete(
                job_terminals::table.filter(job_terminals::session_id.eq(&session_id)),
            )
            .execute(&mut *conn);

            match delete_result {
                Ok(_) => {
                    log::info!("Deleted terminal record for {}", slug);
                    let _ = services.emitter.emit(
                        "db-change",
                        serde_json::json!({"table": "job_terminals", "action": "delete"}),
                    );
                }
                Err(e) => log::warn!("Failed to delete terminal record: {}", e),
            }
        }

        format!("Successfully killed terminal: {}", slug)
    } else {
        log::warn!("Session not found in memory for terminal: {}", slug);
        format!("Terminal session not running: {}", slug)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_git_commit_command() {
        // Basic git commit commands
        assert!(is_git_commit_command("git commit -m 'msg'"));
        assert!(is_git_commit_command("git commit --amend"));
        assert!(is_git_commit_command("git commit -am 'msg'"));
        assert!(is_git_commit_command("git commit"));

        // Commands with leading operations
        assert!(is_git_commit_command("cd foo && git commit -m 'msg'"));
        assert!(is_git_commit_command("git add . && git commit -m 'msg'"));
        assert!(is_git_commit_command("git add -A; git commit -m 'msg'"));

        // Commands that should NOT match
        assert!(!is_git_commit_command("git status"));
        assert!(!is_git_commit_command("git push"));
        assert!(!is_git_commit_command("git log"));
        assert!(!is_git_commit_command("git diff"));
        assert!(!is_git_commit_command("git add ."));

        // Help commands should NOT match
        assert!(!is_git_commit_command("git commit --help"));
        assert!(!is_git_commit_command("git commit -h"));
        assert!(!is_git_commit_command("git commit -h | less"));
    }

    #[test]
    fn test_is_git_commit_command_edge_cases() {
        // Multiple spaces
        assert!(is_git_commit_command("git  commit -m 'msg'"));

        // Mixed case
        assert!(is_git_commit_command("GIT COMMIT -m 'msg'"));
        assert!(is_git_commit_command("Git Commit -m 'msg'"));

        // Newline separated
        assert!(is_git_commit_command("echo test\ngit commit -m 'msg'"));

        // Complex command chains
        assert!(is_git_commit_command(
            "cd /path/to/repo && git add . && git commit -m 'message'"
        ));
    }
}
