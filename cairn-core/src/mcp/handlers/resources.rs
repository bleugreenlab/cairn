//! MCP resource handlers for terminal and issue resources.
//!
//! Provides list_resources and read_resource handlers for terminal output and issue data.

use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

use super::lookup_run;
use super::slug::build_terminal_uri;
use crate::mcp::types::McpCallbackRequest;
use crate::node_segments::{matches_node_uri_segment, visible_node_segment};
use crate::orchestrator::Orchestrator;
use crate::schema::{executions, issues, job_terminals, jobs, projects};

/// Type alias for read cursor state: session_id -> bytes_read
/// Uses per-terminal cursor so reads "consume" output regardless of which run is reading.
pub type ReadCursorState = Mutex<HashMap<String, usize>>;

/// Terminal read result for read_resource response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalReadResult {
    /// New output since last read (or all output on first read)
    pub output: String,
    /// Terminal status: "running" or "exited"
    pub status: String,
    /// Exit code if terminal has exited
    pub exit_code: Option<i32>,
    /// Bytes returned in this read
    pub new_bytes: usize,
    /// Total bytes in terminal buffer
    pub total_bytes: usize,
}

/// Payload for read_resource request
#[derive(Debug, Clone, Deserialize)]
pub struct ReadResourcePayload {
    pub uri: String,
    /// If true, return full history instead of just new content since last read
    #[serde(default)]
    pub full: bool,
}

/// Resource info for list_resources response (used for both terminals and issues)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceInfo {
    pub uri: String,
    pub name: String,
    pub description: Option<String>,
}

/// Handle list_resources request - returns terminal resources for current job + recent issues
fn visible_terminal_node_segment(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
    node_name: Option<&str>,
) -> Option<String> {
    let recipe_node_id = jobs::table
        .find(job_id)
        .select(jobs::recipe_node_id)
        .first::<Option<String>>(conn)
        .ok()
        .flatten();

    visible_node_segment(recipe_node_id.as_deref(), node_name)
}

fn find_terminal_target_job_id(
    conn: &mut diesel::sqlite::SqliteConnection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_id: &str,
) -> Option<String> {
    let issue_id: String = issues::table
        .inner_join(projects::table)
        .filter(projects::key.eq(project_key))
        .filter(issues::number.eq(issue_number))
        .select(issues::id)
        .first(conn)
        .ok()?;

    let exec_id: String = executions::table
        .filter(executions::issue_id.eq(&issue_id))
        .filter(executions::seq.eq(exec_seq))
        .select(executions::id)
        .first(conn)
        .ok()?;

    let candidates: Vec<(String, Option<String>, Option<String>)> = jobs::table
        .filter(jobs::issue_id.eq(&issue_id))
        .filter(jobs::execution_id.eq(&exec_id))
        .select((jobs::id, jobs::node_name, jobs::recipe_node_id))
        .load(conn)
        .ok()?;

    candidates
        .into_iter()
        .find_map(|(job_id, stored_node_name, recipe_node_id)| {
            if matches_node_uri_segment(
                node_id,
                recipe_node_id.as_deref(),
                stored_node_name.as_deref(),
            ) {
                Some(job_id)
            } else {
                None
            }
        })
}

pub async fn handle_list_resources(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Database error: {}", e),
    };

    let mut resources: Vec<ResourceInfo> = Vec::new();

    // Try to get run context for terminal resources
    let run_context = lookup_run(&mut conn, request).ok();

    // 1. Terminal resources (if we have a run context)
    if let Some(ref ctx) = run_context {
        let terminals: Vec<(Option<String>, String, Option<String>, String)> = job_terminals::table
            .filter(job_terminals::job_id.eq(&ctx.job_id))
            .filter(job_terminals::slug.is_not_null())
            .select((
                job_terminals::slug,
                job_terminals::command,
                job_terminals::description,
                job_terminals::status,
            ))
            .load(&mut *conn)
            .unwrap_or_default();

        for (slug, command, description, status) in terminals {
            if let Some(slug) = slug {
                let node_segment =
                    visible_terminal_node_segment(&mut *conn, &ctx.job_id, ctx.job_name.as_deref());
                let uri = build_terminal_uri(
                    &ctx.project_key,
                    ctx.issue_number,
                    ctx.exec_seq,
                    node_segment.as_deref(),
                    &slug,
                );
                let name = description.clone().unwrap_or_else(|| {
                    if command.len() > 30 {
                        format!("{}...", &command[..30])
                    } else {
                        command
                    }
                });
                resources.push(ResourceInfo {
                    uri,
                    name,
                    description: Some(format!("Terminal ({})", status)),
                });
            }
        }
    }

    // 2. Issue resources - get recent active/waiting issues from current project
    let project_id = run_context
        .as_ref()
        .map(|ctx| ctx.project_id.clone())
        .or_else(|| {
            // Try to get project from cwd
            let cwd = &request.cwd;
            projects::table
                .filter(projects::repo_path.eq(cwd))
                .select(projects::id)
                .first::<String>(&mut *conn)
                .ok()
        });

    if let Some(project_id) = project_id {
        // Get project key for URI
        let project_key: Option<String> = projects::table
            .filter(projects::id.eq(&project_id))
            .select(projects::key)
            .first(&mut *conn)
            .ok();

        if let Some(project_key) = project_key {
            // Get recent issues (active/waiting first, then by updated_at)
            // Limit to 20 most recent
            let issue_rows: Vec<(i32, String, String)> = issues::table
                .filter(issues::project_id.eq(&project_id))
                .order((
                    diesel::dsl::sql::<diesel::sql_types::Integer>(
                        "CASE status WHEN 'Active' THEN 0 WHEN 'Waiting' THEN 1 ELSE 2 END",
                    ),
                    issues::updated_at.desc(),
                ))
                .limit(20)
                .select((issues::number, issues::title, issues::status))
                .load(&mut *conn)
                .unwrap_or_default();

            for (number, title, status) in issue_rows {
                resources.push(ResourceInfo {
                    uri: format!("cairn://{}/{}", project_key, number),
                    name: format!("{}-{}", project_key, number),
                    description: Some(format!("[{}] {}", status, title)),
                });
            }
        }
    }

    serde_json::to_string(&resources).unwrap_or_else(|_| "[]".to_string())
}

/// Handle read_resource request - returns terminal output for a given URI
///
/// Uses cursor-based reading: each read returns only new content since the last read.
/// The cursor is tracked per (run_id, session_id) so different runs see full history.
pub async fn handle_read_resource(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    read_cursors: &ReadCursorState,
) -> String {
    let payload: ReadResourcePayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!("read_resource: uri={}", payload.uri);

    // Parse the URI to extract slug and query params
    let parsed = match parse_terminal_uri(&payload.uri) {
        Some(p) => p,
        None => {
            return serde_json::to_string(&TerminalReadResult {
                output: format!("Invalid terminal URI: {}", payload.uri),
                status: "error".to_string(),
                exit_code: None,
                new_bytes: 0,
                total_bytes: 0,
            })
            .unwrap_or_else(|_| "{}".to_string())
        }
    };

    // Full history if requested via payload OR query param
    let full = payload.full || parsed.full;

    // Look up the terminal by slug
    // For project-level terminals (node_id is None), look up by project_id
    // For node-level terminals, look up by job_id from run context
    let terminal_info = {
        let mut conn = match orch.db.conn.lock() {
            Ok(c) => c,
            Err(e) => return format!("Database error: {}", e),
        };

        if parsed.node_id.is_none() {
            // Project-level terminal: look up by project_key
            let project_id: Option<String> = projects::table
                .filter(projects::key.eq(&parsed.project_key))
                .select(projects::id)
                .first(&mut *conn)
                .ok();

            match project_id {
                Some(pid) => job_terminals::table
                    .filter(job_terminals::project_id.eq(&pid))
                    .filter(job_terminals::job_id.is_null())
                    .filter(job_terminals::slug.eq(&parsed.slug))
                    .select((
                        job_terminals::session_id,
                        job_terminals::status,
                        job_terminals::exit_code,
                    ))
                    .first::<(String, String, Option<i32>)>(&mut *conn)
                    .ok(),
                None => None,
            }
        } else {
            // Node-level terminal: resolve target job from parsed URI components
            // (not the caller's run context — any agent can read any terminal by URI)
            let target_job_id = (|| -> Option<String> {
                let issue_number = parsed.issue_number?;
                let exec_seq = parsed.exec_seq?;
                let node_id = parsed.node_id.as_deref()?;

                find_terminal_target_job_id(
                    &mut *conn,
                    &parsed.project_key,
                    issue_number,
                    exec_seq,
                    node_id,
                )
            })();

            match target_job_id {
                Some(job_id) => job_terminals::table
                    .filter(job_terminals::job_id.eq(&job_id))
                    .filter(job_terminals::slug.eq(&parsed.slug))
                    .select((
                        job_terminals::session_id,
                        job_terminals::status,
                        job_terminals::exit_code,
                    ))
                    .first::<(String, String, Option<i32>)>(&mut *conn)
                    .ok(),
                None => None,
            }
        }
    };

    let (session_id, status, exit_code) = match terminal_info {
        Some(info) => info,
        None => {
            return serde_json::to_string(&TerminalReadResult {
                output: format!("Terminal not found: {}", parsed.slug),
                status: "error".to_string(),
                exit_code: None,
                new_bytes: 0,
                total_bytes: 0,
            })
            .unwrap_or_else(|_| "{}".to_string())
        }
    };

    // Get buffer content with cursor-based reading (consumed on read)
    // If full=true, return complete history; otherwise return only new content
    let (output, new_bytes, total_bytes) =
        get_buffer_content_from_cursor(orch, read_cursors, &session_id, &status, full);

    serde_json::to_string(&TerminalReadResult {
        output,
        status,
        exit_code,
        new_bytes,
        total_bytes,
    })
    .unwrap_or_else(|_| "{}".to_string())
}

/// Parsed terminal URI components
struct ParsedTerminalUri {
    project_key: String,
    issue_number: Option<i32>,
    exec_seq: Option<i32>,
    node_id: Option<String>,
    slug: String,
    full: bool,
}

/// Parse a terminal URI into components.
///
/// Supports formats:
/// - `cairn://PROJECT/NUMBER/EXEC/NODE/terminal/SLUG` (node-level terminal, canonical)
/// - `cairn://PROJECT/terminal/SLUG` (project-level terminal)
///
/// Query param: `?full=true` for complete history
fn parse_terminal_uri(uri: &str) -> Option<ParsedTerminalUri> {
    // Split off query string if present
    let (uri_path, query) = if let Some(idx) = uri.find('?') {
        (&uri[..idx], Some(&uri[idx + 1..]))
    } else {
        (uri, None)
    };

    let path = uri_path.strip_prefix("cairn://")?;
    let parts: Vec<&str> = path.split('/').collect();

    // Parse query params for full=true
    let full = query
        .map(|q| q.split('&').any(|p| p == "full=true" || p == "full=1"))
        .unwrap_or(false);

    match parts.as_slice() {
        // cairn://PROJECT/NUMBER/EXEC/NODE/terminal/SLUG (canonical format with exec_seq)
        [project_key, number, exec_seq_str, node_id, "terminal", slug]
            if exec_seq_str.parse::<i32>().is_ok() =>
        {
            Some(ParsedTerminalUri {
                project_key: project_key.to_string(),
                issue_number: number.parse().ok(),
                exec_seq: exec_seq_str.parse().ok(),
                node_id: Some(node_id.to_string()),
                slug: slug.to_string(),
                full,
            })
        }
        // cairn://PROJECT/terminal/SLUG (project-level)
        [project_key, "terminal", slug] => Some(ParsedTerminalUri {
            project_key: project_key.to_string(),
            issue_number: None,
            exec_seq: None,
            node_id: None,
            slug: slug.to_string(),
            full,
        }),
        _ => None,
    }
}

/// Get buffer content from cursor position, updating cursor after read.
/// Returns (output, new_bytes, total_bytes).
/// Uses per-terminal cursor so reads "consume" output regardless of which run is reading.
/// If `full` is true, returns complete history from the start (ignores cursor).
fn get_buffer_content_from_cursor(
    orch: &Orchestrator,
    read_cursors: &ReadCursorState,
    session_id: &str,
    status: &str,
    full: bool,
) -> (String, usize, usize) {
    // Try to get buffer content
    let buffer_result = (|| {
        let sessions = orch.pty_state.sessions.lock().ok()?;
        let session_arc = sessions.get(session_id)?;
        let session = session_arc.lock().ok()?;
        let buffer = session.output_buffer.as_ref()?;
        let buf_guard = buffer.lock().ok()?;
        let bytes: Vec<u8> = buf_guard.iter().copied().collect();
        Some(bytes)
    })();

    let bytes = match buffer_result {
        Some(b) => b,
        None => {
            let msg = if status == "running" {
                "(no output yet)"
            } else {
                "(terminal ended - no buffered output)"
            };
            return (msg.to_string(), 0, 0);
        }
    };

    let total_bytes = bytes.len();

    // If full=true, read from start; otherwise use cursor position
    let cursor_pos = if full {
        0
    } else {
        let cursors = read_cursors.lock().unwrap_or_else(|e| e.into_inner());
        *cursors.get(session_id).unwrap_or(&0)
    };

    log::info!(
        "read_resource cursor: session_id={}, cursor_pos={}, total_bytes={}, full={}",
        &session_id[..session_id.len().min(8)],
        cursor_pos,
        total_bytes,
        full
    );

    // Slice from cursor position to get content
    let new_bytes = total_bytes.saturating_sub(cursor_pos);

    let output = if cursor_pos < total_bytes {
        let raw = String::from_utf8_lossy(&bytes[cursor_pos..]).to_string();
        // Strip ANSI escape sequences for cleaner agent consumption
        strip_ansi_sequences(&raw)
    } else {
        "(no new output)".to_string()
    };

    // Update cursor to current buffer length (even for full reads, to mark as "seen")
    {
        let mut cursors = read_cursors.lock().unwrap_or_else(|e| e.into_inner());
        cursors.insert(session_id.to_string(), total_bytes);
    }

    (output, new_bytes, total_bytes)
}

/// Strip ANSI escape sequences from terminal output for cleaner agent consumption.
/// Removes color codes, cursor movement, and other control sequences.
/// Process raw PTY output into clean text by emulating basic terminal semantics.
///
/// Handles: ANSI escape sequences, carriage return (line overwrite),
/// backspace, and other control characters. Produces the "screen content"
/// an interactive terminal would display, not the raw byte stream.
fn strip_ansi_sequences(input: &str) -> String {
    // We emulate a simple terminal: a list of completed lines plus a current line buffer.
    let mut lines: Vec<String> = Vec::new();
    let mut line: Vec<char> = Vec::new();
    let mut cursor: usize = 0; // cursor position within current line
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\x1b' => {
                // ESC - start of escape sequence
                if let Some(&next) = chars.peek() {
                    match next {
                        '[' => {
                            // CSI sequence: ESC [ (params) final_byte
                            chars.next(); // consume '['
                                          // Collect parameter bytes
                            let mut params = String::new();
                            while let Some(&ch) = chars.peek() {
                                if ch.is_ascii_alphabetic() || ch == '@' || ch == '`' {
                                    break;
                                }
                                params.push(ch);
                                chars.next();
                            }
                            // Consume final byte
                            let final_byte = chars.next();
                            // Handle cursor movement CSI sequences
                            match final_byte {
                                Some('K') => {
                                    // Erase in Line
                                    let n: usize = params.parse().unwrap_or(0);
                                    match n {
                                        0 => line.truncate(cursor), // erase to end
                                        1 => {
                                            // erase to start
                                            for i in 0..cursor.min(line.len()) {
                                                line[i] = ' ';
                                            }
                                        }
                                        2 => {
                                            line.clear();
                                            cursor = 0;
                                        }
                                        _ => {}
                                    }
                                }
                                Some('C') => {
                                    // Cursor Forward
                                    let n: usize = params.parse().unwrap_or(1).max(1);
                                    cursor = (cursor + n).min(line.len());
                                }
                                Some('D') => {
                                    // Cursor Back
                                    let n: usize = params.parse().unwrap_or(1).max(1);
                                    cursor = cursor.saturating_sub(n);
                                }
                                _ => {
                                    // Other CSI sequences (colors, etc.) — skip
                                }
                            }
                        }
                        ']' => {
                            // OSC sequence: ESC ] ... (ST or BEL)
                            chars.next();
                            while let Some(ch) = chars.next() {
                                if ch == '\x07' {
                                    break;
                                }
                                if ch == '\x1b' && chars.peek() == Some(&'\\') {
                                    chars.next();
                                    break;
                                }
                            }
                        }
                        '(' | ')' | '*' | '+' => {
                            chars.next();
                            chars.next();
                        }
                        _ => {
                            chars.next();
                        }
                    }
                }
            }
            '\n' => {
                // Newline — commit current line and start fresh
                lines.push(line.iter().collect());
                line.clear();
                cursor = 0;
            }
            '\r' => {
                // Carriage return: if followed by \n it's a line ending (cursor to 0,
                // \n will commit). Otherwise it's a line rewrite — clear the old content
                // so only the new text remains (matches user intent for progress bars,
                // prompt rewrites, etc.)
                if chars.peek() == Some(&'\n') {
                    cursor = 0;
                } else {
                    line.clear();
                    cursor = 0;
                }
            }
            '\x08' => {
                // Backspace — move cursor back one position
                cursor = cursor.saturating_sub(1);
            }
            '\x7f' => {
                // DEL — backspace + delete character
                if cursor > 0 {
                    cursor -= 1;
                    if cursor < line.len() {
                        line.remove(cursor);
                    }
                }
            }
            _ if c.is_control() && c != '\t' => {
                // Skip other control characters (except tab)
            }
            _ => {
                // Printable character — write at cursor position
                if cursor < line.len() {
                    line[cursor] = c;
                } else {
                    // Extend line to cursor position if needed
                    while line.len() < cursor {
                        line.push(' ');
                    }
                    line.push(c);
                }
                cursor += 1;
            }
        }
    }

    // Don't forget the last line if non-empty
    if !line.is_empty() {
        lines.push(line.iter().collect());
    }

    // Clean up: trim trailing whitespace per line, collapse consecutive blank lines
    let cleaned: Vec<&str> = lines.iter().map(|l| l.trim_end()).collect();

    // Collapse runs of blank lines into a single blank line
    let mut result = String::new();
    let mut prev_blank = false;
    for line in &cleaned {
        if line.is_empty() {
            if !prev_blank && !result.is_empty() {
                result.push('\n');
            }
            prev_blank = true;
        } else {
            if !result.is_empty() {
                result.push('\n');
            }
            result.push_str(line);
            prev_blank = false;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::diesel_models::{NewExecution, NewJob, NewJobTerminal};
    use crate::orchestrator::Orchestrator;
    use crate::services::testing::TestServicesBuilder;
    use crate::test_utils::{create_test_issue, create_test_project, test_diesel_conn};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

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

    fn insert_issue_job_with_terminal(
        conn: &mut diesel::sqlite::SqliteConnection,
        issue_id: &str,
        project_id: &str,
        recipe_node_id: &str,
        node_name: Option<&str>,
        slug: &str,
    ) -> String {
        let execution_id = Uuid::new_v4().to_string();
        let job_id = Uuid::new_v4().to_string();
        let terminal_id = Uuid::new_v4().to_string();
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

        diesel::insert_into(jobs::table)
            .values(&NewJob {
                id: &job_id,
                execution_id: Some(&execution_id),
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
                node_name,
                base_branch: None,
                current_turn_id: None,
            })
            .execute(conn)
            .unwrap();

        diesel::insert_into(job_terminals::table)
            .values(&NewJobTerminal {
                id: &terminal_id,
                job_id: Some(&job_id),
                project_id: None,
                run_id: None,
                session_id: "session-1",
                command: "cargo test",
                title: None,
                description: Some("Test terminal"),
                status: "running",
                exit_code: None,
                created_at: now,
                exited_at: None,
                slug: Some(slug),
            })
            .execute(conn)
            .unwrap();

        job_id
    }

    #[test]
    fn test_parse_terminal_uri_node_level() {
        // Canonical 6-segment format: PROJECT/NUMBER/EXEC/NODE/terminal/SLUG
        let result =
            parse_terminal_uri("cairn://CAIRN/123/1/builder-1/terminal/dev-server").unwrap();
        assert_eq!(result.project_key, "CAIRN");
        assert_eq!(result.issue_number, Some(123));
        assert_eq!(result.exec_seq, Some(1));
        assert_eq!(result.node_id, Some("builder-1".to_string()));
        assert_eq!(result.slug, "dev-server");
        assert!(!result.full);
    }

    #[test]
    fn test_parse_terminal_uri_project_level() {
        let result = parse_terminal_uri("cairn://CAIRN/terminal/build").unwrap();
        assert_eq!(result.project_key, "CAIRN");
        assert_eq!(result.issue_number, None);
        assert_eq!(result.node_id, None);
        assert_eq!(result.slug, "build");
        assert!(!result.full);
    }

    #[test]
    fn test_parse_terminal_uri_with_full_flag() {
        let result =
            parse_terminal_uri("cairn://CAIRN/123/1/builder-1/terminal/dev-server?full=true")
                .unwrap();
        assert_eq!(result.slug, "dev-server");
        assert!(result.full);

        let result2 = parse_terminal_uri("cairn://CAIRN/terminal/build?full=1").unwrap();
        assert!(result2.full);
    }

    #[test]
    fn test_parse_terminal_uri_invalid() {
        assert!(parse_terminal_uri("file:///test.txt").is_none());
        assert!(parse_terminal_uri("cairn://CAIRN/slug").is_none());
        assert!(parse_terminal_uri("invalid").is_none());
        // Old terminal:// scheme no longer supported
        assert!(parse_terminal_uri("terminal://CAIRN/main/build").is_none());
        // 5-segment format without exec_seq is not valid
        assert!(parse_terminal_uri("cairn://CAIRN/123/builder-1/terminal/dev-server").is_none());
        // 6-segment with non-numeric exec_seq is not valid
        assert!(
            parse_terminal_uri("cairn://CAIRN/123/abc/builder-1/terminal/dev-server").is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_read_resource_accepts_legacy_recipe_node_id_terminal_uri() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "CAIRN");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test Issue");
        let recipe_node_id = "54e54f2d-4ff1-45c5-ad0e-5e5f5846ea67";
        insert_issue_job_with_terminal(
            &mut conn,
            &issue_id,
            &project_id,
            recipe_node_id,
            None,
            "dev-server",
        );

        let orch = test_orchestrator(conn);
        let read_cursors = ReadCursorState::default();
        let request = crate::mcp::types::McpCallbackRequest {
            cwd: "/tmp/test-repo".to_string(),
            run_id: None,
            tool: "read_resource".to_string(),
            payload: serde_json::json!({
                "uri": format!(
                    "cairn://CAIRN/1/1/{}/terminal/dev-server",
                    recipe_node_id
                )
            }),
            tool_use_id: None,
        };

        let response = handle_read_resource(&orch, &request, &read_cursors).await;
        let parsed: TerminalReadResult = serde_json::from_str(&response).unwrap();

        assert_eq!(parsed.status, "running");
        assert_ne!(parsed.output, "Terminal not found: dev-server");
    }

    #[test]
    fn test_build_terminal_uri_node_level() {
        let uri = build_terminal_uri("CAIRN", Some(123), Some(1), Some("builder-1"), "dev-server");
        assert_eq!(uri, "cairn://CAIRN/123/1/builder-1/terminal/dev-server");
    }

    #[test]
    fn test_build_terminal_uri_project_level() {
        let uri = build_terminal_uri("CAIRN", None, None, None, "build");
        assert_eq!(uri, "cairn://CAIRN/terminal/build");
    }

    #[test]
    fn test_build_terminal_uri_missing_exec_seq_falls_back_to_project() {
        // When issue_number exists but exec_seq is None, URI drops to project-level
        let uri = build_terminal_uri("CAIRN", Some(123), None, Some("builder-1"), "dev-server");
        assert_eq!(uri, "cairn://CAIRN/terminal/dev-server");
    }

    #[test]
    fn test_build_terminal_uri_missing_node_falls_back_to_project() {
        // When issue_number and exec_seq exist but node_id is None
        let uri = build_terminal_uri("CAIRN", Some(123), Some(1), None, "dev-server");
        assert_eq!(uri, "cairn://CAIRN/terminal/dev-server");
    }

    #[test]
    fn test_strip_ansi_plain_text() {
        assert_eq!(strip_ansi_sequences("hello world"), "hello world");
    }

    #[test]
    fn test_strip_ansi_color_codes() {
        assert_eq!(
            strip_ansi_sequences("\x1b[32mgreen\x1b[0m text"),
            "green text"
        );
    }

    #[test]
    fn test_strip_ansi_carriage_return_overwrite() {
        // Bare \r (not followed by \n) clears the line — only new content remains
        assert_eq!(strip_ansi_sequences("loading...\rdone!"), "done!");
        // Multiple rewrites — only final version kept
        assert_eq!(strip_ansi_sequences("10%\r50%\r100%"), "100%");
    }

    #[test]
    fn test_strip_ansi_cr_lf_sequence() {
        // Normal \r\n line endings should produce clean lines
        assert_eq!(strip_ansi_sequences("line1\r\nline2\r\n"), "line1\nline2");
    }

    #[test]
    fn test_strip_ansi_backspace() {
        // "abc" then 2 backspaces then "xy" → "axy"
        assert_eq!(strip_ansi_sequences("abc\x08\x08xy"), "axy");
    }

    #[test]
    fn test_strip_ansi_autocomplete_simulation() {
        // Simulates: user types "ca", autocomplete overwrites with "cargo build"
        // PTY output: "ca" then \r to go back, then full "cargo build" overwrites
        assert_eq!(
            strip_ansi_sequences("$ ca\r$ cargo build\n"),
            "$ cargo build"
        );
    }

    #[test]
    fn test_strip_ansi_erase_line() {
        // ESC[K erases from cursor to end of line
        assert_eq!(strip_ansi_sequences("hello world\r$ cmd\x1b[K\n"), "$ cmd");
    }

    #[test]
    fn test_strip_ansi_osc_sequences() {
        // OSC title-setting sequences should be stripped
        assert_eq!(strip_ansi_sequences("\x1b]0;my-title\x07$ ls\n"), "$ ls");
    }
}
