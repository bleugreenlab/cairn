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
use crate::orchestrator::Orchestrator;
use crate::schema::{issues, job_terminals, projects};

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
                let uri = build_terminal_uri(
                    &ctx.project_key,
                    ctx.issue_number,
                    ctx.job_name.as_deref(),
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
            // Node-level terminal: look up by job_id from run context
            let run_context = match lookup_run(&mut conn, request) {
                Ok(ctx) => ctx,
                Err(e) => {
                    return serde_json::to_string(&TerminalReadResult {
                        output: format!("No run context: {}", e),
                        status: "error".to_string(),
                        exit_code: None,
                        new_bytes: 0,
                        total_bytes: 0,
                    })
                    .unwrap_or_else(|_| "{}".to_string())
                }
            };

            job_terminals::table
                .filter(job_terminals::job_id.eq(&run_context.job_id))
                .filter(job_terminals::slug.eq(&parsed.slug))
                .select((
                    job_terminals::session_id,
                    job_terminals::status,
                    job_terminals::exit_code,
                ))
                .first::<(String, String, Option<i32>)>(&mut *conn)
                .ok()
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
#[allow(dead_code)] // project_key, issue_number, node_id reserved for future validation
struct ParsedTerminalUri {
    project_key: String,
    issue_number: Option<i32>,
    node_id: Option<String>,
    slug: String,
    full: bool,
}

/// Parse a terminal URI into components.
///
/// Supports both formats:
/// - `cairn://PROJECT/NUMBER/NODE/terminal/SLUG` (node-level terminal)
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
        // cairn://PROJECT/NUMBER/NODE/terminal/SLUG
        [project_key, number, node_id, "terminal", slug] => Some(ParsedTerminalUri {
            project_key: project_key.to_string(),
            issue_number: number.parse().ok(),
            node_id: Some(node_id.to_string()),
            slug: slug.to_string(),
            full,
        }),
        // cairn://PROJECT/terminal/SLUG
        [project_key, "terminal", slug] => Some(ParsedTerminalUri {
            project_key: project_key.to_string(),
            issue_number: None,
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
fn strip_ansi_sequences(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // ESC character - start of escape sequence
            if let Some(&next) = chars.peek() {
                match next {
                    '[' => {
                        // CSI sequence: ESC [ ... final_byte
                        chars.next(); // consume '['
                                      // Skip until we hit a letter (final byte of CSI)
                        while let Some(&ch) = chars.peek() {
                            chars.next();
                            if ch.is_ascii_alphabetic() || ch == '@' || ch == '`' {
                                break;
                            }
                        }
                    }
                    ']' => {
                        // OSC sequence: ESC ] ... (ST or BEL)
                        chars.next(); // consume ']'
                                      // Skip until ST (ESC \) or BEL (\x07)
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
                        // Character set designation - skip next 2 chars
                        chars.next();
                        chars.next();
                    }
                    _ => {
                        // Other escape - skip just the next char
                        chars.next();
                    }
                }
            }
        } else if c == '\r' {
            // Carriage return - skip (we'll keep newlines only)
            continue;
        } else if c.is_control() && c != '\n' && c != '\t' {
            // Skip other control characters except newline and tab
            continue;
        } else {
            result.push(c);
        }
    }

    // Clean up multiple consecutive newlines
    let mut cleaned = String::new();
    let mut prev_newline = false;
    for c in result.chars() {
        if c == '\n' {
            if !prev_newline {
                cleaned.push(c);
            }
            prev_newline = true;
        } else {
            prev_newline = false;
            cleaned.push(c);
        }
    }

    cleaned.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_terminal_uri_node_level() {
        let result = parse_terminal_uri("cairn://CAIRN/123/builder-1/terminal/dev-server").unwrap();
        assert_eq!(result.project_key, "CAIRN");
        assert_eq!(result.issue_number, Some(123));
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
            parse_terminal_uri("cairn://CAIRN/123/builder-1/terminal/dev-server?full=true")
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
    }

    #[test]
    fn test_build_terminal_uri_node_level() {
        let uri = build_terminal_uri("CAIRN", Some(123), Some("builder-1"), "dev-server");
        assert_eq!(uri, "cairn://CAIRN/123/builder-1/terminal/dev-server");
    }

    #[test]
    fn test_build_terminal_uri_project_level() {
        let uri = build_terminal_uri("CAIRN", None, None, "build");
        assert_eq!(uri, "cairn://CAIRN/terminal/build");
    }
}
