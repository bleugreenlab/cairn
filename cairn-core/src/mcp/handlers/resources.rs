//! MCP resource handlers for terminal and issue resources.
//!
//! Provides the read_resource handler for terminal output.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

use crate::mcp::types::McpCallbackRequest;

use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_common::query::split_target_query;
use cairn_common::uri::{parse_uri, CairnResource};
use turso::params;

/// Type alias for read cursor state: session_id -> bytes_read
/// Uses per-terminal cursor so reads "consume" output regardless of which run is reading.
pub type ReadCursorState = Mutex<HashMap<String, usize>>;

/// Terminal read result for read_resource response.
///
/// `output` carries a one-line status banner followed by the rendered buffer.
/// The structured status fields mirror the banner so callers can branch on them
/// without parsing text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalReadResult {
    /// Status banner plus rendered terminal output (the full buffer by default,
    /// or only the bytes appended since the cursor when read with `?new=true`).
    pub output: String,
    /// Terminal status: "running", "exited", or "error".
    pub status: String,
    /// Exit code if the terminal has exited.
    pub exit_code: Option<i32>,
    /// Bytes returned in this read.
    pub new_bytes: usize,
    /// Total bytes in the terminal buffer.
    pub total_bytes: usize,
    /// Seconds the terminal has been (or was) running, measured from spawn time.
    #[serde(default)]
    pub runtime_secs: Option<i64>,
    /// Seconds since the most recent output byte; `None` when there has been no
    /// output (or the live session is gone).
    #[serde(default)]
    pub idle_secs: Option<i64>,
}

/// Payload for read_resource request. Terminal-specific params (`new`, `full`,
/// `annotations`) ride in the URI query string and are parsed there; any extra
/// JSON fields are ignored, so an older client sending `full` still deserializes.
#[derive(Debug, Clone, Deserialize)]
pub struct ReadResourcePayload {
    pub uri: String,
}

async fn find_terminal_target_job_id(
    conn: &turso::Connection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    node_id: &str,
) -> DbResult<Option<String>> {
    let lookup_key = project_key.to_uppercase();
    let mut issue_rows = conn
        .query(
            "
            SELECT i.id
            FROM issues i
            JOIN projects p ON i.project_id = p.id
            WHERE p.key = ?1 AND i.number = ?2
            LIMIT 1
            ",
            params![lookup_key.as_str(), issue_number],
        )
        .await?;

    let Some(issue_row) = issue_rows.next().await? else {
        return Ok(None);
    };
    let issue_id = issue_row.text(0)?;

    let mut exec_rows = conn
        .query(
            "
            SELECT id
            FROM executions
            WHERE issue_id = ?1 AND seq = ?2
            LIMIT 1
            ",
            params![issue_id.as_str(), exec_seq],
        )
        .await?;

    let Some(exec_row) = exec_rows.next().await? else {
        return Ok(None);
    };
    let exec_id = exec_row.text(0)?;

    let mut exact_rows = conn
        .query(
            "
            SELECT id
            FROM jobs
            WHERE issue_id = ?1
              AND execution_id = ?2
              AND parent_job_id IS NULL
              AND uri_segment = ?3
            LIMIT 1
            ",
            params![issue_id.as_str(), exec_id.as_str(), node_id],
        )
        .await?;
    if let Some(row) = exact_rows.next().await? {
        return Ok(Some(row.text(0)?));
    }

    Ok(None)
}

async fn find_task_terminal_target_job_id(
    conn: &turso::Connection,
    project_key: &str,
    issue_number: i32,
    exec_seq: i32,
    parent_node_id: &str,
    task_name: &str,
) -> DbResult<Option<String>> {
    let lookup_key = project_key.to_uppercase();
    let mut rows = conn
        .query(
            "
            SELECT child.id
            FROM jobs parent
            JOIN jobs child ON child.parent_job_id = parent.id
            JOIN issues i ON parent.issue_id = i.id
            JOIN projects p ON i.project_id = p.id
            JOIN executions e ON parent.execution_id = e.id
            WHERE p.key = ?1
              AND i.number = ?2
              AND e.seq = ?3
              AND parent.parent_job_id IS NULL
              AND parent.uri_segment = ?4
              AND child.uri_segment = ?5
            LIMIT 1
            ",
            params![
                lookup_key.as_str(),
                issue_number,
                exec_seq,
                parent_node_id,
                task_name
            ],
        )
        .await?;
    rows.next().await?.map(|row| row.text(0)).transpose()
}

/// Handle read_resource request - returns terminal output for a given URI.
///
/// Reads are idempotent by default: a plain read returns the full buffer and
/// does NOT advance the per-terminal cursor, so repeated reads are stable.
/// `?new=true` switches to incremental mode, returning only the bytes appended
/// since the last `new=true` read and advancing the cursor.
pub async fn handle_read_resource(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    read_cursors: &ReadCursorState,
) -> String {
    let payload: ReadResourcePayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

    log::info!("read_resource: uri={}", payload.uri);

    let parsed = match parse_terminal_uri(&payload.uri) {
        Ok(parsed) => parsed,
        Err(message) => return terminal_error_json(message),
    };

    let consume = parsed.new;

    let info = match lookup_terminal_info(&orch.db.local, &parsed).await {
        Ok(Some(info)) => info,
        Ok(None) => {
            let slugs = list_terminal_slugs_in_scope(&orch.db.local, &parsed)
                .await
                .unwrap_or_default();
            return terminal_error_json(not_found_message(&parsed.slug, &slugs));
        }
        Err(error) => {
            return terminal_error_json(format!(
                "Failed to look up terminal '{}': {error}",
                parsed.slug
            ))
        }
    };

    let (output, new_bytes, total_bytes, idle_secs) = get_buffer_content(
        orch,
        read_cursors,
        &info.session_id,
        &info.status,
        consume,
        info.output_tail.as_deref(),
    );

    let runtime_secs = {
        let end = info.exited_at.unwrap_or_else(unix_now);
        Some((end - info.created_at).max(0))
    };

    let banner = format_status_banner(
        &parsed.slug,
        &info.status,
        info.exit_code,
        runtime_secs,
        idle_secs,
        total_bytes,
    );
    let mut body = format!("{banner}\n\n{output}");

    if !parsed.annotations_suppressed {
        append_inline_annotations(orch, &mut body, &parsed.anchor_uri).await;
    }

    serde_json::to_string(&TerminalReadResult {
        output: body,
        status: info.status,
        exit_code: info.exit_code,
        new_bytes,
        total_bytes,
        runtime_secs,
        idle_secs,
    })
    .unwrap_or_else(|_| "{}".to_string())
}

fn terminal_error_json(message: String) -> String {
    serde_json::to_string(&TerminalReadResult {
        output: message,
        status: "error".to_string(),
        exit_code: None,
        new_bytes: 0,
        total_bytes: 0,
        runtime_secs: None,
        idle_secs: None,
    })
    .unwrap_or_else(|_| "{}".to_string())
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Human-readable status banner shown as the first line of a terminal read.
/// Distinguishes "running but quiet" (running + age of last output) from "dead"
/// (exited + code) so a polling agent has something actionable.
fn format_status_banner(
    slug: &str,
    status: &str,
    exit_code: Option<i32>,
    runtime_secs: Option<i64>,
    idle_secs: Option<i64>,
    total_bytes: usize,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    if status == "running" {
        parts.push("running".to_string());
        if let Some(rt) = runtime_secs {
            parts.push(format!("{} elapsed", fmt_duration(rt)));
        }
        if total_bytes == 0 {
            parts.push("no output yet".to_string());
        } else if let Some(idle) = idle_secs {
            parts.push(format!("last output {} ago", fmt_duration(idle)));
        }
    } else {
        match exit_code {
            Some(code) => parts.push(format!("exited code {code}")),
            None => parts.push(status.to_string()),
        }
        if let Some(rt) = runtime_secs {
            parts.push(format!("ran {}", fmt_duration(rt)));
        }
    }
    format!("[terminal {slug}: {}]", parts.join(", "))
}

/// Compact duration: `45s`, `2m05s`, `1h03m`.
fn fmt_duration(secs: i64) -> String {
    let s = secs.max(0);
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    }
}

fn not_found_message(slug: &str, slugs: &[String]) -> String {
    if slugs.is_empty() {
        format!(
            "Terminal '{slug}' not found; no terminals exist in this scope. \
             Create one with a write (mode=create) on this URI."
        )
    } else {
        format!(
            "Terminal '{slug}' not found. Existing terminals in this scope: {}.",
            slugs.join(", ")
        )
    }
}

#[derive(Debug, Clone)]
struct TerminalInfo {
    session_id: String,
    status: String,
    exit_code: Option<i32>,
    created_at: i64,
    exited_at: Option<i64>,
    /// Retained tail of the buffer for an exited terminal whose live PTY session
    /// is gone, so a post-exit read still returns its final output.
    output_tail: Option<String>,
}

async fn lookup_terminal_info(
    db: &LocalDb,
    parsed: &ParsedTerminalUri,
) -> DbResult<Option<TerminalInfo>> {
    let project_key = parsed.project_key.clone();
    let slug = parsed.slug.clone();
    let issue_number = parsed.issue_number;
    let exec_seq = parsed.exec_seq;
    let node_id = parsed.node_id.clone();
    let task_name = parsed.task_name.clone();

    db.read(|conn| {
        let project_key = project_key.clone();
        let slug = slug.clone();
        let node_id = node_id.clone();
        let task_name = task_name.clone();
        Box::pin(async move {
            let row = if node_id.is_none() {
                let lookup_key = project_key.to_uppercase();
                let mut rows = conn
                    .query(
                        "
                        SELECT jt.session_id, jt.status, jt.exit_code, jt.created_at, jt.exited_at, jt.output_tail
                        FROM job_terminals jt
                        JOIN projects p ON jt.project_id = p.id
                        WHERE p.key = ?1
                          AND jt.job_id IS NULL
                          AND jt.slug = ?2
                        LIMIT 1
                        ",
                        params![lookup_key.as_str(), slug.as_str()],
                    )
                    .await?;
                rows.next().await?
            } else {
                let (Some(issue_number), Some(exec_seq), Some(node_id)) =
                    (issue_number, exec_seq, node_id.as_deref())
                else {
                    return Ok(None);
                };
                let job_id = if let Some(task_name) = task_name.as_deref() {
                    find_task_terminal_target_job_id(
                        conn,
                        &project_key,
                        issue_number,
                        exec_seq,
                        node_id,
                        task_name,
                    )
                    .await?
                } else {
                    find_terminal_target_job_id(conn, &project_key, issue_number, exec_seq, node_id)
                        .await?
                };
                let Some(job_id) = job_id else {
                    return Ok(None);
                };
                let mut rows = conn
                    .query(
                        "
                        SELECT session_id, status, exit_code, created_at, exited_at, output_tail
                        FROM job_terminals
                        WHERE job_id = ?1 AND slug = ?2
                        LIMIT 1
                        ",
                        params![job_id.as_str(), slug.as_str()],
                    )
                    .await?;
                rows.next().await?
            };

            row.map(|row| {
                Ok(TerminalInfo {
                    session_id: row.text(0)?,
                    status: row.text(1)?,
                    exit_code: row.opt_i64(2)?.map(|value| value as i32),
                    created_at: row.i64(3)?,
                    exited_at: row.opt_i64(4)?,
                    output_tail: row.opt_text(5)?,
                })
            })
            .transpose()
        })
    })
    .await
}

/// List the terminal slugs that exist in the same scope as `parsed`, for a
/// helpful "not found" message. Project scope lists project-level terminals;
/// node scope lists the node job's terminals.
async fn list_terminal_slugs_in_scope(
    db: &LocalDb,
    parsed: &ParsedTerminalUri,
) -> DbResult<Vec<String>> {
    let project_key = parsed.project_key.clone();
    let issue_number = parsed.issue_number;
    let exec_seq = parsed.exec_seq;
    let node_id = parsed.node_id.clone();
    let task_name = parsed.task_name.clone();

    db.read(|conn| {
        let project_key = project_key.clone();
        let node_id = node_id.clone();
        let task_name = task_name.clone();
        Box::pin(async move {
            let mut rows = if node_id.is_none() {
                let lookup_key = project_key.to_uppercase();
                conn.query(
                    "
                    SELECT jt.slug
                    FROM job_terminals jt
                    JOIN projects p ON jt.project_id = p.id
                    WHERE p.key = ?1 AND jt.job_id IS NULL AND jt.slug IS NOT NULL
                    ORDER BY jt.slug
                    ",
                    params![lookup_key.as_str()],
                )
                .await?
            } else {
                let (Some(issue_number), Some(exec_seq), Some(node_id)) =
                    (issue_number, exec_seq, node_id.as_deref())
                else {
                    return Ok(Vec::new());
                };
                let job_id = if let Some(task_name) = task_name.as_deref() {
                    find_task_terminal_target_job_id(
                        conn,
                        &project_key,
                        issue_number,
                        exec_seq,
                        node_id,
                        task_name,
                    )
                    .await?
                } else {
                    find_terminal_target_job_id(conn, &project_key, issue_number, exec_seq, node_id)
                        .await?
                };
                let Some(job_id) = job_id else {
                    return Ok(Vec::new());
                };
                conn.query(
                    "
                    SELECT slug
                    FROM job_terminals
                    WHERE job_id = ?1 AND slug IS NOT NULL
                    ORDER BY slug
                    ",
                    params![job_id.as_str()],
                )
                .await?
            };

            let mut slugs = Vec::new();
            while let Some(row) = rows.next().await? {
                slugs.push(row.text(0)?);
            }
            Ok(slugs)
        })
    })
    .await
}

/// Parsed terminal URI components
#[derive(Debug)]
struct ParsedTerminalUri {
    project_key: String,
    issue_number: Option<i32>,
    exec_seq: Option<i32>,
    node_id: Option<String>,
    task_name: Option<String>,
    slug: String,
    /// `?new=true`: incremental cursor read instead of the idempotent full buffer.
    new: bool,
    annotations_suppressed: bool,
    anchor_uri: String,
}

/// Query keys owned by the batch view layer (line windowing + universal grep).
/// They legitimately ride on a terminal target, so the terminal parser ignores
/// them instead of rejecting them; the batch layer strips and applies them.
const VIEW_GRAMMAR_KEYS: &[&str] = &[
    "offset",
    "limit",
    "char_offset",
    "grep",
    "-i",
    "-n",
    "-A",
    "-B",
    "-C",
    "context",
    "head_limit",
    "multiline",
    "output_mode",
];

/// Detect malformed task terminal-like shapes and return a targeted hint.
pub(crate) fn task_terminal_hint(uri: &str) -> Option<String> {
    let identity = split_target_query(uri)
        .map(|split| split.identity)
        .unwrap_or_else(|_| uri.to_string());
    let path = identity.strip_prefix("cairn://")?;
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let n = segments.len();
    if n >= 3 && segments[n - 1] == "terminal" && segments[n - 3] == "task" {
        return Some(
            "Task terminal URI is missing a slug. Expected \
             `cairn://p/PROJECT/NUMBER/EXEC/NODE/task/TASK/terminal/SLUG`."
                .to_string(),
        );
    }
    None
}

fn terminal_shape_error(identity: &str) -> String {
    if let Some(hint) = task_terminal_hint(identity) {
        return hint;
    }
    format!(
        "Not a terminal URI: '{identity}'. Expected `cairn://p/PROJECT/terminal/SLUG`, \
         `cairn://p/PROJECT/NUMBER/EXEC/NODE/terminal/SLUG`, or \
         `cairn://p/PROJECT/NUMBER/EXEC/NODE/task/TASK/terminal/SLUG`."
    )
}

/// Parse a terminal URI into components, applying the terminal-specific query
/// params (`new`, `full`, `annotations`) and ignoring the universal read grammar
/// (`offset`/`limit`/`grep`...), which the batch view layer owns. Any other param
/// is an error that names the offending key; a non-terminal URI shape returns an
/// error that names the expected shape.
fn parse_terminal_uri(uri: &str) -> Result<ParsedTerminalUri, String> {
    let split =
        split_target_query(uri).map_err(|error| format!("Malformed URI '{uri}': {error}"))?;
    let mut new = false;
    let mut annotations_suppressed = false;
    for param in &split.params {
        match param.key.as_str() {
            "full" => match param.value.as_str() {
                "true" | "1" | "false" | "0" | "" => {}
                other => {
                    return Err(format!(
                        "Invalid value '{other}' for `full` (expected true or false)."
                    ))
                }
            },
            "new" => match param.value.as_str() {
                "true" | "1" => new = true,
                "false" | "0" | "" => {}
                other => {
                    return Err(format!(
                        "Invalid value '{other}' for `new` (expected true or false)."
                    ))
                }
            },
            "annotations" => match param.value.as_str() {
                "none" => annotations_suppressed = true,
                "" => {}
                other => {
                    return Err(format!(
                        "Invalid value '{other}' for `annotations` (expected `none`)."
                    ))
                }
            },
            key if VIEW_GRAMMAR_KEYS.contains(&key) => {}
            key => {
                return Err(format!(
                    "Unsupported query parameter `{key}` for terminal reads. Supported: `new`, \
                     `full`, `annotations`, plus the universal `offset`/`limit`/`grep` grammar."
                ))
            }
        }
    }

    let resource =
        parse_uri(&split.identity).ok_or_else(|| terminal_shape_error(&split.identity))?;
    let anchor_uri = resource.to_uri();
    match resource {
        CairnResource::NodeTerminal {
            project,
            number,
            exec_seq,
            node_id,
            slug,
        } => Ok(ParsedTerminalUri {
            project_key: project,
            issue_number: Some(number),
            exec_seq: Some(exec_seq),
            node_id: Some(node_id),
            task_name: None,
            slug,
            new,
            annotations_suppressed,
            anchor_uri,
        }),
        CairnResource::TaskTerminal {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            slug,
        } => Ok(ParsedTerminalUri {
            project_key: project,
            issue_number: Some(number),
            exec_seq: Some(exec_seq),
            node_id: Some(node_id),
            task_name: Some(task_name),
            slug,
            new,
            annotations_suppressed,
            anchor_uri,
        }),
        CairnResource::ProjectTerminal { project, slug } => Ok(ParsedTerminalUri {
            project_key: project,
            issue_number: None,
            exec_seq: None,
            node_id: None,
            task_name: None,
            slug,
            new,
            annotations_suppressed,
            anchor_uri,
        }),
        _ => Err(terminal_shape_error(&split.identity)),
    }
}

async fn append_inline_annotations(_orch: &Orchestrator, _output: &mut String, _anchor_uri: &str) {}

/// Read the live buffer for `session_id` and render it via the cursor model.
/// Returns (output, new_bytes, total_bytes, idle_secs). Idempotent reads
/// (`consume == false`) return the whole buffer and leave the cursor untouched;
/// incremental reads (`consume == true`) return bytes since the cursor and
/// advance it.
fn get_buffer_content(
    orch: &Orchestrator,
    read_cursors: &ReadCursorState,
    session_id: &str,
    status: &str,
    consume: bool,
    fallback_tail: Option<&str>,
) -> (String, usize, usize, Option<i64>) {
    let snapshot = (|| {
        let sessions = orch.pty_state.sessions.lock().ok()?;
        let session_arc = sessions.get(session_id)?;
        let session = session_arc.lock().ok()?;
        let buffer = session.output_buffer.as_ref()?;
        let bytes: Vec<u8> = buffer.lock().ok()?.iter().copied().collect();
        let idle = session
            .last_output_at
            .as_ref()
            .and_then(|ts| ts.lock().ok().map(|t| *t))
            .and_then(|t| std::time::SystemTime::now().duration_since(t).ok())
            .map(|d| d.as_secs() as i64);
        Some((bytes, idle))
    })();

    let (bytes, idle) = match snapshot {
        Some(value) => value,
        None => {
            // No live PTY session. For an exited terminal, fall back to the
            // retained output tail so a post-exit read still shows final output.
            if status != "running" {
                if let Some(tail) = fallback_tail.filter(|t| !t.is_empty()) {
                    let len = tail.len();
                    return (tail.to_string(), len, len, None);
                }
            }
            let msg = if status == "running" {
                "(no output yet)"
            } else {
                "(terminal ended - no buffered output)"
            };
            return (msg.to_string(), 0, 0, None);
        }
    };

    let prior_cursor = {
        let cursors = read_cursors.lock().unwrap_or_else(|e| e.into_inner());
        *cursors.get(session_id).unwrap_or(&0)
    };

    let (output, new_bytes, total_bytes, next_cursor) =
        render_buffer_slice(&bytes, prior_cursor, consume);

    if consume {
        let mut cursors = read_cursors.lock().unwrap_or_else(|e| e.into_inner());
        cursors.insert(session_id.to_string(), next_cursor);
    }

    let idle_secs = if total_bytes == 0 { None } else { idle };
    (output, new_bytes, total_bytes, idle_secs)
}

/// Pure cursor/render core, isolated for testing. `consume == false` reads the
/// whole buffer and leaves the cursor where it was; `consume == true` reads from
/// the cursor and advances it to the end. Returns
/// (output, new_bytes, total_bytes, next_cursor).
fn render_buffer_slice(
    bytes: &[u8],
    cursor: usize,
    consume: bool,
) -> (String, usize, usize, usize) {
    let total = bytes.len();
    let start = if consume { cursor.min(total) } else { 0 };
    let new_bytes = total.saturating_sub(start);
    let output = if total == 0 {
        "(no output yet)".to_string()
    } else if start < total {
        strip_ansi_sequences(&String::from_utf8_lossy(&bytes[start..]))
    } else {
        "(no new output)".to_string()
    };
    let next_cursor = if consume { total } else { cursor };
    (output, new_bytes, total, next_cursor)
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
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    #[test]
    fn fmt_duration_scales() {
        assert_eq!(fmt_duration(5), "5s");
        assert_eq!(fmt_duration(65), "1m05s");
        assert_eq!(fmt_duration(3725), "1h02m");
        assert_eq!(fmt_duration(-3), "0s");
    }

    #[test]
    fn parse_accepts_universal_grammar_and_terminal_params() {
        // The CAIRN-1489 repro set: bare, the universal grammar, and the
        // terminal-specific params — all must parse instead of being rejected.
        for uri in [
            "cairn://p/CAIRN/terminal/ci",
            "cairn://p/CAIRN/terminal/ci?limit=50",
            "cairn://p/CAIRN/terminal/ci?grep=foo",
            "cairn://p/CAIRN/terminal/ci?offset=-20",
            "cairn://p/CAIRN/terminal/ci?head_limit=10&grep=foo",
            "cairn://p/CAIRN/terminal/ci?new=true",
            "cairn://p/CAIRN/terminal/ci?full=true",
            "cairn://p/CAIRN/terminal/ci?annotations=none",
            "cairn://p/CAIRN/42/2/builder/terminal/dev?limit=5",
            "cairn://p/CAIRN/42/2/builder/task/Explore/terminal/ci?limit=5",
            "cairn://p/CAIRN/42/2/builder/task/Explore/terminal/ci?new=true",
        ] {
            assert!(parse_terminal_uri(uri).is_ok(), "expected Ok for {uri}");
        }
        assert!(
            parse_terminal_uri("cairn://p/CAIRN/terminal/ci?new=true")
                .unwrap()
                .new
        );
        assert!(
            !parse_terminal_uri("cairn://p/CAIRN/terminal/ci")
                .unwrap()
                .new
        );
    }

    #[test]
    fn parse_rejects_unknown_param_naming_it() {
        let err = parse_terminal_uri("cairn://p/CAIRN/terminal/ci?bogus=1").unwrap_err();
        assert!(err.contains("bogus"), "{err}");
        assert!(err.contains("Supported"), "{err}");
    }

    #[test]
    fn parse_non_terminal_uri_explains_shape() {
        let err = parse_terminal_uri("cairn://p/CAIRN/1").unwrap_err();
        assert!(err.contains("Expected"), "{err}");
    }

    #[test]
    fn parse_accepts_task_terminal_shape() {
        let parsed = parse_terminal_uri("cairn://p/CAIRN/1/1/builder/task/Explore/terminal/ci")
            .expect("task terminal URI should parse");
        assert_eq!(parsed.project_key, "CAIRN");
        assert_eq!(parsed.node_id.as_deref(), Some("builder"));
        assert_eq!(parsed.task_name.as_deref(), Some("Explore"));
        assert_eq!(parsed.slug, "ci");
        assert!(
            task_terminal_hint("cairn://p/CAIRN/1/1/builder/task/Explore/terminal/ci").is_none()
        );
        let err =
            parse_terminal_uri("cairn://p/CAIRN/1/1/builder/task/Explore/terminal").unwrap_err();
        assert!(err.contains("missing a slug"), "{err}");
    }

    #[test]
    fn render_buffer_slice_is_idempotent_until_consumed() {
        let bytes = b"hello\nworld\n";
        // Idempotent: full buffer, cursor untouched, repeatable.
        let (out1, new1, total1, cur1) = render_buffer_slice(bytes, 0, false);
        let (out2, _new2, _total2, cur2) = render_buffer_slice(bytes, 0, false);
        assert_eq!(out1, out2);
        assert_eq!(cur1, 0);
        assert_eq!(cur2, 0);
        assert_eq!(new1, bytes.len());
        assert_eq!(total1, bytes.len());
        assert!(out1.contains("hello") && out1.contains("world"));

        // Consume: first read returns all and advances; second returns nothing new.
        let (_o, n_first, total, next) = render_buffer_slice(bytes, 0, true);
        assert_eq!(n_first, bytes.len());
        assert_eq!(next, total);
        let (o2, n_second, _t, next2) = render_buffer_slice(bytes, next, true);
        assert_eq!(n_second, 0);
        assert_eq!(next2, total);
        assert_eq!(o2, "(no new output)");
    }

    #[test]
    fn banner_distinguishes_running_quiet_from_exited() {
        let running_quiet = format_status_banner("ci", "running", None, Some(70), Some(40), 1024);
        assert!(running_quiet.contains("running"));
        assert!(running_quiet.contains("elapsed"));
        assert!(running_quiet.contains("last output"));

        let running_no_output = format_status_banner("ci", "running", None, Some(5), None, 0);
        assert!(running_no_output.contains("no output yet"));

        let exited = format_status_banner("ci", "exited", Some(0), Some(83), None, 2048);
        assert!(exited.contains("exited code 0"));
        assert!(exited.contains("ran"));
    }

    #[test]
    fn not_found_message_lists_or_explains() {
        let empty = not_found_message("ci", &[]);
        assert!(empty.contains("no terminals exist"));
        let listed = not_found_message("ci", &["build".to_string(), "dev".to_string()]);
        assert!(listed.contains("build, dev"), "{listed}");
    }

    // ---- integration: real orchestrator + seeded job_terminals row ----

    async fn seeded_orch() -> Orchestrator {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
        use std::sync::Arc;

        let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    async fn exec(orch: &Orchestrator, sql: &'static str) {
        orch.db
            .local
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(sql, ()).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    async fn read_terminal(orch: &Orchestrator, uri: &str) -> TerminalReadResult {
        let request = McpCallbackRequest {
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: "read_resource".to_string(),
            payload: serde_json::json!({ "uri": uri }),
            tool_use_id: None,
        };
        let cursors = StdMutex::new(HashMap::new());
        let raw = handle_read_resource(orch, &request, &cursors).await;
        serde_json::from_str(&raw).unwrap()
    }

    async fn seed_node_and_task_terminals(orch: &Orchestrator) {
        exec(
            orch,
            "INSERT INTO workspaces (id, name, created_at, updated_at)
             VALUES ('ws-nt', 'WS', 1, 1)",
        )
        .await;
        exec(
            orch,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-nt', 'ws-nt', 'NT', 'NT', '/tmp/repo', 1, 1)",
        )
        .await;
        exec(
            orch,
            "INSERT INTO issues (id, project_id, number, title, status, progress, attention, created_at, updated_at)
             VALUES ('issue-nt', 'proj-nt', 42, 'I', 'active', 'active', 'none', 1, 1)",
        )
        .await;
        exec(
            orch,
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-nt', 'recipe', 'issue-nt', 'proj-nt', 'running', 1, 2)",
        )
        .await;
        exec(
            orch,
            "INSERT INTO jobs (id, project_id, issue_id, execution_id, uri_segment, status, created_at, updated_at)
             VALUES ('parent-nt', 'proj-nt', 'issue-nt', 'exec-nt', 'builder', 'running', 1, 1)",
        )
        .await;
        exec(
            orch,
            "INSERT INTO jobs (id, project_id, issue_id, execution_id, parent_job_id, uri_segment, status, created_at, updated_at)
             VALUES ('task-nt', 'proj-nt', 'issue-nt', 'exec-nt', 'parent-nt', 'Explore', 'running', 1, 1)",
        )
        .await;
        exec(
            orch,
            "INSERT INTO job_terminals
               (id, job_id, project_id, run_id, session_id, command, status, created_at, slug, output_tail)
             VALUES ('term-parent-nt', 'parent-nt', NULL, NULL, 'sess-parent-nt', 'ci', 'exited', 1, 'ci', 'parent-output')",
        )
        .await;
        exec(
            orch,
            "INSERT INTO job_terminals
               (id, job_id, project_id, run_id, session_id, command, status, created_at, slug, output_tail)
             VALUES ('term-task-nt', 'task-nt', NULL, NULL, 'sess-task-nt', 'ci', 'exited', 1, 'ci', 'task-output')",
        )
        .await;
        exec(
            orch,
            "INSERT INTO job_terminals
               (id, job_id, project_id, run_id, session_id, command, status, created_at, slug, output_tail)
             VALUES ('term-task-other-nt', 'task-nt', NULL, NULL, 'sess-task-other-nt', 'other', 'exited', 1, 'task-only', 'task-only-output')",
        )
        .await;
    }

    async fn seed_project_terminal(orch: &Orchestrator) {
        exec(
            orch,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-t', 'default', 'TM', 'TM', '/tmp/repo', 1, 1)",
        )
        .await;
        exec(
            orch,
            "INSERT INTO job_terminals
               (id, job_id, project_id, run_id, session_id, command, status, created_at, slug)
             VALUES ('term-1', NULL, 'proj-t', NULL, 'sess-1', 'sleep 1', 'running', 1, 'ci')",
        )
        .await;
    }

    #[tokio::test]
    async fn task_terminal_read_is_scoped_to_child_job() {
        let orch = seeded_orch().await;
        seed_node_and_task_terminals(&orch).await;

        let parent = read_terminal(&orch, "cairn://p/NT/42/2/builder/terminal/ci").await;
        assert_eq!(parent.status, "exited");
        assert!(parent.output.contains("parent-output"), "{}", parent.output);
        assert!(!parent.output.contains("task-output"), "{}", parent.output);

        let task = read_terminal(&orch, "cairn://p/NT/42/2/builder/task/Explore/terminal/ci").await;
        assert_eq!(task.status, "exited");
        assert!(task.output.contains("task-output"), "{}", task.output);
        assert!(!task.output.contains("parent-output"), "{}", task.output);

        let missing = read_terminal(&orch, "cairn://p/NT/42/2/builder/terminal/task-only").await;
        assert_eq!(missing.status, "error");
        assert!(
            missing
                .output
                .contains("Existing terminals in this scope: ci."),
            "{}",
            missing.output
        );
        assert!(
            !missing.output.contains("task-only-output"),
            "{}",
            missing.output
        );
    }

    #[tokio::test]
    async fn created_terminal_is_readable_with_universal_grammar() {
        let orch = seeded_orch().await;
        seed_project_terminal(&orch).await;

        // Bare, the universal grammar, the canonical form, and `new=true` all
        // resolve — never the old "Invalid terminal URI" rejection.
        for uri in [
            "cairn://p/TM/terminal/ci",
            "cairn://p/TM/terminal/ci?limit=50",
            "cairn://p/TM/terminal/ci?grep=foo",
            "cairn://p/TM/terminal/ci?offset=-20",
            "cairn://p/TM/terminal/ci?new=true",
        ] {
            let result = read_terminal(&orch, uri).await;
            assert_ne!(
                result.status, "error",
                "unexpected error for {uri}: {}",
                result.output
            );
            assert!(
                !result.output.contains("Invalid terminal URI"),
                "stale rejection for {uri}"
            );
            assert!(
                result.output.contains("[terminal ci:"),
                "missing banner for {uri}"
            );
        }
    }

    #[tokio::test]
    async fn plain_read_does_not_consume() {
        let orch = seeded_orch().await;
        seed_project_terminal(&orch).await;
        let first = read_terminal(&orch, "cairn://p/TM/terminal/ci").await;
        let second = read_terminal(&orch, "cairn://p/TM/terminal/ci").await;
        // No live session, so both report the same buffer state and never flip to
        // "(no new output)" the way a consuming read would.
        assert_eq!(first.status, "running");
        assert_eq!(second.status, "running");
        assert_eq!(first.total_bytes, second.total_bytes);
        assert!(first.output.contains("(no output yet)"));
        assert!(second.output.contains("(no output yet)"));
    }

    #[tokio::test]
    async fn missing_slug_lists_existing_slugs() {
        let orch = seeded_orch().await;
        seed_project_terminal(&orch).await;
        let result = read_terminal(&orch, "cairn://p/TM/terminal/nope").await;
        assert_eq!(result.status, "error");
        assert!(result.output.contains("nope"), "{}", result.output);
        assert!(result.output.contains("ci"), "{}", result.output);
    }
}
