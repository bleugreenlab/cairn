//! Transcript loading and compact resource formatting.

use std::collections::HashMap;

use turso::params;

use crate::storage::RowExt;

/// A loaded transcript event with the turn and timestamp coordinates the digest
/// renderer groups by. The verbose `raw`/`turn` formatters consume the legacy
/// `(run_id, sequence, event_type, data)` tuple via [`EventRow::to_transcript_row`].
#[derive(Debug, Clone)]
pub(super) struct EventRow {
    pub run_id: String,
    pub sequence: i32,
    pub event_type: String,
    pub data: String,
    pub turn_id: Option<String>,
    pub created_at: i64,
}

impl EventRow {
    pub(super) fn to_transcript_row(&self) -> crate::transcripts::TranscriptRow {
        (
            self.run_id.clone(),
            self.sequence,
            self.event_type.clone(),
            self.data.clone(),
        )
    }
}

/// Load events for a job in correct temporal order.
///
/// Uses two-query strategy: loads run ordering from `runs.created_at`,
/// then sorts events by `(run_position, created_at, rowid)`.
///
/// - `run_position` fixes cross-run ordering (UUID v4 alphabetical != temporal)
/// - `created_at` separates events across seconds
/// - `rowid` (via insertion-order index) is the stable intra-second tiebreaker,
///   because `sequence` is unreliable on cold resume
pub(super) async fn load_job_events_ordered(
    conn: &turso::Connection,
    job_id: &str,
    store: Option<&dyn crate::archival::store::ContentStore>,
    private_route_db: Option<&crate::storage::LocalDb>,
) -> Vec<EventRow> {
    // 1. Load run_ids ordered by creation time
    let mut ordered_run_ids: Vec<String> = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT id
            FROM runs
            WHERE job_id = ?1
            ORDER BY created_at ASC
            ",
            (job_id,),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            if let Ok(run_id) = row.text(0) {
                ordered_run_ids.push(run_id);
            }
        }
    }

    if ordered_run_ids.is_empty() {
        return Vec::new();
    }

    // Select full event rows (archival columns included) so completed-node
    // transcripts — the archived case — reconstruct from git coordinates before
    // formatting. Live `full` rows pass through untouched.
    let columns = crate::runs::queries::EVENT_COLUMNS;
    let mut events: Vec<crate::models::Event> = Vec::new();
    for run_id in ordered_run_ids {
        let sql = format!(
            "SELECT {columns} FROM events WHERE run_id = ?1 ORDER BY created_at ASC, rowid ASC"
        );
        if let Ok(mut rows) = conn.query(&sql, (run_id.as_str(),)).await {
            while let Ok(Some(row)) = rows.next().await {
                if let Ok(event) = crate::runs::queries::event_from_row(&row) {
                    events.push(event);
                }
            }
        }
    }

    crate::archival::reconstruct::reconstruct_events_with_conn_and_routes(
        conn,
        events,
        store,
        private_route_db,
    )
    .await
    .into_iter()
    .map(|event| EventRow {
        run_id: event.run_id,
        sequence: event.sequence,
        event_type: event.event_type,
        data: event.data,
        turn_id: event.turn_id,
        created_at: event.created_at,
    })
    .collect()
}

pub(super) async fn load_turn_events(
    conn: &turso::Connection,
    turn_id: &str,
    store: Option<&dyn crate::archival::store::ContentStore>,
    private_route_db: Option<&crate::storage::LocalDb>,
) -> Vec<(String, i32, String, String)> {
    let columns = crate::runs::queries::EVENT_COLUMNS;
    let sql = format!("SELECT {columns} FROM events WHERE turn_id = ?1 ORDER BY sequence ASC");
    let mut events: Vec<crate::models::Event> = Vec::new();
    if let Ok(mut rows) = conn.query(&sql, (turn_id,)).await {
        while let Ok(Some(row)) = rows.next().await {
            if let Ok(event) = crate::runs::queries::event_from_row(&row) {
                events.push(event);
            }
        }
    }
    crate::archival::reconstruct::reconstruct_events_with_conn_and_routes(
        conn,
        events,
        store,
        private_route_db,
    )
    .await
    .into_iter()
    .map(|event| (event.run_id, event.sequence, event.event_type, event.data))
    .collect()
}

/// Get the Nth run for a job (1-indexed)
pub(super) async fn get_nth_run_id(
    conn: &turso::Connection,
    job_id: &str,
    run_seq: i32,
) -> Option<String> {
    if run_seq < 1 {
        return None;
    }

    let mut rows = conn
        .query(
            "
            SELECT id
            FROM runs
            WHERE job_id = ?1
            ORDER BY created_at ASC
            LIMIT 1 OFFSET ?2
            ",
            params![job_id, (run_seq - 1) as i64],
        )
        .await
        .ok()?;

    rows.next().await.ok().flatten()?.text(0).ok()
}

/// Get a single event by run_id and sequence, returning full formatted content
pub(super) async fn get_single_event(
    conn: &turso::Connection,
    run_id: &str,
    event_seq: i32,
    store: Option<&dyn crate::archival::store::ContentStore>,
    private_route_db: Option<&crate::storage::LocalDb>,
) -> String {
    let columns = crate::runs::queries::EVENT_COLUMNS;
    let sql = format!(
        "SELECT {columns}, change_preview_status, change_applied_at
         FROM events WHERE run_id = ?1 AND sequence = ?2 LIMIT 1"
    );
    let mut rows = match conn.query(&sql, params![run_id, event_seq as i64]).await {
        Ok(rows) => rows,
        Err(_) => return format!("Event with sequence {} not found", event_seq),
    };
    let Ok(Some(row)) = rows.next().await else {
        return format!("Event with sequence {} not found", event_seq);
    };
    // change_preview_* ride just past EVENT_COLUMNS; key off EVENT_COLUMN_COUNT so
    // adding an event column shifts them in lockstep rather than reading the wrong
    // slot.
    let preview_status = row
        .opt_text(crate::runs::queries::EVENT_COLUMN_COUNT)
        .ok()
        .flatten();
    let preview_applied = row
        .opt_i64(crate::runs::queries::EVENT_COLUMN_COUNT + 1)
        .ok()
        .flatten();
    let Ok(event) = crate::runs::queries::event_from_row(&row) else {
        return format!("Event with sequence {} not found", event_seq);
    };

    let mut events = crate::archival::reconstruct::reconstruct_events_with_conn_and_routes(
        conn,
        vec![event],
        store,
        private_route_db,
    )
    .await;
    let event = events.remove(0);
    let resolved_tool_name =
        resolve_single_event_tool_name(conn, run_id, &event.event_type, &event.data).await;
    format_single_event(
        &event.event_type,
        &event.data,
        preview_status,
        preview_applied,
        resolved_tool_name.as_deref(),
    )
}

async fn resolve_single_event_tool_name(
    conn: &turso::Connection,
    run_id: &str,
    event_type: &str,
    data: &str,
) -> Option<String> {
    if !matches!(event_type, "result" | "tool_result") {
        return None;
    }

    let event_data: serde_json::Value = serde_json::from_str(data).ok()?;
    if event_data
        .get("toolName")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .is_some()
    {
        return None;
    }

    let tool_use_id = tool_use_id_from_event(&event_data)?;

    resolve_tool_name(conn, run_id, tool_use_id).await
}

fn tool_use_id_from_event(event_data: &serde_json::Value) -> Option<&str> {
    event_data
        .get("toolUseId")
        .or_else(|| event_data.get("tool_use_id"))
        .and_then(|value| value.as_str())
}

async fn resolve_tool_name(
    conn: &turso::Connection,
    run_id: &str,
    tool_use_id: &str,
) -> Option<String> {
    let mut rows = conn
        .query(
            "
            SELECT data
            FROM events
            WHERE run_id = ?1 AND event_type = 'assistant'
            ORDER BY created_at ASC, rowid ASC
            ",
            params![run_id],
        )
        .await
        .ok()?;

    while let Ok(Some(row)) = rows.next().await {
        let Ok(data) = row.text(0) else { continue };
        let Ok(event_data) = serde_json::from_str::<serde_json::Value>(&data) else {
            continue;
        };
        let Some(tool_uses) = event_data
            .get("toolUses")
            .and_then(|value| value.as_array())
        else {
            continue;
        };

        for tool in tool_uses {
            let id = tool.get("id").and_then(|value| value.as_str());
            let name = tool.get("name").and_then(|value| value.as_str());
            if id == Some(tool_use_id) {
                return name.filter(|value| !value.is_empty()).map(str::to_string);
            }
        }
    }

    None
}

/// Format a single event with full content (no truncation)
fn format_single_event(
    event_type: &str,
    data: &str,
    change_preview_status: Option<String>,
    change_applied_at: Option<i64>,
    resolved_tool_name: Option<&str>,
) -> String {
    let event_data: serde_json::Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(_) => return "Error parsing event data".to_string(),
    };

    let mut output = String::new();

    match event_type {
        "assistant" => {
            if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                if !content.is_empty() {
                    output.push_str("**Assistant:** ");
                    output.push_str(content);
                    output.push_str("\n\n");
                }
            }

            // Handle tool_uses (tool calls)
            if let Some(tool_uses) = event_data.get("toolUses").and_then(|t| t.as_array()) {
                for tool in tool_uses {
                    let name = tool
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("unknown");
                    let input = tool
                        .get("input")
                        .map(|i| {
                            if i.is_string() {
                                i.as_str().unwrap_or("").to_string()
                            } else {
                                serde_json::to_string_pretty(i).unwrap_or_default()
                            }
                        })
                        .unwrap_or_default();

                    output.push_str(&format!("**Tool Call ({}):**\n", name));
                    output.push_str(&input);
                    output.push_str("\n\n");
                }
            }
        }
        "user" => {
            if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                if !content.is_empty() {
                    output.push_str("**User:** ");
                    output.push_str(content);
                    output.push_str("\n\n");
                }
            }
        }
        "result" | "tool_result" => {
            if let Some(result) = event_data.get("toolResult") {
                let tool_name = event_data
                    .get("toolName")
                    .and_then(|t| t.as_str())
                    .filter(|value| !value.is_empty())
                    .or(resolved_tool_name);
                let is_error = event_data
                    .get("isError")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);

                match (is_error, tool_name) {
                    (true, Some(name)) => {
                        output.push_str(&format!("**Tool Result error ({}):**\n", name))
                    }
                    (true, None) => output.push_str("**Tool Result error:**\n"),
                    (false, Some(name)) => {
                        output.push_str(&format!("**Tool Result ({}):**\n", name))
                    }
                    (false, None) => output.push_str("**Tool Result:**\n"),
                }

                let result_text = json_value_to_display(result);
                if result_text.trim().is_empty() {
                    output.push_str("(empty result)");
                } else {
                    output.push_str(&result_text);
                }
                output.push_str("\n\n");
            }
        }
        _ => {
            // Return raw JSON for other event types
            output.push_str(&format!("**Event ({}):**\n", event_type));
            output.push_str(
                &serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| data.to_string()),
            );
        }
    }

    if let Some(status) = change_preview_status {
        output.push_str("**Change Preview:**\n");
        output.push_str(&format!("- Status: `{}`\n", status));
        if let Some(applied_at) = change_applied_at {
            output.push_str(&format!("- Applied at: `{}`\n", applied_at));
        }
        if status == "pending" {
            output.push_str(
                "- Action: use `write` with this event URI as `target` and `mode: \"apply\"`\n",
            );
        }
        output.push('\n');
    }

    if output.is_empty() {
        "No content in this event.".to_string()
    } else {
        output
    }
}

// Tool-input summary preview cap (shared by the digest row builders).
const TOOL_SUMMARY_PREVIEW: usize = 80;

// Digest truncation thresholds. User content gets a generous window (its first
// turn often replays the whole plan); assistant prose is the narrative and is
// cheap once tool noise is gone, so it truncates only at a high ceiling.
const USER_CONTENT_THRESHOLD: usize = 3000;
const USER_CONTENT_PREVIEW: usize = 1200;
const DIGEST_ASSISTANT_THRESHOLD: usize = 6000;
const DIGEST_ASSISTANT_PREVIEW: usize = 2000;
const ERROR_LINE_PREVIEW: usize = 200;
const ROW_LABEL_PREVIEW: usize = 120;

fn json_value_to_display(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.to_string(),
        _ => serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()),
    }
}

/// Truncate text and return (display_text, was_truncated, original_len)
fn truncate_with_info(text: &str, threshold: usize, preview_len: usize) -> (String, bool, usize) {
    let original_len = text.len();
    if original_len <= threshold {
        (text.to_string(), false, original_len)
    } else {
        // Find a safe truncation point (don't break in middle of multi-byte char)
        let safe_len = text
            .char_indices()
            .take_while(|(i, _)| *i < preview_len)
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);
        (format!("{}...", &text[..safe_len]), true, original_len)
    }
}

fn cleaned_tool_name(tool_name: &str) -> &str {
    if tool_name.starts_with("mcp__") {
        tool_name.rsplit("__").next().unwrap_or(tool_name)
    } else {
        tool_name
    }
}

fn single_line_preview(text: &str, max_len: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let safe_len = trimmed
        .char_indices()
        .take_while(|(i, _)| *i < max_len)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(trimmed.len());

    if trimmed.len() > safe_len {
        format!("{}...", &trimmed[..safe_len])
    } else {
        trimmed.to_string()
    }
}

fn summarize_scalar(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) => {
            let preview = single_line_preview(text, TOOL_SUMMARY_PREVIEW);
            if preview.is_empty() {
                None
            } else {
                Some(preview)
            }
        }
        serde_json::Value::Number(number) => Some(number.to_string()),
        serde_json::Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    }
}

fn summarize_tool_input(tool_name: &str, input: &serde_json::Value) -> Option<String> {
    let obj = input.as_object()?;
    let cleaned = cleaned_tool_name(tool_name);

    match cleaned {
        "Bash" | "bash" => obj
            .get("description")
            .or_else(|| obj.get("command"))
            .and_then(summarize_scalar),
        "Read" | "read" => obj
            .get("file_path")
            .or_else(|| obj.get("path"))
            .and_then(summarize_scalar),
        "Edit" | "edit" | "filechange" => {
            if let Some(changes) = obj.get("changes").and_then(|v| v.as_array()) {
                let count = changes.len();
                Some(format!(
                    "{} file{}",
                    count,
                    if count == 1 { "" } else { "s" }
                ))
            } else {
                obj.get("file_path").and_then(summarize_scalar)
            }
        }
        "Write" | "write" => obj.get("file_path").and_then(summarize_scalar),
        "Grep" | "grep" => obj
            .get("pattern")
            .and_then(summarize_scalar)
            .map(|pattern| format!("\"{}\"", pattern)),
        "Glob" | "glob" => obj.get("pattern").and_then(summarize_scalar),
        "task" | "Task" => obj.get("description").and_then(summarize_scalar),
        "message" => obj.get("content").and_then(|value| {
            summarize_scalar(value).map(|content| match obj.get("to").and_then(|v| v.as_str()) {
                Some(target) if !target.is_empty() => format!("→ {} {}", target, content),
                _ => format!("→ project {}", content),
            })
        }),
        _ => obj
            .get("command")
            .or_else(|| obj.get("file_path"))
            .or_else(|| obj.get("path"))
            .or_else(|| obj.get("pattern"))
            .or_else(|| obj.get("query"))
            .or_else(|| obj.get("url"))
            .or_else(|| obj.get("title"))
            .or_else(|| obj.get("description"))
            .and_then(summarize_scalar),
    }
}

fn line_count(text: &str) -> usize {
    text.lines()
        .count()
        .max(if text.is_empty() { 0 } else { 1 })
}

fn figure_for_read_result(text: &str) -> Option<String> {
    let count = line_count(text);
    (count > 0).then(|| format!("[{} lines]", count))
}

fn figure_for_run_result(text: &str) -> Option<String> {
    let patterns = ["Exit code:", "exit code", "exit status", "status:"];
    for line in text.lines().rev().take(20) {
        let lower = line.to_ascii_lowercase();
        if patterns.iter().any(|pattern| lower.contains(pattern)) {
            if let Some(code) = lower
                .split(|ch: char| !ch.is_ascii_digit())
                .find(|part| !part.is_empty())
            {
                return Some(format!("[exit {}]", code));
            }
        }
    }

    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("exit_code")
                .or_else(|| value.get("exitCode"))
                .or_else(|| value.get("code"))
                .and_then(|code| code.as_i64())
                .map(|code| format!("[exit {}]", code))
        })
}

fn count_prefixed_diff_lines(text: &str) -> Option<String> {
    let mut adds = 0usize;
    let mut dels = 0usize;
    for line in text.lines() {
        if line.starts_with("+++") || line.starts_with("---") {
            continue;
        }
        if line.starts_with('+') {
            adds += 1;
        } else if line.starts_with('-') {
            dels += 1;
        }
    }
    (adds > 0 || dels > 0).then(|| format!("(+{} −{})", adds, dels))
}

fn figure_for_tool_io(tool_name: Option<&str>, text: &str) -> Option<String> {
    match tool_name.map(cleaned_tool_name) {
        Some("Read" | "read") => figure_for_read_result(text),
        Some("Bash" | "bash" | "run" | "Run") => figure_for_run_result(text),
        Some("Edit" | "edit" | "Write" | "write" | "change" | "filechange") => {
            count_prefixed_diff_lines(text)
        }
        Some("Grep" | "grep") => {
            let count = line_count(text);
            (count > 0).then(|| format!("[{} matches]", count))
        }
        _ => None,
    }
}

// ============================================================================
// Digest renderer — the default `/chat` projection
// ============================================================================
//
// The digest is the text projection of the frontend's flat-row tool model
// (docs/tool-call-presentation.md): turn-structured, one row per target, a
// right-pinned count figure, no inline JSON bodies. Drill-down is `chat/turn/N`
// (advertised in the header and each turn heading); the verbose stream is at
// `chat/raw`.

pub(super) struct DigestMeta<'a> {
    pub label: &'a str,
    pub project: &'a str,
    pub number: i32,
    pub exec_seq: i32,
    pub status: &'a str,
}

/// A tool result keyed by toolUseId, used to attach metrics/errors to buffered
/// call rows in a single pass.
struct ResultInfo {
    text: String,
    is_error: bool,
}

/// One rendered digest row: `<bullet> <text> [metric] — <detail> <trailer>`.
struct DigestRow {
    error: bool,
    text: String,
    metric: Option<String>,
    detail: Option<String>,
    trailer: Option<String>,
}

impl DigestRow {
    fn render(&self) -> String {
        let bullet = if self.error { "✗" } else { "·" };
        let mut line = format!("{} {}", bullet, self.text.trim_end());
        if let Some(metric) = self.metric.as_deref().filter(|m| !m.is_empty()) {
            line.push(' ');
            line.push_str(metric);
        }
        if let Some(detail) = self.detail.as_deref().filter(|d| !d.is_empty()) {
            line.push_str(" — ");
            line.push_str(detail);
        }
        if let Some(trailer) = self.trailer.as_deref().filter(|t| !t.is_empty()) {
            line.push(' ');
            line.push_str(trailer);
        }
        line
    }
}

fn plural(n: usize) -> &'static str {
    if n == 1 {
        ""
    } else {
        "s"
    }
}

fn short_sha(sha: &str) -> String {
    sha.chars().take(7).collect()
}

fn first_nonempty_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
}

fn first_result_line(text: &str) -> String {
    first_nonempty_line(text)
        .map(|line| single_line_preview(&line, ERROR_LINE_PREVIEW))
        .unwrap_or_else(|| "(no output)".to_string())
}

fn shorten_target(target: &str) -> String {
    if let Some(rest) = target.strip_prefix("cairn:~/") {
        rest.to_string()
    } else if let Some(rest) = target.strip_prefix("file:") {
        if rest.is_empty() {
            "(worktree root)".to_string()
        } else {
            rest.to_string()
        }
    } else {
        target.to_string()
    }
}

/// Render a read target as `path ?facets`, splitting the query string and
/// re-emitting its facets in canonical order (offset/limit → grep+modifiers →
/// glob → other), mirroring the frontend's facet tokens.
fn render_read_target(raw: &str) -> String {
    let (path, query) = match raw.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (raw, None),
    };
    let path = path.strip_prefix("file:").unwrap_or(path);
    let mut out = if path.is_empty() {
        "(worktree root)".to_string()
    } else {
        path.to_string()
    };
    if let Some(query) = query {
        let facets = render_query_facets(query);
        if !facets.is_empty() {
            out.push(' ');
            out.push_str(&facets);
        }
    }
    out
}

fn render_query_facets(query: &str) -> String {
    let mut offset = None;
    let mut limit = None;
    let mut grep = None;
    let mut grep_mods: Vec<String> = Vec::new();
    let mut glob = None;
    let mut others: Vec<String> = Vec::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((key, value)) => (key, Some(value)),
            None => (pair, None),
        };
        let token = match value {
            Some(value) => format!("{}={}", key, value),
            None => key.to_string(),
        };
        match key {
            "offset" => offset = Some(token),
            "limit" => limit = Some(token),
            "grep" => grep = Some(token),
            "-i" | "-A" | "-B" | "-C" | "context" | "head_limit" | "output_mode" => {
                grep_mods.push(token)
            }
            "glob" => glob = Some(token),
            _ => others.push(token),
        }
    }
    let mut tokens: Vec<String> = Vec::new();
    tokens.extend(offset);
    tokens.extend(limit);
    if let Some(grep) = grep {
        tokens.push(grep);
        tokens.extend(grep_mods);
    }
    tokens.extend(glob);
    tokens.extend(others);
    if tokens.is_empty() {
        String::new()
    } else {
        format!("?{}", tokens.join(" "))
    }
}

/// Split a multi-target read/run result into `(header_inner, body)` sections,
/// keyed by the `=== <uri> [suffix] ===` headers the read tool emits.
fn read_sections(text: &str) -> Vec<(String, String)> {
    let mut sections: Vec<(String, String)> = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.len() >= 8 && trimmed.starts_with("=== ") && trimmed.ends_with(" ===") {
            let inner = trimmed[4..trimmed.len() - 4].to_string();
            sections.push((inner, String::new()));
        } else if let Some(last) = sections.last_mut() {
            if !last.1.is_empty() {
                last.1.push('\n');
            }
            last.1.push_str(line);
        }
    }
    sections
}

/// Derive a count figure for one read section, preferring the header suffix
/// (`[lines A–B of T]` → `[T lines]`, `[N matches…]` → `[N matches]`) and
/// falling back to a body line count.
fn metric_from_section(header_inner: &str, body: &str) -> Option<String> {
    if header_inner.ends_with(']') {
        if let Some(start) = header_inner.rfind('[') {
            let raw = &header_inner[start + 1..header_inner.len() - 1];
            if let Some(rest) = raw.strip_prefix("lines ") {
                if let Some((_, total)) = rest.rsplit_once(" of ") {
                    return Some(format!("[{} lines]", total.trim()));
                }
            }
            if raw.contains("matches") {
                let count = raw.split_whitespace().next().unwrap_or("0");
                return Some(format!("[{} matches]", count));
            }
            return Some(format!("[{}]", raw));
        }
    }
    figure_for_read_result(body)
}

fn collect_results(events: &[EventRow]) -> HashMap<String, ResultInfo> {
    let mut map: HashMap<String, ResultInfo> = HashMap::new();
    for event in events {
        if !matches!(event.event_type.as_str(), "result" | "tool_result") {
            continue;
        }
        let Ok(data) = serde_json::from_str::<serde_json::Value>(&event.data) else {
            continue;
        };
        let Some(id) = tool_use_id_from_event(&data).map(str::to_string) else {
            continue;
        };
        let is_error = data
            .get("isError")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let text = data
            .get("toolResult")
            .map(json_value_to_display)
            .unwrap_or_default();
        map.insert(id, ResultInfo { text, is_error });
    }
    map
}

fn rows_for_tool_call(
    name: &str,
    input: &serde_json::Value,
    result: Option<&ResultInfo>,
) -> Vec<DigestRow> {
    let cleaned = cleaned_tool_name(name);
    let mut rows = match cleaned.to_ascii_lowercase().as_str() {
        "read" => read_rows(input, result),
        "write" | "change" | "filechange" => write_rows(input, result),
        "run" | "bash" => run_rows(input, result),
        "task" => task_rows(input, result),
        "message" => message_rows(name, input, result),
        _ => generic_rows(cleaned, name, input, result),
    };
    // An unpaired call (in-flight or interrupted) has no result; mark its
    // otherwise-blank rows pending.
    if result.is_none() {
        for row in &mut rows {
            if !row.error && row.metric.is_none() {
                row.metric = Some("[pending]".to_string());
            }
        }
    }
    rows
}

fn read_rows(input: &serde_json::Value, result: Option<&ResultInfo>) -> Vec<DigestRow> {
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    let error_detail = result
        .filter(|r| r.is_error)
        .map(|r| first_result_line(&r.text));
    let obj = input.as_object();

    if let Some(paths) = obj
        .and_then(|o| o.get("paths"))
        .and_then(|value| value.as_array())
    {
        let sections = result
            .filter(|r| !r.is_error)
            .map(|r| read_sections(&r.text))
            .unwrap_or_default();
        let mut rows = Vec::new();
        for (index, path) in paths.iter().enumerate() {
            let Some(path) = path.as_str() else { continue };
            let metric = sections
                .get(index)
                .and_then(|(header, body)| metric_from_section(header, body));
            rows.push(DigestRow {
                error: is_error && index == 0,
                text: format!("read {}", render_read_target(path)),
                metric: if is_error { None } else { metric },
                detail: if is_error && index == 0 {
                    error_detail.clone()
                } else {
                    None
                },
                trailer: None,
            });
        }
        if !rows.is_empty() {
            return rows;
        }
    }

    let path = obj
        .and_then(|o| o.get("path").or_else(|| o.get("file_path")))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let metric = result
        .filter(|r| !r.is_error)
        .and_then(|r| figure_for_read_result(&r.text));
    vec![DigestRow {
        error: is_error,
        text: format!("read {}", render_read_target(path)),
        metric,
        detail: error_detail,
        trailer: None,
    }]
}

fn write_rows(input: &serde_json::Value, result: Option<&ResultInfo>) -> Vec<DigestRow> {
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    let report = result.and_then(|r| serde_json::from_str::<serde_json::Value>(&r.text).ok());

    let applied: HashMap<u64, (String, String)> = report
        .as_ref()
        .and_then(|report| report.get("applied"))
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let index = item.get("index").and_then(|value| value.as_u64())?;
                    let summary = item
                        .get("summary")
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let kind = item
                        .get("kind")
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    Some((index, (summary, kind)))
                })
                .collect()
        })
        .unwrap_or_default();

    let failures: HashMap<u64, String> = report
        .as_ref()
        .and_then(|report| report.get("failures"))
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let index = item.get("index").and_then(|value| value.as_u64())?;
                    let error = item
                        .get("error")
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    Some((index, error))
                })
                .collect()
        })
        .unwrap_or_default();

    let commit_trailer = report
        .as_ref()
        .and_then(|report| report.get("commit"))
        .and_then(|commit| commit.get("sha"))
        .and_then(|value| value.as_str())
        .filter(|sha| !sha.is_empty())
        .map(|sha| format!("[committed {}]", short_sha(sha)));

    let changes = input
        .as_object()
        .and_then(|o| o.get("changes"))
        .and_then(|value| value.as_array());

    let Some(changes) = changes else {
        return vec![DigestRow {
            error: is_error,
            text: "write".to_string(),
            metric: None,
            detail: result
                .filter(|r| r.is_error)
                .map(|r| first_result_line(&r.text)),
            trailer: commit_trailer,
        }];
    };

    let mut rows: Vec<DigestRow> = Vec::new();
    for (index, change) in changes.iter().enumerate() {
        let key = index as u64;
        let target = change
            .get("target")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let mode = change
            .get("mode")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let short = shorten_target(target);

        if let Some(error) = failures.get(&key) {
            rows.push(DigestRow {
                error: true,
                text: format!("write {}", short),
                metric: None,
                detail: Some(single_line_preview(error, ERROR_LINE_PREVIEW)),
                trailer: None,
            });
            continue;
        }

        let (summary, kind) = applied.get(&key).cloned().unwrap_or_default();
        let is_file = kind == "file" || (kind.is_empty() && target.starts_with("file:"));
        let metric = if is_file {
            diffstat_for_change(mode, change.get("payload"))
        } else {
            None
        };
        // File rows carry the diffstat metric; the applied summary just echoes
        // the marker+path, so only resource rows get a summary blurb.
        let detail = if is_file {
            None
        } else {
            first_nonempty_line(&summary).map(|line| single_line_preview(&line, ERROR_LINE_PREVIEW))
        };
        rows.push(DigestRow {
            error: false,
            text: format!("write {}", short),
            metric,
            detail,
            trailer: None,
        });
    }

    if let Some(trailer) = commit_trailer {
        if let Some(last) = rows.iter_mut().rev().find(|row| !row.error) {
            last.trailer = Some(trailer);
        } else {
            rows.push(DigestRow {
                error: false,
                text: "write".to_string(),
                metric: None,
                detail: None,
                trailer: Some(trailer),
            });
        }
    }

    if rows.is_empty() {
        rows.push(DigestRow {
            error: is_error,
            text: "write".to_string(),
            metric: None,
            detail: None,
            trailer: None,
        });
    }

    rows
}

fn diffstat_for_change(mode: &str, payload: Option<&serde_json::Value>) -> Option<String> {
    let payload = payload?;
    if let Some(diff) = payload.get("diff").and_then(|value| value.as_str()) {
        return count_prefixed_diff_lines(diff);
    }
    if let Some(patch) = payload.get("patch").and_then(|value| value.as_str()) {
        return count_prefixed_diff_lines(patch);
    }
    if let (Some(old), Some(new)) = (
        payload.get("old_string").and_then(|value| value.as_str()),
        payload.get("new_string").and_then(|value| value.as_str()),
    ) {
        return Some(format!(
            "(+{} −{})",
            new.lines().count(),
            old.lines().count()
        ));
    }
    if matches!(mode, "create" | "replace" | "append") {
        if let Some(content) = payload.get("content").and_then(|value| value.as_str()) {
            return Some(format!("(+{} −0)", content.lines().count()));
        }
    }
    None
}

fn command_label(value: &serde_json::Value) -> String {
    value
        .get("description")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| value.get("command").and_then(|v| v.as_str()))
        .or_else(|| value.get("target").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string()
}

fn run_rows(input: &serde_json::Value, result: Option<&ResultInfo>) -> Vec<DigestRow> {
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    let error_detail = result
        .filter(|r| r.is_error)
        .map(|r| first_result_line(&r.text));

    if let Some(commands) = input
        .as_object()
        .and_then(|o| o.get("commands"))
        .and_then(|value| value.as_array())
    {
        let sections = result.map(|r| read_sections(&r.text)).unwrap_or_default();
        let single = commands.len() == 1;
        let mut rows = Vec::new();
        for (index, command) in commands.iter().enumerate() {
            let metric = if is_error {
                None
            } else if let Some((_, body)) = sections.get(index) {
                figure_for_run_result(body)
            } else if single {
                result.and_then(|r| figure_for_run_result(&r.text))
            } else {
                None
            };
            rows.push(DigestRow {
                error: is_error && index == 0,
                text: format!(
                    "run {}",
                    single_line_preview(&command_label(command), ROW_LABEL_PREVIEW)
                ),
                metric,
                detail: if is_error && index == 0 {
                    error_detail.clone()
                } else {
                    None
                },
                trailer: None,
            });
        }
        if !rows.is_empty() {
            return rows;
        }
    }

    let label = command_label(input);
    let metric = result
        .filter(|r| !r.is_error)
        .and_then(|r| figure_for_run_result(&r.text));
    vec![DigestRow {
        error: is_error,
        text: format!("run {}", single_line_preview(&label, ROW_LABEL_PREVIEW)),
        metric,
        detail: error_detail,
        trailer: None,
    }]
}

fn task_rows(input: &serde_json::Value, result: Option<&ResultInfo>) -> Vec<DigestRow> {
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    let description = input
        .as_object()
        .and_then(|o| o.get("description").or_else(|| o.get("prompt")))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    vec![DigestRow {
        error: is_error,
        text: format!(
            "task {}",
            single_line_preview(description, ROW_LABEL_PREVIEW)
        ),
        metric: None,
        detail: result
            .filter(|r| r.is_error)
            .map(|r| first_result_line(&r.text)),
        trailer: None,
    }]
}

fn message_rows(
    name: &str,
    input: &serde_json::Value,
    result: Option<&ResultInfo>,
) -> Vec<DigestRow> {
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    let summary = summarize_tool_input(name, input).unwrap_or_default();
    vec![DigestRow {
        error: is_error,
        text: if summary.is_empty() {
            "message".to_string()
        } else {
            format!("message {}", summary)
        },
        metric: None,
        detail: result
            .filter(|r| r.is_error)
            .map(|r| first_result_line(&r.text)),
        trailer: None,
    }]
}

fn generic_rows(
    verb: &str,
    name: &str,
    input: &serde_json::Value,
    result: Option<&ResultInfo>,
) -> Vec<DigestRow> {
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    let summary = summarize_tool_input(name, input).unwrap_or_default();
    let text = if summary.is_empty() {
        verb.to_string()
    } else {
        format!("{} {}", verb, summary)
    };
    let metric = result
        .filter(|r| !r.is_error)
        .and_then(|r| figure_for_tool_io(Some(name), &r.text));
    vec![DigestRow {
        error: is_error,
        text,
        metric,
        detail: result
            .filter(|r| r.is_error)
            .map(|r| first_result_line(&r.text)),
        trailer: None,
    }]
}

pub(super) fn format_transcript_digest(
    events: &[EventRow],
    base_uri: &str,
    meta: &DigestMeta,
    turn_sequences: &HashMap<String, i32>,
    latest: bool,
    turn_offset: Option<i64>,
    turn_limit: Option<usize>,
) -> String {
    if events.is_empty() {
        return "No runs found for this node.".to_string();
    }

    let results = collect_results(events);

    struct Block<'a> {
        turn_id: Option<&'a str>,
        events: Vec<&'a EventRow>,
    }
    let mut blocks: Vec<Block> = Vec::new();
    for event in events {
        let key = event.turn_id.as_deref();
        match blocks.last_mut() {
            Some(block) if key.is_none() || block.turn_id == key => block.events.push(event),
            _ => blocks.push(Block {
                turn_id: key,
                events: vec![event],
            }),
        }
    }

    let run_count = {
        let mut seen: Vec<&str> = Vec::new();
        for event in events {
            if !seen.contains(&event.run_id.as_str()) {
                seen.push(event.run_id.as_str());
            }
        }
        seen.len()
    };
    let date = chrono::DateTime::from_timestamp(events[0].created_at, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_default();

    let mut out = format!(
        "# {} — {}-{}/{} · {} run{} · {} turn{} · {} event{} · {} · {}\n",
        meta.label,
        meta.project,
        meta.number,
        meta.exec_seq,
        run_count,
        plural(run_count),
        blocks.len(),
        plural(blocks.len()),
        events.len(),
        plural(events.len()),
        meta.status,
        date,
    );
    out.push_str(&format!(
        "Full turn: {base}/turn/N · raw stream: {base}/raw\n",
        base = base_uri
    ));

    let mut order: Vec<usize> = (0..blocks.len()).collect();
    if latest {
        order.reverse();
    }
    let start = match turn_offset.unwrap_or(0) {
        raw if raw < 0 => blocks.len().saturating_sub(raw.unsigned_abs() as usize),
        raw => (raw as usize).min(blocks.len()),
    };
    let end = turn_limit
        .map(|limit| (start + limit).min(order.len()))
        .unwrap_or(order.len());
    let order = &order[start..end];
    for &block_index in order {
        let block = &blocks[block_index];
        let (turn_label, linkable) = match block.turn_id.and_then(|id| turn_sequences.get(id)) {
            Some(seq) => (seq.to_string(), true),
            None => ((block_index + 1).to_string(), false),
        };
        let time = chrono::DateTime::from_timestamp(
            block.events.first().map(|e| e.created_at).unwrap_or(0),
            0,
        )
        .map(|dt| dt.format("%H:%M").to_string())
        .unwrap_or_default();
        out.push_str(&format!("\n## Turn {} · {}\n", turn_label, time));
        let turn_ref = linkable.then_some(turn_label.as_str());
        render_block(&mut out, &block.events, &results, base_uri, turn_ref);
    }

    out
}

fn render_block(
    out: &mut String,
    events: &[&EventRow],
    results: &HashMap<String, ResultInfo>,
    base_uri: &str,
    turn_ref: Option<&str>,
) {
    let mut seen_user = false;
    for event in events {
        let Ok(data) = serde_json::from_str::<serde_json::Value>(&event.data) else {
            continue;
        };
        match event.event_type.as_str() {
            "user" => {
                let Some(content) = data
                    .get("content")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                else {
                    continue;
                };
                // The turn's leading user event is the user message; later user
                // events in the same turn (and any system-reminder content) are
                // injections — load-bearing base-branch updates / direct messages.
                if seen_user || content.contains("<system-reminder>") {
                    out.push_str(&format!(
                        "⚠ {}\n",
                        single_line_preview(content, ERROR_LINE_PREVIEW)
                    ));
                } else {
                    seen_user = true;
                    push_truncated(
                        out,
                        "**User:** ",
                        content,
                        USER_CONTENT_THRESHOLD,
                        USER_CONTENT_PREVIEW,
                        base_uri,
                        turn_ref,
                    );
                }
            }
            "assistant" => {
                if let Some(content) = data
                    .get("content")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                {
                    push_truncated(
                        out,
                        "**Assistant:** ",
                        content,
                        DIGEST_ASSISTANT_THRESHOLD,
                        DIGEST_ASSISTANT_PREVIEW,
                        base_uri,
                        turn_ref,
                    );
                }
                if let Some(tool_uses) = data.get("toolUses").and_then(|value| value.as_array()) {
                    for tool in tool_uses {
                        let name = tool
                            .get("name")
                            .and_then(|value| value.as_str())
                            .unwrap_or("unknown");
                        let id = tool.get("id").and_then(|value| value.as_str());
                        let result = id.and_then(|id| results.get(id));
                        let input = tool
                            .get("input")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        for row in rows_for_tool_call(name, &input, result) {
                            out.push_str(&row.render());
                            out.push('\n');
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

fn push_truncated(
    out: &mut String,
    prefix: &str,
    content: &str,
    threshold: usize,
    preview: usize,
    base_uri: &str,
    turn_ref: Option<&str>,
) {
    let (display, truncated, _original_len) = truncate_with_info(content, threshold, preview);
    out.push_str(prefix);
    out.push_str(display.trim_end());
    out.push('\n');
    if truncated {
        let total = content.lines().count();
        let shown = display.lines().count().max(1);
        let remaining = total.saturating_sub(shown);
        match turn_ref {
            Some(turn) => out.push_str(&format!(
                "[+{} lines → {}/turn/{}]\n",
                remaining, base_uri, turn
            )),
            None => out.push_str(&format!("[+{} lines]\n", remaining)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use cairn_common::query::QueryParam;

    fn param(key: &str, value: &str) -> QueryParam {
        QueryParam {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn format_single_event_renders_tool_result_without_name() {
        let data = serde_json::json!({
            "toolUseId": "toolu_1",
            "toolResult": "line 1\nline 2",
            "isError": false
        })
        .to_string();

        let rendered = format_single_event("tool_result", &data, None, None, None);

        assert!(rendered.contains("**Tool Result:**"));
        assert!(rendered.contains("line 1\nline 2"));
        assert!(!rendered.contains("No content in this event"));
    }

    #[test]
    fn format_single_event_renders_structured_error_and_empty_results() {
        let structured = serde_json::json!({
            "toolResult": { "message": "bad", "code": 7 },
            "toolName": "read",
            "isError": true
        })
        .to_string();
        let rendered = format_single_event("tool_result", &structured, None, None, None);
        assert!(rendered.contains("**Tool Result error (read):**"));
        assert!(rendered.contains("\"message\": \"bad\""));

        let empty = serde_json::json!({ "toolResult": "   " }).to_string();
        let rendered = format_single_event("tool_result", &empty, None, None, None);
        assert!(rendered.contains("(empty result)"));
    }

    #[tokio::test]
    async fn load_turn_events_reconstructs_archived_zstd() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.keep().join("transcript-archival.db");
        let db = LocalDb::open(path).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        let original = "{\"eventType\":\"user\",\"content\":\"archived turn content\"}";
        let blob = crate::archival::compress(original.as_bytes()).unwrap();
        db.write(move |conn| {
            let blob = blob.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO runs(id, status, created_at, updated_at) VALUES ('run','exited',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns(id, session_id, run_id, sequence, created_at, updated_at)
                     VALUES ('turn-1','sess','run',1,1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id, storage_mode, data_blob, codec)
                     VALUES ('ev','run',1,1,'user','{\"_archived\":true}',1,'turn-1','zstd',?1,'zstd_v1')",
                    (blob,),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let rows = db
            .read(|conn| {
                Box::pin(async move {
                    Ok::<Vec<(String, i32, String, String)>, crate::storage::DbError>(
                        load_turn_events(conn, "turn-1", None, None).await,
                    )
                })
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        // The stub `data` was reconstructed back to the original event JSON.
        assert_eq!(rows[0].3, original);
    }

    /// Parity guard for the deleted `grep_transcript`: the universal grep over a
    /// rendered transcript body selects the same matched-line *contents* the old
    /// transcript grep did for `grep`/`-i`/`context` and for
    /// `grep`/`-i`/`head_limit` (without context). The output format gains a
    /// line-number prefix and `head_limit` now caps output lines rather than
    /// matches; both are intentional unifications, so parity is over content.
    #[test]
    fn universal_grep_matches_old_transcript_selection() {
        use crate::mcp::handlers::search::{build_grep_payload, grep_materialized_body};

        let transcript = "alpha\nBeta match\ngamma\nsecond MATCH\nomega";

        // Strip the path-less `N:`/`N-` prefix to recover line contents.
        fn contents(body: &str) -> Vec<String> {
            body.lines()
                .filter(|line| *line != "--")
                .map(|line| {
                    let rest = line.trim_start_matches(|c: char| c.is_ascii_digit());
                    rest.get(1..).unwrap_or("").to_string()
                })
                .collect()
        }

        // grep + -i + context=1: both matches selected, every line in context.
        let payload = build_grep_payload(
            &[param("-i", ""), param("-C", "1")],
            "match".to_string(),
            None,
            None,
            Some("content".to_string()),
            None,
            None,
        )
        .unwrap();
        let (rendered, matches) = grep_materialized_body(transcript, &payload);
        assert_eq!(matches, 2);
        assert_eq!(
            contents(&rendered),
            vec!["alpha", "Beta match", "gamma", "second MATCH", "omega"],
        );

        // grep + -i + head_limit=1 (no context): only the first matched line.
        let payload = build_grep_payload(
            &[param("-i", ""), param("head_limit", "1")],
            "match".to_string(),
            None,
            None,
            Some("content".to_string()),
            None,
            None,
        )
        .unwrap();
        let (rendered, _matches) = grep_materialized_body(transcript, &payload);
        assert_eq!(contents(&rendered), vec!["Beta match"]);

        // No matches: the shared finalizer's message.
        let payload = build_grep_payload(
            &[],
            "zeta".to_string(),
            None,
            None,
            Some("content".to_string()),
            None,
            None,
        )
        .unwrap();
        let (rendered, matches) = grep_materialized_body(transcript, &payload);
        assert_eq!(matches, 0);
        assert_eq!(rendered, "No matches found for pattern 'zeta'");
    }

    #[tokio::test]
    async fn get_single_event_resolves_tool_result_name() {
        let db = LocalDb::open(tempfile::tempdir().unwrap().keep().join("transcript.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        conn.execute(
            "INSERT INTO runs (id, created_at, updated_at) VALUES ('run-1', 1, 1)",
            (),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
             VALUES ('event-1', 'run-1', 1, 1, 'assistant', ?1, 1)",
            (serde_json::json!({
                "toolUses": [{ "id": "toolu_1", "name": "read", "input": { "path": "src/lib.rs" } }]
            })
            .to_string(),),
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
             VALUES ('event-2', 'run-1', 2, 2, 'tool_result', ?1, 2)",
            (serde_json::json!({ "toolUseId": "toolu_1", "toolResult": "body" }).to_string(),),
        )
        .await
        .unwrap();

        let rendered = get_single_event(&conn, "run-1", 2, None, None).await;

        assert!(rendered.contains("**Tool Result (read):**"));
        assert!(rendered.contains("body"));
    }

    fn digest_ev(seq: i32, turn: &str, kind: &str, data: serde_json::Value) -> EventRow {
        EventRow {
            run_id: "run-1".to_string(),
            sequence: seq,
            event_type: kind.to_string(),
            data: data.to_string(),
            turn_id: Some(turn.to_string()),
            created_at: 1_700_000_000 + seq as i64,
        }
    }

    fn digest_meta() -> DigestMeta<'static> {
        DigestMeta {
            label: "builder",
            project: "CAIRN",
            number: 1666,
            exec_seq: 1,
            status: "complete",
        }
    }

    #[test]
    fn digest_renders_header_turns_and_read_rows() {
        let events = vec![
            digest_ev(
                0,
                "t1",
                "user",
                serde_json::json!({ "content": "do the thing" }),
            ),
            digest_ev(
                1,
                "t1",
                "assistant",
                serde_json::json!({
                    "content": "Working on it",
                    "toolUses": [{
                        "id": "r1", "name": "mcp__cairn__read",
                        "input": { "paths": ["file:src/lib.rs", "file:jobs.rs?grep=foo&-C=30"] }
                    }]
                }),
            ),
            digest_ev(
                2,
                "t1",
                "tool_result",
                serde_json::json!({
                    "toolUseId": "r1",
                    "toolResult": "=== file:src/lib.rs [lines 1–50 of 50] ===\nbody\n=== file:jobs.rs [2 matches] ===\nm1\nm2"
                }),
            ),
        ];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        let out = format_transcript_digest(
            &events,
            "cairn://p/CAIRN/1666/1/builder/chat",
            &digest_meta(),
            &turns,
            false,
            None,
            None,
        );
        assert!(out.contains("# builder — CAIRN-1666/1 · 1 run · 1 turn · 3 events · complete"));
        assert!(out.contains("raw stream: cairn://p/CAIRN/1666/1/builder/chat/raw"));
        assert!(out.contains("## Turn 1 · "));
        assert!(out.contains("**Assistant:** Working on it"));
        assert!(out.contains("· read src/lib.rs [50 lines]"));
        assert!(out.contains("· read jobs.rs ?grep=foo -C=30 [2 matches]"));
    }

    #[test]
    fn digest_write_rows_show_diffstat_and_commit() {
        let events = vec![
            digest_ev(
                0,
                "t1",
                "assistant",
                serde_json::json!({
                    "toolUses": [{
                        "id": "w1", "name": "mcp__cairn__write",
                        "input": { "changes": [
                            { "target": "file:src/app.rs", "mode": "patch", "payload": { "diff": "@@\n+added one\n+added two\n-removed" } },
                            { "target": "cairn:~/todos", "mode": "replace", "payload": {} }
                        ]}
                    }]
                }),
            ),
            digest_ev(
                1,
                "t1",
                "tool_result",
                serde_json::json!({
                    "toolUseId": "w1",
                    "toolResult": serde_json::json!({
                        "applied": [
                            { "index": 0, "target": "file:src/app.rs", "mode": "patch", "kind": "file", "summary": "~src/app.rs" },
                            { "index": 1, "target": "cairn:~/todos", "mode": "replace", "kind": "resource", "summary": "Replaced 3 todos for builder (0 completed)" }
                        ],
                        "commit": { "status": "committed", "sha": "6102407abcdef" }
                    }).to_string()
                }),
            ),
        ];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        let out =
            format_transcript_digest(&events, "base", &digest_meta(), &turns, false, None, None);
        assert!(out.contains("· write src/app.rs (+2 −1)"));
        assert!(out.contains("[committed 6102407]"));
        assert!(out.contains("· write todos — Replaced 3 todos for builder (0 completed)"));
    }

    #[test]
    fn digest_run_row_shows_exit_status() {
        let events = vec![
            digest_ev(
                0,
                "t1",
                "assistant",
                serde_json::json!({
                    "toolUses": [{ "id": "c1", "name": "mcp__cairn__run", "input": { "commands": [{ "command": "cargo test" }] } }]
                }),
            ),
            digest_ev(
                1,
                "t1",
                "tool_result",
                serde_json::json!({ "toolUseId": "c1", "toolResult": "running tests\nExit code: 0" }),
            ),
        ];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        let out =
            format_transcript_digest(&events, "base", &digest_meta(), &turns, false, None, None);
        assert!(out.contains("· run cargo test [exit 0]"));
    }

    #[test]
    fn digest_error_row_inlines_first_result_line() {
        let events = vec![
            digest_ev(
                0,
                "t1",
                "assistant",
                serde_json::json!({
                    "toolUses": [{ "id": "c1", "name": "mcp__cairn__run", "input": { "commands": [{ "command": "git rebase origin/main" }] } }]
                }),
            ),
            digest_ev(
                1,
                "t1",
                "tool_result",
                serde_json::json!({ "toolUseId": "c1", "isError": true, "toolResult": "Failed to spawn process: No such file or directory" }),
            ),
        ];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        let out =
            format_transcript_digest(&events, "base", &digest_meta(), &turns, false, None, None);
        assert!(out.contains(
            "✗ run git rebase origin/main — Failed to spawn process: No such file or directory"
        ));
    }

    #[test]
    fn digest_user_message_truncates_with_turn_link() {
        let big = (1..=300)
            .map(|i| format!("this is line {i} of the user message plan replay"))
            .collect::<Vec<_>>()
            .join("\n");
        let events = vec![digest_ev(
            0,
            "t1",
            "user",
            serde_json::json!({ "content": big }),
        )];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        let out = format_transcript_digest(
            &events,
            "cairn://p/CAIRN/1666/1/builder/chat",
            &digest_meta(),
            &turns,
            false,
            None,
            None,
        );
        assert!(out.contains("**User:**"));
        assert!(out.contains("→ cairn://p/CAIRN/1666/1/builder/chat/turn/1]"));
    }

    #[test]
    fn digest_latest_reverses_turn_order() {
        let events = vec![
            digest_ev(0, "t1", "user", serde_json::json!({ "content": "first" })),
            digest_ev(1, "t2", "user", serde_json::json!({ "content": "second" })),
        ];
        let turns =
            std::collections::HashMap::from([("t1".to_string(), 1i32), ("t2".to_string(), 2i32)]);
        let normal =
            format_transcript_digest(&events, "base", &digest_meta(), &turns, false, None, None);
        let latest =
            format_transcript_digest(&events, "base", &digest_meta(), &turns, true, None, None);
        assert!(normal.find("## Turn 1").unwrap() < normal.find("## Turn 2").unwrap());
        assert!(latest.find("## Turn 2").unwrap() < latest.find("## Turn 1").unwrap());
    }

    #[test]
    fn digest_offset_and_limit_page_whole_turns() {
        let events = vec![
            digest_ev(0, "t1", "user", serde_json::json!({ "content": "first" })),
            digest_ev(1, "t2", "user", serde_json::json!({ "content": "second" })),
            digest_ev(2, "t3", "user", serde_json::json!({ "content": "third" })),
        ];
        let turns = std::collections::HashMap::from([
            ("t1".to_string(), 1i32),
            ("t2".to_string(), 2i32),
            ("t3".to_string(), 3i32),
        ]);

        let out = format_transcript_digest(
            &events,
            "base",
            &digest_meta(),
            &turns,
            false,
            Some(1),
            Some(1),
        );

        assert!(!out.contains("## Turn 1"));
        assert!(out.contains("## Turn 2"));
        assert!(!out.contains("## Turn 3"));
        assert!(out.contains("3 turns"));
    }

    #[test]
    fn digest_unpaired_call_renders_pending() {
        let events = vec![digest_ev(
            0,
            "t1",
            "assistant",
            serde_json::json!({
                "toolUses": [{ "id": "r1", "name": "mcp__cairn__read", "input": { "paths": ["file:src/x.rs"] } }]
            }),
        )];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        let out =
            format_transcript_digest(&events, "base", &digest_meta(), &turns, false, None, None);
        assert!(out.contains("· read src/x.rs [pending]"));
    }
}
