//! Transcript loading and compact resource formatting.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use cairn_db::turso::params;
use cairn_llm::continuation::{
    self, cleaned_tool_name, count_prefixed_diff_lines, figure_for_read_result,
    figure_for_run_result, first_nonempty_line, first_result_line, json_value_to_display,
    single_line_preview, summarize_scalar, summarize_tool_input, DigestOptions, DigestRow, Entry,
    Marker, RowContext, ToolCall, ToolOutcome, ToolRenderers, Transcript, Turn, ERROR_LINE_PREVIEW,
    ROW_LABEL_PREVIEW,
};
use serde::Serialize;

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

/// Load the raw transcript content for one immutable era span.
///
/// The summary model benefits from the event payloads rather than the compact
/// UI projection: user and assistant prose remain intact, while tool events
/// retain the target and outcome that made the turn meaningful.
pub(crate) async fn era_source_digest(
    db: &crate::storage::LocalDb,
    job_id: &str,
    start_block: i64,
    end_block: i64,
) -> Result<String, String> {
    let conn = db.connect().await.map_err(|error| error.to_string())?;
    let events = load_job_events_ordered(
        &conn,
        job_id,
        db.team_id().map(|_| db.content_store().as_ref()),
        db.private_route_db().map(|db| db.as_ref()),
    )
    .await;
    let blocks = group_turn_blocks(&events);
    let start = start_block.max(0) as usize;
    let end = (end_block.max(start_block).max(0) as usize).min(blocks.len().saturating_sub(1));
    let mut digest = String::new();
    for (index, block) in blocks
        .iter()
        .enumerate()
        .take(end.saturating_add(1))
        .skip(start)
    {
        digest.push_str(&format!("\n## Turn {}\n", index + 1));
        for event in &block.events {
            digest.push_str(&event.event_type);
            digest.push_str(": ");
            digest.push_str(&event.data);
            digest.push('\n');
        }
    }
    // An era is normally bounded by fold policy. This is a final guard for
    // legacy first-pass spans and pathological tool output.
    digest.truncate(digest.floor_char_boundary(96_000));
    Ok(digest)
}

impl EventRow {
    fn to_transcript_row(&self) -> crate::transcripts::TranscriptRow {
        (
            self.run_id.clone(),
            self.sequence,
            self.event_type.clone(),
            self.data.clone(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RawTranscriptFormat {
    Markdown,
    Json,
}

pub(super) fn parse_raw_transcript_format(
    params: &[cairn_common::query::QueryParam],
) -> Result<RawTranscriptFormat, String> {
    let unexpected: Vec<&str> = params
        .iter()
        .filter(|param| param.key != "format")
        .map(|param| param.key.as_str())
        .collect();
    if !unexpected.is_empty() {
        return Err(format!(
            "Query parameters are not supported on raw transcript resources: {}",
            unexpected.join(", ")
        ));
    }

    match params.iter().rev().find(|param| param.key == "format") {
        None => Ok(RawTranscriptFormat::Markdown),
        Some(param) if param.value == "json" => Ok(RawTranscriptFormat::Json),
        Some(param) => Err(format!(
            "Unsupported raw transcript format '{}'; expected json",
            param.value
        )),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StructuredTranscriptEvent<'a> {
    run_id: &'a str,
    sequence: i32,
    turn_id: Option<&'a str>,
    event_type: &'a str,
    created_at: i64,
    payload: serde_json::Value,
}

/// Render reconstructed transcript rows for the raw resource. JSON mode is
/// JSONL: one complete event value per line, so the shared line window pages by
/// events and never applies digest preview/truncation rules to event payloads.
pub(super) fn format_raw_transcript(events: &[EventRow], format: RawTranscriptFormat) -> String {
    match format {
        RawTranscriptFormat::Markdown => {
            let rows: Vec<crate::transcripts::TranscriptRow> =
                events.iter().map(EventRow::to_transcript_row).collect();
            crate::transcripts::format_transcript_full(&rows)
        }
        RawTranscriptFormat::Json => events
            .iter()
            .map(|event| {
                let payload = serde_json::from_str(&event.data)
                    .unwrap_or_else(|_| serde_json::Value::String(event.data.clone()));
                serde_json::to_string(&StructuredTranscriptEvent {
                    run_id: &event.run_id,
                    sequence: event.sequence,
                    turn_id: event.turn_id.as_deref(),
                    event_type: &event.event_type,
                    created_at: event.created_at,
                    payload,
                })
                .expect("structured transcript event serialization cannot fail")
            })
            .collect::<Vec<_>>()
            .join("\n"),
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
    conn: &cairn_db::turso::Connection,
    job_id: &str,
    store: Option<&dyn crate::storage::ContentStore>,
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

    crate::storage::events::reconstruct::reconstruct_events_with_conn_and_routes(
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
    conn: &cairn_db::turso::Connection,
    turn_id: &str,
    store: Option<&dyn crate::storage::ContentStore>,
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
    crate::storage::events::reconstruct::reconstruct_events_with_conn_and_routes(
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
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
    run_id: &str,
    event_seq: i32,
    store: Option<&dyn crate::storage::ContentStore>,
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

    let mut events = crate::storage::events::reconstruct::reconstruct_events_with_conn_and_routes(
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
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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
        crate::transcripts::CONTINUATION_EVENT_TYPE => {
            output.push_str(crate::transcripts::CONTINUATION_MARKER_LINE);
            output.push_str("\n\n");
        }
        crate::transcripts::LAUNCH_EVENT_TYPE => {
            // Addressing one event by id is a deliberate request for that exact
            // event, so this projection keeps the body and corrects only the
            // speaker (CAIRN-3408).
            if let Some(content) = event_data.get("content").and_then(|c| c.as_str()) {
                if !content.is_empty() {
                    output.push_str(crate::transcripts::LAUNCH_LABEL);
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
            output.push_str(&format!("- Applied: {}\n", crate::clock::age(applied_at)));
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

// Inline write-diff body (opt-in `diffs=true`): the per-write body cap with an
// explicit truncation marker, and the create/replace/append content preview
// window before a `[+K more lines]` marker so a large new file can't flood the
// digest.
const WRITE_BODY_MAX_LINES: usize = 40;
const WRITE_CONTENT_PREVIEW_LINES: usize = 20;

// ============================================================================
// Digest renderer — the default `/chat` projection
// ============================================================================
//
// The digest is the text projection of the frontend's flat-row tool model
// (docs/tool-call-presentation.md): turn-structured, one row per target, a
// right-pinned count figure, no inline JSON bodies. Drill-down is `chat/turn/N`
// (advertised in the header and each turn heading); the verbose stream is at
// `chat/raw`.
//
// The turn renderer itself is `cairn_llm::continuation::render_digest`. What is
// Cairn's lives here: the header line, the mapping from stored events onto the
// facet's neutral transcript, and the row builders for Cairn's own tools.

pub(super) struct DigestMeta<'a> {
    pub label: &'a str,
    pub project: &'a str,
    pub coordinate: DigestCoordinate<'a>,
    pub status: &'a str,
}

/// How a transcript header names the work it belongs to.
///
/// The coordinate is the reader's way back to the surrounding work, so it has
/// to be an address they can actually follow. Threads have no issue number or
/// execution seq; rendering them through the issue shape produced the literal
/// `CAIRN-0/0`, which names nothing and reads as a real coordinate.
#[derive(Clone, Copy)]
pub(super) enum DigestCoordinate<'a> {
    /// An issue node or one of its tasks: `CAIRN-3744/2`.
    Issue { number: i32, exec_seq: i32 },
    /// A thread session or one of its tasks: `CAIRN/thread-ux`.
    Thread { name: &'a str },
    /// No addressable coordinate. The cold-resume seed holds only the job, and
    /// its header is explicitly cosmetic.
    Unaddressed,
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

fn collect_results(events: &[EventRow]) -> HashMap<String, ToolOutcome> {
    let mut map: HashMap<String, ToolOutcome> = HashMap::new();
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
        let content = data
            .get("toolResult")
            .map(json_value_to_display)
            .unwrap_or_default();
        map.insert(id, ToolOutcome { content, is_error });
    }
    map
}

fn read_rows(call: &ToolCall, _ctx: &RowContext) -> Vec<DigestRow> {
    let input = &call.input;
    let result = call.outcome.as_ref();
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    let error_detail = result
        .filter(|r| r.is_error)
        .map(|r| first_result_line(&r.content));
    let obj = input.as_object();

    if let Some(paths) = obj
        .and_then(|o| o.get("paths"))
        .and_then(|value| value.as_array())
    {
        let sections = result
            .filter(|r| !r.is_error)
            .map(|r| read_sections(&r.content))
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
                body: None,
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
        .and_then(|r| figure_for_read_result(&r.content));
    vec![DigestRow {
        error: is_error,
        text: format!("read {}", render_read_target(path)),
        metric,
        detail: error_detail,
        trailer: None,
        body: None,
    }]
}

fn write_rows(call: &ToolCall, ctx: &RowContext) -> Vec<DigestRow> {
    let input = &call.input;
    let result = call.outcome.as_ref();
    let inline_diffs = ctx.inline_diffs;
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    let report = result.and_then(|r| serde_json::from_str::<serde_json::Value>(&r.content).ok());

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
                .map(|r| first_result_line(&r.content)),
            trailer: commit_trailer,
            body: None,
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
                body: None,
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
        // Under `diffs=true` a file write also renders its change body beneath
        // the row so a reseed reader can reconstruct the edit; resource writes
        // keep their summary-only rendering.
        let body = if is_file && inline_diffs {
            write_body_for_change(mode, change.get("payload"))
        } else {
            None
        };
        rows.push(DigestRow {
            error: false,
            text: format!("write {}", short),
            metric,
            detail,
            trailer: None,
            body,
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
                body: None,
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
            body: None,
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

/// Build the inline change body for a file-target write row under `diffs=true`:
/// a capped, reconstructable rendering of the edit. `diff`/`patch` payloads
/// render as-is; `old_string`/`new_string` render as a `-`/`+` block;
/// create/replace/append `content` renders as a `+`-prefixed preview. Returns
/// `None` for a payload shape with nothing to render.
fn write_body_for_change(mode: &str, payload: Option<&serde_json::Value>) -> Option<String> {
    let payload = payload?;
    if let Some(diff) = payload.get("diff").and_then(|value| value.as_str()) {
        return Some(cap_body_lines(
            diff.trim_end_matches('\n'),
            WRITE_BODY_MAX_LINES,
        ));
    }
    if let Some(patch) = payload.get("patch").and_then(|value| value.as_str()) {
        return Some(cap_body_lines(
            patch.trim_end_matches('\n'),
            WRITE_BODY_MAX_LINES,
        ));
    }
    if let (Some(old), Some(new)) = (
        payload.get("old_string").and_then(|value| value.as_str()),
        payload.get("new_string").and_then(|value| value.as_str()),
    ) {
        let mut block = String::new();
        for line in old.lines() {
            block.push_str("- ");
            block.push_str(line);
            block.push('\n');
        }
        for line in new.lines() {
            block.push_str("+ ");
            block.push_str(line);
            block.push('\n');
        }
        return Some(cap_body_lines(
            block.trim_end_matches('\n'),
            WRITE_BODY_MAX_LINES,
        ));
    }
    if matches!(mode, "create" | "replace" | "append") {
        if let Some(content) = payload.get("content").and_then(|value| value.as_str()) {
            return Some(preview_added_content(content));
        }
    }
    None
}

/// Preview added content (create/replace/append) as the first
/// `WRITE_CONTENT_PREVIEW_LINES` lines prefixed `+`, then a `[+K more lines]`
/// marker so a large new file can't flood the digest.
fn preview_added_content(content: &str) -> String {
    let total = content.lines().count();
    let mut out = String::new();
    for line in content.lines().take(WRITE_CONTENT_PREVIEW_LINES) {
        out.push_str("+ ");
        out.push_str(line);
        out.push('\n');
    }
    let remaining = total.saturating_sub(total.min(WRITE_CONTENT_PREVIEW_LINES));
    if remaining > 0 {
        out.push_str(&format!("[+{} more lines]", remaining));
    }
    out.trim_end_matches('\n').to_string()
}

/// Cap a rendered body to `max` lines, appending an explicit `[+K more lines]`
/// truncation marker when it overflows.
fn cap_body_lines(body: &str, max: usize) -> String {
    let total = body.lines().count();
    if total <= max {
        return body.to_string();
    }
    let mut out = body.lines().take(max).collect::<Vec<_>>().join("\n");
    let remaining = total.saturating_sub(max);
    out.push('\n');
    out.push_str(&format!("[+{} more lines]", remaining));
    out
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

fn run_rows(call: &ToolCall, _ctx: &RowContext) -> Vec<DigestRow> {
    let input = &call.input;
    let result = call.outcome.as_ref();
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    let error_detail = result
        .filter(|r| r.is_error)
        .map(|r| first_result_line(&r.content));

    if let Some(commands) = input
        .as_object()
        .and_then(|o| o.get("commands"))
        .and_then(|value| value.as_array())
    {
        let sections = result
            .map(|r| read_sections(&r.content))
            .unwrap_or_default();
        let single = commands.len() == 1;
        let mut rows = Vec::new();
        for (index, command) in commands.iter().enumerate() {
            let metric = if is_error {
                None
            } else if let Some((_, body)) = sections.get(index) {
                figure_for_run_result(body)
            } else if single {
                result.and_then(|r| figure_for_run_result(&r.content))
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
                body: None,
            });
        }
        if !rows.is_empty() {
            return rows;
        }
    }

    let label = command_label(input);
    let metric = result
        .filter(|r| !r.is_error)
        .and_then(|r| figure_for_run_result(&r.content));
    vec![DigestRow {
        error: is_error,
        text: format!("run {}", single_line_preview(&label, ROW_LABEL_PREVIEW)),
        metric,
        detail: error_detail,
        trailer: None,
        body: None,
    }]
}

fn task_rows(call: &ToolCall, _ctx: &RowContext) -> Vec<DigestRow> {
    let input = &call.input;
    let result = call.outcome.as_ref();
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
            .map(|r| first_result_line(&r.content)),
        trailer: None,
        body: None,
    }]
}

/// `→ <target> <content>`, how a `message` call reads in a row. Only the exact
/// `message` name gets it; any other spelling keeps the fallback summary.
fn message_summary(name: &str, input: &serde_json::Value) -> Option<String> {
    let obj = input.as_object()?;
    if cleaned_tool_name(name) != "message" {
        return summarize_tool_input(name, input);
    }
    obj.get("content").and_then(|value| {
        summarize_scalar(value).map(|content| match obj.get("to").and_then(|v| v.as_str()) {
            Some(target) if !target.is_empty() => format!("→ {} {}", target, content),
            _ => format!("→ project {}", content),
        })
    })
}

fn message_rows(call: &ToolCall, _ctx: &RowContext) -> Vec<DigestRow> {
    let result = call.outcome.as_ref();
    let is_error = result.map(|r| r.is_error).unwrap_or(false);
    let summary = message_summary(&call.name, &call.input).unwrap_or_default();
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
            .map(|r| first_result_line(&r.content)),
        trailer: None,
        body: None,
    }]
}

/// The row builders for Cairn's own tools, keyed the way
/// [`ToolRenderers`] folds names (`mcp__cairn__read`, `Read`, and `read` all
/// reach `read`). Every other tool renders through the facet's generic
/// fallback.
fn cairn_tool_renderers() -> &'static ToolRenderers {
    static RENDERERS: LazyLock<ToolRenderers> = LazyLock::new(|| {
        let mut tools = ToolRenderers::new();
        tools.register("read", read_rows);
        for name in ["write", "change", "filechange"] {
            tools.register(name, write_rows);
        }
        for name in ["run", "bash"] {
            tools.register(name, run_rows);
        }
        tools.register("task", task_rows);
        tools.register("message", message_rows);
        tools
    });
    &RENDERERS
}

/// Default-options entry retained only for the `digest_*` unit tests, which
/// assert the no-fidelity-flags output is byte-identical. Production reads call
/// [`format_transcript_digest_with`] directly with parsed options.
#[cfg(test)]
fn format_transcript_digest(
    events: &[EventRow],
    base_uri: &str,
    meta: &DigestMeta,
    turn_sequences: &HashMap<String, i32>,
    latest: bool,
    turn_offset: Option<i64>,
    turn_limit: Option<usize>,
) -> String {
    format_transcript_digest_with(
        events,
        base_uri,
        meta,
        turn_sequences,
        crate::clock::host(),
        &DigestOptions {
            latest,
            turn_offset,
            turn_limit,
            unabridged: false,
            inline_diffs: false,
        },
    )
}

/// One rendered turn of a transcript: the events sharing a `turn_id`, with any
/// turn-less events folded into the block they follow.
///
/// This grouping is the job's single chronological turn sequence, and a block's
/// index in it is the coordinate the `/chat?offset=&limit=` re-read address
/// pages by. Thread compaction addresses its dropped chapters that way, so the
/// grouping has exactly one definition rather than one per consumer.
pub(super) struct TurnBlock<'a> {
    pub turn_id: Option<&'a str>,
    pub events: Vec<&'a EventRow>,
}

pub(super) fn group_turn_blocks(events: &[EventRow]) -> Vec<TurnBlock<'_>> {
    let mut blocks: Vec<TurnBlock> = Vec::new();
    for event in events {
        let key = event.turn_id.as_deref();
        match blocks.last_mut() {
            Some(block) if key.is_none() || block.turn_id == key => block.events.push(event),
            _ => blocks.push(TurnBlock {
                turn_id: key,
                events: vec![event],
            }),
        }
    }
    blocks
}

pub(super) fn format_transcript_digest_with(
    events: &[EventRow],
    base_uri: &str,
    meta: &DigestMeta,
    turn_sequences: &HashMap<String, i32>,
    clock: &crate::clock::HostClock,
    opts: &DigestOptions,
) -> String {
    if events.is_empty() {
        return "No runs found for this node.".to_string();
    }

    let transcript = transcript_from_rows(events, base_uri, turn_sequences);

    let run_count = {
        let mut seen: Vec<&str> = Vec::new();
        for event in events {
            if !seen.contains(&event.run_id.as_str()) {
                seen.push(event.run_id.as_str());
            }
        }
        seen.len()
    };
    // The heading is dated by the first event's local day, which is also the
    // day `render_digest` seeds its per-turn rollover from: a turn on a
    // different local day than this heading states its own date rather than
    // silently inheriting one that no longer applies.
    let date = clock
        .local_date(events[0].created_at)
        .map(|date| date.format("%Y-%m-%d").to_string())
        .unwrap_or_default();

    let coordinate = match meta.coordinate {
        DigestCoordinate::Issue { number, exec_seq } => {
            format!(" — {}/{}/{}", meta.project, number, exec_seq)
        }
        DigestCoordinate::Thread { name } => format!(" — {}/{}", meta.project, name),
        DigestCoordinate::Unaddressed => String::new(),
    };
    let turns = transcript.turns.len();
    let mut out = format!(
        "# {}{} · {} run{} · {} turn{} · {} event{} · {} · {}\n",
        meta.label,
        coordinate,
        run_count,
        plural(run_count),
        turns,
        plural(turns),
        events.len(),
        plural(events.len()),
        meta.status,
        date,
    );
    out.push_str(&format!(
        "Full turn: {base}/turn/N · raw stream: {base}/raw\n",
        base = base_uri
    ));
    out.push_str(&continuation::render_digest(
        &transcript,
        cairn_tool_renderers(),
        clock,
        opts,
    ));
    out
}

/// Cairn's stored events as the facet's neutral [`Transcript`]: one turn per
/// [`group_turn_blocks`] block, including a block whose events all render
/// nothing, so a turn's position stays the `/chat?offset=` coordinate.
///
/// A turn is numbered and linked (`{base_uri}/turn/{seq}`) only when its
/// `turn_id` is in `turn_sequences`. Tool results join their calls from one
/// pass over the whole slice, so a result stored in a later block still pairs.
/// A result that pairs with no call stays where it occurred, as a marker the
/// digest does not render and the verbatim measure counts: native resume
/// re-sends it all the same.
pub(super) fn transcript_from_rows(
    events: &[EventRow],
    base_uri: &str,
    turn_sequences: &HashMap<String, i32>,
) -> Transcript {
    let results = collect_results(events);
    let call_ids = tool_call_ids(events);
    let turns = group_turn_blocks(events)
        .iter()
        .map(|block| {
            let number = block
                .turn_id
                .and_then(|id| turn_sequences.get(id))
                .map(|seq| i64::from(*seq));
            Turn {
                number,
                link: number.map(|seq| format!("{base_uri}/turn/{seq}")),
                started_at: block.events.first().map(|event| event.created_at),
                entries: block
                    .events
                    .iter()
                    .flat_map(|event| entries_for_event(event, &results, &call_ids))
                    .collect(),
            }
        })
        .collect();
    Transcript { turns }
}

fn tool_call_ids(events: &[EventRow]) -> HashSet<String> {
    events
        .iter()
        .filter(|event| event.event_type == "assistant")
        .filter_map(|event| serde_json::from_str::<serde_json::Value>(&event.data).ok())
        .flat_map(|data| {
            data.get("toolUses")
                .and_then(|value| value.as_array())
                .cloned()
                .unwrap_or_default()
        })
        .filter_map(|tool| {
            tool.get("id")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .collect()
}

fn entries_for_event(
    event: &EventRow,
    results: &HashMap<String, ToolOutcome>,
    call_ids: &HashSet<String>,
) -> Vec<Entry> {
    let Ok(data) = serde_json::from_str::<serde_json::Value>(&event.data) else {
        return Vec::new();
    };
    let marker = |skim: Option<&str>, full: Option<String>, verbatim: Option<String>| {
        vec![Entry::Marker(Marker {
            skim: skim.map(str::to_string),
            full,
            verbatim,
        })]
    };
    match event.event_type.as_str() {
        "user" => match content_of(&data) {
            "" => Vec::new(),
            content => vec![Entry::User(content.to_string())],
        },
        "assistant" => {
            let mut entries = Vec::new();
            let content = content_of(&data);
            if !content.is_empty() {
                entries.push(Entry::Assistant(content.to_string()));
            }
            if let Some(tool_uses) = data.get("toolUses").and_then(|value| value.as_array()) {
                for tool in tool_uses {
                    let id = tool.get("id").and_then(|value| value.as_str());
                    entries.push(Entry::Tool(ToolCall {
                        id: id.unwrap_or_default().to_string(),
                        name: tool
                            .get("name")
                            .and_then(|value| value.as_str())
                            .unwrap_or("unknown")
                            .to_string(),
                        input: tool
                            .get("input")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null),
                        outcome: id.and_then(|id| results.get(id)).cloned(),
                    }));
                }
            }
            entries
        }
        // A seed marker (CAIRN-2534): one line at every fidelity, so
        // `messages=full` never re-expands the embedded prior context and a
        // chain of reseeds stays bounded. The label is deliberately neutral
        // about what produced the seed — a stale session's digest, or a thread
        // compaction that may have fired while the session was still warm. The
        // verbatim measure has never counted it.
        "user:seed" => marker(Some("· [prior context compacted]"), None, None),
        // Cairn's own resume nudge (CAIRN-3175). A marker at every fidelity,
        // `messages=full` included, because there is no operator message here to
        // render faithfully, and a turn that opened this way must never read as
        // one the operator started.
        crate::transcripts::CONTINUATION_EVENT_TYPE => marker(
            Some(crate::transcripts::CONTINUATION_MARKER_LINE),
            None,
            Some(crate::transcripts::CONTINUATION_MARKER_LINE.to_string()),
        ),
        // The prompt the job was launched on (CAIRN-3408). Unlike a seed, its
        // body is not recursive, so the fidelity knob decides.
        //
        // The skim digest collapses it. That is the projection a watching parent
        // receives inlined in its catch-up ride-along, and turn 1 there is the
        // issue description that same parent wrote when it filed the child — its
        // own words quoted back at it, once per turn, forever under a long-lived
        // thread.
        //
        // `messages=full` expands it verbatim, which is what a session rebuilt
        // from a reseed or thread-compaction digest reads to recover the task it
        // was given. The marker stands even for an empty prompt: the launch turn
        // happened either way, and a silently absent turn would misreport the
        // transcript's shape.
        crate::transcripts::LAUNCH_EVENT_TYPE => {
            let content = content_of(&data);
            let body = !content.is_empty();
            marker(
                Some(crate::transcripts::LAUNCH_MARKER_LINE),
                body.then(|| format!("{}{}", crate::transcripts::LAUNCH_LABEL, content.trim_end())),
                body.then(|| format!("{}{content}", crate::transcripts::LAUNCH_LABEL)),
            )
        }
        kind @ ("system:session_boundary" | "system:compact_boundary") => marker(
            None,
            None,
            crate::transcripts::system_boundary_line(kind, &data),
        ),
        "result" | "tool_result" => {
            let paired = tool_use_id_from_event(&data).is_some_and(|id| call_ids.contains(id));
            match data.get("toolResult") {
                Some(result) if !paired => {
                    let name = data
                        .get("toolName")
                        .and_then(|value| value.as_str())
                        .unwrap_or("tool");
                    marker(
                        None,
                        None,
                        Some(format!(
                            "**Tool Result ({name}):**\n{}",
                            json_value_to_display(result)
                        )),
                    )
                }
                _ => Vec::new(),
            }
        }
        _ => Vec::new(),
    }
}

/// The `content` string of a user-slot event payload, or empty when absent.
fn content_of(data: &serde_json::Value) -> &str {
    data.get("content")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
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

    fn raw_event(
        sequence: i32,
        turn_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> EventRow {
        EventRow {
            run_id: "run-1".to_string(),
            sequence,
            event_type: event_type.to_string(),
            data: payload.to_string(),
            turn_id: Some(turn_id.to_string()),
            created_at: 1_700_000_000 + i64::from(sequence),
        }
    }

    #[test]
    fn structured_raw_transcript_preserves_original_tool_input() {
        let input = serde_json::json!({
            "commands": [{
                "command": "cargo test -p cairn-core resources::transcript::tests",
                "description": "Run transcript tests"
            }],
            "sequential": true
        });
        let events = vec![raw_event(
            7,
            "turn-2",
            "assistant",
            serde_json::json!({
                "content": "running tests",
                "toolUses": [{ "id": "toolu_1", "name": "run", "input": input.clone() }]
            }),
        )];

        let rendered = format_raw_transcript(&events, RawTranscriptFormat::Json);
        let event: serde_json::Value = serde_json::from_str(&rendered).unwrap();

        assert_eq!(event["runId"], "run-1");
        assert_eq!(event["sequence"], 7);
        assert_eq!(event["turnId"], "turn-2");
        assert_eq!(event["eventType"], "assistant");
        assert_eq!(event["createdAt"], 1_700_000_007i64);
        assert_eq!(event["payload"]["toolUses"][0]["id"], "toolu_1");
        assert_eq!(event["payload"]["toolUses"][0]["name"], "run");
        assert_eq!(event["payload"]["toolUses"][0]["input"], input);
    }

    #[test]
    fn raw_transcript_format_param_is_opt_in_and_rejects_unknown_values() {
        assert_eq!(
            parse_raw_transcript_format(&[param("format", "json")]).unwrap(),
            RawTranscriptFormat::Json
        );
        assert!(parse_raw_transcript_format(&[param("format", "markdown")])
            .unwrap_err()
            .contains("expected json"));
        assert!(parse_raw_transcript_format(&[param("other", "value")])
            .unwrap_err()
            .contains("other"));
    }

    #[test]
    fn raw_transcript_defaults_to_existing_markdown_renderer() {
        let events = vec![raw_event(
            1,
            "turn-1",
            "user",
            serde_json::json!({ "content": "unchanged default" }),
        )];
        let legacy_rows = events
            .iter()
            .map(EventRow::to_transcript_row)
            .collect::<Vec<_>>();

        assert_eq!(
            format_raw_transcript(&events, RawTranscriptFormat::Markdown),
            crate::transcripts::format_transcript_full(&legacy_rows)
        );
        assert_eq!(
            parse_raw_transcript_format(&[]).unwrap(),
            RawTranscriptFormat::Markdown
        );
    }

    #[test]
    fn structured_raw_transcript_pages_one_event_per_line_with_truthful_continuation() {
        use cairn_common::read::{NaturalUnit, ReadSegment, SegmentKind, SegmentMeta};

        let events = vec![
            raw_event(
                1,
                "turn-1",
                "user",
                serde_json::json!({ "content": "first" }),
            ),
            raw_event(
                2,
                "turn-2",
                "assistant",
                serde_json::json!({ "content": "second" }),
            ),
        ];
        let jsonl = format_raw_transcript(&events, RawTranscriptFormat::Json);
        assert_eq!(jsonl.lines().count(), 2);
        for line in jsonl.lines() {
            serde_json::from_str::<serde_json::Value>(line).unwrap();
        }

        let window = crate::storage::render::window_text_lines(&jsonl, None, Some(1));
        let mut meta = SegmentMeta::new(
            "cairn://p/cairn/2703/1/builder/chat/raw?format=json&limit=1",
            SegmentKind::Resource,
            NaturalUnit::Line,
        );
        meta.total_units = Some(window.total);
        meta.shown_units = window.shown;
        meta.offset = window.offset;
        meta.limit = Some(1);
        let rendered = crate::storage::render::render_segment(
            ReadSegment::text(window.body, meta),
            10_000,
            &crate::token_meters::O200K_TOKEN_METER,
        );

        assert!(rendered.text.contains("\"turnId\":\"turn-1\""));
        assert!(!rendered.text.contains("\"turnId\":\"turn-2\""));
        assert!(rendered.text.contains(
            "continue: cairn://p/cairn/2703/1/builder/chat/raw?format=json&offset=1&limit=1"
        ));
    }

    #[tokio::test]
    async fn load_job_events_reconstructs_archived_and_live_for_structured_output() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.keep().join("structured-transcript-archival.db");
        let db = LocalDb::open(path).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();

        let tool_input = serde_json::json!({
            "commands": [{ "command": "echo archived" }]
        });
        let archived = serde_json::json!({
            "content": "archived",
            "toolUses": [{ "id": "toolu_archived", "name": "run", "input": tool_input.clone() }]
        })
        .to_string();
        let live = serde_json::json!({ "content": "live" }).to_string();
        let blob = crate::storage::compress(archived.as_bytes()).unwrap();
        db.write(move |conn| {
            let blob = blob.clone();
            let live = live.clone();
            Box::pin(async move {
                for sql in [
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('project-mixed','default','Mixed','MIX','/tmp/mixed',1,1)",
                    "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at) VALUES ('issue-mixed','project-mixed',1,'Mixed transcript','active',1,1)",
                    "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq) VALUES ('exec-mixed','recipe','issue-mixed','project-mixed','complete',1,1)",
                    "INSERT INTO jobs(id, execution_id, issue_id, project_id, status, uri_segment, created_at, updated_at) VALUES ('job-mixed','exec-mixed','issue-mixed','project-mixed','complete','builder',1,1)",
                    "INSERT INTO runs(id, job_id, issue_id, status, created_at, updated_at) VALUES ('run-mixed','job-mixed','issue-mixed','exited',1,1)",
                ] {
                    conn.execute(sql, ()).await?;
                }
                conn.execute(
                    "INSERT INTO turns(id, session_id, run_id, sequence, created_at, updated_at) VALUES ('turn-a','sess','run-mixed',1,1,1), ('turn-b','sess','run-mixed',2,2,2)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id, storage_mode, data_blob, codec) VALUES ('ev-archived','run-mixed',1,1,'assistant','{\"_archived\":true}',1,'turn-a','zstd',?1,'zstd_v1')",
                    (blob,),
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id) VALUES ('ev-live','run-mixed',2,2,'user',?1,2,'turn-b')",
                    (live,),
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
                    Ok::<Vec<EventRow>, crate::storage::DbError>(
                        load_job_events_ordered(conn, "job-mixed", None, None).await,
                    )
                })
            })
            .await
            .unwrap();
        let rendered = format_raw_transcript(&rows, RawTranscriptFormat::Json);
        let events = rendered
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["payload"]["content"], "archived");
        assert_eq!(events[0]["payload"]["toolUses"][0]["input"], tool_input);
        assert_eq!(events[1]["payload"]["content"], "live");
        for event in &events {
            assert!(event.get("runId").is_some());
            assert!(event.get("sequence").is_some());
            assert!(event.get("turnId").is_some());
            assert!(event.get("eventType").is_some());
            assert!(event.get("createdAt").is_some());
            assert!(event.get("payload").is_some());
        }
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
        let blob = crate::storage::compress(original.as_bytes()).unwrap();
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
    /// line-number prefix; `head_limit` caps matches, as its name says, so
    /// parity is over content.
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
        let window = grep_materialized_body(transcript, &payload).unwrap();
        assert_eq!(window.match_count, Some(2));
        assert_eq!(
            contents(&window.body),
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
        let window = grep_materialized_body(transcript, &payload).unwrap();
        assert_eq!(contents(&window.body), vec!["Beta match"]);
        // One of two matches shown, and the window says so rather than letting
        // the body pass for the whole transcript's hits.
        assert_eq!(window.match_count, Some(2));
        assert_eq!(window.shown, Some(1));

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
        let window = grep_materialized_body(transcript, &payload).unwrap();
        assert_eq!(window.match_count, Some(0));
        assert_eq!(window.body, "No matches found for pattern 'zeta'");
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
            project: "cairn",
            coordinate: DigestCoordinate::Issue {
                number: 1666,
                exec_seq: 1,
            },
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
            "cairn://p/cairn/1666/1/builder/chat",
            &digest_meta(),
            &turns,
            false,
            None,
            None,
        );
        assert!(out.contains("# builder — cairn/1666/1 · 1 run · 1 turn · 3 events · complete"));
        assert!(out.contains("raw stream: cairn://p/cairn/1666/1/builder/chat/raw"));
        assert!(out.contains("## Turn 1 · "));
        assert!(out.contains("**Assistant:** Working on it"));
        assert!(out.contains("· read src/lib.rs [50 lines]"));
        assert!(out.contains("· read jobs.rs ?grep=foo -C=30 [2 matches]"));
    }

    /// CAIRN-3428: a turn header used to render a bare UTC `HH:MM` while resume
    /// markers a few lines away rendered host-local, neither saying which — so an
    /// agent read the wrong hour off its own transcript. The header now carries
    /// the clock it was rendered on, and the digest's date is that same clock's
    /// calendar day rather than UTC's.
    #[test]
    fn digest_header_and_turn_headers_name_their_clock() {
        let events = vec![digest_ev(
            0,
            "t1",
            "assistant",
            serde_json::json!({ "content": "working" }),
        )];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        let out = format_transcript_digest(
            &events,
            "cairn://p/cairn/1666/1/builder/chat",
            &digest_meta(),
            &turns,
            false,
            None,
            None,
        );

        let header = out
            .lines()
            .find(|line| line.starts_with("## Turn 1 · "))
            .expect("the digest renders a turn header");
        let rendered = header.trim_start_matches("## Turn 1 · ");
        let (hhmm, zone) = rendered
            .split_once(' ')
            .expect("a turn header time is followed by the clock it is on");
        assert_eq!(
            hhmm.len(),
            5,
            "turn header time reads HH:MM, got {rendered}"
        );
        assert!(hhmm.contains(':'));
        assert!(!zone.is_empty(), "the zone label is never blank");

        let expected_date = crate::clock::date(events[0].created_at).unwrap();
        assert!(
            out.lines().next().unwrap().ends_with(&expected_date),
            "the digest dates itself on the same clock its turn headers use"
        );
    }

    fn digest_ev_at(seq: i32, turn: &str, created_at: i64) -> EventRow {
        let mut event = digest_ev(
            seq,
            turn,
            "assistant",
            serde_json::json!({ "content": "working" }),
        );
        event.created_at = created_at;
        event
    }

    /// A digest can span local midnight. When it does, a bare `HH:MM` on a turn
    /// past the boundary is not an absolute time: the reader attaches it to the
    /// date in the heading, which by then is the wrong day. The first turn on
    /// each new local day states that day.
    ///
    /// The events are placed relative to a real local midnight on whatever host
    /// runs this, so the assertion holds in any zone rather than only in the one
    /// the author happened to be sitting in.
    #[test]
    fn digest_turn_headers_state_the_date_when_the_local_day_turns_over() {
        let clock = crate::clock::host();
        let anchor = 1_700_000_000;
        let day = clock.local_date(anchor).unwrap();
        let midnight = (1..=48 * 60)
            .map(|minutes| anchor + minutes * 60)
            .find(|at| clock.local_date(*at) != Some(day))
            .expect("a local day boundary lies within 48h of any instant");

        let events = vec![
            digest_ev_at(0, "t1", midnight - 20 * 60),
            digest_ev_at(1, "t2", midnight + 15 * 60),
            digest_ev_at(2, "t3", midnight + 30 * 60),
        ];
        let turns = std::collections::HashMap::from([
            ("t1".to_string(), 1i32),
            ("t2".to_string(), 2i32),
            ("t3".to_string(), 3i32),
        ]);
        let out = format_transcript_digest(
            &events,
            "cairn://p/cairn/1666/1/builder/chat",
            &digest_meta(),
            &turns,
            false,
            None,
            None,
        );

        let stamp = |turn: &str| {
            out.lines()
                .find(|line| line.starts_with(&format!("## Turn {turn} · ")))
                .unwrap_or_else(|| panic!("the digest renders turn {turn}"))
                .trim_start_matches(&format!("## Turn {turn} · "))
                .to_string()
        };

        let first_day = clock.date(midnight - 20 * 60).unwrap();
        let second_day = clock.date(midnight + 15 * 60).unwrap();
        assert_ne!(
            first_day, second_day,
            "the fixture straddles local midnight"
        );

        assert!(
            out.lines().next().unwrap().ends_with(&first_day),
            "the heading dates itself on the first event's local day"
        );
        assert!(
            !stamp("1").starts_with(&first_day),
            "a turn on the heading's own day does not repeat it, got {}",
            stamp("1")
        );
        assert!(
            stamp("2").starts_with(&second_day),
            "the first turn past local midnight states its own day, got {}",
            stamp("2")
        );
        assert!(
            !stamp("3").starts_with(&second_day),
            "only the turn that crosses restates the day, got {}",
            stamp("3")
        );
    }

    /// Latest-first reverses the rendered order, so the boundary falls on a
    /// different turn — the one that steps back into the earlier day.
    #[test]
    fn a_latest_first_digest_marks_the_boundary_where_it_renders_it() {
        let clock = crate::clock::host();
        let anchor = 1_700_000_000;
        let day = clock.local_date(anchor).unwrap();
        let midnight = (1..=48 * 60)
            .map(|minutes| anchor + minutes * 60)
            .find(|at| clock.local_date(*at) != Some(day))
            .expect("a local day boundary lies within 48h of any instant");

        let events = vec![
            digest_ev_at(0, "t1", midnight - 20 * 60),
            digest_ev_at(1, "t2", midnight + 15 * 60),
        ];
        let turns =
            std::collections::HashMap::from([("t1".to_string(), 1i32), ("t2".to_string(), 2i32)]);
        let out = format_transcript_digest(
            &events,
            "cairn://p/cairn/1666/1/builder/chat",
            &digest_meta(),
            &turns,
            true,
            None,
            None,
        );

        let earlier_day = clock.date(midnight - 20 * 60).unwrap();
        let turn_one = out
            .lines()
            .find(|line| line.starts_with("## Turn 1 · "))
            .unwrap()
            .trim_start_matches("## Turn 1 · ");
        assert!(
            turn_one.starts_with(&earlier_day),
            "rendered after the newer turn, turn 1 steps back a day and says so, got {turn_one}"
        );
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
            "cairn://p/cairn/1666/1/builder/chat",
            &digest_meta(),
            &turns,
            false,
            None,
            None,
        );
        assert!(out.contains("**User:**"));
        assert!(out.contains("→ cairn://p/cairn/1666/1/builder/chat/turn/1]"));
    }

    #[test]
    fn digest_never_attributes_a_synthesized_continuation_to_the_user() {
        // A resume Cairn synthesized for itself is stored as `user:continuation`
        // (CAIRN-3175). At every fidelity the digest must render it as a marker
        // and never under the **User:** label, or an agent reading the transcript
        // — its own, or another node's — sees an instruction nobody gave.
        let events = vec![
            digest_ev(
                0,
                "t1",
                "user:continuation",
                serde_json::json!({ "content": "Automatic resume: Cairn restarted this turn" }),
            ),
            digest_ev(
                1,
                "t1",
                "assistant",
                serde_json::json!({ "content": "picking the work back up" }),
            ),
        ];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        for unabridged in [false, true] {
            let out = format_transcript_digest_with(
                &events,
                "cairn://p/cairn/1666/1/builder/chat",
                &digest_meta(),
                &turns,
                crate::clock::host(),
                &DigestOptions {
                    latest: false,
                    turn_offset: None,
                    turn_limit: None,
                    unabridged,
                    inline_diffs: false,
                },
            );
            assert!(
                out.contains(crate::transcripts::CONTINUATION_MARKER_LINE),
                "continuation marker missing (unabridged={unabridged}): {out}"
            );
            assert!(
                !out.contains("**User:**"),
                "a synthesized continuation must never wear the user label (unabridged={unabridged}): {out}"
            );
            assert!(
                !out.contains("Automatic resume: Cairn restarted this turn"),
                "Cairn's own wake text is not conversation (unabridged={unabridged}): {out}"
            );
        }
    }

    #[test]
    fn digest_collapses_the_launch_prompt_but_keeps_the_operator_message() {
        // The CAIRN-3408 specimen, in the shape the live DB holds it: turn 1 is
        // the child's launch prompt (the issue description the PARENT wrote when
        // it filed the child) and a later turn carries the one message an
        // operator actually typed. The default digest is what a watching parent
        // receives inlined in its catch-up ride-along, so the launch body must
        // collapse — quoting a parent's own words back at it, once per turn, is
        // the whole defect — while the operator's message survives untouched.
        let authored_by_the_parent =
            "# Catch-up ride-along echoes the recipient's own authored launch prompt";
        let typed_by_the_operator = "u dont need to cd prefix";
        let events = vec![
            digest_ev(
                0,
                "t1",
                crate::transcripts::LAUNCH_EVENT_TYPE,
                serde_json::json!({ "content": authored_by_the_parent }),
            ),
            digest_ev(
                1,
                "t2",
                "user",
                serde_json::json!({ "content": typed_by_the_operator }),
            ),
        ];
        let turns =
            std::collections::HashMap::from([("t1".to_string(), 1i32), ("t2".to_string(), 2i32)]);

        let ride_along = format_transcript_digest_with(
            &events,
            "cairn://p/cairn/3405/1/builder/chat",
            &digest_meta(),
            &turns,
            crate::clock::host(),
            &DigestOptions {
                latest: false,
                turn_offset: Some(0),
                turn_limit: None,
                unabridged: false,
                inline_diffs: false,
            },
        );
        assert!(
            ride_along.contains(crate::transcripts::LAUNCH_MARKER_LINE),
            "launch marker missing: {ride_along}"
        );
        assert!(
            !ride_along.contains(authored_by_the_parent),
            "the recipient's own launch prompt must not ride along: {ride_along}"
        );
        assert!(
            ride_along.contains(typed_by_the_operator),
            "the genuine operator message must survive: {ride_along}"
        );
        assert!(
            !ride_along.contains(&format!("**User:** {authored_by_the_parent}")),
            "a launch prompt must never wear the user label: {ride_along}"
        );

        // `messages=full` is what a reseed or thread-compaction digest reads, and
        // a session rebuilt from one needs the task it was given. There the body
        // returns — under an honest speaker, never as **User:**.
        let reseed = format_transcript_digest_with(
            &events,
            "cairn://p/cairn/3405/1/builder/chat",
            &digest_meta(),
            &turns,
            crate::clock::host(),
            &DigestOptions {
                latest: false,
                turn_offset: None,
                turn_limit: None,
                unabridged: true,
                inline_diffs: false,
            },
        );
        assert!(
            reseed.contains(&format!(
                "{}{authored_by_the_parent}",
                crate::transcripts::LAUNCH_LABEL
            )),
            "a rebuilt session must keep its task statement: {reseed}"
        );
        assert!(
            !reseed.contains(&format!("**User:** {authored_by_the_parent}")),
            "a launch prompt must never wear the user label: {reseed}"
        );
    }

    #[test]
    fn digest_collapses_seed_and_keeps_trigger_verbatim_in_both_modes() {
        // A `user:seed` event (carrying a large embedded prior digest) followed
        // by the trigger's own `user` event. In BOTH default and unabridged
        // modes the seed must collapse to exactly one marker line — its embedded
        // body never resurfacing — while the trigger user message renders
        // verbatim (CAIRN-2534).
        let embedded = "EMBEDDED_PRIOR_DIGEST_BODY_THAT_MUST_NOT_APPEAR";
        let seed_body = format!("Reseed header\n\n# builder — prior\n{embedded}");
        let events = vec![
            digest_ev(
                0,
                "t1",
                "user:seed",
                serde_json::json!({ "content": seed_body }),
            ),
            digest_ev(
                1,
                "t1",
                "user",
                serde_json::json!({ "content": "please continue the plan" }),
            ),
        ];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        for unabridged in [false, true] {
            let out = format_transcript_digest_with(
                &events,
                "cairn://p/cairn/1666/1/builder/chat",
                &digest_meta(),
                &turns,
                crate::clock::host(),
                &DigestOptions {
                    latest: false,
                    turn_offset: None,
                    turn_limit: None,
                    unabridged,
                    inline_diffs: false,
                },
            );
            assert!(
                out.contains("[prior context compacted]"),
                "seed marker missing (unabridged={unabridged}): {out}"
            );
            assert!(
                !out.contains(embedded),
                "embedded prior digest leaked (unabridged={unabridged}): {out}"
            );
            assert!(
                out.contains("**User:** please continue the plan"),
                "trigger not verbatim (unabridged={unabridged}): {out}"
            );
            assert_eq!(
                out.matches("[prior context compacted]").count(),
                1,
                "seed should collapse to exactly one line (unabridged={unabridged}): {out}"
            );
        }
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

    fn digest_opts(latest: bool, unabridged: bool, inline_diffs: bool) -> DigestOptions {
        DigestOptions {
            latest,
            turn_offset: None,
            turn_limit: None,
            unabridged,
            inline_diffs,
        }
    }

    #[test]
    fn digest_messages_full_renders_user_message_unabridged() {
        let big = (1..=300)
            .map(|i| format!("user instruction line {i} that must survive a reseed"))
            .collect::<Vec<_>>()
            .join("\n");
        let events = vec![digest_ev(
            0,
            "t1",
            "user",
            serde_json::json!({ "content": big }),
        )];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        let base = "cairn://p/cairn/1666/1/builder/chat";

        // Default: truncated with a turn-link trailer; the tail is dropped.
        let truncated = format_transcript_digest_with(
            &events,
            base,
            &digest_meta(),
            &turns,
            crate::clock::host(),
            &digest_opts(false, false, false),
        );
        assert!(truncated.contains("→ cairn://p/cairn/1666/1/builder/chat/turn/1]"));
        assert!(!truncated.contains("user instruction line 300"));

        // messages=full: the whole message renders, no truncation trailer.
        let full = format_transcript_digest_with(
            &events,
            base,
            &digest_meta(),
            &turns,
            crate::clock::host(),
            &digest_opts(false, true, false),
        );
        assert!(full.contains("user instruction line 1 that must survive a reseed"));
        assert!(full.contains("user instruction line 300 that must survive a reseed"));
        assert!(!full.contains("→ cairn://p/cairn/1666/1/builder/chat/turn/1]"));
    }

    #[test]
    fn digest_messages_full_renders_assistant_message_unabridged() {
        let big = (1..=400)
            .map(|i| format!("assistant reasoning step {i} preserved across the reseed"))
            .collect::<Vec<_>>()
            .join("\n");
        let events = vec![digest_ev(
            0,
            "t1",
            "assistant",
            serde_json::json!({ "content": big }),
        )];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);

        let truncated = format_transcript_digest_with(
            &events,
            "base",
            &digest_meta(),
            &turns,
            crate::clock::host(),
            &digest_opts(false, false, false),
        );
        assert!(!truncated.contains("assistant reasoning step 400"));

        let full = format_transcript_digest_with(
            &events,
            "base",
            &digest_meta(),
            &turns,
            crate::clock::host(),
            &digest_opts(false, true, false),
        );
        assert!(full.contains("assistant reasoning step 400 preserved across the reseed"));
    }

    #[test]
    fn digest_diffs_inlines_file_write_bodies() {
        let events = vec![
            digest_ev(
                0,
                "t1",
                "assistant",
                serde_json::json!({
                    "toolUses": [{
                        "id": "w1", "name": "mcp__cairn__write",
                        "input": { "changes": [
                            { "target": "file:a.rs", "mode": "patch", "payload": { "diff": "@@\n+alpha\n-beta" } },
                            { "target": "file:b.rs", "mode": "patch", "payload": { "old_string": "old one\nold two", "new_string": "new one" } },
                            { "target": "file:c.rs", "mode": "create", "payload": { "content": "l1\nl2\nl3" } },
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
                            { "index": 0, "target": "file:a.rs", "mode": "patch", "kind": "file", "summary": "~a.rs" },
                            { "index": 1, "target": "file:b.rs", "mode": "patch", "kind": "file", "summary": "~b.rs" },
                            { "index": 2, "target": "file:c.rs", "mode": "create", "kind": "file", "summary": "+c.rs" },
                            { "index": 3, "target": "cairn:~/todos", "mode": "replace", "kind": "resource", "summary": "Replaced 2 todos" }
                        ]
                    }).to_string()
                }),
            ),
        ];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);

        // Default: diffstat only, no inlined bodies.
        let plain = format_transcript_digest_with(
            &events,
            "base",
            &digest_meta(),
            &turns,
            crate::clock::host(),
            &digest_opts(false, false, false),
        );
        assert!(plain.contains("· write a.rs (+1 −1)"));
        assert!(!plain.contains("    +alpha"));

        // diffs=true: each file write renders its indented body; resource write
        // keeps summary-only rendering.
        let full = format_transcript_digest_with(
            &events,
            "base",
            &digest_meta(),
            &turns,
            crate::clock::host(),
            &digest_opts(false, false, true),
        );
        assert!(full.contains("    +alpha"));
        assert!(full.contains("    -beta"));
        assert!(full.contains("    - old one"));
        assert!(full.contains("    - old two"));
        assert!(full.contains("    + new one"));
        assert!(full.contains("    + l1"));
        assert!(full.contains("· write todos — Replaced 2 todos"));
    }

    #[test]
    fn digest_diffs_caps_large_create_content() {
        let content = (1..=360)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let events = vec![
            digest_ev(
                0,
                "t1",
                "assistant",
                serde_json::json!({
                    "toolUses": [{
                        "id": "w1", "name": "mcp__cairn__write",
                        "input": { "changes": [
                            { "target": "file:big.rs", "mode": "create", "payload": { "content": content } }
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
                            { "index": 0, "target": "file:big.rs", "mode": "create", "kind": "file", "summary": "+big.rs" }
                        ]
                    }).to_string()
                }),
            ),
        ];
        let turns = std::collections::HashMap::from([("t1".to_string(), 1i32)]);
        let full = format_transcript_digest_with(
            &events,
            "base",
            &digest_meta(),
            &turns,
            crate::clock::host(),
            &digest_opts(false, false, true),
        );
        assert!(full.contains("    + line 1"));
        assert!(full.contains("    + line 20"));
        assert!(!full.contains("    + line 21"));
        // 360 total − 20 previewed = 340 remaining.
        assert!(full.contains("[+340 more lines]"));
    }

    #[test]
    fn digest_fidelity_params_compose_with_latest_and_paging() {
        let big1 = (1..=300)
            .map(|i| format!("turn one line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let big2 = (1..=300)
            .map(|i| format!("turn two line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let events = vec![
            digest_ev(0, "t1", "user", serde_json::json!({ "content": big1 })),
            digest_ev(1, "t2", "user", serde_json::json!({ "content": big2 })),
        ];
        let turns =
            std::collections::HashMap::from([("t1".to_string(), 1i32), ("t2".to_string(), 2i32)]);

        // latest reverses to [t2, t1]; the turn window (offset 0, limit 1) keeps
        // only the newest turn, and messages=full renders it unabridged.
        let opts = DigestOptions {
            latest: true,
            turn_offset: Some(0),
            turn_limit: Some(1),
            unabridged: true,
            inline_diffs: false,
        };
        let out = format_transcript_digest_with(
            &events,
            "base",
            &digest_meta(),
            &turns,
            crate::clock::host(),
            &opts,
        );
        assert!(out.contains("## Turn 2"));
        assert!(!out.contains("## Turn 1"));
        assert!(out.contains("turn two line 300"));
        assert!(!out.contains("turn one line"));
        // Unabridged: no truncation trailer anywhere.
        assert!(!out.contains("[+"));
    }

    // ── Golden digests ───────────────────────────────────────────────────────
    //
    // The `digest_*` tests above check that a digest contains the right lines.
    // The goldens pin every byte, over a corpus that reaches each event type and
    // tool kind, on a fixed zone so the output does not depend on the machine
    // running it. Regenerate with `CAIRN_UPDATE_GOLDENS=1` only when a change to
    // the digest is intended, and review the golden diff as that change.

    const GOLDEN_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/resources/testdata/digest");

    struct GoldenCase {
        name: &'static str,
        base: &'static str,
        events: Vec<EventRow>,
        turns: std::collections::HashMap<String, i32>,
        /// Variants rendered beside the `default` and `reseed` fidelities every
        /// case gets.
        extra: Vec<(&'static str, DigestOptions)>,
    }

    fn golden_case(
        name: &'static str,
        base: &'static str,
        events: Vec<EventRow>,
        turns: &[(&str, i32)],
    ) -> GoldenCase {
        GoldenCase {
            name,
            base,
            events,
            turns: turns
                .iter()
                .map(|(id, seq)| (id.to_string(), *seq))
                .collect(),
            extra: Vec::new(),
        }
    }

    fn paged(latest: bool, turn_offset: Option<i64>, turn_limit: Option<usize>) -> DigestOptions {
        DigestOptions {
            latest,
            turn_offset,
            turn_limit,
            unabridged: false,
            inline_diffs: false,
        }
    }

    fn numbered_lines(n: usize, line: impl Fn(usize) -> String) -> String {
        (1..=n).map(line).collect::<Vec<_>>().join("\n")
    }

    /// An event a minute and a second after the previous sequence number, so
    /// turn stamps advance visibly across the corpus.
    fn golden_ev(
        run: &str,
        seq: i32,
        turn: Option<&str>,
        kind: &str,
        data: serde_json::Value,
    ) -> EventRow {
        EventRow {
            run_id: run.to_string(),
            sequence: seq,
            event_type: kind.to_string(),
            data: data.to_string(),
            turn_id: turn.map(str::to_string),
            created_at: 1_700_000_000 + i64::from(seq) * 61,
        }
    }

    fn result_ev(run: &str, seq: i32, turn: Option<&str>, data: serde_json::Value) -> EventRow {
        golden_ev(run, seq, turn, "tool_result", data)
    }

    /// One transcript that reaches every event type the digest distinguishes and
    /// every tool-row builder, including the shapes only the fallback renders.
    fn every_event_and_tool() -> GoldenCase {
        use serde_json::json;
        let long_reply = numbered_lines(200, |i| {
            format!("assistant narrative line {i} explaining the next step in detail")
        });
        let t1 = Some("t1");
        let t2 = Some("t2");
        let t3 = Some("t3");
        let t4 = Some("t4");
        let t5 = Some("t5");
        let mut unparseable = golden_ev("run-1", 31, t2, "user", json!(null));
        unparseable.data = "not json".to_string();
        let events = vec![
            // Turn 1: the launch prompt, an empty user event, an injection that
            // leads, the operator's message, a later injection, Claude tools, and
            // a turn-less event that folds into the block before it.
            golden_ev(
                "run-1",
                0,
                t1,
                "user:launch",
                json!({ "content": "# Launch\n\nDo the whole thing.\n" }),
            ),
            golden_ev("run-1", 1, t1, "user", json!({ "content": "" })),
            golden_ev(
                "run-1",
                2,
                t1,
                "user",
                json!({ "content": "<system-reminder>base advanced</system-reminder>" }),
            ),
            golden_ev(
                "run-1",
                3,
                t1,
                "user",
                json!({ "content": "Please fix the parser.\nIt drops trailing commas.  \n" }),
            ),
            golden_ev(
                "run-1",
                4,
                t1,
                "user",
                json!({ "content": "Also: a direct message\narrived mid-turn." }),
            ),
            golden_ev(
                "run-1",
                5,
                t1,
                "assistant",
                json!({
                    "content": "Looking around first.",
                    "toolUses": [
                        { "id": "bash-1", "name": "Bash", "input": { "command": "cargo test -p parser", "description": "Run parser tests" } },
                        { "id": "read-1", "name": "Read", "input": { "file_path": "/repo/src/parser.rs" } },
                        { "id": "grep-1", "name": "Grep", "input": { "pattern": "trailing_comma" } },
                        { "id": "edit-1", "name": "Edit", "input": { "file_path": "/repo/src/parser.rs", "old_string": "a", "new_string": "b" } },
                        { "id": "cwrite-1", "name": "Write", "input": { "file_path": "/repo/src/new.rs", "content": "fn x() {}\n" } },
                        { "id": "glob-1", "name": "Glob", "input": { "pattern": "**/*.rs" } }
                    ]
                }),
            ),
            result_ev(
                "run-1",
                6,
                t1,
                json!({ "toolUseId": "bash-1", "toolResult": "test result: FAILED\nExit code: 101" }),
            ),
            result_ev(
                "run-1",
                7,
                t1,
                json!({ "toolUseId": "read-1", "toolResult": "1\tfn parse() {\n2\t}\n3\t" }),
            ),
            result_ev(
                "run-1",
                8,
                t1,
                json!({ "toolUseId": "grep-1", "toolResult": "src/parser.rs:10:trailing_comma\nsrc/lexer.rs:4:trailing_comma" }),
            ),
            result_ev(
                "run-1",
                9,
                t1,
                json!({ "toolUseId": "edit-1", "toolResult": "--- a\n+++ b\n-a\n+b" }),
            ),
            result_ev(
                "run-1",
                10,
                t1,
                json!({ "toolUseId": "cwrite-1", "toolResult": "File created" }),
            ),
            result_ev(
                "run-1",
                11,
                t1,
                json!({ "tool_use_id": "glob-1", "toolResult": "src/a.rs\nsrc/b.rs" }),
            ),
            golden_ev(
                "run-1",
                12,
                None,
                "assistant",
                json!({ "content": "Tests fail on trailing commas." }),
            ),
            // Turn 2, absent from the turn map: a continuation, prose long enough
            // to truncate without a turn link, Codex-native and MCP tools, a
            // pending call, an error result, a nameless call, both boundary
            // kinds, an ignored event type, an orphan result, and a payload that
            // does not parse.
            golden_ev(
                "run-1",
                20,
                t2,
                "user:continuation",
                json!({ "content": "Automatic resume: Cairn restarted this turn" }),
            ),
            golden_ev(
                "run-1",
                21,
                t2,
                "assistant",
                json!({
                    "content": long_reply,
                    "toolUses": [
                        { "id": "fc-1", "name": "fileChange", "input": { "changes": [{ "path": "/repo/src/parser.rs", "kind": "update", "diff": "@@\n-old\n+new\n+newer" }] } },
                        { "id": "ce-1", "name": "commandExecution", "input": { "command": "cargo build" } },
                        { "id": "mcp-1", "name": "mcp__x__y", "input": { "query": "where is the parser" } },
                        { "id": "pending-1", "name": "mcp__cairn__read", "input": { "paths": ["file:src/pending.rs"] } },
                        { "id": "run-err", "name": "mcp__cairn__run", "input": { "commands": [{ "command": "git push", "description": "Push the branch" }, { "command": "git status" }] } },
                        { "id": "anon-1", "input": { "url": "https://example.com" } }
                    ]
                }),
            ),
            result_ev(
                "run-1",
                22,
                t2,
                json!({ "toolUseId": "fc-1", "toolResult": "Success. Updated the following files:\nM src/parser.rs" }),
            ),
            result_ev(
                "run-1",
                23,
                t2,
                json!({ "toolUseId": "ce-1", "toolResult": "Finished dev\nexit status: 0" }),
            ),
            result_ev(
                "run-1",
                24,
                t2,
                json!({ "toolUseId": "mcp-1", "toolResult": { "hits": 2, "files": ["src/parser.rs"] } }),
            ),
            result_ev(
                "run-1",
                25,
                t2,
                json!({ "toolUseId": "run-err", "isError": true, "toolResult": "\n  error: failed to push some refs\nhint: pull first" }),
            ),
            result_ev(
                "run-1",
                26,
                t2,
                json!({ "toolUseId": "anon-1", "toolResult": "ok" }),
            ),
            golden_ev(
                "run-1",
                27,
                t2,
                "system:session_boundary",
                json!({ "raw": { "kind": "provider_change", "fromBackend": "claude", "toBackend": "codex" } }),
            ),
            golden_ev(
                "run-1",
                28,
                t2,
                "system:compact_boundary",
                json!({ "raw": { "provider": "claude" } }),
            ),
            golden_ev("run-1", 29, t2, "system", json!({ "content": "ignored" })),
            result_ev(
                "run-1",
                30,
                t2,
                json!({ "toolUseId": "nobody", "toolResult": "orphaned output" }),
            ),
            unparseable,
            // Turn 3, a second run: a seed, the trigger, and Cairn's own tools.
            golden_ev(
                "run-2",
                40,
                t3,
                "user:seed",
                json!({ "content": "Reseed header\n\n# builder — prior\nEMBEDDED_PRIOR_DIGEST" }),
            ),
            golden_ev(
                "run-2",
                41,
                t3,
                "user",
                json!({ "content": "please continue" }),
            ),
            golden_ev(
                "run-2",
                42,
                t3,
                "assistant",
                json!({
                    "toolUses": [
                        { "id": "task-1", "name": "Task", "input": { "description": "map the lexer", "prompt": "Trace the lexer" } },
                        { "id": "msg-1", "name": "mcp__cairn__message", "input": { "to": "cairn://p/cairn/1/messages", "content": "Landed the parser fix" } },
                        { "id": "msg-2", "name": "message", "input": { "content": "broadcast to the project" } },
                        { "id": "w-1", "name": "mcp__cairn__write", "input": { "changes": [
                            { "target": "file:src/parser.rs", "mode": "patch", "payload": { "old_string": "old", "new_string": "new\nnewer" } },
                            { "target": "cairn:~/todos", "mode": "patch", "payload": {} },
                            { "target": "file:src/broken.rs", "mode": "patch", "payload": { "diff": "@@\n+x" } }
                        ] } }
                    ]
                }),
            ),
            golden_ev(
                "run-2",
                43,
                t3,
                "result",
                json!({ "toolUseId": "task-1", "toolResult": "Lexer mapped", "isError": false }),
            ),
            result_ev(
                "run-2",
                44,
                t3,
                json!({ "toolUseId": "msg-1", "isError": true, "toolResult": { "error": "unknown channel" } }),
            ),
            result_ev(
                "run-2",
                45,
                t3,
                json!({ "toolUseId": "msg-2", "toolResult": "sent" }),
            ),
            result_ev(
                "run-2",
                46,
                t3,
                json!({
                    "toolUseId": "w-1",
                    "toolResult": json!({
                        "applied": [
                            { "index": 0, "kind": "file", "summary": "~src/parser.rs" },
                            { "index": 1, "kind": "resource", "summary": "Updated 2 todos\nsecond line" }
                        ],
                        "failures": [{ "index": 2, "error": "anchor not found:\n  @@ +x" }],
                        "commit": { "status": "committed", "sha": "abcdef0123456" }
                    })
                    .to_string()
                }),
            ),
            // Turn 4 renders nothing but its heading.
            golden_ev("run-2", 50, t4, "system", json!({})),
            golden_ev("run-2", 51, t4, "assistant", json!({ "content": "" })),
            // Turn 5: an empty launch prompt still stands as a marker.
            golden_ev("run-2", 60, t5, "user:launch", json!({ "content": "" })),
            golden_ev("run-2", 61, t5, "user", json!({ "content": "last word" })),
        ];
        let mut case = golden_case(
            "every_event_and_tool",
            "cairn://p/cairn/1666/1/builder/chat",
            events,
            &[("t1", 1), ("t3", 3), ("t4", 4), ("t5", 5)],
        );
        case.extra = vec![("latest", paged(true, None, None))];
        case
    }

    /// The event vectors of every `digest_*` test above, plus
    /// [`every_event_and_tool`].
    fn golden_corpus(clock: &crate::clock::HostClock) -> Vec<GoldenCase> {
        use serde_json::json;
        let chat = "cairn://p/cairn/1666/1/builder/chat";
        let mut cases = Vec::new();

        cases.push(golden_case(
            "read_rows",
            chat,
            vec![
                digest_ev(0, "t1", "user", json!({ "content": "do the thing" })),
                digest_ev(
                    1,
                    "t1",
                    "assistant",
                    json!({
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
                    json!({
                        "toolUseId": "r1",
                        "toolResult": "=== file:src/lib.rs [lines 1–50 of 50] ===\nbody\n=== file:jobs.rs [2 matches] ===\nm1\nm2"
                    }),
                ),
            ],
            &[("t1", 1)],
        ));

        cases.push(golden_case(
            "clock_named",
            chat,
            vec![digest_ev(
                0,
                "t1",
                "assistant",
                json!({ "content": "working" }),
            )],
            &[("t1", 1)],
        ));

        let anchor = 1_700_000_000;
        let day = clock.local_date(anchor).unwrap();
        let midnight = (1..=48 * 60)
            .map(|minutes| anchor + minutes * 60)
            .find(|at| clock.local_date(*at) != Some(day))
            .unwrap();
        let mut local_midnight = golden_case(
            "local_midnight",
            chat,
            vec![
                digest_ev_at(0, "t1", midnight - 20 * 60),
                digest_ev_at(1, "t2", midnight + 15 * 60),
                digest_ev_at(2, "t3", midnight + 30 * 60),
            ],
            &[("t1", 1), ("t2", 2), ("t3", 3)],
        );
        local_midnight.extra = vec![("latest", paged(true, None, None))];
        cases.push(local_midnight);

        cases.push(golden_case(
            "write_rows",
            "base",
            vec![
                digest_ev(
                    0,
                    "t1",
                    "assistant",
                    json!({
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
                    json!({
                        "toolUseId": "w1",
                        "toolResult": json!({
                            "applied": [
                                { "index": 0, "target": "file:src/app.rs", "mode": "patch", "kind": "file", "summary": "~src/app.rs" },
                                { "index": 1, "target": "cairn:~/todos", "mode": "replace", "kind": "resource", "summary": "Replaced 3 todos for builder (0 completed)" }
                            ],
                            "commit": { "status": "committed", "sha": "6102407abcdef" }
                        }).to_string()
                    }),
                ),
            ],
            &[("t1", 1)],
        ));

        cases.push(golden_case(
            "run_exit",
            "base",
            vec![
                digest_ev(
                    0,
                    "t1",
                    "assistant",
                    json!({
                        "toolUses": [{ "id": "c1", "name": "mcp__cairn__run", "input": { "commands": [{ "command": "cargo test" }] } }]
                    }),
                ),
                digest_ev(
                    1,
                    "t1",
                    "tool_result",
                    json!({ "toolUseId": "c1", "toolResult": "running tests\nExit code: 0" }),
                ),
            ],
            &[("t1", 1)],
        ));

        cases.push(golden_case(
            "error_row",
            "base",
            vec![
                digest_ev(
                    0,
                    "t1",
                    "assistant",
                    json!({
                        "toolUses": [{ "id": "c1", "name": "mcp__cairn__run", "input": { "commands": [{ "command": "git rebase origin/main" }] } }]
                    }),
                ),
                digest_ev(
                    1,
                    "t1",
                    "tool_result",
                    json!({ "toolUseId": "c1", "isError": true, "toolResult": "Failed to spawn process: No such file or directory" }),
                ),
            ],
            &[("t1", 1)],
        ));

        cases.push(golden_case(
            "user_truncation",
            chat,
            vec![digest_ev(
                0,
                "t1",
                "user",
                json!({ "content": numbered_lines(300, |i| format!("this is line {i} of the user message plan replay")) }),
            )],
            &[("t1", 1)],
        ));

        cases.push(golden_case(
            "continuation",
            chat,
            vec![
                digest_ev(
                    0,
                    "t1",
                    "user:continuation",
                    json!({ "content": "Automatic resume: Cairn restarted this turn" }),
                ),
                digest_ev(
                    1,
                    "t1",
                    "assistant",
                    json!({ "content": "picking the work back up" }),
                ),
            ],
            &[("t1", 1)],
        ));

        cases.push(golden_case(
            "launch_prompt",
            "cairn://p/cairn/3405/1/builder/chat",
            vec![
                digest_ev(
                    0,
                    "t1",
                    crate::transcripts::LAUNCH_EVENT_TYPE,
                    json!({ "content": "# Catch-up ride-along echoes the recipient's own authored launch prompt" }),
                ),
                digest_ev(1, "t2", "user", json!({ "content": "u dont need to cd prefix" })),
            ],
            &[("t1", 1), ("t2", 2)],
        ));

        cases.push(golden_case(
            "seed_collapse",
            chat,
            vec![
                digest_ev(
                    0,
                    "t1",
                    "user:seed",
                    json!({ "content": "Reseed header\n\n# builder — prior\nEMBEDDED_PRIOR_DIGEST_BODY_THAT_MUST_NOT_APPEAR" }),
                ),
                digest_ev(1, "t1", "user", json!({ "content": "please continue the plan" })),
            ],
            &[("t1", 1)],
        ));

        let mut latest_order = golden_case(
            "latest_order",
            "base",
            vec![
                digest_ev(0, "t1", "user", json!({ "content": "first" })),
                digest_ev(1, "t2", "user", json!({ "content": "second" })),
            ],
            &[("t1", 1), ("t2", 2)],
        );
        latest_order.extra = vec![("latest", paged(true, None, None))];
        cases.push(latest_order);

        let mut paging_window = golden_case(
            "paging_window",
            "base",
            vec![
                digest_ev(0, "t1", "user", json!({ "content": "first" })),
                digest_ev(1, "t2", "user", json!({ "content": "second" })),
                digest_ev(2, "t3", "user", json!({ "content": "third" })),
            ],
            &[("t1", 1), ("t2", 2), ("t3", 3)],
        );
        paging_window.extra = vec![
            ("offset1_limit1", paged(false, Some(1), Some(1))),
            ("offset_neg2", paged(false, Some(-2), None)),
            ("latest_offset1", paged(true, Some(1), None)),
        ];
        cases.push(paging_window);

        cases.push(golden_case(
            "pending_call",
            "base",
            vec![digest_ev(
                0,
                "t1",
                "assistant",
                json!({
                    "toolUses": [{ "id": "r1", "name": "mcp__cairn__read", "input": { "paths": ["file:src/x.rs"] } }]
                }),
            )],
            &[("t1", 1)],
        ));

        cases.push(golden_case(
            "user_unabridged",
            chat,
            vec![digest_ev(
                0,
                "t1",
                "user",
                json!({ "content": numbered_lines(300, |i| format!("user instruction line {i} that must survive a reseed")) }),
            )],
            &[("t1", 1)],
        ));

        cases.push(golden_case(
            "assistant_unabridged",
            "base",
            vec![digest_ev(
                0,
                "t1",
                "assistant",
                json!({ "content": numbered_lines(400, |i| format!("assistant reasoning step {i} preserved across the reseed")) }),
            )],
            &[("t1", 1)],
        ));

        cases.push(golden_case(
            "write_bodies",
            "base",
            vec![
                digest_ev(
                    0,
                    "t1",
                    "assistant",
                    json!({
                        "toolUses": [{
                            "id": "w1", "name": "mcp__cairn__write",
                            "input": { "changes": [
                                { "target": "file:a.rs", "mode": "patch", "payload": { "diff": "@@\n+alpha\n-beta" } },
                                { "target": "file:b.rs", "mode": "patch", "payload": { "old_string": "old one\nold two", "new_string": "new one" } },
                                { "target": "file:c.rs", "mode": "create", "payload": { "content": "l1\nl2\nl3" } },
                                { "target": "cairn:~/todos", "mode": "replace", "payload": {} }
                            ]}
                        }]
                    }),
                ),
                digest_ev(
                    1,
                    "t1",
                    "tool_result",
                    json!({
                        "toolUseId": "w1",
                        "toolResult": json!({
                            "applied": [
                                { "index": 0, "target": "file:a.rs", "mode": "patch", "kind": "file", "summary": "~a.rs" },
                                { "index": 1, "target": "file:b.rs", "mode": "patch", "kind": "file", "summary": "~b.rs" },
                                { "index": 2, "target": "file:c.rs", "mode": "create", "kind": "file", "summary": "+c.rs" },
                                { "index": 3, "target": "cairn:~/todos", "mode": "replace", "kind": "resource", "summary": "Replaced 2 todos" }
                            ]
                        }).to_string()
                    }),
                ),
            ],
            &[("t1", 1)],
        ));

        cases.push(golden_case(
            "create_cap",
            "base",
            vec![
                digest_ev(
                    0,
                    "t1",
                    "assistant",
                    json!({
                        "toolUses": [{
                            "id": "w1", "name": "mcp__cairn__write",
                            "input": { "changes": [
                                { "target": "file:big.rs", "mode": "create", "payload": { "content": numbered_lines(360, |i| format!("line {i}")) } }
                            ]}
                        }]
                    }),
                ),
                digest_ev(
                    1,
                    "t1",
                    "tool_result",
                    json!({
                        "toolUseId": "w1",
                        "toolResult": json!({
                            "applied": [
                                { "index": 0, "target": "file:big.rs", "mode": "create", "kind": "file", "summary": "+big.rs" }
                            ]
                        }).to_string()
                    }),
                ),
            ],
            &[("t1", 1)],
        ));

        let mut fidelity_paging = golden_case(
            "fidelity_paging",
            "base",
            vec![
                digest_ev(
                    0,
                    "t1",
                    "user",
                    json!({ "content": numbered_lines(300, |i| format!("turn one line {i}")) }),
                ),
                digest_ev(
                    1,
                    "t2",
                    "user",
                    json!({ "content": numbered_lines(300, |i| format!("turn two line {i}")) }),
                ),
            ],
            &[("t1", 1), ("t2", 2)],
        );
        fidelity_paging.extra = vec![(
            "latest_limit1_unabridged",
            DigestOptions {
                latest: true,
                turn_offset: Some(0),
                turn_limit: Some(1),
                unabridged: true,
                inline_diffs: false,
            },
        )];
        cases.push(fidelity_paging);

        cases.push(every_event_and_tool());
        cases
    }

    #[test]
    fn digest_output_matches_goldens() {
        let clock = crate::clock::HostClock::fixed("America/Los_Angeles");
        let update = std::env::var("CAIRN_UPDATE_GOLDENS").as_deref() == Ok("1");
        let dir = std::path::Path::new(GOLDEN_DIR);
        let mut expected_files = std::collections::BTreeSet::new();
        let mut failures = Vec::new();

        for case in golden_corpus(&clock) {
            let mut variants = vec![
                ("default", paged(false, None, None)),
                (
                    "reseed",
                    DigestOptions {
                        unabridged: true,
                        inline_diffs: true,
                        ..paged(false, None, None)
                    },
                ),
            ];
            variants.extend(case.extra);
            for (variant, opts) in variants {
                let rendered = format_transcript_digest_with(
                    &case.events,
                    case.base,
                    &digest_meta(),
                    &case.turns,
                    &clock,
                    &opts,
                );
                let file = format!("{}.{variant}.md", case.name);
                let path = dir.join(&file);
                expected_files.insert(file.clone());
                if update {
                    std::fs::create_dir_all(dir).unwrap();
                    std::fs::write(&path, &rendered).unwrap();
                    continue;
                }
                match std::fs::read_to_string(&path) {
                    Ok(golden) if golden == rendered => {}
                    Ok(golden) => {
                        let line = golden
                            .lines()
                            .zip(rendered.lines())
                            .position(|(want, got)| want != got)
                            .map_or_else(
                                || "length".to_string(),
                                |index| format!("line {}", index + 1),
                            );
                        failures.push(format!("{file}: differs at {line}"));
                    }
                    Err(error) => failures.push(format!("{file}: {error}")),
                }
            }
        }

        if !update {
            for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.ends_with(".md") && !expected_files.contains(&name) {
                    failures.push(format!("{name}: no corpus case renders this golden"));
                }
            }
        }

        assert!(
            failures.is_empty(),
            "digest output drifted from its goldens (rerun with CAIRN_UPDATE_GOLDENS=1 only for an intended change):\n{}",
            failures.join("\n")
        );
    }
}
