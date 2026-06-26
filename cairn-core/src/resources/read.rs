//! URI resource readers.
//!
//! Provides read support for `cairn://*` URIs. Transport protocols should
//! decode their own payloads and delegate here.

use super::common::{
    affordance_for_kind, find_query_value, reject_query_params, resolve_home_relative_resource_uri,
};
use super::files::{
    read_issue_changed, read_issue_changed_projection, read_node_changed,
    read_node_changed_projection,
};
use super::issue::{
    read_issue, read_issue_comment, read_issue_comments, read_issue_execution,
    read_issue_executions,
};
use super::labels::{read_label, read_labels};
use super::memories::{read_node_memories_collection, read_node_memory};
use super::messages::{read_issue_messages, read_node_messages, read_project_messages};
use super::node::{
    read_job_todos, read_node, read_node_artifact, read_node_chat, read_node_chat_event,
    read_node_chat_raw, read_node_chat_turn, read_node_permission, read_node_permissions,
    read_node_question, read_node_questions, read_node_tasks, read_node_wakes, read_task,
    read_task_artifact, read_task_chat, read_task_chat_event, read_task_chat_raw,
    read_task_chat_turn, read_task_permission, read_task_permissions,
};
use super::symbols::{read_node_symbols, read_project_symbols};

use super::actions::{read_action, read_actions_collection};
use super::agents::{read_agent, read_agents_collection};
use super::project::{
    produce_project_issues, read_project, read_project_issues, read_project_search,
    read_project_settings, read_projects,
};
use super::recipes::{read_recipe, read_recipes_collection};
use super::settings::read_settings;

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::query::{split_target_query, QueryParam};
use cairn_common::read::{Affordance, ImageBlock, NaturalUnit, SegmentKind};
use cairn_common::uri::{parse_uri, CairnResource};
use std::path::{Path, PathBuf};
use turso::Value;

// ============================================================================
// Router
// ============================================================================

/// The structured result of resolving a `cairn://` resource: separated content
/// and affordance, plus the natural-unit metadata the batch view layer needs to
/// window and to render the enriched header.
///
/// - `Line` documents/collections carry the unwindowed body; the view applies
///   `offset`/`limit` as a line window.
/// - `Record` collections (`/issues`, `/messages`) are already item-windowed by
///   the producer (SQL/cursor pushdown); the view must not re-window by lines.
pub(crate) struct RenderedResource {
    pub content: String,
    pub natural_unit: NaturalUnit,
    pub unit_noun: Option<&'static str>,
    pub total_units: Option<usize>,
    pub shown_units: Option<usize>,
    /// `Line`: raw line offset (negative = tail). `Record`: item offset (>= 0).
    pub offset: Option<i64>,
    pub limit: Option<usize>,
    /// `Match`: real matches over the produced body (not context lines). Drives
    /// the `[N matches]` header suffix for a universal-grep result.
    pub match_count: Option<usize>,
    pub affordance: Option<Affordance>,
    /// Image blocks (e.g. a browser screenshot) lifted into the read envelope
    /// alongside the text body. Empty for ordinary text resources.
    pub images: Vec<ImageBlock>,
}

impl RenderedResource {
    /// A `Line`-unit result the view windows by `offset`/`limit`.
    fn line(
        content: String,
        affordance: Option<Affordance>,
        offset: Option<i64>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            content,
            natural_unit: NaturalUnit::Line,
            unit_noun: None,
            total_units: None,
            shown_units: None,
            offset,
            limit,
            match_count: None,
            affordance,
            images: Vec::new(),
        }
    }

    /// A `Match`-unit result: the universal grep already paginated the body by
    /// `head_limit`, so the view never re-windows it. `match_count` is the real
    /// match total over the shown body.
    fn grep(content: String, match_count: usize, affordance: Option<Affordance>) -> Self {
        Self {
            content,
            natural_unit: NaturalUnit::Match,
            unit_noun: None,
            total_units: None,
            shown_units: None,
            offset: None,
            limit: None,
            match_count: Some(match_count),
            affordance,
            images: Vec::new(),
        }
    }

    /// A `Record`-unit result: the producer already applied item offset/limit.
    fn records(
        content: String,
        unit_noun: &'static str,
        shown_units: usize,
        offset: usize,
        limit: usize,
        affordance: Option<Affordance>,
    ) -> Self {
        Self {
            content,
            natural_unit: NaturalUnit::Record,
            unit_noun: Some(unit_noun),
            total_units: None,
            shown_units: Some(shown_units),
            offset: Some(offset as i64),
            limit: Some(limit),
            match_count: None,
            affordance,
            images: Vec::new(),
        }
    }

    /// A chat digest result: the producer already applied turn offset/limit.
    fn turns(
        content: String,
        shown_units: usize,
        offset: usize,
        limit: Option<usize>,
        total_units: usize,
        affordance: Option<Affordance>,
    ) -> Self {
        Self {
            content,
            natural_unit: NaturalUnit::Turn,
            unit_noun: Some("turns"),
            total_units: Some(total_units),
            shown_units: Some(shown_units),
            offset: Some(offset as i64),
            limit,
            match_count: None,
            affordance,
            images: Vec::new(),
        }
    }
}

fn rendered_chat_turn_count(content: &str) -> usize {
    content
        .lines()
        .filter(|line| line.starts_with("## Turn "))
        .count()
}

fn rendered_chat_total_turns(content: &str) -> Option<usize> {
    let header = content.lines().next()?;
    let marker = " turn";
    let before_marker = header.split(marker).next()?;
    before_marker
        .rsplit('·')
        .next()
        .map(str::trim)
        .and_then(|value| value.parse::<usize>().ok())
}

const DB_SQL_DEFAULT_LIMIT: usize = 100;
const DB_SQL_MAX_LIMIT: usize = 1_000;

#[derive(Debug)]
struct DbSqlProjection {
    sql: String,
    offset: usize,
    limit: usize,
}

fn parse_db_sql_projection(params: &[QueryParam]) -> Result<DbSqlProjection, String> {
    parse_db_sql_projection_with(params, &[], "cairn://db")
}

/// Shared parser for the `cairn://db` / `cairn://dev/db` SQL projections.
/// `allowed_extra` names resource-specific params beyond `sql`/`offset`/`limit`
/// (the dev/db `at` selector); `resource` labels error messages.
fn parse_db_sql_projection_with(
    params: &[QueryParam],
    allowed_extra: &[&str],
    resource: &str,
) -> Result<DbSqlProjection, String> {
    let sql = find_query_value(params, "sql")
        .ok_or_else(|| format!("{resource} requires a 'sql' query parameter"))?
        .to_string();
    let offset = match find_query_value(params, "offset") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| format!("Invalid integer for query parameter 'offset': {value}"))?,
        None => 0,
    };
    let requested_limit = match find_query_value(params, "limit") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| format!("Invalid integer for query parameter 'limit': {value}"))?,
        None => DB_SQL_DEFAULT_LIMIT,
    };
    let limit = requested_limit.min(DB_SQL_MAX_LIMIT);

    if let Some(unsupported) = params.iter().find(|param| {
        !matches!(param.key.as_str(), "sql" | "offset" | "limit")
            && !allowed_extra.contains(&param.key.as_str())
    }) {
        return Err(format!(
            "Unsupported query parameter '{}' for {resource}",
            unsupported.key
        ));
    }
    validate_read_only_sql(&sql)?;
    Ok(DbSqlProjection { sql, offset, limit })
}

fn validate_read_only_sql(sql: &str) -> Result<(), String> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err("SQL query is empty".to_string());
    }
    reject_statement_chain(trimmed)?;
    let tokens = sql_tokens(trimmed);
    // EXPLAIN and EXPLAIN QUERY PLAN describe a statement's bytecode / query
    // plan without ever executing it, so both are read-only regardless of the
    // inner statement. Strip the prefix and validate the described statement is
    // itself a permitted read shape: this keeps the contract's spirit (no
    // mutation ever reaches the engine) while enabling query-plan inspection.
    let statement = match tokens.first().map(String::as_str) {
        Some("EXPLAIN") => {
            let rest = if tokens.get(1).map(String::as_str) == Some("QUERY")
                && tokens.get(2).map(String::as_str) == Some("PLAN")
            {
                &tokens[3..]
            } else {
                &tokens[1..]
            };
            if rest.is_empty() {
                return Err("EXPLAIN requires a statement to explain".to_string());
            }
            rest
        }
        _ => &tokens[..],
    };
    validate_statement_tokens(statement)
}

fn validate_statement_tokens(tokens: &[String]) -> Result<(), String> {
    let first = tokens
        .first()
        .map(String::as_str)
        .ok_or_else(|| "SQL query is empty".to_string())?;
    match first {
        "SELECT" => Ok(()),
        "WITH" => {
            let mutating = [
                "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "ATTACH", "DETACH",
                "REPLACE", "VACUUM", "REINDEX", "ANALYZE",
            ];
            if let Some(token) = tokens.iter().find(|token| mutating.contains(&token.as_str())) {
                Err(format!("SQL query is not read-only: contains {token}"))
            } else {
                Ok(())
            }
        }
        "PRAGMA" => validate_schema_pragma(tokens),
        other => Err(format!(
            "Unsupported SQL statement '{other}'; cairn://db permits SELECT, read-only WITH, EXPLAIN, and schema PRAGMAs"
        )),
    }
}

fn reject_statement_chain(sql: &str) -> Result<(), String> {
    let mut chars = sql.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '\'' | '"' | '`' => skip_quoted(ch, &mut chars),
            '[' => skip_bracket_identifier(&mut chars),
            '-' if chars.peek().is_some_and(|(_, next)| *next == '-') => {
                chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next)| *next == '*') => {
                chars.next();
                skip_block_comment(&mut chars);
            }
            ';' => {
                if sql[idx + ch.len_utf8()..].trim().is_empty() {
                    return Ok(());
                }
                return Err("SQL query must contain exactly one statement".to_string());
            }
            _ => {}
        }
    }
    Ok(())
}

fn sql_tokens(sql: &str) -> Vec<String> {
    let mut chars = sql.char_indices().peekable();
    let mut tokens = Vec::new();
    while let Some((_, ch)) = chars.next() {
        match ch {
            '\'' | '"' | '`' => skip_quoted(ch, &mut chars),
            '[' => skip_bracket_identifier(&mut chars),
            '-' if chars.peek().is_some_and(|(_, next)| *next == '-') => {
                chars.next();
                skip_line_comment(&mut chars);
            }
            '/' if chars.peek().is_some_and(|(_, next)| *next == '*') => {
                chars.next();
                skip_block_comment(&mut chars);
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut token = String::from(c);
                while let Some((_, next)) = chars.peek().copied() {
                    if next.is_ascii_alphanumeric() || next == '_' {
                        token.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(token.to_ascii_uppercase());
            }
            _ => {}
        }
    }
    tokens
}

fn skip_quoted(quote: char, chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    while let Some((_, ch)) = chars.next() {
        if ch == quote {
            if chars.peek().is_some_and(|(_, next)| *next == quote) {
                chars.next();
            } else {
                break;
            }
        }
    }
}

fn skip_bracket_identifier(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    for (_, ch) in chars.by_ref() {
        if ch == ']' {
            break;
        }
    }
}

fn skip_line_comment(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    for (_, ch) in chars.by_ref() {
        if ch == '\n' {
            break;
        }
    }
}

fn skip_block_comment(chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>) {
    let mut previous = '\0';
    for (_, ch) in chars.by_ref() {
        if previous == '*' && ch == '/' {
            break;
        }
        previous = ch;
    }
}

fn validate_schema_pragma(tokens: &[String]) -> Result<(), String> {
    let allowed = ["TABLE_LIST", "TABLE_INFO", "INDEX_LIST", "INDEX_INFO"];
    match tokens.get(1).map(String::as_str) {
        Some(name) if allowed.contains(&name) => Ok(()),
        Some(name) => Err(format!(
            "Unsupported PRAGMA '{name}'; allowed schema PRAGMAs are table_list, table_info, index_list, and index_info"
        )),
        None => Err("PRAGMA requires a schema pragma name".to_string()),
    }
}

async fn produce_db_sql_resource(
    orch: &Orchestrator,
    params: &[QueryParam],
    affordance: Option<Affordance>,
) -> RenderedResource {
    let projection = match parse_db_sql_projection(params) {
        Ok(projection) => projection,
        Err(error) => return RenderedResource::line(error, affordance, None, None),
    };
    match run_db_sql_projection(orch, &projection).await {
        Ok((body, shown)) => RenderedResource::records(
            body,
            "rows",
            shown,
            projection.offset,
            projection.limit,
            affordance,
        ),
        Err(error) => {
            RenderedResource::line(format!("SQL query failed: {error}"), affordance, None, None)
        }
    }
}

/// `cairn://dev/db` — relay a read-only SQL projection to a running
/// `dev:instance`. With no `sql`, list registered instances; otherwise resolve
/// the selected instance and query its own `cairn://db` over its callback
/// server, returning the cleaned row body. The instance re-validates the
/// read-only statement policy on its side.
async fn produce_dev_db_sql_resource(
    params: &[QueryParam],
    affordance: Option<Affordance>,
) -> RenderedResource {
    use super::dev_instances;

    if find_query_value(params, "sql").is_none() {
        return RenderedResource::line(
            dev_instances::render_listing().await,
            affordance,
            None,
            None,
        );
    }
    let projection = match parse_db_sql_projection_with(params, &["at"], "cairn://dev/db") {
        Ok(projection) => projection,
        Err(error) => return RenderedResource::line(error, affordance, None, None),
    };
    let instance = match dev_instances::resolve_instance(find_query_value(params, "at")).await {
        Ok(instance) => instance,
        Err(error) => return RenderedResource::line(error.message(), affordance, None, None),
    };
    match dev_instances::query_db(
        &instance,
        &projection.sql,
        projection.offset,
        projection.limit,
    )
    .await
    {
        Ok(body) => RenderedResource::line(body, affordance, None, None),
        Err(error) => RenderedResource::line(error, affordance, None, None),
    }
}

/// `cairn://dev` — the process-introspection collection entrypoint: list running
/// dev instances and the available sub-tools (db, pid).
async fn produce_dev_collection_resource(affordance: Option<Affordance>) -> RenderedResource {
    RenderedResource::line(
        super::dev_instances::render_collection().await,
        affordance,
        None,
        None,
    )
}

/// `cairn://dev/pid` — report the OS process id(s) of running dev instance(s).
/// Each instance reports its own `std::process::id()` over its MCP callback
/// server. `?at=` selects one; otherwise every running instance is listed.
async fn produce_dev_pid_resource(
    params: &[QueryParam],
    affordance: Option<Affordance>,
) -> RenderedResource {
    RenderedResource::line(
        super::dev_instances::render_pids(find_query_value(params, "at")).await,
        affordance,
        None,
        None,
    )
}

async fn run_db_sql_projection(
    orch: &Orchestrator,
    projection: &DbSqlProjection,
) -> crate::storage::DbResult<(String, usize)> {
    let sql = projection
        .sql
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string();
    let offset = projection.offset;
    let limit = projection.limit;
    orch.db
        .local
        .read(move |conn| {
            Box::pin(async move {
                // Best-effort connection guard. The SQL validator and read transaction
                // are the durable safety layers; query_only prevents accidental writes
                // if Turso supports the SQLite pragma in this build.
                let _ = conn.execute("PRAGMA query_only = ON", ()).await;
                let mut rows = conn.query(&sql, ()).await?;
                let columns = rows.column_names();
                let mut rendered_rows = Vec::new();
                let mut seen = 0usize;
                while let Some(row) = rows.next().await? {
                    if seen < offset {
                        seen += 1;
                        continue;
                    }
                    if rendered_rows.len() >= limit {
                        break;
                    }
                    let mut rendered = Vec::with_capacity(row.column_count());
                    for idx in 0..row.column_count() {
                        rendered.push(render_sql_value(row.get_value(idx)?));
                    }
                    rendered_rows.push(rendered.join("\t"));
                    seen += 1;
                }
                let body = render_sql_rows(columns, &rendered_rows);
                Ok((body, rendered_rows.len()))
            })
        })
        .await
}

fn render_sql_rows(columns: Vec<String>, rendered_rows: &[String]) -> String {
    let header = if columns.is_empty() {
        String::new()
    } else {
        columns
            .iter()
            .enumerate()
            .map(|(idx, name)| {
                if name.is_empty() {
                    format!("column_{}", idx + 1)
                } else {
                    escape_sql_text(name)
                }
            })
            .collect::<Vec<_>>()
            .join("\t")
    };
    if rendered_rows.is_empty() {
        if header.is_empty() {
            "(0 rows)".to_string()
        } else {
            format!("{header}\n(0 rows)")
        }
    } else if header.is_empty() {
        rendered_rows.join("\n")
    } else {
        format!("{}\n{}", header, rendered_rows.join("\n"))
    }
}

fn render_sql_value(value: Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Text(value) => escape_sql_text(&value),
        Value::Blob(bytes) => {
            let prefix = bytes
                .iter()
                .take(8)
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            if prefix.is_empty() {
                format!("<blob {} bytes>", bytes.len())
            } else {
                format!("<blob {} bytes hex={}>", bytes.len(), prefix)
            }
        }
    }
}

fn escape_sql_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}

// ============================================================================
// cairn://logs — read-only projection of the app's JSONL log entries
// ============================================================================

/// Max bytes read from the tail of a daily log file before rendering. Daily
/// files can grow to tens of MB; a diagnostic read only needs the recent tail,
/// and the shared line-window view pages within it.
const LOGS_TAIL_MAX_BYTES: u64 = 4 * 1024 * 1024;

/// Max log entries rendered from the tail window. `?offset=-N` tails the most
/// recent N of these via the shared line-window view.
const LOGS_MAX_ENTRIES: usize = 2_000;

/// Map the `process` selector to the log file prefix written by
/// `cairn_common::logging` (`ProcessTag::prefix`).
fn logs_process_prefix(process: &str) -> Option<&'static str> {
    match process {
        "app" => Some("cairn-app"),
        "mcp" => Some("cairn-mcp"),
        "server" => Some("cairn-server"),
        _ => None,
    }
}

/// Resolve the log file for `prefix`, optionally pinned to `date` (`YYYY-MM-DD`).
/// Without a date the newest matching file is chosen: filenames embed the date
/// (`<prefix>.<date>.jsonl`), so a lexical max over the name selects it.
fn resolve_log_file(dir: &Path, prefix: &str, date: Option<&str>) -> Option<PathBuf> {
    if let Some(date) = date {
        let path = dir.join(format!("{prefix}.{date}.jsonl"));
        return path.exists().then_some(path);
    }
    let mut newest: Option<(String, PathBuf)> = None;
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(prefix) && name.ends_with(".jsonl") {
            let replace = newest
                .as_ref()
                .map(|(best, _)| name > *best)
                .unwrap_or(true);
            if replace {
                newest = Some((name, entry.path()));
            }
        }
    }
    newest.map(|(_, path)| path)
}

/// Read up to [`LOGS_MAX_ENTRIES`] trailing lines from a JSONL log file, bounded
/// to the last [`LOGS_TAIL_MAX_BYTES`]. A mid-file seek can land inside a line,
/// so the first (possibly partial) line of a seeked read is dropped.
fn read_log_tail_lines(path: &Path) -> std::io::Result<Vec<String>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    let start = size.saturating_sub(LOGS_TAIL_MAX_BYTES);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let text = String::from_utf8_lossy(&bytes);
    let mut lines: Vec<&str> = text.lines().collect();
    if start > 0 && !lines.is_empty() {
        lines.remove(0);
    }
    let take_from = lines.len().saturating_sub(LOGS_MAX_ENTRIES);
    Ok(lines[take_from..].iter().map(|s| s.to_string()).collect())
}

/// Escape control characters so each rendered entry stays a single greppable
/// line (the universal grep path treats one entry as one line).
fn escape_log_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}

/// Render a non-string structured field value compactly for `key=value` tails.
fn log_field_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Render one JSONL log line as a single plain text line:
/// `TIMESTAMP LEVEL target: message [key=value ...]`. Returns `None` for blank
/// or unparseable lines. Extra structured fields beyond `message` are appended
/// in sorted-key order for determinism.
fn render_log_entry(line: &str) -> Option<String> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    let timestamp = value
        .get("timestamp")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let level = value
        .get("level")
        .and_then(|v| v.as_str())
        .unwrap_or("INFO");
    let target = value.get("target").and_then(|v| v.as_str()).unwrap_or("");
    let fields = value.get("fields");
    let message = fields
        .and_then(|f| f.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("");

    let mut rendered = format!(
        "{timestamp} {level:<5} {target}: {}",
        escape_log_text(message)
    );

    if let Some(serde_json::Value::Object(map)) = fields {
        let mut extras: Vec<(&String, &serde_json::Value)> = map
            .iter()
            .filter(|(key, _)| key.as_str() != "message")
            .collect();
        extras.sort_by(|a, b| a.0.cmp(b.0));
        for (key, val) in extras {
            rendered.push(' ');
            rendered.push_str(key);
            rendered.push('=');
            rendered.push_str(&escape_log_text(&log_field_value(val)));
        }
    }
    Some(rendered)
}

/// Body producer for `cairn://logs`: validate the `process`/`date` selectors,
/// resolve the daily file under `dir`, and render its recent entries as plain
/// lines (most recent last). `offset`/`limit`/grep are consumed by the shared
/// view layer before this is called, so only `process`/`date` reach here.
fn logs_resource_body(dir: &Path, params: &[QueryParam]) -> String {
    for param in params {
        if param.key != "process" && param.key != "date" {
            return format!(
                "Unsupported query parameter '{}' for cairn://logs (supported: process, date)",
                param.key
            );
        }
    }

    let process = find_query_value(params, "process").unwrap_or("app");
    let prefix = match logs_process_prefix(process) {
        Some(prefix) => prefix,
        None => {
            return format!(
                "Unknown process '{process}' for cairn://logs (expected app, mcp, or server)"
            )
        }
    };
    let date = find_query_value(params, "date");

    if !dir.exists() {
        return format!("No log files found: {} does not exist", dir.display());
    }

    let path = match resolve_log_file(dir, prefix, date) {
        Some(path) => path,
        None => {
            return match date {
                Some(date) => format!("No {process} log file for {date} in {}", dir.display()),
                None => format!("No {process} log files in {}", dir.display()),
            }
        }
    };

    let lines = match read_log_tail_lines(&path) {
        Ok(lines) => lines,
        Err(error) => return format!("Failed to read {}: {error}", path.display()),
    };

    let rendered: Vec<String> = lines
        .iter()
        .filter_map(|line| render_log_entry(line))
        .collect();

    if rendered.is_empty() {
        format!("(no log entries in {})", path.display())
    } else {
        rendered.join("\n")
    }
}

/// Grep-family params consumed by the universal grep stage before the body is
/// rendered. `glob` is deliberately excluded: it is `/changed`'s own pushdown
/// filter, applied by the renderer before grep, and is rejected on every other
/// materialized body by `body_grep_payload`.
const GREP_FAMILY_KEYS: &[&str] = &[
    "grep",
    "-i",
    "-n",
    "-A",
    "-B",
    "-C",
    "context",
    "head_limit",
    "limit",
    "offset",
    "multiline",
    "output_mode",
];

/// A render limit large enough to materialize the entire filtered set of a
/// record collection (`/issues`, `/messages`) under grep, replacing the default
/// SQL/cursor page so grep composes over every matching record.
const GREP_RECORD_RENDER_LIMIT: usize = 1_000_000;

/// Drop `keys` from `params`, preserving order and any other params.
fn strip_params(params: &[QueryParam], keys: &[&str]) -> Vec<QueryParam> {
    params
        .iter()
        .filter(|param| !keys.contains(&param.key.as_str()))
        .cloned()
        .collect()
}

/// Read a Cairn URI resource, flattening the structured producer back to a
/// String: content, then the affordance block separated by a blank line. The
/// non-batch callers (archival, grep adapters) consume this flattened form.
pub(crate) async fn read_cairn_resource(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    uri: &str,
) -> String {
    let rendered = produce_cairn_resource(orch, request, uri).await;
    match rendered.affordance {
        Some(affordance) if !affordance.block.is_empty() => {
            format!("{}\n\n{}", rendered.content, affordance.block)
        }
        _ => rendered.content,
    }
}

/// Resolve a `cairn://` resource into a [`RenderedResource`]: separated content
/// and affordance with natural-unit metadata. Owns all universal query handling;
/// `offset`/`limit` are consumed here so the per-resource renderers only ever
/// see their own projections, and every `Line`-unit resource is windowed by the
/// shared view layer (fixing the `chat?limit=N` unsupported-param regression).
pub(crate) async fn produce_cairn_resource(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    uri: &str,
) -> RenderedResource {
    // The MCP gateway family carries an external resource URI tail that may
    // itself contain '?'; route it before split_target_query, which would
    // otherwise steal that query as Cairn params. The family has no Cairn-side
    // projections, so the whole tail belongs to the external resource.
    if let Some(CairnResource::Mcp { server, resource }) = parse_uri(uri) {
        let content =
            crate::mcp::handlers::mcp_resources::handle_mcp_read(orch, request, server, resource)
                .await;
        return RenderedResource::line(content, None, None, None);
    }

    let split = match split_target_query(uri) {
        Ok(split) => split,
        Err(error) => {
            return RenderedResource::line(
                format!("Invalid resource URI: {} ({})", uri, error),
                None,
                None,
                None,
            )
        }
    };

    let identity =
        match resolve_home_relative_resource_uri(&orch.db.local, request, &split.identity).await {
            Ok(identity) => identity,
            Err(error) => return RenderedResource::line(error, None, None, None),
        };

    let resource = match parse_uri(&identity) {
        Some(r) => r,
        None => {
            return RenderedResource::line(
                format!("Invalid resource URI: {}", uri),
                None,
                None,
                None,
            )
        }
    };

    let affordance = {
        // Node/task artifacts derive their affordance example from the schema the
        // addressed name actually validates against, so a copied example is a
        // valid write even for a custom schema (CAIRN #170). Fall back to the
        // static contract block when no schema resolves.
        let block = match &resource {
            CairnResource::NodeArtifact {
                project,
                number,
                exec_seq,
                node_id,
                name,
            } => super::node::artifact_affordance_block(
                orch,
                project,
                *number,
                *exec_seq,
                node_id,
                None,
                name.as_deref(),
                resource.kind(),
            )
            .await
            .unwrap_or_else(|| affordance_for_kind(resource.kind())),
            CairnResource::TaskArtifact {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                name,
            } => super::node::artifact_affordance_block(
                orch,
                project,
                *number,
                *exec_seq,
                node_id,
                Some(task_name),
                name.as_deref(),
                resource.kind(),
            )
            .await
            .unwrap_or_else(|| affordance_for_kind(resource.kind())),
            _ => affordance_for_kind(resource.kind()),
        };
        (!block.is_empty()).then_some(Affordance {
            kind: SegmentKind::Resource,
            block,
        })
    };

    // Universal view-window params, consumed centrally.
    let view_offset = find_query_value(&split.params, "offset").and_then(|v| v.parse::<i64>().ok());
    let view_limit = find_query_value(&split.params, "limit").and_then(|v| v.parse::<usize>().ok());

    // Universal grep: when `grep` is present this is a view projection over the
    // resource's produced text, not a per-resource feature. Render the full
    // (pushdown-filtered) body, then run the same in-memory grep the file path
    // uses, emitting the identical line-number-prefixed contract. `/changed`
    // keeps `glob` as its own pushdown (consumed before grep); every other
    // materialized body rejects `glob`/`type`.
    let allow_glob = matches!(
        resource,
        CairnResource::Changed { .. } | CairnResource::NodeChanged { .. }
    );
    match crate::mcp::handlers::search::body_grep_payload(&split.params, allow_glob) {
        Err(error) => return RenderedResource::line(error, affordance, None, None),
        Ok(Some(payload)) => {
            // Record collections page by SQL/cursor; under grep the entire
            // filtered set must be rendered before grep, so force an unbounded
            // limit in place of the default page.
            let record_collection = matches!(
                resource,
                CairnResource::ProjectIssues { .. }
                    | CairnResource::ProjectMessages { .. }
                    | CairnResource::IssueMessages { .. }
                    | CairnResource::NodeMessages { .. }
                    | CairnResource::TaskMessages { .. }
            );
            let mut body_params = strip_params(&split.params, GREP_FAMILY_KEYS);
            if record_collection {
                body_params.push(QueryParam {
                    key: "limit".to_string(),
                    value: GREP_RECORD_RENDER_LIMIT.to_string(),
                });
            }
            let rendered_body = if let CairnResource::ProjectIssues { project } = &resource {
                match produce_project_issues(&orch.db.local, project, &body_params).await {
                    Ok(page) => page.body,
                    Err(error) => return RenderedResource::line(error, affordance, None, None),
                }
            } else {
                render_resource_body(orch, request, &resource, body_params).await
            };
            let (content, match_count) =
                crate::mcp::handlers::search::grep_materialized_body(&rendered_body, &payload);
            return RenderedResource::grep(content, match_count, affordance);
        }
        Ok(None) => {}
    }

    if matches!(resource, CairnResource::Db) {
        return produce_db_sql_resource(orch, &split.params, affordance).await;
    }

    if matches!(resource, CairnResource::DevDb) {
        return produce_dev_db_sql_resource(&split.params, affordance).await;
    }

    if matches!(resource, CairnResource::Dev) {
        return produce_dev_collection_resource(affordance).await;
    }

    if matches!(resource, CairnResource::DevPid) {
        return produce_dev_pid_resource(&split.params, affordance).await;
    }

    // Browser screenshot: a host-native capture returned as an image block rather
    // than text. Intercept before the text body path (other browser reads are
    // line-unit text). Only `?screenshot` diverts here; content reads fall
    // through to the normal text renderer below.
    if matches!(
        resource,
        CairnResource::NodeBrowser { .. }
            | CairnResource::TaskBrowser { .. }
            | CairnResource::ProjectBrowser { .. }
    ) {
        match super::browsers::parse_browser_read_mode(&split.params) {
            Ok(super::browsers::BrowserReadMode::Screenshot) => {
                let (header, image) =
                    super::browsers::render_browser_screenshot(orch, &resource).await;
                let mut rendered = RenderedResource::line(header, affordance, None, None);
                if let Some(image) = image {
                    rendered.images.push(image);
                }
                return rendered;
            }
            // Content/console/network reads are line-unit text; fall through.
            Ok(_) => {}
            Err(error) => return RenderedResource::line(error, affordance, None, None),
        }
    }

    // `/issues`: unit = issue. limit/offset push down to SQL; the header reports
    // a truthful total from a COUNT over the same filters.
    if let CairnResource::ProjectIssues { project } = &resource {
        return match produce_project_issues(&orch.db.local, project, &split.params).await {
            Ok(page) => RenderedResource {
                content: page.body,
                natural_unit: NaturalUnit::Record,
                unit_noun: Some("issues"),
                total_units: Some(page.total),
                shown_units: Some(page.shown),
                offset: Some(page.offset as i64),
                limit: view_limit,
                match_count: None,
                affordance,
                images: Vec::new(),
            },
            Err(error) => RenderedResource::line(error, affordance, view_offset, view_limit),
        };
    }

    // `/messages` (project/issue/node/task): unit = message. `limit` is the
    // cursor query's pushdown window; `offset` is not meaningful on a
    // reverse-cursor stream and is dropped. Total is not cheaply knowable, so
    // the suffix is omitted (total_units = None).
    let is_messages = matches!(
        resource,
        CairnResource::ProjectMessages { .. }
            | CairnResource::IssueMessages { .. }
            | CairnResource::NodeMessages { .. }
            | CairnResource::TaskMessages { .. }
    );
    if is_messages {
        let body_params = strip_params(&split.params, &["offset"]);
        let content = render_resource_body(orch, request, &resource, body_params).await;
        return RenderedResource {
            content,
            natural_unit: NaturalUnit::Record,
            unit_noun: Some("messages"),
            total_units: None,
            shown_units: None,
            offset: None,
            limit: view_limit,
            match_count: None,
            affordance,
            images: Vec::new(),
        };
    }

    let is_chat_digest = matches!(
        resource,
        CairnResource::NodeChat { .. } | CairnResource::TaskChat { .. }
    );
    if is_chat_digest {
        let content = render_resource_body(orch, request, &resource, split.params.clone()).await;
        let shown_turns = rendered_chat_turn_count(&content);
        let total_turns = rendered_chat_total_turns(&content).unwrap_or(shown_turns);
        let resolved_offset = match view_offset.unwrap_or(0) {
            raw if raw < 0 => total_turns.saturating_sub(raw.unsigned_abs() as usize),
            raw => (raw as usize).min(total_turns),
        };
        return RenderedResource::turns(
            content,
            shown_turns,
            resolved_offset,
            view_limit,
            total_turns,
            affordance,
        );
    }

    // Resources whose renderer owns its own params (search/grep projections):
    // keep every param except `offset` (which the view applies as a line
    // window), and never re-window by `limit`.
    let renderer_owns_params = matches!(
        resource,
        CairnResource::Changed { .. } | CairnResource::NodeChanged { .. }
    ) || matches!(resource, CairnResource::Project { .. })
        && find_query_value(&split.params, "search").is_some();

    // Browser ?console/?network/?interactive reads consume `limit` as a
    // page-side buffer/element cap inside the producer (the bridge request),
    // not just as a view line window. Keep `limit` in their body params (strip
    // only `offset`) so the cap actually reaches the producer; the view still
    // applies `view_limit` to the rendered window below.
    let is_browser_resource = matches!(
        resource,
        CairnResource::NodeBrowser { .. }
            | CairnResource::TaskBrowser { .. }
            | CairnResource::ProjectBrowser { .. }
    );

    let body_params = if renderer_owns_params || is_browser_resource {
        strip_params(&split.params, &["offset"])
    } else {
        strip_params(&split.params, &["offset", "limit"])
    };
    let content = render_resource_body(orch, request, &resource, body_params).await;
    RenderedResource::line(
        content,
        affordance,
        view_offset,
        if renderer_owns_params {
            None
        } else {
            view_limit
        },
    )
}

/// Render a resolved resource's body content (no affordance: the producer
/// attaches it). Universal `offset`/`limit` are already consumed upstream, so
/// each arm only sees its own projections.
async fn render_resource_body(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    resource: &CairnResource,
    params: Vec<QueryParam>,
) -> String {
    match resource.clone() {
        CairnResource::Db => produce_db_sql_resource(orch, &params, None).await.content,
        CairnResource::Dev => produce_dev_collection_resource(None).await.content,
        CairnResource::DevDb => produce_dev_db_sql_resource(&params, None).await.content,
        CairnResource::DevPid => produce_dev_pid_resource(&params, None).await.content,
        CairnResource::Logs => logs_resource_body(&cairn_common::paths::cairn_log_dir(), &params),
        CairnResource::Project { project } => {
            if find_query_value(&params, "search").is_some() {
                read_project_search(orch, &project, &params).await
            } else if let Some(error) = reject_query_params("project", &params) {
                error
            } else {
                read_project(&orch.db.local, &project).await
            }
        }
        CairnResource::ProjectIssues { project } => {
            read_project_issues(&orch.db.local, &project, &params).await
        }
        CairnResource::Issue { project, number } => {
            if let Some(error) = reject_query_params("issue", &params) {
                error
            } else {
                read_issue(&orch.db.local, &project, number).await
            }
        }
        CairnResource::IssueExecutions { project, number } => {
            if let Some(error) = reject_query_params("issue executions", &params) {
                error
            } else {
                read_issue_executions(&orch.db.local, &project, number).await
            }
        }
        CairnResource::IssueExecution {
            project,
            number,
            exec_seq,
        } => {
            if let Some(error) = reject_query_params("execution snapshot", &params) {
                error
            } else {
                read_issue_execution(&orch.db.local, &project, number, exec_seq).await
            }
        }
        CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            // `?diff=full` inlines the live PR patch when this node is a `pr`
            // action node; any other query param is unsupported here.
            let unexpected: Vec<&str> = params
                .iter()
                .filter(|param| param.key != "diff")
                .map(|param| param.key.as_str())
                .collect();
            if !unexpected.is_empty() {
                format!(
                    "Query parameters are not supported on node resources: {}",
                    unexpected.join(", ")
                )
            } else {
                let diff_full = find_query_value(&params, "diff") == Some("full");
                read_node(orch, &project, number, exec_seq, &node_id, diff_full).await
            }
        }
        CairnResource::Task {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => {
            if let Some(error) = reject_query_params("task", &params) {
                error
            } else {
                read_task(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    &task_name,
                )
                .await
            }
        }
        CairnResource::NodeChat {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            read_node_chat(
                &orch.db.local,
                &project,
                number,
                exec_seq,
                &node_id,
                &params,
            )
            .await
        }
        CairnResource::NodeChatRaw {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            if let Some(error) = reject_query_params("node chat", &params) {
                error
            } else {
                read_node_chat_raw(&orch.db.local, &project, number, exec_seq, &node_id).await
            }
        }
        CairnResource::NodeChatTurn {
            project,
            number,
            exec_seq,
            node_id,
            turn_seq,
        } => {
            if let Some(error) = reject_query_params("node chat turn", &params) {
                error
            } else {
                read_node_chat_turn(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    turn_seq,
                )
                .await
            }
        }
        CairnResource::NodeChatEvent {
            project,
            number,
            exec_seq,
            node_id,
            run_seq,
            event_seq,
        } => {
            if let Some(error) = reject_query_params("node chat event", &params) {
                error
            } else {
                read_node_chat_event(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    run_seq,
                    event_seq,
                )
                .await
            }
        }
        CairnResource::NodeArtifact {
            project,
            number,
            exec_seq,
            node_id,
            name,
        } => {
            // `?diff=full` inlines the live PR patch on a `/pr` artifact; any
            // other query param is unsupported here.
            let unexpected: Vec<&str> = params
                .iter()
                .filter(|param| param.key != "diff")
                .map(|param| param.key.as_str())
                .collect();
            if !unexpected.is_empty() {
                format!(
                    "Query parameters are not supported on node artifact resources: {}",
                    unexpected.join(", ")
                )
            } else {
                let diff_full = find_query_value(&params, "diff") == Some("full");
                read_node_artifact(
                    orch,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    name.as_deref(),
                    diff_full,
                )
                .await
            }
        }
        CairnResource::TaskChat {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => {
            read_task_chat(
                &orch.db.local,
                &project,
                number,
                exec_seq,
                &node_id,
                &task_name,
                &params,
            )
            .await
        }
        CairnResource::TaskChatRaw {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => {
            if let Some(error) = reject_query_params("task chat", &params) {
                error
            } else {
                read_task_chat_raw(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    &task_name,
                )
                .await
            }
        }
        CairnResource::TaskChatTurn {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            turn_seq,
        } => {
            if let Some(error) = reject_query_params("task chat turn", &params) {
                error
            } else {
                read_task_chat_turn(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    &task_name,
                    turn_seq,
                )
                .await
            }
        }
        CairnResource::TaskChatEvent {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            run_seq,
            event_seq,
        } => {
            if let Some(error) = reject_query_params("task chat event", &params) {
                error
            } else {
                read_task_chat_event(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    &task_name,
                    run_seq,
                    event_seq,
                )
                .await
            }
        }
        CairnResource::TaskArtifact {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            name: _,
        } => {
            if let Some(error) = reject_query_params("task artifact", &params) {
                error
            } else {
                read_task_artifact(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    &task_name,
                )
                .await
            }
        }
        CairnResource::JobTodos {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => {
            if let Some(error) = reject_query_params("todos", &params) {
                error
            } else {
                read_job_todos(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    task_name.as_deref(),
                )
                .await
            }
        }
        CairnResource::NodeTasks {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            if let Some(error) = reject_query_params("node tasks", &params) {
                error
            } else {
                read_node_tasks(&orch.db.local, &project, number, exec_seq, &node_id).await
            }
        }
        CairnResource::NodeWakes {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            if let Some(error) = reject_query_params("node wakes", &params) {
                error
            } else {
                read_node_wakes(&orch.db.local, &project, number, exec_seq, &node_id).await
            }
        }
        CairnResource::NodeQuestions {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            if let Some(error) = reject_query_params("node questions", &params) {
                error
            } else {
                read_node_questions(&orch.db.local, &project, number, exec_seq, &node_id).await
            }
        }
        CairnResource::NodeQuestion {
            project,
            number,
            exec_seq,
            node_id,
            segment,
        } => {
            if let Some(error) = reject_query_params("node question", &params) {
                error
            } else {
                read_node_question(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    &segment,
                )
                .await
            }
        }
        CairnResource::NodePermissions {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            if let Some(error) = reject_query_params("node permissions", &params) {
                error
            } else {
                read_node_permissions(&orch.db.local, &project, number, exec_seq, &node_id).await
            }
        }
        CairnResource::NodePermission {
            project,
            number,
            exec_seq,
            node_id,
            segment,
        } => {
            if let Some(error) = reject_query_params("node permission", &params) {
                error
            } else {
                read_node_permission(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    &segment,
                )
                .await
            }
        }
        CairnResource::TaskPermissions {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => {
            if let Some(error) = reject_query_params("task permissions", &params) {
                error
            } else {
                read_task_permissions(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    &task_name,
                )
                .await
            }
        }
        CairnResource::TaskPermission {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            segment,
        } => {
            if let Some(error) = reject_query_params("task permission", &params) {
                error
            } else {
                read_task_permission(
                    &orch.db.local,
                    &project,
                    number,
                    exec_seq,
                    &node_id,
                    &task_name,
                    &segment,
                )
                .await
            }
        }
        CairnResource::Changed { project, number } => {
            if params.is_empty() {
                read_issue_changed(&orch.db.local, &project, number).await
            } else {
                read_issue_changed_projection(&orch.db.local, &project, number, &params).await
            }
        }
        CairnResource::NodeChanged {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            if params.is_empty() {
                read_node_changed(orch, &project, number, exec_seq, &node_id).await
            } else {
                read_node_changed_projection(orch, &project, number, exec_seq, &node_id, &params)
                    .await
            }
        }
        CairnResource::ProjectMessages { project } => {
            read_project_messages(&orch.db.local, &project, &params).await
        }
        CairnResource::IssueMessages { project, number } => {
            read_issue_messages(&orch.db.local, &project, number, &params).await
        }
        CairnResource::IssueComments { project, number } => {
            if let Some(error) = reject_query_params("issue comments", &params) {
                error
            } else {
                read_issue_comments(&orch.db.local, &project, number).await
            }
        }
        CairnResource::IssueComment {
            project,
            number,
            comment_seq,
        } => {
            if let Some(error) = reject_query_params("issue comment", &params) {
                error
            } else {
                read_issue_comment(&orch.db.local, &project, number, comment_seq).await
            }
        }
        // Canonical node/task messaging read target (read/append symmetry).
        CairnResource::NodeMessages {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            read_node_messages(
                &orch.db.local,
                &project,
                number,
                exec_seq,
                &node_id,
                None,
                &params,
            )
            .await
        }
        CairnResource::TaskMessages {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => {
            read_node_messages(
                &orch.db.local,
                &project,
                number,
                exec_seq,
                &node_id,
                Some(&task_name),
                &params,
            )
            .await
        }
        CairnResource::Skills => {
            if let Some(error) = reject_query_params("skills", &params) {
                error
            } else {
                crate::mcp::handlers::skills_resources::read_skills_collection(orch, request, None)
                    .await
            }
        }
        CairnResource::Skill { skill_id, path } => {
            if let Some(error) = reject_query_params("skill", &params) {
                error
            } else {
                crate::mcp::handlers::skills_resources::read_skill(
                    orch, request, &skill_id, &path, None,
                )
                .await
            }
        }
        CairnResource::ProjectSkills { project } => {
            if let Some(error) = reject_query_params("project skills", &params) {
                error
            } else {
                crate::mcp::handlers::skills_resources::read_skills_collection(
                    orch,
                    request,
                    Some(&project),
                )
                .await
            }
        }
        CairnResource::ProjectSkill {
            project,
            skill_id,
            path,
        } => {
            if let Some(error) = reject_query_params("project skill", &params) {
                error
            } else {
                crate::mcp::handlers::skills_resources::read_skill(
                    orch,
                    request,
                    &skill_id,
                    &path,
                    Some(&project),
                )
                .await
            }
        }
        CairnResource::Labels => {
            if let Some(error) = reject_query_params("labels", &params) {
                error
            } else {
                read_labels(&orch.db.local).await
            }
        }
        CairnResource::Label { label_id } => {
            if let Some(error) = reject_query_params("label", &params) {
                error
            } else {
                read_label(&orch.db.local, &label_id).await
            }
        }
        CairnResource::NodeSymbols {
            project,
            number,
            exec_seq,
            node_id,
            symbol,
        } => {
            read_node_symbols(
                orch,
                &project,
                number,
                exec_seq,
                &node_id,
                symbol.as_deref(),
                &params,
            )
            .await
        }
        CairnResource::ProjectSymbols { project, symbol } => {
            read_project_symbols(orch, &project, symbol.as_deref(), &params).await
        }
        CairnResource::NodeMemories {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            if let Some(error) = reject_query_params("node memories", &params) {
                error
            } else {
                read_node_memories_collection(orch, &project, number, exec_seq, &node_id).await
            }
        }
        CairnResource::NodeMemory {
            project,
            number,
            exec_seq,
            node_id,
            memory_seq,
        } => {
            if let Some(error) = reject_query_params("node memory", &params) {
                error
            } else {
                read_node_memory(orch, &project, number, exec_seq, &node_id, memory_seq).await
            }
        }
        CairnResource::Recipes => {
            if let Some(error) = reject_query_params("recipes", &params) {
                error
            } else {
                read_recipes_collection(orch, request, None).await
            }
        }
        CairnResource::Recipe { recipe_id } => {
            if let Some(error) = reject_query_params("recipe", &params) {
                error
            } else {
                read_recipe(orch, request, &recipe_id, None).await
            }
        }
        CairnResource::ProjectRecipes { project } => {
            if let Some(error) = reject_query_params("project recipes", &params) {
                error
            } else {
                read_recipes_collection(orch, request, Some(&project)).await
            }
        }
        CairnResource::ProjectRecipe { project, recipe_id } => {
            if let Some(error) = reject_query_params("project recipe", &params) {
                error
            } else {
                read_recipe(orch, request, &recipe_id, Some(&project)).await
            }
        }
        CairnResource::Agents => {
            if let Some(error) = reject_query_params("agents", &params) {
                error
            } else {
                read_agents_collection(orch, request, None).await
            }
        }
        CairnResource::Agent { agent_id } => {
            if let Some(error) = reject_query_params("agent", &params) {
                error
            } else {
                read_agent(orch, request, &agent_id, None).await
            }
        }
        CairnResource::ProjectAgents { project } => {
            if let Some(error) = reject_query_params("project agents", &params) {
                error
            } else {
                read_agents_collection(orch, request, Some(&project)).await
            }
        }
        CairnResource::ProjectAgent { project, agent_id } => {
            if let Some(error) = reject_query_params("project agent", &params) {
                error
            } else {
                read_agent(orch, request, &agent_id, Some(&project)).await
            }
        }
        CairnResource::Actions => {
            if let Some(error) = reject_query_params("actions", &params) {
                error
            } else {
                read_actions_collection(orch, request, None).await
            }
        }
        CairnResource::Action { action_id } => {
            if let Some(error) = reject_query_params("action", &params) {
                error
            } else {
                read_action(orch, &action_id, None).await
            }
        }
        CairnResource::ProjectActions { project } => {
            if let Some(error) = reject_query_params("project actions", &params) {
                error
            } else {
                read_actions_collection(orch, request, Some(&project)).await
            }
        }
        CairnResource::ProjectAction { project, action_id } => {
            if let Some(error) = reject_query_params("project action", &params) {
                error
            } else {
                read_action(orch, &action_id, Some(&project)).await
            }
        }
        CairnResource::Settings => {
            if let Some(error) = reject_query_params("settings", &params) {
                error
            } else {
                read_settings(orch).await
            }
        }
        CairnResource::Projects => {
            if let Some(error) = reject_query_params("projects", &params) {
                error
            } else {
                read_projects(&orch.db.local).await
            }
        }
        CairnResource::ProjectSettings { project } => {
            if let Some(error) = reject_query_params("project settings", &params) {
                error
            } else {
                read_project_settings(orch, &project).await
            }
        }
        // Terminal URIs are routed to read_resource by the MCP binary and should never reach here
        CairnResource::NodeTerminal { .. }
        | CairnResource::TaskTerminal { .. }
        | CairnResource::ProjectTerminal { .. } => {
            "Terminal URIs are handled by read_resource".to_string()
        }
        // Browser reads render the durable job_browsers row (url/title/status)
        // plus, when a webview is live, an extraction of the current page content
        // via the injected content-script bridge. `?format=markdown|text`.
        // `?screenshot` is handled earlier in `produce_cairn_resource` (it
        // returns an image block); reaching here under screenshot means the
        // image path was bypassed (e.g. combined with `?grep`).
        CairnResource::NodeBrowser { .. }
        | CairnResource::TaskBrowser { .. }
        | CairnResource::ProjectBrowser { .. } => {
            match super::browsers::parse_browser_read_mode(&params) {
                Ok(super::browsers::BrowserReadMode::Content(format)) => {
                    super::browsers::render_browser(orch, resource, format).await
                }
                Ok(super::browsers::BrowserReadMode::Console { limit }) => {
                    super::browsers::render_browser_console(orch, resource, limit).await
                }
                Ok(super::browsers::BrowserReadMode::Network { limit }) => {
                    super::browsers::render_browser_network(orch, resource, limit).await
                }
                Ok(super::browsers::BrowserReadMode::Interactive { limit }) => {
                    super::browsers::render_browser_interactive(orch, resource, limit).await
                }
                Ok(super::browsers::BrowserReadMode::Screenshot) => {
                    "Browser screenshots are images; read cairn:~/browser?screenshot on its own (not combined with grep).".to_string()
                }
                Err(error) => error,
            }
        }
        CairnResource::Bug => {
            "cairn://bug is write-only; use change with mode=append to submit reports".to_string()
        }
        CairnResource::Help => {
            if let Some(error) = reject_query_params("help", &params) {
                error
            } else {
                crate::system_prompt::cairn_help()
            }
        }
        // Web search: the query rides in `?q=` as literal text (no URL-encoding).
        // Results are normalized to a ranked title · url · snippet list by the
        // active typed provider.
        CairnResource::WebSearch => {
            let query = find_query_value(&params, "q")
                .map(str::trim)
                .unwrap_or_default();
            if query.is_empty() {
                "Web search needs a query: read cairn://websearch?q=<your query> (the text after q= is the literal query; spaces are fine).".to_string()
            } else {
                match crate::mcp::handlers::search_web::search_web(orch, query).await {
                    Ok(results) => results,
                    Err(message) => message,
                }
            }
        }
        CairnResource::Mcp { server, resource } => {
            crate::mcp::handlers::mcp_resources::handle_mcp_read(orch, request, server, resource)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::query::parse_query_params;
    use std::sync::Arc;

    fn db_params(query: &str) -> Vec<QueryParam> {
        parse_query_params(query).unwrap()
    }

    #[test]
    fn db_sql_validation_accepts_read_shapes() {
        validate_read_only_sql("SELECT id, title FROM issues").unwrap();
        validate_read_only_sql("WITH recent AS (SELECT id FROM issues) SELECT * FROM recent")
            .unwrap();
        validate_read_only_sql("PRAGMA table_list").unwrap();
        validate_read_only_sql("PRAGMA table_info(issues)").unwrap();
        // EXPLAIN and EXPLAIN QUERY PLAN describe a read statement without
        // executing it; both are permitted for query-plan inspection.
        validate_read_only_sql("EXPLAIN SELECT id FROM issues").unwrap();
        validate_read_only_sql("EXPLAIN QUERY PLAN SELECT id FROM runs WHERE session_id = 'x'")
            .unwrap();
        validate_read_only_sql(
            "EXPLAIN QUERY PLAN WITH recent AS (SELECT id FROM issues) SELECT * FROM recent",
        )
        .unwrap();
    }

    #[test]
    fn db_sql_validation_rejects_writes_and_statement_chains() {
        for sql in [
            "",
            "INSERT INTO issues(id) VALUES ('x')",
            "UPDATE issues SET title = 'x'",
            "DELETE FROM issues",
            "CREATE TABLE t(id INTEGER)",
            "DROP TABLE issues",
            "ATTACH 'other.db' AS other",
            "SELECT 1; SELECT 2",
            "WITH changed AS (DELETE FROM issues RETURNING id) SELECT * FROM changed",
            // EXPLAIN does not execute, but the described statement must still be
            // a read shape so no mutation ever reaches the engine.
            "EXPLAIN DELETE FROM issues",
            "EXPLAIN QUERY PLAN UPDATE issues SET title = 'x'",
            "EXPLAIN",
            "EXPLAIN QUERY PLAN",
        ] {
            assert!(
                validate_read_only_sql(sql).is_err(),
                "expected rejection for {sql:?}"
            );
        }
    }

    #[test]
    fn db_sql_validation_ignores_semicolons_and_keywords_inside_literals() {
        validate_read_only_sql("SELECT 'INSERT; still text';").unwrap();
        validate_read_only_sql("SELECT \"DELETE\" FROM issues").unwrap();
        validate_read_only_sql("SELECT 1 -- DROP TABLE no-op\n").unwrap();
    }

    #[test]
    fn db_sql_projection_params_require_sql_and_cap_limit() {
        let err = parse_db_sql_projection(&db_params("limit=5")).unwrap_err();
        assert!(err.contains("sql"), "{err}");

        let parsed =
            parse_db_sql_projection(&db_params("sql=SELECT 1&offset=2&limit=5001")).unwrap();
        assert_eq!(parsed.offset, 2);
        assert_eq!(parsed.limit, DB_SQL_MAX_LIMIT);
    }

    #[test]
    fn dev_db_projection_allows_at_and_keeps_db_unchanged() {
        // dev/db requires sql like db, but labels the error with its own URI and
        // accepts the `at` selector alongside sql/offset/limit.
        let err = parse_db_sql_projection_with(&db_params("at=foo"), &["at"], "cairn://dev/db")
            .unwrap_err();
        assert!(
            err.contains("cairn://dev/db") && err.contains("sql"),
            "{err}"
        );

        let parsed = parse_db_sql_projection_with(
            &db_params("sql=SELECT 1&at=main&offset=1&limit=10"),
            &["at"],
            "cairn://dev/db",
        )
        .unwrap();
        assert_eq!(parsed.offset, 1);
        assert_eq!(parsed.limit, 10);

        // An unknown param is still rejected, and the read-only policy holds.
        let unknown = vec![
            QueryParam {
                key: "sql".to_string(),
                value: "SELECT 1".to_string(),
            },
            QueryParam {
                key: "bogus".to_string(),
                value: "1".to_string(),
            },
        ];
        assert!(
            parse_db_sql_projection_with(&unknown, &["at"], "cairn://dev/db")
                .unwrap_err()
                .contains("bogus")
        );
        assert!(parse_db_sql_projection_with(
            &db_params("sql=DELETE FROM issues"),
            &["at"],
            "cairn://dev/db"
        )
        .is_err());

        // cairn://db is unchanged: it does not accept the dev/db selector.
        let db_with_at = vec![
            QueryParam {
                key: "sql".to_string(),
                value: "SELECT 1".to_string(),
            },
            QueryParam {
                key: "at".to_string(),
                value: "x".to_string(),
            },
        ];
        assert!(parse_db_sql_projection(&db_with_at)
            .unwrap_err()
            .contains("at"));
    }

    #[test]
    fn sql_value_rendering_escapes_model_hostile_values() {
        assert_eq!(render_sql_value(Value::Null), "NULL");
        assert_eq!(render_sql_value(Value::Integer(42)), "42");
        assert_eq!(render_sql_value(Value::Real(1.5)), "1.5");
        assert_eq!(
            render_sql_value(Value::Text("a\tb\nc\\d".to_string())),
            "a\\tb\\nc\\\\d"
        );
        assert_eq!(
            render_sql_value(Value::Blob(vec![0, 1, 2, 10, 255, 254, 253, 252, 251])),
            "<blob 9 bytes hex=0001020afffefdfc>"
        );
    }

    async fn db_projection_orch() -> (Orchestrator, tempfile::TempDir) {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, SearchIndex};

        let db_dir = tempfile::tempdir().unwrap();
        let local = LocalDb::open(db_dir.path().join("projection.db"))
            .await
            .unwrap();
        local
            .exclusive(|conn| {
                Box::pin(async move {
                    conn.execute_batch(
                        "
                        CREATE TABLE db_projection_rows (
                            id INTEGER PRIMARY KEY,
                            label TEXT,
                            note TEXT,
                            data BLOB,
                            amount REAL
                        );
                        INSERT INTO db_projection_rows(id, label, note, data, amount)
                        VALUES
                            (1, 'alpha', NULL, X'', 1.25),
                            (2, 'bravo', 'line1
line2', X'0001020AFF', 2.5),
                            (3, 'charlie', 'tab	text', X'FF', 3.75);
                        ",
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        let worktree = tempfile::tempdir().unwrap();
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            worktree.path().to_path_buf(),
        )
        .build();
        (orch, db_dir)
    }

    #[tokio::test]
    async fn db_sql_projection_renders_rows_and_record_metadata() {
        let (orch, _db_dir) = db_projection_orch().await;
        let params = db_params(
            "sql=SELECT id, label, note, data FROM db_projection_rows ORDER BY id&offset=1&limit=2",
        );

        let rendered = produce_db_sql_resource(&orch, &params, None).await;

        assert_eq!(rendered.natural_unit, NaturalUnit::Record);
        assert_eq!(rendered.unit_noun, Some("rows"));
        assert_eq!(rendered.offset, Some(1));
        assert_eq!(rendered.limit, Some(2));
        assert_eq!(rendered.shown_units, Some(2));
        assert_eq!(
            rendered.content,
            "id\tlabel\tnote\tdata\n2\tbravo\tline1\\nline2\t<blob 5 bytes hex=0001020aff>\n3\tcharlie\ttab\\ttext\t<blob 1 bytes hex=ff>"
        );
    }

    #[tokio::test]
    async fn db_sql_projection_reports_empty_rows_with_header() {
        let (orch, _db_dir) = db_projection_orch().await;
        let params = db_params("sql=SELECT id, label FROM db_projection_rows WHERE id > 99");

        let rendered = produce_db_sql_resource(&orch, &params, None).await;

        assert_eq!(rendered.shown_units, Some(0));
        assert_eq!(rendered.content, "id\tlabel\n(0 rows)");
    }

    // =========================================================================
    // cairn://logs
    // =========================================================================

    fn log_line(timestamp: &str, level: &str, target: &str, message: &str) -> String {
        format!(
            r#"{{"timestamp":"{timestamp}","level":"{level}","target":"{target}","fields":{{"message":"{message}"}}}}"#
        )
    }

    fn write_log_file(dir: &Path, name: &str, lines: &[String]) {
        let mut body = lines.join("\n");
        body.push('\n');
        std::fs::write(dir.join(name), body).unwrap();
    }

    #[test]
    fn render_log_entry_renders_plain_greppable_line() {
        let line = log_line(
            "2026-06-13T10:00:00Z",
            "ERROR",
            "cairn_core::db",
            "connect failed",
        );
        let rendered = render_log_entry(&line).unwrap();
        assert_eq!(
            rendered,
            "2026-06-13T10:00:00Z ERROR cairn_core::db: connect failed"
        );
    }

    #[test]
    fn render_log_entry_appends_extra_fields_sorted_and_escapes_newlines() {
        let line = r#"{"timestamp":"t","level":"INFO","target":"x","fields":{"message":"line1\nline2","zeta":"z","alpha":1}}"#;
        let rendered = render_log_entry(line).unwrap();
        assert_eq!(rendered, "t INFO  x: line1\\nline2 alpha=1 zeta=z");
        // Single physical line: the embedded newline was escaped.
        assert_eq!(rendered.lines().count(), 1);
    }

    #[test]
    fn render_log_entry_skips_blank_and_invalid_lines() {
        assert!(render_log_entry("").is_none());
        assert!(render_log_entry("   ").is_none());
        assert!(render_log_entry("not json").is_none());
    }

    #[test]
    fn logs_resource_body_renders_recent_entries_most_recent_last() {
        let dir = tempfile::tempdir().unwrap();
        write_log_file(
            dir.path(),
            "cairn-app.2026-06-13.jsonl",
            &[
                log_line("2026-06-13T01:00:00Z", "INFO", "x", "first"),
                log_line("2026-06-13T02:00:00Z", "ERROR", "x", "second"),
            ],
        );
        let body = logs_resource_body(dir.path(), &[]);
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("first"));
        assert!(lines[1].contains("second"));
    }

    #[test]
    fn logs_resource_body_process_selector_picks_file_family() {
        let dir = tempfile::tempdir().unwrap();
        write_log_file(
            dir.path(),
            "cairn-app.2026-06-13.jsonl",
            &[log_line("t", "INFO", "x", "app-entry")],
        );
        write_log_file(
            dir.path(),
            "cairn-mcp.2026-06-13.jsonl",
            &[log_line("t", "INFO", "x", "mcp-entry")],
        );

        let app = logs_resource_body(dir.path(), &db_params("process=app"));
        assert!(app.contains("app-entry") && !app.contains("mcp-entry"));

        let mcp = logs_resource_body(dir.path(), &db_params("process=mcp"));
        assert!(mcp.contains("mcp-entry") && !mcp.contains("app-entry"));
    }

    #[test]
    fn logs_resource_body_defaults_to_newest_file() {
        let dir = tempfile::tempdir().unwrap();
        write_log_file(
            dir.path(),
            "cairn-app.2026-06-12.jsonl",
            &[log_line("t", "INFO", "x", "older")],
        );
        write_log_file(
            dir.path(),
            "cairn-app.2026-06-13.jsonl",
            &[log_line("t", "INFO", "x", "newer")],
        );
        let body = logs_resource_body(dir.path(), &[]);
        assert!(body.contains("newer") && !body.contains("older"));

        let pinned = logs_resource_body(dir.path(), &db_params("date=2026-06-12"));
        assert!(pinned.contains("older") && !pinned.contains("newer"));
    }

    #[test]
    fn logs_resource_body_rejects_unknown_params_and_processes() {
        let dir = tempfile::tempdir().unwrap();
        assert!(logs_resource_body(dir.path(), &db_params("bogus=1"))
            .contains("Unsupported query parameter"));
        assert!(
            logs_resource_body(dir.path(), &db_params("process=nope")).contains("Unknown process")
        );
    }

    #[test]
    fn logs_resource_body_reports_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let body = logs_resource_body(dir.path(), &[]);
        assert!(body.contains("No app log files"), "{body}");
    }

    #[test]
    fn logs_body_composes_with_universal_grep() {
        use crate::mcp::handlers::search::{body_grep_payload, grep_materialized_body};
        let dir = tempfile::tempdir().unwrap();
        write_log_file(
            dir.path(),
            "cairn-app.2026-06-13.jsonl",
            &[
                log_line("t1", "INFO", "x", "all good"),
                log_line("t2", "ERROR", "x", "boom"),
                log_line("t3", "INFO", "x", "fine"),
            ],
        );
        // Mirror the produce_cairn_resource grep path: render the body, then run
        // the same in-memory grep every materialized resource body uses.
        let body = logs_resource_body(dir.path(), &[]);
        let payload = body_grep_payload(&db_params("grep=ERROR"), false)
            .unwrap()
            .unwrap();
        let (rendered, matches) = grep_materialized_body(&body, &payload);
        assert_eq!(matches, 1);
        assert!(rendered.contains("boom"));
        assert!(!rendered.contains("all good"));
    }

    /// End-to-end regression for the read router's param threading: a browser
    /// `?interactive&limit=N` read must carry `limit` all the way into the
    /// page-side `ListInteractive` request, not strip it as a mere view window.
    /// Drives `produce_cairn_resource` against a stub webview that captures the
    /// bridge request and replies, then asserts the emitted request kind+limit.
    #[tokio::test]
    async fn browser_interactive_read_threads_limit_to_the_producer() {
        use crate::browsers::BrowserCommand;
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

        let dir = tempfile::tempdir().unwrap();
        let local = LocalDb::open(dir.path().join("browser-read.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        local
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-brw', 'default', 'Browsers', 'BRW', '/tmp/brw', 1, 1)",
                        (),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        let worktree = tempfile::tempdir().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            worktree.path().to_path_buf(),
        )
        .browser_command_tx(Some(tx))
        .build();

        // Stub webview: reply to the ListInteractive bridge request (so the read
        // completes) and capture the exact request JSON the producer emitted.
        let responses = orch.browser_bridge_responses.clone();
        let (captured_tx, captured_rx) = tokio::sync::oneshot::channel::<String>();
        tokio::spawn(async move {
            let mut captured_tx = Some(captured_tx);
            while let Some(cmd) = rx.recv().await {
                if let BrowserCommand::Bridge {
                    request_id,
                    request_json,
                    ..
                } = cmd
                {
                    if let Some(c) = captured_tx.take() {
                        let _ = c.send(request_json);
                    }
                    let reply = serde_json::json!({ "ok": true, "elements": [] }).to_string();
                    let _ = responses.send((request_id, reply));
                }
            }
        });

        let request = McpCallbackRequest {
            cwd: String::new(),
            run_id: None,
            tool: "read".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        };
        let rendered =
            produce_cairn_resource(&orch, &request, "cairn://p/BRW/browser?interactive&limit=3")
                .await;
        assert!(
            rendered.content.contains("Interactive elements"),
            "{}",
            rendered.content
        );

        let request_json = captured_rx.await.expect("bridge request captured");
        let value: serde_json::Value = serde_json::from_str(&request_json).unwrap();
        assert_eq!(value["kind"], "listInteractive");
        // The regression: without threading, limit arrives null and only the
        // outer line window applies.
        assert_eq!(value["limit"], 3);
    }
}
