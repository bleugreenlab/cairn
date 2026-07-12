//! Pure event -> tool-invocation extraction and URI-shape classification.
//!
//! No DB, no I/O: [`extract_run_invocations`] takes a run's reconstructed events
//! and returns one [`ToolInvocation`] per tool use, with `is_error` and
//! `result_ts` linked from the matching `tool_result` event (by tool-use id).
//! Shared by the backfill and
//! unit-tested in isolation.
//!
//! "Target shape" is the tuple (verb, scheme, kind) parsed from a tool's target:
//! the coarse verb (read/write/run) from the tool name, the URI scheme
//! (file/cairn/http/shell), and a resource kind (file extension, cairn resource
//! kind, host, or shell command).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use cairn_db::models::Event;

/// Analytics-owned wire projection kept structurally identical to the stored
/// transcript event so extraction preserves the existing deserialization boundary.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolUseInfo {
    id: String,
    name: String,
    input: Value,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TranscriptEvent {
    event_type: String,
    session_id: Option<String>,
    parent_tool_use_id: Option<String>,
    content: Option<String>,
    thinking: Option<String>,
    tool_name: Option<String>,
    tool_input: Option<Value>,
    tool_uses: Option<Vec<ToolUseInfo>>,
    tool_use_id: Option<String>,
    tool_result: Option<String>,
    is_error: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thinking_ms: Option<i64>,
    raw: Option<Value>,
}

/// Max stored length of a representative target path.
const MAX_TARGET_LEN: usize = 300;

/// One extracted tool invocation, ready to upsert into `tool_invocations`.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolInvocation {
    pub event_id: String,
    pub tool_use_id: String,
    pub run_id: String,
    pub ts: i64,
    pub verb: String,
    pub tool_name: String,
    pub target_scheme: String,
    pub target_kind: String,
    pub target_path: Option<String>,
    pub is_error: bool,
    pub result_ts: Option<i64>,
}

impl ToolInvocation {
    /// Stable upsert key: one row per (event, tool use).
    pub fn id(&self) -> String {
        format!("{}:{}", self.event_id, self.tool_use_id)
    }
}

/// Extract every tool invocation from a run's reconstructed events.
///
/// Two passes, composed from the per-event primitives so the run-level backfill
/// and the per-event live maintenance share one extraction/classification path:
/// first [`tool_result_error`] over every `tool_result` event builds the
/// tool-use-id -> (is_error, result timestamp) map, then
/// [`extract_event_invocations`] over every assistant event yields the rows
/// (result fields linked from the map).
pub fn extract_run_invocations(events: &[Event]) -> Vec<ToolInvocation> {
    let mut results: HashMap<String, (bool, i64)> = HashMap::new();
    for event in events {
        if event.event_type != "tool_result" {
            continue;
        }
        if let Some((id, is_error)) = tool_result_error(&event.data) {
            results.insert(id, (is_error, event.created_at));
        }
    }

    let mut out = Vec::new();
    for event in events {
        if event.event_type != "assistant" {
            continue;
        }
        for mut row in
            extract_event_invocations(&event.id, &event.run_id, event.created_at, &event.data)
        {
            if let Some((is_error, result_ts)) = results.get(&row.tool_use_id).copied() {
                row.is_error = is_error;
                row.result_ts = Some(result_ts);
            }
            out.push(row);
        }
    }
    out
}

/// Extract the tool invocations carried by a SINGLE assistant event's `data`
/// JSON, with `is_error = false` (the matching `tool_result` arrives as its own
/// later event). The per-event counterpart of [`extract_run_invocations`]'s
/// second pass, used by the live insert-time Tier B maintenance so a streamed
/// assistant event's tool uses are indexed without reconstructing the whole run.
pub fn extract_event_invocations(
    event_id: &str,
    run_id: &str,
    created_at: i64,
    data: &str,
) -> Vec<ToolInvocation> {
    let Ok(parsed) = serde_json::from_str::<TranscriptEvent>(data) else {
        return Vec::new();
    };
    let Some(uses) = parsed.tool_uses else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for use_info in uses {
        let base = normalize_base(&use_info.name);
        let verb = verb_for(&base).to_string();
        let (scheme, kind, path) = classify(&base, &use_info.input);
        out.push(ToolInvocation {
            event_id: event_id.to_string(),
            tool_use_id: use_info.id.clone(),
            run_id: run_id.to_string(),
            ts: created_at,
            verb,
            tool_name: base,
            target_scheme: scheme,
            target_kind: kind,
            target_path: path,
            is_error: false,
            result_ts: None,
        });
    }
    out
}

/// The `(tool_use_id, is_error)` a SINGLE `tool_result` event links, or `None`
/// when its data carries no tool-use id. Used both by the run-level error map
/// and by the per-event Tier B maintenance (which updates the matching row).
pub fn tool_result_error(data: &str) -> Option<(String, bool)> {
    let parsed = serde_json::from_str::<TranscriptEvent>(data).ok()?;
    let id = parsed.tool_use_id?;
    Some((id, parsed.is_error))
}

/// Strip an MCP prefix (`mcp__cairn__read` -> `read`) and lowercase.
fn normalize_base(name: &str) -> String {
    name.rsplit("__")
        .next()
        .unwrap_or(name)
        .to_ascii_lowercase()
}

/// Coarse verb for a normalized base tool name.
fn verb_for(base: &str) -> &'static str {
    match base {
        "read" | "grep" | "glob" | "webfetch" | "websearch" | "notebookread" => "read",
        "write" | "edit" | "multiedit" | "notebookedit" | "create" => "write",
        "run" | "bash" | "execute" | "killshell" | "kill_shell" => "run",
        _ => "other",
    }
}

/// Classify a tool use's target into (scheme, kind, path).
fn classify(base: &str, input: &Value) -> (String, String, Option<String>) {
    match extract_target(base, input) {
        Some((raw, true)) => classify_shell(&raw),
        Some((raw, false)) => classify_uri(&raw),
        None => ("none".to_string(), "none".to_string(), None),
    }
}

/// Pull a representative target string from a tool input. The bool marks a raw
/// shell command (no URI) versus a URI/path target.
fn extract_target(base: &str, input: &Value) -> Option<(String, bool)> {
    if matches!(
        base,
        "run" | "bash" | "execute" | "killshell" | "kill_shell"
    ) {
        if let Some(arr) = input.get("commands").and_then(Value::as_array) {
            if let Some(first) = arr.first() {
                if let Some(target) = str_field(first, "target") {
                    return Some((target, false));
                }
                if let Some(command) = str_field(first, "command") {
                    return Some((command, true));
                }
            }
        }
        if let Some(command) = str_field(input, "command") {
            return Some((command, true));
        }
        return None;
    }
    // write: changes[0].target
    if let Some(arr) = input.get("changes").and_then(Value::as_array) {
        if let Some(target) = arr.first().and_then(|c| str_field(c, "target")) {
            return Some((target, false));
        }
    }
    // read: paths[0]
    if let Some(arr) = input.get("paths").and_then(Value::as_array) {
        if let Some(first) = arr.first().and_then(Value::as_str) {
            return Some((first.to_string(), false));
        }
    }
    // native web fetch/search
    if let Some(url) = str_field(input, "url") {
        return Some((url, false));
    }
    // native file tools carry a bare filesystem path; normalize to a `file:`
    // URI so it classifies through the same path as cairn read targets.
    for key in ["file_path", "path", "notebook_path"] {
        if let Some(value) = str_field(input, key) {
            let normalized = if value.contains("://")
                || value.starts_with("file:")
                || value.starts_with("cairn:")
            {
                value
            } else {
                format!("file:{value}")
            };
            return Some((normalized, false));
        }
    }
    // generic single target field (may already be a file:/cairn: URI)
    if let Some(value) = str_field(input, "target") {
        return Some((value, false));
    }
    None
}

fn str_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// A raw shell command: scheme `shell`, kind = the invoked program basename.
fn classify_shell(command: &str) -> (String, String, Option<String>) {
    let program = command.split_whitespace().next().unwrap_or("");
    let base = program.rsplit('/').next().unwrap_or(program);
    let kind = if base.is_empty() {
        "other".to_string()
    } else {
        base.to_string()
    };
    ("shell".to_string(), kind, None)
}

fn classify_uri(raw: &str) -> (String, String, Option<String>) {
    let target = raw.trim();
    if let Some(rest) = target.strip_prefix("file:") {
        let path = rest.split('?').next().unwrap_or(rest);
        return ("file".to_string(), file_kind(path), Some(truncate(path)));
    }
    if target.starts_with("cairn:") {
        let path = target.split('?').next().unwrap_or(target);
        return ("cairn".to_string(), cairn_kind(path), Some(truncate(path)));
    }
    if target.starts_with("http://") || target.starts_with("https://") {
        return ("http".to_string(), host_of(target), Some(truncate(target)));
    }
    (
        "other".to_string(),
        "other".to_string(),
        Some(truncate(target)),
    )
}

/// File kind = lowercase extension, or `dir` for a directory-looking path.
fn file_kind(path: &str) -> String {
    if path.is_empty() || path.ends_with('/') {
        return "dir".to_string();
    }
    let last = path.rsplit('/').next().unwrap_or(path);
    match last.rsplit_once('.') {
        Some((stem, ext))
            if !stem.is_empty()
                && !ext.is_empty()
                && ext.len() <= 8
                && ext.chars().all(|c| c.is_ascii_alphanumeric()) =>
        {
            ext.to_ascii_lowercase()
        }
        _ => "dir".to_string(),
    }
}

/// Resource kind for a `cairn:` URI: a known segment keyword, else a depth-based
/// fallback (project / issue / node).
fn cairn_kind(path: &str) -> String {
    let body = path
        .trim_start_matches("cairn://")
        .trim_start_matches("cairn:");
    let lower = body.to_ascii_lowercase();
    let segments: Vec<&str> = lower.split('/').filter(|s| !s.is_empty()).collect();
    const KEYWORDS: &[&str] = &[
        "symbols",
        "chat",
        "diff",
        "changed",
        "messages",
        "memories",
        "todos",
        "tasks",
        "questions",
        "terminal",
        "skills",
        "recipes",
        "workflows",
        "executions",
        "create-pr",
        "plan",
        "issues",
        // `dev` must precede `db` so cairn://dev/db and cairn://dev/pid bucket
        // as the dev family, not as the host-db `db` kind.
        "dev",
        "db",
        "bug",
        "help",
        "mcp",
        "branch",
        "kind",
    ];
    for keyword in KEYWORDS {
        if segments.iter().any(|s| s == keyword) {
            return (*keyword).to_string();
        }
    }
    if segments.first() == Some(&"p") {
        return match segments.len() {
            0..=2 => "project".to_string(),
            3 => "issue".to_string(),
            _ => "node".to_string(),
        };
    }
    if body.starts_with('~') {
        return "node".to_string();
    }
    "resource".to_string()
}

fn host_of(url: &str) -> String {
    url.split("://")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
        .filter(|h| !h.is_empty())
        .unwrap_or("other")
        .to_ascii_lowercase()
}

fn truncate(s: &str) -> String {
    if s.len() <= MAX_TARGET_LEN {
        s.to_string()
    } else {
        // Cut on a char boundary at or below the limit.
        let mut end = MAX_TARGET_LEN;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        s[..end].to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(id: &str, event_type: &str, data: Value) -> Event {
        Event {
            id: id.to_string(),
            run_id: "run-1".to_string(),
            session_id: None,
            sequence: 0,
            timestamp: 0,
            event_type: event_type.to_string(),
            data: data.to_string(),
            parent_tool_use_id: None,
            created_at: 100,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            turn_id: None,
            thinking_tokens: None,
            cost_usd: None,
            storage_mode: None,
            content_commit: None,
            content_change_id: None,
            content_render_sha: None,
            data_blob: None,
            codec: None,
            read_segments: None,
        }
    }

    fn assistant_with_tool(id: &str, name: &str, input: Value) -> Event {
        event(
            id,
            "assistant",
            json!({
                "eventType": "assistant",
                "isError": false,
                "toolUses": [{ "id": id, "name": name, "input": input }],
            }),
        )
    }

    #[test]
    fn classifies_cairn_read_paths() {
        let ev = assistant_with_tool(
            "t1",
            "mcp__cairn__read",
            json!({ "paths": ["file:src/lib.rs?offset=10", "file:other.ts"] }),
        );
        let rows = extract_run_invocations(&[ev]);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.verb, "read");
        assert_eq!(r.tool_name, "read");
        assert_eq!(r.target_scheme, "file");
        assert_eq!(r.target_kind, "rs");
        assert_eq!(r.target_path.as_deref(), Some("src/lib.rs"));
        assert!(!r.is_error);
    }

    #[test]
    fn classifies_cairn_write_change_target() {
        let ev = assistant_with_tool(
            "t2",
            "write",
            json!({ "changes": [{ "target": "cairn://p/CAIRN/1821", "mode": "append" }] }),
        );
        let rows = extract_run_invocations(&[ev]);
        let r = &rows[0];
        assert_eq!(r.verb, "write");
        assert_eq!(r.target_scheme, "cairn");
        assert_eq!(r.target_kind, "issue");
    }

    #[test]
    fn classifies_shell_run_command() {
        let ev = assistant_with_tool(
            "t3",
            "run",
            json!({ "commands": [{ "command": "cargo test -p cairn-core" }] }),
        );
        let r = &extract_run_invocations(&[ev])[0];
        assert_eq!(r.verb, "run");
        assert_eq!(r.target_scheme, "shell");
        assert_eq!(r.target_kind, "cargo");
        assert_eq!(r.target_path, None);
    }

    #[test]
    fn classifies_run_skill_target() {
        let ev = assistant_with_tool(
            "t4",
            "run",
            json!({ "commands": [{ "target": "cairn://skills/ui/scripts/x" }] }),
        );
        let r = &extract_run_invocations(&[ev])[0];
        assert_eq!(r.target_scheme, "cairn");
        assert_eq!(r.target_kind, "skills");
    }

    #[test]
    fn classifies_native_read_file_path_and_http() {
        let read = assistant_with_tool("t5", "Read", json!({ "file_path": "/abs/path/main.py" }));
        let r = &extract_run_invocations(&[read])[0];
        assert_eq!(r.verb, "read");
        assert_eq!(r.target_scheme, "file");
        assert_eq!(r.target_kind, "py");

        let web = assistant_with_tool(
            "t6",
            "WebFetch",
            json!({ "url": "https://example.com/spec/page" }),
        );
        let w = &extract_run_invocations(&[web])[0];
        assert_eq!(w.target_scheme, "http");
        assert_eq!(w.target_kind, "example.com");
    }

    #[test]
    fn links_error_from_matching_tool_result() {
        let assistant = assistant_with_tool("u1", "read", json!({ "paths": ["file:missing.rs"] }));
        let result = event(
            "r1",
            "tool_result",
            json!({
                "eventType": "tool_result",
                "toolUseId": "u1",
                "toolName": "read",
                "toolResult": "error: not found",
                "isError": true,
            }),
        );
        let mut result = result;
        result.created_at = 140;
        let rows = extract_run_invocations(&[assistant, result]);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_error);
        assert_eq!(rows[0].result_ts, Some(140));
    }

    #[test]
    fn unmatched_tool_use_has_no_result_timestamp() {
        let assistant = assistant_with_tool("u-missing", "read", json!({ "paths": ["file:x.rs"] }));
        let rows = extract_run_invocations(&[assistant]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].result_ts, None);
    }

    #[test]
    fn cairn_kind_variants() {
        assert_eq!(cairn_kind("cairn://p/CAIRN/1821/1/builder/chat"), "chat");
        assert_eq!(cairn_kind("cairn://p/CAIRN/1821/changed"), "changed");
        assert_eq!(cairn_kind("cairn://p/CAIRN/1821/1/builder/diff"), "diff");
        assert_eq!(cairn_kind("cairn://db"), "db");
        assert_eq!(cairn_kind("cairn://dev"), "dev");
        assert_eq!(cairn_kind("cairn://dev/db?sql=SELECT 1"), "dev");
        assert_eq!(cairn_kind("cairn://dev/pid"), "dev");
        assert_eq!(cairn_kind("cairn:~/memories"), "memories");
        assert_eq!(cairn_kind("cairn://p/CAIRN"), "project");
        assert_eq!(cairn_kind("cairn://p/CAIRN/1821"), "issue");
        assert_eq!(cairn_kind("cairn:~/symbols/Foo"), "symbols");
    }

    #[test]
    fn file_kind_handles_dirs_and_dotfiles() {
        assert_eq!(file_kind("src/"), "dir");
        assert_eq!(file_kind("src"), "dir");
        assert_eq!(file_kind("a/b/c.rs"), "rs");
        assert_eq!(file_kind("Cargo.toml"), "toml");
        assert_eq!(file_kind(".gitignore"), "dir");
    }

    #[test]
    fn id_combines_event_and_tool_use() {
        let ev = assistant_with_tool("e1", "read", json!({ "paths": ["file:x.rs"] }));
        let rows = extract_run_invocations(&[ev]);
        assert_eq!(rows[0].id(), "e1:e1");
    }
}
