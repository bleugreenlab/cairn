use super::*;

// ---------------------------------------------------------------------------
// Event-shape helpers. The stored `data` is a serialized `TranscriptEvent`
// (camelCase): tool calls carry `toolUses: [{id, name, input}]` on the assistant
// event; the paired `tool_result` carries `toolUseId` + `toolResult` but no
// `toolInput`, so reads are paired back to their call to recover `paths`.
// ---------------------------------------------------------------------------

/// Map every tool-call id to its `(name, input)` from the assistant events, so a
/// `tool_result` can recover the tool name and (for reads) the requested paths.
pub(super) fn build_tool_map(events: &[Event]) -> HashMap<String, (String, Value)> {
    let mut map = HashMap::new();
    for event in events {
        if event.event_type != "assistant" {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        let Some(tool_uses) = value.get("toolUses").and_then(|v| v.as_array()) else {
            continue;
        };
        for tool in tool_uses {
            let id = tool.get("id").and_then(|v| v.as_str());
            let name = tool.get("name").and_then(|v| v.as_str());
            if let (Some(id), Some(name)) = (id, name) {
                let input = tool.get("input").cloned().unwrap_or(Value::Null);
                map.insert(id.to_string(), (name.to_string(), input));
            }
        }
    }
    map
}

/// Normalize a recorded tool name to the bare MCP tool name the classifier
/// dispatches on (`read`, `write`, `run`). Both backends record MCP calls
/// server-prefixed and identically shaped: the Claude CLI emits `mcp__cairn__read`
/// and Codex builds `format!("mcp__{server}__{tool}")` over `[mcp_servers.cairn]`
/// (backends/codex/runtime.rs), so a real session's `toolUses[].name` is
/// `mcp__cairn__write`, never the bare `write`.
/// Strip the `mcp__<server>` prefix and return the trailing tool segment,
/// tolerating either a `__` or `.` delimiter before the tool name; a non-MCP
/// name passes through unchanged.
pub(super) fn normalize_tool_name(name: &str) -> &str {
    let Some(rest) = name.strip_prefix("mcp__") else {
        return name;
    };
    // `rest` is `<server>__<tool>`; the tool is the final `__`-delimited segment.
    // Peel a trailing `.`-delimited tail too, so a dot-joined server/tool pairing
    // still resolves to the bare tool name.
    let after_underscores = rest.rsplit("__").next().unwrap_or(rest);
    after_underscores
        .rsplit('.')
        .next()
        .unwrap_or(after_underscores)
}

pub(super) fn event_tool_use_id(data: &str) -> Option<String> {
    let value: Value = serde_json::from_str(data).ok()?;
    value
        .get("toolUseId")
        .or_else(|| value.get("tool_use_id"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

pub(super) fn tool_result_text(data: &str) -> Option<String> {
    let value: Value = serde_json::from_str(data).ok()?;
    value
        .get("toolResult")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Read targets from a read tool call's input (`{ "paths": [...] }`). `None` if
/// any element is not a string — such a read cannot be git-addressed.
pub(super) fn read_paths(input: &Value) -> Option<Vec<String>> {
    input
        .get("paths")?
        .as_array()?
        .iter()
        .map(|v| v.as_str().map(str::to_string))
        .collect()
}

/// What a committed write's change report says: the commit, and the indices of
/// the batch's changes that applied (a non-atomic batch can commit some and
/// fail others).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ChangeReport {
    pub(super) sha: String,
    pub(super) applied: HashSet<usize>,
}

/// Parse a write's change report. `None` when nothing committed.
pub(super) fn parse_change_report(tool_result: &str) -> Option<ChangeReport> {
    // The report is the result's leading JSON value; project-check verdicts are
    // appended after it as plain text, so the whole result is not JSON.
    let value: Value = serde_json::Deserializer::from_str(tool_result)
        .into_iter::<Value>()
        .next()?
        .ok()?;
    let sha = value.get("commit")?.get("sha")?.as_str()?.to_string();
    let applied = value
        .get("applied")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("index")?.as_u64())
                .filter_map(|index| usize::try_from(index).ok())
                .collect()
        })
        .unwrap_or_default();
    Some(ChangeReport { sha, applied })
}

/// The sha a run's commit barrier reported (`✓ Committed changes (<sha>)`).
///
/// The barrier line follows the batch's command output, which can itself
/// contain the marker (a grep over this very code, say), so the last
/// occurrence wins and only a hex sha is accepted.
pub(super) fn run_commit_sha(tool_result: &str) -> Option<String> {
    const MARKER: &str = "\u{2713} Committed changes (";
    let start = tool_result.rfind(MARKER)? + MARKER.len();
    let rest = &tool_result[start..];
    let sha = &rest[..rest.find(')')?];
    let is_sha = (7..=64).contains(&sha.len()) && sha.bytes().all(|b| b.is_ascii_hexdigit());
    is_sha.then(|| sha.to_string())
}

/// The gitcoord-read stub: drop the heavy rendered `toolResult`, pin
/// `toolInput.paths` (the contract reconstruction dispatches on). Other fields
/// (eventType, toolUseId) are preserved for a list-row label.
pub(super) fn read_stub(data: &str, paths: &[String]) -> String {
    let mut map = serde_json::from_str::<Value>(data)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    map.insert("toolInput".to_string(), json!({ "paths": paths }));
    map.insert("toolResult".to_string(), Value::Null);
    // Claude records the rendered read a SECOND time under `raw.tool_use_result`
    // (content blocks whose `text` duplicates `toolResult` byte for byte). Nulling
    // `toolResult` alone would leave the whole read in the stub, so drop the
    // duplicate too. Nothing reads `tool_use_result`, so reconstruction does not
    // re-inject it. Codex never duplicates here: its reader keeps `raw` only for
    // non-text MCP content, which a read never produces.
    if let Some(Value::Object(raw)) = map.get_mut("raw") {
        raw.remove("tool_use_result");
    }
    Value::Object(map).to_string()
}

/// The hybrid-read stub: like [`read_stub`] plus the `hybrid_read` marker and
/// the indices of the git-addressed file sections.
/// [`crate::storage::events::encoding::decode`] discriminates on columns
/// (`content_render_sha` presence), not this marker; the marker is for list-row
/// labels and debugging.
pub(super) fn hybrid_stub(data: &str, paths: &[String], indices: &[usize]) -> String {
    let mut map = serde_json::from_str::<Value>(data)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    map.insert("toolInput".to_string(), json!({ "paths": paths }));
    map.insert("toolResult".to_string(), Value::Null);
    map.insert("archived".to_string(), json!("hybrid_read"));
    map.insert("sections".to_string(), json!(indices));
    if let Some(Value::Object(raw)) = map.get_mut("raw") {
        raw.remove("tool_use_result");
    }
    Value::Object(map).to_string()
}

/// The zstd stub: just enough to render a list-row label. The full original
/// `data` lives compressed in `data_blob` and is restored on read.
pub(super) fn zstd_stub(event: &Event, tool_name: Option<&str>) -> String {
    let mut map = serde_json::Map::new();
    map.insert("eventType".to_string(), json!(event.event_type));
    if let Some(name) = tool_name {
        map.insert("toolName".to_string(), json!(name));
    }
    if let Ok(Value::Object(original)) = serde_json::from_str::<Value>(&event.data) {
        if let Some(id) = original.get("toolUseId") {
            map.insert("toolUseId".to_string(), id.clone());
        }
    }
    map.insert("archived".to_string(), json!("zstd"));
    Value::Object(map).to_string()
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
