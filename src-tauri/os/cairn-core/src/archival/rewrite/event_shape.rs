use super::*;

// ---------------------------------------------------------------------------
// Event-shape helpers. The stored `data` is a serialized `TranscriptEvent`
// (camelCase): tool calls carry `toolUses: [{id, name, input}]` on the assistant
// event; the paired `tool_result` carries `toolUseId` + `toolResult` but no
// `toolInput`, so reads are paired back to their call to recover `paths`.
// ---------------------------------------------------------------------------

/// Map every tool-call id to its `(name, input)` from the assistant events, so a
/// `tool_result` can recover the tool name and (for reads) the requested paths.
pub(crate) fn build_tool_map(events: &[Event]) -> HashMap<String, (String, Value)> {
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
/// `mcp__cairn__write`, never the bare `write` the classifier matched on before.
/// Strip the `mcp__<server>` prefix and return the trailing tool segment,
/// tolerating either a `__` or `.` delimiter before the tool name; a non-MCP
/// name passes through unchanged.
pub(crate) fn normalize_tool_name(name: &str) -> &str {
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

pub(crate) fn event_tool_use_id(data: &str) -> Option<String> {
    let value: Value = serde_json::from_str(data).ok()?;
    value
        .get("toolUseId")
        .or_else(|| value.get("tool_use_id"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// The zstd stub: just enough to render a list-row label. The full original
/// `data` lives compressed in `data_blob` and is restored on read.
pub(crate) fn zstd_stub(event: &Event, tool_name: Option<&str>) -> String {
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
