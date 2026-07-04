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

pub(super) fn tool_result_text(data: &str) -> Option<String> {
    let value: Value = serde_json::from_str(data).ok()?;
    value
        .get("toolResult")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Read targets from a read tool call's input (`{ "paths": [...] }`). `None` if
/// any element is not a string — such a read cannot be gitcoord-addressed.
pub(super) fn read_paths(input: &Value) -> Option<Vec<String>> {
    input
        .get("paths")?
        .as_array()?
        .iter()
        .map(|v| v.as_str().map(str::to_string))
        .collect()
}

/// The committed sha from a change report's `commit.sha` (a short sha to be
/// resolved to full against the live worktree). `None` when nothing committed.
pub(super) fn change_commit_sha(tool_result: &str) -> Option<String> {
    let value: Value = serde_json::from_str(tool_result).ok()?;
    value
        .get("commit")?
        .get("sha")?
        .as_str()
        .map(str::to_string)
}

/// The short sha a run's commit-barrier reported (`✓ Committed changes (<sha>)`).
pub(super) fn run_commit_sha(tool_result: &str) -> Option<String> {
    const MARKER: &str = "Committed changes (";
    let start = tool_result.find(MARKER)? + MARKER.len();
    let rest = &tool_result[start..];
    let end = rest.find(')')?;
    let sha = rest[..end].trim();
    if sha.is_empty() {
        None
    } else {
        Some(sha.to_string())
    }
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
    // duplicate here. Nothing in the codebase reads `tool_use_result` (verified
    // by grep across Rust + frontend), so reconstruction does not re-inject it.
    // Codex never duplicates here: its reader keeps `raw` only for non-text MCP
    // content, which a read never produces.
    if let Some(Value::Object(raw)) = map.get_mut("raw") {
        raw.remove("tool_use_result");
    }
    Value::Object(map).to_string()
}

/// The hybrid-read stub: like [`read_stub`] (drop the heavy `toolResult`, pin
/// `toolInput.paths`, strip the duplicate `raw.tool_use_result`) plus the
/// `hybrid_read` marker and the indices of the git-addressed file sections.
/// [`crate::storage::events::encoding::decode`] discriminates on columns (`content_render_sha`
/// presence), not this marker; the marker is for list-row labels and debugging.
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

/// Resolve a (possibly short) sha to its full 40-hex form against the live
/// worktree, peeling tags to the commit. Cached: an execution often re-reports
/// the same commit across events.
/// Map an already-full git sha to its archival coordinate. Under jj, forward to
/// the current in-pack commit-id plus the stable change-id (so the coordinate
/// survives jj's auto-rebase); under plain git (or when jj cannot resolve it),
/// identity with no change-id.
pub(super) fn forward_map(worktree: &Path, jj: Option<&JjEnv>, full: &str) -> Coord {
    match jj.and_then(|jj| crate::jj::forward_resolve_commit(jj, worktree, full)) {
        Some((change_id, current)) => (current, Some(change_id)),
        None => (full.to_string(), None),
    }
}

/// Resolve a recorded (possibly short, possibly jj-rewritten) sha to an archival
/// coordinate. Under jj the recorded id is resolved and forward-mapped through jj
/// **directly** — a production jj workspace is non-colocated (`.jj`, no `.git`),
/// so `git rev-parse` in the worktree is not available, and jj resolves a short or
/// already-hidden commit-id all the same. Plain git (or a jj id jj cannot resolve)
/// falls back to expanding the short sha via `git rev-parse`. `None` only when
/// neither layer resolves it.
pub(super) fn resolve_coord(
    worktree: &Path,
    jj: Option<&JjEnv>,
    short: &str,
    cache: &mut HashMap<String, String>,
) -> Option<Coord> {
    if let Some(jj) = jj {
        if let Some((change_id, current)) = crate::jj::forward_resolve_commit(jj, worktree, short) {
            return Some((current, Some(change_id)));
        }
    }
    let full = resolve_full(worktree, short, cache)?;
    Some((full, None))
}

pub(super) fn resolve_full(
    worktree: &Path,
    sha: &str,
    cache: &mut HashMap<String, String>,
) -> Option<String> {
    if let Some(full) = cache.get(sha) {
        return Some(full.clone());
    }
    let output = Command::new("git")
        .current_dir(worktree)
        .args(["rev-parse", "--verify", &format!("{sha}^{{commit}}")])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let full = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if full.is_empty() {
        return None;
    }
    cache.insert(sha.to_string(), full.clone());
    Some(full)
}

/// Resolve the git repository that holds the execution's objects and the tip the
/// archival range is built against. A production jj workspace is `.jj`-only with
/// no `.git`, so git cannot run there: after `export_git` the objects live in the
/// project repo's object database and the tip is jj's `@-` (its `git HEAD`
/// analogue). Plain git resolves both from the worktree itself.
pub(super) fn pack_source(
    worktree: &Path,
    repo_path: &str,
    jj: Option<&JjEnv>,
) -> Result<(PathBuf, String), String> {
    if let Some(jj) = jj {
        if crate::jj::is_jj_dir(worktree) {
            let tip = crate::jj::head_commit(jj, worktree)
                .map_err(|e| format!("resolving jj tip for archival: {e}"))?;
            return Ok((PathBuf::from(repo_path), tip));
        }
    }
    let tip =
        git_head(worktree).ok_or_else(|| "resolving worktree HEAD for archival".to_string())?;
    Ok((worktree.to_path_buf(), tip))
}

fn git_head(worktree: &Path) -> Option<String> {
    let output = Command::new("git")
        .current_dir(worktree)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}
