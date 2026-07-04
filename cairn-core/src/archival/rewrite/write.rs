use super::*;

/// Pair every committed write to its assistant event. A write's commit sha is
/// reported on its `tool_result`, but the heavy payload it coordinates lives on
/// the assistant event that issued the call; this maps the shared `tool_use_id`
/// to the resolved full commit sha so the assistant pass can find it. A batch
/// that did not commit (preview, resource-only, failure) is absent from the map.
pub(super) fn build_write_commits(
    worktree: &Path,
    jj: Option<&JjEnv>,
    events: &[Event],
    tool_map: &HashMap<String, (String, Value)>,
    sha_cache: &mut HashMap<String, String>,
) -> HashMap<String, Coord> {
    let mut commits = HashMap::new();
    for event in events {
        if !matches!(event.event_type.as_str(), "tool_result" | "result") {
            continue;
        }
        let Some(id) = event_tool_use_id(&event.data) else {
            continue;
        };
        let is_write = tool_map
            .get(&id)
            .map(|(name, _)| normalize_tool_name(name) == "write")
            .unwrap_or(false);
        if !is_write {
            continue;
        }
        if let Some(coord) = tool_result_text(&event.data)
            .and_then(|tr| change_commit_sha(&tr))
            .and_then(|short| resolve_coord(worktree, jj, &short, sha_cache))
        {
            commits.insert(id, coord);
        }
    }
    commits
}

/// Gitcoord-strip the assistant event of a write batch that committed: drop the
/// heavy `toolUses[].input.changes[*].payload` bytes, keep the rest of the event
/// (model text + change skeletons) zstd-compressed in `data_blob`, and pin the
/// batch commit in `content_commit`. A non-assistant event, an assistant event
/// with no committed write, or one bundling writes to several distinct commits
/// (ambiguous coordinate) falls to plain zstd, which preserves it verbatim.
pub(super) fn try_assistant_write(
    event: &Event,
    tool_name: Option<&str>,
    write_commits: &HashMap<String, Coord>,
    updates: &mut Vec<EventUpdate>,
    summary: &mut ArchiveSummary,
) -> Result<(), String> {
    match committed_write_for_event(&event.data, write_commits) {
        Some((commit, change_id)) => {
            let remainder = strip_change_payloads(&event.data);
            let blob = compress(remainder.as_bytes())?;
            let data = gitcoord_write_stub(event);
            summary.gitcoord_write += 1;
            summary.bytes_after += data.len() + blob.len();
            updates.push(EventUpdate {
                id: event.id.clone(),
                shape: ArchivedShape::GitcoordWrite { commit, data, blob },
                change_id,
            });
        }
        None => push_zstd(event, tool_name, updates, summary)?,
    }
    Ok(())
}

/// The single commit an assistant event's write batch landed, or `None` when no
/// tool use committed or several did to distinct commits (a single
/// `content_commit` cannot address them, so the row is kept verbatim as zstd).
fn committed_write_for_event(data: &str, write_commits: &HashMap<String, Coord>) -> Option<Coord> {
    let value: Value = serde_json::from_str(data).ok()?;
    let tool_uses = value.get("toolUses")?.as_array()?;
    let mut found: Option<Coord> = None;
    for tool in tool_uses {
        let Some(id) = tool.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(coord) = write_commits.get(id) else {
            continue;
        };
        match &found {
            None => found = Some(coord.clone()),
            // Ambiguity is judged on the commit alone: a single content_commit
            // cannot address two distinct commits.
            Some(existing) if existing.0 == coord.0 => {}
            Some(_) => return None,
        }
    }
    found
}

/// Drop every `changes[*].payload` from an assistant event's tool call, keeping
/// target/mode and the rest of the call shell. The committed content is
/// regenerated from the coordinate on reconstruction.
///
/// Strips both copies the backends record: the authoritative
/// `toolUses[].input.changes[*].payload`, and the single-tool-use backwards-compat
/// duplicate `toolInput.changes[*].payload` (`agent_process::stream` mirrors a
/// lone tool call's input into a top-level `toolInput`). The renderer and
/// reconstruction key off `toolUses`, so the `toolInput` copy is vestigial — but
/// it is just as heavy, so an archived `data_blob` must retain neither.
pub(super) fn strip_change_payloads(data: &str) -> String {
    let mut value = serde_json::from_str::<Value>(data).unwrap_or(Value::Null);
    if let Some(tool_uses) = value.get_mut("toolUses").and_then(|v| v.as_array_mut()) {
        for tool in tool_uses {
            strip_changes_payloads(tool.get_mut("input"));
        }
    }
    strip_changes_payloads(value.get_mut("toolInput"));
    value.to_string()
}

/// Remove `payload` from every element of an input value's `changes` array.
/// Both `toolUses[].input` and the duplicate top-level `toolInput` carry the same
/// `{ changes, commit_msg }` shape, so one helper services both.
fn strip_changes_payloads(input: Option<&mut Value>) {
    if let Some(changes) = input
        .and_then(|v| v.as_object_mut())
        .and_then(|input| input.get_mut("changes"))
        .and_then(|v| v.as_array_mut())
    {
        for change in changes {
            if let Some(obj) = change.as_object_mut() {
                obj.remove("payload");
            }
        }
    }
}

/// The gitcoord-write stub kept in `data`: just enough for a list-row label
/// before reconstruction restores the remainder from `data_blob`.
fn gitcoord_write_stub(event: &Event) -> String {
    json!({ "eventType": event.event_type, "archived": "gitcoord_write" }).to_string()
}

pub(super) fn push_zstd(
    event: &Event,
    tool_name: Option<&str>,
    updates: &mut Vec<EventUpdate>,
    summary: &mut ArchiveSummary,
) -> Result<(), String> {
    let blob = compress(event.data.as_bytes())?;
    let data = zstd_stub(event, tool_name);
    summary.zstd += 1;
    summary.bytes_after += data.len() + blob.len();
    updates.push(EventUpdate {
        id: event.id.clone(),
        shape: ArchivedShape::Zstd { data, blob },
        change_id: None,
    });
    Ok(())
}
