use super::*;

/// A write batch that committed, resolved against the repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommittedWrite {
    /// The full commit id the batch landed.
    pub(super) commit: String,
    /// Indices of the batch's changes the report lists as applied.
    pub(super) applied: HashSet<usize>,
}

/// The modes whose committed effect is exactly the target file's diff, which
/// reconstruction re-injects in place of the payload. Every other mode's
/// payload (a rename's names, a `unified_patch` envelope) carries something the
/// diff does not, so it stays.
const FILE_CONTENT_MODES: [&str; 4] = ["create", "replace", "append", "patch"];

/// Pair every committed write to its assistant event. A write's commit sha is
/// reported on its `tool_result`, but the heavy payload it coordinates lives on
/// the assistant event that issued the call; this maps the shared `tool_use_id`
/// to the resolved commit so the assistant pass can find it. A batch that did
/// not commit (preview, resource-only, failure) or whose commit the repository
/// no longer knows is absent from the map; the latter are counted in the second
/// return value.
pub(super) fn build_write_commits(
    commits: &mut CommitResolver<'_>,
    events: &[Event],
    tool_map: &HashMap<String, (String, Value)>,
) -> (HashMap<String, CommittedWrite>, usize) {
    let mut resolved = HashMap::new();
    let mut unresolved = 0;
    for event in events {
        if !matches!(event.event_type.as_str(), "tool_result" | "result") {
            continue;
        }
        let Some(id) = event_tool_use_id(&event.data) else {
            continue;
        };
        let is_write = tool_map
            .get(&id)
            .is_some_and(|(name, _)| normalize_tool_name(name) == "write");
        if !is_write {
            continue;
        }
        let Some(report) =
            tool_result_text(&event.data).and_then(|text| parse_change_report(&text))
        else {
            continue;
        };
        match commits.resolve_full(&report.sha) {
            Some(commit) => {
                resolved.insert(
                    id,
                    CommittedWrite {
                        commit,
                        applied: report.applied,
                    },
                );
            }
            None => unresolved += 1,
        }
    }
    (resolved, unresolved)
}

/// Map every run `tool_result` whose commit barrier landed a commit the
/// repository still knows to that full commit id, keyed by event id.
pub(super) fn build_run_commits(
    commits: &mut CommitResolver<'_>,
    events: &[Event],
    tool_map: &HashMap<String, (String, Value)>,
) -> HashMap<String, String> {
    let mut resolved = HashMap::new();
    for event in events {
        if !matches!(event.event_type.as_str(), "tool_result" | "result") {
            continue;
        }
        let is_run = event_tool_use_id(&event.data)
            .and_then(|id| tool_map.get(&id))
            .is_some_and(|(name, _)| normalize_tool_name(name) == "run");
        if !is_run {
            continue;
        }
        if let Some(full) = tool_result_text(&event.data)
            .and_then(|text| run_commit_sha(&text))
            .and_then(|sha| commits.resolve_full(&sha))
        {
            resolved.insert(event.id.clone(), full);
        }
    }
    resolved
}

/// Gitcoord-strip the assistant event of a write batch whose commit the
/// execution pack keeps: drop the payload of every change reconstruction will
/// re-inject from the commit's diff, keep the rest of the event (model text,
/// change skeletons, and every other payload) zstd-compressed in `data_blob`,
/// and pin the batch commit in `content_commit`.
///
/// A payload is dropped only when its change applied, its mode is a
/// file-content mode, and its `file:` path has a section in the commit's diff:
/// exactly the changes [`reconstruct`] fills back in. Resource targets in a
/// combo write, `unified_patch` envelopes, renames, and failed items keep their
/// payloads, so nothing the agent wrote is lost.
///
/// Every other non-result event falls to plain zstd, which preserves it
/// verbatim: one with no committed write, one bundling writes to several
/// distinct commits (a single coordinate cannot address them), one whose commit
/// the repository no longer knows or whose diff cannot be rendered, and one
/// with no payload to drop.
pub(super) fn try_assistant_write(
    event: &Event,
    tool_name: Option<&str>,
    write_commits: &HashMap<String, CommittedWrite>,
    store: Option<&ObjectStore>,
    diff_paths: &mut HashMap<String, Option<HashSet<String>>>,
    updates: &mut Vec<EventUpdate>,
    summary: &mut ArchiveSummary,
) -> Result<(), String> {
    let (Some(commit), Some(store)) = (committed_write_for_event(&event.data, write_commits), store)
    else {
        return push_zstd(event, tool_name, updates, summary);
    };
    let Some(paths) = diff_paths
        .entry(commit.clone())
        .or_insert_with(|| reconstruct::commit_diff_paths(commit, store))
        .as_ref()
    else {
        return push_zstd(event, tool_name, updates, summary);
    };
    let (remainder, stripped) = strip_change_payloads(&event.data, |tool_id, index, change| {
        write_commits
            .get(tool_id)
            .is_some_and(|write| write.applied.contains(&index))
            && reinjected_from_diff(change, paths)
    });
    if stripped == 0 {
        return push_zstd(event, tool_name, updates, summary);
    }
    let blob = compress(remainder.as_bytes())?;
    let data = gitcoord_write_stub(event);
    summary.gitcoord_write += 1;
    summary.bytes_after += data.len() + blob.len();
    updates.push(EventUpdate {
        id: event.id.clone(),
        shape: ArchivedShape::GitcoordWrite {
            commit: commit.clone(),
            data,
            blob,
        },
    });
    Ok(())
}

/// Whether reconstruction regenerates this change's payload from the commit's
/// diff: a file-content mode on a non-empty `file:` path the diff touches.
fn reinjected_from_diff(change: &serde_json::Map<String, Value>, paths: &HashSet<String>) -> bool {
    let file_content = change
        .get("mode")
        .and_then(Value::as_str)
        .is_some_and(|mode| FILE_CONTENT_MODES.contains(&mode));
    let path = change
        .get("target")
        .and_then(Value::as_str)
        .and_then(reconstruct::repo_relative_path);
    file_content && path.is_some_and(|path| !path.is_empty() && paths.contains(&path))
}

/// The single commit an assistant event's write batch landed, or `None` when no
/// tool use committed or several did to distinct commits.
fn committed_write_for_event<'a>(
    data: &str,
    write_commits: &'a HashMap<String, CommittedWrite>,
) -> Option<&'a String> {
    let value: Value = serde_json::from_str(data).ok()?;
    let tool_uses = value.get("toolUses")?.as_array()?;
    let mut found: Option<&String> = None;
    for tool in tool_uses {
        let Some(commit) = tool
            .get("id")
            .and_then(|v| v.as_str())
            .and_then(|id| write_commits.get(id))
            .map(|write| &write.commit)
        else {
            continue;
        };
        match found {
            None => found = Some(commit),
            Some(existing) if existing == commit => {}
            Some(_) => return None,
        }
    }
    found
}

/// Drop the `changes[*].payload` of every change `strippable(tool_id, index,
/// change)` selects, keeping target/mode and the rest of the call shell, and
/// return the remainder with the number of payloads dropped.
///
/// Strips both copies the backends record: the authoritative
/// `toolUses[].input.changes[*].payload`, and the single-tool-use duplicate
/// `toolInput.changes[*].payload` (`agent_process::stream` mirrors a lone tool
/// call's input into a top-level `toolInput`). The renderer and reconstruction
/// key off `toolUses`, so the `toolInput` copy is vestigial — but it is just as
/// heavy, so a payload dropped from one is dropped from both.
pub(super) fn strip_change_payloads(
    data: &str,
    mut strippable: impl FnMut(&str, usize, &serde_json::Map<String, Value>) -> bool,
) -> (String, usize) {
    let mut value = serde_json::from_str::<Value>(data).unwrap_or(Value::Null);
    let mut stripped = 0;
    let mut lone_tool_id: Option<String> = None;
    if let Some(tool_uses) = value.get_mut("toolUses").and_then(|v| v.as_array_mut()) {
        if let [only] = tool_uses.as_slice() {
            lone_tool_id = only.get("id").and_then(Value::as_str).map(str::to_string);
        }
        for tool in tool_uses {
            let id = tool
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            stripped += strip_changes_payloads(tool.get_mut("input"), &id, &mut strippable);
        }
    }
    if let Some(id) = lone_tool_id {
        strip_changes_payloads(value.get_mut("toolInput"), &id, &mut strippable);
    }
    (value.to_string(), stripped)
}

/// Remove `payload` from each selected element of an input value's `changes`
/// array, returning how many were removed.
fn strip_changes_payloads<F>(input: Option<&mut Value>, tool_id: &str, strippable: &mut F) -> usize
where
    F: FnMut(&str, usize, &serde_json::Map<String, Value>) -> bool,
{
    let Some(changes) = input
        .and_then(|v| v.as_object_mut())
        .and_then(|input| input.get_mut("changes"))
        .and_then(|v| v.as_array_mut())
    else {
        return 0;
    };
    let mut stripped = 0;
    for (index, change) in changes.iter_mut().enumerate() {
        if let Some(obj) = change.as_object_mut() {
            if obj.contains_key("payload") && strippable(tool_id, index, obj) {
                obj.remove("payload");
                stripped += 1;
            }
        }
    }
    stripped
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
    });
    Ok(())
}
