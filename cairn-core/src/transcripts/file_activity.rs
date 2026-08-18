//! Which files a tool call named, extracted from its own structured input.
//!
//! File paths touched by an agent live nowhere structured today: they are buried
//! inside a tool call's `input` JSON, reachable only by re-parsing a transcript.
//! This module lifts them out once, at the moment the assistant event carrying
//! the call lands, into the `job_file_activity` stream a live workspace map
//! renders.
//!
//! ## What a row means
//!
//! A row says **this tool call named this path with this intent**. It is read
//! off the caller's declared target, so it does not assert the write landed —
//! the same best-effort standing the `tool_invocations` rollup beside it has.
//! Extraction is pure, total, and allocation-light: an input shape it does not
//! recognize yields no rows rather than a guess, because a wrong path on a map
//! is worse than a quiet one.
//!
//! ## What it deliberately does not cover
//!
//! A `run` batch that commits changes tracked files this module cannot name: the
//! commands are opaque shell, and the resulting file list exists only in the
//! batch's result, not its input. Those files are already durably attributed
//! per job by `file_changes` (the OUTCOME view, recomputed from the branch
//! diff); this stream is the TIMELINE view, and inventing entries for a commit
//! it cannot see would make the two disagree.

use crate::storage::DbResult;
use cairn_db::turso::{params, Connection};
use serde_json::Value;

/// What a tool call declared it was doing to a path.
///
/// `Read` is kept rather than dropped because attention is most of what a live
/// map shows — a job that read twenty files and edited one was working, and a
/// stream that recorded only the edit would render it as idle. The label is what
/// keeps the two legible apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileAction {
    Read,
    Edit,
    Create,
    Delete,
}

impl FileAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Edit => "edit",
            Self::Create => "create",
            Self::Delete => "delete",
        }
    }
}

/// One file a tool call named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileTouch {
    /// Provider tool-use id, so a row traces back to the invocation that made it.
    pub tool_use_id: String,
    pub path: String,
    pub action: FileAction,
}

/// Longest stored path. Matches `tool_extract`'s target bound: a path longer
/// than this is a pathological input, not a file anyone is watching.
const MAX_PATH_LEN: usize = 300;

/// Every file the tool calls in ONE assistant event's `data` JSON named.
///
/// The single extraction entry point: the durable write and the live broadcast
/// both call it on the same event data, so the row that lands and the frame that
/// goes out cannot describe different files. Total by construction — malformed
/// or unrecognized data yields an empty vec.
pub fn touches_from_event_data(data: &str) -> Vec<FileTouch> {
    let Ok(parsed) = serde_json::from_str::<Value>(data) else {
        return Vec::new();
    };
    let Some(uses) = parsed.get("toolUses").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for use_info in uses {
        let Some(tool_use_id) = str_field(use_info, "id") else {
            continue;
        };
        let Some(name) = str_field(use_info, "name") else {
            continue;
        };
        let Some(input) = use_info.get("input") else {
            continue;
        };
        for (path, action) in touches_from_tool_input(&normalize_base(&name), input) {
            let touch = FileTouch {
                tool_use_id: tool_use_id.clone(),
                path,
                action,
            };
            // One touch per (path, action) per call: a batch that reads the same
            // file twice paid attention to it once.
            if !out.contains(&touch) {
                out.push(touch);
            }
        }
    }
    out
}

/// Strip an MCP prefix (`mcp__cairn__write` -> `write`) and lowercase, matching
/// `cairn_analytics::tool_extract`'s normalization so both read the same names.
fn normalize_base(name: &str) -> String {
    name.rsplit("__")
        .next()
        .unwrap_or(name)
        .to_ascii_lowercase()
}

/// The (path, action) pairs one tool call's input declares.
fn touches_from_tool_input(base: &str, input: &Value) -> Vec<(String, FileAction)> {
    let mut out = Vec::new();
    match base {
        // Cairn's `write`: every `file:` target in `changes[]`, with the mode
        // naming the intent. A `unified_patch` carries a bare `file:` target and
        // names its files inside the envelope instead.
        "write" => {
            let Some(changes) = input.get("changes").and_then(Value::as_array) else {
                return out;
            };
            for change in changes {
                let mode = str_field(change, "mode").unwrap_or_default();
                if mode == "unified_patch" {
                    let patch = change
                        .get("payload")
                        .and_then(|payload| payload.get("patch"))
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    out.extend(patch_envelope_touches(patch));
                    continue;
                }
                let Some(target) = str_field(change, "target") else {
                    continue;
                };
                let Some(path) = file_target_path(&target) else {
                    continue;
                };
                out.push((path, write_mode_action(&mode)));
            }
        }
        // Cairn's `read`: every `file:` entry in `paths[]`.
        "read" => {
            if let Some(paths) = input.get("paths").and_then(Value::as_array) {
                for entry in paths {
                    let Some(path) = entry.as_str().and_then(file_target_path) else {
                        continue;
                    };
                    out.push((path, FileAction::Read));
                }
            }
        }
        // Native file tools carry a bare filesystem path. `Write` overwrites as
        // readily as it creates, so only an explicit `create` mode above is
        // allowed to claim creation.
        "edit" | "multiedit" | "notebookedit" => {
            out.extend(native_path(input).map(|path| (path, FileAction::Edit)));
        }
        "grep" | "glob" | "notebookread" => {
            out.extend(native_path(input).map(|path| (path, FileAction::Read)));
        }
        _ => {}
    }
    out.into_iter()
        .filter(|(path, _)| !path.is_empty())
        .map(|(path, action)| (truncate(&path), action))
        .collect()
}

/// The intent a `write` change's mode declares.
///
/// Everything that is neither creation nor removal is an edit: `patch`,
/// `replace`, `append`, `rename`, `apply`, and `revert` all leave a file that
/// existed before and exists after.
fn write_mode_action(mode: &str) -> FileAction {
    match mode {
        "create" => FileAction::Create,
        "delete" => FileAction::Delete,
        _ => FileAction::Edit,
    }
}

/// The files a native `*** Begin Patch` envelope adds, updates, or deletes.
///
/// Worth parsing rather than skipping because `unified_patch` is the multi-file
/// write mode: its target is the bare worktree root, so without this a
/// four-file patch would register as no file activity at all.
fn patch_envelope_touches(patch: &str) -> Vec<(String, FileAction)> {
    let mut out = Vec::new();
    for line in patch.lines() {
        let Some(rest) = line.strip_prefix("*** ") else {
            continue;
        };
        let touch = rest
            .strip_prefix("Add File: ")
            .map(|path| (path, FileAction::Create))
            .or_else(|| {
                rest.strip_prefix("Update File: ")
                    .map(|path| (path, FileAction::Edit))
            })
            .or_else(|| {
                rest.strip_prefix("Delete File: ")
                    .map(|path| (path, FileAction::Delete))
            });
        if let Some((path, action)) = touch {
            let path = path.trim();
            if !path.is_empty() {
                out.push((path.to_string(), action));
            }
        }
    }
    out
}

/// The worktree-relative path a `file:` URI names, or `None` for any other
/// scheme. Query scoping (`?grep=`, `?offset=`) is not part of the path.
///
/// A bare `file:` is the worktree root, not a file, and yields `None`: a map
/// pin on "the repository" is noise.
fn file_target_path(target: &str) -> Option<String> {
    let rest = target.trim().strip_prefix("file:")?;
    let path = rest.split('?').next().unwrap_or(rest).trim();
    (!path.is_empty()).then(|| path.to_string())
}

/// The bare filesystem path a native tool carries, under whichever key it uses.
fn native_path(input: &Value) -> Option<String> {
    ["file_path", "notebook_path", "path"]
        .into_iter()
        .find_map(|key| str_field(input, key))
}

fn str_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn truncate(s: &str) -> String {
    if s.len() <= MAX_PATH_LEN {
        return s.to_string();
    }
    let mut end = MAX_PATH_LEN;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

/// Record one assistant event's file touches, inside that event's own insert
/// transaction.
///
/// Rides the event write for the same reason the analytics rollups do: it is the
/// one seam every durable-event insert funnels through (live, finalize, and
/// resume alike), so no backend can land a tool call the stream misses. The
/// owning job is resolved through `runs` in the same statement rather than by a
/// separate read, so a row can never be attributed to a job the run did not have
/// at insert time. A run with no job yet writes nothing.
///
/// `INSERT OR REPLACE` keyed on `{event_id}:{tool_use_id}:{ordinal}` makes a
/// re-insert of the same event idempotent.
pub(crate) async fn record_touches_conn(
    conn: &Connection,
    event_id: &str,
    run_id: &str,
    created_at: i64,
    touches: &[FileTouch],
) -> DbResult<()> {
    for (ordinal, touch) in touches.iter().enumerate() {
        conn.execute(
            "INSERT OR REPLACE INTO job_file_activity
                 (id, job_id, file_path, action, created_at)
             SELECT ?1, COALESCE(r.job_id, j.id), ?2, ?3, ?4
             FROM runs r LEFT JOIN jobs j ON j.id = r.job_id
             WHERE r.id = ?5 AND COALESCE(r.job_id, j.id) IS NOT NULL
             LIMIT 1",
            params![
                format!("{event_id}:{}:{ordinal}", touch.tool_use_id),
                touch.path.as_str(),
                touch.action.as_str(),
                created_at,
                run_id
            ],
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assistant_data(name: &str, input: Value) -> String {
        json!({
            "eventType": "assistant",
            "isError": false,
            "toolUses": [{ "id": "toolu_1", "name": name, "input": input }],
        })
        .to_string()
    }

    fn paths(touches: &[FileTouch]) -> Vec<(&str, &str)> {
        touches
            .iter()
            .map(|touch| (touch.path.as_str(), touch.action.as_str()))
            .collect()
    }

    #[test]
    fn write_changes_carry_their_mode_as_the_action() {
        let data = assistant_data(
            "mcp__cairn__write",
            json!({
                "changes": [
                    { "target": "file:src/lib.rs", "mode": "patch" },
                    { "target": "file:src/new.rs", "mode": "create" },
                    { "target": "file:src/gone.rs", "mode": "delete" },
                    { "target": "file:src/renamed.rs", "mode": "replace" },
                ]
            }),
        );

        assert_eq!(
            paths(&touches_from_event_data(&data)),
            vec![
                ("src/lib.rs", "edit"),
                ("src/new.rs", "create"),
                ("src/gone.rs", "delete"),
                ("src/renamed.rs", "edit"),
            ]
        );
    }

    #[test]
    fn write_ignores_resource_targets_and_the_bare_worktree_root() {
        // A cairn:// mutation is not file activity, and `file:` alone is the
        // repository rather than a file anyone is watching.
        let data = assistant_data(
            "write",
            json!({
                "changes": [
                    { "target": "cairn://p/CAIRN/4226", "mode": "append" },
                    { "target": "cairn:~/todos", "mode": "replace" },
                    { "target": "file:", "mode": "patch" },
                ]
            }),
        );

        assert!(touches_from_event_data(&data).is_empty());
    }

    #[test]
    fn unified_patch_names_every_file_inside_its_envelope() {
        // The whole reason to parse the envelope: the change's own target is the
        // bare worktree root, so a multi-file patch would otherwise vanish.
        let patch = "*** Begin Patch\n\
            *** Update File: src/lib.rs\n\
            @@ -1,1 +1,1 @@\n\
            -old()\n\
            +new()\n\
            *** Add File: src/new.rs\n\
            +pub fn new() {}\n\
            *** Delete File: src/old.rs\n\
            *** End Patch\n";
        let data = assistant_data(
            "write",
            json!({
                "changes": [{
                    "target": "file:",
                    "mode": "unified_patch",
                    "payload": { "patch": patch },
                }]
            }),
        );

        assert_eq!(
            paths(&touches_from_event_data(&data)),
            vec![
                ("src/lib.rs", "edit"),
                ("src/new.rs", "create"),
                ("src/old.rs", "delete"),
            ]
        );
    }

    #[test]
    fn read_paths_are_labeled_reads_with_query_scoping_stripped() {
        let data = assistant_data(
            "mcp__cairn__read",
            json!({
                "paths": [
                    "file:src/lib.rs?offset=10&limit=20",
                    "file:src/other.ts",
                    "cairn://p/CAIRN/4226",
                    "https://example.com/spec",
                ]
            }),
        );

        assert_eq!(
            paths(&touches_from_event_data(&data)),
            vec![("src/lib.rs", "read"), ("src/other.ts", "read")]
        );
    }

    #[test]
    fn native_file_tools_carry_a_bare_path() {
        let edit = assistant_data(
            "Edit",
            json!({ "file_path": "/abs/path/main.py", "old_string": "a", "new_string": "b" }),
        );
        assert_eq!(
            paths(&touches_from_event_data(&edit)),
            vec![("/abs/path/main.py", "edit")]
        );

        let grep = assistant_data("Grep", json!({ "pattern": "foo", "path": "src" }));
        assert_eq!(
            paths(&touches_from_event_data(&grep)),
            vec![("src", "read")]
        );
    }

    #[test]
    fn run_batches_record_nothing() {
        // A shell batch's changed files live in its RESULT, not its input.
        // `file_changes` already attributes them; guessing here would make the
        // two views disagree.
        let data = assistant_data(
            "run",
            json!({
                "commands": [{ "command": "cargo fmt" }],
                "commit_msg": "fmt",
            }),
        );

        assert!(touches_from_event_data(&data).is_empty());
    }

    #[test]
    fn repeated_targets_in_one_call_collapse_to_one_touch() {
        let data = assistant_data(
            "read",
            json!({ "paths": ["file:src/lib.rs", "file:src/lib.rs?grep=fn"] }),
        );

        assert_eq!(
            paths(&touches_from_event_data(&data)),
            vec![("src/lib.rs", "read")]
        );
    }

    #[test]
    fn a_read_and_a_write_of_one_path_stay_distinct() {
        let data = json!({
            "eventType": "assistant",
            "isError": false,
            "toolUses": [
                { "id": "a", "name": "read", "input": { "paths": ["file:src/lib.rs"] } },
                { "id": "b", "name": "write", "input": {
                    "changes": [{ "target": "file:src/lib.rs", "mode": "patch" }] } },
            ],
        })
        .to_string();

        let touches = touches_from_event_data(&data);
        assert_eq!(
            paths(&touches),
            vec![("src/lib.rs", "read"), ("src/lib.rs", "edit")]
        );
        assert_eq!(touches[0].tool_use_id, "a");
        assert_eq!(touches[1].tool_use_id, "b");
    }

    #[test]
    fn malformed_and_toolless_events_yield_nothing() {
        assert!(touches_from_event_data("not json").is_empty());
        assert!(touches_from_event_data("{}").is_empty());
        assert!(touches_from_event_data(
            &json!({ "eventType": "assistant", "content": "just text" }).to_string()
        )
        .is_empty());
        // A recognized tool with an input shape it never carries is a shape this
        // module does not know, not a path to guess at.
        assert!(
            touches_from_event_data(&assistant_data("write", json!({ "changes": "oops" })))
                .is_empty()
        );
    }

    #[test]
    fn a_pathological_path_is_truncated_on_a_char_boundary() {
        let long = "é".repeat(400);
        let data = assistant_data("read", json!({ "paths": [format!("file:{long}")] }));
        let touches = touches_from_event_data(&data);
        assert_eq!(touches.len(), 1);
        assert!(touches[0].path.len() <= MAX_PATH_LEN);
        assert!(touches[0].path.chars().all(|c| c == 'é'));
    }
}
