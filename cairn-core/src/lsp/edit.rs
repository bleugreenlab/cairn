//! Apply an LSP `WorkspaceEdit` to worktree files.
//!
//! This is the one genuinely new, correctness-critical piece the rename op adds
//! on top of the read-only engine: turning a server-computed `WorkspaceEdit`
//! into concrete post-edit file contents (and file-move operations) on disk.
//!
//! Two shapes are handled:
//! - `changes`: a `{ uri -> TextEdit[] }` map.
//! - `documentChanges`: an ordered array of `TextDocumentEdit` and resource
//!   operations (`CreateFile`/`RenameFile`/`DeleteFile`). Processed in array
//!   order so a `RenameFile` followed by edits keyed on the new URI still apply
//!   to the original file's content.
//!
//! ## UTF-16 offsets
//!
//! An LSP `Position.character` counts UTF-16 code units, not bytes or Unicode
//! scalar values. A line with any non-ASCII text before the identifier would
//! corrupt a byte-indexed splice, so the applier maps each `(line, character)`
//! to a byte offset by walking the line and accumulating `char::len_utf16`. The
//! position encoding stays UTF-16 (the LSP default) end to end.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde_json::Value;

use super::client::uri_to_path;

/// One file's resulting state after applying a rename's edits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEdit {
    /// Worktree-absolute path of the file the edit reads from (the source path).
    /// For an in-place edit this is also the write target; for a move it is the
    /// path to delete.
    pub worktree_path: PathBuf,
    /// The full post-edit content to write. `None` means delete `worktree_path`.
    pub new_content: Option<String>,
    /// Destination path when the symbol's file is renamed/moved. When set, the
    /// new content is written here and `worktree_path` is removed.
    pub move_to: Option<PathBuf>,
    /// Number of individual text-edit sites applied to this file (report detail).
    pub site_count: usize,
}

/// Why turning a `WorkspaceEdit` into file edits failed. Distinct from a
/// transport-level [`super::LspError`]: the server answered, but the answer
/// could not be applied (out of bounds, unreadable, or malformed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameEditError {
    /// An edit targets a path that does not map under the worktree subroot — a
    /// workspace-symbol rename should never touch files outside it.
    OutsideWorktree(String),
    /// A file an edit references could not be read from the worktree.
    Read { path: String, message: String },
    /// The `WorkspaceEdit` JSON did not have the expected shape.
    Malformed(String),
}

impl std::fmt::Display for RenameEditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RenameEditError::OutsideWorktree(path) => write!(
                f,
                "rename would edit '{path}', which is outside the worktree; refusing to apply"
            ),
            RenameEditError::Read { path, message } => {
                write!(f, "failed to read '{path}' for rename: {message}")
            }
            RenameEditError::Malformed(message) => {
                write!(f, "language server returned a malformed rename edit: {message}")
            }
        }
    }
}

impl std::error::Error for RenameEditError {}

/// A single text edit: a half-open `[start, end)` range (UTF-16 positions) and
/// its replacement text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RawTextEdit {
    start: (u32, u32),
    end: (u32, u32),
    new_text: String,
}

/// Per-file accumulation while parsing the edit. Keyed on the file's *source*
/// path (the path before any move in this same edit).
#[derive(Default)]
struct Pending {
    edits: Vec<RawTextEdit>,
    move_to: Option<PathBuf>,
    deleted: bool,
    created: bool,
}

/// Translate a server-side absolute path back to a worktree-absolute path,
/// returning `None` when the path falls outside the rename's allowed root. The
/// orchestrator supplies the reroute-aware mapping; here `None` is the signal to
/// reject the whole rename.
pub type Translate<'a> = dyn Fn(&Path) -> Option<PathBuf> + 'a;

/// Turn a `textDocument/rename` `WorkspaceEdit` into the concrete per-file
/// edits to apply, reading current content from the (translated) worktree paths.
/// Every referenced URI is mapped through `translate`; an unmappable URI aborts
/// the whole rename rather than silently writing outside the worktree.
pub fn plan_workspace_edit(
    edit: &Value,
    translate: &Translate<'_>,
) -> Result<Vec<FileEdit>, RenameEditError> {
    let mut order: Vec<PathBuf> = Vec::new();
    let mut pending: HashMap<PathBuf, Pending> = HashMap::new();
    // Maps a post-rename path to the entry key (its pre-rename source path) so
    // later edits keyed on the new URI land on the original file's content.
    let mut redirect: HashMap<PathBuf, PathBuf> = HashMap::new();

    if let Some(doc_changes) = edit.get("documentChanges").and_then(Value::as_array) {
        for change in doc_changes {
            match change.get("kind").and_then(Value::as_str) {
                Some("rename") => {
                    let old = uri_field(change, "oldUri")?;
                    let new = uri_field(change, "newUri")?;
                    let key = redirect.get(&old).cloned().unwrap_or(old);
                    ensure(&mut order, &mut pending, &key).move_to = Some(new.clone());
                    redirect.insert(new, key);
                }
                Some("create") => {
                    let uri = uri_field(change, "uri")?;
                    ensure(&mut order, &mut pending, &uri).created = true;
                }
                Some("delete") => {
                    let uri = uri_field(change, "uri")?;
                    let key = redirect.get(&uri).cloned().unwrap_or(uri);
                    ensure(&mut order, &mut pending, &key).deleted = true;
                }
                Some(other) => {
                    return Err(RenameEditError::Malformed(format!(
                        "unknown resource operation '{other}'"
                    )))
                }
                None => {
                    // TextDocumentEdit: { textDocument: { uri, version }, edits }.
                    let uri = change
                        .get("textDocument")
                        .and_then(|td| td.get("uri"))
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            RenameEditError::Malformed(
                                "textDocument edit missing textDocument.uri".to_string(),
                            )
                        })?;
                    let path = uri_to_path(uri).ok_or_else(|| {
                        RenameEditError::Malformed(format!("non-file edit uri '{uri}'"))
                    })?;
                    let key = redirect.get(&path).cloned().unwrap_or(path);
                    let entry = ensure(&mut order, &mut pending, &key);
                    for raw in change.get("edits").and_then(Value::as_array).into_iter().flatten() {
                        entry.edits.push(parse_text_edit(raw)?);
                    }
                }
            }
        }
    } else if let Some(changes) = edit.get("changes").and_then(Value::as_object) {
        for (uri, edits) in changes {
            let path = uri_to_path(uri)
                .ok_or_else(|| RenameEditError::Malformed(format!("non-file edit uri '{uri}'")))?;
            let entry = ensure(&mut order, &mut pending, &path);
            for raw in edits.as_array().into_iter().flatten() {
                entry.edits.push(parse_text_edit(raw)?);
            }
        }
    } else {
        // A null/empty edit set: the server found nothing to rename.
        return Ok(Vec::new());
    }

    let mut out = Vec::with_capacity(order.len());
    for key in order {
        let entry = pending.remove(&key).expect("insertion-tracked key present");
        let worktree_path = translate(&key)
            .ok_or_else(|| RenameEditError::OutsideWorktree(key.to_string_lossy().into_owned()))?;

        if entry.deleted {
            out.push(FileEdit {
                worktree_path,
                new_content: None,
                move_to: None,
                site_count: 0,
            });
            continue;
        }

        let content = if entry.created && !worktree_path.exists() {
            String::new()
        } else {
            std::fs::read_to_string(&worktree_path).map_err(|e| RenameEditError::Read {
                path: worktree_path.to_string_lossy().into_owned(),
                message: e.to_string(),
            })?
        };
        let new_content = apply_text_edits(&content, &entry.edits)?;
        let move_to = match entry.move_to {
            Some(dest) => Some(translate(&dest).ok_or_else(|| {
                RenameEditError::OutsideWorktree(dest.to_string_lossy().into_owned())
            })?),
            None => None,
        };
        out.push(FileEdit {
            worktree_path,
            new_content: Some(new_content),
            move_to,
            site_count: entry.edits.len(),
        });
    }
    Ok(out)
}

fn ensure<'a>(
    order: &mut Vec<PathBuf>,
    pending: &'a mut HashMap<PathBuf, Pending>,
    key: &Path,
) -> &'a mut Pending {
    if !pending.contains_key(key) {
        order.push(key.to_path_buf());
        pending.insert(key.to_path_buf(), Pending::default());
    }
    pending.get_mut(key).expect("just inserted")
}

fn uri_field(change: &Value, field: &str) -> Result<PathBuf, RenameEditError> {
    let uri = change
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| RenameEditError::Malformed(format!("resource op missing {field}")))?;
    uri_to_path(uri).ok_or_else(|| RenameEditError::Malformed(format!("non-file uri '{uri}'")))
}

fn parse_text_edit(raw: &Value) -> Result<RawTextEdit, RenameEditError> {
    let range = raw
        .get("range")
        .ok_or_else(|| RenameEditError::Malformed("text edit missing range".to_string()))?;
    let start = position(range.get("start"))?;
    let end = position(range.get("end"))?;
    let new_text = raw
        .get("newText")
        .and_then(Value::as_str)
        .ok_or_else(|| RenameEditError::Malformed("text edit missing newText".to_string()))?
        .to_string();
    Ok(RawTextEdit {
        start,
        end,
        new_text,
    })
}

fn position(value: Option<&Value>) -> Result<(u32, u32), RenameEditError> {
    let value =
        value.ok_or_else(|| RenameEditError::Malformed("range missing endpoint".to_string()))?;
    let line = value
        .get("line")
        .and_then(Value::as_u64)
        .ok_or_else(|| RenameEditError::Malformed("position missing line".to_string()))?;
    let character = value
        .get("character")
        .and_then(Value::as_u64)
        .ok_or_else(|| RenameEditError::Malformed("position missing character".to_string()))?;
    Ok((line as u32, character as u32))
}

/// Apply `edits` to `content`, splicing by byte offset. Edits are applied in
/// descending start order so an earlier edit never shifts a later one's offsets.
fn apply_text_edits(content: &str, edits: &[RawTextEdit]) -> Result<String, RenameEditError> {
    if edits.is_empty() {
        return Ok(content.to_string());
    }
    let line_starts = line_start_offsets(content);
    let mut spans: Vec<(usize, usize, &str)> = Vec::with_capacity(edits.len());
    for edit in edits {
        let start = byte_offset(content, &line_starts, edit.start)?;
        let end = byte_offset(content, &line_starts, edit.end)?;
        if end < start {
            return Err(RenameEditError::Malformed(
                "text edit range end precedes start".to_string(),
            ));
        }
        spans.push((start, end, edit.new_text.as_str()));
    }
    // Descending by start: applying high offsets first keeps lower offsets valid.
    spans.sort_by(|a, b| b.0.cmp(&a.0));
    let mut buf = content.to_string();
    let mut prev_start = buf.len() + 1;
    for (start, end, new_text) in spans {
        if end > prev_start {
            return Err(RenameEditError::Malformed(
                "overlapping text edits in rename".to_string(),
            ));
        }
        buf.replace_range(start..end, new_text);
        prev_start = start;
    }
    Ok(buf)
}

/// Byte offset of the start of each line (line 0 at offset 0, line i+1 after the
/// i-th `\n`). A trailing position one line past the last `\n` is valid.
fn line_start_offsets(content: &str) -> Vec<usize> {
    let mut starts = vec![0usize];
    for (idx, byte) in content.bytes().enumerate() {
        if byte == b'\n' {
            starts.push(idx + 1);
        }
    }
    starts
}

/// Map a UTF-16 `(line, character)` position to a byte offset into `content`.
fn byte_offset(
    content: &str,
    line_starts: &[usize],
    pos: (u32, u32),
) -> Result<usize, RenameEditError> {
    let line = pos.0 as usize;
    let line_start = *line_starts.get(line).ok_or_else(|| {
        RenameEditError::Malformed(format!("position line {} out of range", pos.0))
    })?;
    let line_end = line_starts.get(line + 1).copied().unwrap_or(content.len());
    let line_str = &content[line_start..line_end];
    Ok(line_start + utf16_col_to_byte(line_str, pos.1))
}

/// Byte index within `line` of the position `col` UTF-16 code units in. Counts
/// `char::len_utf16` per scalar; clamps to the line end when `col` runs past it.
fn utf16_col_to_byte(line: &str, col: u32) -> usize {
    let mut remaining = col;
    for (byte_idx, ch) in line.char_indices() {
        if remaining == 0 {
            return byte_idx;
        }
        let units = ch.len_utf16() as u32;
        if units > remaining {
            // `col` falls inside a surrogate pair; clamp to the char start.
            return byte_idx;
        }
        remaining -= units;
    }
    line.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn identity(p: &Path) -> Option<PathBuf> {
        Some(p.to_path_buf())
    }

    fn uri(path: &Path) -> String {
        super::super::client::path_to_uri(path)
    }

    #[test]
    fn utf16_offset_handles_non_ascii_before_identifier() {
        // `"\u{e9}\u{e9}"` is two UTF-16 units but four bytes; an identifier after
        // it must splice at the right byte offset.
        let line = "let s = \"\u{e9}\u{e9}\"; old_name";
        // Column of `old_name`: count UTF-16 units up to it.
        let col = line.find("old_name").map(|byte_idx| {
            line[..byte_idx].chars().map(|c| c.len_utf16()).sum::<usize>() as u32
        }).unwrap();
        assert_eq!(utf16_col_to_byte(line, col), line.find("old_name").unwrap());
    }

    #[test]
    fn changes_form_applies_single_edit() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.rs");
        std::fs::write(&file, "let old_name = 1;\n").unwrap();
        let edit = json!({
            "changes": {
                uri(&file): [{
                    "range": {"start": {"line": 0, "character": 4}, "end": {"line": 0, "character": 12}},
                    "newText": "new_name"
                }]
            }
        });
        let plan = plan_workspace_edit(&edit, &identity).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].new_content.as_deref(), Some("let new_name = 1;\n"));
        assert_eq!(plan[0].site_count, 1);
        assert!(plan[0].move_to.is_none());
    }

    #[test]
    fn document_changes_form_applies_edits() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.rs");
        std::fs::write(&file, "fn old() {}\n").unwrap();
        let edit = json!({
            "documentChanges": [{
                "textDocument": {"uri": uri(&file), "version": 1},
                "edits": [{
                    "range": {"start": {"line": 0, "character": 3}, "end": {"line": 0, "character": 6}},
                    "newText": "renamed"
                }]
            }]
        });
        let plan = plan_workspace_edit(&edit, &identity).unwrap();
        assert_eq!(plan[0].new_content.as_deref(), Some("fn renamed() {}\n"));
    }

    #[test]
    fn multiple_edits_in_one_file_apply_descending() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.rs");
        std::fs::write(&file, "x = old; y = old;\n").unwrap();
        // Two edits, supplied ascending; applier must not corrupt the second.
        let edit = json!({
            "changes": {
                uri(&file): [
                    {"range": {"start": {"line": 0, "character": 4}, "end": {"line": 0, "character": 7}}, "newText": "NEW"},
                    {"range": {"start": {"line": 0, "character": 13}, "end": {"line": 0, "character": 16}}, "newText": "NEW"}
                ]
            }
        });
        let plan = plan_workspace_edit(&edit, &identity).unwrap();
        assert_eq!(plan[0].new_content.as_deref(), Some("x = NEW; y = NEW;\n"));
        assert_eq!(plan[0].site_count, 2);
    }

    #[test]
    fn multi_file_edit_produces_one_fileedit_each() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.rs");
        let b = dir.path().join("b.rs");
        std::fs::write(&a, "old()\n").unwrap();
        std::fs::write(&b, "old()\n").unwrap();
        let edit = json!({
            "changes": {
                uri(&a): [{"range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 3}}, "newText": "new"}],
                uri(&b): [{"range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 3}}, "newText": "new"}]
            }
        });
        let mut plan = plan_workspace_edit(&edit, &identity).unwrap();
        plan.sort_by(|x, y| x.worktree_path.cmp(&y.worktree_path));
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].new_content.as_deref(), Some("new()\n"));
        assert_eq!(plan[1].new_content.as_deref(), Some("new()\n"));
    }

    #[test]
    fn rename_file_move_carries_destination() {
        let dir = tempfile::tempdir().unwrap();
        let foo = dir.path().join("foo.rs");
        let bar = dir.path().join("bar.rs");
        std::fs::write(&foo, "pub fn f() {}\n").unwrap();
        let edit = json!({
            "documentChanges": [{
                "kind": "rename",
                "oldUri": uri(&foo),
                "newUri": uri(&bar)
            }]
        });
        let plan = plan_workspace_edit(&edit, &identity).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].worktree_path, foo);
        assert_eq!(plan[0].move_to.as_deref(), Some(bar.as_path()));
        assert_eq!(plan[0].new_content.as_deref(), Some("pub fn f() {}\n"));
    }

    #[test]
    fn out_of_worktree_edit_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let outside = dir.path().join("outside.rs");
        std::fs::write(&outside, "old\n").unwrap();
        let edit = json!({
            "changes": {
                uri(&outside): [{"range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 3}}, "newText": "new"}]
            }
        });
        // A translator that maps nothing: every edit is out of bounds.
        let reject = |_: &Path| None;
        let err = plan_workspace_edit(&edit, &reject).unwrap_err();
        assert!(matches!(err, RenameEditError::OutsideWorktree(_)));
    }

    #[test]
    fn empty_edit_yields_no_file_edits() {
        assert!(plan_workspace_edit(&Value::Null, &identity).unwrap().is_empty());
    }
}
