//! Repair of the provider-side conversation the Claude CLI replays on resume.
//!
//! `claude --resume <id>` does not read Cairn's transcript: it replays its own
//! JSONL record at `<config dir>/projects/<mangled cwd>/<id>.jsonl` and assembles
//! the provider payload from it. Cairn owns that file on disk between turns, so
//! this module is Cairn's seam on the assembler.
//!
//! A single content block the API will never accept makes that replay fail
//! forever: one empty text block turns every later resume — including a
//! well-formed operator message — into `400 messages: text content blocks must
//! be non-empty`, and the session is unrecoverable (CAIRN-3263). The observed
//! specimen was written by the provider, not by Cairn: the model spent a turn's
//! whole output budget on thinking and emitted `{"type":"text","text":""}` with
//! `stop_reason: end_turn`, which the CLI persisted verbatim.
//!
//! What is repaired is deliberately narrow, and drawn from a census of every
//! Claude transcript on a working machine (1,775 files) rather than from a list
//! of shapes an API might dislike:
//!
//! - **Empty text blocks** (3 occurrences, 2 files) are fatal on every replay.
//!   They are dropped; a message left with no blocks at all keeps one honest
//!   elision marker so the record does not silently lose a turn.
//! - **Empty thinking blocks** (43,059 occurrences) are ubiquitous and benign —
//!   those sessions resume fine. They are also signed, so rewriting one would
//!   invalidate its signature. Left alone.
//! - **Dangling `tool_use` blocks** (18 occurrences) sit at the end of an
//!   interrupted turn, and the CLI already repairs them on replay. Left alone.
//!
//! Repair is best-effort by design and never blocks a resume, but it is never
//! silent: every rewritten block is logged with its file, entry index, entry
//! uuid, role, and block index, so the upstream defect stays visible.

use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

const FINGERPRINT_BYTES: u64 = 4096;
const REPAIR_INDEX_CAPACITY: usize = 1024;

#[derive(Clone, Copy)]
struct RepairIndexEntry {
    validated_len: u64,
    fingerprint: u64,
    modified: Option<SystemTime>,
    identity: Option<FileIdentity>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    file: u64,
}

#[cfg(unix)]
fn file_identity(metadata: &std::fs::Metadata) -> Option<FileIdentity> {
    use std::os::unix::fs::MetadataExt;
    Some(FileIdentity {
        device: metadata.dev(),
        file: metadata.ino(),
    })
}

#[cfg(not(unix))]
fn file_identity(_metadata: &std::fs::Metadata) -> Option<FileIdentity> {
    // std does not expose a stable cross-platform file id. On these targets the
    // cache still uses length, modification time, and the bounded fingerprint.
    None
}

type RepairIndexKey = (PathBuf, String);

struct RepairIndex {
    entries: HashMap<RepairIndexKey, RepairIndexEntry>,
    insertion_order: VecDeque<RepairIndexKey>,
    capacity: usize,
}

impl RepairIndex {
    fn new() -> Self {
        Self::with_capacity(REPAIR_INDEX_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            insertion_order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn get(&self, key: &RepairIndexKey) -> Option<&RepairIndexEntry> {
        self.entries.get(key)
    }

    fn insert(&mut self, key: RepairIndexKey, entry: RepairIndexEntry) {
        if let Some(existing) = self.entries.get_mut(&key) {
            *existing = entry;
            return;
        }
        if self.entries.len() == self.capacity {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
        self.insertion_order.push_back(key.clone());
        self.entries.insert(key, entry);
    }

    fn remove(&mut self, key: &RepairIndexKey) {
        if self.entries.remove(key).is_some() {
            self.insertion_order.retain(|queued| queued != key);
        }
    }

    #[cfg(test)]
    fn contains_key(&self, key: &RepairIndexKey) -> bool {
        self.entries.contains_key(key)
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn repair_index() -> &'static Mutex<RepairIndex> {
    static INDEX: OnceLock<Mutex<RepairIndex>> = OnceLock::new();
    INDEX.get_or_init(|| Mutex::new(RepairIndex::new()))
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RepairPass {
    pub(crate) bytes_read: u64,
    pub(crate) lines_parsed: usize,
    pub(crate) used_full_scan: bool,
}

/// Stands in for a message whose every block was empty. A message with no
/// content blocks is as rejectable as an empty one, so something must remain;
/// this says plainly what happened instead of fabricating a turn.
const ELISION_MARKER: &str =
    "[Cairn elided an empty content block here so this conversation could resume.]";

/// What was done to one rejectable block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepairAction {
    /// The block was dropped; the message kept its other blocks.
    DroppedEmptyText,
    /// The message had nothing usable left, so the marker replaced its content.
    SubstitutedElisionMarker,
}

impl RepairAction {
    fn label(self) -> &'static str {
        match self {
            RepairAction::DroppedEmptyText => "dropped empty text block",
            RepairAction::SubstitutedElisionMarker => "substituted elision marker",
        }
    }
}

/// One repaired block, carrying enough position and origin to trace it back to
/// the turn that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TranscriptRepair {
    /// Zero-based line index of the transcript entry.
    pub(crate) entry: usize,
    /// The entry's own uuid, which is also how its successor links back to it.
    pub(crate) uuid: Option<String>,
    /// `user` or `assistant`.
    pub(crate) role: String,
    /// Index within the entry's content array (0 for whole-string content).
    pub(crate) block: usize,
    pub(crate) action: RepairAction,
}

/// Repair the transcript the CLI is about to replay, if it needs it. Called
/// immediately before a `--resume` / `--fork-session` spawn, when no CLI process
/// holds the file open.
pub(crate) fn repair_before_resume(
    config_dir: Option<&Path>,
    working_dir: &Path,
    backend_id: &str,
) -> RepairPass {
    let Some(path) = transcript_path(config_dir, working_dir, backend_id) else {
        // A first resume after the transcript was pruned, or a backend id this
        // machine never ran. Nothing to repair; the CLI reports the miss.
        log::debug!("No Claude transcript found for session {backend_id}; skipping repair");
        return RepairPass::default();
    };

    let mut index = repair_index()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    repair_path(&path, backend_id, &mut index)
}

fn repair_path(path: &Path, backend_id: &str, index: &mut RepairIndex) -> RepairPass {
    let canonical_path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let key = (canonical_path, backend_id.to_string());
    let mut pass = RepairPass::default();
    if let Some(previous) = index.get(&key).copied() {
        if let Ok(metadata) = std::fs::metadata(path) {
            let len = metadata.len();
            let identity = file_identity(&metadata);
            let same_identity = previous.identity.is_none() || previous.identity == identity;
            if same_identity
                && len == previous.validated_len
                && metadata.modified().ok() == previous.modified
            {
                return pass;
            }
            // The append fast path deliberately trusts only stable file identity
            // plus a bounded fingerprint immediately before the old EOF. An
            // arbitrary in-place mutation earlier in a growing file cannot be
            // distinguished portably without rereading the full prefix. Same-size
            // changes do fall back because they cannot be legitimate appends.
            if same_identity
                && len > previous.validated_len
                && fingerprint_at(path, previous.validated_len, &mut pass)
                    == Some(previous.fingerprint)
            {
                if let Some(fingerprint) =
                    validate_appended_tail(path, previous.validated_len, len, &mut pass)
                {
                    index.insert(
                        key,
                        RepairIndexEntry {
                            validated_len: len,
                            fingerprint,
                            modified: metadata.modified().ok(),
                            identity,
                        },
                    );
                    return pass;
                }
            }
        }
    }

    pass.used_full_scan = true;
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            log::warn!(
                "Could not read Claude transcript {}: {error}",
                path.display()
            );
            index.remove(&key);
            return pass;
        }
    };
    pass.bytes_read += bytes.len() as u64;
    let complete = bytes.ends_with(b"\n");
    let Ok(contents) = String::from_utf8(bytes) else {
        log::warn!(
            "Claude transcript {} is not valid UTF-8; leaving it untouched",
            path.display()
        );
        index.remove(&key);
        return pass;
    };
    let certain = lines_are_json(&contents, &mut pass.lines_parsed);
    if let Some((repaired, repairs)) = repair_transcript(&contents) {
        if let Err(error) = write_atomic(path, &repaired) {
            log::warn!(
                "Could not rewrite repaired Claude transcript {}: {error}",
                path.display()
            );
            index.remove(&key);
            return pass;
        }
        log_repairs(path, backend_id, &repairs);
        let repaired_bytes = repaired.as_bytes();
        if complete && certain {
            if let Ok(metadata) = std::fs::metadata(path) {
                index.insert(
                    key,
                    RepairIndexEntry {
                        validated_len: repaired_bytes.len() as u64,
                        fingerprint: fingerprint(repaired_bytes),
                        modified: metadata.modified().ok(),
                        identity: file_identity(&metadata),
                    },
                );
            } else {
                index.remove(&key);
            }
        } else {
            index.remove(&key);
        }
    } else if complete && certain {
        if let Ok(metadata) = std::fs::metadata(path) {
            index.insert(
                key,
                RepairIndexEntry {
                    validated_len: contents.len() as u64,
                    fingerprint: fingerprint(contents.as_bytes()),
                    modified: metadata.modified().ok(),
                    identity: file_identity(&metadata),
                },
            );
        } else {
            index.remove(&key);
        }
    } else {
        index.remove(&key);
    }
    pass
}

fn validate_appended_tail(
    path: &Path,
    offset: u64,
    len: u64,
    pass: &mut RepairPass,
) -> Option<u64> {
    let mut file = std::fs::File::open(path).ok()?;
    file.seek(SeekFrom::Start(offset)).ok()?;
    let mut bytes = Vec::with_capacity((len - offset) as usize);
    file.read_to_end(&mut bytes).ok()?;
    pass.bytes_read += bytes.len() as u64;
    if !bytes.ends_with(b"\n") {
        return None;
    }
    let tail = std::str::from_utf8(&bytes).ok()?;
    if !lines_are_json(tail, &mut pass.lines_parsed) || repair_transcript(tail).is_some() {
        return None;
    }
    fingerprint_at(path, len, pass)
}

fn lines_are_json(contents: &str, parsed: &mut usize) -> bool {
    contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .all(|line| {
            *parsed += 1;
            serde_json::from_str::<Value>(line).is_ok()
        })
}

fn fingerprint_at(path: &Path, offset: u64, pass: &mut RepairPass) -> Option<u64> {
    let start = offset.saturating_sub(FINGERPRINT_BYTES);
    let mut file = std::fs::File::open(path).ok()?;
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut bytes = vec![0; (offset - start) as usize];
    file.read_exact(&mut bytes).ok()?;
    pass.bytes_read += bytes.len() as u64;
    Some(fingerprint(&bytes))
}

fn fingerprint(bytes: &[u8]) -> u64 {
    let tail = &bytes[bytes.len().saturating_sub(FINGERPRINT_BYTES as usize)..];
    let mut hasher = DefaultHasher::new();
    tail.hash(&mut hasher);
    hasher.finish()
}

fn log_repairs(path: &Path, backend_id: &str, repairs: &[TranscriptRepair]) {
    log::warn!(
        "Repaired {} rejectable content block(s) in Claude transcript {} before resuming session {backend_id}",
        repairs.len(), path.display()
    );
    for repair in repairs {
        log::warn!(
            "  entry #{} (uuid={}, role={}) block #{}: {}",
            repair.entry,
            repair.uuid.as_deref().unwrap_or("unknown"),
            repair.role,
            repair.block,
            repair.action.label()
        );
    }
}

/// Repair every rejectable block in a JSONL transcript. Returns `None` when the
/// transcript is already clean, so a healthy file is never rewritten. Lines that
/// do not parse, and entry kinds the provider never replays, are copied through
/// byte-for-byte.
pub(crate) fn repair_transcript(contents: &str) -> Option<(String, Vec<TranscriptRepair>)> {
    let mut repairs = Vec::new();
    let mut out = String::with_capacity(contents.len());
    for (index, line) in contents.lines().enumerate() {
        match repair_entry(index, line, &mut repairs) {
            Some(rewritten) => out.push_str(&rewritten),
            None => out.push_str(line),
        }
        out.push('\n');
    }
    (!repairs.is_empty()).then_some((out, repairs))
}

/// Repair one transcript entry, returning its rewritten JSON only when a block
/// actually changed.
fn repair_entry(index: usize, line: &str, repairs: &mut Vec<TranscriptRepair>) -> Option<String> {
    if line.trim().is_empty() {
        return None;
    }
    let mut entry: Value = serde_json::from_str(line).ok()?;
    // Only the two conversational kinds reach the provider. Sidecar entries
    // (`queue-operation`, `ai-title`, `last-prompt`, summaries) are the CLI's
    // own bookkeeping and are never replayed as messages.
    let role = match entry.get("type").and_then(Value::as_str) {
        Some(role @ ("user" | "assistant")) => role.to_string(),
        _ => return None,
    };
    let uuid = entry
        .get("uuid")
        .and_then(Value::as_str)
        .map(str::to_string);
    let content = entry.get_mut("message")?.get_mut("content")?;

    let found = repair_content(content);
    if found.is_empty() {
        return None;
    }
    repairs.extend(found.into_iter().map(|(block, action)| TranscriptRepair {
        entry: index,
        uuid: uuid.clone(),
        role: role.clone(),
        block,
        action,
    }));
    Some(entry.to_string())
}

/// Rewrite a message's content in place, reporting `(block index, action)` for
/// each block changed.
fn repair_content(content: &mut Value) -> Vec<(usize, RepairAction)> {
    match content {
        // Whole-string content is shorthand for a single text block, and an
        // empty one is rejected the same way.
        Value::String(text) if text.trim().is_empty() => {
            *content = Value::String(ELISION_MARKER.to_string());
            vec![(0, RepairAction::SubstitutedElisionMarker)]
        }
        Value::Array(blocks) => {
            let empty: Vec<usize> = blocks
                .iter()
                .enumerate()
                .filter(|(_, block)| is_empty_text_block(block))
                .map(|(index, _)| index)
                .collect();
            if blocks.is_empty() {
                blocks.push(marker_block());
                return vec![(0, RepairAction::SubstitutedElisionMarker)];
            }
            if empty.is_empty() {
                return Vec::new();
            }
            let nothing_left = empty.len() == blocks.len();
            blocks.retain(|block| !is_empty_text_block(block));
            let action = if nothing_left {
                blocks.push(marker_block());
                RepairAction::SubstitutedElisionMarker
            } else {
                RepairAction::DroppedEmptyText
            };
            empty.into_iter().map(|index| (index, action)).collect()
        }
        _ => Vec::new(),
    }
}

/// A text block the provider rejects: empty, whitespace-only, or missing its
/// `text` field entirely.
fn is_empty_text_block(block: &Value) -> bool {
    block.get("type").and_then(Value::as_str) == Some("text")
        && block
            .get("text")
            .and_then(Value::as_str)
            .map(|text| text.trim().is_empty())
            .unwrap_or(true)
}

fn marker_block() -> Value {
    json!({ "type": "text", "text": ELISION_MARKER })
}

/// Locate the CLI's JSONL record for one session. The CLI derives the directory
/// name from its process cwd, so the path is computed the same way and then
/// confirmed on disk; a miss falls back to searching for the id, since the
/// mangling is the CLI's convention rather than a contract.
fn transcript_path(
    config_dir: Option<&Path>,
    working_dir: &Path,
    backend_id: &str,
) -> Option<PathBuf> {
    // The id becomes a file name, so refuse anything that is not the uuid shape
    // the CLI issues rather than letting it walk the filesystem.
    if backend_id.is_empty()
        || !backend_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return None;
    }
    let projects = claude_config_dir(config_dir)?.join("projects");
    let file_name = format!("{backend_id}.jsonl");

    // The CLI records its cwd after the OS resolves it, so on macOS a `/var/…`
    // scratch dir lands under `-private-var-…`. Canonicalize before mangling.
    let resolved = std::fs::canonicalize(working_dir).unwrap_or_else(|_| working_dir.to_path_buf());
    let direct = projects.join(mangle_cwd(&resolved)).join(&file_name);
    if direct.is_file() {
        return Some(direct);
    }

    std::fs::read_dir(&projects)
        .ok()?
        .flatten()
        .map(|entry| entry.path().join(&file_name))
        .find(|candidate| candidate.is_file())
}

/// The Claude CLI's config root: an explicit managed profile, else the ambient
/// `CLAUDE_CONFIG_DIR`, else `~/.claude`.
fn claude_config_dir(explicit: Option<&Path>) -> Option<PathBuf> {
    if let Some(dir) = explicit {
        return Some(dir.to_path_buf());
    }
    if let Some(dir) = std::env::var_os("CLAUDE_CONFIG_DIR") {
        return Some(PathBuf::from(dir));
    }
    dirs::home_dir().map(|home| home.join(".claude"))
}

/// The CLI's project-directory name for a cwd: every non-alphanumeric character
/// becomes `-`, so `/Users/mitch/.cairn/scratch/CAIRN.3263.1.builder` becomes
/// `-Users-mitch--cairn-scratch-CAIRN-3263-1-builder`.
fn mangle_cwd(path: &Path) -> String {
    path.to_string_lossy()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// Replace the transcript through a sibling temp file so a crash mid-write
/// cannot leave a half-written conversation, carrying the original's
/// permissions across (these files are private by default).
fn write_atomic(path: &Path, contents: &str) -> std::io::Result<()> {
    let temp = path.with_extension("jsonl.cairn-repair");
    std::fs::write(&temp, contents)?;
    if let Ok(metadata) = std::fs::metadata(path) {
        let _ = std::fs::set_permissions(&temp, metadata.permissions());
    }
    std::fs::rename(&temp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// The CAIRN-3242 specimen, verbatim in shape: a thinking entry and the
    /// empty-text entry of the same assistant message, then the CLI's own nudge.
    fn poisoned_transcript() -> String {
        [
            r#"{"type":"user","uuid":"u1","message":{"role":"user","content":"try recreating the pr"}}"#,
            r#"{"type":"assistant","uuid":"a1","message":{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":"sig"}],"stop_reason":"end_turn"}}"#,
            r#"{"type":"assistant","uuid":"a2","message":{"role":"assistant","content":[{"type":"text","text":""}],"stop_reason":"end_turn"}}"#,
            r#"{"type":"user","uuid":"u2","isMeta":true,"message":{"role":"user","content":"[Your previous response had no visible output.]"}}"#,
        ]
        .join("\n")
            + "\n"
    }

    fn content_of(line: &str) -> Value {
        serde_json::from_str::<Value>(line).unwrap()["message"]["content"].clone()
    }

    #[test]
    fn empty_text_block_is_replaced_and_reported_with_position_and_origin() {
        let (repaired, repairs) = repair_transcript(&poisoned_transcript()).unwrap();

        assert_eq!(repairs.len(), 1);
        let repair = &repairs[0];
        assert_eq!(repair.entry, 2);
        assert_eq!(repair.uuid.as_deref(), Some("a2"));
        assert_eq!(repair.role, "assistant");
        assert_eq!(repair.block, 0);
        assert_eq!(repair.action, RepairAction::SubstitutedElisionMarker);

        let lines: Vec<&str> = repaired.lines().collect();
        assert_eq!(lines.len(), 4);
        assert_eq!(content_of(lines[2])[0]["text"], json!(ELISION_MARKER));
        // Nothing else moved: the signed empty thinking block is untouched, and
        // every other entry is byte-identical.
        let original: Vec<String> = poisoned_transcript().lines().map(str::to_string).collect();
        assert_eq!(lines[0], original[0]);
        assert_eq!(lines[1], original[1]);
        assert_eq!(lines[3], original[3]);
    }

    #[test]
    fn repaired_transcript_has_no_rejectable_blocks_left() {
        let (repaired, _) = repair_transcript(&poisoned_transcript()).unwrap();
        for line in repaired.lines() {
            let entry: Value = serde_json::from_str(line).unwrap();
            let content = &entry["message"]["content"];
            if let Some(blocks) = content.as_array() {
                assert!(!blocks.is_empty());
                assert!(blocks.iter().all(|block| !is_empty_text_block(block)));
            }
            if let Some(text) = content.as_str() {
                assert!(!text.trim().is_empty());
            }
        }
        // The repair is idempotent: a repaired transcript needs no second pass.
        assert!(repair_transcript(&repaired).is_none());
    }

    #[test]
    fn healthy_transcript_is_left_alone() {
        let healthy = concat!(
            r#"{"type":"assistant","uuid":"a1","message":{"role":"assistant","content":[{"type":"text","text":"on it"}]}}"#,
            "\n",
            r#"{"type":"queue-operation","uuid":"q1"}"#,
            "\n"
        );
        assert!(repair_transcript(healthy).is_none());
    }

    #[test]
    fn empty_text_beside_a_tool_use_is_dropped_not_substituted() {
        let line = r#"{"type":"assistant","uuid":"a1","message":{"role":"assistant","content":[{"type":"text","text":"   "},{"type":"tool_use","id":"toolu_1","name":"read","input":{}}]}}"#;
        let (repaired, repairs) = repair_transcript(&format!("{line}\n")).unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(repairs[0].action, RepairAction::DroppedEmptyText);
        let content = content_of(repaired.lines().next().unwrap());
        assert_eq!(content.as_array().unwrap().len(), 1);
        assert_eq!(content[0]["type"], json!("tool_use"));
    }

    #[test]
    fn empty_string_content_and_empty_array_content_both_get_the_marker() {
        let transcript = concat!(
            r#"{"type":"user","uuid":"u1","message":{"role":"user","content":""}}"#,
            "\n",
            r#"{"type":"assistant","uuid":"a1","message":{"role":"assistant","content":[]}}"#,
            "\n"
        );
        let (repaired, repairs) = repair_transcript(transcript).unwrap();
        assert_eq!(repairs.len(), 2);
        assert!(repairs
            .iter()
            .all(|repair| repair.action == RepairAction::SubstitutedElisionMarker));
        let lines: Vec<&str> = repaired.lines().collect();
        assert_eq!(content_of(lines[0]), json!(ELISION_MARKER));
        assert_eq!(content_of(lines[1])[0]["text"], json!(ELISION_MARKER));
    }

    #[test]
    fn signed_empty_thinking_blocks_and_dangling_tool_uses_are_not_touched() {
        // Both shapes are common in healthy transcripts that resume fine;
        // rewriting a signed thinking block would invalidate its signature.
        let transcript = concat!(
            r#"{"type":"assistant","uuid":"a1","message":{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":"sig"}]}}"#,
            "\n",
            r#"{"type":"assistant","uuid":"a2","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"run","input":{}}]}}"#,
            "\n"
        );
        assert!(repair_transcript(transcript).is_none());
    }

    #[test]
    fn unparseable_and_blank_lines_survive_verbatim() {
        let transcript = concat!(
            r#"{"type":"assistant","uuid":"a1","message":{"role":"assistant","content":[{"type":"text","text":""}]}}"#,
            "\n",
            "{not json at all\n",
            "\n"
        );
        let (repaired, repairs) = repair_transcript(transcript).unwrap();
        assert_eq!(repairs.len(), 1);
        let lines: Vec<&str> = repaired.lines().collect();
        assert_eq!(lines[1], "{not json at all");
        assert_eq!(lines[2], "");
    }

    #[test]
    fn cwd_mangling_matches_the_cli_convention() {
        assert_eq!(
            mangle_cwd(Path::new(
                "/Users/mitch/.cairn/scratch/CAIRN.3263.1.builder"
            )),
            "-Users-mitch--cairn-scratch-CAIRN-3263-1-builder"
        );
        assert_eq!(
            mangle_cwd(Path::new(
                "/private/var/folders/tm/d63k08q/T/cairn-scratch-CAIRN.3242.1.builder"
            )),
            "-private-var-folders-tm-d63k08q-T-cairn-scratch-CAIRN-3242-1-builder"
        );
    }

    #[test]
    fn transcript_path_finds_the_session_and_refuses_a_traversing_id() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("profile");
        let cwd = temp.path().join("work");
        std::fs::create_dir_all(&cwd).unwrap();
        let resolved = std::fs::canonicalize(&cwd).unwrap();
        let project_dir = config.join("projects").join(mangle_cwd(&resolved));
        std::fs::create_dir_all(&project_dir).unwrap();
        let transcript = project_dir.join("session-1.jsonl");
        std::fs::write(&transcript, "{}\n").unwrap();

        assert_eq!(
            transcript_path(Some(&config), &cwd, "session-1"),
            Some(transcript.clone())
        );
        // A session started under a different cwd is still found by id.
        assert_eq!(
            transcript_path(Some(&config), temp.path(), "session-1"),
            Some(transcript)
        );
        assert_eq!(transcript_path(Some(&config), &cwd, "missing"), None);
        assert_eq!(transcript_path(Some(&config), &cwd, "../escape"), None);
    }

    #[test]
    fn repair_before_resume_rewrites_the_file_in_place_and_leaves_healthy_files_untouched() {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("profile");
        let cwd = temp.path().join("work");
        std::fs::create_dir_all(&cwd).unwrap();
        let resolved = std::fs::canonicalize(&cwd).unwrap();
        let project_dir = config.join("projects").join(mangle_cwd(&resolved));
        std::fs::create_dir_all(&project_dir).unwrap();
        let path = project_dir.join("session-1.jsonl");
        std::fs::write(&path, poisoned_transcript()).unwrap();

        repair_before_resume(Some(&config), &cwd, "session-1");

        let after = std::fs::read_to_string(&path).unwrap();
        assert!(after.contains(ELISION_MARKER));
        assert!(repair_transcript(&after).is_none());
        assert!(!project_dir.join("session-1.jsonl.cairn-repair").exists());

        // A second resume of the now-healthy session leaves the bytes alone.
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        repair_before_resume(Some(&config), &cwd, "session-1");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), after);
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), mtime);
    }

    #[test]
    fn a_missing_transcript_is_not_an_error() {
        let temp = tempfile::tempdir().unwrap();
        repair_before_resume(Some(temp.path()), temp.path(), "session-absent");
    }

    fn healthy_line(uuid: &str, text: &str) -> String {
        json!({
            "type": "assistant",
            "uuid": uuid,
            "message": {"role": "assistant", "content": [{"type": "text", "text": text}]}
        })
        .to_string()
            + "\n"
    }

    #[test]
    fn repair_index_evicts_the_oldest_session_at_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let mut index = RepairIndex::with_capacity(2);
        let paths = (0..3)
            .map(|number| temp.path().join(format!("session-{number}.jsonl")))
            .collect::<Vec<_>>();
        for (number, path) in paths.iter().enumerate() {
            std::fs::write(path, healthy_line(&format!("a{number}"), "healthy")).unwrap();
            repair_path(path, &format!("session-{number}"), &mut index);
        }

        assert_eq!(index.entries.len(), 2);
        assert!(!index.contains_key(&(paths[0].clone(), "session-0".into())));
        assert!(repair_path(&paths[0], "session-0", &mut index).used_full_scan);
        assert!(!repair_path(&paths[2], "session-2", &mut index).used_full_scan);
    }

    #[test]
    fn unchanged_and_append_only_passes_do_not_scan_the_validated_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let prefix = (0..200)
            .map(|index| healthy_line(&format!("a{index}"), &"x".repeat(100)))
            .collect::<String>();
        std::fs::write(&path, &prefix).unwrap();
        let mut index = RepairIndex::new();

        let initial = repair_path(&path, "session", &mut index);
        assert!(initial.used_full_scan);
        assert_eq!(initial.lines_parsed, 200);

        let unchanged = repair_path(&path, "session", &mut index);
        assert_eq!(unchanged, RepairPass::default());

        let appended = healthy_line("new", "tail");
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(appended.as_bytes())
            .unwrap();
        let append_pass = repair_path(&path, "session", &mut index);
        assert!(!append_pass.used_full_scan);
        assert_eq!(append_pass.lines_parsed, 1);
        assert!(append_pass.bytes_read <= appended.len() as u64 + 2 * FINGERPRINT_BYTES);
        assert!(append_pass.bytes_read < prefix.len() as u64);
    }

    #[test]
    fn truncation_replacement_incomplete_and_malformed_tails_fall_back() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let original = healthy_line("a1", "original");
        let mut index = RepairIndex::new();

        std::fs::write(&path, &original).unwrap();
        repair_path(&path, "session", &mut index);
        std::fs::write(&path, healthy_line("a2", "x")).unwrap();
        assert!(repair_path(&path, "session", &mut index).used_full_scan);

        std::fs::write(&path, &original).unwrap();
        repair_path(&path, "session", &mut index);
        std::fs::write(&path, "{}\n").unwrap();
        assert!(repair_path(&path, "session", &mut index).used_full_scan);

        std::fs::write(&path, &original).unwrap();
        repair_path(&path, "session", &mut index);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{\"type\":")
            .unwrap();
        assert!(repair_path(&path, "session", &mut index).used_full_scan);

        std::fs::write(&path, &original).unwrap();
        repair_path(&path, "session", &mut index);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{not json}\n")
            .unwrap();
        assert!(repair_path(&path, "session", &mut index).used_full_scan);
        assert!(!index.contains_key(&(path.clone(), "session".to_string())));
    }

    #[test]
    fn rejectable_append_falls_back_repairs_and_indexes_the_rewrite() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        std::fs::write(&path, healthy_line("a1", "healthy")).unwrap();
        let mut index = RepairIndex::new();
        repair_path(&path, "session", &mut index);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(poisoned_transcript().as_bytes())
            .unwrap();

        let repaired = repair_path(&path, "session", &mut index);
        assert!(repaired.used_full_scan);
        assert!(std::fs::read_to_string(&path)
            .unwrap()
            .contains(ELISION_MARKER));
        assert_eq!(
            repair_path(&path, "session", &mut index),
            RepairPass::default()
        );
    }

    #[test]
    fn malformed_repairable_transcript_is_rewritten_but_not_cached() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let transcript = format!("{}{{not_json}}\n", poisoned_transcript());
        std::fs::write(&path, transcript).unwrap();
        let mut index = RepairIndex::new();

        let repaired = repair_path(&path, "session", &mut index);
        assert!(repaired.used_full_scan);
        assert!(std::fs::read_to_string(&path)
            .unwrap()
            .contains(ELISION_MARKER));
        assert!(index.is_empty());
        assert!(repair_path(&path, "session", &mut index).used_full_scan);
    }

    #[test]
    fn incomplete_repairable_transcript_is_rewritten_but_not_cached() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let transcript = poisoned_transcript().trim_end_matches('\n').to_string();
        std::fs::write(&path, transcript).unwrap();
        let mut index = RepairIndex::new();

        let repaired = repair_path(&path, "session", &mut index);
        assert!(repaired.used_full_scan);
        assert!(std::fs::read_to_string(&path)
            .unwrap()
            .contains(ELISION_MARKER));
        assert!(index.is_empty());
        assert!(repair_path(&path, "session", &mut index).used_full_scan);
    }

    #[test]
    fn same_size_large_prefix_rewrite_falls_back_outside_bounded_fingerprint() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let original = (0..100)
            .map(|index| healthy_line(&format!("a{index}"), &"x".repeat(100)))
            .collect::<String>();
        std::fs::write(&path, &original).unwrap();
        let mut index = RepairIndex::new();
        repair_path(&path, "session", &mut index);

        let mut rewritten = original.into_bytes();
        let mutation = rewritten.iter().position(|byte| *byte == b'x').unwrap();
        rewritten[mutation] = b'y';
        std::fs::write(&path, rewritten).unwrap();

        let pass = repair_path(&path, "session", &mut index);
        assert!(pass.used_full_scan);
        assert!(pass.bytes_read > FINGERPRINT_BYTES);
    }

    #[cfg(unix)]
    #[test]
    fn replacement_file_with_same_contents_does_not_hit_cached_identity() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let replacement = temp.path().join("replacement.jsonl");
        let contents = healthy_line("a1", "healthy");
        std::fs::write(&path, &contents).unwrap();
        let mut index = RepairIndex::new();
        repair_path(&path, "session", &mut index);
        let original_identity = file_identity(&std::fs::metadata(&path).unwrap());

        std::fs::write(&replacement, &contents).unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        assert_ne!(
            original_identity,
            file_identity(&std::fs::metadata(&path).unwrap())
        );

        assert!(repair_path(&path, "session", &mut index).used_full_scan);
    }
}
