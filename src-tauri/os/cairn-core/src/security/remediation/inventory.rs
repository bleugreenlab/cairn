//! Finding every record that carries a disclosed credential, without making a
//! copy of it in the process.
//!
//! # The counting rule
//!
//! An incident report wants to quote what it found. It is the natural thing to
//! write and it is the one thing this module must never do: a report that quotes
//! the credential has copied it into a *new* durable store, on the exact day
//! someone decided the old ones were too dangerous to keep. The remediation
//! would then need remediating.
//!
//! So the rule is that a scan yields a **count and an address**, never a span.
//! It is held structurally rather than by care: [`count_occurrences`] is the only
//! function here that sees record content, it returns `usize`, and
//! [`AffectedRecord`] has no field a span could be put in. There is no plumbing
//! between the bytes and the report, so writing the tempting line does not
//! compile.
//!
//! # Matching is the live matcher
//!
//! Occurrences are counted with the same [`StreamingScrubber`] the process,
//! terminal, and executor seams scrub with. Reusing it means "what counts as an
//! occurrence here" and "what gets redacted there" cannot drift into two
//! answers, and it brings the encoded forms and chunk-boundary handling along
//! for free. The scrubbed bytes it hands back are dropped immediately — this
//! module wants the detections, not the output.
//!
//! # Why the scan must run before rotation
//!
//! Matching is by registered value, so a scan can only find a credential the
//! registry still holds. Rotating the credential first would make the disclosure
//! unfindable: the old value is never registered again by anyone, and every
//! record carrying it becomes invisible while remaining exactly as readable to
//! whoever opens the database.
//!
//! That is why [`super::respond`] rotates last and revokes first. Revocation
//! neuters the credential without unregistering it, so it buys containment at no
//! cost to the inventory; rotation ends the ability to find what is left behind.
//! The ordering is not a preference, it is forced.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cairn_common::security::{RegistrySnapshot, SecretId, StreamingScrubber};
use cairn_db::storage::{DbResult, LocalDb, RowExt};

use super::sink::{SinkKind, ALL_SINKS};

/// One durable record found to carry the disclosed credential.
///
/// Deliberately has no field that could hold record content. See the module
/// docs: the absence is the invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffectedRecord {
    /// Which store.
    pub sink: SinkKind,
    /// A non-secret address within that store: a row id, or a file path.
    pub locator: String,
    /// How many registered forms matched. Evidence of extent, not of content.
    pub occurrences: usize,
}

/// What a scan of every reachable store found.
#[derive(Debug, Clone, Default)]
pub struct Inventory {
    /// Records that matched, across every scanned store.
    pub records: Vec<AffectedRecord>,
    /// Stores this build cannot reach, each with the reason an operator has to
    /// handle it. Present even when nothing matched anywhere else: the point is
    /// that the operator learns what was *not* looked at.
    pub manual: Vec<(SinkKind, &'static str)>,
}

impl Inventory {
    /// Total occurrences across every affected record.
    pub fn total_occurrences(&self) -> usize {
        self.records.iter().map(|record| record.occurrences).sum()
    }

    /// The distinct stores that had at least one match.
    pub fn affected_sinks(&self) -> Vec<SinkKind> {
        let mut sinks: Vec<SinkKind> = Vec::new();
        for record in &self.records {
            if !sinks.contains(&record.sink) {
                sinks.push(record.sink);
            }
        }
        sinks
    }
}

/// Count occurrences of one registered secret in `text`.
///
/// The only function in this module that sees record content, and it returns a
/// number. Everything the scanners know about a record's contents comes through
/// here, which is what makes "no plaintext reaches a report" a property of the
/// call graph rather than of reviewer attention.
pub fn count_occurrences(snapshot: &Arc<RegistrySnapshot>, secret: &SecretId, text: &str) -> usize {
    count_occurrences_bytes(snapshot, secret, text.as_bytes())
}

/// [`count_occurrences`] over bytes, for file content that need not be UTF-8.
pub fn count_occurrences_bytes(
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
    bytes: &[u8],
) -> usize {
    if snapshot.is_empty() {
        return 0;
    }
    let mut scrubber = StreamingScrubber::with_snapshot(Arc::clone(snapshot));
    // The scrubbed output is deliberately discarded. We are asking whether the
    // credential is here, not producing a clean copy.
    let _ = scrubber.push(bytes);
    let _ = scrubber.flush();
    occurrences_of(&scrubber, secret)
}

/// Sum a scrubber's detections for one secret.
///
/// A process holds many credentials at once, and the snapshot matches all of
/// them. Filtering by id is what keeps one disclosure from quarantining records
/// that carry a different, undisclosed credential.
fn occurrences_of(scrubber: &StreamingScrubber, secret: &SecretId) -> usize {
    scrubber
        .detections()
        .entries()
        .iter()
        .filter(|detection| detection.secret_id.as_ref() == Some(secret))
        .map(|detection| detection.count)
        .sum()
}

/// The directories the file-backed stores live in.
///
/// Explicit rather than read from the environment at each call, for two
/// reasons. A response that has to scan a gigabyte of logs should be able to say
/// *which* logs — an operator restoring an archived directory has the same
/// question as a test with a temp directory. And a scanner that silently reaches
/// into `~/.cairn` is one that cannot be exercised without touching the machine
/// it runs on, which is how a test suite ends up depending on the developer's
/// own log history.
#[derive(Debug, Clone)]
pub struct InventoryRoots {
    /// Where the rotated JSONL logs and the runner's stderr file live.
    pub log_dir: PathBuf,
    /// The scratch root whose per-job `terminals/` directories hold persisted
    /// scrollback.
    pub scratch_root: PathBuf,
}

impl InventoryRoots {
    /// The locations this installation actually uses.
    pub fn host() -> Self {
        Self {
            log_dir: cairn_common::paths::cairn_log_dir(),
            scratch_root: cairn_common::scratch::scratch_root(),
        }
    }
}

impl Default for InventoryRoots {
    fn default() -> Self {
        Self::host()
    }
}

/// Scan every reachable store for `secret`, in this installation's own
/// directories.
pub async fn take_inventory(
    db: &LocalDb,
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
) -> DbResult<Inventory> {
    take_inventory_in(db, snapshot, secret, &InventoryRoots::host()).await
}

/// Scan every reachable store for `secret`, with the file-backed stores rooted
/// at `roots`.
///
/// Derived stores are not scanned: their handling does not depend on what a scan
/// would find. See [`super::sink`].
pub async fn take_inventory_in(
    db: &LocalDb,
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
    roots: &InventoryRoots,
) -> DbResult<Inventory> {
    let mut records = Vec::new();

    let (transcripts, segments) = scan_transcript_events(db, snapshot, secret).await?;
    records.extend(transcripts);
    records.extend(scan_archival_segments(db, snapshot, secret, segments).await?);
    records.extend(scan_history_packs(db, snapshot, secret).await?);
    for spec in SCANNED_TABLES {
        records.extend(scan_text_column(db, snapshot, secret, *spec).await?);
    }
    records.extend(scan_process_logs(snapshot, secret, &roots.log_dir));
    records.extend(scan_terminal_logs(snapshot, secret, &roots.scratch_root));

    let manual = ALL_SINKS
        .iter()
        .filter_map(|sink| match sink.reach() {
            super::sink::Reach::Manual(reason) => Some((*sink, reason)),
            super::sink::Reach::Automatic => None,
        })
        .collect();

    Ok(Inventory { records, manual })
}

/// A store that is one or more text columns of one table.
#[derive(Clone, Copy)]
struct TextColumns {
    sink: SinkKind,
    table: &'static str,
    id_column: &'static str,
    /// Every column of the table that can carry free text. Listing them
    /// explicitly rather than scanning `SELECT *` is deliberate: a column added
    /// later is a decision to make here, not one made silently by a wildcard.
    text_columns: &'static [&'static str],
}

const ARTIFACTS: TextColumns = TextColumns {
    sink: SinkKind::Artifact,
    table: "artifacts",
    id_column: "id",
    text_columns: &["data"],
};

const MESSAGES: TextColumns = TextColumns {
    sink: SinkKind::Message,
    table: "messages",
    id_column: "id",
    text_columns: &["content"],
};

const ISSUES: TextColumns = TextColumns {
    sink: SinkKind::IssueBody,
    table: "issues",
    id_column: "id",
    text_columns: &["title", "description"],
};

const REPL_EXCHANGES: TextColumns = TextColumns {
    sink: SinkKind::ReplExchange,
    table: "repl_exchanges",
    id_column: "id",
    text_columns: &["code", "value", "stdout", "stderr", "error", "note"],
};

const TERMINAL_TAILS: TextColumns = TextColumns {
    sink: SinkKind::TerminalTail,
    table: "job_terminals",
    id_column: "id",
    text_columns: &["output_tail", "command"],
};

const STREAM_CHUNKS: TextColumns = TextColumns {
    sink: SinkKind::StreamChunk,
    table: "message_stream_chunks",
    id_column: "id",
    text_columns: &["data"],
};

/// Keyed by `input_hash` rather than a row id: the cache holds one row per
/// (check, inputs) per project and environment, and every one of them carries
/// the same output. Purging by the hash therefore takes the sibling copies an
/// id-keyed purge would leave behind.
const CHECK_RESULTS: TextColumns = TextColumns {
    sink: SinkKind::CheckResultCache,
    table: "check_result_cache",
    id_column: "input_hash",
    text_columns: &["output_tail"],
};

/// Every table-backed store the inventory scans. Iterated rather than called one
/// by one so adding a store is one line and cannot be half-added.
const SCANNED_TABLES: &[TextColumns] = &[
    ARTIFACTS,
    MESSAGES,
    ISSUES,
    REPL_EXCHANGES,
    TERMINAL_TAILS,
    STREAM_CHUNKS,
    CHECK_RESULTS,
];

async fn scan_text_column(
    db: &LocalDb,
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
    spec: TextColumns,
) -> DbResult<Vec<AffectedRecord>> {
    let columns = spec.text_columns.join(", ");
    let sql = format!("SELECT {}, {columns} FROM {}", spec.id_column, spec.table);
    let column_count = spec.text_columns.len();
    let snapshot = Arc::clone(snapshot);
    let secret = secret.clone();
    let sink = spec.sink;

    db.read(move |conn| {
        let snapshot = Arc::clone(&snapshot);
        let secret = secret.clone();
        let sql = sql.clone();
        Box::pin(async move {
            let mut out = Vec::new();
            let mut rows = conn.query(&sql, ()).await?;
            while let Some(row) = rows.next().await? {
                let id = row.text(0)?;
                let mut occurrences = 0;
                for index in 1..=column_count {
                    if let Some(text) = row.opt_text(index)? {
                        occurrences += count_occurrences(&snapshot, &secret, &text);
                    }
                }
                if occurrences > 0 {
                    out.push(AffectedRecord {
                        sink,
                        locator: id,
                        occurrences,
                    });
                }
            }
            DbResult::Ok(out)
        })
    })
    .await
}

/// Scan transcript event rows, live and archived alike.
///
/// An archived row's text sits compressed in `data_blob`, so scanning only
/// `data` would report a clean transcript for exactly the rows that have been
/// around longest. Git-addressed content is deliberately not resolved here: what
/// a `gitcoord` row points at is the repository's own history, which is an
/// [`SinkKind::ExternalCopy`] concern and is reported as manual rather than
/// silently half-scanned.
///
/// Also returns every segment-blob hash the archived `blobbed` rows reference
/// (a system prompt's static segments, a system init's skeleton and tool set),
/// so [`scan_archival_segments`] can scan the ones held in the content store.
/// The rows are not reconstructed here: a blobbed row's `data` is the stub that
/// names its hashes.
async fn scan_transcript_events(
    db: &LocalDb,
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
) -> DbResult<(Vec<AffectedRecord>, BTreeSet<String>)> {
    let snapshot = Arc::clone(snapshot);
    let secret = secret.clone();
    db.read(move |conn| {
        let snapshot = Arc::clone(&snapshot);
        let secret = secret.clone();
        Box::pin(async move {
            let mut out = Vec::new();
            let mut segments = BTreeSet::new();
            let mut rows = conn
                .query(
                    "SELECT id, data, data_blob, codec, storage_mode, content_commit FROM events",
                    (),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                let id = row.text(0)?;
                let data = row.text(1)?;
                let blobbed =
                    row.opt_text(4)?.as_deref() == Some("gitcoord") && row.opt_text(5)?.is_none();
                if blobbed {
                    segments.extend(cairn_db::storage::events::segment_hashes(&data));
                }
                let mut occurrences = count_occurrences(&snapshot, &secret, &data);
                if let Some(blob) = row.opt_blob(2)? {
                    let codec = row
                        .opt_text(3)?
                        .unwrap_or_else(|| cairn_db::storage::CODEC_NONE.to_string());
                    if let Ok(bytes) = cairn_db::storage::decompress(&codec, &blob) {
                        occurrences += count_occurrences_bytes(&snapshot, &secret, &bytes);
                    }
                }
                if occurrences > 0 {
                    out.push(AffectedRecord {
                        sink: SinkKind::TranscriptEvent,
                        locator: id,
                        occurrences,
                    });
                }
            }
            DbResult::Ok((out, segments))
        })
    })
    .await
}

/// `archival_blobs` rows read per page while scanning them.
const SEGMENT_PAGE_ROWS: i64 = 100;

/// Scan the content-addressed archival segment blobs, wherever they live.
///
/// These are the static segments of a system prompt (and a system init's
/// skeleton and tool set), moved out of their events at archival and
/// deduplicated by hash. A credential interpolated into a system prompt lands
/// here, shared by every run whose prompt hashed the same — which is why the
/// blob is addressed and quarantined by its own hash rather than through any
/// one event, and why the read gate withholds by that hash whichever store
/// served it.
///
/// Two homes, each segment counted once: the rows still in `archival_blobs`
/// (compressed, draining into the file archive), paged by hash; then every
/// `referenced` hash not found there, fetched from the content store. A
/// referenced segment the store cannot produce because it is offline or
/// failing fails the inventory rather than reading as clean.
async fn scan_archival_segments(
    db: &LocalDb,
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
    referenced: BTreeSet<String>,
) -> DbResult<Vec<AffectedRecord>> {
    let mut out = Vec::new();
    let segment = |hash: String, bytes: &[u8]| {
        let occurrences = count_occurrences_bytes(snapshot, secret, bytes);
        (occurrences > 0).then_some(AffectedRecord {
            sink: SinkKind::ArchivalBlob,
            locator: hash,
            occurrences,
        })
    };

    // A team replica has no `archival_blobs` table.
    let mut in_table: HashSet<String> = HashSet::new();
    if db.team_content_store().is_none() {
        let mut after = String::new();
        loop {
            let page: Vec<(String, Option<Vec<u8>>)> = db
                .query_all(
                    "SELECT hash, content FROM archival_blobs WHERE hash > ?1
                     ORDER BY hash LIMIT ?2",
                    (after.clone(), SEGMENT_PAGE_ROWS),
                    |row| Ok((row.text(0)?, row.opt_blob(1)?)),
                )
                .await?;
            let Some(last) = page.last().map(|(hash, _)| hash.clone()) else {
                break;
            };
            for (hash, blob) in page {
                in_table.insert(hash.clone());
                let Some(blob) = blob else {
                    continue;
                };
                if let Ok(bytes) =
                    cairn_db::storage::decompress(cairn_db::storage::CODEC_ZSTD_V1, &blob)
                {
                    out.extend(segment(hash, &bytes));
                }
            }
            after = last;
        }
    }

    let Some(store) = db.content_store() else {
        return Ok(out);
    };
    for hash in referenced {
        if in_table.contains(&hash) {
            continue;
        }
        match store.get(&hash).await {
            Ok(Some(bytes)) => out.extend(segment(hash, &bytes)),
            // Gone from every home: nothing left to serve or scan.
            Ok(None) => {}
            Err(error) => {
                return Err(cairn_db::storage::DbError::internal(format!(
                    "archival segment {hash} could not be scanned: {error}"
                )))
            }
        }
    }
    Ok(out)
}

/// `history_tier` rows read per page while scanning history packs.
const PACK_PAGE_ROWS: i64 = 100;

/// Scan the `events` rows of every catalogued history pack, of every scope
/// kind: executions and thread ranges alike.
///
/// A pack is a second copy of its scope's transcript, so a credential in a
/// packed event is reported twice: once under [`SinkKind::TranscriptEvent`] for
/// the hot row, once under [`SinkKind::HistoryPack`] for the packed one, both by
/// event id. Packs are visited a catalog page at a time and decoded one frame
/// at a time, on the blocking pool. A pack the store cannot produce because it
/// is offline or failing fails the inventory rather than reading as clean; a
/// catalogued object the store no longer has is logged and passed over, since
/// no reader can serve it either.
async fn scan_history_packs(
    db: &LocalDb,
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
) -> DbResult<Vec<AffectedRecord>> {
    // A team replica has no catalog of its own.
    if db.team_content_store().is_some() {
        return Ok(Vec::new());
    }
    let Some(store) = db.content_store().cloned() else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    let mut after = String::new();
    loop {
        let page: Vec<(String, String)> = db
            .query_all(
                "SELECT scope_id, content_hash FROM history_tier WHERE scope_id > ?1
                 ORDER BY scope_id LIMIT ?2",
                (after.clone(), PACK_PAGE_ROWS),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await?;
        let Some(last) = page.last().map(|(scope, _)| scope.clone()) else {
            break;
        };
        for (scope_id, hash) in page {
            let bytes = match store.get(&hash).await {
                Ok(Some(bytes)) => bytes,
                Ok(None) => {
                    log::warn!(
                        "remediation: history pack {hash} of {scope_id} is catalogued but not in the archive"
                    );
                    continue;
                }
                Err(error) => {
                    return Err(cairn_db::storage::DbError::internal(format!(
                        "history pack {hash} of {scope_id} could not be scanned: {error}"
                    )))
                }
            };
            let (snapshot, secret) = (Arc::clone(snapshot), secret.clone());
            let found = tokio::task::spawn_blocking(move || scan_pack(bytes, &snapshot, &secret))
                .await
                .map_err(|error| {
                    cairn_db::storage::DbError::internal(format!(
                        "scanning history pack {hash}: {error}"
                    ))
                })?
                .map_err(|error| {
                    cairn_db::storage::DbError::internal(format!(
                        "history pack {hash} of {scope_id} could not be scanned: {error}"
                    ))
                })?;
            out.extend(found);
        }
        after = last;
    }
    Ok(out)
}

/// Every packed event carrying `secret`, counted over every string the row
/// holds: the reconstructed `data`, and an unresolved row's stored form.
fn scan_pack(
    bytes: Vec<u8>,
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
) -> Result<Vec<AffectedRecord>, cairn_db::storage::history_pack::PackError> {
    use cairn_db::storage::history_pack::{HistoryPack, Section};
    let pack = HistoryPack::open(bytes)?;
    let runs: Vec<String> = pack
        .manifest()
        .runs
        .iter()
        .map(|run| run.id.clone())
        .collect();
    let mut out = Vec::new();
    for run in runs {
        for row in pack.rows(Section::Events, &run) {
            let row = row?;
            let occurrences: usize = row
                .values()
                .map(|value| count_in_json(snapshot, secret, value))
                .sum();
            if occurrences == 0 {
                continue;
            }
            if let Some(id) = row.get("id").and_then(serde_json::Value::as_str) {
                out.push(AffectedRecord {
                    sink: SinkKind::HistoryPack,
                    locator: id.to_string(),
                    occurrences,
                });
            }
        }
    }
    Ok(out)
}

fn count_in_json(
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
    value: &serde_json::Value,
) -> usize {
    match value {
        serde_json::Value::String(text) => count_occurrences(snapshot, secret, text),
        serde_json::Value::Array(items) => items
            .iter()
            .map(|item| count_in_json(snapshot, secret, item))
            .sum(),
        serde_json::Value::Object(fields) => fields
            .values()
            .map(|item| count_in_json(snapshot, secret, item))
            .sum(),
        _ => 0,
    }
}

/// Scan the rotated JSONL logs and the runner's stderr file.
///
/// These are the residual this issue was opened for: a record written before its
/// process registered the credential was never scrubbed at the writer, and a
/// file rotated away by an older build was never scrubbed at all.
pub fn scan_process_logs(
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
    dir: &Path,
) -> Vec<AffectedRecord> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        // The rotated JSONL files plus the launchd-redirected stderr log. Both
        // are durable records of this process's output.
        let is_log = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                (name.starts_with("cairn-") && name.ends_with(".jsonl"))
                    || name.ends_with(".err.log")
            });
        if !is_log {
            continue;
        }
        if let Some(record) = scan_file(&path, SinkKind::ProcessLog, snapshot, secret) {
            out.push(record);
        }
    }
    out.sort_by(|a, b| a.locator.cmp(&b.locator));
    out
}

/// Scan every job's persisted terminal scrollback.
pub fn scan_terminal_logs(
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
    scratch_root: &Path,
) -> Vec<AffectedRecord> {
    let Ok(jobs) = std::fs::read_dir(scratch_root) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for job in jobs.flatten() {
        let terminals = job.path().join("terminals");
        let Ok(logs) = std::fs::read_dir(&terminals) else {
            continue;
        };
        for log in logs.flatten() {
            let path = log.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
                continue;
            }
            if let Some(record) = scan_file(&path, SinkKind::TerminalLog, snapshot, secret) {
                out.push(record);
            }
        }
    }
    out.sort_by(|a, b| a.locator.cmp(&b.locator));
    out
}

/// Chunk size for streaming a file through the scrubber. The scrubber carries a
/// bounded suffix across chunk boundaries itself, so this is a memory bound
/// rather than a correctness parameter.
const FILE_SCAN_CHUNK: usize = 256 * 1024;

/// Stream one file past the matcher, keeping only the count.
///
/// Streamed rather than read whole because a daily log is allowed to reach a
/// gigabyte, and a remediation pass that exhausts memory is a remediation pass
/// that does not run.
fn scan_file(
    path: &Path,
    sink: SinkKind,
    snapshot: &Arc<RegistrySnapshot>,
    secret: &SecretId,
) -> Option<AffectedRecord> {
    use std::io::Read;

    if snapshot.is_empty() {
        return None;
    }
    let mut file = std::fs::File::open(path).ok()?;
    let mut scrubber = StreamingScrubber::with_snapshot(Arc::clone(snapshot));
    let mut buffer = vec![0u8; FILE_SCAN_CHUNK];
    loop {
        match file.read(&mut buffer) {
            Ok(0) => break,
            // The scrubbed bytes are dropped: this is a search, not a rewrite.
            Ok(read) => {
                let _ = scrubber.push(&buffer[..read]);
            }
            Err(_) => return None,
        }
    }
    let _ = scrubber.flush();
    let occurrences = occurrences_of(&scrubber, secret);
    (occurrences > 0).then(|| AffectedRecord {
        sink,
        locator: display_path(path),
        occurrences,
    })
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::security::{SecretCategory, SecretMaterial, SecretRegistry};

    const VALUE: &str = "sk-live-Qa9Zm2Xp7Lr4Kt8Wd3Nv";

    fn registry_with(id: &str, value: &str) -> (SecretRegistry, SecretId) {
        let registry = SecretRegistry::new();
        let secret_id = SecretId::new(id);
        let guard = registry
            .register(
                secret_id.clone(),
                SecretCategory::ProviderKey,
                "test".to_string(),
                SecretMaterial::from_string(value.to_string()),
            )
            .expect("registerable");
        guard.retain_for_process();
        (registry, secret_id)
    }

    #[test]
    fn counting_finds_every_occurrence_and_returns_only_a_number() {
        let (registry, secret) = registry_with("provider-key", VALUE);
        let snapshot = registry.snapshot();
        let text = format!("first {VALUE} then {VALUE} again");
        assert_eq!(count_occurrences(&snapshot, &secret, &text), 2);
    }

    #[test]
    fn counting_finds_encoded_forms_too() {
        // The same matcher the live seams use, so a base64-encoded credential in
        // a historical log is found by the inventory exactly as it would be
        // redacted in live output.
        use base64::Engine;
        let (registry, secret) = registry_with("provider-key", VALUE);
        let snapshot = registry.snapshot();
        let encoded = base64::engine::general_purpose::STANDARD.encode(VALUE);
        assert!(count_occurrences(&snapshot, &secret, &encoded) > 0);
    }

    #[test]
    fn a_record_that_does_not_carry_the_secret_counts_zero() {
        let (registry, secret) = registry_with("provider-key", VALUE);
        let snapshot = registry.snapshot();
        assert_eq!(
            count_occurrences(&snapshot, &secret, "ordinary output with no credential"),
            0
        );
    }

    #[test]
    fn another_secrets_occurrences_are_not_attributed_to_this_one() {
        // Two credentials are registered at once in any real process. An
        // incident is about one of them, and counting the other's matches would
        // quarantine records that have nothing to do with this disclosure.
        let registry = SecretRegistry::new();
        let mine = SecretId::new("mine");
        let theirs = SecretId::new("theirs");
        registry
            .register(
                mine.clone(),
                SecretCategory::ProviderKey,
                "test".to_string(),
                SecretMaterial::from_string(VALUE.to_string()),
            )
            .expect("registerable")
            .retain_for_process();
        registry
            .register(
                theirs.clone(),
                SecretCategory::ProviderKey,
                "test".to_string(),
                SecretMaterial::from_string("other-Zx91Kd73Lm52Qp".to_string()),
            )
            .expect("registerable")
            .retain_for_process();
        let snapshot = registry.snapshot();

        assert_eq!(
            count_occurrences(&snapshot, &mine, "carries other-Zx91Kd73Lm52Qp only"),
            0
        );
        assert_eq!(
            count_occurrences(&snapshot, &theirs, "carries other-Zx91Kd73Lm52Qp only"),
            1
        );
    }

    #[test]
    fn an_empty_registry_finds_nothing() {
        // The post-rotation state. Worth pinning because it is the failure mode
        // the module docs warn about: after rotation the scan goes quiet while
        // the records stay exactly as readable.
        let registry = SecretRegistry::new();
        let snapshot = registry.snapshot();
        assert_eq!(
            count_occurrences(&snapshot, &SecretId::new("gone"), VALUE),
            0
        );
    }

    #[test]
    fn a_file_scan_reports_a_path_and_a_count_and_no_content() {
        let (registry, secret) = registry_with("provider-key", VALUE);
        let snapshot = registry.snapshot();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cairn-runner.2026-01-01.jsonl");
        std::fs::write(
            &path,
            format!("{{\"message\":\"connecting with {VALUE}\"}}\nsecond line\n"),
        )
        .unwrap();

        let record = scan_file(&path, SinkKind::ProcessLog, &snapshot, &secret).expect("a match");
        assert_eq!(record.sink, SinkKind::ProcessLog);
        assert_eq!(record.occurrences, 1);
        assert_eq!(record.locator, path.to_string_lossy());
        // The record names where, not what. There is no field to check for the
        // value because there is no such field.
        assert!(!record.locator.contains(VALUE));
    }

    /// A credential interpolated into a system prompt lands in a segment the
    /// file archive holds. The inventory finds it there, reports it once by its
    /// hash even while a not-yet-offloaded `archival_blobs` copy also exists,
    /// and the read gate withholds it by that hash.
    #[tokio::test]
    async fn a_secret_in_a_store_held_segment_is_found_once_and_withheld() {
        use crate::archival::rewrite::testsupport::{
            archive, insert_event, load_events, migrated_test_db, seed_chain, system_prompt,
        };
        use cairn_db::storage::quarantine::{quarantine, QuarantineSet, ARCHIVAL_BLOB_SINK};

        let (registry, secret) = registry_with("prompt-key", VALUE);
        let snapshot = registry.snapshot();
        let db = migrated_test_db("remediation-store-segment.db").await;
        seed_chain(&db, "/nonexistent", false).await;
        let leaked = format!("use key {VALUE} for the provider. ").repeat(20);
        let (prompt, _) = system_prompt(&[
            ("backend_base", &"BACKEND-BASE ".repeat(200)),
            ("cairn", leaked.as_str()),
            ("dynamic", "\n\ncwd=/work"),
        ]);
        insert_event(&db, "sp", "run", 1, 1, "system:prompt", &prompt).await;
        archive(&db, None).await.unwrap();
        let hash = cairn_db::storage::content_store::content_hash(leaked.as_bytes());
        assert!(db
            .content_store()
            .unwrap()
            .get(&hash)
            .await
            .unwrap()
            .is_some());

        let roots = InventoryRoots {
            log_dir: tempfile::tempdir().unwrap().keep(),
            scratch_root: tempfile::tempdir().unwrap().keep(),
        };
        let found = take_inventory_in(&db, &snapshot, &secret, &roots)
            .await
            .unwrap();
        let segments: Vec<&AffectedRecord> = found
            .records
            .iter()
            .filter(|record| record.sink == SinkKind::ArchivalBlob)
            .collect();
        assert_eq!(segments.len(), 1, "{:?}", found.records);
        assert_eq!(segments[0].locator, hash);
        assert_eq!(segments[0].occurrences, 20);

        // The same segment also still in archival_blobs: still one record.
        db.execute(
            "INSERT INTO archival_blobs(hash, content, created_at) VALUES (?1, ?2, 1)",
            (
                hash.clone(),
                cairn_db::turso::Value::Blob(
                    cairn_db::storage::compress(leaked.as_bytes()).unwrap(),
                ),
            ),
        )
        .await
        .unwrap();
        let again = take_inventory_in(&db, &snapshot, &secret, &roots)
            .await
            .unwrap();
        let count = again
            .records
            .iter()
            .filter(|record| record.sink == SinkKind::ArchivalBlob)
            .count();
        assert_eq!(count, 1, "never double-counted");

        quarantine().install_for(
            db.path(),
            QuarantineSet::from_entries([(
                ARCHIVAL_BLOB_SINK.to_string(),
                hash.clone(),
                "incident-store-segment".to_string(),
            )]),
        );
        let served = cairn_db::storage::reconstruct_events(&db, load_events(&db).await).await;
        assert!(!served[0].data.contains(VALUE), "withheld on read");
        assert!(served[0].data.contains("incident-store-segment"));
        quarantine().install_for(db.path(), QuarantineSet::from_entries(Vec::new()));
    }

    /// The fixture execution with `VALUE` written into one of its events, then
    /// packed and catalogued. Returns that event's id.
    async fn packed_leak(db: &LocalDb, prefix: &str) -> String {
        use cairn_db::storage::history_pack::testing::{publish_for_test, seed_fixture_execution};
        let names = seed_fixture_execution(db, prefix).await;
        db.execute(
            "UPDATE events SET data = ?1 WHERE id = ?2",
            (
                serde_json::json!({"eventType": "user", "content": format!("use {VALUE} now")})
                    .to_string(),
                names.plain.clone(),
            ),
        )
        .await
        .unwrap();
        publish_for_test(db, "exec-1").await;
        names.plain
    }

    fn empty_roots() -> InventoryRoots {
        InventoryRoots {
            log_dir: tempfile::tempdir().unwrap().keep(),
            scratch_root: tempfile::tempdir().unwrap().keep(),
        }
    }

    /// A packed copy of a leaked event is a second record, found by event id,
    /// and a quarantine under the pack's key alone withholds it from a
    /// pack-sourced read.
    #[tokio::test]
    async fn a_secret_in_a_history_pack_is_found_by_event_id_and_withheld() {
        use cairn_db::storage::events::history::HistorySource;
        use cairn_db::storage::quarantine::{quarantine, QuarantineSet, HISTORY_PACK_SINK};

        let (registry, secret) = registry_with("pack-key", VALUE);
        let snapshot = registry.snapshot();
        let db = cairn_db::storage::migrated_test_db("remediation-history-pack.db").await;
        let leaked = packed_leak(&db, "inv").await;

        let found = take_inventory_in(&db, &snapshot, &secret, &empty_roots())
            .await
            .unwrap();
        let by_sink = |sink: SinkKind| -> Vec<(String, usize)> {
            found
                .records
                .iter()
                .filter(|record| record.sink == sink)
                .map(|record| (record.locator.clone(), record.occurrences))
                .collect()
        };
        assert_eq!(by_sink(SinkKind::HistoryPack), [(leaked.clone(), 1)]);
        assert_eq!(by_sink(SinkKind::TranscriptEvent), [(leaked.clone(), 1)]);

        quarantine().install_for(
            db.path(),
            QuarantineSet::from_entries([(
                HISTORY_PACK_SINK.to_string(),
                leaked.clone(),
                "incident-pack".to_string(),
            )]),
        );
        db.set_history_source_override(Some(HistorySource::Pack));
        let served = crate::resources::render_job_chat_raw_json(&db, "job-1").await;
        db.set_history_source_override(None);
        quarantine().install_for(db.path(), QuarantineSet::from_entries(Vec::new()));
        assert!(!served.contains(VALUE), "withheld on a pack read");
        assert!(served.contains("incident-pack"), "{served}");
    }

    #[tokio::test]
    async fn releasing_a_quarantine_drops_the_unevicted_pack_for_a_repack() {
        use crate::security::remediation::store::{open_incident, DiscoveredVia};
        use cairn_db::storage::history_pack::catalog::tier_entry;
        use cairn_db::storage::quarantine::{quarantine_record, HISTORY_PACK_SINK};

        let db = cairn_db::storage::migrated_test_db("remediation-pack-release.db").await;
        let leaked = packed_leak(&db, "rel").await;
        let incident = open_incident(
            &db,
            "pack-key",
            None,
            DiscoveredVia::Declared,
            None,
            None,
            1,
        )
        .await
        .unwrap();

        quarantine_record(&db, HISTORY_PACK_SINK, &leaked, &incident, 1)
            .await
            .unwrap();
        assert!(
            super::super::release(&db, &incident, SinkKind::HistoryPack, &leaked, "operator")
                .await
                .unwrap()
        );
        assert_eq!(
            tier_entry(&db, "exec-1").await.unwrap(),
            None,
            "the pack was made while the event was withheld, so it must be rebuilt"
        );
        for table in ["history_tier_runs", "history_tier_sessions"] {
            assert_eq!(
                db.query_opt_i64(
                    format!("SELECT COUNT(*) FROM {table} WHERE scope_id = 'exec-1'"),
                    ()
                )
                .await
                .unwrap(),
                Some(0),
                "{table}: the dropped scope's mappings go with its row"
            );
        }

        // An evicted execution's catalog is never dropped: its hot row is gone.
        cairn_db::storage::history_pack::testing::publish_for_test(&db, "exec-1").await;
        db.execute("UPDATE history_tier SET evicted_at = 5", ())
            .await
            .unwrap();
        quarantine_record(&db, HISTORY_PACK_SINK, &leaked, &incident, 2)
            .await
            .unwrap();
        assert!(
            super::super::release(&db, &incident, SinkKind::HistoryPack, &leaked, "operator")
                .await
                .unwrap()
        );
        assert!(tier_entry(&db, "exec-1").await.unwrap().is_some());
    }

    #[test]
    fn a_file_scan_finds_a_credential_split_across_a_read_chunk() {
        // A gigabyte log is read in chunks, and a credential that straddles a
        // chunk boundary is the case a naive chunked search misses. The streaming
        // scrubber carries the boundary; this pins that we get that for free.
        let (registry, secret) = registry_with("provider-key", VALUE);
        let snapshot = registry.snapshot();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cairn-app.2026-01-01.jsonl");

        // Land the credential so it spans the boundary of the first read.
        let padding = "x".repeat(FILE_SCAN_CHUNK - (VALUE.len() / 2));
        std::fs::write(&path, format!("{padding}{VALUE} trailing")).unwrap();

        let record = scan_file(&path, SinkKind::ProcessLog, &snapshot, &secret)
            .expect("a credential spanning a chunk boundary is still found");
        assert_eq!(record.occurrences, 1);
    }
}
