//! History packs: one execution's transcript history as a single
//! content-addressed object in the archive's file store.
//!
//! A pack is what lets an execution's rows leave `cairn.db` (cairn/5545): it is
//! written while the hot rows are still authoritative, verified against them,
//! and catalogued in `history_tier`, and from then on the read seam
//! ([`crate::storage::events::history`]) can answer every transcript read from
//! it instead. Writing one never changes a hot row, and nothing reads a pack
//! in production until an eviction sets `history_tier.evicted_at`.
//!
//! # Format v1
//!
//! ```text
//! magic "CAIRNHP\x01"            8 bytes
//! frame*                        independent zstd frames, each JSONL rows
//! manifest frame                zstd JSON: Manifest
//! index frame                   zstd JSON: [IndexEntry]
//! footer                        40 bytes: manifest_off u64, manifest_len u64,
//!                                         index_off u64, index_len u64 (all LE),
//!                                         magic "CAIRNHPE"
//! ```
//!
//! - A **frame** holds at most [`FRAME_ROWS`] rows or [`FRAME_RAW_BYTES`] of raw
//!   JSONL, so one frame is one read batch, and never spans two keys. The key
//!   is the run id for `events`, `event_vibes`, `event_read_tokens` and
//!   `tool_invocations`, and the job id for `turn_embeddings`. A reader
//!   decompresses only the frames of the key it wants. The byte limit includes
//!   JSON encoding, `rowid`, and the trailing newline. A single row exceeding
//!   it is rejected with [`PackError::Corrupt`], identifying its section, key,
//!   rowid and encoded size; no row is truncated.
//! - Frames are grouped by section in [`Section::ALL`] order, then by key in
//!   the order the writer met them (the manifest's run order), and rows within
//!   a key are in the source table's rowid order, except `event_vibes`,
//!   `event_read_tokens` and `turn_embeddings`, which follow the order of the
//!   events or turns they belong to.
//! - Every row is a JSON object of the row's columns encoded by
//!   [`encode::json_value`], plus `rowid`. An `events` row is exactly the
//!   project export's event row ([`encode::finish_event_object`]): `data` is the
//!   reconstructed text, `data_blob` is null, `reconstruction` labels the
//!   outcome, and an unresolved row carries `unresolved` and its `stored` form.
//!   A row the disclosure quarantine withheld at pack time is written as its
//!   withholding notice with no stored form, so a quarantined credential is
//!   never copied into the archive.
//! - The **object key** is the sha256 of the whole object. The manifest
//!   carries no timestamp (`packed_at` lives only in the catalog), the zstd
//!   level and the column order are fixed, so the same history always packs to
//!   the same bytes and a re-pack is idempotent.
//! - The **manifest** names the scope, the execution's jobs, runs (with each
//!   run's sessions, turns, event count and highest event rowid), sessions and
//!   turns, so a turn or session read resolves to runs inside the pack, and
//!   every candidate table's section: packed (with its row count, the sha256
//!   of its concatenated raw JSONL, and its columns' declared types, so a blob
//!   column decodes back from base64) or not packed, with the reason.
//!
//! A pack decodes from its bytes alone: nothing here opens a database
//! ([`HistoryPack::open`], [`HistoryPack::verify`], [`HistoryPack::run_events`]).
//!
//! # Token rollup
//!
//! The `events` section carries every live column, the token columns,
//! `cost_usd`, the model columns, `reported_model` and `account_id` among them, frozen as of
//! pack time. The `token_rollup` fold reads those columns from hot rows, so once
//! an execution is evicted its runs cannot be re-folded: before C2 evicts, it
//! must mark tiered runs as not re-foldable and keep their rollups, or a
//! re-fold shrinks them or drops them to `unknown`.

pub mod budget;
pub mod catalog;
pub mod encode;
#[cfg(any(test, feature = "test-utils"))]
pub mod testing;
#[cfg(test)]
mod tests;
pub mod write;

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};
use sha2::{Digest, Sha256};

use crate::models::Event;
use crate::storage::events::history::{EventExtras, HistoryRow};
use crate::storage::events::{
    decompress, CODEC_ZSTD_V1, RECONSTRUCT_BATCH_BYTES, RECONSTRUCT_BATCH_ROWS,
};

/// The bytes every pack starts with: the name and format version 1.
pub const MAGIC: &[u8; 8] = b"CAIRNHP\x01";
/// The bytes every pack ends with.
pub const FOOTER_MAGIC: &[u8; 8] = b"CAIRNHPE";
/// The format this module writes and the only one it reads.
pub const FORMAT: u32 = 1;
/// The fixed footer: four little-endian `u64`s and [`FOOTER_MAGIC`].
pub const FOOTER_LEN: usize = 40;
/// Rows per frame at most: one reconstruct batch.
pub const FRAME_ROWS: usize = RECONSTRUCT_BATCH_ROWS;
/// Hard maximum raw JSONL bytes per frame: one reconstruct batch's byte bound.
/// Rows whose encoded JSONL exceeds this are rejected, never truncated.
pub const FRAME_RAW_BYTES: usize = RECONSTRUCT_BATCH_BYTES;

/// The note every manifest carries on what eviction costs the token rollup.
pub const TOKEN_ROLLUP_NOTE: &str = "the events section carries every token column, cost_usd, the model columns, reported_model and account_id as of pack time; once evicted these runs cannot be re-folded, so their token_rollup rows must be kept";

/// A packed table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Section {
    Events,
    EventVibes,
    EventReadTokens,
    ToolInvocations,
    TurnEmbeddings,
}

impl Section {
    /// Every packed section, in the order a pack's frames are grouped.
    pub const ALL: [Section; 5] = [
        Section::Events,
        Section::EventVibes,
        Section::EventReadTokens,
        Section::ToolInvocations,
        Section::TurnEmbeddings,
    ];

    /// The table the section's rows come from.
    pub fn table(self) -> &'static str {
        match self {
            Section::Events => "events",
            Section::EventVibes => "event_vibes",
            Section::EventReadTokens => "event_read_tokens",
            Section::ToolInvocations => "tool_invocations",
            Section::TurnEmbeddings => "turn_embeddings",
        }
    }

    /// Why the section is packed, as the manifest states it.
    pub fn reason(self) -> &'static str {
        match self {
            Section::Events => "the transcript itself, reconstructed",
            Section::EventVibes => {
                "colours are not reproducible byte for byte from a fresh embedding"
            }
            Section::EventReadTokens => "the app transcript window's lean projection reads it",
            Section::ToolInvocations => "analytics rows extracted from the events",
            Section::TurnEmbeddings => "per-turn search vectors; recomputing them is a paid call",
        }
    }
}

/// Candidate tables a pack deliberately leaves out, and why. Stated in every
/// manifest so a reader of the pack knows they were considered.
pub const NOT_PACKED: [(&str, &str); 3] = [
    ("session_skyline_cache", "a rebuildable cache"),
    (
        "message_stream_chunks",
        "cairn/5585 evicts a terminal stream's chunks on its own",
    ),
    (
        "execution_history",
        "the git range pack is already its own store object, through execution_history.pack_hash",
    ),
];

/// Where one frame sits in the pack and what it holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    pub section: Section,
    pub key: String,
    pub first_rowid: i64,
    pub rows: u32,
    pub offset: u64,
    pub len: u64,
    pub raw_len: u64,
}

/// What a pack covers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    /// `execution` in format 1; cairn/5587 adds a thread-range kind.
    pub kind: String,
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestJob {
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRun {
    pub id: String,
    pub job_id: Option<String>,
    /// Every session an event of the run names, and the run's own.
    pub session_ids: Vec<String>,
    /// Every turn an event of the run names.
    pub turn_ids: Vec<String>,
    pub event_rows: u64,
    pub max_rowid: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestSession {
    pub id: String,
    pub job_id: Option<String>,
    pub event_rows: u64,
    pub max_rowid: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestTurn {
    pub id: String,
    pub job_id: Option<String>,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
    pub sequence: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDecl {
    pub name: String,
    pub decl_type: String,
}

/// One candidate table's place in the pack.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SectionEntry {
    pub table: String,
    pub packed: bool,
    pub reason: String,
    /// For a packed section: its row count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    /// For a packed section: the sha256 of its frames' raw JSONL, concatenated
    /// in index order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// For a packed section: its columns, in row order, with declared types.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<ColumnDecl>>,
}

/// Everything a pack says about itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    /// The newest migration applied to the database the rows came from.
    pub migration_high_water: Option<String>,
    pub scope: Scope,
    pub project_id: Option<String>,
    pub issue_id: Option<String>,
    pub execution_id: String,
    pub jobs: Vec<ManifestJob>,
    pub runs: Vec<ManifestRun>,
    pub sessions: Vec<ManifestSession>,
    pub turns: Vec<ManifestTurn>,
    pub sections: Vec<SectionEntry>,
    pub token_rollup: String,
}

impl Manifest {
    /// A packed section's entry.
    pub fn section(&self, section: Section) -> Option<&SectionEntry> {
        self.sections
            .iter()
            .find(|entry| entry.packed && entry.table == section.table())
    }

    /// The runs whose events name `session_id`, in pack order.
    pub fn runs_of_session<'a>(
        &'a self,
        session_id: &'a str,
    ) -> impl Iterator<Item = &'a ManifestRun> {
        self.runs
            .iter()
            .filter(move |run| run.session_ids.iter().any(|id| id == session_id))
    }

    /// The runs whose events name `turn_id`, in pack order.
    pub fn runs_of_turn<'a>(&'a self, turn_id: &'a str) -> impl Iterator<Item = &'a ManifestRun> {
        self.runs
            .iter()
            .filter(move |run| run.turn_ids.iter().any(|id| id == turn_id))
    }
}

/// Why a pack could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PackError {
    #[error("history pack truncated")]
    Truncated,
    #[error("not a history pack (bad magic)")]
    BadMagic,
    #[error("unsupported history pack format {0}")]
    UnsupportedFormat(u32),
    #[error("history pack corrupt: {0}")]
    Corrupt(String),
    /// The archive the pack lives in is not there.
    #[error("archive offline: {}", path.display())]
    Offline { path: PathBuf },
}

impl From<crate::storage::ContentStoreError> for PackError {
    fn from(error: crate::storage::ContentStoreError) -> Self {
        match error {
            crate::storage::ContentStoreError::Offline { path } => PackError::Offline { path },
            other => PackError::Corrupt(other.to_string()),
        }
    }
}

fn corrupt(what: impl std::fmt::Display) -> PackError {
    PackError::Corrupt(what.to_string())
}

/// One opened pack.
pub struct HistoryPack {
    bytes: Vec<u8>,
    manifest: Manifest,
    index: Vec<IndexEntry>,
    /// `(section, key)` -> positions in `index`, in index order.
    frames_of: HashMap<(Section, String), Vec<usize>>,
}

impl std::fmt::Debug for HistoryPack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryPack")
            .field("bytes", &self.bytes.len())
            .field("execution_id", &self.manifest.execution_id)
            .field("frames", &self.index.len())
            .finish()
    }
}

impl HistoryPack {
    /// Validate the magic, the footer, the manifest and the index. Frames are
    /// decompressed only when read; [`Self::verify`] reads them all.
    pub fn open(bytes: Vec<u8>) -> Result<HistoryPack, PackError> {
        if bytes.len() < MAGIC.len() + FOOTER_LEN {
            return Err(PackError::Truncated);
        }
        if &bytes[..MAGIC.len()] != MAGIC {
            return match bytes[..MAGIC.len() - 1] == MAGIC[..MAGIC.len() - 1] {
                true => Err(PackError::UnsupportedFormat(u32::from(
                    bytes[MAGIC.len() - 1],
                ))),
                false => Err(PackError::BadMagic),
            };
        }
        let footer = &bytes[bytes.len() - FOOTER_LEN..];
        if &footer[32..] != FOOTER_MAGIC {
            return Err(PackError::Truncated);
        }
        let word = |at: usize| u64::from_le_bytes(footer[at..at + 8].try_into().unwrap());
        let (manifest_off, manifest_len) = (word(0), word(8));
        let (index_off, index_len) = (word(16), word(24));
        let body_end = (bytes.len() - FOOTER_LEN) as u64;
        let manifest_raw = frame_slice(&bytes, manifest_off, manifest_len, body_end)?;
        let index_raw = frame_slice(&bytes, index_off, index_len, body_end)?;
        let manifest: Manifest = serde_json::from_slice(&inflate(manifest_raw)?)
            .map_err(|error| corrupt(format!("manifest: {error}")))?;
        if manifest.format != FORMAT {
            return Err(PackError::UnsupportedFormat(manifest.format));
        }
        let index: Vec<IndexEntry> = serde_json::from_slice(&inflate(index_raw)?)
            .map_err(|error| corrupt(format!("index: {error}")))?;
        let frames_end = manifest_off.min(index_off);
        let mut frames_of: HashMap<(Section, String), Vec<usize>> = HashMap::new();
        for (position, entry) in index.iter().enumerate() {
            frame_slice(&bytes, entry.offset, entry.len, frames_end)?;
            frames_of
                .entry((entry.section, entry.key.clone()))
                .or_default()
                .push(position);
        }
        Ok(HistoryPack {
            bytes,
            manifest,
            index,
            frames_of,
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn index(&self) -> &[IndexEntry] {
        &self.index
    }

    /// The pack's size in bytes.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The raw JSONL of one frame.
    fn frame_raw(&self, entry: &IndexEntry) -> Result<Vec<u8>, PackError> {
        let slice = frame_slice(
            &self.bytes,
            entry.offset,
            entry.len,
            self.bytes.len() as u64,
        )?;
        let raw = inflate(slice)?;
        if raw.len() as u64 != entry.raw_len {
            return Err(corrupt(format!(
                "{:?} frame of {} inflates to {} bytes, index says {}",
                entry.section,
                entry.key,
                raw.len(),
                entry.raw_len
            )));
        }
        Ok(raw)
    }

    /// Every row of `section` under `key`, in pack order, decompressing one
    /// frame at a time.
    pub fn rows<'a>(
        &'a self,
        section: Section,
        key: &str,
    ) -> impl Iterator<Item = Result<Map<String, Json>, PackError>> + 'a {
        let positions = self
            .frames_of
            .get(&(section, key.to_string()))
            .cloned()
            .unwrap_or_default();
        positions.into_iter().flat_map(move |position| {
            let rows: Vec<Result<Map<String, Json>, PackError>> =
                match self.frame_raw(&self.index[position]) {
                    Ok(raw) => split_rows(&raw).collect(),
                    Err(error) => vec![Err(error)],
                };
            rows
        })
    }

    /// [`Self::rows`], skipping every frame whose rows all sit at or below
    /// `after_rowid`: a frame ends before the next one's `first_rowid`, so it
    /// can be skipped without inflating it. Rows at or below `after_rowid` in
    /// the first frame kept are the caller's to drop. Only meaningful for a
    /// section in rowid order within a key.
    pub fn rows_after<'a>(
        &'a self,
        section: Section,
        key: &str,
        after_rowid: i64,
    ) -> impl Iterator<Item = Result<Map<String, Json>, PackError>> + 'a {
        let positions = self
            .frames_of
            .get(&(section, key.to_string()))
            .cloned()
            .unwrap_or_default();
        let kept: Vec<usize> = positions
            .iter()
            .enumerate()
            .filter(|(at, _)| {
                positions
                    .get(at + 1)
                    .is_none_or(|next| self.index[*next].first_rowid > after_rowid)
            })
            .map(|(_, position)| *position)
            .collect();
        kept.into_iter().flat_map(move |position| {
            let rows: Vec<Result<Map<String, Json>, PackError>> =
                match self.frame_raw(&self.index[position]) {
                    Ok(raw) => split_rows(&raw).collect(),
                    Err(error) => vec![Err(error)],
                };
            rows
        })
    }

    /// One run's events, decoded, in rowid order.
    pub fn run_events(&self, run_id: &str) -> Result<Vec<HistoryRow>, PackError> {
        self.rows(Section::Events, run_id)
            .map(|row| decode_event_row(&row?))
            .collect()
    }

    /// Check every packed section against the manifest: each frame inflates to
    /// the length and row count its index entry states, and each section's row
    /// count and raw sha256 match.
    pub fn verify(&self) -> Result<(), PackError> {
        let mut seen: BTreeMap<Section, (u64, Sha256)> = BTreeMap::new();
        for entry in &self.index {
            let raw = self.frame_raw(entry)?;
            let rows = raw.iter().filter(|byte| **byte == b'\n').count() as u64;
            if rows != u64::from(entry.rows) {
                return Err(corrupt(format!(
                    "{:?} frame of {} holds {rows} rows, index says {}",
                    entry.section, entry.key, entry.rows
                )));
            }
            let (count, hasher) = seen
                .entry(entry.section)
                .or_insert_with(|| (0, Sha256::new()));
            *count += rows;
            hasher.update(&raw);
        }
        for section in Section::ALL {
            let Some(stated) = self.manifest.section(section) else {
                if seen.contains_key(&section) {
                    return Err(corrupt(format!(
                        "{section:?} frames with no manifest section"
                    )));
                }
                continue;
            };
            let (rows, sha256) = match seen.remove(&section) {
                Some((rows, hasher)) => (rows, hex(&hasher.finalize())),
                None => (0, hex(&Sha256::digest(b""))),
            };
            if stated.rows != Some(rows) {
                return Err(corrupt(format!(
                    "{section:?} holds {rows} rows, manifest says {:?}",
                    stated.rows
                )));
            }
            if stated.sha256.as_deref() != Some(sha256.as_str()) {
                return Err(corrupt(format!(
                    "{section:?} sha256 does not match the manifest"
                )));
            }
        }
        Ok(())
    }
}

/// The bytes `[offset, offset + len)`, which must end at or before `end`.
fn frame_slice(bytes: &[u8], offset: u64, len: u64, end: u64) -> Result<&[u8], PackError> {
    let stop = offset.checked_add(len).ok_or(PackError::Truncated)?;
    if (offset as usize) < MAGIC.len() || stop > end || stop > bytes.len() as u64 {
        return Err(PackError::Truncated);
    }
    Ok(&bytes[offset as usize..stop as usize])
}

fn inflate(frame: &[u8]) -> Result<Vec<u8>, PackError> {
    decompress(CODEC_ZSTD_V1, frame).map_err(corrupt)
}

fn split_rows(raw: &[u8]) -> impl Iterator<Item = Result<Map<String, Json>, PackError>> + '_ {
    raw.split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| match serde_json::from_slice::<Json>(line) {
            Ok(Json::Object(object)) => Ok(object),
            Ok(_) => Err(corrupt("a row that is not an object")),
            Err(error) => Err(corrupt(format!("row: {error}"))),
        })
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// An `events` section row back as the [`HistoryRow`] it was written from.
///
/// The row was reconstructed at pack time, so this does no reconstruction: the
/// event's archival columns are as stored, `data` is what reconstruction made
/// of it, and `data_blob` is null unless the row was unresolved, in which case
/// it is the stored blob again, exactly as a hot read leaves it.
pub fn decode_event_row(object: &Map<String, Json>) -> Result<HistoryRow, PackError> {
    let text = |name: &str| -> Result<String, PackError> {
        object
            .get(name)
            .and_then(Json::as_str)
            .map(str::to_string)
            .ok_or_else(|| corrupt(format!("event row without {name}")))
    };
    let opt_text = |name: &str| object.get(name).and_then(Json::as_str).map(str::to_string);
    let int = |name: &str| -> Result<i64, PackError> {
        object
            .get(name)
            .and_then(Json::as_i64)
            .ok_or_else(|| corrupt(format!("event row without {name}")))
    };
    let opt_int = |name: &str| object.get(name).and_then(Json::as_i64);
    let opt_i32 = |name: &str| opt_int(name).map(|value| value as i32);
    let reconstruction = object
        .get("reconstruction")
        .and_then(Json::as_str)
        .and_then(encode::outcome_from_label)
        .ok_or_else(|| corrupt("event row without a reconstruction label"))?;
    let data_blob = match reconstruction.is_unresolved() {
        true => object
            .get("stored")
            .and_then(|stored| stored.get("data_blob"))
            .and_then(|blob| match encode::stored_value(blob, "BLOB") {
                turso::Value::Blob(bytes) => Some(bytes),
                _ => None,
            }),
        false => None,
    };
    let event = Event {
        id: text("id")?,
        run_id: text("run_id")?,
        session_id: opt_text("session_id"),
        sequence: int("sequence")? as i32,
        timestamp: int("timestamp")?,
        event_type: text("event_type")?,
        data: text("data")?,
        parent_tool_use_id: opt_text("parent_tool_use_id"),
        created_at: int("created_at")?,
        input_tokens: opt_i32("input_tokens"),
        cache_read_tokens: opt_i32("cache_read_tokens"),
        cache_create_tokens: opt_i32("cache_create_tokens"),
        output_tokens: opt_i32("output_tokens"),
        turn_id: opt_text("turn_id"),
        thinking_tokens: opt_i32("thinking_tokens"),
        storage_mode: opt_text("storage_mode"),
        content_commit: opt_text("content_commit"),
        content_render_sha: opt_text("content_render_sha"),
        data_blob,
        codec: opt_text("codec"),
        content_change_id: opt_text("content_change_id"),
        cost_usd: object.get("cost_usd").and_then(Json::as_f64),
        read_segments: None,
    };
    Ok(HistoryRow {
        event,
        rowid: int("rowid")?,
        reconstruction,
        extras: EventExtras {
            reported_model: opt_text("reported_model"),
            change_preview_status: opt_text("change_preview_status"),
            change_applied_at: opt_int("change_applied_at"),
            account_id: opt_text("account_id"),
        },
    })
}
