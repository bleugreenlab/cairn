//! The one read seam for transcript history.
//!
//! Every transcript reader resolves its scope through hot skeleton tables (runs,
//! turns, sessions, jobs) and then asks here for the events. A [`HistoryReader`]
//! answers from wherever they live: the hot `events` table, or the execution's
//! history pack ([`crate::storage::history_pack`]) once eviction has removed the
//! hot rows. The caller cannot tell the two apart. Both give the same
//! [`HistoryRow`]s in the same order: a run's events are keyed by rowid, the
//! pack carries each row's rowid, and a hot row that reconstructed completely
//! has its `data_blob` cleared just as a pack row has none.
//!
//! # Source
//!
//! A run is pack-sourced only when `history_tier_runs` joined to `history_tier`
//! has `evicted_at` set. A turn or a session resolves the same way through its
//! job's execution: its events live in that execution (`turns.job_id` or
//! `sessions.job_id`, then `jobs.execution_id`). A team replica has no catalog
//! and reads hot. Tests and the clone harness force a source through
//! `LocalDb::set_history_source_override`, compiled only under `test-utils`.
//!
//! # The gate
//!
//! The disclosure quarantine runs last for both sources. A hot row goes through
//! [`reconstruct_events_reporting`]. A pack row was reconstructed at pack time,
//! so nothing regenerates it, and it is gated on either of its two keys: the
//! transcript event and its copy in the pack. A row quarantined after packing
//! is withheld on read like any other; a row stored as its notice re-gates to
//! itself.
//!
//! # Errors
//!
//! A pack behind a catalog row that cannot be fetched is a [`HistoryError`],
//! never a silent stub: `archive offline: <path>` names what to reconnect.
//!
//! # Readers not moved here
//!
//! Derived readers keep their own statements over the hot table. Each of these
//! needs a guard before C2 evicts, and this list is that checklist:
//!
//! | Reader | What it must do once rows can be evicted |
//! |---|---|
//! | embedder `load_turn_events` (`embeddings/turns.rs`) | skip turns of tiered runs; their vectors are in the pack |
//! | vibe recompute `run_steps`, `draw_sample` (`embeddings/vibe_recompute.rs`) | skip tiered runs; their colours are in the pack |
//! | search rebuild `load_event_documents` (`search_index.rs`) | enumerate `history_tier` and index from pack `events` frames |
//! | search `load_archived_event_document` (`search_index.rs`) | event-id lookup; the incremental path never sees evicted rows, since the trigger guard sits in cairn/5545 section 4 |
//! | analytics `cost_events`, the token fold (`cairn-analytics/src/queries.rs`) | tiered runs are not re-foldable; their rollups are kept |
//! | analytics `load_run_events` (`cairn-analytics/src/queries.rs`) | reconcile tool invocations from packed events, or skip tiered runs while retaining their completed reconciliation and watermarks |
//! | attention catch-up `newest_assistant_prose` (`cairn-attention/src/delivery.rs`) | read the bounded turn window from packs too; missing hot prose must not silently drop its catch-up excerpt |
//! | remediation `scan_transcript_events` | hot only; packs get their own scanner |
//! | Home's indexed candidates (`resources/home.rs`) | enumerate packed prose before tiering thread ranges; body hydration already uses the seam |
//! | legacy route facts without a job id (`routes/log.rs`) | preserve a durable run locator before evicting the hot row their metadata lookup needs |
//! | search hit locations (`search.rs`) | resolve event coordinates from packs as well as hot events |
//! | skyline/vibe scope and session reads (`embeddings/queries.rs`) | merge packed vibes and resolve ownership without an inner join requiring hot events |
//! | node activity (`resources/node.rs`) | retain tier-aware counts and last-activity metadata; assistant preview is intentionally hot-only |
//! | thread unread/read watermarks (`jobs/queries.rs`) | retain unread counts and cursor semantics across tiered ranges |
//! | child activity, memory thresholds and empty-digest diagnostics | use tier-aware metadata instead of treating absent hot events as no history |
//! | cold-resume/provider-cache clocks (`execution/jobs/persistence.rs`) | retain last event and provider-request metadata for tiered sessions |
//! | remediation derived rebuild (`security/remediation/rebuild.rs`) | resolve affected turns from packed events |
//! | channel follows (`channels/router.rs`) | make delayed catch-up history-aware before advancing cursors past evicted events; live-edge metadata must include tiered history |
//! | project migrate/export membership | enumerate pack objects and archived rows; never copy host-local catalog pointers unchanged |
//!
//! Live transaction readers remain on their owning connections: sequence
//! allocation, tool correlation, prompt/resume deduplication, change-preview
//! application, stop reconciliation and stream finalization. Before eviction,
//! exclude runs still referenced by unresolved delivery/recovery work (durable
//! suspends and delegation follow-ups included), and preserve idempotent
//! finalized-stream retries either through the archive or an enforced live-only
//! horizon. They cannot silently treat an evicted record as never recorded.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value as Json;
use turso::Value;

use super::columns::{event_from_row, event_from_row_at, EVENT_COLUMNS, EVENT_COLUMN_COUNT};
use super::reconstruct::{reconstruct_events_reporting, Reconstruction, RECONSTRUCT_BATCH_ROWS};
use crate::models::Event;
use crate::storage::history_pack::{decode_event_row, HistoryPack, PackError, Section};
use crate::storage::quarantine::withhold_pack_rows_reporting;
use crate::storage::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};

/// The live `events` columns beyond [`EVENT_COLUMNS`] that a reader consumes.
///
/// A pack carries every live column; a column added here is read from both
/// sources, and one added to the table but not here still travels in the pack
/// for whoever needs it later.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventExtras {
    pub reported_model: Option<String>,
    pub selected_model: Option<String>,
    pub change_preview_status: Option<String>,
    pub change_applied_at: Option<i64>,
    pub account_id: Option<String>,
}

const EXTRA_COLUMNS: &str =
    "reported_model, change_preview_status, change_applied_at, account_id, selected_model";
const EXTRA_COLUMN_COUNT: usize = 5;

/// One event as a reader sees it.
#[derive(Debug, Clone)]
pub struct HistoryRow {
    pub event: Event,
    pub rowid: i64,
    pub reconstruction: Reconstruction,
    pub extras: EventExtras,
}

/// Where a page's rows came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Provenance {
    Hot,
    Pack {
        execution_id: String,
        content_hash: String,
    },
}

/// A forced source, for tests and the clone harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySource {
    /// Read hot whatever the catalog says.
    Hot,
    /// Read from the pack whenever a catalog row names one, evicted or not.
    Pack,
}

/// One read's rows.
#[derive(Debug, Clone)]
pub struct Page {
    pub rows: Vec<HistoryRow>,
    pub provenance: Provenance,
    /// Rows the source yielded, including any that failed to parse, so a page
    /// shorter than its limit still means the scope is exhausted.
    pub fetched: usize,
    /// The highest rowid yielded, including any row that failed to parse: the
    /// keyset to resume after. `None` on an empty page.
    pub last_rowid: Option<i64>,
    /// Maximum raw (created_at, rowid) key, including rows that failed to decode.
    pub last_chat_key: Option<(i64, i64)>,
}

impl Page {
    fn hot(rows: Vec<HistoryRow>, fetched: usize, last_rowid: Option<i64>) -> Page {
        let last_chat_key = rows
            .iter()
            .map(|row| (row.event.created_at, row.rowid))
            .max();
        Page {
            rows,
            provenance: Provenance::Hot,
            fetched,
            last_rowid,
            last_chat_key,
        }
    }

    fn sorted_for_chat(mut self) -> Page {
        sort_chat_order(&mut self.rows);
        self
    }

    /// The events, without their rowids and labels.
    pub fn into_events(self) -> Vec<Event> {
        self.rows.into_iter().map(|row| row.event).collect()
    }
}

/// `(created_at, rowid)`: the order a job's or a session's chat reads in.
/// `created_at` separates events across seconds, and `rowid` (insertion order)
/// is the stable tiebreaker within one.
pub fn sort_chat_order(rows: &mut [HistoryRow]) {
    rows.sort_by_key(|row| (row.event.created_at, row.rowid));
}

/// A body a reader never renders, projected away.
///
/// A transcript list reads none of an unrendered event's body, and those are
/// the heaviest rows a transcript holds. Hot, the `CASE` here keeps the engine
/// from reading their pages at all; `withhold` then writes what the row carries
/// instead, before reconstruction, so an archived body is never decompressed.
/// From a pack the same `withhold` runs on the decoded row, so both sources
/// return the same event.
pub struct BodyProjection {
    select_columns: String,
    is_unread: fn(&str) -> bool,
    withhold: fn(&mut Event),
}

impl BodyProjection {
    /// `unread_sql` is `is_unread` as a SQL predicate over `event_type`.
    pub fn new(unread_sql: &str, is_unread: fn(&str) -> bool, withhold: fn(&mut Event)) -> Self {
        let select_columns = EVENT_COLUMNS
            .split(',')
            .map(str::trim)
            .map(|column| match column {
                "data" => format!("CASE WHEN {unread_sql} THEN '' ELSE data END"),
                "data_blob" => format!("CASE WHEN {unread_sql} THEN NULL ELSE data_blob END"),
                other => other.to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        BodyProjection {
            select_columns,
            is_unread,
            withhold,
        }
    }
}

/// Which of a scope's events a read wants.
#[derive(Clone, Copy, Default)]
pub struct EventFilter<'a> {
    /// Only these event types: applied in SQL hot, in memory from a pack.
    pub event_types: Option<&'a [&'a str]>,
    /// Bodies the reader never renders.
    pub bodies: Option<&'a BodyProjection>,
}

impl<'a> EventFilter<'a> {
    pub fn types(event_types: &'a [&'a str]) -> Self {
        EventFilter {
            event_types: Some(event_types),
            bodies: None,
        }
    }

    pub fn projecting(bodies: &'a BodyProjection) -> Self {
        EventFilter {
            event_types: None,
            bodies: Some(bodies),
        }
    }

    fn wants(&self, event_type: &str) -> bool {
        self.event_types
            .is_none_or(|types| types.contains(&event_type))
    }

    pub fn columns(&self) -> String {
        let base = self
            .bodies
            .map_or(EVENT_COLUMNS, |bodies| bodies.select_columns.as_str());
        format!("{base}, {EXTRA_COLUMNS}, rowid")
    }

    /// `AND +event_type IN (...)`, numbered from `first`, and its binds.
    ///
    /// The unary `+` keeps `event_type` out of index selection. Without it Turso
    /// seeks `idx_events_type_run` on the type list and sorts, which walks every
    /// event of those types in the database rather than the one run or session
    /// asked for.
    fn type_clause(&self, first: usize) -> (String, Vec<Value>) {
        let Some(types) = self.event_types else {
            return (String::new(), Vec::new());
        };
        let placeholders = (0..types.len())
            .map(|offset| format!("?{}", first + offset))
            .collect::<Vec<_>>()
            .join(", ");
        (
            format!(" AND +event_type IN ({placeholders})"),
            types.iter().map(|t| Value::Text(t.to_string())).collect(),
        )
    }
}

/// Why a history read failed.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error(transparent)]
    Db(#[from] DbError),
    /// The archive holding the pack is not there.
    #[error("archive offline: {}", path.display())]
    Offline { path: PathBuf },
    /// The catalog names a pack the store does not hold.
    #[error("history pack {content_hash} is missing from the archive")]
    Missing { content_hash: String },
    /// The catalog names a pack but this database has no store to fetch it from.
    #[error("history pack {content_hash} cannot be fetched: no archive store")]
    NoStore { content_hash: String },
    #[error(transparent)]
    Pack(PackError),
}

impl From<PackError> for HistoryError {
    fn from(error: PackError) -> Self {
        match error {
            PackError::Offline { path } => HistoryError::Offline { path },
            other => HistoryError::Pack(other),
        }
    }
}

impl From<HistoryError> for DbError {
    fn from(error: HistoryError) -> Self {
        match error {
            HistoryError::Db(error) => error,
            other => DbError::internal(other.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Source {
    Hot,
    Pack {
        execution_id: String,
        content_hash: String,
    },
}

/// Identity and ordering only, without fetching or reconstructing a body.
#[derive(Debug, Clone)]
pub struct HistoryKey {
    pub id: String,
    pub run_id: String,
    pub sequence: i64,
    pub rowid: i64,
}

fn history_key_from_row(row: &turso::Row) -> DbResult<HistoryKey> {
    Ok(HistoryKey {
        id: row.text(0)?,
        run_id: row.text(1)?,
        sequence: row.i64(2)?,
        rowid: row.i64(3)?,
    })
}

/// Where [`HistoryReader::locate_event`] found an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLocation {
    pub run_id: String,
    pub sequence: i64,
}

/// The event rows a read segments lookup found.
#[derive(Debug, Default)]
pub struct ReadSegments {
    /// event id -> its `event_read_tokens.segments_json`.
    pub json: HashMap<String, String>,
    /// The asked-for runs answered from a pack. Their cache rows are frozen: a
    /// miss there is not a row to backfill.
    pub pack_runs: HashSet<String>,
}

/// The statements the seam runs against the hot table. Named so their plans can
/// be pinned.
pub mod statements {
    use super::EventFilter;

    /// One keyset page of a run, in rowid order, on `idx_events_run_id`.
    pub fn run_page(filter: &EventFilter<'_>) -> String {
        let (types, _) = filter.type_clause(4);
        format!(
            "SELECT {} FROM events WHERE run_id = ?1 AND rowid > ?2{types} ORDER BY rowid ASC LIMIT ?3",
            filter.columns()
        )
    }

    /// A bounded chronological page for first-match scans.
    pub fn run_chat_page(filter: &EventFilter<'_>) -> String {
        let (types, _) = filter.type_clause(5);
        format!(
            "SELECT {} FROM events WHERE run_id = ?1 AND (created_at > ?2 OR (created_at = ?2 AND rowid > ?3)){types} ORDER BY created_at ASC, rowid ASC LIMIT ?4",
            filter.columns()
        )
    }

    /// A run's events below a sequence, newest first, on
    /// `idx_events_run_sequence` with no sort.
    pub fn run_window(filter: &EventFilter<'_>) -> String {
        let (types, _) = filter.type_clause(5);
        format!(
            "SELECT {} FROM events WHERE run_id = ?1 AND sequence < ?2 AND sequence >= ?3{types} ORDER BY sequence DESC LIMIT ?4",
            filter.columns()
        )
    }

    /// One event by its per-run ordinal.
    pub fn run_sequence() -> String {
        format!(
            "SELECT {} FROM events WHERE run_id = ?1 AND sequence = ?2 LIMIT 1",
            EventFilter::default().columns()
        )
    }

    /// One exact rowid seek, with a run guard that cannot redirect the plan.
    pub fn run_rowid() -> String {
        format!(
            "SELECT {} FROM events WHERE rowid = ?2 AND +run_id = ?1",
            EventFilter::default().columns()
        )
    }

    /// A signed sequence-ordered slice on the run/sequence index. Without
    /// the explicit index Turso chooses the run-id index and sorts full bodies.
    pub fn run_slice() -> String {
        format!(
            "SELECT {} FROM events INDEXED BY idx_events_run_sequence WHERE run_id = ?1 ORDER BY sequence ASC LIMIT ?2 OFFSET ?3",
            EventFilter::default().columns()
        )
    }

    /// Select only keys through the global lineage ordering and count offset.
    /// The outer rowid join fetches bodies after that selection, never into the
    /// metadata sorter. Its selected metadata keys let Rust restore final order.
    pub fn sessions_after_count(session_count: usize) -> String {
        let sessions = (1..=session_count)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let session_order = (1..=session_count)
            .map(|i| format!("WHEN ?{i} THEN {}", i - 1))
            .collect::<Vec<_>>()
            .join(" ");
        let offset = session_count + 1;
        format!(
            "SELECT {}, pick.run_unknown, pick.run_created_at, pick.run_rowid,
                    pick.sequence_key, pick.session_position
             FROM (
                 SELECT e.rowid AS event_rowid, CASE WHEN r.id IS NULL THEN 1 ELSE 0 END AS run_unknown,
                        r.created_at AS run_created_at, r.rowid AS run_rowid, e.sequence AS sequence_key,
                        CASE e.session_id {session_order} ELSE {session_count} END AS session_position
                 FROM events e
                 LEFT JOIN runs r ON r.id = e.run_id AND r.session_id IN ({sessions})
                 WHERE e.session_id IN ({sessions})
                 ORDER BY run_unknown ASC, run_created_at ASC, run_rowid ASC,
                          sequence_key ASC, session_position ASC, event_rowid ASC
                 LIMIT -1 OFFSET ?{offset}
             ) pick
             CROSS JOIN events e ON e.rowid = pick.event_rowid",
            super::HotRow::columns(Some("e"))
        )
    }
    /// A session's metadata, paged without touching data or data_blob.
    pub const SESSION_KEYS: &str = "SELECT id, run_id, sequence, rowid FROM events
         WHERE session_id = ?1 AND rowid > ?2 ORDER BY rowid ASC LIMIT ?3";

    /// A run's events named by `count` ids, bound from `?2`, on the primary key.
    pub fn run_ids(count: usize) -> String {
        let placeholders = (0..count)
            .map(|offset| format!("?{}", offset + 2))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT {} FROM events WHERE id IN ({placeholders}) AND +run_id = ?1",
            EventFilter::default().columns()
        )
    }

    /// One keyset page of a turn, in rowid order, on `idx_events_turn_id`.
    pub fn turn_page() -> String {
        format!(
            "SELECT {} FROM events WHERE turn_id = ?1 AND rowid > ?2 ORDER BY rowid ASC LIMIT ?3",
            EventFilter::default().columns()
        )
    }

    /// One keyset page of a session, in rowid order, on
    /// `idx_events_session_id`. `except_run` binds `?4` when present.
    pub fn session_page(filter: &EventFilter<'_>, except_run: bool) -> String {
        let first_type = if except_run { 5 } else { 4 };
        let (types, _) = filter.type_clause(first_type);
        let except = if except_run { " AND run_id != ?4" } else { "" };
        format!(
            "SELECT {} FROM events WHERE session_id = ?1 AND rowid > ?2{except}{types} ORDER BY rowid ASC LIMIT ?3",
            filter.columns()
        )
    }

    /// A session's newest events.
    pub fn session_tail() -> String {
        format!(
            "SELECT {} FROM events WHERE session_id = ?1 ORDER BY rowid DESC LIMIT ?2",
            EventFilter::default().columns()
        )
    }

    /// An event's run and sequence, by primary key.
    pub const LOCATE_EVENT: &str = "SELECT run_id, sequence FROM events WHERE id = ?1";
    /// The first sequence of a turn in a run.
    pub const TURN_START: &str =
        "SELECT MIN(sequence) FROM events WHERE turn_id = ?1 AND run_id = ?2";
    /// A session's newest event rowid.
    pub const SESSION_MAX_ROWID: &str = "SELECT MAX(rowid) FROM events WHERE session_id = ?1";
    /// Resolve a batch of runs by their primary keys, ignoring orphan mappings.
    pub fn run_sources(count: usize) -> String {
        let placeholders = vec!["?"; count].join(", ");
        format!(
            "SELECT r.run_id, h.execution_id, h.content_hash, h.evicted_at
             FROM history_tier_runs r
             JOIN history_tier h ON h.execution_id = r.execution_id
             WHERE r.run_id IN ({placeholders})"
        )
    }

    /// Resolve both scope lists in one statement per bounded batch.
    pub fn scope_sources(run_count: usize, session_count: usize) -> String {
        let mut parts = Vec::new();
        if run_count > 0 {
            let placeholders = vec!["?"; run_count].join(", ");
            parts.push(format!(
                "SELECT 'run', r.run_id, h.execution_id, h.content_hash, h.evicted_at
                 FROM history_tier_runs r JOIN history_tier h ON h.execution_id = r.execution_id
                 WHERE r.run_id IN ({placeholders})"
            ));
        }
        if session_count > 0 {
            let placeholders = vec!["?"; session_count].join(", ");
            parts.push(format!(
                "SELECT 'session', s.id, h.execution_id, h.content_hash, h.evicted_at
                 FROM sessions s JOIN jobs j ON j.id = s.job_id
                 JOIN history_tier h ON h.execution_id = j.execution_id
                 WHERE s.id IN ({placeholders})"
            ));
        }
        parts.join(" UNION ALL ")
    }

    /// The catalog row behind a turn.
    pub const SOURCE_OF_TURN: &str = "SELECT h.execution_id, h.content_hash, h.evicted_at
         FROM turns t
         JOIN jobs j ON j.id = t.job_id
         JOIN history_tier h ON h.execution_id = j.execution_id
         WHERE t.id = ?1";
    /// The catalog row behind a session.
    pub const SOURCE_OF_SESSION: &str = "SELECT h.execution_id, h.content_hash, h.evicted_at
         FROM sessions s
         JOIN jobs j ON j.id = s.job_id
         JOIN history_tier h ON h.execution_id = j.execution_id
         WHERE s.id = ?1";
}

/// Reads transcript history for one logical read. Caches resolved run sources
/// and opened packs, including turn and session sources across pages.
/// Short-lived: make one per read. Holds no connection between calls.
pub struct HistoryReader<'db> {
    db: &'db LocalDb,
    runs: HashMap<String, Source>,
    packs: HashMap<String, Arc<HistoryPack>>,
    turns: HashMap<String, Source>,
    sessions: HashMap<String, Source>,
}

impl<'db> HistoryReader<'db> {
    pub fn new(db: &'db LocalDb) -> Self {
        HistoryReader {
            db,
            runs: HashMap::new(),
            packs: HashMap::new(),
            turns: HashMap::new(),
            sessions: HashMap::new(),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn forced(&self) -> Option<HistorySource> {
        self.db.history_source_override()
    }

    #[cfg(not(any(test, feature = "test-utils")))]
    fn forced(&self) -> Option<HistorySource> {
        None
    }

    /// No archive source can be selected. This is a synchronous capability
    /// check, including test overrides, with no statement or pool checkout.
    pub fn is_hot_only(&self) -> bool {
        !self.db.has_history_catalog()
            || self.forced() == Some(HistorySource::Hot)
            || (self.forced() != Some(HistorySource::Pack) && !self.db.has_evicted_history())
    }

    /// Whether a catalog row sends reads to its pack.
    fn pack_sourced(&self, evicted_at: Option<i64>) -> bool {
        match self.forced() {
            Some(HistorySource::Hot) => false,
            Some(HistorySource::Pack) => true,
            None => evicted_at.is_some(),
        }
    }

    /// Resolve `run_ids`' sources in one batched lookup, so a walk over a job's
    /// or a lineage's runs asks once rather than once per run.
    pub async fn prime_runs(&mut self, run_ids: &[String]) -> Result<(), HistoryError> {
        let missing: Vec<String> = run_ids
            .iter()
            .filter(|run| !self.runs.contains_key(*run))
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        if self.is_hot_only() {
            self.runs
                .extend(missing.into_iter().map(|run| (run, Source::Hot)));
            return Ok(());
        }
        for chunk in missing.chunks(RECONSTRUCT_BATCH_ROWS) {
            let rows = self
                .db
                .query_all(
                    statements::run_sources(chunk.len()),
                    chunk.iter().cloned().map(Value::Text).collect::<Vec<_>>(),
                    |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?, row.opt_i64(3)?)),
                )
                .await?;
            let mut resolved: HashMap<String, Source> = HashMap::new();
            for (run, execution_id, content_hash, evicted_at) in rows {
                if self.pack_sourced(evicted_at) {
                    resolved.insert(
                        run,
                        Source::Pack {
                            execution_id,
                            content_hash,
                        },
                    );
                }
            }
            for run in chunk {
                let source = resolved.remove(run).unwrap_or(Source::Hot);
                self.runs.insert(run.clone(), source);
            }
        }
        Ok(())
    }

    /// Prime a request's sessions and runs together, including negative hot
    /// answers. Subsequent pages and projections reuse these decisions.
    pub async fn prime_sessions_and_runs(
        &mut self,
        session_ids: &[String],
        run_ids: &[String],
    ) -> Result<(), HistoryError> {
        let sessions: Vec<_> = session_ids
            .iter()
            .filter(|id| !self.sessions.contains_key(*id))
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let runs: Vec<_> = run_ids
            .iter()
            .filter(|id| !self.runs.contains_key(*id))
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if self.is_hot_only() {
            self.sessions
                .extend(sessions.into_iter().map(|id| (id, Source::Hot)));
            self.runs
                .extend(runs.into_iter().map(|id| (id, Source::Hot)));
            return Ok(());
        }
        for start in (0..sessions.len().max(runs.len())).step_by(RECONSTRUCT_BATCH_ROWS) {
            let session_chunk = &sessions
                [start.min(sessions.len())..(start + RECONSTRUCT_BATCH_ROWS).min(sessions.len())];
            let run_chunk =
                &runs[start.min(runs.len())..(start + RECONSTRUCT_BATCH_ROWS).min(runs.len())];
            let binds: Vec<_> = run_chunk
                .iter()
                .chain(session_chunk)
                .cloned()
                .map(Value::Text)
                .collect();
            let rows = self
                .db
                .query_all(
                    statements::scope_sources(run_chunk.len(), session_chunk.len()),
                    binds,
                    |row| {
                        Ok((
                            row.text(0)?,
                            row.text(1)?,
                            row.text(2)?,
                            row.text(3)?,
                            row.opt_i64(4)?,
                        ))
                    },
                )
                .await?;
            self.sessions
                .extend(session_chunk.iter().cloned().map(|id| (id, Source::Hot)));
            self.runs
                .extend(run_chunk.iter().cloned().map(|id| (id, Source::Hot)));
            for (kind, id, execution_id, content_hash, evicted_at) in rows {
                if self.pack_sourced(evicted_at) {
                    let source = Source::Pack {
                        execution_id,
                        content_hash,
                    };
                    if kind == "session" {
                        self.sessions.insert(id, source);
                    } else {
                        self.runs.insert(id, source);
                    }
                }
            }
        }
        Ok(())
    }

    async fn source_of_run(&mut self, run_id: &str) -> Result<Source, HistoryError> {
        if !self.runs.contains_key(run_id) {
            self.prime_runs(&[run_id.to_string()]).await?;
        }
        Ok(self.runs.get(run_id).cloned().unwrap_or(Source::Hot))
    }

    /// The source behind a turn or a session, from its catalog statement.
    async fn source_of_scope(&self, sql: &str, id: &str) -> Result<Source, HistoryError> {
        if self.is_hot_only() {
            return Ok(Source::Hot);
        }
        let row = self
            .db
            .query_opt(sql.to_string(), (id.to_string(),), |row| {
                Ok((row.text(0)?, row.text(1)?, row.opt_i64(2)?))
            })
            .await?;
        Ok(match row {
            Some((execution_id, content_hash, evicted_at)) if self.pack_sourced(evicted_at) => {
                Source::Pack {
                    execution_id,
                    content_hash,
                }
            }
            _ => Source::Hot,
        })
    }

    async fn source_of_turn(&mut self, turn_id: &str) -> Result<Source, HistoryError> {
        if let Some(source) = self.turns.get(turn_id) {
            return Ok(source.clone());
        }
        let source = self
            .source_of_scope(statements::SOURCE_OF_TURN, turn_id)
            .await?;
        self.turns.insert(turn_id.to_string(), source.clone());
        Ok(source)
    }

    async fn source_of_session(&mut self, session_id: &str) -> Result<Source, HistoryError> {
        if let Some(source) = self.sessions.get(session_id) {
            return Ok(source.clone());
        }
        let source = self
            .source_of_scope(statements::SOURCE_OF_SESSION, session_id)
            .await?;
        self.sessions.insert(session_id.to_string(), source.clone());
        Ok(source)
    }

    /// Fetch and open a pack once per reader.
    async fn pack(&mut self, content_hash: &str) -> Result<Arc<HistoryPack>, HistoryError> {
        if let Some(pack) = self.packs.get(content_hash) {
            return Ok(pack.clone());
        }
        let store = self
            .db
            .content_store()
            .ok_or_else(|| HistoryError::NoStore {
                content_hash: content_hash.to_string(),
            })?;
        let bytes = store
            .get(content_hash)
            .await
            .map_err(PackError::from)?
            .ok_or_else(|| match store.local_root() {
                Some(path) => HistoryError::Offline {
                    path: path.to_path_buf(),
                },
                None => HistoryError::Missing {
                    content_hash: content_hash.to_string(),
                },
            })?;
        let pack = Arc::new(HistoryPack::open(bytes)?);
        self.packs.insert(content_hash.to_string(), pack.clone());
        Ok(pack)
    }

    /// Hydrate selected candidates without re-seeking hot bodies. Only rows
    /// whose runs resolve to packs take an archive read; preserve input order.
    pub async fn hydrate_selected_hot_rows(
        &mut self,
        rows: Vec<HotRow>,
        filter: EventFilter<'_>,
    ) -> Result<Vec<HistoryRow>, HistoryError> {
        let runs: Vec<_> = rows
            .iter()
            .map(|row| row.event.run_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        self.prime_runs(&runs).await?;
        let mut hot = Vec::new();
        let mut selected = Vec::new();
        for (index, row) in rows.into_iter().enumerate() {
            match self.source_of_run(&row.event.run_id).await? {
                Source::Hot => {
                    selected.push((index, None));
                    hot.push(row);
                }
                Source::Pack {
                    execution_id,
                    content_hash,
                } => {
                    let found = self
                        .run_event_by_rowid(&row.event.run_id, row.rowid)
                        .await?
                        .filter(|found| found.event.id == row.event.id);
                    if let Some(found) = found {
                        let found = pack_page(vec![found], filter, execution_id, content_hash)
                            .rows
                            .pop();
                        selected.push((index, found));
                    }
                }
            }
        }
        let mut hot = reconstruct_hot_rows(self.db, hot, filter).await.into_iter();
        Ok(selected
            .into_iter()
            .filter_map(|(_, row)| row.or_else(|| hot.next()))
            .collect())
    }

    /// The earliest matching row in chat order. The predicate must be pure:
    /// hot pages visit chronologically, while a pack visits in indexed rowid
    /// order and retains the earliest match. Projection and quarantine run
    /// before the predicate for both sources.
    pub async fn find_run_event(
        &mut self,
        run_id: &str,
        filter: EventFilter<'_>,
        matches: impl Fn(&HistoryRow) -> bool,
    ) -> Result<Option<HistoryRow>, HistoryError> {
        match self.source_of_run(run_id).await? {
            Source::Hot => {
                let mut after = None;
                loop {
                    let page = self
                        .run_events_chat_page(run_id, after, RECONSTRUCT_BATCH_ROWS, filter)
                        .await?;
                    let exhausted = page.fetched < RECONSTRUCT_BATCH_ROWS;
                    after = page.last_chat_key;
                    if let Some(row) = page.rows.into_iter().find(&matches) {
                        return Ok(Some(row));
                    }
                    if exhausted || after.is_none() {
                        return Ok(None);
                    }
                }
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let pack = self.pack(&content_hash).await?;
                let mut best: Option<HistoryRow> = None;
                for row in pack_rows(&pack, run_id, i64::MIN) {
                    let row = row?;
                    if !filter.wants(&row.event.event_type) {
                        continue;
                    }
                    let key = (row.event.created_at, row.rowid);
                    if best
                        .as_ref()
                        .is_some_and(|best| key >= (best.event.created_at, best.rowid))
                    {
                        continue;
                    }
                    let row = pack_page(
                        vec![row],
                        filter,
                        execution_id.clone(),
                        content_hash.clone(),
                    )
                    .rows
                    .pop()
                    .expect("one selected pack row");
                    if matches(&row) {
                        best = Some(row);
                    }
                }
                Ok(best)
            }
        }
    }

    /// A bounded chronological page, so first-match consumers can stop before
    /// reconstructing later pages. The cursor is (created_at, rowid).
    pub async fn run_events_chat_page(
        &mut self,
        run_id: &str,
        after: Option<(i64, i64)>,
        limit: usize,
        filter: EventFilter<'_>,
    ) -> Result<Page, HistoryError> {
        match self.source_of_run(run_id).await? {
            Source::Hot => {
                let (_, type_binds) = filter.type_clause(5);
                let (created_at, rowid) = after.unwrap_or((i64::MIN, i64::MIN));
                let mut binds = vec![
                    Value::Text(run_id.to_string()),
                    Value::Integer(created_at),
                    Value::Integer(rowid),
                    Value::Integer(limit_bind(limit)),
                ];
                binds.extend(type_binds);
                self.hot_page(statements::run_chat_page(&filter), binds, filter)
                    .await
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let pack = self.pack(&content_hash).await?;
                // The pack indexes rowid, not time. Inspect every frame for
                // chronological selection, retaining only the requested top-k.
                let mut selected = BTreeMap::new();
                for row in pack_rows(&pack, run_id, i64::MIN) {
                    let row = row?;
                    let key = (row.event.created_at, row.rowid);
                    if after.is_some_and(|after| key <= after)
                        || !filter.wants(&row.event.event_type)
                    {
                        continue;
                    }
                    selected.insert(key, row);
                    if selected.len() > limit.max(1) {
                        selected.pop_last();
                    }
                }
                Ok(pack_page(
                    selected.into_values().collect(),
                    filter,
                    execution_id,
                    content_hash,
                ))
            }
        }
    }

    /// One keyset page of a run, in rowid order. `after_rowid` is exclusive;
    /// start from `i64::MIN`.
    pub async fn run_events_page(
        &mut self,
        run_id: &str,
        after_rowid: i64,
        limit: usize,
        filter: EventFilter<'_>,
    ) -> Result<Page, HistoryError> {
        match self.source_of_run(run_id).await? {
            Source::Hot => {
                let (types, type_binds) = filter.type_clause(4);
                debug_assert!(types.is_empty() == type_binds.is_empty());
                let mut binds = vec![
                    Value::Text(run_id.to_string()),
                    Value::Integer(after_rowid),
                    Value::Integer(limit_bind(limit)),
                ];
                binds.extend(type_binds);
                self.hot_page(statements::run_page(&filter), binds, filter)
                    .await
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let pack = self.pack(&content_hash).await?;
                let mut rows = Vec::new();
                for row in pack_rows(&pack, run_id, after_rowid) {
                    let row = row?;
                    if filter.wants(&row.event.event_type) {
                        rows.push(row);
                        if rows.len() >= limit.max(1) {
                            break;
                        }
                    }
                }
                Ok(pack_page(rows, filter, execution_id, content_hash))
            }
        }
    }

    /// Every event of a run, in chat order `(created_at, rowid)`.
    pub async fn run_events(
        &mut self,
        run_id: &str,
        filter: EventFilter<'_>,
    ) -> Result<Page, HistoryError> {
        if self.source_of_run(run_id).await? == Source::Hot {
            let (_, type_binds) = filter.type_clause(4);
            let mut binds = vec![
                Value::Text(run_id.to_string()),
                Value::Integer(i64::MIN),
                Value::Integer(-1),
            ];
            binds.extend(type_binds);
            return Ok(self
                .hot_page(statements::run_page(&filter), binds, filter)
                .await?
                .sorted_for_chat());
        }
        let mut all = Vec::new();
        let mut after = i64::MIN;
        let provenance = loop {
            let page = self
                .run_events_page(run_id, after, RECONSTRUCT_BATCH_ROWS, filter)
                .await?;
            let exhausted = page.fetched < RECONSTRUCT_BATCH_ROWS;
            all.extend(page.rows);
            match page.last_rowid {
                Some(last) if !exhausted => after = last,
                _ => break page.provenance,
            }
        };
        let fetched = all.len();
        let last_rowid = all.iter().map(|row| row.rowid).max();
        let last_chat_key = all
            .iter()
            .map(|row| (row.event.created_at, row.rowid))
            .max();
        Ok(Page {
            rows: all,
            provenance,
            fetched,
            last_rowid,
            last_chat_key,
        }
        .sorted_for_chat())
    }

    /// A run's events with `floor <= sequence < below`, newest first, at most
    /// `limit` of them.
    pub async fn run_events_window(
        &mut self,
        run_id: &str,
        below_seq: i64,
        floor_seq: i64,
        limit: usize,
        filter: EventFilter<'_>,
    ) -> Result<Page, HistoryError> {
        match self.source_of_run(run_id).await? {
            Source::Hot => {
                let (_, type_binds) = filter.type_clause(5);
                let mut binds = vec![
                    Value::Text(run_id.to_string()),
                    Value::Integer(below_seq),
                    Value::Integer(floor_seq),
                    Value::Integer(i64::try_from(limit).unwrap_or(i64::MAX)),
                ];
                binds.extend(type_binds);
                self.hot_page(statements::run_window(&filter), binds, filter)
                    .await
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let pack = self.pack(&content_hash).await?;
                let mut rows = pack.run_events(run_id)?;
                rows.retain(|row| {
                    let sequence = i64::from(row.event.sequence);
                    sequence < below_seq
                        && sequence >= floor_seq
                        && filter.wants(&row.event.event_type)
                });
                rows.sort_by_key(|row| std::cmp::Reverse(row.event.sequence));
                rows.truncate(limit);
                Ok(pack_page(rows, filter, execution_id, content_hash))
            }
        }
    }

    /// One event by its original rowid, scoped to its run. The pack index
    /// selects only the frame that can hold the key; a miss never becomes the
    /// next row, nor a read of the next frame.
    pub async fn run_event_by_rowid(
        &mut self,
        run_id: &str,
        rowid: i64,
    ) -> Result<Option<HistoryRow>, HistoryError> {
        let filter = EventFilter::default();
        let page = match self.source_of_run(run_id).await? {
            Source::Hot => {
                let binds = vec![Value::Text(run_id.to_string()), Value::Integer(rowid)];
                self.hot_page(statements::run_rowid(), binds, filter)
                    .await?
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let pack = self.pack(&content_hash).await?;
                let frame = pack
                    .index()
                    .iter()
                    .filter(|entry| {
                        entry.section == Section::Events
                            && entry.key == run_id
                            && entry.first_rowid <= rowid
                    })
                    .max_by_key(|entry| entry.first_rowid);
                let mut rows = Vec::new();
                if let Some(frame) = frame {
                    for raw in pack
                        .rows_after(Section::Events, run_id, rowid)
                        .take(frame.rows as usize)
                    {
                        let row = decode_event_row(&raw?)?;
                        if row.rowid >= rowid {
                            if row.rowid == rowid && row.event.run_id == run_id {
                                rows.push(row);
                            }
                            break;
                        }
                    }
                }
                pack_page(rows, filter, execution_id, content_hash)
            }
        };
        Ok(page.rows.into_iter().next())
    }

    /// The event at `sequence` in a run.
    pub async fn run_event_by_sequence(
        &mut self,
        run_id: &str,
        sequence: i64,
    ) -> Result<Option<HistoryRow>, HistoryError> {
        let filter = EventFilter::default();
        let page = match self.source_of_run(run_id).await? {
            Source::Hot => {
                let binds = vec![Value::Text(run_id.to_string()), Value::Integer(sequence)];
                self.hot_page(statements::run_sequence(), binds, filter)
                    .await?
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let pack = self.pack(&content_hash).await?;
                let mut rows = pack.run_events(run_id)?;
                rows.retain(|row| i64::from(row.event.sequence) == sequence);
                rows.truncate(1);
                pack_page(rows, filter, execution_id, content_hash)
            }
        };
        Ok(page.rows.into_iter().next())
    }

    /// A run's sequence-ordered slice, with SQL's signed LIMIT/OFFSET semantics:
    /// a negative limit is unlimited, a negative offset is zero. Hot selection
    /// happens on metadata before any body is read or reconstructed.
    pub async fn run_events_slice(
        &mut self,
        run_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Page, HistoryError> {
        let filter = EventFilter::default();
        match self.source_of_run(run_id).await? {
            Source::Hot => {
                self.hot_page(
                    statements::run_slice(),
                    vec![
                        Value::Text(run_id.to_string()),
                        Value::Integer(limit),
                        Value::Integer(offset),
                    ],
                    filter,
                )
                .await
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let pack = self.pack(&content_hash).await?;
                let mut rows = pack.run_events(run_id)?;
                rows.sort_by_key(|row| (row.event.sequence, row.rowid));
                let take = if limit < 0 {
                    usize::MAX
                } else {
                    usize::try_from(limit).unwrap_or(usize::MAX)
                };
                let rows = rows
                    .into_iter()
                    .skip(usize::try_from(offset.max(0)).unwrap_or(usize::MAX))
                    .take(take)
                    .collect();
                Ok(pack_page(rows, filter, execution_id, content_hash))
            }
        }
    }

    /// A hot lineage after its cached count, ordered by run creation ordinal,
    /// sequence, lineage-session position and rowid. Keys are selected before
    /// bodies in one autocommit statement. None asks the caller to use its
    /// pack-aware metadata/frame path; deciding that costs no statement.
    pub async fn hot_sessions_after_count(
        &self,
        session_ids: &[String],
        offset: i64,
    ) -> Result<Option<Page>, HistoryError> {
        if !self.is_hot_only() {
            return Ok(None);
        }
        if session_ids.is_empty() {
            return Ok(Some(Page::hot(Vec::new(), 0, None)));
        }
        let mut binds: Vec<Value> = session_ids.iter().cloned().map(Value::Text).collect();
        binds.push(Value::Integer(offset));
        let mut selected = self
            .db
            .query_all(
                statements::sessions_after_count(session_ids.len()),
                binds,
                |row| {
                    let i = HotRow::COLUMN_COUNT;
                    let key = (
                        row.i64(i)?,
                        row.opt_i64(i + 1)?,
                        row.opt_i64(i + 2)?,
                        row.i64(i + 3)?,
                        row.i64(i + 4)?,
                        row.i64(EVENT_COLUMN_COUNT + EXTRA_COLUMN_COUNT)?,
                    );
                    Ok((key, select_hot_row(row)?))
                },
            )
            .await?;
        selected.sort_by_key(|(key, _)| *key);
        let mut page = HotPage::default();
        for (_, row) in selected {
            page.push(row);
        }
        Ok(Some(
            page.reconstruct(self.db, EventFilter::default()).await,
        ))
    }

    /// A session's identities in insertion order. Count-based consumers can
    /// discard their cached prefix before asking for the selected bodies.
    pub async fn session_event_keys(
        &mut self,
        session_id: &str,
    ) -> Result<Vec<HistoryKey>, HistoryError> {
        match self.source_of_session(session_id).await? {
            Source::Hot => {
                let mut all = Vec::new();
                let mut after = i64::MIN;
                loop {
                    let keys = self
                        .db
                        .query_all(
                            statements::SESSION_KEYS,
                            vec![
                                Value::Text(session_id.to_string()),
                                Value::Integer(after),
                                Value::Integer(RECONSTRUCT_BATCH_ROWS as i64),
                            ],
                            history_key_from_row,
                        )
                        .await?;
                    let count = keys.len();
                    if let Some(last) = keys.last() {
                        after = last.rowid;
                    }
                    all.extend(keys);
                    if count < RECONSTRUCT_BATCH_ROWS {
                        break;
                    }
                }
                Ok(all)
            }
            Source::Pack { content_hash, .. } => {
                let pack = self.pack(&content_hash).await?;
                let mut keys = Vec::new();
                for run in pack.manifest().runs_of_session(session_id) {
                    for row in pack.run_events(&run.id)? {
                        if row.event.session_id.as_deref() == Some(session_id) {
                            keys.push(HistoryKey {
                                id: row.event.id,
                                run_id: row.event.run_id,
                                sequence: i64::from(row.event.sequence),
                                rowid: row.rowid,
                            });
                        }
                    }
                }
                keys.sort_by_key(|key| key.rowid);
                Ok(keys)
            }
        }
    }

    /// The events of a run named by `ids`, in rowid order, read in chunks.
    pub async fn run_events_by_id(
        &mut self,
        run_id: &str,
        ids: &[String],
    ) -> Result<Vec<HistoryRow>, HistoryError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        const CHUNK: usize = 256;
        let filter = EventFilter::default();
        let mut rows = match self.source_of_run(run_id).await? {
            Source::Hot => {
                let mut rows = Vec::new();
                for chunk in ids.chunks(CHUNK) {
                    let mut binds = vec![Value::Text(run_id.to_string())];
                    binds.extend(chunk.iter().cloned().map(Value::Text));
                    let sql = statements::run_ids(chunk.len());
                    rows.extend(self.hot_page(sql, binds, filter).await?.rows);
                }
                rows
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let wanted: HashSet<&str> = ids.iter().map(String::as_str).collect();
                let pack = self.pack(&content_hash).await?;
                let mut rows = pack.run_events(run_id)?;
                rows.retain(|row| wanted.contains(row.event.id.as_str()));
                pack_page(rows, filter, execution_id, content_hash).rows
            }
        };
        rows.sort_by_key(|row| row.rowid);
        Ok(rows)
    }

    /// Which of `run_ids` holds `event_id`, and at what sequence. For a caller
    /// holding only an event id and the runs it could belong to. A pack-sourced
    /// run is scanned, since a pack has no index by event id; a hot row answers
    /// by primary key.
    pub async fn locate_event(
        &mut self,
        run_ids: &[String],
        event_id: &str,
    ) -> Result<Option<EventLocation>, HistoryError> {
        self.prime_runs(run_ids).await?;
        let mut any_hot = false;
        for run_id in run_ids {
            match self.runs.get(run_id).cloned().unwrap_or(Source::Hot) {
                Source::Hot => any_hot = true,
                Source::Pack { content_hash, .. } => {
                    let pack = self.pack(&content_hash).await?;
                    for row in pack.rows(Section::Events, run_id) {
                        let row = row?;
                        if row.get("id").and_then(Json::as_str) == Some(event_id) {
                            let sequence = row.get("sequence").and_then(Json::as_i64);
                            return Ok(sequence.map(|sequence| EventLocation {
                                run_id: run_id.clone(),
                                sequence,
                            }));
                        }
                    }
                }
            }
        }
        if !any_hot {
            return Ok(None);
        }
        let found = self
            .db
            .query_opt(statements::LOCATE_EVENT, (event_id.to_string(),), |row| {
                Ok((row.text(0)?, row.i64(1)?))
            })
            .await?;
        Ok(found.and_then(|(run_id, sequence)| {
            run_ids
                .contains(&run_id)
                .then_some(EventLocation { run_id, sequence })
        }))
    }

    /// A turn's events, in `(sequence, rowid)` order.
    pub async fn turn_events(&mut self, turn_id: &str) -> Result<Page, HistoryError> {
        let filter = EventFilter::default();
        let mut page = match self.source_of_turn(turn_id).await? {
            Source::Hot => {
                self.hot_page(
                    statements::turn_page(),
                    vec![
                        Value::Text(turn_id.to_string()),
                        Value::Integer(i64::MIN),
                        Value::Integer(-1),
                    ],
                    filter,
                )
                .await?
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let pack = self.pack(&content_hash).await?;
                let run_ids: Vec<String> = pack
                    .manifest()
                    .runs_of_turn(turn_id)
                    .map(|run| run.id.clone())
                    .collect();
                let mut rows = Vec::new();
                for run_id in run_ids {
                    rows.extend(
                        pack.run_events(&run_id)?
                            .into_iter()
                            .filter(|row| row.event.turn_id.as_deref() == Some(turn_id)),
                    );
                }
                pack_page(rows, filter, execution_id, content_hash)
            }
        };
        page.rows.sort_by_key(|row| (row.event.sequence, row.rowid));
        page.last_rowid = page.rows.iter().map(|row| row.rowid).max();
        Ok(page)
    }

    /// The first sequence of a turn in a run.
    pub async fn turn_start_sequence(
        &mut self,
        turn_id: &str,
        run_id: &str,
    ) -> Result<Option<i64>, HistoryError> {
        match self.source_of_run(run_id).await? {
            Source::Hot => Ok(self
                .db
                .query_one(
                    statements::TURN_START,
                    (turn_id.to_string(), run_id.to_string()),
                    |row| row.opt_i64(0),
                )
                .await?),
            Source::Pack { content_hash, .. } => {
                let pack = self.pack(&content_hash).await?;
                Ok(pack
                    .run_events(run_id)?
                    .iter()
                    .filter(|row| row.event.turn_id.as_deref() == Some(turn_id))
                    .map(|row| i64::from(row.event.sequence))
                    .min())
            }
        }
    }

    /// One keyset page of a session, in rowid order, optionally leaving out one
    /// run. `after_rowid` is exclusive; start from `i64::MIN`.
    pub async fn session_events_page(
        &mut self,
        session_id: &str,
        except_run: Option<&str>,
        after_rowid: i64,
        limit: usize,
        filter: EventFilter<'_>,
    ) -> Result<Page, HistoryError> {
        match self.source_of_session(session_id).await? {
            Source::Hot => {
                let first_type = if except_run.is_some() { 5 } else { 4 };
                let (_, type_binds) = filter.type_clause(first_type);
                let mut binds = vec![
                    Value::Text(session_id.to_string()),
                    Value::Integer(after_rowid),
                    Value::Integer(limit_bind(limit)),
                ];
                if let Some(run) = except_run {
                    binds.push(Value::Text(run.to_string()));
                }
                binds.extend(type_binds);
                self.hot_page(
                    statements::session_page(&filter, except_run.is_some()),
                    binds,
                    filter,
                )
                .await
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let pack = self.pack(&content_hash).await?;
                let rows =
                    pack_session_page(&pack, session_id, except_run, after_rowid, limit, filter)?;
                Ok(pack_page(rows, filter, execution_id, content_hash))
            }
        }
    }

    /// Every event of a session after `after_rowid`, in rowid order.
    pub async fn session_events_after(
        &mut self,
        session_id: &str,
        except_run: Option<&str>,
        after_rowid: i64,
        filter: EventFilter<'_>,
    ) -> Result<Page, HistoryError> {
        if self.source_of_session(session_id).await? == Source::Hot {
            let (_, type_binds) = filter.type_clause(if except_run.is_some() { 5 } else { 4 });
            let mut binds = vec![
                Value::Text(session_id.to_string()),
                Value::Integer(after_rowid),
                Value::Integer(-1),
            ];
            if let Some(run) = except_run {
                binds.push(Value::Text(run.to_string()));
            }
            binds.extend(type_binds);
            return self
                .hot_page(
                    statements::session_page(&filter, except_run.is_some()),
                    binds,
                    filter,
                )
                .await;
        }
        let mut all = Vec::new();
        let mut after = after_rowid;
        let provenance = loop {
            let page = self
                .session_events_page(
                    session_id,
                    except_run,
                    after,
                    RECONSTRUCT_BATCH_ROWS,
                    filter,
                )
                .await?;
            let exhausted = page.fetched < RECONSTRUCT_BATCH_ROWS;
            all.extend(page.rows);
            match page.last_rowid {
                Some(last) if !exhausted => after = last,
                _ => break page.provenance,
            }
        };
        let fetched = all.len();
        let last_rowid = all.iter().map(|row| row.rowid).max();
        let last_chat_key = all
            .iter()
            .map(|row| (row.event.created_at, row.rowid))
            .max();
        Ok(Page {
            rows: all,
            provenance,
            fetched,
            last_rowid,
            last_chat_key,
        })
    }

    /// Every event of a session, in chat order
    /// `(created_at, rowid)`.
    pub async fn session_events(
        &mut self,
        session_id: &str,
        except_run: Option<&str>,
        filter: EventFilter<'_>,
    ) -> Result<Page, HistoryError> {
        Ok(self
            .session_events_after(session_id, except_run, i64::MIN, filter)
            .await?
            .sorted_for_chat())
    }

    /// A session's newest `limit` events, oldest of them first.
    pub async fn session_tail(
        &mut self,
        session_id: &str,
        limit: usize,
    ) -> Result<Page, HistoryError> {
        let filter = EventFilter::default();
        let mut page = match self.source_of_session(session_id).await? {
            Source::Hot => {
                let binds = vec![
                    Value::Text(session_id.to_string()),
                    Value::Integer(limit_bind(limit)),
                ];
                self.hot_page(statements::session_tail(), binds, filter)
                    .await?
            }
            Source::Pack {
                execution_id,
                content_hash,
            } => {
                let pack = self.pack(&content_hash).await?;
                let mut rows = pack_session_rows(&pack, session_id, None)?;
                let skip = rows.len().saturating_sub(limit);
                rows.drain(..skip);
                pack_page(rows, filter, execution_id, content_hash)
            }
        };
        page.rows.sort_by_key(|row| row.rowid);
        Ok(page)
    }

    /// A session's newest event rowid.
    pub async fn session_max_rowid(
        &mut self,
        session_id: &str,
    ) -> Result<Option<i64>, HistoryError> {
        match self.source_of_session(session_id).await? {
            Source::Hot => Ok(self
                .db
                .query_one(
                    statements::SESSION_MAX_ROWID,
                    (session_id.to_string(),),
                    |row| row.opt_i64(0),
                )
                .await?),
            Source::Pack { content_hash, .. } => {
                let pack = self.pack(&content_hash).await?;
                Ok(pack
                    .manifest()
                    .sessions
                    .iter()
                    .find(|session| session.id == session_id)
                    .and_then(|session| session.max_rowid))
            }
        }
    }

    /// `event_read_tokens` rows for `(run_id, event_id)` pairs, keyed by event
    /// id. One statement per chunk of hot ids; pack rows come from each run's
    /// `event_read_tokens` frames.
    pub async fn read_segments(
        &mut self,
        events: &[(String, String)],
    ) -> Result<ReadSegments, HistoryError> {
        const CHUNK: usize = 400;
        let mut found = ReadSegments::default();
        let runs: Vec<String> = events
            .iter()
            .map(|(run, _)| run.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        self.prime_runs(&runs).await?;

        let mut hot_ids = Vec::new();
        let mut pack_wanted: HashMap<String, (String, HashSet<&str>)> = HashMap::new();
        for (run, event) in events {
            match self.runs.get(run).cloned().unwrap_or(Source::Hot) {
                Source::Hot => hot_ids.push(event.clone()),
                Source::Pack { content_hash, .. } => {
                    pack_wanted
                        .entry(run.clone())
                        .or_insert_with(|| (content_hash, HashSet::new()))
                        .1
                        .insert(event.as_str());
                }
            }
        }
        for chunk in hot_ids.chunks(CHUNK) {
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let rows = self
                .db
                .query_all(
                    format!(
                        "SELECT event_id, segments_json FROM event_read_tokens
                         WHERE event_id IN ({placeholders})"
                    ),
                    chunk.iter().cloned().map(Value::Text).collect::<Vec<_>>(),
                    |row| Ok((row.text(0)?, row.text(1)?)),
                )
                .await?;
            found.json.extend(rows);
        }
        for (run, (content_hash, wanted)) in pack_wanted {
            let pack = self.pack(&content_hash).await?;
            for row in pack.rows(Section::EventReadTokens, &run) {
                let row = row?;
                let Some(event) = row.get("event_id").and_then(Json::as_str) else {
                    continue;
                };
                if !wanted.contains(event) {
                    continue;
                }
                if let Some(json) = row.get("segments_json").and_then(Json::as_str) {
                    found.json.insert(event.to_string(), json.to_string());
                }
            }
            found.pack_runs.insert(run);
        }
        Ok(found)
    }

    /// Run one hot statement in its own short read and build its rows.
    async fn hot_page(
        &self,
        sql: String,
        binds: Vec<Value>,
        filter: EventFilter<'_>,
    ) -> Result<Page, HistoryError> {
        let selected = self.db.query_all(sql, binds, select_hot_row).await?;
        let mut raw = HotPage::default();
        for row in selected {
            raw.push(row);
        }
        Ok(raw.reconstruct(self.db, filter).await)
    }
}

#[derive(Default)]
pub struct HotPage {
    pub rows: Vec<HotRow>,
    pub fetched: usize,
    pub last_rowid: Option<i64>,
    /// Maximum raw (created_at, rowid) key, including rows that failed to decode.
    pub last_chat_key: Option<(i64, i64)>,
}

impl HotPage {
    fn push(&mut self, (rowid, created_at, row): (i64, i64, Option<HotRow>)) {
        self.fetched += 1;
        let key = (created_at, rowid);
        self.last_chat_key = Some(self.last_chat_key.map_or(key, |last| last.max(key)));
        self.last_rowid = Some(self.last_rowid.map_or(rowid, |last| last.max(rowid)));
        if let Some(row) = row {
            self.rows.push(row);
        }
    }

    /// Release the selecting connection before reconstructing archived bodies.
    pub async fn reconstruct(self, db: &LocalDb, filter: EventFilter<'_>) -> Page {
        let mut page = Page::hot(
            reconstruct_hot_rows(db, self.rows, filter).await,
            self.fetched,
            self.last_rowid,
        );
        page.last_chat_key = self.last_chat_key;
        page
    }
}

/// An already-selected hot event, not reconstructed or quarantine-gated yet.
pub struct HotRow {
    pub event: Event,
    pub rowid: i64,
    pub extras: EventExtras,
}

impl HotRow {
    pub const COLUMN_COUNT: usize = EVENT_COLUMN_COUNT + EXTRA_COLUMN_COUNT + 1;

    /// The canonical full event, extras and rowid projection.
    pub fn columns(alias: Option<&str>) -> String {
        let columns = format!("{EVENT_COLUMNS}, {EXTRA_COLUMNS}, rowid");
        columns
            .split(',')
            .map(str::trim)
            .map(|column| match alias {
                Some(alias) => format!("{alias}.{column}"),
                None => column.to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn from_row(row: &turso::Row) -> DbResult<Self> {
        Self::from_row_at(row, 0)
    }

    pub fn from_row_at(row: &turso::Row, offset: usize) -> DbResult<Self> {
        Ok(Self {
            event: event_from_row_at(row, offset)?,
            rowid: row.i64(offset + EVENT_COLUMN_COUNT + EXTRA_COLUMN_COUNT)?,
            extras: EventExtras {
                reported_model: row.opt_text(offset + EVENT_COLUMN_COUNT)?,
                change_preview_status: row.opt_text(offset + EVENT_COLUMN_COUNT + 1)?,
                change_applied_at: row.opt_i64(offset + EVENT_COLUMN_COUNT + 2)?,
                account_id: row.opt_text(offset + EVENT_COLUMN_COUNT + 3)?,
                selected_model: row.opt_text(offset + EVENT_COLUMN_COUNT + 4)?,
            },
        })
    }
}

/// Select rows on an existing checkout. Reconstruct only after releasing it;
/// selection itself must not acquire another connection or fetch archive bytes.
pub async fn gather_hot_page(
    conn: &TrackedConnection,
    sql: &str,
    binds: Vec<Value>,
) -> DbResult<HotPage> {
    let mut rows = conn.query(sql, binds).await?;
    let mut page = HotPage::default();
    while let Some(row) = rows.next().await? {
        page.push(select_hot_row(&row)?);
    }
    Ok(page)
}

fn select_hot_row(row: &turso::Row) -> DbResult<(i64, i64, Option<HotRow>)> {
    let rowid = row.i64(EVENT_COLUMN_COUNT + EXTRA_COLUMN_COUNT)?;
    // A malformed event still advances the keyset and counts toward exhaustion.
    let hot = match event_from_row(row) {
        Ok(event) => Some(HotRow {
            event,
            rowid,
            extras: extras_from_row(row)?,
        }),
        Err(_) => None,
    };
    Ok((rowid, row.i64(8)?, hot))
}

fn extras_from_row(row: &turso::Row) -> DbResult<EventExtras> {
    Ok(EventExtras {
        reported_model: row.opt_text(EVENT_COLUMN_COUNT)?,
        change_preview_status: row.opt_text(EVENT_COLUMN_COUNT + 1)?,
        change_applied_at: row.opt_i64(EVENT_COLUMN_COUNT + 2)?,
        account_id: row.opt_text(EVENT_COLUMN_COUNT + 3)?,
        selected_model: row.opt_text(EVENT_COLUMN_COUNT + 4)?,
    })
}

fn limit_bind(limit: usize) -> i64 {
    i64::try_from(limit.max(1)).unwrap_or(i64::MAX)
}

/// Reconstruct hot rows and label them. A body the reader never renders is
/// withheld first, so an archived one is never decompressed or fetched. The
/// quarantine gate runs last, inside [`reconstruct_events_reporting`]. A row
/// that resolved has no further use for its compressed form, and clearing it
/// makes the row equal to its pack form.
pub async fn reconstruct_hot_rows(
    db: &LocalDb,
    rows: Vec<HotRow>,
    filter: EventFilter<'_>,
) -> Vec<HistoryRow> {
    let mut rows = rows.into_iter();
    let mut all = Vec::new();
    loop {
        let batch: Vec<_> = rows.by_ref().take(RECONSTRUCT_BATCH_ROWS).collect();
        if batch.is_empty() {
            break;
        }
        all.extend(reconstruct_hot_batch(db, batch, filter).await);
    }
    all
}

async fn reconstruct_hot_batch(
    db: &LocalDb,
    rows: Vec<HotRow>,
    filter: EventFilter<'_>,
) -> Vec<HistoryRow> {
    let mut events = Vec::with_capacity(rows.len());
    let mut keys = Vec::with_capacity(rows.len());
    for row in rows {
        let mut event = row.event;
        if let Some(bodies) = filter.bodies {
            if (bodies.is_unread)(&event.event_type) {
                (bodies.withhold)(&mut event);
            }
        }
        events.push(event);
        keys.push((row.rowid, row.extras));
    }
    reconstruct_events_reporting(db, events)
        .await
        .into_iter()
        .zip(keys)
        .map(|((mut event, reconstruction), (rowid, extras))| {
            if !reconstruction.is_unresolved() {
                event.data_blob = None;
            }
            HistoryRow {
                event,
                rowid,
                reconstruction,
                extras,
            }
        })
        .collect()
}

/// A run's rows from a pack, past `after_rowid`, skipping whole frames that
/// end at or before it. Lazily decoded: a caller that stops early inflates
/// nothing further.
fn pack_rows<'p>(
    pack: &'p HistoryPack,
    run_id: &'p str,
    after_rowid: i64,
) -> impl Iterator<Item = Result<HistoryRow, PackError>> + 'p {
    pack.rows_after(Section::Events, run_id, after_rowid)
        .filter_map(
            move |row| match row.and_then(|row| decode_event_row(&row)) {
                Ok(row) if row.rowid <= after_rowid => None,
                other => Some(other),
            },
        )
}

/// Merge run cursors in rowid order, holding only one frame and one head per
/// run. Filter after merging: seeking a matching head in each run could decode
/// a whole filtered-out run before returning an early page from another.
fn pack_session_page(
    pack: &HistoryPack,
    session_id: &str,
    except_run: Option<&str>,
    after_rowid: i64,
    limit: usize,
    filter: EventFilter<'_>,
) -> Result<Vec<HistoryRow>, PackError> {
    let mut cursors: Vec<_> = pack
        .manifest()
        .runs_of_session(session_id)
        .filter(|run| Some(run.id.as_str()) != except_run)
        .map(|run| pack_rows(pack, &run.id, after_rowid))
        .collect();
    let mut heads = Vec::with_capacity(cursors.len());
    let mut order = BinaryHeap::new();
    for (index, cursor) in cursors.iter_mut().enumerate() {
        let head = cursor.next().transpose()?;
        if let Some(row) = &head {
            order.push(Reverse((row.rowid, index)));
        }
        heads.push(head);
    }
    let mut rows = Vec::new();
    while let Some(Reverse((_, index))) = order.pop() {
        let row = heads[index].take().expect("queued cursor has a head");
        if row.event.session_id.as_deref() == Some(session_id)
            && filter.wants(&row.event.event_type)
        {
            rows.push(row);
            if rows.len() == limit.max(1) {
                // Do not peek into the next frame after filling the page.
                break;
            }
        }
        let head = cursors[index].next().transpose()?;
        if let Some(row) = &head {
            order.push(Reverse((row.rowid, index)));
        }
        heads[index] = head;
    }
    Ok(rows)
}

/// A session's rows from a pack, in rowid order.
fn pack_session_rows(
    pack: &HistoryPack,
    session_id: &str,
    except_run: Option<&str>,
) -> Result<Vec<HistoryRow>, PackError> {
    let mut rows = Vec::new();
    let run_ids: Vec<&str> = pack
        .manifest()
        .runs_of_session(session_id)
        .map(|run| run.id.as_str())
        .filter(|run| Some(*run) != except_run)
        .collect();
    for run_id in run_ids {
        rows.extend(
            pack.run_events(run_id)?
                .into_iter()
                .filter(|row| row.event.session_id.as_deref() == Some(session_id)),
        );
    }
    rows.sort_by_key(|row| row.rowid);
    Ok(rows)
}

/// Finish rows decoded from a pack: apply the body projection, then the
/// quarantine gate, last, on both of the row's keys. A projected body reads as
/// stored plain, because a hot read never reconstructs one.
fn pack_page(
    mut rows: Vec<HistoryRow>,
    filter: EventFilter<'_>,
    execution_id: String,
    content_hash: String,
) -> Page {
    if let Some(bodies) = filter.bodies {
        for row in rows.iter_mut() {
            if (bodies.is_unread)(&row.event.event_type) {
                (bodies.withhold)(&mut row.event);
                row.reconstruction = Reconstruction::Stored;
            }
        }
    }
    let mut events: Vec<Event> = rows.iter().map(|row| row.event.clone()).collect();
    let withheld = withhold_pack_rows_reporting(&mut events);
    for ((row, event), incident) in rows.iter_mut().zip(events).zip(withheld) {
        row.event = event;
        if incident.is_some() {
            row.reconstruction = Reconstruction::Withheld;
            row.event.data_blob = None;
        }
    }
    let fetched = rows.len();
    let last_rowid = rows.iter().map(|row| row.rowid).max();
    let last_chat_key = rows
        .iter()
        .map(|row| (row.event.created_at, row.rowid))
        .max();
    Page {
        rows,
        provenance: Provenance::Pack {
            execution_id,
            content_hash,
        },
        fetched,
        last_rowid,
        last_chat_key,
    }
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;
