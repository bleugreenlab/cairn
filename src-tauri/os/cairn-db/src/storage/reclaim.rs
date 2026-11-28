//! Bounded offline reclaim: a compacted copy of a database made table by
//! table in short transactions, and the verification that must pass before
//! the copy may replace the original.
//!
//! Under MVCC the engine reuses freed pages but never returns them, so the
//! file only shrinks when a compacted copy replaces it. `VACUUM INTO` cannot
//! make that copy at production size on the pinned engine. It opens its
//! destination in MVCC mode and writes every table, index, trigger and view
//! inside one transaction, and every row it writes is an in-memory row version
//! until that transaction's single commit. Resident memory is therefore at
//! least the whole image: on the 30 GB production database it passed 80 GB
//! without finishing. No engine setting bounds it: `cache_size` covers only
//! the page cache, and the MVCC store has no spill.
//!
//! [`copy_compacted`] bounds memory by never letting the destination hold more
//! than one fold's worth of row versions. Each table is read in rowid keyset
//! pages, each page is written in its own destination transaction, and when
//! the destination's logical log passes [`ReclaimLimits::fold_bytes`] the copy
//! folds it, which drops the folded row versions from memory. The order of the
//! schema follows from the same constraint:
//!
//! - Tables, then their explicit indexes, go first, on empty tables. Under
//!   MVCC a `CREATE INDEX` on a full table fills the whole index inside one
//!   statement, another unbounded transaction, and `REINDEX` is refused. Index
//!   entries written alongside each row are folded with the rows.
//! - Views and triggers go last, so no trigger fires on a copied row (the
//!   search-outbox triggers, the run-activity trigger, the provenance
//!   validators). Views go before triggers because an `INSTEAD OF` trigger
//!   names its view.
//!
//! Before any of that, the whole schema is replayed into an empty scratch
//! database and compared with the source, so a schema the copy cannot carry
//! fails in seconds rather than after hours of rows.
//!
//! The schema is read from `sqlite_master` deduplicated by `(type, name)`,
//! keeping the last row. The pinned engine has let a database list one trigger
//! twice (cairn/7751), and replaying both rows fails on the second `CREATE`.
//! The copy and its verification both name every duplicate they dropped.
//!
//! Rowids are preserved on every table: retention, the archival progress
//! cursor and the census all page by rowid. AUTOINCREMENT counters are carried
//! over too, including a counter above the table's largest rowid.
//!
//! [`verify`] reads both files whole and compares them: schema, AUTOINCREMENT
//! counters, and per table the row count, largest rowid and a digest of every
//! row's content in rowid order. It then checks the copy on its own:
//! `integrity_check` and the migration chain. It reports the tables the
//! retention registry has no rule for, but a missing rule is not a failure of
//! the copy; `every_table_has_a_retention_rule` is where that fails.
//!
//! [`rehearse`] runs every step but the row copy, against a snapshot of the
//! real database, so the tool can be checked against the production schema in
//! minutes before an operator window.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::local_db::require_db_path;
use crate::storage::census::quote_identifier as quote;
use crate::storage::retention::audit_tables;
use crate::storage::{
    db_set_paths, db_set_size, DbError, DbResult, DrainBudget, LocalDb, MigrationRunner, RowExt,
    TrackedConnection, TURSO_MIGRATIONS,
};
use crate::turso::Value;

/// Rows read and written as one page, at most.
pub const PAGE_ROWS: usize = 2_000;
/// Value bytes that end a page early. A single larger row is a page by itself.
pub const PAGE_BYTES: usize = 8 * 1024 * 1024;
/// Destination logical-log size that triggers a fold: the production fold
/// threshold. A fold's cost grows faster than its size. Copying the first
/// 2 GiB of the 36 GB production clone (11-03, external NVMe, host load
/// 25-60), 16 MiB folds took 0.42 s each (p50) for a 0.74 GB peak at
/// 4.5 MiB/s; 64 MiB folds took 2.9 s for a 1.13 GB peak at 3.4 MiB/s; and a
/// 256 MiB fold outran the 20 s checkpoint bound and failed the copy.
pub const DEFAULT_FOLD_BYTES: u64 = 16 * 1024 * 1024;
/// A source whose log is still larger than this after folding is refused,
/// the rule the census delete probe uses: an unfolded log means the source's
/// main file is not the whole database, and reading it would be slower and
/// hold the log's row versions in memory.
const SOURCE_LOG_LIMIT: u64 = 16 * 1024 * 1024;
/// Consecutive unreadable memory readings after which the copy stops.
const UNREADABLE_READINGS: u32 = 3;
/// The prefix of the backing tables that hold sequences, AUTOINCREMENT's
/// counters among them. The engine creates them with their tables.
const SEQUENCE_BACKING_PREFIX: &str = "__turso_internal_seq_";
/// The MVCC store's per-file metadata (its persisted transaction timestamp),
/// which every MVCC database creates for itself on open.
const MVCC_META_TABLE: &str = "__turso_internal_mvcc_meta";
/// The engine's name for the implicit sequence behind an AUTOINCREMENT table.
const AUTOINCREMENT_SEQUENCE_PREFIX: &str = "__turso_internal_autoincrement_";
/// The migration runner's ledger (`storage/migration.rs`): the one table whose
/// rows a rehearsal copies, so the migration check means on the empty copy
/// what it means on a full one.
const MIGRATION_LEDGER: &str = "cairn_schema_migrations";
/// The order the schema is replayed in; see the module docs.
const REPLAY_ORDER: [&str; 4] = ["table", "index", "view", "trigger"];

/// How large the copy's pages and folds are, and the memory it may use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ReclaimLimits {
    /// Resident memory past which the copy stops and removes its destination.
    pub cap_bytes: u64,
    /// Destination logical-log size that triggers a fold.
    pub fold_bytes: u64,
    pub page_rows: usize,
    pub page_bytes: usize,
    /// Stop once this many value bytes are copied, leaving a partial copy:
    /// for measuring memory against fold size without copying everything.
    pub stop_after_bytes: Option<u64>,
}

impl ReclaimLimits {
    /// The production pages and folds under a memory cap.
    pub const fn with_cap(cap_bytes: u64) -> Self {
        Self {
            cap_bytes,
            fold_bytes: DEFAULT_FOLD_BYTES,
            page_rows: PAGE_ROWS,
            page_bytes: PAGE_BYTES,
            stop_after_bytes: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReclaimError {
    #[error("reclaim refused: {0}")]
    Refused(String),
    #[error(
        "resident memory reached {resident} bytes, past the {cap} byte cap, after copying \
         {rows} rows; the copy stopped and its destination was removed"
    )]
    MemoryCap { resident: u64, cap: u64, rows: u64 },
    #[error(
        "resident memory could not be read {0} times in a row, so the cap could not be \
         enforced; the copy stopped and its destination was removed"
    )]
    MemoryUnreadable(u32),
    #[error("folding the logical log of {path} failed: {error}")]
    Fold { path: String, error: String },
    #[error(
        "the source's schema cannot be replayed into an empty database, so no row was \
         copied: {}",
        .0.join("; ")
    )]
    Schema(Vec<String>),
    #[error(transparent)]
    Db(#[from] DbError),
}

impl From<turso::Error> for ReclaimError {
    fn from(error: turso::Error) -> Self {
        Self::Db(error.into())
    }
}

/// One table's copy.
#[derive(Debug, Clone, Default, Serialize)]
pub struct TableCopy {
    pub table: String,
    pub rows: u64,
    /// Bytes of values read: text and blob lengths, eight per number.
    pub bytes: u64,
    pub pages: u64,
    pub ms: u64,
}

/// One fold of a logical log.
#[derive(Debug, Clone, Default, Serialize)]
pub struct FoldRecord {
    /// Value bytes copied when the fold ran.
    pub after_bytes: u64,
    pub log_bytes_before: u64,
    pub log_bytes_after: u64,
    pub ms: u64,
}

/// Wall time of each phase of the copy, in milliseconds.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CopyPhases {
    pub source_fold_ms: u64,
    /// The whole schema replayed into an empty scratch database and compared.
    pub schema_check_ms: u64,
    pub schema_ms: u64,
    pub rows_ms: u64,
    /// AUTOINCREMENT counters, views and triggers.
    pub finish_ms: u64,
    pub final_fold_ms: u64,
    pub total_ms: u64,
}

/// What [`copy_compacted`] did.
#[derive(Debug, Clone, Serialize)]
pub struct CopyReport {
    pub limits: ReclaimLimits,
    pub source_page_size: i64,
    pub dest_page_size: i64,
    pub source_fold: FoldRecord,
    pub tables: Vec<TableCopy>,
    pub folds: Vec<FoldRecord>,
    pub rows: u64,
    pub bytes: u64,
    /// AUTOINCREMENT counters raised above the largest copied rowid.
    pub sequences_raised: Vec<(String, i64)>,
    /// The largest reading of this process's resident memory, and how many
    /// readings were taken (one per page and one per fold).
    pub peak_resident_bytes: u64,
    pub readings: u64,
    /// The copy stopped at [`ReclaimLimits::stop_after_bytes`]: rows are
    /// missing and no counters, views or triggers were created.
    pub partial: bool,
    /// A [`rehearse`] copy: the whole schema and the counters, but only the
    /// migration ledger's rows.
    pub rehearsal: bool,
    /// `(type, name)`s the source's `sqlite_master` lists more than once, and
    /// how many rows were dropped for them in all.
    pub schema_duplicates: Vec<SchemaDuplicate>,
    pub schema_rows_dropped: usize,
    pub source_set_bytes: u64,
    pub dest_set_bytes: u64,
    pub phases: CopyPhases,
}

/// Progress the copy reports as it goes.
#[derive(Debug)]
pub enum CopyProgress<'a> {
    Table(&'a TableCopy),
    Fold(&'a FoldRecord),
}

/// Copy `source` into a new database at `dest`, compacted, in bounded memory.
///
/// `dest` must be a `.db` path none of whose file set exists. Nothing else may
/// have either database open: the copy folds both, and a fold needs a moment
/// with no transaction open. Folding the source is a write to it: its pending
/// logical log moves into its main file, as at every boot, so its files change
/// while its rows do not. Beyond that the source is only read. On any failure
/// the destination set is removed.
pub async fn copy_compacted(
    source: &Path,
    dest: &Path,
    limits: ReclaimLimits,
    observe: impl FnMut(CopyProgress<'_>),
) -> Result<CopyReport, ReclaimError> {
    copy_with_reader(
        source,
        dest,
        limits,
        &cairn_common::memory::own_resident_bytes,
        observe,
    )
    .await
}

/// Which rows a copy carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Every table's.
    Copy,
    /// Only the migration ledger's: [`rehearse`].
    Rehearse,
}

/// What [`rehearse`] found.
#[derive(Debug, Clone, Serialize)]
pub struct Rehearsal {
    pub copy: CopyReport,
    pub verification: Verification,
}

/// Every step of [`copy_compacted`] and [`verify`] but the row copy, against
/// `source`, which should be a snapshot: the source fold and its log bound,
/// the schema check, the destination's schema, counters, views and triggers,
/// then the schema, counter, `integrity_check`, migration and retention
/// checks. Only the migration ledger's rows are copied, and the row digests
/// and `foreign_key_check`, which read every row, are not run. The empty copy
/// at `dest` is removed afterwards, whatever the outcome.
pub async fn rehearse(
    source: &Path,
    dest: &Path,
    limits: ReclaimLimits,
) -> Result<Rehearsal, ReclaimError> {
    rehearse_with_reader(
        source,
        dest,
        limits,
        &cairn_common::memory::own_resident_bytes,
    )
    .await
}

async fn rehearse_with_reader(
    source: &Path,
    dest: &Path,
    limits: ReclaimLimits,
    read_memory: &(dyn Fn() -> Option<u64> + Send + Sync),
) -> Result<Rehearsal, ReclaimError> {
    let limits = ReclaimLimits {
        stop_after_bytes: None,
        ..limits
    };
    let copy = run_copy(source, dest, limits, Mode::Rehearse, read_memory, |_| {}).await?;
    let verification = verify_in(source, dest, Mode::Rehearse).await;
    for member in db_set_paths(dest) {
        let _ = std::fs::remove_file(member);
    }
    Ok(Rehearsal {
        copy,
        verification: verification?,
    })
}

/// [`copy_compacted`] with the memory reading supplied, so a test can stand
/// in a reader that fails.
async fn copy_with_reader(
    source: &Path,
    dest: &Path,
    limits: ReclaimLimits,
    read_memory: &(dyn Fn() -> Option<u64> + Send + Sync),
    observe: impl FnMut(CopyProgress<'_>),
) -> Result<CopyReport, ReclaimError> {
    run_copy(source, dest, limits, Mode::Copy, read_memory, observe).await
}

async fn run_copy(
    source: &Path,
    dest: &Path,
    limits: ReclaimLimits,
    mode: Mode,
    read_memory: &(dyn Fn() -> Option<u64> + Send + Sync),
    mut observe: impl FnMut(CopyProgress<'_>),
) -> Result<CopyReport, ReclaimError> {
    require_db_path(dest)?;
    let occupied: Vec<String> = db_set_paths(dest)
        .iter()
        .filter(|member| member.exists())
        .map(|member| member.display().to_string())
        .collect();
    if !occupied.is_empty() {
        return Err(ReclaimError::Refused(format!(
            "the destination set already exists: {}",
            occupied.join(", ")
        )));
    }
    let mut guard = MemoryGuard::new(limits.cap_bytes, read_memory);
    // Fail closed before anything is written: a cap that cannot be read now
    // could not be enforced later either.
    guard.check(0)?;
    let result = copy_into(source, dest, limits, mode, &mut guard, &mut observe).await;
    match result {
        Ok(mut report) => {
            report.peak_resident_bytes = guard.peak;
            report.readings = guard.readings;
            report.dest_set_bytes = db_set_size(dest);
            Ok(report)
        }
        Err(error) => {
            // `copy_into` has dropped the destination handle by now, so its
            // files are closed.
            for member in db_set_paths(dest) {
                let _ = std::fs::remove_file(member);
            }
            Err(error)
        }
    }
}

async fn copy_into(
    source_path: &Path,
    dest_path: &Path,
    limits: ReclaimLimits,
    mode: Mode,
    guard: &mut MemoryGuard<'_>,
    observe: &mut impl FnMut(CopyProgress<'_>),
) -> Result<CopyReport, ReclaimError> {
    let started = Instant::now();
    let mut phases = CopyPhases::default();

    let source = LocalDb::open(source_path).await?;
    let source_fold = fold(&source, 0).await?;
    if source_fold.log_bytes_after > SOURCE_LOG_LIMIT {
        return Err(ReclaimError::Refused(format!(
            "the source's logical log is still {} bytes after folding; a copy needs it \
             under {SOURCE_LOG_LIMIT}",
            source_fold.log_bytes_after
        )));
    }
    phases.source_fold_ms = source_fold.ms;
    let source_page_size = scalar(&source, "PRAGMA page_size").await?.unwrap_or(0);
    let (entries, schema_duplicates) = read_schema(&source).await?;
    refuse_unknown_schema(&source).await?;
    phases.schema_check_ms = check_replay(&entries, dest_path).await?;

    let schema_started = Instant::now();
    let dest = LocalDb::open(dest_path).await?;
    let mut dest_page_size = scalar(&dest, "PRAGMA page_size").await?.unwrap_or(0);
    if source_page_size > 0 && dest_page_size != source_page_size {
        dest.consume_query(&format!("PRAGMA page_size = {source_page_size}"))
            .await?;
        dest_page_size = scalar(&dest, "PRAGMA page_size").await?.unwrap_or(0);
    }
    let mut writer = Writer::default();
    for kind in ["table", "index"] {
        for entry in entries.iter().filter(|entry| entry.kind == kind) {
            if let Some(sql) = &entry.sql {
                writer.get(&dest).await?.execute(sql, ()).await?;
            }
        }
    }
    phases.schema_ms = millis(schema_started);

    let rows_started = Instant::now();
    let mut tables = Vec::new();
    let mut folds = Vec::new();
    let (mut rows, mut bytes) = (0u64, 0u64);
    let mut partial = false;
    let copied = |entry: &&SchemaEntry| {
        entry.kind == "table" && (mode == Mode::Copy || entry.name == MIGRATION_LEDGER)
    };
    'tables: for entry in entries.iter().filter(copied) {
        let table_started = Instant::now();
        let shape = TableShape::read(&source, &entry.name).await?;
        let insert = shape.insert_sql();
        let mut copy = TableCopy {
            table: entry.name.clone(),
            ..TableCopy::default()
        };
        let mut after = None;
        loop {
            let page = read_page(&source, &shape, after, limits).await?;
            let Some(&(last, _)) = page.rows.last() else {
                break;
            };
            after = Some(last);
            copy.rows += page.rows.len() as u64;
            copy.bytes += page.bytes;
            copy.pages += 1;
            rows += page.rows.len() as u64;
            bytes += page.bytes;
            let conn = writer.get(&dest).await?;
            write_page(
                conn,
                dest.concurrent_begin(),
                &insert,
                shape.rowid_alias,
                page.rows,
            )
            .await?;
            guard.check(rows)?;
            if dest.logical_log_bytes() >= limits.fold_bytes {
                // The fold drains the connection gate, so nothing of the
                // copy's may stay open across it.
                writer.release();
                let record = fold(&dest, bytes).await?;
                observe(CopyProgress::Fold(&record));
                folds.push(record);
                guard.check(rows)?;
            }
            if limits.stop_after_bytes.is_some_and(|stop| bytes >= stop) {
                partial = true;
                copy.ms = millis(table_started);
                observe(CopyProgress::Table(&copy));
                tables.push(copy);
                break 'tables;
            }
        }
        copy.ms = millis(table_started);
        observe(CopyProgress::Table(&copy));
        tables.push(copy);
    }
    phases.rows_ms = millis(rows_started);

    let finish_started = Instant::now();
    let mut sequences_raised = Vec::new();
    if !partial {
        sequences_raised = raise_sequences(&source, &dest, &mut writer).await?;
        for kind in ["view", "trigger"] {
            for entry in entries.iter().filter(|entry| entry.kind == kind) {
                if let Some(sql) = &entry.sql {
                    writer.get(&dest).await?.execute(sql, ()).await?;
                }
            }
        }
    }
    phases.finish_ms = millis(finish_started);

    writer.release();
    let final_fold = fold(&dest, bytes).await?;
    phases.final_fold_ms = final_fold.ms;
    folds.push(final_fold);
    phases.total_ms = millis(started);
    drop(dest);
    drop(source);

    Ok(CopyReport {
        limits,
        source_page_size,
        dest_page_size,
        source_fold,
        tables,
        folds,
        rows,
        bytes,
        sequences_raised,
        peak_resident_bytes: 0,
        readings: 0,
        partial,
        rehearsal: mode == Mode::Rehearse,
        schema_rows_dropped: schema_duplicates
            .iter()
            .map(|duplicate| duplicate.dropped_rowids.len())
            .sum(),
        schema_duplicates,
        source_set_bytes: db_set_size(source_path),
        dest_set_bytes: 0,
        phases,
    })
}

/// The destination connection the copy writes on, held across pages and
/// released before every fold.
///
/// A connection of its own rather than a pooled write, because foreign keys
/// must be off while tables are filled in an order that ignores their
/// references, and `PRAGMA foreign_keys` is a no-op inside a transaction. A
/// connection from `connect` is never pooled, so the pragma dies with it.
#[derive(Default)]
struct Writer {
    conn: Option<TrackedConnection>,
}

impl Writer {
    async fn get(&mut self, db: &LocalDb) -> DbResult<&TrackedConnection> {
        if self.conn.is_none() {
            let conn = db.connect().await?;
            conn.execute("PRAGMA foreign_keys = OFF", ()).await?;
            self.conn = Some(conn);
        }
        Ok(self.conn.as_ref().expect("set above"))
    }

    fn release(&mut self) {
        self.conn = None;
    }
}

/// Reads this process's resident memory against the cap, failing closed.
struct MemoryGuard<'a> {
    cap: u64,
    read: &'a (dyn Fn() -> Option<u64> + Send + Sync),
    peak: u64,
    readings: u64,
    unreadable: u32,
}

impl<'a> MemoryGuard<'a> {
    fn new(cap: u64, read: &'a (dyn Fn() -> Option<u64> + Send + Sync)) -> Self {
        Self {
            cap,
            read,
            peak: 0,
            readings: 0,
            unreadable: 0,
        }
    }

    fn check(&mut self, rows: u64) -> Result<(), ReclaimError> {
        self.readings += 1;
        let Some(resident) = (self.read)() else {
            self.unreadable += 1;
            // The first reading has no history to fall back on.
            if self.unreadable >= UNREADABLE_READINGS || self.readings == 1 {
                return Err(ReclaimError::MemoryUnreadable(self.unreadable));
            }
            return Ok(());
        };
        self.unreadable = 0;
        self.peak = self.peak.max(resident);
        if resident > self.cap {
            return Err(ReclaimError::MemoryCap {
                resident,
                cap: self.cap,
                rows,
            });
        }
        Ok(())
    }
}

async fn fold(db: &LocalDb, after_bytes: u64) -> Result<FoldRecord, ReclaimError> {
    // Nothing but this copy has the database open, so a short drain is enough.
    let report = db
        .checkpoint(
            3,
            Duration::from_millis(100),
            DrainBudget::fixed(Duration::from_millis(250)),
        )
        .await?;
    if let Some(error) = report.error {
        return Err(ReclaimError::Fold {
            path: db.path().display().to_string(),
            error,
        });
    }
    Ok(FoldRecord {
        after_bytes,
        log_bytes_before: report.log_bytes_before,
        log_bytes_after: report.log_bytes_after,
        ms: report.duration.as_millis() as u64,
    })
}

fn millis(since: Instant) -> u64 {
    since.elapsed().as_millis() as u64
}

async fn scalar(db: &LocalDb, sql: &str) -> DbResult<Option<i64>> {
    db.query_opt(sql.to_string(), (), |row| row.i64(0)).await
}

/// One `sqlite_master` entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct SchemaEntry {
    pub kind: String,
    pub name: String,
    pub table: String,
    pub sql: Option<String>,
}

/// Whether an entry belongs to the engine rather than the schema: SQLite's
/// own tables and Turso's internal ones, which the engine creates itself.
fn engine_owned(name: &str) -> bool {
    name.starts_with("sqlite_") || name.starts_with("__turso")
}

/// A `(type, name)` that `sqlite_master` lists more than once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaDuplicate {
    pub kind: String,
    pub name: String,
    /// The row kept: the last one.
    pub kept_rowid: i64,
    /// The earlier rows, dropped.
    pub dropped_rowids: Vec<i64>,
    /// A dropped row's table or SQL differs from the kept row's, beyond the
    /// whitespace [`normalized_sql`] ignores.
    pub texts_differ: bool,
}

/// The schema's entries in creation order, less the engine's own and with
/// one entry per `(type, name)`, the last listed; and the duplicates dropped.
async fn read_schema(db: &LocalDb) -> DbResult<(Vec<SchemaEntry>, Vec<SchemaDuplicate>)> {
    let rows = db
        .query_all(
            "SELECT rowid, type, name, tbl_name, sql FROM sqlite_master ORDER BY rowid",
            (),
            |row| {
                Ok((
                    row.i64(0)?,
                    SchemaEntry {
                        kind: row.text(1)?,
                        name: row.text(2)?,
                        table: row.text(3)?,
                        sql: row.opt_text(4)?,
                    },
                ))
            },
        )
        .await?;
    Ok(dedupe(
        rows.into_iter()
            .filter(|(_, entry)| !engine_owned(&entry.name))
            .collect(),
    ))
}

fn schema_key(entry: &SchemaEntry) -> (String, String) {
    (entry.kind.clone(), entry.name.clone())
}

/// `rows` (rowid, entry) in rowid order, one entry kept per `(type, name)`:
/// the last, at its own place in the order.
fn dedupe(rows: Vec<(i64, SchemaEntry)>) -> (Vec<SchemaEntry>, Vec<SchemaDuplicate>) {
    let last: BTreeMap<(String, String), usize> = rows
        .iter()
        .enumerate()
        .map(|(idx, (_, entry))| (schema_key(entry), idx))
        .collect();
    let mut duplicates: BTreeMap<(String, String), SchemaDuplicate> = BTreeMap::new();
    for (idx, (rowid, entry)) in rows.iter().enumerate() {
        let key = schema_key(entry);
        let kept = last[&key];
        if idx == kept {
            continue;
        }
        let (kept_rowid, kept_entry) = &rows[kept];
        let duplicate = duplicates.entry(key).or_insert_with(|| SchemaDuplicate {
            kind: entry.kind.clone(),
            name: entry.name.clone(),
            kept_rowid: *kept_rowid,
            dropped_rowids: Vec::new(),
            texts_differ: false,
        });
        duplicate.dropped_rowids.push(*rowid);
        duplicate.texts_differ |= !same_definition(entry, kept_entry);
    }
    let entries = rows
        .into_iter()
        .enumerate()
        .filter(|(idx, (_, entry))| last[&schema_key(entry)] == *idx)
        .map(|(_, (_, entry))| entry)
        .collect();
    (entries, duplicates.into_values().collect())
}

/// Replay `entries` into an empty scratch database beside `dest`, in the
/// copy's order, and compare what it holds with them: the copy will execute
/// the same statements, so a statement that fails or stores something else
/// here would fail or differ there, after the rows. Returns the check's wall
/// time; the scratch database is removed either way.
async fn check_replay(entries: &[SchemaEntry], dest: &Path) -> Result<u64, ReclaimError> {
    let started = Instant::now();
    let dir = dest
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let scratch = tempfile::Builder::new()
        .prefix(".reclaim-schema-check-")
        .tempdir_in(dir)
        .map_err(|error| {
            ReclaimError::Refused(format!(
                "could not create the schema check's scratch directory in {}: {error}",
                dir.display()
            ))
        })?;
    let db = LocalDb::open(scratch.path().join("schema-check.db")).await?;
    let mut writer = Writer::default();
    let mut problems = Vec::new();
    for kind in REPLAY_ORDER {
        for entry in entries.iter().filter(|entry| entry.kind == kind) {
            if let Some(sql) = &entry.sql {
                if let Err(error) = writer.get(&db).await?.execute(sql, ()).await {
                    problems.push(format!("{} {}: {error}", entry.kind, entry.name));
                }
            }
        }
    }
    writer.release();
    if problems.is_empty() {
        let (replayed, duplicates) = read_schema(&db).await?;
        problems = schema_differences(entries, &replayed);
        problems.extend(duplicates.iter().map(listed_twice));
    }
    drop(db);
    drop(scratch);
    if problems.is_empty() {
        Ok(millis(started))
    } else {
        Err(ReclaimError::Schema(problems))
    }
}

fn listed_twice(duplicate: &SchemaDuplicate) -> String {
    format!(
        "the copy lists {} {} {} times",
        duplicate.kind,
        duplicate.name,
        duplicate.dropped_rowids.len() + 1
    )
}

/// Refuse a schema holding something this copy would not carry: engine
/// state other than what the destination recreates for itself (sequence
/// backing tables, created with their tables, and the MVCC metadata table),
/// such as custom types, namespaces or incremental-view state; or a virtual
/// table, whose rows live elsewhere.
async fn refuse_unknown_schema(db: &LocalDb) -> Result<(), ReclaimError> {
    let rows = db
        .query_all("SELECT type, name, sql FROM sqlite_master", (), |row| {
            Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?))
        })
        .await?;
    let unknown: Vec<String> = rows
        .into_iter()
        .filter(|(kind, name, sql)| {
            let recreated = kind == "table"
                && (name.starts_with(SEQUENCE_BACKING_PREFIX) || name == MVCC_META_TABLE);
            let internal = name.starts_with("__turso") && !recreated;
            let virtual_table = sql.as_deref().is_some_and(|sql| {
                sql.trim_start()
                    .to_ascii_uppercase()
                    .starts_with("CREATE VIRTUAL")
            });
            internal || virtual_table
        })
        .map(|(kind, name, _)| format!("{kind} {name}"))
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(ReclaimError::Refused(format!(
        "the source holds schema this copy does not carry: {}",
        unknown.join(", ")
    )))
}

/// A table's columns, and whether one of them is the rowid.
struct TableShape {
    name: String,
    columns: Vec<String>,
    /// The table has an INTEGER PRIMARY KEY column, which is the rowid itself:
    /// writing that column writes the rowid, and naming both would conflict.
    rowid_alias: bool,
}

impl TableShape {
    async fn read(db: &LocalDb, table: &str) -> DbResult<Self> {
        let sql = format!(
            "SELECT name, type, pk FROM pragma_table_info('{}')",
            table.replace('\'', "''")
        );
        let columns = db
            .query_all(sql, (), |row| {
                Ok((
                    row.text(0)?,
                    row.opt_text(1)?.unwrap_or_default(),
                    row.i64(2)?,
                ))
            })
            .await?;
        let keys: Vec<&(String, String, i64)> =
            columns.iter().filter(|(_, _, pk)| *pk > 0).collect();
        let rowid_alias = keys.len() == 1 && keys[0].1.eq_ignore_ascii_case("INTEGER");
        if columns.is_empty() {
            return Err(DbError::internal(format!("table {table} has no columns")));
        }
        Ok(Self {
            name: table.to_string(),
            columns: columns.into_iter().map(|(name, _, _)| name).collect(),
            rowid_alias,
        })
    }

    fn column_list(&self) -> String {
        self.columns
            .iter()
            .map(|column| quote(column))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The first page's read, and every later page's, which starts after the
    /// last rowid read. Every row comes back as its rowid then its columns.
    fn select_sql(&self, after: bool) -> String {
        format!(
            "SELECT rowid, {} FROM {} {} ORDER BY rowid LIMIT ?",
            self.column_list(),
            quote(&self.name),
            if after { "WHERE rowid > ?" } else { "" }
        )
    }

    fn insert_sql(&self) -> String {
        let (names, count) = if self.rowid_alias {
            (self.column_list(), self.columns.len())
        } else {
            (
                format!("rowid, {}", self.column_list()),
                self.columns.len() + 1,
            )
        };
        format!(
            "INSERT INTO {}({names}) VALUES ({})",
            quote(&self.name),
            vec!["?"; count].join(", ")
        )
    }
}

struct Page {
    /// Each row's rowid and its columns' values.
    rows: Vec<(i64, Vec<Value>)>,
    bytes: u64,
}

fn value_bytes(value: &Value) -> u64 {
    match value {
        Value::Text(text) => text.len() as u64,
        Value::Blob(blob) => blob.len() as u64,
        _ => 8,
    }
}

/// The next page of `shape`'s rows after rowid `after`, in one short read: up
/// to `page_rows` rows, ending early once `page_bytes` of values are read.
async fn read_page(
    db: &LocalDb,
    shape: &TableShape,
    after: Option<i64>,
    limits: ReclaimLimits,
) -> DbResult<Page> {
    let sql = shape.select_sql(after.is_some());
    let mut params = Vec::with_capacity(2);
    if let Some(after) = after {
        params.push(Value::Integer(after));
    }
    params.push(Value::Integer(limits.page_rows as i64));
    let page_bytes = limits.page_bytes as u64;
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn.query(&sql, params).await?;
            let mut page = Page {
                rows: Vec::new(),
                bytes: 0,
            };
            while let Some(row) = rows.next().await? {
                let rowid = match row.get_value(0)? {
                    Value::Integer(rowid) => rowid,
                    other => {
                        return Err(DbError::internal(format!("rowid read as {other:?}")));
                    }
                };
                let mut values = Vec::with_capacity(row.column_count().saturating_sub(1));
                for idx in 1..row.column_count() {
                    let value = row.get_value(idx)?;
                    page.bytes += value_bytes(&value);
                    values.push(value);
                }
                page.rows.push((rowid, values));
                if page.bytes >= page_bytes {
                    break;
                }
            }
            Ok(page)
        })
    })
    .await
}

/// Write one page in one destination transaction, with each row at its rowid.
async fn write_page(
    conn: &TrackedConnection,
    begin: &str,
    insert: &str,
    rowid_alias: bool,
    rows: Vec<(i64, Vec<Value>)>,
) -> DbResult<()> {
    conn.execute(begin, ()).await?;
    let written: DbResult<()> = async {
        let mut statement = conn.prepare(insert).await?;
        for (rowid, values) in rows {
            let params = if rowid_alias {
                values
            } else {
                let mut params = Vec::with_capacity(values.len() + 1);
                params.push(Value::Integer(rowid));
                params.extend(values);
                params
            };
            statement.execute(params).await?;
        }
        drop(statement);
        conn.execute("COMMIT", ()).await?;
        Ok(())
    }
    .await;
    if written.is_err() {
        let _ = conn.execute("ROLLBACK", ()).await;
    }
    written
}

/// Each AUTOINCREMENT table's counter, from the `sqlite_sequence` mirror the
/// engine keeps of its implicit sequences. Empty when no table has one.
async fn sequences(db: &LocalDb) -> DbResult<BTreeMap<String, i64>> {
    let exists = db
        .query_opt(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_sequence'",
            (),
            |row| row.i64(0),
        )
        .await?
        .is_some();
    if !exists {
        return Ok(BTreeMap::new());
    }
    Ok(db
        .query_all("SELECT name, seq FROM sqlite_sequence", (), |row| {
            Ok((row.text(0)?, row.i64(1)?))
        })
        .await?
        .into_iter()
        .collect())
}

/// Raise each destination counter that sits below the source's: a table whose
/// newest rows were deleted keeps a counter above its largest rowid, and the
/// copy's explicit rowids only carried the counter as far as that rowid.
///
/// On the pinned engine the counter is an implicit sequence and
/// `sqlite_sequence` mirrors it, so the counter is set with `setval` on the
/// sequence rather than by writing the mirror, which the engine would
/// overwrite.
async fn raise_sequences(
    source: &LocalDb,
    dest: &LocalDb,
    writer: &mut Writer,
) -> DbResult<Vec<(String, i64)>> {
    let wanted = sequences(source).await?;
    let have = sequences(dest).await?;
    let mut raised = Vec::new();
    for (table, seq) in wanted {
        if have.get(&table).is_some_and(|current| *current >= seq) {
            continue;
        }
        let name = format!("{AUTOINCREMENT_SEQUENCE_PREFIX}{table}").replace('\'', "''");
        let conn = writer.get(dest).await?;
        let mut rows = conn
            .query(format!("SELECT setval('{name}', {seq})"), ())
            .await?;
        while rows.next().await?.is_some() {}
        drop(rows);
        raised.push((table, seq));
    }
    Ok(raised)
}

/// One table compared across the two files.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TableDigest {
    pub table: String,
    pub source_rows: u64,
    pub dest_rows: u64,
    pub source_max_rowid: Option<i64>,
    pub dest_max_rowid: Option<i64>,
    /// SHA-256 over every row in rowid order: the rowid, then each value
    /// with its type.
    pub source_sha256: String,
    pub dest_sha256: String,
}

impl TableDigest {
    pub fn matches(&self) -> bool {
        self.source_rows == self.dest_rows
            && self.source_max_rowid == self.dest_max_rowid
            && self.source_sha256 == self.dest_sha256
    }
}

/// `PRAGMA foreign_key_check` on both files, where the engine has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ForeignKeyCheck {
    /// Both files report the same violations (usually none).
    Same {
        violations: usize,
    },
    Different {
        source: usize,
        dest: usize,
    },
    /// The engine refused the pragma; the per-table digests still show the
    /// copy carries every reference the source did.
    Unsupported {
        error: String,
    },
    /// A rehearsal, which copies no rows to check.
    NotRun,
}

/// Wall time of each phase of the verification, in milliseconds.
#[derive(Debug, Clone, Default, Serialize)]
pub struct VerifyPhases {
    pub schema_ms: u64,
    /// Both files read whole, side by side.
    pub digest_ms: u64,
    pub integrity_check_ms: u64,
    pub foreign_key_check_ms: u64,
    pub migrations_ms: u64,
    pub retention_audit_ms: u64,
    pub total_ms: u64,
}

/// What [`verify`] found.
#[derive(Debug, Clone, Serialize)]
pub struct Verification {
    /// A [`rehearse`] verification: no row digests or `foreign_key_check`.
    pub rehearsal: bool,
    /// Duplicates in the source's `sqlite_master`, compared as their last row.
    pub source_schema_duplicates: Vec<SchemaDuplicate>,
    /// Schema entries present on one side only, or different between them,
    /// and any entry the copy lists twice.
    pub schema_differences: Vec<String>,
    /// AUTOINCREMENT counters that differ.
    pub sequence_differences: Vec<String>,
    pub tables: Vec<TableDigest>,
    /// The copy's `integrity_check` rows: `["ok"]` when clean.
    pub integrity_check: Vec<String>,
    pub foreign_key_check: ForeignKeyCheck,
    /// Migrations the source has not applied, and those the migration runner
    /// applied to the copy. They match when the copy is at the source's
    /// schema; both are empty when the source was at this binary's head.
    pub source_pending_migrations: Vec<String>,
    pub migrations_applied: Vec<String>,
    /// The migration runner failed on the copy.
    pub migration_error: Option<String>,
    /// Tables in the copy with no retention rule. Reported, not a failure: a
    /// missing rule says nothing about whether the copy is faithful, and
    /// `every_table_has_a_retention_rule` is where it fails.
    pub unruled_tables: Vec<String>,
    pub phases: VerifyPhases,
}

impl Verification {
    /// Why the copy may not replace the source; empty when it may.
    pub fn failures(&self) -> Vec<String> {
        let mut failures = Vec::new();
        if !self.schema_differences.is_empty() {
            failures.push(format!(
                "schema differs: {}",
                self.schema_differences.join("; ")
            ));
        }
        if !self.sequence_differences.is_empty() {
            failures.push(format!(
                "AUTOINCREMENT counters differ: {}",
                self.sequence_differences.join("; ")
            ));
        }
        for table in self.tables.iter().filter(|table| !table.matches()) {
            failures.push(format!(
                "{}: {} rows (max rowid {:?}) in the source, {} (max rowid {:?}) in the copy, \
                 digests {}",
                table.table,
                table.source_rows,
                table.source_max_rowid,
                table.dest_rows,
                table.dest_max_rowid,
                if table.source_sha256 == table.dest_sha256 {
                    "equal"
                } else {
                    "differ"
                }
            ));
        }
        if self.integrity_check != ["ok"] {
            failures.push(format!(
                "integrity_check: {}",
                self.integrity_check.join("; ")
            ));
        }
        if let ForeignKeyCheck::Different { source, dest } = &self.foreign_key_check {
            failures.push(format!(
                "foreign_key_check: {source} violations in the source, {dest} in the copy"
            ));
        }
        if let Some(error) = &self.migration_error {
            failures.push(format!("the migration runner failed on the copy: {error}"));
        } else if self.migrations_applied != self.source_pending_migrations {
            failures.push(format!(
                "the migration runner applied {:?} to the copy, where the source was missing {:?}",
                self.migrations_applied, self.source_pending_migrations
            ));
        }
        failures
    }

    pub fn passed(&self) -> bool {
        self.failures().is_empty()
    }
}

/// Compare a copy made by [`copy_compacted`] against its source, and check the
/// copy on its own. Reads both files whole.
///
/// The migration check runs the migration runner on the copy, so it comes last
/// among the comparisons: when the source was behind this binary, the copy
/// leaves verification at this binary's schema, as the runner would have left
/// it at the next boot.
pub async fn verify(source_path: &Path, dest_path: &Path) -> Result<Verification, ReclaimError> {
    verify_in(source_path, dest_path, Mode::Copy).await
}

async fn verify_in(
    source_path: &Path,
    dest_path: &Path,
    mode: Mode,
) -> Result<Verification, ReclaimError> {
    let started = Instant::now();
    let mut phases = VerifyPhases::default();
    let source = LocalDb::open(source_path).await?;
    let dest = LocalDb::open(dest_path).await?;

    let phase = Instant::now();
    let (source_schema, source_schema_duplicates) = read_schema(&source).await?;
    let (dest_schema, dest_duplicates) = read_schema(&dest).await?;
    let mut schema_differences = schema_differences(&source_schema, &dest_schema);
    schema_differences.extend(dest_duplicates.iter().map(listed_twice));
    let sequence_differences = differences(
        &sequences(&source).await?.into_iter().collect::<Vec<_>>(),
        &sequences(&dest).await?.into_iter().collect::<Vec<_>>(),
        |(table, seq)| format!("{table} = {seq}"),
    );
    phases.schema_ms = millis(phase);

    let phase = Instant::now();
    let mut tables = Vec::new();
    let digested = |entry: &&SchemaEntry| entry.kind == "table" && mode == Mode::Copy;
    for entry in source_schema.iter().filter(digested) {
        let shape = TableShape::read(&source, &entry.name).await?;
        let limits = ReclaimLimits::with_cap(u64::MAX);
        let ((source_rows, source_max_rowid, source_sha256), dest_digest) = tokio::try_join!(
            digest(&source, &shape, limits),
            digest(&dest, &shape, limits)
        )?;
        let (dest_rows, dest_max_rowid, dest_sha256) = dest_digest;
        tables.push(TableDigest {
            table: entry.name.clone(),
            source_rows,
            dest_rows,
            source_max_rowid,
            dest_max_rowid,
            source_sha256,
            dest_sha256,
        });
    }
    phases.digest_ms = millis(phase);

    let phase = Instant::now();
    let integrity_check = pragma_rows(&dest, "PRAGMA integrity_check").await?;
    phases.integrity_check_ms = millis(phase);

    let phase = Instant::now();
    let foreign_key_check = match mode {
        Mode::Rehearse => ForeignKeyCheck::NotRun,
        Mode::Copy => foreign_key_check(&source, &dest).await,
    };
    phases.foreign_key_check_ms = millis(phase);

    let phase = Instant::now();
    let runner = MigrationRunner::new(TURSO_MIGRATIONS.to_vec());
    let source_pending_migrations = runner.pending(&source).await?;
    let (migrations_applied, migration_error) = match runner.run(&dest).await {
        Ok(applied) => (applied, None),
        Err(error) => (Vec::new(), Some(error.to_string())),
    };
    phases.migrations_ms = millis(phase);

    let phase = Instant::now();
    let unruled_tables = audit_tables(&dest).await?.unruled;
    phases.retention_audit_ms = millis(phase);
    phases.total_ms = millis(started);

    Ok(Verification {
        rehearsal: mode == Mode::Rehearse,
        source_schema_duplicates,
        schema_differences,
        sequence_differences,
        tables,
        integrity_check,
        foreign_key_check,
        source_pending_migrations,
        migrations_applied,
        migration_error,
        unruled_tables,
        phases,
    })
}

async fn foreign_key_check(source: &LocalDb, dest: &LocalDb) -> ForeignKeyCheck {
    match (
        pragma_rows(source, "PRAGMA foreign_key_check").await,
        pragma_rows(dest, "PRAGMA foreign_key_check").await,
    ) {
        (Ok(mut source), Ok(mut dest)) => {
            source.sort();
            dest.sort();
            if source == dest {
                ForeignKeyCheck::Same {
                    violations: source.len(),
                }
            } else {
                ForeignKeyCheck::Different {
                    source: source.len(),
                    dest: dest.len(),
                }
            }
        }
        (Err(error), _) | (_, Err(error)) => ForeignKeyCheck::Unsupported {
            error: error.to_string(),
        },
    }
}

/// `sql` with the whitespace outside quotes removed.
///
/// The engine stores its own rendering of a CREATE statement rather than the
/// text it was given, and two renderings of one statement differ in
/// whitespace alone: the source stores `CHECK(x)` and `REFERENCES t(id)` where
/// executing that same text again stores `CHECK (x)` and `REFERENCES t (id)`,
/// and a trigger body's statements on separate lines in one and on one line in
/// the other. The copy executes the source's text verbatim, so whitespace is
/// the only thing the rendering can change. Quoted literals and identifiers
/// are kept byte for byte, so a changed string or name still differs.
pub(crate) fn normalized_sql(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut quote: Option<char> = None;
    for ch in sql.chars() {
        if let Some(open) = quote {
            out.push(ch);
            if ch == open {
                quote = None;
            }
            continue;
        }
        if ch.is_whitespace() {
            continue;
        }
        if matches!(ch, '\'' | '"' | '`' | '[') {
            quote = Some(if ch == '[' { ']' } else { ch });
        }
        out.push(ch);
    }
    out
}

/// Both entries name one table and have the same SQL modulo whitespace.
fn same_definition(a: &SchemaEntry, b: &SchemaEntry) -> bool {
    a.table == b.table
        && a.sql.as_deref().map(normalized_sql) == b.sql.as_deref().map(normalized_sql)
}

/// Schema entries on one side only, and entries whose table or SQL differ
/// beyond the engine's whitespace (see [`normalized_sql`]). Both sides are
/// deduplicated already ([`read_schema`]).
fn schema_differences(source: &[SchemaEntry], dest: &[SchemaEntry]) -> Vec<String> {
    let source: BTreeMap<(String, String), &SchemaEntry> = source
        .iter()
        .map(|entry| (schema_key(entry), entry))
        .collect();
    let dest: BTreeMap<(String, String), &SchemaEntry> = dest
        .iter()
        .map(|entry| (schema_key(entry), entry))
        .collect();
    let mut out = Vec::new();
    for ((kind, name), entry) in &source {
        match dest.get(&(kind.clone(), name.clone())) {
            None => out.push(format!("only in the source: {kind} {name}")),
            Some(copy) if !same_definition(entry, copy) => out.push(format!(
                "{kind} {name} differs: the source has {:?} on {}, the copy {:?} on {}",
                entry.sql, entry.table, copy.sql, copy.table
            )),
            Some(_) => {}
        }
    }
    for (kind, name) in dest.keys() {
        if !source.contains_key(&(kind.clone(), name.clone())) {
            out.push(format!("only in the copy: {kind} {name}"));
        }
    }
    out
}

/// Entries on one side only, named by `label`. An entry changed between the
/// sides shows as one on each.
fn differences<T: Ord + Clone>(
    source: &[T],
    dest: &[T],
    label: impl Fn(&T) -> String,
) -> Vec<String> {
    let source: BTreeSet<&T> = source.iter().collect();
    let dest: BTreeSet<&T> = dest.iter().collect();
    let mut out: Vec<String> = source
        .difference(&dest)
        .map(|entry| format!("only in the source: {}", label(entry)))
        .collect();
    out.extend(
        dest.difference(&source)
            .map(|entry| format!("only in the copy: {}", label(entry))),
    );
    out
}

/// A table's row count, largest rowid, and content digest, read in pages.
async fn digest(
    db: &LocalDb,
    shape: &TableShape,
    limits: ReclaimLimits,
) -> DbResult<(u64, Option<i64>, String)> {
    let mut hasher = Sha256::new();
    let mut rows = 0u64;
    let mut after = None;
    loop {
        let page = read_page(db, shape, after, limits).await?;
        let Some(&(last, _)) = page.rows.last() else {
            break;
        };
        after = Some(last);
        rows += page.rows.len() as u64;
        for (rowid, values) in &page.rows {
            hasher.update(rowid.to_le_bytes());
            for value in values {
                hash_value(&mut hasher, value);
            }
        }
    }
    Ok((rows, after, format!("{:x}", hasher.finalize())))
}

/// Feed one value to the digest with a type tag and, for text and blobs, a
/// length, so neither a change of type nor a shifted boundary between two
/// values hashes the same.
fn hash_value(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::Null => hasher.update([0u8]),
        Value::Integer(integer) => {
            hasher.update([1u8]);
            hasher.update(integer.to_le_bytes());
        }
        Value::Real(real) => {
            hasher.update([2u8]);
            hasher.update(real.to_bits().to_le_bytes());
        }
        Value::Text(text) => {
            hasher.update([3u8]);
            hasher.update((text.len() as u64).to_le_bytes());
            hasher.update(text.as_bytes());
        }
        Value::Blob(blob) => {
            hasher.update([4u8]);
            hasher.update((blob.len() as u64).to_le_bytes());
            hasher.update(blob);
        }
    }
}

/// Every row a whole-database pragma returns, each rendered as one string.
/// On a connection of its own, because a whole-file check outlasts the
/// pooled read bound on a large database.
async fn pragma_rows(db: &LocalDb, sql: &str) -> DbResult<Vec<String>> {
    let conn = db.connect().await?;
    let mut rows = conn.query(sql, ()).await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        let mut fields = Vec::with_capacity(row.column_count());
        for idx in 0..row.column_count() {
            fields.push(match row.get_value(idx)? {
                Value::Null => "NULL".to_string(),
                Value::Integer(integer) => integer.to_string(),
                Value::Real(real) => real.to_string(),
                Value::Text(text) => text,
                Value::Blob(blob) => format!("<blob {} B>", blob.len()),
            });
        }
        out.push(fields.join("|"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
