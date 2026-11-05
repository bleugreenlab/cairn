//! The daily storage census: what each table holds and how fast it grows,
//! measured on the live database in a few seconds of short reads.
//!
//! The clone census (`examples/db_census.rs`) sums every row of every table and
//! takes minutes, so it runs once, on a copy. This module runs every day on the
//! live file, so it never reads a table whole and never counts rows:
//!
//! - **Bounds.** `min(rowid)` and `max(rowid)` are seeks to the ends of the
//!   table's b-tree. The high-water's advance between two censuses is the rows
//!   inserted in between, whatever has since been deleted.
//! - **Rows and bytes.** A few short windows of consecutive rows, read at
//!   spread-out rowids, each measure two things in their stretch of the rowid
//!   space: how densely live rows occupy it, and their mean payload. Scaling
//!   each stratum by its width and summing gives the table's live rows and
//!   payload (a stratified estimate). A table that fits in its first window is
//!   read whole and reported exactly.
//! - **History.** The bytes the history tier moves per event row, recalibrated
//!   on a sample of issues resolved 30-60 days ago: the ones the hot budget
//!   evicts next ([`HistoryCalibration`]).
//! - **Archive.** The size of the history tier's archive directory.
//!
//! Payload is defined once, by [`payload_expr`], so the clone census and this
//! one measure the same thing: the byte length of every text and blob value
//! plus a small per-row constant, without indexes or b-tree slack.
//!
//! Every read is a single statement or one short read transaction, and every
//! one is timed; the report names the slowest. A census stops measuring at its
//! time budget and names the tables it did not reach rather than overrunning.
//!
//! Each census is stored as one row of `storage_census` ([`record`]), and the
//! newest [`KEPT_CENSUSES`] are kept. Successive rows give the 90-day
//! projection ([`Projection`]), and the newest row answers "how many rows" for
//! readers that must not count ([`latest_table_rows`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};

use crate::storage::retention::{retention_for, DEFAULT_BUDGET_MB};
use crate::storage::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use crate::turso::Value;

const DAY_S: i64 = 86_400;

/// How often the census runs.
pub const INTERVAL: Duration = Duration::from_secs(DAY_S as u64);

/// Censuses kept in `storage_census`: more than a year of daily rows, about
/// 50 KB each.
pub const KEPT_CENSUSES: i64 = 400;

/// The longest a single census read should take. Reads past it are counted in
/// [`CensusReport::reads_over_target`]; none is cut.
pub const READ_TARGET: Duration = Duration::from_millis(250);

/// A census's whole time budget. Every table is surveyed first, then the
/// largest are sampled, so a census that reaches it leaves the smallest
/// tables measured by their survey alone.
///
/// Eight seconds against an acceptance of ten: the budget is checked between
/// reads, so a census overruns it by its last read and the archive walk.
pub const TIME_BUDGET: Duration = Duration::from_secs(8);

/// The part of [`TIME_BUDGET`] held back for the history calibration.
const HISTORY_BUDGET: Duration = Duration::from_millis(2_000);

// A read's cost is the bytes of the rows it touches: the engine assembles
// every row it reads whole, overflow pages included, and on a cold cache that
// is random I/O (about 1 ms per 4 KiB page on the 11-02 clone). So windows and
// tables are budgeted in bytes, not rows.

/// Windows per table at most, the first at the table's lowest rowid.
const STRATA: i64 = 12;
/// Windows per sampled table at least.
const MIN_STRATA: i64 = 3;
const MIN_WINDOW_ROWS: usize = 1;
const MAX_WINDOW_ROWS: usize = 64;
/// The payload one window aims to read; wide rows get short windows.
const WINDOW_BYTES: f64 = 128.0 * 1024.0;
/// The payload one table's windows aim to read.
const TABLE_SAMPLE_BYTES: f64 = 512.0 * 1024.0;

/// Resolution ages (days) the history calibration samples.
pub const HISTORY_BAND_DAYS: (i64, i64) = (30, 60);
/// How far back an issue's creation may lie for the calibration to find it.
const BAND_LOOKBACK_DAYS: i64 = 75;
/// Issue rows the calibration probes for band members, at most.
const HISTORY_PROBES: usize = 192;
const HISTORY_MAX_ISSUES: usize = 24;
/// The calibration stops once it has read this much history.
const HISTORY_BYTES: i64 = 4 * 1024 * 1024;
/// Events per calibration read: an uncompressed event can run to tens of
/// kilobytes, and a cold page of 512 of them took 454 ms on the 11-02 clone.
const EVENT_PAGE_ROWS: i64 = 128;
/// Ids bound per `IN` list.
const ID_CHUNK: usize = 64;

/// Issue statuses after which the archival sweep treats an issue's
/// executions as resolved history.
const RESOLVED_STATUSES: &[&str] = &["merged", "closed"];

const PROJECTION_HORIZON_DAYS: f64 = 90.0;
/// Censuses (the current one included) a projection needs.
pub const PROJECTION_MIN_CENSUSES: usize = 3;
/// Censuses older than this do not enter the projection's fit.
const PROJECTION_LOOKBACK_S: i64 = 30 * DAY_S;
const PROJECTION_MAX_CENSUSES: i64 = 31;
/// Tables the projection names by growth.
const PROJECTION_FASTEST: usize = 10;

const BYTES_PER_MB: f64 = 1_000_000.0;

/// The history tier's archive root: `<cairn home>/archive`.
pub fn default_archive_dir() -> PathBuf {
    cairn_common::paths::cairn_home().join("archive")
}

/// Data directories a live Cairn keeps its database in, relative to `$HOME`.
const LIVE_DIRS: &[&str] = &[
    ".cairn",
    ".cairn-dev",
    "Library/Application Support/com.cairn.desktop",
];

/// Names a sealed test process's seal root (`cairn_common::test_seal`).
const SEALED_ENV: &str = "CAIRN_TEST_SEALED";

/// Every directory a live Cairn could keep its database in, as this process
/// can name them. A test binary has sealed its `HOME` away by the time it
/// asks, so its seal's git ceilings, which list the homes it moved away from,
/// are read too. No `HOME` means nothing can be judged, so it is an error.
fn live_directories(
    home: Option<PathBuf>,
    cairn_home: Option<PathBuf>,
    git_ceilings: Option<std::ffi::OsString>,
) -> Result<Vec<PathBuf>, String> {
    let home = home.filter(|home| !home.as_os_str().is_empty()).ok_or(
        "HOME is unset, so whether this path is a live Cairn data directory cannot be judged",
    )?;
    let mut live: Vec<PathBuf> = LIVE_DIRS.iter().map(|dir| home.join(dir)).collect();
    live.extend(cairn_home.filter(|path| !path.as_os_str().is_empty()));
    if let Some(ceilings) = git_ceilings {
        live.extend(std::env::split_paths(&ceilings));
    }
    Ok(live)
}

/// Why `path` may not be opened given these live directories: its parent is one
/// of them, it sits in one's archive, or its parent cannot be resolved. A
/// directory that does not exist cannot hold what `path` might be.
fn live_directory_refusal(path: &Path, live: &[PathBuf]) -> Result<(), String> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let dir = std::fs::canonicalize(parent).map_err(|error| {
        format!(
            "{}: cannot resolve its directory, so it may be a live Cairn data directory: {error}",
            path.display()
        )
    })?;
    for live in live
        .iter()
        .filter_map(|dir| std::fs::canonicalize(dir).ok())
    {
        if dir == live {
            return Err(format!(
                "{} is in a live Cairn data directory; clone it first",
                path.display()
            ));
        }
        if dir.starts_with(live.join("archive")) {
            return Err(format!("{} is in a live Cairn archive", path.display()));
        }
    }
    Ok(())
}

/// Refuse a database a clone-only tool must not open: one in a live Cairn data
/// directory or its archive. Fails closed when that cannot be judged. Deeper
/// paths such as `~/.cairn/scratch/...` hold no live database and pass.
pub fn refuse_live_directory(path: &Path) -> Result<(), String> {
    let live = live_directories(
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from),
        std::env::var_os("CAIRN_HOME").map(PathBuf::from),
        std::env::var_os(SEALED_ENV).and_then(|_| std::env::var_os("GIT_CEILING_DIRECTORIES")),
    )?;
    live_directory_refusal(path, &live)
}

/// A column, as the payload estimate needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// Declared integer, real or numeric: counted as four bytes when set.
    pub numeric: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, declared_type: &str) -> Self {
        let declared = declared_type.to_ascii_uppercase();
        Self {
            name: name.into(),
            numeric: declared.contains("INT")
                || declared.contains("REAL")
                || declared.contains("NUM"),
        }
    }
}

/// Quote an SQL identifier, doubling embedded quotes.
pub fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// One row's stored payload: the byte length of every text and blob value
/// (cast to blob, since `length()` of text counts characters), four bytes per
/// non-null number, and a fixed record header. Indexes and b-tree slack are
/// not in it.
pub fn payload_expr(columns: &[Column]) -> String {
    let mut terms = vec![(columns.len() + 4).to_string()];
    for column in columns {
        let quoted = quote_identifier(&column.name);
        if column.numeric {
            terms.push(format!("(CASE WHEN {quoted} IS NULL THEN 0 ELSE 4 END)"));
        } else {
            terms.push(format!("coalesce(length(CAST({quoted} AS BLOB)), 0)"));
        }
    }
    terms.join(" + ")
}

/// One table in one census.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableCensus {
    pub table: String,
    /// The table's retention rule, as [`crate::storage::retention::Retention::summary`] states it.
    pub rule: String,
    pub budget_mb: u32,
    pub low_water: Option<i64>,
    /// `max(rowid)`.
    pub high_water: Option<i64>,
    /// Live rows, estimated unless `exact`.
    pub rows: i64,
    /// Payload bytes, estimated unless `exact`.
    pub bytes: i64,
    pub exact: bool,
    pub sampled_rows: u32,
    /// Mean payload of the newest stratum's rows: what a new row costs.
    pub new_row_bytes: Option<f64>,
    pub over_budget: bool,
    /// The rowid high-water's advance per day since the previous census:
    /// rows inserted, plus rowids the allocator skipped.
    pub rowids_per_day: Option<f64>,
    /// The change in live rows per day since the previous census.
    pub rows_per_day: Option<f64>,
    /// Rowids vacated per day since the previous census: the high-water's
    /// advance less the change in live rows. For a table an evictor deletes
    /// from, that is its deletions, and beside a growing backlog a flat line
    /// here is a starved rule. It also counts rowids the allocator skipped:
    /// this engine advances the allocator on every ignored conflicting
    /// `INSERT OR IGNORE`, so a table with a hot "ensure" climbs its
    /// high-water while its rows stay put.
    pub vacated_per_day: Option<f64>,
    /// Why the table went unmeasured: a declared `rowid` column, the time
    /// budget, or a read error.
    pub error: Option<String>,
    /// Windows read: 1 for a table measured by its survey alone.
    pub strata: u32,
    /// Today's sample alone, before pooling with earlier censuses; `rows` and
    /// `bytes` are the pooled figures. Absent for an exact table.
    pub day_rows: Option<i64>,
    pub day_bytes: Option<i64>,
    /// The decayed sample weight behind the pooled figures.
    pub pool_weight: f64,
    /// Time spent measuring the table.
    pub measure_ms: u64,
}

impl TableCensus {
    fn measured(&self) -> bool {
        self.error.is_none()
    }

    /// What one more row of this table costs, for growth in bytes.
    fn row_bytes(&self) -> f64 {
        self.new_row_bytes.unwrap_or(if self.rows > 0 {
            self.bytes as f64 / self.rows as f64
        } else {
            0.0
        })
    }
}

/// Bytes the history tier moves per event row, measured on issues resolved in
/// [`HISTORY_BAND_DAYS`]. History is an issue's events plus its tool
/// invocations, turn embeddings and skyline rows. The hot budget multiplies an
/// execution's event rows by this to rank evictions without reading the bytes
/// it moves. Unlike the clone census's definition it leaves out stream
/// chunks: the evictor deletes a finalized stream a day after it ends, so
/// chunks still present at 30 days measure the evictor's lag, not history.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HistoryCalibration {
    pub band_days: (i64, i64),
    /// Estimated issues created within the lookback and resolved in the band.
    pub band_issues: usize,
    pub sampled_issues: usize,
    pub event_rows: i64,
    pub history_bytes: i64,
    /// Pooled with the previous censuses' constants like a sampled table's
    /// estimates, weighted by event rows: one day's sample is a few issues.
    pub bytes_per_event_row: f64,
    /// Today's sample alone.
    pub day_bytes_per_event_row: f64,
    /// The decayed event-row weight behind the pooled constant.
    pub pool_weight: f64,
    /// Standard error of today's ratio over its sampled issues, relative to it.
    pub relative_standard_error: Option<f64>,
}

impl HistoryCalibration {
    /// Pool today's constant with the previous census's.
    fn pool(&mut self, before: Option<&HistoryCalibration>) {
        self.day_bytes_per_event_row = self.bytes_per_event_row;
        let weight = self.event_rows as f64;
        self.pool_weight = weight;
        let Some(before) = before.filter(|b| b.pool_weight > 0.0) else {
            return;
        };
        let carried = POOL_DECAY * before.pool_weight;
        self.bytes_per_event_row = (weight * self.day_bytes_per_event_row
            + carried * before.bytes_per_event_row)
            / (weight + carried);
        self.pool_weight = weight + carried;
    }
}

/// The archive directory's size.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ArchiveCensus {
    pub path: String,
    pub exists: bool,
    pub bytes: u64,
    pub files: u64,
    pub error: Option<String>,
}

/// One retention class's share of a projection.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RuleProjection {
    pub bytes: i64,
    pub projected_bytes: i64,
    pub bytes_per_day: f64,
}

/// One table's projected growth.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableGrowth {
    pub table: String,
    pub rule: String,
    pub bytes_per_day: f64,
    pub projected_bytes: i64,
}

/// Payload in [`PROJECTION_HORIZON_DAYS`], from the trend in each table's live
/// rows across the stored censuses. A table's growth is the slope of its live
/// rows (what its rule keeps, not what it inserts) times what a new row
/// costs. A table bounded by its rule shows a flat trend however fast it
/// inserts; `by_rule` says which classes of rule the remaining growth sits in.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Projection {
    pub censuses: usize,
    pub span_days: f64,
    pub horizon_days: f64,
    pub payload_bytes: i64,
    pub projected_bytes: i64,
    pub bytes_per_day: f64,
    pub mb_per_billion_tokens: Option<f64>,
    /// Keyed by [`crate::storage::retention::Retention::kind`].
    pub by_rule: BTreeMap<String, RuleProjection>,
    pub fastest: Vec<TableGrowth>,
}

/// One census.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CensusReport {
    /// Unix seconds.
    pub taken_at: i64,
    pub duration_ms: u64,
    pub reads: u64,
    pub reads_over_target: u64,
    pub slowest_read_ms: u64,
    pub slowest_read: String,
    /// The slowest reads, slowest first.
    pub slow_reads: Vec<SlowRead>,
    pub file_bytes: u64,
    pub log_bytes: u64,
    pub page_size: Option<i64>,
    pub page_count: Option<i64>,
    pub freelist_count: Option<i64>,
    /// Estimated payload across every measured table.
    pub payload_bytes: i64,
    pub tables: Vec<TableCensus>,
    pub over_budget: Vec<String>,
    pub unmeasured: Vec<String>,
    pub history: Option<HistoryCalibration>,
    pub archive: ArchiveCensus,
    /// Tokens per day since the previous census, or over the last day.
    pub tokens_per_day: Option<f64>,
    pub projection: Option<Projection>,
    /// Every rule that deletes rows (an evictor rule or a bespoke owner's), with
    /// its tables' backlog over budget and the rowids they vacated since the
    /// previous census.
    pub deleting_rules: Vec<RuleCensus>,
}

/// One deleting rule across its tables.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RuleCensus {
    /// As [`crate::storage::retention::Retention::summary`] states it.
    pub rule: String,
    pub tables: Vec<String>,
    pub rows: i64,
    pub bytes: i64,
    /// Bytes past the tables' budgets, summed over the tables over theirs.
    pub over_budget_bytes: i64,
    /// Summed over the tables; see [`TableCensus::vacated_per_day`].
    pub vacated_per_day: Option<f64>,
}

/// Group the tables whose rules delete rows by rule.
fn deleting_rules(tables: &[TableCensus]) -> Vec<RuleCensus> {
    let mut rules: BTreeMap<String, RuleCensus> = BTreeMap::new();
    for table in tables.iter().filter(|t| t.measured()) {
        if !matches!(rule_kind(&table.table), "evict" | "owned_by") {
            continue;
        }
        let rule = rules
            .entry(table.rule.clone())
            .or_insert_with(|| RuleCensus {
                rule: table.rule.clone(),
                ..RuleCensus::default()
            });
        rule.tables.push(table.table.clone());
        rule.rows += table.rows;
        rule.bytes += table.bytes;
        let budget = (f64::from(table.budget_mb) * BYTES_PER_MB) as i64;
        rule.over_budget_bytes += (table.bytes - budget).max(0);
        if let Some(vacated) = table.vacated_per_day {
            *rule.vacated_per_day.get_or_insert(0.0) += vacated;
        }
    }
    rules.into_values().collect()
}

impl CensusReport {
    /// A compact form for a log line or profiler record: the headline, the
    /// tables over budget, the `top` largest tables, the history constant, the
    /// archive and the projection. The stored row holds every table.
    pub fn summary(&self, top: usize) -> Json {
        let mut largest: Vec<&TableCensus> = self.tables.iter().filter(|t| t.measured()).collect();
        largest.sort_by_key(|t| std::cmp::Reverse(t.bytes));
        let table = |t: &TableCensus| {
            json!({
                "table": t.table,
                "rows": t.rows,
                "bytes": t.bytes,
                "budget_mb": t.budget_mb,
                "rowids_per_day": t.rowids_per_day,
                "rows_per_day": t.rows_per_day,
            })
        };
        json!({
            "taken_at": self.taken_at,
            "duration_ms": self.duration_ms,
            "reads": self.reads,
            "reads_over_target": self.reads_over_target,
            "slowest_read_ms": self.slowest_read_ms,
            "slowest_read": self.slowest_read,
            "file_bytes": self.file_bytes,
            "log_bytes": self.log_bytes,
            "payload_bytes": self.payload_bytes,
            "tables": self.tables.len(),
            "unmeasured": self.unmeasured,
            "over_budget": self
                .tables
                .iter()
                .filter(|t| t.over_budget)
                .map(table)
                .collect::<Vec<_>>(),
            "largest": largest.into_iter().take(top).map(table).collect::<Vec<_>>(),
            "history": self.history,
            "archive": self.archive,
            "tokens_per_day": self.tokens_per_day,
            "projection": self.projection,
            "deleting_rules": self.deleting_rules,
        })
    }
}

/// Reads a census reports by name.
const SLOW_READS_KEPT: usize = 10;

/// Times every census read and keeps the slowest few.
struct Clock {
    started: Instant,
    reads: u64,
    over_target: u64,
    slowest: Duration,
    slowest_what: String,
    /// The slowest reads, slowest first.
    slow: Vec<SlowRead>,
}

impl Clock {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            reads: 0,
            over_target: 0,
            slowest: Duration::ZERO,
            slowest_what: String::new(),
            slow: Vec::new(),
        }
    }

    async fn time<T>(
        &mut self,
        what: impl FnOnce() -> String,
        read: impl Future<Output = DbResult<T>>,
    ) -> DbResult<T> {
        let started = Instant::now();
        let result = read.await;
        let took = started.elapsed();
        self.reads += 1;
        if took > READ_TARGET {
            self.over_target += 1;
        }
        let ms = took.as_millis() as u64;
        let kept = self.slow.len() == SLOW_READS_KEPT;
        if !kept || self.slow.last().is_some_and(|last| ms > last.ms) {
            let what = what();
            if took > self.slowest {
                self.slowest = took;
                self.slowest_what = what.clone();
            }
            if kept {
                self.slow.pop();
            }
            let at = self.slow.partition_point(|read| read.ms >= ms);
            self.slow.insert(at, SlowRead { what, ms });
        }
        result
    }
}

/// One of a census's slowest reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlowRead {
    pub what: String,
    pub ms: u64,
}

/// A deterministic stream of window offsets, seeded per census and table.
struct Offsets(u64);

impl Offsets {
    fn new(seed: u64, table: &str) -> Self {
        let mut state = seed;
        for byte in table.bytes() {
            state = state.rotate_left(5) ^ u64::from(byte);
        }
        Self(state)
    }

    /// splitmix64.
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: i64) -> i64 {
        if bound <= 1 {
            0
        } else {
            (self.next() % bound as u64) as i64
        }
    }
}

fn placeholders(from: usize, count: usize) -> String {
    (from..from + count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn table_exists(db: &LocalDb, table: &str) -> DbResult<bool> {
    Ok(db
        .query_opt(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            (table.to_string(),),
            |row| row.i64(0),
        )
        .await?
        .is_some())
}

/// Every table the census measures: the user tables of `sqlite_schema`, less
/// the engine's own.
async fn table_names(db: &LocalDb) -> DbResult<Vec<String>> {
    db.query_all(
        "SELECT name FROM sqlite_schema WHERE type = 'table' \
         AND substr(name, 1, 7) <> 'sqlite_' AND substr(name, 1, 7) <> '__turso' \
         ORDER BY name",
        (),
        |row| row.text(0),
    )
    .await
}

async fn columns(db: &LocalDb, table: &str) -> DbResult<Vec<Column>> {
    db.query_all(
        format!("PRAGMA table_info({})", quote_identifier(table)),
        (),
        |row| {
            Ok(Column::new(
                row.text(1)?,
                &row.opt_text(2)?.unwrap_or_default(),
            ))
        },
    )
    .await
}

/// A table's lowest and highest rowid.
async fn rowid_bounds(db: &LocalDb, table: &str) -> DbResult<(Option<i64>, Option<i64>)> {
    let quoted = quote_identifier(table);
    db.read(move |conn| {
        Box::pin(async move {
            Ok((
                first_i64(conn, &min_rowid_sql(&quoted)).await?,
                first_i64(conn, &max_rowid_sql(&quoted)).await?,
            ))
        })
    })
    .await
}

async fn first_i64(conn: &TrackedConnection, sql: &str) -> DbResult<Option<i64>> {
    let mut rows = conn.query(sql, ()).await?;
    Ok(match rows.next().await? {
        Some(row) => row.opt_i64(0)?,
        None => None,
    })
}

fn min_rowid_sql(quoted: &str) -> String {
    format!("SELECT min(rowid) FROM {quoted}")
}

fn max_rowid_sql(quoted: &str) -> String {
    format!("SELECT max(rowid) FROM {quoted}")
}

/// Up to `?2` consecutive rows from rowid `?1`, each as `(rowid, payload)`.
fn window_sql(quoted: &str, payload: &str) -> String {
    format!("SELECT rowid, {payload} FROM {quoted} WHERE rowid >= ?1 ORDER BY rowid LIMIT ?2")
}

/// Up to `?3` rows with rowids in `[?1, ?2)`, each as `(rowid, payload)`.
fn interval_sql(quoted: &str, payload: &str) -> String {
    format!(
        "SELECT rowid, {payload} FROM {quoted} WHERE rowid >= ?1 AND rowid < ?2 \
         ORDER BY rowid LIMIT ?3"
    )
}

async fn window_on(
    conn: &TrackedConnection,
    sql: &str,
    from: i64,
    limit: usize,
) -> DbResult<Vec<(i64, i64)>> {
    let mut rows = conn.query(sql, (from, limit as i64)).await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push((row.i64(0)?, row.opt_i64(1)?.unwrap_or(0)));
    }
    Ok(out)
}

fn mean(rows: &[(i64, i64)]) -> f64 {
    if rows.is_empty() {
        0.0
    } else {
        rows.iter().map(|(_, b)| *b as f64).sum::<f64>() / rows.len() as f64
    }
}

/// One stretch `[start, end)` of a table's rowid space, the rows its window
/// read, and the density of live rows (per rowid) that window measured.
struct Stratum {
    start: i64,
    end: i64,
    rows: Vec<(i64, i64)>,
    /// `None` when the window could not measure it (a single row read from
    /// the table's first rowid); such a stratum takes the others' mean.
    density: Option<f64>,
}

/// The density a window over `[from, to)` measured. Counting the rows in a
/// fixed interval is unbiased, so that is the measure whenever the interval
/// was read whole. A window cut short by its row limit ends on a row and
/// covers `[from, last]`; it only hits the limit where rows are dense, which
/// is where that end matters least.
fn interval_density(from: i64, to: i64, limit: usize, rows: &[(i64, i64)]) -> f64 {
    match rows.last() {
        Some(&(last, _)) if rows.len() >= limit => rows.len() as f64 / (last - from + 1) as f64,
        _ => rows.len() as f64 / (to - from).max(1) as f64,
    }
}

/// The density of a window that starts on a row (the table's first): the
/// gaps between its rows. Counting rows over `[first, last]` instead would
/// count the starting row without the gap before it, overstating a sparse
/// table.
fn gap_density(rows: &[(i64, i64)]) -> Option<f64> {
    let (&(first, _), &(last, _)) = (rows.first()?, rows.last()?);
    (rows.len() >= 2 && last > first).then(|| (rows.len() - 1) as f64 / (last - first) as f64)
}

/// Rows and payload a set of strata imply: each stratum's density times its
/// width, at the mean payload of the rows its window read. Also the newest
/// stratum's mean payload.
fn estimate(strata: &[Stratum]) -> (f64, f64, Option<f64>) {
    let known: Vec<f64> = strata.iter().filter_map(|s| s.density).collect();
    let fallback = if known.is_empty() {
        1.0
    } else {
        known.iter().sum::<f64>() / known.len() as f64
    };
    let (mut rows, mut bytes, mut newest) = (0.0, 0.0, None);
    for stratum in strata {
        let density = stratum.density.unwrap_or(fallback).min(1.0);
        let estimated = density * (stratum.end - stratum.start) as f64;
        rows += estimated;
        if !stratum.rows.is_empty() {
            let mean = mean(&stratum.rows);
            bytes += estimated * mean;
            newest = Some(mean);
        }
    }
    (rows, bytes, newest)
}

/// Rows per window at a row width, so a window reads about [`WINDOW_BYTES`]
/// (and a row wider than that is read alone).
fn window_rows(mean_bytes: f64) -> usize {
    if mean_bytes <= 0.0 {
        return MAX_WINDOW_ROWS;
    }
    ((WINDOW_BYTES / mean_bytes) as usize).clamp(MIN_WINDOW_ROWS, MAX_WINDOW_ROWS)
}

/// What one short read learns of a table: its rowid bounds and a first window
/// from the lowest rowid, sized by what a row costs (the previous census's
/// figure, or a one-row probe). A table that ends inside it is measured
/// exactly.
struct Survey {
    /// A declared column named `rowid` hides the rowid; such a table is not
    /// sized, because its bounds would be a scan.
    rowid_hidden: bool,
    window_sql: String,
    interval_sql: String,
    low: Option<i64>,
    high: Option<i64>,
    first: Vec<(i64, i64)>,
    /// The row at the high-water, read when the first window did not reach it.
    newest: Vec<(i64, i64)>,
    rows_per_window: usize,
    complete: bool,
}

impl Survey {
    fn first_stratum(&self) -> Option<Stratum> {
        let (Some(low), Some(high)) = (self.low, self.high) else {
            return None;
        };
        Some(Stratum {
            start: low,
            end: high + 1,
            rows: self.first.iter().chain(&self.newest).copied().collect(),
            density: gap_density(&self.first),
        })
    }

    /// The widest row the survey saw at either end of the table.
    fn widest(&self) -> f64 {
        mean(&self.first).max(mean(&self.newest)).max(1.0)
    }

    /// What the survey implies at its widest rows, to order sampling by: the
    /// oldest rows can be a tenth the width of the newest (`executions`), and a
    /// table ranked by them alone can miss the deadline it most needs to meet.
    fn rough_bytes(&self) -> f64 {
        self.first_stratum().map_or(0.0, |stratum| {
            let (rows, _, _) = estimate(&[stratum]);
            rows * self.widest()
        })
    }

    /// The table as the survey alone measures it: exact when it read the
    /// whole table, otherwise one stratum.
    fn measured(&self) -> Measured {
        if self.rowid_hidden {
            return Measured {
                rowid_hidden: true,
                ..Measured::default()
            };
        }
        let (Some(low), Some(high)) = (self.low, self.high) else {
            return Measured {
                exact: true,
                ..Measured::default()
            };
        };
        if self.complete {
            return Measured {
                low: Some(low),
                high: Some(high),
                rows: self.first.len() as i64,
                bytes: self.first.iter().map(|(_, b)| *b).sum(),
                exact: true,
                sampled_rows: self.first.len() as u32,
                strata: 1,
                ..Measured::default()
            };
        }
        let strata: Vec<Stratum> = self.first_stratum().into_iter().collect();
        let (rows, bytes, newest) = estimate(&strata);
        Measured {
            low: Some(low),
            high: Some(high),
            rows: rows.round() as i64,
            bytes: bytes.round() as i64,
            exact: false,
            sampled_rows: (self.first.len() + self.newest.len()) as u32,
            new_row_bytes: newest,
            strata: 1,
            rowid_hidden: false,
        }
    }
}

async fn survey(db: &LocalDb, table: &str, prior_row_bytes: Option<f64>) -> DbResult<Survey> {
    let quoted = quote_identifier(table);
    db.read(move |conn| {
        Box::pin(async move {
            let mut columns = Vec::new();
            {
                let mut rows = conn
                    .query(&format!("PRAGMA table_info({quoted})"), ())
                    .await?;
                while let Some(row) = rows.next().await? {
                    columns.push(Column::new(
                        row.text(1)?,
                        &row.opt_text(2)?.unwrap_or_default(),
                    ));
                }
            }
            let payload = payload_expr(&columns);
            let mut survey = Survey {
                rowid_hidden: columns.iter().any(|c| c.name.eq_ignore_ascii_case("rowid")),
                window_sql: window_sql(&quoted, &payload),
                interval_sql: interval_sql(&quoted, &payload),
                low: None,
                high: None,
                first: Vec::new(),
                newest: Vec::new(),
                rows_per_window: 0,
                complete: true,
            };
            if survey.rowid_hidden {
                return Ok(survey);
            }
            survey.low = first_i64(conn, &min_rowid_sql(&quoted)).await?;
            survey.high = first_i64(conn, &max_rowid_sql(&quoted)).await?;
            let Some(low) = survey.low else {
                return Ok(survey);
            };
            let sql = &survey.window_sql;
            let (first, rows_per_window) = match prior_row_bytes {
                Some(bytes) => {
                    let rows = window_rows(bytes);
                    (window_on(conn, sql, low, rows).await?, rows)
                }
                None => {
                    let mut first = window_on(conn, sql, low, 1).await?;
                    let rows = window_rows(mean(&first));
                    if let Some(&(last, _)) = first.last().filter(|_| rows > 1) {
                        first.extend(window_on(conn, sql, last + 1, rows - 1).await?);
                    }
                    (first, rows)
                }
            };
            survey.complete = first.len() < rows_per_window
                || first.last().map(|(rowid, _)| *rowid) == survey.high;
            if let (false, Some(high)) = (survey.complete, survey.high) {
                survey.newest = window_on(conn, sql, high, 1).await?;
            }
            survey.first = first;
            survey.rows_per_window = rows_per_window;
            Ok(survey)
        })
    })
    .await
}

/// The windows after the survey's: strata across the rowid space, as many as
/// [`TABLE_SAMPLE_BYTES`] buys at the table's row width, each read at a random
/// rowid inside its stratum.
async fn sample(
    db: &LocalDb,
    table: &str,
    survey: &Survey,
    seed: u64,
    clock: &mut Clock,
) -> DbResult<Measured> {
    let (Some(low), Some(high)) = (survey.low, survey.high) else {
        return Ok(survey.measured());
    };
    if survey.complete || survey.rowid_hidden {
        return Ok(survey.measured());
    }
    // The lowest rows are the oldest and need not look like the rest: on
    // `executions` they are a tenth the width of today's. The newest row sizes
    // the windows too, and any wider window met on the way shrinks the rest.
    let mut widest = survey.widest();
    let mut rows_per_window = survey.rows_per_window.min(window_rows(widest));
    let span = high - low + 1;
    let per_window = rows_per_window as f64 * widest;
    let count = ((TABLE_SAMPLE_BYTES / per_window) as i64)
        .clamp(MIN_STRATA, STRATA)
        .min(span)
        .max(1);
    let bound = |k: i64| low + (i128::from(span) * i128::from(k) / i128::from(count)) as i64;
    let mut offsets = Offsets::new(seed, table);
    let mut sampled = survey.first.len() + survey.newest.len();
    // Every stratum, the first included, is measured by counting the rows in
    // an interval at a random place inside it. The survey's window from the
    // lowest rowid sizes the windows but stands for no stratum: the oldest rows
    // of an upserted table (`channel_state_card`: 2,410 rows over 59 million
    // rowids on the 11-02 clone) are a dense cluster the rest is not.
    //
    // An interval is long enough to hold about `rows_per_window` rows at the
    // density the earlier intervals found, so a sparse table is read in long
    // intervals and a dense one in short ones. A length fixed before its own
    // read keeps each interval's count unbiased.
    let mut expected = gap_density(&survey.first).unwrap_or(1.0);
    let (mut found, mut covered) = (0.0, 0.0);
    let mut strata = Vec::with_capacity(count as usize);
    for k in 0..count {
        let (start, end) = (bound(k), bound(k + 1));
        let width = end - start;
        let length = ((rows_per_window as f64 / expected.max(1e-12)).ceil() as i64).clamp(1, width);
        let from = start + offsets.below(width - length + 1);
        let to = from + length;
        let rows = clock
            .time(
                || format!("{table}: window at rowid {from}"),
                db.query_all(
                    survey.interval_sql.clone(),
                    (from, to, rows_per_window as i64),
                    |row| Ok((row.i64(0)?, row.opt_i64(1)?.unwrap_or(0))),
                ),
            )
            .await?;
        sampled += rows.len();
        let density = interval_density(from, to, rows_per_window, &rows);
        found += rows.len() as f64;
        covered += match rows.last() {
            Some(&(last, _)) if rows.len() >= rows_per_window => (last - from + 1) as f64,
            _ => length as f64,
        };
        // Half a row keeps an empty interval from reading as zero density,
        // which would make the next interval the whole stratum.
        expected = (found + 0.5) / covered;
        if !rows.is_empty() {
            widest = widest.max(mean(&rows));
        }
        rows_per_window = rows_per_window.min(window_rows(widest));
        strata.push(Stratum {
            start,
            end,
            rows,
            density: Some(density),
        });
    }
    let (rows, bytes, newest) = estimate(&strata);
    Ok(Measured {
        low: Some(low),
        high: Some(high),
        rows: rows.round() as i64,
        bytes: bytes.round() as i64,
        exact: false,
        sampled_rows: sampled as u32,
        new_row_bytes: newest,
        strata: count as u32,
        rowid_hidden: false,
    })
}

/// A table's measurement before its rule, budget and growth are attached.
#[derive(Debug, Default)]
struct Measured {
    low: Option<i64>,
    high: Option<i64>,
    rows: i64,
    bytes: i64,
    exact: bool,
    sampled_rows: u32,
    new_row_bytes: Option<f64>,
    strata: u32,
    rowid_hidden: bool,
}

/// Survey and sample one table, with no deadline between the two.
#[cfg(test)]
async fn measure_table(
    db: &LocalDb,
    table: &str,
    seed: u64,
    clock: &mut Clock,
) -> DbResult<Measured> {
    let survey = clock
        .time(|| format!("{table}: survey"), survey(db, table, None))
        .await?;
    sample(db, table, &survey, seed, clock).await
}

fn apply(census: &mut TableCensus, measured: Measured) {
    if measured.rowid_hidden {
        census.error = Some("a declared rowid column hides the rowid".to_string());
        return;
    }
    census.low_water = measured.low;
    census.high_water = measured.high;
    census.rows = measured.rows;
    census.bytes = measured.bytes;
    census.exact = measured.exact;
    census.sampled_rows = measured.sampled_rows;
    census.strata = measured.strata;
    census.new_row_bytes = measured.new_row_bytes;
    census.over_budget = census.bytes as f64 > f64::from(census.budget_mb) * BYTES_PER_MB;
}

fn rule_summary(table: &str) -> (String, u32) {
    match retention_for(table) {
        Some(entry) => (entry.rule.summary(), entry.budget_mb),
        None => ("no rule".to_string(), DEFAULT_BUDGET_MB),
    }
}

fn rule_kind(table: &str) -> &'static str {
    retention_for(table).map_or("unruled", |entry| entry.rule.kind())
}

/// The payload expressions of the tables history is made of.
struct HistoryPayloads {
    events: String,
    tool_invocations: String,
    skyline: String,
    turn_embeddings: String,
}

impl HistoryPayloads {
    async fn read(db: &LocalDb, clock: &mut Clock) -> DbResult<Self> {
        let mut payload = Vec::new();
        for table in [
            "events",
            "tool_invocations",
            "session_skyline_cache",
            "turn_embeddings",
        ] {
            let cols = clock
                .time(|| format!("{table}: columns"), columns(db, table))
                .await?;
            payload.push(payload_expr(&cols));
        }
        let mut payload = payload.into_iter();
        let mut next = || payload.next().unwrap_or_default();
        Ok(Self {
            events: next(),
            tool_invocations: next(),
            skyline: next(),
            turn_embeddings: next(),
        })
    }
}

/// The first rowid in `[low, end)` of `issues` created at or after `cutoff`,
/// by binary search over rowids (issues are inserted in creation order). Each
/// probe reads the first row at or after the midpoint. Deleted issues leave
/// gaps, so a probe may land past the midpoint; every branch still shrinks the
/// interval, which is what ends the search. `None` when no issue qualifies.
async fn first_issue_created_after(
    db: &LocalDb,
    mut low: i64,
    mut end: i64,
    cutoff: i64,
    clock: &mut Clock,
    deadline: Instant,
) -> DbResult<Option<i64>> {
    let mut found = None;
    while low < end && Instant::now() < deadline {
        let mid = low + (end - low) / 2;
        let probe = clock
            .time(
                || "issues: creation-time probe".to_string(),
                db.query_opt(
                    "SELECT rowid, created_at FROM issues WHERE rowid >= ?1 ORDER BY rowid LIMIT 1",
                    (mid,),
                    |row| Ok((row.i64(0)?, row.opt_i64(1)?.unwrap_or(0))),
                ),
            )
            .await?;
        match probe {
            // No row in [mid, end): the answer, if any, lies below mid.
            None => end = mid,
            Some((rowid, _)) if rowid >= end => end = mid,
            Some((rowid, created)) if created < cutoff => low = rowid + 1,
            // `rowid` qualifies and nothing exists in [mid, rowid), so the
            // answer is `rowid` or lies below mid.
            Some((rowid, _)) => {
                found = Some(rowid);
                end = mid;
            }
        }
    }
    Ok(found)
}

/// A random sample of the issues resolved in the band, and an estimate of how
/// many there are.
#[derive(Debug, Default)]
struct Band {
    issues: Vec<String>,
    estimated: usize,
}

/// Probe random rowids among the issues created between the lookback and the
/// band's newest edge (nothing created later resolved inside the band), each
/// probe one row, and keep those resolved in the band. Walking the range
/// instead would read every issue's description: 6 s cold for the whole table
/// on the 11-02 clone.
async fn sample_band(
    db: &LocalDb,
    now: i64,
    seed: u64,
    clock: &mut Clock,
    deadline: Instant,
) -> DbResult<Band> {
    let (newest_days, oldest_days) = HISTORY_BAND_DAYS;
    let (band_from, band_to) = (now - oldest_days * DAY_S, now - newest_days * DAY_S);
    let (low, high) = clock
        .time(
            || "issues: rowid bounds".to_string(),
            rowid_bounds(db, "issues"),
        )
        .await?;
    let (Some(low), Some(high)) = (low, high) else {
        return Ok(Band::default());
    };
    let end = high + 1;
    let lookback = now - BAND_LOOKBACK_DAYS * DAY_S;
    let start = first_issue_created_after(db, low, end, lookback, clock, deadline)
        .await?
        .unwrap_or(end);
    let stop = first_issue_created_after(db, start, end, band_to, clock, deadline)
        .await?
        .unwrap_or(end);
    if stop <= start {
        return Ok(Band::default());
    }
    let mut offsets = Offsets::new(seed, "history");
    let mut seen = HashSet::new();
    let mut issues = Vec::new();
    for _ in 0..HISTORY_PROBES {
        if issues.len() >= HISTORY_MAX_ISSUES || Instant::now() >= deadline {
            break;
        }
        let at = start + offsets.below(stop - start);
        let probe = clock
            .time(
                || "issues: band probe".to_string(),
                db.query_opt(
                    "SELECT rowid, id, status, completed_at FROM issues \
                     WHERE rowid >= ?1 ORDER BY rowid LIMIT 1",
                    (at,),
                    |row| Ok((row.i64(0)?, row.text(1)?, row.text(2)?, row.opt_i64(3)?)),
                ),
            )
            .await?;
        let Some((rowid, id, status, completed)) = probe else {
            continue;
        };
        if rowid >= stop || !seen.insert(rowid) {
            continue;
        }
        let resolved = RESOLVED_STATUSES.contains(&status.as_str());
        if resolved && completed.is_some_and(|at| at >= band_from && at < band_to) {
            issues.push(id);
        }
    }
    let estimated = if seen.is_empty() {
        0
    } else {
        (issues.len() as f64 / seen.len() as f64 * (stop - start) as f64).round() as usize
    };
    Ok(Band { issues, estimated })
}

/// One issue's history: event rows, and the bytes of its events and derived
/// rows.
#[derive(Debug, Default, Clone, Copy)]
struct Tally {
    event_rows: i64,
    bytes: i64,
}

async fn sum_in(
    db: &LocalDb,
    clock: &mut Clock,
    what: &str,
    sql_head: &str,
    ids: &[String],
) -> DbResult<i64> {
    let mut total = 0;
    for chunk in ids.chunks(ID_CHUNK) {
        let sql = format!("{sql_head} ({})", placeholders(1, chunk.len()));
        let values: Vec<Value> = chunk.iter().cloned().map(Value::Text).collect();
        total += clock
            .time(
                || what.to_string(),
                db.query_opt(sql, values, |row| Ok(row.opt_i64(0)?.unwrap_or(0))),
            )
            .await?
            .unwrap_or(0);
    }
    Ok(total)
}

async fn ids(
    db: &LocalDb,
    clock: &mut Clock,
    what: &str,
    sql: &str,
    key: &str,
) -> DbResult<Vec<String>> {
    clock
        .time(
            || what.to_string(),
            db.query_all(sql.to_string(), (key.to_string(),), |row| row.text(0)),
        )
        .await
}

/// `None` when the deadline passes part-way through the issue's events: a
/// half-read issue would bias the ratio, so it leaves the sample.
async fn issue_history(
    db: &LocalDb,
    issue: &str,
    payload: &HistoryPayloads,
    clock: &mut Clock,
    deadline: Instant,
) -> DbResult<Option<Tally>> {
    let mut tally = Tally::default();
    let jobs = ids(
        db,
        clock,
        "jobs of an issue",
        "SELECT id FROM jobs WHERE issue_id = ?1",
        issue,
    )
    .await?;
    let (mut runs, mut sessions, mut turns) = (Vec::new(), Vec::new(), Vec::new());
    for job in &jobs {
        runs.extend(
            ids(
                db,
                clock,
                "runs of a job",
                "SELECT id FROM runs WHERE job_id = ?1",
                job,
            )
            .await?,
        );
        sessions.extend(
            ids(
                db,
                clock,
                "sessions of a job",
                "SELECT id FROM sessions WHERE job_id = ?1",
                job,
            )
            .await?,
        );
        turns.extend(
            ids(
                db,
                clock,
                "turns of a job",
                "SELECT id FROM turns WHERE job_id = ?1",
                job,
            )
            .await?,
        );
    }
    let events_page = history_events_sql(&payload.events);
    for run in &runs {
        let mut after = i64::MIN;
        loop {
            let page = clock
                .time(
                    || "events of a run".to_string(),
                    db.query_all(
                        events_page.clone(),
                        (run.clone(), after, EVENT_PAGE_ROWS),
                        |row| Ok((row.i64(0)?, row.opt_i64(1)?.unwrap_or(0))),
                    ),
                )
                .await?;
            tally.event_rows += page.len() as i64;
            tally.bytes += page.iter().map(|(_, b)| *b).sum::<i64>();
            match page.last() {
                Some(&(sequence, _)) if page.len() as i64 == EVENT_PAGE_ROWS => after = sequence,
                _ => break,
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
        }
        tally.bytes += clock
            .time(
                || "tool invocations of a run".to_string(),
                db.query_opt(
                    format!(
                        "SELECT coalesce(sum({}), 0) FROM tool_invocations WHERE run_id = ?1",
                        payload.tool_invocations
                    ),
                    (run.clone(),),
                    |row| Ok(row.opt_i64(0)?.unwrap_or(0)),
                ),
            )
            .await?
            .unwrap_or(0);
    }
    let skyline = format!(
        "SELECT coalesce(sum({}), 0) FROM session_skyline_cache WHERE session_id IN",
        payload.skyline
    );
    tally.bytes += sum_in(db, clock, "skyline rows", &skyline, &sessions).await?;
    let embeddings = format!(
        "SELECT coalesce(sum({}), 0) FROM turn_embeddings WHERE turn_id IN",
        payload.turn_embeddings
    );
    tally.bytes += sum_in(db, clock, "turn embeddings", &embeddings, &turns).await?;
    Ok(Some(tally))
}

/// One page of a run's events in sequence order, through the unique
/// `(run_id, sequence)` index, so each page seeks to where the last ended.
fn history_events_sql(payload: &str) -> String {
    format!(
        "SELECT sequence, {payload} FROM events \
         WHERE run_id = ?1 AND sequence > ?2 ORDER BY sequence LIMIT ?3"
    )
}

async fn calibrate_history(
    db: &LocalDb,
    now: i64,
    seed: u64,
    clock: &mut Clock,
    deadline: Instant,
) -> DbResult<Option<HistoryCalibration>> {
    let band = sample_band(db, now, seed, clock, deadline).await?;
    if band.issues.is_empty() {
        return Ok(None);
    }
    let payload = HistoryPayloads::read(db, clock).await?;
    let mut tallies = Vec::new();
    let mut read = 0;
    for issue in &band.issues {
        if read >= HISTORY_BYTES || Instant::now() >= deadline {
            break;
        }
        let Some(tally) = issue_history(db, issue, &payload, clock, deadline).await? else {
            break;
        };
        read += tally.bytes;
        tallies.push(tally);
    }
    let rows: i64 = tallies.iter().map(|t| t.event_rows).sum();
    let bytes: i64 = tallies.iter().map(|t| t.bytes).sum();
    if rows == 0 {
        return Ok(None);
    }
    let ratio = bytes as f64 / rows as f64;
    Ok(Some(HistoryCalibration {
        band_days: HISTORY_BAND_DAYS,
        band_issues: band.estimated,
        sampled_issues: tallies.len(),
        event_rows: rows,
        history_bytes: bytes,
        bytes_per_event_row: ratio,
        day_bytes_per_event_row: ratio,
        pool_weight: rows as f64,
        relative_standard_error: ratio_standard_error(&tallies, ratio),
    }))
}

/// The usual standard error of a ratio estimator over cluster samples,
/// relative to the ratio.
fn ratio_standard_error(tallies: &[Tally], ratio: f64) -> Option<f64> {
    let n = tallies.len() as f64;
    if tallies.len() < 2 || ratio <= 0.0 {
        return None;
    }
    let mean_rows = tallies.iter().map(|t| t.event_rows as f64).sum::<f64>() / n;
    if mean_rows <= 0.0 {
        return None;
    }
    let residuals: f64 = tallies
        .iter()
        .map(|t| (t.bytes as f64 - ratio * t.event_rows as f64).powi(2))
        .sum();
    let standard_error = (residuals / (n * (n - 1.0))).sqrt() / mean_rows;
    Some(standard_error / ratio)
}

/// The archive directory's size, walked without following links.
pub fn measure_archive(path: &Path) -> ArchiveCensus {
    let mut census = ArchiveCensus {
        path: path.display().to_string(),
        ..ArchiveCensus::default()
    };
    if !path.is_dir() {
        return census;
    }
    census.exists = true;
    let mut pending = vec![path.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) => {
                census.error = Some(format!("{}: {error}", dir.display()));
                continue;
            }
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                pending.push(entry.path());
            } else if meta.is_file() {
                census.bytes += meta.len();
                census.files += 1;
            }
        }
    }
    census
}

async fn pragma(db: &LocalDb, clock: &mut Clock, name: &'static str) -> Option<i64> {
    clock
        .time(
            || format!("PRAGMA {name}"),
            db.query_opt(format!("PRAGMA {name}"), (), |row| row.i64(0)),
        )
        .await
        .ok()
        .flatten()
}

/// Tokens per day over `[from, to)`, from the hourly `token_rollup`.
async fn tokens_per_day(
    db: &LocalDb,
    clock: &mut Clock,
    from: i64,
    to: i64,
) -> DbResult<Option<f64>> {
    if to <= from {
        return Ok(None);
    }
    let total = clock
        .time(
            || "token_rollup: tokens since the previous census".to_string(),
            db.query_opt(
                "SELECT coalesce(sum(input_tokens + cache_read_tokens + cache_create_tokens + output_tokens), 0) \
                 FROM token_rollup WHERE bucket_start >= ?1 AND bucket_start < ?2",
                (from, to),
                |row| Ok(row.opt_i64(0)?.unwrap_or(0)),
            ),
        )
        .await?
        .unwrap_or(0);
    Ok(Some(total as f64 * DAY_S as f64 / (to - from) as f64))
}

/// Least-squares slope of `points` (x, y).
fn slope(points: &[(f64, f64)]) -> Option<f64> {
    let n = points.len() as f64;
    if points.len() < 2 {
        return None;
    }
    let mean_x = points.iter().map(|(x, _)| x).sum::<f64>() / n;
    let mean_y = points.iter().map(|(_, y)| y).sum::<f64>() / n;
    let sxx: f64 = points.iter().map(|(x, _)| (x - mean_x).powi(2)).sum();
    if sxx <= 0.0 {
        return None;
    }
    let sxy: f64 = points
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum();
    Some(sxy / sxx)
}

/// Project `current` from it and `previous` (newest first). `None` with fewer
/// than [`PROJECTION_MIN_CENSUSES`] censuses or less than a day between the
/// oldest and the current one.
pub fn project(current: &CensusReport, previous: &[CensusReport]) -> Option<Projection> {
    let series: Vec<&CensusReport> = previous
        .iter()
        .filter(|census| {
            census.taken_at < current.taken_at
                && census.taken_at >= current.taken_at - PROJECTION_LOOKBACK_S
        })
        .chain(std::iter::once(current))
        .collect();
    let oldest = series.iter().map(|c| c.taken_at).min()?;
    let span_days = (current.taken_at - oldest) as f64 / DAY_S as f64;
    if series.len() < PROJECTION_MIN_CENSUSES || span_days < 1.0 {
        return None;
    }
    let by_table: Vec<HashMap<&str, &TableCensus>> = series
        .iter()
        .map(|census| {
            census
                .tables
                .iter()
                .filter(|t| t.measured())
                .map(|t| (t.table.as_str(), t))
                .collect()
        })
        .collect();
    let mut projection = Projection {
        censuses: series.len(),
        span_days,
        horizon_days: PROJECTION_HORIZON_DAYS,
        ..Projection::default()
    };
    let mut growth = Vec::new();
    for table in current.tables.iter().filter(|t| t.measured()) {
        let points: Vec<(f64, f64)> = series
            .iter()
            .zip(&by_table)
            .filter_map(|(census, tables)| {
                tables.get(table.table.as_str()).map(|t| {
                    (
                        (census.taken_at - current.taken_at) as f64 / DAY_S as f64,
                        t.rows as f64,
                    )
                })
            })
            .collect();
        let bytes_per_day = if points.len() >= PROJECTION_MIN_CENSUSES {
            slope(&points).unwrap_or(0.0) * table.row_bytes()
        } else {
            0.0
        };
        let projected =
            (table.bytes as f64 + bytes_per_day * PROJECTION_HORIZON_DAYS).max(0.0) as i64;
        projection.payload_bytes += table.bytes;
        projection.projected_bytes += projected;
        projection.bytes_per_day += bytes_per_day;
        let class = projection
            .by_rule
            .entry(rule_kind(&table.table).to_string())
            .or_default();
        class.bytes += table.bytes;
        class.projected_bytes += projected;
        class.bytes_per_day += bytes_per_day;
        growth.push(TableGrowth {
            table: table.table.clone(),
            rule: table.rule.clone(),
            bytes_per_day,
            projected_bytes: projected,
        });
    }
    growth.sort_by(|a, b| b.bytes_per_day.total_cmp(&a.bytes_per_day));
    growth.truncate(PROJECTION_FASTEST);
    projection.fastest = growth;
    projection.mb_per_billion_tokens = current
        .tokens_per_day
        .filter(|tokens| *tokens > 0.0)
        .map(|tokens| projection.bytes_per_day / BYTES_PER_MB / (tokens / 1e9));
    Some(projection)
}

/// How much of a table's pooled sample survives each day.
const POOL_DECAY: f64 = 0.75;

/// Pool a sampled table's estimate with the previous census's. One day's
/// windows land on a dozen or so rows, which misjudges tables whose rows vary
/// widely (a few huge `execution_history` packs among small ones) or whose
/// rowids cluster. Each census lands its windows elsewhere, so the table
/// carries two decayed, sample-weighted means, live rows per rowid and bytes
/// per row, and applies them to today's exact rowid span. An eviction still
/// shows the same day, because it moves the span; sampling noise averages
/// over about `1 / (1 - POOL_DECAY)` days of windows.
fn pool(census: &mut TableCensus, before: Option<&TableCensus>) {
    census.day_rows = None;
    census.day_bytes = None;
    census.pool_weight = 0.0;
    let (Some(low), Some(high)) = (census.low_water, census.high_water) else {
        return;
    };
    if census.exact || census.rows <= 0 {
        return;
    }
    let span = (high - low + 1) as f64;
    let weight = f64::from(census.sampled_rows).max(1.0);
    census.day_rows = Some(census.rows);
    census.day_bytes = Some(census.bytes);
    census.pool_weight = weight;
    let Some(before) =
        before.filter(|b| b.measured() && !b.exact && b.pool_weight > 0.0 && b.rows > 0)
    else {
        return;
    };
    let (Some(before_low), Some(before_high)) = (before.low_water, before.high_water) else {
        return;
    };
    let carried = POOL_DECAY * before.pool_weight;
    let mix = |today: f64, then: f64| (weight * today + carried * then) / (weight + carried);
    let density = mix(
        census.rows as f64 / span,
        before.rows as f64 / (before_high - before_low + 1) as f64,
    )
    .min(1.0);
    let row_bytes = mix(
        census.bytes as f64 / census.rows as f64,
        before.bytes as f64 / before.rows as f64,
    );
    census.rows = (density * span).round() as i64;
    census.bytes = (census.rows as f64 * row_bytes).round() as i64;
    census.pool_weight = weight + carried;
}

/// Take a census of `db` at `now` (unix seconds) within [`TIME_BUDGET`].
pub async fn take(db: &LocalDb, now: i64, archive_dir: &Path) -> DbResult<CensusReport> {
    take_within(db, now, archive_dir, TIME_BUDGET).await
}

/// [`take`] with an explicit time budget.
pub async fn take_within(
    db: &LocalDb,
    now: i64,
    archive_dir: &Path,
    budget: Duration,
) -> DbResult<CensusReport> {
    let started = Instant::now();
    let previous = if table_exists(db, "storage_census").await? {
        recent(db, PROJECTION_MAX_CENSUSES).await?
    } else {
        Vec::new()
    };
    let budget = budget.saturating_sub(started.elapsed());
    take_after(db, now, archive_dir, budget, &previous).await
}

/// [`take_within`] after the given censuses (newest first) rather than the
/// stored ones: what replaying a run of censuses against one clone needs.
pub async fn take_after(
    db: &LocalDb,
    now: i64,
    archive_dir: &Path,
    budget: Duration,
    previous: &[CensusReport],
) -> DbResult<CensusReport> {
    let mut clock = Clock::new();
    let tables_deadline = clock.started + budget.saturating_sub(HISTORY_BUDGET);
    let deadline = clock.started + budget;
    let seed = now as u64;

    let last = previous.iter().find(|census| census.taken_at < now);
    let last_tables: HashMap<&str, &TableCensus> = last
        .map(|census| {
            census
                .tables
                .iter()
                .map(|t| (t.table.as_str(), t))
                .collect()
        })
        .unwrap_or_default();

    let names = clock
        .time(|| "sqlite_schema: tables".to_string(), table_names(db))
        .await?;

    // Survey every table in one short read each: small tables are measured
    // exactly here, and the rest learn their bounds and row width.
    let mut tables = Vec::with_capacity(names.len());
    let mut pending = Vec::new();
    for name in names {
        let (rule, budget_mb) = rule_summary(&name);
        let mut census = TableCensus {
            table: name.clone(),
            rule,
            budget_mb,
            ..TableCensus::default()
        };
        if Instant::now() >= tables_deadline {
            census.error = Some("not reached within the census's time budget".to_string());
            tables.push(census);
            continue;
        }
        let prior_row_bytes = last_tables
            .get(name.as_str())
            .filter(|t| t.measured() && t.rows > 0)
            .map(|t| t.bytes as f64 / t.rows as f64);
        let started = Instant::now();
        let surveyed = clock
            .time(
                || format!("{name}: survey"),
                survey(db, &name, prior_row_bytes),
            )
            .await;
        census.measure_ms = started.elapsed().as_millis() as u64;
        match surveyed {
            Ok(survey) => {
                apply(&mut census, survey.measured());
                if !survey.complete && !survey.rowid_hidden {
                    pending.push((tables.len(), survey));
                }
            }
            Err(error) => census.error = Some(error.to_string()),
        }
        tables.push(census);
    }

    // Sample the largest first, so a census that runs out of time leaves only
    // small tables at their survey's one-window estimate.
    pending.sort_by(|(_, a), (_, b)| b.rough_bytes().total_cmp(&a.rough_bytes()));
    for (index, survey) in pending {
        if Instant::now() >= tables_deadline {
            break;
        }
        let census = &mut tables[index];
        let started = Instant::now();
        match sample(db, &census.table.clone(), &survey, seed, &mut clock).await {
            Ok(measured) => apply(census, measured),
            Err(error) => census.error = Some(error.to_string()),
        }
        census.measure_ms += started.elapsed().as_millis() as u64;
    }

    for census in tables.iter_mut().filter(|t| t.measured()) {
        pool(census, last_tables.get(census.table.as_str()).copied());
        census.over_budget = census.bytes as f64 > f64::from(census.budget_mb) * BYTES_PER_MB;
    }

    if let Some(last) = last {
        let days = (now - last.taken_at) as f64 / DAY_S as f64;
        for census in tables.iter_mut().filter(|t| t.measured()) {
            let Some(before) = last_tables
                .get(census.table.as_str())
                .filter(|t| t.measured())
            else {
                continue;
            };
            if let (Some(high), Some(before_high)) = (census.high_water, before.high_water) {
                census.rowids_per_day = Some((high - before_high) as f64 / days);
            }
            census.rows_per_day = Some((census.rows - before.rows) as f64 / days);
            if let (Some(rowids), Some(rows)) = (census.rowids_per_day, census.rows_per_day) {
                census.vacated_per_day = Some((rowids - rows).max(0.0));
            }
        }
    }

    let mut history = match calibrate_history(db, now, seed, &mut clock, deadline).await {
        Ok(history) => history,
        Err(error) => {
            log::warn!("storage census: history calibration failed: {error}");
            None
        }
    };
    if let Some(history) = history.as_mut() {
        let before = previous
            .iter()
            .filter(|census| census.taken_at < now)
            .find_map(|census| census.history.as_ref());
        history.pool(before);
    }

    let archive_path = archive_dir.to_path_buf();
    let archive = tokio::task::spawn_blocking(move || measure_archive(&archive_path))
        .await
        .unwrap_or_else(|error| ArchiveCensus {
            path: archive_dir.display().to_string(),
            error: Some(error.to_string()),
            ..ArchiveCensus::default()
        });

    let page_size = pragma(db, &mut clock, "page_size").await;
    let page_count = pragma(db, &mut clock, "page_count").await;
    let freelist_count = pragma(db, &mut clock, "freelist_count").await;
    let tokens_from = last.map_or(now - DAY_S, |census| census.taken_at);
    let tokens = tokens_per_day(db, &mut clock, tokens_from, now)
        .await
        .unwrap_or(None);

    let mut report = CensusReport {
        taken_at: now,
        duration_ms: 0,
        reads: clock.reads,
        reads_over_target: clock.over_target,
        slowest_read_ms: clock.slowest.as_millis() as u64,
        slowest_read: clock.slowest_what.clone(),
        slow_reads: clock.slow.clone(),
        file_bytes: std::fs::metadata(db.path()).map_or(0, |meta| meta.len()),
        log_bytes: db.logical_log_bytes(),
        page_size,
        page_count,
        freelist_count,
        payload_bytes: tables
            .iter()
            .filter(|t| t.measured())
            .map(|t| t.bytes)
            .sum(),
        over_budget: tables
            .iter()
            .filter(|t| t.over_budget)
            .map(|t| t.table.clone())
            .collect(),
        unmeasured: tables
            .iter()
            .filter(|t| !t.measured())
            .map(|t| t.table.clone())
            .collect(),
        tables,
        history,
        archive,
        tokens_per_day: tokens,
        projection: None,
        deleting_rules: Vec::new(),
    };
    report.deleting_rules = deleting_rules(&report.tables);
    report.projection = project(&report, previous);
    report.duration_ms = clock.started.elapsed().as_millis() as u64;
    Ok(report)
}

/// Store `report` as the newest census and drop all but the newest
/// [`KEPT_CENSUSES`], in one short write. Returns the new row's id.
pub async fn record(db: &LocalDb, report: &CensusReport) -> DbResult<i64> {
    let json = serde_json::to_string(report)
        .map_err(|error| DbError::internal(format!("encoding the census: {error}")))?;
    let values = vec![
        Value::Integer(report.taken_at),
        Value::Integer(report.duration_ms as i64),
        Value::Integer(report.slowest_read_ms as i64),
        Value::Integer(report.payload_bytes),
        Value::Integer(report.file_bytes as i64),
        report
            .history
            .as_ref()
            .map_or(Value::Null, |h| Value::Real(h.bytes_per_event_row)),
        Value::Text(json),
    ];
    db.write(move |conn| {
        let values = values.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO storage_census (taken_at, duration_ms, slowest_read_ms, payload_bytes, \
                 file_bytes, bytes_per_event_row, report_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                values,
            )
            .await?;
            let id = {
                let mut rows = conn.query("SELECT max(id) FROM storage_census", ()).await?;
                match rows.next().await? {
                    Some(row) => row.i64(0)?,
                    None => 0,
                }
            };
            conn.execute(
                "DELETE FROM storage_census WHERE id <= ?1",
                (id - KEPT_CENSUSES,),
            )
            .await?;
            Ok(id)
        })
    })
    .await
}

/// The newest `limit` censuses, newest first. A row that no longer decodes is
/// skipped.
pub async fn recent(db: &LocalDb, limit: i64) -> DbResult<Vec<CensusReport>> {
    let rows = db
        .query_all(
            "SELECT id, report_json FROM storage_census ORDER BY id DESC LIMIT ?1",
            (limit,),
            |row| Ok((row.i64(0)?, row.text(1)?)),
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(id, json)| match serde_json::from_str(&json) {
            Ok(report) => Some(report),
            Err(error) => {
                log::warn!("storage census {id} does not decode: {error}");
                None
            }
        })
        .collect())
}

/// When the newest census was taken, if any.
pub async fn latest_taken_at(db: &LocalDb) -> DbResult<Option<i64>> {
    if !table_exists(db, "storage_census").await? {
        return Ok(None);
    }
    db.query_opt(
        "SELECT taken_at FROM storage_census ORDER BY id DESC LIMIT 1",
        (),
        |row| row.i64(0),
    )
    .await
}

/// The newest census's live-row estimate per table.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRowCounts {
    pub taken_at: i64,
    pub rows: HashMap<String, i64>,
}

/// Per-table live rows from the newest census, read from one stored row, for
/// readers that must report a table's size without counting it. `None` when
/// no census has been taken (or the database has no census table).
pub async fn latest_table_rows(db: &LocalDb) -> DbResult<Option<TableRowCounts>> {
    if !table_exists(db, "storage_census").await? {
        return Ok(None);
    }
    Ok(recent(db, 1)
        .await?
        .into_iter()
        .next()
        .map(|census| TableRowCounts {
            taken_at: census.taken_at,
            rows: census
                .tables
                .into_iter()
                .filter(|t| t.error.is_none())
                .map(|t| (t.table, t.rows))
                .collect(),
        }))
}

/// The newest calibrated bytes per history event row and when it was
/// measured: the constant the hot budget estimates an execution's history by.
pub async fn latest_bytes_per_event_row(db: &LocalDb) -> DbResult<Option<(f64, i64)>> {
    if !table_exists(db, "storage_census").await? {
        return Ok(None);
    }
    db.query_opt(
        "SELECT bytes_per_event_row, taken_at FROM storage_census \
         WHERE bytes_per_event_row IS NOT NULL ORDER BY id DESC LIMIT 1",
        (),
        |row| Ok((row.f64(0)?, row.i64(1)?)),
    )
    .await
}

#[cfg(test)]
mod tests;
