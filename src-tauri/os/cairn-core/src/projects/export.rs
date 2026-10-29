//! Whole-project export: every row a project owns, written to a self-contained
//! directory for offline analysis.
//!
//! # What "owns" means
//!
//! The table list is the project ownership manifest in [`super::migrate`] — the
//! same `COPY_ORDER` entries project deletion sweeps and move-to-team copies —
//! read as SELECTs instead of DELETEs. Deletion can leave tables to `ON DELETE
//! CASCADE` and to foreign-key-free ledgers that die with the runner; an export
//! cannot, so [`EXPORT_ONLY`] names the project-owned tables the sweep does not
//! need, [`CHANNEL_KEY_ADDRESSED`] and [`EXPORT_KEYED`] the ones addressed by
//! project key, and [`EXPORT_EXCLUDED`] what is deliberately left out and why.
//! The `export_plan_covers_every_project_owned_table` test derives the
//! project-owned set from both lineages' schemas, so a migration that adds a
//! project-owned table fails it rather than silently shrinking the export.
//!
//! # Consistency against a live project
//!
//! No transaction is held across the export. A read transaction left open on
//! the live orchestrator blocks the logical-log checkpoint and keeps every row
//! version written after it began in the heap, so an export holding one for the
//! forty-odd minutes a large project takes would grow the orchestrator's memory
//! with the log (tenet 1260). The export instead reads in two phases:
//!
//! 1. **Membership.** Before any row is fetched, every planned table's
//!    selection runs as `SELECT rowid FROM <table> WHERE <term>`, one short
//!    statement per term, in both databases. That fixes which rows each table
//!    will contain, all as close to one instant as the selections allow;
//!    `tables[].selected_at_ms` records when each was taken.
//! 2. **Pages.** Rows are then fetched in rowid order, a bounded number of
//!    rowids per short read transaction, with the selecting term checked again.
//!    A freed rowid can be reused by another project's insert, and the re-check
//!    is what keeps that row out of this export. A selected row absent from its
//!    page (deleted, or no longer the project's) is counted as `vanished`.
//!
//! So membership is fixed per table at its selection, each row is exported as
//! of its page, and tables are not mutually atomic. The manifest says so and
//! records, per table, when it was selected, when its first and last pages were
//! read, and its longest single transaction, which is the number that says the
//! export stayed out of the checkpoint's way.
//!
//! Until `export_probe_against_a_real_database_copy` has passed on a copy of a
//! production database, run exports of cairn-sized projects against an APFS
//! clone rather than the live instance. That probe is what shows each page stays
//! short at production size (tenet 1260).
//!
//! # Events
//!
//! `events` rows are passed through the transcript reader
//! ([`reconstruct_events_reporting`]) in bounded batches, so an archived row
//! comes out as the plain content an agent saw rather than as its git
//! coordinate or compressed blob, and the disclosure quarantine withholds
//! exactly what it withholds from a transcript. A row the reader cannot fully
//! resolve is written anyway, marked `unresolved`, with its stored form beside
//! it, and counted in the manifest; a withheld row carries no stored form.
//!
//! # Layout
//!
//! `tables/<table>.jsonl` holds one row per line, column names as keys, blob
//! values base64, every other value as stored (timestamps are not normalized;
//! the manifest labels the units each timestamp column was observed in).
//! `manifest.json` is written last, so its presence means the export finished.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::future::Future;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use cairn_db::turso::{Connection, Row, Value};
use serde_json::{json, Map, Value as Json};

use super::migrate::{
    delete_only, direct, owned, owner, OwnerIds, OwnerSet, TableCopySpec, TableLineage,
    CHANNEL_KEY_ADDRESSED, COPY_ORDER,
};
use crate::models::Event;
use crate::storage::events::columns::{event_from_row, EVENT_COLUMNS, EVENT_COLUMN_COUNT};
use crate::storage::{
    reconstruct_events_reporting, DbError, LocalDb, Reconstruction, RowExt, TrackedConnection,
    RECONSTRUCT_BATCH_BYTES, RECONSTRUCT_BATCH_ROWS,
};

/// Version of the on-disk layout described in the module docs. Version 2
/// replaced the single export-long snapshot with per-table membership and
/// short-transaction pages.
const FORMAT_VERSION: u32 = 2;

/// Bytes buffered per table file before a blocking write is handed off.
const WRITE_CHUNK_BYTES: usize = 8 * 1024 * 1024;

const fn export_only(table: &'static str, predicate: &'static str) -> TableCopySpec {
    delete_only(table, predicate)
}

/// The workspace label vocabulary is not project-owned, so the sweep never
/// deletes it (`1 = 0`); an export takes the labels the project's issues carry.
const EXPORT_LABELS: TableCopySpec = export_only(
    "labels",
    "id IN (SELECT label_id FROM issue_labels WHERE issue_id IN (SELECT id FROM issues WHERE project_id = ?1))",
);

/// Project-owned tables the ownership manifest does not list, because deletion
/// reaches them through `ON DELETE CASCADE` or does not need them (runner-local
/// ledgers keyed by job, run, or session id with no foreign key). Same
/// predicate grammar as the manifest, and the same lineage resolution through
/// `TABLE_SCOPES`: a private entry that reaches a shared id goes through an
/// [`OwnerSet`] so it is correct for a team project too.
const EXPORT_ONLY: &[TableCopySpec] = &[
    // Shared lineage: predicates may subquery the shared tables directly.
    export_only("execution_gates", "execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    export_only("execution_supersessions", "issue_id IN (SELECT id FROM issues WHERE project_id = ?1)"),
    export_only("job_file_activity", "job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    export_only("job_presence", "project_id = ?1 OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1)"),
    export_only("pack_catalog_backfill_attempts", "execution_id IN (SELECT id FROM executions WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1))"),
    export_only("pr_resolution_attributions", "merge_request_id IN (SELECT id FROM merge_requests WHERE project_id = ?1 OR issue_id IN (SELECT id FROM issues WHERE project_id = ?1) OR job_id IN (SELECT id FROM jobs WHERE project_id = ?1))"),
    export_only("tool_analytics_hourly", "project_id = ?1 OR run_id IN (SELECT id FROM runs WHERE project_id = ?1)"),
    // Private lineage, naming the project directly.
    export_only("config_disables", "project_id = ?1"),
    export_only("execution_queue", "project_id = ?1"),
    export_only("executor_preparation_refusals", "project_id = ?1"),
    export_only("held_publications", "project_id = ?1"),
    export_only("ingress_endpoints", "project_id = ?1"),
    export_only("ingress_events", "project_id = ?1"),
    export_only("ref_store_maintenance", "project_id = ?1"),
    export_only("ref_store_samples", "project_id = ?1"),
    export_only("route_firings", "project_id = ?1"),
    // Private lineage, reached through shared ids.
    owned("authorization_events", &[owner(OwnerSet::Runs, "run_id IN {ids}")]),
    owned("fact_log", &[direct("project_id = ?1"), owner(OwnerSet::Jobs, "actor_job_id IN {ids}")]),
    owned("fact_log_wakes", &[direct("project_id = ?1"), owner(OwnerSet::Jobs, "job_id IN {ids}")]),
    owned("pending_injections", &[owner(OwnerSet::Sessions, "session_id IN {ids}")]),
    owned("prepared_ordinary_launches", &[owner(OwnerSet::Jobs, "job_id IN {ids}"), owner(OwnerSet::Runs, "run_id IN {ids}")]),
    owned("prepared_runtime_calls", &[owner(OwnerSet::Runs, "run_id IN {ids}")]),
    owned("response_invocations", &[direct("project_id = ?1"), owner(OwnerSet::Runs, "caller_run_id IN {ids}")]),
    owned("runtime_admission_requests", &[owner(OwnerSet::Jobs, "request_id IN (SELECT request_id FROM prepared_ordinary_launches WHERE job_id IN {ids})"), owner(OwnerSet::Runs, "request_id IN (SELECT request_id FROM prepared_runtime_calls WHERE run_id IN {ids})")]),
    owned("team_ordinary_admission_authority", &[owner(OwnerSet::Jobs, "request_id IN (SELECT request_id FROM prepared_ordinary_launches WHERE job_id IN {ids})")]),
    owned("thread_automations", &[owner(OwnerSet::Threads, "thread_id IN {ids}")]),
    owned("thread_compaction_entries", &[owner(OwnerSet::Jobs, "job_id IN {ids}")]),
    owned("thread_compaction_marks", &[owner(OwnerSet::Jobs, "job_id IN {ids}")]),
    owned("thread_compactions", &[owner(OwnerSet::Jobs, "job_id IN {ids}")]),
    owned("thread_era_children", &[owner(OwnerSet::Jobs, "job_id IN {ids}")]),
    owned("thread_eras", &[owner(OwnerSet::Jobs, "job_id IN {ids}")]),
    owned("turn_embeddings", &[direct("project_id = ?1"), owner(OwnerSet::Jobs, "job_id IN {ids}")]),
    owned("workflow_call", &[owner(OwnerSet::Runs, "run_id IN {ids}"), owner(OwnerSet::Runs, "workflow_run_id IN {ids}")]),
    owned("workflow_journal", &[owner(OwnerSet::Runs, "run_id IN {ids}")]),
    owned("workflow_run", &[direct("project_id = ?1"), owner(OwnerSet::Jobs, "job_id IN {ids}"), owner(OwnerSet::Runs, "run_id IN {ids}")]),
    owned("write_replay_ledger", &[owner(OwnerSet::Runs, "run_id IN {ids}")]),
];

/// Private tables that name the project by key, beside the channel ledgers the
/// sweep already addresses that way. Each predicate binds `?1` to the canonical
/// lowercase key.
const EXPORT_KEYED: &[(&str, &str)] = &[("project_routes", "project_key = ?1")];

/// Tables the coverage test finds project-owned (by foreign-key chain or by an
/// owner-id column) that the export deliberately leaves out.
/// A manifest entry named here is dropped from the plan, so the sweep keeps it
/// while the export does not.
const EXPORT_EXCLUDED: &[(&str, &str)] = &[
    (
        "ci_logs_cache",
        "its run_id is a GitHub Actions run id, not a Cairn run; a refetchable cache of CI logs",
    ),
    (
        "search_outbox",
        "the search indexer's work queue: each row is (source_table, source_id, op, status), no content, naming a row exported in its own table; selecting it costs hours, because source_id is unindexed and checked against every event id the project has",
    ),
];

/// Where a planned table's rows are selected from and how.
#[derive(Debug, Clone)]
enum Selection {
    /// An ownership-manifest entry (or an export-only one in the same grammar).
    Owned(&'static TableCopySpec),
    /// OR'd key-addressed predicates, each binding `?1` to the project key.
    Keyed(Vec<&'static str>),
}

#[derive(Debug, Clone)]
struct PlannedTable {
    table: &'static str,
    lineage: TableLineage,
    selection: Selection,
}

/// Every table the export writes, in manifest order (parents before children).
fn export_plan() -> Result<Vec<PlannedTable>, DbError> {
    let mut plan = Vec::new();
    let excluded = |table: &str| EXPORT_EXCLUDED.iter().any(|(name, _)| *name == table);
    for spec in COPY_ORDER.iter().chain(EXPORT_ONLY) {
        if excluded(spec.table) {
            continue;
        }
        let spec = if spec.table == EXPORT_LABELS.table {
            &EXPORT_LABELS
        } else {
            spec
        };
        plan.push(PlannedTable {
            table: spec.table,
            lineage: spec.lineage()?,
            selection: Selection::Owned(spec),
        });
    }
    let mut keyed: Vec<(&'static str, Vec<&'static str>)> = Vec::new();
    let key_specs = CHANNEL_KEY_ADDRESSED
        .iter()
        .map(|spec| (spec.table, spec.sweep_where_sql))
        .chain(EXPORT_KEYED.iter().copied());
    for (table, predicate) in key_specs {
        match keyed.iter_mut().find(|(name, _)| *name == table) {
            Some((_, predicates)) => predicates.push(predicate),
            None => keyed.push((table, vec![predicate])),
        }
    }
    for (table, predicates) in keyed {
        plan.push(PlannedTable {
            table,
            lineage: super::migrate::table_lineage(table)?,
            selection: Selection::Keyed(predicates),
        });
    }
    Ok(plan)
}

/// What one export wrote, for the caller's one-line answer.
#[derive(Debug, Clone)]
pub struct ExportReport {
    pub path: PathBuf,
    pub tables: usize,
    pub rows: u64,
    pub bytes: u64,
    pub events: EventTally,
    /// Tables with rows selected for the export that were gone by the time
    /// their page was read, and how many.
    pub vanished: Vec<(String, u64)>,
    /// The longest single read transaction the export held.
    pub longest_transaction_ms: u128,
    pub elapsed_ms: u128,
}

impl ExportReport {
    pub fn summary(&self, project_key: &str) -> String {
        let mut text = format!(
            "Exported project {project_key} to {}: {} tables, {} rows, {} bytes in {:.1}s. \
             events: {} ({} plain, {} restored from archive, {} unresolved, {} withheld)",
            self.path.display(),
            self.tables,
            self.rows,
            self.bytes,
            self.elapsed_ms as f64 / 1000.0,
            self.events.rows,
            self.events.stored,
            self.events.restored,
            self.events.unresolved(),
            self.events.withheld,
        );
        text.push_str(&format!(
            ". Longest read transaction: {} ms",
            self.longest_transaction_ms
        ));
        // Informational, not a warning: on a live project, rows deleted or
        // moved between a table's selection and its page are expected.
        if !self.vanished.is_empty() {
            let total: u64 = self.vanished.iter().map(|(_, rows)| rows).sum();
            text.push_str(&format!(
                ". {total} selected rows were gone by the time their page was read \
                 (deleted or no longer the project's while the export ran): {}",
                self.vanished
                    .iter()
                    .map(|(table, rows)| format!("{table} ({rows})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        text
    }
}

/// Per-outcome event counts.
#[derive(Debug, Clone, Default)]
pub struct EventTally {
    pub rows: u64,
    pub stored: u64,
    pub restored: u64,
    pub degraded: u64,
    pub undecodable: u64,
    pub withheld: u64,
}

impl EventTally {
    pub fn unresolved(&self) -> u64 {
        self.degraded + self.undecodable
    }

    fn record(&mut self, outcome: Reconstruction) {
        self.rows += 1;
        match outcome {
            Reconstruction::Stored => self.stored += 1,
            Reconstruction::Restored => self.restored += 1,
            Reconstruction::Degraded => self.degraded += 1,
            Reconstruction::Undecodable => self.undecodable += 1,
            Reconstruction::Withheld => self.withheld += 1,
        }
    }

    fn to_json(&self) -> Json {
        json!({
            "rows": self.rows,
            "stored_plain": self.stored,
            "restored": self.restored,
            "degraded": self.degraded,
            "undecodable": self.undecodable,
            "unresolved": self.unresolved(),
            "withheld": self.withheld,
        })
    }
}

/// Describe what an export of `project_key` would write, without reading any
/// rows or touching `destination`.
pub async fn plan_summary(
    owning_db: &LocalDb,
    project_key: &str,
    destination: &Path,
) -> Result<String, String> {
    validate_destination(destination)?;
    let plan = export_plan().map_err(|error| error.to_string())?;
    let lineage = if owning_db.team_id().is_some() {
        "team replica + private"
    } else {
        "private"
    };
    Ok(format!(
        "Would export project {project_key} ({lineage}) to {}: {} tables, {} excluded",
        destination.display(),
        plan.len(),
        EXPORT_EXCLUDED.len()
    ))
}

fn validate_destination(destination: &Path) -> Result<(), String> {
    if !destination.is_absolute() {
        return Err(format!(
            "payload.path must be an absolute local directory, got {}",
            destination.display()
        ));
    }
    if destination.exists() {
        let mut entries = std::fs::read_dir(destination)
            .map_err(|error| format!("cannot read {}: {error}", destination.display()))?;
        if entries.next().is_some() {
            return Err(format!(
                "{} already exists and is not empty; export into a new or empty directory",
                destination.display()
            ));
        }
    }
    Ok(())
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

/// One short read transaction on a connection of its own.
///
/// Every read the export makes goes through one of these and ends it before
/// the next begins, so the longest transaction the export holds is one
/// membership statement or one page, never the export.
struct ShortRead {
    conn: TrackedConnection,
    started: Instant,
}

impl ShortRead {
    async fn begin(db: &LocalDb) -> Result<Self, String> {
        let started = Instant::now();
        let conn = db.connect().await.map_err(err)?;
        conn.execute(db.concurrent_begin(), ()).await.map_err(err)?;
        Ok(Self { conn, started })
    }

    /// End the transaction and report how long it was open.
    async fn finish(self) -> Duration {
        // A read transaction has nothing to commit; ROLLBACK ends it without
        // asking the engine to validate a write set.
        let _ = self.conn.execute("ROLLBACK", ()).await;
        self.started.elapsed()
    }
}

/// The start-of-export description of one database: its schema version and
/// how far its `events` rowids had reached.
async fn describe(db: &LocalDb, label: &'static str) -> Result<Json, String> {
    let described_at_ms = now_ms();
    let read = ShortRead::begin(db).await?;
    let schema = {
        let mut rows = read
            .conn
            .query(
                "SELECT version, name, (SELECT count(*) FROM cairn_schema_migrations) \
                 FROM cairn_schema_migrations ORDER BY version DESC LIMIT 1",
                (),
            )
            .await
            .map_err(err)?;
        match rows.next().await.map_err(err)? {
            Some(row) => json!({
                "latest_migration": row.text(0).map_err(err)?,
                "latest_migration_name": row.text(1).map_err(err)?,
                "applied_migrations": row.i64(2).map_err(err)?,
            }),
            None => Json::Null,
        }
    };
    let events_max_rowid = {
        let mut rows = read
            .conn
            .query("SELECT max(rowid) FROM events", ())
            .await
            .map_err(err)?;
        match rows.next().await.map_err(err)? {
            Some(row) => row.opt_i64(0).map_err(err)?,
            None => None,
        }
    };
    read.finish().await;
    Ok(json!({
        "database": label,
        "described_at_ms": described_at_ms,
        "schema": schema,
        "events_max_rowid": events_max_rowid,
    }))
}

/// The columns a table writes, read in a short transaction of its own.
async fn read_columns(db: &LocalDb, table: &str) -> Result<Vec<Column>, String> {
    let read = ShortRead::begin(db).await?;
    let columns = table_columns(&read.conn, table).await.map_err(err);
    read.finish().await;
    columns
}

/// One table file, written in large chunks off the async worker.
struct JsonlSink {
    writer: Option<BufWriter<File>>,
    pending: Vec<u8>,
    bytes: u64,
}

impl JsonlSink {
    fn create(path: &Path) -> Result<Self, String> {
        let file = File::create(path)
            .map_err(|error| format!("cannot create {}: {error}", path.display()))?;
        Ok(Self {
            writer: Some(BufWriter::new(file)),
            pending: Vec::with_capacity(WRITE_CHUNK_BYTES),
            bytes: 0,
        })
    }

    async fn push(&mut self, row: &Json) -> Result<(), String> {
        serde_json::to_writer(&mut self.pending, row).map_err(|error| error.to_string())?;
        self.pending.push(b'\n');
        if self.pending.len() >= WRITE_CHUNK_BYTES {
            self.flush(false).await?;
        }
        Ok(())
    }

    async fn flush(&mut self, finish: bool) -> Result<(), String> {
        let mut writer = self.writer.take().ok_or("export sink already closed")?;
        let chunk = std::mem::take(&mut self.pending);
        self.bytes += chunk.len() as u64;
        let writer = tokio::task::spawn_blocking(move || -> std::io::Result<BufWriter<File>> {
            writer.write_all(&chunk)?;
            if finish {
                writer.flush()?;
            }
            Ok(writer)
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| format!("writing export: {error}"))?;
        self.writer = Some(writer);
        Ok(())
    }

    async fn finish(mut self) -> Result<u64, String> {
        self.flush(true).await?;
        Ok(self.bytes)
    }
}

/// How timestamp-like values were observed, by magnitude, so the manifest can
/// label units without normalizing anything.
#[derive(Debug, Default)]
struct TimestampTally(BTreeMap<&'static str, u64>);

impl TimestampTally {
    fn observe(&mut self, value: &Value) {
        let class = match value {
            Value::Null => return,
            Value::Integer(n) => epoch_class(n.unsigned_abs() as f64),
            Value::Real(n) => epoch_class(n.abs()),
            Value::Text(_) => "text",
            Value::Blob(_) => "blob",
        };
        *self.0.entry(class).or_default() += 1;
    }

    fn to_json(&self) -> Json {
        let epoch: Vec<&str> = self
            .0
            .keys()
            .copied()
            .filter(|class| matches!(*class, "s" | "ms" | "us" | "ns" | "text"))
            .collect();
        let unit = match epoch.as_slice() {
            [] => "none",
            [one] => one,
            _ => "mixed",
        };
        json!({ "unit": unit, "observed": self.0 })
    }
}

/// The epoch unit a magnitude implies for dates after 2001. Below 1e9 is not an
/// epoch at all: zero, a duration, or a relative offset.
fn epoch_class(magnitude: f64) -> &'static str {
    if magnitude < 1e9 {
        "below_epoch_range"
    } else if magnitude < 1e11 {
        "s"
    } else if magnitude < 1e14 {
        "ms"
    } else if magnitude < 1e17 {
        "us"
    } else {
        "ns"
    }
}

fn is_timestamp_column(name: &str) -> bool {
    name == "timestamp"
        || name.ends_with("_at")
        || name.contains("_at_")
        || name.ends_with("_ts")
        || name.starts_with("ts_")
}

fn json_value(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Integer(n) => Json::from(*n),
        Value::Real(n) => serde_json::Number::from_f64(*n)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Text(text) => Json::String(text.clone()),
        Value::Blob(bytes) => Json::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
    }
}

#[derive(Debug, Clone)]
struct Column {
    name: String,
    declared: String,
}

async fn table_columns(conn: &Connection, table: &str) -> Result<Vec<Column>, DbError> {
    let mut rows = conn
        .query(&format!("PRAGMA table_info({table})"), ())
        .await?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next().await? {
        columns.push(Column {
            name: row.text(1)?,
            declared: row.opt_text(2)?.unwrap_or_default(),
        });
    }
    Ok(columns)
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The materialized owner id sets a private entry binds when the shared lineage
/// is another database, read from that database at the start of the export.
/// The private tables' membership is selected against these ids, so a shared
/// row created after this point contributes no private rows.
async fn materialize_owner_ids(db: &LocalDb, project_id: &str) -> Result<OwnerIds, String> {
    let mut sets = HashMap::new();
    for set in OwnerSet::ALL {
        let read = ShortRead::begin(db).await?;
        let ids = {
            let mut rows = read
                .conn
                .query(&set.select_ids_sql(), (project_id,))
                .await
                .map_err(err)?;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await.map_err(err)? {
                ids.push(row.text(0).map_err(err)?);
            }
            ids
        };
        read.finish().await;
        sets.insert(set, ids);
    }
    Ok(OwnerIds::Materialized(sets))
}

/// The project's identity, read from the database that owns its row.
struct ProjectRow {
    id: String,
    key: String,
    name: String,
}

async fn load_project(db: &LocalDb, project_key: &str) -> Result<ProjectRow, String> {
    let key = cairn_common::uri::canonical_project(project_key);
    let read = ShortRead::begin(db).await?;
    let project = {
        let mut rows = read
            .conn
            .query(
                "SELECT id, key, name FROM projects WHERE lower(key) = ?1 LIMIT 1",
                (key.as_str(),),
            )
            .await
            .map_err(err)?;
        match rows.next().await.map_err(err)? {
            Some(row) => Some(ProjectRow {
                id: row.text(0).map_err(err)?,
                key: row.text(1).map_err(err)?,
                name: row.text(2).map_err(err)?,
            }),
            None => None,
        }
    };
    read.finish().await;
    project.ok_or_else(|| format!("Project '{project_key}' not found"))
}

/// Run after each page with no transaction held. Production passes a no-op;
/// tests use it to checkpoint between pages.
type PageHook<'h> = &'h mut (dyn FnMut() -> PageHookFuture + Send);
type PageHookFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Export every row `project_key` owns into `destination`.
///
/// `owning_db` is the database that holds the project row (the team replica for
/// a team project, the private database otherwise); `private_db` is always the
/// per-install database. Takes owned handles so the caller can run it on a
/// detached task that outlives a dropped request.
pub async fn export_project(
    private_db: Arc<LocalDb>,
    owning_db: Arc<LocalDb>,
    project_key: String,
    destination: PathBuf,
) -> Result<ExportReport, String> {
    export_project_paged(
        private_db,
        owning_db,
        project_key,
        destination,
        RECONSTRUCT_BATCH_ROWS,
        &mut || -> PageHookFuture { Box::pin(async {}) },
    )
    .await
}

async fn export_project_paged(
    private_db: Arc<LocalDb>,
    owning_db: Arc<LocalDb>,
    project_key: String,
    destination: PathBuf,
    page_rows: usize,
    after_page: PageHook<'_>,
) -> Result<ExportReport, String> {
    let started = Instant::now();
    let exported_at_ms = now_ms();
    validate_destination(&destination)?;
    let plan = export_plan().map_err(err)?;
    let tables_dir = destination.join("tables");
    std::fs::create_dir_all(&tables_dir)
        .map_err(|error| format!("cannot create {}: {error}", tables_dir.display()))?;

    let team = owning_db.team_id().map(|team| team.to_string());
    let (shared_db, shared_label): (&LocalDb, &'static str) = match team {
        Some(_) => (owning_db.as_ref(), "team"),
        None => (private_db.as_ref(), "private"),
    };
    let project = load_project(shared_db, &project_key).await?;
    let owner_ids = match team {
        Some(_) => materialize_owner_ids(shared_db, &project.id)
            .await
            .map_err(|error| format!("materializing owner ids: {error}"))?,
        None => OwnerIds::SameDatabase,
    };
    let canonical_key = cairn_common::uri::canonical_project(&project.key);

    let mut databases = vec![describe(&private_db, "private").await?];
    if team.is_some() {
        databases.push(describe(shared_db, "team").await?);
    }

    // Phase 1: every table's membership, before any row is fetched.
    let mut resolved = Vec::with_capacity(plan.len());
    for planned in &plan {
        let (db, label): (&LocalDb, &'static str) = match planned.lineage {
            TableLineage::Shared => (shared_db, shared_label),
            TableLineage::Private => (private_db.as_ref(), "private"),
        };
        let predicates = table_predicates(planned, &project.id, &owner_ids, &canonical_key)?;
        let columns = read_columns(db, planned.table).await?;
        if columns.is_empty() {
            return Err(format!(
                "{} has no columns in the {label} database",
                planned.table
            ));
        }
        let queries = query_terms(&predicates);
        let membership = select_membership(db, planned.table, &queries)
            .await
            .map_err(|error| format!("selecting {}: {error}", planned.table))?;
        log::info!(
            "project export {}: selected {} rows of {} in {} ms ({:.1}s elapsed)",
            project.key,
            membership.rows.len(),
            planned.table,
            membership.longest.as_millis(),
            started.elapsed().as_secs_f64()
        );
        resolved.push(ResolvedTable {
            planned,
            label,
            columns,
            queries,
            membership,
        });
    }

    // Phase 2: the rows, a page at a time.
    let mut tables = Vec::new();
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    let mut events = EventTally::default();
    let mut vanished = Vec::new();
    let mut longest = Duration::ZERO;
    let mut paging = Paging {
        rows: page_rows.max(1),
        after_page,
    };
    for table in &resolved {
        let db: &LocalDb = match table.planned.lineage {
            TableLineage::Shared => shared_db,
            TableLineage::Private => private_db.as_ref(),
        };
        let name = table.planned.table;
        let written = export_table(db, &owning_db, table, &tables_dir, &mut events, &mut paging)
            .await
            .map_err(|error| format!("exporting {name}: {error}"))?;
        if written.vanished > 0 {
            vanished.push((name.to_string(), written.vanished));
        }
        let table_longest = written.longest.max(table.membership.longest);
        longest = longest.max(table_longest);
        total_rows += written.rows;
        total_bytes += written.bytes;
        log::info!(
            "project export {}: {} rows of {} ({} bytes, {} pages, longest transaction {} ms, \
             {:.1}s elapsed)",
            project.key,
            written.rows,
            name,
            written.bytes,
            written.pages,
            table_longest.as_millis(),
            started.elapsed().as_secs_f64()
        );
        tables.push(json!({
            "table": name,
            "file": format!("tables/{name}.jsonl"),
            "database": table.label,
            "selected": table.membership.rows.len(),
            "rows": written.rows,
            "vanished": written.vanished,
            "pages": written.pages,
            "selected_at_ms": table.membership.selected_at_ms,
            "first_page_at_ms": written.first_page_at_ms,
            "last_page_at_ms": written.last_page_at_ms,
            "longest_transaction_ms": table_longest.as_millis() as u64,
            "bytes": written.bytes,
            "columns": written.columns,
            "timestamps": written.timestamps,
        }));
    }

    let finished_ms = now_ms();
    let manifest = json!({
        "format": "cairn-project-export",
        "format_version": FORMAT_VERSION,
        "project": {
            "key": project.key,
            "id": project.id,
            "name": project.name,
            "lineage": if team.is_some() { "team" } else { "private" },
            "team_id": team,
        },
        "instance": {
            "hostname": hostname::get().ok().map(|name| name.to_string_lossy().into_owned()),
            "cairn_version": env!("CARGO_PKG_VERSION"),
            "private_database": private_db.path().display().to_string(),
        },
        "exported_at_ms": exported_at_ms,
        "finished_at_ms": finished_ms,
        "databases": databases,
        "consistency": format!(
            "no transaction spans the export. Each table's membership is fixed when it is \
             selected (tables[].selected_at_ms), and every table is selected before any row is \
             fetched. Rows are then read in rowid order, at most {} per short read transaction, \
             with the selection re-checked, so a row is exported as of its page and tables are \
             not mutually atomic{}. A selected row absent from its page (deleted, or no longer \
             the project's) is counted in tables[].vanished.",
            paging.rows,
            if team.is_some() {
                "; the private database and the team replica are read separately"
            } else {
                ""
            },
        ),
        "encoding": {
            "rows": "one JSON object per line, column names as keys, in rowid order",
            "blob": "base64 (standard alphabet, padded)",
            "timestamps": "as stored; `tables[].timestamps` labels the unit each timestamp-named column's values imply by magnitude (s, ms, us, ns), `mixed` when more than one occurs",
            "events": "`data` is reconstructed plain content; `reconstruction` names the reader's outcome; an `unresolved` row also carries its stored form under `stored`; a withheld row carries none",
        },
        "events": events.to_json(),
        "totals": {
            "tables": tables.len(),
            "rows": total_rows,
            "bytes": total_bytes,
            "vanished": vanished.iter().map(|(_, rows)| rows).sum::<u64>(),
            "longest_transaction_ms": longest.as_millis() as u64,
        },
        "tables": tables,
        "excluded": EXPORT_EXCLUDED
            .iter()
            .map(|(table, reason)| json!({ "table": table, "reason": reason }))
            .collect::<Vec<_>>(),
    });
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(err)?;
    let partial = destination.join("manifest.json.partial");
    std::fs::write(&partial, &manifest_bytes)
        .and_then(|_| std::fs::rename(&partial, destination.join("manifest.json")))
        .map_err(|error| format!("writing manifest: {error}"))?;

    Ok(ExportReport {
        path: destination.to_path_buf(),
        tables: plan.len(),
        rows: total_rows,
        bytes: total_bytes + manifest_bytes.len() as u64,
        events,
        vanished,
        longest_transaction_ms: longest.as_millis(),
        elapsed_ms: started.elapsed().as_millis(),
    })
}

/// The predicates that select a planned table's rows for this project, each
/// with its own parameter list.
fn table_predicates(
    planned: &PlannedTable,
    project_id: &str,
    owner_ids: &OwnerIds,
    canonical_key: &str,
) -> Result<Vec<(String, Vec<Value>)>, String> {
    Ok(match &planned.selection {
        Selection::Owned(spec) => match planned.lineage {
            TableLineage::Shared => vec![(
                spec.local_where_sql(),
                vec![Value::Text(project_id.to_string())],
            )],
            TableLineage::Private => spec
                .private_predicates(project_id, owner_ids)
                .map_err(err)?,
        },
        Selection::Keyed(predicates) => vec![(
            predicates
                .iter()
                .map(|predicate| format!("({predicate})"))
                .collect::<Vec<_>>()
                .join(" OR "),
            vec![Value::Text(canonical_key.to_string())],
        )],
    })
}

/// The column list a table's pages select, and the index `rowid` lands at
/// after it. `events` selects the canonical event projection first so its rows
/// parse as events.
fn page_select(table: &str, columns: &[Column]) -> (String, usize) {
    if table == "events" {
        let writer = EventWriter::new(columns);
        (writer.select_list(), writer.columns.len())
    } else {
        (
            columns
                .iter()
                .map(|column| quoted(&column.name))
                .collect::<Vec<_>>()
                .join(", "),
            columns.len(),
        )
    }
}

/// One page's statement for one term: the page's rowids, re-checked against
/// the term that selected them.
///
/// The rowid list must be the access path. Whenever the term has an index of
/// its own, the pinned Turso plans `rowid IN (<list>) AND (<term>)` from that
/// index and checks the list per row, whether the list is literals or a
/// `VALUES` subquery, so every page walked all of the project's rows in the
/// table (about a second a page at 100k project events, growing with the
/// project, which makes the export quadratic). So the page's rows are fetched
/// by rowid alone in a derived table, which the term then filters. The derived
/// table carries the table's own name, so a term that names the table, or a
/// subquery in it that does, resolves as it did in the selection.
/// `every_page_statement_seeks_the_rowid_primary_key` holds that plan.
fn page_sql(table: &str, select_list: &str, term: &str, rowids: &[i64]) -> String {
    let list = rowids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {select_list}, rowid FROM \
         (SELECT *, rowid AS rowid FROM {table} WHERE rowid IN ({list})) AS {table} \
         WHERE ({term})"
    )
}

/// The rows one table will contain, fixed when it was selected.
#[derive(Debug)]
struct Membership {
    /// Selected rowids, ascending, each with the index (into the table's
    /// query terms) of the first term that selected it. The page re-checks a
    /// row against that term.
    rows: Vec<(i64, usize)>,
    selected_at_ms: i64,
    /// The longest of the selection's statements.
    longest: Duration,
}

/// A planned table resolved against this project.
struct ResolvedTable<'p> {
    planned: &'p PlannedTable,
    label: &'static str,
    columns: Vec<Column>,
    queries: Vec<(String, Vec<Value>)>,
    membership: Membership,
}

struct Paging<'h> {
    rows: usize,
    after_page: PageHook<'h>,
}

/// The statements that select a table: each predicate's top-level `OR` terms
/// as their own statements.
///
/// Turso builds an `IN (subquery)` list once when the IN stands alone, but
/// rebuilds it for every row when it sits under an OR: the manifest's
/// `search_outbox` predicate, whose last term lists every event id the project
/// has, took over two hours that way on a 186k-row table, against seconds per
/// term on their own.
fn query_terms(predicates: &[(String, Vec<Value>)]) -> Vec<(String, Vec<Value>)> {
    predicates
        .iter()
        .flat_map(|(predicate, params)| match or_terms(predicate) {
            Some(terms) => terms
                .into_iter()
                .map(|term| (term.to_string(), params.clone()))
                .collect(),
            None => vec![(predicate.clone(), params.clone())],
        })
        .collect()
}

/// Select a table's membership: the rowids its terms select, each term one
/// short statement. Terms (and separate predicates) may select a row twice;
/// the union keeps it once.
async fn select_membership(
    db: &LocalDb,
    table: &str,
    queries: &[(String, Vec<Value>)],
) -> Result<Membership, String> {
    let selected_at_ms = now_ms();
    let mut first_term: HashMap<i64, usize> = HashMap::new();
    let mut longest = Duration::ZERO;
    for (index, (term, params)) in queries.iter().enumerate() {
        let read = ShortRead::begin(db).await?;
        {
            let mut rows = read
                .conn
                .query(
                    &format!("SELECT rowid FROM {table} WHERE {term}"),
                    params.clone(),
                )
                .await
                .map_err(err)?;
            while let Some(row) = rows.next().await.map_err(err)? {
                first_term.entry(row.i64(0).map_err(err)?).or_insert(index);
            }
        }
        longest = longest.max(read.finish().await);
    }
    let mut rows: Vec<(i64, usize)> = first_term.into_iter().collect();
    rows.sort_unstable();
    Ok(Membership {
        rows,
        selected_at_ms,
        longest,
    })
}

struct TableWritten {
    rows: u64,
    vanished: u64,
    pages: u64,
    first_page_at_ms: Option<i64>,
    last_page_at_ms: Option<i64>,
    /// The longest page transaction; the selection's is on the membership.
    longest: Duration,
    bytes: u64,
    columns: Json,
    timestamps: Json,
}

/// One fetched row, held until its page's transaction has ended.
enum PageRow {
    Plain(Json),
    Event(Box<(Map<String, Json>, Event)>),
}

/// Write one table's selected rows, a page of rowids per short transaction.
///
/// Within a page, each term present runs as one [`page_sql`] statement over
/// the page's rowids for that term, re-checking the term so a rowid freed and
/// reused by another project's row is never written. Rowids are integers the
/// database produced, so they are inlined rather than bound, leaving the
/// term's own `?N` placeholders untouched. Rows are written in rowid order after the page's
/// transaction has ended, and `events` rows are reconstructed only then.
async fn export_table(
    db: &LocalDb,
    owning_db: &LocalDb,
    table: &ResolvedTable<'_>,
    tables_dir: &Path,
    events: &mut EventTally,
    paging: &mut Paging<'_>,
) -> Result<TableWritten, String> {
    let name = table.planned.table;
    let columns = &table.columns;
    let mut sink = JsonlSink::create(&tables_dir.join(format!("{name}.jsonl")))?;
    let mut tallies: Vec<Option<TimestampTally>> = columns
        .iter()
        .map(|column| is_timestamp_column(&column.name).then(TimestampTally::default))
        .collect();
    let mut writer = (name == "events").then(|| EventWriter::new(columns));
    let (select_list, rowid_index) = page_select(name, columns);

    let mut written = TableWritten {
        rows: 0,
        vanished: 0,
        pages: 0,
        first_page_at_ms: None,
        last_page_at_ms: None,
        longest: Duration::ZERO,
        bytes: 0,
        columns: Json::Null,
        timestamps: Json::Null,
    };
    for page in table.membership.rows.chunks(paging.rows) {
        let mut by_term: BTreeMap<usize, Vec<i64>> = BTreeMap::new();
        for (rowid, term) in page {
            by_term.entry(*term).or_default().push(*rowid);
        }
        let page_at_ms = now_ms();
        written.first_page_at_ms.get_or_insert(page_at_ms);
        written.last_page_at_ms = Some(page_at_ms);

        let mut fetched: BTreeMap<i64, PageRow> = BTreeMap::new();
        let read = ShortRead::begin(db).await?;
        for (term_index, rowids) in &by_term {
            let (term, params) = &table.queries[*term_index];
            let sql = page_sql(name, &select_list, term, rowids);
            let mut rows = read.conn.query(&sql, params.clone()).await.map_err(err)?;
            while let Some(row) = rows.next().await.map_err(err)? {
                let rowid = row.i64(rowid_index).map_err(err)?;
                let item = match &writer {
                    Some(writer) => PageRow::Event(Box::new(writer.read_row(&row, &mut tallies)?)),
                    None => PageRow::Plain(plain_object(&row, columns, &mut tallies)?),
                };
                fetched.insert(rowid, item);
            }
        }
        written.longest = written.longest.max(read.finish().await);
        written.pages += 1;
        written.vanished += (page.len() - fetched.len()) as u64;

        for item in fetched.into_values() {
            match (item, writer.as_mut()) {
                (PageRow::Event(event), Some(writer)) => {
                    writer.push(*event);
                    if writer.is_full() {
                        written.rows += writer.drain(owning_db, &mut sink, events).await?;
                    }
                }
                (PageRow::Plain(object), _) => {
                    sink.push(&object).await?;
                    written.rows += 1;
                }
                (PageRow::Event(_), None) => unreachable!("event rows come only from events"),
            }
        }
        if let Some(writer) = writer.as_mut() {
            written.rows += writer.drain(owning_db, &mut sink, events).await?;
        }
        (paging.after_page)().await;
    }

    written.bytes = sink.finish().await?;
    written.columns = Json::Array(
        columns
            .iter()
            .map(|column| {
                let blob = column.declared.eq_ignore_ascii_case("BLOB");
                json!({
                    "name": column.name,
                    "declared_type": column.declared,
                    "encoding": if blob { "base64" } else { "as stored" },
                })
            })
            .collect(),
    );
    written.timestamps = Json::Object(
        columns
            .iter()
            .zip(&tallies)
            .filter_map(|(column, tally)| {
                tally
                    .as_ref()
                    .map(|tally| (column.name.clone(), tally.to_json()))
            })
            .collect(),
    );
    Ok(written)
}

/// One non-event row as a JSON object, observing its timestamp columns.
fn plain_object(
    row: &Row,
    columns: &[Column],
    tallies: &mut [Option<TimestampTally>],
) -> Result<Json, String> {
    let mut object = Map::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let value = row.get_value(index).map_err(err)?;
        if let Some(tally) = tallies[index].as_mut() {
            tally.observe(&value);
        }
        object.insert(column.name.clone(), json_value(&value));
    }
    Ok(Json::Object(object))
}

/// A predicate's top-level `OR` terms, when it has more than one and each binds
/// `?1` (so the predicate's one parameter list fits every term). Parentheses
/// and quoted literals are respected; anything else returns `None` and the
/// predicate runs whole.
fn or_terms(predicate: &str) -> Option<Vec<&str>> {
    let bytes = predicate.as_bytes();
    let mut terms = Vec::new();
    let (mut depth, mut quote, mut start, mut index) = (0i32, None, 0usize, 0usize);
    while index < bytes.len() {
        let byte = bytes[index];
        match quote {
            Some(open) if byte == open => quote = None,
            Some(_) => {}
            None => match byte {
                b'\'' | b'"' => quote = Some(byte),
                b'(' => depth += 1,
                b')' => depth -= 1,
                b' ' if depth == 0 && bytes[index..].starts_with(b" OR ") => {
                    terms.push(predicate[start..index].trim());
                    start = index + " OR ".len();
                    index = start;
                    continue;
                }
                _ => {}
            },
        }
        index += 1;
    }
    terms.push(predicate[start..].trim());
    (terms.len() > 1 && terms.iter().all(|term| term.contains("?1"))).then_some(terms)
}

/// Batches `events` rows through the transcript reader and writes each as its
/// reconstructed content.
struct EventWriter {
    /// Every `events` column, in select order: the canonical [`EVENT_COLUMNS`]
    /// projection first (so [`event_from_row`] parses the prefix), then any
    /// column it does not name.
    columns: Vec<String>,
    /// For each entry of `columns`, its index in the table's own column list,
    /// which is what the timestamp tallies are keyed by.
    table_index: Vec<usize>,
    batch: Vec<(Map<String, Json>, Event)>,
    batch_bytes: usize,
}

impl EventWriter {
    fn new(table_columns: &[Column]) -> Self {
        let mut columns: Vec<String> = EVENT_COLUMNS
            .split(',')
            .map(|name| name.trim().to_string())
            .collect();
        debug_assert_eq!(columns.len(), EVENT_COLUMN_COUNT);
        for column in table_columns {
            if !columns.contains(&column.name) {
                columns.push(column.name.clone());
            }
        }
        let table_index = columns
            .iter()
            .map(|name| {
                table_columns
                    .iter()
                    .position(|column| &column.name == name)
                    .unwrap_or(usize::MAX)
            })
            .collect();
        Self {
            columns,
            table_index,
            batch: Vec::new(),
            batch_bytes: 0,
        }
    }

    fn select_list(&self) -> String {
        self.columns
            .iter()
            .map(|name| quoted(name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// One fetched row as its output object (less `data`, filled from the
    /// reader) and the stored event the reader reconstructs.
    fn read_row(
        &self,
        row: &Row,
        tallies: &mut [Option<TimestampTally>],
    ) -> Result<(Map<String, Json>, Event), String> {
        let event = event_from_row(row).map_err(|error| error.to_string())?;
        let mut object = Map::with_capacity(self.columns.len() + 2);
        for (index, name) in self.columns.iter().enumerate() {
            let value = row.get_value(index).map_err(|error| error.to_string())?;
            if let Some(Some(tally)) = tallies.get_mut(self.table_index[index]) {
                tally.observe(&value);
            }
            // `data` and `data_blob` are filled from the reader's output.
            if name != "data" && name != "data_blob" {
                object.insert(name.clone(), json_value(&value));
            }
        }
        Ok((object, event))
    }

    fn push(&mut self, (object, event): (Map<String, Json>, Event)) {
        self.batch_bytes += event.data.len() + event.data_blob.as_ref().map_or(0, Vec::len);
        self.batch.push((object, event));
    }

    fn is_full(&self) -> bool {
        self.batch.len() >= RECONSTRUCT_BATCH_ROWS || self.batch_bytes >= RECONSTRUCT_BATCH_BYTES
    }

    async fn drain(
        &mut self,
        owning_db: &LocalDb,
        sink: &mut JsonlSink,
        tally: &mut EventTally,
    ) -> Result<u64, String> {
        if self.batch.is_empty() {
            return Ok(0);
        }
        let batch = std::mem::take(&mut self.batch);
        self.batch_bytes = 0;
        let (objects, stored): (Vec<_>, Vec<_>) = batch.into_iter().unzip();
        let reconstructed = reconstruct_events_reporting(owning_db, stored.clone()).await;
        let count = objects.len() as u64;
        for ((mut object, stored), (event, outcome)) in
            objects.into_iter().zip(stored).zip(reconstructed)
        {
            tally.record(outcome);
            object.insert("data".to_string(), Json::String(event.data));
            object.insert("data_blob".to_string(), Json::Null);
            object.insert(
                "reconstruction".to_string(),
                Json::String(outcome_label(outcome).to_string()),
            );
            if outcome.is_unresolved() {
                object.insert("unresolved".to_string(), Json::Bool(true));
                object.insert("stored".to_string(), stored_form(&stored));
            }
            sink.push(&Json::Object(object)).await?;
        }
        Ok(count)
    }
}

fn outcome_label(outcome: Reconstruction) -> &'static str {
    match outcome {
        Reconstruction::Stored => "stored_plain",
        Reconstruction::Restored => "restored",
        Reconstruction::Degraded => "degraded",
        Reconstruction::Undecodable => "undecodable",
        Reconstruction::Withheld => "withheld",
    }
}

/// The archival columns of an unresolved row exactly as stored, so whatever the
/// reader could not resolve is still in the record for someone who can.
fn stored_form(event: &Event) -> Json {
    json!({
        "storage_mode": event.storage_mode,
        "content_commit": event.content_commit,
        "content_render_sha": event.content_render_sha,
        "codec": event.codec,
        "data": event.data,
        "data_blob": event
            .data_blob
            .as_ref()
            .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
    })
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use super::*;
    use crate::projects::migrate::tests::{migrated_db, seed_complete_project_graph};
    use crate::storage::{compress, TEAM_MIGRATIONS, TURSO_MIGRATIONS};

    /// Columns that name an owner by id. A table carrying one is project-owned
    /// whether or not a foreign key says so: the admission and launch ledgers
    /// key on job and run ids with no constraint at all.
    const OWNER_COLUMNS: &[&str] = &[
        "project_id",
        "project_key",
        "issue_id",
        "thread_id",
        "execution_id",
        "job_id",
        "run_id",
        "session_id",
    ];

    async fn names(db: &LocalDb, sql: String) -> Vec<String> {
        db.query_all(sql, (), |row| row.text(0)).await.unwrap()
    }

    /// Every table in `db` that a foreign-key chain ties to `projects`, or that
    /// names an owner by an owner-id column.
    async fn project_owned_tables(db: &LocalDb) -> BTreeSet<String> {
        let tables = names(
            db,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
             AND name NOT LIKE '__turso%'"
                .to_string(),
        )
        .await;
        let mut edges = Vec::new();
        let mut owned = BTreeSet::new();
        for table in &tables {
            for parent in names(
                db,
                format!("SELECT \"table\" FROM pragma_foreign_key_list('{table}')"),
            )
            .await
            {
                edges.push((table.clone(), parent));
            }
            let columns = names(db, format!("SELECT name FROM pragma_table_info('{table}')")).await;
            if columns
                .iter()
                .any(|column| OWNER_COLUMNS.contains(&column.as_str()))
            {
                owned.insert(table.clone());
            }
        }
        let mut reachable: HashSet<String> = HashSet::from(["projects".to_string()]);
        loop {
            let discovered: Vec<String> = edges
                .iter()
                .filter(|(child, parent)| reachable.contains(parent) && !reachable.contains(child))
                .map(|(child, _)| child.clone())
                .collect();
            if discovered.is_empty() {
                break;
            }
            reachable.extend(discovered);
        }
        owned.extend(reachable);
        owned
    }

    /// Every table a migration ties to a project, in either lineage, is either
    /// exported or excluded with a reason. A new project-owned table fails here
    /// instead of silently leaving the export incomplete.
    #[tokio::test]
    async fn export_plan_covers_every_project_owned_table() {
        let private = migrated_db("export-coverage-private.db", TURSO_MIGRATIONS.to_vec()).await;
        let team = migrated_db("export-coverage-team.db", TEAM_MIGRATIONS.to_vec()).await;
        let mut owned = project_owned_tables(&private).await;
        owned.extend(project_owned_tables(&team).await);

        let plan: HashSet<&str> = export_plan()
            .unwrap()
            .iter()
            .map(|entry| entry.table)
            .collect();
        let excluded: HashSet<&str> = EXPORT_EXCLUDED.iter().map(|(table, _)| *table).collect();
        // The team-only fleet roots are the team's, not any one project's.
        let team_roots = [
            "teams",
            "device_presence",
            "remote_intents",
            "executor_registry",
        ];
        let missing: Vec<&String> = owned
            .iter()
            .filter(|table| {
                !plan.contains(table.as_str())
                    && !excluded.contains(table.as_str())
                    && !team_roots.contains(&table.as_str())
            })
            .collect();
        assert!(
            missing.is_empty(),
            "project-owned tables neither exported nor excluded with a reason: {missing:?}. \
             Add each to the ownership manifest (projects/migrate.rs COPY_ORDER), to \
             EXPORT_ONLY, or to EXPORT_EXCLUDED with why"
        );
    }

    /// The plan names each table once, never both exports and excludes one,
    /// and every entry is a real table in the database its lineage says holds it.
    #[tokio::test]
    async fn export_plan_entries_are_real_distinct_and_lineage_correct() {
        let private = migrated_db("export-plan-private.db", TURSO_MIGRATIONS.to_vec()).await;
        let team = migrated_db("export-plan-team.db", TEAM_MIGRATIONS.to_vec()).await;
        let private_tables: HashSet<String> = names(
            &private,
            "SELECT name FROM sqlite_master WHERE type = 'table'".into(),
        )
        .await
        .into_iter()
        .collect();
        let team_tables: HashSet<String> = names(
            &team,
            "SELECT name FROM sqlite_master WHERE type = 'table'".into(),
        )
        .await
        .into_iter()
        .collect();
        let plan = export_plan().unwrap();
        let mut seen = HashSet::new();
        for entry in &plan {
            assert!(seen.insert(entry.table), "{} is planned twice", entry.table);
            assert!(
                private_tables.contains(entry.table),
                "{} is not a table",
                entry.table
            );
            assert_eq!(
                entry.lineage == TableLineage::Shared,
                team_tables.contains(entry.table),
                "{} is planned against the wrong lineage",
                entry.table
            );
        }
        for (table, reason) in EXPORT_EXCLUDED {
            assert!(
                !seen.contains(table),
                "{table} is both exported and excluded"
            );
            assert!(
                private_tables.contains(*table),
                "excluded {table} is not a table"
            );
            assert!(!reason.is_empty());
        }
    }

    async fn query_plan(db: &LocalDb, sql: &str, binds: Vec<Value>) -> String {
        let sql = format!("EXPLAIN QUERY PLAN {sql}");
        db.read(move |conn| {
            Box::pin(async move {
                let mut rows = conn.query(&sql, binds).await?;
                let mut plan = String::new();
                while let Some(row) = rows.next().await? {
                    plan.push_str(&row.text(3)?);
                    plan.push('\n');
                }
                Ok(plan)
            })
        })
        .await
        .unwrap()
    }

    /// Every page statement the export can issue seeks its rows on the rowid
    /// primary key, for every planned table and every term, private and team.
    ///
    /// Behavior cannot catch the regression this guards: a page planned from
    /// the term's index returns the same rows, only by walking every project
    /// row in the table each page. The plan is the only witness.
    #[tokio::test]
    async fn every_page_statement_seeks_the_rowid_primary_key() {
        let db = migrated_db("export-page-plans.db", TURSO_MIGRATIONS.to_vec()).await;
        let materialized = OwnerIds::Materialized(
            OwnerSet::ALL
                .into_iter()
                .map(|set| (set, vec!["owner-a".to_string(), "owner-b".to_string()]))
                .collect(),
        );
        let mut checked = 0;
        let mut failures = Vec::new();
        for planned in export_plan().unwrap() {
            let columns = read_columns(&db, planned.table).await.unwrap();
            let (select_list, _) = page_select(planned.table, &columns);
            let mut owner_cases = vec![OwnerIds::SameDatabase];
            if planned.lineage == TableLineage::Private {
                owner_cases.push(materialized.clone());
            }
            for owner_ids in &owner_cases {
                let predicates = table_predicates(&planned, "project-p", owner_ids, "prj").unwrap();
                for (term, params) in query_terms(&predicates) {
                    let sql = page_sql(planned.table, &select_list, &term, &[1, 2, 3]);
                    let plan = query_plan(&db, &sql, params).await;
                    checked += 1;
                    let seek = format!("SEARCH {} USING INTEGER PRIMARY KEY", planned.table);
                    if !plan.contains(&seek) {
                        failures.push(format!("{}: {term}\n{plan}", planned.table));
                    }
                }
            }
        }
        assert!(checked > 50, "only {checked} page statements were checked");
        assert!(
            failures.is_empty(),
            "{} page statements do not seek the rowid:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }

    fn no_op_hook() -> impl FnMut() -> PageHookFuture + Send {
        || -> PageHookFuture { Box::pin(async {}) }
    }

    async fn insert_other_project(db: &LocalDb) {
        db.execute_batch(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('project-other', 'ws-delete', 'Other', 'OTHER', '/tmp/other', 1, 1);
             INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
               VALUES ('issue-other', 'project-other', 1, 'Other', 1, 1);",
        )
        .await
        .unwrap();
    }

    /// Membership is fixed at selection and the page re-checks it. A selected
    /// rowid that is gone by its page counts as vanished, and a selected rowid
    /// that now holds another project's row (a freed rowid reused by an insert)
    /// is never written.
    #[tokio::test]
    async fn a_row_gone_by_its_page_is_vanished_and_a_reused_rowid_is_not_written() {
        let db = migrated_db("export-vanished.db", TURSO_MIGRATIONS.to_vec()).await;
        seed_complete_project_graph(&db).await;
        insert_other_project(&db).await;
        let rowid_of = |id: &'static str| {
            let db = &db;
            async move {
                db.query_opt_i64("SELECT rowid FROM issues WHERE id = ?1", (id,))
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        let ours = rowid_of("issue-delete").await;
        let theirs = rowid_of("issue-other").await;
        let missing = ours.max(theirs) + 1_000;

        let plan = export_plan().unwrap();
        let planned = plan.iter().find(|entry| entry.table == "issues").unwrap();
        let mut rows = vec![(ours, 0), (theirs, 0), (missing, 0)];
        rows.sort_unstable();
        let table = ResolvedTable {
            planned,
            label: "private",
            columns: read_columns(&db, "issues").await.unwrap(),
            queries: vec![(
                "project_id = ?1".to_string(),
                vec![Value::Text("project-delete".to_string())],
            )],
            membership: Membership {
                rows,
                selected_at_ms: 0,
                longest: Duration::ZERO,
            },
        };
        let out = tempfile::tempdir().unwrap();
        let mut hook = no_op_hook();
        let mut paging = Paging {
            rows: RECONSTRUCT_BATCH_ROWS,
            after_page: &mut hook,
        };
        let written = export_table(
            &db,
            &db,
            &table,
            out.path(),
            &mut EventTally::default(),
            &mut paging,
        )
        .await
        .unwrap();

        assert_eq!(written.rows, 1);
        assert_eq!(written.vanished, 2);
        assert_eq!(written.pages, 1);
        assert_eq!(db.live_connections(), 0);
        let issues = read_jsonl(&out.path().join("issues.jsonl"));
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0]["id"], "issue-delete");
    }

    /// The point of paging: between any two pages the export holds no
    /// connection, so a checkpoint pass drains at once and folds the log.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_checkpoint_drains_and_folds_between_export_pages() {
        let db = Arc::new(migrated_db("export-checkpoint.db", TURSO_MIGRATIONS.to_vec()).await);
        seed_complete_project_graph(&db).await;
        insert_other_project(&db).await;
        let reports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut hook = {
            let db = db.clone();
            let reports = reports.clone();
            move || -> PageHookFuture {
                let db = db.clone();
                let reports = reports.clone();
                Box::pin(async move {
                    let live = db.live_connections();
                    let report = db
                        .checkpoint(
                            1,
                            Duration::ZERO,
                            crate::storage::DrainBudget::fixed(Duration::from_millis(100)),
                        )
                        .await
                        .unwrap();
                    reports.lock().unwrap().push((live, report));
                })
            }
        };
        let out = tempfile::tempdir().unwrap();
        let report = export_project_paged(
            db.clone(),
            db.clone(),
            "delete".to_string(),
            out.path().join("export"),
            1,
            &mut hook,
        )
        .await
        .unwrap();

        let reports = reports.lock().unwrap();
        assert!(
            reports.len() as u64 >= report.rows,
            "one page per row at a page size of one: {} pages, {} rows",
            reports.len(),
            report.rows
        );
        for (live, checkpoint) in reports.iter() {
            assert_eq!(*live, 0, "a connection outlived its page");
            assert!(checkpoint.drain.drained, "{checkpoint}");
            assert!(checkpoint.succeeded(), "{checkpoint}");
        }
        assert!(report.vanished.is_empty(), "{:?}", report.vanished);
    }

    /// Tenet 1260's acceptance for export, on a real project: every checkpoint
    /// pass during the export drains, no holder is older than five seconds, the
    /// log stays under its ceiling, and no table's longest transaction reaches
    /// two seconds. Run against an APFS clone of the live database set:
    ///
    /// ```text
    /// cp -c cairn.db cairn.db-wal cairn.db-log /tmp/probe/
    /// CAIRN_CHECKPOINT_PROBE_DB=/tmp/probe/cairn.db \
    ///   bun run test:rust -p cairn-core --lib export_probe -- --ignored --nocapture
    /// ```
    ///
    /// `CAIRN_EXPORT_PROBE_PROJECT` names the project (default `cairn`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs a real database copy via CAIRN_CHECKPOINT_PROBE_DB"]
    async fn export_probe_against_a_real_database_copy() {
        use crate::storage::checkpoint_probe::under_checkpoint_pressure;
        let path = std::env::var("CAIRN_CHECKPOINT_PROBE_DB")
            .expect("set CAIRN_CHECKPOINT_PROBE_DB to a copy of a real database set");
        let project =
            std::env::var("CAIRN_EXPORT_PROBE_PROJECT").unwrap_or_else(|_| "cairn".to_string());
        crate::mcp::handlers::read::register_archived_file_renderer();
        let db = Arc::new(LocalDb::open(&path).await.unwrap());
        let out = tempfile::tempdir().unwrap();
        let destination = out.path().join("export");

        let (report, pressure) = under_checkpoint_pressure(
            db.clone(),
            export_project(db.clone(), db.clone(), project.clone(), destination.clone()),
        )
        .await;
        let report = report.unwrap();
        println!("{}", report.summary(&project));
        println!("{pressure}");
        let manifest: Json = serde_json::from_str(
            &std::fs::read_to_string(destination.join("manifest.json")).unwrap(),
        )
        .unwrap();
        let mut slow = Vec::new();
        for table in manifest["tables"].as_array().unwrap() {
            let longest = table["longest_transaction_ms"].as_u64().unwrap();
            println!(
                "  {:<40} selected={:<9} rows={:<9} vanished={:<5} pages={:<6} longest_ms={}",
                table["table"].as_str().unwrap(),
                table["selected"],
                table["rows"],
                table["vanished"],
                table["pages"],
                longest
            );
            if longest >= 2_000 {
                slow.push(format!("{} ({longest} ms)", table["table"]));
            }
        }
        for pass in &pressure.passes {
            if !pass.drain.drained || !pass.succeeded() {
                println!("  failed pass: {pass}");
            }
        }
        pressure.assert_stayed_out_of_the_way();
        assert!(
            slow.is_empty(),
            "tables over the 2 s transaction gate: {slow:?}"
        );
    }

    fn read_jsonl(path: &Path) -> Vec<Json> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn export_writes_the_projects_rows_with_events_reconstructed() {
        let db = Arc::new(migrated_db("export-graph.db", TURSO_MIGRATIONS.to_vec()).await);
        seed_complete_project_graph(&db).await;
        // A second project whose rows must not leak into the export.
        insert_other_project(&db).await;
        let original = "{\"eventType\":\"user\",\"content\":\"archived words\"}";
        let blob = compress(original.as_bytes()).unwrap();
        db.execute(
            "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, storage_mode, data_blob, codec)
               VALUES ('ev-zstd', 'run-delete', 1, 1, 'user', '{\"_archived\":true}', 1, 'zstd', ?1, ?2)",
            (
                Value::Blob(blob),
                Value::Text(crate::storage::CODEC_ZSTD_V1.to_string()),
            ),
        )
        .await
        .unwrap();
        db.execute_batch(&format!(
            "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
               VALUES ('ev-plain', 'run-delete', 2, 2, 'assistant', '{{\"content\":\"plain\"}}', 2);
             INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, storage_mode, content_commit)
               VALUES ('ev-dangling', 'run-delete', 3, 3, 'tool_result',
                       '{{\"toolInput\":{{\"paths\":[\"file:a.txt\"]}}}}', 3, 'gitcoord', '{}');",
            "0".repeat(40)
        ))
        .await
        .unwrap();

        let out = tempfile::tempdir().unwrap();
        let destination = out.path().join("export");
        let report = export_project(
            db.clone(),
            db.clone(),
            "delete".to_string(),
            destination.clone(),
        )
        .await
        .unwrap();
        assert!(report.vanished.is_empty(), "{:?}", report.vanished);
        assert_eq!(db.live_connections(), 0);

        let manifest: Json = serde_json::from_str(
            &std::fs::read_to_string(destination.join("manifest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest["format_version"], 2);
        assert_eq!(manifest["project"]["id"], "project-delete");
        assert_eq!(manifest["project"]["lineage"], "private");
        assert_eq!(manifest["totals"]["vanished"], 0);
        assert_eq!(manifest["databases"][0]["database"], "private");
        for table in manifest["tables"].as_array().unwrap() {
            assert_eq!(table["selected"], table["rows"], "{}", table["table"]);
            assert_eq!(table["vanished"], 0, "{}", table["table"]);
            assert!(table["selected_at_ms"].as_i64().is_some());
            assert!(table["longest_transaction_ms"].as_u64().is_some());
            if table["rows"].as_u64() > Some(0) {
                assert!(table["pages"].as_u64() >= Some(1), "{}", table["table"]);
                assert!(
                    table["first_page_at_ms"].as_i64() >= table["selected_at_ms"].as_i64(),
                    "{}",
                    table["table"]
                );
            }
            let file = destination.join(table["file"].as_str().unwrap());
            assert_eq!(
                read_jsonl(&file).len() as u64,
                table["rows"].as_u64().unwrap()
            );
        }

        let issues = read_jsonl(&destination.join("tables/issues.jsonl"));
        assert_eq!(
            issues.len(),
            1,
            "another project's issue leaked into the export"
        );
        assert_eq!(issues[0]["id"], "issue-delete");
        assert_eq!(read_jsonl(&destination.join("tables/posts.jsonl")).len(), 1);
        assert_eq!(
            read_jsonl(&destination.join("tables/threads.jsonl")).len(),
            1
        );

        let events = read_jsonl(&destination.join("tables/events.jsonl"));
        let by_id = |id: &str| {
            events
                .iter()
                .find(|event| event["id"] == id)
                .unwrap()
                .clone()
        };
        let restored = by_id("ev-zstd");
        assert_eq!(restored["data"], original);
        assert_eq!(restored["data_blob"], Json::Null);
        assert_eq!(restored["reconstruction"], "restored");
        assert_eq!(by_id("ev-plain")["reconstruction"], "stored_plain");
        let dangling = by_id("ev-dangling");
        assert_eq!(dangling["unresolved"], true);
        assert_eq!(dangling["stored"]["storage_mode"], "gitcoord");
        assert_eq!(dangling["stored"]["content_commit"], "0".repeat(40));

        assert_eq!(manifest["events"]["rows"], 3);
        assert_eq!(manifest["events"]["restored"], 1);
        assert_eq!(manifest["events"]["unresolved"], 1);
        let created = &manifest["tables"]
            .as_array()
            .unwrap()
            .iter()
            .find(|table| table["table"] == "events")
            .unwrap()["timestamps"]["created_at"];
        assert_eq!(
            created["unit"], "none",
            "fixture timestamps are below the epoch range"
        );
    }

    #[tokio::test]
    async fn export_refuses_a_relative_or_non_empty_destination() {
        assert!(validate_destination(Path::new("relative/dir")).is_err());
        let out = tempfile::tempdir().unwrap();
        std::fs::write(out.path().join("x"), b"x").unwrap();
        assert!(validate_destination(out.path()).is_err());
        assert!(validate_destination(&out.path().join("fresh")).is_ok());
    }

    #[test]
    fn or_terms_split_only_at_the_top_level() {
        assert_eq!(
            or_terms("project_id = ?1 OR run_id IN (SELECT id FROM runs WHERE project_id = ?1 OR issue_id = ?1)"),
            Some(vec![
                "project_id = ?1",
                "run_id IN (SELECT id FROM runs WHERE project_id = ?1 OR issue_id = ?1)",
            ])
        );
        // One term, a term that does not bind ?1, or an OR inside a literal.
        assert_eq!(or_terms("project_id = ?1"), None);
        assert_eq!(or_terms("project_id = ?1 OR 1 = 0"), None);
        assert_eq!(or_terms("kind = 'a OR b' AND project_id = ?1"), None);
    }

    #[test]
    fn epoch_classes_follow_magnitude() {
        assert_eq!(epoch_class(1.7e9), "s");
        assert_eq!(epoch_class(1.7e12), "ms");
        assert_eq!(epoch_class(1.7e15), "us");
        assert_eq!(epoch_class(1.7e18), "ns");
        assert_eq!(epoch_class(5.0), "below_epoch_range");
        let mut tally = TimestampTally::default();
        tally.observe(&Value::Integer(1_700_000_000));
        tally.observe(&Value::Integer(1_700_000_000_000));
        assert_eq!(tally.to_json()["unit"], "mixed");
    }
}
