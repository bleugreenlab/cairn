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
//! Each database the project's rows live in is read through ONE read
//! transaction held for the whole export: the private database always, and the
//! team replica too for a team project. Turso's MVCC gives that transaction a
//! snapshot, so a project that keeps running while it is exported contributes
//! nothing written after the snapshot opened, and every table's `count(*)` is
//! taken inside the same transaction as the rows it counts. There is no
//! cross-database transaction, so a team project's two snapshots are each
//! consistent but not mutually atomic; the manifest records both. Holding the
//! transaction defers the private database's log checkpoint until the export
//! finishes (see `ConnectionGate` in cairn-db), which is the price of a
//! consistent record and is bounded by the export's duration.
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

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
};

/// Version of the on-disk layout described in the module docs.
const FORMAT_VERSION: u32 = 1;

/// Events reconstructed per reader call. Bounded by count and by stored bytes so
/// one batch never holds more than a few tens of megabytes of transcript.
const EVENT_BATCH_ROWS: usize = 512;
const EVENT_BATCH_BYTES: usize = 32 * 1024 * 1024;

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
    pub mismatched_counts: Vec<String>,
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
        if !self.mismatched_counts.is_empty() {
            text.push_str(&format!(
                ". WARNING: rows written differ from count(*) at the snapshot for: {}",
                self.mismatched_counts.join(", ")
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

/// Describe what an export of `project_key` would write, without opening a
/// snapshot or touching `destination`.
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

/// A read transaction held open for the whole export: one database's snapshot.
struct Snapshot {
    conn: TrackedConnection,
    label: &'static str,
    opened_at_ms: i64,
}

impl Snapshot {
    async fn open(db: &LocalDb, label: &'static str) -> Result<Self, DbError> {
        let conn = db.connect().await?;
        conn.execute(db.concurrent_begin(), ()).await?;
        Ok(Self {
            conn,
            label,
            opened_at_ms: chrono::Utc::now().timestamp_millis(),
        })
    }

    async fn close(self) {
        // A read transaction has nothing to commit; ROLLBACK ends it without
        // asking the engine to validate a write set.
        let _ = self.conn.execute("ROLLBACK", ()).await;
    }

    async fn optional_i64(&self, sql: &str) -> Result<Option<i64>, DbError> {
        let mut rows = self.conn.query(sql, ()).await?;
        match rows.next().await? {
            Some(row) => Ok(row.opt_i64(0)?),
            None => Ok(None),
        }
    }

    async fn describe(&self) -> Result<Json, DbError> {
        let mut rows = self
            .conn
            .query(
                "SELECT version, name, (SELECT count(*) FROM cairn_schema_migrations) \
                 FROM cairn_schema_migrations ORDER BY version DESC LIMIT 1",
                (),
            )
            .await?;
        let schema = match rows.next().await? {
            Some(row) => json!({
                "latest_migration": row.text(0)?,
                "latest_migration_name": row.text(1)?,
                "applied_migrations": row.i64(2)?,
            }),
            None => Json::Null,
        };
        drop(rows);
        let events_max_rowid = self.optional_i64("SELECT max(rowid) FROM events").await?;
        Ok(json!({
            "database": self.label,
            "opened_at_ms": self.opened_at_ms,
            "schema": schema,
            "events_max_rowid": events_max_rowid,
        }))
    }
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
/// is another database, read from that database's snapshot so they agree with
/// the shared rows the export writes.
async fn materialize_from_snapshot(
    snapshot: &Snapshot,
    project_id: &str,
) -> Result<OwnerIds, DbError> {
    let mut sets = HashMap::new();
    for set in OwnerSet::ALL {
        let mut rows = snapshot
            .conn
            .query(&set.select_ids_sql(), (project_id,))
            .await?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await? {
            ids.push(row.text(0)?);
        }
        sets.insert(set, ids);
    }
    Ok(OwnerIds::Materialized(sets))
}

/// The project's identity, read inside the owning snapshot.
struct ProjectRow {
    id: String,
    key: String,
    name: String,
}

async fn load_project(snapshot: &Snapshot, project_key: &str) -> Result<ProjectRow, String> {
    let key = cairn_common::uri::canonical_project(project_key);
    let mut rows = snapshot
        .conn
        .query(
            "SELECT id, key, name FROM projects WHERE lower(key) = ?1 LIMIT 1",
            (key.as_str(),),
        )
        .await
        .map_err(|error| error.to_string())?;
    let row = rows
        .next()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("Project '{project_key}' not found"))?;
    let project = ProjectRow {
        id: row.text(0).map_err(|error| error.to_string())?,
        key: row.text(1).map_err(|error| error.to_string())?,
        name: row.text(2).map_err(|error| error.to_string())?,
    };
    Ok(project)
}

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
    let started = Instant::now();
    validate_destination(&destination)?;
    let plan = export_plan().map_err(|error| error.to_string())?;
    let tables_dir = destination.join("tables");
    std::fs::create_dir_all(&tables_dir)
        .map_err(|error| format!("cannot create {}: {error}", tables_dir.display()))?;

    let team = owning_db.team_id().map(|team| team.to_string());
    let private = Snapshot::open(&private_db, "private")
        .await
        .map_err(|error| format!("opening the private snapshot: {error}"))?;
    let shared = match team {
        Some(_) => Some(
            Snapshot::open(&owning_db, "team")
                .await
                .map_err(|error| format!("opening the team-replica snapshot: {error}"))?,
        ),
        None => None,
    };
    let result = export_within(
        &private,
        shared.as_ref(),
        &owning_db,
        &private_db.path().display().to_string(),
        team.as_deref(),
        &project_key,
        &destination,
        &plan,
        started,
    )
    .await;
    private.close().await;
    if let Some(shared) = shared {
        shared.close().await;
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn export_within(
    private: &Snapshot,
    shared: Option<&Snapshot>,
    owning_db: &LocalDb,
    private_db_path: &str,
    team: Option<&str>,
    project_key: &str,
    destination: &Path,
    plan: &[PlannedTable],
    started: Instant,
) -> Result<ExportReport, String> {
    let shared_snapshot = shared.unwrap_or(private);
    let project = load_project(shared_snapshot, project_key).await?;
    let owner_ids = match shared {
        Some(shared) => materialize_from_snapshot(shared, &project.id)
            .await
            .map_err(|error| format!("materializing owner ids: {error}"))?,
        None => OwnerIds::SameDatabase,
    };
    let canonical_key = cairn_common::uri::canonical_project(&project.key);

    let mut snapshots = vec![private
        .describe()
        .await
        .map_err(|error| error.to_string())?];
    if let Some(shared) = shared {
        snapshots.push(shared.describe().await.map_err(|error| error.to_string())?);
    }

    let mut tables = Vec::new();
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    let mut events = EventTally::default();
    let mut mismatched = Vec::new();
    for planned in plan {
        let snapshot = match planned.lineage {
            TableLineage::Shared => shared_snapshot,
            TableLineage::Private => private,
        };
        let predicates: Vec<(String, Vec<Value>)> = match &planned.selection {
            Selection::Owned(spec) => match planned.lineage {
                TableLineage::Shared => vec![(
                    spec.local_where_sql(),
                    vec![Value::Text(project.id.clone())],
                )],
                TableLineage::Private => spec
                    .private_predicates(&project.id, &owner_ids)
                    .map_err(|error| error.to_string())?,
            },
            Selection::Keyed(predicates) => vec![(
                predicates
                    .iter()
                    .map(|predicate| format!("({predicate})"))
                    .collect::<Vec<_>>()
                    .join(" OR "),
                vec![Value::Text(canonical_key.clone())],
            )],
        };
        log::info!(
            "project export {}: exporting {} ({:.1}s elapsed)",
            project.key,
            planned.table,
            started.elapsed().as_secs_f64()
        );
        let written = export_table(
            snapshot,
            owning_db,
            planned.table,
            &predicates,
            &destination.join("tables"),
            &mut events,
        )
        .await
        .map_err(|error| format!("exporting {}: {error}", planned.table))?;
        if written
            .count_at_snapshot
            .is_some_and(|count| count != written.rows)
        {
            mismatched.push(planned.table.to_string());
        }
        total_rows += written.rows;
        total_bytes += written.bytes;
        log::info!(
            "project export {}: {} rows of {} ({} bytes)",
            project.key,
            written.rows,
            planned.table,
            written.bytes
        );
        tables.push(json!({
            "table": planned.table,
            "file": format!("tables/{}.jsonl", planned.table),
            "database": snapshot.label,
            "rows": written.rows,
            "count_at_snapshot": written.count_at_snapshot,
            "bytes": written.bytes,
            "columns": written.columns,
            "timestamps": written.timestamps,
        }));
    }

    let finished_ms = chrono::Utc::now().timestamp_millis();
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
            "private_database": private_db_path,
        },
        "exported_at_ms": private.opened_at_ms,
        "finished_at_ms": finished_ms,
        "snapshots": snapshots,
        "consistency": if team.is_some() {
            "one read transaction per database (private, team replica); each is a snapshot, the two are not mutually atomic"
        } else {
            "one read transaction over the private database; every table and count(*) comes from the same snapshot"
        },
        "encoding": {
            "rows": "one JSON object per line, column names as keys",
            "blob": "base64 (standard alphabet, padded)",
            "timestamps": "as stored; `tables[].timestamps` labels the unit each timestamp-named column's values imply by magnitude (s, ms, us, ns), `mixed` when more than one occurs",
            "events": "`data` is reconstructed plain content; `reconstruction` names the reader's outcome; an `unresolved` row also carries its stored form under `stored`; a withheld row carries none",
        },
        "events": events.to_json(),
        "totals": { "tables": tables.len(), "rows": total_rows, "bytes": total_bytes },
        "count_mismatches": mismatched,
        "tables": tables,
        "excluded": EXPORT_EXCLUDED
            .iter()
            .map(|(table, reason)| json!({ "table": table, "reason": reason }))
            .collect::<Vec<_>>(),
    });
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| error.to_string())?;
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
        mismatched_counts: mismatched,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

struct TableWritten {
    rows: u64,
    count_at_snapshot: Option<u64>,
    bytes: u64,
    columns: Json,
    timestamps: Json,
}

async fn export_table(
    snapshot: &Snapshot,
    owning_db: &LocalDb,
    table: &str,
    predicates: &[(String, Vec<Value>)],
    tables_dir: &Path,
    events: &mut EventTally,
) -> Result<TableWritten, String> {
    let conn = &*snapshot.conn;
    let columns = table_columns(conn, table)
        .await
        .map_err(|error| error.to_string())?;
    if columns.is_empty() {
        return Err(format!(
            "{table} has no columns in the {} database",
            snapshot.label
        ));
    }
    let mut sink = JsonlSink::create(&tables_dir.join(format!("{table}.jsonl")))?;
    // Each top-level OR term runs as its own query. Turso builds an `IN
    // (subquery)` list once when the IN stands alone, but rebuilds it for every
    // row when it sits under an OR: the manifest's `search_outbox` predicate,
    // whose last term lists every event id the project has, took over two hours
    // that way on a 186k-row table, against seconds per term on their own.
    let queries: Vec<(String, Vec<Value>)> = predicates
        .iter()
        .flat_map(|(predicate, params)| match or_terms(predicate) {
            Some(terms) => terms
                .into_iter()
                .map(|term| (term.to_string(), params.clone()))
                .collect(),
            None => vec![(predicate.clone(), params.clone())],
        })
        .collect();
    // A single predicate's count inside the same snapshot is an independent
    // check on what was written. Split into terms, it is the number of distinct
    // rowids the terms select, each term read on its own: a UNION of them nested
    // in a FROM is again a context where Turso rebuilds each IN list per row.
    // Separate predicates (a team project's private entries, one per id chunk)
    // have no single statement to count.
    let count_at_snapshot = match predicates {
        [(predicate, params)] => match or_terms(predicate) {
            Some(terms) => {
                let mut selected: HashSet<i64> = HashSet::new();
                for term in terms {
                    let mut rows = conn
                        .query(
                            &format!("SELECT rowid FROM {table} WHERE {term}"),
                            params.clone(),
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    while let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
                        selected.insert(row.i64(0).map_err(|error| error.to_string())?);
                    }
                }
                Some(selected.len() as u64)
            }
            None => {
                let mut rows = conn
                    .query(
                        &format!("SELECT count(*) FROM {table} WHERE {predicate}"),
                        params.clone(),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let row = rows.next().await.map_err(|error| error.to_string())?;
                row.map(|row| row.i64(0).map(|n| n as u64))
                    .transpose()
                    .map_err(|error| error.to_string())?
            }
        },
        _ => None,
    };
    // Terms of one predicate, like separate predicates, may select a row twice.
    let dedupe = queries.len() > 1;
    let mut seen: HashSet<i64> = HashSet::new();
    let mut tallies: Vec<Option<TimestampTally>> = columns
        .iter()
        .map(|column| is_timestamp_column(&column.name).then(TimestampTally::default))
        .collect();
    let mut rows_written = 0u64;

    if table == "events" {
        let mut writer = EventWriter::new(&columns);
        for (predicate, params) in &queries {
            let sql = format!(
                "SELECT {}, rowid FROM events WHERE {predicate}",
                writer.select_list()
            );
            let mut rows = conn
                .query(&sql, params.clone())
                .await
                .map_err(|error| error.to_string())?;
            while let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
                if dedupe {
                    let rowid = row
                        .i64(writer.columns.len())
                        .map_err(|error| error.to_string())?;
                    if !seen.insert(rowid) {
                        continue;
                    }
                }
                writer.buffer(&row, &mut tallies)?;
                if writer.is_full() {
                    rows_written += writer.drain(owning_db, &mut sink, events).await?;
                }
            }
        }
        rows_written += writer.drain(owning_db, &mut sink, events).await?;
    } else {
        let select_list = columns
            .iter()
            .map(|column| quoted(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        for (predicate, params) in &queries {
            let sql = format!("SELECT {select_list}, rowid FROM {table} WHERE {predicate}");
            let mut rows = conn
                .query(&sql, params.clone())
                .await
                .map_err(|error| error.to_string())?;
            while let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
                if dedupe {
                    let rowid = row.i64(columns.len()).map_err(|error| error.to_string())?;
                    if !seen.insert(rowid) {
                        continue;
                    }
                }
                let mut object = Map::with_capacity(columns.len());
                for (index, column) in columns.iter().enumerate() {
                    let value = row.get_value(index).map_err(|error| error.to_string())?;
                    if let Some(tally) = tallies[index].as_mut() {
                        tally.observe(&value);
                    }
                    object.insert(column.name.clone(), json_value(&value));
                }
                sink.push(&Json::Object(object)).await?;
                rows_written += 1;
            }
        }
    }

    let bytes = sink.finish().await?;
    Ok(TableWritten {
        rows: rows_written,
        count_at_snapshot,
        bytes,
        columns: Json::Array(
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
        ),
        timestamps: Json::Object(
            columns
                .iter()
                .zip(&tallies)
                .filter_map(|(column, tally)| {
                    tally
                        .as_ref()
                        .map(|tally| (column.name.clone(), tally.to_json()))
                })
                .collect(),
        ),
    })
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

    fn buffer(&mut self, row: &Row, tallies: &mut [Option<TimestampTally>]) -> Result<(), String> {
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
        self.batch_bytes += event.data.len() + event.data_blob.as_ref().map_or(0, Vec::len);
        self.batch.push((object, event));
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.batch.len() >= EVENT_BATCH_ROWS || self.batch_bytes >= EVENT_BATCH_BYTES
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

    /// A snapshot opened before a write does not see it: the property that lets
    /// the export run against a live project and still match its own counts.
    #[tokio::test]
    async fn a_snapshot_does_not_see_writes_made_after_it_opened() {
        let db = migrated_db("export-snapshot.db", TURSO_MIGRATIONS.to_vec()).await;
        seed_complete_project_graph(&db).await;
        let snapshot = Snapshot::open(&db, "private").await.unwrap();
        let before = snapshot
            .optional_i64("SELECT count(*) FROM issues WHERE project_id = 'project-delete'")
            .await
            .unwrap();
        db.execute(
            "INSERT INTO issues(id, project_id, number, title, created_at, updated_at) \
             VALUES ('issue-late', 'project-delete', 2, 'Late', 2, 2)",
            (),
        )
        .await
        .unwrap();
        let during = snapshot
            .optional_i64("SELECT count(*) FROM issues WHERE project_id = 'project-delete'")
            .await
            .unwrap();
        snapshot.close().await;
        assert_eq!(before, Some(1));
        assert_eq!(
            during, before,
            "the snapshot saw a write made after it opened"
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
        db.execute_batch(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('project-other', 'ws-delete', 'Other', 'OTHER', '/tmp/other', 1, 1);
             INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
               VALUES ('issue-other', 'project-other', 1, 'Other', 1, 1);",
        )
        .await
        .unwrap();
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
        assert!(
            report.mismatched_counts.is_empty(),
            "{:?}",
            report.mismatched_counts
        );

        let manifest: Json = serde_json::from_str(
            &std::fs::read_to_string(destination.join("manifest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest["project"]["id"], "project-delete");
        assert_eq!(manifest["project"]["lineage"], "private");
        for table in manifest["tables"].as_array().unwrap() {
            if let Some(count) = table["count_at_snapshot"].as_u64() {
                assert_eq!(table["rows"].as_u64(), Some(count), "{}", table["table"]);
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
