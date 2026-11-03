//! Storage census of a Cairn database CLONE (cairn/5545, tenet 1303).
//!
//! Measures what the database holds, what grows, and what the history tier
//! would move, so retention windows and the hot budget are set from numbers.
//! Point it at an APFS clone of the three-file set, never at the live file:
//!
//!   cp -c ~/.cairn/cairn.db ~/.cairn/cairn.db-log ~/.cairn/cairn.db-wal <dir>/
//!
//! taken with the app quit (exact) or while it runs (approximate: the three
//! files are cloned at slightly different instants). Every subcommand refuses
//! a database inside a live Cairn data directory. `fold` checkpoints the clone
//! it is given; `delete-probe` clones its input again and deletes only from
//! that sibling copy. `scripts/db-census.ts` drives every subcommand, caps
//! each process's memory, and renders the report.
//!
//! Each subcommand prints one JSON document on stdout:
//!
//!   fold <db>                            TRUNCATE-checkpoint the clone's logical log
//!   tables <db> <as_of_unix_s>           per-table rows, payload bytes, 30-day growth
//!   history <db> <as_of_unix_s>          history bytes by owner class and resolution age
//!   sql <db> <statement>                 one ad-hoc statement, rows as JSON
//!   vacuum <db> <dest> --unbounded-memory   time VACUUM INTO, then fold the image
//!   delete-probe <db> <pages> [rows]     delete the oldest events a page per write,
//!                                        on a sibling clone of <db>
//!
//! These are clone-only measurements, not a live instrument. Tables are walked
//! in rowid ranges of 50k rows (one span of `events` reads about 100 MB), a
//! table with sparse rowids is read in one statement, and `history` loads the
//! skeleton tables whole. `vacuum` needs the explicit flag because on the
//! pinned Turso `VACUUM INTO` holds the whole image in memory: 80 GB against
//! the 30 GB production database.
//!
//! Usage:
//!   cargo run --release -p cairn-db --example db_census -- tables <clone>/cairn.db 1793332400

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use cairn_db::storage::{db_set_paths, db_set_size, DrainBudget, LocalDb, TrackedConnection};
use cairn_db::turso::Value;
use serde_json::{json, Value as Json};

type Res<T> = Result<T, String>;

const DAY_S: i64 = 86_400;
/// Rowid span per range read. Large rows make a span expensive, not long:
/// every read here is its own statement on a clone.
const SPAN: i64 = 50_000;
/// Above this many spans the rowids are not dense enough to walk by range.
const MAX_SPANS: i64 = 200_000;
/// The production fold threshold (`db_maintenance::FOLD_THRESHOLD_BYTES`).
const FOLD_BYTES: u64 = 16 * 1024 * 1024;
/// Issue statuses after which an issue's executions are resolved history.
const RESOLVED_ISSUE: &[&str] = &["merged", "closed", "complete", "failed"];
/// Resolution-age windows (days) the hot-window projection is reported for.
const WINDOWS: &[i64] = &[7, 14, 30, 60, 90];

/// Data directories a live Cairn keeps its database in, relative to `$HOME`.
const LIVE_DIRS: &[&str] = &[".cairn", "Library/Application Support/com.cairn.desktop"];

/// Columns tried, in order, as a table's creation time. A row's age only
/// measures growth when the column is stamped once, at insert.
const CREATED_COLUMNS: &[&str] = &[
    "created_at",
    "occurred_at_ms",
    "recorded_at",
    "minted_at",
    "ran_at",
    "written_at",
    "evaluated_at",
    "decided_at",
    "started_at",
    "timestamp",
    "ts",
    "bucket_start",
    "surfaced_at",
    "fired_at",
    "finished_at_unix_ms",
    "attempted_at",
    "found_at",
    "discovered_at",
    "corrected_at",
    "imported_at",
    "applied_at",
    "booted_at",
    "fetched_at",
    "probed_at",
    "marked_at",
    "computed_at",
];

/// Fallback age columns that are rewritten in place: a recent value means the
/// row was touched, not that it is new, so they never count as growth.
const UPDATED_COLUMNS: &[&str] = &["updated_at", "updated_at_unix_ms"];

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // The database, and for `vacuum` the destination.
    let paths = if args.first().map(String::as_str) == Some("vacuum") {
        2
    } else {
        1
    };
    for path in args.iter().skip(1).take(paths) {
        if let Err(error) = check_path(path) {
            eprintln!("db_census: {error}");
            std::process::exit(2);
        }
    }
    let result = match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["fold", db] => fold(db).await,
        ["tables", db, as_of] => tables(db, parse(as_of)).await,
        ["history", db, as_of] => history(db, parse(as_of)).await,
        ["vacuum", db, dest, "--unbounded-memory"] => vacuum(db, dest).await,
        ["vacuum", ..] => Err(
            "vacuum holds the whole image in memory on the pinned Turso \
             (80 GB for a 30 GB database); pass --unbounded-memory to accept that"
                .to_string(),
        ),
        ["delete-probe", db, pages] => delete_probe(db, parse(pages), 500).await,
        ["delete-probe", db, pages, rows] => delete_probe(db, parse(pages), parse(rows)).await,
        ["sql", db, sql] => adhoc(db, sql).await,
        _ => Err(
            "usage: db_census fold|tables|history|vacuum|delete-probe <db> ... \
             (see the module docs)"
                .to_string(),
        ),
    };
    match result {
        Ok(doc) => println!("{}", serde_json::to_string_pretty(&doc).unwrap()),
        Err(error) => {
            eprintln!("db_census: {error}");
            std::process::exit(1);
        }
    }
}

/// Refuse a path inside a live Cairn data directory. A path that does not end
/// in `.db` is refused by `LocalDb::open` itself.
fn check_path(path: &str) -> Res<()> {
    let Some(home) = std::env::var_os("HOME") else {
        return Ok(());
    };
    let dir = Path::new(path)
        .parent()
        .map(|p| {
            if p.as_os_str().is_empty() {
                Path::new(".")
            } else {
                p
            }
        })
        .and_then(|p| std::fs::canonicalize(p).ok());
    for live in LIVE_DIRS {
        let live = std::fs::canonicalize(PathBuf::from(&home).join(live)).ok();
        if live.is_some() && dir == live {
            return Err(format!(
                "{path} is in a live Cairn data directory; clone it first (see the module docs)"
            ));
        }
    }
    Ok(())
}

fn parse(arg: &str) -> i64 {
    arg.parse()
        .unwrap_or_else(|_| panic!("expected an integer, got {arg:?}"))
}

async fn open(path: &str) -> Res<(LocalDb, f64)> {
    let started = Instant::now();
    let db = LocalDb::open(path)
        .await
        .map_err(|e| format!("open {path}: {e}"))?;
    Ok((db, started.elapsed().as_secs_f64()))
}

async fn rows(conn: &TrackedConnection, sql: &str) -> Res<Vec<Vec<Value>>> {
    let mut rows = conn
        .query(sql, ())
        .await
        .map_err(|e| format!("{e}: {sql}"))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await.map_err(|e| format!("{e}: {sql}"))? {
        let mut values = Vec::with_capacity(row.column_count());
        for idx in 0..row.column_count() {
            values.push(row.get_value(idx).map_err(|e| e.to_string())?);
        }
        out.push(values);
    }
    Ok(out)
}

async fn one(conn: &TrackedConnection, sql: &str) -> Res<Vec<Value>> {
    rows(conn, sql)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| format!("no row: {sql}"))
}

fn int(value: &Value) -> i64 {
    match value {
        Value::Integer(i) => *i,
        Value::Real(r) => *r as i64,
        _ => 0,
    }
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::Text(t) => Some(t.clone()),
        _ => None,
    }
}

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn log_bytes(path: &str) -> u64 {
    std::fs::metadata(format!("{path}-log"))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn file_bytes(path: &str) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

async fn fold(path: &str) -> Res<Json> {
    let before = log_bytes(path);
    let (db, open_s) = open(path).await?;
    let report = db
        .checkpoint(3, Duration::from_millis(250), DrainBudget::PRODUCTION)
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({
        "open_s": open_s,
        "log_bytes_before_open": before,
        "log_bytes_after": report.log_bytes_after,
        "checkpoint_s": report.duration.as_secs_f64(),
        "attempts": report.attempts,
        "error": report.error,
        "db_bytes": file_bytes(path),
    }))
}

struct Column {
    name: String,
    numeric: bool,
}

async fn table_names(conn: &TrackedConnection) -> Res<Vec<String>> {
    Ok(rows(
        conn,
        "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso%' ORDER BY name",
    )
    .await?
    .iter()
    .filter_map(|r| text(&r[0]))
    .collect())
}

async fn columns(conn: &TrackedConnection, table: &str) -> Res<Vec<Column>> {
    let sql = format!(
        "SELECT name, type FROM pragma_table_info('{}')",
        table.replace('\'', "''")
    );
    Ok(rows(conn, &sql)
        .await?
        .iter()
        .map(|r| {
            let decl = text(&r[1]).unwrap_or_default().to_ascii_uppercase();
            Column {
                name: text(&r[0]).unwrap_or_default(),
                numeric: decl.contains("INT") || decl.contains("REAL") || decl.contains("NUM"),
            }
        })
        .collect())
}

/// Stored-payload estimate for one row: the byte length of every text and
/// blob value (cast to blob, since `length()` of text counts characters), four
/// bytes per non-null number, and a fixed record header. Indexes and b-tree
/// slack are not in it.
fn payload_expr(cols: &[Column]) -> String {
    let mut terms = vec![(cols.len() + 4).to_string()];
    for col in cols {
        let c = quote(&col.name);
        if col.numeric {
            terms.push(format!("(CASE WHEN {c} IS NULL THEN 0 ELSE 4 END)"));
        } else {
            terms.push(format!("coalesce(length(CAST({c} AS BLOB)), 0)"));
        }
    }
    terms.join(" + ")
}

/// A table's age column, the multiplier that turns unix seconds into its unit,
/// and whether it is a creation time (growth) rather than a touch time.
async fn time_column(
    conn: &TrackedConnection,
    table: &str,
    cols: &[Column],
) -> Res<Option<(String, i64, bool)>> {
    let numeric_named =
        |name: &&str| -> Option<&Column> { cols.iter().find(|c| c.numeric && c.name == *name) };
    let candidate = CREATED_COLUMNS
        .iter()
        .find_map(numeric_named)
        .map(|c| (c, true))
        .or_else(|| {
            UPDATED_COLUMNS
                .iter()
                .find_map(numeric_named)
                .map(|c| (c, false))
        })
        .or_else(|| {
            cols.iter()
                .find(|c| c.numeric && (c.name.ends_with("_at") || c.name.ends_with("_ms")))
                .map(|c| (c, false))
        });
    let Some((col, created)) = candidate else {
        return Ok(None);
    };
    let c = quote(&col.name);
    let sample = one(
        conn,
        &format!(
            "SELECT {c} FROM {} WHERE {c} IS NOT NULL ORDER BY rowid DESC LIMIT 1",
            quote(table)
        ),
    )
    .await
    .ok()
    .map(|r| int(&r[0]))
    .unwrap_or(0);
    let scale = match sample {
        v if v >= 100_000_000_000_000 => 1_000_000,
        v if v >= 100_000_000_000 => 1_000,
        v if v >= 100_000_000 => 1,
        _ => return Ok(None),
    };
    Ok(Some((col.name.clone(), scale, created)))
}

async fn rowid_bounds(conn: &TrackedConnection, table: &str) -> Res<(i64, i64)> {
    let t = quote(table);
    let lo = one(conn, &format!("SELECT min(rowid) FROM {t}")).await?;
    let hi = one(conn, &format!("SELECT max(rowid) FROM {t}")).await?;
    Ok((int(&lo[0]), int(&hi[0])))
}

/// Rowid ranges covering `[lo, hi]`, or one unbounded range when the rowids
/// are too sparse to walk.
fn spans(lo: i64, hi: i64) -> Vec<String> {
    if hi < lo {
        return Vec::new();
    }
    if (hi - lo) / SPAN > MAX_SPANS {
        return vec!["1 = 1".to_string()];
    }
    let mut out = Vec::new();
    let mut start = lo - 1;
    while start < hi {
        let end = (start + SPAN).min(hi);
        out.push(format!("rowid > {start} AND rowid <= {end}"));
        start = end;
    }
    out
}

async fn tables(path: &str, as_of: i64) -> Res<Json> {
    let (db, open_s) = open(path).await?;
    let conn = db.connect().await.map_err(|e| e.to_string())?;
    let started = Instant::now();
    let mut out = Vec::new();
    for table in table_names(&conn).await? {
        let table_started = Instant::now();
        let cols = columns(&conn, &table).await?;
        let payload = payload_expr(&cols);
        let time = time_column(&conn, &table, &cols).await?;
        let recent = match &time {
            Some((col, scale, _)) => format!(
                "{} >= {}",
                quote(col),
                (as_of - 30 * DAY_S).saturating_mul(*scale)
            ),
            None => "0".to_string(),
        };
        let (lo, hi) = rowid_bounds(&conn, &table).await?;
        let (mut count, mut bytes, mut recent_rows, mut recent_bytes) = (0i64, 0i64, 0i64, 0i64);
        for span in spans(lo, hi) {
            let r = one(
                &conn,
                &format!(
                    "SELECT count(*), coalesce(sum({payload}), 0), \
                     coalesce(sum(CASE WHEN {recent} THEN 1 ELSE 0 END), 0), \
                     coalesce(sum(CASE WHEN {recent} THEN {payload} ELSE 0 END), 0) \
                     FROM {} WHERE {span}",
                    quote(&table)
                ),
            )
            .await?;
            count += int(&r[0]);
            bytes += int(&r[1]);
            recent_rows += int(&r[2]);
            recent_bytes += int(&r[3]);
        }
        out.push(json!({
            "table": table,
            "rows": count,
            "payload_bytes": bytes,
            "time_column": time.as_ref().map(|(c, _, _)| c.clone()),
            "time_kind": time.as_ref().map(|(_, _, created)| if *created { "created" } else { "touched" }),
            "rows_30d": time.as_ref().map(|_| recent_rows),
            "payload_bytes_30d": time.as_ref().map(|_| recent_bytes),
            "min_rowid": lo,
            "max_rowid": hi,
            "scan_s": table_started.elapsed().as_secs_f64(),
        }));
        eprintln!(
            "{table}: {count} rows, {bytes} B, {:.1}s",
            table_started.elapsed().as_secs_f64()
        );
    }
    let tokens = token_summary(&conn, as_of).await?;
    let mut pragmas = serde_json::Map::new();
    for pragma in ["page_size", "page_count", "freelist_count"] {
        let value = one(&conn, &format!("PRAGMA {pragma}"))
            .await
            .map(|r| json!(int(&r[0])))
            .unwrap_or_else(|e| json!(e));
        pragmas.insert(pragma.to_string(), value);
    }
    Ok(json!({
        "as_of": as_of,
        "open_s": open_s,
        "scan_s": started.elapsed().as_secs_f64(),
        "db_bytes": file_bytes(path),
        "log_bytes": log_bytes(path),
        "pragmas": pragmas,
        "tokens": tokens,
        "tables": out,
    }))
}

/// Token throughput from `token_rollup` (hour buckets in unix seconds): the
/// denominator for bytes per billion tokens.
async fn token_summary(conn: &TrackedConnection, as_of: i64) -> Res<Json> {
    let total = "input_tokens + cache_read_tokens + cache_create_tokens + output_tokens";
    let window = |days: i64| {
        format!(
            "SELECT coalesce(sum({total}), 0), coalesce(sum(billable_tokens), 0) \
             FROM token_rollup WHERE bucket_start >= {} AND bucket_start < {as_of}",
            as_of - days * DAY_S
        )
    };
    let last30 = one(conn, &window(30)).await?;
    let all = one(
        conn,
        &format!(
            "SELECT coalesce(sum({total}), 0), coalesce(sum(billable_tokens), 0) FROM token_rollup"
        ),
    )
    .await?;
    let daily = rows(
        conn,
        &format!(
            "SELECT bucket_start / {DAY_S}, coalesce(sum({total}), 0) FROM token_rollup \
             WHERE bucket_start >= {} GROUP BY bucket_start / {DAY_S} ORDER BY 1",
            as_of - 60 * DAY_S
        ),
    )
    .await?;
    Ok(json!({
        "tokens_30d": int(&last30[0]),
        "billable_30d": int(&last30[1]),
        "tokens_all": int(&all[0]),
        "billable_all": int(&all[1]),
        "daily": daily.iter().map(|r| json!([int(&r[0]) * DAY_S, int(&r[1])])).collect::<Vec<_>>(),
    }))
}

/// Who a run's history belongs to, which decides whether the sweep can see it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Owner {
    /// An execution whose history is final: its issue resolved, or it was
    /// superseded and finished.
    Resolved,
    /// An execution still live, under an unresolved issue.
    Active,
    /// A thread's own jobs and their children: no issue ever resolves them.
    Thread,
    /// A job with neither an execution nor a thread (chats and orphans).
    Unowned,
    /// Rows whose run or job no longer exists.
    Orphaned,
}

impl Owner {
    fn label(self) -> &'static str {
        match self {
            Owner::Resolved => "resolved_execution",
            Owner::Active => "active_execution",
            Owner::Thread => "thread",
            Owner::Unowned => "no_issue_no_thread",
            Owner::Orphaned => "orphaned",
        }
    }
}

struct Job {
    parent: Option<String>,
    execution: Option<String>,
    thread: Option<String>,
    agent: String,
}

#[derive(Default, Clone)]
struct Tally {
    events_rows: i64,
    /// Rows the archival sweep already compressed in place (`storage_mode` not `full`).
    compressed_rows: i64,
    events_bytes: i64,
    derived_bytes: i64,
    tokens: i64,
}

impl Tally {
    fn add(&mut self, other: &Tally) {
        self.events_rows += other.events_rows;
        self.compressed_rows += other.compressed_rows;
        self.events_bytes += other.events_bytes;
        self.derived_bytes += other.derived_bytes;
        self.tokens += other.tokens;
    }

    fn bytes(&self) -> i64 {
        self.events_bytes + self.derived_bytes
    }

    fn json(&self) -> Json {
        json!({
            "events_rows": self.events_rows,
            "events_bytes": self.events_bytes,
            "derived_bytes": self.derived_bytes,
            "bytes": self.bytes(),
            "tokens": self.tokens,
        })
    }
}

/// Sum `value` per `key` over a table walked by rowid range.
async fn per_key(
    conn: &TrackedConnection,
    table: &str,
    key: &str,
    value: &str,
) -> Res<HashMap<String, (i64, i64)>> {
    let (lo, hi) = rowid_bounds(conn, table).await?;
    let mut out: HashMap<String, (i64, i64)> = HashMap::new();
    for span in spans(lo, hi) {
        let sql = format!(
            "SELECT {key}, count(*), coalesce(sum({value}), 0) FROM {} WHERE {span} GROUP BY {key}",
            quote(table)
        );
        for r in rows(conn, &sql).await? {
            let Some(k) = text(&r[0]) else { continue };
            let slot = out.entry(k).or_default();
            slot.0 += int(&r[1]);
            slot.1 += int(&r[2]);
        }
    }
    Ok(out)
}

async fn payload_of(conn: &TrackedConnection, table: &str) -> Res<String> {
    Ok(payload_expr(&columns(conn, table).await?))
}

async fn history(path: &str, as_of: i64) -> Res<Json> {
    let (db, open_s) = open(path).await?;
    let conn = db.connect().await.map_err(|e| e.to_string())?;
    let started = Instant::now();

    // Small skeleton tables, loaded whole.
    let mut issues: HashMap<String, (bool, i64)> = HashMap::new();
    for r in rows(
        &conn,
        "SELECT id, status, max(coalesce(merged_at, 0), coalesce(closed_at, 0), \
         coalesce(completed_at, 0)) FROM issues",
    )
    .await?
    {
        let status = text(&r[1]).unwrap_or_default();
        issues.insert(
            text(&r[0]).unwrap_or_default(),
            (RESOLVED_ISSUE.contains(&status.as_str()), int(&r[2])),
        );
    }
    // execution id -> (resolved, resolved_at)
    let mut executions: HashMap<String, (bool, i64)> = HashMap::new();
    for r in rows(
        &conn,
        "SELECT id, issue_id, status, coalesce(lifecycle, ''), coalesce(completed_at, 0) FROM executions",
    )
    .await?
    {
        let status = text(&r[2]).unwrap_or_default();
        let finished = status == "complete" || status == "failed";
        let completed = int(&r[4]);
        let issue = text(&r[1]).and_then(|id| issues.get(&id).copied());
        let entry = match issue {
            Some((true, at)) => (true, at.max(completed)),
            _ if finished && text(&r[3]).as_deref() == Some("superseded") => (true, completed),
            _ => (false, 0),
        };
        executions.insert(text(&r[0]).unwrap_or_default(), entry);
    }
    let mut jobs: HashMap<String, Job> = HashMap::new();
    for r in rows(
        &conn,
        "SELECT id, parent_job_id, execution_id, thread_id, coalesce(agent_config_id, '') FROM jobs",
    )
    .await?
    {
        jobs.insert(
            text(&r[0]).unwrap_or_default(),
            Job {
                parent: text(&r[1]),
                execution: text(&r[2]),
                thread: text(&r[3]),
                agent: text(&r[4]).unwrap_or_default(),
            },
        );
    }
    let run_job: HashMap<String, String> = rows(&conn, "SELECT id, job_id FROM runs")
        .await?
        .iter()
        .filter_map(|r| Some((text(&r[0])?, text(&r[1])?)))
        .collect();
    let session_job: HashMap<String, String> = rows(&conn, "SELECT id, job_id FROM sessions")
        .await?
        .iter()
        .filter_map(|r| Some((text(&r[0])?, text(&r[1])?)))
        .collect();
    let stream_run: HashMap<String, String> = rows(&conn, "SELECT id, run_id FROM message_streams")
        .await?
        .iter()
        .filter_map(|r| Some((text(&r[0])?, text(&r[1])?)))
        .collect();
    eprintln!("skeleton loaded in {:.1}s", started.elapsed().as_secs_f64());

    // A job's owner: walk to the root job, which carries the thread or the
    // execution; the execution decides resolved or active.
    let classify = |job_id: &str| -> (Owner, Option<String>, String) {
        let mut id = job_id.to_string();
        let mut thread = None;
        let mut execution = None;
        for _ in 0..64 {
            let Some(job) = jobs.get(&id) else {
                return (Owner::Orphaned, None, String::new());
            };
            thread = thread.or_else(|| job.thread.clone());
            execution = execution.or_else(|| job.execution.clone());
            match &job.parent {
                Some(parent) => id = parent.clone(),
                None => break,
            }
        }
        let agent = jobs.get(&id).map(|j| j.agent.clone()).unwrap_or_default();
        if thread.is_some() {
            return (Owner::Thread, None, agent);
        }
        match execution {
            Some(exec) => match executions.get(&exec) {
                Some((true, _)) => (Owner::Resolved, Some(exec), agent),
                Some((false, _)) => (Owner::Active, Some(exec), agent),
                None => (Owner::Orphaned, None, agent),
            },
            None => (Owner::Unowned, None, agent),
        }
    };

    // Per-run history.
    let mut per_run: HashMap<String, Tally> = HashMap::new();
    let events_payload = payload_of(&conn, "events").await?;
    let by_mode = per_key(
        &conn,
        "events",
        "run_id || '|' || coalesce(storage_mode, 'full')",
        &events_payload,
    )
    .await?;
    for (key, (count, bytes)) in by_mode {
        let (run, mode) = key.rsplit_once('|').unwrap_or((key.as_str(), "full"));
        let t = per_run.entry(run.to_string()).or_default();
        t.events_rows += count;
        t.events_bytes += bytes;
        if mode != "full" {
            t.compressed_rows += count;
        }
    }
    eprintln!("events scanned at {:.1}s", started.elapsed().as_secs_f64());
    let tool_payload = payload_of(&conn, "tool_invocations").await?;
    let tools = per_key(&conn, "tool_invocations", "run_id", &tool_payload).await?;
    let chunk_payload = payload_of(&conn, "message_stream_chunks").await?;
    let chunks = per_key(&conn, "message_stream_chunks", "stream_id", &chunk_payload).await?;
    let tokens = per_key(
        &conn,
        "token_rollup",
        "run_id",
        "input_tokens + cache_read_tokens + cache_create_tokens + output_tokens",
    )
    .await?;
    let embed_payload = payload_of(&conn, "turn_embeddings").await?;
    let embeddings = per_key(&conn, "turn_embeddings", "job_id", &embed_payload).await?;
    let sky_payload = payload_of(&conn, "session_skyline_cache").await?;
    let skyline = per_key(&conn, "session_skyline_cache", "session_id", &sky_payload).await?;
    eprintln!("derived scanned at {:.1}s", started.elapsed().as_secs_f64());
    for (run, (_, bytes)) in tools {
        per_run.entry(run).or_default().derived_bytes += bytes;
    }
    for (stream, (_, bytes)) in chunks {
        let run = stream_run.get(&stream).cloned().unwrap_or_default();
        per_run.entry(run).or_default().derived_bytes += bytes;
    }
    for (run, (_, sum)) in tokens {
        per_run.entry(run).or_default().tokens += sum;
    }

    // Per-job history (embeddings, skyline) joins the job's classification directly.
    let mut per_job: HashMap<String, i64> = HashMap::new();
    for (job, (_, bytes)) in embeddings {
        *per_job.entry(job).or_default() += bytes;
    }
    for (session, (_, bytes)) in skyline {
        let job = session_job.get(&session).cloned().unwrap_or_default();
        *per_job.entry(job).or_default() += bytes;
    }

    // Per-execution pack bytes (the git range packs still inline in execution_history).
    let mut pack_bytes: HashMap<String, i64> = HashMap::new();
    for r in rows(
        &conn,
        "SELECT execution_id, coalesce(length(pack), 0) + coalesce(length(pack_idx), 0) \
         FROM execution_history",
    )
    .await?
    {
        pack_bytes.insert(text(&r[0]).unwrap_or_default(), int(&r[1]));
    }

    let mut by_owner: BTreeMap<Owner, Tally> = BTreeMap::new();
    let mut by_agent: HashMap<(Owner, String), Tally> = HashMap::new();
    let mut by_execution: HashMap<String, Tally> = HashMap::new();
    let mut attribute = |owner: Owner, execution: Option<String>, agent: String, tally: &Tally| {
        by_owner.entry(owner).or_default().add(tally);
        by_agent.entry((owner, agent)).or_default().add(tally);
        if let Some(exec) = execution {
            by_execution.entry(exec).or_default().add(tally);
        }
    };
    for (run, tally) in &per_run {
        let (owner, exec, agent) = match run_job.get(run) {
            Some(job) => classify(job),
            None => (Owner::Orphaned, None, String::new()),
        };
        attribute(owner, exec, agent, tally);
    }
    for (job, bytes) in &per_job {
        let (owner, exec, agent) = classify(job);
        let tally = Tally {
            derived_bytes: *bytes,
            ..Tally::default()
        };
        attribute(owner, exec, agent, &tally);
    }

    // Resolved executions by resolution age, and the hot set each window keeps.
    let mut ages: Vec<(i64, i64, &Tally, i64)> = Vec::new();
    for (exec, tally) in &by_execution {
        if let Some((true, at)) = executions.get(exec) {
            let age_days = if *at > 0 {
                (as_of - at) / DAY_S
            } else {
                i64::MAX
            };
            ages.push((
                age_days,
                *at,
                tally,
                pack_bytes.get(exec).copied().unwrap_or(0),
            ));
        }
    }
    let always_hot: i64 = by_owner
        .iter()
        .filter(|(o, _)| **o != Owner::Resolved)
        .map(|(_, t)| t.bytes())
        .sum();
    let windows: Vec<Json> = WINDOWS
        .iter()
        .map(|w| {
            let (n, bytes) = ages
                .iter()
                .filter(|(age, ..)| age < w)
                .fold((0, 0), |(n, b), (_, _, t, _)| (n + 1, b + t.bytes()));
            let tierable: i64 = ages
                .iter()
                .filter(|(age, ..)| age >= w)
                .map(|(_, _, t, _)| t.bytes())
                .sum();
            json!({
                "window_days": w,
                "resolved_executions_hot": n,
                "resolved_bytes_hot": bytes,
                "hot_history_bytes": bytes + always_hot,
                "tierable_bytes": tierable,
            })
        })
        .collect();
    let mut buckets: BTreeMap<i64, (i64, i64, i64)> = BTreeMap::new();
    for (age, _, tally, pack) in &ages {
        let bucket = [0, 7, 14, 30, 60, 90, 180]
            .iter()
            .rev()
            .find(|b| age >= b)
            .copied()
            .unwrap_or(0);
        let slot = buckets.entry(bucket).or_default();
        slot.0 += 1;
        slot.1 += tally.bytes();
        slot.2 += pack;
    }

    // The per-execution estimate the budget orders candidates by: bytes from
    // the event row count alone (one index count per run), checked against the
    // measured bytes of every resolved execution.
    let (sum_rows, sum_bytes, sum_xy, sum_xx, sum_tokens) = ages.iter().fold(
        (0f64, 0f64, 0f64, 0f64, 0f64),
        |(r, b, xy, xx, tk), (_, _, t, _)| {
            let x = t.events_rows as f64;
            let y = t.bytes() as f64;
            (r + x, b + y, xy + x * y, xx + x * x, tk + t.tokens as f64)
        },
    );
    let per_row_mean = if sum_rows > 0.0 {
        sum_bytes / sum_rows
    } else {
        0.0
    };
    let per_row_fit = if sum_xx > 0.0 { sum_xy / sum_xx } else { 0.0 };
    let per_token = if sum_tokens > 0.0 {
        sum_bytes / sum_tokens
    } else {
        0.0
    };
    let ape = |estimate: &dyn Fn(&Tally) -> f64| -> Json {
        let mut errors: Vec<f64> = ages
            .iter()
            .filter(|(_, _, t, _)| t.bytes() > 0)
            .map(|(_, _, t, _)| ((estimate(t) - t.bytes() as f64) / t.bytes() as f64).abs())
            .collect();
        errors.sort_by(|a, b| a.total_cmp(b));
        let pick = |q: f64| {
            errors
                .get(((errors.len() as f64 - 1.0) * q) as usize)
                .copied()
        };
        // The budget sums estimates over the oldest candidates, so the error
        // that matters is on the sum of the oldest thousand, not one execution.
        let mut oldest: Vec<&(i64, i64, &Tally, i64)> = ages.iter().collect();
        oldest.sort_by_key(|(_, at, ..)| *at);
        let (est, act) = oldest
            .iter()
            .take(1000)
            .fold((0f64, 0f64), |(e, a), (_, _, t, _)| {
                (e + estimate(t), a + t.bytes() as f64)
            });
        json!({
            "median_abs_pct_error": pick(0.5).map(|v| v * 100.0),
            "p90_abs_pct_error": pick(0.9).map(|v| v * 100.0),
            "oldest_1000_sum_pct_error": if act > 0.0 { Some((est - act) / act * 100.0) } else { None },
        })
    };
    let estimators = json!({
        "bytes_per_event_row_mean": per_row_mean,
        "bytes_per_event_row_lsq": per_row_fit,
        "bytes_per_token": per_token,
        "by_rows_mean": ape(&|t: &Tally| t.events_rows as f64 * per_row_mean),
        "by_rows_lsq": ape(&|t: &Tally| t.events_rows as f64 * per_row_fit),
        "by_tokens": ape(&|t: &Tally| t.tokens as f64 * per_token),
    });

    let mut agents: Vec<(&(Owner, String), &Tally)> = by_agent.iter().collect();
    agents.sort_by_key(|(_, t)| -t.bytes());
    Ok(json!({
        "as_of": as_of,
        "open_s": open_s,
        "scan_s": started.elapsed().as_secs_f64(),
        "by_owner": by_owner.iter().map(|(o, t)| (o.label().to_string(), t.json())).collect::<serde_json::Map<_, _>>(),
        "by_root_agent": agents.iter().take(25).map(|((o, a), t)| json!({"owner": o.label(), "agent": a, "tally": t.json()})).collect::<Vec<_>>(),
        "resolved_by_age_days": buckets.iter().map(|(b, (n, bytes, pack))| json!({"from_days": b, "executions": n, "bytes": bytes, "inline_pack_bytes": pack})).collect::<Vec<_>>(),
        "windows": windows,
        "always_hot_bytes": always_hot,
        "estimators": estimators,
        "inline_pack_bytes_total": pack_bytes.values().sum::<i64>(),
        // One row per resolved execution, for fitting estimators offline:
        // [age_days, events_rows, compressed_rows, events_bytes, derived_bytes, tokens, inline_pack_bytes]
        "resolved_executions": ages.iter().map(|(age, _, t, pack)| json!([age, t.events_rows, t.compressed_rows, t.events_bytes, t.derived_bytes, t.tokens, pack])).collect::<Vec<_>>(),
    }))
}

/// One read-only statement against the clone, rows as JSON arrays.
async fn adhoc(path: &str, sql: &str) -> Res<Json> {
    let (db, _) = open(path).await?;
    let conn = db.connect().await.map_err(|e| e.to_string())?;
    let started = Instant::now();
    let out: Vec<Json> = rows(&conn, sql)
        .await?
        .iter()
        .map(|r| {
            Json::Array(
                r.iter()
                    .map(|v| match v {
                        Value::Integer(i) => json!(i),
                        Value::Real(f) => json!(f),
                        Value::Text(t) => json!(t),
                        Value::Blob(b) => json!(format!("<blob {} B>", b.len())),
                        Value::Null => Json::Null,
                    })
                    .collect(),
            )
        })
        .collect();
    Ok(json!({"ms": started.elapsed().as_secs_f64() * 1000.0, "rows": out}))
}

async fn vacuum(path: &str, dest: &str) -> Res<Json> {
    let (db, open_s) = open(path).await?;
    let started = Instant::now();
    db.vacuum_into(Path::new(dest))
        .await
        .map_err(|e| format!("VACUUM INTO: {e}"))?;
    let vacuum_s = started.elapsed().as_secs_f64();
    drop(db);
    let image_set_bytes = db_set_size(Path::new(dest));
    let image_log_bytes = log_bytes(dest);
    let fold = fold(dest).await?;
    Ok(json!({
        "open_s": open_s,
        "vacuum_s": vacuum_s,
        "source_db_bytes": file_bytes(path),
        "image_set_bytes": image_set_bytes,
        "image_log_bytes": image_log_bytes,
        "image_fold": fold,
        "image_db_bytes_after_fold": file_bytes(dest),
    }))
}

/// Clone `path`'s three-file set to a `<stem>.probe.db` sibling, run the
/// delete probe there, and remove the sibling whatever the outcome: the probe
/// never deletes from the database it is given.
async fn delete_probe(path: &str, pages: i64, page_rows: i64) -> Res<Json> {
    let probe = format!("{}.probe.db", path.trim_end_matches(".db"));
    let sources = db_set_paths(Path::new(path));
    let copies = db_set_paths(Path::new(&probe));
    for copy in &copies {
        let _ = std::fs::remove_file(copy);
    }
    for (source, copy) in sources.iter().zip(copies.iter()) {
        if !source.exists() {
            continue;
        }
        let status = std::process::Command::new("cp")
            .arg("-c")
            .arg(source)
            .arg(copy)
            .status()
            .map_err(|e| format!("cp -c {}: {e}", source.display()))?;
        if !status.success() {
            return Err(format!("cp -c {} failed ({status})", source.display()));
        }
    }
    let result = delete_probe_on(&probe, pages, page_rows).await;
    for copy in &copies {
        let _ = std::fs::remove_file(copy);
    }
    result
}

/// Delete the oldest `events` rows a page per write transaction, folding the
/// log whenever it passes the production threshold, and report per-page cost
/// and log growth. Deletes fire every trigger and cascade the live schema
/// carries, so the cost includes the search outbox and `event_vibes`.
async fn delete_probe_on(path: &str, pages: i64, page_rows: i64) -> Res<Json> {
    let (db, open_s) = open(path).await?;
    let outbox_before = {
        let conn = db.connect().await.map_err(|e| e.to_string())?;
        int(&one(&conn, "SELECT max(rowid) FROM search_outbox").await?[0])
    };
    let plan = {
        let conn = db.connect().await.map_err(|e| e.to_string())?;
        rows(
            &conn,
            "EXPLAIN QUERY PLAN DELETE FROM events WHERE rowid IN (1, 2, 3)",
        )
        .await?
        .iter()
        .map(|r| r.iter().filter_map(text).collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
    };
    let mut cursor = 0i64;
    let mut read_ms = Vec::new();
    let mut write_ms = Vec::new();
    let mut folds = Vec::new();
    let mut deleted = 0i64;
    // A fresh clone inherits the live log. Fold it before the first page, or
    // every page past the threshold retries a fold that cannot finish and the
    // probe measures checkpoints rather than deletes.
    let initial_fold = if log_bytes(path) >= FOLD_BYTES {
        let report = db
            .checkpoint(3, Duration::from_millis(250), DrainBudget::PRODUCTION)
            .await
            .map_err(|e| e.to_string())?;
        if report.log_bytes_after >= FOLD_BYTES {
            return Err(format!(
                "could not fold the probe clone's {} B log before probing: {:?}",
                report.log_bytes_before, report.error
            ));
        }
        Some(json!({
            "log_before": report.log_bytes_before,
            "ms": report.duration.as_secs_f64() * 1000.0,
        }))
    } else {
        None
    };
    let mut log_peak = log_bytes(path);
    let mut log_at_last_fold = log_bytes(path);
    let mut rows_since_fold = 0i64;
    let mut log_per_row = Vec::new();
    let started = Instant::now();
    for _ in 0..pages {
        let t = Instant::now();
        let ids: Vec<i64> = {
            let conn = db.connect().await.map_err(|e| e.to_string())?;
            rows(
                &conn,
                &format!("SELECT rowid FROM events WHERE rowid > {cursor} ORDER BY rowid LIMIT {page_rows}"),
            )
            .await?
            .iter()
            .map(|r| int(&r[0]))
            .collect()
        };
        read_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        let Some(last) = ids.last().copied() else {
            break;
        };
        cursor = last;
        let sql = format!(
            "DELETE FROM events WHERE rowid IN ({})",
            ids.iter().map(i64::to_string).collect::<Vec<_>>().join(",")
        );
        let t = Instant::now();
        let n = db
            .write(|conn| {
                let sql = sql.clone();
                Box::pin(async move { Ok(conn.execute(sql.as_str(), ()).await?) })
            })
            .await
            .map_err(|e| e.to_string())?;
        write_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        deleted += n as i64;
        rows_since_fold += n as i64;
        let log = log_bytes(path);
        log_peak = log_peak.max(log);
        if log >= FOLD_BYTES {
            if rows_since_fold > 0 {
                log_per_row.push((log - log_at_last_fold.min(log)) as f64 / rows_since_fold as f64);
            }
            let report = db
                .checkpoint(3, Duration::from_millis(250), DrainBudget::PRODUCTION)
                .await
                .map_err(|e| e.to_string())?;
            folds.push(json!({
                "after_rows": deleted,
                "log_before": report.log_bytes_before,
                "log_after": report.log_bytes_after,
                "ms": report.duration.as_secs_f64() * 1000.0,
                "error": report.error,
            }));
            log_at_last_fold = report.log_bytes_after;
            rows_since_fold = 0;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let outbox_after = {
        let conn = db.connect().await.map_err(|e| e.to_string())?;
        int(&one(&conn, "SELECT max(rowid) FROM search_outbox").await?[0])
    };
    let stats = |v: &mut Vec<f64>| -> Json {
        v.sort_by(|a, b| a.total_cmp(b));
        let pick = |q: f64| v.get(((v.len() as f64 - 1.0) * q) as usize).copied();
        json!({"p50": pick(0.5), "p90": pick(0.9), "p99": pick(0.99), "max": v.last().copied(), "n": v.len()})
    };
    let mut fold_ms: Vec<f64> = folds.iter().filter_map(|f| f["ms"].as_f64()).collect();
    Ok(json!({
        "open_s": open_s,
        "initial_fold": initial_fold,
        "page_rows": page_rows,
        "pages": write_ms.len(),
        "deleted_rows": deleted,
        "elapsed_s": elapsed,
        "rows_per_s": deleted as f64 / elapsed.max(1e-9),
        "read_ms": stats(&mut read_ms),
        "write_ms": stats(&mut write_ms),
        "fold_ms": stats(&mut fold_ms),
        "folds": folds.len(),
        "log_peak_bytes": log_peak,
        "log_bytes_per_deleted_row": stats(&mut log_per_row),
        "search_outbox_rows_added": outbox_after - outbox_before,
        "delete_plan": plan,
    }))
}
