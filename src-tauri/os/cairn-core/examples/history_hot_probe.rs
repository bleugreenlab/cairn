//! Paired hot-reader probe, only for disposable local database/archive copies.
//! Build with --features test-utils. The driver owns snapshot/copy/cleanup;
//! this binary requires its marker and never opens a live data directory.
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Instant;

use async_trait::async_trait;
use cairn_core::internal::mcp::handlers::read::register_archived_file_renderer;
use cairn_db::storage::content_store::{ContentStore, ContentStoreError, FileContentStore};
use cairn_db::storage::{LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
use cairn_session::runs::queries::{
    list_job_transcript_window, load_event_by_id, TranscriptPosition, TranscriptWindow,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

type Res<T> = Result<T, String>;
const MARKER: &str = "HISTORY-HOT-PROBE";
const MARKER_TEXT: &str = "disposable history reader measurement\n";

#[derive(Clone, Serialize, Deserialize)]
struct Case {
    job_id: String,
    before: TranscriptPosition,
    after_rowid: i64,
    run_id: String,
    event_id: String,
    tail_rows: usize,
    limit: usize,
}

struct CheckedStore {
    inner: FileContentStore,
    failures: AtomicUsize,
}

#[async_trait]
impl ContentStore for CheckedStore {
    fn local_root(&self) -> Option<&Path> {
        Some(self.inner.root())
    }
    async fn put(&self, _: &str, _: &[u8]) -> Result<(), ContentStoreError> {
        Err(ContentStoreError::Io(
            "the reader probe forbids archive writes".into(),
        ))
    }
    async fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, ContentStoreError> {
        let result = self.inner.get(hash).await;
        if !matches!(result, Ok(Some(_))) {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

fn guarded_root(db: &Path, archive: &Path) -> Res<()> {
    let db = db.canonicalize().map_err(|e| e.to_string())?;
    let archive = archive.canonicalize().map_err(|e| e.to_string())?;
    if db.extension().and_then(|s| s.to_str()) != Some("db") {
        return Err("the disposable database must end in .db".into());
    }
    let parent = db.parent().ok_or("database has no parent")?;
    let root = parent
        .ancestors()
        .find(|dir| std::fs::read_to_string(dir.join(MARKER)).ok().as_deref() == Some(MARKER_TEXT))
        .ok_or("no disposable measurement marker above database")?;
    if !archive.starts_with(root) {
        return Err("archive must be a copy below the same disposable root".into());
    }
    if let Some(home) = std::env::var_os("HOME") {
        for live in [
            ".cairn",
            ".cairn-dev",
            "Library/Application Support/com.cairn.desktop",
        ] {
            let live = PathBuf::from(&home).join(live);
            if let Ok(live) = live.canonicalize() {
                if db.starts_with(&live) || archive.starts_with(&live) {
                    return Err(format!("refusing live data directory {}", live.display()));
                }
            }
        }
    }
    Ok(())
}

async fn open(db: &Path, archive: &Path) -> Res<(Arc<LocalDb>, Arc<CheckedStore>, f64)> {
    guarded_root(db, archive)?;
    let started = Instant::now();
    let db = Arc::new(LocalDb::open(db).await.map_err(|e| e.to_string())?);
    // open_existing clears orphan temporary files, hence it is used ONLY on
    // the disposable archive copy, never the origin's only archive.
    let inner = FileContentStore::open_existing(archive);
    inner.status().map_err(|e| e.to_string())?;
    let store = Arc::new(CheckedStore {
        inner,
        failures: AtomicUsize::new(0),
    });
    db.set_private_content_store(store.clone())
        .map_err(|_| "store already installed")?;
    Ok((db, store, started.elapsed().as_secs_f64() * 1000.0))
}

async fn case_for(db: &LocalDb, job_id: String, limit: usize) -> Res<Case> {
    let tail = list_job_transcript_window(db, &job_id, TranscriptWindow::Tail { limit })
        .await
        .map_err(|e| e.to_string())?;
    let first = tail.events.first().ok_or("job has no tail events")?;
    let middle = &tail.events[tail.events.len() / 2];
    let before = tail.older.unwrap_or(TranscriptPosition {
        run_seq: *tail
            .run_ordinals
            .get(&middle.run_id)
            .ok_or("missing run ordinal")?,
        sequence: i64::from(middle.sequence),
    });
    let after_rowid = db
        .query_one(
            "SELECT rowid - 1 FROM events WHERE id = ?1",
            (first.id.clone(),),
            |row| row.i64(0),
        )
        .await
        .map_err(|e| e.to_string())?;
    let detail = tail
        .events
        .iter()
        .rev()
        .find(|e| e.event_type == "tool_result")
        .or_else(|| tail.events.last())
        .ok_or("missing detail event")?;
    Ok(Case {
        job_id,
        before,
        after_rowid,
        run_id: detail.run_id.clone(),
        event_id: detail.id.clone(),
        tail_rows: tail.events.len(),
        limit,
    })
}

async fn prepare(db_path: &Path, archive: &Path, prefixes: &[String]) -> Res<Value> {
    let (db, store, _) = open(db_path, archive).await?;
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .map_err(|e| e.to_string())?;
    let mut cases = Vec::new();
    for prefix in prefixes {
        let found = db
            .query_all(
                "SELECT id FROM jobs WHERE id LIKE ?1 LIMIT 2",
                (format!("{prefix}%"),),
                |row| row.text(0),
            )
            .await
            .map_err(|e| e.to_string())?;
        if found.len() != 1 {
            return Err(format!("job prefix {prefix} is missing or ambiguous"));
        }
        cases.push(case_for(&db, found[0].clone(), 500).await?);
    }
    if cases.len() == 1 {
        let candidates = db
            .query_all(
                "SELECT id FROM jobs WHERE id != ?1 ORDER BY created_at DESC LIMIT 80",
                (cases[0].job_id.clone(),),
                |row| row.text(0),
            )
            .await
            .map_err(|e| e.to_string())?;
        for job in candidates {
            if let Ok(case) = case_for(&db, job, 500).await {
                if (10..500).contains(&case.tail_rows) {
                    cases.push(case);
                    break;
                }
            }
        }
    }
    if cases.len() != 2 {
        return Err("provide longest-lineage and ordinary job prefixes".into());
    }
    if store.failures.load(Ordering::Relaxed) != 0 {
        return Err(format!(
            "archive offline: {} (a required object failed during preparation)",
            archive.display()
        ));
    }
    Ok(json!({"cases": cases, "prepared_read_token_caches": true}))
}

async fn call(db: Arc<LocalDb>, case: &Case, mode: &str) -> Res<Value> {
    match mode {
        "tail" => serde_json::to_value(
            list_job_transcript_window(
                &db,
                &case.job_id,
                TranscriptWindow::Tail { limit: case.limit },
            )
            .await
            .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string()),
        "before" => serde_json::to_value(
            list_job_transcript_window(
                &db,
                &case.job_id,
                TranscriptWindow::Before {
                    position: case.before,
                    limit: case.limit,
                },
            )
            .await
            .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string()),
        "after" => serde_json::to_value(
            list_job_transcript_window(
                &db,
                &case.job_id,
                TranscriptWindow::After {
                    rowid: case.after_rowid,
                },
            )
            .await
            .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string()),
        "detail" => serde_json::to_value(
            load_event_by_id(db, &case.run_id, &case.event_id).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string()),
        _ => Err("unknown mode".into()),
    }
}

fn canonical(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(k, v)| (k, canonical(v)))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.into_iter().map(canonical).collect()),
        other => other,
    }
}

async fn measure(
    db_path: &Path,
    archive: &Path,
    cases: &[Case],
    label: &str,
    warmth: &str,
    samples: usize,
) -> Res<Value> {
    if !matches!(warmth, "cold" | "warm") || !(1..=100).contains(&samples) {
        return Err("warmth must be cold/warm and samples 1..100".into());
    }
    let mut measurements = Vec::new();
    for case in cases {
        for mode in ["tail", "before", "after", "detail"] {
            let mut shared = None;
            if warmth == "warm" {
                let (db, store, open_ms) = open(db_path, archive).await?;
                let _ = call(db.clone(), case, mode).await?;
                if store.failures.load(Ordering::Relaxed) != 0 {
                    return Err(format!(
                        "archive offline: {} (warm-up fetch failed)",
                        archive.display()
                    ));
                }
                shared = Some((db, store, open_ms));
            }
            for sample in 0..samples {
                let (db, store, open_ms) = match &shared {
                    Some((db, store, ms)) => (db.clone(), store.clone(), *ms),
                    None => open(db_path, archive).await?,
                };
                let before = (
                    db.read_statement_count(),
                    db.read_checkout_count(),
                    db.read_transaction_count(),
                    db.write_transaction_count(),
                );
                let started = Instant::now();
                let result = call(db.clone(), case, mode).await;
                let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                let counts = (
                    db.read_statement_count() - before.0,
                    db.read_checkout_count() - before.1,
                    db.read_transaction_count() - before.2,
                    db.write_transaction_count() - before.3,
                );
                let (digest, rows, mut error) = match result {
                    Ok(value) => {
                        let rows = value
                            .get("events")
                            .and_then(Value::as_array)
                            .map(Vec::len)
                            .unwrap_or(usize::from(!value.is_null()));
                        let bytes =
                            serde_json::to_vec(&canonical(value)).map_err(|e| e.to_string())?;
                        (Some(format!("{:x}", Sha256::digest(bytes))), rows, None)
                    }
                    Err(error) => (None, 0, Some(error)),
                };
                if store.failures.load(Ordering::Relaxed) != 0 {
                    error = Some(format!(
                        "archive offline: {} (required object fetch failed; no write retry)",
                        archive.display()
                    ));
                }
                measurements.push(json!({
                    "label": label, "job_id": case.job_id, "mode": mode, "warmth": warmth,
                    "sample": sample, "elapsed_ms": elapsed_ms, "open_ms": open_ms,
                    "read_statements": counts.0, "read_checkouts": counts.1,
                    "read_transactions": counts.2, "write_transactions": counts.3,
                    "rows": rows, "digest": digest, "error": error,
                }));
            }
        }
    }
    Ok(
        json!({"measurements": measurements, "os_cache": "uncontrolled", "cold": "fresh LocalDb/engine", "warm": "same engine/pool after one warm-up per operation"}),
    )
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    cairn_build_id::install();
    register_archived_file_renderer();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.as_slice() {
        [command, db, archive, rest @ ..] if command == "prepare" && !rest.is_empty() =>
            prepare(Path::new(db), Path::new(archive), rest).await,
        [command, db, archive, case_file, label, warmth, samples] if command == "measure" => {
            let parsed = std::fs::read(case_file).map_err(|e| e.to_string())
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).map_err(|e| e.to_string()))
                .and_then(|doc| serde_json::from_value::<Vec<Case>>(doc["cases"].clone()).map_err(|e| e.to_string()));
            match (parsed, samples.parse::<usize>()) {
                (Ok(cases), Ok(samples)) => measure(Path::new(db), Path::new(archive), &cases, label, warmth, samples).await,
                _ => Err("invalid cases or sample count".into()),
            }
        }
        _ => Err("usage: history_hot_probe prepare <db> <archive-copy> <long-job-prefix> [ordinary-prefix] | measure <db> <archive-copy> <cases.json> <label> <cold|warm> <samples>".into()),
    };
    match result {
        Ok(value) => println!("{}", value),
        Err(error) => {
            eprintln!("history_hot_probe: {error}");
            std::process::exit(1);
        }
    }
}
