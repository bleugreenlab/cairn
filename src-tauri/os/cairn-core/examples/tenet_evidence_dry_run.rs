//! Calibration dry run for tenet evidence: how near do concluded issues sit to
//! a project's approved tenets, and what would the judge be asked?
//!
//! Point it at a COPY of a workspace database (the `.db`, `-wal`, and `-log`
//! files together), never the live one: it applies pending migrations to what
//! it opens. It writes nothing else. For the last N merged issues of a project
//! it builds the same event text the resolution hook snapshots, embeds the
//! approved tenets and the events through the `/embed` gateway, prints each
//! event's five nearest tenets, and summarizes the distribution of best
//! similarities against `KNN_FLOOR`.
//!
//! With `JUDGE_ARGS_OUT=<path>` it also writes one JSON line per event that
//! clears the floor: the issue URI, the candidate tenet numbers, and the exact
//! decision the judge would ask (`input`, the event text, and `decision`, one
//! choice question per candidate), so the judgement can be run through a
//! decision-capable provider separately.
//!
//! A database whose app has not yet booted with tenets holds none: its
//! standing decisions are still rulings in each thread's store. With
//! `IMPORT_RULINGS=1` this first runs the boot-time ruling import against the
//! copy. The import reads thread stores under `CAIRN_HOME/state/threads`, so
//! point `CAIRN_HOME` at a directory holding COPIES of those stores; this
//! refuses to import when `CAIRN_HOME` is unset or is the real Cairn home.
//!
//! The `/embed` gateway accepts an anonymous device token, which this
//! registers the way a fresh install does. Set `CAIRN_DEVICE_JWT` to use an
//! existing token instead.
//!
//! Usage:
//!   [JUDGE_ARGS_OUT=/tmp/judge.jsonl] [IMPORT_RULINGS=1 CAIRN_HOME=/copy] \
//!     cargo run --example tenet_evidence_dry_run --features internal-api \
//!       -- <DB_PATH> [PROJECT_KEY=cairn] [N=20]

use std::io::Write;

use cairn_core::internal::embeddings::{gateway_embedder, Corpus, Embed, Purpose};
use cairn_core::internal::storage::{LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
use cairn_core::tenets::evidence::{events, judge, vectors, EMBED_CHUNK, KNN_FLOOR, KNN_K};
use cairn_core::tenets::{TenetFilter, TenetStatus, TenetView};
use cairn_core::transitions::Resolution;

struct QuietEmitter;

impl cairn_core::internal::services::EventEmitter for QuietEmitter {
    fn emit(&self, _event: &str, _payload: serde_json::Value) -> Result<(), String> {
        Ok(())
    }
    fn emit_empty(&self, _event: &str) -> Result<(), String> {
        Ok(())
    }
}

/// Turn the copy's thread rulings into migrated tenets, as the app's boot does.
async fn import_rulings(db: &LocalDb) {
    let home = std::env::var_os("CAIRN_HOME")
        .filter(|home| !home.is_empty())
        .map(std::path::PathBuf::from)
        .expect("IMPORT_RULINGS needs CAIRN_HOME pointed at a copy of the thread stores");
    let real = dirs::home_dir().map(|home| home.join(".cairn"));
    let canonical = |path: &std::path::Path| std::fs::canonicalize(path).ok();
    if real.as_deref().and_then(canonical) == canonical(&home) {
        panic!("CAIRN_HOME is the real Cairn home; point it at a copy");
    }
    let mut report = cairn_core::tenets::ImportReport::default();
    cairn_core::tenets::import_database(db, &QuietEmitter, 0, &mut report).await;
    eprintln!(
        "ruling import into the copy: {} tenet(s) inserted, {} error(s)",
        report.inserted(),
        report.errors.len()
    );
    for error in report.errors.iter().take(5) {
        eprintln!("  {error}");
    }
}

async fn register_anonymous_device() -> String {
    let base =
        std::env::var("CAIRN_API_URL").unwrap_or_else(|_| "https://api.cairn.computer".to_string());
    let body: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/tokens/device/anonymous"))
        .json(&serde_json::json!({ "device_id": uuid::Uuid::new_v4().to_string() }))
        .send()
        .await
        .expect("anonymous device registration request")
        .json()
        .await
        .expect("anonymous device registration response");
    body["token"]
        .as_str()
        .expect("anonymous device response carries a token")
        .to_string()
}

async fn embed_all(
    embedder: &dyn Embed,
    jwt: &str,
    texts: Vec<String>,
    purpose: Purpose,
) -> Vec<Vec<f32>> {
    let mut out = Vec::with_capacity(texts.len());
    for chunk in texts.chunks(EMBED_CHUNK) {
        // A transient gateway failure is retried inside the facet, as it is
        // for the worker.
        match embedder
            .embed(Corpus::Resources, purpose, chunk.to_vec())
            .await
        {
            Ok(embedded) => out.extend(embedded.vectors),
            Err(failure) => {
                eprintln!("chunk of {} failed ({failure}); bisecting", chunk.len());
                bisect(jwt, chunk, purpose).await;
                panic!("embedding failed");
            }
        }
    }
    out
}

/// Narrow a failing chunk to the texts the gateway refuses on their own.
///
/// It probes with a fresh embedder each time: the shared one's gate closed on
/// the failure that started the bisection.
async fn bisect(jwt: &str, texts: &[String], purpose: Purpose) {
    let probe = gateway_embedder(jwt.to_string());
    let total: usize = texts.iter().map(String::len).sum();
    if probe
        .embed(Corpus::Resources, purpose, texts.to_vec())
        .await
        .is_ok()
    {
        eprintln!("  {} text(s), {total} bytes: ok", texts.len());
        return;
    }
    if texts.len() == 1 {
        let preview: String = texts[0].chars().take(160).collect();
        eprintln!("  refused alone: {total} bytes: {preview:?}");
        return;
    }
    eprintln!("  {} text(s), {total} bytes: refused", texts.len());
    let (left, right) = texts.split_at(texts.len() / 2);
    Box::pin(bisect(jwt, left, purpose)).await;
    Box::pin(bisect(jwt, right, purpose)).await;
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm = |v: &[f32]| v.iter().map(|x| x * x).sum::<f32>().sqrt();
    let denominator = norm(a) * norm(b);
    if denominator == 0.0 {
        0.0
    } else {
        dot / denominator
    }
}

fn quantile(sorted: &[f32], q: f64) -> f32 {
    if sorted.is_empty() {
        return f32::NAN;
    }
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(db_path) = args.first() else {
        eprintln!("usage: tenet_evidence_dry_run <DB_PATH> [PROJECT_KEY] [N]");
        std::process::exit(1);
    };
    let key = args.get(1).cloned().unwrap_or_else(|| "cairn".to_string());
    let limit: i64 = args.get(2).and_then(|n| n.parse().ok()).unwrap_or(20);

    let db = LocalDb::open(std::path::PathBuf::from(db_path))
        .await
        .expect("open database");
    let applied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .expect("apply migrations");
    eprintln!("applied {} migration(s) to the copy", applied.len());
    if std::env::var("IMPORT_RULINGS").is_ok_and(|value| value == "1") {
        import_rulings(&db).await;
    }

    let project_id = db
        .query_opt(
            "SELECT id FROM projects WHERE lower(key) = lower(?1)",
            (key.clone(),),
            |row| row.text(0),
        )
        .await
        .expect("read project")
        .unwrap_or_else(|| panic!("no project with key {key}"));
    let approved: Vec<TenetView> = cairn_core::tenets::list(
        &db,
        &project_id,
        &TenetFilter {
            statuses: vec![TenetStatus::Approved],
            ..Default::default()
        },
    )
    .await
    .expect("list tenets");
    println!("{} approved tenet(s) in {key}", approved.len());
    if approved.is_empty() {
        println!("nothing to judge against; every event would be skipped without a network call");
        return;
    }

    let issues: Vec<String> = db
        .query_all(
            "SELECT id FROM issues WHERE project_id = ?1 AND status = 'merged'
             ORDER BY COALESCE(merged_at, completed_at, updated_at) DESC LIMIT ?2",
            (project_id.clone(), limit),
            |row| row.text(0),
        )
        .await
        .expect("read merged issues");
    let mut snapshots = Vec::new();
    for issue in &issues {
        let issue = issue.clone();
        let event = db
            .read(move |conn| {
                let issue = issue.clone();
                Box::pin(async move {
                    events::issue_event_conn(conn, &issue, Resolution::Merged, "dry-run", 0).await
                })
            })
            .await
            .expect("build event");
        if let Some(event) = event {
            snapshots.push(event);
        }
    }
    println!("{} merged issue event(s)\n", snapshots.len());

    let jwt = match std::env::var("CAIRN_DEVICE_JWT")
        .ok()
        .filter(|jwt| !jwt.is_empty())
    {
        Some(jwt) => jwt,
        None => register_anonymous_device().await,
    };
    let embedder = gateway_embedder(jwt.clone());

    let tenet_vectors = embed_all(
        embedder.as_ref(),
        &jwt,
        approved.iter().map(vectors::tenet_embed_text).collect(),
        Purpose::Document,
    )
    .await;
    let event_vectors = embed_all(
        embedder.as_ref(),
        &jwt,
        snapshots.iter().map(|event| event.text.clone()).collect(),
        Purpose::Query,
    )
    .await;

    let mut out = std::env::var("JUDGE_ARGS_OUT")
        .ok()
        .map(|path| std::fs::File::create(path).expect("create JUDGE_ARGS_OUT"));
    let mut best = Vec::new();
    let mut all = Vec::new();
    let mut judged_events = 0;
    let mut judged_pairs = 0;
    for (event, query) in snapshots.iter().zip(&event_vectors) {
        let mut scored: Vec<(f32, &TenetView)> = approved
            .iter()
            .zip(&tenet_vectors)
            .map(|(view, vector)| (cosine(vector, query), view))
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0));
        all.extend(scored.iter().map(|(score, _)| *score));
        best.push(scored.first().map(|(score, _)| *score).unwrap_or(0.0));
        let title = event.text.lines().next().unwrap_or_default();
        println!("{title}");
        for (score, view) in scored.iter().take(KNN_K) {
            let mark = if *score >= KNN_FLOOR { "*" } else { " " };
            let text: String = view.row.statement.chars().take(90).collect();
            println!("  {mark} {score:.3}  #{} {text}", view.row.number);
        }
        let candidates: Vec<&TenetView> = scored
            .iter()
            .filter(|(score, _)| *score >= KNN_FLOOR)
            .take(KNN_K)
            .map(|(_, view)| *view)
            .collect();
        if !candidates.is_empty() {
            judged_events += 1;
            judged_pairs += candidates.len();
            if let Some(file) = out.as_mut() {
                let line = serde_json::json!({
                    "source": event.source_uri,
                    "candidates": candidates.iter().map(|view| view.row.number).collect::<Vec<_>>(),
                    "input": event.text,
                    "decision": judge::decision(&candidates),
                });
                writeln!(file, "{line}").expect("write judge args");
            }
        }
        println!();
    }

    best.sort_by(f32::total_cmp);
    all.sort_by(f32::total_cmp);
    println!("best similarity per event (n={}):", best.len());
    println!(
        "  min {:.3} · p25 {:.3} · median {:.3} · p75 {:.3} · max {:.3}",
        quantile(&best, 0.0),
        quantile(&best, 0.25),
        quantile(&best, 0.5),
        quantile(&best, 0.75),
        quantile(&best, 1.0)
    );
    println!("every event-tenet pair (n={}):", all.len());
    println!(
        "  median {:.3} · p90 {:.3} · p99 {:.3}",
        quantile(&all, 0.5),
        quantile(&all, 0.9),
        quantile(&all, 0.99)
    );
    println!(
        "KNN_FLOOR {KNN_FLOOR}: {judged_events} of {} event(s) would reach the judge, with {judged_pairs} tenet(s) in total",
        best.len()
    );
}
