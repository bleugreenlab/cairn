//! Replay of the production token forecast on resolved issues: the refit at
//! `--as-of` (default cairn/6271's cut, 2026-10-19), scored on issues created
//! from then to `--to` (default 2026-11-02), the way 6271's test was scored.
//!
//! Point it at a COPY of a workspace database, never the live one: it applies
//! pending migrations to what it opens, and refuses any path inside a live
//! Cairn data directory. It writes nothing but its JSON report to stdout, and
//! the report is aggregate-only.
//!
//! Labels come from the copy's own `token_forecast_labels` (`--labels db`) or
//! from a JSONL file of decide answers to the same questions on the same model
//! (`--labels-jsonl <path>`), one line per issue with `item_id`
//! (`project/number`), `effort`, `scope` and `filing_group`, as
//! `bench/token-forecast/data/decide-scores.jsonl` holds them.
//!
//! Usage:
//!   cargo run --release -p cairn-core --features internal-api \
//!     --example token_forecast_replay -- <copy>/cairn.db \
//!     --labels-jsonl bench/token-forecast/data/decide-scores.jsonl \
//!     [--as-of EPOCH] [--to EPOCH] [--snapshot-ts EPOCH]

use std::collections::HashMap;
use std::path::Path;

use cairn_core::judgment::token_forecast::fit::{
    self, FitLabel, RefitVerdict, BENCH_CUT, BENCH_TEST_TO, FLOOR,
};
use cairn_core::judgment::token_forecast::{population, question, store, ANSWERER_VERSION};
use cairn_db::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
use serde_json::{json, Value};

const LIVE_DIRS: &[&str] = &[
    ".cairn",
    ".cairn-dev",
    "Library/Application Support/com.cairn.desktop",
];

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn refuse_live(path: &Path) -> Result<(), String> {
    let canonical = std::fs::canonicalize(path).map_err(|e| format!("{}: {e}", path.display()))?;
    if let Some(home) = dirs::home_dir() {
        for live in LIVE_DIRS {
            if canonical.starts_with(home.join(live)) {
                return Err(format!(
                    "{} is inside a live Cairn data directory; replay a copy",
                    canonical.display()
                ));
            }
        }
    }
    Ok(())
}

/// Labels from a decide-scores JSONL file, keyed by issue id through the
/// population's `project/number`.
fn jsonl_labels(
    path: &str,
    by_item: &HashMap<String, String>,
) -> Result<(HashMap<String, FitLabel>, u64), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let mut labels = HashMap::new();
    let mut unmatched = 0;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let row: Value = serde_json::from_str(line).map_err(|e| format!("{path}: {e}"))?;
        let (Some(item), Some(effort), Some(scope)) = (
            row["item_id"].as_str(),
            row["effort"].as_f64(),
            row["scope"].as_f64(),
        ) else {
            continue;
        };
        let Some(issue_id) = by_item.get(&item.to_lowercase()) else {
            unmatched += 1;
            continue;
        };
        labels.insert(
            issue_id.clone(),
            FitLabel {
                score: (effort + scope) / 2.0,
                filing_maker: row["filing_group"].as_str().map(str::to_string),
                text_source: row["text_source"].as_str().map(str::to_string),
            },
        );
    }
    Ok((labels, unmatched))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), String> {
    cairn_build_id::install();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let db_path = args
        .first()
        .filter(|a| !a.starts_with("--"))
        .ok_or("usage: token_forecast_replay <copy.db> --labels db|--labels-jsonl <path>")?;
    refuse_live(Path::new(db_path))?;
    let parse = |name: &str, default: i64| -> Result<i64, String> {
        arg(&args, name).map_or(Ok(default), |v| {
            v.parse().map_err(|e| format!("{name}: {e}"))
        })
    };
    let as_of = parse("--as-of", BENCH_CUT)?;
    let to = parse("--to", BENCH_TEST_TO)?;
    let snapshot = parse("--snapshot-ts", chrono::Utc::now().timestamp())?;

    let db = LocalDb::open(db_path)
        .await
        .map_err(|e| format!("open {db_path}: {e}"))?;
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .map_err(|e| format!("migrating {db_path}: {e}"))?;
    let issues = population::load(&db, FLOOR)
        .await
        .map_err(|e| format!("population: {e}"))?;
    let by_item: HashMap<String, String> = issues
        .iter()
        .map(|i| (i.item_id().to_lowercase(), i.issue_id.clone()))
        .collect();
    let (labels, source, unmatched) = match (arg(&args, "--labels-jsonl"), arg(&args, "--labels")) {
        (Some(path), _) => {
            let (labels, unmatched) = jsonl_labels(&path, &by_item)?;
            (labels, format!("jsonl:{path}"), unmatched)
        }
        (None, Some(db_labels)) if db_labels == "db" => (
            store::fit_labels(&db, ANSWERER_VERSION, question::question_version())
                .await
                .map_err(|e| format!("labels: {e}"))?,
            "db".to_string(),
            0,
        ),
        _ => return Err("give --labels db or --labels-jsonl <path>".into()),
    };

    let refit = fit::refit(
        &issues,
        &labels,
        as_of,
        snapshot,
        ANSWERER_VERSION,
        question::question_version(),
    );
    let calibration = match &refit.verdict {
        RefitVerdict::Accepted(calibration) => calibration,
        RefitVerdict::Rejected(rejected) => {
            println!(
                "{}",
                json!({"refit": "rejected", "reason": rejected.reason, "exclusions": refit.exclusions})
            );
            return Ok(());
        }
    };
    let report = fit::replay(calibration, &issues, &labels, as_of, to, snapshot);
    let doc = json!({
        "labels": {"source": source, "matched": labels.len(), "unmatched": unmatched},
        "population": issues.len(),
        "as_of": as_of,
        "to": to,
        "snapshot": snapshot,
        "question_version": question::question_version(),
        "calibration": {
            "version": calibration.version,
            "fit_n": calibration.fit_n,
            "shift_n": calibration.shift_n,
            "shifts": calibration.shifts,
            "large_actuals_coverage": calibration.large_actuals_coverage,
            "edges": calibration.edges,
            "bins": calibration.bins,
            "exclusions": refit.exclusions,
        },
        "replay": report,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&doc).map_err(|e| e.to_string())?
    );
    Ok(())
}
