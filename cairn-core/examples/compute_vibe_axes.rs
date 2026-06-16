//! Offline regenerator for `vibe_axes.json` — the 2-axis (PHASE / FRICTION)
//! behavioral basis for vibe coloring.
//!
//! Reads a corpus-dump JSONL produced by `examples/vibe_corpus_dump.rs` (each row
//! carries the embeddable `text`, `run_progress`, and `dist_prev_error`), embeds
//! the assistant events through the `/embed` gateway, partitions them by the
//! validated behavioral labels, computes unit mean-difference contrast vectors,
//! and builds the 101-point empirical percentile table per axis.
//!
//! Labels (match `vibe-research/probe.py`):
//!   PHASE    pos: run_progress > 0.85 (ship)        neg: run_progress < 0.15 (explore)
//!   FRICTION pos: dist_prev_error <= 2 (post-error)  neg: dist_prev_error >= 15 (smooth)
//!
//! This is a sanity regenerator, not a byte-exact reproduction of the bundled
//! bootstrap (different corpus sample); the recomputed axes should cosine-match
//! the shipped ones.
//!
//! Usage:
//!   CAIRN_DEVICE_JWT=<device-jwt> [CAIRN_API_URL=http://localhost:3849] \
//!     cargo run --example compute_vibe_axes --features internal-api -- \
//!       <dump.jsonl> [OUT_JSON] [--max N]

use std::sync::Arc;

use cairn_core::internal::api::ApiConfig;
use cairn_core::internal::embeddings::vibes::average_vectors;
use cairn_core::internal::embeddings::{
    EmbeddingClient, InputType, TokenProvider, COHERE_DIMS, COHERE_MODEL,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct DumpRow {
    event_type: String,
    text: Option<String>,
    run_progress: Option<f64>,
    dist_prev_error: Option<i64>,
}

struct Labeled {
    vector: Vec<f32>,
    run_progress: f64,
    dist_prev_error: i64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dump_path = args
        .get(1)
        .filter(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| {
            eprintln!("usage: compute_vibe_axes <dump.jsonl> [OUT_JSON] [--max N]");
            std::process::exit(1);
        });
    let out_path = args
        .get(2)
        .filter(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| {
            concat!(env!("CARGO_MANIFEST_DIR"), "/src/embeddings/vibe_axes.json").to_string()
        });
    let max: Option<usize> = args
        .iter()
        .position(|a| a == "--max")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok());

    let jwt = std::env::var("CAIRN_DEVICE_JWT").ok();
    if jwt.is_none() {
        eprintln!("error: CAIRN_DEVICE_JWT is required to call the gateway");
        std::process::exit(1);
    }
    let token: TokenProvider = {
        let jwt = jwt.clone();
        Arc::new(move || jwt.clone())
    };
    let client = EmbeddingClient::new(ApiConfig::default(), token);

    // 1) Load assistant rows with their behavioral labels.
    let content = std::fs::read_to_string(&dump_path).expect("read dump jsonl");
    let mut rows: Vec<(String, f64, i64)> = Vec::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(row) = serde_json::from_str::<DumpRow>(line) else {
            continue;
        };
        if row.event_type != "assistant" {
            continue;
        }
        let Some(text) = row.text.filter(|t| !t.trim().is_empty()) else {
            continue;
        };
        let prog = row.run_progress.unwrap_or(0.0);
        // No error behind -> treated as far from any error (smooth side).
        let dprev = row.dist_prev_error.unwrap_or(9999);
        rows.push((text, prog, dprev));
        if max.is_some_and(|m| rows.len() >= m) {
            break;
        }
    }
    eprintln!("loaded {} assistant rows", rows.len());

    // 2) Embed in batches; keep each (unit vector, labels).
    let mut labeled: Vec<Labeled> = Vec::new();
    for chunk in rows.chunks(96) {
        let texts: Vec<String> = chunk.iter().map(|(t, _, _)| t.clone()).collect();
        let vecs = client
            .embed(texts, InputType::SearchDocument, Some(COHERE_DIMS))
            .await
            .expect("embed request failed")
            .expect("gateway returned no embeddings (check JWT/account)");
        for ((_, prog, dprev), v) in chunk.iter().zip(vecs.into_iter()) {
            labeled.push(Labeled {
                vector: unit(&v),
                run_progress: *prog,
                dist_prev_error: *dprev,
            });
        }
        eprintln!("  embedded {}/{}", labeled.len(), rows.len());
    }

    // 3) Contrast axes from the label partitions, percentile tables from all rows.
    let axes = [
        (
            "phase",
            contrast_axis(
                &labeled,
                |e| e.run_progress > 0.85,
                |e| e.run_progress < 0.15,
            ),
        ),
        (
            "friction",
            contrast_axis(
                &labeled,
                |e| e.dist_prev_error <= 2,
                |e| e.dist_prev_error >= 15,
            ),
        ),
    ];

    let mut axes_json = Vec::new();
    for (name, axis) in &axes {
        let mut proj: Vec<f32> = labeled.iter().map(|e| dot(&e.vector, axis)).collect();
        proj.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let percentiles: Vec<f32> = (0..=100)
            .map(|p| percentile(&proj, p as f32 / 100.0))
            .collect();
        eprintln!(
            "  {name:8} proj p05={:+.3} p50={:+.3} p95={:+.3}",
            percentiles[5], percentiles[50], percentiles[95]
        );
        axes_json.push(serde_json::json!({
            "name": name,
            "vector": axis,
            "percentiles": percentiles,
        }));
    }

    let out = serde_json::json!({
        "model": COHERE_MODEL,
        "dims": COHERE_DIMS,
        "axes": axes_json,
    });
    std::fs::write(&out_path, serde_json::to_string(&out).expect("serialize")).expect("write axes");
    eprintln!("Wrote {} axes to {}", axes.len(), out_path);
}

fn unit(v: &[f32]) -> Vec<f32> {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n == 0.0 {
        v.to_vec()
    } else {
        v.iter().map(|x| x / n).collect()
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Unit mean-difference contrast vector: `norm(mean(pos) - mean(neg))`.
fn contrast_axis(
    events: &[Labeled],
    pos: impl Fn(&Labeled) -> bool,
    neg: impl Fn(&Labeled) -> bool,
) -> Vec<f32> {
    let pos_vecs: Vec<Vec<f32>> = events
        .iter()
        .filter(|e| pos(e))
        .map(|e| e.vector.clone())
        .collect();
    let neg_vecs: Vec<Vec<f32>> = events
        .iter()
        .filter(|e| neg(e))
        .map(|e| e.vector.clone())
        .collect();
    eprintln!(
        "    partition: pos={} neg={}",
        pos_vecs.len(),
        neg_vecs.len()
    );
    let pm = average_vectors(&pos_vecs);
    let nm = average_vectors(&neg_vecs);
    let diff: Vec<f32> = pm.iter().zip(nm.iter()).map(|(p, n)| p - n).collect();
    unit(&diff)
}

/// numpy-style linear-interpolation percentile over sorted data, `q` in `[0,1]`.
fn percentile(sorted: &[f32], q: f32) -> f32 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = q * (sorted.len() - 1) as f32;
    let lo = rank.floor() as usize;
    let hi = (lo + 1).min(sorted.len() - 1);
    let frac = rank - lo as f32;
    sorted[lo] + frac * (sorted[hi] - sorted[lo])
}
