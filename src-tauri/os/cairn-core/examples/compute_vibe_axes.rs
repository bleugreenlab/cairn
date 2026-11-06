//! Offline regenerator for vibe axes, and the check that the app's recompute
//! reproduces the shipped Cohere v4 axes.
//!
//! Runs the app's own recompute (`embeddings::vibe_recompute`) over a COPY of a
//! workspace database, embedding through Cairn's `/embed` gateway: the same
//! sampling, labels, embedding and computation the app runs for any model
//! without bundled axes. It then prints how closely the learned axes match the
//! bundled `vibe_axes.json` (cosine per axis) and their projection
//! percentiles, and writes the learned set when `--out` is given.
//!
//! The recompute writes to the database it is given (pending migrations, its
//! sample, then the learned set in `vibe_axes`), so always pass a copy. Open a *copy* of the
//! prod db in any case: the running app holds an exclusive lock.
//!
//! Usage:
//!   [CAIRN_DEVICE_JWT=<device-jwt>] [CAIRN_API_URL=http://localhost:3849] \
//!     cargo run --example compute_vibe_axes --features internal-api -- \
//!       <copy.turso.db> [--out OUT_JSON] [--per-class N] [--distribution N] [--seed S]
//!       [--split-half]
//!
//! `--split-half` stores nothing: it embeds the same sample, learns a set from
//! each random half, and prints the half-vs-half cosine per axis and each
//! half's cosine with the shipped axes, which tells sampling noise from drift
//! between the bundle and this history.
//!
//! Without `CAIRN_DEVICE_JWT`, the anonymous device token stored in the copy
//! is used; it decrypts only on the machine whose install wrote it. On macOS a
//! copy-on-write clone (`cp -c`) on the database's own volume costs no disk at
//! first, but pins the live file's rewritten pages for as long as it is kept
//! (docs/database.md "Taking a census").

use std::sync::{Arc, Mutex};

use cairn_core::internal::account::anon_device::stored_anon_jwt;

use cairn_core::internal::embeddings::vibe_axes::{axis_agreement, compute_axes, ClassCounts};
use cairn_core::internal::embeddings::vibe_recompute::{
    embed_sample, learn_until_settled, load_stored_axes, Budget,
};
use cairn_core::internal::embeddings::{gateway_embedder, AxisSet, SpaceKeeper, VibeState};
use cairn_core::internal::storage::{LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};

fn flag<T: std::str::FromStr>(args: &[String], name: &str) -> Option<T> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let Some(db_path) = args.get(1).filter(|a| !a.starts_with("--")).cloned() else {
        eprintln!(
            "usage: compute_vibe_axes <copy.turso.db> [--out OUT_JSON] [--per-class N] [--seed S]"
        );
        std::process::exit(1);
    };
    let out: Option<String> = flag(&args, "--out");
    let budget = Budget {
        per_class: flag(&args, "--per-class").unwrap_or(Budget::DEFAULT.per_class),
        distribution: flag(&args, "--distribution").unwrap_or(Budget::DEFAULT.distribution),
        ..Budget::DEFAULT
    };
    let seed: u64 = flag(&args, "--seed").unwrap_or(1);
    let db = Arc::new(
        LocalDb::open(&db_path)
            .await
            .expect("open db (use a copy!)"),
    );
    let applied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .expect("apply migrations");
    eprintln!("applied {} migration(s) to the copy", applied.len());
    let jwt = match std::env::var("CAIRN_DEVICE_JWT").ok() {
        Some(jwt) => jwt,
        None => match stored_anon_jwt(&db).await {
            Ok(Some(jwt)) => jwt,
            other => {
                eprintln!(
                    "error: set CAIRN_DEVICE_JWT; the copy holds no usable device token ({:?})",
                    other.map(|_| ())
                );
                std::process::exit(1);
            }
        },
    };
    let embedder = gateway_embedder(jwt);
    let space = embedder.space().expect("the gateway resolves");
    if args.iter().any(|a| a == "--split-half") {
        let keeper = SpaceKeeper::new(db.clone(), embedder, Arc::new(Mutex::new(None)));
        split_half(&keeper, &space.model, budget, seed).await;
        return;
    }
    // Whatever this copy learned before is forgotten, so this run learns anew.
    db.execute("DELETE FROM vibe_axes", ())
        .await
        .expect("clear learned axes");
    let keeper = SpaceKeeper::new(db.clone(), embedder, Arc::new(Mutex::new(None)));
    // Empty: no bundled set, so the gateway's own space has none and learns.
    let vibes = VibeState::empty();
    eprintln!(
        "learning axes for {} at {:?} ({} per class, {} for the percentiles, seed {seed})",
        space.model, space.resource_dims, budget.per_class, budget.distribution
    );
    let settled = learn_until_settled(&keeper, &vibes, true, budget, seed).await;
    eprintln!("settled: {settled:?}");
    let health = keeper.embedder().health();
    if let Some(error) = health.last_error {
        eprintln!("stopped at the first provider failure, without retrying: {error}");
    }

    let learned: AxisSet = load_stored_axes(&db)
        .await
        .expect("read learned axes")
        .into_iter()
        .find(|set| set.model == space.model)
        .unwrap_or_else(|| {
            eprintln!("no axes were learned");
            std::process::exit(1);
        });
    let counts: Vec<(String, String)> = db
        .query_all("SELECT model, counts FROM vibe_axes", (), |row| {
            Ok((row.text(0)?, row.text(1)?))
        })
        .await
        .unwrap_or_default();
    for (model, counts) in counts.iter().filter(|(model, _)| *model == learned.model) {
        eprintln!("classes for {model}: {counts}");
    }

    for axis in &learned.axes {
        let p = &axis.percentiles;
        eprintln!(
            "  {:8} proj p05={:+.3} p50={:+.3} p95={:+.3}",
            axis.name, p[5], p[50], p[95]
        );
    }
    if let Some(bundled) = AxisSet::bundled() {
        let [phase, friction] = axis_agreement(&learned, &bundled);
        eprintln!("cosine with the shipped axes: phase {phase:?}, friction {friction:?}");
    }
    if let Some(out) = out {
        std::fs::write(&out, serde_json::to_string(&learned).expect("serialize"))
            .expect("write axes");
        eprintln!("wrote {out}");
    }
}

/// `--split-half`: draw and embed the recompute's sample, store nothing, split
/// it at random into two disjoint halves, learn a set from each, and print how
/// well the halves agree with each other and with the shipped axes. Halves
/// that agree with each other far better than with the shipped set mean the
/// bundle has drifted from this history; halves that agree no better mean the
/// gap is sampling noise. Each half is half the sample, so its agreement
/// understates what the whole sample would reach.
async fn split_half(keeper: &SpaceKeeper, model: &str, budget: Budget, seed: u64) {
    use rand::seq::SliceRandom;
    use rand::SeedableRng;

    let mut samples = match embed_sample(keeper, true, budget, seed).await {
        Ok(samples) => samples,
        Err(error) => {
            eprintln!("no sample: {error}");
            std::process::exit(1);
        }
    };
    samples.shuffle(&mut rand::rngs::StdRng::seed_from_u64(seed));
    let (mut a, mut b) = (Vec::new(), Vec::new());
    for (i, sample) in samples.into_iter().enumerate() {
        if i % 2 == 0 {
            a.push(sample);
        } else {
            b.push(sample);
        }
    }
    for (name, part) in [("a", &a), ("b", &b)] {
        let counts = ClassCounts::of(part.iter().map(|s| (&s.position, s.in_distribution)));
        eprintln!(
            "half {name}: {}",
            serde_json::to_string(&counts).unwrap_or_default()
        );
    }
    // A measurement, not a recompute: any half with members in every class.
    let (Ok(set_a), Ok(set_b)) = (compute_axes(model, &a, 1), compute_axes(model, &b, 1)) else {
        eprintln!("a half lacks a class; nothing to compare");
        std::process::exit(1);
    };
    let [phase, friction] = axis_agreement(&set_a, &set_b);
    eprintln!("half a vs half b: phase {phase:?}, friction {friction:?}");
    if let Some(bundled) = AxisSet::bundled() {
        for (name, set) in [("a", &set_a), ("b", &set_b)] {
            let [phase, friction] = axis_agreement(set, &bundled);
            eprintln!("half {name} vs shipped: phase {phase:?}, friction {friction:?}");
        }
    }
    eprintln!("stored nothing; the embedded sample stays in the copy's vibe_axis_samples");
}
