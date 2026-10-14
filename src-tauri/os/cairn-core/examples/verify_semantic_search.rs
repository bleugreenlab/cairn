//! Exercise the semantic search lane against real history.
//!
//! Point it at a COPY of a workspace database (the three-file MVCC set:
//! `.db`, `-wal`, `-log`), never the live one. It applies pending migrations,
//! sweeps a bounded number of turns into `turn_embeddings` through the `/embed`
//! gateway, then runs each query you give it — first through the retriever
//! alone, then through the whole fused `search_content` path when
//! `SEARCH_INDEX` names a copy of the Tantivy directory.
//!
//! The point is a paraphrase check: a query whose words do not appear in a
//! conversation should still find it.
//!
//! It needs no credential: the `/embed` gateway accepts an anonymous device
//! token, and this registers one the same way a fresh install does. Set
//! `CAIRN_DEVICE_JWT` to use an existing token instead.
//!
//! Usage:
//!   [SWEEP_TURNS=2000] [SEARCH_INDEX=/path/to/copied.search] \
//!     cargo run --example verify_semantic_search --features internal-api \
//!       -- <DB_PATH> "a paraphrase query" ["another"]

use std::sync::Arc;

use cairn_core::internal::api::ApiConfig;
use cairn_core::internal::embeddings::turns::{
    embed_pending, pending_count, turn_excerpts, Scope, SemanticSearch, SWEEP_LIMIT,
};
use cairn_core::internal::embeddings::{EmbeddingClient, TokenProvider};
use cairn_core::internal::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
use cairn_core::search::{search_content, SemanticLane};

/// Register a user-less device token, the same account-free path a fresh
/// install takes to reach the embed gateway.
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

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((db_path, queries)) = args.split_first() else {
        eprintln!("usage: verify_semantic_search <DB_PATH> <QUERY>...");
        std::process::exit(1);
    };

    let jwt = match std::env::var("CAIRN_DEVICE_JWT")
        .ok()
        .filter(|jwt| !jwt.is_empty())
    {
        Some(jwt) => jwt,
        None => register_anonymous_device().await,
    };
    let token: TokenProvider = Arc::new(move || Some(jwt.clone()));
    let client = EmbeddingClient::new(ApiConfig::default(), token);

    let db = LocalDb::open(std::path::PathBuf::from(db_path))
        .await
        .expect("open database");
    let applied = MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .expect("apply migrations");
    eprintln!("applied {} migration(s)", applied.len());

    let budget: usize = std::env::var("SWEEP_TURNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);
    eprintln!(
        "{} turn(s) pending; sweeping up to {budget}",
        pending_count(&db).await.unwrap_or(0)
    );

    let started = std::time::Instant::now();
    let mut embedded = 0usize;
    while embedded < budget {
        match embed_pending(&db, &client, Scope::All, SWEEP_LIMIT).await {
            Ok(Some(summary)) => {
                embedded += summary.embedded + summary.tombstoned;
                if !summary.remaining {
                    break;
                }
                if embedded.is_multiple_of(SWEEP_LIMIT * 8) {
                    eprintln!("  ... {embedded} turns in {:?}", started.elapsed());
                }
            }
            Ok(None) => {
                eprintln!("gateway declined (no account) — stopping");
                break;
            }
            Err(error) => {
                eprintln!("sweep failed: {error}");
                break;
            }
        }
    }
    eprintln!("swept {embedded} turn(s) in {:?}\n", started.elapsed());

    let index = std::env::var("SEARCH_INDEX")
        .ok()
        .map(|path| SearchIndex::open_or_create(path).expect("open search index"));

    let semantic = SemanticSearch::new(client);
    for query in queries {
        println!("=== {query} ===");
        let started = std::time::Instant::now();
        let Some(scored) = semantic.rank_turns(&db, None, query, 15).await else {
            println!("  (no semantic match)\n");
            continue;
        };
        let elapsed = started.elapsed();
        let turn_ids: Vec<String> = scored.iter().map(|t| t.turn_id.clone()).collect();
        let excerpts = turn_excerpts(&db, &turn_ids, 220).await;
        for turn in &scored {
            let excerpt = excerpts
                .get(&turn.turn_id)
                .map(|e| e.text.replace('\n', " "))
                .unwrap_or_else(|| "(no excerpt)".to_string());
            println!("  {:.3}  {excerpt}", turn.similarity);
        }
        println!("  [ranked in {elapsed:?}]");

        let Some(index) = index.as_ref() else {
            println!();
            continue;
        };
        let text_only = search_content(&db, index, query, None, None)
            .await
            .expect("text-only search");
        let started = std::time::Instant::now();
        let fused = search_content(
            &db,
            index,
            query,
            None,
            Some(SemanticLane {
                search: &semantic,
                vectors: &db,
            }),
        )
        .await
        .expect("fused search");
        let elapsed = started.elapsed();

        // Anything the fused answer holds that the text-only answer does not is
        // precisely what the semantic lane contributed.
        let text_ids: std::collections::HashSet<&str> =
            text_only.iter().map(|r| r.id.as_str()).collect();
        println!(
            "  -- fused: {} row(s) in {elapsed:?} (text-only: {}) --",
            fused.len(),
            text_only.len()
        );
        for (position, result) in fused.iter().enumerate().take(12) {
            let lane = if text_ids.contains(result.id.as_str()) {
                "text"
            } else {
                "SEM "
            };
            println!(
                "  {position:>2} {lane} {} ×{}  {}",
                result.uri,
                result.hit_count,
                result
                    .snippet
                    .replace('\n', " ")
                    .chars()
                    .take(90)
                    .collect::<String>()
            );
        }
        println!();
    }
}
