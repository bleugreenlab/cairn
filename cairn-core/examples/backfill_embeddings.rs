//! Bulk backfill embeddings for all assistant events in the database.
//!
//! Usage:
//!   cargo run --example backfill_embeddings -- [DB_PATH] [MODEL_DIR]
//!
//! Defaults:
//!   DB_PATH:   ~/Library/Application Support/com.cairn.desktop/cairn.db
//!   MODEL_DIR: src-tauri/resources/models/embeddings/

use std::path::PathBuf;
use std::time::Instant;

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// Find unembedded assistant events that have actual text content.
/// Uses SQL JSON extraction to skip tool-call-only events.
fn find_embeddable_unembedded(conn: &mut SqliteConnection, limit: i64) -> Vec<(String, String)> {
    // Raw SQL: find assistant events without embeddings that have content or thinking
    diesel::sql_query(
        "SELECT e.id, e.data FROM events e
         LEFT JOIN event_embeddings ee ON ee.event_id = e.id
         WHERE e.event_type = 'assistant'
           AND ee.event_id IS NULL
           AND (
             (json_extract(e.data, '$.content') IS NOT NULL AND json_extract(e.data, '$.content') != '')
             OR (json_extract(e.data, '$.thinking') IS NOT NULL AND json_extract(e.data, '$.thinking') != '')
           )
         ORDER BY e.created_at DESC
         LIMIT ?"
    )
    .bind::<diesel::sql_types::BigInt, _>(limit)
    .load::<RawEvent>(conn)
    .unwrap_or_default()
    .into_iter()
    .map(|r| (r.id, r.data))
    .collect()
}

#[derive(QueryableByName)]
struct RawEvent {
    #[diesel(sql_type = diesel::sql_types::Text)]
    id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    data: String,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let db_path = if args.len() > 1 {
        PathBuf::from(&args[1])
    } else {
        dirs::home_dir()
            .expect("no home dir")
            .join("Library/Application Support/com.cairn.desktop/cairn.db")
    };

    let model_dir = if args.len() > 2 {
        PathBuf::from(&args[2])
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../resources/models/embeddings")
    };

    eprintln!("DB: {}", db_path.display());
    eprintln!("Model: {}", model_dir.display());

    let mut conn = SqliteConnection::establish(db_path.to_str().unwrap())
        .expect("Failed to connect to database");

    diesel::sql_query("PRAGMA journal_mode=WAL")
        .execute(&mut conn)
        .ok();

    // Count state
    let total_assistant: i64 =
        cairn_core::internal::embeddings::queries::count_embeddable_events(&mut conn)
            .expect("count query failed");
    let existing: i64 = cairn_core::internal::embeddings::queries::count_embeddings(&mut conn)
        .expect("count query failed");

    // Count embeddable (has text) unembedded events
    let embeddable_missing: i64 = diesel::sql_query(
        "SELECT COUNT(*) as cnt FROM events e
         LEFT JOIN event_embeddings ee ON ee.event_id = e.id
         WHERE e.event_type = 'assistant'
           AND ee.event_id IS NULL
           AND (
             (json_extract(e.data, '$.content') IS NOT NULL AND json_extract(e.data, '$.content') != '')
             OR (json_extract(e.data, '$.thinking') IS NOT NULL AND json_extract(e.data, '$.thinking') != '')
           )"
    )
    .load::<CountRow>(&mut conn)
    .unwrap_or_default()
    .first()
    .map(|r| r.cnt)
    .unwrap_or(0);

    eprintln!(
        "Events: {} total assistant, {} already embedded, {} embeddable missing",
        total_assistant, existing, embeddable_missing
    );

    if embeddable_missing == 0 {
        eprintln!("Nothing to backfill!");
        return;
    }

    eprintln!("Loading embedding model...");
    let start = Instant::now();
    let mut engine =
        cairn_core::internal::embeddings::EmbeddingEngine::with_local_model(&model_dir)
            .expect("Failed to load embedding model");
    eprintln!("Model loaded in {:.1}s", start.elapsed().as_secs_f64());

    let batch_size: i64 = 256;
    let mut total_embedded = 0u64;
    let overall_start = Instant::now();

    loop {
        let batch = find_embeddable_unembedded(&mut conn, batch_size);

        if batch.is_empty() {
            break;
        }

        // Extract text (should all succeed since SQL pre-filtered)
        let embeddable: Vec<(String, String)> = batch
            .into_iter()
            .filter_map(|(id, data)| {
                cairn_core::internal::embeddings::extract_embeddable_text(&data)
                    .map(|text| (id, text))
            })
            .collect();

        if embeddable.is_empty() {
            break;
        }

        let texts: Vec<String> = embeddable.iter().map(|(_, t)| t.clone()).collect();
        let embeddings = engine.embed(texts).expect("embedding failed");

        for ((event_id, _), embedding) in embeddable.iter().zip(embeddings.iter()) {
            let bytes = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(embedding);
            cairn_core::internal::embeddings::queries::upsert_embedding(
                &mut conn,
                event_id,
                &bytes,
                engine.model_name(),
                engine.dimensions() as i32,
            )
            .expect("upsert failed");
        }

        total_embedded += embeddable.len() as u64;
        let elapsed = overall_start.elapsed().as_secs_f64();
        let rate = total_embedded as f64 / elapsed;
        let remaining = (embeddable_missing as u64 - total_embedded) as f64 / rate;

        eprint!(
            "\r  {}/{} embedded ({:.0}/s, ~{:.0}s remaining)    ",
            total_embedded, embeddable_missing, rate, remaining
        );
    }

    let elapsed = overall_start.elapsed().as_secs_f64();
    eprintln!(
        "\nDone: {} events embedded in {:.1}s ({:.0}/s)",
        total_embedded,
        elapsed,
        total_embedded as f64 / elapsed
    );
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    cnt: i64,
}
