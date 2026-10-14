//! One-time backfill of `resource_embeddings` for the corpus
//! (issues, skills, memories, artifacts) through the `/embed` gateway.
//!
//! Usage:
//!   CAIRN_DEVICE_JWT=<device-jwt> [CAIRN_API_URL=http://localhost:3849] \
//!     cargo run --example backfill_resource_embeddings --features internal-api -- [DB_PATH]

use std::sync::Arc;

use cairn_core::internal::api::ApiConfig;
use cairn_core::internal::embeddings::vector::to_bytes;
use cairn_core::internal::embeddings::{
    queries, EmbeddingClient, InputType, TokenProvider, COHERE_DIMS, COHERE_MODEL,
};
use cairn_core::internal::storage::{LocalDb, RowExt};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db_path = args.get(1).cloned().unwrap_or_else(default_db_path);

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
    let db = LocalDb::open(std::path::PathBuf::from(&db_path))
        .await
        .expect("open db");

    // Per-type embedding sources (CAIRN-1137 updated spec):
    //   issue    -> description (whole prose),  uri cairn://p/{KEY}/{number}
    //   skill    -> description,                uri cairn://skills/{id}
    //   artifact -> deferred to CAIRN-1141: needs the canonical artifact URI
    //               (cairn://p/.../{exec}/{node}/artifact) + prose-only
    //               extraction (excluding diffs/code), which embed-on-write owns.
    //   memory   -> content, canonical node-scoped memory URI (triage similarity only).
    let mut items: Vec<(String, String)> = Vec::new();
    items.extend(load_issues(&db).await);
    items.extend(load_skills(&db).await);
    // Drop empty/whitespace text: Cohere rejects empty inputs and would fail the
    // whole 96-item batch, aborting the run.
    items.retain(|(_, text)| !text.trim().is_empty());
    eprintln!("Embedding {} corpus resources...", items.len());

    let mut done = 0usize;
    for chunk in items.chunks(96) {
        let texts: Vec<String> = chunk.iter().map(|(_, t)| t.clone()).collect();
        match client
            .embed(texts, InputType::SearchDocument, Some(COHERE_DIMS))
            .await
        {
            Ok(Some(vectors)) => {
                for ((uri, _), vector) in chunk.iter().zip(vectors.iter()) {
                    let bytes = to_bytes(vector);
                    if let Err(e) = queries::upsert_resource_embedding_async(
                        &db,
                        uri,
                        &bytes,
                        COHERE_MODEL,
                        COHERE_DIMS as i32,
                    )
                    .await
                    {
                        eprintln!("upsert failed for {uri}: {e}");
                    } else {
                        done += 1;
                    }
                }
            }
            Ok(None) => {
                eprintln!("No account/JWT — aborting.");
                break;
            }
            Err(e) => {
                eprintln!("embed failed: {e}");
                break;
            }
        }
    }
    eprintln!("Backfilled {done} resource embeddings.");
}

fn default_db_path() -> String {
    dirs::data_dir()
        .map(|p| {
            p.join("com.cairn.desktop")
                .join("cairn.turso.db")
                .to_string_lossy()
                .into_owned()
        })
        .unwrap_or_else(|| "cairn.turso.db".to_string())
}

async fn load_issues(db: &LocalDb) -> Vec<(String, String)> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT p.key, i.number, i.title, i.description \
                     FROM issues i JOIN projects p ON p.id = i.project_id",
                    (),
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let key = row.text(0)?;
                let number = row.i64(1)?;
                let _title = row.text(2)?;
                let desc = row.opt_text(3)?.unwrap_or_default();
                out.push((format!("cairn://p/{key}/{number}"), desc));
            }
            Ok(out)
        })
    })
    .await
    .unwrap_or_default()
}

async fn load_skills(db: &LocalDb) -> Vec<(String, String)> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT id, description FROM skill_configs", ())
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                let id = row.text(0)?;
                let description = row.opt_text(1)?.unwrap_or_default();
                out.push((format!("cairn://skills/{id}"), description));
            }
            Ok(out)
        })
    })
    .await
    .unwrap_or_default()
}

// Artifact and memory embedding are deferred (CAIRN-1141 / CAIRN-1140).
