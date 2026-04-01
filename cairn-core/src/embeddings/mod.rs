//! Event embedding computation and storage.
//!
//! Computes vector embeddings for assistant text and thinking events
//! using a local embedding model (no API calls). Embeddings enable
//! semantic analysis: vibe coloring, loop detection, run comparison.

mod engine;
pub mod queries;
pub mod vibes;

pub use engine::{EmbeddingEngine, EmbeddingError};
pub use vibes::VibeState;

use std::sync::{Arc, Mutex};

use diesel::sqlite::SqliteConnection;

/// Startup result: initialized engine + computed VibeState.
pub struct EmbeddingInit {
    pub engine: Arc<Mutex<EmbeddingEngine>>,
    pub vibe_state: Arc<VibeState>,
}

/// Initialize the embedding engine and compute VibeState on a background thread.
///
/// If `model_dir` points to a directory containing bundled model files (model.onnx + tokenizer),
/// loads from there. Otherwise falls back to downloading from HuggingFace.
///
/// Blocks until complete (model init + loci embedding). Returns None if init fails.
pub fn init_blocking(model_dir: Option<std::path::PathBuf>) -> Option<EmbeddingInit> {
    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::Builder::new()
        .name("embedding-init".into())
        .spawn(move || {
            let engine_result = match &model_dir {
                Some(dir) if dir.join("model.onnx").exists() => {
                    log::info!("Loading bundled embedding model from {:?}", dir);
                    EmbeddingEngine::with_local_model(dir)
                }
                _ => {
                    log::info!("Downloading embedding model from HuggingFace");
                    EmbeddingEngine::new()
                }
            };

            let mut engine = match engine_result {
                Ok(e) => e,
                Err(e) => {
                    log::error!("Failed to init embedding engine: {}", e);
                    let _ = tx.send(None);
                    return;
                }
            };
            log::info!(
                "Embedding engine ready: model={}, dims={}",
                engine.model_name(),
                engine.dimensions()
            );

            let vibe_state = match VibeState::new(&mut engine, vibes::default_loci()) {
                Ok(state) => {
                    log::info!("VibeState initialized with {} loci", state.loci.len());
                    state
                }
                Err(e) => {
                    log::error!("Failed to compute VibeState: {}", e);
                    let _ = tx.send(None);
                    return;
                }
            };

            let _ = tx.send(Some(EmbeddingInit {
                engine: Arc::new(Mutex::new(engine)),
                vibe_state: Arc::new(vibe_state),
            }));
        })
        .expect("Failed to spawn embedding init thread");

    rx.recv().ok().flatten()
}

/// Compute and store an embedding for an event inline.
///
/// Called at event storage time. ~5ms per event. Silently skips
/// if the text has no embeddable content.
pub fn embed_event_inline(
    engine: &Mutex<EmbeddingEngine>,
    conn: &mut SqliteConnection,
    event_id: &str,
    data_json: &str,
) {
    let text = match extract_embeddable_text(data_json) {
        Some(t) => t,
        None => return,
    };

    let mut eng = match engine.lock() {
        Ok(e) => e,
        Err(_) => return,
    };

    match eng.embed_one(&text) {
        Ok(embedding) => {
            let bytes = EmbeddingEngine::to_bytes(&embedding);
            if let Err(e) = queries::upsert_embedding(
                conn,
                event_id,
                &bytes,
                eng.model_name(),
                eng.dimensions() as i32,
            ) {
                log::error!("Failed to store embedding for {}: {}", event_id, e);
            }
        }
        Err(e) => {
            log::error!("Embedding failed for {}: {}", event_id, e);
        }
    }
}

/// Backfill missing embeddings for a session. Called on-demand when
/// vibe colors are requested and some events lack embeddings.
///
/// Returns the number of events newly embedded.
pub fn backfill_session(
    engine: &Mutex<EmbeddingEngine>,
    conn: &mut SqliteConnection,
    session_id: &str,
) -> usize {
    use diesel::prelude::*;

    use crate::schema::{event_embeddings, events};

    // Find assistant events in this session that lack embeddings
    let missing: Vec<(String, String)> = match events::table
        .left_join(event_embeddings::table.on(event_embeddings::event_id.eq(events::id)))
        .filter(events::session_id.eq(session_id))
        .filter(events::event_type.eq("assistant"))
        .filter(event_embeddings::event_id.is_null())
        .select((events::id, events::data))
        .load::<(String, String)>(conn)
    {
        Ok(rows) => rows,
        Err(_) => return 0,
    };

    if missing.is_empty() {
        return 0;
    }

    // Filter to events with embeddable text
    let embeddable: Vec<(String, String)> = missing
        .into_iter()
        .filter_map(|(id, data)| extract_embeddable_text(&data).map(|text| (id, text)))
        .collect();

    if embeddable.is_empty() {
        return 0;
    }

    let mut eng = match engine.lock() {
        Ok(e) => e,
        Err(_) => return 0,
    };

    let texts: Vec<String> = embeddable.iter().map(|(_, t)| t.clone()).collect();
    let embeddings = match eng.embed(texts) {
        Ok(e) => e,
        Err(e) => {
            log::error!("Backfill embed failed: {}", e);
            return 0;
        }
    };

    let mut count = 0;
    for ((event_id, _), embedding) in embeddable.iter().zip(embeddings.iter()) {
        let bytes = EmbeddingEngine::to_bytes(embedding);
        if queries::upsert_embedding(
            conn,
            event_id,
            &bytes,
            eng.model_name(),
            eng.dimensions() as i32,
        )
        .is_ok()
        {
            count += 1;
        }
    }

    if count > 0 {
        log::info!("Backfilled {} embeddings for session", count);
    }
    count
}

/// Extract embeddable text from a TranscriptEvent JSON string.
/// Combines content and thinking fields, separated by newline.
pub fn extract_embeddable_text(data_json: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(data_json).ok()?;
    let content = value.get("content").and_then(|v| v.as_str()).unwrap_or("");
    let thinking = value.get("thinking").and_then(|v| v.as_str()).unwrap_or("");

    match (content.is_empty(), thinking.is_empty()) {
        (false, false) => Some(format!("{}\n{}", content, thinking)),
        (false, true) => Some(content.to_string()),
        (true, false) => Some(thinking.to_string()),
        (true, true) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_content_only() {
        let json = r#"{"content": "Hello world", "thinking": ""}"#;
        assert_eq!(
            extract_embeddable_text(json),
            Some("Hello world".to_string())
        );
    }

    #[test]
    fn extract_thinking_only() {
        let json = r#"{"content": "", "thinking": "Let me consider..."}"#;
        assert_eq!(
            extract_embeddable_text(json),
            Some("Let me consider...".to_string())
        );
    }

    #[test]
    fn extract_both_content_and_thinking() {
        let json = r#"{"content": "The answer is 42", "thinking": "I need to calculate"}"#;
        assert_eq!(
            extract_embeddable_text(json),
            Some("The answer is 42\nI need to calculate".to_string())
        );
    }

    #[test]
    fn extract_returns_none_when_both_empty() {
        let json = r#"{"content": "", "thinking": ""}"#;
        assert_eq!(extract_embeddable_text(json), None);
    }

    #[test]
    fn extract_returns_none_when_fields_missing() {
        let json = r#"{"tool_uses": []}"#;
        assert_eq!(extract_embeddable_text(json), None);
    }

    #[test]
    fn extract_returns_none_for_invalid_json() {
        assert_eq!(extract_embeddable_text("not json"), None);
    }

    #[test]
    fn extract_handles_missing_thinking_field() {
        let json = r#"{"content": "Just content"}"#;
        assert_eq!(
            extract_embeddable_text(json),
            Some("Just content".to_string())
        );
    }

    #[test]
    fn extract_handles_null_fields() {
        let json = r#"{"content": null, "thinking": null}"#;
        assert_eq!(extract_embeddable_text(json), None);
    }
}
