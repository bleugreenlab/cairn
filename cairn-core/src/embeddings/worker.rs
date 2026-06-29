//! Async embedding worker.
//!
//! Drains `EmbedJob`s in small batches and routes them by variant:
//!
//! - `Event`: embed the text once, then use the resulting vector for up to two
//!   purposes before discarding it. First, a **vibe color** — only for agent
//!   content (`PositionKind::Agent`, or the legacy vibe-only replay path with no
//!   position metadata), persisted in `event_vibes`, skipped when no vibe
//!   axes are available. Second, **session position** — when the event
//!   carries `PositionMeta`, fold the vector into the in-memory `PositionEngine`:
//!   a per-session live position (persisted to `sessions.current_pos` each
//!   batch) and a per-node/chat summary (persisted to `resource_embeddings` on a
//!   coarse timer and on idle eviction). The vector itself is never persisted —
//!   only the color and the folded state.
//! - `Resource`: embed the text and persist the vector in `resource_embeddings`
//!   keyed by canonical cairn:// URI, for in-engine corpus recall. Independent
//!   of vibe centroids.
//! - `ResourceDelete`: remove the `resource_embeddings` row for a URI (no
//!   network call).
//!
//! Per-URI ordering is preserved because all variants flow through one channel.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};

use super::client::{EmbeddingClient, InputType, COHERE_DIMS, COHERE_MODEL};
use super::position::{EvictionFlush, PositionConfig, PositionEngine, PositionKind, PositionMeta};
use super::queries;
use super::vector;
use super::vibes::VibeState;
use crate::services::EventEmitter;
use crate::storage::LocalDb;

/// A unit of embedding work.
#[derive(Debug, Clone, PartialEq)]
pub enum EmbedJob {
    /// Embed event text. Color agent events; fold position when `position` is
    /// present; discard the vector.
    Event {
        event_id: String,
        text: String,
        /// Position metadata. `None` = vibe-only (legacy replay/recovery path):
        /// the event is colored but contributes nothing to session position,
        /// which avoids double-counting on re-embed.
        position: Option<PositionMeta>,
    },
    /// Embed a corpus resource and persist its vector under `uri`.
    Resource { uri: String, text: String },
    /// Remove the persisted vector for `uri`.
    ResourceDelete { uri: String },
}

impl EmbedJob {
    /// Build a resource job from `uri` and `text`, choosing a delete when the
    /// text is empty or whitespace-only (e.g. a description cleared to blank).
    pub fn resource(uri: &str, text: String) -> Self {
        if text.trim().is_empty() {
            EmbedJob::ResourceDelete {
                uri: uri.to_string(),
            }
        } else {
            EmbedJob::Resource {
                uri: uri.to_string(),
                text,
            }
        }
    }
}

const MAX_BATCH: usize = 64;
const DEBOUNCE_MS: u64 = 50;
/// How often to sweep for idle sessions and flush summary centroids.
const IDLE_CHECK_SECS: u64 = 30;
/// A session/owner untouched for this long is finalized and evicted.
const IDLE_TTL_SECS: u64 = 300;

/// Spawn the worker on the current tokio runtime. Exits when the channel closes.
///
/// `vibe_state` is optional: when absent, `Event` jobs are still embedded (and
/// still fold into session position) but not colored.
pub fn spawn_embed_worker(
    mut rx: UnboundedReceiver<EmbedJob>,
    client: EmbeddingClient,
    db: Arc<LocalDb>,
    vibe_state: Option<Arc<VibeState>>,
    emitter: Arc<dyn EventEmitter>,
) {
    tokio::spawn(async move {
        let mut engine = PositionEngine::new(PositionConfig::default());
        let mut idle = interval(Duration::from_secs(IDLE_CHECK_SECS));
        idle.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                maybe_first = rx.recv() => {
                    let Some(first) = maybe_first else {
                        // Channel closed: final flush of all in-memory state.
                        apply_flush(&db, engine.drain_all()).await;
                        break;
                    };
                    let mut batch = vec![first];
                    let mut closed = false;
                    let deadline = sleep(Duration::from_millis(DEBOUNCE_MS));
                    tokio::pin!(deadline);
                    while batch.len() < MAX_BATCH {
                        tokio::select! {
                            _ = &mut deadline => break,
                            maybe = rx.recv() => match maybe {
                                Some(job) => batch.push(job),
                                None => { closed = true; break; }
                            }
                        }
                    }
                    process_batch(&client, &db, vibe_state.as_deref(), &emitter, &mut engine, batch).await;
                    if closed {
                        apply_flush(&db, engine.drain_all()).await;
                        break;
                    }
                }
                _ = idle.tick() => {
                    // Finalize idle sessions/owners, then upsert summary
                    // centroids for still-active owners on this coarse cadence.
                    apply_flush(&db, engine.evict_idle(Instant::now(), Duration::from_secs(IDLE_TTL_SECS))).await;
                    upsert_owner_summaries(&db, engine.take_dirty_owners()).await;
                }
            }
        }
    });
}

/// A job that needs an embedding produced (Event or Resource).
enum EmbedTarget {
    Event {
        event_id: String,
        position: Option<PositionMeta>,
    },
    Resource {
        uri: String,
    },
}

async fn process_batch(
    client: &EmbeddingClient,
    db: &LocalDb,
    vibe_state: Option<&VibeState>,
    emitter: &Arc<dyn EventEmitter>,
    engine: &mut PositionEngine,
    batch: Vec<EmbedJob>,
) {
    // Deletes need no embedding — apply them directly.
    let mut targets: Vec<EmbedTarget> = Vec::new();
    let mut texts: Vec<String> = Vec::new();
    for job in batch {
        match job {
            EmbedJob::ResourceDelete { uri } => {
                if let Err(e) = queries::delete_resource_embedding_async(db, &uri).await {
                    log::warn!("Failed to delete resource embedding for {}: {}", uri, e);
                }
            }
            EmbedJob::Event {
                event_id,
                text,
                position,
            } => {
                targets.push(EmbedTarget::Event { event_id, position });
                texts.push(text);
            }
            EmbedJob::Resource { uri, text } => {
                targets.push(EmbedTarget::Resource { uri });
                texts.push(text);
            }
        }
    }

    if texts.is_empty() {
        return;
    }

    match client
        .embed(texts, InputType::SearchDocument, Some(COHERE_DIMS))
        .await
    {
        Ok(Some(vectors)) => {
            if vectors.len() != targets.len() {
                log::warn!(
                    "embed: expected {} vectors, got {}",
                    targets.len(),
                    vectors.len()
                );
                return;
            }
            for (target, vector) in targets.iter().zip(vectors.iter()) {
                match target {
                    EmbedTarget::Event { event_id, position } => {
                        if should_color(position) {
                            if let Some(vibe_state) = vibe_state {
                                if let Some(assignment) = vibe_state.assign_one(event_id, vector) {
                                    if let Err(e) = queries::upsert_event_vibe_async(
                                        db,
                                        event_id,
                                        position.as_ref().map(|meta| meta.session_id.as_str()),
                                        &assignment.css_color,
                                        assignment.phase,
                                        assignment.friction,
                                        COHERE_MODEL,
                                    )
                                    .await
                                    {
                                        log::warn!(
                                            "Failed to persist vibe for {}: {}",
                                            event_id,
                                            e
                                        );
                                    } else {
                                        let session_id =
                                            position.as_ref().map(|meta| meta.session_id.as_str());
                                        let issue_id =
                                            match queries::issue_id_for_event_async(db, event_id)
                                                .await
                                            {
                                                Ok(issue_id) => issue_id,
                                                Err(e) => {
                                                    log::warn!(
                                                    "Failed to resolve issue id for vibe {}: {}",
                                                    event_id,
                                                    e
                                                );
                                                    None
                                                }
                                            };
                                        let _ = emitter.emit(
                                            "db-change",
                                            serde_json::json!({
                                                "table": "event_vibes",
                                                "action": "upsert",
                                                "eventId": event_id,
                                                "event_id": event_id,
                                                "sessionId": session_id,
                                                "session_id": session_id,
                                                "issueId": issue_id,
                                                "issue_id": issue_id,
                                            }),
                                        );
                                    }
                                }
                            }
                        }
                        // Fold into session position (live + summary).
                        if let Some(meta) = position {
                            fold_event(engine, db, meta, vector).await;
                        }
                        // Vector intentionally discarded after color + fold.
                    }
                    EmbedTarget::Resource { uri } => {
                        let bytes = vector::to_bytes(vector);
                        if let Err(e) = queries::upsert_resource_embedding_async(
                            db,
                            uri,
                            &bytes,
                            COHERE_MODEL,
                            COHERE_DIMS as i32,
                        )
                        .await
                        {
                            log::warn!("Failed to persist resource embedding for {}: {}", uri, e);
                        }
                    }
                }
            }

            // Flush live positions touched this batch (naturally debounced by
            // the batch cadence). Summary centroids flush on the coarser timer.
            for (session_id, pos) in engine.take_dirty_sessions() {
                persist_current_pos(db, &session_id, &pos).await;
            }
        }
        Ok(None) => {
            // No account connected — skip; colors stay neutral, vectors unwritten,
            // and no position is built (nothing to fold).
        }
        Err(e) => log::warn!("embed batch failed: {}", e),
    }
}

/// Color agent content and user turns (plus the legacy vibe-only replay path
/// with no metadata); never color change signals.
fn should_color(position: &Option<PositionMeta>) -> bool {
    position
        .as_ref()
        .map(|meta| matches!(meta.kind, PositionKind::Agent | PositionKind::User))
        .unwrap_or(true)
}

/// Fold one event vector into the engine, lazily registering the session on
/// first sight. Registration resolves the owning node/chat URI and seeds the
/// live position from any persisted `current_pos` — one code path covering both
/// fresh start (no persisted position) and resume (position reloaded rather
/// than rebuilt). The owner summary is reloaded from its own persisted raw sum
/// so idle gaps and restarts resume the running mean instead of resetting it.
///
/// A transient error resolving the owner skips this event (it will retry on the
/// next one) rather than caching a `None` owner for the session's lifetime,
/// which would permanently disable its summary.
async fn fold_event(
    engine: &mut PositionEngine,
    db: &LocalDb,
    meta: &PositionMeta,
    vector: &[f32],
) {
    let now = Instant::now();
    if !engine.has_session(&meta.session_id) {
        let owner_uri = match queries::resolve_session_owner_uri_async(db, &meta.session_id).await {
            Ok(uri) => uri,
            Err(e) => {
                log::warn!(
                    "position: owner resolution failed for {} ({}); will retry",
                    meta.session_id,
                    e
                );
                return;
            }
        };
        let seed = queries::get_session_current_pos_async(db, &meta.session_id)
            .await
            .ok()
            .flatten()
            .map(|bytes| vector::from_bytes(&bytes));
        // Reload the owner's accumulated summary on first sight (idle eviction
        // or restart cleared it from memory) so its history is not lost. A
        // transient read error here defers the event (retry next) rather than
        // starting the accumulator empty — which would later upsert a partial
        // history over the good persisted sum (the same corruption as a reset).
        // Ok(None) is the legitimate "no prior summary yet" case.
        if let Some(uri) = owner_uri.as_deref() {
            if !engine.has_owner(uri) {
                match queries::get_resource_embedding_async(db, uri).await {
                    Ok(Some(record)) => {
                        engine.seed_owner(uri, vector::from_bytes(&record.embedding), now);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        log::warn!(
                            "position: summary reload failed for {} ({}); will retry",
                            uri,
                            e
                        );
                        return;
                    }
                }
            }
        }
        engine.register_session(&meta.session_id, owner_uri, seed, now);
    }
    engine.fold(&meta.session_id, vector, meta.weight, now);
}

/// Persist the flushed sessions and owner centroids from an eviction/drain.
async fn apply_flush(db: &LocalDb, flush: EvictionFlush) {
    for (session_id, pos) in flush.sessions {
        persist_current_pos(db, &session_id, &pos).await;
    }
    upsert_owner_summaries(db, flush.owners).await;
}

async fn persist_current_pos(db: &LocalDb, session_id: &str, pos: &[f32]) {
    if pos.is_empty() {
        return;
    }
    let bytes = vector::to_bytes(pos);
    if let Err(e) = queries::set_session_current_pos_async(db, session_id, &bytes).await {
        log::warn!("Failed to persist current_pos for {}: {}", session_id, e);
    }
}

async fn upsert_owner_summaries(db: &LocalDb, owners: Vec<(String, Vec<f32>)>) {
    for (uri, summary) in owners {
        if summary.is_empty() {
            continue;
        }
        let bytes = vector::to_bytes(&summary);
        if let Err(e) = queries::upsert_resource_embedding_async(
            db,
            &uri,
            &bytes,
            COHERE_MODEL,
            COHERE_DIMS as i32,
        )
        .await
        {
            log::warn!("Failed to persist summary centroid for {}: {}", uri, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_job_for_nonempty_text() {
        let job = EmbedJob::resource("cairn://p/PROJ/1", "hello".to_string());
        assert_eq!(
            job,
            EmbedJob::Resource {
                uri: "cairn://p/PROJ/1".to_string(),
                text: "hello".to_string(),
            }
        );
    }

    #[test]
    fn resource_job_empty_text_becomes_delete() {
        assert_eq!(
            EmbedJob::resource("cairn://p/PROJ/1", String::new()),
            EmbedJob::ResourceDelete {
                uri: "cairn://p/PROJ/1".to_string(),
            }
        );
    }

    #[test]
    fn resource_job_whitespace_text_becomes_delete() {
        assert_eq!(
            EmbedJob::resource("cairn://skills/x", "  \n\t ".to_string()),
            EmbedJob::ResourceDelete {
                uri: "cairn://skills/x".to_string(),
            }
        );
    }

    #[test]
    fn should_color_gates_to_agent_user_and_legacy() {
        // Legacy vibe-only replay path (no position metadata) is colored.
        assert!(should_color(&None));
        // Agent content and user turns are colored; change signals are not.
        assert!(should_color(&Some(PositionMeta::new(
            "s",
            PositionKind::Agent,
            1.0
        ))));
        assert!(should_color(&Some(PositionMeta::new(
            "s",
            PositionKind::User,
            1.0
        ))));
        assert!(!should_color(&Some(PositionMeta::new(
            "s",
            PositionKind::Change,
            1.0
        ))));
    }
}
