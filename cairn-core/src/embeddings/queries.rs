use diesel::dsl::{exists, not};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use crate::diesel_models::{DbEventEmbedding, NewEventEmbedding};
use crate::schema::{event_embeddings, events};

/// Insert an embedding for an event. Replaces if already exists.
pub fn upsert_embedding(
    conn: &mut SqliteConnection,
    event_id: &str,
    embedding_bytes: &[u8],
    model_name: &str,
    dimensions: i32,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let new = NewEventEmbedding {
        event_id,
        embedding: embedding_bytes,
        model_name,
        dimensions,
        created_at: now,
    };
    diesel::replace_into(event_embeddings::table)
        .values(&new)
        .execute(conn)
        .map_err(|e| format!("Failed to upsert embedding: {}", e))?;
    Ok(())
}

/// Get embedding for a single event.
pub fn get_embedding(
    conn: &mut SqliteConnection,
    event_id: &str,
) -> Result<Option<DbEventEmbedding>, String> {
    event_embeddings::table
        .find(event_id)
        .first::<DbEventEmbedding>(conn)
        .optional()
        .map_err(|e| format!("Failed to get embedding: {}", e))
}

/// Get embeddings for multiple events.
pub fn get_embeddings_for_events(
    conn: &mut SqliteConnection,
    event_ids: &[String],
) -> Result<Vec<DbEventEmbedding>, String> {
    event_embeddings::table
        .filter(event_embeddings::event_id.eq_any(event_ids))
        .load::<DbEventEmbedding>(conn)
        .map_err(|e| format!("Failed to get embeddings: {}", e))
}

/// Get embeddings for all events in a run, ordered by event sequence.
pub fn get_embeddings_for_run(
    conn: &mut SqliteConnection,
    run_id: &str,
) -> Result<Vec<(DbEventEmbedding, i32)>, String> {
    event_embeddings::table
        .inner_join(events::table.on(events::id.eq(event_embeddings::event_id)))
        .filter(events::run_id.eq(run_id))
        .select((event_embeddings::all_columns, events::sequence))
        .order(events::sequence.asc())
        .load::<(DbEventEmbedding, i32)>(conn)
        .map_err(|e| format!("Failed to get run embeddings: {}", e))
}

/// Find event IDs that are eligible for embedding but don't have one yet.
/// Returns (event_id, data) tuples for assistant events with content.
pub fn find_unembedded_events(
    conn: &mut SqliteConnection,
    limit: i64,
) -> Result<Vec<(String, String)>, String> {
    events::table
        .filter(events::event_type.eq("assistant"))
        .filter(not(exists(
            event_embeddings::table.filter(event_embeddings::event_id.eq(events::id)),
        )))
        .select((events::id, events::data))
        .order(events::created_at.desc())
        .limit(limit)
        .load::<(String, String)>(conn)
        .map_err(|e| format!("Failed to find unembedded events: {}", e))
}

/// Count events that have embeddings.
pub fn count_embeddings(conn: &mut SqliteConnection) -> Result<i64, String> {
    event_embeddings::table
        .count()
        .get_result::<i64>(conn)
        .map_err(|e| format!("Failed to count embeddings: {}", e))
}

/// Get embeddings for all events in a session, ordered by event creation time and sequence.
///
/// Joins `event_embeddings` with `events` on `event_id`, filters by `events.session_id`.
/// Used by the vibe coloring system to color skyline bars.
pub fn get_embeddings_for_session(
    conn: &mut SqliteConnection,
    session_id: &str,
) -> Result<Vec<DbEventEmbedding>, String> {
    event_embeddings::table
        .inner_join(events::table.on(events::id.eq(event_embeddings::event_id)))
        .filter(events::session_id.eq(session_id))
        .select(event_embeddings::all_columns)
        .order((events::created_at.asc(), events::sequence.asc()))
        .load::<DbEventEmbedding>(conn)
        .map_err(|e| format!("Failed to get session embeddings: {}", e))
}

/// Count events eligible for embedding (assistant events).
pub fn count_embeddable_events(conn: &mut SqliteConnection) -> Result<i64, String> {
    events::table
        .filter(events::event_type.eq("assistant"))
        .count()
        .get_result::<i64>(conn)
        .map_err(|e| format!("Failed to count embeddable events: {}", e))
}
