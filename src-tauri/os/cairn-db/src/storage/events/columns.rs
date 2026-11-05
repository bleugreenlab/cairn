//! Canonical `events`-table column projection and its `Row -> Event` parser.
//!
//! Single source of truth for the event-column contract: every
//! `SELECT ... FROM events` that maps to an [`Event`] builds its column list from
//! [`EVENT_COLUMNS`] and reads each row with [`event_from_row`], so an
//! events-column change cannot silently drift across duplicate copies. Both live
//! in `storage` — the parser produces a `models::Event` from a `Row` with no DB
//! access — so the read path (including the storage search index) can reach them
//! without an upward edge into the runs module. `runs::queries` re-exports them for its
//! own SQL and for the rest of the crate.

use turso::Row;

use crate::models::Event;
use crate::storage::{DbResult, RowExt};

pub const EVENT_COLUMNS: &str = "id, run_id, session_id, sequence, timestamp, event_type, data,
    parent_tool_use_id, created_at, input_tokens, cache_read_tokens, cache_create_tokens,
    output_tokens, turn_id, thinking_tokens, storage_mode, content_commit, content_render_sha,
    data_blob, codec, content_change_id, cost_usd";

/// Number of columns `EVENT_COLUMNS` projects, and therefore the zero-based
/// index of any extra column appended after it (e.g. `SELECT {EVENT_COLUMNS},
/// rowid` puts `rowid` here). Keep in lockstep with `EVENT_COLUMNS`: adding a
/// column there without bumping this read a trailing column at the wrong index.
pub const EVENT_COLUMN_COUNT: usize = 22;

pub fn event_from_row(row: &Row) -> DbResult<Event> {
    event_from_row_at(row, 0)
}

/// Decode the canonical event projection following a caller's metadata prefix.
pub fn event_from_row_at(row: &Row, offset: usize) -> DbResult<Event> {
    Ok(Event {
        id: row.text(offset)?,
        run_id: row.text(offset + 1)?,
        session_id: row.opt_text(offset + 2)?,
        sequence: row.i64(offset + 3)? as i32,
        timestamp: row.i64(offset + 4)?,
        event_type: row.text(offset + 5)?,
        data: row.text(offset + 6)?,
        parent_tool_use_id: row.opt_text(offset + 7)?,
        created_at: row.i64(offset + 8)?,
        input_tokens: row.opt_i64(offset + 9)?.map(|value| value as i32),
        cache_read_tokens: row.opt_i64(offset + 10)?.map(|value| value as i32),
        cache_create_tokens: row.opt_i64(offset + 11)?.map(|value| value as i32),
        output_tokens: row.opt_i64(offset + 12)?.map(|value| value as i32),
        turn_id: row.opt_text(offset + 13)?,
        thinking_tokens: row.opt_i64(offset + 14)?.map(|value| value as i32),
        storage_mode: row.opt_text(offset + 15)?,
        content_commit: row.opt_text(offset + 16)?,
        content_render_sha: row.opt_text(offset + 17)?,
        data_blob: row.opt_blob(offset + 18)?,
        codec: row.opt_text(offset + 19)?,
        content_change_id: row.opt_text(offset + 20)?,
        cost_usd: row.opt_f64(offset + 21)?,
        read_segments: None,
    })
}

/// The shape of an event whose stored body is prose: the predicate of the
/// partial index `idx_events_home_prose` (migration `0225_home_prose_index`),
/// written against an `e` alias.
///
/// Turso substitutes a partial index only while the query's WHERE terms imply
/// the index's, so every query that walks the index repeats this text verbatim.
/// A rewording on either side keeps every result correct while silently
/// dropping the query back to opening and parsing each row's JSON only to
/// discover the row was never a candidate. `prose_shape_matches_its_index`
/// holds the two spellings together. Project Home's transcript and the catch-up
/// attention line both read through it.
///
/// An archived (`zstd`) row qualifies on its envelope alone, because its prose
/// lives in a compressed blob SQL cannot open. Readers decompress through
/// [`super::reconstruct_events`] and drop the empties afterwards, so the
/// predicate is generous rather than wrong.
///
/// The `CASE` is load-bearing and cannot be flattened into an `AND` chain.
/// Turso's JSON functions raise `malformed JSON` instead of returning NULL, and
/// its `AND` evaluates both sides rather than short-circuiting, so a guard
/// written as `json_valid(data) AND json_type(data, ...)` still reaches
/// `json_type` with the body that cannot be parsed. `CASE` is the one form that
/// does short-circuit. Because an index predicate runs on the WRITE path, the
/// difference is not a failed read but an event the database refuses to store.
pub const PROSE_SHAPE: &str = "((CASE WHEN json_valid(e.data) AND (e.storage_mode IS NULL OR e.storage_mode='full') THEN json_type(e.data,'$.content')='text' AND trim(json_extract(e.data,'$.content'))<>'' ELSE 0 END) OR (e.storage_mode='zstd' AND e.data_blob IS NOT NULL AND e.codec IS NOT NULL))";

/// The model Claude's CLI names on a message it wrote itself (an API error,
/// an interrupted turn) rather than one a model produced.
const SYNTHETIC_MODEL: &str = "<synthetic>";

/// The model the provider says served an event, read from the event's full
/// JSON body: the value the transcript seam stores in `events.reported_model`,
/// and what a session's context meter names when its job selected no model.
///
/// Providers report it in different places (Claude's assistant message and
/// init, an OpenAI-compatible response, a Codex turn), so each known spot is
/// tried in order. Claude's `<synthetic>` names no model and is skipped.
/// An archived event's `data` is only an envelope; read it through
/// [`super::reconstruct_events`] first.
pub fn reported_model(data: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(data).ok()?;
    [
        "/model",
        "/raw/model",
        "/raw/message/model",
        "/raw/response/model",
        "/raw/turn/model",
    ]
    .into_iter()
    .filter_map(|pointer| value.pointer(pointer).and_then(serde_json::Value::as_str))
    .find(|model| !model.is_empty() && *model != SYNTHETIC_MODEL)
    .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::{reported_model, PROSE_SHAPE};

    #[test]
    fn reported_model_reads_each_provider_shape_and_skips_synthetic() {
        assert_eq!(
            reported_model(r#"{"raw":{"message":{"model":"claude-opus-5-5"}}}"#).as_deref(),
            Some("claude-opus-5-5")
        );
        assert_eq!(
            reported_model(r#"{"raw":{"response":{"model":"gpt-6-astra"}}}"#).as_deref(),
            Some("gpt-6-astra")
        );
        assert_eq!(
            reported_model(r#"{"raw":{"message":{"model":"<synthetic>"}}}"#),
            None
        );
        assert_eq!(reported_model(r#"{"raw":null}"#), None);
        assert_eq!(reported_model("not json"), None);
    }

    /// The index predicate and the query predicate are one fact spelled twice.
    ///
    /// Turso substitutes a partial index only while the query's WHERE terms
    /// imply the index's, and it reports nothing when they stop matching: every
    /// reader keeps returning exactly the same rows, having gone back to
    /// reading the whole corpus to produce them. Nothing but this equality
    /// catches a reworded predicate on either side.
    #[test]
    fn prose_shape_matches_its_index() {
        let migration = include_str!("../../../../../turso_migrations/0225_home_prose_index.sql");
        let unaliased = PROSE_SHAPE.replace("e.", "");
        assert!(
            migration.contains(&unaliased),
            "idx_events_home_prose must carry the query's prose predicate \
             verbatim, or Turso silently stops substituting it.\n\
             query wants: {unaliased}\nmigration has: {migration}"
        );
    }
}
