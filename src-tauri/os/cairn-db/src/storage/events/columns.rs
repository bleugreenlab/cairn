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
    Ok(Event {
        id: row.text(0)?,
        run_id: row.text(1)?,
        session_id: row.opt_text(2)?,
        sequence: row.i64(3)? as i32,
        timestamp: row.i64(4)?,
        event_type: row.text(5)?,
        data: row.text(6)?,
        parent_tool_use_id: row.opt_text(7)?,
        created_at: row.i64(8)?,
        input_tokens: row.opt_i64(9)?.map(|value| value as i32),
        cache_read_tokens: row.opt_i64(10)?.map(|value| value as i32),
        cache_create_tokens: row.opt_i64(11)?.map(|value| value as i32),
        output_tokens: row.opt_i64(12)?.map(|value| value as i32),
        turn_id: row.opt_text(13)?,
        thinking_tokens: row.opt_i64(14)?.map(|value| value as i32),
        storage_mode: row.opt_text(15)?,
        content_commit: row.opt_text(16)?,
        content_render_sha: row.opt_text(17)?,
        data_blob: row.opt_blob(18)?,
        codec: row.opt_text(19)?,
        content_change_id: row.opt_text(20)?,
        cost_usd: row.opt_f64(21)?,
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

#[cfg(test)]
mod tests {
    use super::PROSE_SHAPE;

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
