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
