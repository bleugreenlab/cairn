//! A thread's record as its seed reads it: the `intent` row and the `journal`
//! in the thread's own store (thread migration 0005), its live `headlines`
//! (0006), and the tenets the thread proposed. `cairn://skills/thread-db` is how a thread keeps it.

use cairn_db::turso::params;

use crate::storage::{LocalDb, RowExt};
use crate::tenets::TenetView;

/// The journal kinds that stay open until a `resolution` retires them.
pub(crate) const OPEN_KINDS: &[&str] = &["watch", "question", "queue"];

/// How many open rows the seed reads; past this it renders a pointer.
pub(crate) const OPEN_ROWS_READ: usize = 40;

/// How many of the newest other entries the seed reads.
pub(crate) const RECENT_ROWS_READ: usize = 12;

/// How many live headlines the seed reads. The insert cap holds a thread to
/// this many; renewing retired lines can pass it, and those are counted.
pub(crate) const HEADLINES_READ: usize = 10;

/// The `intent` singleton.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Intent {
    pub current_intent: String,
    pub working: String,
}

/// One `journal` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalRow {
    pub id: i64,
    pub ts: String,
    pub kind: String,
    pub note: String,
    /// The row's refs, as stored: a JSON array of URIs.
    pub refs: Vec<String>,
}

/// One live `headlines` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadlineRow {
    pub id: i64,
    pub line: String,
    pub link: String,
    /// Seconds until `until`, as the store's clock read it.
    pub seconds_left: i64,
}

/// What a thread's seed renders from its record.
#[derive(Debug, Clone)]
pub struct ThreadRecord {
    pub intent: Intent,
    /// Unresolved `watch`, `question` and `queue` rows, newest first, at most
    /// [`OPEN_ROWS_READ`].
    pub journal_open: Vec<JournalRow>,
    /// Every unresolved row, including any past the read.
    pub open_total: usize,
    /// The newest entries of the other kinds, at most [`RECENT_ROWS_READ`].
    pub journal_recent: Vec<JournalRow>,
    /// Live headlines, newest first, at most [`HEADLINES_READ`].
    pub headlines: Vec<HeadlineRow>,
    /// Every live headline, including any past the read.
    pub headlines_live: usize,
    /// How far the operator has read the thread's headlines, if ever.
    pub headline_cursor: Option<super::headlines::HeadlineCursor>,
    /// When the record was read, epoch seconds: what the cursor's age is
    /// measured against.
    pub read_at: i64,
    /// Every tenet the thread proposed, oldest first.
    pub tenets: Vec<TenetView>,
    pub thread_id: String,
    pub project_id: String,
    pub project_key: String,
    pub thread_name: String,
}

/// Read the record of the thread that owns `job_id`.
pub async fn load(db: &LocalDb, job_id: &str) -> Result<ThreadRecord, String> {
    let owner = record_owner(db, job_id).await?;
    let store = super::db_store::open(&owner.thread_id).await?;
    let (intent, journal_open, open_total, journal_recent) = read_store(&store).await?;
    let (headlines, headlines_live) = read_headlines(&store).await?;
    let headline_cursor = super::headlines::cursor(db, &owner.thread_id).await?;
    let tenets = crate::tenets::for_thread(db, &owner.thread_id).await?;
    Ok(ThreadRecord {
        intent,
        journal_open,
        open_total,
        journal_recent,
        headlines,
        headlines_live,
        headline_cursor,
        read_at: chrono::Utc::now().timestamp(),
        tenets,
        thread_id: owner.thread_id,
        project_id: owner.project_id,
        project_key: owner.project_key,
        thread_name: owner.thread_name,
    })
}

async fn read_store(
    store: &LocalDb,
) -> Result<(Intent, Vec<JournalRow>, usize, Vec<JournalRow>), String> {
    let kinds = OPEN_KINDS
        .iter()
        .map(|kind| format!("'{kind}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let intent = store
        .query_opt(
            "SELECT current_intent, working FROM intent WHERE id = 1",
            (),
            |row| {
                Ok(Intent {
                    current_intent: row.text(0)?,
                    working: row.text(1)?,
                })
            },
        )
        .await
        .map_err(|error| format!("failed to read the thread's intent: {error}"))?
        .unwrap_or_default();
    let open = store
        .query_all(
            format!(
                "SELECT id, ts, kind, note, refs FROM journal
                 WHERE kind IN ({kinds}) AND resolved_at IS NULL
                 ORDER BY id DESC LIMIT {OPEN_ROWS_READ}"
            ),
            (),
            journal_row,
        )
        .await
        .map_err(|error| format!("failed to read the thread's open journal rows: {error}"))?;
    let open_total = store
        .query_opt_i64(
            format!("SELECT COUNT(*) FROM journal WHERE kind IN ({kinds}) AND resolved_at IS NULL"),
            (),
        )
        .await
        .map_err(|error| format!("failed to count the thread's open journal rows: {error}"))?
        .unwrap_or(0) as usize;
    let recent = store
        .query_all(
            format!(
                "SELECT id, ts, kind, note, refs FROM journal
                 WHERE kind NOT IN ({kinds})
                 ORDER BY id DESC LIMIT {RECENT_ROWS_READ}"
            ),
            (),
            journal_row,
        )
        .await
        .map_err(|error| format!("failed to read the thread's journal: {error}"))?;
    Ok((intent, open, open_total, recent))
}

/// The store's live headlines, newest first, and how many are live in all.
/// Liveness is read against the store's own clock, as the insert cap is.
pub(crate) async fn read_headlines(store: &LocalDb) -> Result<(Vec<HeadlineRow>, usize), String> {
    const LIVE: &str = "until > strftime('%Y-%m-%dT%H:%M:%SZ','now')";
    let rows = store
        .query_all(
            format!(
                "SELECT id, line, ref,
                        CAST(strftime('%s', until) AS INTEGER) - CAST(strftime('%s', 'now') AS INTEGER)
                 FROM headlines WHERE {LIVE}
                 ORDER BY id DESC LIMIT {HEADLINES_READ}"
            ),
            (),
            |row| {
                Ok(HeadlineRow {
                    id: row.i64(0)?,
                    line: row.text(1)?,
                    link: row.text(2)?,
                    seconds_left: row.i64(3)?,
                })
            },
        )
        .await
        .map_err(|error| format!("failed to read the thread's headlines: {error}"))?;
    let live = store
        .query_opt_i64(format!("SELECT COUNT(*) FROM headlines WHERE {LIVE}"), ())
        .await
        .map_err(|error| format!("failed to count the thread's headlines: {error}"))?
        .unwrap_or(0) as usize;
    Ok((rows, live))
}

fn journal_row(row: &cairn_db::turso::Row) -> crate::storage::DbResult<JournalRow> {
    let refs: String = row.text(4)?;
    Ok(JournalRow {
        id: row.i64(0)?,
        ts: row.text(1)?,
        kind: row.text(2)?,
        note: row.text(3)?,
        // The CHECK holds refs to a JSON array; a non-string entry is shown as
        // the JSON it is.
        refs: serde_json::from_str::<Vec<serde_json::Value>>(&refs)
            .unwrap_or_default()
            .into_iter()
            .map(|value| match value {
                serde_json::Value::String(text) => text,
                other => other.to_string(),
            })
            .collect(),
    })
}

struct RecordOwner {
    thread_id: String,
    thread_name: String,
    project_id: String,
    project_key: String,
}

async fn record_owner(db: &LocalDb, job_id: &str) -> Result<RecordOwner, String> {
    db.query_opt(
        "SELECT t.id, t.name, p.id, p.key
         FROM jobs j
         JOIN threads t ON t.id = j.thread_id
         JOIN projects p ON p.id = t.project_id
         WHERE j.id = ?1",
        params![job_id.to_string()],
        |row| {
            Ok(RecordOwner {
                thread_id: row.text(0)?,
                thread_name: row.text(1)?,
                project_id: row.text(2)?,
                project_key: row.text(3)?,
            })
        },
    )
    .await
    .map_err(|error| format!("failed to resolve the job's thread: {error}"))?
    .ok_or_else(|| "the job has no thread owner".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_store_read_takes_open_rows_newest_first_and_leaves_resolved_ones_out() {
        let dir = tempfile::tempdir().unwrap();
        let store = super::super::db_store::open_path(dir.path().join("record.db"))
            .await
            .unwrap();
        store
            .execute_script(
                "UPDATE intent SET current_intent = 'Ship it', working = 'Benchmarks' WHERE id = 1;
                 INSERT INTO journal(kind, note, refs) VALUES ('watch', 'First watch', '[\"cairn://p/cairn/1\"]');
                 INSERT INTO journal(kind, note) VALUES ('decision', 'Chose leases');
                 INSERT INTO journal(kind, note) VALUES ('question', 'Who owns it?');
                 INSERT INTO journal(kind, note) VALUES ('queue', 'Retired already');
                 UPDATE journal SET resolution = 'done' WHERE note = 'Retired already';",
            )
            .await
            .unwrap();
        let (intent, open, total, recent) = read_store(&store).await.unwrap();
        assert_eq!(
            intent,
            Intent {
                current_intent: "Ship it".into(),
                working: "Benchmarks".into()
            }
        );
        assert_eq!(
            open.iter().map(|row| row.note.as_str()).collect::<Vec<_>>(),
            ["Who owns it?", "First watch"]
        );
        assert_eq!(total, 2);
        assert_eq!(open[1].refs, ["cairn://p/cairn/1"]);
        assert_eq!(
            recent
                .iter()
                .map(|row| row.note.as_str())
                .collect::<Vec<_>>(),
            ["Chose leases"]
        );
    }
}
