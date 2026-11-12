//! The operator's side of thread headlines: how far he has read each thread's
//! `headlines` (a cursor in the main database, `headline_read_cursors`), and
//! the merged view across a project's threads that `cairn://p/{project}/headlines`
//! renders.
//!
//! The lines themselves stay in each thread's own store (thread migration
//! 0006). The merged read opens every active thread's store through the
//! per-path cache and reads its live lines; there is no main-database copy of
//! them, because a second write could not join the thread's own transaction.
//!
//! The cursor is per reader, not per device, and only the operator moves it:
//! [`mark_seen`] refuses an agent or external principal, and the resource
//! surface refuses every caller, since a resource write carries no verified
//! operator credential. The desktop's authenticated invoke path is the one
//! writer, as it is for tenet verdicts.

use std::collections::HashMap;

use cairn_common::identity::PrincipalRef;
use cairn_db::storage::TrackedConnection;
use cairn_db::turso::params;
use serde::Serialize;

use crate::storage::{LocalDb, RowExt};

/// The reader key the cursor is stored under: the literal the system already
/// attributes operator authorship to, until an operator principal id exists.
pub const OPERATOR_READER: &str = "operator";

/// The refusal for anything but the operator moving the cursor.
pub const OPERATOR_ONLY_REFUSAL: &str = "marking headlines seen is the operator's: his app moves the cursor as the lines come on screen, never an agent. A thread reads how far he has read in its seed's Headlines section and on its overview";

/// The projection a `db-change` announces when the merged view may have moved:
/// a thread wrote its `headlines` table, or the operator's cursor advanced.
pub const HEADLINES_PROJECTION: &str = "headlines";

/// The live-line predicate, against the store's own clock as the insert cap is.
const LIVE: &str = "until > strftime('%Y-%m-%dT%H:%M:%SZ','now')";

/// Where an advance came from. Kept for diagnosis only: the cursor is one per
/// reader whichever screen moved it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeenOn {
    Desktop,
    Ios,
}

impl SeenOn {
    pub fn as_str(self) -> &'static str {
        match self {
            SeenOn::Desktop => "desktop",
            SeenOn::Ios => "ios",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "desktop" => Ok(SeenOn::Desktop),
            "ios" => Ok(SeenOn::Ios),
            other => Err(format!("seen.on must be desktop or ios, not {other:?}")),
        }
    }
}

/// How far the operator has read one thread's headlines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeadlineCursor {
    /// The highest headline id he has been shown; a greater id is new.
    pub seen_through_id: i64,
    /// When the cursor last moved, epoch seconds.
    pub seen_at: i64,
    pub seen_on: Option<String>,
}

impl HeadlineCursor {
    pub fn has_seen(&self, id: i64) -> bool {
        id <= self.seen_through_id
    }
}

fn cursor_row(row: &cairn_db::turso::Row) -> crate::storage::DbResult<HeadlineCursor> {
    Ok(HeadlineCursor {
        seen_through_id: row.i64(0)?,
        seen_at: row.i64(1)?,
        seen_on: row.opt_text(2)?,
    })
}

const CURSOR_SQL: &str = "SELECT seen_through_id, seen_at, seen_on FROM headline_read_cursors
                          WHERE reader = ?1 AND thread_id = ?2";

/// The operator's cursor on one thread, or `None` if he has never read it.
pub async fn cursor(db: &LocalDb, thread_id: &str) -> Result<Option<HeadlineCursor>, String> {
    db.query_opt(
        CURSOR_SQL,
        params![OPERATOR_READER, thread_id.to_string()],
        cursor_row,
    )
    .await
    .map_err(|error| format!("failed to read the headline cursor: {error}"))
}

/// [`cursor`] on a connection a caller already holds.
pub(crate) async fn cursor_conn(
    conn: &TrackedConnection,
    thread_id: &str,
) -> Result<Option<HeadlineCursor>, String> {
    let mut rows = conn
        .query(CURSOR_SQL, params![OPERATOR_READER, thread_id])
        .await
        .map_err(|error| format!("failed to read the headline cursor: {error}"))?;
    match rows.next().await {
        Ok(Some(row)) => cursor_row(&row)
            .map(Some)
            .map_err(|error| format!("failed to decode the headline cursor: {error}")),
        Ok(None) => Ok(None),
        Err(error) => Err(format!("failed to read the headline cursor: {error}")),
    }
}

/// The one line a thread reads about its reader: how far he has read, and how
/// long ago, or that he never has.
pub fn read_through_line(cursor: Option<&HeadlineCursor>, now: i64) -> String {
    match cursor {
        Some(cursor) => format!(
            "Operator has read through #{}, {}",
            cursor.seen_through_id,
            ago(now.saturating_sub(cursor.seen_at))
        ),
        None => "Operator has never read your headlines".to_string(),
    }
}

fn ago(seconds: i64) -> String {
    let seconds = seconds.max(0);
    match seconds {
        0..=59 => "just now".to_string(),
        60..=3_599 => format!("{}m ago", seconds / 60),
        3_600..=172_799 => format!("{}h ago", seconds / 3_600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

/// What [`mark_seen`] did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SeenOutcome {
    /// Whether the cursor moved. An advance to or behind where it stands, or
    /// onto a thread with no headlines, moves nothing.
    pub moved: bool,
    /// The cursor after the write, if the thread has one.
    pub cursor: Option<HeadlineCursor>,
}

/// Advance the operator's cursor on `thread_id` through headline `through`.
///
/// Monotonic: the stored id is the larger of the old and the new, so a stale
/// pane or a second screen cannot walk it back. Clamped: `through` past the
/// thread's newest headline is cut to that id, so the cursor never claims a
/// line that has not been written yet (ids only grow, so it would otherwise
/// mark that future line seen).
pub async fn mark_seen(
    db: &LocalDb,
    principal: &PrincipalRef,
    thread_id: &str,
    through: i64,
    on: Option<SeenOn>,
    now: i64,
) -> Result<SeenOutcome, String> {
    match principal {
        PrincipalRef::Human { .. } | PrincipalRef::Machine { .. } => {}
        PrincipalRef::Agent { .. } | PrincipalRef::External { .. } => {
            return Err(OPERATOR_ONLY_REFUSAL.to_string())
        }
    }
    if through < 1 {
        return Err(format!(
            "seen.through must be a headline id (1 or more), not {through}"
        ));
    }
    let newest = match super::db_store::open_existing(thread_id).await? {
        Some(store) => store
            .query_opt_i64("SELECT MAX(id) FROM headlines", ())
            .await
            .map_err(|error| format!("failed to read the thread's headlines: {error}"))?
            .unwrap_or(0),
        None => 0,
    };
    let through = through.min(newest);
    if through < 1 {
        return Ok(SeenOutcome {
            moved: false,
            cursor: cursor(db, thread_id).await?,
        });
    }
    let (thread, on) = (thread_id.to_string(), on.map(SeenOn::as_str));
    let moved = db
        .write(move |conn| {
            let thread = thread.clone();
            Box::pin(async move {
                let changed = conn
                    .execute(
                        "INSERT INTO headline_read_cursors
                                (reader, thread_id, seen_through_id, seen_at, seen_on)
                         SELECT ?1, ?2, ?3, ?4, ?5
                          WHERE EXISTS (SELECT 1 FROM threads WHERE id = ?2)
                         ON CONFLICT(thread_id, reader) DO UPDATE SET
                                seen_through_id = MAX(
                                    headline_read_cursors.seen_through_id,
                                    excluded.seen_through_id
                                ),
                                seen_at = excluded.seen_at,
                                seen_on = excluded.seen_on
                         WHERE excluded.seen_through_id > headline_read_cursors.seen_through_id",
                        params![OPERATOR_READER, thread.as_str(), through, now, on],
                    )
                    .await?;
                Ok(changed > 0)
            })
        })
        .await
        .map_err(|error| format!("failed to move the headline cursor: {error}"))?;
    Ok(SeenOutcome {
        moved,
        cursor: cursor(db, thread_id).await?,
    })
}

/// One live line in the merged view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MergedHeadline {
    /// The thread's name, as its URI carries it.
    pub thread: String,
    pub thread_id: String,
    pub id: i64,
    pub line: String,
    #[serde(rename = "ref")]
    pub link: String,
    pub ts: String,
    pub until: String,
    pub seen: bool,
}

/// Which lines a merged read returns.
#[derive(Debug, Clone, Default)]
pub struct HeadlineFilter {
    /// One thread, by name.
    pub thread: Option<String>,
    /// New lines only.
    pub unseen_only: bool,
    pub limit: Option<usize>,
}

/// The merged headlines of a project's active threads.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MergedHeadlines {
    /// New lines first, newest first; then seen lines, newest first.
    pub lines: Vec<MergedHeadline>,
    /// Every line the filter matched, before `limit`.
    pub total: usize,
    /// How many of those are new.
    pub unseen: usize,
    /// Threads whose store could not be read, each with why. Their lines are
    /// missing from `lines`, not hidden.
    pub errors: Vec<String>,
}

/// Read the live headlines of every active thread in `project_id`, marked
/// against the operator's cursors.
pub async fn merged(
    db: &LocalDb,
    project_id: &str,
    filter: &HeadlineFilter,
) -> Result<MergedHeadlines, String> {
    let threads: Vec<(String, String)> = db
        .query_all(
            "SELECT id, name FROM threads WHERE project_id = ?1 AND status = 'active' ORDER BY name",
            params![project_id.to_string()],
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("failed to list the project's threads: {error}"))?
        .into_iter()
        .filter(|(_, name)| filter.thread.as_ref().is_none_or(|wanted| wanted == name))
        .collect();
    let cursors: HashMap<String, i64> = db
        .query_all(
            "SELECT c.thread_id, c.seen_through_id
               FROM threads t
               JOIN headline_read_cursors c ON c.reader = ?2 AND c.thread_id = t.id
              WHERE t.project_id = ?1",
            params![project_id.to_string(), OPERATOR_READER],
            |row| Ok((row.text(0)?, row.i64(1)?)),
        )
        .await
        .map_err(|error| format!("failed to read the headline cursors: {error}"))?
        .into_iter()
        .collect();

    let reads = threads.into_iter().map(|(thread_id, name)| async move {
        let lines = live_lines(&thread_id).await;
        (thread_id, name, lines)
    });
    let mut lines = Vec::new();
    let mut errors = Vec::new();
    for (thread_id, name, read) in futures_util::future::join_all(reads).await {
        let through = cursors.get(&thread_id).copied().unwrap_or(0);
        match read {
            Ok(rows) => {
                lines.extend(
                    rows.into_iter()
                        .map(|(id, line, link, ts, until)| MergedHeadline {
                            thread: name.clone(),
                            thread_id: thread_id.clone(),
                            id,
                            line,
                            link,
                            ts,
                            until,
                            seen: id <= through,
                        }),
                )
            }
            Err(error) => errors.push(format!("{name}: {error}")),
        }
    }
    if filter.unseen_only {
        lines.retain(|line| !line.seen);
    }
    // New before seen; within each, newest first. `ts` is ISO-8601 UTC, so it
    // orders as text; the thread and id break ties within one second.
    lines.sort_by(|a, b| {
        a.seen
            .cmp(&b.seen)
            .then_with(|| b.ts.cmp(&a.ts))
            .then_with(|| a.thread.cmp(&b.thread))
            .then_with(|| b.id.cmp(&a.id))
    });
    let total = lines.len();
    let unseen = lines.iter().filter(|line| !line.seen).count();
    if let Some(limit) = filter.limit {
        lines.truncate(limit);
    }
    Ok(MergedHeadlines {
        lines,
        total,
        unseen,
        errors,
    })
}

pub use cairn_common::headlines::HeadlineAbout;

/// The headline about the first of `uris` a line points at exactly, or else
/// about the issue the first of them that sits under one belongs to: the one
/// rule for which line rides on a notification entry or an operator request.
pub fn about(
    index: &HashMap<String, HeadlineAbout>,
    project_key: &str,
    uris: &[&str],
) -> Option<HeadlineAbout> {
    uris.iter()
        .find_map(|uri| index.get(*uri))
        .or_else(|| {
            uris.iter().find_map(|uri| {
                let number = cairn_uri::parse_uri(uri)?.issue_number()?;
                index.get(&cairn_uri::build_issue_uri(project_key, number))
            })
        })
        .cloned()
}

/// Every address a live headline in `project_id` points at, each with the
/// newest line about it. Seen lines count: having read a line once does not
/// make it any less the reason beside the button.
pub async fn by_ref(
    db: &LocalDb,
    project_id: &str,
    project_key: &str,
) -> Result<HashMap<String, HeadlineAbout>, String> {
    let merged = merged(db, project_id, &HeadlineFilter::default()).await?;
    for error in &merged.errors {
        log::warn!("headlines for {project_key} missing a thread: {error}");
    }
    let mut newest: HashMap<String, (String, HeadlineAbout)> = HashMap::new();
    for line in merged.lines {
        if newest.get(&line.link).is_some_and(|(ts, _)| *ts >= line.ts) {
            continue;
        }
        let root = format!(
            "{}/db",
            cairn_uri::build_thread_uri(project_key, &line.thread)
        );
        let about = HeadlineAbout {
            uri: cairn_uri::build_db_row_uri(&root, "headlines", &line.id.to_string()),
            line: line.line,
        };
        newest.insert(line.link, (line.ts, about));
    }
    Ok(newest
        .into_iter()
        .map(|(link, (_, about))| (link, about))
        .collect())
}

/// Open every active thread's store into the per-path cache.
///
/// The first merged read in a process opens each store and replays its log,
/// which took 2.8 s over the cairn project's 28 threads; once cached the same
/// read takes about a millisecond. Home and the notification rebuild both make
/// that read, so boot pays it once in the background instead of the first
/// screen paying it in the foreground.
pub async fn warm_stores(dbs: Vec<std::sync::Arc<LocalDb>>) {
    let started = std::time::Instant::now();
    let mut opened = 0usize;
    for db in dbs {
        let ids = match db
            .query_all(
                "SELECT id FROM threads WHERE status = 'active'",
                (),
                |row| row.text(0),
            )
            .await
        {
            Ok(ids) => ids,
            Err(error) => {
                log::warn!("headline store warm-up could not list threads: {error}");
                continue;
            }
        };
        for id in ids {
            match super::db_store::open_existing(&id).await {
                Ok(Some(_)) => opened += 1,
                Ok(None) => {}
                Err(error) => log::warn!("headline store warm-up skipped thread {id}: {error}"),
            }
        }
    }
    log::info!(
        "thread stores warmed for headlines; stores={opened}; duration_ms={}",
        started.elapsed().as_millis()
    );
}

type LiveRow = (i64, String, String, String, String);

/// One thread's live lines, or nothing if it has no store yet.
async fn live_lines(thread_id: &str) -> Result<Vec<LiveRow>, String> {
    let Some(store) = super::db_store::open_existing(thread_id).await? else {
        return Ok(Vec::new());
    };
    store
        .query_all(
            format!("SELECT id, line, ref, ts, until FROM headlines WHERE {LIVE} ORDER BY id DESC"),
            (),
            |row| {
                Ok((
                    row.i64(0)?,
                    row.text(1)?,
                    row.text(2)?,
                    row.text(3)?,
                    row.text(4)?,
                ))
            },
        )
        .await
        .map_err(|error| format!("failed to read headlines: {error}"))
}

#[cfg(test)]
mod tests;
