//! Message database operations.
//!
//! Insert, query by channel (cursor-paginated), and query for a run's subscribed channels.

use crate::messages::queued::DeliveryUrgency;
use crate::models::{ChannelType, Message};
use crate::storage::{run_db_blocking, DbError, DbResult, LocalDb, RowExt};
use turso::params;
use uuid::Uuid;

fn db_error(context: &str, error: DbError) -> String {
    format!("{context}: {error}")
}

/// Canonical message column list. Every read path uses this so adding columns
/// only needs one edit here and a matching row-index update in
/// [`message_from_row`].
const MESSAGE_COLUMNS: &str = "id, channel_type, channel_id, sender_run_id, sender_name,
        recipient_run_id, content, created_at, urgency";

/// Convert a database row to a domain Message.
fn message_from_row(row: &turso::Row) -> DbResult<Message> {
    let channel_type = row.text(1)?;
    Ok(Message {
        id: row.text(0)?,
        channel_type: channel_type.parse().unwrap_or(ChannelType::Project),
        channel_id: row.opt_text(2)?,
        sender_run_id: row.opt_text(3)?,
        sender_name: row.text(4)?,
        recipient_run_id: row.opt_text(5)?,
        content: row.text(6)?,
        created_at: row.i64(7)?,
        urgency: row
            .opt_text(8)?
            .as_deref()
            .map(DeliveryUrgency::parse)
            .transpose()
            .map_err(DbError::Row)?,
    })
}

/// Load a single message by id, or `None` when no such row exists. The attention
/// push delivery layer uses this to resolve a `direct:` push's frozen content
/// from the durable `messages` row (the push key carries the message id), which
/// is why migrating directs onto the push queue needs no schema change
/// (CAIRN-1900).
pub async fn get_message_by_id_async(db: &LocalDb, id: &str) -> DbResult<Option<Message>> {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let sql = format!("SELECT {} FROM messages WHERE id = ?1", MESSAGE_COLUMNS);
            let mut rows = conn.query(&sql, params![id.as_str()]).await?;
            rows.next()
                .await?
                .map(|row| message_from_row(&row))
                .transpose()
        })
    })
    .await
}

async fn load_message_by_id(conn: &turso::Connection, id: &str) -> DbResult<Message> {
    let sql = format!("SELECT {} FROM messages WHERE id = ?1", MESSAGE_COLUMNS);
    let mut rows = conn.query(&sql, params![id]).await?;

    rows.next()
        .await?
        .map(|row| message_from_row(&row))
        .transpose()?
        .ok_or_else(|| DbError::Row(format!("inserted message not found: {}", id)))
}

async fn cursor_created_at(conn: &turso::Connection, id: &str) -> DbResult<Option<i64>> {
    let mut rows = conn
        .query("SELECT created_at FROM messages WHERE id = ?1", params![id])
        .await?;

    crate::storage::next_i64(&mut rows, 0).await
}

async fn collect_messages(mut rows: turso::Rows) -> DbResult<Vec<Message>> {
    let mut messages = Vec::new();
    while let Some(row) = rows.next().await? {
        messages.push(message_from_row(&row)?);
    }
    Ok(messages)
}

/// Insert a new message. Returns the created Message.
pub fn insert_message(
    db: &LocalDb,
    channel_type: &ChannelType,
    channel_id: Option<&str>,
    sender_run_id: Option<&str>,
    sender_name: &str,
    recipient_run_id: Option<&str>,
    content: &str,
) -> Result<Message, String> {
    insert_message_with_urgency(
        db,
        channel_type,
        channel_id,
        sender_run_id,
        sender_name,
        recipient_run_id,
        content,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn insert_message_with_urgency(
    db: &LocalDb,
    channel_type: &ChannelType,
    channel_id: Option<&str>,
    sender_run_id: Option<&str>,
    sender_name: &str,
    recipient_run_id: Option<&str>,
    content: &str,
    urgency: Option<DeliveryUrgency>,
) -> Result<Message, String> {
    let channel_type = channel_type.clone();
    let channel_id = channel_id.map(str::to_string);
    let sender_run_id = sender_run_id.map(str::to_string);
    let sender_name = sender_name.to_string();
    let recipient_run_id = recipient_run_id.map(str::to_string);
    let content = content.to_string();

    run_db_blocking(move || async move {
        insert_message_async(
            db,
            &channel_type,
            channel_id.as_deref(),
            sender_run_id.as_deref(),
            &sender_name,
            recipient_run_id.as_deref(),
            &content,
            urgency,
        )
        .await
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn insert_message_async(
    db: &LocalDb,
    channel_type: &ChannelType,
    channel_id: Option<&str>,
    sender_run_id: Option<&str>,
    sender_name: &str,
    recipient_run_id: Option<&str>,
    content: &str,
    urgency: Option<DeliveryUrgency>,
) -> Result<Message, String> {
    let now = chrono::Utc::now().timestamp();
    insert_message_at_async(
        db,
        channel_type,
        channel_id,
        sender_run_id,
        sender_name,
        recipient_run_id,
        content,
        urgency,
        now,
    )
    .await
}

/// Insert a message with an explicit `created_at`. Most callers use
/// [`insert_message_async`] (which stamps `now`); the external-reply path passes
/// the issue's strictly-monotonic `updated_at` so the row's own timestamp is the
/// `cairn watch` catch-up cursor.
#[allow(clippy::too_many_arguments)]
pub(super) async fn insert_message_at_async(
    db: &LocalDb,
    channel_type: &ChannelType,
    channel_id: Option<&str>,
    sender_run_id: Option<&str>,
    sender_name: &str,
    recipient_run_id: Option<&str>,
    content: &str,
    urgency: Option<DeliveryUrgency>,
    created_at: i64,
) -> Result<Message, String> {
    let id = Uuid::new_v4().to_string();
    let channel_type = channel_type.to_string();
    let channel_id = channel_id.map(str::to_string);
    let sender_run_id = sender_run_id.map(str::to_string);
    let sender_name = sender_name.to_string();
    let recipient_run_id = recipient_run_id.map(str::to_string);
    let content = content.to_string();
    let urgency = urgency.map(|u| u.as_str().to_string());

    db.write(|conn| {
        let id = id.clone();
        let channel_type = channel_type.clone();
        let channel_id = channel_id.clone();
        let sender_run_id = sender_run_id.clone();
        let sender_name = sender_name.clone();
        let recipient_run_id = recipient_run_id.clone();
        let content = content.clone();
        let urgency = urgency.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO messages (
                    id, channel_type, channel_id, sender_run_id, sender_name,
                    recipient_run_id, content, created_at, urgency
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    id.as_str(),
                    channel_type.as_str(),
                    channel_id.as_deref(),
                    sender_run_id.as_deref(),
                    sender_name.as_str(),
                    recipient_run_id.as_deref(),
                    content.as_str(),
                    created_at,
                    urgency.as_deref()
                ],
            )
            .await?;

            load_message_by_id(conn, &id).await
        })
    })
    .await
    .map_err(|error| db_error("Failed to insert message", error))
}

/// Insert an issue-channel external reply with an explicit `created_at`.
///
/// The external-reply catch-up cursor (`cairn watch`) is the message's own
/// `created_at`: the caller passes the issue's freshly-bumped, strictly-
/// monotonic `updated_at` so the row's timestamp equals the cursor the live
/// `ExternalMessageReply` event broadcasts. This unifies the message timestamp
/// with the watch cursor, replacing the retired `delivered_at` marker.
pub async fn insert_external_reply(
    db: &LocalDb,
    channel_id: &str,
    sender_run_id: &str,
    sender_name: &str,
    content: &str,
    created_at: i64,
) -> Result<Message, String> {
    insert_message_at_async(
        db,
        &ChannelType::Issue,
        Some(channel_id),
        Some(sender_run_id),
        sender_name,
        None,
        content,
        None,
        created_at,
    )
    .await
}

/// Query messages by channel with cursor-based pagination.
///
/// - `before`: return messages older than this ID (paging backward)
/// - `after`: return messages newer than this ID (catching up)
/// - `since`: only messages created at or after this unix timestamp
/// - `limit`: max results (default 50)
///
/// Returns messages in chronological order (oldest first).
pub fn query_channel(
    db: &LocalDb,
    channel_type: &ChannelType,
    channel_id: Option<&str>,
    before: Option<&str>,
    after: Option<&str>,
    since: Option<i64>,
    limit: Option<i64>,
) -> Result<Vec<Message>, String> {
    let channel_type = channel_type.clone();
    let channel_id = channel_id.map(str::to_string);
    let before = before.map(str::to_string);
    let after = after.map(str::to_string);

    run_db_blocking(move || async move {
        query_channel_async(
            db,
            &channel_type,
            channel_id.as_deref(),
            before.as_deref(),
            after.as_deref(),
            since,
            limit,
        )
        .await
    })
}

pub(super) async fn query_channel_async(
    db: &LocalDb,
    channel_type: &ChannelType,
    channel_id: Option<&str>,
    before: Option<&str>,
    after: Option<&str>,
    since: Option<i64>,
    limit: Option<i64>,
) -> Result<Vec<Message>, String> {
    let limit = limit.unwrap_or(50).min(200);
    let channel_type = channel_type.to_string();
    let channel_id = channel_id.map(str::to_string);
    let before = before.map(str::to_string);
    let after = after.map(str::to_string);

    db.read(|conn| {
        let channel_type = channel_type.clone();
        let channel_id = channel_id.clone();
        let before = before.clone();
        let after = after.clone();
        Box::pin(async move {
            let before_ts = match before.as_deref() {
                Some(id) => cursor_created_at(conn, id).await?,
                None => None,
            };
            let after_ts = match after.as_deref() {
                Some(id) => cursor_created_at(conn, id).await?,
                None => None,
            };

            let sql = format!(
                "SELECT {} FROM messages
                     WHERE channel_type = ?1
                       AND ((?2 IS NULL AND channel_id IS NULL) OR channel_id = ?2)
                       AND (?3 IS NULL OR (created_at < ?3 OR (created_at = ?3 AND id < ?4)))
                       AND (?5 IS NULL OR (created_at > ?5 OR (created_at = ?5 AND id > ?6)))
                       AND (?7 IS NULL OR created_at >= ?7)
                     ORDER BY created_at ASC, id ASC
                     LIMIT ?8",
                MESSAGE_COLUMNS
            );
            let rows = conn
                .query(
                    &sql,
                    params![
                        channel_type.as_str(),
                        channel_id.as_deref(),
                        before_ts,
                        before_ts.and(before.as_deref()),
                        after_ts,
                        after_ts.and(after.as_deref()),
                        since,
                        limit
                    ],
                )
                .await?;

            collect_messages(rows).await
        })
    })
    .await
    .map_err(|error| db_error("Failed to query messages", error))
}

/// Query new channel messages since a timestamp for hook delivery.
/// Returns project + issue channel messages newer than `since`, excluding
/// messages sent by `exclude_run_id` (the caller) and direct messages.
/// `project_key` is the project key (e.g. "CAIRN"), used as channel_id for project channels.
/// `issue_key` is the issue key in KEY/NUMBER format (e.g. "CRN/40"), used as channel_id for issue channels.
pub fn query_new_for_hook(
    db: &LocalDb,
    project_key: &str,
    issue_key: Option<&str>,
    since: i64,
    exclude_run_id: &str,
) -> Result<Vec<Message>, String> {
    let project_key = project_key.to_string();
    let issue_key = issue_key.map(str::to_string);
    let exclude_run_id = exclude_run_id.to_string();

    run_db_blocking(move || async move {
        query_new_for_hook_async(
            db,
            &project_key,
            issue_key.as_deref(),
            since,
            &exclude_run_id,
        )
        .await
    })
}

pub(super) async fn query_new_for_hook_async(
    db: &LocalDb,
    project_key: &str,
    issue_key: Option<&str>,
    since: i64,
    exclude_run_id: &str,
) -> Result<Vec<Message>, String> {
    let project_key = project_key.to_string();
    let issue_key = issue_key.map(str::to_string);
    let exclude_run_id = exclude_run_id.to_string();

    db.read(|conn| {
        let project_key = project_key.clone();
        let issue_key = issue_key.clone();
        let exclude_run_id = exclude_run_id.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {} FROM messages
                     WHERE created_at > ?1
                       AND (sender_run_id IS NULL OR sender_run_id != ?2)
                       AND (
                            (channel_type = 'project' AND channel_id = ?3)
                         OR (?4 IS NOT NULL AND channel_type = 'issue' AND channel_id = ?4)
                       )
                     ORDER BY created_at ASC
                     LIMIT 50",
                MESSAGE_COLUMNS
            );
            let rows = conn
                .query(
                    &sql,
                    params![
                        since,
                        exclude_run_id.as_str(),
                        project_key.as_str(),
                        issue_key.as_deref()
                    ],
                )
                .await?;

            collect_messages(rows).await
        })
    })
    .await
    .map_err(|error| db_error("Failed to query new messages", error))
}

/// Query messages for an issue (both issue channel and direct to agents on that issue).
/// Used for the ExecutionPanel frontend view.
/// `issue_key` is in KEY/NUMBER format (e.g. "CRN/40").
pub fn query_for_issue(
    db: &LocalDb,
    issue_key: &str,
    since: Option<i64>,
) -> Result<Vec<Message>, String> {
    let issue_key = issue_key.to_string();

    run_db_blocking(move || async move { query_for_issue_async(db, &issue_key, since).await })
}

pub(super) async fn query_for_issue_async(
    db: &LocalDb,
    issue_key: &str,
    since: Option<i64>,
) -> Result<Vec<Message>, String> {
    let issue_key = issue_key.to_string();

    db.read(|conn| {
        let issue_key = issue_key.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {} FROM messages
                     WHERE channel_type = 'issue'
                       AND channel_id = ?1
                       AND (?2 IS NULL OR created_at >= ?2)
                     ORDER BY created_at ASC
                     LIMIT 200",
                MESSAGE_COLUMNS
            );
            let rows = conn.query(&sql, params![issue_key.as_str(), since]).await?;

            collect_messages(rows).await
        })
    })
    .await
    .map_err(|error| db_error("Failed to query issue messages", error))
}

/// Query the direct-message stream for a node/task job: every direct message
/// where this job is the sender or the recipient, across all of its runs.
///
/// Direct messages are keyed by `recipient_run_id` (and `sender_run_id`), so a
/// job's full DM history is the union of directs touching any run owned by the
/// job. Returned in chronological order, oldest first.
pub fn query_directs_for_job(
    db: &LocalDb,
    job_id: &str,
    since: Option<i64>,
    limit: Option<i64>,
) -> Result<Vec<Message>, String> {
    let job_id = job_id.to_string();
    run_db_blocking(
        move || async move { query_directs_for_job_async(db, &job_id, since, limit).await },
    )
}

pub(super) async fn query_directs_for_job_async(
    db: &LocalDb,
    job_id: &str,
    since: Option<i64>,
    limit: Option<i64>,
) -> Result<Vec<Message>, String> {
    let job_id = job_id.to_string();
    let limit = limit.unwrap_or(50).min(200);

    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {} FROM messages
                     WHERE channel_type = 'direct'
                       AND (
                            recipient_run_id IN (SELECT id FROM runs WHERE job_id = ?1)
                         OR sender_run_id IN (SELECT id FROM runs WHERE job_id = ?1)
                       )
                       AND (?2 IS NULL OR created_at >= ?2)
                     ORDER BY created_at ASC, id ASC
                     LIMIT ?3",
                MESSAGE_COLUMNS
            );
            let rows = conn
                .query(&sql, params![job_id.as_str(), since, limit])
                .await?;
            collect_messages(rows).await
        })
    })
    .await
    .map_err(|error| db_error("Failed to query node messages", error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;
    use turso::params;

    /// Insert a message with a specific timestamp for deterministic ordering in tests.
    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("messages-test.db").await
    }

    async fn insert_at(
        db: &LocalDb,
        channel_type: &ChannelType,
        channel_id: Option<&str>,
        sender_run_id: Option<&str>,
        sender_name: &str,
        content: &str,
        ts: i32,
    ) -> Message {
        let id = Uuid::new_v4().to_string();
        let channel_type = channel_type.to_string();
        let channel_id = channel_id.map(str::to_string);
        let sender_run_id = sender_run_id.map(str::to_string);
        let sender_name = sender_name.to_string();
        let content = content.to_string();

        db.write(|conn| {
            let id = id.clone();
            let channel_type = channel_type.clone();
            let channel_id = channel_id.clone();
            let sender_run_id = sender_run_id.clone();
            let sender_name = sender_name.clone();
            let content = content.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO messages (
                        id, channel_type, channel_id, sender_run_id, sender_name,
                        recipient_run_id, content, created_at
                     )
                     VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7)",
                    params![
                        id.as_str(),
                        channel_type.as_str(),
                        channel_id.as_deref(),
                        sender_run_id.as_deref(),
                        sender_name.as_str(),
                        content.as_str(),
                        ts
                    ],
                )
                .await?;
                load_message_by_id(conn, &id).await
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_insert_and_query_channel() {
        let db = migrated_db().await;

        let msg = insert_message_async(
            &db,
            &ChannelType::Project,
            Some("proj-1"),
            Some("run-1"),
            "builder-1",
            None,
            "taking src/api/",
            None,
        )
        .await
        .unwrap();

        assert_eq!(msg.sender_name, "builder-1");
        assert_eq!(msg.content, "taking src/api/");
        assert!(matches!(msg.channel_type, ChannelType::Project));

        let results = query_channel_async(
            &db,
            &ChannelType::Project,
            Some("proj-1"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, msg.id);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_cursor_pagination() {
        let db = migrated_db().await;

        // Insert 3 messages with distinct timestamps for deterministic ordering
        let msg1 = insert_at(
            &db,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some("run-1"),
            "builder-1",
            "first",
            1000,
        )
        .await;

        let _msg2 = insert_at(
            &db,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some("run-2"),
            "builder-2",
            "second",
            2000,
        )
        .await;

        let msg3 = insert_at(
            &db,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some("run-1"),
            "builder-1",
            "third",
            3000,
        )
        .await;

        // Query after msg1 → should get msg2, msg3
        let after_first = query_channel_async(
            &db,
            &ChannelType::Issue,
            Some("CRN/1"),
            None,
            Some(&msg1.id),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(after_first.len(), 2);
        assert_eq!(after_first[1].content, "third");

        // Query before msg3 → should get msg1, msg2
        let before_last = query_channel_async(
            &db,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some(&msg3.id),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(before_last.len(), 2);
        assert_eq!(before_last[0].content, "first");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_system_message() {
        let db = migrated_db().await;

        // System messages now carry the run_id of the agent they describe
        let msg = insert_message_async(
            &db,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some("run-1"), // run_id of the agent this event is about
            "system",
            None,
            "builder-1 started",
            None,
        )
        .await
        .unwrap();

        assert_eq!(msg.sender_run_id.as_deref(), Some("run-1"));
        assert_eq!(msg.sender_name, "system");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_direct_message() {
        let db = migrated_db().await;

        let msg = insert_message_async(
            &db,
            &ChannelType::Direct,
            None,
            Some("run-1"),
            "builder-1",
            Some("run-2"),
            "ready for handoff",
            None,
        )
        .await
        .unwrap();

        assert!(matches!(msg.channel_type, ChannelType::Direct));
        assert_eq!(msg.recipient_run_id.as_deref(), Some("run-2"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_query_new_for_hook_excludes_directs() {
        let db = migrated_db().await;

        // A DM to recipient should NOT come back from the channel hook query,
        // which is what makes the queued-direct claim path necessary.
        insert_message_async(
            &db,
            &ChannelType::Direct,
            None,
            Some("sender"),
            "planner",
            Some("recipient"),
            "private",
            None,
        )
        .await
        .unwrap();
        insert_message_async(
            &db,
            &ChannelType::Issue,
            Some("PROJ/1"),
            Some("sender"),
            "planner",
            None,
            "public",
            None,
        )
        .await
        .unwrap();

        let results = query_new_for_hook_async(&db, "proj-1", Some("PROJ/1"), 0, "recipient")
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].content, "public");
    }
}
