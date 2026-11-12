//! Message database operations.
//!
//! Insert, query by channel (cursor-paginated), and query a job's direct
//! stream. A direct message's addressees live in `message_recipients`; every
//! loader here fills [`Message::recipients`] from it, and every direct is
//! written through [`cairn_attention::direct::insert_direct_message_conn`].

use crate::messages::queued::DeliveryUrgency;
use crate::models::{ChannelType, Message};
use crate::storage::{run_db_blocking, DbError, DbResult, LocalDb, RowExt};
use cairn_attention::direct::{insert_direct_message_conn, NewDirect, OnExisting};
use cairn_db::turso::params;
use uuid::Uuid;

fn db_error(context: &str, error: DbError) -> String {
    format!("{context}: {error}")
}

pub async fn query_for_thread(
    db: &LocalDb,
    thread_id: &str,
    since: Option<i64>,
) -> Result<Vec<Message>, String> {
    let thread_id = thread_id.to_string();
    db.read(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {} FROM messages WHERE channel_type = 'thread' AND channel_id = ?1 AND (?2 IS NULL OR created_at >= ?2) ORDER BY created_at ASC LIMIT 200",
                MESSAGE_COLUMNS
            );
            let rows = conn.query(&sql, params![thread_id, since]).await?;
            collect_messages(conn, rows).await
        })
    })
    .await
    .map_err(|error| db_error("Failed to query thread messages", error))
}

/// Canonical message column list. Every read path uses this so adding columns
/// only needs one edit here and a matching row-index update in
/// [`message_from_row`].
const MESSAGE_COLUMNS: &str = "id, channel_type, channel_id, sender_run_id, sender_name,
        content, created_at, urgency";

/// Convert a database row to a domain Message. Its recipients are filled by
/// [`with_recipients`].
fn message_from_row(row: &cairn_db::turso::Row) -> DbResult<Message> {
    let channel_type = row.text(1)?;
    Ok(Message {
        id: row.text(0)?,
        channel_type: channel_type.parse().unwrap_or(ChannelType::Project),
        channel_id: row.opt_text(2)?,
        sender_run_id: row.opt_text(3)?,
        sender_name: row.text(4)?,
        recipients: Vec::new(),
        content: row.text(5)?,
        created_at: row.i64(6)?,
        urgency: row
            .opt_text(7)?
            .as_deref()
            .map(DeliveryUrgency::parse)
            .transpose()
            .map_err(DbError::Row)?,
    })
}

/// Fill each direct's recipients with one batched read of
/// `message_recipients` (its primary key), resolving each distinct recipient
/// job to its address once.
async fn with_recipients(
    conn: &cairn_db::storage::TrackedConnection,
    mut messages: Vec<Message>,
) -> DbResult<Vec<Message>> {
    let directs: Vec<&str> = messages
        .iter()
        .filter(|message| message.channel_type == ChannelType::Direct)
        .map(|message| message.id.as_str())
        .collect();
    if directs.is_empty() {
        return Ok(messages);
    }
    let mut recipients = cairn_attention::direct::load_recipients_conn(conn, &directs).await?;
    for message in &mut messages {
        if let Some(found) = recipients.remove(&message.id) {
            message.recipients = found;
        } else if let Some(found) = recipients.get(&message.id) {
            // The same direct read from both sides of a job's stream.
            message.recipients = found.clone();
        }
    }
    Ok(messages)
}

/// Load a single message by id, or `None` when no such row exists. The attention
/// push delivery layer uses this to resolve a `direct:` push's frozen content
/// from the durable `messages` row (the push key carries the message id).
pub(crate) async fn get_message_by_id_async(db: &LocalDb, id: &str) -> DbResult<Option<Message>> {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move { load_message_conn(conn, &id).await })
    })
    .await
}

/// [`get_message_by_id_async`] inside a caller's transaction.
pub(crate) async fn load_message_conn(
    conn: &cairn_db::storage::TrackedConnection,
    id: &str,
) -> DbResult<Option<Message>> {
    let sql = format!("SELECT {} FROM messages WHERE id = ?1", MESSAGE_COLUMNS);
    let rows = conn.query(&sql, params![id]).await?;
    Ok(collect_messages(conn, rows).await?.into_iter().next())
}

async fn load_message_by_id(
    conn: &cairn_db::storage::TrackedConnection,
    id: &str,
) -> DbResult<Message> {
    load_message_conn(conn, id)
        .await?
        .ok_or_else(|| DbError::Row(format!("inserted message not found: {}", id)))
}

/// Where a cursor message sits in its channel's order: `(created_at, rowid)`.
async fn cursor_position(
    conn: &cairn_db::storage::TrackedConnection,
    id: &str,
) -> DbResult<Option<(i64, i64)>> {
    let mut rows = conn
        .query(
            "SELECT created_at, rowid FROM messages WHERE id = ?1",
            params![id],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| Ok((row.i64(0)?, row.i64(1)?)))
        .transpose()
}

/// Drain a result set into messages, then fill the directs' recipients.
async fn collect_messages(
    conn: &cairn_db::storage::TrackedConnection,
    mut rows: crate::storage::TrackedRows,
) -> DbResult<Vec<Message>> {
    let mut messages = Vec::new();
    while let Some(row) = rows.next().await? {
        messages.push(message_from_row(&row)?);
    }
    drop(rows);
    with_recipients(conn, messages).await
}

/// Insert a channel message (project, issue or thread). Returns the created
/// Message. A direct message has recipients and is written by
/// [`insert_direct_message`].
pub fn insert_message(
    db: &LocalDb,
    channel_type: &ChannelType,
    channel_id: Option<&str>,
    sender_run_id: Option<&str>,
    sender_name: &str,
    content: &str,
) -> Result<Message, String> {
    let channel_type = channel_type.clone();
    let channel_id = channel_id.map(str::to_string);
    let sender_run_id = sender_run_id.map(str::to_string);
    let sender_name = sender_name.to_string();
    let content = content.to_string();

    run_db_blocking(move || async move {
        insert_message_at_async(
            db,
            &channel_type,
            channel_id.as_deref(),
            sender_run_id.as_deref(),
            &sender_name,
            &content,
            chrono::Utc::now().timestamp(),
        )
        .await
    })
}

/// Insert a channel message with an explicit `created_at`. The external-reply
/// path passes the issue's strictly-monotonic `updated_at` so the row's own
/// timestamp is the `cairn watch` catch-up cursor.
async fn insert_message_at_async(
    db: &LocalDb,
    channel_type: &ChannelType,
    channel_id: Option<&str>,
    sender_run_id: Option<&str>,
    sender_name: &str,
    content: &str,
    created_at: i64,
) -> Result<Message, String> {
    if *channel_type == ChannelType::Direct {
        return Err("Failed to insert message: a direct message is written with its recipients by insert_direct_message".to_string());
    }
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
                    content, created_at
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    id.as_str(),
                    channel_type.as_str(),
                    channel_id.as_deref(),
                    sender_run_id.as_deref(),
                    sender_name.as_str(),
                    content.as_str(),
                    created_at
                ],
            )
            .await?;
            load_message_by_id(conn, &id).await
        })
    })
    .await
    .map_err(|error| db_error("Failed to insert message", error))
}

/// Write one direct message and its recipient rows in their own transaction,
/// and return it as stored. A stable id that already holds a different message
/// is refused; one that holds the same message returns it unchanged.
pub async fn insert_direct_message(db: &LocalDb, direct: NewDirect) -> Result<Message, String> {
    db.write(|conn| {
        let direct = direct.clone();
        Box::pin(async move {
            insert_direct_message_conn(conn, &direct, OnExisting::Verify).await?;
            load_message_by_id(conn, &direct.id).await
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
        content,
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
/// Returns messages in chronological order (oldest first); messages written in
/// the same second keep the order they were written in.
pub(crate) fn query_channel(
    db: &LocalDb,
    channel_type: &ChannelType,
    channel_id: &str,
    before: Option<&str>,
    after: Option<&str>,
    since: Option<i64>,
    limit: Option<i64>,
) -> Result<Vec<Message>, String> {
    let channel_type = channel_type.clone();
    let channel_id = channel_id.to_string();
    let before = before.map(str::to_string);
    let after = after.map(str::to_string);

    run_db_blocking(move || async move {
        query_channel_async(
            db,
            &channel_type,
            &channel_id,
            before.as_deref(),
            after.as_deref(),
            since,
            limit,
        )
        .await
    })
}

/// One page of a channel. `idx_messages_channel` holds a channel in
/// `(created_at, rowid)` order, so the page walks its one channel in order and
/// stops at the limit; same-second ties break by rowid for the same reason, so
/// no sorter is needed. The index is named because, with the channel bound as
/// parameters, Turso prefers `idx_messages_channel_created` and reads every
/// message of the channel's type.
fn channel_page_sql() -> String {
    format!(
        "SELECT {MESSAGE_COLUMNS} FROM messages INDEXED BY idx_messages_channel
          WHERE channel_type = ?1 AND channel_id = ?2
            AND (?3 IS NULL OR created_at < ?3 OR (created_at = ?3 AND rowid < ?4))
            AND (?5 IS NULL OR created_at > ?5 OR (created_at = ?5 AND rowid > ?6))
            AND (?7 IS NULL OR created_at >= ?7)
          ORDER BY created_at ASC, rowid ASC
          LIMIT ?8"
    )
}

async fn query_channel_async(
    db: &LocalDb,
    channel_type: &ChannelType,
    channel_id: &str,
    before: Option<&str>,
    after: Option<&str>,
    since: Option<i64>,
    limit: Option<i64>,
) -> Result<Vec<Message>, String> {
    let limit = limit.unwrap_or(50).min(200);
    let channel_type = channel_type.to_string();
    let channel_id = channel_id.to_string();
    let before = before.map(str::to_string);
    let after = after.map(str::to_string);

    db.read(|conn| {
        let channel_type = channel_type.clone();
        let channel_id = channel_id.clone();
        let before = before.clone();
        let after = after.clone();
        Box::pin(async move {
            let before = match before.as_deref() {
                Some(id) => cursor_position(conn, id).await?,
                None => None,
            };
            let after = match after.as_deref() {
                Some(id) => cursor_position(conn, id).await?,
                None => None,
            };

            let rows = conn
                .query(
                    &channel_page_sql(),
                    params![
                        channel_type.as_str(),
                        channel_id.as_str(),
                        before.map(|(at, _)| at),
                        before.map(|(_, rowid)| rowid),
                        after.map(|(at, _)| at),
                        after.map(|(_, rowid)| rowid),
                        since,
                        limit
                    ],
                )
                .await?;

            collect_messages(conn, rows).await
        })
    })
    .await
    .map_err(|error| db_error("Failed to query messages", error))
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

async fn query_for_issue_async(
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

            collect_messages(conn, rows).await
        })
    })
    .await
    .map_err(|error| db_error("Failed to query issue messages", error))
}

/// Query the direct-message stream for a node/task job: every direct message
/// this job was sent (as any of its recipients) or sent from any of its runs.
///
/// The received side is keyed by the recipient job in `message_recipients`;
/// the sent side by `sender_run_id`, so a thread session's messages survive
/// every run it crashes or rotates out of. Returned in chronological order,
/// oldest first.
///
/// Without `since` the window is the newest `limit` messages, because a
/// long-lived recipient's history outgrows any page and an oldest-first window
/// never reaches what was said recently. With `since` the window opens at that
/// instant and reads forward, so a reader can page through older history.
///
/// The stream is read one side at a time ([`directs_for_job_sql`]) and merged
/// here; a direct between two of the job's own runs comes back from both sides
/// and appears once.
pub(crate) fn query_directs_for_job(
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

async fn query_directs_for_job_async(
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
            let newest_first = since.is_none();
            let mut messages = Vec::new();
            for side in [DirectSide::Received, DirectSide::Sent] {
                let mut rows = conn
                    .query(
                        &directs_for_job_sql(side, newest_first),
                        params![job_id.as_str(), since, limit],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    messages.push(message_from_row(&row)?);
                }
            }
            messages.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
            messages.dedup_by(|a, b| a.id == b.id);
            let window = usize::try_from(limit).unwrap_or(0);
            if newest_first {
                messages.drain(..messages.len().saturating_sub(window));
            } else {
                messages.truncate(window);
            }
            with_recipients(conn, messages).await
        })
    })
    .await
    .map_err(|error| db_error("Failed to query node messages", error))
}

/// The two halves of a job's direct stream: what its runs received and what
/// they sent.
#[derive(Clone, Copy)]
enum DirectSide {
    Received,
    Sent,
}

/// One side of a job's direct stream, the `?3` newest (or, reading forward from
/// `?2`, oldest).
///
/// Received: the job's recipient rows in `idx_message_recipients_job`, which
/// holds them in `(created_at, message_id)` order, so the read walks the job's
/// own rows in order and stops at the limit, then fetches each message by its
/// key.
///
/// Sent: from the job's runs into `idx_messages_sender`; naming the index is
/// what puts `runs` first, since left to itself Turso walks every direct in the
/// workspace through `idx_messages_channel_created`. The index is partial on
/// the run id being set, which the statement restates so the index qualifies.
/// The ordering sorts only the job's own sent directs.
fn directs_for_job_sql(side: DirectSide, newest_first: bool) -> String {
    let order = if newest_first { "DESC" } else { "ASC" };
    let columns = qualified_message_columns("m");
    match side {
        DirectSide::Received => format!(
            "SELECT {columns} FROM message_recipients rr INDEXED BY idx_message_recipients_job
               JOIN messages m ON m.id = rr.message_id
              WHERE rr.recipient_job_id = ?1
                AND (?2 IS NULL OR rr.created_at >= ?2)
              ORDER BY rr.created_at {order}, rr.message_id {order}
              LIMIT ?3"
        ),
        DirectSide::Sent => format!(
            "SELECT {columns} FROM runs r
               JOIN messages m INDEXED BY idx_messages_sender ON m.sender_run_id = r.id
              WHERE r.job_id = ?1 AND m.sender_run_id IS NOT NULL
                AND m.channel_type = 'direct'
                AND (?2 IS NULL OR m.created_at >= ?2)
              ORDER BY m.created_at {order}, m.id {order}
              LIMIT ?3"
        ),
    }
}

/// [`MESSAGE_COLUMNS`] qualified by a table alias, for a read that joins.
fn qualified_message_columns(alias: &str) -> String {
    MESSAGE_COLUMNS
        .split(',')
        .map(|column| format!("{alias}.{}", column.trim()))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;
    use cairn_db::turso::params;

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
                        content, created_at
                     )
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
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

        let msg = insert_message_at_async(
            &db,
            &ChannelType::Project,
            Some("proj-1"),
            Some("run-1"),
            "builder-1",
            "taking src/api/",
            1,
        )
        .await
        .unwrap();

        assert_eq!(msg.sender_name, "builder-1");
        assert_eq!(msg.content, "taking src/api/");
        assert!(matches!(msg.channel_type, ChannelType::Project));

        let results =
            query_channel_async(&db, &ChannelType::Project, "proj-1", None, None, None, None)
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
            "CRN/1",
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
            "CRN/1",
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
        let msg = insert_message_at_async(
            &db,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some("run-1"), // run_id of the agent this event is about
            "system",
            "builder-1 started",
            1,
        )
        .await
        .unwrap();

        assert_eq!(msg.sender_run_id.as_deref(), Some("run-1"));
        assert_eq!(msg.sender_name, "system");
    }

    fn direct(id: &str, recipients: &[(&str, &str)]) -> NewDirect {
        NewDirect {
            id: id.to_string(),
            sender_run_id: Some("run-1".to_string()),
            sender_name: "builder-1".to_string(),
            content: "ready for handoff".to_string(),
            urgency: Some(DeliveryUrgency::Steer),
            created_at: 7,
            recipients: recipients
                .iter()
                .map(|(job, run)| cairn_attention::direct::NewRecipient {
                    job_id: job.to_string(),
                    run_id_at_send: Some(run.to_string()),
                    wake: Some(crate::orchestrator::attention_push::Wake::Wake),
                })
                .collect(),
        }
    }

    /// A direct is one message row and one recipient row per addressee, in the
    /// order named, and it loads back with them.
    #[tokio::test(flavor = "current_thread")]
    async fn a_direct_is_stored_once_with_its_recipients_in_order() {
        let db = migrated_db().await;

        let msg = insert_direct_message(&db, direct("d1", &[("j2", "run-2"), ("j3", "run-3")]))
            .await
            .unwrap();

        assert!(matches!(msg.channel_type, ChannelType::Direct));
        let jobs: Vec<_> = msg
            .recipients
            .iter()
            .map(|recipient| {
                (
                    recipient.job_id.as_deref(),
                    recipient.run_id_at_send.as_deref(),
                    recipient.wake,
                )
            })
            .collect();
        assert_eq!(
            jobs,
            [
                (
                    Some("j2"),
                    Some("run-2"),
                    Some(crate::orchestrator::attention_push::Wake::Wake)
                ),
                (
                    Some("j3"),
                    Some("run-3"),
                    Some(crate::orchestrator::attention_push::Wake::Wake)
                ),
            ]
        );
        assert_eq!(
            db.query_one("SELECT COUNT(*) FROM messages", (), |row| row.i64(0))
                .await
                .unwrap(),
            1
        );

        // The same stable id with the same message is the message already
        // stored; with other recipients it is refused.
        let again = insert_direct_message(&db, direct("d1", &[("j2", "run-2"), ("j3", "run-3")]))
            .await
            .unwrap();
        assert_eq!(again.recipients.len(), 2);
        let error = insert_direct_message(&db, direct("d1", &[("j2", "run-2")]))
            .await
            .unwrap_err();
        assert!(
            error.contains("already exists with different content"),
            "{error}"
        );

        let refused = insert_message(&db, &ChannelType::Direct, None, None, "x", "no recipients")
            .unwrap_err();
        assert!(refused.contains("insert_direct_message"), "{refused}");
    }

    /// Messages written in the same second page in the order they were written,
    /// whatever their ids, and a cursor inside the tie resumes after it.
    #[tokio::test(flavor = "current_thread")]
    async fn same_second_messages_page_in_write_order() {
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO messages(id,channel_type,channel_id,sender_name,content,created_at)
               VALUES('c','issue','CRN/1','a','first',1000),
                     ('a','issue','CRN/1','a','second',1000),
                     ('b','issue','CRN/1','a','third',1000),
                     ('z','issue','CRN/2','a','elsewhere',1000);",
        )
        .await
        .unwrap();
        let page = |after: Option<&'static str>| {
            let db = &db;
            async move {
                query_channel_async(db, &ChannelType::Issue, "CRN/1", None, after, None, None)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|message| message.content)
                    .collect::<Vec<_>>()
            }
        };

        assert_eq!(page(None).await, ["first", "second", "third"]);
        assert_eq!(page(Some("c")).await, ["second", "third"]);
    }

    /// A job's direct stream is what it was sent or any of its runs sent, each
    /// message once even when it went between two of the job's own runs, and
    /// nothing of another job's. A message to several recipients is in each
    /// recipient's stream.
    #[tokio::test(flavor = "current_thread")]
    async fn a_jobs_directs_merge_both_sides_once() {
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO jobs(id,project_id,status,created_at,updated_at) VALUES('j','p','running',1,1),('other','p','running',1,1);
             INSERT INTO runs(id,job_id,project_id,status,created_at,updated_at)
               VALUES('r1','j','p','crashed',1,1),('r2','j','p','live',2,2),('o1','other','p','live',1,1);
             INSERT INTO messages(id,channel_type,sender_run_id,sender_name,content,created_at) VALUES
               ('m1','direct','o1','peer','received',100),
               ('m2','direct','r2','me','sent',101),
               ('m3','direct','r1','me','to myself',102),
               ('m4','direct','o1','peer','not mine',103),
               ('m5','direct',NULL,'system','nudge',104),
               ('m6','issue','r1','me','not a direct',105);
             INSERT INTO message_recipients(message_id,position,recipient_job_id,recipient_run_id,created_at) VALUES
               ('m1',0,'other','o1',100),('m1',1,'j','r1',100),
               ('m2',0,'other','o1',101),
               ('m3',0,'j','r2',102),
               ('m4',0,'other','o1',103),
               ('m5',0,'j','r2',104);",
        )
        .await
        .unwrap();
        let contents = |messages: Vec<Message>| {
            messages
                .into_iter()
                .map(|message| message.content)
                .collect::<Vec<_>>()
        };

        let newest = query_directs_for_job_async(&db, "j", None, None)
            .await
            .unwrap();
        assert_eq!(
            newest[0].recipients.len(),
            2,
            "a cc'd message carries every recipient"
        );
        assert_eq!(contents(newest), ["received", "sent", "to myself", "nudge"]);

        let newest_two = query_directs_for_job_async(&db, "j", None, Some(2))
            .await
            .unwrap();
        assert_eq!(contents(newest_two), ["to myself", "nudge"]);

        let forward_two = query_directs_for_job_async(&db, "j", Some(101), Some(2))
            .await
            .unwrap();
        assert_eq!(contents(forward_two), ["sent", "to myself"]);
    }

    /// The channel page seeks its one channel through `idx_messages_channel`
    /// and reads it in index order. A plan on `channel_type` alone reads every
    /// message of that type, and a sorter reads the whole channel before the
    /// limit applies.
    #[tokio::test(flavor = "current_thread")]
    async fn the_channel_page_seeks_its_channel_in_index_order() {
        use cairn_db::turso::Value;
        let db = migrated_db().await;
        let mut binds = vec![Value::Text("issue".into()), Value::Text("CRN/1".into())];
        binds.extend([Value::Integer(1000), Value::Integer(7)]);
        binds.extend([Value::Null, Value::Null, Value::Null, Value::Integer(50)]);
        let plan = crate::storage::query_plan(&db, &channel_page_sql(), binds).await;

        assert!(
            plan.contains("idx_messages_channel (channel_type=? AND channel_id=?)"),
            "the page must seek its one channel:\n{plan}"
        );
        assert!(!plan.contains("SORTER"), "{plan}");
        assert!(!plan.contains("SCAN messages"), "{plan}");
    }

    /// The received side walks the job's own recipient rows in index order and
    /// fetches each message by key, with no sorter, so the limit bounds the
    /// walk. Driven from `messages` instead, it reads every direct in the
    /// workspace (~98k on the 11-02 production clone).
    #[tokio::test(flavor = "current_thread")]
    async fn a_jobs_received_directs_walk_its_recipient_rows_in_order() {
        use cairn_db::turso::Value;
        let db = migrated_db().await;
        for newest_first in [true, false] {
            let binds = vec![Value::Text("j".into()), Value::Null, Value::Integer(50)];
            let plan = crate::storage::query_plan(
                &db,
                &directs_for_job_sql(DirectSide::Received, newest_first),
                binds,
            )
            .await;
            let recipients_first = plan
                .find("idx_message_recipients_job (recipient_job_id=?)")
                .unwrap_or_else(|| panic!("the job's recipient rows drive the read:\n{plan}"));
            let then_message = plan
                .find("SEARCH m USING INDEX sqlite_autoindex_messages_1 (id=?)")
                .or_else(|| plan.find("SEARCH m USING PRIMARY KEY"))
                .unwrap_or_else(|| panic!("each row fetches its message by key:\n{plan}"));
            assert!(recipients_first < then_message, "{plan}");
            assert!(!plan.contains("SORTER"), "{plan}");
            assert!(!plan.contains("idx_messages_channel_created"), "{plan}");
        }
    }

    /// The sent side starts from the job's runs and seeks each run's directs
    /// by sender run id.
    #[tokio::test(flavor = "current_thread")]
    async fn a_jobs_sent_directs_seek_each_of_its_runs() {
        use cairn_db::turso::Value;
        let db = migrated_db().await;
        for (side, seek) in [(
            DirectSide::Sent,
            "SEARCH m USING INDEX idx_messages_sender (sender_run_id=?)",
        )] {
            for newest_first in [true, false] {
                let binds = vec![Value::Text("j".into()), Value::Null, Value::Integer(50)];
                let plan = crate::storage::query_plan(
                    &db,
                    &directs_for_job_sql(side, newest_first),
                    binds,
                )
                .await;
                let runs_first = plan
                    .find("SEARCH r USING INDEX idx_runs_job_id (job_id=?)")
                    .unwrap_or_else(|| panic!("the job's runs drive the read:\n{plan}"));
                let then_messages = plan
                    .find(seek)
                    .unwrap_or_else(|| panic!("each run seeks its own directs:\n{plan}"));
                assert!(runs_first < then_messages, "{plan}");
                assert!(!plan.contains("idx_messages_channel_created"), "{plan}");
            }
        }
    }
}
