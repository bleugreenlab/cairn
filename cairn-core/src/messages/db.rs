//! Message database operations.
//!
//! Insert, query by channel (cursor-paginated), and query for a run's subscribed channels.

use crate::diesel_models::{DbMessage, NewMessage};
use crate::models::{ChannelType, Message};
use crate::schema::messages;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use uuid::Uuid;

/// Convert a DbMessage to a domain Message.
fn to_domain(db: DbMessage) -> Message {
    Message {
        id: db.id,
        channel_type: db.channel_type.parse().unwrap_or(ChannelType::Project),
        channel_id: db.channel_id,
        sender_run_id: db.sender_run_id,
        sender_name: db.sender_name,
        recipient_run_id: db.recipient_run_id,
        content: db.content,
        created_at: db.created_at as i64,
    }
}

/// Insert a new message. Returns the created Message.
pub fn insert_message(
    conn: &mut SqliteConnection,
    channel_type: &ChannelType,
    channel_id: Option<&str>,
    sender_run_id: Option<&str>,
    sender_name: &str,
    recipient_run_id: Option<&str>,
    content: &str,
) -> Result<Message, String> {
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    let new = NewMessage {
        id: &id,
        channel_type: &channel_type.to_string(),
        channel_id,
        sender_run_id,
        sender_name,
        recipient_run_id,
        content,
        created_at: now,
    };

    diesel::insert_into(messages::table)
        .values(&new)
        .execute(conn)
        .map_err(|e| format!("Failed to insert message: {}", e))?;

    let db_msg: DbMessage = messages::table
        .find(&id)
        .first(conn)
        .map_err(|e| format!("Failed to load inserted message: {}", e))?;

    Ok(to_domain(db_msg))
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
    conn: &mut SqliteConnection,
    channel_type: &ChannelType,
    channel_id: Option<&str>,
    before: Option<&str>,
    after: Option<&str>,
    since: Option<i64>,
    limit: Option<i64>,
) -> Result<Vec<Message>, String> {
    let limit = limit.unwrap_or(50).min(200);

    let mut query = messages::table
        .filter(messages::channel_type.eq(channel_type.to_string()))
        .into_boxed();

    // Channel ID filter
    match channel_id {
        Some(cid) => {
            query = query.filter(messages::channel_id.eq(cid));
        }
        None => {
            query = query.filter(messages::channel_id.is_null());
        }
    }

    // Cursor-based pagination
    if let Some(before_id) = before {
        // Get the created_at of the cursor message
        let cursor_ts: Option<i32> = messages::table
            .find(before_id)
            .select(messages::created_at)
            .first(conn)
            .ok();
        if let Some(ts) = cursor_ts {
            query = query.filter(
                messages::created_at
                    .lt(ts)
                    .or(messages::created_at.eq(ts).and(messages::id.lt(before_id))),
            );
        }
    }

    if let Some(after_id) = after {
        let cursor_ts: Option<i32> = messages::table
            .find(after_id)
            .select(messages::created_at)
            .first(conn)
            .ok();
        if let Some(ts) = cursor_ts {
            query = query.filter(
                messages::created_at
                    .gt(ts)
                    .or(messages::created_at.eq(ts).and(messages::id.gt(after_id))),
            );
        }
    }

    // Datetime filter per user feedback
    if let Some(since_ts) = since {
        query = query.filter(messages::created_at.ge(since_ts as i32));
    }

    let rows: Vec<DbMessage> = query
        .order((messages::created_at.asc(), messages::id.asc()))
        .limit(limit)
        .load(conn)
        .map_err(|e| format!("Failed to query messages: {}", e))?;

    Ok(rows.into_iter().map(to_domain).collect())
}

/// Query recent messages across all channels a run is subscribed to.
/// A run subscribes to: its project channel + its issue channel (if any).
///
/// Used for prompt injection at session start.
/// `project_key` is the project key (e.g. "CAIRN"), used as channel_id for project channels.
/// `issue_key` is the issue key in KEY/NUMBER format (e.g. "CRN/40"), used as channel_id for issue channels.
pub fn query_recent_for_run(
    conn: &mut SqliteConnection,
    project_key: &str,
    issue_key: Option<&str>,
    exclude_run_id: Option<&str>,
    limit: i64,
) -> Result<Vec<Message>, String> {
    let mut query = messages::table.into_boxed();

    // Exclude messages about self (system messages carry the run_id of the
    // agent they describe as sender_run_id)
    if let Some(rid) = exclude_run_id {
        query = query.filter(
            messages::sender_run_id
                .is_null()
                .or(messages::sender_run_id.ne(rid)),
        );
    }

    match issue_key {
        Some(iid) => {
            // Project channel OR issue channel
            query = query.filter(
                messages::channel_type
                    .eq("project")
                    .and(messages::channel_id.eq(project_key))
                    .or(messages::channel_type
                        .eq("issue")
                        .and(messages::channel_id.eq(iid))),
            );
        }
        None => {
            // Project channel only
            query = query.filter(
                messages::channel_type
                    .eq("project")
                    .and(messages::channel_id.eq(project_key)),
            );
        }
    }

    let rows: Vec<DbMessage> = query
        .order(messages::created_at.desc())
        .limit(limit)
        .load(conn)
        .map_err(|e| format!("Failed to query recent messages: {}", e))?;

    // Reverse to get chronological order
    let mut msgs: Vec<Message> = rows.into_iter().map(to_domain).collect();
    msgs.reverse();
    Ok(msgs)
}

/// Query new channel messages since a timestamp for hook delivery.
/// Returns project + issue channel messages newer than `since`, excluding
/// messages sent by `exclude_run_id` (the caller) and direct messages.
/// `project_key` is the project key (e.g. "CAIRN"), used as channel_id for project channels.
/// `issue_key` is the issue key in KEY/NUMBER format (e.g. "CRN/40"), used as channel_id for issue channels.
pub fn query_new_for_hook(
    conn: &mut SqliteConnection,
    project_key: &str,
    issue_key: Option<&str>,
    since: i64,
    exclude_run_id: &str,
) -> Result<Vec<Message>, String> {
    let mut query = messages::table
        .filter(messages::created_at.gt(since as i32))
        .filter(
            messages::sender_run_id
                .is_null()
                .or(messages::sender_run_id.ne(exclude_run_id)),
        )
        .into_boxed();

    match issue_key {
        Some(iid) => {
            query = query.filter(
                messages::channel_type
                    .eq("project")
                    .and(messages::channel_id.eq(project_key))
                    .or(messages::channel_type
                        .eq("issue")
                        .and(messages::channel_id.eq(iid))),
            );
        }
        None => {
            query = query.filter(
                messages::channel_type
                    .eq("project")
                    .and(messages::channel_id.eq(project_key)),
            );
        }
    }

    let rows: Vec<DbMessage> = query
        .order(messages::created_at.asc())
        .limit(50)
        .load(conn)
        .map_err(|e| format!("Failed to query new messages: {}", e))?;

    Ok(rows.into_iter().map(to_domain).collect())
}

/// Query messages for an issue (both issue channel and direct to agents on that issue).
/// Used for the ExecutionPanel frontend view.
/// `issue_key` is in KEY/NUMBER format (e.g. "CRN/40").
pub fn query_for_issue(
    conn: &mut SqliteConnection,
    issue_key: &str,
    since: Option<i64>,
) -> Result<Vec<Message>, String> {
    let mut query = messages::table
        .filter(
            messages::channel_type
                .eq("issue")
                .and(messages::channel_id.eq(issue_key)),
        )
        .into_boxed();

    if let Some(since_ts) = since {
        query = query.filter(messages::created_at.ge(since_ts as i32));
    }

    let rows: Vec<DbMessage> = query
        .order(messages::created_at.asc())
        .limit(200)
        .load(conn)
        .map_err(|e| format!("Failed to query issue messages: {}", e))?;

    Ok(rows.into_iter().map(to_domain).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::test_diesel_conn;

    /// Insert a message with a specific timestamp for deterministic ordering in tests.
    fn insert_at(
        conn: &mut SqliteConnection,
        channel_type: &ChannelType,
        channel_id: Option<&str>,
        sender_run_id: Option<&str>,
        sender_name: &str,
        content: &str,
        ts: i32,
    ) -> Message {
        let id = Uuid::new_v4().to_string();
        let new = NewMessage {
            id: &id,
            channel_type: &channel_type.to_string(),
            channel_id,
            sender_run_id,
            sender_name,
            recipient_run_id: None,
            content,
            created_at: ts,
        };
        diesel::insert_into(messages::table)
            .values(&new)
            .execute(conn)
            .unwrap();
        let db_msg: DbMessage = messages::table.find(&id).first(conn).unwrap();
        to_domain(db_msg)
    }

    #[test]
    fn test_insert_and_query_channel() {
        let mut conn = test_diesel_conn();

        let msg = insert_message(
            &mut conn,
            &ChannelType::Project,
            Some("proj-1"),
            Some("run-1"),
            "builder-1",
            None,
            "taking src/api/",
        )
        .unwrap();

        assert_eq!(msg.sender_name, "builder-1");
        assert_eq!(msg.content, "taking src/api/");
        assert!(matches!(msg.channel_type, ChannelType::Project));

        let results = query_channel(
            &mut conn,
            &ChannelType::Project,
            Some("proj-1"),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, msg.id);
    }

    #[test]
    fn test_cursor_pagination() {
        let mut conn = test_diesel_conn();

        // Insert 3 messages with distinct timestamps for deterministic ordering
        let msg1 = insert_at(
            &mut conn,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some("run-1"),
            "builder-1",
            "first",
            1000,
        );

        let _msg2 = insert_at(
            &mut conn,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some("run-2"),
            "builder-2",
            "second",
            2000,
        );

        let msg3 = insert_at(
            &mut conn,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some("run-1"),
            "builder-1",
            "third",
            3000,
        );

        // Query after msg1 → should get msg2, msg3
        let after_first = query_channel(
            &mut conn,
            &ChannelType::Issue,
            Some("CRN/1"),
            None,
            Some(&msg1.id),
            None,
            None,
        )
        .unwrap();
        assert_eq!(after_first.len(), 2);
        assert_eq!(after_first[1].content, "third");

        // Query before msg3 → should get msg1, msg2
        let before_last = query_channel(
            &mut conn,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some(&msg3.id),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(before_last.len(), 2);
        assert_eq!(before_last[0].content, "first");
    }

    #[test]
    fn test_query_recent_for_run() {
        let mut conn = test_diesel_conn();

        // Project message
        insert_at(
            &mut conn,
            &ChannelType::Project,
            Some("proj-1"),
            None,
            "system",
            "PR merged",
            1000,
        );

        // Issue message (KEY/NUMBER format)
        insert_at(
            &mut conn,
            &ChannelType::Issue,
            Some("PROJ/1"),
            Some("run-1"),
            "builder-1",
            "taking api",
            2000,
        );

        // Different issue (should not appear)
        insert_at(
            &mut conn,
            &ChannelType::Issue,
            Some("PROJ/2"),
            Some("run-3"),
            "builder-3",
            "other issue",
            3000,
        );

        let results = query_recent_for_run(&mut conn, "proj-1", Some("PROJ/1"), None, 50).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].content, "PR merged");
        assert_eq!(results[1].content, "taking api");
    }

    #[test]
    fn test_system_message() {
        let mut conn = test_diesel_conn();

        // System messages now carry the run_id of the agent they describe
        let msg = insert_message(
            &mut conn,
            &ChannelType::Issue,
            Some("CRN/1"),
            Some("run-1"), // run_id of the agent this event is about
            "system",
            None,
            "builder-1 started",
        )
        .unwrap();

        assert_eq!(msg.sender_run_id.as_deref(), Some("run-1"));
        assert_eq!(msg.sender_name, "system");
    }

    #[test]
    fn test_query_recent_excludes_own_system_messages() {
        let mut conn = test_diesel_conn();

        // System message about run-1 (e.g. "builder-1 started working")
        insert_at(
            &mut conn,
            &ChannelType::Issue,
            Some("PROJ/1"),
            Some("run-1"), // this is the agent the message is about
            "system",
            "builder-1 started working",
            1000,
        );

        // System message about run-2 (a peer agent)
        insert_at(
            &mut conn,
            &ChannelType::Issue,
            Some("PROJ/1"),
            Some("run-2"),
            "system",
            "builder-2 started working",
            2000,
        );

        // Regular message from run-2
        insert_at(
            &mut conn,
            &ChannelType::Issue,
            Some("PROJ/1"),
            Some("run-2"),
            "builder-2",
            "taking src/api/",
            3000,
        );

        // Query as run-1: should NOT see own system message, but should see peer messages
        let results =
            query_recent_for_run(&mut conn, "proj-1", Some("PROJ/1"), Some("run-1"), 50).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].content, "builder-2 started working");
        assert_eq!(results[1].content, "taking src/api/");

        // Query without exclusion: should see all 3
        let all = query_recent_for_run(&mut conn, "proj-1", Some("PROJ/1"), None, 50).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_direct_message() {
        let mut conn = test_diesel_conn();

        let msg = insert_message(
            &mut conn,
            &ChannelType::Direct,
            None,
            Some("run-1"),
            "builder-1",
            Some("run-2"),
            "ready for handoff",
        )
        .unwrap();

        assert!(matches!(msg.channel_type, ChannelType::Direct));
        assert_eq!(msg.recipient_run_id.as_deref(), Some("run-2"));
    }
}
