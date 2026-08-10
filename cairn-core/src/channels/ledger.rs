use cairn_db::turso::{params, Row};

use crate::storage::{DbResult, LocalDb, RowExt};

const OUTBOUND_COLUMNS: &str = "id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering, options_json, status, provider_guid, caption_guid, created_at, sent_at, resolved_at, last_error";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOutbound<'a> {
    pub id: &'a str,
    pub channel: &'a str,
    pub kind: &'a str,
    pub binding_ref: &'a str,
    pub conversation: &'a str,
    pub job_id: Option<&'a str>,
    pub rendered_text: &'a str,
    pub rendering: &'a str,
    pub created_at: i64,
}

/// Claim the only provider attempt for an intent before crossing the external
/// side-effect boundary. `failed` is the durable ambiguous-outcome state: if the
/// process dies after the provider sends but before `mark_sent`, later sweeps do
/// not put the same intent on the wire again.
pub async fn begin_delivery(db: &LocalDb, id: &str) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'failed', last_error = 'delivery outcome unknown' WHERE id = ?1 AND status = 'pending'",
        (id.to_string(),),
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn set_pending_route_submission(
    db: &LocalDb,
    id: &str,
    submission_json: &str,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET options_json = ?2 WHERE id = ?1 AND kind = 'route' AND status = 'pending'",
        params![id.to_string(), submission_json.to_string()],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

/// The durable follow tables are named `channel_thread_follow` and
/// `channel_thread_focus`, from before threads were an entity of their own. What
/// they hold is a FOLLOW TARGET's URI — a thread or an issue node — so the Rust
/// surface says target; the column names stay put because renaming private
/// channel state buys nothing a comment cannot.
pub async fn is_target_followed(db: &LocalDb, channel: &str, uri: &str) -> Result<bool, String> {
    db.query_opt_i64(
        "SELECT 1 FROM channel_thread_follow WHERE channel = ?1 AND thread_uri = ?2",
        params![channel.to_string(), uri.to_string()],
    )
    .await
    .map(|value| value.is_some())
    .map_err(|error| error.to_string())
}

/// Claims provider cleanup for any sent outbound bubble. Question cleanup uses
/// the stricter helper above because it also carries answer-resolution meaning;
/// thread-update retraction only needs the exactly-once side-effect claim.
pub async fn claim_outbound_cleanup(
    db: &LocalDb,
    id: &str,
    resolved_at: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'resolved', resolved_at = ?2, last_error = NULL WHERE id = ?1 AND status = 'sent'",
        params![id.to_string(), resolved_at],
    ).await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

pub async fn advance_follow_cursor(
    db: &LocalDb,
    channel: &str,
    uri: &str,
    cursor_rowid: i64,
) -> Result<(), String> {
    db.execute(
        "UPDATE channel_thread_follow SET cursor_rowid = MAX(cursor_rowid, ?3) WHERE channel = ?1 AND thread_uri = ?2",
        params![channel.to_string(), uri.to_string(), cursor_rowid.max(0)],
    ).await.map(|_| ()).map_err(|error| error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Follow {
    pub uri: String,
    pub followed_at: i64,
    pub cursor_rowid: i64,
}

pub async fn follow_target(
    db: &LocalDb,
    channel: &str,
    uri: &str,
    followed_at: i64,
    cursor_rowid: i64,
) -> Result<bool, String> {
    db.execute(
        "INSERT OR IGNORE INTO channel_thread_follow (channel, thread_uri, followed_at, cursor_rowid) VALUES (?1, ?2, ?3, ?4)",
        params![channel.to_string(), uri.to_string(), followed_at, cursor_rowid.max(0)],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

/// Move a follow (and the focus, when it points there) onto the canonical URI
/// for the same target, merging with a canonical row that already exists.
///
/// A follow's URI is its identity everywhere downstream: it is the primary key
/// here, the poll's checkmark lookup, and the prefix of the `binding_ref` that
/// makes stream delivery once-only. So an alias and its canonical form are two
/// identities for one conversation — the poll shows the target unfollowed while
/// it is streaming, following it again adds a second row, and every update then
/// arrives twice under two distinct binding refs. Canonicalizing the row is what
/// keeps one target to one identity.
///
/// The merged cursor is the LATER of the two. Events between the two cursors
/// were already delivered under the row that had read further, so taking the
/// maximum leaves no gap and repeats nothing; the earlier `followed_at` is kept
/// because that is when the operator actually started following.
pub async fn canonicalize_follow(
    db: &LocalDb,
    channel: &str,
    from_uri: &str,
    to_uri: &str,
) -> Result<(), String> {
    let channel = channel.to_string();
    let from_uri = from_uri.to_string();
    let to_uri = to_uri.to_string();
    db.write(move |conn| {
        let channel = channel.clone();
        let from_uri = from_uri.clone();
        let to_uri = to_uri.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO channel_thread_follow (channel, thread_uri, followed_at, cursor_rowid)
                 SELECT ?1, ?3, followed_at, cursor_rowid FROM channel_thread_follow
                 WHERE channel = ?1 AND thread_uri = ?2
                 ON CONFLICT(channel, thread_uri) DO UPDATE SET
                   cursor_rowid = MAX(cursor_rowid, excluded.cursor_rowid),
                   followed_at = MIN(followed_at, excluded.followed_at)",
                params![channel.as_str(), from_uri.as_str(), to_uri.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM channel_thread_follow WHERE channel = ?1 AND thread_uri = ?2",
                params![channel.as_str(), from_uri.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE channel_thread_focus SET thread_uri = ?3 WHERE channel = ?1 AND thread_uri = ?2",
                params![channel.as_str(), from_uri.as_str(), to_uri.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

pub async fn unfollow_target(db: &LocalDb, channel: &str, uri: &str) -> Result<bool, String> {
    db.execute(
        "DELETE FROM channel_thread_follow WHERE channel = ?1 AND thread_uri = ?2",
        params![channel.to_string(), uri.to_string()],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn list_follows(db: &LocalDb, channel: &str) -> Result<Vec<Follow>, String> {
    db.query_all(
        "SELECT thread_uri, followed_at, cursor_rowid FROM channel_thread_follow WHERE channel = ?1 ORDER BY followed_at DESC",
        params![channel.to_string()],
        |row| Ok(Follow { uri: row.text(0)?, followed_at: row.i64(1)?, cursor_rowid: row.i64(2)? }),
    ).await.map_err(|error| error.to_string())
}

pub async fn set_focus(
    db: &LocalDb,
    channel: &str,
    uri: &str,
    selected_at: i64,
) -> Result<(), String> {
    db.execute(
        "INSERT INTO channel_thread_focus (channel, thread_uri, selected_at) VALUES (?1, ?2, ?3) ON CONFLICT(channel) DO UPDATE SET thread_uri = excluded.thread_uri, selected_at = excluded.selected_at",
        params![channel.to_string(), uri.to_string(), selected_at],
    ).await.map(|_| ()).map_err(|error| error.to_string())
}

pub async fn get_focus(db: &LocalDb, channel: &str) -> Result<Option<String>, String> {
    db.query_opt(
        "SELECT thread_uri FROM channel_thread_focus WHERE channel = ?1",
        params![channel.to_string()],
        |row| row.text(0),
    )
    .await
    .map_err(|error| error.to_string())
}

/// Preserves an inbound answer when a simultaneous live-snapshot cleanup claimed
/// resolution first. Only the first inbound answer may fill the resolved row.
pub async fn record_answer_after_cleanup_claim(
    db: &LocalDb,
    id: &str,
    answer: &str,
) -> Result<bool, String> {
    let answer_json = serde_json::json!({ "answer": answer }).to_string();
    db.execute(
        "UPDATE channel_outbound SET options_json = ?2 WHERE id = ?1 AND kind = 'question' AND status = 'resolved' AND json_extract(options_json, '$.answer') IS NULL",
        params![id.to_string(), answer_json],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

/// Claims ownership of question cleanup while recording its durable resolution.
/// Only the path that observes `sent` may perform provider side effects.
pub async fn claim_question_cleanup(
    db: &LocalDb,
    id: &str,
    resolved_at: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'resolved', resolved_at = ?2, last_error = NULL WHERE id = ?1 AND kind = 'question' AND status = 'sent'",
        params![id.to_string(), resolved_at],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn mark_expired(db: &LocalDb, id: &str, expired_at: i64) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'expired', resolved_at = ?2 WHERE id = ?1 AND status = 'pending'",
        params![id.to_string(), expired_at],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundRecord {
    pub id: String,
    pub channel: String,
    pub kind: String,
    pub binding_ref: String,
    pub conversation: String,
    pub job_id: Option<String>,
    pub rendered_text: String,
    pub rendering: String,
    pub options_json: Option<String>,
    pub status: String,
    pub provider_guid: Option<String>,
    pub caption_guid: Option<String>,
    pub created_at: i64,
    pub sent_at: Option<i64>,
    pub resolved_at: Option<i64>,
    pub last_error: Option<String>,
}

impl OutboundRecord {
    fn from_row(row: &Row) -> DbResult<Self> {
        Ok(Self {
            id: row.text(0)?,
            channel: row.text(1)?,
            kind: row.text(2)?,
            binding_ref: row.text(3)?,
            conversation: row.text(4)?,
            job_id: row.opt_text(5)?,
            rendered_text: row.text(6)?,
            rendering: row.text(7)?,
            options_json: row.opt_text(8)?,
            status: row.text(9)?,
            provider_guid: row.opt_text(10)?,
            caption_guid: row.opt_text(11)?,
            created_at: row.i64(12)?,
            sent_at: row.opt_i64(13)?,
            resolved_at: row.opt_i64(14)?,
            last_error: row.opt_text(15)?,
        })
    }
}

/// Insert a pending delivery intent. Returns `false` when the channel/kind/binding
/// uniqueness fence proves the gate was already observed by an earlier sweep.
pub async fn insert_intent(db: &LocalDb, intent: &NewOutbound<'_>) -> Result<bool, String> {
    let intent = NewOutbound {
        id: intent.id,
        channel: intent.channel,
        kind: intent.kind,
        binding_ref: intent.binding_ref,
        conversation: intent.conversation,
        job_id: intent.job_id,
        rendered_text: intent.rendered_text,
        rendering: intent.rendering,
        created_at: intent.created_at,
    };
    db.execute(
        "INSERT OR IGNORE INTO channel_outbound (id, channel, kind, binding_ref, conversation, job_id, rendered_text, rendering, status, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'pending', ?9)",
        params![intent.id, intent.channel, intent.kind, intent.binding_ref, intent.conversation, intent.job_id, intent.rendered_text, intent.rendering, intent.created_at],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn get_by_binding(
    db: &LocalDb,
    channel: &str,
    kind: &str,
    binding_ref: &str,
) -> Result<Option<OutboundRecord>, String> {
    db.query_opt(
        format!("SELECT {OUTBOUND_COLUMNS} FROM channel_outbound WHERE channel = ?1 AND kind = ?2 AND binding_ref = ?3"),
        params![channel.to_string(), kind.to_string(), binding_ref.to_string()],
        OutboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

/// Resolve either identifier a provider may expose as the reply target.
pub async fn get_by_provider_guid(
    db: &LocalDb,
    channel: &str,
    guid: &str,
) -> Result<Option<OutboundRecord>, String> {
    db.query_opt(
        format!("SELECT {OUTBOUND_COLUMNS} FROM channel_outbound WHERE channel = ?1 AND (provider_guid = ?2 OR caption_guid = ?2)"),
        params![channel.to_string(), guid.to_string()],
        OutboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

pub async fn list_unresolved(db: &LocalDb, channel: &str) -> Result<Vec<OutboundRecord>, String> {
    db.query_all(
        format!("SELECT {OUTBOUND_COLUMNS} FROM channel_outbound WHERE channel = ?1 AND status IN ('pending', 'sent', 'failed') ORDER BY created_at"),
        params![channel.to_string()],
        OutboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

pub async fn mark_sent(
    db: &LocalDb,
    id: &str,
    provider_guid: &str,
    caption_guid: Option<&str>,
    options_json: Option<&str>,
    sent_at: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'sent', provider_guid = ?2, caption_guid = ?3, options_json = ?4, sent_at = ?5, last_error = NULL WHERE id = ?1 AND status IN ('pending', 'failed')",
        params![id.to_string(), provider_guid.to_string(), caption_guid.map(str::to_string), options_json.map(str::to_string), sent_at],
    ).await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

pub async fn update_options(db: &LocalDb, id: &str, options_json: &str) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_outbound SET options_json = ?2 WHERE id = ?1 AND status = 'sent'",
        params![id.to_string(), options_json.to_string()],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn claim_question_answer(
    db: &LocalDb,
    id: &str,
    answer: &str,
    resolved_at: i64,
) -> Result<bool, String> {
    let answer_json = serde_json::json!({ "answer": answer }).to_string();
    db.execute(
        "UPDATE channel_outbound SET options_json = ?2, status = 'resolved', resolved_at = ?3, last_error = NULL WHERE id = ?1 AND kind = 'question' AND status = 'sent'",
        params![id.to_string(), answer_json, resolved_at],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

pub async fn answered_for_prompt(
    db: &LocalDb,
    channel: &str,
    prompt_id: &str,
) -> Result<Vec<(usize, String)>, String> {
    let prefix = format!("{prompt_id}:%");
    db.read(|conn| {
        let channel = channel.to_string();
        let prefix = prefix.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT binding_ref, json_extract(options_json, '$.answer') FROM channel_outbound WHERE channel = ?1 AND kind = 'question' AND binding_ref LIKE ?2 AND status = 'resolved' AND json_extract(options_json, '$.answer') IS NOT NULL ORDER BY binding_ref",
                params![channel, prefix],
            ).await?;
            let mut answers = Vec::new();
            while let Some(row) = rows.next().await? {
                let binding = row.text(0)?;
                let index = binding.rsplit_once(':').and_then(|(_, value)| value.parse().ok())
                    .ok_or_else(|| crate::storage::DbError::Row(format!("invalid question binding: {binding}")))?;
                answers.push((index, row.text(1)?));
            }
            Ok(answers)
        })
    }).await.map_err(|error| error.to_string())
}

pub async fn mark_resolved(db: &LocalDb, id: &str, resolved_at: i64) -> Result<bool, String> {
    db.execute("UPDATE channel_outbound SET status = 'resolved', resolved_at = ?2, last_error = NULL WHERE id = ?1 AND status != 'resolved'", params![id.to_string(), resolved_at])
        .await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

/// Abandons every intent this channel never delivered. The channel is a live tap
/// on the ask event, so an intent that had not reached the provider by the time
/// the session ended is dead rather than owed: re-sending it after a restart
/// texts the operator prompts their agents have long since moved past. `sent`
/// rows are deliberately left alone, because a reply to an ask that DID go out
/// still binds to its provider GUID however long the operator takes.
pub async fn expire_undelivered(
    db: &LocalDb,
    channel: &str,
    expired_at: i64,
) -> Result<u64, String> {
    db.execute(
        "UPDATE channel_outbound SET status = 'expired', resolved_at = ?2 WHERE channel = ?1 AND status IN ('pending', 'failed')",
        params![channel.to_string(), expired_at],
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn mark_failed(db: &LocalDb, id: &str, error: &str) -> Result<bool, String> {
    db.execute("UPDATE channel_outbound SET status = 'failed', last_error = ?2 WHERE id = ?1 AND status != 'resolved'", params![id.to_string(), error.to_string()])
        .await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

pub async fn get_cursor(db: &LocalDb, channel: &str) -> Result<Option<i64>, String> {
    db.query_opt_i64(
        "SELECT since_rowid FROM channel_cursor WHERE channel = ?1",
        params![channel.to_string()],
    )
    .await
    .map_err(|error| error.to_string())
}

/// Advance a cursor monotonically so duplicate or reordered watch events cannot
/// move restart recovery backwards.
pub async fn advance_cursor(db: &LocalDb, channel: &str, since_rowid: i64) -> Result<i64, String> {
    db.execute(
        "INSERT INTO channel_cursor (channel, since_rowid) VALUES (?1, ?2) ON CONFLICT(channel) DO UPDATE SET since_rowid = MAX(channel_cursor.since_rowid, excluded.since_rowid)",
        params![channel.to_string(), since_rowid],
    ).await.map_err(|error| error.to_string())?;
    get_cursor(db, channel)
        .await?
        .ok_or_else(|| "cursor upsert returned no row".to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundRecord {
    pub id: String,
    pub channel: String,
    pub provider_guid: Option<String>,
    pub sender: String,
    pub text: String,
    pub received_at: i64,
    pub acknowledged_at: Option<i64>,
}

impl InboundRecord {
    fn from_row(row: &Row) -> DbResult<Self> {
        Ok(Self {
            id: row.text(0)?,
            channel: row.text(1)?,
            provider_guid: row.opt_text(2)?,
            sender: row.text(3)?,
            text: row.text(4)?,
            received_at: row.i64(5)?,
            acknowledged_at: row.opt_i64(6)?,
        })
    }
}

pub async fn insert_inbound(db: &LocalDb, record: &InboundRecord) -> Result<bool, String> {
    db.execute(
        "INSERT OR IGNORE INTO channel_inbound (id, channel, provider_guid, sender, text, received_at, acknowledged_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![record.id.clone(), record.channel.clone(), record.provider_guid.clone(), record.sender.clone(), record.text.clone(), record.received_at, record.acknowledged_at],
    ).await.map(|changed| changed == 1).map_err(|error| error.to_string())
}

pub async fn list_inbound(
    db: &LocalDb,
    channel: &str,
    limit: i64,
) -> Result<Vec<InboundRecord>, String> {
    db.query_all(
        "SELECT id, channel, provider_guid, sender, text, received_at, acknowledged_at FROM channel_inbound WHERE channel = ?1 ORDER BY received_at DESC LIMIT ?2",
        params![channel.to_string(), limit.max(0)],
        InboundRecord::from_row,
    ).await.map_err(|error| error.to_string())
}

pub async fn mark_inbound_acknowledged(
    db: &LocalDb,
    id: &str,
    acknowledged_at: i64,
) -> Result<bool, String> {
    db.execute(
        "UPDATE channel_inbound SET acknowledged_at = COALESCE(acknowledged_at, ?2) WHERE id = ?1",
        params![id.to_string(), acknowledged_at],
    )
    .await
    .map(|changed| changed == 1)
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;

    #[tokio::test]
    async fn followed_state_reflects_follow_and_unfollow() {
        let db = migrated_test_db("channel-ledger-follow-state.db").await;
        let thread = "cairn://p/CAIRN/3404";

        assert!(!is_target_followed(&db, "imessage", thread).await.unwrap());
        assert!(follow_target(&db, "imessage", thread, 10, 20)
            .await
            .unwrap());
        assert!(is_target_followed(&db, "imessage", thread).await.unwrap());
        assert!(unfollow_target(&db, "imessage", thread).await.unwrap());
        assert!(!is_target_followed(&db, "imessage", thread).await.unwrap());
    }

    fn intent<'a>(id: &'a str) -> NewOutbound<'a> {
        NewOutbound {
            id,
            channel: "imessage",
            kind: "question",
            binding_ref: "prompt-1:0",
            conversation: "+15551234567",
            job_id: Some("job-1"),
            rendered_text: "Choose",
            rendering: "poll",
            created_at: 10,
        }
    }

    #[tokio::test]
    async fn only_one_resolver_can_claim_question_cleanup() {
        let db = migrated_test_db("channel-ledger-cleanup-claim.db").await;
        assert!(insert_intent(&db, &intent("question")).await.unwrap());
        assert!(mark_sent(&db, "question", "guid", None, None, 11)
            .await
            .unwrap());

        let (first, second) = tokio::join!(
            claim_question_cleanup(&db, "question", 12),
            claim_question_cleanup(&db, "question", 12),
        );

        assert_ne!(first.unwrap(), second.unwrap());
    }

    #[tokio::test]
    async fn prompt_answers_ignore_cleanup_only_resolved_siblings() {
        let db = migrated_test_db("channel-ledger-mixed-question-resolution.db").await;
        let mut answered = intent("answered");
        answered.binding_ref = "prompt-1:0";
        let mut cleanup_only = intent("cleanup-only");
        cleanup_only.binding_ref = "prompt-1:1";
        assert!(insert_intent(&db, &answered).await.unwrap());
        assert!(insert_intent(&db, &cleanup_only).await.unwrap());
        assert!(mark_sent(&db, "answered", "guid-a", None, None, 11)
            .await
            .unwrap());
        assert!(mark_sent(&db, "cleanup-only", "guid-b", None, None, 11)
            .await
            .unwrap());
        assert!(claim_question_answer(&db, "answered", "Ship it", 12)
            .await
            .unwrap());
        assert!(claim_question_cleanup(&db, "cleanup-only", 12)
            .await
            .unwrap());

        assert_eq!(
            answered_for_prompt(&db, "imessage", "prompt-1")
                .await
                .unwrap(),
            vec![(0, "Ship it".into())]
        );
    }

    #[tokio::test]
    async fn intent_fence_and_lifecycle_are_durable() {
        let db = migrated_test_db("channel-ledger-lifecycle.db").await;
        assert!(insert_intent(&db, &intent("first")).await.unwrap());
        assert!(!insert_intent(&db, &intent("duplicate")).await.unwrap());
        assert!(mark_sent(
            &db,
            "first",
            "poll-guid",
            Some("caption-guid"),
            Some("{}"),
            11
        )
        .await
        .unwrap());
        assert_eq!(
            get_by_provider_guid(&db, "imessage", "caption-guid")
                .await
                .unwrap()
                .unwrap()
                .id,
            "first"
        );
        assert!(mark_resolved(&db, "first", 12).await.unwrap());
        assert!(list_unresolved(&db, "imessage").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn expiry_abandons_undelivered_intents_and_spares_delivered_ones() {
        let db = migrated_test_db("channel-ledger-expiry.db").await;
        for (id, binding) in [
            ("never-sent", "prompt-1:0"),
            ("send-errored", "prompt-2:0"),
            ("on-the-phone", "prompt-3:0"),
            ("answered", "prompt-4:0"),
        ] {
            let mut intent = intent(id);
            intent.binding_ref = binding;
            assert!(insert_intent(&db, &intent).await.unwrap());
        }
        assert!(mark_failed(&db, "send-errored", "executor offline")
            .await
            .unwrap());
        assert!(mark_sent(&db, "on-the-phone", "guid-a", None, None, 11)
            .await
            .unwrap());
        assert!(mark_sent(&db, "answered", "guid-b", None, None, 11)
            .await
            .unwrap());
        assert!(mark_resolved(&db, "answered", 12).await.unwrap());

        assert_eq!(expire_undelivered(&db, "imessage", 20).await.unwrap(), 2);

        let unresolved = list_unresolved(&db, "imessage").await.unwrap();
        assert_eq!(
            unresolved
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            vec!["on-the-phone"],
            "only an ask that actually reached the phone stays live"
        );
        // An expired intent keeps its fence, so the sweep can never resurrect it.
        assert!(!insert_intent(&db, &intent("replay")).await.unwrap());
        // A reply to an ask that did go out still binds after the session ends.
        assert_eq!(
            get_by_provider_guid(&db, "imessage", "guid-a")
                .await
                .unwrap()
                .unwrap()
                .id,
            "on-the-phone"
        );
    }

    #[tokio::test]
    async fn cursor_never_regresses_and_inbound_provider_guid_deduplicates() {
        let db = migrated_test_db("channel-ledger-inbound.db").await;
        assert_eq!(advance_cursor(&db, "imessage", 42).await.unwrap(), 42);
        assert_eq!(advance_cursor(&db, "imessage", 9).await.unwrap(), 42);
        let inbound = InboundRecord {
            id: "in-1".into(),
            channel: "imessage".into(),
            provider_guid: Some("message-guid".into()),
            sender: "+15551234567".into(),
            text: "hello".into(),
            received_at: 20,
            acknowledged_at: None,
        };
        assert!(insert_inbound(&db, &inbound).await.unwrap());
        let mut duplicate = inbound.clone();
        duplicate.id = "in-2".into();
        assert!(!insert_inbound(&db, &duplicate).await.unwrap());
        assert!(mark_inbound_acknowledged(&db, "in-1", 21).await.unwrap());
        assert_eq!(
            list_inbound(&db, "imessage", 10).await.unwrap()[0].acknowledged_at,
            Some(21)
        );
    }

    #[tokio::test]
    async fn thread_focus_tracks_the_most_recent_selection_and_follows_survive() {
        let db = migrated_test_db("channel-ledger-thread-focus.db").await;
        assert!(follow_target(&db, "imessage", "cairn://p/CAIRN/1", 10, 42)
            .await
            .unwrap());
        set_focus(&db, "imessage", "cairn://p/CAIRN/1", 10)
            .await
            .unwrap();
        set_focus(&db, "imessage", "cairn://p/CAIRN/2", 11)
            .await
            .unwrap();
        assert_eq!(
            get_focus(&db, "imessage").await.unwrap().as_deref(),
            Some("cairn://p/CAIRN/2")
        );
        assert_eq!(
            list_follows(&db, "imessage").await.unwrap()[0].cursor_rowid,
            42
        );
    }

    #[tokio::test]
    async fn restart_rebases_surviving_follow_to_the_current_live_edge() {
        let db = migrated_test_db("channel-ledger-thread-restart.db").await;
        assert!(follow_target(&db, "imessage", "cairn://p/CAIRN/1", 10, 5)
            .await
            .unwrap());

        advance_follow_cursor(&db, "imessage", "cairn://p/CAIRN/1", 99)
            .await
            .unwrap();

        assert_eq!(
            list_follows(&db, "imessage").await.unwrap()[0].cursor_rowid,
            99,
            "events accumulated before the restarted session's live edge are skipped"
        );
    }

    #[tokio::test]
    async fn refollow_starts_at_the_new_live_edge_instead_of_replaying_backlog() {
        let db = migrated_test_db("channel-ledger-thread-refollow.db").await;
        assert!(follow_target(&db, "imessage", "cairn://p/CAIRN/1", 10, 5)
            .await
            .unwrap());
        assert!(unfollow_target(&db, "imessage", "cairn://p/CAIRN/1")
            .await
            .unwrap());
        assert!(follow_target(&db, "imessage", "cairn://p/CAIRN/1", 20, 99)
            .await
            .unwrap());
        assert_eq!(
            list_follows(&db, "imessage").await.unwrap()[0].cursor_rowid,
            99
        );
    }
}
