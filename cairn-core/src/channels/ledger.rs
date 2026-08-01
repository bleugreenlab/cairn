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

pub async fn record_answer(db: &LocalDb, id: &str, answer: &str) -> Result<bool, String> {
    let answer_json = serde_json::json!({ "answer": answer }).to_string();
    db.execute(
        "UPDATE channel_outbound SET options_json = ?2 WHERE id = ?1 AND status = 'sent'",
        params![id.to_string(), answer_json],
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
                "SELECT binding_ref, json_extract(options_json, '$.answer') FROM channel_outbound WHERE channel = ?1 AND kind = 'question' AND binding_ref LIKE ?2 AND status = 'resolved' ORDER BY binding_ref",
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
}
