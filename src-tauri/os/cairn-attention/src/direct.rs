use std::collections::HashMap;

use crate::storage::{DbError, RowExt};
use cairn_db::lineage::home_uri_for_job_conn as node_uri_for_job_conn;
use cairn_db::turso::{params, Value};
use cairn_domain::{DeliveryUrgency, MessageRecipient, Wake};

/// One addressee of a direct message being written.
#[derive(Debug, Clone)]
pub struct NewRecipient {
    /// The addressed session, node or task job: the durable addressee.
    pub job_id: String,
    /// The job's latest run at send, kept as provenance only.
    pub run_id_at_send: Option<String>,
    /// What this recipient's own mute made of the sender's urgency.
    pub wake: Option<Wake>,
}

/// A direct message being written: one `messages` row and one
/// `message_recipients` row per addressee, in position order.
#[derive(Debug, Clone)]
pub struct NewDirect {
    pub id: String,
    pub sender_run_id: Option<String>,
    pub sender_name: String,
    pub content: String,
    pub urgency: Option<DeliveryUrgency>,
    pub created_at: i64,
    pub recipients: Vec<NewRecipient>,
}

impl NewDirect {
    /// A system-sent direct to one recipient, at `now`. The recipient is its
    /// job; `recipient_run_id` is provenance, absent for a job with no run yet.
    #[allow(clippy::too_many_arguments)]
    pub fn system_to_one(
        id: &str,
        sender_name: &str,
        content: &str,
        urgency: DeliveryUrgency,
        recipient_job_id: &str,
        recipient_run_id: Option<&str>,
        wake: Wake,
        now: i64,
    ) -> Self {
        NewDirect {
            id: id.to_string(),
            sender_run_id: None,
            sender_name: sender_name.to_string(),
            content: content.to_string(),
            urgency: Some(urgency),
            created_at: now,
            recipients: vec![NewRecipient {
                job_id: recipient_job_id.to_string(),
                run_id_at_send: recipient_run_id.map(str::to_string),
                wake: Some(wake),
            }],
        }
    }
}

/// What [`insert_direct_message_conn`] does when a message already has the id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnExisting {
    /// The insert fails, as a plain `INSERT` on a taken key does.
    Fail,
    /// Nothing is written and the existing row stands.
    Ignore,
    /// Nothing is written, and it is an error unless the existing message has
    /// the same sender, content, urgency and recipients.
    Verify,
}

/// The one writer of direct messages: the `messages` row and its recipient
/// rows, inside the caller's transaction. Returns whether the message was newly
/// written; recipients are written only with a new message, so a re-driven
/// write never doubles them.
pub async fn insert_direct_message_conn(
    conn: &cairn_db::storage::TrackedConnection,
    direct: &NewDirect,
    on_existing: OnExisting,
) -> Result<bool, DbError> {
    let verb = match on_existing {
        OnExisting::Fail => "INSERT",
        OnExisting::Ignore | OnExisting::Verify => "INSERT OR IGNORE",
    };
    let inserted = conn
        .execute(
            &format!(
                "{verb} INTO messages (
                    id, channel_type, channel_id, sender_run_id, sender_name,
                    content, created_at, urgency
                 ) VALUES (?1, 'direct', NULL, ?2, ?3, ?4, ?5, ?6)"
            ),
            params![
                direct.id.as_str(),
                direct.sender_run_id.as_deref(),
                direct.sender_name.as_str(),
                direct.content.as_str(),
                direct.created_at,
                direct.urgency.map(DeliveryUrgency::as_str)
            ],
        )
        .await?
        > 0;
    if !inserted {
        if on_existing == OnExisting::Verify {
            verify_existing_direct(conn, direct).await?;
        }
        return Ok(false);
    }
    for (position, recipient) in direct.recipients.iter().enumerate() {
        conn.execute(
            "INSERT INTO message_recipients
               (message_id, position, recipient_job_id, recipient_run_id, created_at, wake)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                direct.id.as_str(),
                position as i64,
                recipient.job_id.as_str(),
                recipient.run_id_at_send.as_deref(),
                direct.created_at,
                recipient.wake.map(Wake::as_str)
            ],
        )
        .await?;
    }
    Ok(true)
}

async fn verify_existing_direct(
    conn: &cairn_db::storage::TrackedConnection,
    direct: &NewDirect,
) -> Result<(), DbError> {
    let mut rows = conn
        .query(
            "SELECT channel_type, sender_run_id, sender_name, content, urgency
               FROM messages WHERE id = ?1",
            params![direct.id.as_str()],
        )
        .await?;
    let same_message = match rows.next().await? {
        Some(row) => {
            row.text(0)? == "direct"
                && row.opt_text(1)? == direct.sender_run_id
                && row.text(2)? == direct.sender_name
                && row.text(3)? == direct.content
                && row.opt_text(4)?.as_deref() == direct.urgency.map(DeliveryUrgency::as_str)
        }
        None => false,
    };
    drop(rows);
    let recorded = load_recipients_conn(conn, &[direct.id.as_str()]).await?;
    let recorded_jobs: Vec<Option<&str>> = recorded
        .get(&direct.id)
        .map(|recipients| {
            recipients
                .iter()
                .map(|recipient| recipient.job_id.as_deref())
                .collect()
        })
        .unwrap_or_default();
    let wanted_jobs: Vec<Option<&str>> = direct
        .recipients
        .iter()
        .map(|recipient| Some(recipient.job_id.as_str()))
        .collect();
    if same_message && recorded_jobs == wanted_jobs {
        Ok(())
    } else {
        Err(DbError::Row(format!(
            "message identity {} already exists with different content",
            direct.id
        )))
    }
}

/// The recipients of each message, in position order, with each recipient's
/// address derived from its job through the canonical home-URI resolver. A
/// message with no recipient rows (every channel message) is absent from the
/// map.
pub async fn load_recipients_conn(
    conn: &cairn_db::storage::TrackedConnection,
    message_ids: &[&str],
) -> Result<HashMap<String, Vec<MessageRecipient>>, DbError> {
    let mut by_message: HashMap<String, Vec<MessageRecipient>> = HashMap::new();
    let mut addresses: HashMap<String, Option<String>> = HashMap::new();
    for chunk in message_ids.chunks(cairn_db::sql::IN_LIST_CHUNK) {
        let binds: Vec<Value> = chunk.iter().map(|id| Value::Text((*id).into())).collect();
        let mut rows = conn.query(&recipients_sql(chunk.len()), binds).await?;
        let mut chunk_rows = Vec::new();
        while let Some(row) = rows.next().await? {
            chunk_rows.push((
                row.text(0)?,
                row.i64(1)?,
                row.opt_text(2)?,
                row.opt_text(3)?,
                row.opt_text(4)?,
            ));
        }
        drop(rows);
        // A page's few recipient rows are ordered here: an `ORDER BY` across
        // a bound id list costs the statement a sorter.
        chunk_rows.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        for (message_id, _, job_id, run_id, wake) in chunk_rows {
            let address = match job_id.as_deref() {
                Some(job) => match addresses.get(job) {
                    Some(address) => address.clone(),
                    None => {
                        let address = node_uri_for_job_conn(conn, job).await?;
                        addresses.insert(job.to_string(), address.clone());
                        address
                    }
                },
                None => None,
            };
            by_message
                .entry(message_id)
                .or_default()
                .push(MessageRecipient {
                    job_id,
                    address,
                    run_id_at_send: run_id,
                    wake: wake.as_deref().and_then(Wake::from_db),
                });
        }
    }
    Ok(by_message)
}

/// The recipient rows of `n` messages, each seeked by its primary key.
fn recipients_sql(n: usize) -> String {
    format!(
        "SELECT message_id, position, recipient_job_id, recipient_run_id, wake
           FROM message_recipients
          WHERE message_id IN ({})",
        cairn_db::sql::placeholders(n)
    )
}

pub fn direct_wake_for_urgency(urgency: DeliveryUrgency) -> crate::push::Wake {
    use crate::push::Wake;
    if urgency == DeliveryUrgency::Interrupt {
        Wake::Interrupt
    } else if urgency.wakes_idle() {
        Wake::Wake
    } else {
        Wake::Passive
    }
}

/// Insert a system direct message and its `direct:{message_id}` attention push in
/// one caller-owned transaction. This is the canonical durable shape for direct
/// delivery: the message row stores the frozen body, and the push row is what the
/// idle/event drain sees and stamps.
pub async fn insert_system_direct_push_conn(
    conn: &cairn_db::storage::TrackedConnection,
    recipient_job_id: &str,
    recipient_run_id: &str,
    content: &str,
    urgency: DeliveryUrgency,
) -> Result<(String, crate::push::Wake), DbError> {
    insert_system_direct_push_with_key_conn(
        conn,
        recipient_job_id,
        recipient_run_id,
        content,
        urgency,
        None,
    )
    .await
}

pub async fn insert_system_direct_push_with_key_conn(
    conn: &cairn_db::storage::TrackedConnection,
    recipient_job_id: &str,
    recipient_run_id: &str,
    content: &str,
    urgency: DeliveryUrgency,
    push_key: Option<&str>,
) -> Result<(String, crate::push::Wake), DbError> {
    let message_id = uuid::Uuid::new_v4().to_string();
    insert_system_direct_push_with_message_id_conn(
        conn,
        recipient_job_id,
        recipient_run_id,
        content,
        urgency,
        push_key,
        &message_id,
    )
    .await
}

/// Write one direct delivery idempotently inside the caller's transaction:
/// the `messages` row under `message_id` and its `direct:` push, each only if
/// absent. A settlement re-driven after a crash, or after a partial commit in
/// another database, writes nothing twice. Returns the push id and its wake.
#[allow(clippy::too_many_arguments)]
pub async fn settle_direct_conn(
    conn: &cairn_db::storage::TrackedConnection,
    recipient_job_id: &str,
    recipient_run_id: &str,
    sender_name: &str,
    content: &str,
    urgency: DeliveryUrgency,
    message_id: &str,
    operator_message_id: Option<&str>,
) -> Result<(String, crate::push::Wake), DbError> {
    use crate::push::Boundary;

    let now = chrono::Utc::now().timestamp();
    let wake = direct_wake_for_urgency(urgency);
    insert_direct_message_conn(
        conn,
        &NewDirect::system_to_one(
            message_id,
            sender_name,
            content,
            urgency,
            recipient_job_id,
            Some(recipient_run_id),
            wake,
            now,
        ),
        OnExisting::Ignore,
    )
    .await?;
    let key = crate::push::direct_push_key(message_id, operator_message_id);
    let mut rows = conn
        .query(
            "SELECT id FROM attention_pushes WHERE recipient = ?1 AND key = ?2 LIMIT 1",
            params![recipient_job_id, key.as_str()],
        )
        .await?;
    if let Some(row) = rows.next().await? {
        return Ok((cairn_db::storage::RowExt::text(&row, 0)?, wake));
    }
    drop(rows);
    let content_ref = node_uri_for_job_conn(conn, recipient_job_id)
        .await?
        .unwrap_or_else(|| recipient_job_id.to_string());
    let push_id = uuid::Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO attention_pushes
           (id, recipient, content_ref, wake, boundary, key, created_at, fingerprint)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            push_id.as_str(),
            recipient_job_id,
            content_ref.as_str(),
            wake.as_str(),
            Boundary::Event.as_str(),
            key.as_str(),
            now,
            message_id
        ],
    )
    .await?;
    Ok((push_id, wake))
}

/// [`insert_system_direct_push_with_key_conn`] with a caller-chosen message id,
/// for a caller whose message row is its own durable delivery record: the push
/// can be dismissed (deleted) while pending, the message row is not.
pub async fn insert_system_direct_push_with_message_id_conn(
    conn: &cairn_db::storage::TrackedConnection,
    recipient_job_id: &str,
    recipient_run_id: &str,
    content: &str,
    urgency: DeliveryUrgency,
    push_key: Option<&str>,
    message_id: &str,
) -> Result<(String, crate::push::Wake), DbError> {
    use crate::push::Boundary;

    let now = chrono::Utc::now().timestamp();
    let message_id = message_id.to_string();
    let wake = direct_wake_for_urgency(urgency);
    insert_direct_message_conn(
        conn,
        &NewDirect::system_to_one(
            &message_id,
            "system",
            content,
            urgency,
            recipient_job_id,
            Some(recipient_run_id),
            wake,
            now,
        ),
        OnExisting::Fail,
    )
    .await?;

    // A stable key coalesces superseded pushes, content_ref remains the recipient
    // home their card links to, and fingerprint names the newest frozen message.
    // Post-comment pushes use the same changing-referent convention.
    let content_ref = node_uri_for_job_conn(conn, recipient_job_id)
        .await?
        .unwrap_or_else(|| recipient_job_id.to_string());
    let push_id = uuid::Uuid::new_v4().to_string();
    let key = push_key
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("direct:{message_id}"));
    let updated = if push_key.is_some() {
        conn.execute(
            "UPDATE attention_pushes
                SET content_ref=?1, wake=?2, boundary=?3, created_at=?4, fingerprint=?5
              WHERE recipient=?6 AND key=?7
                AND delivered_event_id IS NULL AND retired_at IS NULL",
            params![
                content_ref.as_str(),
                wake.as_str(),
                Boundary::Event.as_str(),
                now,
                message_id.as_str(),
                recipient_job_id,
                key.as_str(),
            ],
        )
        .await?
    } else {
        0
    };
    if updated == 0 {
        conn.execute(
            "INSERT INTO attention_pushes
               (id, recipient, content_ref, wake, boundary, key, created_at, fingerprint)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                push_id.as_str(),
                recipient_job_id,
                content_ref.as_str(),
                wake.as_str(),
                Boundary::Event.as_str(),
                key.as_str(),
                now,
                message_id.as_str()
            ],
        )
        .await?;
    }

    Ok((message_id, wake))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Loading a page's recipients is one bound id list seeking the
    /// `message_recipients` primary key, with no sorter.
    #[tokio::test(flavor = "current_thread")]
    async fn recipients_load_through_the_primary_key() {
        let db = cairn_db::storage::migrated_test_db("message-recipients-plan.db").await;
        let sql = recipients_sql(3);
        let binds = ["a", "b", "c"]
            .into_iter()
            .map(|id| Value::Text(id.into()))
            .collect();
        let plan = cairn_db::storage::query_plan(&db, &sql, binds).await;
        assert!(
            plan.contains("sqlite_autoindex_message_recipients_1 (message_id=?)"),
            "{plan}"
        );
        assert!(!plan.contains("SORTER"), "{plan}");
    }
}
