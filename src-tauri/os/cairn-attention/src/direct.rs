use crate::storage::DbError;
use cairn_db::lineage::home_uri_for_job_conn as node_uri_for_job_conn;
use cairn_db::turso::params;
use cairn_domain::DeliveryUrgency;

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
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
    recipient_job_id: &str,
    recipient_run_id: &str,
    content: &str,
    urgency: DeliveryUrgency,
    push_key: Option<&str>,
) -> Result<(String, crate::push::Wake), DbError> {
    use crate::push::Boundary;

    let now = chrono::Utc::now().timestamp();
    let message_id = uuid::Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO messages (
            id, channel_type, channel_id, sender_run_id, sender_name,
            recipient_run_id, content, created_at
         ) VALUES (?1, 'direct', NULL, NULL, 'system', ?2, ?3, ?4)",
        params![message_id.as_str(), recipient_run_id, content, now],
    )
    .await?;

    let wake = direct_wake_for_urgency(urgency);
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
