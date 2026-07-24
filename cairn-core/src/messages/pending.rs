//! Unified read/dismiss surface for system-originated content pending delivery
//! to a job's agent.

use serde::{Deserialize, Serialize};

use crate::messages::{delivery::node_uri_for_job, side_channel};
use crate::orchestrator::{attention_push, session};
use crate::storage::LocalDb;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingDeliverySource {
    Push,
    SideChannel,
    Channel,
}

impl PendingDeliverySource {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "push" => Ok(Self::Push),
            "side_channel" => Ok(Self::SideChannel),
            "channel" => Ok(Self::Channel),
            other => Err(format!("invalid pending delivery source: {other}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingDelivery {
    id: String,
    source: PendingDeliverySource,
    kind: String,
    headline: String,
    detail: Option<String>,
    uri: Option<String>,
    created_at: i64,
}

pub async fn list_pending_deliveries(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<PendingDelivery>, String> {
    let recipient = node_uri_for_job(db, job_id).await;
    let pushes = async {
        match recipient.as_deref() {
            Some(recipient) => attention_push::list_pending_live(db, recipient)
                .await
                .map_err(|error| error.to_string()),
            None => Ok(Vec::new()),
        }
    };
    let notices = side_channel::peek_pending_side_channel_for_job_async(db, job_id);
    let channel_messages = async {
        session::pending_channel_messages_for_job(db, job_id, 20)
            .await
            .map_err(|error| error.to_string())
    };
    let (pushes, notices, channel_messages) = tokio::try_join!(pushes, notices, channel_messages)?;

    let mut items = Vec::new();
    if recipient.is_some() {
        items.extend(pushes.into_iter().map(|push| {
            let prefix = push
                .key
                .split_once(':')
                .map(|(prefix, _)| prefix)
                .unwrap_or(&push.key);
            let (kind, headline) = attention_push::push_kind_headline(prefix);
            PendingDelivery {
                id: push.id,
                source: PendingDeliverySource::Push,
                kind: kind.to_string(),
                headline: headline.to_string(),
                detail: None,
                uri: Some(push.content_ref),
                created_at: push.created_at,
            }
        }));
    }

    items.extend(notices.into_iter().map(|notice| {
        let detail = notice.render();
        PendingDelivery {
            id: notice.id,
            source: PendingDeliverySource::SideChannel,
            kind: "side-channel".to_string(),
            headline: "Side-channel notice".to_string(),
            detail: Some(detail),
            uri: Some(notice.child_uri),
            created_at: notice.created_at,
        }
    }));

    items.extend(channel_messages.into_iter().map(|message| PendingDelivery {
        id: message.rowid.to_string(),
        source: PendingDeliverySource::Channel,
        kind: "channel".to_string(),
        headline: "Channel message".to_string(),
        detail: Some(format!("{}: {}", message.sender_name, message.content)),
        uri: None,
        created_at: message.created_at,
    }));

    items.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    Ok(items)
}

pub async fn dismiss_pending_delivery(
    db: &LocalDb,
    job_id: &str,
    id: &str,
    source: PendingDeliverySource,
) -> Result<(), String> {
    match source {
        PendingDeliverySource::Push => attention_push::delete_pending_by_id(db, id)
            .await
            .map_err(|error| error.to_string()),
        PendingDeliverySource::SideChannel => {
            side_channel::stamp_delivered_by_id_async(db, id).await
        }
        PendingDeliverySource::Channel => {
            let rowid = id
                .parse::<i64>()
                .map_err(|_| format!("invalid channel message id: {id}"))?;
            session::dismiss_channel_message_for_job(db, job_id, rowid)
                .await
                .map_err(|error| error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_parse_accepts_frontend_wire_values() {
        assert_eq!(
            PendingDeliverySource::parse("push").unwrap(),
            PendingDeliverySource::Push
        );
        assert_eq!(
            PendingDeliverySource::parse("side_channel").unwrap(),
            PendingDeliverySource::SideChannel
        );
        assert_eq!(
            PendingDeliverySource::parse("channel").unwrap(),
            PendingDeliverySource::Channel
        );
        assert!(PendingDeliverySource::parse("queued_message").is_err());
    }
}
