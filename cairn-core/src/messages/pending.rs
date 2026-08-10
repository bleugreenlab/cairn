//! Unified read/dismiss surface for system-originated content pending delivery
//! to a job's agent.

use serde::{Deserialize, Serialize};

use crate::messages::side_channel;
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
    // A push's recipient is the watcher JOB id — `attention_pushes.recipient`
    // is a foreign key into `jobs`, and every creator passes a job id. Looking
    // it up by the node URI matched nothing, so this list never showed a single
    // pending push.
    let pushes = async {
        attention_push::list_pending_live(db, job_id)
            .await
            .map_err(|error| error.to_string())
    };
    let notices = side_channel::peek_pending_side_channel_for_job_async(db, job_id);
    let channel_messages = async {
        session::pending_channel_messages_for_job(db, job_id, 20)
            .await
            .map_err(|error| error.to_string())
    };
    let (pushes, notices, channel_messages) = tokio::try_join!(pushes, notices, channel_messages)?;

    let mut items = Vec::new();
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

    /// The listing addresses pushes the way the table stores them. Keyed by the
    /// node URI instead, every lookup missed and the surface silently reported
    /// that nothing was waiting.
    #[tokio::test]
    async fn pending_pushes_are_found_by_job_id() {
        let db = crate::storage::migrated_test_db("pending-deliveries.db").await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','PROJ','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                               created_at, updated_at)
              VALUES('i','p',1,'Issue','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
              VALUES('job-1','p','i','running',1,1);
            ",
        )
        .await
        .unwrap();
        attention_push::push(
            &db,
            "job-1",
            "cairn://p/PROJ/1/1/builder/checks",
            attention_push::Wake::Wake,
            attention_push::Boundary::Event,
            "turn-checks:cairn://p/PROJ/1/1/builder/checks",
        )
        .await
        .unwrap();

        let items = list_pending_deliveries(&db, "job-1").await.unwrap();

        assert_eq!(items.len(), 1, "{items:?}");
        assert_eq!(items[0].kind, "checks");
    }

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
