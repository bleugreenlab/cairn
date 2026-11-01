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
    for push in pushes {
        items.push(push_delivery(db, push).await);
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

async fn push_delivery(db: &LocalDb, push: attention_push::Push) -> PendingDelivery {
    let prefix = push
        .key
        .split_once(':')
        .map(|(prefix, _)| prefix)
        .unwrap_or(&push.key);
    let (kind, headline) = attention_push::push_kind_headline(prefix);
    let mut item = PendingDelivery {
        id: push.id.clone(),
        source: PendingDeliverySource::Push,
        kind: kind.to_string(),
        headline: headline.to_string(),
        detail: None,
        uri: Some(push.content_ref.clone()),
        created_at: push.created_at,
    };
    if prefix != "direct" {
        return item;
    }
    let message = cairn_attention::delivery::direct_push_message(db, &push, |id| async move {
        crate::messages::db::get_message_by_id_async(db, &id)
            .await
            .ok()
            .flatten()
    })
    .await;
    // A direct push's `content_ref` is the recipient's own home, so without its
    // message the card can only fall back to naming the recipient.
    let Some(message) = message else {
        return item;
    };
    let sender = message.sender_name.trim();
    if sender.starts_with("cairn://") {
        // The chip beside the headline renders the sender's label and links to
        // it, completing the sentence; the URI label grammar lives in one place,
        // `formatResourceLabel` in `@cairn/ui`.
        item.headline = "Message from".to_string();
        item.uri = Some(sender.to_string());
    } else if !sender.is_empty() {
        item.headline = format!("Message from {sender}");
        item.uri = None;
    }
    let preview = cairn_attention::delivery::one_line_excerpt(&message.content);
    item.detail = (!preview.is_empty()).then_some(preview);
    item
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
              VALUES('p','w','Project','proj','/tmp/repo',1,1);
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
            "cairn://p/proj/1/1/builder/checks",
            attention_push::Wake::Wake,
            attention_push::Boundary::Event,
            "turn-checks:cairn://p/proj/1/1/builder/checks",
        )
        .await
        .unwrap();

        let items = list_pending_deliveries(&db, "job-1").await.unwrap();

        assert_eq!(items.len(), 1, "{items:?}");
        assert_eq!(items[0].kind, "checks");
    }

    async fn seeded_direct_db(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','proj','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                               created_at, updated_at)
              VALUES('i','p',1,'Issue','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
              VALUES('job-1','p','i','running',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
              VALUES('run-1','p','job-1','i',1,1);
            ",
        )
        .await
        .unwrap();
        db
    }

    /// A direct push links to its recipient's own home, so a card built from
    /// the push alone named the reader rather than the writer.
    #[tokio::test]
    async fn a_direct_message_card_names_and_links_its_sender() {
        let db = seeded_direct_db("pending-direct-sender.db").await;
        db.execute(
            "INSERT INTO messages (id, channel_type, channel_id, sender_run_id, sender_name,
                                   recipient_run_id, content, created_at, urgency)
             VALUES ('msg-1', 'direct', NULL, NULL, 'cairn://p/proj/2/1/planner', 'run-1',
                     'Rebase onto main
  before review.', 1, 'queue')",
            (),
        )
        .await
        .unwrap();
        attention_push::push(
            &db,
            "job-1",
            "cairn://p/proj/1/1/builder",
            attention_push::Wake::Wake,
            attention_push::Boundary::Event,
            &attention_push::direct_push_key("msg-1", None),
        )
        .await
        .unwrap();

        let items = list_pending_deliveries(&db, "job-1").await.unwrap();

        assert_eq!(items.len(), 1, "{items:?}");
        assert_eq!(items[0].kind, "message");
        assert!(items[0].headline.starts_with("Message from"), "{items:?}");
        assert_eq!(items[0].uri.as_deref(), Some("cairn://p/proj/2/1/planner"));
        assert_eq!(
            items[0].detail.as_deref(),
            Some("Rebase onto main before review.")
        );
    }

    /// A coalescing system direct names its message only through the push
    /// fingerprint, and its sender is not a resource the card could link to.
    #[tokio::test]
    async fn a_keyed_system_direct_card_names_system_without_a_link() {
        let db = seeded_direct_db("pending-direct-system.db").await;
        db.write(|conn| {
            Box::pin(async move {
                crate::messages::delivery::insert_system_direct_push_with_key_conn(
                    conn,
                    "job-1",
                    "run-1",
                    "Your branch was reconciled.",
                    crate::messages::queued::DeliveryUrgency::Queue,
                    Some("direct:reconcile:job-1"),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let items = list_pending_deliveries(&db, "job-1").await.unwrap();

        assert_eq!(items.len(), 1, "{items:?}");
        assert_eq!(items[0].headline, "Message from system");
        assert_eq!(items[0].uri, None);
        assert_eq!(
            items[0].detail.as_deref(),
            Some("Your branch was reconciled.")
        );
    }

    /// A push whose message row is gone keeps today's card rather than failing
    /// the whole list.
    #[tokio::test]
    async fn a_direct_push_without_its_message_keeps_the_fallback_card() {
        let db = seeded_direct_db("pending-direct-missing.db").await;
        attention_push::push(
            &db,
            "job-1",
            "cairn://p/proj/1/1/builder",
            attention_push::Wake::Wake,
            attention_push::Boundary::Event,
            &attention_push::direct_push_key("gone", None),
        )
        .await
        .unwrap();

        let items = list_pending_deliveries(&db, "job-1").await.unwrap();

        assert_eq!(items.len(), 1, "{items:?}");
        assert_eq!(items[0].headline, "Direct message");
        assert_eq!(items[0].uri.as_deref(), Some("cairn://p/proj/1/1/builder"));
        assert_eq!(items[0].detail, None);
    }

    #[tokio::test]
    async fn stale_pending_deliveries_do_not_survive_a_reboot() {
        let db = crate::storage::migrated_test_db("pending-deliveries-reboot.db").await;
        let path = db.path().to_path_buf();
        let stale_created_at = chrono::Utc::now().timestamp() - 24 * 60 * 60 - 60;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','proj','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                               created_at, updated_at)
              VALUES('i','p',1,'Issue','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
              VALUES('job-1','p','i','running',1,1);
            ",
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO suppressed_wakes
               (id, subscription_id, job_id, source_kind, source_ref, fact_kind,
                occurrences, latest_detail_uri, content, created_at, updated_at, delivered_at)
             VALUES ('stale', NULL, 'job-1', 'issue', 'cairn://p/proj/1', 'message',
                     1, 'cairn://p/proj/1', 'old comment', ?1, ?1, NULL)",
            [stale_created_at],
        )
        .await
        .unwrap();
        drop(db);

        let reopened = LocalDb::open(path).await.unwrap();
        let items = list_pending_deliveries(&reopened, "job-1").await.unwrap();

        assert!(items.is_empty(), "{items:?}");
        let delivered_at = reopened
            .query_opt(
                "SELECT delivered_at FROM suppressed_wakes WHERE id = 'stale'",
                (),
                |row| Ok(row.get::<Option<i64>>(0)?),
            )
            .await
            .unwrap()
            .flatten();
        assert_eq!(delivered_at, None);
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
