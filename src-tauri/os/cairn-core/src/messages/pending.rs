//! Unified read/dismiss surface for system-originated content pending delivery
//! to a job's agent.
//!
//! The frontend reads it through a per-job projection
//! (`generation_cache::PendingDeliveriesCache`): the runner rebuilds a job's list
//! when one of its input tables moves and announces `pending_deliveries` only
//! when the answer changed, so an open pane refetches on a change to its own
//! list rather than on every write anywhere (cairn/5619).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::db::DbState;
use crate::messages::side_channel;
use crate::orchestrator::{attention_push, session, Orchestrator};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// One job's list as its projection publishes it.
pub type PendingDeliveries = Arc<Vec<PendingDelivery>>;

impl PendingDelivery {
    /// When this item leaves the list with no write to say so: a side-channel
    /// notice is dropped once it is older than its delivery horizon.
    fn expires_at(&self) -> Option<i64> {
        (self.source == PendingDeliverySource::SideChannel)
            .then(|| self.created_at + crate::orchestrator::wakes::NOTICE_DELIVERY_HORIZON_SECS)
    }
}

/// Unix seconds at which a list next changes with time alone, if it ever does.
pub(crate) fn next_expiry(items: &[PendingDelivery]) -> Option<i64> {
    items.iter().filter_map(PendingDelivery::expires_at).min()
}

/// The projection's rebuild: one job's list, read from the database that owns
/// the job.
pub(crate) async fn rebuild_pending_deliveries(
    dbs: &DbState,
    job_id: &str,
) -> Result<Vec<PendingDelivery>, String> {
    let db = crate::execution::routing::owning_db_for_job(dbs, job_id)
        .await
        .map_err(|error| error.to_string())?;
    list_pending_deliveries(&db, job_id).await
}

/// A job's pending deliveries, served from its projection.
pub async fn pending_deliveries_for_job(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<PendingDeliveries, String> {
    orch.pending_deliveries_cache.get(job_id).await
}

/// Dismiss one item, then rebuild the job's list past the floor: the person who
/// dismissed it is looking at the list as they do.
pub async fn dismiss_pending_delivery_for_job(
    orch: &Orchestrator,
    job_id: &str,
    id: &str,
    source: PendingDeliverySource,
) -> Result<(), String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|error| error.to_string())?;
    dismiss_pending_delivery(&db, job_id, id, source).await?;
    orch.pending_deliveries_cache
        .invalidate_now(job_id, "pending-delivery-dismissed");
    Ok(())
}

pub async fn list_pending_deliveries(
    db: &LocalDb,
    job_id: &str,
) -> Result<Vec<PendingDelivery>, String> {
    // "Pending" means a future delivery will carry this. A recipient no turn can
    // reach has nothing pending, whatever rows it still holds; the rows stay
    // untouched so the same items reappear if the refusal lifts.
    if crate::messages::delivery::delivery_refusal(db, job_id)
        .await?
        .is_some()
    {
        return Ok(Vec::new());
    }

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

    /// A job of `status` on an issue's canonical execution, holding one live
    /// `wake` push and one side-channel notice, so a refused recipient has both
    /// kinds of row to withhold.
    async fn seeded_recipient(name: &str, status: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(format!(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','proj','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                               created_at, updated_at)
              VALUES('i','p',1,'Issue','active','active','none',1,1);
            INSERT INTO executions(id, issue_id, project_id, recipe_id, status, started_at, seq)
              VALUES('e','i','p','build','running',1,1);
            INSERT INTO jobs(id, execution_id, project_id, issue_id, status, uri_segment,
                             node_name, created_at, updated_at)
              VALUES('job-1','e','p','i','{status}','builder','builder',1,1);
            "
        ))
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
        crate::orchestrator::wakes::record_live_comment_side_channel_message(
            &db,
            "job-1",
            "cairn://p/proj/9/1/child",
            "[Side-channel] the user messaged your child cairn://p/proj/9/1/child:\nping",
        )
        .await
        .unwrap();
        db
    }

    /// Point `job-1` at a session of `status`, closed for `reason` if given.
    async fn set_current_session(db: &LocalDb, status: &str, reason: Option<&str>) {
        db.execute(
            "INSERT INTO sessions(id, job_id, backend, status, terminal_reason, sequence,
                                  created_at, updated_at)
             VALUES('sess-1','job-1','claude',?1,?2,1,1,1)",
            cairn_db::turso::params![status, reason],
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE jobs SET current_session_id='sess-1' WHERE id='job-1'",
            (),
        )
        .await
        .unwrap();
    }

    /// The withheld rows are untouched: not delivered, not retired, so the
    /// same items come back if the refusal lifts.
    async fn assert_push_untouched(db: &LocalDb) {
        assert_eq!(
            db.query_opt_i64(
                "SELECT COUNT(*) FROM attention_pushes
                  WHERE recipient='job-1' AND delivered_event_id IS NULL AND retired_at IS NULL",
                (),
            )
            .await
            .unwrap(),
            Some(1)
        );
    }

    /// The bulk of the stranded stack: a builder whose issue merged. Its
    /// session refuses every turn, so no reply could carry what it holds.
    #[tokio::test]
    async fn a_merged_issues_complete_job_lists_nothing() {
        let db = seeded_recipient("pending-merged-complete.db", "complete").await;
        set_current_session(&db, "closed", Some("issue_merged")).await;

        let items = list_pending_deliveries(&db, "job-1").await.unwrap();

        assert!(items.is_empty(), "{items:?}");
        assert_push_untouched(&db).await;
    }

    /// A failed job resumes on a reply or a retry with the same job id, so
    /// "failed" is not a terminality predicate: what it holds is still pending.
    #[tokio::test]
    async fn a_failed_job_with_an_open_session_keeps_its_pushes() {
        let db = seeded_recipient("pending-failed-open.db", "failed").await;
        set_current_session(&db, "open", None).await;

        let items = list_pending_deliveries(&db, "job-1").await.unwrap();

        assert_eq!(items.len(), 2, "{items:?}");
    }

    #[tokio::test]
    async fn a_complete_job_with_an_open_session_keeps_its_pushes() {
        let db = seeded_recipient("pending-complete-open.db", "complete").await;
        set_current_session(&db, "open", None).await;

        let items = list_pending_deliveries(&db, "job-1").await.unwrap();

        assert_eq!(items.len(), 2, "{items:?}");
    }

    /// A cancelled job takes no turn whatever its session says, including the
    /// stray open session a cancelled coordinator can keep past its merge.
    #[tokio::test]
    async fn a_cancelled_job_lists_nothing() {
        let db = seeded_recipient("pending-cancelled.db", "cancelled").await;
        set_current_session(&db, "open", None).await;
        assert!(list_pending_deliveries(&db, "job-1")
            .await
            .unwrap()
            .is_empty());

        db.execute(
            "UPDATE sessions SET status='closed', terminal_reason='issue_merged' WHERE id='sess-1'",
            (),
        )
        .await
        .unwrap();
        assert!(list_pending_deliveries(&db, "job-1")
            .await
            .unwrap()
            .is_empty());
        assert_push_untouched(&db).await;
    }

    /// Filtering rather than retiring is what makes a close reversible: the
    /// same push, under the same id, lists again once the thread reopens.
    #[tokio::test]
    async fn a_closed_threads_list_returns_when_it_reopens() {
        let db = crate::storage::migrated_test_db("pending-thread-reopen.db").await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','proj','/tmp/repo',1,1);
            INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
              VALUES('t','p','topic','active','none',1,1);
            INSERT INTO jobs(id, thread_id, project_id, status, uri_segment, node_name,
                             created_at, updated_at)
              VALUES('job-thread','t','p','idle','thread','thread',1,1);
            ",
        )
        .await
        .unwrap();
        attention_push::push(
            &db,
            "job-thread",
            "cairn://p/proj/topic",
            attention_push::Wake::Wake,
            attention_push::Boundary::Event,
            &attention_push::direct_push_key("msg-gone", None),
        )
        .await
        .unwrap();
        let open = list_pending_deliveries(&db, "job-thread").await.unwrap();
        assert_eq!(open.len(), 1, "{open:?}");

        db.execute("UPDATE threads SET status='closed' WHERE id='t'", ())
            .await
            .unwrap();
        let closed = list_pending_deliveries(&db, "job-thread").await.unwrap();
        assert!(closed.is_empty(), "{closed:?}");

        db.execute("UPDATE threads SET status='active' WHERE id='t'", ())
            .await
            .unwrap();
        let reopened = list_pending_deliveries(&db, "job-thread").await.unwrap();
        assert_eq!(reopened, open);
    }

    #[tokio::test]
    async fn a_superseded_executions_job_lists_nothing() {
        let db = seeded_recipient("pending-superseded.db", "idle").await;
        assert_eq!(
            list_pending_deliveries(&db, "job-1").await.unwrap().len(),
            2
        );

        db.execute(
            "UPDATE executions SET lifecycle='superseded' WHERE id='e'",
            (),
        )
        .await
        .unwrap();
        let items = list_pending_deliveries(&db, "job-1").await.unwrap();

        assert!(items.is_empty(), "{items:?}");
        assert_push_untouched(&db).await;
    }

    /// The one closed session that still takes a turn: a launch replaces a
    /// session closed by the already-terminal reconciliation with a fresh one.
    #[tokio::test]
    async fn a_reconciled_closed_session_still_lists() {
        let db = seeded_recipient("pending-reconciled.db", "complete").await;
        set_current_session(
            &db,
            "closed",
            Some(crate::orchestrator::lifecycle::ALREADY_TERMINAL_RECONCILED_REASON),
        )
        .await;

        let items = list_pending_deliveries(&db, "job-1").await.unwrap();

        assert_eq!(items.len(), 2, "{items:?}");
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
                                   content, created_at, urgency)
             VALUES ('msg-1', 'direct', NULL, NULL, 'cairn://p/proj/2/1/planner',
                     'Rebase onto main
  before review.', 1, 'queue')",
            (),
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO message_recipients (message_id, position, recipient_job_id, recipient_run_id, created_at)
             VALUES ('msg-1', 0, 'job-1', 'run-1', 1)",
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

    /// A side-channel notice is the one item that leaves the list with time
    /// alone, so it is the one that sets when the list next changes.
    #[test]
    fn only_side_channel_notices_expire() {
        let item = |id: &str, source, created_at| PendingDelivery {
            id: id.to_string(),
            source,
            kind: "k".to_string(),
            headline: "h".to_string(),
            detail: None,
            uri: None,
            created_at,
        };
        let horizon = crate::orchestrator::wakes::NOTICE_DELIVERY_HORIZON_SECS;

        assert_eq!(
            next_expiry(&[
                item("push", PendingDeliverySource::Push, 5),
                item("channel", PendingDeliverySource::Channel, 6),
            ]),
            None
        );
        assert_eq!(
            next_expiry(&[
                item("push", PendingDeliverySource::Push, 5),
                item("later", PendingDeliverySource::SideChannel, 300),
                item("sooner", PendingDeliverySource::SideChannel, 200),
            ]),
            Some(200 + horizon)
        );
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
