//! The `operator_messages` row, its state, and the transitions every source
//! of *seen* and *answered* goes through.

use cairn_db::turso::params;

use crate::channels::{ledger::OutboundRecord, operator_message};
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

/// The longest claim a message carries, in characters. A longer one is refused
/// by name, never truncated.
pub const MAX_CHARS: usize = 400;

const URI_PREFIX: &str = "cairn://operator/messages/";

/// `cairn://operator/messages/{id}`.
pub fn message_uri(id: &str) -> String {
    format!("{URI_PREFIX}{id}")
}

/// The channel a message was sent through: the provider word of a
/// `cairn://channels/{provider}` row the operator wrote a purpose line for.
/// Discord is not one: it has no single conversation with the operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    App,
    IMessage,
    Telegram,
}

impl Via {
    pub const ALL: [Via; 3] = [Via::App, Via::IMessage, Via::Telegram];

    pub fn as_str(self) -> &'static str {
        match self {
            Via::App => "app",
            Via::IMessage => "imessage",
            Via::Telegram => "telegram",
        }
    }

    pub fn parse(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|via| via.as_str() == word)
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Via::App => "App",
            Via::IMessage => "iMessage",
            Via::Telegram => "Telegram",
        }
    }

    /// `cairn://channels/{provider}`.
    pub fn provider_uri(self) -> String {
        format!("cairn://channels/{}", self.as_str())
    }

    /// Whether a send through it is a transport delivery, with an outbound row.
    pub fn is_phone(self) -> bool {
        self != Via::App
    }
}

/// How the operator came to see a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeenVia {
    /// The transport's read receipt (iMessage `date_read`).
    Receipt,
    /// His headline cursor passed the message's headline.
    Cursor,
    /// He opened it in the app.
    App,
}

impl SeenVia {
    pub fn as_str(self) -> &'static str {
        match self {
            SeenVia::Receipt => "receipt",
            SeenVia::Cursor => "cursor",
            SeenVia::App => "app",
        }
    }

    pub fn parse(word: &str) -> Option<Self> {
        [SeenVia::Receipt, SeenVia::Cursor, SeenVia::App]
            .into_iter()
            .find(|via| via.as_str() == word)
    }

    /// `by read receipt`, as a state line reads it.
    pub fn describe(self) -> &'static str {
        match self {
            SeenVia::Receipt => "by read receipt",
            SeenVia::Cursor => "on his headlines",
            SeenVia::App => "in the app",
        }
    }
}

/// Where the operator's answer came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnsweredVia {
    /// A reply on the transport, attributed to the message by its guid.
    Reply,
    /// His reply in the app, on the message's own URI.
    App,
}

impl AnsweredVia {
    pub fn as_str(self) -> &'static str {
        match self {
            AnsweredVia::Reply => "reply",
            AnsweredVia::App => "app",
        }
    }

    pub fn parse(word: &str) -> Option<Self> {
        match word {
            "reply" => Some(AnsweredVia::Reply),
            "app" => Some(AnsweredVia::App),
            _ => None,
        }
    }
}

/// One message to the operator. Times are epoch seconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub id: String,
    pub project_id: String,
    /// The sender's canonical home: a thread, or an issue node or task.
    pub sender_home: String,
    pub sender_job_id: String,
    pub via: Via,
    pub content: String,
    pub default_text: Option<String>,
    /// The linked headline's canonical row URI.
    pub headline: Option<String>,
    pub headline_thread_id: Option<String>,
    pub headline_row_id: Option<i64>,
    pub created_at: i64,
    pub seen_at: Option<i64>,
    pub seen_via: Option<SeenVia>,
    pub answered_at: Option<i64>,
    pub answered_via: Option<AnsweredVia>,
    /// The `channel_inbound` id of a transport reply, or the thread message id
    /// of an app reply.
    pub answer_ref: Option<String>,
}

impl Message {
    pub fn uri(&self) -> String {
        message_uri(&self.id)
    }

    /// Whether it waits on him: a message that states a default is in his
    /// notification center until he answers it.
    pub fn waits(&self) -> bool {
        self.default_text.is_some()
    }
}

const COLUMNS: &str = "id, project_id, sender_home, sender_job_id, via, content, default_text, headline, headline_thread_id, headline_row_id, created_at, seen_at, seen_via, answered_at, answered_via, answer_ref";

fn from_row(row: &cairn_db::turso::Row) -> DbResult<Message> {
    Ok(Message {
        id: row.text(0)?,
        project_id: row.text(1)?,
        sender_home: row.text(2)?,
        sender_job_id: row.text(3)?,
        // The CHECK admits only the three words.
        via: Via::parse(&row.text(4)?).unwrap_or(Via::App),
        content: row.text(5)?,
        default_text: row.opt_text(6)?,
        headline: row.opt_text(7)?,
        headline_thread_id: row.opt_text(8)?,
        headline_row_id: row.opt_i64(9)?,
        created_at: row.i64(10)?,
        seen_at: row.opt_i64(11)?,
        seen_via: row.opt_text(12)?.as_deref().and_then(SeenVia::parse),
        answered_at: row.opt_i64(13)?,
        answered_via: row.opt_text(14)?.as_deref().and_then(AnsweredVia::parse),
        answer_ref: row.opt_text(15)?,
    })
}

/// The headline a message links: its canonical row URI, and the store row it
/// names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadlineLink {
    pub uri: String,
    pub thread_id: String,
    pub row_id: i64,
}

/// A message about to be written.
#[derive(Debug, Clone)]
pub struct NewMessage {
    pub id: String,
    pub project_id: String,
    pub sender_home: String,
    pub sender_job_id: String,
    pub via: Via,
    pub content: String,
    pub default_text: Option<String>,
    pub headline: Option<HeadlineLink>,
    pub created_at: i64,
}

/// The one outbound row a phone `via` writes beside its message: the
/// operator's configured conversation on that provider and the text exactly as
/// it goes on the wire.
#[derive(Debug, Clone)]
pub struct NewDelivery {
    pub channel: &'static str,
    pub conversation: String,
    pub rendered_text: String,
    pub created_at_ms: i64,
}

/// Write a message and, for a phone `via`, its pending outbound row, in one
/// transaction: a message never exists without the delivery it promised, and
/// a delivery never exists without its message.
pub async fn insert(
    db: &LocalDb,
    message: &NewMessage,
    delivery: Option<&NewDelivery>,
) -> Result<(), String> {
    let message = message.clone();
    let delivery = delivery.cloned();
    db.write(move |conn| {
        let message = message.clone();
        let delivery = delivery.clone();
        Box::pin(async move {
            let (headline, thread_id, row_id) = match &message.headline {
                Some(link) => (
                    Some(link.uri.clone()),
                    Some(link.thread_id.clone()),
                    Some(link.row_id),
                ),
                None => (None, None, None),
            };
            conn.execute(
                &format!(
                    "INSERT INTO operator_messages ({COLUMNS})
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, NULL, NULL, NULL, NULL, NULL)"
                ),
                params![
                    message.id.clone(),
                    message.project_id.clone(),
                    message.sender_home.clone(),
                    message.sender_job_id.clone(),
                    message.via.as_str(),
                    message.content.clone(),
                    message.default_text.clone(),
                    headline,
                    thread_id,
                    row_id,
                    message.created_at
                ],
            )
            .await?;
            if let Some(delivery) = delivery {
                conn.execute(
                    "INSERT INTO channel_outbound (id, channel, kind, binding_ref, conversation, job_id,
                       rendered_text, rendering, status, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'text', 'pending', ?8)",
                    params![
                        message.id.clone(),
                        delivery.channel,
                        operator_message::KIND,
                        operator_message::binding_ref(&message.id),
                        delivery.conversation.clone(),
                        message.sender_job_id.clone(),
                        delivery.rendered_text.clone(),
                        delivery.created_at_ms
                    ],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("failed to record the message: {error}"))
}

pub async fn get(db: &LocalDb, id: &str) -> Result<Option<Message>, String> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM operator_messages WHERE id = ?1"),
        params![id.to_string()],
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// One sender's messages, newest first.
pub async fn for_sender(db: &LocalDb, home: &str, limit: i64) -> Result<Vec<Message>, String> {
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM operator_messages WHERE sender_home = ?1
              ORDER BY created_at DESC, id DESC LIMIT ?2"
        ),
        params![home.to_string(), limit],
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Every message, newest first: the operator's own view.
pub async fn all(db: &LocalDb, limit: i64) -> Result<Vec<Message>, String> {
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM operator_messages ORDER BY created_at DESC, id DESC LIMIT ?1"
        ),
        params![limit],
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// The project's messages that wait on the operator: open while unanswered,
/// and those answered at or after `since`.
pub async fn waiting(db: &LocalDb, project_id: &str, since: i64) -> Result<Vec<Message>, String> {
    db.query_all(
        format!(
            "SELECT {COLUMNS} FROM operator_messages
              WHERE project_id = ?1 AND default_text IS NOT NULL
                AND (answered_at IS NULL OR answered_at >= ?2)"
        ),
        params![project_id.to_string(), since],
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

/// A message's delivery row, if its `via` wrote one.
pub async fn delivery(db: &LocalDb, message: &Message) -> Result<Option<OutboundRecord>, String> {
    if !message.via.is_phone() {
        return Ok(None);
    }
    crate::channels::ledger::get_by_binding(
        db,
        message.via.as_str(),
        operator_message::KIND,
        &operator_message::binding_ref(&message.id),
    )
    .await
}

/// A message's state, from its own row and its delivery row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// Written, not yet on the transport.
    Sent,
    /// On the transport (or in the app, which delivers on write).
    Delivered,
    /// The transport refused it. `retrying` while attempts remain.
    DeliveryFailed {
        error: String,
        attempts: i64,
        retrying: bool,
    },
    /// He has seen it, and not answered. `via` is absent only when the answer
    /// path stamped the time.
    Seen {
        via: Option<SeenVia>,
        since: i64,
    },
    Answered {
        via: Option<AnsweredVia>,
        at: i64,
    },
}

impl State {
    pub fn word(&self) -> &'static str {
        match self {
            State::Sent => "sent",
            State::Delivered => "delivered",
            State::DeliveryFailed { .. } => "delivery_failed",
            State::Seen { .. } => "seen",
            State::Answered { .. } => "answered",
        }
    }

    /// The state as one line a thread reads: `seen by read receipt 3h ago, no
    /// reply` is how it knows he knows.
    pub fn describe(&self, now: i64) -> String {
        match self {
            State::Sent => "sent, not yet delivered".to_string(),
            State::Delivered => "delivered, not yet seen".to_string(),
            State::DeliveryFailed {
                error,
                attempts,
                retrying: true,
            } => format!("delivery failing after {attempts} attempt(s), retrying: {error}"),
            State::DeliveryFailed {
                error, attempts, ..
            } => format!("delivery failed after {attempts} attempt(s): {error}"),
            State::Seen { via, since } => format!(
                "seen{} {}, no reply",
                via.map(|via| format!(" {}", via.describe()))
                    .unwrap_or_default(),
                ago(now.saturating_sub(*since))
            ),
            State::Answered { via, at } => format!(
                "answered {}{}",
                ago(now.saturating_sub(*at)),
                match via {
                    Some(AnsweredVia::Reply) => " by reply",
                    Some(AnsweredVia::App) => " in the app",
                    None => "",
                }
            ),
        }
    }
}

/// `just now`, `12m ago`, `3h ago`, `2d ago`.
pub fn ago(seconds: i64) -> String {
    match seconds.max(0) {
        0..=59 => "just now".to_string(),
        seconds @ 60..=3_599 => format!("{}m ago", seconds / 60),
        seconds @ 3_600..=172_799 => format!("{}h ago", seconds / 3_600),
        seconds => format!("{}d ago", seconds / 86_400),
    }
}

/// Answered dominates seen, and seen dominates delivery: once he has the
/// message, how it reached him no longer changes what the sender should do.
pub fn state(message: &Message, outbound: Option<&OutboundRecord>) -> State {
    if let Some(at) = message.answered_at {
        return State::Answered {
            via: message.answered_via,
            at,
        };
    }
    if let Some(since) = message.seen_at {
        return State::Seen {
            via: message.seen_via,
            since,
        };
    }
    if !message.via.is_phone() {
        return State::Delivered;
    }
    match outbound {
        Some(row) if matches!(row.status.as_str(), "sent" | "resolved" | "cleanup_pending") => {
            State::Delivered
        }
        Some(row) if row.status == "failed" && row.attempts > 0 => State::DeliveryFailed {
            error: row
                .last_error
                .clone()
                .unwrap_or_else(|| "unknown error".to_string()),
            attempts: row.attempts,
            retrying: row.next_attempt_at.is_some()
                && row.attempts < crate::channels::router::MAX_ATTEMPTS,
        },
        _ => State::Sent,
    }
}

/// Which unseen messages a seen source names.
#[derive(Debug, Clone)]
pub enum MessageFilter {
    /// These messages, by id.
    Ids(Vec<String>),
    /// Messages linked to a headline of `thread_id` at or below `through`.
    HeadlineThrough { thread_id: String, through: i64 },
}

/// Mark every unseen message `filter` names as seen `via` at `at`, and send
/// each one's sender a passive notice. Returns the messages that moved; a
/// message already seen moves nothing and earns no second notice.
pub async fn record_seen(
    orch: &Orchestrator,
    filter: MessageFilter,
    via: SeenVia,
    at: i64,
) -> Result<Vec<Message>, String> {
    let moved = orch
        .db
        .local
        .write(move |conn| {
            let filter = filter.clone();
            Box::pin(async move {
                let candidates = match &filter {
                    MessageFilter::Ids(ids) => ids.clone(),
                    MessageFilter::HeadlineThrough { thread_id, through } => {
                        let mut rows = conn
                            .query(
                                "SELECT id FROM operator_messages
                                  WHERE headline_thread_id = ?1 AND headline_row_id <= ?2
                                    AND seen_at IS NULL",
                                params![thread_id.as_str(), *through],
                            )
                            .await?;
                        let mut ids = Vec::new();
                        while let Some(row) = rows.next().await? {
                            ids.push(row.text(0)?);
                        }
                        ids
                    }
                };
                let mut moved = Vec::new();
                for id in candidates {
                    let changed = conn
                        .execute(
                            "UPDATE operator_messages SET seen_at = ?2, seen_via = ?3
                              WHERE id = ?1 AND seen_at IS NULL",
                            params![id.as_str(), at, via.as_str()],
                        )
                        .await?;
                    if changed == 1 {
                        moved.push(id);
                    }
                }
                Ok(moved)
            })
        })
        .await
        .map_err(|error| format!("failed to mark messages seen: {error}"))?;
    let mut seen = Vec::new();
    for id in moved {
        if let Some(message) = get(&orch.db.local, &id).await? {
            notify_seen(orch, &message, via).await;
            seen.push(message);
        }
    }
    if !seen.is_empty() {
        announce(orch, &seen);
    }
    Ok(seen)
}

/// The operator's headline cursor on `thread_id` moved through `through`:
/// every message linked to a headline it now covers is seen.
pub async fn cursor_moved(
    orch: &Orchestrator,
    thread_id: &str,
    through: i64,
    at: i64,
) -> Result<Vec<Message>, String> {
    record_seen(
        orch,
        MessageFilter::HeadlineThrough {
            thread_id: thread_id.to_string(),
            through,
        },
        SeenVia::Cursor,
        at,
    )
    .await
}

/// Record the operator's answer. A message not yet seen is seen by it, with
/// no separate seen notice: the answer itself reaches the sender, at steer.
/// Returns the message, or `None` when it was already answered.
pub async fn record_answered(
    orch: &Orchestrator,
    id: &str,
    via: AnsweredVia,
    answer_ref: Option<&str>,
    at: i64,
) -> Result<Option<Message>, String> {
    let seen_via = match via {
        AnsweredVia::App => Some(SeenVia::App.as_str()),
        AnsweredVia::Reply => None,
    };
    let changed = orch
        .db
        .local
        .execute(
            "UPDATE operator_messages
                SET answered_at = ?2, answered_via = ?3, answer_ref = ?4,
                    seen_via = CASE WHEN seen_at IS NULL THEN ?5 ELSE seen_via END,
                    seen_at = COALESCE(seen_at, ?2)
              WHERE id = ?1 AND answered_at IS NULL",
            params![
                id.to_string(),
                at,
                via.as_str(),
                answer_ref.map(str::to_string),
                seen_via
            ],
        )
        .await
        .map_err(|error| format!("failed to record the answer: {error}"))?;
    if changed == 0 {
        return Ok(None);
    }
    let message = get(&orch.db.local, id).await?;
    if let Some(message) = &message {
        announce(orch, std::slice::from_ref(message));
    }
    Ok(message)
}

/// Attach the thread message an answer was delivered as, once the answer is
/// recorded: the answer is recorded before it is delivered, so the id it was
/// delivered under is only known afterward.
pub async fn attach_answer_ref(db: &LocalDb, id: &str, answer_ref: &str) -> Result<(), String> {
    db.execute(
        "UPDATE operator_messages SET answer_ref = ?2 WHERE id = ?1 AND answer_ref IS NULL",
        params![id.to_string(), answer_ref.to_string()],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("failed to attach the answer's message: {error}"))
}

/// The thread a sender home names when it is a thread's own home (not one of
/// its tasks): the thread's database and id. A thread is addressed by name,
/// so whichever session it holds when something arrives receives it.
async fn sender_thread(
    orch: &Orchestrator,
    home: &str,
) -> Result<Option<(std::sync::Arc<LocalDb>, String)>, String> {
    let Some(cairn_uri::CairnResource::Thread {
        project,
        name,
        path,
    }) = cairn_uri::parse_uri(home)
    else {
        return Ok(None);
    };
    if !path.is_empty() {
        return Ok(None);
    }
    let project = cairn_uri::canonical_project(&project);
    let db = orch.db.for_project(&project).await;
    let thread_id = db
        .query_opt_text(
            "SELECT t.id FROM threads t JOIN projects p ON p.id = t.project_id
              WHERE LOWER(p.key) = ?1 AND t.name = ?2",
            params![project.clone(), name.clone()],
        )
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("{project} has no thread named {name}"))?;
    Ok(Some((db, thread_id)))
}

/// How an answer reached the message's sender, which its header says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnswerHeader<'a> {
    /// The operator answered this message itself: in the app, or by a native
    /// reply on a transport.
    Reply,
    /// The operator texted without a reply-to on `provider`, and the text was
    /// routed here only because this message is the latest any thread sent
    /// him there in 24 hours. It may not be about the message at all.
    Inferred { provider: &'a str },
}

impl AnswerHeader<'_> {
    fn render(self, message_uri: &str) -> String {
        match self {
            Self::Reply => format!("[Reply to {message_uri}]"),
            Self::Inferred { provider } => format!(
                "[Unaddressed text from the operator, delivered to you because {message_uri} is the latest message any thread sent him on {provider} in 24 h]"
            ),
        }
    }
}

/// Deliver the operator's answer to the job that sent the message, headed by
/// how it reached the sender. A thread's own session takes it through the
/// thread's append, which steers it; any other sender (an issue node, or a
/// task of a thread or node) takes it on its own job, never a sibling or its
/// issue's root. Returns the thread message id when there is one.
pub async fn deliver_answer(
    orch: &Orchestrator,
    message: &Message,
    sender_name: &str,
    text: &str,
    header: AnswerHeader<'_>,
) -> Result<Option<String>, String> {
    let delivered = format!("{} {text}", header.render(&message.uri()));
    if let Some((db, thread_id)) = sender_thread(orch, &message.sender_home).await? {
        return crate::messages::delivery::append_thread_message(
            orch,
            &db,
            &thread_id,
            None,
            sender_name,
            &delivered,
        )
        .await
        .map(Some);
    }
    let orch = orch.clone();
    let job_id = message.sender_job_id.clone();
    let attributed = format!("[Direct message from {sender_name}] {delivered}");
    tokio::task::spawn_blocking(move || {
        crate::execution::jobs::continue_job_or_enqueue(
            &orch,
            &job_id,
            Some(&attributed),
            None,
            None,
        )
        .map(|_| ())
    })
    .await
    .map_err(|error| format!("delivering the answer failed: {error}"))??;
    Ok(None)
}

/// A change the app's views and the notification center must see.
fn announce(orch: &Orchestrator, messages: &[Message]) {
    let mut projects = messages
        .iter()
        .filter(|message| message.waits())
        .map(|message| message.project_id.as_str())
        .collect::<Vec<_>>();
    projects.sort_unstable();
    projects.dedup();
    for project_id in projects {
        orch.operator_notifications_cache
            .invalidate(project_id, "operator-message");
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "operator_messages", "action": "update"}),
    );
}

/// The idempotency key of a message's one seen notice.
pub fn seen_notice_key(id: &str) -> String {
    format!("operator-seen:{id}")
}

/// The job a message's notices reach: a thread sender's current session,
/// addressed by the thread's name as [`deliver_answer`] addresses it, or the
/// sending job itself for an issue node or a task.
async fn notice_recipient(orch: &Orchestrator, message: &Message) -> Result<String, String> {
    match sender_thread(orch, &message.sender_home).await? {
        Some((db, thread_id)) => crate::threads::ensure_thread_session(&db, &thread_id).await,
        None => Ok(message.sender_job_id.clone()),
    }
}

/// The sender's passive notice that he saw it. It rides along with the
/// sender's next turn rather than waking it: being seen changes nothing the
/// sender must do now, and a waiting message's default still stands. It is
/// addressed to a job, never a run, because `record_seen` has already stamped
/// the message: a sender with no run now must still find it when it next
/// wakes, since nothing queues it again.
async fn notify_seen(orch: &Orchestrator, message: &Message, via: SeenVia) {
    let job_id = match notice_recipient(orch, message).await {
        Ok(job_id) => job_id,
        Err(error) => {
            log::warn!(
                "operator message {} seen, but its sender {} is unreachable: {error}",
                message.id,
                message.sender_home
            );
            return;
        }
    };
    let mut content = format!("The operator saw {} ({}).", message.uri(), via.describe());
    if let Some(default) = &message.default_text {
        content.push_str(&format!(" No reply yet; your default stands: {default}"));
    }
    let orch = orch.clone();
    let key = seen_notice_key(&message.id);
    let result = tokio::task::spawn_blocking(move || {
        crate::messages::delivery::queue_system_direct_once_to_job(
            &orch,
            &job_id,
            &content,
            DeliveryUrgency::Passive,
            &key,
        )
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => log::warn!(
            "operator message {} seen notice failed: {error}",
            message.id
        ),
        Err(error) => log::warn!(
            "operator message {} seen notice failed: {error}",
            message.id
        ),
    }
}
