//! Telegram provider using Bot API long polling. Runtime admission and routing
//! remain with the parent; this module only owns transport and update mapping.

use super::{
    ChannelCapabilities, ChannelHealth, ChannelProvider, InboundEvent, OutboundAsk,
    OutboundMessage, ResolvedQuestionMessage, SentIds,
};
use async_trait::async_trait;
use futures_util::StreamExt;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use teloxide::{
    prelude::*,
    types::{
        CallbackQuery, InlineKeyboardButton, InlineKeyboardButtonKind, InlineKeyboardMarkup,
        MaybeInaccessibleMessage, Message, MessageId, Update, UpdateKind, UserId,
    },
    update_listeners::AsUpdateStream,
};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelegramLiveness {
    pub active: bool,
    pub started_at: Option<i64>,
    pub last_update_at: Option<i64>,
}

fn button_callback_data(button: &InlineKeyboardButton) -> Option<&str> {
    match &button.kind {
        InlineKeyboardButtonKind::CallbackData(data) => Some(data),
        _ => None,
    }
}

pub struct TelegramProvider {
    bot: Bot,
    chat_id: ChatId,
    allowed_users: HashSet<UserId>,
    tx: mpsc::Sender<InboundEvent>,
    rx: Mutex<Option<mpsc::Receiver<InboundEvent>>>,
    active: AtomicBool,
    started_at: AtomicI64,
    last_update_at: AtomicI64,
    last_error: Mutex<Option<String>>,
}

impl TelegramProvider {
    pub fn new(
        token: impl Into<String>,
        chat_id: ChatId,
        allowed_user_ids: impl IntoIterator<Item = u64>,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(128);
        Arc::new(Self {
            bot: Bot::new(token),
            chat_id,
            allowed_users: allowed_user_ids.into_iter().map(UserId).collect(),
            tx,
            rx: Mutex::new(Some(rx)),
            active: AtomicBool::new(false),
            started_at: AtomicI64::new(0),
            last_update_at: AtomicI64::new(0),
            last_error: Mutex::new(None),
        })
    }

    /// Starts long polling. Aborting the returned task stops the provider.
    pub fn start(self: &Arc<Self>) -> tokio::task::JoinHandle<Result<(), String>> {
        let provider = Arc::clone(self);
        tokio::spawn(async move { provider.run().await })
    }

    async fn run(self: Arc<Self>) -> Result<(), String> {
        self.active.store(true, Ordering::Release);
        self.started_at.store(now(), Ordering::Release);
        let mut listener = teloxide::update_listeners::Polling::builder(self.bot.clone())
            .timeout(Duration::from_secs(10))
            .backoff_strategy(|failures| {
                Duration::from_secs(1u64.checked_shl(failures.min(6)).unwrap_or(60).min(60))
            })
            .delete_webhook()
            .await
            .build();
        let stream = listener.as_stream();
        tokio::pin!(stream);
        while let Some(result) = stream.next().await {
            match result {
                Ok(update) => {
                    self.active.store(true, Ordering::Release);
                    *self
                        .last_error
                        .lock()
                        .expect("Telegram health lock poisoned") = None;
                    self.last_update_at.store(now(), Ordering::Release);
                    if let Some(event) = map_update(&update, self.chat_id, &self.allowed_users) {
                        if let UpdateKind::CallbackQuery(query) = &update.kind {
                            if let Err(error) =
                                self.bot.answer_callback_query(query.id.clone()).await
                            {
                                log::warn!(
                                    "could not acknowledge Telegram callback query: {error}"
                                );
                            }
                        }
                        if self.tx.send(event).await.is_err() {
                            self.active.store(false, Ordering::Release);
                            return Err("Telegram inbound subscriber was dropped".into());
                        }
                    }
                }
                Err(error) => {
                    self.active.store(false, Ordering::Release);
                    *self
                        .last_error
                        .lock()
                        .expect("Telegram health lock poisoned") =
                        Some(format!("Telegram long polling failed: {error}"));
                    log::warn!("Telegram long polling failed; retrying with backoff: {error}");
                }
            }
        }
        self.active.store(false, Ordering::Release);
        Err("Telegram long polling ended".into())
    }

    pub fn liveness(&self) -> TelegramLiveness {
        let started = self.started_at.load(Ordering::Acquire);
        let updated = self.last_update_at.load(Ordering::Acquire);
        TelegramLiveness {
            active: self.active.load(Ordering::Acquire),
            started_at: (started != 0).then_some(started),
            last_update_at: (updated != 0).then_some(updated),
        }
    }

    pub async fn edit_message(
        &self,
        conversation: &str,
        guid: &str,
        text: impl Into<String>,
    ) -> Result<(), String> {
        self.bot
            .edit_message_text(
                self.configured_chat(conversation)?,
                parse_message(guid, self.chat_id)?,
                text,
            )
            .await
            .map(|_| ())
            .map_err(|e| format!("could not edit Telegram message: {e}"))
    }

    fn configured_chat(&self, conversation: &str) -> Result<ChatId, String> {
        let requested = parse_chat(conversation)?;
        (requested == self.chat_id)
            .then_some(requested)
            .ok_or_else(|| {
                format!(
                    "Telegram provider is configured for chat {}",
                    self.chat_id.0
                )
            })
    }
}

#[async_trait]
impl ChannelProvider for TelegramProvider {
    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            structured_asks: true,
            open_options: true,
            edit_in_place: true,
            max_text_len: Some(4096),
        }
    }
    async fn send(&self, message: &OutboundMessage) -> Result<SentIds, String> {
        let rendered = render_ask(&message.context_header, &message.ask);
        let sent = self
            .bot
            .send_message(self.configured_chat(&message.conversation)?, rendered.text)
            .reply_markup(rendered.keyboard)
            .await
            .map_err(|e| format!("could not send Telegram message: {e}"))?;
        Ok(SentIds {
            primary_guid: scoped_message_id(self.chat_id, sent.id),
            caption_guid: None,
        })
    }
    fn subscribe(&self) -> mpsc::Receiver<InboundEvent> {
        self.rx
            .lock()
            .expect("Telegram subscription lock poisoned")
            .take()
            .expect("Telegram provider supports one inbound subscriber")
    }
    fn liveness(&self) -> super::ChannelLiveness {
        let value = self.liveness();
        super::ChannelLiveness {
            active: value.active,
            started_at: value.started_at,
            last_receipt_at: value.last_update_at,
            last_receipt_rowid: None,
        }
    }
    fn health(&self) -> ChannelHealth {
        if self.active.load(Ordering::Acquire) {
            ChannelHealth::Ready
        } else {
            ChannelHealth::Unavailable {
                reason: self
                    .last_error
                    .lock()
                    .expect("Telegram health lock poisoned")
                    .clone()
                    .unwrap_or_else(|| "Telegram long polling is not running".into()),
            }
        }
    }
    async fn cleanup_question(&self, message: &ResolvedQuestionMessage) -> Result<(), String> {
        self.bot
            .edit_message_reply_markup(
                self.configured_chat(&message.conversation)?,
                parse_message(&message.provider_guid, self.chat_id)?,
            )
            .reply_markup(InlineKeyboardMarkup::default())
            .await
            .map(|_| ())
            .map_err(|e| format!("could not clear Telegram keyboard: {e}"))
    }
}

struct RenderedAsk {
    text: String,
    keyboard: InlineKeyboardMarkup,
}
fn render_ask(header: &str, ask: &OutboundAsk) -> RenderedAsk {
    let (body, rows) = match ask {
        OutboundAsk::Question {
            question_index,
            text,
            options,
            ..
        } => (
            text.clone(),
            options
                .iter()
                .enumerate()
                .map(|(i, o)| {
                    vec![InlineKeyboardButton::callback(
                        o.label.clone(),
                        format!("q:{question_index}:{i}"),
                    )]
                })
                .collect(),
        ),
        OutboundAsk::Permission { summary, .. } => (
            summary.clone(),
            vec![vec![
                InlineKeyboardButton::callback("Approve", "p:approve"),
                InlineKeyboardButton::callback("Deny", "p:deny"),
            ]],
        ),
        OutboundAsk::Notify { text } => (text.clone(), Vec::new()),
    };
    let text = if header.trim().is_empty() {
        body
    } else {
        format!("{header}\n\n{body}")
    };
    RenderedAsk {
        text: text.chars().take(4096).collect(),
        keyboard: InlineKeyboardMarkup::new(rows),
    }
}

fn map_update(update: &Update, chat_id: ChatId, allowed: &HashSet<UserId>) -> Option<InboundEvent> {
    match &update.kind {
        UpdateKind::Message(m) => map_message(m, chat_id, allowed),
        UpdateKind::CallbackQuery(q) => map_callback(q, chat_id, allowed),
        _ => None,
    }
}
fn map_message(
    message: &Message,
    chat_id: ChatId,
    allowed: &HashSet<UserId>,
) -> Option<InboundEvent> {
    if message.chat.id != chat_id {
        return None;
    }
    let from = message.from.as_ref()?;
    if !allowed.contains(&from.id) {
        return None;
    }
    let sender = from.id.0.to_string();
    let text = message.text()?.to_owned();
    match message.reply_to_message() {
        Some(reply) => Some(InboundEvent::Reply {
            bound_guid: scoped_message_id(chat_id, reply.id),
            sender,
            text,
        }),
        None => Some(InboundEvent::Bare { sender, text }),
    }
}
fn map_callback(
    query: &CallbackQuery,
    chat_id: ChatId,
    allowed: &HashSet<UserId>,
) -> Option<InboundEvent> {
    if !allowed.contains(&query.from.id) {
        return None;
    }
    let message = query.message.as_ref()?;
    if message.chat().id != chat_id {
        return None;
    }
    let (option_id, option_text) = callback_option(query.data.as_deref()?, message)?;
    Some(InboundEvent::Selection {
        bound_guid: scoped_message_id(chat_id, message.id()),
        sender: query.from.id.0.to_string(),
        option_id,
        option_text,
        selected: true,
    })
}
fn callback_option(data: &str, message: &MaybeInaccessibleMessage) -> Option<(String, String)> {
    if let Some(value) = data.strip_prefix("p:") {
        return match value {
            "approve" => Some(("approve".into(), "Approve".into())),
            "deny" => Some(("deny".into(), "Deny".into())),
            _ => None,
        };
    }
    let index = data
        .strip_prefix("q:")?
        .rsplit(':')
        .next()?
        .parse::<usize>()
        .ok()?;
    let MaybeInaccessibleMessage::Regular(message) = message else {
        return None;
    };
    let button = message
        .reply_markup()?
        .inline_keyboard
        .iter()
        .flatten()
        .find(|b| button_callback_data(b) == Some(data))?;
    Some((index.to_string(), button.text.clone()))
}
fn parse_chat(value: &str) -> Result<ChatId, String> {
    value
        .parse::<i64>()
        .map(ChatId)
        .map_err(|_| format!("invalid Telegram chat id: {value}"))
}
fn scoped_message_id(chat_id: ChatId, message_id: MessageId) -> String {
    format!("{}:{}", chat_id.0, message_id.0)
}
fn parse_message(value: &str, chat_id: ChatId) -> Result<MessageId, String> {
    let (bound_chat, message) = value
        .split_once(':')
        .ok_or_else(|| format!("invalid Telegram message id: {value}"))?;
    if parse_chat(bound_chat)? != chat_id {
        return Err(format!(
            "Telegram message belongs to another chat: {bound_chat}"
        ));
    }
    message
        .parse::<i32>()
        .map(MessageId)
        .map_err(|_| format!("invalid Telegram message id: {value}"))
}
fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn update(mut value: serde_json::Value) -> Update {
        let object = value.as_object_mut().unwrap();
        let id = object.remove("update_id").unwrap().as_u64().unwrap() as u32;
        let kind = if let Some(message) = object.remove("message") {
            UpdateKind::Message(serde_json::from_value(message).unwrap())
        } else if let Some(callback) = object.remove("callback_query") {
            UpdateKind::CallbackQuery(serde_json::from_value(callback).unwrap())
        } else {
            panic!("unsupported Telegram update fixture")
        };
        Update {
            id: teloxide::types::UpdateId(id),
            kind,
        }
    }

    #[test]
    fn renders_asks_with_inline_keyboards() {
        let q = render_ask(
            "Context",
            &OutboundAsk::Question {
                prompt_id: "p".into(),
                question_index: 2,
                text: "Choose".into(),
                options: vec![
                    super::super::AskOption {
                        label: "One".into(),
                        description: None,
                    },
                    super::super::AskOption {
                        label: "Two".into(),
                        description: None,
                    },
                ],
            },
        );
        assert_eq!(q.text, "Context\n\nChoose");
        assert_eq!(
            button_callback_data(&q.keyboard.inline_keyboard[1][0]),
            Some("q:2:1")
        );
        let p = render_ask(
            "",
            &OutboundAsk::Permission {
                request_id: "r".into(),
                summary: "Run?".into(),
            },
        );
        assert_eq!(p.keyboard.inline_keyboard[0][0].text, "Approve");
        assert_eq!(
            button_callback_data(&p.keyboard.inline_keyboard[0][1]),
            Some("p:deny")
        );
    }

    #[test]
    fn maps_allowlisted_messages_and_callbacks() {
        let allowed = HashSet::from([UserId(7)]);
        let bare = update(
            json!({"update_id":1,"message":{"message_id":10,"date":1,"chat":{"id":99,"type":"private"},"from":{"id":7,"is_bot":false,"first_name":"Op"},"text":"hello"}}),
        );
        assert_eq!(
            map_update(&bare, ChatId(99), &allowed),
            Some(InboundEvent::Bare {
                sender: "7".into(),
                text: "hello".into()
            })
        );
        let denied = update(
            json!({"update_id":2,"message":{"message_id":11,"date":1,"chat":{"id":99,"type":"private"},"from":{"id":8,"is_bot":false,"first_name":"No"},"text":"ignored"}}),
        );
        assert_eq!(map_update(&denied, ChatId(99), &allowed), None);
        let callback = update(
            json!({"update_id":3,"callback_query":{"id":"c","chat_instance":"i","from":{"id":7,"is_bot":false,"first_name":"Op"},"data":"p:approve","message":{"message_id":15,"date":1,"chat":{"id":99,"type":"private"}}}}),
        );
        assert_eq!(
            map_update(&callback, ChatId(99), &allowed),
            Some(InboundEvent::Selection {
                bound_guid: "99:15".into(),
                sender: "7".into(),
                option_id: "approve".into(),
                option_text: "Approve".into(),
                selected: true
            })
        );
        assert_eq!(map_update(&bare, ChatId(100), &allowed), None);
        assert_eq!(map_update(&callback, ChatId(100), &allowed), None);
    }

    #[test]
    fn truncates_actionable_messages_without_dropping_the_keyboard() {
        let rendered = render_ask(
            &"h".repeat(4090),
            &OutboundAsk::Permission {
                request_id: "r".into(),
                summary: "approve this".into(),
            },
        );
        assert_eq!(rendered.text.chars().count(), 4096);
        assert_eq!(rendered.keyboard.inline_keyboard[0][0].text, "Approve");
    }
}
