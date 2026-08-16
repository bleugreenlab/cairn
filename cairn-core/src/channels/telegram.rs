//! Telegram provider using Bot API long polling. Runtime admission and routing
//! remain with the parent; this module only owns transport and update mapping.

use super::{
    ChannelCapabilities, ChannelHealth, ChannelProvider, InboundEvent, OutboundAsk,
    OutboundMessage, ResolvedQuestionMessage, SentIds,
};
use async_trait::async_trait;
use futures_util::StreamExt;
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
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
        BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardButtonKind,
        InlineKeyboardMarkup, MaybeInaccessibleMessage, Message, MessageId, ParseMode, Update,
        UpdateKind, UserId,
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

fn telegram_commands() -> Vec<BotCommand> {
    super::commands::CHANNEL_COMMANDS
        .iter()
        .map(|command| BotCommand::new(command.name, command.description))
        .collect()
}

async fn start_after_command_registration<R, P, T>(registration: R, polling: P) -> T
where
    R: std::future::Future<Output = Result<(), String>>,
    P: std::future::Future<Output = T>,
{
    match tokio::time::timeout(Duration::from_secs(5), registration).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => log::warn!("{error}; Telegram polling will continue"),
        Err(_) => log::warn!("Telegram command registration timed out; polling will continue"),
    }
    polling.await
}

fn is_supported_telegram_link(destination: &str) -> bool {
    destination.split_once(':').is_some_and(|(scheme, _)| {
        matches!(
            scheme.to_ascii_lowercase().as_str(),
            "http" | "https" | "tg" | "mailto"
        )
    })
}

fn start_span(
    open: &mut Vec<(String, &'static str, usize, usize)>,
    next_order: &mut usize,
    open_tag: &str,
    close_tag: &'static str,
    start: usize,
) {
    open.push((open_tag.into(), close_tag, start, take_order(next_order)));
}

fn take_order(next_order: &mut usize) -> usize {
    let order = *next_order;
    *next_order += 1;
    order
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
        let polling = Arc::clone(&self);
        self.startup(polling.poll()).await
    }

    async fn startup<P, T>(&self, polling: P) -> T
    where
        P: std::future::Future<Output = T>,
    {
        start_after_command_registration(self.register_commands(), polling).await
    }

    async fn register_commands(&self) -> Result<(), String> {
        self.bot
            .set_my_commands(telegram_commands())
            .await
            .map(|_| ())
            .map_err(|error| format!("could not register Telegram commands: {error}"))
    }

    async fn poll(self: Arc<Self>) -> Result<(), String> {
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
        let chat = self.configured_chat(conversation)?;
        let message = parse_message(guid, self.chat_id)?;
        let rendered = render_telegram_text(&text.into(), 4096);
        retry_on_entity_parse_error(|formatted| {
            let request = self.bot.edit_message_text(
                chat,
                message,
                if formatted {
                    rendered.html.clone()
                } else {
                    rendered.plain.clone()
                },
            );
            async move {
                if formatted {
                    request.parse_mode(ParseMode::Html).await
                } else {
                    request.await
                }
            }
        })
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
        let chat = self.configured_chat(&message.conversation)?;
        let sent = retry_on_entity_parse_error(|formatted| {
            let request = self
                .bot
                .send_message(
                    chat,
                    if formatted {
                        rendered.text.html.clone()
                    } else {
                        rendered.text.plain.clone()
                    },
                )
                .reply_markup(rendered.keyboard.clone());
            async move {
                if formatted {
                    request.parse_mode(ParseMode::Html).await
                } else {
                    request.await
                }
            }
        })
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
    text: TelegramText,
    keyboard: InlineKeyboardMarkup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TelegramText {
    html: String,
    plain: String,
}

#[derive(Debug)]
struct HtmlSpan {
    start: usize,
    end: usize,
    order: usize,
    open: String,
    close: &'static str,
}

fn render_telegram_text(markdown: &str, limit: usize) -> TelegramText {
    let mut plain = String::new();
    let mut visible = 0;
    let mut spans = Vec::new();
    let mut open: Vec<(String, &'static str, usize, usize)> = Vec::new();
    let mut next_order = 0;
    let mut lists: Vec<Option<u64>> = Vec::new();
    let mut link: Option<(String, usize, usize)> = None;

    for event in Parser::new_ext(markdown, Options::ENABLE_STRIKETHROUGH) {
        let position = visible;
        match event {
            Event::Start(Tag::Strong) => {
                start_span(&mut open, &mut next_order, "<b>", "</b>", position)
            }
            Event::Start(Tag::Emphasis) => {
                start_span(&mut open, &mut next_order, "<i>", "</i>", position)
            }
            Event::Start(Tag::Strikethrough) => {
                start_span(&mut open, &mut next_order, "<s>", "</s>", position)
            }
            Event::Start(Tag::Heading { .. }) => {
                start_span(&mut open, &mut next_order, "<b>", "</b>", position)
            }
            Event::Start(Tag::CodeBlock(_)) => {
                start_span(&mut open, &mut next_order, "<pre>", "</pre>", position)
            }
            Event::Start(Tag::Link { dest_url, .. })
            | Event::Start(Tag::Image { dest_url, .. }) => {
                link = Some((
                    dest_url.into_string(),
                    position,
                    take_order(&mut next_order),
                ));
            }
            Event::Start(Tag::List(first)) => {
                if !plain.is_empty() {
                    append_newlines(&mut plain, &mut visible, limit, 1);
                }
                lists.push(first);
            }
            Event::Start(Tag::Item) => {
                let prefix = match lists.last_mut() {
                    Some(Some(ordinal)) => {
                        let value = format!("{ordinal}. ");
                        *ordinal += 1;
                        value
                    }
                    _ => "- ".into(),
                };
                append_visible(
                    &mut plain,
                    &mut visible,
                    limit,
                    &"  ".repeat(lists.len().saturating_sub(1)),
                );
                append_visible(&mut plain, &mut visible, limit, &prefix);
            }
            Event::End(TagEnd::Strong) => close_span(&mut open, &mut spans, "</b>", position),
            Event::End(TagEnd::Emphasis) => close_span(&mut open, &mut spans, "</i>", position),
            Event::End(TagEnd::Strikethrough) => {
                close_span(&mut open, &mut spans, "</s>", position)
            }
            Event::End(TagEnd::Heading(_)) => {
                close_span(&mut open, &mut spans, "</b>", position);
                append_newlines(&mut plain, &mut visible, limit, 2);
            }
            Event::End(TagEnd::CodeBlock) => {
                close_span(&mut open, &mut spans, "</pre>", position);
                append_newlines(&mut plain, &mut visible, limit, 2);
            }
            Event::End(TagEnd::Link) | Event::End(TagEnd::Image) => {
                if let Some((destination, start, order)) = link.take() {
                    if is_supported_telegram_link(&destination) {
                        spans.push(HtmlSpan {
                            start,
                            end: position,
                            order,
                            open: format!("<a href=\"{}\">", escape_html(&destination)),
                            close: "</a>",
                        });
                    } else {
                        append_visible(
                            &mut plain,
                            &mut visible,
                            limit,
                            &format!(" ({destination})"),
                        );
                    }
                }
            }
            Event::End(TagEnd::Item) => append_newlines(&mut plain, &mut visible, limit, 1),
            Event::End(TagEnd::List(_)) => {
                lists.pop();
                append_newlines(
                    &mut plain,
                    &mut visible,
                    limit,
                    if lists.is_empty() { 2 } else { 1 },
                );
            }
            Event::End(TagEnd::Paragraph) => append_newlines(&mut plain, &mut visible, limit, 2),
            Event::Text(text) | Event::Html(text) | Event::InlineHtml(text) => {
                append_visible(&mut plain, &mut visible, limit, &text)
            }
            Event::Code(text) => {
                let start = visible;
                let order = take_order(&mut next_order);
                append_visible(&mut plain, &mut visible, limit, &text);
                spans.push(HtmlSpan {
                    start,
                    end: visible,
                    order,
                    open: "<code>".into(),
                    close: "</code>",
                });
            }
            Event::SoftBreak | Event::HardBreak => {
                append_newlines(&mut plain, &mut visible, limit, 1)
            }
            Event::Rule => append_visible(&mut plain, &mut visible, limit, "---\n"),
            _ => {}
        }
    }
    while let Some((open_tag, close_tag, start, order)) = open.pop() {
        spans.push(HtmlSpan {
            start,
            end: visible,
            order,
            open: open_tag,
            close: close_tag,
        });
    }
    let trimmed = plain.trim_end().chars().count();
    plain.truncate(
        plain
            .char_indices()
            .nth(trimmed)
            .map_or(plain.len(), |(i, _)| i),
    );
    spans.retain(|span| span.start < trimmed && span.end > span.start);
    for span in &mut spans {
        span.end = span.end.min(trimmed);
    }

    let mut html = String::new();
    for (position, ch) in plain.chars().enumerate() {
        let mut starts: Vec<_> = spans.iter().filter(|span| span.start == position).collect();
        starts.sort_by_key(|span| span.order);
        for span in starts {
            html.push_str(&span.open);
        }
        html.push_str(&escape_html(&ch.to_string()));
        let mut ends: Vec<_> = spans
            .iter()
            .filter(|span| span.end == position + 1)
            .collect();
        ends.sort_by_key(|span| std::cmp::Reverse(span.order));
        for span in ends {
            html.push_str(span.close);
        }
    }
    TelegramText { html, plain }
}

fn append_visible(output: &mut String, count: &mut usize, limit: usize, text: &str) {
    for ch in text.chars().take(limit.saturating_sub(*count)) {
        output.push(ch);
        *count += 1;
    }
}

fn append_newlines(output: &mut String, count: &mut usize, limit: usize, wanted: usize) {
    let present = output.chars().rev().take_while(|ch| *ch == '\n').count();
    append_visible(
        output,
        count,
        limit,
        &"\n".repeat(wanted.saturating_sub(present)),
    );
}

fn close_span(
    open: &mut Vec<(String, &'static str, usize, usize)>,
    spans: &mut Vec<HtmlSpan>,
    close: &'static str,
    end: usize,
) {
    if let Some(index) = open
        .iter()
        .rposition(|(_, candidate, _, _)| *candidate == close)
    {
        let (open, close, start, order) = open.remove(index);
        spans.push(HtmlSpan {
            start,
            end,
            order,
            open,
            close,
        });
    }
}

fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn is_entity_parse_error(error: &teloxide::RequestError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("parse entities")
        || message.contains("can't find end tag")
        || message.contains("unsupported start tag")
}

async fn retry_on_entity_parse_error<T, F, Fut>(mut request: F) -> Result<T, teloxide::RequestError>
where
    F: FnMut(bool) -> Fut,
    Fut: std::future::Future<Output = Result<T, teloxide::RequestError>>,
{
    match request(true).await {
        Err(error) if is_entity_parse_error(&error) => {
            log::warn!("Telegram rejected HTML entities; retrying as plain text: {error}");
            request(false).await
        }
        result => result,
    }
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
        text: render_telegram_text(&text, 4096),
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
            conversation: format!("telegram:{chat_id}"),
            bound_guid: scoped_message_id(chat_id, reply.id),
            sender,
            text,
        }),
        None => Some(InboundEvent::Bare {
            conversation: format!("telegram:{chat_id}"),
            sender,
            text,
        }),
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
        conversation: format!("telegram:{chat_id}"),
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
    use std::sync::atomic::{AtomicBool, Ordering};

    async fn mock_bot(success: bool) -> (Bot, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let count = socket.read(&mut chunk).await.unwrap();
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..count]);
                if let Some(header_end) = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4)
                {
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    if request.len() >= header_end + content_length {
                        break;
                    }
                }
            }
            let _ = request_tx.send(String::from_utf8_lossy(&request).into_owned());
            let body = if success {
                r#"{"ok":true,"result":true}"#
            } else {
                r#"{"ok":false,"error_code":500,"description":"registration failed"}"#
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let url = reqwest::Url::parse(&format!("http://{address}/")).unwrap();
        (Bot::new("123:test").set_api_url(url), request_rx)
    }
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
        assert_eq!(q.text.plain, "Context\n\nChoose");
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
                conversation: "telegram:99".into(),
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
                conversation: "telegram:99".into(),
                bound_guid: "99:15".into(),
                sender: "7".into(),
                option_id: "approve".into(),
                option_text: "Approve".into(),
                selected: true
            })
        );
        let deny_callback = update(
            json!({"update_id":4,"callback_query":{"id":"d","chat_instance":"i","from":{"id":7,"is_bot":false,"first_name":"Op"},"data":"p:deny","message":{"message_id":15,"date":1,"chat":{"id":99,"type":"private"}}}}),
        );
        assert_eq!(
            map_update(&deny_callback, ChatId(99), &allowed),
            Some(InboundEvent::Selection {
                conversation: "telegram:99".into(),
                bound_guid: "99:15".into(),
                sender: "7".into(),
                option_id: "deny".into(),
                option_text: "Deny".into(),
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
        assert_eq!(rendered.text.plain.chars().count(), 4096);
        assert_eq!(rendered.text.html.chars().count(), 4096);
        assert_eq!(rendered.keyboard.inline_keyboard[0][0].text, "Approve");
    }

    #[test]
    fn renders_markdown_as_balanced_escaped_telegram_html() {
        let rendered = render_telegram_text(
            "# **Status & safety**\n\n*Nested _emphasis_* and ~~gone~~.\n\nUse `a < b`:\n```rust\nlet x = a & b;\n```\n- [Open <Cairn>](cairn://p/cairn/3945)\n  1. child",
            4096,
        );
        assert_eq!(rendered.html, "<b><b>Status &amp; safety</b></b>\n\n<i>Nested <i>emphasis</i></i> and <s>gone</s>.\n\nUse <code>a &lt; b</code>:\n\n<pre>let x = a &amp; b;\n</pre>\n- Open &lt;Cairn&gt; (cairn://p/cairn/3945)\n  1. child");
        assert!(rendered
            .plain
            .contains("- Open <Cairn> (cairn://p/cairn/3945)\n  1. child"));
    }

    #[test]
    fn truncation_closes_tags_and_never_splits_html_entities() {
        let rendered = render_telegram_text("**1234 & 6789**", 8);
        assert_eq!(rendered.plain, "1234 & 6");
        assert_eq!(rendered.html, "<b>1234 &amp; 6</b>");
    }

    #[test]
    fn equal_boundary_spans_close_in_reverse_nesting_order() {
        assert_eq!(
            render_telegram_text("[**bold**](https://example.com)", 4096).html,
            "<a href=\"https://example.com\"><b>bold</b></a>"
        );
    }

    #[tokio::test]
    async fn entity_parse_errors_retry_once_without_formatting() {
        use teloxide::ApiError;
        let attempts = std::sync::Mutex::new(Vec::new());
        let result = retry_on_entity_parse_error(|formatted| {
            attempts.lock().unwrap().push(formatted);
            async move {
                if formatted {
                    Err(teloxide::RequestError::Api(ApiError::Unknown(
                        "Bad Request: can't parse entities".into(),
                    )))
                } else {
                    Ok("plain")
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), "plain");
        assert_eq!(*attempts.lock().unwrap(), vec![true, false]);
    }

    #[tokio::test]
    async fn startup_registers_commands_and_registration_failure_does_not_stop_polling() {
        let commands = telegram_commands();
        assert_eq!(
            commands.len(),
            super::super::commands::CHANNEL_COMMANDS.len()
        );
        for (telegram, canonical) in commands
            .iter()
            .zip(super::super::commands::CHANNEL_COMMANDS)
        {
            assert_eq!(telegram.command, canonical.name);
            assert_eq!(telegram.description, canonical.description);
        }

        for success in [true, false] {
            let (bot, request) = mock_bot(success).await;
            let (tx, rx) = mpsc::channel(1);
            let provider = TelegramProvider {
                bot,
                chat_id: ChatId(42),
                allowed_users: HashSet::new(),
                tx,
                rx: Mutex::new(Some(rx)),
                active: AtomicBool::new(false),
                started_at: AtomicI64::new(0),
                last_update_at: AtomicI64::new(0),
                last_error: Mutex::new(None),
            };
            let polled = AtomicBool::new(false);
            provider
                .startup(async {
                    polled.store(true, Ordering::SeqCst);
                })
                .await;

            let request = request.await.unwrap();
            let request_line = request.lines().next().unwrap().to_ascii_lowercase();
            assert_eq!(request_line, "post /bot123:test/setmycommands http/1.1");
            assert!(polled.load(Ordering::SeqCst));
        }
    }
}
