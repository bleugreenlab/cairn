//! Discord provider using a local Serenity gateway client.

use super::{
    ChannelCapabilities, ChannelHealth, ChannelProvider, InboundEvent, OutboundAsk,
    OutboundMessage, ResolvedQuestionMessage, SentIds,
};
use async_trait::async_trait;
use serenity::{
    all::{
        ButtonStyle, Channel, ChannelId, ChannelType, Context, CreateActionRow, CreateButton,
        CreateChannel, CreateInteractionResponse, CreateMessage, CreateThread, EditMessage,
        EditThread, EventHandler, GatewayIntents, GuildId, Interaction, Message, MessageId,
        Permissions, Ready, ShardManager, UserId,
    },
    http::Http,
    Client,
};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscordRemoteChannel {
    pub channel_id: u64,
    pub parent_id: Option<u64>,
    pub topic: Option<String>,
    pub archived: bool,
    pub locked: bool,
}

fn permission_degradation(permissions: DiscordGuildPermissions) -> Option<String> {
    let mut missing = Vec::new();
    if !permissions.manage_channels {
        missing.push("MANAGE_CHANNELS");
    }
    if !permissions.manage_threads {
        missing.push("MANAGE_THREADS");
    }
    if !permissions.send_messages_in_threads {
        missing.push("SEND_MESSAGES_IN_THREADS");
    }
    (!missing.is_empty()).then(|| {
        format!(
            "Discord bot is missing required guild permissions: {}. Update the bot role in the configured guild.",
            missing.join(", ")
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiscordGuildPermissions {
    pub manage_channels: bool,
    pub manage_threads: bool,
    pub send_messages_in_threads: bool,
}

#[async_trait]
impl DiscordApi for DiscordProvider {
    async fn inspect_guild_permissions(
        &self,
        guild_id: u64,
    ) -> Result<DiscordGuildPermissions, String> {
        let guild_id = GuildId::new(guild_id);
        let guild = guild_id
            .to_partial_guild(&self.http)
            .await
            .map_err(|error| format!("could not inspect Discord guild {guild_id}: {error}"))?;
        let user = self
            .http
            .get_current_user()
            .await
            .map_err(|error| format!("could not identify the Discord bot: {error}"))?;
        let member = guild_id
            .member(&self.http, user.id)
            .await
            .map_err(|error| format!("could not inspect the Discord bot guild member: {error}"))?;
        let permissions = guild.member_permissions(&member);
        Ok(DiscordGuildPermissions {
            manage_channels: permissions.contains(Permissions::MANAGE_CHANNELS),
            manage_threads: permissions.contains(Permissions::MANAGE_THREADS),
            send_messages_in_threads: permissions.contains(Permissions::SEND_MESSAGES_IN_THREADS),
        })
    }

    async fn inspect_channel(&self, channel_id: u64) -> Result<DiscordRemoteChannel, String> {
        let channel = ChannelId::new(channel_id)
            .to_channel(&self.http)
            .await
            .map_err(|error| format!("could not inspect Discord channel {channel_id}: {error}"))?;
        let Channel::Guild(channel) = channel else {
            return Err(format!(
                "Discord channel {channel_id} is not a guild channel"
            ));
        };
        Ok(DiscordRemoteChannel {
            channel_id,
            parent_id: channel.parent_id.map(|id| id.get()),
            topic: channel.topic,
            archived: channel
                .thread_metadata
                .as_ref()
                .is_some_and(|meta| meta.archived),
            locked: channel
                .thread_metadata
                .as_ref()
                .is_some_and(|meta| meta.locked),
        })
    }

    async fn find_channel_by_marker(
        &self,
        guild_id: u64,
        marker: &str,
    ) -> Result<Option<DiscordRemoteChannel>, String> {
        let channels = GuildId::new(guild_id)
            .channels(&self.http)
            .await
            .map_err(|error| format!("could not list Discord guild channels: {error}"))?;
        Ok(channels
            .values()
            .find(|channel| channel.topic.as_deref() == Some(marker))
            .map(|channel| DiscordRemoteChannel {
                channel_id: channel.id.get(),
                parent_id: channel.parent_id.map(|id| id.get()),
                topic: channel.topic.clone(),
                archived: channel
                    .thread_metadata
                    .as_ref()
                    .is_some_and(|meta| meta.archived),
                locked: channel
                    .thread_metadata
                    .as_ref()
                    .is_some_and(|meta| meta.locked),
            }))
    }

    async fn create_category(
        &self,
        guild_id: u64,
        name: &str,
        _marker: &str,
    ) -> Result<u64, String> {
        GuildId::new(guild_id)
            .create_channel(
                &self.http,
                CreateChannel::new(name).kind(ChannelType::Category),
            )
            .await
            .map(|channel| channel.id.get())
            .map_err(|error| format!("could not create Discord category: {error}"))
    }

    async fn create_text_channel(
        &self,
        guild_id: u64,
        parent_id: u64,
        name: &str,
        marker: &str,
    ) -> Result<u64, String> {
        GuildId::new(guild_id)
            .create_channel(
                &self.http,
                CreateChannel::new(name)
                    .category(ChannelId::new(parent_id))
                    .topic(marker),
            )
            .await
            .map(|channel| channel.id.get())
            .map_err(|error| format!("could not create Discord text channel: {error}"))
    }

    async fn send_message(&self, channel_id: u64, body: &str) -> Result<u64, String> {
        ChannelId::new(channel_id)
            .send_message(&self.http, CreateMessage::new().content(body))
            .await
            .map(|message| message.id.get())
            .map_err(|error| format!("could not send Discord message: {error}"))
    }

    async fn create_public_thread(
        &self,
        channel_id: u64,
        seed_message_id: u64,
        name: &str,
    ) -> Result<u64, String> {
        ChannelId::new(channel_id)
            .create_thread_from_message(
                &self.http,
                MessageId::new(seed_message_id),
                CreateThread::new(name),
            )
            .await
            .map(|thread| thread.id.get())
            .map_err(|error| format!("could not create Discord thread: {error}"))
    }

    async fn edit_message(
        &self,
        channel_id: u64,
        message_id: u64,
        body: &str,
    ) -> Result<(), String> {
        ChannelId::new(channel_id)
            .edit_message(
                &self.http,
                MessageId::new(message_id),
                EditMessage::new().content(body),
            )
            .await
            .map(|_| ())
            .map_err(|error| format!("could not edit Discord message: {error}"))
    }

    async fn set_thread_archived(&self, channel_id: u64, archived: bool) -> Result<(), String> {
        ChannelId::new(channel_id)
            .edit_thread(&self.http, EditThread::new().archived(archived))
            .await
            .map(|_| ())
            .map_err(|error| format!("could not update Discord thread archive state: {error}"))
    }

    async fn lock_thread(&self, channel_id: u64) -> Result<(), String> {
        ChannelId::new(channel_id)
            .edit_thread(&self.http, EditThread::new().locked(true).archived(true))
            .await
            .map(|_| ())
            .map_err(|error| format!("could not lock Discord thread: {error}"))
    }
}

/// The complete side-effect boundary used by Discord surface reconciliation.
///
/// Keeping IDs and lifecycle inputs provider-neutral makes every transition
/// testable without a gateway connection and keeps Serenity out of durable state.
#[async_trait]
pub trait DiscordApi: Send + Sync {
    async fn inspect_guild_permissions(
        &self,
        guild_id: u64,
    ) -> Result<DiscordGuildPermissions, String>;
    async fn inspect_channel(&self, channel_id: u64) -> Result<DiscordRemoteChannel, String>;
    async fn find_channel_by_marker(
        &self,
        guild_id: u64,
        marker: &str,
    ) -> Result<Option<DiscordRemoteChannel>, String>;
    async fn create_category(&self, guild_id: u64, name: &str, marker: &str)
        -> Result<u64, String>;
    async fn create_text_channel(
        &self,
        guild_id: u64,
        parent_id: u64,
        name: &str,
        marker: &str,
    ) -> Result<u64, String>;
    async fn send_message(&self, channel_id: u64, body: &str) -> Result<u64, String>;
    async fn create_public_thread(
        &self,
        channel_id: u64,
        seed_message_id: u64,
        name: &str,
    ) -> Result<u64, String>;
    async fn edit_message(
        &self,
        channel_id: u64,
        message_id: u64,
        body: &str,
    ) -> Result<(), String>;
    async fn set_thread_archived(&self, channel_id: u64, archived: bool) -> Result<(), String>;
    async fn lock_thread(&self, channel_id: u64) -> Result<(), String>;
}

pub(super) async fn ensure_channel_sendable(
    api: &dyn DiscordApi,
    channel_id: u64,
) -> Result<(), String> {
    let remote = api.inspect_channel(channel_id).await?;
    if remote.locked {
        return Err(format!("Discord thread {channel_id} is locked"));
    }
    if remote.archived {
        api.set_thread_archived(channel_id, false).await?;
    }
    Ok(())
}

pub struct DiscordProvider {
    token: String,
    http: Arc<Http>,
    guild_id: GuildId,
    allowed_users: HashSet<UserId>,
    tx: mpsc::Sender<InboundEvent>,
    rx: Mutex<Option<mpsc::Receiver<InboundEvent>>>,
    active: AtomicBool,
    started_at: AtomicI64,
    last_update_at: AtomicI64,
    last_error: Mutex<Option<String>>,
    permission_degradation: Mutex<Option<String>>,
}

impl DiscordProvider {
    pub fn new(
        token: impl Into<String>,
        guild_id: GuildId,
        allowed_user_ids: impl IntoIterator<Item = u64>,
    ) -> Arc<Self> {
        let token = token.into();
        let (tx, rx) = mpsc::channel(128);
        Arc::new(Self {
            http: Arc::new(Http::new(&token)),
            token,
            guild_id,
            allowed_users: allowed_user_ids.into_iter().map(UserId::new).collect(),
            tx,
            rx: Mutex::new(Some(rx)),
            active: AtomicBool::new(false),
            started_at: AtomicI64::new(0),
            last_update_at: AtomicI64::new(0),
            last_error: Mutex::new(None),
            permission_degradation: Mutex::new(None),
        })
    }

    pub async fn start(
        self: &Arc<Self>,
    ) -> Result<
        (
            tokio::task::JoinHandle<Result<(), String>>,
            Arc<ShardManager>,
        ),
        String,
    > {
        let permissions = self
            .inspect_guild_permissions(self.guild_id.get())
            .await
            .map_err(|error| self.record_failure(error))?;
        *self
            .permission_degradation
            .lock()
            .expect("Discord permission health lock poisoned") =
            permission_degradation(permissions);
        let intents = GatewayIntents::GUILDS
            | GatewayIntents::GUILD_MESSAGES
            | GatewayIntents::DIRECT_MESSAGES
            | GatewayIntents::MESSAGE_CONTENT;
        let mut client = Client::builder(&self.token, intents)
            .event_handler_arc(self.clone())
            .await
            .map_err(|error| {
                self.record_failure(format!("Discord authentication failed: {error}"))
            })?;
        let shard_manager = client.shard_manager.clone();
        let provider = self.clone();
        let task = tokio::spawn(async move {
            let result = client.start_autosharded().await;
            provider.active.store(false, Ordering::Release);
            match result {
                Ok(()) => Err(provider.record_failure("Discord gateway stopped".into())),
                Err(error) => {
                    let detail = gateway_error(&error.to_string());
                    Err(provider.record_failure(detail))
                }
            }
        });
        Ok((task, shard_manager))
    }

    fn record_failure(&self, reason: String) -> String {
        *self
            .last_error
            .lock()
            .expect("Discord health lock poisoned") = Some(reason.clone());
        reason
    }

    fn conversation_channel(&self, conversation: &str) -> Result<ChannelId, String> {
        let address = conversation
            .parse::<super::address::ConversationAddress>()
            .map_err(|error| error.to_string())?;
        match address.destination() {
            super::address::ConversationDestination::Discord {
                guild_id,
                channel_id,
            } if *guild_id == self.guild_id.get() => Ok(ChannelId::new(*channel_id)),
            super::address::ConversationDestination::Discord { guild_id, .. } => Err(format!(
                "Discord conversation belongs to guild {guild_id}, not configured guild {}",
                self.guild_id
            )),
            _ => Err("conversation is not a Discord address".into()),
        }
    }
}

#[async_trait]
impl EventHandler for DiscordProvider {
    async fn ready(&self, _ctx: Context, _ready: Ready) {
        self.active.store(true, Ordering::Release);
        self.started_at.store(now(), Ordering::Release);
        *self
            .last_error
            .lock()
            .expect("Discord health lock poisoned") = None;
    }

    async fn message(&self, _ctx: Context, message: Message) {
        self.last_update_at.store(now(), Ordering::Release);
        if let Some(event) = map_message(&message, self.guild_id, &self.allowed_users) {
            if self.tx.send(event).await.is_err() {
                self.active.store(false, Ordering::Release);
                self.record_failure("Discord inbound subscriber was dropped".into());
            }
        }
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        let Interaction::Component(component) = interaction else {
            return;
        };
        self.last_update_at.store(now(), Ordering::Release);
        let event = map_component(&component, self.guild_id, &self.allowed_users);
        if event.is_none() {
            return;
        }
        if let Err(error) = component
            .create_response(&ctx.http, CreateInteractionResponse::Acknowledge)
            .await
        {
            log::warn!("could not acknowledge Discord component interaction: {error}");
        }
        if self
            .tx
            .send(event.expect("mapped Discord interaction"))
            .await
            .is_err()
        {
            self.active.store(false, Ordering::Release);
            self.record_failure("Discord inbound subscriber was dropped".into());
        }
    }
}

#[async_trait]
impl ChannelProvider for DiscordProvider {
    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            structured_asks: true,
            open_options: true,
            edit_in_place: true,
            max_text_len: Some(2000),
        }
    }

    async fn send(&self, message: &OutboundMessage) -> Result<SentIds, String> {
        let rendered = render_ask(&message.context_header, &message.ask);
        let channel = self.conversation_channel(&message.conversation)?;
        ensure_channel_sendable(self, channel.get()).await?;
        let sent = channel
            .send_message(
                &self.http,
                CreateMessage::new()
                    .content(rendered.text)
                    .components(rendered.components),
            )
            .await
            .map_err(|error| {
                self.record_failure(format!("could not send Discord message: {error}"))
            })?;
        Ok(SentIds {
            primary_guid: scoped_message_id(sent.channel_id, sent.id),
            caption_guid: None,
        })
    }

    fn subscribe(&self) -> mpsc::Receiver<InboundEvent> {
        self.rx
            .lock()
            .expect("Discord subscription lock poisoned")
            .take()
            .expect("Discord provider supports one inbound subscriber")
    }

    fn liveness(&self) -> super::ChannelLiveness {
        let started = self.started_at.load(Ordering::Acquire);
        let updated = self.last_update_at.load(Ordering::Acquire);
        super::ChannelLiveness {
            active: self.active.load(Ordering::Acquire),
            started_at: (started != 0).then_some(started),
            last_receipt_at: (updated != 0).then_some(updated),
            last_receipt_rowid: None,
        }
    }

    fn health(&self) -> ChannelHealth {
        if self.active.load(Ordering::Acquire) {
            match self
                .permission_degradation
                .lock()
                .expect("Discord permission health lock poisoned")
                .clone()
            {
                Some(reason) => ChannelHealth::Degraded { reason },
                None => ChannelHealth::Ready,
            }
        } else {
            ChannelHealth::Unavailable {
                reason: self
                    .last_error
                    .lock()
                    .expect("Discord health lock poisoned")
                    .clone()
                    .unwrap_or_else(|| "Discord gateway is not connected".into()),
            }
        }
    }

    async fn cleanup_question(&self, message: &ResolvedQuestionMessage) -> Result<(), String> {
        let channel_id = self.conversation_channel(&message.conversation)?;
        let message_id = parse_message(&message.provider_guid, channel_id)?;
        channel_id
            .edit_message(
                &self.http,
                message_id,
                resolved_question_edit(&message.receipt),
            )
            .await
            .map(|_| ())
            .map_err(|error| format!("could not clear Discord buttons: {error}"))
    }
}

fn resolved_question_edit(receipt: &str) -> EditMessage {
    EditMessage::new()
        .content(truncate_with_ellipsis(receipt, 2000))
        .components(Vec::new())
}

struct RenderedAsk {
    text: String,
    components: Vec<CreateActionRow>,
}

fn render_ask(header: &str, ask: &OutboundAsk) -> RenderedAsk {
    let (body, buttons) = match ask {
        OutboundAsk::Question {
            question_index,
            text,
            options,
            ..
        } => {
            let fallback = options.len() > 25;
            let body = question_body(text, options, fallback);
            let buttons = if !fallback {
                options
                    .iter()
                    .enumerate()
                    .map(|(index, option)| {
                        CreateButton::new(format!("q:{question_index}:{index}"))
                            .label(truncate_chars(&option.label, 80))
                            .style(ButtonStyle::Primary)
                    })
                    .collect()
            } else {
                Vec::new()
            };
            (body, buttons)
        }
        OutboundAsk::Permission { summary, .. } => (
            summary.clone(),
            vec![
                CreateButton::new("p:approve")
                    .label("Approve")
                    .style(ButtonStyle::Success),
                CreateButton::new("p:deny")
                    .label("Deny")
                    .style(ButtonStyle::Danger),
            ],
        ),
        OutboundAsk::Notify { text } => (text.clone(), Vec::new()),
    };
    let text = compose_text(header, &body);
    let components = buttons
        .chunks(5)
        .map(|row| CreateActionRow::Buttons(row.to_vec()))
        .collect();
    RenderedAsk { text, components }
}

fn question_body(text: &str, options: &[super::AskOption], fallback: bool) -> String {
    let base = if fallback {
        let choices = options
            .iter()
            .enumerate()
            .map(|(index, option)| format!("{}. {}", index + 1, option.label))
            .collect::<Vec<_>>()
            .join("\n");
        format!("{text}\n\nReply with an option number:\n{choices}")
    } else {
        text.to_string()
    };

    let reserved_base = base.chars().count().min(200);
    let mut legend_overhead = 2;
    let mut accepted_descriptions = 0;
    let described = options
        .iter()
        .filter_map(|option| {
            option
                .description
                .as_deref()
                .map(str::trim)
                .filter(|description| !description.is_empty())
                .map(|description| {
                    (
                        format!("**{}** — ", truncate_with_ellipsis(&option.label, 40)),
                        description,
                    )
                })
        })
        .take_while(|(prefix, _)| {
            let separator = usize::from(legend_overhead > 2);
            let entry_overhead = separator + prefix.chars().count();
            let description_count = accepted_descriptions + 1;
            if reserved_base + legend_overhead + entry_overhead + description_count > 2000 {
                return false;
            }
            legend_overhead += entry_overhead;
            accepted_descriptions = description_count;
            true
        })
        .collect::<Vec<_>>();
    if described.is_empty() {
        return base;
    }

    let minimum_description_budget = described.len();
    let base = truncate_with_ellipsis(
        &base,
        2000usize.saturating_sub(legend_overhead + minimum_description_budget),
    );
    let mut description_budget = 2000 - base.chars().count() - legend_overhead;
    let mut legend = Vec::with_capacity(described.len());
    for (index, (prefix, description)) in described.iter().enumerate() {
        let remaining_descriptions = described.len() - index;
        let limit = description_budget / remaining_descriptions;
        let description = truncate_with_ellipsis(description, limit);
        description_budget = description_budget.saturating_sub(description.chars().count());
        legend.push(format!("{prefix}{description}"));
    }

    format!("{base}\n\n{}", legend.join("\n"))
}

fn truncate_with_ellipsis(value: &str, limit: usize) -> String {
    let length = value.chars().count();
    if length <= limit {
        return value.to_string();
    }
    match limit {
        0 => String::new(),
        1 => "…".into(),
        _ => format!("{}…", value.chars().take(limit - 1).collect::<String>()),
    }
}

fn compose_text(header: &str, body: &str) -> String {
    let body = truncate_chars(body, 2000);
    let separator = "\n\n";
    let body_len = body.chars().count();
    if header.trim().is_empty() || body_len + separator.chars().count() >= 2000 {
        return body;
    }
    let remaining = 2000 - body_len - separator.chars().count();
    let header = truncate_chars(header, remaining);
    format!("{header}{separator}{body}")
}

fn truncate_chars(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

fn map_message(
    message: &Message,
    guild_id: GuildId,
    allowed: &HashSet<UserId>,
) -> Option<InboundEvent> {
    if message.author.bot || message.guild_id != Some(guild_id) {
        return None;
    }
    let channel_id = message.channel_id;
    let conversation = format!("discord:{guild_id}/{channel_id}");
    let sender = message.author.id.to_string();
    let text = message.content.clone();
    if !allowed.contains(&message.author.id) {
        return Some(InboundEvent::Rejected {
            conversation,
            sender,
            text,
        });
    }
    match message.referenced_message.as_ref() {
        Some(reply) => Some(InboundEvent::Reply {
            conversation,
            bound_guid: scoped_message_id(channel_id, reply.id),
            sender,
            text,
        }),
        None => Some(InboundEvent::Bare {
            conversation,
            sender,
            text,
        }),
    }
}

fn map_component(
    component: &serenity::all::ComponentInteraction,
    guild_id: GuildId,
    allowed: &HashSet<UserId>,
) -> Option<InboundEvent> {
    if component.guild_id != Some(guild_id) {
        return None;
    }
    let channel_id = component.channel_id;
    let conversation = format!("discord:{guild_id}/{channel_id}");
    if !allowed.contains(&component.user.id) {
        return Some(InboundEvent::Rejected {
            conversation,
            sender: component.user.id.to_string(),
            text: "Rejected Discord button interaction".into(),
        });
    }
    let (option_id, option_text) = component_option(&component.data.custom_id, &component.message)?;
    Some(InboundEvent::Selection {
        conversation,
        bound_guid: scoped_message_id(channel_id, component.message.id),
        sender: component.user.id.to_string(),
        option_id,
        option_text,
        selected: true,
    })
}

fn component_option(data: &str, message: &Message) -> Option<(String, String)> {
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
    let label = message
        .components
        .iter()
        .flat_map(|row| &row.components)
        .find_map(|component| match component {
            serenity::all::ActionRowComponent::Button(button)
                if matches!(&button.data, serenity::all::ButtonKind::NonLink { custom_id, .. } if custom_id == data) =>
            {
                button.label.clone()
            }
            _ => None,
        })?;
    Some((index.to_string(), label))
}

fn gateway_error(error: &str) -> String {
    if error.contains("4014") || error.to_ascii_lowercase().contains("disallowed intent") {
        "Discord rejected the gateway intents (4014). Enable MESSAGE CONTENT on the Bot page in the Discord developer portal.".into()
    } else {
        format!("Discord gateway failed: {error}")
    }
}

fn parse_channel(value: &str) -> Result<ChannelId, String> {
    value
        .parse::<u64>()
        .map(ChannelId::new)
        .map_err(|_| format!("invalid Discord channel id: {value}"))
}

fn scoped_message_id(channel_id: ChannelId, message_id: MessageId) -> String {
    format!("{}:{}", channel_id.get(), message_id.get())
}

fn parse_message(value: &str, channel_id: ChannelId) -> Result<MessageId, String> {
    let (bound_channel, message) = value
        .split_once(':')
        .ok_or_else(|| format!("invalid Discord message id: {value}"))?;
    if parse_channel(bound_channel)? != channel_id {
        return Err(format!(
            "Discord message belongs to another channel: {bound_channel}"
        ));
    }
    message
        .parse::<u64>()
        .map(MessageId::new)
        .map_err(|_| format!("invalid Discord message id: {value}"))
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(author: u64, channel: u64, content: &str) -> Message {
        serde_json::from_value(serde_json::json!({
            "id": "10", "guild_id": "1", "channel_id": channel.to_string(), "author": {
                "id": author.to_string(), "username": "operator", "discriminator": "0001",
                "avatar": null, "bot": false, "public_flags": 0
            }, "content": content, "timestamp": "2026-01-01T00:00:00+00:00",
            "edited_timestamp": null, "tts": false, "mention_everyone": false,
            "mentions": [], "mention_roles": [], "attachments": [], "embeds": [],
            "pinned": false, "type": 0, "components": []
        }))
        .unwrap()
    }

    #[test]
    fn renders_asks_with_buttons_and_discord_limit() {
        let rendered = render_ask(
            &"h".repeat(1995),
            &OutboundAsk::Permission {
                request_id: "r".into(),
                summary: "Run?".into(),
            },
        );
        assert_eq!(rendered.text.chars().count(), 2000);
        let CreateActionRow::Buttons(buttons) = &rendered.components[0] else {
            panic!()
        };
        assert_eq!(buttons.len(), 2);

        let question = render_ask(
            "Context",
            &OutboundAsk::Question {
                prompt_id: "p".into(),
                question_index: 2,
                text: "Choose".into(),
                options: vec![super::super::AskOption {
                    label: "One".into(),
                    description: None,
                }],
            },
        );
        assert_eq!(question.text, "Context\n\nChoose");
        let CreateActionRow::Buttons(buttons) = &question.components[0] else {
            panic!()
        };
        assert_eq!(
            serde_json::to_value(&buttons[0]).unwrap()["custom_id"],
            "q:2:0"
        );
    }

    #[test]
    fn renders_question_option_descriptions_as_a_bounded_legend() {
        let render = |descriptions: Vec<Option<String>>| {
            render_ask(
                "",
                &OutboundAsk::Question {
                    prompt_id: "p".into(),
                    question_index: 0,
                    text: "Choose".into(),
                    options: descriptions
                        .into_iter()
                        .enumerate()
                        .map(|(index, description)| super::super::AskOption {
                            label: format!("Option {}", index + 1),
                            description,
                        })
                        .collect(),
                },
            )
        };

        let described = render(vec![
            Some("Start the flagship now".into()),
            None,
            Some("Un-park the docs work".into()),
        ]);
        assert_eq!(
            described.text,
            "Choose\n\n**Option 1** — Start the flagship now\n**Option 3** — Un-park the docs work"
        );

        let simple = render(vec![None, Some("   ".into())]);
        assert_eq!(simple.text, "Choose");

        let long = render(vec![Some("x".repeat(2500))]);
        assert_eq!(long.text.chars().count(), 2000);
        assert!(long.text.ends_with('…'));
        assert!(long.text.contains("\n\n**Option 1** — "));

        let crowded = render((0..25).map(|_| Some("description".repeat(20))).collect());
        assert_eq!(crowded.text.chars().count(), 2000);
        assert!(crowded.text.contains("**Option 25** — "));
        assert!(crowded.text.ends_with('…'));

        let long_question = render_ask(
            "",
            &OutboundAsk::Question {
                prompt_id: "p".into(),
                question_index: 0,
                text: "q".repeat(2500),
                options: vec![super::super::AskOption {
                    label: "A very long option label that still needs a legend".into(),
                    description: Some("The rationale remains visible".into()),
                }],
            },
        );
        assert_eq!(long_question.text.chars().count(), 2000);
        assert!(long_question
            .text
            .contains("\n\n**A very long option label"));
        assert!(long_question.text.contains("…** — "));
        assert!(long_question.text.ends_with('…'));

        let many_described = render_ask(
            "",
            &OutboundAsk::Question {
                prompt_id: "p".into(),
                question_index: 0,
                text: "Choose".into(),
                options: (0..100)
                    .map(|index| super::super::AskOption {
                        label: format!("Option {index} {}", "l".repeat(80)),
                        description: Some("description".repeat(20)),
                    })
                    .collect(),
            },
        );
        assert!(many_described.text.chars().count() <= 2000);
        assert!(many_described
            .text
            .starts_with("Choose\n\nReply with an option number:"));
        assert!(many_described.text.contains("**Option 0"));
        assert!(many_described
            .text
            .lines()
            .filter(|line| line.starts_with("**Option "))
            .all(|line| line
                .split_once(" — ")
                .is_some_and(|(_, description)| { !description.is_empty() })));
    }

    #[test]
    fn reports_every_missing_required_guild_permission() {
        assert_eq!(
            permission_degradation(DiscordGuildPermissions {
                manage_channels: false,
                manage_threads: false,
                send_messages_in_threads: false,
            }),
            Some("Discord bot is missing required guild permissions: MANAGE_CHANNELS, MANAGE_THREADS, SEND_MESSAGES_IN_THREADS. Update the bot role in the configured guild.".into())
        );
        assert_eq!(
            permission_degradation(DiscordGuildPermissions {
                manage_channels: true,
                manage_threads: true,
                send_messages_in_threads: true,
            }),
            None
        );
    }

    #[test]
    fn accepts_dynamic_channels_only_within_the_configured_guild() {
        let provider = DiscordProvider::new("token", GuildId::new(1), [7]);
        assert_eq!(
            provider.conversation_channel("discord:1/99").unwrap(),
            ChannelId::new(99)
        );
        assert!(provider
            .conversation_channel("discord:2/99")
            .unwrap_err()
            .contains("not configured guild 1"));
    }

    #[test]
    fn resolved_question_edit_shows_the_answer_and_removes_buttons() {
        let edit = serde_json::to_value(resolved_question_edit("✓ answered: Docs day")).unwrap();
        assert_eq!(edit["content"], "✓ answered: Docs day");
        assert_eq!(edit["components"], serde_json::json!([]));
    }

    #[test]
    fn maps_allowlisted_messages_and_component_options() {
        let allowed = HashSet::from([UserId::new(7)]);
        let bare = message(7, 99, "hello");
        assert_eq!(
            map_message(&bare, GuildId::new(1), &allowed),
            Some(InboundEvent::Bare {
                conversation: "discord:1/99".into(),
                sender: "7".into(),
                text: "hello".into()
            })
        );
        assert_eq!(
            map_message(&message(8, 99, "ignored"), GuildId::new(1), &allowed),
            Some(InboundEvent::Rejected {
                conversation: "discord:1/99".into(),
                sender: "8".into(),
                text: "ignored".into(),
            })
        );
        assert_eq!(map_message(&bare, GuildId::new(2), &allowed), None);

        let mut selected = serde_json::to_value(message(7, 99, "Choose")).unwrap();
        selected["components"] = serde_json::json!([{
            "type": 1,
            "components": [{
                "type": 2,
                "style": 1,
                "label": "Two",
                "custom_id": "q:2:1",
                "disabled": false
            }]
        }]);
        let selected: Message = serde_json::from_value(selected).unwrap();
        assert_eq!(
            component_option("q:2:1", &selected),
            Some(("1".into(), "Two".into()))
        );
        assert_eq!(
            component_option("p:approve", &selected),
            Some(("approve".into(), "Approve".into()))
        );
        assert_eq!(
            component_option("p:deny", &selected),
            Some(("deny".into(), "Deny".into()))
        );
    }

    #[test]
    fn preserves_ask_body_and_enforces_component_limits() {
        let rendered = render_ask(
            &"context".repeat(400),
            &OutboundAsk::Permission {
                request_id: "r".into(),
                summary: "The permission body must remain visible".into(),
            },
        );
        assert!(rendered
            .text
            .ends_with("The permission body must remain visible"));
        assert_eq!(rendered.text.chars().count(), 2000);

        for body_len in [1998, 1999, 2000] {
            let body = "b".repeat(body_len);
            let composed = compose_text("context", &body);
            assert_eq!(composed, body);
            assert_eq!(composed.chars().count(), body_len);
        }

        let options = (0..26)
            .map(|index| super::super::AskOption {
                label: format!("Option {index} {}", "x".repeat(100)),
                description: None,
            })
            .collect();
        let fallback = render_ask(
            "",
            &OutboundAsk::Question {
                prompt_id: "p".into(),
                question_index: 0,
                text: "Choose".into(),
                options,
            },
        );
        assert!(fallback.components.is_empty());
        assert!(fallback.text.contains("Reply with an option number"));

        let buttons = render_ask(
            "",
            &OutboundAsk::Question {
                prompt_id: "p".into(),
                question_index: 0,
                text: "Choose".into(),
                options: vec![super::super::AskOption {
                    label: "x".repeat(100),
                    description: None,
                }],
            },
        );
        let CreateActionRow::Buttons(buttons) = &buttons.components[0] else {
            panic!()
        };
        assert_eq!(
            serde_json::to_value(&buttons[0]).unwrap()["label"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            80
        );
    }

    #[test]
    fn explains_disallowed_message_content_intent() {
        assert!(gateway_error("gateway closed with code 4014").contains("Enable MESSAGE CONTENT"));
    }
}
