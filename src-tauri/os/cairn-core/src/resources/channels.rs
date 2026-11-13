use crate::{
    channels::{
        bindings::{
            canonical_conversation, BindingKind, MESSAGE_CLASS_AGENT_TEXT, MESSAGE_CLASS_NOTIFY,
            MESSAGE_CLASS_PERMISSION, MESSAGE_CLASS_QUESTION,
        },
        conversation_capabilities, default_project,
        label::{self, Sender},
        ledger, runtime_status, ConversationAddress, ConversationCapabilities,
        ConversationProvider,
    },
    mcp::types::McpCallbackRequest,
    orchestrator::Orchestrator,
};
use cairn_common::query::QueryParam;
use cairn_uri::{parse_uri, CairnResource};
use serde::Serialize;
use std::collections::HashMap;

/// The provider runtime state that is fully up.
const RUNTIME_UP: &str = "bridgeUp";
/// Running, with a gap its detail names. The router still delivers through it
/// (Discord's card-pin permission gap is decoration: cards keep updating
/// unpinned), so it admits agent messages as `bridgeUp` does.
const RUNTIME_DEGRADED: &str = "degraded";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConversationDeliverability {
    Ready,
    Degraded,
    Stopped,
}

fn message_class_names(bits: i64) -> Vec<&'static str> {
    [
        (MESSAGE_CLASS_QUESTION, "question"),
        (MESSAGE_CLASS_PERMISSION, "permission"),
        (MESSAGE_CLASS_NOTIFY, "notify"),
        (MESSAGE_CLASS_AGENT_TEXT, "agent_text"),
    ]
    .into_iter()
    .filter_map(|(bit, name)| (bits & bit != 0).then_some(name))
    .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConversationRow {
    pub address: String,
    pub label: String,
    /// The conversation's `cairn://channels/...` resource; absent on a Discord
    /// channel no binding names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// The binding kind: `structural` or `ambient`.
    pub binding: String,
    pub provider: String,
    pub deliverability: ConversationDeliverability,
    pub target: String,
    pub classes: Vec<&'static str>,
    pub capabilities: ConversationCapabilities,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub failed_count: i64,
    /// A copy-paste write that sends an agent message here, present exactly when
    /// [`ConversationRow::sendable`] holds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send: Option<String>,
    /// Set (and serialized) only on an ambient conversation: one bound to a
    /// whole project, whose classes are fixed at agent text, so the operator
    /// has no agent-text switch to flip there.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub ambient: bool,
    /// The binding's raw class bits; `classes` is their rendering.
    #[serde(skip)]
    message_classes: i64,
    /// The provider runtime's state and detail, from the same status read that
    /// computes `deliverability`.
    #[serde(skip)]
    runtime_state: &'static str,
    #[serde(skip)]
    runtime_detail: Option<String>,
}

impl ConversationRow {
    fn grants_agent_text(&self) -> bool {
        self.message_classes & MESSAGE_CLASS_AGENT_TEXT != 0
    }

    fn runtime_running(&self) -> bool {
        matches!(self.runtime_state, RUNTIME_UP | RUNTIME_DEGRADED)
    }

    /// Whether an agent may send free text here: the operator granted
    /// `agent_text` and the provider runtime is running (up or degraded).
    /// Degradation never withholds a send:
    /// neither a provider-level gap the runtime keeps delivering through nor a
    /// prior outbound failure, which refusing on would let one failure silence
    /// the conversation. Only a stopped, dead, offline, or invalid runtime does.
    /// The one predicate drives both the `send` affordance and write admission.
    pub fn sendable(&self) -> bool {
        self.grants_agent_text() && self.runtime_running()
    }

    fn with_send_affordance(mut self) -> Self {
        self.send = if self.sendable() {
            self.uri.as_deref().map(send_example)
        } else {
            None
        };
        self
    }

    /// Why an agent may not send here, or `None` when it may: the same
    /// predicate as [`ConversationRow::sendable`], in words.
    fn send_refusal(&self) -> Option<String> {
        if self.sendable() {
            return None;
        }
        if !self.grants_agent_text() {
            return Some(format!(
                "agent messages are not enabled for {} (classes: {}). Discord thread and issue channels carry them by default; only the operator can enable them for this conversation, under Settings > Channels.",
                self.label,
                if self.classes.is_empty() {
                    "none".to_string()
                } else {
                    self.classes.join(", ")
                }
            ));
        }
        Some(match self.runtime_detail.as_deref() {
            Some(detail) => format!(
                "{} is {}: {detail}; agent messages are admitted only while the provider is running",
                self.provider, self.runtime_state
            ),
            None => format!(
                "{} is {}; agent messages are admitted only while the provider is running",
                self.provider, self.runtime_state
            ),
        })
    }
}

fn send_example(uri: &str) -> String {
    format!("write({{changes:[{{target:\"{uri}\",mode:\"append\",payload:{{content:\"...\"}}}}]}})")
}

/// The operator's grant or revocation of `agent_text` on one binding.
///
/// This is the only writer of a binding's classes outside the reconciler and
/// migrations, and it is deliberately unreachable from any `cairn://` write: the
/// runner exposes it only as the `set_conversation_agent_text` invoke, which
/// refuses a caller without the desktop operator credential. An agent can
/// therefore never grant itself a conversation.
pub async fn set_agent_text(
    db: &crate::storage::LocalDb,
    conversation: &str,
    target: &str,
    granted: bool,
) -> Result<(), String> {
    let address = conversation
        .parse::<ConversationAddress>()
        .map_err(|error| format!("'{conversation}' is not a conversation address: {error}"))?;
    let provider = address.provider().id();
    if !ledger::set_message_class(
        db,
        provider,
        conversation,
        target,
        MESSAGE_CLASS_AGENT_TEXT,
        granted,
    )
    .await?
    {
        return Err(format!("{conversation} is not bound to {target}"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn row_for_status(
    address: ConversationAddress,
    target: String,
    message_classes: i64,
    binding: &str,
    state: &'static str,
    detail: Option<String>,
    last_error: Option<String>,
    failed_count: i64,
    title: Option<&str>,
) -> ConversationRow {
    let deliverability = if state == RUNTIME_UP {
        if last_error.is_some() {
            ConversationDeliverability::Degraded
        } else {
            ConversationDeliverability::Ready
        }
    } else if state == RUNTIME_DEGRADED {
        ConversationDeliverability::Degraded
    } else {
        ConversationDeliverability::Stopped
    };
    let conversation = address.to_string();
    let provider = address.provider();
    ConversationRow {
        label: label::conversation_label(
            provider,
            &conversation,
            Some(binding),
            Some(&target),
            title,
        ),
        uri: label::conversation_uri(provider, &conversation, Some(binding), Some(&target)),
        binding: binding.to_string(),
        address: conversation,
        provider: provider.id().to_string(),
        deliverability,
        target,
        classes: message_class_names(message_classes),
        capabilities: conversation_capabilities(address.provider()),
        last_error,
        failed_count,
        send: None,
        ambient: binding == BindingKind::Ambient.as_str(),
        message_classes,
        runtime_state: state,
        runtime_detail: detail,
    }
    .with_send_affordance()
}

/// The slice of a provider's runtime status that every one of its conversation
/// rows carries.
#[derive(Debug, Clone)]
struct ProviderRowStatus {
    state: &'static str,
    detail: Option<String>,
    last_send_error: Option<String>,
    failed_count: i64,
}

pub async fn configured_conversations(orch: &Orchestrator) -> Vec<ConversationRow> {
    let channels = crate::config::settings::load_settings(&orch.config_dir).channels;
    let bindings = match ledger::list_active_bindings(&orch.db.local).await {
        Ok(bindings) => bindings,
        Err(_) => return vec![],
    };
    let titles = if channels.telegram.enabled {
        conversation_titles(&bindings, ConversationProvider::Telegram).await
    } else {
        HashMap::new()
    };
    conversation_rows(
        bindings,
        &titles,
        |provider| match provider {
            ConversationProvider::IMessage => channels.imessage.enabled,
            ConversationProvider::Telegram => channels.telegram.enabled,
            ConversationProvider::Discord => channels.discord.enabled,
        },
        |provider| async move {
            let status = runtime_status(orch, provider.id()).await;
            ProviderRowStatus {
                state: status.state,
                detail: status.detail,
                last_send_error: status.last_send_error,
                failed_count: status.failed_count,
            }
        },
    )
    .await
}

/// The provider's own title for each distinct conversation `provider` is bound
/// to, keyed by address: one lookup per chat, each cached by `titles`.
async fn conversation_titles(
    bindings: &[ledger::ActiveBinding],
    provider: ConversationProvider,
) -> HashMap<String, String> {
    let mut titles = HashMap::new();
    let mut asked = std::collections::HashSet::new();
    for binding in bindings
        .iter()
        .filter(|binding| binding.provider == provider.id())
    {
        let Ok(address) = binding.conversation.parse::<ConversationAddress>() else {
            continue;
        };
        let conversation = address.to_string();
        if !asked.insert(conversation.clone()) {
            continue;
        }
        if let Some(title) =
            crate::channels::titles::conversation_title(provider, &conversation).await
        {
            titles.insert(conversation, title);
        }
    }
    titles
}

/// Project active bindings onto conversation rows. A provider's status depends
/// only on the provider, and reading it costs several database queries, so it is
/// looked up once per enabled provider and shared by all of that provider's
/// rows -- Discord alone carries a structural binding per issue thread.
/// `titles` holds the provider's own name for a conversation, by address.
async fn conversation_rows<Status, StatusFuture>(
    bindings: Vec<ledger::ActiveBinding>,
    titles: &HashMap<String, String>,
    enabled: impl Fn(ConversationProvider) -> bool,
    mut status_of: Status,
) -> Vec<ConversationRow>
where
    Status: FnMut(ConversationProvider) -> StatusFuture,
    StatusFuture: std::future::Future<Output = ProviderRowStatus>,
{
    let mut statuses = std::collections::HashMap::<ConversationProvider, ProviderRowStatus>::new();
    let mut rows = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let provider = match binding.provider.as_str() {
            "imessage" => ConversationProvider::IMessage,
            "telegram" => ConversationProvider::Telegram,
            "discord" => ConversationProvider::Discord,
            _ => continue,
        };
        if !enabled(provider) {
            continue;
        }
        let status = match statuses.get(&provider) {
            Some(status) => status.clone(),
            None => {
                let status = status_of(provider).await;
                statuses.insert(provider, status.clone());
                status
            }
        };
        let ambient = binding.binding_kind == BindingKind::Ambient.as_str();
        match binding.conversation.parse::<ConversationAddress>() {
            Ok(address) => {
                let title = titles.get(&address.to_string()).map(String::as_str);
                rows.push(row_for_status(
                    address,
                    binding.target_uri,
                    binding.message_classes,
                    &binding.binding_kind,
                    status.state,
                    status.detail,
                    status.last_send_error,
                    status.failed_count,
                    title,
                ))
            }
            Err(error) => rows.push(ConversationRow {
                address: binding.conversation,
                label: format!("invalid configuration · {}", provider.display_name()),
                uri: None,
                binding: binding.binding_kind,
                provider: provider.id().to_string(),
                deliverability: ConversationDeliverability::Stopped,
                target: binding.target_uri,
                classes: message_class_names(binding.message_classes),
                capabilities: conversation_capabilities(provider),
                last_error: Some(status.detail.unwrap_or_else(|| error.to_string())),
                failed_count: status.failed_count,
                send: None,
                ambient,
                message_classes: binding.message_classes,
                runtime_state: "invalid",
                runtime_detail: None,
            }),
        }
    }
    rows
}

/// A send admitted to a conversation: its parsed canonical address, and the
/// enumeration row that admitted it (whose `last_error`, if any, is a prior
/// outbound failure the caller reports as a warning).
#[derive(Debug, Clone)]
pub struct AdmittedSend {
    pub address: ConversationAddress,
    pub row: ConversationRow,
}

/// Who reads or writes a channel conversation. One reachability rule,
/// [`ChannelReader::reaches`], decides both what a reader may read and what it
/// may send to; [`ConversationRow::sendable`] still applies on top of it.
///
/// The operator is a reader only when the request proves it: the runner's
/// channels door ([`operator_read`], [`operator_write`]) stamps
/// [`DispatchOrigin::DesktopOperator`] after the invoke presented the desktop
/// operator credential. A request that merely speaks for no run proves nobody
/// (the machine credential, a resident process, and a project service all
/// arrive that way), so it is refused.
///
/// [`DispatchOrigin::DesktopOperator`]: cairn_common::protocol::DispatchOrigin::DesktopOperator
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelReader {
    /// The verified desktop operator: reaches every active binding.
    Operator,
    /// A thread, or a task a thread spawned: reaches its thread's channel,
    /// every conversation bound to the thread with agent text, and its
    /// project's ambient channel.
    Thread {
        home: String,
        job_id: String,
        thread: String,
        project: String,
    },
    /// An issue node or task: reaches its issue's channel and its project's
    /// ambient channel.
    Issue {
        home: String,
        job_id: String,
        issue: String,
        project: String,
    },
}

impl ChannelReader {
    pub fn from_home(home: &str, job_id: &str) -> Result<Self, String> {
        let canonical = |project: &str| cairn_uri::canonical_project(project);
        match parse_uri(home) {
            Some(CairnResource::Thread { project, name, .. }) => Ok(Self::Thread {
                home: home.to_string(),
                job_id: job_id.to_string(),
                thread: cairn_uri::canonicalize_uri_identity(&cairn_uri::build_thread_uri(
                    &project, &name,
                )),
                project: canonical(&project),
            }),
            Some(
                CairnResource::Node {
                    project, number, ..
                }
                | CairnResource::Task {
                    project, number, ..
                },
            ) => Ok(Self::Issue {
                home: home.to_string(),
                job_id: job_id.to_string(),
                issue: format!("{}/{number}", cairn_uri::build_project_uri(&canonical(&project))),
                project: canonical(&project),
            }),
            _ => Err(format!(
                "{home} is neither a thread nor an issue node, so it reaches no channel conversation"
            )),
        }
    }

    fn describe(&self) -> &str {
        match self {
            Self::Operator => "the operator",
            Self::Thread { thread, .. } => thread,
            Self::Issue { home, .. } => home,
        }
    }

    fn project(&self) -> Option<&str> {
        match self {
            Self::Operator => None,
            Self::Thread { project, .. } | Self::Issue { project, .. } => Some(project),
        }
    }

    /// Whether this reader may read, and (when the row is sendable) send to,
    /// the conversation `row` binds.
    pub fn reaches(&self, row: &ConversationRow) -> bool {
        let structural = row.binding == BindingKind::Structural.as_str();
        let home_ambient = self.project().is_some_and(|project| {
            row.ambient && row.target == cairn_uri::build_project_uri(project)
        });
        match self {
            Self::Operator => true,
            Self::Thread { thread, .. } => {
                home_ambient || (row.target == *thread && (structural || row.grants_agent_text()))
            }
            Self::Issue { issue, .. } => home_ambient || (row.target == *issue && structural),
        }
    }
}

/// The refusal a request that speaks for no run gets: nothing proves who sent
/// it, so it reaches no conversation.
const RUN_LESS_REFUSAL: &str = "Refused: this request speaks for no run and carries no operator credential, so it reaches no channel conversation. A live thread or issue node reaches its own; the operator reads them all from the desktop.";

/// The reader a channels request comes from: the verified desktop operator
/// when the channels door stamped it, otherwise the authenticated agent, which
/// must be a thread or an issue node. A request that speaks for no run is
/// refused rather than trusted.
pub async fn channel_reader(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<ChannelReader, String> {
    if request.is_desktop_operator() {
        return Ok(ChannelReader::Operator);
    }
    if request.run_id.as_deref().is_none_or(str::is_empty) || request.is_unverified_local() {
        return Err(RUN_LESS_REFUSAL.to_string());
    }
    let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
        .await
        .map_err(|error| {
            format!("Only a live thread or issue node reaches channel conversations, and this request's run could not be authenticated: {error}")
        })?;
    ChannelReader::from_home(&agent.home, &agent.job_id)
}

/// Every active binding `reader` reaches on `provider`.
pub async fn reachable(
    orch: &Orchestrator,
    reader: &ChannelReader,
    provider: ConversationProvider,
) -> Vec<ConversationRow> {
    configured_conversations(orch)
        .await
        .into_iter()
        .filter(|row| row.provider == provider.id() && reader.reaches(row))
        .collect()
}

/// Admit an agent message to a conversation [`reach_conversation`] resolved
/// for its sender: a row the sender reaches must be sendable.
pub fn admit_reached(reached: &ReachedConversation) -> Result<AdmittedSend, String> {
    admit_send_to(&reached.rows, &reached.resolved.conversation)
}

fn provider_uri_of(provider: &str) -> String {
    format!("{}/{provider}", label::CHANNELS_URI)
}

/// The admission decision over an already-read (and reachability-filtered)
/// enumeration. Every refusal names what would have been accepted.
fn admit_send_to(rows: &[ConversationRow], conversation: &str) -> Result<AdmittedSend, String> {
    let address = conversation
        .parse::<ConversationAddress>()
        .map_err(|error| format!("'{conversation}' is not a conversation address ({error})"))?;
    let canonical = canonical_conversation(address.provider().id(), conversation)
        .map_err(|error| format!("'{conversation}': {error}"))?;

    let bound = rows
        .iter()
        .filter(|row| row.address == canonical)
        .collect::<Vec<_>>();
    if let Some(row) = bound.iter().find(|row| row.sendable()) {
        return Ok(AdmittedSend {
            address,
            row: (*row).clone(),
        });
    }
    let Some(row) = bound
        .iter()
        .find(|row| row.grants_agent_text())
        .or_else(|| bound.first())
    else {
        let mut sendable = rows
            .iter()
            .filter(|row| row.sendable())
            .filter_map(|row| row.uri.as_deref())
            .collect::<Vec<_>>();
        sendable.dedup();
        let accepted = if sendable.is_empty() {
            "no conversation is currently sendable".to_string()
        } else {
            format!("sendable conversations: {}", sendable.join(", "))
        };
        return Err(format!(
            "this conversation has no active binding on an enabled provider, and agent messages go only to bound conversations; {accepted} (read {})",
            provider_uri_of(address.provider().id())
        ));
    };
    Err(row
        .send_refusal()
        .unwrap_or_else(|| "this conversation is not sendable".to_string()))
}

/// Refuse content the provider would truncate, given the overflow its own
/// renderer reports for the message as it will actually be sent (attribution
/// line included). Providers cut over-long text rather than splitting it, so
/// admitting it would silently drop the tail while the send still reports
/// success.
pub fn check_send_length(
    provider: &str,
    overflow: usize,
    max_text_len: Option<usize>,
) -> Result<(), String> {
    if overflow == 0 {
        return Ok(());
    }
    let limit = max_text_len
        .map(|limit| format!(" {limit}-character"))
        .unwrap_or_default();
    Err(format!(
        "payload.content does not fit {provider}'s{limit} message limit once rendered under its attribution line; the provider would cut the last {overflow} characters, so shorten it by at least {overflow} characters"
    ))
}

const PROVIDERS: [ConversationProvider; 3] = [
    ConversationProvider::Discord,
    ConversationProvider::IMessage,
    ConversationProvider::Telegram,
];

/// The in-app channel's provider word. It is a row on `cairn://channels` with a
/// purpose line like any other, but not a transport: it has no conversations,
/// and the messages it carries are the operator's own, at `cairn://operator`.
pub const APP_PROVIDER: &str = "app";

const OPERATOR_URI: &str = "cairn://operator";

/// The refusal for a conversation path under the app channel.
fn app_has_no_conversations() -> String {
    format!(
        "{}/{APP_PROVIDER} is the in-app channel and has no conversations: messages to the operator, whichever channel carries them, are at {OPERATOR_URI}.",
        label::CHANNELS_URI
    )
}

fn provider_named(provider: &str) -> Result<ConversationProvider, String> {
    if provider == APP_PROVIDER {
        return Err(app_has_no_conversations());
    }
    provider.parse::<ConversationProvider>().map_err(|_| {
        format!(
            "Unknown channel provider '{provider}'; expected app, discord, imessage, or telegram"
        )
    })
}

/// An epoch-milliseconds instant as `2026-11-12 05:29:18.189Z`.
pub(crate) fn at_ms(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|at| at.format("%Y-%m-%d %H:%M:%S%.3fZ").to_string())
        .unwrap_or_else(|| ms.to_string())
}

/// A serde enum's wire word (`present`, `bridgeUp`).
fn word<T: Serialize + std::fmt::Debug>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{value:?}"))
}

fn provider_purpose(
    channels: &crate::models::ChannelsConfig,
    provider: ConversationProvider,
) -> (bool, Option<String>) {
    match provider {
        ConversationProvider::Discord => {
            (channels.discord.enabled, channels.discord.purpose.clone())
        }
        ConversationProvider::IMessage => {
            (channels.imessage.enabled, channels.imessage.purpose.clone())
        }
        ConversationProvider::Telegram => {
            (channels.telegram.enabled, channels.telegram.purpose.clone())
        }
    }
}

fn health_line(state: &str, detail: Option<&str>) -> String {
    match detail {
        Some(detail) => format!("{state}: {detail}"),
        None => state.to_string(),
    }
}

/// How the operator sets a purpose line, for a channel that has none.
pub const PURPOSE_HOW: &str =
    "the operator sets one with patch {provider, purpose} on cairn://channels, from the desktop";

/// One channel provider as `cairn://channels` and `cairn://operator` both
/// render it, so the two can never describe a channel differently.
#[derive(Debug, Clone)]
pub struct ProviderSummary {
    /// `app`, `discord`, `imessage`, or `telegram`.
    pub provider: &'static str,
    pub display_name: &'static str,
    /// `cairn://channels/{provider}`.
    pub uri: String,
    pub enabled: bool,
    /// Whether the operator's own conversation on it is configured (iMessage
    /// `to`, Telegram `chat_id`, Discord `guild_id`); always true for the app.
    pub configured: bool,
    pub purpose: Option<String>,
    pub health: String,
    pub presence: String,
    /// What last-seen means on this channel, and when, in epoch milliseconds.
    pub last_seen_label: &'static str,
    pub last_seen_ms: Option<i64>,
}

impl ProviderSummary {
    pub fn last_seen(&self) -> String {
        format!(
            "{}: {}",
            self.last_seen_label,
            self.last_seen_ms
                .map(at_ms)
                .unwrap_or_else(|| "never".to_string())
        )
    }
}

/// The app channel's last-seen: when the operator last moved his headline
/// cursor on any thread, which is the app's one record of him reading.
async fn last_read_headlines_ms(orch: &Orchestrator) -> Option<i64> {
    orch.db
        .local
        .query_opt(
            "SELECT MAX(seen_at) FROM headline_read_cursors WHERE reader = ?1",
            cairn_db::turso::params![crate::threads::headlines::OPERATOR_READER],
            |row| {
                use crate::storage::RowExt;
                row.opt_i64(0)
            },
        )
        .await
        .ok()
        .flatten()
        .flatten()
        .map(|seconds| seconds * 1000)
}

/// Every channel provider's summary: the app first, then the transports.
pub async fn provider_summaries(orch: &Orchestrator) -> Vec<ProviderSummary> {
    let channels = crate::config::settings::load_settings(&orch.config_dir).channels;
    let app_presence = crate::channels::operator_presence_status().await;
    let mut summaries = vec![ProviderSummary {
        provider: APP_PROVIDER,
        display_name: "App",
        uri: format!("{}/{APP_PROVIDER}", label::CHANNELS_URI),
        enabled: true,
        configured: true,
        purpose: channels.app.purpose.clone(),
        health: "in-app".to_string(),
        presence: format!(
            "{} (mode {}); inferred from the desktop",
            word(&app_presence.presence),
            word(&app_presence.mode)
        ),
        last_seen_label: "last read headlines",
        last_seen_ms: last_read_headlines_ms(orch).await,
    }];
    for provider in PROVIDERS {
        let (enabled, purpose) = provider_purpose(&channels, provider);
        let configured = match provider {
            ConversationProvider::Discord => !channels.discord.guild_id.trim().is_empty(),
            ConversationProvider::IMessage => !channels.imessage.to.trim().is_empty(),
            ConversationProvider::Telegram => !channels.telegram.chat_id.trim().is_empty(),
        };
        let status = runtime_status(orch, provider.id()).await;
        let presence = format!(
            "{} (mode {})",
            word(&status.operator_presence),
            word(&status.presence_mode)
        );
        let presence = if provider == ConversationProvider::IMessage {
            format!("{presence}; read through the local iMessage presence probe when the desktop reports none")
        } else {
            presence
        };
        summaries.push(ProviderSummary {
            provider: provider.id(),
            display_name: provider.display_name(),
            uri: label::provider_uri(provider),
            enabled,
            configured,
            purpose,
            health: health_line(status.state, status.detail.as_deref()),
            presence,
            last_seen_label: "last operator message",
            last_seen_ms: ledger::last_operator_message(&orch.db.local, provider.id())
                .await
                .ok()
                .flatten(),
        });
    }
    summaries
}

/// `cairn://channels`: one section per provider. Presence and when the
/// operator last wrote are his, so the root answers the same readers the
/// provider and conversation renders do: a run-less caller is refused.
pub async fn render_channels(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    params: &[QueryParam],
) -> String {
    if let Err(refusal) = channel_reader(orch, request).await {
        return refusal;
    }
    if let Some(param) = params.first() {
        return format!(
            "Unsupported query parameter '{}' for {}",
            param.key,
            label::CHANNELS_URI
        );
    }
    let mut out = String::from(
        "# Channels\n\nThe channel providers: the in-app channel and the external transports. Each transport lists the conversations you can reach at its link; read a conversation for its messages and its send. The purpose line says which messages to the operator belong on it (cairn://operator).\n",
    );
    for summary in provider_summaries(orch).await {
        let conversations = if summary.provider == APP_PROVIDER {
            format!("none; messages to the operator are at {OPERATOR_URI}")
        } else {
            summary.uri.clone()
        };
        out.push_str(&format!(
            "\n## {}\n\n- conversations: {conversations}\n- enabled: {}\n- health: {}\n- purpose: {}\n- operator presence: {}\n- {}\n",
            summary.display_name,
            if summary.enabled { "yes" } else { "no" },
            summary.health,
            summary
                .purpose
                .clone()
                .unwrap_or_else(|| format!("none set ({PURPOSE_HOW})")),
            summary.presence,
            summary.last_seen(),
        ));
    }
    out.push_str(&unaddressed_text_section(orch).await);
    out
}

/// How the operator sets the default project.
const DEFAULT_PROJECT_HOW: &str = "the operator sets it under Settings > Channels > Default project, or with patch {defaultProject} on cairn://channels from the desktop; empty clears it";

/// The `cairn://channels` section saying where bare phone text goes, ending
/// with the general thread the third rule resolves to right now.
async fn unaddressed_text_section(orch: &Orchestrator) -> String {
    let resolved = match default_project::resolve(orch).await {
        Err(error) => format!("unknown; the default project could not be read: {error}"),
        Ok(None) => {
            "none; the workspace has no project, so unaddressed text is refused".to_string()
        }
        Ok(Some(project)) => {
            let uri = default_project::general_thread_uri(&project.key);
            let source = format!(
                "default project: {}, {}",
                project.key,
                project.source.word()
            );
            match default_project::open_general_thread(orch, &project.key).await {
                Ok(true) => format!("{uri} ({source})"),
                Ok(false) => format!(
                    "none; {uri} is missing or closed ({source}), so unaddressed text is refused with no_default_thread"
                ),
                Err(error) => format!("{uri} ({source}); whether it is open could not be read: {error}"),
            }
        }
    };
    format!(
        "\n## Unaddressed text\n\nBare text the operator sends on iMessage or Telegram goes to the first of:\n\n1. the message it natively replies to (reply-to wins);\n2. else the thread whose operator message on that transport is the latest within 24 h;\n3. else the default project's general thread.\n\nA single outstanding question or permission ask from the same handle still takes bare text first.\n\n- general thread: {resolved}\n- default project: {DEFAULT_PROJECT_HOW}\n"
    )
}

/// `cairn://channels/app`: the app channel's row, pointing at where its
/// messages are.
async fn render_app_provider(orch: &Orchestrator) -> String {
    let summaries = provider_summaries(orch).await;
    let app = summaries
        .iter()
        .find(|summary| summary.provider == APP_PROVIDER)
        .expect("the app channel is always summarized");
    format!(
        "# App channel\n\nThe in-app channel: a message to the operator sent through it stays in Cairn and reaches him in the app. It has no conversations; messages to the operator, whichever channel carries them, are at {OPERATOR_URI}.\n\n- purpose: {}\n- operator presence: {}\n- {}\n",
        app.purpose
            .clone()
            .unwrap_or_else(|| format!("none set, so it is not offered ({PURPOSE_HOW})")),
        app.presence,
        app.last_seen(),
    )
}

/// The operator's `patch {provider, purpose}` or `patch {defaultProject}` on
/// `cairn://channels`. Only a request the channels door stamped as the verified
/// desktop operator may make either. An empty value clears it.
pub async fn set_purpose(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    if !request.is_desktop_operator() {
        return Err("Only the operator may set a channel provider's purpose or the default project, from the desktop; agents read them on cairn://channels.".to_string());
    }
    if let Some(value) = payload.get("defaultProject") {
        return set_default_project(orch, value, dry_run).await;
    }
    let provider = payload
        .get("provider")
        .and_then(serde_json::Value::as_str)
        .ok_or("payload.provider is required: app, discord, imessage, or telegram")?;
    let (provider_id, display_name) = if provider == APP_PROVIDER {
        (APP_PROVIDER, "App")
    } else {
        let provider = provider_named(provider)?;
        (provider.id(), provider.display_name())
    };
    let purpose = match payload.get("purpose") {
        Some(serde_json::Value::String(purpose)) => {
            Some(purpose.trim()).filter(|purpose| !purpose.is_empty())
        }
        Some(serde_json::Value::Null) => None,
        _ => {
            return Err(
                "payload.purpose is required and must be a string; an empty string clears it"
                    .to_string(),
            )
        }
    };
    let verb = if purpose.is_some() { "Set" } else { "Cleared" };
    if dry_run {
        return Ok(format!(
            "Would {} {display_name}'s purpose.",
            verb.to_lowercase(),
        ));
    }
    let purpose = purpose.map(str::to_string);
    crate::config::settings::mutate_workspace_settings(
        &orch.config_dir,
        &format!("channels: {provider_id} purpose"),
        |document| {
            use serde_yaml::{Mapping, Value};
            let mapping = |value: &mut Value, key: &str| -> Result<(), String> {
                if value.is_null() {
                    *value = Value::Mapping(Mapping::new());
                }
                value
                    .is_mapping()
                    .then_some(())
                    .ok_or_else(|| format!("settings key {key} is not a mapping"))
            };
            let channels = document
                .entry(Value::from("channels"))
                .or_insert(Value::Null);
            mapping(channels, "channels")?;
            let entry = channels
                .as_mapping_mut()
                .expect("checked mapping")
                .entry(Value::from(provider_id))
                .or_insert(Value::Null);
            mapping(entry, provider_id)?;
            let entry = entry.as_mapping_mut().expect("checked mapping");
            match purpose {
                Some(purpose) => {
                    entry.insert(Value::from("purpose"), Value::from(purpose));
                }
                None => {
                    entry.remove(Value::from("purpose"));
                }
            }
            Ok(())
        },
    )?;
    Ok(format!(
        "{verb} {display_name}'s purpose; read {} to see it.",
        label::CHANNELS_URI
    ))
}

/// Every project key in the workspace, canonical and sorted.
async fn project_keys(orch: &Orchestrator) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    for db in orch.db.all_dbs().await {
        keys.extend(
            db.query_all("SELECT key FROM projects", (), |row| {
                use crate::storage::RowExt;
                row.text(0)
            })
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(cairn_uri::canonical_project),
        );
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

/// The operator's `patch {defaultProject}`: an existing project's canonical
/// key, or an empty string to clear it back to the oldest project. Writing it
/// also drops the retired `defaultThread`, which would otherwise be converted
/// again on the next load.
async fn set_default_project(
    orch: &Orchestrator,
    value: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    let requested =
        match value {
            serde_json::Value::String(key) => {
                Some(cairn_uri::canonical_project(key.trim())).filter(|key| !key.is_empty())
            }
            serde_json::Value::Null => None,
            _ => return Err(
                "payload.defaultProject must be a project key string; an empty string clears it"
                    .to_string(),
            ),
        };
    if let Some(key) = &requested {
        let known = project_keys(orch).await?;
        if !known.contains(key) {
            return Err(format!(
                "Unknown project '{key}': defaultProject must name a project in this workspace ({})",
                if known.is_empty() {
                    "there are none".to_string()
                } else {
                    format!("known: {}", known.join(", "))
                }
            ));
        }
    }
    if dry_run {
        return Ok(match &requested {
            Some(key) => format!("Would set the default project to {key}."),
            None => "Would clear the default project.".to_string(),
        });
    }
    crate::config::settings::mutate_workspace_settings(
        &orch.config_dir,
        "channels: default project",
        |document| {
            use serde_yaml::{Mapping, Value};
            let channels = document
                .entry(Value::from("channels"))
                .or_insert(Value::Null);
            if channels.is_null() {
                *channels = Value::Mapping(Mapping::new());
            }
            let channels = channels
                .as_mapping_mut()
                .ok_or("settings key channels is not a mapping")?;
            channels.remove(Value::from("defaultThread"));
            match &requested {
                Some(key) => {
                    channels.insert(Value::from("defaultProject"), Value::from(key.as_str()));
                }
                None => {
                    channels.remove(Value::from("defaultProject"));
                }
            }
            Ok(())
        },
    )?;
    Ok(match requested {
        Some(key) => format!(
            "Set the default project to {key}; read {} to see it.",
            label::CHANNELS_URI
        ),
        None => format!(
            "Cleared the default project, so it is the oldest project; read {} to see it.",
            label::CHANNELS_URI
        ),
    })
}

/// The request the channels door makes its reads and writes with: no run, and
/// the verified desktop operator as its origin. The door builds it only after
/// the invoke presented the desktop operator credential.
fn operator_request(tool: &str) -> McpCallbackRequest {
    McpCallbackRequest {
        tool: tool.to_string(),
        origin: Some(cairn_common::protocol::DispatchOrigin::DesktopOperator),
        ..Default::default()
    }
}

/// The resource a door target names, or the refusal for one the door does not
/// open: the channels tree and the operator's own messages.
fn channels_target(target: &str) -> Result<(CairnResource, Vec<QueryParam>), String> {
    let split = cairn_common::query::split_target_query(target)?;
    let resource = parse_uri(&split.identity)
        .filter(|resource| {
            matches!(
                resource,
                CairnResource::Channels
                    | CairnResource::ChannelsProvider { .. }
                    | CairnResource::ChannelsConversation { .. }
                    | CairnResource::ChannelsAmbientFeedback
                    | CairnResource::Operator
                    | CairnResource::OperatorMessage { .. }
            )
        })
        .ok_or_else(|| {
            format!("{target} is not a channels resource; the operator's channels door reads and writes cairn://channels, the conversations under it, the ambient routing labels, and cairn://operator and its messages")
        })?;
    Ok((resource, split.params))
}

/// The verified desktop operator reading the channels tree (every provider,
/// every active binding, every conversation's history) or his own messages.
/// A read here writes nothing: the app marks a message seen with its own
/// [`operator_write`] once it has rendered it. The caller has already checked
/// the desktop operator credential.
pub async fn operator_read(orch: &Orchestrator, target: &str) -> Result<String, String> {
    let (resource, params) = channels_target(target)?;
    let request = operator_request("read_batch");
    Ok(match resource {
        CairnResource::Channels => render_channels(orch, &request, &params).await,
        CairnResource::ChannelsProvider { provider } => {
            render_provider(orch, &request, &provider, &params).await
        }
        CairnResource::ChannelsConversation { provider, path } => {
            render_conversation(orch, &request, &provider, &path, &params).await
        }
        CairnResource::ChannelsAmbientFeedback => render_ambient_feedback(orch, &params).await,
        CairnResource::Operator => {
            crate::resources::operator::render_operator(orch, &request, &params).await
        }
        CairnResource::OperatorMessage { id } => {
            crate::resources::operator::render_message(orch, &request, &id).await
        }
        _ => unreachable!("channels_target admits only the channels tree and the operator"),
    })
}

/// The verified desktop operator writing under `cairn://channels`, through the
/// same gate and dispatch an agent's write takes. The caller has already
/// checked the desktop operator credential.
pub async fn operator_write(
    orch: &Orchestrator,
    target: &str,
    mode: crate::mcp::types::ChangeMode,
    payload: Option<serde_json::Value>,
) -> Result<String, String> {
    channels_target(target)?;
    let item = crate::mcp::types::ChangeItem {
        target: target.to_string(),
        mode,
        payload,
    };
    crate::resources::mutations::dispatch_resource_change(
        orch,
        &operator_request("write"),
        0,
        &item,
        false,
    )
    .await
    .map(|applied| applied.summary)
    .map_err(|failure| failure.error)
}

pub(crate) fn bounded_limit(
    params: &[QueryParam],
    uri: &str,
    default: i64,
    max: i64,
) -> Result<i64, String> {
    match params.iter().find(|param| param.key == "limit") {
        Some(param) => param
            .value
            .trim()
            .parse::<i64>()
            .map(|limit| limit.clamp(1, max))
            .map_err(|_| {
                format!(
                    "limit must be a positive number for {uri}; got '{}'",
                    param.value
                )
            }),
        None => Ok(default),
    }
}

/// The canonical form of a conversation string an outbound or inbound row
/// carries: a route row on Telegram or iMessage is keyed by the bare chat id
/// or handle.
fn canonical_row_conversation(provider: ConversationProvider, conversation: &str) -> String {
    if conversation.contains(':') {
        conversation.to_string()
    } else {
        format!("{}:{conversation}", provider.id())
    }
}

/// `cairn://channels/{provider}`: the conversations the reader can reach,
/// newest activity first.
pub async fn render_provider(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    provider: &str,
    params: &[QueryParam],
) -> String {
    let listing = match channel_reader(orch, request).await {
        Ok(reader) => provider_listing(orch, &reader, provider, params).await,
        Err(refusal) => Err(refusal),
    };
    match listing {
        Ok(body) | Err(body) => body,
    }
}

async fn provider_listing(
    orch: &Orchestrator,
    reader: &ChannelReader,
    provider: &str,
    params: &[QueryParam],
) -> Result<String, String> {
    if provider == APP_PROVIDER {
        return Ok(render_app_provider(orch).await);
    }
    let provider = provider_named(provider)?;
    let uri = label::provider_uri(provider);
    if let Some(param) = params
        .iter()
        .find(|param| !matches!(param.key.as_str(), "limit" | "project"))
    {
        return Err(format!(
            "Unsupported query parameter '{}' for {uri}",
            param.key
        ));
    }
    let limit = bounded_limit(params, &uri, 100, 1000)?;
    let project = params
        .iter()
        .find(|param| param.key == "project")
        .map(|param| cairn_uri::canonical_project(param.value.trim()));
    let mut rows = reachable(orch, reader, provider)
        .await
        .into_iter()
        .filter(|row| {
            project.as_deref().is_none_or(|project| {
                parse_uri(&row.target).and_then(|target| target.project().map(str::to_string))
                    == Some(project.to_string())
            })
        })
        .collect::<Vec<_>>();
    let activity = ledger::last_activity(&orch.db.local, provider.id())
        .await
        .unwrap_or_default()
        .into_iter()
        .fold(
            std::collections::HashMap::<String, i64>::new(),
            |mut latest, (conversation, at)| {
                let key = canonical_row_conversation(provider, &conversation);
                let entry = latest.entry(key).or_insert(at);
                *entry = (*entry).max(at);
                latest
            },
        );
    rows.sort_by(|a, b| {
        activity
            .get(&b.address)
            .cmp(&activity.get(&a.address))
            .then_with(|| a.label.cmp(&b.label))
    });
    let total = rows.len();
    rows.truncate(limit as usize);
    let status = runtime_status(orch, provider.id()).await;
    let mut out = format!(
        "# {} conversations\n\n{total} conversation{} {} can reach{}. Provider health: {}.\n",
        provider.display_name(),
        if total == 1 { "" } else { "s" },
        reader.describe(),
        if total > rows.len() {
            format!(", newest {} shown (raise ?limit=)", rows.len())
        } else {
            String::new()
        },
        health_line(status.state, status.detail.as_deref()),
    );
    for row in &rows {
        out.push_str(&format!(
            "\n- {} — {}\n  address: {} · binding: {} · target: {} · classes: {} · last activity: {}\n  {}\n",
            row.label,
            row.uri.as_deref().unwrap_or("(no resource)"),
            row.address,
            row.binding,
            row.target,
            if row.classes.is_empty() {
                "none".to_string()
            } else {
                row.classes.join(", ")
            },
            activity
                .get(&row.address)
                .map(|at| at_ms(*at))
                .unwrap_or_else(|| "none".to_string()),
            match (&row.send, row.send_refusal()) {
                (Some(send), _) => format!("send (agent_text): {send}"),
                (None, Some(refusal)) => format!("send: not available; {refusal}"),
                (None, None) => "send: not available; this conversation has no resource".to_string(),
            }
        ));
    }
    Ok(out)
}

/// `cairn://channels/{provider}/{conversation}`: one conversation's header and
/// its messages, oldest first.
pub async fn render_conversation(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    provider: &str,
    path: &[String],
    params: &[QueryParam],
) -> String {
    let view = match channel_reader(orch, request).await {
        Ok(reader) => conversation_view(orch, &reader, provider, path, params).await,
        Err(refusal) => Err(refusal),
    };
    match view {
        Ok(body) | Err(body) => body,
    }
}

/// A conversation URI resolved, with the rows the reader reaches it through.
pub struct ReachedConversation {
    pub resolved: label::ResolvedConversation,
    pub uri: String,
    pub rows: Vec<ConversationRow>,
}

impl ReachedConversation {
    /// The row the conversation is presented and sent through: a sendable one
    /// when there is one, else its first binding.
    pub fn row(&self) -> Option<&ConversationRow> {
        self.rows
            .iter()
            .find(|row| row.sendable())
            .or_else(|| self.rows.first())
    }
}

/// A resolved conversation's label, carrying the provider's own title for it.
async fn titled_label(resolved: &label::ResolvedConversation) -> String {
    let title =
        crate::channels::titles::conversation_title(resolved.provider, &resolved.conversation)
            .await;
    resolved.label(title.as_deref())
}

/// Resolve a conversation URI and refuse it unless `reader` reaches it.
pub async fn reach_conversation(
    orch: &Orchestrator,
    reader: &ChannelReader,
    provider: &str,
    path: &[String],
) -> Result<ReachedConversation, String> {
    let provider = provider_named(provider)?;
    let resolved = label::resolve_conversation_uri(&orch.db.local, provider, path).await?;
    let uri = CairnResource::ChannelsConversation {
        provider: provider.id().to_string(),
        path: path.to_vec(),
    }
    .to_uri();
    let bound = configured_conversations(orch)
        .await
        .into_iter()
        .filter(|row| row.address == resolved.conversation)
        .collect::<Vec<_>>();
    let rows = bound
        .iter()
        .filter(|row| reader.reaches(row))
        .cloned()
        .collect::<Vec<_>>();
    if bound.is_empty() {
        return Err(format!(
            "{} ({uri}) has no active binding on an enabled provider, so no agent reaches it; read {} for the conversations you can reach.",
            titled_label(&resolved).await,
            label::provider_uri(provider)
        ));
    }
    if rows.is_empty() {
        return Err(format!(
            "{} ({uri}) is not a conversation {} can reach. A thread reaches its own channel, conversations bound to it with agent text, and its project's #ambient; an issue node reaches its issue's channel and its project's #ambient. Read {} for the conversations you can reach.",
            titled_label(&resolved).await,
            reader.describe(),
            label::provider_uri(provider)
        ));
    }
    Ok(ReachedConversation {
        resolved,
        uri,
        rows,
    })
}

/// `default_project` is the default project as it resolves now, which is what
/// a `no_default_thread` rejection names.
fn inbound_rejection(reason: &str, default_project: Option<&str>) -> String {
    let why = match reason {
        "allowlist" => "the sender is not on this provider's allowlist".to_string(),
        "policy" => "this provider's inbound capabilities refuse it".to_string(),
        "unmatched" => "it replied to nothing Cairn is waiting on".to_string(),
        "unbound_conversation" => "this channel is bound to no Cairn thread".to_string(),
        "no_default_thread" => match default_project {
            Some(project) => {
                format!("the default project {project} has no open general thread")
            }
            None => "the workspace has no project to be the default".to_string(),
        },
        _ => String::new(),
    };
    if why.is_empty() {
        format!("rejected: {reason}")
    } else {
        format!("rejected: {reason} ({why})")
    }
}

/// The typed address an ambient message opened with, as a reader knows it:
/// a Discord channel mention `<#id>` becomes `#name` through its binding.
async fn render_addressed_by(
    db: &crate::storage::LocalDb,
    conversation: &str,
    addressed_by: &str,
) -> String {
    let guild = conversation
        .strip_prefix("discord:")
        .and_then(|rest| rest.split_once('/'))
        .map(|(guild, _)| guild);
    let mut tokens = Vec::new();
    for token in addressed_by.split_whitespace() {
        let channel = token
            .strip_prefix("<#")
            .and_then(|token| token.strip_suffix('>'));
        let named = match (guild, channel) {
            (Some(guild), Some(channel)) => ledger::lookup_conversation_target(
                db,
                "discord",
                &format!("discord:{guild}/{channel}"),
            )
            .await
            .ok()
            .flatten()
            .map(|target| {
                let project = parse_uri(&target)
                    .and_then(|resource| resource.project().map(str::to_string))
                    .unwrap_or_default();
                format!(
                    "#{}",
                    crate::channels::label::discord_channel_name(Some(&target), &project, false)
                )
            }),
            _ => None,
        };
        tokens.push(named.unwrap_or_else(|| token.to_string()));
    }
    tokens.join(" ")
}

/// An outbound row as an inbound's reason names it: an operator message by its
/// `cairn://operator` URI, anything else by its `out` id, with when it was
/// sent. `None` when the row is no longer in the ledger.
async fn outbound_reference(
    db: &crate::storage::LocalDb,
    outbound: &str,
) -> (String, Option<String>) {
    let record = ledger::get_by_id(db, outbound).await.ok().flatten();
    let Some(record) = record else {
        return (format!("out {outbound}"), None);
    };
    let label = (record.kind == crate::channels::operator_message::KIND)
        .then(|| crate::channels::operator_message::message_id(&record.binding_ref))
        .flatten()
        .map(|id| format!("{OPERATOR_URI}/{id}"))
        .unwrap_or_else(|| format!("out {}", record.id));
    let time = record.sent_at.map(|sent| format!("sent {}", at_ms(sent)));
    (label, time)
}

/// Why an inbound reached `target`, from its `addressed_by` token.
async fn inbound_reason(
    db: &crate::storage::LocalDb,
    provider: ConversationProvider,
    conversation: &str,
    target: &str,
    addressed_by: &str,
) -> String {
    match ledger::InboundAddress::parse(addressed_by) {
        ledger::InboundAddress::Typed(typed) => format!(
            "delivered to {target}, addressed by {}",
            render_addressed_by(db, conversation, &typed).await
        ),
        ledger::InboundAddress::Reply { outbound } => {
            let (label, time) = outbound_reference(db, &outbound).await;
            match time {
                Some(time) => format!("delivered to {target}, as your reply to {label} ({time})"),
                None => format!("delivered to {target}, as your reply to {label}"),
            }
        }
        ledger::InboundAddress::RecentOperatorMessage { outbound } => {
            let (label, time) = outbound_reference(db, &outbound).await;
            format!(
                "delivered to {target}: no reply-to, and {target} sent you the latest operator message on {} within 24 h ({})",
                provider.id(),
                time.map(|time| format!("{label}, {time}")).unwrap_or(label)
            )
        }
        ledger::InboundAddress::DefaultProject { .. } => format!(
            "delivered to {target}: no reply-to and no operator message on {} in the last 24 h, so the default project's general thread",
            provider.id()
        ),
    }
}

fn quoted(text: &str) -> String {
    text.lines()
        .map(|line| format!("  > {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn outbound_sender(message: &ledger::ConversationMessage) -> String {
    let binding_ref = message.reference.as_deref().unwrap_or_default();
    let sender = if message.sender == crate::channels::agent_message::KIND {
        crate::channels::agent_message::sender_home(binding_ref)
            .map(Sender::Agent)
            .unwrap_or(Sender::Cairn)
    } else if let Some(route) = binding_ref
        .strip_prefix("route:")
        .and_then(|rest| rest.split(':').next())
    {
        Sender::Route(route)
    } else {
        Sender::Cairn
    };
    label::sender_label(sender)
}

/// How long a presence-aware route notice may be held while the operator is at
/// the desktop: the router's attention grace.
const ATTENTION_GRACE_MS: i64 = 3 * 60 * 1000;
const MAX_ATTEMPTS: i64 = 6;

fn delivery_state(message: &ledger::ConversationMessage) -> String {
    let attempts = message.attempts.unwrap_or(0);
    match message.state.as_deref().unwrap_or("pending") {
        "pending" if message.sender == "route" && attempts == 0 => format!(
            "pending (an undirected route notice may be held for operator presence until {})",
            at_ms(message.at + ATTENTION_GRACE_MS)
        ),
        "pending" => "pending".to_string(),
        "sent" => match message.sent_at {
            Some(sent) => format!("sent {}", at_ms(sent)),
            None => "sent".to_string(),
        },
        "cleanup_pending" => format!(
            "sent {}; its cleanup is pending",
            message.sent_at.map(at_ms).unwrap_or_default()
        ),
        "failed" => {
            let error = message.detail.as_deref().unwrap_or("no error recorded");
            match message.next_attempt_at {
                Some(next) if attempts < MAX_ATTEMPTS => format!(
                    "failed (attempt {attempts}/{MAX_ATTEMPTS}): {error}, retry at {}",
                    at_ms(next)
                ),
                _ => {
                    format!("failed (attempt {attempts}/{MAX_ATTEMPTS}): {error}; no further retry")
                }
            }
        }
        "resolved" => match message.resolved_at {
            Some(resolved) => format!("resolved {}", at_ms(resolved)),
            None => "resolved".to_string(),
        },
        other => other.to_string(),
    }
}

fn firing_line(
    firing: &ledger::InboundFiring,
    acknowledged: Option<&ledger::ConversationMessage>,
) -> String {
    let mut line = format!("    - route {}: {}", firing.route_id, firing.status);
    if let Some(reason) = &firing.drop_reason {
        line.push_str(&format!(" ({reason})"));
    }
    if let Some(error) = &firing.error {
        line.push_str(&format!(": {error}"));
    }
    match acknowledged {
        Some(ack) => line.push_str(&format!(
            " → acknowledgement {}, created {}, {}",
            ack.id,
            at_ms(ack.at),
            delivery_state(ack)
        )),
        None => {
            if let Some(id) = firing
                .sink_ref
                .as_deref()
                .and_then(|sink| sink.strip_prefix("channel_outbound:"))
            {
                line.push_str(&format!(" → acknowledgement {id}"));
            } else if let Some(target) = firing
                .sink_ref
                .as_deref()
                .filter(|sink| sink.starts_with("cairn://"))
            {
                line.push_str(&format!(" → {} {target}", firing.sink_kind));
            }
        }
    }
    line
}

async fn conversation_view(
    orch: &Orchestrator,
    reader: &ChannelReader,
    provider: &str,
    path: &[String],
    params: &[QueryParam],
) -> Result<String, String> {
    let reached = reach_conversation(orch, reader, provider, path).await?;
    let uri = reached.uri.clone();
    if let Some(param) = params
        .iter()
        .find(|param| !matches!(param.key.as_str(), "since" | "limit" | "direction"))
    {
        return Err(format!(
            "Unsupported query parameter '{}' for {uri}",
            param.key
        ));
    }
    let limit = bounded_limit(params, &uri, 50, 500)?;
    let since_ms = match params.iter().find(|param| param.key == "since") {
        Some(param) => param
            .value
            .trim()
            .parse::<i64>()
            .map(|seconds| seconds.saturating_mul(1000))
            .map_err(|_| {
                format!(
                    "since must be epoch seconds for {uri}; got '{}'",
                    param.value
                )
            })?,
        None => 0,
    };
    let direction = match params
        .iter()
        .find(|param| param.key == "direction")
        .map(|param| param.value.trim())
    {
        None | Some("all") => ledger::DirectionFilter::All,
        Some("in") => ledger::DirectionFilter::In,
        Some("out") => ledger::DirectionFilter::Out,
        Some(other) => {
            return Err(format!(
                "Unknown direction '{other}' for {uri}; expected in, out, or all"
            ))
        }
    };
    let resolved = &reached.resolved;
    let provider = resolved.provider;
    let conversation = resolved.conversation.as_str();
    let db = &orch.db.local;
    let messages =
        ledger::conversation_messages(db, provider.id(), conversation, since_ms, direction, limit)
            .await?;
    let inbound_ids = messages
        .iter()
        .filter(|message| message.direction == ledger::MessageDirection::In)
        .map(|message| message.id.clone())
        .collect::<Vec<_>>();
    let firings = ledger::firings_for_inbound(db, &inbound_ids).await?;
    // Each acknowledgement is found from its firing, so what became of an
    // inbound message never depends on which outbound rows this page holds.
    let mut acknowledgements = Vec::with_capacity(firings.len());
    for firing in &firings {
        acknowledgements.push(ledger::route_acknowledgement(db, provider.id(), firing).await?);
    }

    // Resolved only when a rejection on this page names it.
    let default_project = if messages
        .iter()
        .any(|message| message.state.as_deref() == Some("no_default_thread"))
    {
        default_project::resolve(orch)
            .await
            .ok()
            .flatten()
            .map(|project| project.key)
    } else {
        None
    };

    let row = reached.row();
    let status = runtime_status(orch, provider.id()).await;
    let binding = resolved.primary();
    let heading = match row {
        Some(row) => row.label.clone(),
        None => titled_label(resolved).await,
    };
    let mut out = format!(
        "# {heading}\n\n{uri}\naddress: {conversation} · binding: {} · target: {}\nhealth: {}\n{}\n",
        binding
            .map(|binding| binding.binding_kind.as_str())
            .unwrap_or("none"),
        binding
            .map(|binding| binding.target_uri.as_str())
            .unwrap_or("none"),
        health_line(status.state, status.detail.as_deref()),
        match row {
            Some(row) if row.sendable() => format!(
                "send (agent_text): {}; to reply to a message, add replyTo with its `in` or `out` id below",
                send_example(&uri)
            ),
            Some(row) => format!(
                "send: not available; {}",
                row.send_refusal().unwrap_or_default()
            ),
            None => "send: not available; this conversation has no active binding".to_string(),
        }
    );
    out.push_str(&format!(
        "\n## Messages\n\n{} shown, oldest first{}.\n",
        messages.len(),
        if messages.len() as i64 == limit {
            format!(" (the newest {limit}; raise ?limit= or set ?since= for others)")
        } else {
            String::new()
        }
    ));
    for message in &messages {
        match message.direction {
            ledger::MessageDirection::In => {
                let sender = match message.state.as_deref() {
                    Some("allowlist") => Sender::Stranger(&message.sender),
                    _ => Sender::Operator,
                };
                out.push_str(&format!(
                    "\n- in {} · {} · {}\n{}\n",
                    message.id,
                    at_ms(message.at),
                    label::sender_label(sender),
                    quoted(&message.text)
                ));
                let own_firings = firings
                    .iter()
                    .zip(&acknowledgements)
                    .filter(|(firing, _)| firing.inbound_id == message.id)
                    .collect::<Vec<_>>();
                let action = match (message.state.as_deref(), message.reference.as_deref()) {
                    (Some(reason), _) => inbound_rejection(reason, default_project.as_deref()),
                    (None, Some(target))
                        if message.binding_kind.as_deref()
                            == Some(BindingKind::Ambient.as_str())
                            && message.detail.is_none()
                            && matches!(parse_uri(target), Some(CairnResource::Project { .. })) =>
                    {
                        if own_firings.is_empty() {
                            "logged for routes; no route fired".to_string()
                        } else {
                            "logged for routes".to_string()
                        }
                    }
                    (None, Some(target)) => match message.detail.as_deref() {
                        Some(addressed_by) => {
                            inbound_reason(db, provider, conversation, target, addressed_by).await
                        }
                        None => format!("delivered to {target}"),
                    },
                    (None, None) => "recorded".to_string(),
                };
                out.push_str(&format!("  {action}\n"));
                for (firing, acknowledgement) in own_firings {
                    out.push_str(&firing_line(firing, acknowledgement.as_ref()));
                    out.push('\n');
                }
            }
            ledger::MessageDirection::Out => {
                let reply = match (&message.reply_to, &message.reply_to_guid) {
                    (Some(to), Some(_)) => format!(" · reply to {to}"),
                    (Some(to), None) => format!(
                        " · reply to {to} (sent plainly; {} has no native reply)",
                        provider.display_name()
                    ),
                    (None, _) => String::new(),
                };
                out.push_str(&format!(
                    "\n- out {} · {} · {} · {}{reply}\n{}\n  {}\n",
                    message.id,
                    at_ms(message.at),
                    outbound_sender(message),
                    message.sender,
                    quoted(&message.text),
                    delivery_state(message)
                ));
            }
        }
    }
    Ok(out)
}

const AMBIENT_FEEDBACK_URI: &str = "cairn://channels/ambient/feedback";
const AMBIENT_FEEDBACK_DEFAULT_LIMIT: i64 = 50;
const AMBIENT_FEEDBACK_MAX_LIMIT: i64 = 500;
const AMBIENT_OUTCOMES: [&str; 6] = [
    "admitted",
    "below_threshold",
    "refused",
    "unanswerable",
    "failed",
    crate::storage::ambient_feedback::OUTCOME_UNRECORDED,
];

/// The read's filter and row limit, or the sentence refusing a query value.
pub(crate) fn ambient_feedback_query(
    params: &[QueryParam],
) -> Result<(crate::storage::ambient_feedback::AmbientFeedbackFilter, i64), String> {
    use crate::storage::ambient_feedback::{AmbientFeedbackFilter, Engagement};
    let mut filter = AmbientFeedbackFilter::default();
    let mut limit = AMBIENT_FEEDBACK_DEFAULT_LIMIT;
    for param in params {
        let value = param.value.trim();
        match param.key.as_str() {
            "since" => {
                filter.since = Some(value.parse().map_err(|_| {
                    format!("since must be epoch seconds for {AMBIENT_FEEDBACK_URI}; got '{value}'")
                })?)
            }
            "limit" => {
                let parsed: i64 = value.parse().map_err(|_| {
                    format!("limit must be a positive number for {AMBIENT_FEEDBACK_URI}; got '{value}'")
                })?;
                limit = parsed.clamp(1, AMBIENT_FEEDBACK_MAX_LIMIT);
            }
            "verdict" if matches!(value, "not_mine" | "reroute") => {
                filter.verdict = Some(value.to_string())
            }
            "verdict" => {
                return Err(format!(
                    "Unknown verdict '{value}' for {AMBIENT_FEEDBACK_URI}; expected not_mine or reroute"
                ))
            }
            "outcome" if AMBIENT_OUTCOMES.contains(&value) => {
                filter.outcome = Some(value.to_string())
            }
            "outcome" => {
                return Err(format!(
                    "Unknown outcome '{value}' for {AMBIENT_FEEDBACK_URI}; expected one of {}",
                    AMBIENT_OUTCOMES.join(", ")
                ))
            }
            "thread" => match cairn_uri::parse_uri(value) {
                Some(cairn_uri::CairnResource::Thread { path, .. }) if path.is_empty() => {
                    filter.thread = Some(cairn_uri::canonicalize_uri_identity(value))
                }
                _ => {
                    return Err(format!(
                        "thread must be a thread URI such as cairn://p/PROJECT/THREAD for {AMBIENT_FEEDBACK_URI}; got '{value}'"
                    ))
                }
            },
            "engaged" => {
                filter.engaged = Some(match value {
                    "true" => Engagement::Engaged,
                    "false" => Engagement::NotEngaged,
                    "unchecked" => Engagement::Unchecked,
                    _ => {
                        return Err(format!(
                            "Unknown engaged '{value}' for {AMBIENT_FEEDBACK_URI}; expected true, false, or unchecked"
                        ))
                    }
                })
            }
            other => {
                return Err(format!(
                    "Unsupported query parameter '{other}' for {AMBIENT_FEEDBACK_URI}"
                ))
            }
        }
    }
    Ok((filter, limit))
}

fn utc(seconds: i64) -> String {
    chrono::DateTime::from_timestamp(seconds, 0)
        .map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| seconds.to_string())
}

/// One label-set row as the read renders it.
pub(crate) fn render_ambient_feedback_row(
    row: &crate::storage::ambient_feedback::AmbientFeedbackRow,
) -> String {
    let event = crate::routes::fact_row_uri(Some(&row.project_key), row.event_id);
    let recipient = row.recipient_thread.as_deref().unwrap_or("(refused side)");
    let mut lines = vec![format!(
        "- {event} · {} → {recipient} · event at {}",
        row.outcome,
        utc(row.event_at)
    )];
    let probability = row
        .probability
        .map(|p| format!("{p:.2}"))
        .unwrap_or_else(|| "-".into());
    lines.push(format!(
        "  pick {} ({probability}, {}, rev {}) · picks {} · addresses from {}",
        row.pick.as_deref().unwrap_or("-"),
        row.measured_by.as_deref().unwrap_or("-"),
        row.revision.as_deref().unwrap_or("-"),
        row.picks_json.as_deref().unwrap_or("-"),
        row.addresses_from
    ));
    lines.push(match row.verdict.as_deref() {
        Some(verdict) => {
            let to = row
                .reroute_to
                .as_deref()
                .map(|to| format!(" → {to}"))
                .unwrap_or_default();
            let note = row
                .note
                .as_deref()
                .map(|note| format!(" · note: {note}"))
                .unwrap_or_default();
            format!(
                "  verdict {verdict}{to} by {} at {}{note}",
                row.labelled_by.as_deref().unwrap_or("-"),
                row.labelled_at.map(utc).unwrap_or_else(|| "-".into())
            )
        }
        None => "  verdict none".to_string(),
    });
    lines.push(match (row.engagement_checked_at, row.engaged) {
        (Some(checked), Some(engaged)) => {
            let by = row
                .engaged_by
                .as_deref()
                .map(|by| format!(" by {by}"))
                .unwrap_or_default();
            format!(
                "  engaged {}{by} · checked {}",
                if engaged { "yes" } else { "no" },
                utc(checked)
            )
        }
        _ => "  engagement unchecked (judged 48 hours after the event)".to_string(),
    });
    lines.join("\n")
}

/// `cairn://channels/ambient/feedback`: the label set, newest event first,
/// under a header counting the rows and the `unrecorded` ones among them.
pub async fn render_ambient_feedback(orch: &Orchestrator, params: &[QueryParam]) -> String {
    let (filter, limit) = match ambient_feedback_query(params) {
        Ok(query) => query,
        Err(error) => return error,
    };
    let db = &orch.db.local;
    let (counts, rows) = match (
        crate::storage::ambient_feedback::counts(db, &filter).await,
        crate::storage::ambient_feedback::list(db, &filter, limit).await,
    ) {
        (Ok(counts), Ok(rows)) => (counts, rows),
        (Err(error), _) | (_, Err(error)) => {
            return format!("Reading the ambient routing label set failed: {error}")
        }
    };
    let (total, unrecorded) = counts;
    let mut out = format!(
        "# Ambient routing feedback\n\n{total} rows; {unrecorded} unrecorded (events the sweep reached after the route's firing journal evicted them; the durable decision record is cairn/6317). Showing {}.\n",
        rows.len()
    );
    for row in &rows {
        out.push('\n');
        out.push_str(&render_ambient_feedback_row(row));
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::bindings::MESSAGE_CLASSES_ALL;

    fn row(
        address: ConversationAddress,
        classes: i64,
        state: &'static str,
        detail: Option<&str>,
        last_error: Option<&str>,
    ) -> ConversationRow {
        row_for_status(
            address,
            "cairn://p/cairn/general".into(),
            classes,
            "structural",
            state,
            detail.map(str::to_string),
            last_error.map(str::to_string),
            i64::from(last_error.is_some()),
            None,
        )
    }

    #[test]
    fn runtime_and_outbound_failure_combine_into_deliverability() {
        let ready = ConversationAddress::telegram("123").unwrap();
        assert_eq!(
            row(ready.clone(), 1, "bridgeUp", None, None).deliverability,
            ConversationDeliverability::Ready
        );
        assert_eq!(
            row(ready.clone(), 1, "bridgeUp", None, Some("send failed")).deliverability,
            ConversationDeliverability::Degraded
        );
        assert_eq!(
            row(ready.clone(), 1, "degraded", None, None).deliverability,
            ConversationDeliverability::Degraded
        );
        assert_eq!(
            row(ready, 1, "stopped", None, None).deliverability,
            ConversationDeliverability::Stopped
        );
    }

    #[test]
    fn row_shape_uses_stable_field_names() {
        let row = row(
            ConversationAddress::discord("1", "2").unwrap(),
            MESSAGE_CLASS_QUESTION | MESSAGE_CLASS_NOTIFY,
            "bridgeUp",
            None,
            None,
        );
        assert_eq!(
            serde_json::to_value(row).unwrap(),
            serde_json::json!({
                "address": "discord:1/2",
                "label": "#general · cairn · Discord",
                "uri": "cairn://channels/discord/cairn/general",
                "binding": "structural",
                "provider": "discord",
                "deliverability": "ready",
                "target": "cairn://p/cairn/general",
                "classes": ["question", "notify"],
                "capabilities": {
                    "append_to_conversation": true,
                    "create_into_container": true
                },
                "failed_count": 0
            })
        );
    }

    #[test]
    fn a_sendable_row_advertises_the_send_write_and_its_grant() {
        let sendable = row(
            ConversationAddress::discord("1", "2").unwrap(),
            MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
            "bridgeUp",
            None,
            None,
        );
        let value = serde_json::to_value(&sendable).unwrap();
        assert_eq!(
            value["classes"],
            serde_json::json!(["question", "permission", "notify", "agent_text"])
        );
        assert_eq!(
            value["send"],
            "write({changes:[{target:\"cairn://channels/discord/cairn/general\",mode:\"append\",payload:{content:\"...\"}}]})"
        );
        // Internal admission state never leaks into the projection.
        for hidden in ["message_classes", "runtime_state", "runtime_detail"] {
            assert!(value.get(hidden).is_none(), "{hidden} must not serialize");
        }

        // Degraded only by a prior outbound failure: still sendable.
        assert!(row(
            ConversationAddress::discord("1", "2").unwrap(),
            MESSAGE_CLASS_AGENT_TEXT,
            "bridgeUp",
            None,
            Some("send failed"),
        )
        .send
        .is_some());

        for unsendable in [
            row(
                ConversationAddress::discord("1", "2").unwrap(),
                MESSAGE_CLASSES_ALL,
                "bridgeUp",
                None,
                None,
            ),
            row(
                ConversationAddress::discord("1", "2").unwrap(),
                MESSAGE_CLASS_AGENT_TEXT,
                "dead",
                Some("channel is not running"),
                None,
            ),
        ] {
            assert!(!unsendable.sendable());
            assert!(serde_json::to_value(&unsendable)
                .unwrap()
                .get("send")
                .is_none());
        }
    }

    #[test]
    fn a_degraded_provider_still_admits_agent_text_and_only_a_stopped_one_refuses() {
        let pin_gap = "Cards are not being pinned: Missing Permissions in Discord channel 2";
        let degraded = row(
            ConversationAddress::discord("1", "2").unwrap(),
            MESSAGE_CLASS_AGENT_TEXT,
            "degraded",
            Some(pin_gap),
            None,
        );
        assert_eq!(
            degraded.deliverability,
            ConversationDeliverability::Degraded
        );
        assert!(degraded.send.is_some());
        let admitted = admit_send_to(std::slice::from_ref(&degraded), "discord:1/2").unwrap();
        assert_eq!(admitted.address.to_string(), "discord:1/2");

        for (state, detail) in [
            ("stopped", None),
            ("dead", Some("channel is not running")),
            ("executorOffline", Some("executor offline")),
        ] {
            let down = row(
                ConversationAddress::discord("1", "2").unwrap(),
                MESSAGE_CLASS_AGENT_TEXT,
                state,
                detail,
                None,
            );
            assert!(down.send.is_none(), "{state}");
            let refusal = admit_send_to(&[down], "discord:1/2").unwrap_err();
            assert!(
                refusal.starts_with(&format!("discord is {state}"))
                    && refusal.contains("admitted only while the provider is running"),
                "{refusal}"
            );
        }
    }

    #[tokio::test]
    async fn an_ancient_failure_the_provider_has_sent_past_reads_ready() {
        let db = crate::storage::migrated_test_db("channels-ancient-failure.db").await;
        let outbound =
            |id: &'static str, binding_ref: &'static str, created_at| ledger::NewOutbound {
                id,
                channel: "discord",
                kind: "review",
                binding_ref,
                conversation: "discord:1/2",
                job_id: None,
                rendered_text: "Ready for review",
                rendering: "text",
                created_at,
            };
        // Weeks ago: a review notice into a since-locked thread failed.
        assert!(
            ledger::insert_intent(&db, &outbound("locked", "review-1", 1_000))
                .await
                .unwrap()
        );
        assert!(ledger::begin_delivery(&db, "locked", 1_100).await.unwrap());
        assert!(
            ledger::mark_failed(&db, "locked", "thread is locked", 1_200, None)
                .await
                .unwrap()
        );
        // Since then, the provider has delivered.
        for (id, binding_ref, at) in [
            ("later-1", "review-2", 10_000),
            ("later-2", "review-3", 20_000),
        ] {
            assert!(ledger::insert_intent(&db, &outbound(id, binding_ref, at))
                .await
                .unwrap());
            assert!(ledger::mark_sent(&db, id, id, None, None, at + 1)
                .await
                .unwrap());
        }

        let (last_error, undelivered) = crate::channels::outbound_status(&db, "discord").await;
        let rendered = serde_json::to_value(row_for_status(
            ConversationAddress::discord("1", "2").unwrap(),
            "cairn://p/cairn/general".into(),
            MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
            "structural",
            RUNTIME_UP,
            None,
            last_error,
            undelivered.failed,
            None,
        ))
        .unwrap();
        assert_eq!(rendered["deliverability"], "ready", "{rendered}");
        assert_eq!(rendered["failed_count"], 0, "{rendered}");
        assert!(rendered.get("last_error").is_none(), "{rendered}");
        assert!(rendered.get("send").is_some(), "{rendered}");
    }

    fn enumeration() -> Vec<ConversationRow> {
        vec![
            row(
                ConversationAddress::discord("1", "2").unwrap(),
                MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
                "bridgeUp",
                None,
                None,
            ),
            row(
                ConversationAddress::discord("1", "3").unwrap(),
                MESSAGE_CLASSES_ALL,
                "bridgeUp",
                None,
                None,
            ),
            row(
                ConversationAddress::telegram("42").unwrap(),
                MESSAGE_CLASS_AGENT_TEXT,
                "dead",
                Some("channel is not running"),
                None,
            ),
        ]
    }

    #[test]
    fn admission_accepts_only_a_sendable_bound_row() {
        let admitted = admit_send_to(&enumeration(), "discord:1/2").unwrap();
        assert_eq!(admitted.address.to_string(), "discord:1/2");
        assert!(admitted.row.last_error.is_none());
    }

    #[test]
    fn every_refusal_names_what_would_be_accepted() {
        let rows = enumeration();
        let refusal = |conversation: &str| admit_send_to(&rows, conversation).unwrap_err();

        let not_canonical = refusal("Discord:1/2");
        assert!(
            not_canonical.contains("expected discord:1/2"),
            "{not_canonical}"
        );
        let unbound = refusal("discord:9/9");
        assert!(
            unbound.contains("no active binding")
                && unbound.contains("cairn://channels/discord/cairn/general"),
            "{unbound}"
        );
        let not_granted = refusal("discord:1/3");
        assert!(
            not_granted.contains("agent messages are not enabled for #general · cairn · Discord")
                && not_granted.contains("question, permission, notify")
                && not_granted.contains("only the operator can enable them"),
            "{not_granted}"
        );
        let down = refusal("telegram:42");
        assert!(
            down.contains("telegram is dead: channel is not running"),
            "{down}"
        );
    }

    #[tokio::test]
    async fn the_operator_grant_moves_agent_text_and_refuses_what_cannot_deliver() {
        use crate::channels::bindings::BindingKind;
        let db = crate::storage::migrated_test_db("channels-operator-agent-text.db").await;
        assert!(ledger::bind_target(
            &db,
            "discord",
            "discord:1/2",
            "cairn://p/cairn/general",
            BindingKind::Structural,
            MESSAGE_CLASSES_ALL,
            1,
        )
        .await
        .unwrap());
        let classes = |conversation: &'static str| {
            let db = &db;
            async move {
                db.query_opt_i64(
                    "SELECT message_classes FROM channel_conversation_binding WHERE conversation = ?1",
                    (conversation,),
                )
                .await
                .unwrap()
            }
        };

        set_agent_text(&db, "discord:1/2", "cairn://p/cairn/general", true)
            .await
            .unwrap();
        assert_eq!(
            classes("discord:1/2").await,
            Some(MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT)
        );
        set_agent_text(&db, "discord:1/2", "cairn://p/cairn/general", false)
            .await
            .unwrap();
        assert_eq!(classes("discord:1/2").await, Some(MESSAGE_CLASSES_ALL));

        let unbound = set_agent_text(&db, "discord:1/9", "cairn://p/cairn/general", true)
            .await
            .unwrap_err();
        assert!(unbound.contains("not bound"), "{unbound}");
        assert!(
            set_agent_text(&db, "Discord:1/2", "cairn://p/cairn/general", true)
                .await
                .is_err()
        );
    }

    /// An ambient conversation is enumerated, marked, and sendable like any
    /// agent-text binding, but the operator's switch cannot change it.
    #[tokio::test]
    async fn ambient_conversations_are_marked_sendable_and_fixed() {
        use crate::channels::bindings::BindingKind;
        let db = crate::storage::migrated_test_db("channels-ambient-row.db").await;
        assert!(ledger::bind_target(
            &db,
            "discord",
            "discord:1/9",
            "cairn://p/cairn",
            BindingKind::Ambient,
            MESSAGE_CLASS_AGENT_TEXT,
            1,
        )
        .await
        .unwrap());
        assert!(ledger::bind_target(
            &db,
            "discord",
            "discord:1/2",
            "cairn://p/cairn/general",
            BindingKind::Structural,
            MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT,
            1,
        )
        .await
        .unwrap());

        let rows = conversation_rows(
            ledger::list_active_bindings(&db).await.unwrap(),
            &HashMap::new(),
            |_| true,
            |_| async {
                ProviderRowStatus {
                    state: RUNTIME_UP,
                    detail: None,
                    last_send_error: None,
                    failed_count: 0,
                }
            },
        )
        .await;
        let ambient = rows
            .iter()
            .find(|row| row.address == "discord:1/9")
            .unwrap();
        assert!(ambient.sendable(), "threads reply into the ambient channel");
        let rendered = serde_json::to_value(ambient).unwrap();
        assert_eq!(rendered["ambient"], true, "{rendered}");
        assert_eq!(rendered["target"], "cairn://p/cairn");
        assert_eq!(rendered["label"], "#ambient · cairn · Discord");
        assert_eq!(rendered["uri"], "cairn://channels/discord/cairn");
        assert_eq!(rendered["binding"], "ambient");
        assert_eq!(rendered["classes"], serde_json::json!(["agent_text"]));
        let structural = rows
            .iter()
            .find(|row| row.address == "discord:1/2")
            .unwrap();
        assert!(serde_json::to_value(structural)
            .unwrap()
            .get("ambient")
            .is_none());

        for granted in [false, true] {
            assert_eq!(
                set_agent_text(&db, "discord:1/9", "cairn://p/cairn", granted)
                    .await
                    .unwrap_err(),
                ledger::AMBIENT_CLASSES_FIXED
            );
        }
    }

    /// The enumeration's cost at the registry's live shape: 871 active bindings
    /// (discord 863, imessage 6, telegram 2) over a ~6,300-row outbound ledger.
    /// The status lookup issues the same queries `runtime_status` does. Run with
    /// `bun run test:rust -p cairn-core --lib -E 'test(/measure_conversation_enumeration/)' --run-ignored only --no-capture`.
    #[tokio::test]
    #[ignore = "measurement: seeds a live-sized binding registry and outbound ledger"]
    async fn measure_conversation_enumeration_at_live_scale() {
        use crate::channels::{bindings::BindingKind, state_card};
        use std::time::Instant;
        const OUTBOUND: usize = 6_296;

        let db = crate::storage::migrated_test_db("channels-enumeration-scale.db").await;
        let conversation = |provider: &str, n: usize| match provider {
            "discord" => format!("discord:1/{n}"),
            "imessage" => format!("imessage:user{n}@example.com"),
            _ => format!("telegram:{}", 100 + n),
        };
        for (provider, count) in [("discord", 863), ("imessage", 6), ("telegram", 2)] {
            for n in 0..count {
                assert!(ledger::bind_target(
                    &db,
                    provider,
                    &conversation(provider, n),
                    &format!("cairn://p/cairn/{}", n + 1),
                    BindingKind::Structural,
                    MESSAGE_CLASSES_ALL,
                    1,
                )
                .await
                .unwrap_or_else(|error| panic!("{provider} binding {n}: {error}")));
            }
        }
        for chunk in (0..OUTBOUND).collect::<Vec<_>>().chunks(500) {
            let values = chunk
                .iter()
                .map(|&n| {
                    let channel = match n % 20 {
                        0 => "imessage",
                        1 => "telegram",
                        _ => "discord",
                    };
                    let to = conversation(channel, 0);
                    if n % 25 == 0 {
                        format!("('o-{n}','{channel}','review','ref-{n}','{to}','text','text','failed',{n},NULL,NULL,'send failed',{n})")
                    } else {
                        format!("('o-{n}','{channel}','review','ref-{n}','{to}','text','text','sent',{n},'g-{n}',{n},NULL,NULL)")
                    }
                })
                .collect::<Vec<_>>()
                .join(",");
            db.execute_batch(&format!(
                "INSERT INTO channel_outbound (id, channel, kind, binding_ref, conversation, rendered_text, rendering, status, created_at, provider_guid, sent_at, last_error, last_attempt_at) VALUES {values};"
            ))
            .await
            .unwrap();
        }

        let status_of = |provider: ConversationProvider| {
            let db = &db;
            async move {
                if provider == ConversationProvider::Discord {
                    state_card::latest_pin_failure(db, "discord", 0)
                        .await
                        .unwrap();
                }
                ledger::list_inbound(db, provider.id(), 50).await.unwrap();
                let (last_send_error, undelivered) =
                    crate::channels::outbound_status(db, provider.id()).await;
                ProviderRowStatus {
                    state: RUNTIME_UP,
                    detail: None,
                    last_send_error,
                    failed_count: undelivered.failed,
                }
            }
        };
        let bindings = ledger::list_active_bindings(&db).await.unwrap();
        assert_eq!(bindings.len(), 871);

        // The per-binding lookup the enumeration used to make.
        let started = Instant::now();
        for binding in &bindings {
            let provider = match binding.provider.as_str() {
                "imessage" => ConversationProvider::IMessage,
                "telegram" => ConversationProvider::Telegram,
                _ => ConversationProvider::Discord,
            };
            status_of(provider).await;
        }
        let per_binding = started.elapsed();

        let started = Instant::now();
        let rows = conversation_rows(bindings, &HashMap::new(), |_| true, status_of).await;
        let per_provider = started.elapsed();
        assert_eq!(rows.len(), 871);

        eprintln!(
            "871 bindings, {OUTBOUND} outbound rows: per-binding status {per_binding:?} ({:?}/lookup), per-provider enumeration {per_provider:?}",
            per_binding / 871
        );
    }

    fn bound_row(conversation: &str, target: &str, binding: &str, classes: i64) -> ConversationRow {
        row_for_status(
            conversation.parse().unwrap(),
            target.into(),
            classes,
            binding,
            RUNTIME_UP,
            None,
            None,
            0,
            None,
        )
    }

    #[test]
    fn reachability_is_one_rule_for_threads_issues_and_the_operator() {
        let agent = MESSAGES_ALL_AGENT;
        let own = bound_row("discord:1/2", "cairn://p/prj/general", "structural", agent);
        let other = bound_row("discord:1/3", "cairn://p/prj/other", "structural", agent);
        let issue = bound_row("discord:1/4", "cairn://p/prj/42", "structural", agent);
        let ambient = bound_row(
            "discord:1/9",
            "cairn://p/prj",
            "ambient",
            MESSAGE_CLASS_AGENT_TEXT,
        );
        let elsewhere = bound_row(
            "discord:1/8",
            "cairn://p/zzz",
            "ambient",
            MESSAGE_CLASS_AGENT_TEXT,
        );
        let rows = [&own, &other, &issue, &ambient, &elsewhere];
        let reached = |reader: &ChannelReader| {
            rows.iter()
                .filter(|row| reader.reaches(row))
                .map(|row| row.address.as_str())
                .collect::<Vec<_>>()
        };

        let thread = ChannelReader::from_home("cairn://p/PRJ/general", "job").unwrap();
        assert_eq!(reached(&thread), ["discord:1/2", "discord:1/9"]);
        let task = ChannelReader::from_home("cairn://p/prj/general/task/scan", "job").unwrap();
        assert_eq!(reached(&task), reached(&thread));
        let node = ChannelReader::from_home("cairn://p/prj/42/1/builder", "job").unwrap();
        assert_eq!(reached(&node), ["discord:1/4", "discord:1/9"]);
        let node_task =
            ChannelReader::from_home("cairn://p/prj/42/1/builder/task/review", "job").unwrap();
        assert_eq!(reached(&node_task), reached(&node));
        assert!(ChannelReader::from_home("cairn://skills", "job").is_err());
    }

    const MESSAGES_ALL_AGENT: i64 = MESSAGE_CLASSES_ALL | MESSAGE_CLASS_AGENT_TEXT;

    /// A Telegram chat is listed by the title its running provider gives it,
    /// fetched when read rather than stored on the binding.
    #[tokio::test]
    async fn a_telegram_chat_is_listed_by_its_title() {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, SearchIndex};
        use std::sync::Arc;
        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
                .await
                .unwrap();
        assert!(ledger::bind_target(
            &local,
            "telegram",
            "telegram:42",
            "cairn://p/prj/general",
            BindingKind::Structural,
            MESSAGES_ALL_AGENT,
            1,
        )
        .await
        .unwrap());
        let config_dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(
            crate::config::settings::get_settings_path(&config_dir),
            "channels:\n  telegram:\n    enabled: true\n    chatId: '42'\n",
        )
        .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let orch = OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build();

        let _runtime = crate::channels::register_test_runtime(
            "telegram",
            crate::channels::titles::tests::TitledProvider::new(Some("Ops")),
        );
        let listing = operator_read(&orch, "cairn://channels/telegram")
            .await
            .unwrap();
        assert!(listing.contains("Ops · Telegram"), "{listing}");
        assert!(!listing.contains("operator chat"), "{listing}");
    }

    /// A project `prj` with a `channels` thread, Discord enabled, its ambient
    /// channel `discord:1/9`, and `#channels` at `discord:1/2`.
    async fn conversation_orchestrator() -> Orchestrator {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::{LocalDb, SearchIndex};
        use std::sync::Arc;
        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("t.db"))
                .await
                .unwrap();
        local
            .execute_script(
                "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
                 INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at)
                 VALUES ('p','w','P','prj','/tmp/p',1,1);
                 INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
                 VALUES ('t-channels', 'p', 'channels', 'active', 1, 1);",
            )
            .await
            .unwrap();
        for (conversation, target, kind, classes) in [
            (
                "discord:1/9",
                "cairn://p/prj",
                BindingKind::Ambient,
                MESSAGE_CLASS_AGENT_TEXT,
            ),
            (
                "discord:1/2",
                "cairn://p/prj/channels",
                BindingKind::Structural,
                MESSAGES_ALL_AGENT,
            ),
        ] {
            assert!(
                ledger::bind_target(&local, "discord", conversation, target, kind, classes, 1,)
                    .await
                    .unwrap()
            );
        }
        let config_dir = tempfile::tempdir().unwrap().keep();
        std::fs::write(
            crate::config::settings::get_settings_path(&config_dir),
            "channels:\n  discord:\n    enabled: true\n    guildId: '1'\n",
        )
        .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build()
    }

    fn run_less_request(
        origin: Option<cairn_common::protocol::DispatchOrigin>,
    ) -> McpCallbackRequest {
        McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
            origin,
        }
    }

    /// The `channels` thread, which reaches `#channels` and the project's
    /// `#ambient`.
    fn channels_thread() -> ChannelReader {
        ChannelReader::from_home("cairn://p/prj/channels", "job").unwrap()
    }

    async fn view(orch: &Orchestrator, path: &[&str], params: &[(&str, &str)]) -> String {
        let path = path
            .iter()
            .map(|segment| segment.to_string())
            .collect::<Vec<_>>();
        let params = params
            .iter()
            .map(|(key, value)| QueryParam {
                key: key.to_string(),
                value: value.to_string(),
            })
            .collect::<Vec<_>>();
        conversation_view(orch, &channels_thread(), "discord", &path, &params)
            .await
            .unwrap()
    }

    /// A request that speaks for no run (the machine credential, a resident
    /// process, a project service) proves nobody, so it lists no binding and
    /// reads no history.
    #[tokio::test]
    async fn a_run_less_request_reaches_no_conversation() {
        let orch = conversation_orchestrator().await;
        // A read_batch the machine credential makes, held exactly as the
        // callback holds it.
        let mut machine = run_less_request(None);
        machine.tool = "read_batch".into();
        crate::mcp::run_bearer::hold_to_machine_credential(&mut machine).unwrap();
        assert!(machine.is_unverified_local());
        for request in [run_less_request(None), machine] {
            let root = render_channels(&orch, &request, &[]).await;
            let listing = render_provider(&orch, &request, "discord", &[]).await;
            let history =
                render_conversation(&orch, &request, "discord", &["prj".to_string()], &[]).await;
            for body in [root, listing, history] {
                assert!(
                    body.starts_with("Refused: this request speaks for no run")
                        && !body.contains("#ambient"),
                    "{body}"
                );
            }
        }
    }

    /// A bare route alias can be both an iMessage handle and a Telegram chat
    /// id; one provider's conversation never shows the other's rows.
    #[tokio::test]
    async fn a_colliding_bare_alias_stays_within_its_provider() {
        let db = crate::storage::migrated_test_db("channels-alias-collision.db").await;
        for (id, channel) in [("o-imessage", "imessage"), ("o-telegram", "telegram")] {
            assert!(ledger::insert_intent(
                &db,
                &ledger::NewOutbound {
                    id,
                    channel,
                    kind: "route",
                    binding_ref: "route:r:channel-message:in-1",
                    conversation: "1555123456",
                    job_id: None,
                    rendered_text: "ack",
                    rendering: "text",
                    created_at: 1,
                },
            )
            .await
            .unwrap());
        }
        let ids = ledger::conversation_messages(
            &db,
            "telegram",
            "telegram:1555123456",
            0,
            ledger::DirectionFilter::All,
            50,
        )
        .await
        .unwrap()
        .into_iter()
        .map(|message| message.id)
        .collect::<Vec<_>>();
        assert_eq!(ids, ["o-telegram"]);
        let firing = ledger::InboundFiring {
            inbound_id: "in-1".into(),
            route_id: "r".into(),
            status: "fired".into(),
            drop_reason: None,
            error: None,
            sink_kind: "channel".into(),
            sink_ref: Some("telegram:1555123456".into()),
        };
        assert_eq!(
            ledger::route_acknowledgement(&db, "telegram", &firing)
                .await
                .unwrap()
                .map(|ack| ack.id)
                .as_deref(),
            Some("o-telegram")
        );
    }

    async fn inbound(
        db: &crate::storage::LocalDb,
        id: &str,
        text: &str,
        target: &str,
        addressed_by: Option<&str>,
        received_at: i64,
    ) {
        assert!(ledger::insert_inbound(
            db,
            &ledger::InboundRecord {
                id: id.into(),
                channel: "discord".into(),
                provider_guid: None,
                sender: "1234".into(),
                text: text.into(),
                received_at,
                rejection_reason: None,
                acknowledged_at: None,
                conversation: Some("discord:1/9".into()),
                target_uri: Some(target.into()),
                binding_kind: Some("ambient".into()),
                addressed_by: addressed_by.map(str::to_string),
                message_guid: None,
            },
        )
        .await
        .unwrap());
    }

    /// The two messages of the night this resource was built for, read back off
    /// the ledger: unaddressed text a route acknowledged three minutes later,
    /// and text addressed by a channel mention to the thread it names.
    #[tokio::test]
    async fn a_reply_shows_what_it_replied_to_and_whether_natively() {
        let _runtime = crate::channels::register_test_runtime(
            "discord",
            crate::channels::titles::tests::TitledProvider::replying(true),
        );
        let orch = conversation_orchestrator().await;
        let db = &orch.db.local;
        inbound(db, "in-1", "yo", "cairn://p/prj", None, 1).await;
        for (id, guid) in [("out-native", Some("9:50")), ("out-plain", None)] {
            let binding_ref =
                crate::channels::agent_message::binding_ref(id, "cairn://p/prj/channels");
            assert!(ledger::insert_reply_intent(
                db,
                &ledger::NewOutbound {
                    id,
                    channel: "discord",
                    kind: "message",
                    binding_ref: &binding_ref,
                    conversation: "discord:1/9",
                    job_id: None,
                    rendered_text: "answer",
                    rendering: "text",
                    created_at: 2,
                },
                ledger::OutboundReply { to: "in-1", guid },
            )
            .await
            .unwrap());
        }
        let rendered = view(&orch, &["prj"], &[]).await;
        let line = |id: &str| {
            rendered
                .lines()
                .find(|line| line.starts_with(&format!("- out {id} ")))
                .unwrap_or_else(|| panic!("{id}: {rendered}"))
                .to_string()
        };
        assert!(
            line("out-native").ends_with(" · reply to in-1"),
            "{rendered}"
        );
        assert!(
            line("out-plain")
                .ends_with(" · reply to in-1 (sent plainly; Discord has no native reply)"),
            "{rendered}"
        );
        assert!(
            rendered.contains("add replyTo with its `in` or `out` id below"),
            "{rendered}"
        );
    }

    #[tokio::test]
    async fn a_conversation_shows_what_the_router_did_with_each_message() {
        let orch = conversation_orchestrator().await;
        let db = &orch.db.local;
        let created = chrono::DateTime::parse_from_rfc3339("2026-11-12T05:29:18.189Z")
            .unwrap()
            .timestamp_millis();
        inbound(db, "in-1", "yo", "cairn://p/prj", None, created - 100).await;
        db.execute(
            "INSERT INTO route_firings (id, route_id, scope_key, seq, trigger_source, fact_identity,
                                        status, sink_kind, sink_ref, created_at)
             VALUES ('f-1', 'ambient-channel', 'workspace', 1, 'channel_message',
                     'channel-message:in-1', 'fired', 'channel', 'discord:1/9', ?1)",
            (created,),
        )
        .await
        .unwrap();
        let outbound = |id: &'static str, kind: &'static str, binding_ref: &'static str, at| {
            ledger::NewOutbound {
                id,
                channel: "discord",
                kind,
                binding_ref,
                conversation: "discord:1/9",
                job_id: None,
                rendered_text: "Logged as an event.",
                rendering: "text",
                created_at: at,
            }
        };
        assert!(ledger::insert_intent(
            db,
            &outbound(
                "ack-1",
                "route",
                "route:ambient-channel:channel-message:in-1",
                created
            )
        )
        .await
        .unwrap());
        let sent = created + 183_782; // 2026-11-12 05:32:21.971Z
        assert!(ledger::mark_sent(db, "ack-1", "g-1", None, None, sent)
            .await
            .unwrap());
        inbound(
            db,
            "in-2",
            "yo",
            "cairn://p/prj/channels",
            Some("<#2>"),
            created + 200_000,
        )
        .await;
        let message_ref =
            crate::channels::agent_message::binding_ref("out-f", "cairn://p/prj/channels");
        let message_ref: &'static str = Box::leak(message_ref.into_boxed_str());
        assert!(ledger::insert_intent(
            db,
            &outbound("out-f", "message", message_ref, created + 300_000)
        )
        .await
        .unwrap());
        assert!(ledger::begin_delivery(db, "out-f", created + 300_001)
            .await
            .unwrap());
        assert!(ledger::mark_failed(
            db,
            "out-f",
            "Missing Access",
            created + 300_002,
            Some(created + 305_002)
        )
        .await
        .unwrap());

        let rendered = view(&orch, &["prj"], &[]).await;
        assert!(
            rendered.starts_with("# #ambient · prj · Discord"),
            "{rendered}"
        );
        let acknowledged = "  logged for routes\n    - route ambient-channel: fired → acknowledgement ack-1, created 2026-11-12 05:29:18.189Z, sent 2026-11-12 05:32:21.971Z";
        assert!(rendered.contains(acknowledged), "{rendered}");
        assert!(
            rendered.contains("delivered to cairn://p/prj/channels, addressed by #channels"),
            "{rendered}"
        );
        assert!(
            rendered.contains("cairn://p/prj/channels · message")
                && rendered.contains("failed (attempt 1/6): Missing Access, retry at"),
            "{rendered}"
        );
        assert!(
            rendered.contains("route ambient-channel · route"),
            "{rendered}"
        );
        for line in rendered.lines().filter(|line| line.contains("discord:")) {
            assert!(
                line.starts_with("address: "),
                "a provider address leaked outside the address field: {line}\n{rendered}"
            );
        }
        let positions = ["in in-1", "out ack-1", "in in-2", "out out-f"].map(|needle| {
            rendered
                .find(needle)
                .unwrap_or_else(|| panic!("{needle}: {rendered}"))
        });
        assert!(
            positions.windows(2).all(|pair| pair[0] < pair[1]),
            "oldest first: {rendered}"
        );

        // What became of a message does not depend on the page: with the
        // acknowledgement itself filtered out, its delivery is still shown.
        let only_in = view(&orch, &["prj"], &[("direction", "in")]).await;
        assert!(
            !only_in.contains("out ack-1") && only_in.contains("in in-2"),
            "{only_in}"
        );
        assert!(only_in.contains(acknowledged), "{only_in}");

        let listing = provider_listing(&orch, &channels_thread(), "discord", &[])
            .await
            .unwrap();
        assert!(
            listing.contains("- #ambient · prj · Discord — cairn://channels/discord/prj")
                && listing.contains(
                    "- #channels · prj · Discord — cairn://channels/discord/prj/channels"
                ),
            "{listing}"
        );
        assert!(
            listing.find("#ambient").unwrap() < listing.find("#channels ·").unwrap(),
            "newest activity first: {listing}"
        );
    }

    /// Each unaddressed-text rule names itself on the message it routed, and a
    /// refusal names the default project that had no open general thread.
    #[tokio::test]
    async fn each_inbound_says_which_rule_routed_it() {
        let orch = conversation_orchestrator().await;
        let db = &orch.db.local;
        let at = chrono::DateTime::parse_from_rfc3339("2026-11-12T05:29:18.189Z")
            .unwrap()
            .timestamp_millis();
        let operator_ref = crate::channels::operator_message::binding_ref("m-1");
        assert!(ledger::insert_intent(
            db,
            &ledger::NewOutbound {
                id: "out-op",
                channel: "discord",
                kind: crate::channels::operator_message::KIND,
                binding_ref: &operator_ref,
                conversation: "discord:1/9",
                job_id: None,
                rendered_text: "Ship it?",
                rendering: "text",
                created_at: at,
            },
        )
        .await
        .unwrap());
        assert!(ledger::mark_sent(db, "out-op", "g-op", None, None, at)
            .await
            .unwrap());
        let rows = [
            ("in-reply", Some("reply:out-op"), None),
            ("in-recent", Some("recent:out-op"), None),
            ("in-gone", Some("reply:out-gone"), None),
            ("in-default", Some("default:prj"), None),
            ("in-typed", Some("<#2>"), None),
            ("in-refused", None, Some("no_default_thread")),
        ];
        for (offset, (id, addressed_by, rejection)) in rows.into_iter().enumerate() {
            assert!(ledger::insert_inbound(
                db,
                &ledger::InboundRecord {
                    id: id.into(),
                    channel: "discord".into(),
                    provider_guid: None,
                    sender: "1234".into(),
                    text: "yes".into(),
                    received_at: at + 1 + offset as i64,
                    rejection_reason: rejection.map(str::to_string),
                    acknowledged_at: None,
                    conversation: Some("discord:1/9".into()),
                    target_uri: rejection
                        .is_none()
                        .then(|| "cairn://p/prj/channels".to_string()),
                    binding_kind: Some("none".into()),
                    addressed_by: addressed_by.map(str::to_string),
                    message_guid: None,
                },
            )
            .await
            .unwrap());
        }

        let rendered = view(&orch, &["prj"], &[]).await;
        for expected in [
            "delivered to cairn://p/prj/channels, as your reply to cairn://operator/m-1 (sent 2026-11-12 05:29:18.189Z)",
            "delivered to cairn://p/prj/channels: no reply-to, and cairn://p/prj/channels sent you the latest operator message on discord within 24 h (cairn://operator/m-1, sent 2026-11-12 05:29:18.189Z)",
            "delivered to cairn://p/prj/channels, as your reply to out out-gone\n",
            "delivered to cairn://p/prj/channels: no reply-to and no operator message on discord in the last 24 h, so the default project's general thread",
            "delivered to cairn://p/prj/channels, addressed by #channels",
            "rejected: no_default_thread (the default project prj has no open general thread)",
        ] {
            assert!(rendered.contains(expected), "{expected}\n{rendered}");
        }
    }

    /// The root says where bare text goes and which general thread the third
    /// rule resolves to now, and says so when that thread is not open.
    #[tokio::test]
    async fn the_root_names_the_general_thread_unaddressed_text_reaches() {
        let orch = conversation_orchestrator().await;
        let request = operator_request("read_batch");
        let closed = render_channels(&orch, &request, &[]).await;
        assert!(
            closed.contains("## Unaddressed text")
                && closed.contains("reply-to wins")
                && closed.contains("latest within 24 h")
                && closed.contains("A single outstanding question or permission ask")
                && closed.contains("- general thread: none; cairn://p/prj/general is missing or closed (default project: prj, oldest project)"),
            "{closed}"
        );
        orch.db
            .local
            .execute_script(
                "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
                 VALUES ('t-general', 'p', 'general', 'active', 1, 1);",
            )
            .await
            .unwrap();
        let open = render_channels(&orch, &request, &[]).await;
        assert!(
            open.contains(
                "- general thread: cairn://p/prj/general (default project: prj, oldest project)"
            ),
            "{open}"
        );
    }

    #[test]
    fn content_over_the_provider_limit_is_refused_not_truncated() {
        assert!(check_send_length("discord", 0, Some(2000)).is_ok());
        let refusal = check_send_length("telegram", 25, Some(4096)).unwrap_err();
        assert!(
            refusal.contains("4096-character") && refusal.contains("cut the last 25 characters"),
            "{refusal}"
        );
        assert!(check_send_length("imessage", 0, None).is_ok());
    }
}
