//! Provider-neutral primitives for delivering human attention gates externally.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::mpsc;

static CHANNEL_RUNTIMES: OnceLock<Mutex<HashMap<&'static str, Arc<dyn ChannelProvider>>>> =
    OnceLock::new();
static ROUTER_BLOCKERS: OnceLock<Mutex<HashMap<&'static str, String>>> = OnceLock::new();
static ADMISSION_STATES: OnceLock<Mutex<HashMap<&'static str, (&'static str, String)>>> =
    OnceLock::new();
static IMESSAGE_ADMISSION_WAKE: OnceLock<tokio::sync::Notify> = OnceLock::new();
static TELEGRAM_ADMISSION_WAKE: OnceLock<tokio::sync::Notify> = OnceLock::new();
static DISCORD_ADMISSION_WAKE: OnceLock<tokio::sync::Notify> = OnceLock::new();
static DISCORD_SURFACE_WAKE: OnceLock<tokio::sync::Notify> = OnceLock::new();
static OPERATOR_PRESENCE_STATE: OnceLock<Mutex<OperatorPresenceState>> = OnceLock::new();
static DESKTOP_ACTIVITY_STATE: OnceLock<Mutex<DesktopActivityState>> = OnceLock::new();
static OPERATOR_PRESENCE_CHANGED: OnceLock<tokio::sync::Notify> = OnceLock::new();
static ROUTE_SUBMISSIONS: OnceLock<
    Mutex<HashMap<&'static str, mpsc::UnboundedSender<crate::routes::ChannelSubmission>>>,
> = OnceLock::new();
const ADMISSION_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const ADMISSION_REPORT_INTERVAL: Duration = Duration::from_secs(60);
const DESKTOP_ACTIVITY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Default)]
struct AdmissionFailureReporter {
    last_error: Option<String>,
    last_reported_at: Option<Instant>,
    suppressed: u64,
}

pub fn wake_discord_surfaces() {
    DISCORD_SURFACE_WAKE
        .get_or_init(tokio::sync::Notify::new)
        .notify_one();
}

pub async fn ensure_discord_thread_surface(
    orch: &crate::orchestrator::Orchestrator,
    db: &crate::storage::LocalDb,
    project_id: &str,
    thread_name: &str,
) -> Result<(), String> {
    let config = crate::config::settings::load_settings(&orch.config_dir)
        .channels
        .discord;
    if !config.enabled {
        return Ok(());
    }
    let guild_id = config
        .guild_id
        .parse::<u64>()
        .map_err(|_| "Discord guild ID must be an unsigned integer".to_string())?;
    let project = crate::projects::crud::get_db(db, project_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("Project not found after thread creation: {project_id}"))?;
    let now = chrono::Utc::now().timestamp();
    let category = discord_surfaces::ensure_surface(
        db,
        guild_id,
        discord_surfaces::SurfaceKind::ProjectCategory,
        &project.key,
        None,
        None,
        now,
    )
    .await?;
    let target = format!("cairn://p/{}/{}", project.key, thread_name);
    discord_surfaces::ensure_surface(
        db,
        guild_id,
        discord_surfaces::SurfaceKind::ThreadChannel,
        &project.key,
        Some(&target),
        Some(category.id),
        now,
    )
    .await?;
    wake_discord_surfaces();
    Ok(())
}

fn route_submission_provider(
    submission: &crate::routes::ChannelSubmission,
) -> Option<&'static str> {
    submission
        .destination
        .as_ref()
        .map(|address| address.provider().id())
}

pub async fn configured_conversations_json(
    orch: &crate::orchestrator::Orchestrator,
) -> serde_json::Value {
    serde_json::to_value(crate::resources::channels::configured_conversations(orch).await)
        .expect("conversation rows serialize")
}

async fn supervise_discord(orch: crate::orchestrator::Orchestrator) {
    loop {
        let config = crate::config::settings::load_settings(&orch.config_dir)
            .channels
            .discord;
        if !config.enabled {
            clear_admission_state("discord");
            tokio::select! {
                () = DISCORD_ADMISSION_WAKE.get_or_init(tokio::sync::Notify::new).notified() => {},
                () = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {},
            }
            continue;
        }
        let error = if config.guild_id.trim().is_empty() {
            Some("Discord channel requires a guild ID".to_string())
        } else if config.guild_id.parse::<u64>().is_err() {
            Some("Discord guild ID must be an unsigned integer".to_string())
        } else if config.allow_from.is_empty() {
            Some("Discord channel requires at least one allowed user ID".to_string())
        } else {
            None
        };
        let allowed = config
            .allow_from
            .iter()
            .map(|id| id.parse::<u64>())
            .collect::<Result<Vec<_>, _>>();
        let token = crate::security::broker::web_provider_key(
            "channel/discord",
            "BOT_TOKEN",
            "connect the Discord channel",
        );
        let validation = error
            .or_else(|| {
                allowed
                    .as_ref()
                    .err()
                    .map(|_| "Discord allowlist entries must be numeric user IDs".to_string())
            })
            .or_else(|| {
                token
                    .as_ref()
                    .is_none()
                    .then(|| "Discord channel token is missing from the keychain".to_string())
            });
        if let Some(error) = validation {
            set_admission_state("discord", Some(("stopped", error)));
            tokio::select! {
                () = DISCORD_ADMISSION_WAKE.get_or_init(tokio::sync::Notify::new).notified() => {},
                () = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {},
            }
            continue;
        }
        let provider = discord::DiscordProvider::new(
            token.unwrap().expose().to_string(),
            serenity::all::GuildId::new(
                config
                    .guild_id
                    .parse::<u64>()
                    .expect("validated Discord guild ID"),
            ),
            allowed.unwrap(),
        );
        runtime_slot()
            .lock()
            .expect("channel runtime lock poisoned")
            .insert("discord", provider.clone());
        clear_admission_state("discord");
        set_router_blocker("discord", None);
        let mut tasks = router::spawn(
            orch.clone(),
            provider.clone(),
            "discord",
            config.guild_id.clone(),
            config.route.clone(),
            config.inbound_capabilities,
        );
        let (mut gateway, shard_manager) = match provider.start().await {
            Ok(gateway) => gateway,
            Err(error) => {
                set_admission_state("discord", Some(("stopped", error)));
                runtime_slot()
                    .lock()
                    .expect("channel runtime lock poisoned")
                    .remove("discord");
                for task in tasks.drain(..) {
                    task.abort();
                }
                tokio::time::sleep(ADMISSION_RETRY_INTERVAL).await;
                continue;
            }
        };
        let surface_orch = orch.clone();
        let surface_provider: Arc<dyn discord::DiscordApi> = provider.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let now = chrono::Utc::now().timestamp();
                for db in surface_orch.db.all_dbs().await {
                    let reconciler = discord_surfaces::DiscordSurfaceReconciler::with_binding_db(
                        db,
                        surface_orch.db.local.clone(),
                        surface_provider.clone(),
                    );
                    if let Err(error) = reconciler.reconcile_due(now).await {
                        log::warn!("Discord surface reconciliation failed: {error}");
                    }
                }
                tokio::select! {
                    () = DISCORD_SURFACE_WAKE.get_or_init(tokio::sync::Notify::new).notified() => {},
                    () = tokio::time::sleep(Duration::from_secs(30)) => {},
                }
            }
        }));
        let mut gateway_finished = false;
        loop {
            tokio::select! {
                result = &mut gateway => {
                    gateway_finished = true;
                    if let Ok(Err(error)) = result {
                        set_admission_state("discord", Some(("stopped", error)));
                    }
                    break;
                }
                () = DISCORD_ADMISSION_WAKE.get_or_init(tokio::sync::Notify::new).notified() => break,
                () = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {
                    let current = crate::config::settings::load_settings(&orch.config_dir).channels.discord;
                    if current != config { break; }
                }
            }
        }
        if !gateway_finished {
            shard_manager.shutdown_all().await;
            let _ = gateway.await;
        }
        for task in tasks.drain(..) {
            task.abort();
        }
        runtime_slot()
            .lock()
            .expect("channel runtime lock poisoned")
            .remove("discord");
        route_submission_slot()
            .lock()
            .expect("route submission slot poisoned")
            .remove("discord");
        set_router_blocker("discord", None);
    }
}

pub fn retry_discord_admission() {
    DISCORD_ADMISSION_WAKE
        .get_or_init(tokio::sync::Notify::new)
        .notify_waiters();
}

pub fn retry_telegram_admission() {
    TELEGRAM_ADMISSION_WAKE
        .get_or_init(tokio::sync::Notify::new)
        .notify_waiters();
}

fn route_submission_slot(
) -> &'static Mutex<HashMap<&'static str, mpsc::UnboundedSender<crate::routes::ChannelSubmission>>>
{
    ROUTE_SUBMISSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Hands a channel route to the optional external-channel runtime. Non-channel
/// sinks are independent of channel admission, so absence here is deliberately soft.
pub fn submit_route(
    submission: crate::routes::ChannelSubmission,
) -> Result<(), Box<crate::routes::ChannelSubmission>> {
    let senders = route_submission_slot()
        .lock()
        .expect("route submission slot poisoned")
        .clone();
    if senders.is_empty() {
        return Err(Box::new(submission));
    }

    let directed_provider = route_submission_provider(&submission);
    let mut delivered = false;
    for (provider, sender) in senders {
        if directed_provider.is_some_and(|directed| directed != provider) {
            continue;
        }
        if sender.send(submission.clone()).is_ok() {
            delivered = true;
        } else {
            route_submission_slot()
                .lock()
                .expect("route submission slot poisoned")
                .remove(provider);
        }
    }
    delivered.then_some(()).ok_or(Box::new(submission))
}

fn resolve_presence_with_lock(
    state: &mut OperatorPresenceState,
    inferred: OperatorPresence,
    locked: bool,
    now: Instant,
) -> OperatorPresence {
    if locked && state.mode == OperatorPresenceMode::Active {
        state.mode = OperatorPresenceMode::Auto;
    }
    resolve_presence(state, inferred, now)
}

fn select_inferred_presence(
    desktop: Option<OperatorPresence>,
    channel_fallback: OperatorPresence,
) -> OperatorPresence {
    desktop.unwrap_or(channel_fallback)
}

#[derive(Debug, Default)]
struct DesktopActivityState {
    last_sample: Option<DesktopPresenceSample>,
}

#[derive(Debug, Clone, Copy)]
struct DesktopPresenceSample {
    sampled_at: Instant,
    idle_seconds: u64,
    locked: bool,
}

fn desktop_activity_state() -> &'static Mutex<DesktopActivityState> {
    DESKTOP_ACTIVITY_STATE.get_or_init(|| Mutex::new(DesktopActivityState::default()))
}

/// Records activity observed by the desktop app. The runner owns the timestamp
/// so callers cannot forge freshness with a client clock.
///
/// The wake is emitted for a CHANGE in inferred presence, not for the report.
/// The desktop beacon reports every five seconds for as long as the app is open,
/// and almost every one of those samples says exactly what the last one said;
/// waking every channel sweep on each of them made the sweep loops' presence arm
/// permanently ready, which doubles the sweep rate at idle and removes the sleep
/// entirely once a sweep runs longer than its own cadence (CAIRN-4208).
pub fn report_desktop_presence(idle_seconds: u64, locked: bool) {
    let now = Instant::now();
    let changed = {
        let mut state = desktop_activity_state()
            .lock()
            .expect("desktop activity state lock poisoned");
        // Both sides are inferred at the same instant, so this compares the
        // sample rather than the ageing of the previous one.
        let before = desktop_inferred_presence(state.last_sample, now);
        state.last_sample = Some(DesktopPresenceSample {
            sampled_at: now,
            idle_seconds,
            locked,
        });
        before != desktop_inferred_presence(state.last_sample, now)
    };
    if changed {
        OPERATOR_PRESENCE_CHANGED
            .get_or_init(tokio::sync::Notify::new)
            .notify_one();
    }
}

fn desktop_inferred_presence(
    sample: Option<DesktopPresenceSample>,
    now: Instant,
) -> Option<(OperatorPresence, bool)> {
    sample.map(|sample| {
        let sample_age = now.saturating_duration_since(sample.sampled_at);
        let effective_idle = Duration::from_secs(sample.idle_seconds).saturating_add(sample_age);
        let presence = if !sample.locked && effective_idle < DESKTOP_ACTIVITY_TIMEOUT {
            OperatorPresence::Present
        } else {
            OperatorPresence::Away
        };
        (presence, sample.locked)
    })
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorPresenceStatus {
    pub mode: OperatorPresenceMode,
    pub presence: OperatorPresence,
}

/// Returns the runner's single resolved presence truth without requiring a
/// channel runtime. A missing provider fails closed to away unless the operator
/// has explicitly pinned active, so presence consumers always have a state.
pub async fn operator_presence_status() -> OperatorPresenceStatus {
    let runtime = runtime_slot()
        .lock()
        .expect("channel runtime lock poisoned")
        .clone();
    let provider = runtime.get("imessage").or_else(|| runtime.values().next());
    let presence = operator_presence(provider.map(|provider| provider.as_ref())).await;
    let mode = presence_state()
        .lock()
        .expect("operator presence state lock poisoned")
        .mode;
    OperatorPresenceStatus { mode, presence }
}

fn admission_state_slot() -> &'static Mutex<HashMap<&'static str, (&'static str, String)>> {
    ADMISSION_STATES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn set_admission_state(provider: &'static str, state: Option<(&'static str, String)>) {
    let mut states = admission_state_slot()
        .lock()
        .expect("channel admission state lock poisoned");
    match state {
        Some(state) => {
            states.insert(provider, state);
        }
        None => {
            states.remove(provider);
        }
    }
}

fn clear_admission_state(provider: &'static str) {
    set_admission_state(provider, None);
}

pub fn retry_admission() {
    IMESSAGE_ADMISSION_WAKE
        .get_or_init(tokio::sync::Notify::new)
        .notify_waiters();
}

fn router_blocker_slot() -> &'static Mutex<HashMap<&'static str, String>> {
    ROUTER_BLOCKERS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn set_router_blocker(provider: &'static str, error: Option<String>) {
    let mut blockers = router_blocker_slot()
        .lock()
        .expect("channel router blocker lock poisoned");
    match error {
        Some(error) => {
            blockers.insert(provider, error);
        }
        None => {
            blockers.remove(provider);
        }
    }
}

/// Why an outbound message exists. Direct operator responses remain conversation;
/// standing subscriptions are Cairn-initiated pushes and obey presence policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OutboundInitiator {
    OperatorInbound,
    OperatorSubscription,
    CairnPush,
}

impl OutboundInitiator {
    pub fn is_presence_aware(self) -> bool {
        matches!(self, Self::OperatorSubscription | Self::CairnPush)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedQuestionMessage {
    pub conversation: String,
    pub provider_guid: String,
    pub caption_guid: Option<String>,
    pub sent_at: i64,
    pub receipt: String,
}

impl AdmissionFailureReporter {
    fn record(&mut self, now: Instant, error: &str) -> Option<String> {
        let changed = self.last_error.as_deref() != Some(error);
        let report_due = self
            .last_reported_at
            .is_none_or(|last| now.duration_since(last) >= ADMISSION_REPORT_INTERVAL);
        if changed || report_due {
            let suffix = if changed || self.suppressed == 0 {
                String::new()
            } else {
                format!(" ({} identical attempts suppressed)", self.suppressed)
            };
            self.last_error = Some(error.to_string());
            self.last_reported_at = Some(now);
            self.suppressed = 0;
            Some(format!("{error}{suffix}"))
        } else {
            self.suppressed = self.suppressed.saturating_add(1);
            None
        }
    }
}

async fn supervise_admission<L, C, F, Fut, R, W, WFut>(
    mut load: L,
    mut run: F,
    mut report: R,
    mut wait_to_retry: W,
) where
    L: FnMut() -> Option<C>,
    F: FnMut(C) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
    R: FnMut(String),
    W: FnMut() -> WFut,
    WFut: std::future::Future<Output = ()>,
{
    let mut failures = AdmissionFailureReporter::default();
    loop {
        let Some(config) = load() else {
            return;
        };
        match run(config).await {
            Ok(()) => return,
            Err(error) => {
                if let Some(message) = failures.record(Instant::now(), &error) {
                    report(message);
                }
                wait_to_retry().await;
            }
        }
    }
}

fn runtime_slot() -> &'static Mutex<HashMap<&'static str, Arc<dyn ChannelProvider>>> {
    CHANNEL_RUNTIMES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OperatorPresence {
    Present,
    Away,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum OperatorPresenceMode {
    #[default]
    Auto,
    Active,
    Idle,
}

#[derive(Debug)]
struct OperatorPresenceState {
    mode: OperatorPresenceMode,
}

impl Default for OperatorPresenceState {
    fn default() -> Self {
        Self {
            mode: OperatorPresenceMode::Auto,
        }
    }
}

fn presence_state() -> &'static Mutex<OperatorPresenceState> {
    OPERATOR_PRESENCE_STATE.get_or_init(|| Mutex::new(OperatorPresenceState::default()))
}

fn resolve_presence(
    state: &mut OperatorPresenceState,
    inferred: OperatorPresence,
    _now: Instant,
) -> OperatorPresence {
    match state.mode {
        OperatorPresenceMode::Auto => inferred,
        OperatorPresenceMode::Active => OperatorPresence::Present,
        OperatorPresenceMode::Idle => OperatorPresence::Away,
    }
}

pub fn set_operator_presence_mode(mode: OperatorPresenceMode) {
    let mut state = presence_state()
        .lock()
        .expect("operator presence state lock poisoned");
    state.mode = mode;
    OPERATOR_PRESENCE_CHANGED
        .get_or_init(tokio::sync::Notify::new)
        .notify_one();
}

pub(super) async fn wait_for_presence_change() {
    OPERATOR_PRESENCE_CHANGED
        .get_or_init(tokio::sync::Notify::new)
        .notified()
        .await;
}

pub async fn operator_presence(provider: Option<&dyn ChannelProvider>) -> OperatorPresence {
    let mode = presence_state()
        .lock()
        .expect("operator presence state lock poisoned")
        .mode;
    let desktop_inferred = desktop_inferred_presence(
        desktop_activity_state()
            .lock()
            .expect("desktop activity state lock poisoned")
            .last_sample,
        Instant::now(),
    );
    let channel_fallback = match desktop_inferred {
        Some(_) => OperatorPresence::Away,
        None => match provider {
            Some(provider) => provider.operator_presence().await,
            None if mode == OperatorPresenceMode::Active => OperatorPresence::Present,
            None => OperatorPresence::Away,
        },
    };
    let inferred = select_inferred_presence(
        desktop_inferred.map(|(presence, _)| presence),
        channel_fallback,
    );
    resolve_presence_with_lock(
        &mut presence_state()
            .lock()
            .expect("operator presence state lock poisoned"),
        inferred,
        desktop_inferred.is_some_and(|(_, locked)| locked),
        Instant::now(),
    )
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelRuntimeStatus {
    pub provider: &'static str,
    pub state: &'static str,
    pub detail: Option<String>,
    pub feed_state: &'static str,
    pub watch_started_at: Option<i64>,
    pub last_receipt_at: Option<i64>,
    pub last_receipt_rowid: Option<i64>,
    pub unsolicited_inbound: Vec<ChannelInboundSummary>,
    pub presence_mode: OperatorPresenceMode,
    pub operator_presence: OperatorPresence,
    pub last_send_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelInboundSummary {
    pub id: String,
    pub sender: String,
    pub text: String,
    pub received_at: i64,
}

fn classify_runtime_health(
    health: Option<ChannelHealth>,
    router_blocker: Option<String>,
) -> (&'static str, Option<String>) {
    match router_blocker {
        Some(error) => (
            "dead",
            Some(format!("blocked: backlog seal failed: {error}")),
        ),
        None => match health {
            Some(ChannelHealth::Ready) => ("bridgeUp", None),
            Some(ChannelHealth::Degraded { reason }) => ("degraded", Some(reason)),
            Some(ChannelHealth::Unavailable { reason }) if reason.contains("offline") => {
                ("executorOffline", Some(reason))
            }
            Some(ChannelHealth::Unavailable { reason }) => ("dead", Some(reason)),
            None => ("dead", Some("channel is not running".into())),
        },
    }
}

pub async fn runtime_status(
    orch: &crate::orchestrator::Orchestrator,
    provider_id: &'static str,
) -> ChannelRuntimeStatus {
    let runtime = runtime_slot()
        .lock()
        .expect("channel runtime lock poisoned")
        .get(provider_id)
        .cloned();
    let health = runtime.as_ref().map(|provider| provider.health());
    let liveness = runtime.as_ref().map(|provider| provider.liveness());
    let operator_presence = operator_presence(
        runtime
            .as_deref()
            .map(|provider| provider as &dyn ChannelProvider),
    )
    .await;
    let presence_mode = presence_state()
        .lock()
        .expect("operator presence state lock poisoned")
        .mode;
    let router_blocker = router_blocker_slot()
        .lock()
        .expect("channel router blocker lock poisoned")
        .get(provider_id)
        .cloned();
    let admission = admission_state_slot()
        .lock()
        .expect("channel admission state lock poisoned")
        .get(provider_id)
        .cloned();
    let (state, detail) = if runtime.is_none() {
        admission
            .map(|(state, detail)| (state, Some(detail)))
            .unwrap_or_else(|| classify_runtime_health(health, router_blocker))
    } else {
        classify_runtime_health(health, router_blocker)
    };
    let unsolicited_inbound = ledger::list_inbound(&orch.db.local, provider_id, 50)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|record| ChannelInboundSummary {
            id: record.id,
            sender: record.sender,
            text: record.text,
            received_at: record.received_at,
        })
        .collect();
    let last_send_error = ledger::latest_send_error(&orch.db.local, provider_id)
        .await
        .unwrap_or_default();
    ChannelRuntimeStatus {
        provider: provider_id,
        state,
        detail,
        feed_state: feed_state(liveness, chrono::Utc::now().timestamp()),
        watch_started_at: liveness.and_then(|value| value.started_at),
        last_receipt_at: liveness.and_then(|value| value.last_receipt_at),
        last_receipt_rowid: liveness.and_then(|value| value.last_receipt_rowid),
        unsolicited_inbound,
        presence_mode,
        operator_presence,
        last_send_error,
    }
}

const TELEGRAM_OWN_ID_GUIDANCE: &str =
    "This is the bot's own ID; use your Telegram user ID — message @userinfobot to get it";

fn telegram_bot_id(token: &str) -> Option<&str> {
    let (prefix, _) = token.trim().split_once(':')?;
    (!prefix.is_empty() && prefix.bytes().all(|byte| byte.is_ascii_digit())).then_some(prefix)
}

pub fn telegram_identity_error(
    chat_id: &str,
    allow_from: &[String],
    token: &str,
) -> Option<String> {
    let bot_id = telegram_bot_id(token)?;
    (chat_id.trim() == bot_id || allow_from.iter().any(|id| id.trim() == bot_id))
        .then(|| TELEGRAM_OWN_ID_GUIDANCE.to_string())
}

pub(crate) fn telegram_identity_error_for_brokered_token(
    chat_id: &str,
    allow_from: &[String],
    token: &crate::security::broker::BrokeredSecret,
) -> Option<String> {
    telegram_identity_error(chat_id, allow_from, token.expose())
}

const RECEIPT_RECENCY_SECONDS: i64 = 5 * 60;

fn feed_state(liveness: Option<ChannelLiveness>, now: i64) -> &'static str {
    let Some(liveness) = liveness else {
        return "stopped";
    };
    if !liveness.active {
        return "stopped";
    }
    match (liveness.started_at, liveness.last_receipt_at) {
        (_, Some(receipt)) if now.saturating_sub(receipt) <= RECEIPT_RECENCY_SECONDS => "receiving",
        (Some(started), None) if now.saturating_sub(started) <= RECEIPT_RECENCY_SECONDS => {
            "connected"
        }
        _ => "silent",
    }
}

pub mod address;
pub mod bindings;
pub mod commands;
pub mod discord;
pub mod discord_surfaces;
pub mod imessage;
pub mod ledger;
pub mod router;
pub mod telegram;

pub use address::{
    conversation_capabilities, ConversationAddress, ConversationAddressError,
    ConversationCapabilities, ConversationDestination, ConversationProvider,
};

/// Starts the configured external channel service on runner hosts.
pub fn spawn_configured(orch: crate::orchestrator::Orchestrator) {
    crate::routes::spawn_attention_routes(orch.clone());
    tokio::spawn(supervise_telegram(orch.clone()));
    tokio::spawn(supervise_discord(orch.clone()));
    tokio::spawn(async move {
        loop {
            let config = crate::config::settings::load_settings(&orch.config_dir)
                .channels
                .imessage;
            if config.enabled && !config.to.trim().is_empty() {
                if let Some(name) = config.executor.as_deref() {
                    if !orch.fleet.named_executor_is_connected(name) {
                        set_admission_state(
                            "imessage",
                            Some((
                                "waitingForExecutor",
                                format!("Waiting for executor `{name}` to attach"),
                            )),
                        );
                        tokio::select! {
                            () = orch.fleet.wait_for_named_executor(name) => {},
                            () = IMESSAGE_ADMISSION_WAKE.get_or_init(tokio::sync::Notify::new).notified() => {},
                            () = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {},
                        }
                        continue;
                    }
                }
                supervise_admission(
                    || {
                        let config = crate::config::settings::load_settings(&orch.config_dir)
                            .channels
                            .imessage;
                        (config.enabled && !config.to.trim().is_empty()).then_some(config)
                    },
                    |config| spawn_imessage(orch.clone(), config),
                    |report| {
                        set_admission_state("imessage", Some(("stopped", report.clone())));
                        log::error!("{report}")
                    },
                    || {
                        let orch = orch.clone();
                        async move {
                            let executor = crate::config::settings::load_settings(&orch.config_dir)
                                .channels
                                .imessage
                                .executor;
                            if let Some(name) = executor.as_deref() {
                                if !orch.fleet.named_executor_is_connected(name) {
                                    tokio::select! {
                                        () = orch.fleet.wait_for_named_executor(name) => {},
                                        () = IMESSAGE_ADMISSION_WAKE.get_or_init(tokio::sync::Notify::new).notified() => {},
                                        () = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {},
                                    }
                                    return;
                                }
                            }
                            tokio::select! {
                                () = IMESSAGE_ADMISSION_WAKE.get_or_init(tokio::sync::Notify::new).notified() => {},
                                () = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {},
                            }
                        }
                    },
                )
                .await;
                clear_admission_state("imessage");
            } else {
                clear_admission_state("imessage");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    });
}

async fn spawn_imessage(
    orch: crate::orchestrator::Orchestrator,
    config: crate::models::IMessageChannelConfig,
) -> Result<(), String> {
    use cairn_common::executor_protocol::{OwnerDeathPolicy, ResidencyFootprint};

    let Some(name) = config.executor.as_deref() else {
        return Err(
            "iMessage channel requires a named executor so Messages account and TCC identity are explicit; channel remains stopped"
                .into(),
        );
    };
    let executor: Arc<dyn imessage::IMessageExecutor> =
        match crate::fleet::service_placement::acquire_service_lease(
            &orch,
            crate::fleet::service_placement::ServiceIdentity {
                id: "channel-imessage",
                label: "iMessage channel",
            },
            name,
            ResidencyFootprint {
                memory_bytes: 64 * 1024 * 1024,
                disk_growth_bytes: 64 * 1024 * 1024,
            },
            OwnerDeathPolicy {
                heartbeat_timeout_ms: 5 * 60 * 1000,
                reclaim_grace_ms: 60 * 1000,
            },
        )
        .await
        {
            Ok(lease) => {
                let lease = Arc::new(lease);
                lease.spawn_renewal();
                Arc::new(imessage::PlacedProcessExecutor::new(lease))
            }
            Err(error) => {
                return Err(format!(
                    "iMessage channel executor `{name}` is unavailable; channel remains stopped: {error}"
                ));
            }
        };
    let provider = Arc::new(imessage::IMessageProvider::new(
        executor.clone(),
        config.allow_from.clone(),
    ));
    set_admission_state("imessage", None);
    set_router_blocker("imessage", None);
    runtime_slot()
        .lock()
        .expect("channel runtime lock poisoned")
        .insert("imessage", provider.clone());
    let health_task = provider.spawn_health_monitor();
    let watch_provider = provider.clone();
    let cursor_db = orch.db.local.clone();
    let watch_task = tokio::spawn(async move {
        loop {
            let since = ledger::get_cursor(&cursor_db, "imessage")
                .await
                .ok()
                .flatten()
                .unwrap_or(0);
            let (cursor_tx, mut cursor_rx) = tokio::sync::mpsc::channel(128);
            let mut watch = watch_provider.spawn_watch(since, cursor_tx);
            let failure = loop {
                tokio::select! {
                    rowid = cursor_rx.recv() => match rowid {
                        Some(rowid) => {
                            if let Err(error) = ledger::advance_cursor(&cursor_db, "imessage", rowid).await {
                                log::warn!("failed to persist iMessage cursor: {error}");
                            }
                        }
                        None => break watch.await,
                    },
                    result = &mut watch => break result,
                }
            };
            while let Ok(rowid) = cursor_rx.try_recv() {
                if let Err(error) = ledger::advance_cursor(&cursor_db, "imessage", rowid).await {
                    log::warn!("failed to persist iMessage cursor: {error}");
                }
            }
            match failure {
                Ok(Ok(())) => log::warn!("iMessage watch ended; restarting from durable cursor"),
                Ok(Err(error)) => {
                    log::warn!("iMessage watch failed; restarting from durable cursor: {error}")
                }
                Err(error) => log::warn!(
                    "iMessage watch task failed; restarting from durable cursor: {error}"
                ),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    let mut tasks = vec![health_task, watch_task];
    tasks.extend(router::spawn(
        orch.clone(),
        provider.clone(),
        "imessage",
        config.to.clone(),
        config.route.clone(),
        config.inbound_capabilities,
    ));

    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let current = crate::config::settings::load_settings(&orch.config_dir)
            .channels
            .imessage;
        if current != config {
            for task in tasks {
                task.abort();
            }
            executor.shutdown().await;
            runtime_slot()
                .lock()
                .expect("channel runtime lock poisoned")
                .remove("imessage");
            set_router_blocker("imessage", None);
            return Ok(());
        }
    }
}

async fn supervise_telegram(orch: crate::orchestrator::Orchestrator) {
    loop {
        let config = crate::config::settings::load_settings(&orch.config_dir)
            .channels
            .telegram;
        if !config.enabled {
            clear_admission_state("telegram");
            tokio::select! {
                () = TELEGRAM_ADMISSION_WAKE.get_or_init(tokio::sync::Notify::new).notified() => {},
                () = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {},
            }
            continue;
        }
        let error = if config.chat_id.trim().is_empty() {
            Some("Telegram channel requires a chat ID".to_string())
        } else if config.chat_id.parse::<i64>().is_err() {
            Some("Telegram chat ID must be a signed integer".to_string())
        } else if config.allow_from.is_empty() {
            Some("Telegram channel requires at least one allowed user ID".to_string())
        } else {
            None
        };
        let allowed = config
            .allow_from
            .iter()
            .map(|id| id.parse::<u64>())
            .collect::<Result<Vec<_>, _>>();
        let token = crate::security::broker::web_provider_key(
            "channel/telegram",
            "BOT_TOKEN",
            "connect the Telegram channel",
        );
        let validation = error
            .or_else(|| {
                allowed
                    .as_ref()
                    .err()
                    .map(|_| "Telegram allowlist entries must be numeric user IDs".to_string())
            })
            .or_else(|| {
                token
                    .as_ref()
                    .is_none()
                    .then(|| "Telegram channel token is missing from the keychain".to_string())
            })
            .or_else(|| {
                telegram_identity_error(
                    &config.chat_id,
                    &config.allow_from,
                    token.as_ref()?.expose(),
                )
            });
        if let Some(error) = validation {
            set_admission_state("telegram", Some(("stopped", error)));
            tokio::select! {
                () = TELEGRAM_ADMISSION_WAKE.get_or_init(tokio::sync::Notify::new).notified() => {},
                () = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {},
            }
            continue;
        }
        let chat_id = config
            .chat_id
            .parse::<i64>()
            .expect("validated Telegram chat ID");
        let provider = telegram::TelegramProvider::new(
            token.unwrap().expose().to_string(),
            teloxide::types::ChatId(chat_id),
            allowed.unwrap(),
        );
        runtime_slot()
            .lock()
            .expect("channel runtime lock poisoned")
            .insert("telegram", provider.clone());
        clear_admission_state("telegram");
        set_router_blocker("telegram", None);
        let mut tasks = router::spawn(
            orch.clone(),
            provider.clone(),
            "telegram",
            config.chat_id.clone(),
            config.route.clone(),
            config.inbound_capabilities,
        );
        let mut polling = provider.start();
        let mut polling_finished = false;
        loop {
            tokio::select! {
                result = &mut polling => {
                    polling_finished = true;
                    log::warn!("Telegram polling stopped: {result:?}");
                    break;
                }
                () = TELEGRAM_ADMISSION_WAKE.get_or_init(tokio::sync::Notify::new).notified() => {
                    break;
                }
                () = tokio::time::sleep(ADMISSION_RETRY_INTERVAL) => {
                    let current = crate::config::settings::load_settings(&orch.config_dir).channels.telegram;
                    if current != config { break; }
                }
            }
        }
        if !polling_finished {
            polling.abort();
            let _ = polling.await;
        }
        for task in tasks.drain(..) {
            task.abort();
        }
        runtime_slot()
            .lock()
            .expect("channel runtime lock poisoned")
            .remove("telegram");
        route_submission_slot()
            .lock()
            .expect("route submission slot poisoned")
            .remove("telegram");
        set_router_blocker("telegram", None);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelCapabilities {
    pub structured_asks: bool,
    pub open_options: bool,
    pub edit_in_place: bool,
    pub max_text_len: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AskOption {
    pub label: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum OutboundAsk {
    Question {
        prompt_id: String,
        question_index: usize,
        text: String,
        options: Vec<AskOption>,
    },
    Permission {
        request_id: String,
        summary: String,
    },
    Notify {
        text: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutboundMessage {
    pub intent_id: String,
    pub conversation: String,
    pub initiated_by: OutboundInitiator,
    pub ask: OutboundAsk,
    pub context_header: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentIds {
    pub primary_guid: String,
    pub caption_guid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum InboundEvent {
    Selection {
        conversation: String,
        bound_guid: String,
        sender: String,
        option_id: String,
        option_text: String,
        selected: bool,
    },
    Selections {
        conversation: String,
        bound_guid: String,
        sender: String,
        changes: Vec<PollSelectionChange>,
    },
    Reply {
        conversation: String,
        bound_guid: String,
        sender: String,
        text: String,
    },
    Bare {
        conversation: String,
        sender: String,
        text: String,
    },
    /// Recording-only input from a sender rejected at the provider boundary.
    Rejected {
        conversation: String,
        sender: String,
        text: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollSelectionChange {
    pub option_id: String,
    pub option_text: String,
    pub selected: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChannelLiveness {
    pub active: bool,
    pub started_at: Option<i64>,
    pub last_receipt_at: Option<i64>,
    pub last_receipt_rowid: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum ChannelHealth {
    Ready,
    Degraded { reason: String },
    Unavailable { reason: String },
}

#[async_trait]
pub trait ChannelProvider: Send + Sync {
    fn capabilities(&self) -> ChannelCapabilities;
    async fn send(&self, message: &OutboundMessage) -> Result<SentIds, String>;
    fn subscribe(&self) -> mpsc::Receiver<InboundEvent>;
    fn health(&self) -> ChannelHealth;
    fn liveness(&self) -> ChannelLiveness {
        ChannelLiveness::default()
    }
    /// Headless fallback for a provider running on the same console as the
    /// runner. Remotely placed providers must use the default Away result:
    /// service placement is never evidence of operator presence.
    async fn operator_presence(&self) -> OperatorPresence {
        OperatorPresence::Away
    }
    async fn cleanup_question(&self, _message: &ResolvedQuestionMessage) -> Result<(), String> {
        Ok(())
    }
}

/// Renders the provider-independent plain-text floor for an outbound ask.
pub fn render_text_floor(ask: &OutboundAsk) -> String {
    match ask {
        OutboundAsk::Question { text, options, .. } => {
            if options.is_empty() {
                return format!("{text}\n\nReply to this message with your answer.");
            }

            let options = options
                .iter()
                .enumerate()
                .map(|(index, option)| match &option.description {
                    Some(description) if !description.trim().is_empty() => {
                        format!("{}. {} — {}", index + 1, option.label, description)
                    }
                    _ => format!("{}. {}", index + 1, option.label),
                })
                .collect::<Vec<_>>()
                .join("\n");
            format!(
                "{text}\n\n{options}\n\nReply to this message with a number or your answer."
            )
        }
        OutboundAsk::Permission { summary, .. } => format!(
            "{summary}\n\n1. Approve\n2. Deny\n\nReply to this message with a number or your answer."
        ),
        OutboundAsk::Notify { text } => text.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[serial_test::serial(operator_presence)]
    async fn presence_change_is_retained_until_the_router_waits() {
        set_operator_presence_mode(OperatorPresenceMode::Idle);
        tokio::time::timeout(Duration::from_millis(50), wait_for_presence_change())
            .await
            .expect("a presence change emitted before the wait must retain a wake permit");
        set_operator_presence_mode(OperatorPresenceMode::Auto);
        wait_for_presence_change().await;
    }

    #[tokio::test]
    #[serial_test::serial(operator_presence)]
    async fn presence_status_remains_operable_without_a_channel_runtime() {
        assert!(runtime_slot().lock().unwrap().is_empty());

        set_operator_presence_mode(OperatorPresenceMode::Active);
        let active = operator_presence_status().await;
        assert_eq!(active.mode, OperatorPresenceMode::Active);
        assert_eq!(active.presence, OperatorPresence::Present);

        set_operator_presence_mode(OperatorPresenceMode::Idle);
        let idle = operator_presence_status().await;
        assert_eq!(idle.mode, OperatorPresenceMode::Idle);
        assert_eq!(idle.presence, OperatorPresence::Away);

        set_operator_presence_mode(OperatorPresenceMode::Auto);
    }

    #[test]
    fn desktop_inference_is_independent_of_remote_channel_residency() {
        assert_eq!(
            select_inferred_presence(Some(OperatorPresence::Present), OperatorPresence::Away,),
            OperatorPresence::Present
        );
        assert_eq!(
            select_inferred_presence(Some(OperatorPresence::Away), OperatorPresence::Present),
            OperatorPresence::Away
        );
    }

    #[test]
    fn directed_route_selects_only_its_addressed_provider() {
        use crate::routes::{ChannelSubmission, RouteFact};

        let submission = ChannelSubmission {
            route_id: "route-test".into(),
            scope_key: "workspace".into(),
            project_id: None,
            fact: RouteFact {
                source: "attention".into(),
                identity: "directed".into(),
                fields: Default::default(),
                origin: None,
                summary: None,
                route_provenance: None,
            },
            transforms_json: None,
            created_at: 1,
            binding_ref: "route:route-test:directed".into(),
            text: "notify".into(),
            context: "[Cairn]".into(),
            job_id: None,
            initiated_by: None,
            destination: Some("discord:1/2".parse().unwrap()),
        };
        assert_eq!(route_submission_provider(&submission), Some("discord"));
        let providers = ["imessage", "telegram", "discord"];
        assert_eq!(
            providers
                .into_iter()
                .filter(|provider| route_submission_provider(&submission) == Some(*provider))
                .collect::<Vec<_>>(),
            ["discord"]
        );
    }

    #[test]
    fn desktop_activity_expires_after_the_presence_window() {
        let start = Instant::now();
        assert_eq!(desktop_inferred_presence(None, start), None);
        let sample = DesktopPresenceSample {
            sampled_at: start,
            idle_seconds: 0,
            locked: false,
        };
        assert_eq!(
            desktop_inferred_presence(
                Some(sample),
                start + DESKTOP_ACTIVITY_TIMEOUT - Duration::from_millis(1),
            ),
            Some((OperatorPresence::Present, false))
        );
        assert_eq!(
            desktop_inferred_presence(Some(sample), start + DESKTOP_ACTIVITY_TIMEOUT),
            Some((OperatorPresence::Away, false))
        );
    }

    #[test]
    fn manual_presence_remains_pinned_despite_contrary_inference() {
        let start = Instant::now();
        let mut state = OperatorPresenceState {
            mode: OperatorPresenceMode::Idle,
        };

        assert_eq!(
            resolve_presence(&mut state, OperatorPresence::Present, start),
            OperatorPresence::Away
        );
        assert_eq!(
            resolve_presence(
                &mut state,
                OperatorPresence::Present,
                start + Duration::from_secs(3_600)
            ),
            OperatorPresence::Away
        );
        assert_eq!(state.mode, OperatorPresenceMode::Idle);
    }

    #[test]
    fn screen_lock_releases_a_manual_active_pin() {
        let mut state = OperatorPresenceState {
            mode: OperatorPresenceMode::Active,
        };
        assert_eq!(
            resolve_presence_with_lock(&mut state, OperatorPresence::Away, true, Instant::now()),
            OperatorPresence::Away
        );
        assert_eq!(state.mode, OperatorPresenceMode::Auto);
    }

    #[test]
    fn matching_inference_does_not_clear_a_manual_presence() {
        let start = Instant::now();
        let mut state = OperatorPresenceState {
            mode: OperatorPresenceMode::Active,
        };
        assert_eq!(
            resolve_presence(&mut state, OperatorPresence::Present, start),
            OperatorPresence::Present
        );
        assert_eq!(state.mode, OperatorPresenceMode::Active);
    }

    #[tokio::test]
    async fn admission_failures_are_paced_collapsed_and_recover_promptly() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = Arc::new(AtomicUsize::new(0));
        let reports = Arc::new(Mutex::new(Vec::new()));
        let configs = Arc::new(Mutex::new(["offline", "offline", "attached"].into_iter()));
        let started = Instant::now();
        supervise_admission(
            {
                let configs = configs.clone();
                move || configs.lock().unwrap().next()
            },
            {
                let attempts = attempts.clone();
                move |config| {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if config == "offline" {
                            Err("executor disconnected".to_string())
                        } else {
                            assert_eq!(attempt, 2);
                            Ok(())
                        }
                    }
                }
            },
            {
                let reports = reports.clone();
                move |report| reports.lock().unwrap().push(report)
            },
            || tokio::time::sleep(Duration::from_millis(10)),
        )
        .await;

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(started.elapsed() >= Duration::from_millis(20));
        assert_eq!(
            reports.lock().unwrap().as_slice(),
            ["executor disconnected"]
        );
    }

    #[tokio::test]
    async fn executor_attachment_retries_without_waiting_for_timer_fallback() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = Arc::new(AtomicUsize::new(0));
        let attached = Arc::new(tokio::sync::Notify::new());
        let (attempted_tx, mut attempted_rx) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(supervise_admission(
            || Some(()),
            {
                let attempts = attempts.clone();
                move |()| {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    let attempted_tx = attempted_tx.clone();
                    async move {
                        attempted_tx.send(()).unwrap();
                        if attempt == 0 {
                            Err("executor disconnected".to_string())
                        } else {
                            Ok(())
                        }
                    }
                }
            },
            |_| {},
            {
                let attached = attached.clone();
                move || {
                    let attached = attached.clone();
                    async move {
                        tokio::select! {
                            () = attached.notified() => {},
                            () = tokio::time::sleep(Duration::from_secs(30)) => {
                                panic!("timer fallback expired before executor attachment")
                            },
                        }
                    }
                }
            },
        ));
        attempted_rx.recv().await.unwrap();
        attached.notify_waiters();
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .expect("attachment should wake admission immediately")
            .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn disabling_channel_stops_retrying_stale_admission_config() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = Arc::new(AtomicUsize::new(0));
        let configs = Arc::new(Mutex::new([Some("offline"), None].into_iter()));
        supervise_admission(
            {
                let configs = configs.clone();
                move || configs.lock().unwrap().next().flatten()
            },
            {
                let attempts = attempts.clone();
                move |_config| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Err("executor disconnected".to_string()) }
                }
            },
            |_| {},
            || tokio::time::sleep(Duration::from_millis(1)),
        )
        .await;

        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn admission_failure_report_summarizes_suppressed_attempts_periodically() {
        let start = Instant::now();
        let mut reporter = AdmissionFailureReporter::default();
        assert_eq!(
            reporter.record(start, "executor disconnected").as_deref(),
            Some("executor disconnected")
        );
        assert_eq!(
            reporter.record(start + Duration::from_secs(2), "executor disconnected"),
            None
        );
        assert_eq!(
            reporter.record(start + Duration::from_secs(4), "executor disconnected"),
            None
        );
        assert_eq!(
            reporter
                .record(start + ADMISSION_REPORT_INTERVAL, "executor disconnected")
                .as_deref(),
            Some("executor disconnected (2 identical attempts suppressed)")
        );
    }

    #[test]
    fn not_runnable_transition_clears_waiting_or_stopped_admission_state() {
        for state in ["waitingForExecutor", "stopped"] {
            set_admission_state("imessage", Some((state, "not runnable".into())));
            clear_admission_state("imessage");
            assert!(admission_state_slot().lock().unwrap().is_empty());
        }
    }

    #[test]
    fn backlog_seal_failure_overrides_ready_bridge_health() {
        let (state, detail) = classify_runtime_health(
            Some(ChannelHealth::Ready),
            Some("database unavailable".into()),
        );

        assert_eq!(state, "dead");
        assert_eq!(
            detail.as_deref(),
            Some("blocked: backlog seal failed: database unavailable")
        );
    }

    #[test]
    fn feed_state_never_promotes_bridge_health_to_receipt_health() {
        let now = 1_000;
        assert_eq!(feed_state(None, now), "stopped");
        assert_eq!(
            feed_state(
                Some(ChannelLiveness {
                    active: true,
                    started_at: Some(now - 10),
                    last_receipt_at: None,
                    last_receipt_rowid: None,
                }),
                now,
            ),
            "connected"
        );
        assert_eq!(
            feed_state(
                Some(ChannelLiveness {
                    active: true,
                    started_at: Some(now - 600),
                    last_receipt_at: Some(now - 10),
                    last_receipt_rowid: Some(42),
                }),
                now,
            ),
            "receiving"
        );
        assert_eq!(
            feed_state(
                Some(ChannelLiveness {
                    active: true,
                    started_at: Some(now - 600),
                    last_receipt_at: Some(now - 301),
                    last_receipt_rowid: Some(42),
                }),
                now,
            ),
            "silent"
        );
    }

    #[test]
    fn renders_numbered_question_options() {
        let ask = OutboundAsk::Question {
            prompt_id: "prompt".into(),
            question_index: 0,
            text: "Which path?".into(),
            options: vec![
                AskOption {
                    label: "Legacy".into(),
                    description: Some("Preserve current behavior".into()),
                },
                AskOption {
                    label: "New".into(),
                    description: None,
                },
            ],
        };

        assert_eq!(
            render_text_floor(&ask),
            "Which path?\n\n1. Legacy — Preserve current behavior\n2. New\n\nReply to this message with a number or your answer."
        );
    }

    #[test]
    fn renders_permissions_and_free_text_questions() {
        let permission = OutboundAsk::Permission {
            request_id: "request".into(),
            summary: "Run an external command?".into(),
        };
        assert_eq!(
            render_text_floor(&permission),
            "Run an external command?\n\n1. Approve\n2. Deny\n\nReply to this message with a number or your answer."
        );

        let free_text = OutboundAsk::Question {
            prompt_id: "prompt".into(),
            question_index: 0,
            text: "What should change?".into(),
            options: vec![],
        };
        assert_eq!(
            render_text_floor(&free_text),
            "What should change?\n\nReply to this message with your answer."
        );
    }

    #[test]
    fn rejects_the_telegram_bots_own_id_as_a_recipient_or_sender() {
        let token = "123456789:secret";
        assert_eq!(
            telegram_identity_error("123456789", &["42".into()], token).as_deref(),
            Some(TELEGRAM_OWN_ID_GUIDANCE)
        );
        assert_eq!(
            telegram_identity_error("42", &["123456789".into()], token).as_deref(),
            Some(TELEGRAM_OWN_ID_GUIDANCE)
        );
        assert_eq!(telegram_identity_error("42", &["7".into()], token), None);
    }
}
