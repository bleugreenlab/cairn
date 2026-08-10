//! Provider-neutral primitives for delivering human attention gates externally.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

static IMESSAGE_RUNTIME: OnceLock<Mutex<Option<Arc<imessage::IMessageProvider>>>> = OnceLock::new();
static IMESSAGE_ROUTER_BLOCKER: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static IMESSAGE_ADMISSION_STATE: OnceLock<Mutex<Option<(&'static str, String)>>> = OnceLock::new();
static IMESSAGE_ADMISSION_WAKE: OnceLock<tokio::sync::Notify> = OnceLock::new();
static OPERATOR_PRESENCE_STATE: OnceLock<Mutex<OperatorPresenceState>> = OnceLock::new();
static DESKTOP_ACTIVITY_STATE: OnceLock<Mutex<DesktopActivityState>> = OnceLock::new();
static OPERATOR_PRESENCE_CHANGED: OnceLock<tokio::sync::Notify> = OnceLock::new();
static ROUTE_SUBMISSIONS: OnceLock<
    Mutex<Option<mpsc::UnboundedSender<crate::routes::ChannelSubmission>>>,
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

fn route_submission_slot(
) -> &'static Mutex<Option<mpsc::UnboundedSender<crate::routes::ChannelSubmission>>> {
    ROUTE_SUBMISSIONS.get_or_init(|| Mutex::new(None))
}

/// Hands a channel route to the optional external-channel runtime. Non-channel
/// sinks are independent of channel admission, so absence here is deliberately soft.
pub fn submit_route(
    submission: crate::routes::ChannelSubmission,
) -> Result<(), Box<crate::routes::ChannelSubmission>> {
    let sender = route_submission_slot()
        .lock()
        .expect("route submission slot poisoned")
        .clone();
    let Some(sender) = sender else {
        return Err(Box::new(submission));
    };
    sender.send(submission).map_err(|error| {
        *route_submission_slot()
            .lock()
            .expect("route submission slot poisoned") = None;
        Box::new(error.0)
    })
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
pub fn report_desktop_presence(idle_seconds: u64, locked: bool) {
    desktop_activity_state()
        .lock()
        .expect("desktop activity state lock poisoned")
        .last_sample = Some(DesktopPresenceSample {
        sampled_at: Instant::now(),
        idle_seconds,
        locked,
    });
    OPERATOR_PRESENCE_CHANGED
        .get_or_init(tokio::sync::Notify::new)
        .notify_one();
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
    let presence = operator_presence(
        runtime
            .as_deref()
            .map(|provider| provider as &dyn ChannelProvider),
    )
    .await;
    let mode = presence_state()
        .lock()
        .expect("operator presence state lock poisoned")
        .mode;
    OperatorPresenceStatus { mode, presence }
}

fn admission_state_slot() -> &'static Mutex<Option<(&'static str, String)>> {
    IMESSAGE_ADMISSION_STATE.get_or_init(|| Mutex::new(None))
}

fn set_admission_state(state: Option<(&'static str, String)>) {
    *admission_state_slot()
        .lock()
        .expect("channel admission state lock poisoned") = state;
}

fn clear_admission_state() {
    set_admission_state(None);
}

pub fn retry_admission() {
    IMESSAGE_ADMISSION_WAKE
        .get_or_init(tokio::sync::Notify::new)
        .notify_waiters();
}

fn router_blocker_slot() -> &'static Mutex<Option<String>> {
    IMESSAGE_ROUTER_BLOCKER.get_or_init(|| Mutex::new(None))
}

pub(super) fn set_router_blocker(error: Option<String>) {
    *router_blocker_slot()
        .lock()
        .expect("channel router blocker lock poisoned") = error;
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

fn runtime_slot() -> &'static Mutex<Option<Arc<imessage::IMessageProvider>>> {
    IMESSAGE_RUNTIME.get_or_init(|| Mutex::new(None))
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

pub async fn runtime_status(orch: &crate::orchestrator::Orchestrator) -> ChannelRuntimeStatus {
    let runtime = runtime_slot()
        .lock()
        .expect("channel runtime lock poisoned")
        .clone();
    let health = runtime.as_ref().map(|provider| provider.health());
    let liveness = runtime.as_ref().map(|provider| provider.watch_liveness());
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
        .clone();
    let admission = admission_state_slot()
        .lock()
        .expect("channel admission state lock poisoned")
        .clone();
    let (state, detail) = if runtime.is_none() {
        admission
            .map(|(state, detail)| (state, Some(detail)))
            .unwrap_or_else(|| classify_runtime_health(health, router_blocker))
    } else {
        classify_runtime_health(health, router_blocker)
    };
    let unsolicited_inbound = ledger::list_inbound(&orch.db.local, "imessage", 50)
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
    ChannelRuntimeStatus {
        provider: "imessage",
        state,
        detail,
        feed_state: feed_state(liveness, chrono::Utc::now().timestamp()),
        watch_started_at: liveness.and_then(|value| value.started_at),
        last_receipt_at: liveness.and_then(|value| value.last_receipt_at),
        last_receipt_rowid: liveness.and_then(|value| value.last_receipt_rowid),
        unsolicited_inbound,
        presence_mode,
        operator_presence,
    }
}

const RECEIPT_RECENCY_SECONDS: i64 = 5 * 60;

fn feed_state(liveness: Option<imessage::WatchLiveness>, now: i64) -> &'static str {
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

pub mod imessage;
pub mod ledger;
pub mod router;

/// Starts the configured external channel service on runner hosts.
pub fn spawn_configured(orch: crate::orchestrator::Orchestrator) {
    crate::routes::spawn_attention_routes(orch.clone());
    tokio::spawn(async move {
        loop {
            let config = crate::config::settings::load_settings(&orch.config_dir)
                .channels
                .imessage;
            if config.enabled && !config.to.trim().is_empty() {
                if let Some(name) = config.executor.as_deref() {
                    if !orch.fleet.named_executor_is_connected(name) {
                        set_admission_state(Some((
                            "waitingForExecutor",
                            format!("Waiting for executor `{name}` to attach"),
                        )));
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
                        set_admission_state(Some(("stopped", report.clone())));
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
                clear_admission_state();
            } else {
                clear_admission_state();
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
    set_admission_state(None);
    set_router_blocker(None);
    *runtime_slot()
        .lock()
        .expect("channel runtime lock poisoned") = Some(provider.clone());
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
        config.clone(),
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
            let mut runtime = runtime_slot()
                .lock()
                .expect("channel runtime lock poisoned");
            if runtime
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &provider))
            {
                *runtime = None;
            }
            set_router_blocker(None);
            return Ok(());
        }
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
        bound_guid: String,
        sender: String,
        option_id: String,
        option_text: String,
        selected: bool,
    },
    Selections {
        bound_guid: String,
        sender: String,
        changes: Vec<PollSelectionChange>,
    },
    Reply {
        bound_guid: String,
        sender: String,
        text: String,
    },
    Bare {
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
        assert!(runtime_slot().lock().unwrap().is_none());

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
            set_admission_state(Some((state, "not runnable".into())));
            clear_admission_state();
            assert!(admission_state_slot().lock().unwrap().is_none());
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
                Some(imessage::WatchLiveness {
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
                Some(imessage::WatchLiveness {
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
                Some(imessage::WatchLiveness {
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
}
