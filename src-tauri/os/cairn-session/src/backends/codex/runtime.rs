use crate::agent_process::process::AgentProcessState;
use cairn_models::types::usage::ProviderUsageSnapshot;

/// Host admission of a provider-initiated turn, after the host reconciles its job.
pub enum GoalTurnAdmission {
    Admitted,
    Deferred { predecessor_turn_id: String },
    SafetyPaused,
}

pub trait CodexHost: Send + Sync {
    fn transcripts(&self) -> &dyn crate::transcripts::TranscriptHost;
    fn error(&self, message: &str);
    fn server_request(
        &self,
        method: Option<&str>,
        message: &Value,
        id: &Value,
    ) -> Result<(), String>;
    /// Answer the server's request for fresh ChatGPT tokens. Spends a refresh
    /// token, so it stays with the host.
    fn refresh_tokens(&self, id: &Value) -> Result<(), String>;
    fn capture_output(&self, output: Value);
    fn observe_final_output(
        &self,
        cairn_turn_id: &str,
        provider_turn_id: &str,
        text: &str,
    ) -> Option<TranscriptEvent>;
    fn account_usage(&self, snapshot: ProviderUsageSnapshot);
    fn context_snapshot(&self, snapshot: ContextTokenState);
    fn admit_goal_turn(&self, provider_turn_id: &str) -> Result<Option<GoalTurnAdmission>, String>;
    fn defer_goal_turn(&self, provider_turn_id: &str) -> Result<(), String>;
    fn interrupt(&self) -> Result<(), String>;
    fn artifact_handoff(&self);
    fn watchdog_arm_failed(&self) -> Result<(), String>;
    fn recover_watchdog(
        &self,
        identity: &crate::runs::watchdog_ledger::WatchdogIdentity,
        now: i64,
    ) -> Result<(), String>;
    fn capacity_failure(&self) -> bool;
    fn complete(&self, completion: SessionTurnCompletion, context: CodexTurnContext);

    fn reader_closed(&self);
}

#[derive(Clone)]
pub struct CodexRuntime {
    pub process_state: Arc<AgentProcessState>,
    pub boot_id: String,
    pub host: Arc<dyn CodexHost>,
}
use super::events::{
    build_codex_compaction_event, emit_codex_command_complete, emit_codex_command_output_delta,
    emit_streaming_delta, finalize_agent_message, finalize_streaming, handle_agent_message_delta,
    handle_reasoning_delta, store_event, terminal_tool_called_for_run,
};
use super::json_string;
use super::protocol::StreamingState;
use crate::agent_process::completion::{CodexTurnContext, TurnFailureDisposition};
use crate::agent_process::completion::{
    SessionLifetime, SessionTurnCompletion, SessionTurnOutcome,
};
use crate::agent_process::stream::TokenCounts;
use crate::agent_process::stream::{ToolUseInfo, TranscriptEvent};
use crate::transcripts::stream_store::append_chunks;
use cairn_db::storage::LocalDb;
use cairn_domain::ContextTokenState;
use cairn_models::usage::codex_rate_limits::codex_rate_limit_snapshot_from_value;
use cairn_models::wire::claude_stream::Usage;
use cairn_models::wire::codex_app_server::{
    context_usage, extract_command_execution, extract_mcp_result,
    extract_raw_response_message_text, is_selected_model_capacity_error, normalize_capacity_error,
    notification_turn_id, summarize_command_result, summarize_file_change_result,
    CodexTurnUsageAccumulator, Notification, ProviderTurnStatus,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Normalize provider status without deciding the Cairn run or workflow state.
pub(crate) fn codex_turn_completion(
    status: ProviderTurnStatus<'_>,
    usage: Option<Usage>,
    ephemeral: bool,
) -> SessionTurnCompletion {
    SessionTurnCompletion {
        outcome: match status {
            ProviderTurnStatus::Completed => SessionTurnOutcome::Completed,
            ProviderTurnStatus::Interrupted => SessionTurnOutcome::Interrupted,
            ProviderTurnStatus::Other(status) => SessionTurnOutcome::Failed {
                status: status.to_string(),
            },
        },
        usage: TokenCounts::from_optional_usage(usage.as_ref()),
        session_lifetime: if ephemeral {
            SessionLifetime::OneShot
        } else {
            SessionLifetime::Persistent
        },
    }
}

pub(crate) const CODEX_TURN_NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub(crate) const CODEX_POST_TOOL_SILENCE_TIMEOUT: Duration = Duration::from_secs(45);
pub(crate) const CODEX_TURN_WATCHDOG_POLL: Duration = Duration::from_secs(1);
pub(crate) const CODEX_TURN_WATCHDOG_HEARTBEAT: Duration = Duration::from_secs(15);

pub(crate) fn is_deferred_goal_turn(
    deferred: &HashSet<String>,
    provider_turn_id: Option<&str>,
) -> bool {
    provider_turn_id.is_some_and(|turn_id| deferred.contains(turn_id))
}

pub(crate) fn claim_deferred_goal_turn(
    deferred: &mut HashSet<String>,
    provider_turn_id: &str,
) -> bool {
    deferred.insert(provider_turn_id.to_string())
}

pub(crate) fn consume_deferred_goal_terminal(
    deferred: &mut HashSet<String>,
    provider_turn_id: Option<&str>,
) -> bool {
    provider_turn_id.is_some_and(|turn_id| deferred.remove(turn_id))
}

impl From<CodexTurnWatchdogPhase> for crate::runs::watchdog_ledger::WatchdogPhase {
    fn from(phase: CodexTurnWatchdogPhase) -> Self {
        match phase {
            CodexTurnWatchdogPhase::ProviderProgress => Self::ProviderProgress,
            CodexTurnWatchdogPhase::ToolOutstanding => Self::ToolOutstanding,
            CodexTurnWatchdogPhase::PostToolContinuation => Self::PostToolContinuation,
        }
    }
}

pub(crate) fn watchdog_db<T>(
    db: Arc<LocalDb>,
    operation: impl std::future::Future<Output = cairn_db::storage::DbResult<T>> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    let _ = db;
    cairn_db::storage::run_db_blocking(
        move || async move { operation.await.map_err(|e| e.to_string()) },
    )
}

pub(crate) fn watchdog_lifecycle(
    db: Arc<LocalDb>,
    identity: crate::runs::watchdog_ledger::WatchdogIdentity,
    kind: crate::runs::watchdog_ledger::WatchdogLifecycleKind,
    phase: Option<CodexTurnWatchdogPhase>,
    reason: Option<String>,
) -> Result<(), String> {
    let event = crate::runs::watchdog_ledger::NewWatchdogLifecycle {
        id: Uuid::new_v4().to_string(),
        identity,
        kind,
        phase: phase.map(Into::into),
        reason,
        details: None,
        successor_run_id: None,
        successor_turn_id: None,
        created_at: chrono::Utc::now().timestamp(),
    };
    watchdog_db(db.clone(), async move {
        crate::runs::watchdog_ledger::append_watchdog_lifecycle(&db, event).await
    })
}

pub(crate) struct OwnedCodexWatchdog {
    state: Arc<Mutex<CodexTurnProgressWatchdog>>,
    identity: Arc<Mutex<Option<crate::runs::watchdog_ledger::WatchdogIdentity>>>,
    cancel: mpsc::Sender<()>,
    join: Option<thread::JoinHandle<()>>,
    db: Arc<LocalDb>,
    run_id: String,
    session_id: Option<String>,
    runner_boot_id: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum WatchdogArmError {
    MissingSession,
    OwnershipConflict,
    Database(String),
}

impl std::fmt::Display for WatchdogArmError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingSession => write!(formatter, "the run has no session id"),
            Self::OwnershipConflict => {
                write!(formatter, "an active lease already owns this provider turn")
            }
            Self::Database(error) => {
                write!(formatter, "the watchdog database write failed: {error}")
            }
        }
    }
}

pub(crate) fn checked_watchdog_arm(outcome: Result<bool, String>) -> Result<(), WatchdogArmError> {
    match outcome {
        Ok(true) => Ok(()),
        Ok(false) => Err(WatchdogArmError::OwnershipConflict),
        Err(error) => Err(WatchdogArmError::Database(error)),
    }
}

pub(crate) fn required_watchdog_session(
    session_id: Option<String>,
) -> Result<String, WatchdogArmError> {
    session_id.ok_or(WatchdogArmError::MissingSession)
}

impl OwnedCodexWatchdog {
    pub fn spawn(
        host: &CodexRuntime,
        db: Arc<LocalDb>,
        run_id: &str,
        session_id: Option<String>,
    ) -> Self {
        let state = Arc::new(Mutex::new(CodexTurnProgressWatchdog::new(
            CODEX_TURN_NO_PROGRESS_TIMEOUT,
            CODEX_POST_TOOL_SILENCE_TIMEOUT,
        )));
        let identity = Arc::new(Mutex::new(
            None::<crate::runs::watchdog_ledger::WatchdogIdentity>,
        ));
        let (cancel, cancelled) = mpsc::channel();
        let worker_state = state.clone();
        let worker_identity = identity.clone();
        let worker_db = db.clone();
        let worker_host = host.clone();
        let worker_run_id = run_id.to_string();
        let worker_name = format!("codex-turn-watchdog-{}", &run_id[..run_id.len().min(8)]);
        let join = thread::Builder::new().name(worker_name).spawn(move || {
            let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
                let mut last_phase = None;
                let mut last_persisted_progress = 0;
                let mut last_heartbeat = Instant::now();
                loop {
                    match cancelled.recv_timeout(CODEX_TURN_WATCHDOG_POLL) {
                        Ok(()) | Err(RecvTimeoutError::Disconnected) => return true,
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                    let snapshot = match worker_state.lock() {
                        Ok(mut guard) => {
                            let now = Instant::now();
                            let expiry = guard.expired(now);
                            let phase = guard.active_turn_id.as_ref().map(|_| guard.phase);
                            let deadline = guard.deadline_at(now, chrono::Utc::now().timestamp());
                            (phase, deadline, expiry, guard.progress_sequence)
                        }
                        Err(poisoned) => {
                            drop(poisoned.into_inner());
                            log::error!("Codex watchdog state poisoned for run {}; worker exiting for durable reconciliation", worker_run_id);
                            return false;
                        }
                    };
                    let current_identity = worker_identity.lock().ok().and_then(|guard| guard.clone());
                    let Some(current_identity) = current_identity else { continue };
                    let wall_now = chrono::Utc::now().timestamp();
                    if let (Some(phase), Some(deadline)) = (snapshot.0, snapshot.1) {
                        if last_phase != Some(phase) || last_persisted_progress != snapshot.3 {
                            let id = current_identity.clone();
                            let phase_db = worker_db.clone();
                            match watchdog_db(phase_db.clone(), async move {
                                crate::runs::watchdog_ledger::refresh_watchdog_progress(
                                    &phase_db, &id, phase.into(), deadline, wall_now, wall_now,
                                ).await
                            }) {
                                Ok(true) => {
                                    let phase_changed = last_phase.is_some() && last_phase != Some(phase);
                                    last_persisted_progress = snapshot.3;
                                    if phase_changed {
                                    if let Err(error) = watchdog_lifecycle(worker_db.clone(), current_identity.clone(), crate::runs::watchdog_ledger::WatchdogLifecycleKind::PhaseTransition, Some(phase), None) {
                                        log::error!("Failed to persist Codex watchdog phase lifecycle for {:?}: {}", current_identity, error);
                                    }
                                    }
                                }
                                Ok(false) if last_phase.is_some() => log::error!("Codex watchdog phase CAS lost for {:?}", current_identity),
                                Ok(false) => {}
                                Err(error) => log::error!("Failed to persist Codex watchdog phase for {:?}: {}", current_identity, error),
                            }
                            last_phase = Some(phase);
                        }
                    }
                    if last_heartbeat.elapsed() >= CODEX_TURN_WATCHDOG_HEARTBEAT {
                        let id = current_identity.clone();
                        let heartbeat_db = worker_db.clone();
                        match watchdog_db(heartbeat_db.clone(), async move { crate::runs::watchdog_ledger::heartbeat_watchdog(&heartbeat_db, &id, wall_now).await }) {
                            Ok(true) => {}
                            Ok(false) => log::error!("Codex watchdog heartbeat lost active lease for {:?}", current_identity),
                            Err(error) => log::error!("Failed to persist Codex watchdog heartbeat for {:?}: {}", current_identity, error),
                        }
                        last_heartbeat = Instant::now();
                    }
                    if let Some(expiry) = snapshot.2 {
                        let id = current_identity.clone();
                        let expiry_db = worker_db.clone();
                        let expired = watchdog_db(expiry_db.clone(), async move { crate::runs::watchdog_ledger::expire_watchdog(&expiry_db, &id, wall_now).await });
                        match expired {
                            Ok(true) => {
                                if let Err(error) = watchdog_lifecycle(worker_db.clone(), current_identity.clone(), crate::runs::watchdog_ledger::WatchdogLifecycleKind::Expired, Some(expiry.phase), Some(format!("no provider progress for {:?}", expiry.elapsed))) {
                                    log::error!("Failed to persist Codex watchdog expiry lifecycle for {:?}: {}", current_identity, error);
                                }
                                if let Err(error) = worker_host.host.recover_watchdog(&current_identity, wall_now) {
                                    log::error!("Codex watchdog recovery failed for {:?}: {}", current_identity, error);
                                }
                            }
                            Ok(false) => log::error!("Codex watchdog expiry lost active lease for {:?}", current_identity),
                            Err(error) => log::error!("Failed to persist Codex watchdog expiry for {:?}: {}", current_identity, error),
                        }
                        return true;
                    }
                }
            }));
            if !matches!(outcome, Ok(true)) {
                let kind = if outcome.is_err() { crate::runs::watchdog_ledger::WatchdogLifecycleKind::WorkerPanicked } else { crate::runs::watchdog_ledger::WatchdogLifecycleKind::WorkerExitedUnexpectedly };
                if let Some(id) = worker_identity.lock().ok().and_then(|guard| guard.clone()) {
                    if let Err(error) = watchdog_lifecycle(worker_db.clone(), id.clone(), kind, None, Some("owned watchdog worker exited before disarm".into())) {
                        log::error!("Failed to diagnose Codex watchdog worker exit for {:?}: {}", id, error);
                    }
                }
            }
        }).expect("spawn owned Codex turn watchdog");
        Self {
            state,
            identity,
            cancel,
            join: Some(join),
            db,
            run_id: run_id.to_string(),
            session_id,
            runner_boot_id: host.boot_id.clone(),
        }
    }

    pub fn arm(&self, turn_id: &str) -> Result<(), WatchdogArmError> {
        let session_id = required_watchdog_session(self.session_id.clone())?;
        self.disarm("provider_turn_replaced");
        let identity = crate::runs::watchdog_ledger::WatchdogIdentity {
            run_id: self.run_id.clone(),
            session_id,
            provider_turn_id: turn_id.to_string(),
            generation: Uuid::new_v4().to_string(),
        };
        let now = chrono::Utc::now().timestamp();
        let lease = crate::runs::watchdog_ledger::NewWatchdogLease {
            identity: identity.clone(),
            runner_boot_id: self.runner_boot_id.clone(),
            phase: crate::runs::watchdog_ledger::WatchdogPhase::ProviderProgress,
            phase_deadline_at: now + CODEX_TURN_NO_PROGRESS_TIMEOUT.as_secs() as i64,
            now,
        };
        let arm_db = self.db.clone();
        checked_watchdog_arm(watchdog_db(arm_db.clone(), async move {
            crate::runs::watchdog_ledger::arm_watchdog(&arm_db, lease, Uuid::new_v4().to_string())
                .await
        }))?;
        if let Ok(mut guard) = self.identity.lock() {
            *guard = Some(identity);
        }
        match self.state.lock() {
            Ok(mut guard) => guard.start_turn(turn_id, Instant::now()),
            Err(_) => log::error!(
                "Codex watchdog state poisoned after durably arming run {}",
                self.run_id
            ),
        }
        Ok(())
    }

    pub fn disarm_turn(&self, provider_turn_id: Option<&str>, reason: &str) {
        let identity = self.identity.lock().ok().and_then(|guard| guard.clone());
        if provider_turn_id.is_some_and(|turn_id| {
            identity.as_ref().map(|id| id.provider_turn_id.as_str()) != Some(turn_id)
        }) {
            return;
        }
        if let Some(identity) = identity {
            let now = chrono::Utc::now().timestamp();
            let id = identity.clone();
            let db = self.db.clone();
            let durable_reason = reason.to_string();
            match watchdog_db(db.clone(), async move {
                crate::runs::watchdog_ledger::disarm_watchdog(&db, &id, &durable_reason, now).await
            }) {
                Ok(true) => {
                    if let Err(error) = watchdog_lifecycle(
                        self.db.clone(),
                        identity,
                        crate::runs::watchdog_ledger::WatchdogLifecycleKind::Disarmed,
                        None,
                        Some(reason.to_string()),
                    ) {
                        log::error!(
                            "Failed to persist Codex watchdog disarm lifecycle for run {}: {}",
                            self.run_id,
                            error
                        );
                    }
                }
                Ok(false) => {}
                Err(error) => log::error!(
                    "Failed to durably disarm Codex watchdog for run {}: {}",
                    self.run_id,
                    error
                ),
            }
        }
        if let Ok(mut guard) = self.state.lock() {
            guard.clear_turn();
        }
        if let Ok(mut guard) = self.identity.lock() {
            *guard = None;
        }
    }

    pub fn disarm(&self, reason: &str) {
        self.disarm_turn(None, reason);
    }
}

impl Drop for OwnedCodexWatchdog {
    fn drop(&mut self) {
        self.disarm("reader_shutdown");
        let _ = self.cancel.send(());
        if let Some(join) = self.join.take() {
            if join.join().is_err() {
                log::error!(
                    "Codex watchdog join observed a panic for run {}",
                    self.run_id
                );
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ProviderTurnFailureState {
    capacity_message: Option<String>,
    terminalized: bool,
}

/// The provider turn a notification belongs to: the id it names, else the turn
/// this reader last saw start.
pub fn provider_turn_id(
    msg: &Value,
    current_turn_id: &Arc<Mutex<Option<String>>>,
) -> Option<String> {
    turn_id_or_cached(notification_turn_id(msg), current_turn_id)
}

fn turn_id_or_cached(
    notification_turn_id: Option<&str>,
    current_turn_id: &Arc<Mutex<Option<String>>>,
) -> Option<String> {
    notification_turn_id
        .map(str::to_string)
        .or_else(|| current_turn_id.lock().ok().and_then(|guard| guard.clone()))
}

pub(crate) fn clear_provider_turn_if_current(
    completed_turn_id: Option<&str>,
    current_turn_id: &Arc<Mutex<Option<String>>>,
    progress_watchdog: &Arc<Mutex<CodexTurnProgressWatchdog>>,
) {
    if !clear_cached_provider_turn_if_current(completed_turn_id, current_turn_id) {
        return;
    }

    let Some(completed_turn_id) = completed_turn_id else {
        return;
    };
    if let Ok(mut watchdog) = progress_watchdog.lock() {
        if watchdog.active_turn_id.as_deref() == Some(completed_turn_id) {
            watchdog.clear_turn();
        }
    }
}

pub fn clear_cached_provider_turn_if_current(
    completed_turn_id: Option<&str>,
    current_turn_id: &Arc<Mutex<Option<String>>>,
) -> bool {
    let Some(completed_turn_id) = completed_turn_id else {
        return false;
    };
    let Ok(mut guard) = current_turn_id.lock() else {
        return false;
    };
    if guard.as_deref() != Some(completed_turn_id) {
        return false;
    }
    *guard = None;
    true
}

/// The context-gauge snapshot for a token-usage notification. Occupancy comes
/// from the wire crate's reading of `tokenUsage.last`, never `total`.
pub(crate) fn codex_context_tokens_from_notification(
    params: &Value,
    run_id: &str,
    session_id: Option<String>,
    backend: String,
    model: Option<String>,
    captured_at: i64,
) -> Option<ContextTokenState> {
    let context = context_usage(params)?;
    Some(ContextTokenState {
        run_id: run_id.to_string(),
        session_id,
        backend,
        model,
        used_tokens: context.used_tokens,
        context_window: context.context_window,
        // Codex only streams the effective modelContextWindow. Its separate
        // auto-compact threshold is not on the notification, so leave this for
        // a future Codex-protocol surface rather than guessing.
        auto_compact_limit: None,
        reasoning_tokens: Some(context.reasoning_tokens),
        last_output_tokens: Some(context.last_output_tokens),
        captured_at,
    })
}

pub(crate) fn codex_tool_result_event(
    session_id: Option<String>,
    tool_use_id: Option<String>,
    tool_result: String,
    is_error: bool,
    raw: Option<Value>,
) -> TranscriptEvent {
    TranscriptEvent {
        event_type: "tool_result".to_string(),
        session_id,
        parent_tool_use_id: None,
        content: None,
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id,
        tool_result: Some(tool_result),
        is_error,
        thinking_ms: None,
        queued_message_id: None,
        raw,
    }
}

pub(crate) fn codex_compaction_event_from_completed_item(msg: &Value) -> Option<TranscriptEvent> {
    let Notification::ItemCompleted { item } = Notification::parse(msg) else {
        return None;
    };
    if item.get("type").and_then(|v| v.as_str()) != Some("contextCompaction") {
        return None;
    }
    Some(build_codex_compaction_event(item.clone()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodexTurnWatchdogPhase {
    ProviderProgress,
    ToolOutstanding,
    PostToolContinuation,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CodexTurnWatchdogExpiry {
    turn_id: String,
    phase: CodexTurnWatchdogPhase,
    elapsed: Duration,
}

#[derive(Debug)]
pub(crate) struct CodexTurnProgressWatchdog {
    active_turn_id: Option<String>,
    last_forward_progress_at: Instant,
    general_timeout: Duration,
    post_tool_timeout: Duration,
    phase: CodexTurnWatchdogPhase,
    outstanding_tool_ids: HashSet<String>,
    fired: bool,
    progress_sequence: u64,
}

impl CodexTurnProgressWatchdog {
    pub fn new(general_timeout: Duration, post_tool_timeout: Duration) -> Self {
        Self {
            active_turn_id: None,
            last_forward_progress_at: Instant::now(),
            general_timeout,
            post_tool_timeout,
            phase: CodexTurnWatchdogPhase::ProviderProgress,
            outstanding_tool_ids: HashSet::new(),
            fired: false,
            progress_sequence: 0,
        }
    }

    pub fn start_turn(&mut self, turn_id: &str, now: Instant) {
        self.active_turn_id = Some(turn_id.to_string());
        self.last_forward_progress_at = now;
        self.phase = CodexTurnWatchdogPhase::ProviderProgress;
        self.outstanding_tool_ids.clear();
        self.fired = false;
        self.progress_sequence = self.progress_sequence.wrapping_add(1);
    }

    pub fn clear_turn(&mut self) {
        self.active_turn_id = None;
        self.phase = CodexTurnWatchdogPhase::ProviderProgress;
        self.outstanding_tool_ids.clear();
        self.fired = false;
    }

    pub fn record_forward_progress(&mut self, now: Instant) {
        if self.active_turn_id.is_some() {
            self.last_forward_progress_at = now;
            self.progress_sequence = self.progress_sequence.wrapping_add(1);
            if self.outstanding_tool_ids.is_empty() {
                self.phase = CodexTurnWatchdogPhase::ProviderProgress;
            }
        }
    }

    pub fn start_tool(&mut self, tool_id: String, now: Instant) {
        if self.active_turn_id.is_some() {
            self.outstanding_tool_ids.insert(tool_id);
            self.phase = CodexTurnWatchdogPhase::ToolOutstanding;
            self.last_forward_progress_at = now;
            self.progress_sequence = self.progress_sequence.wrapping_add(1);
        }
    }

    pub fn complete_tool(&mut self, tool_id: Option<&str>, now: Instant) -> bool {
        let matched = tool_id.is_some_and(|id| self.outstanding_tool_ids.remove(id));
        if self.outstanding_tool_ids.is_empty() {
            self.phase = CodexTurnWatchdogPhase::PostToolContinuation;
            self.last_forward_progress_at = now;
            self.progress_sequence = self.progress_sequence.wrapping_add(1);
        }
        matched
    }

    pub fn deadline_at(&self, now: Instant, wall_now: i64) -> Option<i64> {
        self.active_turn_id.as_ref()?;
        let timeout = match self.phase {
            CodexTurnWatchdogPhase::ProviderProgress => self.general_timeout,
            CodexTurnWatchdogPhase::PostToolContinuation => self.post_tool_timeout,
            CodexTurnWatchdogPhase::ToolOutstanding => self.general_timeout,
        };
        let elapsed = now.duration_since(self.last_forward_progress_at);
        Some(wall_now.saturating_add(timeout.saturating_sub(elapsed).as_secs() as i64))
    }

    pub fn expired(&mut self, now: Instant) -> Option<CodexTurnWatchdogExpiry> {
        if self.fired || self.phase == CodexTurnWatchdogPhase::ToolOutstanding {
            return None;
        }
        let turn_id = self.active_turn_id.as_ref()?;
        let elapsed = now.duration_since(self.last_forward_progress_at);
        let timeout = match self.phase {
            CodexTurnWatchdogPhase::ProviderProgress => self.general_timeout,
            CodexTurnWatchdogPhase::PostToolContinuation => self.post_tool_timeout,
            CodexTurnWatchdogPhase::ToolOutstanding => return None,
        };
        if elapsed < timeout {
            return None;
        }
        self.fired = true;
        Some(CodexTurnWatchdogExpiry {
            turn_id: turn_id.clone(),
            phase: self.phase,
            elapsed,
        })
    }
}

impl CodexRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn reader_thread_app_server(
        host: &CodexRuntime,
        emitter: &Arc<dyn cairn_platform::services::emitter::EventEmitter>,
        run_id: &str,
        session_id: Option<String>,
        notifications: crossbeam_channel::Receiver<Value>,
        current_turn_id: Arc<Mutex<Option<String>>>,
        backend_key: String,
        run_db: Arc<LocalDb>,
        // Some(_) for a pooled ephemeral call (CAIRN-2549): the reader consumes a
        // per-call channel fed by the pool dispatcher (not the shared
        // `client.notifications()`), finalizes its one-shot turn explicitly, must
        // never signal the shared app-server, and deregisters its routing entries
        // on exit. None for the legacy process-per-session path.
        ephemeral: bool,
    ) {
        log::debug!("codex_app_server: reader started");
        let mut streaming_state: Option<StreamingState> = None;
        let mut usage_accumulator = CodexTurnUsageAccumulator::default();
        let mut last_direct_assistant_text: Option<String> = None;
        let mut finalized_assistant_by_turn: HashMap<String, String> = HashMap::new();
        let mut pending_tool_ids: HashSet<String> = HashSet::new();
        let mut tool_output_chars: HashMap<String, i32> = HashMap::new();
        let mut terminal_tool_suspended = false;
        // `false` means the provider completed a response after returning a tool
        // result without Cairn recording another response item. This is reset at
        // the tool-result boundary, not at the provider-turn boundary: Codex can
        // run several model responses inside one turn.
        let mut recorded_item_after_tool_result = true;
        let mut provider_turn_failures: HashMap<String, ProviderTurnFailureState> = HashMap::new();
        let mut deferred_goal_turns: HashSet<String> = HashSet::new();
        let owned_watchdog =
            OwnedCodexWatchdog::spawn(host, run_db.clone(), run_id, session_id.clone());
        let progress_watchdog = owned_watchdog.state.clone();

        let init_event = TranscriptEvent {
            event_type: "system:init".to_string(),
            session_id: session_id.clone(),
            parent_tool_use_id: None,
            content: Some("Codex session started".to_string()),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            queued_message_id: None,
            raw: None,
        };
        store_event(host, run_id, session_id.as_deref(), &init_event);

        for msg in notifications.iter() {
            let notification = Notification::parse(&msg);
            if let Notification::AuthTokensRefreshRequest { id } = notification {
                if let Err(e) = host.host.refresh_tokens(id) {
                    log::warn!("Failed to answer Codex token refresh request: {}", e);
                }
                continue;
            }
            if let Notification::ServerRequest { id, method } = notification {
                let response = host.host.server_request(Some(method), &msg, id);
                if let Err(e) = response {
                    log::warn!("Failed to answer Codex request {:?}: {}", method, e);
                }
                continue;
            }

            if terminal_tool_called_for_run(host, run_id)
                && matches!(
                    notification,
                    Notification::AgentMessageDelta { .. }
                        | Notification::ReasoningDelta { .. }
                        | Notification::RawResponseItemCompleted { .. }
                )
            {
                continue;
            }

            macro_rules! interrupt_terminal_tool_at_boundary {
                () => {
                    if !terminal_tool_suspended
                        && pending_tool_ids.is_empty()
                        && terminal_tool_called_for_run(host, run_id)
                    {
                        log::info!(
                            "Terminal artifact/tool reached Codex tool boundary for run {}; pausing, interrupting, and warming",
                            &run_id[..run_id.len().min(8)]
                        );
                        if let Err(error) =
                            host.host.interrupt()
                        {
                            let evidence = format!(
                                "Codex terminal handoff refused because its goal could not be paused and its turn interrupted: {error}"
                            );
                            log::error!("{evidence} (run {run_id})");
                            host.host.error(&evidence);
                            break;
                        }
                        terminal_tool_suspended = true;
                        host.host.artifact_handoff();
                        crate::backends::codex::events::emit_codex_run_turn_completed(emitter, run_id);
                    }
                };
            }

            match notification {
                // Answered above, before the terminal-tool filter.
                Notification::ServerRequest { .. }
                | Notification::AuthTokensRefreshRequest { .. } => {}
                Notification::TurnStarted { turn_id } => {
                    if let Some(turn_id) = turn_id {
                        if !ephemeral {
                            if is_deferred_goal_turn(&deferred_goal_turns, Some(turn_id)) {
                                continue;
                            }
                            match host.host.admit_goal_turn(turn_id) {
                                Ok(Some(GoalTurnAdmission::Deferred {
                                    predecessor_turn_id,
                                })) => {
                                    log::info!(
                                        "Deferring Codex native goal turn {turn_id} behind non-terminal Cairn turn {predecessor_turn_id}"
                                    );
                                    claim_deferred_goal_turn(&mut deferred_goal_turns, turn_id);
                                    continue;
                                }
                                Ok(Some(GoalTurnAdmission::SafetyPaused)) => {
                                    log::warn!(
                                        "Rejecting raced Codex goal turn {turn_id}: repeated-output safety pause is still armed"
                                    );
                                    claim_deferred_goal_turn(&mut deferred_goal_turns, turn_id);
                                    if let Err(error) = host.host.defer_goal_turn(turn_id) {
                                        log::error!(
                                            "Failed to interrupt safety-paused Codex goal turn {turn_id}: {error}"
                                        );
                                    }
                                    continue;
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    let evidence = format!(
                                        "Codex native goal turn {turn_id} could not enter Cairn's turn lifecycle: {error}"
                                    );
                                    log::error!("{evidence}");
                                    host.host.error(&evidence);
                                    let _ = host.host.watchdog_arm_failed();
                                    break;
                                }
                            }
                        }
                        if let Err(error) = owned_watchdog.arm(turn_id) {
                            let evidence = format!(
                                "Codex provider turn {turn_id} was interrupted because durable watchdog ownership could not be acquired: {error}"
                            );
                            log::error!("{evidence} (run {run_id})");
                            host.host.error(&evidence);
                            if let Err(terminalize_error) = host.host.watchdog_arm_failed() {
                                log::error!(
                                    "Failed to terminalize unowned Codex provider turn {turn_id} for run {run_id}: {terminalize_error}"
                                );
                            }
                            break;
                        }
                        if let Ok(mut guard) = current_turn_id.lock() {
                            *guard = Some(turn_id.to_string());
                        }
                    }
                    usage_accumulator.start_turn();
                }
                Notification::AgentMessageDelta { delta } => {
                    recorded_item_after_tool_result = true;
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        watchdog.record_forward_progress(Instant::now());
                    }
                    if let Some(delta) = delta {
                        handle_agent_message_delta(
                            host,
                            &run_db,
                            emitter,
                            run_id,
                            session_id.as_deref(),
                            &mut streaming_state,
                            delta,
                        );
                    }
                }
                Notification::ReasoningDelta { delta } => {
                    recorded_item_after_tool_result = true;
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        watchdog.record_forward_progress(Instant::now());
                    }
                    if let Some(text) = delta {
                        handle_reasoning_delta(
                            host,
                            &run_db,
                            emitter,
                            run_id,
                            session_id.as_deref(),
                            &mut streaming_state,
                            text,
                        );
                    }
                }
                Notification::ItemStarted { item } => {
                    if let Some(item_type) = item.get("type").and_then(|v| v.as_str()) {
                        match item_type {
                            "commandExecution" => {
                                finalize_streaming(
                                    host,
                                    &run_db,
                                    emitter,
                                    &mut streaming_state,
                                    session_id.as_deref(),
                                );
                                let tool_use_id = json_string(item.pointer("/id"))
                                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                                pending_tool_ids.insert(tool_use_id.clone());
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.start_tool(tool_use_id.clone(), Instant::now());
                                }
                                let (command_display, command_vec) = extract_command_execution(
                                    item.pointer("/command").unwrap_or(&Value::Null),
                                );
                                let cwd =
                                    item.pointer("/cwd").and_then(|v| v.as_str()).unwrap_or(".");
                                let tool_input = serde_json::json!({
                                    "command": command_display,
                                    "commandArgs": command_vec,
                                    "cwd": cwd,
                                });
                                let event = TranscriptEvent {
                                    event_type: "assistant".to_string(),
                                    session_id: session_id.clone(),
                                    parent_tool_use_id: None,
                                    content: None,
                                    thinking: None,
                                    tool_name: None,
                                    tool_input: None,
                                    tool_uses: Some(vec![ToolUseInfo {
                                        id: tool_use_id,
                                        name: "bash".to_string(),
                                        input: tool_input,
                                    }]),
                                    tool_use_id: None,
                                    tool_result: None,
                                    is_error: false,
                                    thinking_ms: None,
                                    queued_message_id: None,
                                    raw: None,
                                };
                                store_event(host, run_id, session_id.as_deref(), &event);
                            }
                            "fileChange" => {
                                finalize_streaming(
                                    host,
                                    &run_db,
                                    emitter,
                                    &mut streaming_state,
                                    session_id.as_deref(),
                                );
                                let tool_use_id = json_string(item.pointer("/id"))
                                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                                pending_tool_ids.insert(tool_use_id.clone());
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.start_tool(tool_use_id.clone(), Instant::now());
                                }
                                let mut tool_input = serde_json::json!({});
                                if let Value::Object(ref mut map) = tool_input {
                                    if let Some(changes) = item.pointer("/changes").cloned() {
                                        map.insert("changes".into(), changes);
                                    }
                                    if let Some(summary) = item.pointer("/summary").cloned() {
                                        map.insert("summary".into(), summary);
                                    }
                                }
                                let event = TranscriptEvent {
                                    event_type: "assistant".to_string(),
                                    session_id: session_id.clone(),
                                    parent_tool_use_id: None,
                                    content: None,
                                    thinking: None,
                                    tool_name: None,
                                    tool_input: None,
                                    tool_uses: Some(vec![ToolUseInfo {
                                        id: tool_use_id,
                                        name: "edit".to_string(),
                                        input: tool_input,
                                    }]),
                                    tool_use_id: None,
                                    tool_result: None,
                                    is_error: false,
                                    thinking_ms: None,
                                    queued_message_id: None,
                                    raw: None,
                                };
                                store_event(host, run_id, session_id.as_deref(), &event);
                            }
                            "mcpToolCall" => {
                                finalize_streaming(
                                    host,
                                    &run_db,
                                    emitter,
                                    &mut streaming_state,
                                    session_id.as_deref(),
                                );
                                let tool_use_id = json_string(item.pointer("/id"))
                                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                                pending_tool_ids.insert(tool_use_id.clone());
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.start_tool(tool_use_id.clone(), Instant::now());
                                }
                                let server = item
                                    .pointer("/server")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                let tool_name =
                                    item.pointer("/tool").and_then(|v| v.as_str()).unwrap_or("");
                                let arguments =
                                    item.pointer("/arguments").cloned().unwrap_or(Value::Null);
                                let full_name = if server.is_empty() {
                                    tool_name.to_string()
                                } else {
                                    format!("mcp__{}__{}", server, tool_name)
                                };
                                let event = TranscriptEvent {
                                    event_type: "assistant".to_string(),
                                    session_id: session_id.clone(),
                                    parent_tool_use_id: None,
                                    content: None,
                                    thinking: None,
                                    tool_name: None,
                                    tool_input: None,
                                    tool_uses: Some(vec![ToolUseInfo {
                                        id: tool_use_id,
                                        name: full_name,
                                        input: arguments,
                                    }]),
                                    tool_use_id: None,
                                    tool_result: None,
                                    is_error: false,
                                    thinking_ms: None,
                                    queued_message_id: None,
                                    raw: None,
                                };
                                store_event(host, run_id, session_id.as_deref(), &event);
                            }
                            _ => {
                                log::debug!("Unhandled Codex item/started type: {}", item_type);
                            }
                        }
                    }
                }
                Notification::ItemCompleted { item } => {
                    if let Some(item_type) = item.get("type").and_then(|v| v.as_str()) {
                        match item_type {
                            "agentMessage" => {
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.record_forward_progress(Instant::now());
                                }
                                if terminal_tool_called_for_run(host, run_id) {
                                    continue;
                                }
                                if let Some(text) = item.pointer("/text").and_then(|v| v.as_str()) {
                                    recorded_item_after_tool_result = true;
                                    if streaming_state.is_none()
                                        && last_direct_assistant_text.as_deref() == Some(text)
                                    {
                                        continue;
                                    }
                                    finalize_agent_message(
                                        host,
                                        &run_db,
                                        emitter,
                                        run_id,
                                        session_id.as_deref(),
                                        &mut streaming_state,
                                        text,
                                    );
                                    if streaming_state.is_none() {
                                        last_direct_assistant_text = Some(text.to_string());
                                    }
                                }
                            }
                            "commandExecution" => {
                                let tool_use_id = json_string(item.pointer("/id"));
                                let (result_text, is_error) = summarize_command_result(item);
                                if let Some(id) = tool_use_id.as_deref() {
                                    let output_chars = tool_output_chars.remove(id);
                                    emit_codex_command_complete(emitter, run_id, id, output_chars);
                                }
                                let event = codex_tool_result_event(
                                    session_id.clone(),
                                    tool_use_id.clone(),
                                    result_text,
                                    is_error,
                                    None,
                                );
                                store_event(host, run_id, session_id.as_deref(), &event);
                                recorded_item_after_tool_result = false;
                                if let Some(id) = tool_use_id.as_deref() {
                                    pending_tool_ids.remove(id);
                                }
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.complete_tool(tool_use_id.as_deref(), Instant::now());
                                }
                                interrupt_terminal_tool_at_boundary!();
                            }
                            "fileChange" => {
                                let tool_use_id = json_string(item.pointer("/id"));
                                let (result_text, is_error) = summarize_file_change_result(item);
                                let event = codex_tool_result_event(
                                    session_id.clone(),
                                    tool_use_id.clone(),
                                    result_text,
                                    is_error,
                                    None,
                                );
                                store_event(host, run_id, session_id.as_deref(), &event);
                                recorded_item_after_tool_result = false;
                                if let Some(id) = tool_use_id.as_deref() {
                                    pending_tool_ids.remove(id);
                                }
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.complete_tool(tool_use_id.as_deref(), Instant::now());
                                }
                                interrupt_terminal_tool_at_boundary!();
                            }
                            "mcpToolCall" => {
                                let tool_use_id = json_string(item.pointer("/id"));
                                let result_value = item.pointer("/result").cloned();
                                let (result_text, is_err, raw_value) =
                                    extract_mcp_result(&result_value);
                                let event = codex_tool_result_event(
                                    session_id.clone(),
                                    tool_use_id.clone(),
                                    result_text,
                                    is_err,
                                    raw_value,
                                );
                                store_event(host, run_id, session_id.as_deref(), &event);
                                recorded_item_after_tool_result = false;
                                if let Some(id) = tool_use_id.as_deref() {
                                    pending_tool_ids.remove(id);
                                }
                                if let Ok(mut watchdog) = progress_watchdog.lock() {
                                    watchdog.complete_tool(tool_use_id.as_deref(), Instant::now());
                                }
                                interrupt_terminal_tool_at_boundary!();
                            }
                            "contextCompaction" => {
                                recorded_item_after_tool_result = true;
                                if let Some(event) =
                                    codex_compaction_event_from_completed_item(&msg)
                                {
                                    store_event(host, run_id, session_id.as_deref(), &event);
                                }
                            }
                            _ => {
                                log::debug!("Unhandled Codex item/completed type: {}", item_type);
                            }
                        }
                    }
                }
                Notification::RawResponseItemCompleted { turn_id, item } => {
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        watchdog.record_forward_progress(Instant::now());
                    }
                    if let Some(text) = extract_raw_response_message_text(item) {
                        if !ephemeral {
                            if let Some(provider_turn_id) =
                                turn_id_or_cached(turn_id, &current_turn_id)
                            {
                                finalized_assistant_by_turn.insert(provider_turn_id, text.clone());
                            }
                        }
                        if let Some(ref mut state) = streaming_state {
                            // Codex streamed no content deltas but produced a final raw
                            // message: seed it once so the stream isn't empty.
                            if state.acc.content_is_empty() {
                                state.acc.push_content(&text);
                                match append_chunks(
                                    run_db.clone(),
                                    &state.stream_id,
                                    state.version,
                                    &state.acc.take_pending(),
                                ) {
                                    Ok(result) => {
                                        recorded_item_after_tool_result = true;
                                        state.version = result.version;
                                        let d = state.acc.take_emit_delta();
                                        emit_streaming_delta(emitter, run_id, &state.stream_id, &d);
                                    }
                                    Err(error) => {
                                        log::warn!(
                                            "Failed to append Codex raw message for {}: {}",
                                            run_id,
                                            error
                                        );
                                    }
                                }
                            }
                        } else if last_direct_assistant_text.as_deref() != Some(text.as_str()) {
                            let event = TranscriptEvent {
                                event_type: "assistant".to_string(),
                                session_id: session_id.clone(),
                                parent_tool_use_id: None,
                                content: Some(text.clone()),
                                thinking: None,
                                tool_name: None,
                                tool_input: None,
                                tool_uses: None,
                                tool_use_id: None,
                                tool_result: None,
                                is_error: false,
                                thinking_ms: None,
                                queued_message_id: None,
                                raw: None,
                            };
                            store_event(host, run_id, session_id.as_deref(), &event);
                            recorded_item_after_tool_result = true;
                            last_direct_assistant_text = Some(text);
                        }
                    }
                }
                Notification::TurnCompleted { turn_id, status } => {
                    let completed_provider_turn_id = turn_id_or_cached(turn_id, &current_turn_id);
                    let mut repeat_safety_pause = false;
                    if status == ProviderTurnStatus::Completed && !ephemeral {
                        if let (Some(session_id), Some(provider_turn_id), Some(cairn_turn_id)) = (
                            session_id.as_deref(),
                            completed_provider_turn_id.as_deref(),
                            host.process_state.get_current_turn_id(run_id),
                        ) {
                            if let Some(text) = finalized_assistant_by_turn.remove(provider_turn_id)
                            {
                                if let Some(notice) = host.host.observe_final_output(
                                    &cairn_turn_id,
                                    provider_turn_id,
                                    &text,
                                ) {
                                    repeat_safety_pause = true;
                                    store_event(host, run_id, Some(session_id), &notice);
                                    if let Err(error) = host.host.defer_goal_turn(provider_turn_id)
                                    {
                                        log::error!("Failed to pause repeated-output Codex turn {provider_turn_id}: {error}");
                                    }
                                }
                            }
                        }
                    } else if let Some(provider_turn_id) = completed_provider_turn_id.as_deref() {
                        finalized_assistant_by_turn.remove(provider_turn_id);
                    }
                    clear_provider_turn_if_current(
                        completed_provider_turn_id.as_deref(),
                        &current_turn_id,
                        &progress_watchdog,
                    );
                    if consume_deferred_goal_terminal(
                        &mut deferred_goal_turns,
                        completed_provider_turn_id.as_deref(),
                    ) {
                        log::info!(
                            "Observed terminal notification for deferred Codex goal turn {:?}; predecessor remains authoritative",
                            completed_provider_turn_id
                        );
                        continue;
                    }
                    // Publish provider terminality before finalization. Recovery
                    // may be waiting for this exact notification while the
                    // finalizer performs database and orchestration work.
                    log::info!(
                        "codex provider turn terminal notification observed: kind=turn/completed provider_turn_id={:?} observed_at_ms={}",
                        completed_provider_turn_id,
                        chrono::Utc::now().timestamp_millis()
                    );
                    // Schema-constrained call (CAIRN-2505): when the turn's final
                    // agent message is the native structured output, capture it as
                    // the return artifact server-side BEFORE the run finalizes.
                    // Self-guarding (a no-op for any run without an output
                    // contract) and idempotent (a no-op if the model also wrote
                    // cairn:~/return), so non-call turns and schema-less codex
                    // sessions are unaffected.
                    if status == ProviderTurnStatus::Completed {
                        if let Some(value) = last_direct_assistant_text
                            .as_deref()
                            .and_then(|text| serde_json::from_str::<Value>(text).ok())
                        {
                            host.host.capture_output(value);
                        }
                    }
                    let failure_disposition = if status == ProviderTurnStatus::Completed {
                        if let Some(turn_id) = completed_provider_turn_id.as_deref() {
                            provider_turn_failures.remove(turn_id);
                        }
                        TurnFailureDisposition::Unhandled
                    } else if let Some(turn_id) = completed_provider_turn_id.as_deref() {
                        let state = provider_turn_failures
                            .entry(turn_id.to_string())
                            .or_default();
                        if status.as_str() == "failed"
                            && !state.terminalized
                            && state.capacity_message.is_some()
                        {
                            state.terminalized = host.host.capacity_failure();
                        }
                        if state.terminalized {
                            TurnFailureDisposition::AlreadyHandled
                        } else {
                            TurnFailureDisposition::Unhandled
                        }
                    } else {
                        TurnFailureDisposition::Unhandled
                    };
                    let completed_response_usage = usage_accumulator.last_response_usage();
                    if status == ProviderTurnStatus::Completed && !recorded_item_after_tool_result {
                        super::events::store_codex_zero_item_response_event(
                            host,
                            run_id,
                            session_id.as_deref(),
                            completed_provider_turn_id.as_deref(),
                            completed_response_usage.as_ref(),
                        );
                    }
                    finalize_streaming(
                        host,
                        &run_db,
                        emitter,
                        &mut streaming_state,
                        session_id.as_deref(),
                    );
                    host.host.complete(
                        codex_turn_completion(
                            status,
                            usage_accumulator.take_turn_usage(),
                            ephemeral,
                        ),
                        CodexTurnContext {
                            repeat_safety_pause,
                            failure_disposition,
                        },
                    );
                    owned_watchdog.disarm_turn(
                        completed_provider_turn_id.as_deref(),
                        "provider_turn_completed",
                    );
                    // A pooled call is a single one-shot turn: its turn/completed
                    // is terminal, so stop consuming and let the thread exit.
                    if ephemeral {
                        break;
                    }
                }
                Notification::TurnAborted { turn_id, reason } => {
                    let aborted_provider_turn_id = turn_id_or_cached(turn_id, &current_turn_id);
                    if consume_deferred_goal_terminal(
                        &mut deferred_goal_turns,
                        aborted_provider_turn_id.as_deref(),
                    ) {
                        clear_provider_turn_if_current(
                            aborted_provider_turn_id.as_deref(),
                            &current_turn_id,
                            &progress_watchdog,
                        );
                        log::info!(
                            "Observed abort for deferred Codex goal turn {:?}; predecessor remains authoritative",
                            aborted_provider_turn_id
                        );
                        continue;
                    }
                    clear_provider_turn_if_current(
                        aborted_provider_turn_id.as_deref(),
                        &current_turn_id,
                        &progress_watchdog,
                    );
                    // Interrupt acknowledgement is the terminal notification,
                    // not completion of Cairn's downstream finalization.
                    log::info!(
                        "codex provider turn terminal notification observed: kind=turn/aborted provider_turn_id={:?} observed_at_ms={}",
                        aborted_provider_turn_id,
                        chrono::Utc::now().timestamp_millis()
                    );
                    let status = ProviderTurnStatus::from_abort_reason(reason);
                    if status == ProviderTurnStatus::Interrupted {
                        log::info!("codex turn aborted ({}), handling interrupt", reason);
                    }
                    finalize_streaming(
                        host,
                        &run_db,
                        emitter,
                        &mut streaming_state,
                        session_id.as_deref(),
                    );
                    host.host.complete(
                        codex_turn_completion(
                            status,
                            usage_accumulator.take_turn_usage(),
                            ephemeral,
                        ),
                        CodexTurnContext {
                            repeat_safety_pause: false,
                            failure_disposition: TurnFailureDisposition::Unhandled,
                        },
                    );
                    if let Some(turn_id) = aborted_provider_turn_id.as_deref() {
                        provider_turn_failures.remove(turn_id);
                    }
                    owned_watchdog
                        .disarm_turn(aborted_provider_turn_id.as_deref(), "provider_turn_aborted");
                    // A pooled call's aborted turn is terminal (killed or
                    // failed): stop consuming and exit the thread.
                    if ephemeral {
                        break;
                    }
                }
                Notification::Error {
                    turn_id,
                    message,
                    will_retry,
                } => {
                    let error_provider_turn_id = turn_id_or_cached(turn_id, &current_turn_id);
                    let message = message.unwrap_or("Unknown Codex error");
                    if is_deferred_goal_turn(
                        &deferred_goal_turns,
                        error_provider_turn_id.as_deref(),
                    ) {
                        log::info!(
                            "Ignoring provider error for deferred Codex goal turn {:?}: {}",
                            error_provider_turn_id,
                            message
                        );
                        continue;
                    }
                    if will_retry {
                        log::warn!("Codex retryable error: {}", message);
                        if is_selected_model_capacity_error(message) {
                            if let Some(turn_id) = error_provider_turn_id {
                                provider_turn_failures
                                    .entry(turn_id)
                                    .or_default()
                                    .capacity_message = Some(normalize_capacity_error(message));
                            }
                        }
                    } else {
                        let already_terminalized = error_provider_turn_id
                            .as_ref()
                            .and_then(|turn_id| provider_turn_failures.get(turn_id))
                            .is_some_and(|state| state.terminalized);
                        if already_terminalized {
                            continue;
                        }
                        if is_selected_model_capacity_error(message) {
                            finalize_streaming(
                                host,
                                &run_db,
                                emitter,
                                &mut streaming_state,
                                session_id.as_deref(),
                            );
                            if host.host.capacity_failure() {
                                if let Some(turn_id) = error_provider_turn_id {
                                    let state = provider_turn_failures.entry(turn_id).or_default();
                                    state.capacity_message =
                                        Some(normalize_capacity_error(message));
                                    state.terminalized = true;
                                }
                                continue;
                            }
                        }
                        log::error!("Codex fatal error: {}", message);
                        finalize_streaming(
                            host,
                            &run_db,
                            emitter,
                            &mut streaming_state,
                            session_id.as_deref(),
                        );
                        host.host.error(&format!("Codex error: {}", message));
                        host.host.complete(
                            codex_turn_completion(
                                ProviderTurnStatus::Other("error"),
                                usage_accumulator.take_turn_usage(),
                                ephemeral,
                            ),
                            CodexTurnContext {
                                repeat_safety_pause: false,
                                failure_disposition: TurnFailureDisposition::DiagnosticRecorded,
                            },
                        );
                        if let Some(turn_id) = error_provider_turn_id {
                            provider_turn_failures
                                .entry(turn_id)
                                .or_default()
                                .terminalized = true;
                        }
                        // A pooled call's fatal error is terminal: exit the thread.
                        if ephemeral {
                            break;
                        }
                    }
                }
                Notification::TokenUsageUpdated { params } => {
                    if let Some(params) = params {
                        let model = host.process_state.get_model(run_id);
                        if let Some(state) = codex_context_tokens_from_notification(
                            params,
                            run_id,
                            session_id.clone(),
                            backend_key.clone(),
                            model,
                            chrono::Utc::now().timestamp(),
                        ) {
                            usage_accumulator.observe(params);
                            host.host.context_snapshot(state);
                        }
                    }
                }
                Notification::RateLimitsUpdated { rate_limits } => {
                    if let Some(snapshot) =
                        codex_rate_limit_snapshot_from_value(rate_limits.clone())
                    {
                        host.host.account_usage(snapshot);
                    }
                }
                Notification::CommandOutputDelta { item_id, delta } => {
                    let output_chars = if item_id.is_empty() {
                        0
                    } else {
                        let count = tool_output_chars.entry(item_id.to_string()).or_insert(0);
                        *count += delta.chars().count() as i32;
                        *count
                    };
                    emit_codex_command_output_delta(emitter, run_id, item_id, delta, output_chars);
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        watchdog.record_forward_progress(Instant::now());
                    }
                }
                Notification::Ignored { .. } => {}
                Notification::Unknown { method } => {
                    log::debug!("Unhandled Codex notification: {:?}", method);
                }
            }
        }

        owned_watchdog.disarm("notification_stream_closed");
        finalize_streaming(
            host,
            &run_db,
            emitter,
            &mut streaming_state,
            session_id.as_deref(),
        );

        // Pooled ephemeral call: drop this call's routing entries so the pool no
        // longer maps its threadId. Idempotent with crash teardown (which fails
        // in-flight calls and drops the whole pool). Reached whether the reader
        // broke on a terminal turn or the channel closed under it.
        host.host.reader_closed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn completion_report_preserves_unknown_status_and_usage_without_host_policy() {
        for ephemeral in [false, true] {
            for (status, expected) in [
                ("completed", SessionTurnOutcome::Completed),
                ("interrupted", SessionTurnOutcome::Interrupted),
                (
                    "new_provider_failure",
                    SessionTurnOutcome::Failed {
                        status: "new_provider_failure".into(),
                    },
                ),
            ] {
                let usage = serde_json::from_value(json!({"input_tokens": 11, "output_tokens": 7, "cache_read_input_tokens": 3, "cache_creation_input_tokens": 2, "output_tokens_details": {"thinking_tokens": 5}})).unwrap();
                let report = codex_turn_completion(
                    ProviderTurnStatus::from_wire(status),
                    Some(usage),
                    ephemeral,
                );
                assert_eq!(report.outcome, expected);
                assert_eq!(
                    report.usage,
                    TokenCounts {
                        input: Some(11),
                        output: Some(7),
                        cache_read: Some(3),
                        cache_create: Some(2),
                        thinking: Some(5)
                    }
                );
                assert_eq!(
                    report.session_lifetime,
                    if ephemeral {
                        SessionLifetime::OneShot
                    } else {
                        SessionLifetime::Persistent
                    }
                );
            }
        }
        assert_eq!(
            codex_turn_completion(ProviderTurnStatus::Completed, None, false).usage,
            TokenCounts::default()
        );
    }
    #[test]
    fn deferred_goal_notifications_and_terminals_are_idempotent() {
        let mut deferred = HashSet::new();
        assert!(claim_deferred_goal_turn(&mut deferred, "provider-1"));
        assert!(!claim_deferred_goal_turn(&mut deferred, "provider-1"));
        assert_eq!(deferred.len(), 1);
        assert!(is_deferred_goal_turn(&deferred, Some("provider-1")));
        assert!(!is_deferred_goal_turn(&deferred, Some("provider-2")));

        assert!(consume_deferred_goal_terminal(
            &mut deferred,
            Some("provider-1")
        ));
        assert!(!consume_deferred_goal_terminal(
            &mut deferred,
            Some("provider-1")
        ));
        assert!(deferred.is_empty());
    }
    #[test]
    fn watchdog_arm_missing_session_is_a_failure() {
        assert_eq!(
            required_watchdog_session(None),
            Err(WatchdogArmError::MissingSession)
        );
    }
    #[test]
    fn watchdog_arm_ownership_conflict_is_a_failure() {
        assert_eq!(
            checked_watchdog_arm(Ok(false)),
            Err(WatchdogArmError::OwnershipConflict)
        );
    }
    #[test]
    fn watchdog_arm_database_error_is_a_failure() {
        assert_eq!(
            checked_watchdog_arm(Err("database unavailable".into())),
            Err(WatchdogArmError::Database("database unavailable".into()))
        );
    }
    #[test]
    fn context_compaction_event_only_comes_from_completed_item() {
        let started = json!({
            "method": "item/started",
            "params": {
                "item": {
                    "id": "compact-1",
                    "type": "contextCompaction",
                    "previousContextWindow": 200_000
                }
            }
        });
        let completed = json!({
            "method": "item/completed",
            "params": {
                "item": {
                    "id": "compact-1",
                    "type": "contextCompaction",
                    "previousContextWindow": 200_000
                }
            }
        });

        let events: Vec<_> = [&started, &completed]
            .into_iter()
            .filter_map(codex_compaction_event_from_completed_item)
            .collect();

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.event_type, "system:compact_boundary");
        assert_eq!(event.content.as_deref(), Some("Context compacted"));
        assert_eq!(event.raw.as_ref().unwrap()["provider"], "codex");
        assert_eq!(
            event.raw.as_ref().unwrap()["compaction"],
            completed.pointer("/params/item").unwrap().clone()
        );
    }
    #[test]
    fn turn_progress_watchdog_uses_phase_deadlines_and_fires_once_after_jump() {
        let start = Instant::now();
        let general = Duration::from_secs(10);
        let mut watchdog = CodexTurnProgressWatchdog::new(general, Duration::from_secs(3));
        watchdog.start_turn("turn-1", start);

        assert_eq!(
            watchdog.expired(start + general - Duration::from_millis(1)),
            None
        );
        let expiry = watchdog
            .expired(start + general + Duration::from_secs(20))
            .expect("elapsed jump expires once");
        assert_eq!(expiry.turn_id, "turn-1");
        assert_eq!(expiry.phase, CodexTurnWatchdogPhase::ProviderProgress);
        assert_eq!(expiry.elapsed, Duration::from_secs(30));
        assert_eq!(watchdog.expired(start + Duration::from_secs(31)), None);
    }
    #[test]
    fn exact_tool_ids_hold_the_watchdog_until_every_tool_completes() {
        let start = Instant::now();
        let mut watchdog =
            CodexTurnProgressWatchdog::new(Duration::from_secs(10), Duration::from_secs(3));
        watchdog.start_turn("turn-1", start);
        watchdog.start_tool("tool-1".to_string(), start);
        watchdog.start_tool("tool-2".to_string(), start);

        assert_eq!(watchdog.expired(start + Duration::from_secs(60)), None);
        assert!(watchdog.complete_tool(Some("tool-1"), start + Duration::from_secs(60)));
        assert_eq!(watchdog.phase, CodexTurnWatchdogPhase::ToolOutstanding);
        assert_eq!(watchdog.expired(start + Duration::from_secs(90)), None);
        assert!(watchdog.complete_tool(Some("tool-2"), start + Duration::from_secs(90)));
        assert_eq!(watchdog.phase, CodexTurnWatchdogPhase::PostToolContinuation);
        assert_eq!(
            watchdog
                .expired(start + Duration::from_secs(93))
                .unwrap()
                .phase,
            CodexTurnWatchdogPhase::PostToolContinuation
        );
    }
    #[test]
    fn unmatched_completion_preserves_known_outstanding_tools() {
        let start = Instant::now();
        let mut watchdog =
            CodexTurnProgressWatchdog::new(Duration::from_secs(10), Duration::from_secs(3));
        watchdog.start_turn("turn-1", start);
        watchdog.start_tool("started-id".to_string(), start);

        assert!(!watchdog.complete_tool(Some("different-id"), start + Duration::from_secs(1)));
        assert_eq!(
            watchdog.outstanding_tool_ids,
            HashSet::from(["started-id".to_string()])
        );
        assert_eq!(watchdog.phase, CodexTurnWatchdogPhase::ToolOutstanding);
        assert_eq!(watchdog.expired(start + Duration::from_secs(60)), None);
    }
    #[test]
    fn only_provider_continuation_leaves_post_tool_phase() {
        let start = Instant::now();
        let mut watchdog =
            CodexTurnProgressWatchdog::new(Duration::from_secs(10), Duration::from_secs(3));
        watchdog.start_turn("turn-1", start);
        watchdog.start_tool("tool-1".to_string(), start);
        watchdog.complete_tool(Some("tool-1"), start + Duration::from_secs(1));

        watchdog.record_forward_progress(start + Duration::from_secs(2));
        assert_eq!(watchdog.phase, CodexTurnWatchdogPhase::ProviderProgress);
        assert_eq!(watchdog.expired(start + Duration::from_secs(11)), None);
        assert_eq!(
            watchdog
                .expired(start + Duration::from_secs(12))
                .unwrap()
                .phase,
            CodexTurnWatchdogPhase::ProviderProgress
        );
    }
    #[test]
    fn turn_progress_watchdog_clear_turn_disarms_and_clears_tools() {
        let start = Instant::now();
        let mut watchdog =
            CodexTurnProgressWatchdog::new(Duration::from_secs(10), Duration::from_secs(3));
        watchdog.start_turn("turn-1", start);
        watchdog.start_tool("tool-1".to_string(), start);
        watchdog.clear_turn();

        assert!(watchdog.outstanding_tool_ids.is_empty());
        assert_eq!(watchdog.expired(start + Duration::from_secs(60)), None);
    }
    #[test]
    fn watchdog_progress_revision_tracks_each_deadline_refresh() {
        let start = Instant::now();
        let mut watchdog =
            CodexTurnProgressWatchdog::new(Duration::from_secs(10), Duration::from_secs(3));
        assert_eq!(watchdog.progress_sequence, 0);
        watchdog.start_turn("turn-1", start);
        assert_eq!(watchdog.progress_sequence, 1);
        watchdog.record_forward_progress(start + Duration::from_secs(1));
        assert_eq!(watchdog.progress_sequence, 2);
        watchdog.start_tool("tool-1".to_string(), start + Duration::from_secs(2));
        assert_eq!(watchdog.progress_sequence, 3);
        watchdog.complete_tool(Some("tool-1"), start + Duration::from_secs(3));
        assert_eq!(watchdog.progress_sequence, 4);
    }
    #[test]
    fn late_completion_does_not_clear_newer_provider_turn() {
        let current = Arc::new(Mutex::new(Some("turn-2".to_string())));
        let watchdog = Arc::new(Mutex::new(CodexTurnProgressWatchdog::new(
            Duration::from_secs(10),
            Duration::from_secs(3),
        )));
        watchdog
            .lock()
            .unwrap()
            .start_turn("turn-2", Instant::now());

        clear_provider_turn_if_current(Some("turn-1"), &current, &watchdog);

        assert_eq!(current.lock().unwrap().as_deref(), Some("turn-2"));
        assert_eq!(
            watchdog.lock().unwrap().active_turn_id.as_deref(),
            Some("turn-2")
        );
    }
}
