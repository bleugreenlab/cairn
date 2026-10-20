//! Claude stream-json session driver and stream assembly.
use crate::agent_process::completion::{ClaudeTurnContext, ClaudeTurnOutcome};
use crate::agent_process::memory::{MemoryProbe, OsMemoryProbe};
use crate::agent_process::process::{AgentProcessState, RunOccupancy};
use crate::agent_process::stream::{TokenCounts, TranscriptEvent, TranscriptJson};
use crate::agent_process::turn_boundary::{
    should_interrupt_terminal_tool_at_boundary, TurnBoundaryChecker,
};
use crate::backends::run_state::{run_backend_db, set_session_backend_id};
use crate::config::BackendFailure;
use crate::transcripts::stream_store::{
    abort_stream, append_chunks, open_stream, ActiveMessageStream, EmitDelta, EventInsert,
    StreamAccumulator, StreamingToolWrite,
};
use cairn_common::ids;
use cairn_db::storage::LocalDb;
use cairn_domain::ContextTokenState;
use cairn_models::session::events::SessionEvent;
use cairn_models::types::catalog::{claude_reported_context_window, claude_session_window};
use cairn_models::types::usage::{ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow};
use cairn_models::wire::claude_stream::{
    extract_tool_start, message_start_usage, parse_thinking_tokens_estimate,
    result_is_rate_limited, stderr_reports_session_id_in_use, stderr_reports_unknown_session,
    terminal_provider_depletion, ClaudeEvent, ControlResponseInner, DeltaContent,
    ProviderDepletion, RateLimitInfo, StreamEventInner, StreamEvidence, Usage,
};
use cairn_platform::services::emitter::EventEmitter;
use std::{
    collections::HashSet,
    io::BufRead,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

/// Application reactions to provider evidence. One host is bound to one run.
/// The driver owns native protocol ordering; the host owns jobs and credentials.
pub trait ClaudeHost: Send + Sync {
    fn transcripts(&self) -> &dyn crate::transcripts::TranscriptHost;
    fn error(&self, message: &str);
    /// Stop the process through host-owned admission release policy.
    fn stop_process(&self);
    fn startup_rejected(&self, message: &str);
    fn initialized(&self);
    fn capture_output(&self, output: serde_json::Value);
    fn rate_limit(&self, snapshot: ProviderUsageSnapshot, info: Option<&RateLimitInfo>) -> bool;
    /// Hold a turn the provider refused for a structured usage limit, without
    /// completing it: a terminal HTTP 429, or a terminal error result after the
    /// CLI's synthetic credits message. Called before any completion of that
    /// turn, so account health and any replacement pin are durable before
    /// queued work can dispatch. Returns whether the account's health was
    /// persisted.
    fn provider_limit_result(&self, report: ProviderLimitReport<'_>) -> bool;
    fn context_snapshot(&self, snapshot: ContextTokenState);
    fn complete(&self, outcome: ClaudeTurnOutcome, context: ClaudeTurnContext);
    fn artifact_handoff(&self);
    fn interrupt(&self) -> Result<(), String>;
    fn recover_watchdog(
        &self,
        identity: &crate::runs::watchdog_ledger::WatchdogIdentity,
        now: i64,
    ) -> Result<(), String>;
    /// The CLI was refused with a 401 and is retrying; with its 401 wait set it
    /// is waiting for a different token to be pushed. Must not block the reader.
    fn credential_rejected(&self);
    /// The CLI acknowledged a pushed credential of `generation`.
    fn credential_acknowledged(&self, generation: u64);

    fn eof(&self, report: ClaudeEof);
}

/// What the reader observed about a turn the provider refused for a usage limit.
#[derive(Debug, Clone, Copy)]
pub struct ProviderLimitReport<'a> {
    /// The terminal result's text, the only place the CLI states a reset clock.
    pub result_text: Option<&'a str>,
    /// The turn's classification: credit exhaustion, reported by an earlier
    /// structured error, wins over a bare rolling-window 429.
    pub depletion: ProviderDepletion,
    /// This turn's blocking `rate_limit_event` already persisted the provider's
    /// own block, which a reset-less re-probe quarantine must not overwrite.
    pub block_recorded: bool,
    /// The reset that blocking event declared.
    pub blocking_reset: Option<i64>,
    /// A tool call was issued and its result never reached the stream before the
    /// refusal. The CLI may already have run it, so resuming without an operator
    /// could run it again.
    pub tool_in_flight: bool,
    pub now: i64,
}

pub struct ClaudeEof {
    pub evidence: StreamEvidence,
    pub persisted_rate_limit_quarantine: bool,
    /// The depletion classified during the last turn, if any.
    pub pending_depletion: Option<ProviderDepletion>,
    /// Whether a terminal result already handed that turn to
    /// [`ClaudeHost::provider_limit_result`].
    pub depletion_handled: bool,
    /// Whether the last turn's blocking `rate_limit_event` persisted a block.
    pub turn_block_recorded: bool,
    /// The reset that blocking event declared.
    pub blocking_reset: Option<i64>,
    /// As [`ProviderLimitReport::tool_in_flight`], at the moment stdout closed.
    pub tool_in_flight: bool,
    pub terminal_tool_called: bool,
    pub backend_failure: Option<BackendFailure>,
}

#[derive(Clone)]
pub struct ClaudeRuntime {
    pub process_state: Arc<AgentProcessState>,
    pub emitter: Arc<dyn EventEmitter>,
    pub boot_id: String,
    pub host: Arc<dyn ClaudeHost>,
}
impl ClaudeRuntime {
    fn was_interrupted(&self, run_id: &str) -> bool {
        self.process_state
            .get_occupancy(run_id)
            .is_some_and(|o| matches!(o, RunOccupancy::Idle))
    }
}
pub const CLAUDE_BACKEND_NAME: &str = "Claude";
/// A reset-less terminal 429 proves the account is unavailable now, but says
/// nothing about a permanent exhaustion window. Quarantine briefly, then route
/// one launch back as a re-probe rather than inventing immediate recovery or
/// requiring a manual refresh to escape the snapshot forever.
pub const TERMINAL_429_REPROBE_AFTER: Duration = Duration::from_secs(5 * 60);
pub(crate) const TOOL_INPUT_PREVIEW_MAX_CHARS: usize = 512;
pub(crate) const CLAUDE_TURN_NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub(crate) const CLAUDE_TURN_WATCHDOG_POLL: Duration = Duration::from_secs(1);
pub(crate) const CLAUDE_TURN_WATCHDOG_HEARTBEAT: Duration = Duration::from_secs(30);

pub(crate) fn replace_claude_watchdog_lease(
    db: Arc<LocalDb>,
    slot: &Arc<Mutex<Option<crate::runs::watchdog_ledger::WatchdogIdentity>>>,
    run_id: &str,
    session_id: &str,
    turn_id: &str,
    runner_boot_id: &str,
) -> Result<(), String> {
    let replacement =
        arm_claude_watchdog_lease(db.clone(), run_id, session_id, turn_id, runner_boot_id)?;
    let previous = slot
        .lock()
        .map_err(|_| "Claude watchdog identity lock was poisoned".to_string())?
        .replace(replacement);
    if let Some(previous) = previous {
        disarm_claude_watchdog_lease(db, &previous, "provider_turn_replaced");
    }

    Ok(())
}

pub(crate) fn heartbeat_claude_watchdog_lease(
    db: Arc<LocalDb>,
    identity: &crate::runs::watchdog_ledger::WatchdogIdentity,
) {
    let identity = identity.clone();
    let now = chrono::Utc::now().timestamp();
    if let Err(error) = run_backend_db(CLAUDE_BACKEND_NAME, async move {
        crate::runs::watchdog_ledger::heartbeat_watchdog(&db, &identity, now)
            .await
            .map_err(|error| error.to_string())
    }) {
        log::error!("Failed to heartbeat durable Claude watchdog: {error}");
    }
}

pub fn disarm_claude_watchdog_lease(
    db: Arc<LocalDb>,
    identity: &crate::runs::watchdog_ledger::WatchdogIdentity,
    reason: &str,
) {
    let disarmed_identity = identity.clone();
    let durable_reason = reason.to_string();
    let now = chrono::Utc::now().timestamp();
    match run_backend_db(CLAUDE_BACKEND_NAME, {
        let db = db.clone();
        async move {
            crate::runs::watchdog_ledger::disarm_watchdog(
                &db,
                &disarmed_identity,
                &durable_reason,
                now,
            )
            .await
            .map_err(|error| error.to_string())
        }
    }) {
        Ok(true) => record_claude_watchdog_lifecycle(
            db,
            identity,
            crate::runs::watchdog_ledger::WatchdogLifecycleKind::Disarmed,
            None,
            reason,
        ),
        Ok(false) => {}
        Err(error) => log::error!("Failed to durably disarm Claude watchdog: {error}"),
    }
}

pub fn record_claude_watchdog_lifecycle(
    db: Arc<LocalDb>,
    identity: &crate::runs::watchdog_ledger::WatchdogIdentity,
    kind: crate::runs::watchdog_ledger::WatchdogLifecycleKind,
    phase: Option<crate::runs::watchdog_ledger::WatchdogPhase>,
    reason: &str,
) {
    let event = crate::runs::watchdog_ledger::NewWatchdogLifecycle {
        id: Uuid::new_v4().to_string(),
        identity: identity.clone(),
        kind,
        phase,
        reason: Some(reason.to_string()),
        details: Some(serde_json::json!({ "provider": "claude" })),
        successor_run_id: None,
        successor_turn_id: None,
        created_at: chrono::Utc::now().timestamp(),
    };
    if let Err(error) = run_backend_db(CLAUDE_BACKEND_NAME, async move {
        crate::runs::watchdog_ledger::append_watchdog_lifecycle(&db, event)
            .await
            .map_err(|error| error.to_string())
    }) {
        log::error!("Failed to persist Claude watchdog lifecycle: {error}");
    }
}

pub fn arm_claude_watchdog_lease(
    db: Arc<LocalDb>,
    run_id: &str,
    session_id: &str,
    turn_id: &str,
    runner_boot_id: &str,
) -> Result<crate::runs::watchdog_ledger::WatchdogIdentity, String> {
    use crate::runs::watchdog_ledger::{
        arm_watchdog, NewWatchdogLease, WatchdogIdentity, WatchdogPhase,
    };
    let identity = WatchdogIdentity {
        run_id: run_id.to_string(),
        session_id: session_id.to_string(),
        provider_turn_id: turn_id.to_string(),
        generation: Uuid::new_v4().to_string(),
    };
    let now = chrono::Utc::now().timestamp();
    let armed = identity.clone();
    let runner_boot_id = runner_boot_id.to_string();
    run_backend_db(CLAUDE_BACKEND_NAME, async move {
        arm_watchdog(
            &db,
            NewWatchdogLease {
                identity: armed,
                runner_boot_id,
                phase: WatchdogPhase::ProviderProgress,
                phase_deadline_at: now + CLAUDE_TURN_NO_PROGRESS_TIMEOUT.as_secs() as i64,
                now,
            },
            Uuid::new_v4().to_string(),
        )
        .await
        .map_err(|error| error.to_string())
    })?
    .then_some(identity)
    .ok_or_else(|| "Claude watchdog lease was not armed".to_string())
}

pub fn update_claude_watchdog_lease(
    db: Arc<LocalDb>,
    identity: &crate::runs::watchdog_ledger::WatchdogIdentity,
    phase: crate::runs::watchdog_ledger::WatchdogPhase,
) {
    use crate::runs::watchdog_ledger::refresh_watchdog_progress;
    let identity = identity.clone();
    let now = chrono::Utc::now().timestamp();
    let deadline = now + CLAUDE_TURN_NO_PROGRESS_TIMEOUT.as_secs() as i64;
    if let Err(error) = run_backend_db(CLAUDE_BACKEND_NAME, async move {
        refresh_watchdog_progress(&db, &identity, phase, deadline, now, now)
            .await
            .map_err(|error| error.to_string())
    }) {
        log::error!("Failed to update durable Claude watchdog lease: {error}");
    }
}

pub fn sanitize_mcp_diagnostic(line: &str) -> String {
    let policy = cairn_common::security::sanitize::RedactionPolicy::default();
    let mut sanitizer = cairn_common::security::sanitize::Sanitizer::structural(&policy);
    sanitizer.text(line).chars().take(500).collect()
}

fn mcp_tool_inventory_diagnostic(tools: &[&str]) -> String {
    const MAX_NAMES: usize = 16;
    const MAX_NAME_CHARS: usize = 96;

    let mut names = tools
        .iter()
        .take(MAX_NAMES)
        .map(|name| {
            // Redact before truncating so a cut credential cannot evade matching.
            // Debug quoting keeps untrusted names from injecting diagnostic lines.
            let quoted = format!("{:?}", sanitize_mcp_diagnostic(name));
            let mut chars = quoted.chars();
            let mut bounded: String = chars.by_ref().take(MAX_NAME_CHARS).collect();
            if chars.next().is_some() {
                bounded.push('…');
            }
            bounded
        })
        .collect::<Vec<_>>();
    if tools.len() > MAX_NAMES {
        names.push(format!("… (+{} more)", tools.len() - MAX_NAMES));
    }
    format!("[{}]", names.join(", "))
}

pub fn validate_claude_init(
    data: &serde_json::Value,
    stderr_diagnostics: &[String],
) -> Result<(), String> {
    let tools = data
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();
    let missing = crate::agent_process::toolkits::CORE_VERBS
        .iter()
        .copied()
        .filter(|required| !tools.contains(required))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }

    let server_status = data
        .get("mcp_servers")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|server| server.get("name").and_then(serde_json::Value::as_str) == Some("cairn"))
        .map(|server| {
            server
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
        })
        .collect::<Vec<_>>();
    let diagnostics = if stderr_diagnostics.is_empty() {
        "Claude reported no MCP startup diagnostics".to_string()
    } else {
        stderr_diagnostics.join(" | ")
    };
    Err(format!(
        "Claude started without the required Cairn MCP tools (cairn server status: {}). Declared tools: {}; missing tools: {}. MCP startup diagnostics: {diagnostics}",
        if server_status.is_empty() { "not reported".to_string() } else { server_status.join(", ") },
        mcp_tool_inventory_diagnostic(&tools),
        mcp_tool_inventory_diagnostic(&missing),
    ))
}

/// Bounds a provider turn that has started but emits no further protocol
/// progress. Tool calls are excluded: their host-owned execution has its own
/// deadlines and may legitimately remain silent for much longer than a model
/// inference. This specifically covers a resumed Claude process that accepts a
/// launch but never emits even an init/result event (CAIRN-3561).
#[derive(Debug)]
pub(crate) struct ClaudeTurnProgressWatchdog {
    active_turn_id: Option<String>,
    last_forward_progress_at: Instant,
    timeout: Duration,
    pending_tool_count: usize,
    fired: bool,
}

impl ClaudeTurnProgressWatchdog {
    pub fn new(timeout: Duration) -> Self {
        Self {
            active_turn_id: None,
            last_forward_progress_at: Instant::now(),
            timeout,
            pending_tool_count: 0,
            fired: false,
        }
    }

    pub fn observe_turn(&mut self, turn_id: Option<&str>, now: Instant) -> bool {
        if self.active_turn_id.as_deref() != turn_id {
            self.active_turn_id = turn_id.map(str::to_owned);
            self.last_forward_progress_at = now;
            self.pending_tool_count = 0;
            self.fired = false;
            return true;
        }
        false
    }

    pub fn record_forward_progress(&mut self, now: Instant) {
        if self.active_turn_id.is_some() {
            self.last_forward_progress_at = now;
        }
    }

    pub fn set_pending_tool_count(&mut self, count: usize) {
        self.pending_tool_count = count;
    }

    pub fn expired(&mut self, now: Instant) -> Option<String> {
        if self.fired || self.pending_tool_count > 0 {
            return None;
        }
        let turn_id = self.active_turn_id.as_ref()?;
        if now.duration_since(self.last_forward_progress_at) < self.timeout {
            return None;
        }
        self.fired = true;
        Some(turn_id.clone())
    }
}

/// Estimated resident memory of a single ephemeral (`cairn:~/calls`) `claude`
/// process. Claude is permanently CLI-bound, so each call is a dedicated
/// `claude --print` process carrying a Node.js runtime; ~450 MB is the
/// steady-state RSS observed for one (CAIRN-2543, where a per-call fan-out of
/// dozens of such processes reached tens of GB aggregate and OS-killed the
/// runner). It is an *estimate*, not a live measurement — the admission ceiling
/// derived from it only needs the right order of magnitude to bound fan-out
/// memory, so it is a fixed named constant rather than a per-process probe.
pub(crate) const CLAUDE_CALL_PROCESS_RSS_ESTIMATE: u64 = 450 * 1024 * 1024;

/// Divisor applied to total physical RAM to get the memory budget the ceiling
/// may spend on concurrent `claude` processes. `4` == 25% of RAM: ample
/// headroom for the agent processes, the runner, and the OS, while still letting
/// a large workstation run a wide fan-out fully parallel.
pub(crate) const CLAUDE_CALL_RAM_BUDGET_DIVISOR: u64 = 4;

/// Floor on the derived ceiling so even a small machine gets useful call
/// parallelism (and an unmeasurable system stays protective).
pub(crate) const CLAUDE_CALL_CONCURRENCY_FLOOR: usize = 4;

/// Cap on the derived ceiling to keep process count and file-descriptor
/// pressure sane no matter how much RAM the budget would otherwise permit.
pub(crate) const CLAUDE_CALL_CONCURRENCY_CAP: usize = 64;

/// Pure ceiling formula: `clamp(budget / per-process RSS, FLOOR, CAP)`, where
/// the budget is a fixed fraction of total physical RAM. Side-effect-free and
/// keyed only on `total_ram_bytes` so the clamp boundaries are unit-testable
/// with injected RAM values, never the host's real memory.
pub fn claude_call_concurrency_ceiling(total_ram_bytes: u64) -> usize {
    let budget = total_ram_bytes / CLAUDE_CALL_RAM_BUDGET_DIVISOR;
    let raw = (budget / CLAUDE_CALL_PROCESS_RSS_ESTIMATE) as usize;
    raw.clamp(CLAUDE_CALL_CONCURRENCY_FLOOR, CLAUDE_CALL_CONCURRENCY_CAP)
}

/// Claude CLI can report a terminal failure as a `Result` whose subtype is still
/// `success`. `is_error` is authoritative; this helper turns the structured
/// failure into a named node-visible diagnosis. Only an actual HTTP 429 is called
/// a rate limit. The generic case stays neutral because a terminal result can
/// arrive before or after a tool call and this payload does not prove which.
pub(crate) fn terminal_result_error_message(data: &serde_json::Value) -> &'static str {
    if result_is_rate_limited(data) {
        "The provider refused the request because its rate limit was reached (HTTP 429). The turn is interrupted and resumable once provider capacity is available."
    } else {
        "The provider returned a terminal error. The turn is interrupted and resumable."
    }
}

/// Ceiling on simultaneous ephemeral (`cairn:~/calls`) `claude` processes,
/// derived ONCE at startup from total physical RAM (see
/// [`claude_call_concurrency_ceiling`]). Admission caps fan-out here so width
/// stops translating into process count: at the derived ceiling an N-call
/// fan-out queues onto the slots and Claude call memory stays bounded to
/// `ceiling × ~450 MB` regardless of N. The cap exists to bound MEMORY, so it is
/// sized to the machine's RAM rather than a fixed count (CAIRN-2557, superseding
/// the fixed 6 of CAIRN-2548). When system memory is unmeasurable it falls back
/// to the protective [`CLAUDE_CALL_CONCURRENCY_FLOOR`]. Computed via `LazyLock`
/// on first access — the runner/server force and log it at startup.
pub static CLAUDE_CALL_MAX_CONCURRENCY: std::sync::LazyLock<usize> = std::sync::LazyLock::new(
    || {
        let system = OsMemoryProbe.system_memory();
        let ceiling = match system {
            Some(mem) => claude_call_concurrency_ceiling(mem.total),
            None => CLAUDE_CALL_CONCURRENCY_FLOOR,
        };
        log::info!(
            "Claude ephemeral-call concurrency ceiling: {ceiling} (total physical RAM: {}, budget ~{}%, per-process estimate {} MB)",
            system
                .map(|m| format!("{} MiB", m.total / (1024 * 1024)))
                .unwrap_or_else(|| "unmeasurable".to_string()),
            100 / CLAUDE_CALL_RAM_BUDGET_DIVISOR,
            CLAUDE_CALL_PROCESS_RSS_ESTIMATE / (1024 * 1024),
        );
        ceiling
    },
);

/// State for tracking a durable streaming message.
#[derive(Debug)]
pub struct StreamingState {
    stream_id: String,
    version: i32,
    acc: StreamAccumulator,
    /// When the message stream opened, used to measure reasoning duration.
    opened_at: std::time::Instant,
    /// Thinking-phase duration in ms, captured at the thinking→content boundary
    /// (first content delta / consolidated assistant event). None until thinking
    /// ends; lets finalize report thinking time rather than whole-turn time.
    thinking_ms: Option<i64>,
    /// Backend wall-clock anchor (epoch ms) for the thinking phase, set once on
    /// the first non-zero thinking-token count. Emitted on every `streaming-update`
    /// so any client (including one that opens mid-think) ticks the live duration
    /// from the true start instead of its own client clock.
    thinking_started_at_ms: Option<i64>,
    /// Non-persisted live context for a tool call whose JSON input is currently
    /// being constructed by the backend stream.
    tool_write: Option<StreamingToolWrite>,
}

impl StreamingState {
    pub fn new(stream: &ActiveMessageStream) -> Self {
        Self {
            stream_id: stream.stream.id.clone(),
            version: stream.stream.version,
            acc: StreamAccumulator::new(),
            opened_at: std::time::Instant::now(),
            thinking_ms: None,
            thinking_started_at_ms: None,
            tool_write: None,
        }
    }

    /// Record the thinking→content boundary once, the first time content (a text
    /// delta or the consolidated assistant message) follows the thinking phase.
    /// Idempotent: later calls are no-ops so the duration stays the thinking time.
    pub fn capture_thinking_done(&mut self) {
        if self.thinking_ms.is_none() {
            self.thinking_ms = Some(self.opened_at.elapsed().as_millis() as i64);
        }
    }

    /// Record the backend wall-clock start of the thinking phase, once, the first
    /// time a non-zero thinking-token count is observed for this stream. Idempotent
    /// and gated on `tokens > 0` so the anchor lands on the first real thinking
    /// count and never moves afterward.
    pub fn record_thinking_started(&mut self, tokens: Option<u32>) {
        if self.thinking_started_at_ms.is_none() && tokens.map(|t| t > 0).unwrap_or(false) {
            self.thinking_started_at_ms = Some(chrono::Utc::now().timestamp_millis());
        }
    }

    pub fn start_tool_write(&mut self, id: Option<String>, name: String) {
        self.tool_write = Some(StreamingToolWrite {
            id,
            name,
            input_chars: 0,
            status: "constructing".to_string(),
            input_preview: Some(String::new()),
        });
    }

    pub fn push_tool_input_delta(&mut self, partial_json: &str) {
        if let Some(tool_write) = self.tool_write.as_mut() {
            tool_write.input_chars += partial_json.chars().count() as i32;
            if let Some(preview) = tool_write.input_preview.as_mut() {
                let remaining =
                    TOOL_INPUT_PREVIEW_MAX_CHARS.saturating_sub(preview.chars().count());
                if remaining > 0 {
                    preview.extend(partial_json.chars().take(remaining));
                }
            }
        }
    }
}

pub(crate) fn trace_claude_first_nonempty_delta(
    host: &ClaudeRuntime,
    run_id: &str,
    session_id: Option<&str>,
    mode: &str,
    bytes: usize,
) {
    let current_turn = host.process_state.get_current_turn_id(run_id);
    let turn_gate = current_turn.as_deref().unwrap_or("untracked");
    if crate::timing::mark_first(format!("claude-nonempty:{run_id}:{turn_gate}")) {
        // This marks the first visible prose/thinking payload, not first-token latency:
        // a tool-first turn may not reach it until after one or more tools finish.
        let mut timing = crate::timing::ResumeTimingEvent::new("claude_first_nonempty_delta");
        timing.run_id = Some(run_id);
        timing.session_id = session_id;
        timing.turn_id = current_turn.as_deref();
        timing.mode = Some(mode);
        timing.bytes = Some(bytes);
        timing.emit();
    }
}

pub(crate) fn trace_claude_first_any_delta(
    host: &ClaudeRuntime,
    run_id: &str,
    session_id: Option<&str>,
    mode: &str,
    bytes: usize,
) {
    let current_turn = host.process_state.get_current_turn_id(run_id);
    let turn_gate = current_turn.as_deref().unwrap_or("untracked");
    if crate::timing::mark_first(format!("claude-any-delta:{run_id}:{turn_gate}")) {
        let mut timing = crate::timing::ResumeTimingEvent::new("claude_first_any_delta");
        timing.run_id = Some(run_id);
        timing.session_id = session_id;
        timing.turn_id = current_turn.as_deref();
        timing.mode = Some(mode);
        timing.bytes = Some(bytes);
        timing.emit();
    }
}

/// Emit a true-delta `streaming-update`: only the newly-produced scalar tail,
/// plus the current absolute lengths so the frontend can detect gaps and
/// self-heal against the DB snapshot.
#[allow(clippy::too_many_arguments)]
pub fn emit_streaming_delta(
    host: &ClaudeRuntime,
    run_id: &str,
    event_id: &str,
    delta: &EmitDelta,
    thinking_tokens: Option<u32>,
    thinking_started_at: Option<i64>,
    thinking_ms: Option<i64>,
    tool_write: Option<&StreamingToolWrite>,
) {
    if crate::timing::mark_first(format!("claude-streaming-emit:{event_id}")) {
        let mut event = crate::timing::ResumeTimingEvent::new("claude_first_streaming_emit");
        let current_turn = host.process_state.get_current_turn_id(run_id);
        event.run_id = Some(run_id);
        event.turn_id = current_turn.as_deref();
        event.stream_id = Some(event_id);
        event.bytes = Some(
            delta.content_delta.as_ref().map_or(0, String::len)
                + delta.thinking_delta.as_ref().map_or(0, String::len),
        );
        event.emit();
    }
    let current_turn = host.process_state.get_current_turn_id(run_id);
    crate::transcripts::stream_store::trace_first_frontend_delta_emit(
        run_id,
        None,
        current_turn.as_deref(),
        event_id,
        delta,
    );
    let _ = host.emitter.emit(
        "streaming-update",
        serde_json::json!({
            "run_id": run_id,
            "event_id": event_id,
            "content_delta": delta.content_delta,
            "content_len": delta.content_len,
            "thinking_delta": delta.thinking_delta,
            "thinking_len": delta.thinking_len,
            "thinking_tokens": thinking_tokens,
            "thinking_started_at": thinking_started_at,
            "thinking_ms": thinking_ms,
            "tool_write": tool_write,
        }),
    );
}

/// Evidence belonging to this turn, reset when the next user input arrives.
#[derive(Default)]
pub struct ClaudeResultContext {
    pub pending_depletion: Option<ProviderDepletion>,
    pub block_recorded: bool,
    pub terminal_interrupt_ack_pending: bool,
    pub blocking_reset: Option<i64>,
    /// Some tool_use of this stream has no tool_result yet.
    pub tool_in_flight: bool,
}

/// The production Result decision boundary. The clock is supplied so recorded
/// provider payloads can exercise the same routing without waiting on a provider.
pub fn dispatch_claude_result(
    host: &dyn ClaudeHost,
    is_error: bool,
    data: &serde_json::Value,
    context: ClaudeResultContext,
    now: i64,
) -> bool {
    // Every structured depletion holds: a terminal 429, and a terminal error
    // after the CLI's synthetic credits message, whose result text is generic.
    if let Some(depletion) = terminal_provider_depletion(is_error, data, context.pending_depletion)
    {
        return host.provider_limit_result(ProviderLimitReport {
            result_text: data.get("result").and_then(serde_json::Value::as_str),
            depletion,
            block_recorded: context.block_recorded,
            blocking_reset: context.blocking_reset,
            tool_in_flight: context.tool_in_flight,
            now,
        });
    }
    host.complete(
        claude_result_outcome(is_error, data),
        ClaudeTurnContext {
            terminal_tool_completed: false,
            terminal_interrupt_ack_pending: context.terminal_interrupt_ack_pending,
        },
    );
    false
}

pub(crate) fn claude_result_outcome(is_error: bool, data: &serde_json::Value) -> ClaudeTurnOutcome {
    if is_error {
        ClaudeTurnOutcome::Failed {
            message: terminal_result_error_message(data).to_string(),
        }
    } else {
        ClaudeTurnOutcome::Completed
    }
}

pub(crate) fn claude_context_used_tokens(usage: &Usage) -> i64 {
    usage.input_tokens as i64
        + usage.cache_creation_input_tokens.unwrap_or(0) as i64
        + usage.cache_read_input_tokens.unwrap_or(0) as i64
        + usage.output_tokens as i64
}

/// Build a context-token snapshot from a single inference's usage. The Claude
/// CLI's `message_delta` usage carries the full per-inference occupancy
/// (prompt + cache + output), so the summed figure is the live context fill for
/// the current model call. The window is the one the CLI reported, else the
/// one for the model it resolved at init, else for `model`, the effective
/// model ([`claude_session_window`]). Pure (no orchestrator), so it can be
/// unit-tested.
pub(crate) fn build_claude_context_snapshot(
    run_id: &str,
    session_id: Option<&str>,
    model: Option<String>,
    resolved_model: Option<&str>,
    reported_window: Option<i64>,
    usage: &Usage,
) -> ContextTokenState {
    let context_window = claude_session_window(reported_window, resolved_model, model.as_deref());
    ContextTokenState {
        run_id: run_id.to_string(),
        session_id: session_id.map(str::to_string),
        backend: "claude".to_string(),
        model,
        used_tokens: claude_context_used_tokens(usage),
        context_window,
        auto_compact_limit: None,
        reasoning_tokens: usage
            .output_tokens_details
            .as_ref()
            .and_then(|details| details.thinking_tokens)
            .map(|tokens| tokens as i64),
        last_output_tokens: Some(usage.output_tokens as i64),
        captured_at: chrono::Utc::now().timestamp(),
    }
}

/// Resolve the run's model and push a context-token snapshot to the frontend.
/// Shared by the live `message_delta` path (fires once per inference within a
/// turn) and the end-of-turn `Result` path. Key on the durable `session_id`
/// variable so resume/fork runs stay attributed to the same gauge.
pub(crate) fn emit_claude_context_snapshot(
    host: &ClaudeRuntime,
    run_id: &str,
    session_id: Option<&str>,
    usage: &Usage,
) {
    let model = host.process_state.effective_model(run_id);
    let (resolved, reported_window) = host.process_state.reported_model_and_window(run_id);
    host.host.context_snapshot(build_claude_context_snapshot(
        run_id,
        session_id,
        model,
        resolved.as_deref(),
        reported_window,
        usage,
    ));
}

/// Keep the context window a turn's `result` reports for the session's model
/// (`modelUsage[model].contextWindow`): the CLI's own answer, which depends on
/// its version as much as on the model, outranks any table.
fn record_claude_reported_window(host: &ClaudeRuntime, run_id: &str, result: &serde_json::Value) {
    let (resolved, _) = host.process_state.reported_model_and_window(run_id);
    if let Some(window) = result
        .get("modelUsage")
        .and_then(|usage| claude_reported_context_window(usage, resolved.as_deref()))
    {
        host.process_state
            .set_reported_context_window(run_id, window);
    }
}

/// Build a coarse usage snapshot from a `rate_limit_event`. The CLI reports a
/// status + reset windows, not a precise usage percent, so this surfaces one
/// window carrying the reset time and a status-derived remaining figure
/// (allowed -> 100%, blocking -> 0%). Coarse, but it gives the live usage panel
/// a real signal where Claude previously had none. `source` distinguishes it
/// from the precise PTY-probe snapshot.
pub(crate) fn claude_rate_limit_snapshot(info: &RateLimitInfo) -> ProviderUsageSnapshot {
    let remaining_percent = if info.is_blocking() { 0.0 } else { 100.0 };
    let status = if info.status.is_empty() {
        "unknown"
    } else {
        info.status.as_str()
    };
    let window = ProviderUsageWindow {
        id: info
            .rate_limit_type
            .clone()
            .unwrap_or_else(|| "claude".to_string()),
        label: format!("Rate limit ({status})"),
        scope: ProviderUsageScope::RollingWindow,
        scope_target: None,
        used_percent: 100.0 - remaining_percent,
        remaining_percent,
        resets_at: info.resets_at,
        reset_at_text: None,
        window_duration_mins: None,
    };
    ProviderUsageSnapshot {
        backend: "claude".to_string(),
        source: "claude_rate_limit_event".to_string(),
        captured_at: chrono::Utc::now().timestamp(),
        windows: vec![window],
        credits: None,
        reset_credits: None,
        error: None,
        error_kind: None,
        unsupported_reason: None,
        raw: serde_json::to_value(info).ok(),
        model_breakdown: None,
    }
}

/// Map a stderr line to the typed backend failure it proves, if any. The CLI's
/// wording is recognized by the wire crate; this names what it means for the
/// session: a `--resume` handle the CLI could not find, or a fresh
/// `--session-id` the CLI had already created a conversation under.
pub fn classify_stderr_failure(line: &str) -> Option<BackendFailure> {
    if stderr_reports_unknown_session(line) {
        Some(BackendFailure::SessionUnresolvable)
    } else if stderr_reports_session_id_in_use(line) {
        Some(BackendFailure::SessionIdInUse)
    } else {
        None
    }
}

/// The stderr drain for a spawned agent process plus any failure it classified.
///
/// [`StderrWatch::settle`] joins the drain before reading, so a diagnosis the
/// process printed on its way out is never lost to a race with stdout EOF.
/// Joining is bounded rather than polled: the read loop that just ended proves
/// the child closed stdout, and stderr is closed by the same process exit, so
/// the drain is already finishing. A timeout here would either race the
/// classification or tax every crash path with a fixed wait.
pub struct StderrWatch {
    join: thread::JoinHandle<()>,
    failure: Arc<Mutex<Option<BackendFailure>>>,
    mcp_diagnostics: Arc<Mutex<Vec<String>>>,
}

impl StderrWatch {
    /// Drain `stderr` on a background thread, logging every line as today and
    /// recording the first line that classifies as a typed backend failure.
    pub fn spawn(stderr: Box<dyn BufRead + Send>) -> Self {
        let failure = Arc::new(Mutex::new(None));
        let thread_failure = failure.clone();
        let mcp_diagnostics = Arc::new(Mutex::new(Vec::new()));
        let thread_mcp_diagnostics = mcp_diagnostics.clone();
        let join = thread::spawn(move || {
            log::debug!("stderr_thread: started");
            for line in stderr.lines().map_while(Result::ok) {
                log::error!("claude stderr: {}", line);
                if let Some(classified) = classify_stderr_failure(&line) {
                    if let Ok(mut slot) = thread_failure.lock() {
                        slot.get_or_insert(classified);
                    }
                }
                if line.to_ascii_lowercase().contains("mcp") {
                    let sanitized = sanitize_mcp_diagnostic(&line);
                    if let Ok(mut diagnostics) = thread_mcp_diagnostics.lock() {
                        if diagnostics.len() < 5 {
                            diagnostics.push(sanitized.chars().take(500).collect());
                        }
                    }
                }
            }
            log::debug!("stderr_thread: ended");
        });
        Self {
            join,
            failure,
            mcp_diagnostics,
        }
    }

    /// Join the drain, then report whatever it classified.
    pub fn settle(self) -> Option<BackendFailure> {
        self.settle_with_diagnostics().0
    }

    pub fn settle_with_diagnostics(self) -> (Option<BackendFailure>, Vec<String>) {
        if self.join.join().is_err() {
            log::warn!("stderr_thread panicked; treating its diagnosis as absent");
        }
        let failure = self.failure.lock().ok().and_then(|slot| *slot);
        let diagnostics = self
            .mcp_diagnostics
            .lock()
            .map(|diagnostics| diagnostics.clone())
            .unwrap_or_default();
        (failure, diagnostics)
    }
}

pub(crate) fn finalize_streaming_message(
    host: &ClaudeRuntime,
    db: &Arc<LocalDb>,
    run_id: &str,
    // The finalize emit is owned by `finalize_stream_emit` (it scopes from the
    // finalized stream's own run/session), so this path no longer reads the
    // passed session id; kept in the signature for call-site symmetry.
    _session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    mut final_event: Option<TranscriptEvent>,
    counts: TokenCounts,
) {
    let Some(mut state) = streaming_state.take() else {
        return;
    };
    // Flush buffered chunks before finalize: finalize_stream reconstructs the
    // final content from the chunk rows, so unflushed tokens would be lost.
    if !state.acc.pending_is_empty() {
        match append_chunks(
            db.clone(),
            &state.stream_id,
            state.version,
            &state.acc.take_pending(),
        ) {
            Ok(result) => state.version = result.version,
            Err(error) => log::warn!(
                "Failed to flush stream {} before finalize for run {}: {}",
                state.stream_id,
                run_id,
                error
            ),
        }
    }
    // Force the live slot to catch up to the full content before the streaming
    // snapshot is swapped for the final assistant event.
    if state.acc.has_unemitted() {
        let delta = state.acc.take_emit_delta();
        emit_streaming_delta(
            host,
            run_id,
            &state.stream_id,
            &delta,
            None,
            state.thinking_started_at_ms,
            state.thinking_ms,
            state.tool_write.as_ref(),
        );
    }
    // Stamp reasoning duration on the finalized event when this message carried
    // thinking. The duration is the thinking phase only — stream open to the
    // thinking→content boundary captured during streaming — not the whole turn.
    // Fall back to elapsed-at-finalize only when the boundary was never seen
    // (e.g. a thinking-only message with no following content). Non-reasoning
    // messages leave it unset so they carry no duration.
    if counts.thinking.map(|tokens| tokens > 0).unwrap_or(false) {
        if let Some(event) = final_event.as_mut() {
            let ms = state
                .thinking_ms
                .unwrap_or_else(|| state.opened_at.elapsed().as_millis() as i64);
            event.thinking_ms = Some(ms);
        }
    }
    match host.host.transcripts().finalize_stream(
        &state.stream_id,
        state.version,
        final_event,
        counts,
    ) {
        Ok(_) => {}
        Err(error) => log::error!(
            "Failed to finalize stream {} for run {}: {}",
            state.stream_id,
            run_id,
            error
        ),
    }
}

/// Fold a newly-arrived consolidated assistant event into one already parked
/// for deferred finalization. claude-code can emit more than one consolidated
/// `assistant` event under a single stream before the trailing `message_delta`
/// (a batched multi-tool turn); parking is a single slot, so without merging
/// the earlier event's tool calls are lost. Dedup by tool id so this is correct
/// whether the events are cumulative or disjoint.
pub fn merge_pending_assistant(parked: &mut TranscriptEvent, incoming: &TranscriptEvent) {
    // tool_uses: append, skipping ids already present.
    let mut uses = parked.tool_uses.take().unwrap_or_default();
    if let Some(incoming_uses) = incoming.tool_uses.as_ref() {
        for u in incoming_uses {
            if !uses.iter().any(|existing| existing.id == u.id) {
                uses.push(u.clone());
            }
        }
    }
    // content: keep parked unless empty; append new only if distinct (guards the
    // cumulative case where incoming repeats the parked text).
    match (&parked.content, &incoming.content) {
        (None, Some(c)) => parked.content = Some(c.clone()),
        (Some(p), Some(c)) if c != p && !p.contains(c.as_str()) && !c.contains(p.as_str()) => {
            parked.content = Some(format!("{p}\n{c}"));
        }
        (Some(p), Some(c)) if c.contains(p.as_str()) => parked.content = Some(c.clone()),
        _ => {}
    }
    // thinking: keep parked unless empty. `Some("")` counts as empty -- a parked
    // message carrying only private reasoning has no prose to protect, and
    // treating it as present would swallow the visible prose arriving behind it.
    if parked.thinking.as_deref().unwrap_or("").is_empty() {
        parked.thinking = incoming.thinking.clone();
    }
    // Recompute legacy single-tool fields to match from_claude_event semantics.
    if uses.len() == 1 {
        parked.tool_name = Some(uses[0].name.clone());
        parked.tool_input = Some(uses[0].input.clone());
    } else {
        parked.tool_name = None;
        parked.tool_input = None;
    }
    parked.tool_uses = if uses.is_empty() { None } else { Some(uses) };
}

/// Commit a genuine, fully-emitted assistant message that is still awaiting
/// deferred finalization, before a suppression path would discard the in-flight
/// stream (CAIRN-1611).
///
/// The reader parks the consolidated `assistant` event in
/// `pending_final_assistant_event` until its trailing `message_delta` usage
/// arrives (the order Opus-class models emit). A terminal-tool or host-interrupt
/// guard must commit that parked message rather than null it: it is a real
/// transcript event the model produced, never transport noise.
///
/// Normally the pending message is paired with its still-open streaming row and
/// is finalized in place. If that pairing was already broken (the stream row is
/// gone but a pending event remains), the message is inserted directly so it is
/// never silently dropped.
pub fn flush_pending_assistant_before_suppress(
    host: &ClaudeRuntime,
    db: &Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    pending_final_assistant_event: &mut Option<TranscriptEvent>,
    pending_delta_usage: Option<&Usage>,
) {
    let Some(pending) = pending_final_assistant_event.take() else {
        return;
    };
    let counts = TokenCounts::from_optional_usage(pending_delta_usage);
    if streaming_state.is_some() {
        finalize_streaming_message(
            host,
            db,
            run_id,
            session_id,
            streaming_state,
            Some(pending),
            counts,
        );
        return;
    }
    // Defensive: the streaming row is already gone, so finalize_streaming_message
    // would early-return and silently drop this message. Insert it directly so a
    // genuine assistant event is never lost.
    let now = chrono::Utc::now().timestamp() as i32;
    let data = serde_json::to_string(&pending).unwrap_or_default();
    let current_turn = host.process_state.get_current_turn_id(run_id);
    match host.host.transcripts().insert_event(EventInsert {
        id: Uuid::new_v4().to_string(),
        run_id: run_id.to_string(),
        session_id: session_id.map(str::to_string),
        timestamp: now,
        event_type: pending.event_type.clone(),
        data,
        parent_tool_use_id: pending.parent_tool_use_id.clone(),
        created_at: now,
        input_tokens: counts.input,
        cache_read_tokens: counts.cache_read,
        cache_create_tokens: counts.cache_create,
        output_tokens: counts.output,
        thinking_tokens: counts.thinking,
        turn_id: current_turn,
        cost_usd: None,
    }) {
        Ok(true) => {}
        Ok(false) => log::error!(
            "Pending assistant event for run {} was not stored before suppression (duplicate id)",
            &run_id[..run_id.len().min(8)]
        ),
        Err(error) => log::error!(
            "Failed to insert pending assistant for run {} before suppression: {}",
            &run_id[..run_id.len().min(8)],
            error
        ),
    }
}

impl ClaudeRuntime {
    /// Reader thread: reads Claude's stream-json stdout and stores events in the database.
    #[allow(clippy::too_many_arguments)]
    pub fn reader_thread(
        host: &ClaudeRuntime,
        run_id: &str,
        session_id: Option<String>,
        confirm_backend_id_after_init: bool,
        stdout: Box<dyn std::io::BufRead + Send>,
        mut stderr_watch: Option<StderrWatch>,
        run_db: Arc<LocalDb>,
    ) {
        log::debug!("reader_thread: started");
        let thread_start = std::time::Instant::now();
        log::debug!("[PROFILE] Reader thread started");
        let mut lines_read: u64 = 0;
        let mut first_event_logged = false;
        let mut first_stdout_logged = false;
        let mut parse_metrics = crate::agent_process::stream::ParseMetrics::default();
        let mut read_failures: usize = 0;
        let mut boundary_checker = TurnBoundaryChecker::new();
        let mut terminal_tool_suspended = false;
        // Set when we send the terminal-tool boundary interrupt to end a work
        // turn; consumed by the next `Result{is_error}` (the interrupt ack) so
        // that stray ack is not misread as a crash of the turn that has since
        // resumed on this warm process (the memory-review turn). See CAIRN-1576.
        let mut terminal_interrupt_ack_pending = false;
        let mut streaming_state: Option<StreamingState> = None;
        let mut last_thinking_tokens: Option<u32> = None;
        let mut pending_thinking_tokens: Option<u32> = None;
        let mut pending_delta_usage: Option<Usage> = None;
        let mut pending_final_assistant_event: Option<TranscriptEvent> = None;
        let mut backend_id_confirmed = !confirm_backend_id_after_init;
        let mut pending_tool_ids = HashSet::<String>::new();
        let progress_watchdog = Arc::new(Mutex::new(ClaudeTurnProgressWatchdog::new(
            CLAUDE_TURN_NO_PROGRESS_TIMEOUT,
        )));
        let durable_watchdog = Arc::new(Mutex::new(None));
        if let Ok(mut watchdog) = progress_watchdog.lock() {
            let current_turn = host.process_state.get_current_turn_id(run_id);
            watchdog.observe_turn(current_turn.as_deref(), Instant::now());
            if let (Some(session_id), Some(turn_id)) =
                (session_id.as_deref(), current_turn.as_deref())
            {
                match arm_claude_watchdog_lease(
                    run_db.clone(),
                    run_id,
                    session_id,
                    turn_id,
                    &host.boot_id,
                ) {
                    Ok(identity) => *durable_watchdog.lock().unwrap() = Some(identity),
                    Err(error) => log::error!(
                        "Failed to initially arm durable Claude watchdog for run {run_id}: {error}; the watchdog worker will retry"
                    ),
                }
            }
        }
        let watchdog_alive = Arc::new(AtomicBool::new(true));
        {
            let watchdog = progress_watchdog.clone();
            let durable_watchdog = durable_watchdog.clone();
            let alive = watchdog_alive.clone();
            let host = host.clone();
            let run_id = run_id.to_string();
            let session_id = session_id.clone();
            let watchdog_run_db = run_db.clone();
            thread::spawn(move || {
                let mut last_durable_heartbeat = Instant::now();
                while alive.load(Ordering::Acquire) {
                    thread::sleep(CLAUDE_TURN_WATCHDOG_POLL);
                    if !alive.load(Ordering::Acquire) {
                        break;
                    }
                    if last_durable_heartbeat.elapsed() >= CLAUDE_TURN_WATCHDOG_HEARTBEAT {
                        if let Some(identity) =
                            durable_watchdog.lock().ok().and_then(|guard| guard.clone())
                        {
                            heartbeat_claude_watchdog_lease(watchdog_run_db.clone(), &identity);
                        }
                        last_durable_heartbeat = Instant::now();
                    }
                    let current_turn = host.process_state.get_current_turn_id(&run_id);
                    let expired_turn = watchdog.lock().ok().and_then(|mut guard| {
                        let now = Instant::now();
                        let turn_changed = guard.observe_turn(current_turn.as_deref(), now);
                        let durable_missing = durable_watchdog
                            .lock()
                            .map(|guard| guard.is_none())
                            .unwrap_or(true);
                        if turn_changed || durable_missing {
                            if let (Some(session_id), Some(turn_id)) =
                                (session_id.as_deref(), current_turn.as_deref())
                            {
                                match replace_claude_watchdog_lease(
                                    watchdog_run_db.clone(),
                                    &durable_watchdog,
                                    &run_id,
                                    session_id,
                                    turn_id,
                                    &host.boot_id,
                                ) {
                                    Ok(()) => {}
                                    Err(error) => log::error!(
                                        "Failed to durably arm Claude watchdog for run {run_id}: {error}"
                                    ),
                                }
                            }
                        }
                        guard.expired(now)
                    });
                    let Some(turn_id) = expired_turn else {
                        continue;
                    };
                    log::error!(
                        "Claude turn {turn_id} for run {} made no forward progress for {:?}; aborting silent provider process",
                        &run_id[..run_id.len().min(8)],
                        CLAUDE_TURN_NO_PROGRESS_TIMEOUT
                    );
                    let diagnosis = format!(
                        "Claude produced no provider progress for {} seconds after the turn started. Cairn detected the wedged process and is attempting recovery.",
                        CLAUDE_TURN_NO_PROGRESS_TIMEOUT.as_secs()
                    );
                    host.host.error(&diagnosis);
                    let identity = durable_watchdog.lock().ok().and_then(|guard| guard.clone());
                    let Some(identity) = identity else {
                        host.host.error("Cairn could not recover the silent Claude turn because its durable watchdog lease was unavailable.",);
                        break;
                    };
                    let wall_now = chrono::Utc::now().timestamp();
                    let expired_identity = identity.clone();
                    let expired_db = watchdog_run_db.clone();
                    match run_backend_db(CLAUDE_BACKEND_NAME, async move {
                        crate::runs::watchdog_ledger::expire_watchdog(
                            &expired_db,
                            &expired_identity,
                            wall_now,
                        )
                        .await
                        .map_err(|error| error.to_string())
                    }) {
                        Ok(true) => {
                            record_claude_watchdog_lifecycle(
                                watchdog_run_db.clone(),
                                &identity,
                                crate::runs::watchdog_ledger::WatchdogLifecycleKind::Expired,
                                None,
                                "no_provider_progress",
                            );
                            if let Err(error) = host.host.recover_watchdog(&identity, wall_now) {
                                log::error!(
                                    "Durable Claude watchdog recovery failed for turn {turn_id}: {error}"
                                );
                            }
                        }
                        Ok(false) => log::error!(
                            "Claude watchdog expiry lost durable lease for turn {turn_id}"
                        ),
                        Err(error) => log::error!(
                            "Failed to persist Claude watchdog expiry for turn {turn_id}: {error}"
                        ),
                    }
                    break;
                }
            });
        }

        // Grab the terminal_tool_called flag for this process.
        // When set, we stop storing events — the session is complete.
        let terminal_tool_flag = host
            .process_state
            .processes
            .lock()
            .ok()
            .and_then(|p| p.get(run_id).map(|proc| proc.terminal_tool_called.clone()))
            .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));

        // EOF-classification signals, observed purely within this reader thread
        // (set in the loop, read at EOF in the same thread) — plain locals, no
        // atomics needed. The stream evidence makes a Result-then-EOF a clean
        // completion regardless of occupancy, and its rate-limit flags drive the
        // recoverable-exit path and the crash diagnostic. Whether the host
        // persisted a quarantine is the host's answer, not stream evidence.
        let mut evidence = StreamEvidence::default();
        let mut persisted_rate_limit_quarantine = false;
        // The CLI reports credit exhaustion as a structured assistant error and
        // only then a generic error result, so the classification is carried to
        // the turn's Result. A new user message starts a new turn.
        let mut pending_depletion: Option<ProviderDepletion> = None;
        let mut depletion_handled = false;
        let mut turn_block_recorded = false;
        let mut blocking_reset = None;

        log::trace!("reader_thread: about to read lines");
        for line_result in stdout.lines() {
            let line = match line_result {
                Ok(l) => {
                    lines_read += 1;
                    if !first_stdout_logged {
                        let mut event =
                            crate::timing::ResumeTimingEvent::new("claude_first_stdout")
                                .elapsed(thread_start);
                        event.run_id = Some(run_id);
                        event.session_id = session_id.as_deref();
                        event.bytes = Some(l.len());
                        event.emit();
                        first_stdout_logged = true;
                    }
                    if !l.contains("\"type\":\"stream_event\"") {
                        log::trace!(
                            "reader_thread: line {}: {}",
                            lines_read,
                            &l[..l.len().min(100)]
                        );
                    }
                    l
                }
                Err(e) => {
                    read_failures += 1;
                    log::debug!("reader_thread: error reading line: {}", e);
                    log::error!("Error reading line: {}", e);
                    continue;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            let parse_started = std::time::Instant::now();
            match parse_metrics.parse(&line) {
                Ok((event, raw)) => {
                    if !first_event_logged {
                        let mut timing =
                            crate::timing::ResumeTimingEvent::new("claude_first_parsed_event")
                                .elapsed(parse_started);
                        timing.run_id = Some(run_id);
                        timing.session_id = session_id.as_deref();
                        timing.count = Some(parse_metrics.failures);
                        timing.emit();
                        first_event_logged = true;
                    }
                    evidence.observe(&event);
                    if matches!(&event, ClaudeEvent::User { .. }) {
                        pending_depletion = None;
                        depletion_handled = false;
                        turn_block_recorded = false;
                        blocking_reset = None;
                    }
                    if let Some(depletion) = event.provider_depletion() {
                        pending_depletion = Some(depletion);
                    }
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        let now = Instant::now();
                        let current_turn = host.process_state.get_current_turn_id(run_id);
                        let changed = watchdog.active_turn_id.as_deref() != current_turn.as_deref();
                        watchdog.observe_turn(current_turn.as_deref(), now);
                        watchdog.record_forward_progress(now);
                        if changed {
                            if let (Some(session_id), Some(turn_id)) =
                                (session_id.as_deref(), current_turn.as_deref())
                            {
                                if let Err(error) = replace_claude_watchdog_lease(
                                    run_db.clone(),
                                    &durable_watchdog,
                                    run_id,
                                    session_id,
                                    turn_id,
                                    &host.boot_id,
                                ) {
                                    log::error!(
                                        "Failed to durably replace Claude watchdog for run {run_id}: {error}"
                                    );
                                }
                            }
                        } else if let Some(identity) =
                            durable_watchdog.lock().ok().and_then(|guard| guard.clone())
                        {
                            update_claude_watchdog_lease(
                                run_db.clone(),
                                &identity,
                                crate::runs::watchdog_ledger::WatchdogPhase::ProviderProgress,
                            );
                        }
                    }
                    // A terminal Result seen at any point means the turn completed
                    // (recorded in `evidence` above, before any branch, so a later
                    // EOF finalizes Exited).
                    if event.is_terminal() {
                        // A schema-constrained call returns its result through the
                        // CLI's StructuredOutput tool, surfaced on the terminal
                        // result event as `structured_output`. Store it as the
                        // call's return artifact server-side (CAIRN-2505) BEFORE the
                        // run finalizes below, so the parent resumes to a
                        // schema-valid artifact instead of relying on the model to
                        // have written `cairn:~/return`. Best-effort + idempotent.
                        if let Some(structured) = raw
                            .get("structured_output")
                            .filter(|v| !v.is_null())
                            .cloned()
                        {
                            host.host.capture_output(structured);
                        }
                    }

                    if let ClaudeEvent::System { subtype, data, .. } = &event {
                        if subtype == "init" && validate_claude_init(data, &[]).is_err() {
                            // Close the child first so both output pipes reach
                            // EOF, then join stderr. Snapshotting the parallel
                            // drain here would race the actionable MCP error.
                            host.host.stop_process();
                            let diagnostics = stderr_watch
                                .take()
                                .map(StderrWatch::settle_with_diagnostics)
                                .map(|(_, diagnostics)| diagnostics)
                                .unwrap_or_default();
                            let error = validate_claude_init(data, &diagnostics)
                                .expect_err("missing Cairn tools remains invalid");
                            log::error!(
                                "Rejecting Claude launch without Cairn MCP tools for run {}: {}",
                                &run_id[..run_id.len().min(8)],
                                error
                            );
                            host.host.startup_rejected(&error);
                            break;
                        }
                        if subtype == "init" {
                            // A valid init is direct evidence that Claude Code
                            // successfully launched on this account. Re-probe now
                            // so routing and the provider card converge without
                            // an operator refresh.
                            host.host.initialized();
                            if let Some(model) = data
                                .get("model")
                                .and_then(serde_json::Value::as_str)
                                .filter(|model| !model.is_empty())
                            {
                                host.process_state.set_reported_model(run_id, model);
                            }
                        }
                    }

                    // The facet's one rule for credential events; the host
                    // routes both into the credential coordinator.
                    match cairn_models::session::events::claude::credential_event(&event) {
                        Some(SessionEvent::CredentialRejected) => {
                            log::info!(
                                "Claude run {} was refused with a 401 and is retrying",
                                &run_id[..run_id.len().min(8)]
                            );
                            host.host.credential_rejected();
                        }
                        Some(SessionEvent::CredentialAcknowledged { generation }) => {
                            host.host.credential_acknowledged(generation);
                        }
                        _ => {}
                    }

                    if !backend_id_confirmed {
                        if let ClaudeEvent::System {
                            subtype,
                            session_id: backend_session_id,
                            ..
                        } = &event
                        {
                            if subtype == "init" {
                                if let Some(ref sid) = session_id {
                                    if let Err(error) = set_session_backend_id(
                                        CLAUDE_BACKEND_NAME,
                                        &run_db,
                                        sid,
                                        backend_session_id,
                                    ) {
                                        log::warn!(
                                            "Failed to confirm Claude backend_id for session {}: {}",
                                            &sid[..sid.len().min(8)],
                                            error
                                        );
                                    } else {
                                        host.process_state
                                            .set_backend_session_id(run_id, backend_session_id);
                                        backend_id_confirmed = true;
                                    }
                                } else {
                                    backend_id_confirmed = true;
                                }
                            }
                        }
                    }

                    // The `init` event reports the tools the CLI actually
                    // declared to the model. Under `--tools ""` that is only
                    // MCP tools; anything else means the CLI broke the
                    // contract, and the tripwire logs it.
                    if let ClaudeEvent::System { subtype, data, .. } = &event {
                        if subtype == "init" {
                            let declared: Vec<String> = data
                                .get("tools")
                                .and_then(|t| t.as_array())
                                .map(|tools| {
                                    tools
                                        .iter()
                                        .filter_map(|t| t.as_str().map(str::to_string))
                                        .collect()
                                })
                                .unwrap_or_default();
                            crate::agent_process::toolkits::report_declared_surface(
                                run_id, &declared,
                            );
                        }
                    }

                    // Handle control responses
                    if let ClaudeEvent::ControlResponse {
                        request_id,
                        response,
                    } = &event
                    {
                        match response {
                            ControlResponseInner::Success { .. } => {
                                log::info!(
                                    "Control request {} succeeded for run {}",
                                    &request_id[..request_id.len().min(8)],
                                    &run_id[..run_id.len().min(8)]
                                );
                            }
                            ControlResponseInner::Error { message } => {
                                log::warn!(
                                    "Control request {} failed for run {}: {:?}",
                                    &request_id[..request_id.len().min(8)],
                                    &run_id[..run_id.len().min(8)],
                                    message
                                );
                            }
                        }
                        continue;
                    }

                    // Account rate-limit update: log it (legibility) and surface
                    // it live to the usage panel. Not stored as a transcript
                    // event. A blocking status arms the recoverable-exit path.
                    if let ClaudeEvent::RateLimitEvent { rate_limit_info } = &event {
                        log::info!(
                            "Rate-limit event for run {}: status={} type={:?} resets_at={:?} overage_resets_at={:?}",
                            &run_id[..run_id.len().min(8)],
                            rate_limit_info.status,
                            rate_limit_info.rate_limit_type,
                            rate_limit_info.resets_at,
                            rate_limit_info.overage_resets_at,
                        );
                        if rate_limit_info.is_blocking() {
                            blocking_reset = rate_limit_info
                                .resets_at
                                .or(rate_limit_info.overage_resets_at)
                                .or(blocking_reset);
                        }
                        let snapshot = claude_rate_limit_snapshot(rate_limit_info);
                        if host.host.rate_limit(snapshot, Some(rate_limit_info)) {
                            persisted_rate_limit_quarantine = true;
                            turn_block_recorded = true;
                        }
                        continue;
                    }
                    if let ClaudeEvent::System { subtype, data, .. } = &event {
                        if subtype == "thinking_tokens" {
                            if let Some(tokens) = parse_thinking_tokens_estimate(data) {
                                last_thinking_tokens = Some(tokens);
                                if let Some(ref mut state) = streaming_state {
                                    state.record_thinking_started(last_thinking_tokens);
                                    let delta = state.acc.take_emit_delta();
                                    emit_streaming_delta(
                                        host,
                                        run_id,
                                        &state.stream_id,
                                        &delta,
                                        last_thinking_tokens,
                                        state.thinking_started_at_ms,
                                        state.thinking_ms,
                                        state.tool_write.as_ref(),
                                    );
                                } else {
                                    pending_thinking_tokens = Some(tokens);
                                }
                            }
                            continue;
                        }
                    }
                    // Unmodeled event type: ignore cleanly (no warn, no store).
                    if matches!(&event, ClaudeEvent::Unknown) {
                        continue;
                    }

                    // Handle streaming events (skip if session ended via terminal tool)
                    if let ClaudeEvent::StreamEvent {
                        inner,
                        parent_tool_use_id,
                        ..
                    } = &event
                    {
                        if terminal_tool_flag.load(std::sync::atomic::Ordering::Acquire) {
                            // Commit any assistant message parked for deferred
                            // finalization before discarding the post-boundary
                            // stream (CAIRN-1611). The `message_delta` that would
                            // normally finalize it is one of the events this guard
                            // skips, so flush here or the tool-call event is lost.
                            flush_pending_assistant_before_suppress(
                                host,
                                &run_db,
                                run_id,
                                session_id.as_deref(),
                                &mut streaming_state,
                                &mut pending_final_assistant_event,
                                pending_delta_usage.as_ref(),
                            );
                            continue;
                        }
                        match inner {
                            StreamEventInner::MessageStart { message } => {
                                let current_turn = host.process_state.get_current_turn_id(run_id);
                                let turn_gate = current_turn.as_deref().unwrap_or("untracked");
                                if crate::timing::mark_first(format!(
                                    "claude-message-start:{run_id}:{turn_gate}"
                                )) {
                                    let mut timing = crate::timing::ResumeTimingEvent::new(
                                        "claude_first_message_start",
                                    );
                                    timing.run_id = Some(run_id);
                                    timing.session_id = session_id.as_deref();
                                    timing.turn_id = current_turn.as_deref();
                                    if let Some(usage) = message_start_usage(message.as_ref()) {
                                        timing.input_tokens = Some(u64::from(usage.input_tokens));
                                        timing.cache_read_tokens =
                                            usage.cache_read_input_tokens.map(u64::from);
                                        timing.cache_creation_tokens =
                                            usage.cache_creation_input_tokens.map(u64::from);
                                    }
                                    timing.emit();
                                }
                                if streaming_state.is_some() {
                                    log::warn!("New MessageStart while a stream is still active");
                                    finalize_streaming_message(
                                        host,
                                        &run_db,
                                        run_id,
                                        session_id.as_deref(),
                                        &mut streaming_state,
                                        pending_final_assistant_event.take(),
                                        TokenCounts::from_optional_usage(
                                            pending_delta_usage.as_ref(),
                                        ),
                                    );
                                }
                                pending_delta_usage = None;
                                last_thinking_tokens = pending_thinking_tokens.take();
                                match open_stream(
                                    run_db.clone(),
                                    run_id,
                                    session_id.as_deref(),
                                    current_turn.as_deref(),
                                    "claude",
                                ) {
                                    Ok(stream) => {
                                        let mut new_state = StreamingState::new(&stream);
                                        if last_thinking_tokens.is_some() {
                                            new_state.record_thinking_started(last_thinking_tokens);
                                            let delta = new_state.acc.take_emit_delta();
                                            emit_streaming_delta(
                                                host,
                                                run_id,
                                                &new_state.stream_id,
                                                &delta,
                                                last_thinking_tokens,
                                                new_state.thinking_started_at_ms,
                                                new_state.thinking_ms,
                                                new_state.tool_write.as_ref(),
                                            );
                                        }
                                        streaming_state = Some(new_state);
                                        host.host.transcripts().streams_changed("insert");
                                    }
                                    Err(error) => {
                                        log::warn!(
                                            "Failed to open Claude stream for {}: {}",
                                            run_id,
                                            error
                                        );
                                    }
                                }
                            }
                            StreamEventInner::ContentBlockStart { content_block, .. } => {
                                if let Some(content_block) = content_block.as_ref() {
                                    let mode = content_block
                                        .get("type")
                                        .and_then(serde_json::Value::as_str)
                                        .and_then(|kind| match kind {
                                            "text" => Some("text"),
                                            "thinking" => Some("thinking"),
                                            "tool_use" => Some("tool_use"),
                                            _ => None,
                                        });
                                    if let Some(mode) = mode {
                                        trace_claude_first_any_delta(
                                            host,
                                            run_id,
                                            session_id.as_deref(),
                                            mode,
                                            0,
                                        );
                                    }
                                }
                                if let (Some(ref mut state), Some(content_block)) =
                                    (streaming_state.as_mut(), content_block.as_ref())
                                {
                                    if let Some((id, name)) = extract_tool_start(content_block) {
                                        state.start_tool_write(id, name);
                                        let delta = state.acc.take_emit_delta();
                                        emit_streaming_delta(
                                            host,
                                            run_id,
                                            &state.stream_id,
                                            &delta,
                                            last_thinking_tokens,
                                            state.thinking_started_at_ms,
                                            state.thinking_ms,
                                            state.tool_write.as_ref(),
                                        );
                                    }
                                }
                            }
                            StreamEventInner::ContentBlockDelta { delta, .. } => {
                                let delta_trace = match delta {
                                    DeltaContent::TextDelta { text } if !text.is_empty() => {
                                        Some(("text", text.len()))
                                    }
                                    DeltaContent::ThinkingDelta { thinking }
                                        if !thinking.is_empty() =>
                                    {
                                        Some(("thinking", thinking.len()))
                                    }
                                    DeltaContent::InputJsonDelta { partial_json }
                                        if !partial_json.is_empty() =>
                                    {
                                        Some(("tool_use", partial_json.len()))
                                    }
                                    _ => None,
                                };
                                if let Some((mode, bytes)) = delta_trace {
                                    trace_claude_first_any_delta(
                                        host,
                                        run_id,
                                        session_id.as_deref(),
                                        mode,
                                        bytes,
                                    );
                                    if mode != "tool_use" {
                                        trace_claude_first_nonempty_delta(
                                            host,
                                            run_id,
                                            session_id.as_deref(),
                                            mode,
                                            bytes,
                                        );
                                    }
                                }
                                if let Some(ref mut state) = streaming_state {
                                    match delta {
                                        DeltaContent::TextDelta { text } => {
                                            state.acc.push_content(text);
                                            state.capture_thinking_done();
                                        }
                                        DeltaContent::ThinkingDelta { thinking } => {
                                            state.acc.push_thinking(thinking);
                                        }
                                        DeltaContent::InputJsonDelta { partial_json } => {
                                            state.push_tool_input_delta(partial_json);
                                            let delta = state.acc.take_emit_delta();
                                            emit_streaming_delta(
                                                host,
                                                run_id,
                                                &state.stream_id,
                                                &delta,
                                                last_thinking_tokens,
                                                state.thinking_started_at_ms,
                                                state.thinking_ms,
                                                state.tool_write.as_ref(),
                                            );
                                        }
                                        DeltaContent::Unknown => {}
                                    }
                                    let now = std::time::Instant::now();
                                    if state.acc.should_flush(now) {
                                        match append_chunks(
                                            run_db.clone(),
                                            &state.stream_id,
                                            state.version,
                                            &state.acc.take_pending(),
                                        ) {
                                            Ok(result) => state.version = result.version,
                                            Err(error) => {
                                                log::warn!(
                                                    "Failed to flush Claude stream chunks for {}: {}",
                                                    run_id,
                                                    error
                                                );
                                            }
                                        }
                                    }
                                    if state.acc.should_emit(now) {
                                        state.record_thinking_started(last_thinking_tokens);
                                        let delta = state.acc.take_emit_delta();
                                        emit_streaming_delta(
                                            host,
                                            run_id,
                                            &state.stream_id,
                                            &delta,
                                            last_thinking_tokens,
                                            state.thinking_started_at_ms,
                                            state.thinking_ms,
                                            state.tool_write.as_ref(),
                                        );
                                    }
                                }
                            }
                            StreamEventInner::MessageDelta { usage, .. } => {
                                pending_delta_usage = usage.as_ref().and_then(|usage| {
                                    serde_json::from_value::<Usage>(usage.clone()).ok()
                                });
                                // Live context-gauge update. Each inference's
                                // message_delta usage is the full per-inference
                                // occupancy, so push a snapshot now rather than
                                // waiting for the end-of-turn Result. Guard out
                                // subagent streams so they don't overwrite the
                                // primary session's gauge.
                                if parent_tool_use_id.is_none() {
                                    if let Some(usage) = pending_delta_usage.as_ref() {
                                        emit_claude_context_snapshot(
                                            host,
                                            run_id,
                                            session_id.as_deref(),
                                            usage,
                                        );
                                    }
                                }
                                let counts =
                                    TokenCounts::from_optional_usage(pending_delta_usage.as_ref());
                                if pending_final_assistant_event.is_some()
                                    && streaming_state.is_some()
                                {
                                    finalize_streaming_message(
                                        host,
                                        &run_db,
                                        run_id,
                                        session_id.as_deref(),
                                        &mut streaming_state,
                                        pending_final_assistant_event.take(),
                                        counts,
                                    );
                                }
                            }
                            _ => {}
                        }
                        continue;
                    }

                    // After a terminal artifact/tool has armed suspension, keep storing
                    // tool_result + Result events so native history is complete, but suppress
                    // any assistant/transport continuation the agent starts before our boundary
                    // interrupt lands.
                    if terminal_tool_flag.load(std::sync::atomic::Ordering::Acquire)
                        && !matches!(
                            &event,
                            ClaudeEvent::User { .. } | ClaudeEvent::Result { .. }
                        )
                    {
                        // Commit a genuine assistant message still parked for
                        // deferred finalization before this guard discards its
                        // stream (CAIRN-1611). The flush takes the paired stream
                        // when it commits, so the abort below only fires for an
                        // orphaned post-boundary placeholder with no real message.
                        flush_pending_assistant_before_suppress(
                            host,
                            &run_db,
                            run_id,
                            session_id.as_deref(),
                            &mut streaming_state,
                            &mut pending_final_assistant_event,
                            pending_delta_usage.as_ref(),
                        );
                        // Delete any streaming placeholder that started after the flag.
                        if let Some(state) = streaming_state.take() {
                            let _ = abort_stream(
                                run_db.clone(),
                                &state.stream_id,
                                state.version,
                                "terminal_tool",
                            );
                            host.host.transcripts().streams_changed("update");
                        }
                        continue;
                    }

                    if terminal_tool_flag.load(std::sync::atomic::Ordering::Acquire)
                        && matches!(&event, ClaudeEvent::Result { .. })
                    {
                        // Still process Result for lifecycle (warm transition, finalization).
                        // After a successful turn-ending tool like `return`, Claude can emit a
                        // terminal Result with `is_error=true` because we intentionally interrupt
                        // the session. At that point the tool side effects are already committed,
                        // so the Result is only transport cleanup and must not overwrite success.
                        if let ClaudeEvent::Result { is_error, data, .. } = &event {
                            if *is_error {
                                log::info!(
                                    "Ignoring Result.is_error for run {} because a terminal tool already completed the turn",
                                    &run_id[..run_id.len().min(8)]
                                );
                            }

                            host.host.complete(
                                claude_result_outcome(*is_error, data),
                                ClaudeTurnContext {
                                    terminal_tool_completed: true,
                                    terminal_interrupt_ack_pending,
                                },
                            );
                        }
                        continue;
                    }

                    // Finalize streaming placeholder before Result event
                    if matches!(&event, ClaudeEvent::Result { .. }) {
                        finalize_streaming_message(
                            host,
                            &run_db,
                            run_id,
                            session_id.as_deref(),
                            &mut streaming_state,
                            pending_final_assistant_event.take(),
                            TokenCounts::from_optional_usage(pending_delta_usage.as_ref()),
                        );
                    }

                    let transcript_event = TranscriptEvent::from_claude_event(&event, raw.clone());
                    if let Some(tool_uses) = transcript_event.tool_uses.as_ref() {
                        pending_tool_ids.extend(tool_uses.iter().map(|tool| tool.id.clone()));
                    }
                    if transcript_event.event_type == "tool_result" {
                        if let Some(tool_use_id) = transcript_event.tool_use_id.as_ref() {
                            pending_tool_ids.remove(tool_use_id);
                        }
                    }
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        let was_suppressed = watchdog.pending_tool_count > 0;
                        watchdog.set_pending_tool_count(pending_tool_ids.len());
                        let is_suppressed = watchdog.pending_tool_count > 0;
                        if was_suppressed != is_suppressed {
                            if let Some(identity) =
                                durable_watchdog.lock().ok().and_then(|guard| guard.clone())
                            {
                                record_claude_watchdog_lifecycle(
                                    run_db.clone(),
                                    &identity,
                                    crate::runs::watchdog_ledger::WatchdogLifecycleKind::PhaseTransition,
                                    Some(if is_suppressed {
                                        crate::runs::watchdog_ledger::WatchdogPhase::ToolOutstanding
                                    } else {
                                        crate::runs::watchdog_ledger::WatchdogPhase::PostToolContinuation
                                    }),
                                    if is_suppressed {
                                        "tool_outstanding"
                                    } else {
                                        "tool_result_observed"
                                    },
                                );
                            }
                        }
                    }
                    if let Some(identity) =
                        durable_watchdog.lock().ok().and_then(|guard| guard.clone())
                    {
                        update_claude_watchdog_lease(
                            run_db.clone(),
                            &identity,
                            if pending_tool_ids.is_empty() {
                                crate::runs::watchdog_ledger::WatchdogPhase::PostToolContinuation
                            } else {
                                crate::runs::watchdog_ledger::WatchdogPhase::ToolOutstanding
                            },
                        );
                    }

                    // Handle Assistant events during streaming
                    let is_assistant = matches!(&event, ClaudeEvent::Assistant { .. });
                    let has_content =
                        transcript_event.content.is_some() || transcript_event.tool_uses.is_some();
                    let has_thinking = transcript_event.thinking.is_some();

                    // Partial Assistant event: reasoning complete, no content yet. The
                    // reasoning may be visible prose or PRIVATE (withheld, so the
                    // message carries no prose at all); both mark the same boundary,
                    // so this keys on the absence of content rather than the presence
                    // of prose.
                    if streaming_state.is_some() && is_assistant && !has_content {
                        if let Some(ref mut state) = streaming_state {
                            state.capture_thinking_done();
                            state.record_thinking_started(last_thinking_tokens);
                            let delta = state.acc.take_emit_delta();
                            emit_streaming_delta(
                                host,
                                run_id,
                                &state.stream_id,
                                &delta,
                                last_thinking_tokens,
                                state.thinking_started_at_ms,
                                state.thinking_ms,
                                state.tool_write.as_ref(),
                            );
                        }
                        continue;
                    }

                    // An assistant message carrying only private reasoning has nothing
                    // to persist: its volume rides the turn's `thinking_tokens`, and a
                    // row here would be an empty orphan in the transcript.
                    if is_assistant && !has_content && !has_thinking {
                        continue;
                    }

                    // Complete Assistant event (has content or tool_uses) - finalize placeholder.
                    // Claude Opus can emit the consolidated assistant event before the
                    // message_delta usage that carries exact thinking tokens, so hold the
                    // assistant event briefly and finalize when message_delta arrives.
                    if streaming_state.is_some() && is_assistant && has_content {
                        if let Some(state) = streaming_state.as_mut() {
                            state.capture_thinking_done();
                        }
                        let counts = TokenCounts::from_optional_usage(pending_delta_usage.as_ref());
                        if pending_delta_usage.is_some() {
                            finalize_streaming_message(
                                host,
                                &run_db,
                                run_id,
                                session_id.as_deref(),
                                &mut streaming_state,
                                Some((*transcript_event).clone()),
                                counts,
                            );
                        } else {
                            // claude-code can emit a second consolidated assistant
                            // event before the trailing message_delta (a batched
                            // multi-tool turn). Merge into the parked slot rather
                            // than overwriting it, or the earlier event's tool
                            // calls are lost (CAIRN-2249).
                            match pending_final_assistant_event.as_mut() {
                                Some(parked) => merge_pending_assistant(parked, &transcript_event),
                                None => {
                                    pending_final_assistant_event =
                                        Some((*transcript_event).clone())
                                }
                            }
                        }
                        continue;
                    }

                    // After a host-initiated interrupt (durable dependency wait
                    // via suspend_run_for_durable_wait, or user stop_session) the
                    // CLI cancels the in-flight tool call and emits an interrupt
                    // notice ("[Request interrupted by user]") + an error
                    // tool_result ("The user doesn't want to proceed..."). Those
                    // are transport artifacts of our own interrupt, not real agent
                    // output, so suppress them. The window opens when occupancy
                    // flips to Idle and closes when the run resumes (begin_turn /
                    // transition_to_active). Let Result fall through to its handler
                    // below so the warm transition logic stays unchanged.
                    //
                    // The terminal-tool boundary interrupt is the same thing with
                    // a tighter race: it warms the work turn and resumes the
                    // memory-review turn before its own interrupt notice arrives,
                    // so occupancy is no longer Idle by then. `terminal_interrupt_
                    // ack_pending` covers exactly that window, so the work turn's
                    // "[Request interrupted by user]" notice is suppressed instead
                    // of being stored against the now-current review turn
                    // (CAIRN-1576).
                    if (host.was_interrupted(run_id) || terminal_interrupt_ack_pending)
                        && !matches!(&event, ClaudeEvent::Result { .. })
                    {
                        // The genuine assistant message that issued the
                        // now-interrupted tool call may still be parked for
                        // deferred finalization (its `message_delta` never arrived
                        // before the abort). Commit it before discarding the
                        // interrupt-notice stream (CAIRN-1611).
                        flush_pending_assistant_before_suppress(
                            host,
                            &run_db,
                            run_id,
                            session_id.as_deref(),
                            &mut streaming_state,
                            &mut pending_final_assistant_event,
                            pending_delta_usage.as_ref(),
                        );
                        if let Some(state) = streaming_state.take() {
                            let _ = abort_stream(
                                run_db.clone(),
                                &state.stream_id,
                                state.version,
                                "host_interrupt",
                            );
                            host.host.transcripts().streams_changed("update");
                        }
                        continue;
                    }

                    // Store event in database
                    {
                        let now = chrono::Utc::now().timestamp() as i32;
                        let event_id = ids::mint_child(run_id);
                        let event_type = transcript_event.event_type.clone();
                        let data = transcript_event.to_event_json();

                        let event_counts = TokenCounts::default();

                        if transcript_event.parent_tool_use_id.is_none() {
                            if let ClaudeEvent::Result { data, .. } = &event {
                                record_claude_reported_window(host, run_id, data);
                                if let Some(usage) = pending_delta_usage.as_ref() {
                                    emit_claude_context_snapshot(
                                        host,
                                        run_id,
                                        session_id.as_deref(),
                                        usage,
                                    );
                                }
                            }
                        }

                        let current_turn = host.process_state.get_current_turn_id(run_id);
                        let inserted = host.host.transcripts().insert_event(EventInsert {
                            id: event_id.clone(),
                            run_id: run_id.to_string(),
                            session_id: session_id.clone(),
                            timestamp: now,
                            event_type: event_type.clone(),
                            data: data.clone(),
                            parent_tool_use_id: transcript_event.parent_tool_use_id.clone(),
                            created_at: now,
                            input_tokens: event_counts.input,
                            cache_read_tokens: event_counts.cache_read,
                            cache_create_tokens: event_counts.cache_create,
                            output_tokens: event_counts.output,
                            thinking_tokens: event_counts.thinking,
                            turn_id: current_turn.clone(),
                            cost_usd: None,
                        });
                        let inserted = match inserted {
                            Ok(inserted) => inserted,
                            Err(error) => {
                                // A dropped transcript write is silent data loss:
                                // the event never reaches the chat, the digest, or
                                // replay. Surface it rather than folding it into a
                                // "nothing inserted" bool (CAIRN-3290).
                                log::error!(
                                    "Failed to store {} event for run {}: {}",
                                    event_type,
                                    &run_id[..run_id.len().min(8)],
                                    error
                                );
                                false
                            }
                        };

                        if inserted {
                            host.host.transcripts().event_committed(
                                &event_id,
                                &event_type,
                                &data,
                                event_counts,
                            );
                        }

                        // Todos are written through the `write` tool against a job's
                        // todos URI and stored directly in the todos table — no event
                        // sniffing needed.
                    }

                    // Check for turn completion
                    if let ClaudeEvent::Result { is_error, data, .. } = &event {
                        let depletion =
                            terminal_provider_depletion(*is_error, data, pending_depletion);
                        let terminal_rate_limit = depletion.is_some();
                        if terminal_rate_limit {
                            evidence.record_terminal_rate_limit();
                        }
                        // The terminal-tool boundary interrupt's ack is the next
                        // Result after we sent it. Consume the expectation on any
                        // Result so it can never leak into a later turn's outcome.
                        let ack_pending = terminal_interrupt_ack_pending;
                        terminal_interrupt_ack_pending = false;
                        if dispatch_claude_result(
                            host.host.as_ref(),
                            *is_error,
                            data,
                            ClaudeResultContext {
                                pending_depletion,
                                block_recorded: turn_block_recorded,
                                terminal_interrupt_ack_pending: ack_pending,
                                blocking_reset,
                                tool_in_flight: !pending_tool_ids.is_empty(),
                            },
                            chrono::Utc::now().timestamp(),
                        ) {
                            persisted_rate_limit_quarantine = true;
                        }
                        depletion_handled |= terminal_rate_limit;
                    }

                    let at_boundary = boundary_checker.update(&transcript_event);
                    if should_interrupt_terminal_tool_at_boundary(
                        terminal_tool_flag.as_ref(),
                        at_boundary,
                        &mut terminal_tool_suspended,
                    ) {
                        log::info!(
                            "Terminal artifact/tool reached turn boundary for run {}; interrupting and warming",
                            &run_id[..run_id.len().min(8)]
                        );
                        if let Err(error) = host.host.interrupt() {
                            log::warn!(
                                "Failed to interrupt run {} after terminal artifact/tool boundary: {}",
                                &run_id[..run_id.len().min(8)],
                                error
                            );
                        }
                        // The interrupt produces a delayed Result{is_error} ack. The
                        // work turn is warmed right here, and the memory-review turn
                        // may resume before that ack arrives — flag it so the ack is
                        // swallowed rather than crashing the review turn.
                        terminal_interrupt_ack_pending = true;
                        host.host.artifact_handoff();
                    }
                }
                Err(e) => {
                    log::warn!("Failed to parse event: {} - line: {}", e, line);
                }
            }
        }

        watchdog_alive.store(false, Ordering::Release);
        if let Some(identity) = durable_watchdog.lock().ok().and_then(|guard| guard.clone()) {
            disarm_claude_watchdog_lease(run_db.clone(), &identity, "reader_shutdown");
        }
        let mut parse_summary =
            crate::timing::ResumeTimingEvent::new("claude_stream_parse_summary");
        parse_summary.run_id = Some(run_id);
        parse_summary.session_id = session_id.as_deref();
        parse_summary.duration_us = Some(parse_metrics.duration.as_micros());
        parse_summary.parse_attempts = Some(parse_metrics.attempts);
        parse_summary.parse_failures = Some(parse_metrics.failures + read_failures);
        parse_summary.emit();

        // Finalize any remaining durable stream on EOF
        finalize_streaming_message(
            host,
            &run_db,
            run_id,
            session_id.as_deref(),
            &mut streaming_state,
            pending_final_assistant_event.take(),
            TokenCounts::from_optional_usage(pending_delta_usage.as_ref()),
        );

        log::debug!("reader_thread: loop ended after {} lines", lines_read);

        host.host.eof(ClaudeEof {
            evidence,
            persisted_rate_limit_quarantine,
            pending_depletion,
            depletion_handled,
            turn_block_recorded,
            blocking_reset,
            tool_in_flight: !pending_tool_ids.is_empty(),
            terminal_tool_called: terminal_tool_flag.load(Ordering::Acquire),
            backend_failure: stderr_watch.and_then(StderrWatch::settle),
        });

        // EOF retries a transient release failure from finalization. The host
        // retains the dead handle until the generation-fenced release succeeds.
        host.host.stop_process();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_models::wire::claude_stream::{parse_event, ClaudeEvent, Usage};
    #[test]
    fn silent_resumed_turn_expires_at_the_bounded_window() {
        let timeout = Duration::from_secs(10);
        let started = Instant::now();
        let mut watchdog = ClaudeTurnProgressWatchdog::new(timeout);
        watchdog.observe_turn(Some("wait-resolved-turn"), started);

        assert_eq!(
            watchdog.expired(started + timeout - Duration::from_millis(1)),
            None
        );
        assert_eq!(
            watchdog.expired(started + timeout),
            Some("wait-resolved-turn".to_string())
        );
        assert_eq!(watchdog.expired(started + timeout * 2), None);
    }
    #[test]
    fn provider_progress_and_new_turn_reset_the_silence_window() {
        let timeout = Duration::from_secs(10);
        let started = Instant::now();
        let mut watchdog = ClaudeTurnProgressWatchdog::new(timeout);
        watchdog.observe_turn(Some("turn-1"), started);
        watchdog.record_forward_progress(started + Duration::from_secs(8));
        assert_eq!(watchdog.expired(started + Duration::from_secs(12)), None);

        watchdog.observe_turn(Some("turn-2"), started + Duration::from_secs(15));
        assert_eq!(watchdog.expired(started + Duration::from_secs(20)), None);
        assert_eq!(
            watchdog.expired(started + Duration::from_secs(25)),
            Some("turn-2".to_string())
        );
    }
    #[test]
    fn outstanding_tool_call_disarms_provider_silence_detection() {
        let timeout = Duration::from_secs(10);
        let started = Instant::now();
        let mut watchdog = ClaudeTurnProgressWatchdog::new(timeout);
        watchdog.observe_turn(Some("turn-1"), started);
        watchdog.set_pending_tool_count(1);
        assert_eq!(watchdog.expired(started + Duration::from_secs(60)), None);

        watchdog.set_pending_tool_count(0);
        assert_eq!(
            watchdog.expired(started + Duration::from_secs(60)),
            Some("turn-1".to_string())
        );
    }
    #[test]
    fn terminal_api_refusal_is_named_even_when_cli_calls_it_success() {
        let (event, _) = parse_event(
            r#"{"type":"result","subtype":"success","session_id":"session-1","is_error":true,"api_error_status":429,"terminal_reason":"api_error","result":"You've hit your session limit"}"#,
        )
        .unwrap();
        let ClaudeEvent::Result { is_error, data, .. } = event else {
            panic!("fixture must parse as a terminal result");
        };
        assert!(is_error);
        let super::ClaudeTurnOutcome::Failed { message } =
            super::claude_result_outcome(is_error, &data)
        else {
            panic!("is_error must override the success subtype in the host report");
        };
        assert!(message.contains("HTTP 429"));
    }
    #[test]
    fn terminal_api_error_without_429_is_not_called_a_rate_limit() {
        let data = serde_json::json!({
            "api_error_status": 500,
            "terminal_reason": "api_error"
        });
        let message = terminal_result_error_message(&data);
        assert!(!message.contains("429"));
        assert!(!message.contains("rate limit"));
    }
    #[test]
    fn terminal_error_without_tool_history_uses_a_neutral_diagnosis() {
        let message = terminal_result_error_message(&serde_json::json!({}));
        assert_eq!(
            message,
            "The provider returned a terminal error. The turn is interrupted and resumable."
        );
        assert!(!message.contains("tool"));
        assert!(!message.contains("continuation"));
    }
    fn test_streaming_state() -> super::StreamingState {
        super::StreamingState {
            stream_id: "stream-test".to_string(),
            version: 0,
            acc: crate::transcripts::stream_store::StreamAccumulator::new(),
            opened_at: std::time::Instant::now(),
            thinking_ms: None,
            thinking_started_at_ms: None,
            tool_write: None,
        }
    }
    #[test]
    fn records_thinking_anchor_once_on_first_nonzero_count() {
        let mut state = test_streaming_state();
        assert!(state.thinking_started_at_ms.is_none());

        // A zero or absent count must not arm the anchor.
        state.record_thinking_started(Some(0));
        assert!(state.thinking_started_at_ms.is_none());
        state.record_thinking_started(None);
        assert!(state.thinking_started_at_ms.is_none());

        // The first non-zero count sets the wall-clock anchor.
        state.record_thinking_started(Some(50));
        let first = state.thinking_started_at_ms;
        assert!(first.is_some());

        // Later counts never move it (the anchor stays the thinking start).
        state.record_thinking_started(Some(100));
        assert_eq!(state.thinking_started_at_ms, first);
    }
    #[test]
    fn capture_thinking_done_freezes_duration_once() {
        let mut state = test_streaming_state();
        assert!(state.thinking_ms.is_none());
        state.capture_thinking_done();
        let frozen = state.thinking_ms;
        assert!(frozen.is_some());
        // Idempotent: a later boundary signal does not overwrite the duration.
        state.capture_thinking_done();
        assert_eq!(state.thinking_ms, frozen);
    }
    #[test]
    fn claude_context_used_tokens_sums_last_inference_usage() {
        let usage = Usage {
            input_tokens: 7,
            cache_creation_input_tokens: Some(6_200),
            cache_read_input_tokens: Some(31_468),
            output_tokens: 38,
            output_tokens_details: None,
        };

        assert_eq!(claude_context_used_tokens(&usage), 37_713);
    }
    #[test]
    fn build_claude_context_snapshot_carries_session_and_summed_usage() {
        // message_delta-shaped usage: prompt + cache + output is the full
        // per-inference occupancy the live gauge should report.
        let usage = Usage {
            input_tokens: 2_979,
            cache_creation_input_tokens: Some(22_522),
            cache_read_input_tokens: Some(0),
            output_tokens: 4_147,
            output_tokens_details: None,
        };

        let state = build_claude_context_snapshot(
            "run-1",
            Some("session-durable"),
            Some("sonnet".to_string()),
            None,
            None,
            &usage,
        );

        assert_eq!(state.run_id, "run-1");
        assert_eq!(state.session_id.as_deref(), Some("session-durable"));
        assert_eq!(state.backend, "claude");
        assert_eq!(state.used_tokens, 29_648);
        // Before init the bare alias is all there is, and `sonnet` is 200k on
        // CLIs before the 5.5 aliases.
        assert_eq!(state.context_window, Some(200_000));
        assert_eq!(state.last_output_tokens, Some(4_147));
    }

    /// CLI 2.1.302 resolves `sonnet` and `haiku` to the 5.5 generation, whose
    /// window is 1M; the alias alone would read 200k.
    #[test]
    fn build_claude_context_snapshot_reads_5_5_windows_from_the_resolved_model() {
        let usage = Usage {
            input_tokens: 4,
            cache_creation_input_tokens: Some(9_000),
            cache_read_input_tokens: Some(150_000),
            output_tokens: 996,
            output_tokens_details: None,
        };
        for (alias, resolved) in [
            ("sonnet", "claude-sonnet-5-5"),
            ("haiku", "claude-haiku-5-5"),
        ] {
            let state = build_claude_context_snapshot(
                "run-1",
                Some("session"),
                Some(alias.to_string()),
                Some(resolved),
                None,
                &usage,
            );
            assert_eq!(state.model.as_deref(), Some(alias));
            assert_eq!(state.context_window, Some(1_000_000), "{alias}");
            assert_eq!(state.used_tokens, 160_000);
        }
        // The window the CLI reported outranks every table answer.
        let state = build_claude_context_snapshot(
            "run-1",
            Some("session"),
            Some("haiku".to_string()),
            Some("claude-haiku-4-5-20251001"),
            Some(200_000),
            &usage,
        );
        assert_eq!(state.context_window, Some(200_000));
    }

    #[test]
    fn build_claude_context_snapshot_without_a_model_has_no_window() {
        let usage = Usage {
            input_tokens: 4,
            cache_creation_input_tokens: Some(19_000),
            cache_read_input_tokens: Some(150_000),
            output_tokens: 996,
            output_tokens_details: None,
        };

        let state =
            build_claude_context_snapshot("run-1", Some("session"), None, None, None, &usage);

        assert_eq!(state.model, None);
        assert_eq!(state.context_window, None);
        assert_eq!(state.used_tokens, 170_000);
    }
}

#[cfg(test)]
mod startup_inventory_tests {
    use super::{mcp_tool_inventory_diagnostic, sanitize_mcp_diagnostic, validate_claude_init};
    use crate::agent_process::toolkits::CORE_VERBS;
    use serde_json::json;

    #[test]
    fn reports_declared_and_exact_missing_verbs() {
        for missing in CORE_VERBS {
            let mut tools: Vec<_> = CORE_VERBS
                .into_iter()
                .filter(|tool| *tool != missing)
                .collect();
            tools.extend(["Read", "Write", "Bash", "mcp__other__run"]);
            let error = validate_claude_init(
                &json!({"tools": tools, "mcp_servers": [{"name": "cairn", "status": "connected"}]}),
                &[],
            )
            .unwrap_err();
            assert!(error.contains("cairn server status: connected"));
            assert!(error.contains(&format!("missing tools: [{missing:?}]")));
            assert!(error.contains("Declared tools: ["));
            assert!(error.contains("\"Read\""));
            assert!(error.contains("\"mcp__other__run\""));
        }
    }

    #[test]
    fn malformed_or_empty_inventory_never_satisfies_required_verbs() {
        for init in [
            json!({}),
            json!({"tools": null}),
            json!({"tools": "mcp__cairn__read"}),
            json!({"tools": []}),
            json!({"tools": [null, 42, {"name": "mcp__cairn__run"}]}),
        ] {
            let error = validate_claude_init(&init, &[]).unwrap_err();
            assert!(error.contains("Declared tools: []"));
            for required in CORE_VERBS {
                assert!(error.contains(required));
            }
        }
    }

    #[test]
    fn validation_uses_full_inventory_not_diagnostic_preview() {
        let mut tools = vec!["Read"; 100];
        tools.extend(CORE_VERBS);
        assert!(validate_claude_init(&json!({"tools": tools}), &[]).is_ok());
    }

    #[test]
    fn inventory_bounds_name_count_and_unicode_length() {
        let long = "界".repeat(10_000);
        let mut tools = vec![long.as_str(); 100];
        tools.push("not-in-preview");
        let preview = mcp_tool_inventory_diagnostic(&tools);
        assert!(preview.chars().count() < 1_700);
        assert!(preview.contains("… (+85 more)"));
        assert!(!preview.contains("not-in-preview"));
        assert_eq!(mcp_tool_inventory_diagnostic(&[]), "[]");
        assert!(!mcp_tool_inventory_diagnostic(&["read"; 16]).contains("more"));
    }

    #[test]
    fn inventory_redacts_credentials_and_escapes_control_characters() {
        let secret = "unregistered-profile-token-4Qf8Jv2Kp9";
        let credential = format!("Authorization: Bearer {secret}");
        let diagnostic = sanitize_mcp_diagnostic(&format!("MCP failed: {credential}"));
        let error = validate_claude_init(
            &json!({"tools": [credential, "tool\nforged\r\tline"]}),
            &[diagnostic],
        )
        .unwrap_err();
        assert!(!error.contains(secret));
        assert!(error.contains("[REDACTED]"));
        assert!(!error.contains(['\n', '\r', '\t']));
        assert!(error.contains("tool\\nforged\\r\\tline"));
        assert!(error.contains("MCP failed:"));
    }
}

#[cfg(test)]
mod depletion_order_tests {
    //! The reader must hand a turn's depletion to the host before it completes
    //! that turn: completion can flush a queued reply into a new launch, which
    //! has to see the depleted account's health and the replacement pin.
    use super::*;
    use crate::transcripts::stream_store::FinalizedStream;
    use crate::transcripts::TranscriptHost;

    // Credit exhaustion arrives as a structured error before a generic 429
    // result; the hold must learn it is credits, not a rolling window that heals.
    #[test]
    fn a_credit_depletion_reaches_the_terminal_hold_as_credits() {
        let host = RecordingHost::default();
        dispatch_claude_result(
            &host,
            true,
            &serde_json::json!({"api_error_status": 429}),
            ClaudeResultContext {
                pending_depletion: Some(ProviderDepletion::Credits),
                block_recorded: false,
                ..Default::default()
            },
            0,
        );
        assert_eq!(
            *host.calls.lock().unwrap(),
            ["provider_limit:Credits:false:None"]
        );
    }

    #[derive(Default)]
    struct RecordingHost {
        calls: Mutex<Vec<String>>,
        snapshots: Mutex<Vec<ContextTokenState>>,
    }

    impl RecordingHost {
        fn record(&self, call: String) {
            self.calls.lock().unwrap().push(call);
        }
    }

    impl TranscriptHost for RecordingHost {
        fn insert_event(&self, _event: EventInsert) -> Result<bool, String> {
            Ok(true)
        }
        fn finalize_stream(
            &self,
            _stream_id: &str,
            _version: i32,
            _event: Option<TranscriptEvent>,
            _counts: TokenCounts,
        ) -> Result<FinalizedStream, String> {
            Err("no streaming in this script".to_string())
        }
        fn streams_changed(&self, _action: &str) {}
        fn event_committed(&self, _id: &str, _kind: &str, _data: &str, _counts: TokenCounts) {}
    }

    impl ClaudeHost for RecordingHost {
        fn transcripts(&self) -> &dyn TranscriptHost {
            self
        }
        fn stop_process(&self) {}
        fn credential_rejected(&self) {}
        fn credential_acknowledged(&self, _generation: u64) {}
        fn error(&self, _message: &str) {}
        fn startup_rejected(&self, _message: &str) {}
        fn initialized(&self) {}
        fn capture_output(&self, _output: serde_json::Value) {}
        fn rate_limit(
            &self,
            _snapshot: ProviderUsageSnapshot,
            info: Option<&RateLimitInfo>,
        ) -> bool {
            let blocking = info.is_none_or(|info| info.is_blocking());
            self.record(format!("rate_limit:{blocking}"));
            blocking
        }
        fn provider_limit_result(&self, report: ProviderLimitReport<'_>) -> bool {
            let ProviderLimitReport {
                depletion,
                block_recorded,
                blocking_reset,
                tool_in_flight,
                ..
            } = report;
            self.record(format!(
                "provider_limit:{depletion:?}:{block_recorded}:{blocking_reset:?}"
            ));
            if tool_in_flight {
                self.record("tool_in_flight".to_string());
            }
            true
        }
        fn context_snapshot(&self, snapshot: ContextTokenState) {
            self.snapshots.lock().unwrap().push(snapshot);
        }
        fn complete(&self, _outcome: ClaudeTurnOutcome, context: ClaudeTurnContext) {
            let _ = context;
            self.record("complete".to_string());
        }
        fn artifact_handoff(&self) {}
        fn interrupt(&self) -> Result<(), String> {
            Ok(())
        }
        fn recover_watchdog(
            &self,
            _identity: &crate::runs::watchdog_ledger::WatchdogIdentity,
            _now: i64,
        ) -> Result<(), String> {
            Ok(())
        }
        fn eof(&self, report: ClaudeEof) {
            self.record(format!(
                "eof:handled={}:blocking={}",
                report.depletion_handled, report.evidence.saw_blocking_rate_limit
            ));
        }
    }

    async fn run_script(lines: &[&str]) -> Vec<String> {
        let db = Arc::new(cairn_db::storage::migrated_test_db("claude-depletion-order.db").await);
        let host = Arc::new(RecordingHost::default());
        let runtime = ClaudeRuntime {
            process_state: Arc::new(AgentProcessState::default()),
            emitter: Arc::new(cairn_platform::services::emitter::CapturingEmitter::default()),
            boot_id: "boot".to_string(),
            host: host.clone(),
        };
        let stdout = format!("{}\n", lines.join("\n"));
        std::thread::spawn(move || {
            ClaudeRuntime::reader_thread(
                &runtime,
                "run-depletion",
                None,
                false,
                Box::new(std::io::Cursor::new(stdout.into_bytes())),
                None,
                db,
            );
        })
        .join()
        .expect("reader thread");
        let calls = host.calls.lock().unwrap().clone();
        calls
    }

    // A job that selected no model still runs on one: the CLI names it in its
    // init. The live gauge reads the window from that reported model rather
    // than inventing a default for a model it never saw.
    #[tokio::test]
    async fn the_live_gauge_reads_the_window_of_the_model_the_cli_reported() {
        let db = Arc::new(cairn_db::storage::migrated_test_db("claude-reported-model.db").await);
        let host = Arc::new(RecordingHost::default());
        let process_state = Arc::new(AgentProcessState::default());
        process_state.processes.lock().unwrap().register(
            "run-reported".to_string(),
            crate::agent_process::process::RunHandle::test_handle(None, None),
        );
        let runtime = ClaudeRuntime {
            process_state,
            emitter: Arc::new(cairn_platform::services::emitter::CapturingEmitter::default()),
            boot_id: "boot".to_string(),
            host: host.clone(),
        };
        let init = serde_json::json!({
            "type": "system",
            "subtype": "init",
            "session_id": "s1",
            "model": "claude-opus-5-5",
            "tools": crate::agent_process::toolkits::CORE_VERBS,
        });
        let delta = serde_json::json!({
            "type": "stream_event",
            "session_id": "s1",
            "event": {
                "type": "message_delta",
                "delta": {},
                "usage": {
                    "input_tokens": 4,
                    "cache_read_input_tokens": 100_000,
                    "cache_creation_input_tokens": 16_000,
                    "output_tokens": 996
                }
            }
        });
        let stdout = format!("{init}\n{delta}\n");
        std::thread::spawn(move || {
            ClaudeRuntime::reader_thread(
                &runtime,
                "run-reported",
                None,
                false,
                Box::new(std::io::Cursor::new(stdout.into_bytes())),
                None,
                db,
            );
        })
        .join()
        .expect("reader thread");

        let snapshots = host.snapshots.lock().unwrap().clone();
        let [snapshot] = snapshots.as_slice() else {
            panic!("expected one live snapshot, got {snapshots:?}");
        };
        assert_eq!(snapshot.used_tokens, 117_000);
        assert_eq!(snapshot.model.as_deref(), Some("claude-opus-5-5"));
        assert_eq!(snapshot.context_window, Some(1_000_000));
    }

    // The table reads `claude-sonnet-5` as 200k, the window CLI 2.1.220 gives
    // it; a CLI that reports 1M for it in a turn's result is believed from then
    // on, for every later inference of the process.
    #[tokio::test]
    async fn the_live_gauge_adopts_the_window_a_result_reports() {
        let db = Arc::new(cairn_db::storage::migrated_test_db("claude-reported-window.db").await);
        let host = Arc::new(RecordingHost::default());
        let process_state = Arc::new(AgentProcessState::default());
        process_state.processes.lock().unwrap().register(
            "run-window".to_string(),
            crate::agent_process::process::RunHandle::test_handle(None, None),
        );
        let runtime = ClaudeRuntime {
            process_state: process_state.clone(),
            emitter: Arc::new(cairn_platform::services::emitter::CapturingEmitter::default()),
            boot_id: "boot".to_string(),
            host: host.clone(),
        };
        let init = serde_json::json!({
            "type": "system",
            "subtype": "init",
            "session_id": "s1",
            "model": "claude-sonnet-5",
            "tools": crate::agent_process::toolkits::CORE_VERBS,
        });
        let delta = serde_json::json!({
            "type": "stream_event",
            "session_id": "s1",
            "event": {
                "type": "message_delta",
                "delta": {},
                "usage": {
                    "input_tokens": 4,
                    "cache_read_input_tokens": 170_000,
                    "cache_creation_input_tokens": 0,
                    "output_tokens": 996
                }
            }
        });
        let result = serde_json::json!({
            "type": "result",
            "subtype": "success",
            "session_id": "s1",
            "is_error": false,
            "result": "done",
            "modelUsage": {
                "claude-sonnet-5": {"contextWindow": 1_000_000, "maxOutputTokens": 64_000}
            }
        });
        let stdout = format!("{init}\n{delta}\n{result}\n{delta}\n");
        std::thread::spawn(move || {
            ClaudeRuntime::reader_thread(
                &runtime,
                "run-window",
                None,
                false,
                Box::new(std::io::Cursor::new(stdout.into_bytes())),
                None,
                db,
            );
        })
        .join()
        .expect("reader thread");

        assert_eq!(
            process_state.reported_model_and_window("run-window"),
            (Some("claude-sonnet-5".to_string()), Some(1_000_000))
        );
        let windows: Vec<Option<i64>> = host
            .snapshots
            .lock()
            .unwrap()
            .iter()
            .map(|snapshot| snapshot.context_window)
            .collect();
        assert_eq!(windows.first(), Some(&Some(200_000)), "{windows:?}");
        assert_eq!(windows.last(), Some(&Some(1_000_000)), "{windows:?}");
    }

    // The credits shape: a structured synthetic error, then a generic error
    // result with no 429 status. It holds like any other limit and never
    // completes the turn.
    #[tokio::test]
    async fn a_credits_message_before_a_generic_error_result_holds() {
        let calls = run_script(&[
            r#"{"type":"assistant","uuid":"a1","session_id":"s1","is_api_error_message":true,"api_error_status":429,"error_details":"credits_required","message":{"role":"assistant","content":[{"type":"text","text":"Manage credits with /usage-credits"}]}}"#,
            r#"{"type":"result","subtype":"success","session_id":"s1","is_error":true,"result":"You're out of usage credits."}"#,
        ])
        .await;
        assert_eq!(
            calls,
            [
                "provider_limit:Credits:false:None",
                "eof:handled=true:blocking=true"
            ]
        );
    }

    // A blocking limit event and then the process exits with no result: the
    // reader reports it at EOF, unhandled, for the host to hold.
    #[tokio::test]
    async fn a_blocking_exit_without_a_result_reaches_eof_unhandled() {
        let calls = run_script(&[
            r#"{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetsAt":4102444800}}"#,
        ])
        .await;
        assert_eq!(
            calls,
            ["rate_limit:true", "eof:handled=false:blocking=true"]
        );
    }

    #[tokio::test]
    async fn nonblocking_event_does_not_supply_a_terminal_hold_boundary() {
        let calls = run_script(&[
            r#"{"type":"rate_limit_event","rate_limit_info":{"status":"allowed","resetsAt":4102444800}}"#,
            r#"{"type":"result","subtype":"success","session_id":"s1","is_error":true,"api_error_status":429}"#,
        ]).await;
        assert_eq!(
            calls,
            [
                "rate_limit:false",
                "provider_limit:RollingWindow { resets_at: None }:false:None",
                "eof:handled=true:blocking=true"
            ]
        );
    }

    // Recorded 2026-09-03 (run ef12efb6): the CLI ran a tool and sent its
    // follow-up request (`num_turns: 2`), which was refused, yet no tool_result
    // reached the stream first. Automatic resume could repeat that tool call.
    #[tokio::test]
    async fn a_limit_with_an_unanswered_tool_call_reports_the_tool_in_flight() {
        let fixture = include_str!("../../tests/fixtures/claude_limit_tool_in_flight.jsonl");
        let calls = run_script(&fixture.lines().collect::<Vec<_>>()).await;
        assert_eq!(
            calls,
            [
                "provider_limit:RollingWindow { resets_at: None }:false:None",
                "tool_in_flight",
                "eof:handled=true:blocking=true",
            ]
        );
    }

    // The clean ordering seen in 83 of the 86 recorded mid-turn limits: the
    // tool's result is on the stream before the refused request.
    #[tokio::test]
    async fn a_limit_after_the_tool_result_does_not_report_a_tool_in_flight() {
        let fixture = include_str!("../../tests/fixtures/claude_limit_tool_answered.jsonl");
        let calls = run_script(&fixture.lines().collect::<Vec<_>>()).await;
        assert_eq!(
            calls,
            [
                "provider_limit:RollingWindow { resets_at: None }:false:None",
                "eof:handled=true:blocking=true",
            ]
        );
    }

    #[test]
    fn lookalike_error_and_success_never_enter_provider_hold() {
        let host = RecordingHost::default();
        for (is_error, status) in [(true, 500), (false, 429)] {
            super::dispatch_claude_result(
                &host,
                is_error,
                &serde_json::json!({"api_error_status":status,
                    "result":"You've hit your session limit · resets 2:40pm (America/New_York)"}),
                super::ClaudeResultContext::default(),
                1788545269,
            );
        }
        assert_eq!(*host.calls.lock().unwrap(), ["complete", "complete"]);
    }

    #[tokio::test]
    async fn a_turns_provider_block_is_passed_to_the_terminal_hold() {
        let calls = run_script(&[
            r#"{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetsAt":4102444800}}"#,
            r#"{"type":"result","subtype":"success","session_id":"s1","is_error":true,"api_error_status":429}"#,
        ])
        .await;
        assert_eq!(
            calls,
            [
                "rate_limit:true",
                "provider_limit:RollingWindow { resets_at: None }:true:Some(4102444800)",
                "eof:handled=true:blocking=true",
            ]
        );
    }
}
