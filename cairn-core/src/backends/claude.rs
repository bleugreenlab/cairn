//! Claude CLI backend implementation.
//!
//! Handles spawning the Claude CLI process, managing stdin/stdout communication,
//! and reading the stream-json event stream into the database.

use crate::agent_process::args::{build_claude_args, ClaudeArgsConfig};
use crate::agent_process::memory::{MemoryProbe, OsMemoryProbe};
use crate::agent_process::stream::{
    ClaudeEvent, DeltaContent, RateLimitInfo, StreamEventInner, TokenCounts, TranscriptEvent, Usage,
};
use crate::agent_process::turn_boundary::{
    should_interrupt_terminal_tool_at_boundary, TurnBoundaryChecker,
};
use crate::backends::context_window::{claude_context_window, ClaudeContextOptIn};

use super::run_state::{
    is_task_spawned_run, resolve_run_db, run_backend_db, run_job_id, run_status,
    set_session_backend_id, transition_run_to_live,
};
use crate::backends::{OptionChoice, OptionKind, ProviderOptionDescriptor, ProviderOptionKey};
use crate::models::{
    ContextTokenState, ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow, RunStatus,
};
use crate::orchestrator::session::{
    assemble_prompt_segments, flatten_prompt_segments, get_claude_path, insert_error_event,
    persist_system_prompt_event, write_system_prompt_file,
};
use crate::orchestrator::Orchestrator;
use crate::services::SpawnConfig;
use crate::storage::{DbError, LocalDb, RowExt};
use crate::transcripts::stream_store::{
    abort_stream, append_chunks, finalize_stream_emit, open_stream, process_post_commit_outbox,
    ActiveMessageStream, EmitDelta, EventInsert, StreamAccumulator, StreamingToolWrite,
};
use cairn_common::ids;
use cairn_db::turso::params;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

use super::{AgentBackend, BackendFailure, DiscoveredModel, ResolvedTools, SessionConfig};

const CLAUDE_BACKEND_NAME: &str = "Claude";
/// Refusal shown when a Claude session has no credential Cairn owns. Naming the
/// remedy matters: the machine may well have a signed-in Claude CLI, and the
/// point is that Cairn deliberately will not use it.
const NO_CLAUDE_CREDENTIAL: &str = "No Claude account is available for this session. \
     Sign in to a Claude account, or add an Anthropic API key, in Settings → Providers. \
     Cairn only runs sessions on accounts it manages, never on the Claude CLI login \
     that happens to be active outside it.";
const TOOL_INPUT_PREVIEW_MAX_CHARS: usize = 512;
const CLAUDE_TURN_NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const CLAUDE_TURN_WATCHDOG_POLL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
struct ClaudeLaunchContract {
    args: Vec<String>,
    env: HashMap<String, String>,
}

fn sanitize_mcp_diagnostic(line: &str) -> String {
    let policy = crate::security::sanitize::RedactionPolicy::default();
    let mut sanitizer = crate::security::sanitize::Sanitizer::structural(&policy);
    sanitizer.text(line).chars().take(500).collect()
}

fn build_claude_launch_contract(
    args: Vec<String>,
    run_id: &str,
    mcp_secret: &str,
    user: &crate::identity::UserIdentity,
    api_key: Option<&str>,
) -> Result<ClaudeLaunchContract, String> {
    let mut env = HashMap::from([
        ("CAIRN_RUN_ID".to_string(), run_id.to_string()),
        ("CAIRN_MCP_SECRET".to_string(), mcp_secret.to_string()),
        ("ENABLE_TOOL_SEARCH".to_string(), "false".to_string()),
        ("CLAUDE_CODE_ENABLE_TASKS".to_string(), "false".to_string()),
        ("MCP_TOOL_TIMEOUT".to_string(), "604800000".to_string()),
        ("GIT_AUTHOR_NAME".to_string(), user.name.clone()),
        ("GIT_AUTHOR_EMAIL".to_string(), user.email.clone()),
        ("GIT_COMMITTER_NAME".to_string(), user.name.clone()),
        ("GIT_COMMITTER_EMAIL".to_string(), user.email.clone()),
    ]);
    match &user.claude_auth {
        Some(crate::identity::ClaudeAuth::ApiKey(_)) => {
            env.insert(
                "ANTHROPIC_API_KEY".to_string(),
                api_key
                    .ok_or_else(|| "Claude API-key launch is missing its brokered key".to_string())?
                    .to_string(),
            );
        }
        Some(crate::identity::ClaudeAuth::ConfigDir(path)) => {
            env.insert(
                "CLAUDE_CONFIG_DIR".to_string(),
                path.to_string_lossy().into_owned(),
            );
        }
        None => return Err(NO_CLAUDE_CREDENTIAL.to_string()),
    }
    Ok(ClaudeLaunchContract { args, env })
}

fn validate_claude_init(
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
    if crate::agent_process::toolkits::CORE_VERBS
        .iter()
        .all(|required| tools.contains(required))
    {
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
        "Claude started without the required Cairn MCP tools (cairn server status: {}). MCP startup diagnostics: {diagnostics}",
        if server_status.is_empty() { "not reported".to_string() } else { server_status.join(", ") }
    ))
}

/// Bounds a provider turn that has started but emits no further protocol
/// progress. Tool calls are excluded: their host-owned execution has its own
/// deadlines and may legitimately remain silent for much longer than a model
/// inference. This specifically covers a resumed Claude process that accepts a
/// launch but never emits even an init/result event (CAIRN-3561).
#[derive(Debug)]
struct ClaudeTurnProgressWatchdog {
    active_turn_id: Option<String>,
    last_forward_progress_at: Instant,
    timeout: Duration,
    pending_tool_count: usize,
    fired: bool,
}

impl ClaudeTurnProgressWatchdog {
    fn new(timeout: Duration) -> Self {
        Self {
            active_turn_id: None,
            last_forward_progress_at: Instant::now(),
            timeout,
            pending_tool_count: 0,
            fired: false,
        }
    }

    fn observe_turn(&mut self, turn_id: Option<&str>, now: Instant) {
        if self.active_turn_id.as_deref() != turn_id {
            self.active_turn_id = turn_id.map(str::to_owned);
            self.last_forward_progress_at = now;
            self.pending_tool_count = 0;
            self.fired = false;
        }
    }

    fn record_forward_progress(&mut self, now: Instant) {
        if self.active_turn_id.is_some() {
            self.last_forward_progress_at = now;
        }
    }

    fn set_pending_tool_count(&mut self, count: usize) {
        self.pending_tool_count = count;
    }

    fn expired(&mut self, now: Instant) -> Option<String> {
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
const CLAUDE_CALL_PROCESS_RSS_ESTIMATE: u64 = 450 * 1024 * 1024;

/// Divisor applied to total physical RAM to get the memory budget the ceiling
/// may spend on concurrent `claude` processes. `4` == 25% of RAM: ample
/// headroom for the agent processes, the runner, and the OS, while still letting
/// a large workstation run a wide fan-out fully parallel.
const CLAUDE_CALL_RAM_BUDGET_DIVISOR: u64 = 4;

/// Floor on the derived ceiling so even a small machine gets useful call
/// parallelism (and an unmeasurable system stays protective).
const CLAUDE_CALL_CONCURRENCY_FLOOR: usize = 4;

/// Cap on the derived ceiling to keep process count and file-descriptor
/// pressure sane no matter how much RAM the budget would otherwise permit.
const CLAUDE_CALL_CONCURRENCY_CAP: usize = 64;

/// Pure ceiling formula: `clamp(budget / per-process RSS, FLOOR, CAP)`, where
/// the budget is a fixed fraction of total physical RAM. Side-effect-free and
/// keyed only on `total_ram_bytes` so the clamp boundaries are unit-testable
/// with injected RAM values, never the host's real memory.
pub(crate) fn claude_call_concurrency_ceiling(total_ram_bytes: u64) -> usize {
    let budget = total_ram_bytes / CLAUDE_CALL_RAM_BUDGET_DIVISOR;
    let raw = (budget / CLAUDE_CALL_PROCESS_RSS_ESTIMATE) as usize;
    raw.clamp(CLAUDE_CALL_CONCURRENCY_FLOOR, CLAUDE_CALL_CONCURRENCY_CAP)
}

/// Claude CLI can report a terminal failure as a `Result` whose subtype is still
/// `success`. `is_error` is authoritative; this helper turns the structured
/// failure into a named node-visible diagnosis. Only an actual HTTP 429 is called
/// a rate limit. The generic case stays neutral because a terminal result can
/// arrive before or after a tool call and this payload does not prove which.
fn terminal_result_error_message(data: &serde_json::Value) -> &'static str {
    if data
        .get("api_error_status")
        .and_then(serde_json::Value::as_u64)
        == Some(429)
    {
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
struct StreamingState {
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

#[cfg(test)]
mod tests {
    use super::{
        build_claude_context_snapshot, claude_context_used_tokens, parse_thinking_tokens_estimate,
        should_confirm_backend_id_after_init, terminal_result_error_message, ClaudeBackend,
        ClaudeTurnProgressWatchdog,
    };
    use crate::agent_process::stream::{parse_event, ClaudeEvent, Usage};
    use std::time::{Duration, Instant};

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
        assert!(terminal_result_error_message(&data).contains("HTTP 429"));
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

    use crate::backends::{AgentBackend, SessionStart};

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
    fn parse_thinking_tokens_estimate_reads_absolute_estimate() {
        let data = serde_json::json!({
            "estimated_tokens": 150,
            "estimated_tokens_delta": 100,
        });
        assert_eq!(parse_thinking_tokens_estimate(&data), Some(150));
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
            &usage,
        );

        assert_eq!(state.run_id, "run-1");
        assert_eq!(state.session_id.as_deref(), Some("session-durable"));
        assert_eq!(state.backend, "claude");
        assert_eq!(state.used_tokens, 29_648);
        assert_eq!(state.context_window, Some(200_000));
        assert_eq!(state.last_output_tokens, Some(4_147));
    }

    #[test]
    fn claude_confirms_backend_id_for_new_sessions() {
        assert!(should_confirm_backend_id_after_init(&SessionStart::New {
            session_id: "session-new".to_string(),
        }));
    }

    #[test]
    fn claude_does_not_reconfirm_backend_id_for_resumed_sessions() {
        assert!(!should_confirm_backend_id_after_init(
            &SessionStart::Resume {
                session_id: "session-resume".to_string(),
                backend_id: "backend-existing".to_string(),
            }
        ));
    }

    #[test]
    fn claude_confirms_backend_id_for_forked_sessions() {
        assert!(should_confirm_backend_id_after_init(&SessionStart::Fork {
            session_id: "session-fork".to_string(),
            source_backend_id: "backend-source".to_string(),
        }));
    }

    #[test]
    fn claude_disallows_host_harness_builtin_tools() {
        let host_harness_tools = ["SendMessage", "ReportFindings"];
        let resolved = ClaudeBackend.resolve_tools(
            &host_harness_tools
                .iter()
                .map(|tool| tool.to_string())
                .collect::<Vec<_>>(),
            &[],
        );

        for tool in host_harness_tools {
            assert!(
                resolved.disallowed.contains(&tool.to_string()),
                "{tool} must be passed through --disallowedTools: {:?}",
                resolved.disallowed
            );
            assert!(
                !resolved.allowed.contains(&tool.to_string()),
                "{tool} must not remain on the Claude allow-list: {:?}",
                resolved.allowed
            );
        }
    }
}

impl StreamingState {
    fn new(stream: &ActiveMessageStream) -> Self {
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
    fn capture_thinking_done(&mut self) {
        if self.thinking_ms.is_none() {
            self.thinking_ms = Some(self.opened_at.elapsed().as_millis() as i64);
        }
    }

    /// Record the backend wall-clock start of the thinking phase, once, the first
    /// time a non-zero thinking-token count is observed for this stream. Idempotent
    /// and gated on `tokens > 0` so the anchor lands on the first real thinking
    /// count and never moves afterward.
    fn record_thinking_started(&mut self, tokens: Option<u32>) {
        if self.thinking_started_at_ms.is_none() && tokens.map(|t| t > 0).unwrap_or(false) {
            self.thinking_started_at_ms = Some(chrono::Utc::now().timestamp_millis());
        }
    }

    fn start_tool_write(&mut self, id: Option<String>, name: String) {
        self.tool_write = Some(StreamingToolWrite {
            id,
            name,
            input_chars: 0,
            status: "constructing".to_string(),
            input_preview: Some(String::new()),
        });
    }

    fn push_tool_input_delta(&mut self, partial_json: &str) {
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

fn extract_tool_start(content_block: &serde_json::Value) -> Option<(Option<String>, String)> {
    if content_block.get("type").and_then(|value| value.as_str()) != Some("tool_use") {
        return None;
    }
    let name = content_block.get("name").and_then(|value| value.as_str())?;
    let id = content_block
        .get("id")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    Some((id, name.to_string()))
}

/// Emit a true-delta `streaming-update`: only the newly-produced scalar tail,
/// plus the current absolute lengths so the frontend can detect gaps and
/// self-heal against the DB snapshot.
#[allow(clippy::too_many_arguments)]
fn emit_streaming_delta(
    orch: &Orchestrator,
    run_id: &str,
    event_id: &str,
    delta: &EmitDelta,
    thinking_tokens: Option<u32>,
    thinking_started_at: Option<i64>,
    thinking_ms: Option<i64>,
    tool_write: Option<&StreamingToolWrite>,
) {
    if crate::resume_timing::mark_first(format!("claude-streaming-emit:{run_id}")) {
        let mut event = crate::resume_timing::ResumeTimingEvent::new("claude_first_streaming_emit");
        event.run_id = Some(run_id);
        event.stream_id = Some(event_id);
        event.bytes = Some(
            delta.content_delta.as_ref().map_or(0, String::len)
                + delta.thinking_delta.as_ref().map_or(0, String::len),
        );
        event.emit();
    }
    crate::transcripts::stream_store::trace_first_frontend_delta_emit(
        run_id, None, event_id, delta,
    );
    let _ = orch.services.emitter.emit(
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

fn parse_thinking_tokens_estimate(data: &serde_json::Value) -> Option<u32> {
    data.get("estimated_tokens")
        .and_then(|value| value.as_u64())
        .and_then(|tokens| u32::try_from(tokens).ok())
}

/// Whether the host already transitioned this run's process to warm (occupancy=Idle).
///
/// `stop_session_internal` in `orchestrator::lifecycle` flips occupancy to Idle right
/// after writing an interrupt control request to Claude's stdin. Claude's subsequent
/// `Result{is_error:true}` / EOF is transport cleanup, not a real crash.
fn was_host_interrupted(orch: &Orchestrator, run_id: &str) -> bool {
    orch.process_state
        .get_occupancy(run_id)
        .map(|o| matches!(o, crate::agent_process::process::RunOccupancy::Idle))
        .unwrap_or(false)
}

fn claude_context_used_tokens(usage: &Usage) -> i64 {
    usage.input_tokens as i64
        + usage.cache_creation_input_tokens.unwrap_or(0) as i64
        + usage.cache_read_input_tokens.unwrap_or(0) as i64
        + usage.output_tokens as i64
}

/// Build a context-token snapshot from a single inference's usage. The Claude
/// CLI's `message_delta` usage carries the full per-inference occupancy
/// (prompt + cache + output), so the summed figure is the live context fill for
/// the current model call. Pure (no orchestrator), so it can be unit-tested.
fn build_claude_context_snapshot(
    run_id: &str,
    session_id: Option<&str>,
    model: Option<String>,
    usage: &Usage,
) -> ContextTokenState {
    let context_window = claude_context_window(
        model.as_deref().unwrap_or("unknown"),
        ClaudeContextOptIn::default(),
    );
    ContextTokenState {
        run_id: run_id.to_string(),
        session_id: session_id.map(str::to_string),
        backend: "claude".to_string(),
        model,
        used_tokens: claude_context_used_tokens(usage),
        context_window: Some(context_window),
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
fn emit_claude_context_snapshot(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    usage: &Usage,
) {
    let model = orch.process_state.get_model(run_id);
    orch.store_context_token_snapshot(build_claude_context_snapshot(
        run_id, session_id, model, usage,
    ));
}

fn should_finalize_task_run_on_terminal_tool_eof(
    terminal_tool_called: bool,
    run_status: Option<&str>,
    is_task_spawned: bool,
) -> bool {
    terminal_tool_called && run_status == Some("running") && is_task_spawned
}

/// Stable discriminant name for a parsed event, for diagnostics.
fn claude_event_kind(event: &ClaudeEvent) -> &'static str {
    match event {
        ClaudeEvent::System { .. } => "system",
        ClaudeEvent::User { .. } => "user",
        ClaudeEvent::Assistant { .. } => "assistant",
        ClaudeEvent::Result { .. } => "result",
        ClaudeEvent::StreamEvent { .. } => "stream_event",
        ClaudeEvent::ControlResponse { .. } => "control_response",
        ClaudeEvent::RateLimitEvent { .. } => "rate_limit_event",
        ClaudeEvent::Unknown => "unknown",
    }
}

/// Build a coarse usage snapshot from a `rate_limit_event`. The CLI reports a
/// status + reset windows, not a precise usage percent, so this surfaces one
/// window carrying the reset time and a status-derived remaining figure
/// (allowed -> 100%, blocking -> 0%). Coarse, but it gives the live usage panel
/// a real signal where Claude previously had none. `source` distinguishes it
/// from the precise PTY-probe snapshot.
fn claude_rate_limit_snapshot(info: &RateLimitInfo) -> ProviderUsageSnapshot {
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
        unsupported_reason: None,
        raw: serde_json::to_value(info).ok(),
        model_breakdown: None,
    }
}

/// The Claude CLI's wording when `--resume <id>` finds no conversation for the
/// process's working directory (the CLI keys its conversation store by cwd).
/// Matched here, at the boundary that emits it; no other layer sees the string.
fn classify_stderr_failure(line: &str) -> Option<BackendFailure> {
    line.contains("No conversation found with session ID")
        .then_some(BackendFailure::SessionUnresolvable)
}

/// The stderr drain for a spawned agent process plus any failure it classified.
///
/// [`StderrWatch::settle`] joins the drain before reading, so a diagnosis the
/// process printed on its way out is never lost to a race with stdout EOF.
/// Joining is bounded rather than polled: the read loop that just ended proves
/// the child closed stdout, and stderr is closed by the same process exit, so
/// the drain is already finishing. A timeout here would either race the
/// classification or tax every crash path with a fixed wait.
struct StderrWatch {
    join: thread::JoinHandle<()>,
    failure: Arc<Mutex<Option<BackendFailure>>>,
    mcp_diagnostics: Arc<Mutex<Vec<String>>>,
}

impl StderrWatch {
    /// Drain `stderr` on a background thread, logging every line as today and
    /// recording the first line that classifies as a typed backend failure.
    fn spawn(stderr: Box<dyn BufRead + Send>) -> Self {
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
    fn settle(self) -> Option<BackendFailure> {
        self.settle_with_diagnostics().0
    }

    fn settle_with_diagnostics(self) -> (Option<BackendFailure>, Vec<String>) {
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

/// How a Claude process's stdout-EOF should finalize its run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EofVerdict {
    /// Completed: host-warmed, produced a terminal Result, or a terminal-tool
    /// task that exited before its Result. Finalize Exited (turn marked complete).
    Exited,
    /// Exited mid-run without a terminal Result while still running. Finalize
    /// Crashed, which *interrupts* the running turn rather than failing it —
    /// leaving the job resumable (Pending), not completed-and-advanced and not
    /// cascade-failed. A blocking rate-limit exit lands here too; the rate-limit
    /// signal refines the diagnostic/error message, not the verdict.
    Crashed,
    /// Already terminal before EOF; nothing to finalize.
    AlreadyTerminal,
}

/// Classify an EOF outcome from process/run facts. Pure so the matrix is
/// unit-testable; the reader thread gathers the inputs and dispatches.
fn classify_eof(
    was_warm: bool,
    saw_terminal_result: bool,
    run_status: Option<&str>,
    terminal_tool_called: bool,
    is_task_spawned: bool,
) -> EofVerdict {
    // Host already warmed the process, or it emitted a terminal Result before
    // closing stdout: it completed. Closing stdout afterward is never a crash.
    if was_warm || saw_terminal_result {
        return EofVerdict::Exited;
    }
    // Task-spawned run that called its terminal tool but exited before a Result.
    if should_finalize_task_run_on_terminal_tool_eof(
        terminal_tool_called,
        run_status,
        is_task_spawned,
    ) {
        return EofVerdict::Exited;
    }
    // No terminal Result and the run is still "running": the process died
    // mid-turn. Finalize Crashed, which *interrupts* the running turn rather than
    // completing it — leaving the job resumable (derives Pending), not
    // completed-and-advanced and not cascade-failed. A blocking rate-limit exit
    // lands here too: it is recoverable on reset, and the rate-limit signal
    // refines the diagnostic/error message in the reader thread, NOT the verdict.
    // (Finalizing Exited would apply TurnState::Complete and advance downstream
    // onto work the blocked account never finished.)
    if run_status == Some("running") {
        return EofVerdict::Crashed;
    }
    EofVerdict::AlreadyTerminal
}

#[derive(Debug)]
struct RateLimitRetryTarget {
    job_id: String,
    session_id: String,
    turn_id: String,
    project_id: Option<String>,
    account_id: String,
}

fn rate_limit_retry_target(
    db: &Arc<LocalDb>,
    run_id: &str,
) -> Result<Option<RateLimitRetryTarget>, String> {
    let db = db.clone();
    let run_id = run_id.to_string();
    run_backend_db(CLAUDE_BACKEND_NAME, async move {
        db.read(|conn| Box::pin(async move {
            let mut rows = conn.query(
                "SELECT j.id, j.current_session_id, j.current_turn_id, COALESCE(r.project_id, i.project_id), s.account_id
                   FROM runs r
                   JOIN jobs j ON j.id = r.job_id
                   LEFT JOIN issues i ON i.id = r.issue_id
                   JOIN sessions s ON s.id = j.current_session_id
                  WHERE r.id = ?1
                  LIMIT 1",
                (run_id.as_str(),),
            ).await?;
            let Some(row) = rows.next().await? else { return Ok(None); };
            let Some(session_id) = row.opt_text(1)? else { return Ok(None); };
            let Some(turn_id) = row.opt_text(2)? else { return Ok(None); };
            let Some(account_id) = row.opt_text(4)? else { return Ok(None); };
            Ok(Some(RateLimitRetryTarget {
                job_id: row.text(0)?, session_id, turn_id,
                project_id: row.opt_text(3)?, account_id,
            }))
        })).await.map_err(|error| error.to_string())
    })
}

/// Look up `(job_id, execution_id)` for a run, for the crash diagnostic.
fn run_job_execution(db: &Arc<LocalDb>, run_id: &str) -> (Option<String>, Option<String>) {
    let db = db.clone();
    let run_id = run_id.to_string();
    run_backend_db(CLAUDE_BACKEND_NAME, async move {
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT jobs.id, jobs.execution_id
                         FROM runs INNER JOIN jobs ON jobs.id = runs.job_id
                         WHERE runs.id = ?1",
                        params![run_id.as_str()],
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| Ok::<_, DbError>((row.opt_text(0)?, row.opt_text(1)?)))
                    .transpose()
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .flatten()
    .unwrap_or((None, None))
}

fn finalize_streaming_message(
    orch: &Orchestrator,
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
            orch,
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
    match finalize_stream_emit(
        db.clone(),
        orch.db.local.clone(),
        &orch.services.emitter,
        &state.stream_id,
        state.version,
        final_event,
        counts,
    ) {
        Ok(finalized) => process_post_commit_outbox(orch, &finalized.outbox_entries),
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
fn merge_pending_assistant(parked: &mut TranscriptEvent, incoming: &TranscriptEvent) {
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
    // thinking: keep parked unless empty.
    if parked.thinking.is_none() {
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
fn flush_pending_assistant_before_suppress(
    orch: &Orchestrator,
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
            orch,
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
    let current_turn = orch.process_state.get_current_turn_id(run_id);
    match crate::transcripts::stream_store::insert_event_emit(
        db.clone(),
        &orch.services.emitter,
        EventInsert {
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
        },
    ) {
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

/// Claude CLI agent backend.
pub struct ClaudeBackend;

fn should_confirm_backend_id_after_init(session_start: &crate::backends::SessionStart) -> bool {
    matches!(
        session_start,
        crate::backends::SessionStart::New { .. } | crate::backends::SessionStart::Fork { .. }
    )
}

impl AgentBackend for ClaudeBackend {
    fn name(&self) -> &str {
        "Claude"
    }

    fn is_available(&self) -> Result<(), String> {
        crate::env::find_binary("claude").map(|_| ())
    }

    fn discover_models(&self) -> Result<Vec<DiscoveredModel>, String> {
        discover_claude_models()
    }

    fn option_descriptors(&self) -> Vec<ProviderOptionDescriptor> {
        vec![ProviderOptionDescriptor {
            key: ProviderOptionKey::ReasoningEffort,
            label: "Effort".to_string(),
            kind: OptionKind::Enum,
            choices: ["low", "medium", "high", "xhigh", "max"]
                .into_iter()
                .map(|value| OptionChoice {
                    value: value.to_string(),
                    label: value.to_string(),
                })
                .collect(),
            default: None,
        }]
    }

    fn resolve_tools(&self, agent_tools: &[String], agent_disallowed: &[String]) -> ResolvedTools {
        use crate::agent_process::toolkits;

        // Resolve agent tool names to the Cairn allow-list (friendly verbs
        // aliased to Cairn verbs; native + dead names dropped).
        let mut allowed = toolkits::resolve_tools(agent_tools);

        // Temporary permissions floor: the three core verbs are always allowed
        // for every agent (CAIRN-1172). Without this a read-only agent's
        // `write cairn:~/return` trips the permission prompt and wedges the run.
        toolkits::ensure_core_verbs(&mut allowed);

        // The retired `mcp__cairn__return` tool is no longer injected: returning
        // is `write cairn:~/return`, and schema-constrained calls capture the
        // native structured output server-side (CAIRN-2505). No MCP handler ever
        // dispatched a `return` tool, so it was dead weight on the allow-list.

        // Defensively strip always-disallowed tools from allowed (in case an
        // agent config lists planning/todo tools).
        allowed.retain(|t| !crate::models::ALWAYS_DISALLOWED_TOOLS.contains(&t.as_str()));

        // Native is fully off: the disallow list is the union of all native
        // tools, the always-disallowed planning/todo tools, and the agent's own
        // disallowed list. This is the single source for `--disallowedTools`.
        let mut disallowed: Vec<String> = crate::models::ALL_NATIVE_TOOLS
            .iter()
            .chain(crate::models::ALWAYS_DISALLOWED_TOOLS.iter())
            .map(|t| t.to_string())
            .collect();
        disallowed.extend(agent_disallowed.iter().cloned());

        // Built-ins caught escaping an earlier session in this process. A CLI
        // update can add a built-in that predates no list; quarantining what was
        // actually observed keeps it off every later session.
        disallowed.extend(toolkits::quarantined_tools());

        disallowed.sort();
        disallowed.dedup();

        ResolvedTools {
            allowed,
            disallowed,
        }
    }

    fn start_session(&self, config: SessionConfig, orch: &Orchestrator) -> Result<(), String> {
        let start_time = std::time::Instant::now();

        let session_id = Some(config.session_start.session_id().to_string());
        let mut event = crate::resume_timing::ResumeTimingEvent::new("claude_prepare_start");
        event.run_id = Some(&config.run_id);
        event.session_id = session_id.as_deref();
        event.mode = Some(if config.session_start.replayed_backend_id().is_some() {
            "resume"
        } else {
            "new"
        });
        event.bytes = Some(config.prompt.len());
        event.emit();

        // Effective model recorded on the process handle for warm-reuse
        // reconciliation (captured before `config.model` is moved into args).
        let effective_model = config.model.as_ref().map(|m| m.as_str().to_string());

        // Translate canonical permissions to Claude CLI flags.
        //
        // The three verbs are always allow-listed and native tools are
        // disallowed, so no CLI permission prompt is ever needed: escape gating
        // happens inside the verb handlers (the worktree fence). Allow mode
        // skips provider prompts entirely; ask/deny rely solely on the fence and
        // emit no permission flag.
        let use_skip_permissions = matches!(config.permissions.fence, crate::models::Fence::Allow);

        let workspace_instructions = crate::workspace::instructions::read_workspace_instructions();
        let project_instructions = crate::workspace::instructions::read_project_instructions(
            std::path::Path::new(&config.working_dir),
        );

        // Assemble the uniform system-prompt stack (cairn + workspace + project +
        // role + orientation) shared by every backend, then deliver it as ONE
        // file via --system-prompt-file, fully replacing CC's default. The file
        // bytes equal the persisted segment concatenation exactly. Project/local
        // CLAUDE.md native memory (a separate context-injection mechanism) is
        // suppressed via --setting-sources user in the CLI args, so the project
        // instruction layer reaches Claude exactly once — through the assembled
        // `project` segment, not also through CC auto-discovery.
        let prompt_segments = assemble_prompt_segments(
            &crate::system_prompt::cairn_system_prompt(config.ambient),
            workspace_instructions.as_deref(),
            project_instructions.as_deref(),
            config.system_prompt_content.as_deref(),
            config.system_prompt_dynamic_tail.as_deref(),
        );
        let system_prompt_path =
            write_system_prompt_file(&config.run_id, &flatten_prompt_segments(&prompt_segments))?;

        persist_system_prompt_event(
            orch,
            &config.run_id,
            session_id.as_deref(),
            "claude",
            &prompt_segments,
        );

        // Resolve the run's owning DB ONCE (CAIRN-2208) and thread it through the
        // transcript/stream/run-state writes below. A team run lives wholly in its
        // synced replica; targeting the private DB would fail the
        // message_streams→runs foreign key. Resolved before the process spawns so a
        // closed replica fails fast rather than orphaning a live agent.
        let run_db = resolve_run_db(CLAUDE_BACKEND_NAME, orch, &config.run_id)?;

        // Write hook settings file for memory surfacing (passed via --settings)
        let hook_settings_path =
            crate::memories::hooks::write_hook_settings_file(orch.mcp_callback_port).ok();

        // Build Claude CLI arguments
        let args_config = ClaudeArgsConfig {
            mcp_config: config.mcp_config_json.clone(),
            skip_permissions: use_skip_permissions,
            model: config.model,
            session_start: config.session_start.clone(),
            prompt: config.prompt.clone(),
            // Claude's CLI replaced --max-thinking-tokens with --effort. Prefer an
            // explicit effort; fall back to "high" for legacy presets that still
            // only carry a (now-ignored) max_thinking_tokens budget.
            effort: config
                .reasoning_effort
                .clone()
                .or_else(|| config.max_thinking_tokens.map(|_| "high".to_string())),
            allowed_tools: config.allowed_tools,
            disallowed_tools: config.disallowed_tools,
            system_prompt_file: Some(system_prompt_path),
            settings_path: hook_settings_path,
            bidirectional: config.bidirectional,
            json_schema: config.output_schema.clone(),
        };
        let claude_args = build_claude_args(&args_config);

        // Get cached claude path (resolves once on first use)
        let claude_path = get_claude_path(&orch.process_state)?;

        log::debug!("ClaudeBackend: command built, claude_path={}", claude_path);
        log::debug!("ClaudeBackend: argument count={}", claude_args.len());
        log::debug!("ClaudeBackend: working_dir={}", config.working_dir);

        let mut event =
            crate::resume_timing::ResumeTimingEvent::new("claude_args_ready").elapsed(start_time);
        event.run_id = Some(&config.run_id);
        event.session_id = session_id.as_deref();
        event.count = Some(claude_args.len());
        event.emit();

        // Get MCP authentication secret (shared secret for TOTP-style passcodes)
        let mcp_secret = orch
            .mcp_auth
            .get_secret_for_mcp()
            .map_err(|e| format!("Failed to get MCP auth secret: {}", e))?;
        log::info!("Using MCP auth secret for run {}", config.run_id);

        // Inject user identity into Claude process environment
        // Prefer pre-resolved identity from SessionConfig (includes project overrides)
        let identity = config
            .identity
            .as_ref()
            .cloned()
            .or_else(|| orch.get_identity());

        // The Cairn-managed Claude profile, when the identity carries one. It is
        // also where the CLI keeps the conversation a resume replays, so the
        // transcript repair below needs the same path.
        let claude_config_dir = identity.as_ref().and_then(|user| match &user.claude_auth {
            Some(crate::identity::ClaudeAuth::ConfigDir(path)) => Some(path.clone()),
            _ => None,
        });

        // Fail closed. A session that spawns without an explicit credential
        // runs on whatever account is signed in at the user level: invisible to
        // usage routing, unattributable, and impossible to switch away from
        // when it runs out. Refusing to start says so; starting anyway looks
        // like it worked and quietly spends someone else's subscription.
        let user = identity.ok_or_else(|| NO_CLAUDE_CREDENTIAL.to_string())?;
        // Forward Claude auth for remote/headless sessions.
        //
        // The CLI reads its credential from this environment, so the plaintext
        // is unavoidable; what the broker adds is that the value is a
        // registered scrub target before it is injected, and that the injection
        // appears in the lease inventory. A `ConfigDir` is a path rather than a
        // credential and deliberately does not go through it — registering an
        // ordinary path is the over-registration failure the broker exists to
        // avoid.
        let backend_account = user.email.as_str();
        const ANTHROPIC_PROVIDER: &str = "anthropic";
        let brokered_api_key = match &user.claude_auth {
            Some(crate::identity::ClaudeAuth::ApiKey(key)) => {
                let leased = crate::security::broker::backend::agent_credential(
                    ANTHROPIC_PROVIDER,
                    backend_account,
                    crate::security::broker::backend::CLAUDE_ROLE,
                    key,
                )?;
                log::info!("Setting ANTHROPIC_API_KEY (len={})", leased.len());
                Some(leased)
            }
            Some(crate::identity::ClaudeAuth::ConfigDir(path)) => {
                crate::identity::claude_profile::provision_profile(path)?;
                None
            }
            None => return Err(NO_CLAUDE_CREDENTIAL.to_string()),
        };

        // Keep the final process boundary inspectable: the exact argument vector
        // and environment used by the real spawn are assembled together.
        let launch = build_claude_launch_contract(
            claude_args,
            &config.run_id,
            &mcp_secret,
            &user,
            brokered_api_key.as_deref(),
        )?;
        let mut spawn_config = SpawnConfig::new(&claude_path)
            .args(&launch.args)
            .cwd(&config.working_dir)
            .stdin(true);
        for (key, value) in &launch.env {
            spawn_config = spawn_config.env(key, value);
        }

        log::info!(
            "Injected user identity into session: {} <{}>",
            user.name,
            user.email
        );

        // A resume hands the CLI its own persisted conversation to replay, and a
        // single content block the API rejects there kills the session for good:
        // every later resume, including a well-formed operator message, fails
        // with the same 400 (CAIRN-3263). Repair it now, while no CLI process
        // holds the file open.
        if let Some(backend_id) = config.session_start.replayed_backend_id() {
            let repair_started = std::time::Instant::now();
            let repair = super::claude_transcript::repair_before_resume(
                claude_config_dir.as_deref(),
                std::path::Path::new(&config.working_dir),
                backend_id,
            );
            let mut event =
                crate::resume_timing::ResumeTimingEvent::new("claude_transcript_repair")
                    .elapsed(repair_started);
            event.run_id = Some(&config.run_id);
            event.session_id = session_id.as_deref();
            event.mode = Some(if repair.used_full_scan {
                "full"
            } else {
                "incremental"
            });
            event.count = Some(repair.lines_parsed);
            event.bytes = Some(repair.bytes_read.min(usize::MAX as u64) as usize);
            event.emit();
        }

        // Check if we need to evict a warm process to make room
        orch.collect_warm_if_needed();
        let spawn_started = std::time::Instant::now();

        log::debug!("ClaudeBackend: about to spawn");
        let mut child = orch.services.process.spawn(spawn_config).map_err(|e| {
            log::debug!("ClaudeBackend: spawn failed: {}", e);
            insert_error_event(
                orch,
                &config.run_id,
                session_id.as_deref(),
                &format!("Failed to start Claude: {}", e),
            );
            e
        })?;
        log::debug!("ClaudeBackend: spawned, pid={}", child.id());
        let mut event = crate::resume_timing::ResumeTimingEvent::new("claude_process_spawned")
            .elapsed(spawn_started);
        event.run_id = Some(&config.run_id);
        event.session_id = session_id.as_deref();
        event.emit();

        // Transition run to Running AFTER successful spawn (sets started_at accurately)
        log::debug!("ClaudeBackend: transitioning run to running");
        if let Err(e) = transition_run_to_live(CLAUDE_BACKEND_NAME, orch, &run_db, &config.run_id) {
            log::warn!("Failed to transition run to running: {}", e);
            // Job is already Running from start_job's transition_job call — no write needed
        }
        log::debug!("ClaudeBackend: run transitioned to running");

        let stdout = child.take_stdout().ok_or("Failed to capture stdout")?;
        let stderr = child.take_stderr();
        let stdin = child
            .take_stdin()
            .map(|w| crate::agent_process::process::wrap_plain_stdin(w));
        let confirm_backend_id_after_init =
            should_confirm_backend_id_after_init(&config.session_start);

        // Drain stderr on its own thread, keeping a joinable handle so the
        // reader thread can read any typed failure it classified before
        // deciding what stdout EOF meant.
        let stderr_watch = stderr.map(StderrWatch::spawn);

        // Store the process handle with stdin for bidirectional communication
        let child_arc = Arc::new(Mutex::new(Some(child)));
        let stdin_arc = Arc::new(Mutex::new(stdin));

        // Get job_id for warm process tracking
        let process_job_id: Option<String> =
            run_job_id(CLAUDE_BACKEND_NAME, &run_db, &config.run_id);

        {
            let mut active_process = crate::agent_process::process::ActiveProcess::new(
                child_arc.clone(),
                stdin_arc.clone(),
                session_id.clone(),
                process_job_id,
            );
            active_process.model = effective_model.clone();
            orch.process_state
                .register_process(config.run_id.clone(), active_process)?;
        }

        // In bidirectional mode, send the initial prompt via stdin
        if args_config.bidirectional {
            let mut stdin_guard = stdin_arc.lock().map_err(|e| e.to_string())?;
            if let Some(ref mut stdin_writer) = *stdin_guard {
                let content =
                    crate::agent_process::stdin::build_message_content(&config.message_content)?;

                let initial_message = serde_json::json!({
                    "type": "user",
                    "message": {
                        "role": "user",
                        "content": content
                    }
                });
                writeln!(stdin_writer, "{}", initial_message)
                    .map_err(|e| format!("Failed to send initial prompt via stdin: {}", e))?;
                stdin_writer
                    .flush()
                    .map_err(|e| format!("Failed to flush stdin: {}", e))?;
                let mut event =
                    crate::resume_timing::ResumeTimingEvent::new("claude_initial_stdin_written")
                        .elapsed(start_time);
                event.run_id = Some(&config.run_id);
                event.session_id = session_id.as_deref();
                event.bytes = Some(initial_message.to_string().len());
                event.emit();
                log::info!(
                    "Sent initial prompt via stdin ({} chars)",
                    config.prompt.len()
                );
            }
        }

        // Clone what we need for the reader thread
        let run_id = config.run_id.clone();
        let orch = orch.clone();
        let emitter = orch.services.emitter.clone();

        let thread_session_id = session_id.clone();

        // Spawn thread to read stdout and emit events
        thread::spawn(move || {
            Self::reader_thread(
                &orch,
                &emitter,
                &run_id,
                thread_session_id,
                confirm_backend_id_after_init,
                stdout,
                stderr_watch,
                run_db,
            );
        });

        let mut event = crate::resume_timing::ResumeTimingEvent::new("claude_reader_started")
            .elapsed(start_time);
        event.run_id = Some(&config.run_id);
        event.session_id = session_id.as_deref();
        event.emit();
        Ok(())
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn supports_warm_processes(&self) -> bool {
        true
    }

    fn call_batch_capability(&self) -> crate::backends::CallBatchCapability {
        // Claude is permanently CLI-bound: each ephemeral call is a dedicated
        // `claude --print` process (~450 MB RSS). The admission ceiling is the
        // only lever on fan-out memory, so cap concurrency here at a bound
        // derived from physical RAM (CAIRN-2557).
        crate::backends::CallBatchCapability {
            shape: crate::backends::CallBatchShape::DedicatedProcess,
            max_concurrency: Some(*CLAUDE_CALL_MAX_CONCURRENCY),
        }
    }

    fn send_user_message(
        &self,
        stdin: &mut dyn crate::agent_process::process::BackendStdin,
        content: &crate::agent_process::stdin::MessageContent,
        session_id: &str,
        parent_tool_use_id: Option<&str>,
        _working_dir: Option<&str>,
    ) -> Result<(), String> {
        crate::agent_process::stdin::send_user_message(
            stdin,
            session_id,
            content,
            parent_tool_use_id,
        )
    }

    fn send_interrupt(
        &self,
        stdin: &mut dyn crate::agent_process::process::BackendStdin,
    ) -> Result<(), String> {
        let request_id = uuid::Uuid::new_v4().to_string();
        crate::agent_process::stdin::send_interrupt_request(stdin, &request_id)
    }

    fn send_set_model(
        &self,
        stdin: &mut dyn crate::agent_process::process::BackendStdin,
        model: &str,
    ) -> Result<(), String> {
        let request_id = uuid::Uuid::new_v4().to_string();
        crate::agent_process::stdin::send_set_model_request(stdin, &request_id, model)
    }

    fn send_set_permission_mode(
        &self,
        stdin: &mut dyn crate::agent_process::process::BackendStdin,
        mode: &str,
    ) -> Result<(), String> {
        let request_id = uuid::Uuid::new_v4().to_string();
        crate::agent_process::stdin::send_set_permission_mode_request(stdin, &request_id, mode)
    }
}

fn discover_claude_models() -> Result<Vec<DiscoveredModel>, String> {
    let claude_path = crate::env::find_binary("claude")?;
    let status = Command::new(&claude_path)
        .args(["auth", "status", "--json"])
        .output()
        .map_err(|e| format!("Failed to run Claude auth status: {}", e))?;

    if !status.status.success() {
        let stderr = String::from_utf8_lossy(&status.stderr).trim().to_string();
        return Err(if stderr.is_empty() {
            format!("Claude auth status exited with {}", status.status)
        } else {
            format!("Claude auth status failed: {}", stderr)
        });
    }

    let auth_json: serde_json::Value = serde_json::from_slice(&status.stdout)
        .map_err(|e| format!("Failed to parse Claude auth status JSON: {}", e))?;
    let logged_in = auth_json
        .get("loggedIn")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !logged_in {
        return Err("Claude CLI is not authenticated".to_string());
    }

    Ok(vec![
        DiscoveredModel {
            id: "haiku".to_string(),
            model: "haiku".to_string(),
            display_name: "haiku".to_string(),
            description: Some("Fast Claude alias".to_string()),
            hidden: false,
            is_default: false,
            default_reasoning_effort: None,
            supported_reasoning_efforts: vec![],
            context_window: Some(claude_context_window(
                "haiku",
                ClaudeContextOptIn::default(),
            )),
            canonical_slug: None,
            serving_account_ids: Vec::new(),
            pricing: None,
            supported_parameters: Vec::new(),
            router: false,
            architecture_modality: None,
            wire_protocol: None,
        },
        DiscoveredModel {
            id: "sonnet".to_string(),
            model: "sonnet".to_string(),
            display_name: "sonnet".to_string(),
            description: Some("Balanced Claude alias".to_string()),
            hidden: false,
            is_default: true,
            default_reasoning_effort: None,
            supported_reasoning_efforts: vec![],
            context_window: Some(claude_context_window(
                "sonnet",
                ClaudeContextOptIn::default(),
            )),
            canonical_slug: None,
            serving_account_ids: Vec::new(),
            pricing: None,
            supported_parameters: Vec::new(),
            router: false,
            architecture_modality: None,
            wire_protocol: None,
        },
        DiscoveredModel {
            id: "opus".to_string(),
            model: "opus".to_string(),
            display_name: "opus".to_string(),
            description: Some("High-capability Claude alias".to_string()),
            hidden: false,
            is_default: false,
            default_reasoning_effort: None,
            supported_reasoning_efforts: vec![],
            context_window: Some(claude_context_window("opus", ClaudeContextOptIn::default())),
            canonical_slug: None,
            serving_account_ids: Vec::new(),
            pricing: None,
            supported_parameters: Vec::new(),
            router: false,
            architecture_modality: None,
            wire_protocol: None,
        },
        DiscoveredModel {
            id: "fable".to_string(),
            model: "fable".to_string(),
            display_name: "fable".to_string(),
            description: Some("Most capable Claude alias".to_string()),
            hidden: false,
            is_default: false,
            default_reasoning_effort: None,
            supported_reasoning_efforts: vec![],
            context_window: Some(claude_context_window(
                "fable",
                ClaudeContextOptIn::default(),
            )),
            canonical_slug: None,
            serving_account_ids: Vec::new(),
            pricing: None,
            supported_parameters: Vec::new(),
            router: false,
            architecture_modality: None,
            wire_protocol: None,
        },
    ])
}

impl ClaudeBackend {
    /// Reader thread: reads Claude's stream-json stdout and stores events in the database.
    #[allow(clippy::too_many_arguments)]
    fn reader_thread(
        orch: &Orchestrator,
        emitter: &Arc<dyn crate::services::EventEmitter>,
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
        let mut first_message_start_logged = false;
        let mut first_nonempty_delta_logged = false;
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
        if let Ok(mut watchdog) = progress_watchdog.lock() {
            let current_turn = orch.process_state.get_current_turn_id(run_id);
            watchdog.observe_turn(current_turn.as_deref(), Instant::now());
        }
        let watchdog_alive = Arc::new(AtomicBool::new(true));
        {
            let watchdog = progress_watchdog.clone();
            let alive = watchdog_alive.clone();
            let orch = orch.clone();
            let run_id = run_id.to_string();
            let session_id = session_id.clone();
            let watchdog_run_db = run_db.clone();
            thread::spawn(move || {
                while alive.load(Ordering::Acquire) {
                    thread::sleep(CLAUDE_TURN_WATCHDOG_POLL);
                    if !alive.load(Ordering::Acquire) {
                        break;
                    }
                    let current_turn = orch.process_state.get_current_turn_id(&run_id);
                    let expired_turn = watchdog.lock().ok().and_then(|mut guard| {
                        let now = Instant::now();
                        guard.observe_turn(current_turn.as_deref(), now);
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
                    insert_error_event(&orch, &run_id, session_id.as_deref(), &diagnosis);
                    let (job_id, _) = run_job_execution(&watchdog_run_db, &run_id);
                    let Some(job_id) = job_id else {
                        let failure = "Cairn could not identify the job for the silent Claude turn, so automatic recovery was not possible.";
                        insert_error_event(&orch, &run_id, session_id.as_deref(), failure);
                        let _ = crate::orchestrator::lifecycle::kill_session_with_reason(
                            &orch, &run_id, "crash",
                        );
                        break;
                    };
                    if let Err(error) = crate::orchestrator::lifecycle::kill_session_with_reason(
                        &orch,
                        &run_id,
                        crate::orchestrator::lifecycle::PROVIDER_SILENCE_RECOVERY_EXIT_REASON,
                    ) {
                        let failure = format!(
                            "Cairn could not stop the silent Claude turn for recovery: {error}"
                        );
                        insert_error_event(&orch, &run_id, session_id.as_deref(), &failure);
                        crate::orchestrator::lifecycle::report_recovery_launch_failure(
                            &orch, &run_id,
                        );
                        break;
                    }
                    let recovery_prompt = format!(
                        "{diagnosis} Continue from the completed tool result already present in the transcript."
                    );
                    match crate::execution::jobs::continue_job_impl(
                        &orch,
                        &job_id,
                        Some(&recovery_prompt),
                        None,
                        Some(crate::execution::jobs::ResumeContext {
                            suppress_user_event: true,
                            ..Default::default()
                        }),
                    ) {
                        Ok(_) => insert_error_event(
                            &orch,
                            &run_id,
                            session_id.as_deref(),
                            "Cairn started a recovery turn after the silent Claude continuation.",
                        ),
                        Err(error) => {
                            let failure = format!(
                                "Cairn could not start the recovery turn after aborting the silent Claude continuation: {error}"
                            );
                            log::error!(
                                "Failed to self-resume silent Claude turn {turn_id} for job {job_id}: {error}"
                            );
                            insert_error_event(&orch, &run_id, session_id.as_deref(), &failure);
                            crate::orchestrator::lifecycle::report_recovery_launch_failure(
                                &orch, &run_id,
                            );
                        }
                    }
                    break;
                }
            });
        }

        // Grab the terminal_tool_called flag for this process.
        // When set, we stop storing events — the session is complete.
        let terminal_tool_flag = orch
            .process_state
            .processes
            .lock()
            .ok()
            .and_then(|p| p.get(run_id).map(|proc| proc.terminal_tool_called.clone()))
            .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));

        // EOF-classification signals, observed purely within this reader thread
        // (set in the loop, read at EOF in the same thread) — plain locals, no
        // atomics needed. `saw_terminal_result` makes a Result-then-EOF a clean
        // completion regardless of occupancy; the rate-limit flags drive the
        // recoverable-exit path and the crash diagnostic.
        let mut saw_terminal_result = false;
        let mut saw_rate_limit_event = false;
        let mut saw_blocking_rate_limit = false;
        let mut last_event_kind: &'static str = "none";

        log::trace!("reader_thread: about to read lines");
        for line_result in stdout.lines() {
            let line = match line_result {
                Ok(l) => {
                    lines_read += 1;
                    if !first_stdout_logged {
                        let mut event =
                            crate::resume_timing::ResumeTimingEvent::new("claude_first_stdout")
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
                        let mut timing = crate::resume_timing::ResumeTimingEvent::new(
                            "claude_first_parsed_event",
                        )
                        .elapsed(parse_started);
                        timing.run_id = Some(run_id);
                        timing.session_id = session_id.as_deref();
                        timing.count = Some(parse_metrics.failures);
                        timing.emit();
                        first_event_logged = true;
                    }
                    last_event_kind = claude_event_kind(&event);
                    if let Ok(mut watchdog) = progress_watchdog.lock() {
                        let now = Instant::now();
                        let current_turn = orch.process_state.get_current_turn_id(run_id);
                        watchdog.observe_turn(current_turn.as_deref(), now);
                        watchdog.record_forward_progress(now);
                    }
                    // A terminal Result seen at any point means the turn completed;
                    // capture it before any branch so a later EOF finalizes Exited.
                    if matches!(&event, ClaudeEvent::Result { .. }) {
                        saw_terminal_result = true;
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
                            let orch_capture = orch.clone();
                            let run_id_capture = run_id.to_string();
                            match run_backend_db(CLAUDE_BACKEND_NAME, async move {
                                crate::mcp::handlers::comments_artifacts::capture_call_structured_output(
                                    &orch_capture,
                                    &run_id_capture,
                                    structured,
                                )
                                .await
                            }) {
                                Ok(_) => {}
                                Err(e) => log::warn!(
                                    "Failed to capture structured_output for run {}: {}",
                                    &run_id[..run_id.len().min(8)],
                                    e
                                ),
                            }
                        }
                    }

                    if let ClaudeEvent::System { subtype, data, .. } = &event {
                        if subtype == "init" {
                            if validate_claude_init(data, &[]).is_err() {
                                // Close the child first so both output pipes reach
                                // EOF, then join stderr. Snapshotting the parallel
                                // drain here would race the actionable MCP error.
                                orch.process_state.stop_and_remove(run_id);
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
                                insert_error_event(orch, run_id, session_id.as_deref(), &error);
                                crate::orchestrator::lifecycle::finalize_run(
                                    orch,
                                    run_id,
                                    RunStatus::Crashed,
                                );
                                break;
                            }
                        }
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
                                        backend_id_confirmed = true;
                                    }
                                } else {
                                    backend_id_confirmed = true;
                                }
                            }
                        }
                    }

                    // The `init` event reports the tools the CLI actually
                    // declared to the model, which is the only direct evidence
                    // of what `--disallowedTools` failed to suppress.
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
                            let leaked =
                                crate::agent_process::toolkits::record_declared_surface(&declared);
                            if !leaked.is_empty() {
                                log::warn!(
                                    "Claude CLI declared {} tool(s) outside Cairn's MCP surface: {}. \
                                     Quarantined for later sessions in this process; add them to \
                                     ALWAYS_DISALLOWED_TOOLS (cairn-db models/toolkit.rs) to keep \
                                     them off the first session after a restart.",
                                    leaked.len(),
                                    leaked.join(", "),
                                );
                            }
                        }
                    }

                    // Handle control responses
                    if let ClaudeEvent::ControlResponse {
                        request_id,
                        response,
                    } = &event
                    {
                        use crate::agent_process::stream::ControlResponseInner;
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
                        saw_rate_limit_event = true;
                        if rate_limit_info.is_blocking() {
                            saw_blocking_rate_limit = true;
                        }
                        log::info!(
                            "Rate-limit event for run {}: status={} type={:?} resets_at={:?} overage_resets_at={:?}",
                            &run_id[..run_id.len().min(8)],
                            rate_limit_info.status,
                            rate_limit_info.rate_limit_type,
                            rate_limit_info.resets_at,
                            rate_limit_info.overage_resets_at,
                        );
                        let snapshot = claude_rate_limit_snapshot(rate_limit_info);
                        orch.store_provider_usage_snapshot(snapshot.clone());
                        if rate_limit_info.is_blocking() {
                            if let Ok(Some(target)) = rate_limit_retry_target(&run_db, run_id) {
                                let blocked_until = rate_limit_info
                                    .resets_at
                                    .or(rate_limit_info.overage_resets_at);
                                if let Err(error) = orch.record_account_health(
                                    &target.account_id,
                                    snapshot.windows,
                                    blocked_until,
                                ) {
                                    log::warn!("Failed to record Claude account block: {error}");
                                }
                            }
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
                                        orch,
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
                                orch,
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
                            StreamEventInner::MessageStart { .. } => {
                                let current_turn = orch.process_state.get_current_turn_id(run_id);
                                if !first_message_start_logged {
                                    let mut timing = crate::resume_timing::ResumeTimingEvent::new(
                                        "claude_first_message_start",
                                    );
                                    timing.run_id = Some(run_id);
                                    timing.session_id = session_id.as_deref();
                                    timing.turn_id = current_turn.as_deref();
                                    timing.emit();
                                    first_message_start_logged = true;
                                }
                                if streaming_state.is_some() {
                                    log::warn!("New MessageStart while a stream is still active");
                                    finalize_streaming_message(
                                        orch,
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
                                                orch,
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
                                        let _ = emitter.emit(
                                            "db-change",
                                            crate::notify::event_db_change_for_run(
                                                orch.db.local.clone(),
                                                run_id,
                                                session_id.as_deref(),
                                                "insert",
                                            ),
                                        );
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
                                if let (Some(ref mut state), Some(content_block)) =
                                    (streaming_state.as_mut(), content_block.as_ref())
                                {
                                    if let Some((id, name)) = extract_tool_start(content_block) {
                                        state.start_tool_write(id, name);
                                        let delta = state.acc.take_emit_delta();
                                        emit_streaming_delta(
                                            orch,
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
                                        Some(("content", text.len()))
                                    }
                                    DeltaContent::ThinkingDelta { thinking }
                                        if !thinking.is_empty() =>
                                    {
                                        Some(("thinking", thinking.len()))
                                    }
                                    _ => None,
                                };
                                if !first_nonempty_delta_logged {
                                    if let Some((mode, bytes)) = delta_trace {
                                        let mut timing =
                                            crate::resume_timing::ResumeTimingEvent::new(
                                                "claude_first_nonempty_delta",
                                            );
                                        timing.run_id = Some(run_id);
                                        timing.session_id = session_id.as_deref();
                                        timing.mode = Some(mode);
                                        timing.bytes = Some(bytes);
                                        timing.emit();
                                        first_nonempty_delta_logged = true;
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
                                                orch,
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
                                            orch,
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
                                            orch,
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
                                        orch,
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
                            orch,
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
                            let _ = emitter.emit(
                                "db-change",
                                crate::notify::event_db_change_for_run(
                                    orch.db.local.clone(),
                                    run_id,
                                    session_id.as_deref(),
                                    "update",
                                ),
                            );
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
                        if let ClaudeEvent::Result { is_error, .. } = &event {
                            if *is_error {
                                log::info!(
                                    "Ignoring Result.is_error for run {} because a terminal tool already completed the turn",
                                    &run_id[..run_id.len().min(8)]
                                );
                            }

                            let is_task_spawned =
                                is_task_spawned_run(CLAUDE_BACKEND_NAME, &run_db, run_id);

                            if is_task_spawned {
                                crate::orchestrator::lifecycle::finalize_run(
                                    orch,
                                    run_id,
                                    RunStatus::Exited,
                                );
                                orch.process_state.transition_to_warm(run_id);
                            } else {
                                crate::orchestrator::lifecycle::transition_to_warm_state(
                                    orch, run_id, None,
                                );
                                let _ = emitter.emit(
                                    "run-turn-completed",
                                    serde_json::json!({
                                        "run_id": run_id,
                                        "is_warm": true,
                                    }),
                                );
                            }
                        }
                        continue;
                    }

                    // Finalize streaming placeholder before Result event
                    if matches!(&event, ClaudeEvent::Result { .. }) {
                        finalize_streaming_message(
                            orch,
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
                        watchdog.set_pending_tool_count(pending_tool_ids.len());
                    }

                    // Handle Assistant events during streaming
                    let is_assistant = matches!(&event, ClaudeEvent::Assistant { .. });
                    let has_content =
                        transcript_event.content.is_some() || transcript_event.tool_uses.is_some();
                    let has_thinking = transcript_event.thinking.is_some();

                    // Partial Assistant event (thinking complete, no content yet)
                    if streaming_state.is_some() && is_assistant && has_thinking && !has_content {
                        if let Some(ref mut state) = streaming_state {
                            state.capture_thinking_done();
                            state.record_thinking_started(last_thinking_tokens);
                            let delta = state.acc.take_emit_delta();
                            emit_streaming_delta(
                                orch,
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
                                orch,
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
                    if (was_host_interrupted(orch, run_id) || terminal_interrupt_ack_pending)
                        && !matches!(&event, ClaudeEvent::Result { .. })
                    {
                        // The genuine assistant message that issued the
                        // now-interrupted tool call may still be parked for
                        // deferred finalization (its `message_delta` never arrived
                        // before the abort). Commit it before discarding the
                        // interrupt-notice stream (CAIRN-1611).
                        flush_pending_assistant_before_suppress(
                            orch,
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
                            let _ = emitter.emit(
                                "db-change",
                                crate::notify::event_db_change_for_run(
                                    orch.db.local.clone(),
                                    run_id,
                                    session_id.as_deref(),
                                    "update",
                                ),
                            );
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
                            if let ClaudeEvent::Result { .. } = &event {
                                if let Some(usage) = pending_delta_usage.as_ref() {
                                    emit_claude_context_snapshot(
                                        orch,
                                        run_id,
                                        session_id.as_deref(),
                                        usage,
                                    );
                                }
                            }
                        }

                        let current_turn = orch.process_state.get_current_turn_id(run_id);
                        let inserted = crate::transcripts::stream_store::insert_event_emit(
                            run_db.clone(),
                            emitter,
                            EventInsert {
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
                            },
                        );
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
                            // Embed events for vibe coloring (agent content) and
                            // session position (user / agent / change feeds).
                            // Position needs a session id to key on; without one
                            // we still color agent events.
                            if let Some(session) = session_id.as_deref() {
                                match event_type.as_str() {
                                    "assistant" => {
                                        if let Some(text) =
                                            crate::embeddings::extract_embeddable_text(&data)
                                        {
                                            orch.enqueue_position_embed(
                                                session,
                                                &event_id,
                                                crate::embeddings::PositionKind::Agent,
                                                text,
                                                event_counts.output,
                                            );
                                        }
                                        if let Some(signal) =
                                            crate::embeddings::extract_change_signal_text(&data)
                                        {
                                            orch.enqueue_position_embed(
                                                session,
                                                &event_id,
                                                crate::embeddings::PositionKind::Change,
                                                signal,
                                                event_counts.output,
                                            );
                                        }
                                    }
                                    "user" => {
                                        if let Some(text) =
                                            crate::embeddings::extract_embeddable_text(&data)
                                        {
                                            orch.enqueue_position_embed(
                                                session,
                                                &event_id,
                                                crate::embeddings::PositionKind::User,
                                                text,
                                                event_counts.input,
                                            );
                                        }
                                    }
                                    _ => {}
                                }
                            } else if event_type == "assistant" {
                                if let Some(text) =
                                    crate::embeddings::extract_embeddable_text(&data)
                                {
                                    orch.enqueue_event_embed(&event_id, text);
                                }
                            }
                        }

                        // Todos are written through the `write` tool against a job's
                        // todos URI and stored directly in the todos table — no event
                        // sniffing needed.
                    }

                    // Check for turn completion
                    if let ClaudeEvent::Result { is_error, data, .. } = &event {
                        // The terminal-tool boundary interrupt's ack is the next
                        // Result after we sent it. Consume the expectation on any
                        // Result so it can never leak into a later turn's outcome.
                        let swallow_terminal_ack = *is_error && terminal_interrupt_ack_pending;
                        terminal_interrupt_ack_pending = false;
                        if swallow_terminal_ack {
                            // This Result is the ack of the terminal-tool boundary
                            // interrupt we sent to end the *work* turn. That turn was
                            // already completed and warmed at the boundary, and the
                            // post-completion memory-review turn may have since resumed
                            // on this same warm process. Swallow the ack so it is not
                            // misread as a crash of the now-current (review) turn —
                            // which would mark that turn Interrupted and trip premature
                            // review completion (CAIRN-1576).
                            log::info!(
                                "Swallowed terminal-tool interrupt ack for run {} (work turn already warmed)",
                                &run_id[..run_id.len().min(8)]
                            );
                        } else if *is_error {
                            // Host-initiated interrupts (durable dependency wait via
                            // suspend_run_for_durable_wait, or user stop_session) flip the
                            // process occupancy to Idle before Claude's interrupt-induced
                            // Result{is_error:true} arrives. Treat that as a warm
                            // transition, not a crash — mirrors Codex's
                            // handle_codex_interrupted_turn.
                            if was_host_interrupted(orch, run_id) {
                                log::info!(
                                    "Result.is_error for run {} was a host-initiated interrupt; leaving run live and warm",
                                    &run_id[..run_id.len().min(8)]
                                );
                                crate::orchestrator::lifecycle::transition_to_warm_state(
                                    orch, run_id, None,
                                );
                                let _ = emitter.emit(
                                    "run-turn-completed",
                                    serde_json::json!({
                                        "run_id": run_id,
                                        "is_warm": true,
                                    }),
                                );
                            } else {
                                insert_error_event(
                                    orch,
                                    run_id,
                                    session_id.as_deref(),
                                    terminal_result_error_message(data),
                                );
                                crate::orchestrator::lifecycle::finalize_run(
                                    orch,
                                    run_id,
                                    RunStatus::Crashed,
                                );
                            }
                        } else {
                            // Check if this is a task-spawned run (has parent_job_id)
                            let is_task_spawned =
                                is_task_spawned_run(CLAUDE_BACKEND_NAME, &run_db, run_id);

                            if is_task_spawned {
                                crate::orchestrator::lifecycle::finalize_run(
                                    orch,
                                    run_id,
                                    RunStatus::Exited,
                                );
                                orch.process_state.transition_to_warm(run_id);
                                log::info!(
                                    "Task-spawned run {} completed and finalized",
                                    &run_id[..run_id.len().min(8)]
                                );
                            } else {
                                crate::orchestrator::lifecycle::transition_to_warm_state(
                                    orch, run_id, None,
                                );

                                let _ = emitter.emit(
                                    "run-turn-completed",
                                    serde_json::json!({
                                        "run_id": run_id,
                                        "is_warm": true,
                                    }),
                                );

                                log::info!(
                                    "Turn completed for run {}, process now warm",
                                    &run_id[..run_id.len().min(8)]
                                );
                            }
                        }
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
                        if let Err(error) =
                            crate::backends::stdin::send_interrupt(&orch.process_state, run_id)
                        {
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
                        crate::orchestrator::lifecycle::transition_to_warm_state(
                            orch,
                            run_id,
                            Some(crate::models::TurnEndReason::ArtifactHandoff),
                        );
                        let _ = emitter.emit(
                            "run-turn-completed",
                            serde_json::json!({
                                "run_id": run_id,
                                "is_warm": true,
                            }),
                        );
                    }
                }
                Err(e) => {
                    log::warn!("Failed to parse event: {} - line: {}", e, line);
                }
            }
        }

        watchdog_alive.store(false, Ordering::Release);
        let mut parse_summary =
            crate::resume_timing::ResumeTimingEvent::new("claude_stream_parse_summary");
        parse_summary.run_id = Some(run_id);
        parse_summary.session_id = session_id.as_deref();
        parse_summary.duration_us = Some(parse_metrics.duration.as_micros());
        parse_summary.parse_attempts = Some(parse_metrics.attempts);
        parse_summary.parse_failures = Some(parse_metrics.failures + read_failures);
        parse_summary.emit();

        // Finalize any remaining durable stream on EOF
        finalize_streaming_message(
            orch,
            &run_db,
            run_id,
            session_id.as_deref(),
            &mut streaming_state,
            pending_final_assistant_event.take(),
            TokenCounts::from_optional_usage(pending_delta_usage.as_ref()),
        );

        log::debug!("reader_thread: loop ended after {} lines", lines_read);

        // Stdout closed - process has terminated.
        //
        // A genuine self-crash still has its RunHandle in the registry at this
        // point — the reader thread itself removes it below, only after this
        // classification. So if the handle is already gone, the orchestrator
        // removed it via an intentional kill (warm-process eviction, user stop,
        // model-change restart). That is a host-initiated termination, not a
        // crash: finalizing it as `Exited` leaves a suspended (Yielded) turn
        // intact so a delegated-wait parent stays resumable, instead of being
        // marked failed and cascading failure to downstream nodes.
        let was_warm = was_host_interrupted(orch, run_id)
            || orch.process_state.get_occupancy(run_id).is_none();
        let run_status_val = run_status(CLAUDE_BACKEND_NAME, &run_db, run_id);
        let terminal_tool_called = terminal_tool_flag.load(std::sync::atomic::Ordering::Acquire);
        let task_spawned = is_task_spawned_run(CLAUDE_BACKEND_NAME, &run_db, run_id);
        // Settle the stderr drain BEFORE classifying, so a diagnosis the process
        // printed on its way out (e.g. an unresolvable resume handle) is
        // available to every arm rather than racing this thread's exit.
        let backend_failure = stderr_watch.and_then(StderrWatch::settle);

        match classify_eof(
            was_warm,
            saw_terminal_result,
            run_status_val.as_deref(),
            terminal_tool_called,
            task_spawned,
        ) {
            EofVerdict::Exited => {
                log::info!(
                    "Process {} reached EOF in a completed state, finalizing as Exited",
                    &run_id[..run_id.len().min(8)]
                );
                crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Exited);
            }
            EofVerdict::Crashed => {
                // Structured diagnostic: the next 04:22-style failure must be
                // attributable to a specific job/turn/execution and to whether a
                // rate-limit event preceded the exit. (Child exit status is not
                // available at stdout-EOF in the reader thread; run_status +
                // last_event are the actionable signals here.) `finalize_run`
                // interrupts the still-running turn rather than failing it, so the
                // job stays resumable: a blocking rate-limit exit recovers on
                // reset, and a genuine crash recovers via a follow-up message.
                let (job_id, execution_id) = run_job_execution(&run_db, run_id);
                let turn_id = orch.process_state.get_current_turn_id(run_id);
                log::warn!(
                    "Claude process EOF without terminal result — finalizing Crashed (running turn interrupted, resumable). \
                     run_id={run_id} job_id={job_id:?} execution_id={execution_id:?} \
                     turn_id={turn_id:?} last_event={last_event_kind} \
                     saw_rate_limit_event={saw_rate_limit_event} \
                     saw_blocking_rate_limit={saw_blocking_rate_limit} run_status={run_status_val:?}"
                );

                if saw_blocking_rate_limit {
                    if let Ok(Some(target)) = rate_limit_retry_target(&run_db, run_id) {
                        if let Some((replacement_id, _)) = orch.select_routed_identity(
                            crate::identity::RoutedProvider::Claude,
                            target.project_id.as_deref(),
                            None,
                            Some(&target.account_id),
                        ) {
                            let old_label = orch
                                .list_accounts(target.project_id.as_deref())
                                .into_iter()
                                .find(|account| account.id == target.account_id)
                                .map(|account| account.label)
                                .unwrap_or_else(|| target.account_id.clone());
                            let new_label = orch
                                .list_accounts(target.project_id.as_deref())
                                .into_iter()
                                .find(|account| account.id == replacement_id)
                                .map(|account| account.label)
                                .unwrap_or_else(|| replacement_id.clone());
                            let repinned = crate::storage::run_db_blocking({
                                let db = run_db.clone();
                                let session_id = target.session_id.clone();
                                let replacement_id = replacement_id.clone();
                                move || async move {
                                    crate::sessions::queries::set_account_id(
                                        &db,
                                        &session_id,
                                        &replacement_id,
                                    )
                                    .await
                                }
                            });
                            if repinned.is_ok() {
                                // Remove the exhausted process before launching its immediate successor.
                                if let Ok(mut processes) = orch.process_state.processes.lock() {
                                    processes.remove(run_id);
                                }
                                crate::orchestrator::lifecycle::fail_run(
                                    orch,
                                    run_id,
                                    "rate_limit_switch",
                                );
                                if let Ok(Some(retry_turn_id)) =
                                    crate::execution::jobs::claim_retry_successor_if_head_matches(
                                        orch,
                                        run_db.clone(),
                                        &target.job_id,
                                        &target.session_id,
                                        &target.turn_id,
                                    )
                                {
                                    let message = format!("Rate limit reached on {old_label}. Continuing on {new_label}.");
                                    let _ = crate::messages::transcript::insert_system_message_sync(
                                        orch,
                                        run_id,
                                        session_id.as_deref(),
                                        Some(&target.turn_id),
                                        &message,
                                        serde_json::json!({"provider":"claude","kind":"rate_limit_account_switch","fromAccountId":target.account_id,"toAccountId":replacement_id}),
                                    );
                                    if let Err(error) =
                                        crate::execution::jobs::continue_automatic_retry(
                                            orch,
                                            &target.job_id,
                                            &retry_turn_id,
                                        )
                                    {
                                        log::warn!(
                                            "Claude account failover retry did not launch: {error}"
                                        );
                                        let _ = crate::execution::jobs::abandon_pending_retry_if_head_matches(run_db.clone(), &target.job_id, &retry_turn_id);
                                    }
                                    return;
                                }
                                return;
                            }
                        }
                    }
                }

                let error_message = if saw_blocking_rate_limit {
                    "Process exited after the account rate limit was reached, before completing the turn. The turn is interrupted and resumable once the limit resets."
                } else if backend_failure == Some(BackendFailure::SessionUnresolvable) {
                    // Deliberately descriptive rather than promissory: the
                    // digest-reseed fallback in `finalize_run` may decline (an
                    // already-attempted session, an active head turn), so this
                    // text must not guarantee a recovery.
                    "The provider could not resolve this session's conversation handle, so the resume produced no output. Cairn's own transcript is intact."
                } else {
                    "Process terminated unexpectedly without completing"
                };
                insert_error_event(orch, run_id, session_id.as_deref(), error_message);

                // Record the typed diagnosis on the run before finalizing, so
                // the lifecycle reaction reads a durable fact off `runs` rather
                // than re-parsing provider text. Mirrors `fail_run`'s ordering.
                if let Some(failure) = backend_failure {
                    if let Err(e) = crate::orchestrator::lifecycle::set_exit_reason(
                        orch,
                        run_id,
                        failure.exit_reason(),
                    ) {
                        log::warn!("Failed to record exit reason for run {}: {}", run_id, e);
                    }
                }

                crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Crashed);
            }
            EofVerdict::AlreadyTerminal => {}
        }

        // Cleanup process handle
        if let Ok(mut processes) = orch.process_state.processes.lock() {
            processes.remove(run_id);
            log::debug!(
                "Removed process {} from process map",
                &run_id[..run_id.len().min(8)]
            );
        }
    }
}

#[cfg(test)]
mod launch_contract_tests {
    use super::{build_claude_launch_contract, sanitize_mcp_diagnostic, validate_claude_init};
    use crate::agent_process::args::{build_claude_args, ClaudeArgsConfig};
    use crate::backends::SessionStart;
    use crate::identity::{ClaudeAuth, UserIdentity};

    const MCP_CONFIG: &str =
        r#"{"mcpServers":{"cairn":{"command":"cairn-cmd","args":["mcp-serve"]}}}"#;

    fn identity(auth: ClaudeAuth) -> UserIdentity {
        UserIdentity {
            user_id: "user-1".to_string(),
            email: "agent@example.com".to_string(),
            name: "Cairn Agent".to_string(),
            claude_auth: Some(auth),
            codex_auth: None,
            github_token: None,
        }
    }

    fn args() -> Vec<String> {
        build_claude_args(&ClaudeArgsConfig {
            mcp_config: MCP_CONFIG.to_string(),
            skip_permissions: false,
            model: None,
            session_start: SessionStart::New {
                session_id: "session-1".to_string(),
            },
            prompt: "test".to_string(),
            effort: None,
            allowed_tools: crate::agent_process::toolkits::CORE_VERBS
                .iter()
                .map(|verb| verb.to_string())
                .collect(),
            disallowed_tools: vec![],
            system_prompt_file: None,
            settings_path: None,
            bidirectional: false,
            json_schema: None,
        })
    }

    fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
        args.iter()
            .position(|argument| argument == flag)
            .and_then(|index| args.get(index + 1))
            .map(String::as_str)
    }

    #[test]
    fn profile_launch_keeps_inline_mcp_and_uses_only_managed_auth() {
        let profile = std::path::PathBuf::from("/managed/claude/profile");
        let launch = build_claude_launch_contract(
            args(),
            "run-1",
            "mcp-secret",
            &identity(ClaudeAuth::ConfigDir(profile.clone())),
            None,
        )
        .unwrap();

        assert_eq!(flag_value(&launch.args, "--mcp-config"), Some(MCP_CONFIG));
        assert!(launch.args.iter().any(|arg| arg == "--strict-mcp-config"));
        assert_eq!(
            launch.env.get("CLAUDE_CONFIG_DIR").map(String::as_str),
            profile.to_str()
        );
        assert!(!launch.env.contains_key("ANTHROPIC_API_KEY"));
        assert!(!launch.env.contains_key("CLAUDE_CODE_OAUTH_TOKEN"));
        assert_eq!(
            launch.env.get("CAIRN_MCP_SECRET").map(String::as_str),
            Some("mcp-secret")
        );
        assert_eq!(
            launch.env.get("CAIRN_RUN_ID").map(String::as_str),
            Some("run-1")
        );
    }

    #[test]
    fn api_key_launch_retains_the_same_strict_inline_mcp_contract() {
        let launch = build_claude_launch_contract(
            args(),
            "run-1",
            "mcp-secret",
            &identity(ClaudeAuth::ApiKey("stored-key".to_string())),
            Some("brokered-key"),
        )
        .unwrap();
        assert_eq!(flag_value(&launch.args, "--mcp-config"), Some(MCP_CONFIG));
        assert!(launch.args.iter().any(|arg| arg == "--strict-mcp-config"));
        assert_eq!(
            launch.env.get("ANTHROPIC_API_KEY").map(String::as_str),
            Some("brokered-key")
        );
        assert!(!launch.env.contains_key("CLAUDE_CONFIG_DIR"));
    }

    #[test]
    fn init_with_cairn_tools_succeeds() {
        let init = serde_json::json!({
            "tools": ["mcp__cairn__read", "mcp__cairn__write", "mcp__cairn__run"],
            "mcp_servers": [{"name": "cairn", "status": "connected"}]
        });
        assert!(validate_claude_init(&init, &[]).is_ok());
    }

    #[test]
    fn init_with_connected_server_but_incomplete_inventory_fails_clearly() {
        for tools in [
            serde_json::json!([]),
            serde_json::json!(["Read", "mcp__unrelated__search"]),
            serde_json::json!(["mcp__cairn__read"]),
            serde_json::json!(["mcp__cairn__read", "mcp__cairn__write"]),
        ] {
            let error = validate_claude_init(
                &serde_json::json!({
                    "tools": tools,
                    "mcp_servers": [{"name": "cairn", "status": "connected"}]
                }),
                &[],
            )
            .unwrap_err();
            assert!(error.contains("without the required Cairn MCP tools"));
            assert!(error.contains("connected"));
        }
    }

    #[test]
    fn sanitized_mcp_stderr_is_surfaced_in_init_failure() {
        // This value is deliberately NOT registered. Profile-scoped Claude can
        // print credentials Cairn never held, so exact-value scrubbing is not
        // sufficient at this untrusted stderr boundary.
        let secret = "unregistered-profile-token-4Qf8Jv2Kp9";
        let diagnostic = sanitize_mcp_diagnostic(&format!(
            "MCP server cairn failed with authorization: Bearer {secret}"
        ));
        assert!(!diagnostic.contains(secret));
        let error = validate_claude_init(
            &serde_json::json!({"tools": [], "mcp_servers": []}),
            &[diagnostic],
        )
        .unwrap_err();
        assert!(error.contains("MCP server cairn failed"));
        assert!(!error.contains(secret));
    }
}

#[cfg(test)]
mod stderr_failure_tests {
    use super::{classify_stderr_failure, BackendFailure, StderrWatch};

    fn watch_over(lines: &str) -> Option<BackendFailure> {
        StderrWatch::spawn(Box::new(std::io::Cursor::new(lines.to_string()))).settle()
    }

    #[test]
    fn settle_reports_a_diagnosis_the_process_printed_before_exiting() {
        // The join in `settle` is what makes this deterministic: without it the
        // reader thread could classify EOF before the drain recorded the line.
        assert_eq!(
            watch_over(
                "(node:1) ExperimentalWarning: stream/web\n\
                 No conversation found with session ID: 5c7fe52a-b0bd-4f3b-8bde-d7af9a2b2b93"
            ),
            Some(BackendFailure::SessionUnresolvable)
        );
    }

    #[test]
    fn settle_reports_nothing_for_a_silent_or_ordinary_exit() {
        assert_eq!(watch_over(""), None);
        assert_eq!(watch_over("some unrelated warning\nand another"), None);
    }

    /// CAIRN-3104: the stderr drain must be settled BEFORE the EOF verdict is
    /// classified, so every arm sees a diagnosis the process printed on its way
    /// out. Guarded structurally because the ordering is load-bearing and
    /// hoisting the settle below the match would silently reintroduce the race
    /// between the drain and this thread's exit.
    #[test]
    fn the_stderr_drain_is_settled_before_the_eof_verdict() {
        const SOURCE: &str = include_str!("claude.rs");
        let start = SOURCE
            .find("fn reader_thread")
            .expect("reader_thread present in source");
        let body = &SOURCE[start..];
        let settle = body
            .find("StderrWatch::settle")
            .expect("stderr settle call present");
        let classify = body
            .find("match classify_eof(")
            .expect("classify_eof dispatch present");
        assert!(
            settle < classify,
            "reader_thread: the stderr drain must settle before classify_eof (cairn-3104)"
        );
    }

    #[test]
    fn the_cli_resume_miss_classifies_as_session_unresolvable() {
        // Verbatim from the runner log that opened CAIRN-3104.
        assert_eq!(
            classify_stderr_failure(
                "No conversation found with session ID: 5c7fe52a-b0bd-4f3b-8bde-d7af9a2b2b93"
            ),
            Some(BackendFailure::SessionUnresolvable)
        );
    }

    #[test]
    fn ordinary_stderr_classifies_as_nothing() {
        assert_eq!(classify_stderr_failure(""), None);
        assert_eq!(
            classify_stderr_failure("(node:12345) ExperimentalWarning: stream/web is experimental"),
            None
        );
        // Merely naming a session is not this failure.
        assert_eq!(
            classify_stderr_failure("Resuming session ID: 5c7fe52a-b0bd-4f3b-8bde-d7af9a2b2b93"),
            None
        );
    }
}

#[cfg(test)]
mod terminal_tool_tests {
    use super::{classify_eof, should_finalize_task_run_on_terminal_tool_eof, EofVerdict};

    #[test]
    fn terminal_tool_task_eof_is_treated_as_completed() {
        assert!(should_finalize_task_run_on_terminal_tool_eof(
            true,
            Some("running"),
            true
        ));
        assert!(!should_finalize_task_run_on_terminal_tool_eof(
            false,
            Some("running"),
            true
        ));
        assert!(!should_finalize_task_run_on_terminal_tool_eof(
            true,
            Some("exited"),
            true
        ));
        assert!(!should_finalize_task_run_on_terminal_tool_eof(
            true,
            Some("running"),
            false
        ));
    }

    #[test]
    fn classify_eof_warm_is_exited() {
        // Host-warmed (interrupt/eviction) closes stdout: completed, not a crash.
        assert_eq!(
            classify_eof(true, false, Some("running"), false, false),
            EofVerdict::Exited
        );
    }

    #[test]
    fn classify_eof_terminal_result_then_eof_is_exited() {
        // A run that emitted a terminal Result and then closed stdout completed,
        // regardless of occupancy — belt-and-suspenders over the warm path.
        assert_eq!(
            classify_eof(false, true, Some("running"), false, false),
            EofVerdict::Exited
        );
    }

    #[test]
    fn classify_eof_no_result_running_not_warm_is_crashed() {
        // No terminal Result and still running → Crashed, which interrupts the
        // running turn (leaving the job resumable). A blocking rate-limit exit
        // lands here too: the rate-limit signal refines the error message, not
        // the verdict — finalizing Exited would wrongly complete the turn and
        // advance downstream onto work the blocked account never finished.
        assert_eq!(
            classify_eof(false, false, Some("running"), false, false),
            EofVerdict::Crashed
        );
    }

    #[test]
    fn classify_eof_task_spawned_terminal_tool_is_exited() {
        assert_eq!(
            classify_eof(false, false, Some("running"), true, true),
            EofVerdict::Exited
        );
    }

    #[test]
    fn classify_eof_already_terminal_when_not_running() {
        assert_eq!(
            classify_eof(false, false, Some("exited"), false, false),
            EofVerdict::AlreadyTerminal
        );
    }
}

/// CAIRN-1611: a fully-emitted assistant message parked for deferred
/// finalization must be committed, not dropped, when a terminal-tool or
/// host-interrupt guard suppresses the in-flight stream.
#[cfg(test)]
mod flush_pending_tests {
    use super::{
        claude_event_kind, flush_pending_assistant_before_suppress, merge_pending_assistant,
        ClaudeBackend, StreamingState,
    };
    use crate::agent_process::stream::{parse_event, ToolUseInfo, TranscriptEvent};
    use crate::db::DbState;
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS};
    use crate::transcripts::stream_store::open_stream;
    use cairn_db::turso::params;
    use std::sync::Arc;

    async fn test_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("flush-pending-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    fn build_orch(db: LocalDb) -> Orchestrator {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    async fn insert_run(orch: &Orchestrator, id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        orch.db
            .local
            .write(|conn| {
                let id = id.to_string();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO runs(
                             id, status, session_id, created_at, updated_at
                         )
                         VALUES (?1, 'live', 'session-1', ?2, ?3)",
                        params![id.as_str(), now, now],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    fn write_tool_assistant(tool_use_id: &str) -> TranscriptEvent {
        TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some("session-1".to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: Some(vec![ToolUseInfo {
                id: tool_use_id.to_string(),
                name: "mcp__cairn__write".to_string(),
                input: serde_json::json!({ "changes": [] }),
            }]),
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            queued_message_id: None,
            raw: None,
        }
    }

    fn read_tool_assistant(tool_use_id: &str) -> TranscriptEvent {
        TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some("session-1".to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: Some(vec![ToolUseInfo {
                id: tool_use_id.to_string(),
                name: "mcp__cairn__read".to_string(),
                input: serde_json::json!({ "paths": [] }),
            }]),
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            queued_message_id: None,
            raw: None,
        }
    }

    async fn count_events(orch: &Orchestrator, run_id: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM events WHERE run_id = '{run_id}'");
        orch.db
            .local
            .query_one(sql.as_str(), (), |row| row.i64(0))
            .await
            .unwrap()
    }

    async fn count_assistant_with_tooluse(
        orch: &Orchestrator,
        run_id: &str,
        tool_use_id: &str,
    ) -> i64 {
        let sql = format!(
            "SELECT COUNT(*) FROM events \
             WHERE run_id = '{run_id}' AND event_type = 'assistant' \
             AND data LIKE '%{tool_use_id}%'"
        );
        orch.db
            .local
            .query_one(sql.as_str(), (), |row| row.i64(0))
            .await
            .unwrap()
    }

    /// Every persisted sequence for a run, in ascending order.
    async fn sequences(orch: &Orchestrator, run_id: &str) -> Vec<i64> {
        let sql =
            format!("SELECT sequence FROM events WHERE run_id = '{run_id}' ORDER BY sequence ASC");
        orch.db
            .local
            .query_all(sql, (), |row| row.i64(0))
            .await
            .unwrap()
    }

    /// One streamed, tool-using turn in the wire order that produced CAIRN-3290.
    ///
    /// The consolidated `assistant` event arrives BEFORE its trailing
    /// `message_delta` (the order Opus-class models emit), so it parks; the
    /// `tool_result` for its tool call then lands while the stream row is still
    /// open holding a reserved slot. That interleaving is what made the old
    /// counter hand the same number to two events and skip the next.
    fn streamed_tool_turn_fixture() -> String {
        [
            r#"{"type":"system","subtype":"init","session_id":"session-1","cwd":"/tmp","model":"claude-opus-4","tools":["mcp__cairn__read","mcp__cairn__write","mcp__cairn__run"],"mcp_servers":[{"name":"cairn","status":"connected"}]}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"message_start","message":{"id":"msg_01","role":"assistant","model":"claude-opus-4"}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Reading the file."}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_stop","index":0}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_01","name":"mcp__cairn__read","input":{}}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"paths\":[\"file:src/lib.rs\"]}"}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"content_block_stop","index":1}}"#,
            r#"{"type":"assistant","uuid":"evt-assistant","session_id":"session-1","message":{"role":"assistant","content":[{"type":"text","text":"Reading the file."},{"type":"tool_use","id":"toolu_01","name":"mcp__cairn__read","input":{"paths":["file:src/lib.rs"]}}]}}"#,
            r#"{"type":"user","uuid":"evt-tool-result","session_id":"session-1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01","content":"pub fn main() {}"}]}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"input_tokens":11,"output_tokens":22,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}"#,
            r#"{"type":"stream_event","session_id":"session-1","event":{"type":"message_stop"}}"#,
            r#"{"type":"result","subtype":"success","session_id":"session-1","is_error":false,"duration_ms":1200,"num_turns":1,"total_cost_usd":0.01,"usage":{"input_tokens":11,"output_tokens":22,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#,
        ]
        .join("\n")
    }

    /// The fixture must keep exercising the branches it claims to. If the wire
    /// shapes drift, this fails loudly rather than letting the regression test
    /// silently run over a stream of `Unknown`.
    #[test]
    fn fixture_parses_into_the_branches_under_test() {
        let kinds: Vec<&'static str> = streamed_tool_turn_fixture()
            .lines()
            .map(|line| claude_event_kind(&parse_event(line).expect("fixture line parses").0))
            .collect();
        assert!(!kinds.contains(&"unknown"));
        assert_eq!(kinds.first().copied(), Some("system"));
        assert_eq!(kinds.last().copied(), Some("result"));
        assert!(kinds.contains(&"assistant"));
        assert!(kinds.contains(&"user"));
        assert!(kinds.contains(&"stream_event"));
    }

    /// CAIRN-3290: the persisted transcript for one streamed tool turn carries
    /// each sequence exactly once, with no gap.
    #[tokio::test]
    async fn reader_thread_persists_each_sequence_exactly_once() {
        let orch = build_orch(test_db().await);
        insert_run(&orch, "run-reader").await;

        let orch_thread = orch.clone();
        let run_db = orch.db.local.clone();
        let emitter = orch.services.emitter.clone();
        let stdout = Box::new(std::io::Cursor::new(
            streamed_tool_turn_fixture().into_bytes(),
        ));
        std::thread::spawn(move || {
            ClaudeBackend::reader_thread(
                &orch_thread,
                &emitter,
                "run-reader",
                Some("session-1".to_string()),
                false,
                stdout,
                None,
                run_db,
            );
        })
        .join()
        .unwrap();

        let sequences = sequences(&orch, "run-reader").await;
        assert_eq!(
            sequences,
            (0..sequences.len() as i64).collect::<Vec<_>>(),
            "sequences must be exactly 0..n — no duplicate, no gap"
        );

        // The turn's four durable events: the init notice, the assistant message
        // that issued the tool call, its result, and the terminal result.
        let types = orch
            .db
            .local
            .query_all(
                "SELECT event_type FROM events WHERE run_id = 'run-reader' ORDER BY sequence ASC",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap();
        assert_eq!(
            types,
            vec![
                "system:init".to_string(),
                "assistant".to_string(),
                "tool_result".to_string(),
                "result:success".to_string(),
            ],
            "the assistant message sorts where it STARTED streaming, ahead of the \
             tool_result that arrived before its trailing message_delta"
        );
    }

    #[tokio::test]
    async fn flush_commits_pending_assistant_paired_with_stream() {
        let orch = build_orch(test_db().await);
        insert_run(&orch, "run-paired").await;
        let opened = open_stream(
            orch.db.local.clone(),
            "run-paired",
            Some("session-1"),
            None,
            "claude",
        )
        .unwrap();
        let mut streaming_state = Some(StreamingState::new(&opened));
        let mut pending = Some(write_tool_assistant("toolu_paired"));

        flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-paired",
            Some("session-1"),
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert!(pending.is_none(), "pending must be cleared after commit");
        assert!(
            streaming_state.is_none(),
            "stream must be consumed by finalize"
        );
        assert_eq!(
            count_assistant_with_tooluse(&orch, "run-paired", "toolu_paired").await,
            1,
            "the tool-call assistant event must be persisted"
        );
    }

    #[tokio::test]
    async fn flush_inserts_directly_when_stream_already_gone() {
        let orch = build_orch(test_db().await);
        insert_run(&orch, "run-orphan").await;
        let mut streaming_state: Option<StreamingState> = None;
        let mut pending = Some(write_tool_assistant("toolu_orphan"));

        flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-orphan",
            Some("session-1"),
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert!(pending.is_none(), "pending must be cleared after insert");
        assert_eq!(
            count_assistant_with_tooluse(&orch, "run-orphan", "toolu_orphan").await,
            1,
            "the tool-call assistant event must be persisted via direct insert"
        );
    }

    #[tokio::test]
    async fn flush_is_noop_without_pending_assistant() {
        let orch = build_orch(test_db().await);
        insert_run(&orch, "run-empty").await;
        let mut streaming_state: Option<StreamingState> = None;
        let mut pending: Option<TranscriptEvent> = None;

        flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-empty",
            Some("session-1"),
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert_eq!(
            count_events(&orch, "run-empty").await,
            0,
            "no pending message means nothing is written"
        );
    }

    // CAIRN-2249: a batched multi-tool turn arrives as two consolidated
    // assistant events before the trailing message_delta. The park slot must
    // merge them rather than overwrite, so every tool call survives.
    #[test]
    fn merge_appends_disjoint_tool_uses() {
        let mut parked = read_tool_assistant("toolu_read");
        let incoming = write_tool_assistant("toolu_write");
        merge_pending_assistant(&mut parked, &incoming);
        let uses = parked.tool_uses.as_ref().unwrap();
        assert_eq!(uses.len(), 2);
        assert_eq!(uses[0].id, "toolu_read");
        assert_eq!(uses[1].id, "toolu_write");
        assert!(
            parked.tool_name.is_none(),
            "multi-tool event must clear the legacy single-tool name"
        );
    }

    #[test]
    fn merge_dedups_cumulative_tool_uses() {
        // The cumulative shape: incoming repeats the parked `read` then adds
        // `write`. Dedup-by-id must keep a single `read`.
        let mut parked = read_tool_assistant("toolu_read");
        let mut incoming = read_tool_assistant("toolu_read");
        incoming.tool_uses = Some(vec![
            ToolUseInfo {
                id: "toolu_read".to_string(),
                name: "mcp__cairn__read".to_string(),
                input: serde_json::json!({ "paths": [] }),
            },
            ToolUseInfo {
                id: "toolu_write".to_string(),
                name: "mcp__cairn__write".to_string(),
                input: serde_json::json!({ "changes": [] }),
            },
        ]);
        merge_pending_assistant(&mut parked, &incoming);
        let uses = parked.tool_uses.as_ref().unwrap();
        assert_eq!(uses.len(), 2, "duplicate read must not be re-added");
        assert_eq!(uses[0].id, "toolu_read");
        assert_eq!(uses[1].id, "toolu_write");
    }

    #[test]
    fn merge_retains_parked_content_when_incoming_empty() {
        let mut parked = read_tool_assistant("toolu_read");
        parked.content = Some("I'll dig".to_string());
        let incoming = write_tool_assistant("toolu_write");
        merge_pending_assistant(&mut parked, &incoming);
        assert_eq!(parked.content.as_deref(), Some("I'll dig"));
    }

    #[test]
    fn merge_keeps_single_tool_legacy_fields() {
        let mut parked = read_tool_assistant("toolu_read");
        let incoming = read_tool_assistant("toolu_read");
        merge_pending_assistant(&mut parked, &incoming);
        let uses = parked.tool_uses.as_ref().unwrap();
        assert_eq!(uses.len(), 1, "the repeated read collapses to one");
        assert_eq!(
            parked.tool_name.as_deref(),
            Some("mcp__cairn__read"),
            "a sole surviving tool repopulates the legacy single-tool name"
        );
    }

    // Reader-level regression: parking a `read` event then merging a `write`
    // event (no pending_delta_usage) and finalizing must persist a single
    // assistant event that carries BOTH tool calls.
    #[tokio::test]
    async fn merge_then_flush_persists_all_batched_tool_calls() {
        let orch = build_orch(test_db().await);
        insert_run(&orch, "run-merge").await;
        let opened = open_stream(
            orch.db.local.clone(),
            "run-merge",
            Some("session-1"),
            None,
            "claude",
        )
        .unwrap();
        let mut streaming_state = Some(StreamingState::new(&opened));

        // Simulate the park site for a batched turn: read parked, write merged.
        let mut pending = Some(read_tool_assistant("toolu_read"));
        let write_event = write_tool_assistant("toolu_write");
        match pending.as_mut() {
            Some(parked) => merge_pending_assistant(parked, &write_event),
            None => pending = Some(write_event.clone()),
        }

        flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-merge",
            Some("session-1"),
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert_eq!(
            count_assistant_with_tooluse(&orch, "run-merge", "toolu_read").await,
            1,
            "the read tool call must survive the batched turn"
        );
        assert_eq!(
            count_assistant_with_tooluse(&orch, "run-merge", "toolu_write").await,
            1,
            "the write tool call must survive the batched turn"
        );
    }
}
