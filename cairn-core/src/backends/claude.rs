//! Claude CLI backend implementation.
//!
//! Handles spawning the Claude CLI process, managing stdin/stdout communication,
//! and reading the stream-json event stream into the database.

use crate::agent_process::args::{build_claude_args, ClaudeArgsConfig};
use crate::agent_process::stream::{
    parse_event, ClaudeEvent, DeltaContent, RateLimitInfo, StreamEventInner, TokenCounts,
    TranscriptEvent, Usage,
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
use std::io::{BufRead, Write};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use uuid::Uuid;

use super::{AgentBackend, DiscoveredModel, ResolvedTools, SessionConfig};

const CLAUDE_BACKEND_NAME: &str = "Claude";
const TOOL_INPUT_PREVIEW_MAX_CHARS: usize = 512;

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
        should_confirm_backend_id_after_init, ClaudeBackend,
    };
    use crate::agent_process::stream::Usage;
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
) -> bool {
    let Some(mut state) = streaming_state.take() else {
        return false;
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
        Ok(finalized) => {
            process_post_commit_outbox(orch, &finalized.outbox_entries);
            true
        }
        Err(error) => {
            log::warn!(
                "Failed to finalize stream {} for run {}: {}",
                state.stream_id,
                run_id,
                error
            );
            false
        }
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
/// Returns `true` when an event was committed (the caller bumps `sequence`).
/// Normally the pending message is paired with its still-open streaming row and
/// is finalized in place. If that pairing was already broken (the stream row is
/// gone but a pending event remains), the message is inserted directly so it is
/// never silently dropped.
#[allow(clippy::too_many_arguments)]
fn flush_pending_assistant_before_suppress(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    sequence: i32,
    streaming_state: &mut Option<StreamingState>,
    pending_final_assistant_event: &mut Option<TranscriptEvent>,
    pending_delta_usage: Option<&Usage>,
) -> bool {
    let Some(pending) = pending_final_assistant_event.take() else {
        return false;
    };
    let counts = TokenCounts::from_optional_usage(pending_delta_usage);
    if streaming_state.is_some() {
        return finalize_streaming_message(
            orch,
            db,
            run_id,
            session_id,
            streaming_state,
            Some(pending),
            counts,
        );
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
            sequence,
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
        Ok(inserted) => inserted,
        Err(error) => {
            log::warn!(
                "Failed to insert pending assistant for run {} before suppression: {}",
                &run_id[..run_id.len().min(8)],
                error
            );
            false
        }
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
            &crate::system_prompt::cairn_system_prompt(),
            workspace_instructions.as_deref(),
            project_instructions.as_deref(),
            config.system_prompt_content.as_deref(),
            config.system_prompt_dynamic_tail.as_deref(),
        );
        let system_prompt_path =
            write_system_prompt_file(&config.run_id, &flatten_prompt_segments(&prompt_segments))?;

        let initial_sequence = persist_system_prompt_event(
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
        log::debug!("ClaudeBackend: args={:?}", claude_args);
        log::debug!("ClaudeBackend: working_dir={}", config.working_dir);

        log::info!("[PROFILE] Command built: {:?}", start_time.elapsed());
        log::info!("Spawning claude: {} {:?}", claude_path, claude_args);

        // Get MCP authentication secret (shared secret for TOTP-style passcodes)
        let mcp_secret = orch
            .mcp_auth
            .get_secret_for_mcp()
            .map_err(|e| format!("Failed to get MCP auth secret: {}", e))?;
        log::info!("Using MCP auth secret for run {}", config.run_id);

        // Build spawn config and spawn using ProcessSpawner service
        let mut spawn_config = SpawnConfig::new(&claude_path)
            .args(&claude_args)
            .cwd(&config.working_dir)
            .env("CAIRN_RUN_ID", &config.run_id)
            .env("CAIRN_MCP_SECRET", &mcp_secret)
            .env("ENABLE_TOOL_SEARCH", "false")
            // Disable Claude Code's native Task* tools (TaskCreate/TaskUpdate/TaskList).
            // Cairn's task tracking surface is `cairn:~/todos` via the `write` verb;
            // the native tools are not exposed and their idle reminder fires on a
            // wall-clock cadence regardless, burning tokens on polite acknowledgements.
            // Removing the tools removes the reminder at the source.
            .env("CLAUDE_CODE_ENABLE_TASKS", "false")
            // Raise the Claude CLI's MCP tool-call timeout far above any legal
            // Cairn callback. The host owns execution: `run` items are capped
            // per-item (≤600s, returning partial output on expiry) and blocking
            // `write` appends to a node's tasks/questions collection await a
            // sub-agent task or user answer with no host-side deadline. cairn-cmd
            // sizes its HTTP callback ceiling just under this (see
            // `UNBOUNDED_CALLBACK_TIMEOUT` in cairn-cmd). The CLI default (~120s,
            // observed) would abandon the call above cairn-cmd and leave the
            // agent with no output while the host keeps running. 7 days is an
            // effective "never" while still bounding a truly wedged socket;
            // promote-to-terminal means nothing should ever actually run this
            // long attached. (`MCP_TIMEOUT` governs server *startup*, a separate
            // concern, and is left at its default.)
            .env("MCP_TOOL_TIMEOUT", "604800000")
            .stdin(true);

        // Inject user identity into Claude process environment
        // Prefer pre-resolved identity from SessionConfig (includes project overrides)
        if let Some(user) = config
            .identity
            .as_ref()
            .cloned()
            .or_else(|| orch.get_identity())
        {
            spawn_config = spawn_config
                .env("GIT_AUTHOR_NAME", &user.name)
                .env("GIT_AUTHOR_EMAIL", &user.email)
                .env("GIT_COMMITTER_NAME", &user.name)
                .env("GIT_COMMITTER_EMAIL", &user.email);

            // Forward Claude auth token for remote/headless sessions
            match &user.claude_auth {
                Some(crate::identity::ClaudeAuth::OAuthToken(token)) => {
                    log::info!("Setting CLAUDE_CODE_OAUTH_TOKEN (len={})", token.len());
                    spawn_config = spawn_config.env("CLAUDE_CODE_OAUTH_TOKEN", token);
                }
                Some(crate::identity::ClaudeAuth::ApiKey(key)) => {
                    log::info!("Setting ANTHROPIC_API_KEY (len={})", key.len());
                    spawn_config = spawn_config.env("ANTHROPIC_API_KEY", key);
                }
                None => {} // Use ambient auth (local claude login)
            }

            log::info!(
                "Injected user identity into session: {} <{}>",
                user.name,
                user.email
            );
        }

        // Check if we need to evict a warm process to make room
        orch.collect_warm_if_needed();

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
        log::info!("[PROFILE] Process spawned: {:?}", start_time.elapsed());

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

        // Spawn thread to log stderr
        if let Some(stderr) = stderr {
            thread::spawn(move || {
                log::debug!("stderr_thread: started");
                for line in stderr.lines().map_while(Result::ok) {
                    log::debug!("claude stderr: {}", line);
                    log::error!("claude stderr: {}", line);
                }
                log::debug!("stderr_thread: ended");
            });
        }

        // Store the process handle with stdin for bidirectional communication
        let child_arc = Arc::new(Mutex::new(Some(child)));
        let stdin_arc = Arc::new(Mutex::new(stdin));

        // Get job_id for warm process tracking
        let process_job_id: Option<String> =
            run_job_id(CLAUDE_BACKEND_NAME, &run_db, &config.run_id);

        {
            let mut processes = orch
                .process_state
                .processes
                .lock()
                .map_err(|e| e.to_string())?;
            let mut active_process = crate::agent_process::process::ActiveProcess::new(
                child_arc.clone(),
                stdin_arc.clone(),
                session_id.clone(),
                process_job_id,
            );
            active_process.model = effective_model.clone();
            processes.register(config.run_id.clone(), active_process);
        }

        // In bidirectional mode, send the initial prompt via stdin
        if args_config.bidirectional {
            let mut stdin_guard = stdin_arc.lock().map_err(|e| e.to_string())?;
            if let Some(ref mut stdin_writer) = *stdin_guard {
                let content = crate::agent_process::stdin::build_message_content(
                    &config.prompt,
                    Some(&config.working_dir),
                    None,
                );

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

        let thread_session_id = session_id;

        // Spawn thread to read stdout and emit events
        thread::spawn(move || {
            Self::reader_thread(
                &orch,
                &emitter,
                &run_id,
                thread_session_id,
                confirm_backend_id_after_init,
                initial_sequence,
                stdout,
                run_db,
            );
        });

        log::info!(
            "[PROFILE] ClaudeBackend::start_session returning: {:?}",
            start_time.elapsed()
        );
        Ok(())
    }

    fn supports_resume(&self) -> bool {
        true
    }

    fn supports_warm_processes(&self) -> bool {
        true
    }

    fn send_user_message(
        &self,
        stdin: &mut dyn crate::agent_process::process::BackendStdin,
        content: &str,
        session_id: &str,
        parent_tool_use_id: Option<&str>,
        working_dir: Option<&str>,
    ) -> Result<(), String> {
        crate::agent_process::stdin::send_user_message_with_images(
            stdin,
            session_id,
            content,
            parent_tool_use_id,
            working_dir,
            None,
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
            pricing: None,
            supported_parameters: Vec::new(),
            router: false,
            architecture_modality: None,
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
            pricing: None,
            supported_parameters: Vec::new(),
            router: false,
            architecture_modality: None,
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
            pricing: None,
            supported_parameters: Vec::new(),
            router: false,
            architecture_modality: None,
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
            pricing: None,
            supported_parameters: Vec::new(),
            router: false,
            architecture_modality: None,
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
        initial_sequence: i32,
        stdout: Box<dyn std::io::BufRead + Send>,
        run_db: Arc<LocalDb>,
    ) {
        log::debug!("reader_thread: started");
        let thread_start = std::time::Instant::now();
        log::debug!("[PROFILE] Reader thread started");
        let mut sequence = initial_sequence;
        let mut first_event_logged = false;
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
                    if !l.contains("\"type\":\"stream_event\"") {
                        log::trace!(
                            "reader_thread: line {}: {}",
                            sequence,
                            &l[..l.len().min(100)]
                        );
                    }
                    l
                }
                Err(e) => {
                    log::debug!("reader_thread: error reading line: {}", e);
                    log::error!("Error reading line: {}", e);
                    continue;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            match parse_event(&line) {
                Ok((event, raw)) => {
                    last_event_kind = claude_event_kind(&event);
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
                        orch.store_provider_usage_snapshot(claude_rate_limit_snapshot(
                            rate_limit_info,
                        ));
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

                    if !first_event_logged {
                        log::info!(
                            "[PROFILE] First event received: {:?}",
                            thread_start.elapsed()
                        );
                        first_event_logged = true;
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
                            if flush_pending_assistant_before_suppress(
                                orch,
                                &run_db,
                                run_id,
                                session_id.as_deref(),
                                sequence,
                                &mut streaming_state,
                                &mut pending_final_assistant_event,
                                pending_delta_usage.as_ref(),
                            ) {
                                sequence += 1;
                            }
                            continue;
                        }
                        match inner {
                            StreamEventInner::MessageStart { .. } => {
                                if streaming_state.is_some() {
                                    log::warn!("New MessageStart while a stream is still active");
                                    if finalize_streaming_message(
                                        orch,
                                        &run_db,
                                        run_id,
                                        session_id.as_deref(),
                                        &mut streaming_state,
                                        pending_final_assistant_event.take(),
                                        TokenCounts::from_optional_usage(
                                            pending_delta_usage.as_ref(),
                                        ),
                                    ) {
                                        sequence += 1;
                                    }
                                }
                                pending_delta_usage = None;
                                last_thinking_tokens = pending_thinking_tokens.take();
                                let current_turn = orch.process_state.get_current_turn_id(run_id);
                                match open_stream(
                                    run_db.clone(),
                                    run_id,
                                    session_id.as_deref(),
                                    current_turn.as_deref(),
                                    "claude",
                                    Some(sequence),
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
                                    && finalize_streaming_message(
                                        orch,
                                        &run_db,
                                        run_id,
                                        session_id.as_deref(),
                                        &mut streaming_state,
                                        pending_final_assistant_event.take(),
                                        counts,
                                    )
                                {
                                    sequence += 1;
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
                        if flush_pending_assistant_before_suppress(
                            orch,
                            &run_db,
                            run_id,
                            session_id.as_deref(),
                            sequence,
                            &mut streaming_state,
                            &mut pending_final_assistant_event,
                            pending_delta_usage.as_ref(),
                        ) {
                            sequence += 1;
                        }
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
                        sequence += 1;
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
                                    orch, run_id,
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
                        sequence += 1;
                        continue;
                    }

                    // Finalize streaming placeholder before Result event
                    if matches!(&event, ClaudeEvent::Result { .. })
                        && finalize_streaming_message(
                            orch,
                            &run_db,
                            run_id,
                            session_id.as_deref(),
                            &mut streaming_state,
                            pending_final_assistant_event.take(),
                            TokenCounts::from_optional_usage(pending_delta_usage.as_ref()),
                        )
                    {
                        sequence += 1;
                    }

                    let transcript_event = TranscriptEvent::from_claude_event(&event, raw.clone());

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
                            if finalize_streaming_message(
                                orch,
                                &run_db,
                                run_id,
                                session_id.as_deref(),
                                &mut streaming_state,
                                Some(transcript_event.clone()),
                                counts,
                            ) {
                                sequence += 1;
                            }
                        } else {
                            // claude-code can emit a second consolidated assistant
                            // event before the trailing message_delta (a batched
                            // multi-tool turn). Merge into the parked slot rather
                            // than overwriting it, or the earlier event's tool
                            // calls are lost (CAIRN-2249).
                            match pending_final_assistant_event.as_mut() {
                                Some(parked) => merge_pending_assistant(parked, &transcript_event),
                                None => {
                                    pending_final_assistant_event = Some(transcript_event.clone())
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
                        if flush_pending_assistant_before_suppress(
                            orch,
                            &run_db,
                            run_id,
                            session_id.as_deref(),
                            sequence,
                            &mut streaming_state,
                            &mut pending_final_assistant_event,
                            pending_delta_usage.as_ref(),
                        ) {
                            sequence += 1;
                        }
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
                        sequence += 1;
                        continue;
                    }

                    // Store event in database
                    {
                        let now = chrono::Utc::now().timestamp() as i32;
                        let event_id = ids::mint_child(run_id);
                        let event_type = transcript_event.event_type.clone();
                        let data = serde_json::to_string(&transcript_event).unwrap_or_default();

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
                                sequence,
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
                        )
                        .unwrap_or(false);

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
                    if let ClaudeEvent::Result { is_error, .. } = &event {
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
                                    orch, run_id,
                                );
                                let _ = emitter.emit(
                                    "run-turn-completed",
                                    serde_json::json!({
                                        "run_id": run_id,
                                        "is_warm": true,
                                    }),
                                );
                            } else {
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
                                    orch, run_id,
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
                        crate::orchestrator::lifecycle::transition_to_warm_state(orch, run_id);
                        let _ = emitter.emit(
                            "run-turn-completed",
                            serde_json::json!({
                                "run_id": run_id,
                                "is_warm": true,
                            }),
                        );
                    }

                    sequence += 1;
                }
                Err(e) => {
                    log::warn!("Failed to parse event: {} - line: {}", e, line);
                }
            }
        }

        // Finalize any remaining durable stream on EOF
        let _ = finalize_streaming_message(
            orch,
            &run_db,
            run_id,
            session_id.as_deref(),
            &mut streaming_state,
            pending_final_assistant_event.take(),
            TokenCounts::from_optional_usage(pending_delta_usage.as_ref()),
        );

        log::debug!("reader_thread: loop ended after {} lines", sequence);

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

                let error_message = if saw_blocking_rate_limit {
                    "Process exited after the account rate limit was reached, before completing the turn. The turn is interrupted and resumable once the limit resets."
                } else {
                    "Process terminated unexpectedly without completing"
                };
                insert_error_event(orch, run_id, session_id.as_deref(), error_message);

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
    use super::{flush_pending_assistant_before_suppress, merge_pending_assistant, StreamingState};
    use crate::agent_process::stream::{ToolUseInfo, TranscriptEvent};
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
            raw: None,
        }
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
            Some(0),
        )
        .unwrap();
        let mut streaming_state = Some(StreamingState::new(&opened));
        let mut pending = Some(write_tool_assistant("toolu_paired"));

        let committed = flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-paired",
            Some("session-1"),
            0,
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert!(committed, "a paired pending assistant should commit");
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

        let committed = flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-orphan",
            Some("session-1"),
            7,
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert!(
            committed,
            "defensive insert must commit the orphaned message"
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

        let committed = flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-empty",
            Some("session-1"),
            0,
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert!(!committed, "no pending message means nothing to commit");
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
            Some(0),
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

        let committed = flush_pending_assistant_before_suppress(
            &orch,
            &orch.db.local,
            "run-merge",
            Some("session-1"),
            0,
            &mut streaming_state,
            &mut pending,
            None,
        );

        assert!(committed, "the merged pending assistant should commit");
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
