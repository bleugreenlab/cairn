use super::protocol::StreamingState;
use super::runtime::CodexRuntime;
use crate::agent_process::stream::TranscriptJson;
use crate::agent_process::stream::{TokenCounts, TranscriptEvent};
use crate::transcripts::stream_store::{append_chunks, open_stream, EmitDelta, EventInsert};
use cairn_common::ids;
use cairn_db::storage::LocalDb;
use cairn_llm::wire::claude_stream::Usage;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Persist positive evidence that Codex completed its post-tool response without
/// producing a recordable item. This event is independent of recovery
/// eligibility: artifact handoffs, uncertain classification, and a spent
/// recovery budget must retain the evidence that the provider response itself
/// completed empty.
pub fn store_codex_zero_item_response_event(
    host: &CodexRuntime,
    run_id: &str,
    session_id: Option<&str>,
    provider_turn_id: Option<&str>,
    usage: Option<&Usage>,
) {
    let event = TranscriptEvent {
        event_type: "system:codex_zero_item_response".to_string(),
        session_id: session_id.map(str::to_string),
        parent_tool_use_id: None,
        content: None,
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        queued_message_id: None,
        raw: Some(json!({
            "provider_turn_id": provider_turn_id,
            "output_tokens": usage.map(|value| value.output_tokens),
            "reasoning_output_tokens": usage.and_then(|value| {
                value.output_tokens_details.as_ref()?.thinking_tokens
            }),
            "recorded_item_count": 0,
        })),
    };
    let event_id = ids::mint_child(run_id);
    store_event_with_id(
        host,
        run_id,
        session_id,
        &event_id,
        &event,
        TokenCounts::from_optional_usage(usage),
    );
}

#[allow(clippy::too_many_arguments)]
pub fn store_event(
    host: &CodexRuntime,
    run_id: &str,
    session_id: Option<&str>,
    event: &TranscriptEvent,
) {
    let event_id = ids::mint_child(run_id);
    store_event_with_id(
        host,
        run_id,
        session_id,
        &event_id,
        event,
        TokenCounts::default(),
    );
}

/// Emit a true-delta `streaming-update`: only the newly-produced scalar tail
/// plus current absolute lengths so the frontend can detect gaps and self-heal.
pub fn emit_streaming_delta(
    emitter: &Arc<dyn cairn_platform::services::emitter::EventEmitter>,
    run_id: &str,
    event_id: &str,
    delta: &EmitDelta,
) {
    let _ = emitter.emit(
        "streaming-update",
        serde_json::json!({
            "run_id": run_id,
            "event_id": event_id,
            "content_delta": delta.content_delta,
            "content_len": delta.content_len,
            "thinking_delta": delta.thinking_delta,
            "thinking_len": delta.thinking_len,
        }),
    );
}

/// Flush the accumulator's buffered chunks to the DB and advance the stream
/// version. Caller ensures there is something to flush.
fn flush_codex_pending(state: &mut StreamingState, run_db: &Arc<LocalDb>) -> Result<(), String> {
    let pending = state.acc.take_pending();
    let result = append_chunks(run_db.clone(), &state.stream_id, state.version, &pending)?;
    state.version = result.version;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn ensure_stream_open(
    host: &CodexRuntime,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
) -> Result<(), String> {
    if streaming_state.is_some() {
        return Ok(());
    }
    let current_turn = host.process_state.get_current_turn_id(run_id);
    let stream = open_stream(
        run_db.clone(),
        run_id,
        session_id,
        current_turn.as_deref(),
        "codex",
    )?;
    *streaming_state = Some(StreamingState::new(&stream, run_id));
    host.host.transcripts().streams_changed("insert");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn handle_agent_message_delta(
    host: &CodexRuntime,
    run_db: &Arc<LocalDb>,
    emitter: &Arc<dyn cairn_platform::services::emitter::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    delta: &str,
) {
    if ensure_stream_open(host, run_db, run_id, session_id, streaming_state).is_err() {
        return;
    }

    if let Some(ref mut state) = streaming_state {
        state.acc.push_content(delta);
        let now = std::time::Instant::now();
        if state.acc.should_flush(now) {
            if let Err(error) = flush_codex_pending(state, run_db) {
                log::warn!(
                    "Failed to flush Codex content chunks for {}: {}",
                    run_id,
                    error
                );
            }
        }
        if state.acc.should_emit(now) {
            let d = state.acc.take_emit_delta();
            emit_streaming_delta(emitter, run_id, &state.stream_id, &d);
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn handle_reasoning_delta(
    host: &CodexRuntime,
    run_db: &Arc<LocalDb>,
    emitter: &Arc<dyn cairn_platform::services::emitter::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    delta: &str,
) {
    if ensure_stream_open(host, run_db, run_id, session_id, streaming_state).is_err() {
        return;
    }

    if let Some(ref mut state) = streaming_state {
        state.acc.push_thinking(delta);
        let now = std::time::Instant::now();
        if state.acc.should_flush(now) {
            if let Err(error) = flush_codex_pending(state, run_db) {
                log::warn!(
                    "Failed to flush Codex reasoning chunks for {}: {}",
                    run_id,
                    error
                );
            }
        }
        if state.acc.should_emit(now) {
            let d = state.acc.take_emit_delta();
            emit_streaming_delta(emitter, run_id, &state.stream_id, &d);
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn finalize_agent_message(
    host: &CodexRuntime,
    run_db: &Arc<LocalDb>,
    emitter: &Arc<dyn cairn_platform::services::emitter::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    text: &str,
) {
    if streaming_state.is_some() {
        finalize_streaming_with_event(
            host,
            run_db,
            emitter,
            streaming_state,
            Some(TranscriptEvent {
                event_type: "assistant".to_string(),
                session_id: session_id.map(|s| s.to_string()),
                parent_tool_use_id: None,
                content: Some(text.to_string()),
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
            }),
        );
    } else {
        let event = TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: session_id.map(|s| s.to_string()),
            parent_tool_use_id: None,
            content: Some(text.to_string()),
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
        store_event(host, run_id, session_id, &event);
    }
}

#[allow(clippy::too_many_arguments)]
pub fn store_usage_result_event(
    host: &CodexRuntime,
    run_id: &str,
    session_id: Option<&str>,
    event_type: &str,
    is_error: bool,
    counts: TokenCounts,
    store_when_empty: bool,
) {
    let has_usage = counts.input.unwrap_or(0) != 0
        || counts.output.unwrap_or(0) != 0
        || counts.cache_read.unwrap_or(0) != 0
        || counts.cache_create.unwrap_or(0) != 0
        || counts.thinking.unwrap_or(0) != 0;
    if !store_when_empty && !has_usage {
        return;
    }
    let event = TranscriptEvent {
        event_type: event_type.to_string(),
        session_id: session_id.map(str::to_string),
        parent_tool_use_id: None,
        content: None,
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error,
        thinking_ms: None,
        queued_message_id: None,
        raw: None,
    };
    let event_id = ids::mint_child(run_id);
    store_event_with_id(host, run_id, session_id, &event_id, &event, counts);
}

pub fn emit_codex_run_turn_completed(
    emitter: &Arc<dyn cairn_platform::services::emitter::EventEmitter>,
    run_id: &str,
) {
    let _ = emitter.emit(
        "run-turn-completed",
        serde_json::json!({
            "runId": run_id,
            "run_id": run_id,
            "isWarm": true,
            "is_warm": true
        }),
    );
}

pub fn emit_codex_command_output_delta(
    emitter: &Arc<dyn cairn_platform::services::emitter::EventEmitter>,
    run_id: &str,
    tool_use_id: &str,
    delta: &str,
    output_chars: i32,
) {
    if delta.is_empty() || tool_use_id.is_empty() {
        return;
    }

    let _ = emitter.emit(
        "tool-output-delta",
        serde_json::json!({
            "runId": run_id,
            "toolUseId": tool_use_id,
            "delta": delta,
            "outputChars": output_chars,
        }),
    );
}

pub fn emit_codex_command_complete(
    emitter: &Arc<dyn cairn_platform::services::emitter::EventEmitter>,
    run_id: &str,
    tool_use_id: &str,
    output_chars: Option<i32>,
) {
    if tool_use_id.is_empty() {
        return;
    }

    let _ = emitter.emit(
        "tool-output-complete",
        serde_json::json!({
            "runId": run_id,
            "toolUseId": tool_use_id,
            "outputChars": output_chars,
        }),
    );
}

pub fn terminal_tool_called_for_run(host: &CodexRuntime, run_id: &str) -> bool {
    host.process_state
        .processes
        .lock()
        .ok()
        .and_then(|processes| {
            processes
                .get(run_id)
                .map(|process| process.terminal_tool_called.load(Ordering::Acquire))
        })
        .unwrap_or(false)
}

pub fn build_codex_compaction_event(raw: Value) -> TranscriptEvent {
    TranscriptEvent {
        event_type: "system:compact_boundary".to_string(),
        session_id: None,
        parent_tool_use_id: None,
        content: Some("Context compacted".to_string()),
        thinking: None,
        tool_name: None,
        tool_input: None,
        tool_uses: None,
        tool_use_id: None,
        tool_result: None,
        is_error: false,
        thinking_ms: None,
        queued_message_id: None,
        raw: Some(json!({
            "provider": "codex",
            "compaction": raw,
        })),
    }
}

#[allow(clippy::too_many_arguments)]
fn store_event_with_id(
    host: &CodexRuntime,
    run_id: &str,
    session_id: Option<&str>,
    event_id: &str,
    event: &TranscriptEvent,
    counts: TokenCounts,
) {
    store_event_with_id_for_turn(host, run_id, session_id, event_id, event, counts, None);
}

#[allow(clippy::too_many_arguments)]
fn store_event_with_id_for_turn(
    host: &CodexRuntime,
    run_id: &str,
    session_id: Option<&str>,
    event_id: &str,
    event: &TranscriptEvent,
    counts: TokenCounts,
    explicit_turn_id: Option<&str>,
) {
    let now = chrono::Utc::now().timestamp() as i32;
    // Transcript crossing (CAIRN-3822). Codex's discrete event path reaches the
    // durable row and the frontend emission without passing through the
    // streaming finalizer, so it needs the guard in its own right.
    let data = event.clone().observed().to_event_json();

    let current_turn = explicit_turn_id
        .map(str::to_string)
        .or_else(|| host.process_state.get_current_turn_id(run_id));
    if host
        .host
        .transcripts()
        .insert_event(EventInsert {
            id: event_id.to_string(),
            run_id: run_id.to_string(),
            session_id: session_id.map(str::to_string),
            timestamp: now,
            event_type: event.event_type.clone(),
            data: data.clone(),
            parent_tool_use_id: event.parent_tool_use_id.clone(),
            created_at: now,
            input_tokens: counts.input,
            cache_read_tokens: counts.cache_read,
            cache_create_tokens: counts.cache_create,
            output_tokens: counts.output,
            thinking_tokens: counts.thinking,
            turn_id: current_turn.clone(),
            cost_usd: None,
        })
        .unwrap_or(false)
    {
        host.host
            .transcripts()
            .event_committed(event_id, &event.event_type, &data, counts);
    }
}

fn finalize_streaming_with_event(
    host: &CodexRuntime,
    run_db: &Arc<LocalDb>,
    emitter: &Arc<dyn cairn_platform::services::emitter::EventEmitter>,
    streaming_state: &mut Option<StreamingState>,
    final_event: Option<TranscriptEvent>,
) {
    let Some(mut state) = streaming_state.take() else {
        return;
    };
    // Flush buffered chunks before finalize: finalize_stream reconstructs the
    // final content from chunk rows, so unflushed tokens would be lost.
    if !state.acc.pending_is_empty() {
        if let Err(error) = flush_codex_pending(&mut state, run_db) {
            log::warn!(
                "Failed to flush Codex stream {} before finalize: {}",
                state.stream_id,
                error
            );
        }
    }
    // Force the live slot to the full content before the snapshot is swapped.
    if state.acc.has_unemitted() {
        let d = state.acc.take_emit_delta();
        emit_streaming_delta(emitter, &state.run_id, &state.stream_id, &d);
    }
    match host.host.transcripts().finalize_stream(
        &state.stream_id,
        state.version,
        final_event,
        TokenCounts::default(),
    ) {
        Ok(_) => {}
        Err(error) => {
            log::warn!(
                "Failed to finalize Codex stream {}: {}",
                state.stream_id,
                error
            );
        }
    }
}

pub fn finalize_streaming(
    host: &CodexRuntime,
    run_db: &Arc<LocalDb>,
    emitter: &Arc<dyn cairn_platform::services::emitter::EventEmitter>,
    streaming_state: &mut Option<StreamingState>,
    _session_id: Option<&str>,
) {
    finalize_streaming_with_event(host, run_db, emitter, streaming_state, None);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_platform::services::emitter::CapturingEmitter;
    use cairn_platform::services::emitter::EventEmitter;
    use std::sync::Arc;

    #[test]
    fn command_output_delta_uses_tool_output_counter_payload() {
        let capture = Arc::new(CapturingEmitter::new());
        let emitter: Arc<dyn EventEmitter> = capture.clone();

        emit_codex_command_output_delta(&emitter, "run-1", "tool-1", "hello\n", 6);

        let events = capture.events_named("tool-output-delta");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["runId"], "run-1");
        assert_eq!(events[0]["toolUseId"], "tool-1");
        assert_eq!(events[0]["delta"], "hello\n");
        assert_eq!(events[0]["outputChars"], 6);
    }

    #[test]
    fn command_complete_uses_tool_output_complete_payload() {
        let capture = Arc::new(CapturingEmitter::new());
        let emitter: Arc<dyn EventEmitter> = capture.clone();

        emit_codex_command_complete(&emitter, "run-1", "tool-1", Some(7));

        let events = capture.events_named("tool-output-complete");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["runId"], "run-1");
        assert_eq!(events[0]["toolUseId"], "tool-1");
        assert_eq!(events[0]["outputChars"], 7);
    }
}
