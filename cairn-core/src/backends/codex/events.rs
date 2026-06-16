use super::protocol::StreamingState;
use super::CODEX_BACKEND_NAME;
use crate::agent_process::stream::{TokenCounts, TranscriptEvent, Usage};
use crate::backends::run_state::is_task_spawned_run;
use crate::models::{
    ProviderCreditsSnapshot, ProviderUsageScope, ProviderUsageSnapshot, ProviderUsageWindow,
    RunStatus,
};
use crate::orchestrator::session::insert_error_event;
use crate::orchestrator::Orchestrator;
use crate::transcripts::stream_store::{
    append_chunks, finalize_stream, open_stream, process_post_commit_outbox, EmitDelta, EventInsert,
};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use uuid::Uuid;

pub(super) fn extract_app_server_delta(msg: &Value) -> Option<&str> {
    msg.pointer("/params/delta")
        .and_then(|v| v.as_str())
        .or_else(|| msg.pointer("/params/delta/text").and_then(|v| v.as_str()))
        .or_else(|| msg.pointer("/params/textDelta").and_then(|v| v.as_str()))
}

pub(super) fn extract_command_execution(command: &Value) -> (String, Vec<String>) {
    if let Some(command_str) = command.as_str() {
        return (command_str.to_string(), vec![command_str.to_string()]);
    }

    let command_vec: Vec<String> = command
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|val| val.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let command_display = if command_vec.is_empty() {
        String::new()
    } else {
        command_vec.join(" ")
    };
    (command_display, command_vec)
}

pub(super) fn extract_raw_response_message_text(item: &Value) -> Option<String> {
    if item.get("type").and_then(|v| v.as_str()) != Some("message") {
        return None;
    }

    let content = item.get("content")?.as_array()?;
    let parts: Vec<&str> = content
        .iter()
        .filter_map(|entry| {
            let item_type = entry.get("type").and_then(|v| v.as_str())?;
            match item_type {
                "output_text" | "input_text" => entry.get("text").and_then(|v| v.as_str()),
                _ => None,
            }
        })
        .filter(|text| !text.is_empty())
        .collect();

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

pub(super) fn store_event(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    sequence: i32,
    event: &TranscriptEvent,
) {
    let event_id = Uuid::new_v4().to_string();
    store_event_with_id(
        orch,
        run_id,
        session_id,
        sequence,
        &event_id,
        event,
        TokenCounts::default(),
    );
    let _ = emitter.emit("db-change", event_db_change(run_id, session_id, "insert"));
}

pub(super) fn event_db_change(
    run_id: &str,
    session_id: Option<&str>,
    action: &str,
) -> serde_json::Value {
    serde_json::json!({
        "table": "events",
        "action": action,
        "runId": run_id,
        "run_id": run_id,
        "sessionId": session_id,
        "session_id": session_id,
    })
}

/// Emit a true-delta `streaming-update`: only the newly-produced scalar tail
/// plus current absolute lengths so the frontend can detect gaps and self-heal.
pub(super) fn emit_streaming_delta(
    emitter: &Arc<dyn crate::services::EventEmitter>,
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
pub(super) fn flush_codex_pending(
    orch: &Orchestrator,
    state: &mut StreamingState,
) -> Result<(), String> {
    let pending = state.acc.take_pending();
    let result = append_chunks(
        orch.db.local.clone(),
        &state.stream_id,
        state.version,
        &pending,
    )?;
    state.version = result.version;
    Ok(())
}

pub(super) fn ensure_stream_open(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    sequence: i32,
) -> Result<(), String> {
    if streaming_state.is_some() {
        return Ok(());
    }
    let current_turn = orch.process_state.get_current_turn_id(run_id);
    let stream = open_stream(
        orch.db.local.clone(),
        run_id,
        session_id,
        current_turn.as_deref(),
        "codex",
        Some(sequence),
    )?;
    *streaming_state = Some(StreamingState::new(&stream, run_id, session_id));
    let _ = emitter.emit("db-change", event_db_change(run_id, session_id, "insert"));
    Ok(())
}

pub(super) fn handle_agent_message_delta(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    sequence: &mut i32,
    delta: &str,
) {
    if ensure_stream_open(
        orch,
        emitter,
        run_id,
        session_id,
        streaming_state,
        *sequence,
    )
    .is_err()
    {
        return;
    }

    if let Some(ref mut state) = streaming_state {
        state.acc.push_content(delta);
        let now = std::time::Instant::now();
        if state.acc.should_flush(now) {
            if let Err(error) = flush_codex_pending(orch, state) {
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

pub(super) fn handle_reasoning_delta(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    sequence: i32,
    delta: &str,
) {
    if ensure_stream_open(orch, emitter, run_id, session_id, streaming_state, sequence).is_err() {
        return;
    }

    if let Some(ref mut state) = streaming_state {
        state.acc.push_thinking(delta);
        let now = std::time::Instant::now();
        if state.acc.should_flush(now) {
            if let Err(error) = flush_codex_pending(orch, state) {
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

pub(super) fn finalize_agent_message(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    sequence: &mut i32,
    text: &str,
) {
    if streaming_state.is_some() {
        finalize_streaming_with_event(
            orch,
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
                raw: None,
            }),
            sequence,
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
            raw: None,
        };
        store_event(orch, emitter, run_id, session_id, *sequence, &event);
        *sequence += 1;
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_turn_completed(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
    session_id: Option<&str>,
    streaming_state: &mut Option<StreamingState>,
    sequence: &mut i32,
    status: &str,
    usage: Option<Usage>,
) {
    finalize_streaming(orch, emitter, streaming_state, session_id, sequence);
    let counts = TokenCounts::from_optional_usage(usage.as_ref());

    match status {
        "completed" => {
            let result_event = TranscriptEvent {
                event_type: "result:success".to_string(),
                session_id: session_id.map(|s| s.to_string()),
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
                raw: None,
            };
            let event_id = Uuid::new_v4().to_string();
            store_event_with_id(
                orch,
                run_id,
                session_id,
                *sequence,
                &event_id,
                &result_event,
                counts,
            );
            let _ = emitter.emit("db-change", event_db_change(run_id, session_id, "insert"));
            *sequence += 1;

            let is_task = is_task_spawned_run(CODEX_BACKEND_NAME, orch, run_id);
            if is_task {
                crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Exited);
                orch.process_state.transition_to_warm(run_id);
            } else {
                crate::orchestrator::lifecycle::transition_to_warm_state(orch, run_id);
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
        }
        "interrupted" => {
            handle_codex_interrupted_turn(orch, emitter, run_id);
        }
        _ => {
            insert_error_event(
                orch,
                run_id,
                session_id,
                &format!("Codex turn failed: {}", status),
            );
            crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Crashed);
        }
    }
}

pub(super) fn emit_codex_run_turn_completed(
    emitter: &Arc<dyn crate::services::EventEmitter>,
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

pub(super) fn terminal_tool_called_for_run(orch: &Orchestrator, run_id: &str) -> bool {
    orch.process_state
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

pub(super) fn should_finalize_task_run_on_interrupted_turn(
    terminal_tool_called: bool,
    is_task_spawned: bool,
) -> bool {
    terminal_tool_called && is_task_spawned
}

pub(super) fn handle_codex_interrupted_turn(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    run_id: &str,
) {
    if should_finalize_task_run_on_interrupted_turn(
        terminal_tool_called_for_run(orch, run_id),
        is_task_spawned_run(CODEX_BACKEND_NAME, orch, run_id),
    ) {
        log::info!(
            "codex interrupted after terminal tool for task-spawned run {}; finalizing as completed",
            &run_id[..run_id.len().min(8)]
        );
        crate::orchestrator::lifecycle::finalize_run(orch, run_id, RunStatus::Exited);
        orch.process_state.transition_to_warm(run_id);
    } else {
        crate::orchestrator::lifecycle::transition_to_warm_state(orch, run_id);
        emit_codex_run_turn_completed(emitter, run_id);
    }
}

pub(super) fn build_codex_compaction_event(raw: Value) -> TranscriptEvent {
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
        raw: Some(json!({
            "provider": "codex",
            "compaction": raw,
        })),
    }
}

pub(super) fn codex_rate_limit_snapshot_from_value(raw: Value) -> Option<ProviderUsageSnapshot> {
    let mut windows = Vec::new();
    for id in ["primary", "secondary"] {
        let Some(window) = raw.get(id) else {
            continue;
        };
        if window.is_null() {
            continue;
        }
        let used_percent = window.get("usedPercent").and_then(Value::as_f64)?;
        let window_duration_mins = window
            .get("windowDurationMins")
            .and_then(Value::as_i64)
            .map(|mins| mins as i32);
        windows.push(ProviderUsageWindow {
            id: id.to_string(),
            label: codex_rate_limit_window_label(id, window_duration_mins),
            scope: codex_rate_limit_scope(window_duration_mins),
            scope_target: None,
            used_percent,
            remaining_percent: (100.0 - used_percent).clamp(0.0, 100.0),
            resets_at: window.get("resetsAt").and_then(Value::as_i64),
            reset_at_text: None,
            window_duration_mins,
        });
    }

    let credits = raw.get("credits").and_then(|value| {
        if value.is_null() {
            return None;
        }
        Some(ProviderCreditsSnapshot {
            balance: value.get("balance").and_then(Value::as_f64),
            total_granted: value.get("totalGranted").and_then(Value::as_f64),
            total_used: value.get("totalUsed").and_then(Value::as_f64),
            currency: value
                .get("currency")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    });

    Some(ProviderUsageSnapshot {
        backend: "codex".to_string(),
        source: "codex_rate_limits".to_string(),
        captured_at: chrono::Utc::now().timestamp(),
        windows,
        credits,
        error: None,
        unsupported_reason: None,
        raw: Some(raw),
    })
}

pub(super) fn codex_rate_limit_scope(window_duration_mins: Option<i32>) -> ProviderUsageScope {
    if window_duration_mins == Some(10_080) {
        ProviderUsageScope::Weekly
    } else {
        ProviderUsageScope::RollingWindow
    }
}

pub(super) fn codex_rate_limit_window_label(id: &str, window_duration_mins: Option<i32>) -> String {
    match window_duration_mins {
        Some(10_080) => "Weekly window".to_string(),
        Some(mins) if mins % 60 == 0 => format!("{}-hour window", mins / 60),
        Some(mins) => format!("{}-minute window", mins),
        None => match id {
            "primary" => "Primary window".to_string(),
            "secondary" => "Secondary window".to_string(),
            _ => "Usage window".to_string(),
        },
    }
}

pub(super) fn store_event_with_id(
    orch: &Orchestrator,
    run_id: &str,
    session_id: Option<&str>,
    sequence: i32,
    event_id: &str,
    event: &TranscriptEvent,
    counts: TokenCounts,
) {
    let now = chrono::Utc::now().timestamp() as i32;
    let data = serde_json::to_string(event).unwrap_or_default();

    let current_turn = orch.process_state.get_current_turn_id(run_id);
    if crate::transcripts::stream_store::insert_event(
        orch.db.local.clone(),
        EventInsert {
            id: event_id.to_string(),
            run_id: run_id.to_string(),
            session_id: session_id.map(str::to_string),
            sequence,
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
        },
    )
    .unwrap_or(false)
    {
        // Sync event to cloud
        orch.sync(crate::sync::SyncMessage::Event(crate::sync::SyncEvent {
            id: event_id.to_string(),
            run_id: run_id.to_string(),
            session_id: session_id.map(|s| s.to_string()),
            sequence: Some(sequence),
            event_type: event.event_type.clone(),
            data: Some(data.clone()),
            input_tokens: counts.input,
            output_tokens: counts.output,
            cache_read_tokens: counts.cache_read,
            cache_create_tokens: counts.cache_create,
            thinking_tokens: counts.thinking,
            created_at: Some(now as i64),
            turn_id: current_turn.clone(),
        }));

        // Embed events for vibe coloring (agent content) and session position
        // (user / agent / change feeds). Position needs a session id to key on;
        // without one we still color agent events.
        if let Some(session) = session_id {
            match event.event_type.as_str() {
                "assistant" => {
                    if let Some(text) = crate::embeddings::extract_embeddable_text(&data) {
                        orch.enqueue_position_embed(
                            session,
                            event_id,
                            crate::embeddings::PositionKind::Agent,
                            text,
                            counts.output,
                        );
                    }
                    if let Some(signal) = crate::embeddings::extract_change_signal_text(&data) {
                        orch.enqueue_position_embed(
                            session,
                            event_id,
                            crate::embeddings::PositionKind::Change,
                            signal,
                            counts.output,
                        );
                    }
                }
                "user" => {
                    if let Some(text) = crate::embeddings::extract_embeddable_text(&data) {
                        orch.enqueue_position_embed(
                            session,
                            event_id,
                            crate::embeddings::PositionKind::User,
                            text,
                            counts.input,
                        );
                    }
                }
                _ => {}
            }
        } else if event.event_type == "assistant" {
            if let Some(text) = crate::embeddings::extract_embeddable_text(&data) {
                orch.enqueue_event_embed(event_id, text);
            }
        }
    }
}

pub(super) fn finalize_streaming_with_event(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    streaming_state: &mut Option<StreamingState>,
    final_event: Option<TranscriptEvent>,
    sequence: &mut i32,
) {
    let Some(mut state) = streaming_state.take() else {
        return;
    };
    // Flush buffered chunks before finalize: finalize_stream reconstructs the
    // final content from chunk rows, so unflushed tokens would be lost.
    if !state.acc.pending_is_empty() {
        if let Err(error) = flush_codex_pending(orch, &mut state) {
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
    match finalize_stream(
        orch.db.local.clone(),
        &state.stream_id,
        state.version,
        final_event,
        TokenCounts::default(),
    ) {
        Ok(finalized) => {
            let _ = emitter.emit(
                "db-change",
                event_db_change(&state.run_id, state.session_id.as_deref(), "insert"),
            );
            process_post_commit_outbox(orch, &finalized.outbox_entries);
            *sequence += 1;
        }
        Err(error) => {
            log::warn!(
                "Failed to finalize Codex stream {}: {}",
                state.stream_id,
                error
            );
        }
    }
}

pub(super) fn finalize_streaming(
    orch: &Orchestrator,
    emitter: &Arc<dyn crate::services::EventEmitter>,
    streaming_state: &mut Option<StreamingState>,
    _session_id: Option<&str>,
    sequence: &mut i32,
) {
    finalize_streaming_with_event(orch, emitter, streaming_state, None, sequence);
}

/// Extract display text from a Codex MCP CallToolResult.
///
/// The wire format is `Result<CallToolResult, String>` serialized as:
///   `{"Ok": {"content": [{"type":"text","text":"..."}], "is_error": false}}`
///   or `{"Err": "message"}`
///
/// Returns (display_text, is_error, optional raw value for storage).
pub(super) fn extract_mcp_result(result: &Option<Value>) -> (String, bool, Option<Value>) {
    let Some(val) = result else {
        return ("Completed".to_string(), false, None);
    };

    // Handle Result::Err
    if let Some(err_str) = val.get("Err").and_then(|v| v.as_str()) {
        return (format!("Error: {}", err_str), true, Some(val.clone()));
    }

    // Handle Result::Ok -> CallToolResult
    let call_result = val.get("Ok").unwrap_or(val);

    let is_err = call_result
        .get("is_error")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Extract text from content array: [{"type":"text","text":"..."}, ...]
    let text = call_result
        .get("content")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    if item.get("type").and_then(|t| t.as_str()) == Some("text") {
                        item.get("text").and_then(|t| t.as_str())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();

    let display = if text.is_empty() {
        if is_err {
            "Error".to_string()
        } else {
            "Completed".to_string()
        }
    } else {
        text
    };

    // Only store raw if there's non-text content (images, structured_content, etc.)
    let has_non_text = call_result
        .get("content")
        .and_then(|v| v.as_array())
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("type").and_then(|t| t.as_str()) != Some("text"))
        });
    let raw = if has_non_text || call_result.get("structured_content").is_some() {
        Some(val.clone())
    } else {
        None
    };

    (display, is_err, raw)
}

pub(super) fn summarize_command_result(item: &Value) -> (String, bool) {
    let aggregated = item
        .get("aggregatedOutput")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let stdout = item
        .get("stdout")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let stderr = item
        .get("stderr")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| format!("stderr: {}", s));
    let exit_code = item.get("exitCode").and_then(|v| v.as_i64());
    let status = item
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("completed");

    let result_text = aggregated
        .or(stdout)
        .or(stderr)
        .unwrap_or_else(|| match exit_code {
            Some(0) => "Command completed successfully".to_string(),
            Some(code) => format!("Exit code {}", code),
            None => status.to_string(),
        });

    let is_error = exit_code.is_some_and(|c| c != 0) || matches!(status, "failed" | "declined");
    (result_text, is_error)
}

pub(super) fn summarize_file_change_result(item: &Value) -> (String, bool) {
    if let Some(err) = item.get("error").and_then(|v| v.as_str()) {
        return (format!("Error: {}", err), true);
    }
    let status = item
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("completed");
    let text = match status {
        "completed" => "File changes applied".to_string(),
        "declined" => "File changes declined".to_string(),
        "failed" => "File changes failed".to_string(),
        other => format!("File changes {}", other),
    };
    let is_error = matches!(status, "failed" | "declined");
    (text, is_error)
}
