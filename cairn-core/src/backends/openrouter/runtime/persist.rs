//! The live assistant stream and transcript-event writes: open/append/finalize a
//! streaming assistant event, and store assistant messages, tool-call events,
//! tool results, and the terminal success result as transcript events.

use super::conversation::render_tool_result;
use super::wire::{OpenRouterUsage, ToolCall};
use crate::agent_process::stream::{TokenCounts, ToolUseInfo, TranscriptEvent};
use crate::backends::openrouter::{repair, OPENROUTER_BACKEND_KEY};
use crate::dispatch::DispatchOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::LocalDb;
use crate::transcripts::stream_store::{
    append_chunks, finalize_stream_emit, insert_event_emit, open_stream,
    process_post_commit_outbox, EmitDelta, EventInsert, StreamAccumulator, StreamChunkInput,
};
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub(super) struct AssistantStreamState {
    stream_id: String,
    version: i32,
    acc: StreamAccumulator,
    run_db: Arc<LocalDb>,
}

impl AssistantStreamState {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn open(
        orch: &Orchestrator,
        run_db: Arc<LocalDb>,
        run_id: &str,
        session_id: &str,
        turn_id: Option<&str>,
        sequence: i32,
    ) -> Result<Self, String> {
        let stream = open_stream(
            run_db.clone(),
            run_id,
            Some(session_id),
            turn_id,
            OPENROUTER_BACKEND_KEY,
            Some(sequence),
        )?;
        let _ = orch.services.emitter.emit(
            "db-change",
            crate::notify::event_db_change_for_run(
                orch.db.local.clone(),
                run_id,
                Some(session_id),
                "insert",
            ),
        );
        Ok(Self {
            stream_id: stream.stream_id().to_string(),
            version: stream.version(),
            acc: StreamAccumulator::new(),
            run_db,
        })
    }

    pub(super) fn append(
        &mut self,
        orch: &Orchestrator,
        run_id: &str,
        text: &str,
    ) -> Result<(), String> {
        self.acc.push_content(text);
        let now = std::time::Instant::now();
        if self.acc.should_flush(now) {
            self.flush()?;
        }
        if self.acc.should_emit(now) {
            let delta = self.acc.take_emit_delta();
            emit_streaming_delta(orch, run_id, &self.stream_id, &delta);
        }
        Ok(())
    }

    pub(super) fn append_thinking(
        &mut self,
        orch: &Orchestrator,
        run_id: &str,
        text: &str,
    ) -> Result<(), String> {
        self.acc.push_thinking(text);
        let now = std::time::Instant::now();
        if self.acc.should_flush(now) {
            self.flush()?;
        }
        if self.acc.should_emit(now) {
            let delta = self.acc.take_emit_delta();
            emit_streaming_delta(orch, run_id, &self.stream_id, &delta);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.acc.pending_is_empty() {
            return Ok(());
        }
        let pending = self.acc.take_pending();
        let appended = append_chunks(self.run_db.clone(), &self.stream_id, self.version, &pending)?;
        self.version = appended.version;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn finalize(
        mut self,
        orch: &Orchestrator,
        run_id: &str,
        session_id: &str,
        text: String,
        thinking: String,
        reasoning_details: Vec<Value>,
        usage: Option<&OpenRouterUsage>,
        generation_id: Option<&str>,
        model: Option<&str>,
    ) -> Result<(), String> {
        self.flush()?;
        if self.acc.has_unemitted() {
            let delta = self.acc.take_emit_delta();
            emit_streaming_delta(orch, run_id, &self.stream_id, &delta);
        }
        let event = TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some(session_id.to_string()),
            parent_tool_use_id: None,
            // None on an empty body so a reasoning-only turn (think then call a
            // tool) yields a valid thinking-only event instead of content: "".
            content: if text.is_empty() { None } else { Some(text) },
            thinking: (!thinking.is_empty()).then_some(thinking),
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: Some(json!({
                "backend": OPENROUTER_BACKEND_KEY,
                "generationId": generation_id,
                "model": model,
                "usage": usage,
                "streamed": true,
                "reasoningDetails": reasoning_details,
            })),
        };
        let finalized = finalize_stream_emit(
            self.run_db.clone(),
            orch.db.local.clone(),
            &orch.services.emitter,
            &self.stream_id,
            self.version,
            Some(event),
            usage.map(OpenRouterUsage::token_counts).unwrap_or_default(),
        )?;
        process_post_commit_outbox(orch, &finalized.outbox_entries);
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn store_assistant_message(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    text: &str,
    usage: Option<&OpenRouterUsage>,
    generation_id: Option<&str>,
    model: Option<&str>,
    total_cost: Option<f64>,
) -> Result<(), String> {
    let stream = open_stream(
        run_db.clone(),
        run_id,
        Some(session_id),
        turn_id,
        OPENROUTER_BACKEND_KEY,
        Some(sequence),
    )?;
    let appended = append_chunks(
        run_db.clone(),
        stream.stream_id(),
        stream.version(),
        &[StreamChunkInput::content(text.to_string())],
    )?;
    let event = TranscriptEvent {
        event_type: "assistant".to_string(),
        session_id: Some(session_id.to_string()),
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
        raw: Some(json!({
            "backend": OPENROUTER_BACKEND_KEY,
            "generationId": generation_id,
            "model": model,
            "totalCost": total_cost,
            "usage": usage,
        })),
    };
    let finalized = finalize_stream_emit(
        run_db.clone(),
        orch.db.local.clone(),
        &orch.services.emitter,
        stream.stream_id(),
        appended.version,
        Some(event),
        usage.map(OpenRouterUsage::token_counts).unwrap_or_default(),
    )?;
    process_post_commit_outbox(orch, &finalized.outbox_entries);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_streaming_delta(orch: &Orchestrator, run_id: &str, event_id: &str, delta: &EmitDelta) {
    let _ = orch.services.emitter.emit(
        "streaming-update",
        json!({
            "run_id": run_id,
            "event_id": event_id,
            "content_delta": delta.content_delta,
            "content_len": delta.content_len,
            "thinking_delta": delta.thinking_delta,
            "thinking_len": delta.thinking_len,
        }),
    );
}

fn tool_use_infos(tool_calls: &[ToolCall]) -> Vec<ToolUseInfo> {
    tool_calls
        .iter()
        .map(|call| ToolUseInfo {
            id: call.id.clone(),
            name: call.function.name.clone(),
            input: repair::parse_tool_arguments(&call.function.arguments).value(),
        })
        .collect()
}

fn single_tool_summary(tool_uses: &[ToolUseInfo]) -> (Option<String>, Option<Value>) {
    if tool_uses.len() == 1 {
        (
            Some(tool_uses[0].name.clone()),
            Some(tool_uses[0].input.clone()),
        )
    } else {
        (None, None)
    }
}

/// The usage to record on a non-streamed tool-call assistant event.
///
/// When the generation's content or reasoning streamed, its usage already landed
/// on the finalized streamed assistant event (`AssistantStreamState::finalize`),
/// so attaching it again here would double-count in the token rollup. A
/// non-streamed tool-call response (no content/reasoning deltas, so no stream was
/// opened) has no other assistant event for that generation, so its tokens must
/// be recorded on this event or they are dropped from the usage breakdown even
/// though the generation's cost is still counted on the cumulative result event.
pub(super) fn tool_call_usage(
    streamed_text: bool,
    usage: Option<&OpenRouterUsage>,
) -> Option<&OpenRouterUsage> {
    if streamed_text {
        None
    } else {
        usage
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn store_assistant_tool_call(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    text: &str,
    tool_calls: &[ToolCall],
    usage: Option<&OpenRouterUsage>,
    generation_id: Option<&str>,
    model: Option<&str>,
    reasoning_details: Option<&Value>,
) -> Result<(), String> {
    let tool_uses = tool_use_infos(tool_calls);
    let (tool_name, tool_input) = single_tool_summary(&tool_uses);
    insert_transcript_event(
        orch,
        run_db,
        run_id,
        session_id,
        turn_id,
        sequence,
        TranscriptEvent {
            event_type: "assistant".to_string(),
            session_id: Some(session_id.to_string()),
            parent_tool_use_id: None,
            content: if text.is_empty() {
                None
            } else {
                Some(text.to_string())
            },
            thinking: None,
            tool_name,
            tool_input,
            tool_uses: Some(tool_uses),
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            raw: Some(json!({
                "backend": OPENROUTER_BACKEND_KEY,
                "generationId": generation_id,
                "model": model,
                "usage": usage,
                // Replayed verbatim on resume so the tool-requesting assistant
                // message carries its original thinking block before tool_use.
                "reasoningDetails": reasoning_details,
            })),
        },
        usage.map(OpenRouterUsage::token_counts).unwrap_or_default(),
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn store_tool_result(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    tool_call_id: &str,
    result: &DispatchOutput,
) -> Result<(), String> {
    insert_transcript_event(
        orch,
        run_db,
        run_id,
        session_id,
        turn_id,
        sequence,
        TranscriptEvent {
            event_type: "tool_result".to_string(),
            session_id: Some(session_id.to_string()),
            parent_tool_use_id: None,
            content: None,
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: Some(tool_call_id.to_string()),
            tool_result: Some(render_tool_result(result.clone())),
            is_error: false,
            thinking_ms: None,
            raw: Some(json!({"backend": OPENROUTER_BACKEND_KEY, "reminders": result.reminders})),
        },
        TokenCounts::default(),
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn store_success_result(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    usage: Option<&OpenRouterUsage>,
    generation_id: Option<&str>,
    model: Option<&str>,
    total_cost: Option<f64>,
) -> Result<(), String> {
    insert_transcript_event(
        orch,
        run_db,
        run_id,
        session_id,
        turn_id,
        sequence,
        TranscriptEvent {
            event_type: "result:success".to_string(),
            session_id: Some(session_id.to_string()),
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
            raw: Some(json!({
                "backend": OPENROUTER_BACKEND_KEY,
                "generationId": generation_id,
                "model": model,
                "totalCost": total_cost,
                "usage": usage,
            })),
        },
        usage.map(OpenRouterUsage::token_counts).unwrap_or_default(),
        total_cost,
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_transcript_event(
    orch: &Orchestrator,
    run_db: &Arc<LocalDb>,
    run_id: &str,
    session_id: &str,
    turn_id: Option<&str>,
    sequence: i32,
    event: TranscriptEvent,
    counts: TokenCounts,
    cost_usd: Option<f64>,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    let data = serde_json::to_string(&event).map_err(|error| error.to_string())?;
    insert_event_emit(
        run_db.clone(),
        &orch.services.emitter,
        EventInsert {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.to_string(),
            session_id: Some(session_id.to_string()),
            sequence,
            timestamp: now,
            event_type: event.event_type.clone(),
            data,
            parent_tool_use_id: event.parent_tool_use_id.clone(),
            created_at: now,
            input_tokens: counts.input,
            cache_read_tokens: counts.cache_read,
            cache_create_tokens: counts.cache_create,
            output_tokens: counts.output,
            thinking_tokens: counts.thinking,
            turn_id: turn_id.map(str::to_string),
            cost_usd,
        },
    )?;
    Ok(())
}
