//! Rebuild the OpenAI-style message array the OpenRouter turn sends: assemble the
//! system + prior transcript + new user message, reorder tool results to match
//! their assistant call order, map stored transcript events to chat messages, and
//! render a dispatch result (with reminders) into tool-message text.

use super::wire::{default_function_type, ChatMessage, ToolCall, ToolFunction};
use crate::agent_process::stream::TranscriptEvent;
use crate::backends::{SessionConfig, SessionStart};
use crate::dispatch::DispatchOutput;
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, RowExt};
use serde_json::Value;

/// Concatenate assembled prompt segments into the full system prompt. This is
/// byte-identical to what `persist_system_prompt_event` records, so the wire
/// system message equals the persisted/displayed prompt with no drift.
pub(super) fn build_conversation_messages(
    orch: &Orchestrator,
    config: &SessionConfig,
    session_id: &str,
    system_prompt: &str,
) -> Result<Vec<ChatMessage>, String> {
    let mut messages = vec![ChatMessage::system(system_prompt.to_string())];
    if !matches!(config.session_start, SessionStart::New { .. }) {
        messages.extend(load_prior_chat_messages(orch, session_id, &config.run_id)?);
    }
    messages.push(ChatMessage::user(config.prompt.clone()));
    Ok(messages)
}

fn load_prior_chat_messages(
    orch: &Orchestrator,
    session_id: &str,
    current_run_id: &str,
) -> Result<Vec<ChatMessage>, String> {
    let session_id = session_id.to_string();
    let current_run_id = current_run_id.to_string();
    let messages = run_db_blocking(|| async move {
        orch.db
            .local
            .read(|conn| {
                let session_id = session_id.clone();
                let current_run_id = current_run_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT event_type, data FROM events
                             WHERE session_id = ?1
                               AND run_id != ?2
                               AND event_type IN ('user', 'assistant', 'tool_result')
                             ORDER BY created_at ASC, rowid ASC
                             LIMIT 200",
                            (session_id.as_str(), current_run_id.as_str()),
                        )
                        .await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        let event_type = row.text(0)?;
                        let data = row.text(1)?;
                        let Ok(event) = serde_json::from_str::<TranscriptEvent>(&data) else {
                            continue;
                        };
                        if let Some(message) = transcript_event_to_chat_message(&event_type, event)
                        {
                            out.push(message);
                        }
                    }
                    Ok(out)
                })
            })
            .await
            .map_err(|error| error.to_string())
    })?;
    Ok(reorder_tool_results_by_call_order(messages))
}

/// Reorder each assistant message's following `tool` results to match the order
/// of that assistant message's `tool_calls`, regardless of the order the events
/// were persisted in. The OpenRouter rebuild reads transcript events by
/// `(created_at, rowid)`, but a suspended turn persists the un-executed calls'
/// placeholder results at suspend time and the suspended call's real result only
/// later on resume (a higher sequence). Without this, a suspended non-last tool
/// call replays its later sibling's result before its own, which is not the
/// OpenAI/OpenRouter tool-result ordering for the assistant message. The sort is
/// stable and keyed by `tool_call_id` position, so an already-ordered run is a
/// no-op and any result whose id is not in the call list is left at the end.
pub(super) fn reorder_tool_results_by_call_order(messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
    let mut result: Vec<ChatMessage> = Vec::with_capacity(messages.len());
    let mut iter = messages.into_iter().peekable();
    while let Some(message) = iter.next() {
        let call_order: Option<Vec<String>> = if message.role == "assistant" {
            message
                .tool_calls
                .as_ref()
                .map(|calls| calls.iter().map(|call| call.id.clone()).collect())
        } else {
            None
        };
        result.push(message);
        let Some(order) = call_order else {
            continue;
        };
        // Gather the contiguous run of tool results that answer this assistant
        // message, then order them by their position in its `tool_calls`.
        let mut tools: Vec<ChatMessage> = Vec::new();
        while iter.peek().map(|m| m.role == "tool").unwrap_or(false) {
            tools.push(iter.next().expect("peeked tool message"));
        }
        tools.sort_by_key(|tool| {
            tool.tool_call_id
                .as_ref()
                .and_then(|id| order.iter().position(|call_id| call_id == id))
                .unwrap_or(usize::MAX)
        });
        result.extend(tools);
    }
    result
}

pub(super) fn transcript_event_to_chat_message(
    event_type: &str,
    event: TranscriptEvent,
) -> Option<ChatMessage> {
    match event_type {
        "user" => event.content.map(ChatMessage::user),
        "assistant" => {
            let tool_calls = event.tool_uses.as_ref().map(|uses| {
                uses.iter()
                    .map(|tool| ToolCall {
                        id: tool.id.clone(),
                        r#type: default_function_type(),
                        function: ToolFunction {
                            name: tool.name.clone(),
                            arguments: serde_json::to_string(&tool.input)
                                .unwrap_or_else(|_| "{}".to_string()),
                        },
                    })
                    .collect::<Vec<_>>()
            });
            // Replay structured reasoning verbatim and in original order; stored
            // under either casing depending on which writer persisted the event.
            let reasoning_details = event
                .raw
                .as_ref()
                .and_then(|raw| {
                    raw.get("reasoning_details")
                        .or_else(|| raw.get("reasoningDetails"))
                })
                // Writers store `null` (no reasoning) or `[]`; treat both as absent
                // so a non-reasoning tool-call turn does not replay `reasoning_details: null`.
                .filter(|value| {
                    !value.is_null() && !matches!(value, Value::Array(items) if items.is_empty())
                })
                .cloned();
            if event.content.is_none() && tool_calls.as_ref().map(Vec::is_empty).unwrap_or(true) {
                None
            } else {
                Some(ChatMessage {
                    role: "assistant".to_string(),
                    content: event.content,
                    tool_call_id: None,
                    tool_calls: tool_calls.filter(|calls| !calls.is_empty()),
                    reasoning_details,
                })
            }
        }
        "tool_result" => event
            .tool_use_id
            .zip(event.tool_result)
            .map(|(tool_call_id, content)| ChatMessage::tool(tool_call_id, content)),
        _ => None,
    }
}

pub(super) fn render_tool_result(output: DispatchOutput) -> String {
    if output.reminders.is_empty() {
        return output.content;
    }
    let mut rendered = output.content;
    for reminder in output.reminders {
        rendered.push_str("\n\n<system-reminder>\n");
        rendered.push_str(&reminder);
        rendered.push_str("\n</system-reminder>");
    }
    rendered
}
