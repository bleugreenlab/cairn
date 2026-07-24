//! Rebuild the OpenAI-style message array the OpenRouter turn sends: assemble the
//! system + prior transcript + new user message, normalize assistant/tool groups,
//! and map stored transcript events to chat messages.

use super::wire::{default_function_type, ChatMessage, ToolCall, ToolFunction};
use crate::agent_process::stream::TranscriptEvent;
use crate::backends::{SessionConfig, SessionStart};
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, RowExt};
use serde_json::Value;
use std::collections::HashMap;

const INTERRUPTED_TOOL_RESULT: &str = "Interrupted before the tool result was recorded.";

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
                             ORDER BY created_at ASC, rowid ASC",
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
    Ok(normalize_tool_call_groups(messages))
}

/// Reconstruct protocol-valid assistant/tool groups from persisted history.
/// Results may be stored after unrelated events when a foreground prompt
/// suspends a turn, so association is by call id rather than adjacency.
pub(super) fn normalize_tool_call_groups(messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
    let mut stored_results = HashMap::new();
    let mut duplicate_results = Vec::new();
    for message in messages.iter().filter(|message| message.role == "tool") {
        let Some(tool_call_id) = message.tool_call_id.as_ref() else {
            continue;
        };
        if stored_results
            .insert(tool_call_id.clone(), message.clone())
            .is_some()
        {
            duplicate_results.push(tool_call_id.clone());
        }
    }

    let mut result = Vec::with_capacity(messages.len());
    let mut synthesized = Vec::new();
    for message in messages
        .into_iter()
        .filter(|message| message.role != "tool")
    {
        let call_ids = message
            .tool_calls
            .as_ref()
            .filter(|_| message.role == "assistant")
            .map(|calls| calls.iter().map(|call| call.id.clone()).collect::<Vec<_>>());
        result.push(message);
        for call_id in call_ids.into_iter().flatten() {
            if let Some(tool_result) = stored_results.remove(&call_id) {
                result.push(tool_result);
            } else {
                synthesized.push(call_id.clone());
                result.push(ChatMessage::tool(
                    call_id,
                    INTERRUPTED_TOOL_RESULT.to_string(),
                ));
            }
        }
    }

    if !synthesized.is_empty() || !duplicate_results.is_empty() || !stored_results.is_empty() {
        let orphan_ids = stored_results.keys().cloned().collect::<Vec<_>>();
        log::warn!(
            "Repaired OpenRouter tool history: synthesized={:?}, duplicates={:?}, orphans={:?}",
            synthesized,
            duplicate_results,
            orphan_ids
        );
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
