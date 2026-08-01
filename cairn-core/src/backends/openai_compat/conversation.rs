//! Rebuild the OpenAI-style message array an OpenAI-compatible turn sends: assemble the
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

/// Stands in for a tool result that was stored empty. A blank content block is
/// rejectable, and a dropped tool message would orphan its call, so the empty
/// result is stated rather than sent or removed.
const EMPTY_TOOL_RESULT: &str = "The tool returned no output.";

/// Why a wholly empty turn is refused instead of assembled.
const EMPTY_USER_MESSAGE: &str = "Refusing to start a turn with an empty user message: an empty text block is rejected by the provider and would poison every later replay of this conversation.";

/// Concatenate assembled prompt segments into the full system prompt. This is
/// byte-identical to what `persist_system_prompt_event` records, so the wire
/// system message equals the persisted/displayed prompt with no drift.
pub(crate) fn build_conversation_messages(
    orch: &Orchestrator,
    config: &SessionConfig,
    session_id: &str,
    system_prompt: &str,
) -> Result<Vec<ChatMessage>, String> {
    if config.message_content.is_blank() {
        return Err(EMPTY_USER_MESSAGE.to_string());
    }
    let mut messages = vec![ChatMessage::system(system_prompt.to_string())];
    if !matches!(config.session_start, SessionStart::New { .. }) {
        messages.extend(load_prior_chat_messages(
            orch,
            session_id,
            &config.run_id,
            &config.project_id,
            &config.project_key,
        )?);
    }
    messages.push(ChatMessage::user_content(&config.message_content));
    Ok(messages)
}

fn load_prior_chat_messages(
    orch: &Orchestrator,
    session_id: &str,
    current_run_id: &str,
    project_id: &str,
    project_key: &str,
) -> Result<Vec<ChatMessage>, String> {
    let session_id = session_id.to_string();
    let current_run_id = current_run_id.to_string();
    let project_id = project_id.to_string();
    let project_key = project_key.to_string();
    let messages = run_db_blocking(|| async move {
        let session_db = crate::projects::crud::owning_db(&orch.db, &project_id).await?;
        let rows = session_db
            .query_all(
                "SELECT event_type, data FROM events
                 WHERE session_id = ?1
                   AND run_id != ?2
                   AND event_type IN ('user', 'assistant', 'tool_result')
                 ORDER BY created_at ASC, rowid ASC",
                (session_id.clone(), current_run_id.clone()),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .map_err(|error| error.to_string())?;
        let mut out = Vec::new();
        for (event_type, data) in rows {
            let Ok(event) = serde_json::from_str::<TranscriptEvent>(&data) else {
                continue;
            };
            // Blank stored content is dropped rather than replayed: an empty
            // text block is rejected by the provider, and replaying one turns a
            // single bad turn into a conversation that can never resume
            // (CAIRN-3263).
            let message = if event_type == "user" {
                match event.content.filter(|text| !text.trim().is_empty()) {
                    Some(content) => {
                        let content = crate::agent_process::stdin::resolve_stable_images(
                            &orch.db,
                            &project_id,
                            &project_key,
                            content,
                        )
                        .await?;
                        Some(ChatMessage::user_content(&content))
                    }
                    None => None,
                }
            } else {
                transcript_event_to_chat_message(&event_type, event)
            };
            if let Some(message) = message {
                out.push(message);
            }
        }
        Ok::<_, String>(out)
    })?;
    Ok(normalize_tool_call_groups(messages))
}

/// Reconstruct protocol-valid assistant/tool groups from persisted history.
/// Results may be stored after unrelated events when a foreground prompt
/// suspends a turn, so association is by call id rather than adjacency.
pub(crate) fn normalize_tool_call_groups(messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
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

pub(crate) fn transcript_event_to_chat_message(
    event_type: &str,
    event: TranscriptEvent,
) -> Option<ChatMessage> {
    match event_type {
        "user" => event
            .content
            .filter(|text| !text.trim().is_empty())
            .map(ChatMessage::user),
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
            // A turn whose text came back blank replays as its tool calls
            // alone, and as nothing at all when it has none.
            let content = event.content.filter(|text| !text.trim().is_empty());
            let tool_calls = tool_calls.filter(|calls| !calls.is_empty());
            if content.is_none() && tool_calls.is_none() {
                None
            } else {
                Some(ChatMessage {
                    role: "assistant".to_string(),
                    content: content.map(super::wire::ChatContent::Text),
                    tool_call_id: None,
                    tool_calls,
                    reasoning_details,
                })
            }
        }
        "tool_result" => event
            .tool_use_id
            .zip(event.tool_result)
            .map(|(tool_call_id, content)| {
                let content = if content.trim().is_empty() {
                    EMPTY_TOOL_RESULT.to_string()
                } else {
                    content
                };
                ChatMessage::tool(tool_call_id, content)
            }),
        _ => None,
    }
}
