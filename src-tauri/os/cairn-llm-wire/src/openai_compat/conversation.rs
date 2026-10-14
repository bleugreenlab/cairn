//! Rebuild the OpenAI-style message array an OpenAI-compatible turn sends: assemble the
//! system + prior transcript + new user message, normalize assistant/tool groups,
//! and map stored transcript events to chat messages.

use super::wire::{default_function_type, ChatMessage, ToolCall, ToolFunction};
use crate::replay::ReplayEvent;
use crate::replay::{stored_reasoning_details, EMPTY_TOOL_RESULT, INTERRUPTED_TOOL_RESULT};
use std::collections::HashMap;

/// Reconstruct protocol-valid assistant/tool groups from persisted history.
/// Results may be stored after unrelated events when a foreground prompt
/// suspends a turn, so association is by call id rather than adjacency.
pub fn normalize_tool_call_groups(messages: Vec<ChatMessage>) -> Vec<ChatMessage> {
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

pub fn transcript_event_to_chat_message(
    event_type: &str,
    event: ReplayEvent,
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
            // Replay structured reasoning verbatim and in original order.
            let reasoning_details = stored_reasoning_details(&event);
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
                    reasoning: None,
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
