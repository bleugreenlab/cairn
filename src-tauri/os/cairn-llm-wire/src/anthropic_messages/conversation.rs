//! Rebuild the Anthropic Messages conversation a turn sends, and trim it to fit.
//!
//! Two rules make this protocol's replay different from a chat-completions one,
//! and both are structural rather than cosmetic:
//!
//! - There is no `tool` role. A tool result is a `tool_result` block inside the
//!   USER message that follows the assistant turn which called it, so pairing a
//!   call to its result moves whole messages around rather than reordering
//!   siblings.
//! - Roles must alternate. Two adjacent user messages are rejected, so adjacent
//!   same-role messages are coalesced into one — which is exactly what lets a
//!   batch of tool results and the next user prompt share a message.

use super::wire::{ContentBlock, MessagesMessage, ASSISTANT_ROLE, SYSTEM_ROLE, USER_ROLE};
use crate::replay::ReplayEvent;
use crate::replay::{stored_reasoning_details, EMPTY_TOOL_RESULT, INTERRUPTED_TOOL_RESULT};
use crate::{context_fit_budget, CHARS_PER_TOKEN, PROTECT_RECENT_MESSAGES};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;

/// Map one persisted transcript event into its Messages-shaped message.
pub fn transcript_event_to_message(
    event_type: &str,
    event: ReplayEvent,
) -> Option<MessagesMessage> {
    match event_type {
        "assistant" => {
            let mut content = Vec::new();
            // Thinking leads the turn. Anthropic requires the thinking block
            // that produced a tool call to precede it, with its signature
            // intact, or the request is rejected on replay.
            if let Some(details) = stored_reasoning_details(&event) {
                content.extend(replayable_thinking(details));
            }
            if let Some(text) = event.content.filter(|text| !text.trim().is_empty()) {
                content.push(ContentBlock::Text { text });
            }
            for tool in event.tool_uses.unwrap_or_default() {
                content.push(ContentBlock::ToolUse {
                    id: tool.id,
                    name: tool.name,
                    input: tool.input,
                });
            }
            // Thinking alone is not a turn: a message carrying only reasoning
            // says nothing the model can continue from, and Anthropic rejects a
            // thinking block that answers nothing.
            let speaks = content.iter().any(|block| {
                !matches!(
                    block,
                    ContentBlock::Thinking { .. } | ContentBlock::RedactedThinking { .. }
                )
            });
            speaks.then(|| MessagesMessage::assistant(content))
        }
        "tool_result" => event
            .tool_use_id
            .zip(event.tool_result)
            .map(|(tool_use_id, content)| {
                let content = if content.trim().is_empty() {
                    EMPTY_TOOL_RESULT.to_string()
                } else {
                    content
                };
                MessagesMessage::tool_result(tool_use_id, content)
            }),
        _ => None,
    }
}

/// The thinking blocks a stored assistant event can legally replay.
///
/// Reasoning is persisted as the protocol's own block array, so this is a
/// round-trip rather than a translation; anything that is not a thinking block
/// (or does not parse) is dropped rather than guessed at.
fn replayable_thinking(details: Value) -> Vec<ContentBlock> {
    serde_json::from_value::<Vec<ContentBlock>>(details)
        .unwrap_or_default()
        .into_iter()
        .filter(|block| {
            matches!(
                block,
                ContentBlock::Thinking { .. } | ContentBlock::RedactedThinking { .. }
            )
        })
        .collect()
}

/// Reconstruct protocol-valid call/result pairing from persisted history.
///
/// Results may be stored after unrelated events when a foreground prompt
/// suspends a turn, so association is by call id rather than adjacency: every
/// result is lifted out of wherever it sat and re-emitted in the user message
/// immediately after the assistant turn that called it. A call whose result
/// never landed gets a stated placeholder, because an unanswered `tool_use` is
/// rejected outright.
pub fn normalize_tool_groups(messages: Vec<MessagesMessage>) -> Vec<MessagesMessage> {
    let mut results: HashMap<String, ContentBlock> = HashMap::new();
    let mut duplicates = Vec::new();
    let mut stripped: Vec<MessagesMessage> = Vec::with_capacity(messages.len());
    for mut message in messages {
        if message.role == USER_ROLE {
            message.content.retain(|block| match block {
                ContentBlock::ToolResult { tool_use_id, .. } => {
                    if results.insert(tool_use_id.clone(), block.clone()).is_some() {
                        duplicates.push(tool_use_id.clone());
                    }
                    false
                }
                _ => true,
            });
            if message.content.is_empty() {
                continue;
            }
        }
        stripped.push(message);
    }

    let mut synthesized = Vec::new();
    let mut out = Vec::with_capacity(stripped.len());
    for message in stripped {
        let call_ids = if message.role == ASSISTANT_ROLE {
            message.tool_use_ids()
        } else {
            Vec::new()
        };
        out.push(message);
        if call_ids.is_empty() {
            continue;
        }
        let content = call_ids
            .into_iter()
            .map(|call_id| {
                results.remove(&call_id).unwrap_or_else(|| {
                    synthesized.push(call_id.clone());
                    ContentBlock::ToolResult {
                        tool_use_id: call_id,
                        content: INTERRUPTED_TOOL_RESULT.to_string(),
                        is_error: false,
                    }
                })
            })
            .collect();
        out.push(MessagesMessage {
            role: USER_ROLE.to_string(),
            content,
        });
    }

    if !synthesized.is_empty() || !duplicates.is_empty() || !results.is_empty() {
        log::warn!(
            "Repaired Anthropic Messages tool history: synthesized={:?}, duplicates={:?}, orphans={:?}",
            synthesized,
            duplicates,
            results.keys().collect::<Vec<_>>()
        );
    }
    coalesce_roles(out)
}

/// Append one tool result to a live conversation in the shape replay rebuilds
/// it in: results answering one assistant turn share a single user message.
///
/// The result joins the trailing message when that message holds only tool
/// results, and starts a new user message otherwise, so a result never merges
/// into a real user prompt.
pub fn push_tool_result(
    conversation: &mut Vec<MessagesMessage>,
    tool_use_id: String,
    content: String,
) {
    let result = MessagesMessage::tool_result(tool_use_id, content);
    match conversation.last_mut() {
        Some(last)
            if last.role == USER_ROLE
                && !last.content.is_empty()
                && last
                    .content
                    .iter()
                    .all(|block| matches!(block, ContentBlock::ToolResult { .. })) =>
        {
            last.content.extend(result.content);
        }
        _ => conversation.push(result),
    }
}

/// Merge adjacent same-role messages, dropping any left with no content.
///
/// The protocol requires alternating roles, so this is correctness rather than
/// tidiness: without it, a batch of tool results followed by the user's next
/// prompt would send two user messages in a row and be rejected.
pub fn coalesce_roles(messages: Vec<MessagesMessage>) -> Vec<MessagesMessage> {
    let mut out: Vec<MessagesMessage> = Vec::with_capacity(messages.len());
    for message in messages {
        if message.content.is_empty() {
            continue;
        }
        match out.last_mut() {
            Some(previous) if previous.role == message.role && message.role != SYSTEM_ROLE => {
                previous.content.extend(message.content);
            }
            _ => out.push(message),
        }
    }
    out
}

// === Outgoing context trimming ===

fn estimate_message_tokens(message: &MessagesMessage) -> i64 {
    (message.estimated_chars() as i64) / CHARS_PER_TOKEN + 4
}

pub fn estimate_conversation_tokens(messages: &[MessagesMessage]) -> i64 {
    messages.iter().map(estimate_message_tokens).sum()
}

/// Return a view of `messages` that fits under the model's real context window,
/// collapsing aged tool outputs only when necessary. Borrows the input untouched
/// when the window is unknown or the request already fits. The stored transcript
/// is never affected — only this reconstructed outgoing array.
pub fn fit_conversation<'a>(
    messages: &'a [MessagesMessage],
    context_window: Option<i64>,
) -> Cow<'a, [MessagesMessage]> {
    let Some(window) = context_window.filter(|window| *window > 0) else {
        return Cow::Borrowed(messages);
    };
    let budget = context_fit_budget(window);
    if estimate_conversation_tokens(messages) <= budget {
        return Cow::Borrowed(messages);
    }
    Cow::Owned(trim_conversation_to_budget(messages, budget))
}

/// Collapse the oldest tool-result blocks to short markers until the estimated
/// request fits under `budget`. The system prompt, user turns, and assistant
/// reasoning / tool-call decisions are never touched, and the most recent
/// exchanges are protected.
pub fn trim_conversation_to_budget(
    messages: &[MessagesMessage],
    budget: i64,
) -> Vec<MessagesMessage> {
    let mut trimmed = messages.to_vec();
    // Map each call id to the tool it invoked so a collapsed marker can name its
    // source. Borrows `messages`, not `trimmed`.
    let mut tool_names: HashMap<&str, &str> = HashMap::new();
    for message in messages {
        for block in &message.content {
            if let ContentBlock::ToolUse { id, name, .. } = block {
                tool_names.insert(id.as_str(), name.as_str());
            }
        }
    }

    let protect_from = trimmed.len().saturating_sub(PROTECT_RECENT_MESSAGES);
    let mut estimate = estimate_conversation_tokens(messages);
    for message in trimmed.iter_mut().take(protect_from) {
        if estimate <= budget {
            break;
        }
        let before = estimate_message_tokens(message);
        for block in message.content.iter_mut() {
            let ContentBlock::ToolResult {
                tool_use_id,
                content,
                ..
            } = block
            else {
                continue;
            };
            let lines = content.lines().count().max(1);
            let name = tool_names
                .get(tool_use_id.as_str())
                .copied()
                .unwrap_or("tool");
            *content = format!("[{name} output elided — {lines} lines]");
        }
        estimate -= before - estimate_message_tokens(message);
    }
    trimmed
}
