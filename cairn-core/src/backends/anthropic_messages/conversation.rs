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
use crate::agent_process::stream::TranscriptEvent;
use crate::backends::http_loop::transcript::{
    load_prior_rows, stored_reasoning_details, ReplayRow, EMPTY_TOOL_RESULT,
    INTERRUPTED_TOOL_RESULT,
};
use crate::backends::http_loop::{context_fit_budget, CHARS_PER_TOKEN, PROTECT_RECENT_MESSAGES};
use crate::backends::{SessionConfig, SessionStart};
use crate::orchestrator::Orchestrator;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;

/// Why a wholly empty turn is refused instead of assembled.
const EMPTY_USER_MESSAGE: &str = "Refusing to start a turn with an empty user message: an empty content block is rejected by the provider and would poison every later replay of this conversation.";

pub(crate) fn build_conversation_messages(
    orch: &Orchestrator,
    config: &SessionConfig,
    session_id: &str,
    system_prompt: &str,
) -> Result<Vec<MessagesMessage>, String> {
    if config.message_content.is_blank() {
        return Err(EMPTY_USER_MESSAGE.to_string());
    }
    let mut messages = vec![MessagesMessage::system(system_prompt.to_string())];
    if !matches!(config.session_start, SessionStart::New { .. }) {
        messages.extend(load_prior_messages(
            orch,
            session_id,
            &config.run_id,
            &config.project_id,
            &config.project_key,
        )?);
    }
    messages.push(MessagesMessage::user_content(&config.message_content));
    Ok(coalesce_roles(messages))
}

fn load_prior_messages(
    orch: &Orchestrator,
    session_id: &str,
    current_run_id: &str,
    project_id: &str,
    project_key: &str,
) -> Result<Vec<MessagesMessage>, String> {
    let rows = load_prior_rows(orch, session_id, current_run_id, project_id, project_key)?;
    let messages = rows
        .into_iter()
        .filter_map(|row| match row {
            ReplayRow::User(content) => Some(MessagesMessage::user_content(&content)),
            ReplayRow::Event { event_type, event } => {
                transcript_event_to_message(&event_type, *event)
            }
        })
        .collect();
    Ok(normalize_tool_groups(messages))
}

/// Map one persisted transcript event into its Messages-shaped message.
pub(crate) fn transcript_event_to_message(
    event_type: &str,
    event: TranscriptEvent,
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
pub(crate) fn normalize_tool_groups(messages: Vec<MessagesMessage>) -> Vec<MessagesMessage> {
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

/// Merge adjacent same-role messages, dropping any left with no content.
///
/// The protocol requires alternating roles, so this is correctness rather than
/// tidiness: without it, a batch of tool results followed by the user's next
/// prompt would send two user messages in a row and be rejected.
pub(crate) fn coalesce_roles(messages: Vec<MessagesMessage>) -> Vec<MessagesMessage> {
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

pub(crate) fn estimate_conversation_tokens(messages: &[MessagesMessage]) -> i64 {
    messages.iter().map(estimate_message_tokens).sum()
}

/// Return a view of `messages` that fits under the model's real context window,
/// collapsing aged tool outputs only when necessary. Borrows the input untouched
/// when the window is unknown or the request already fits. The stored transcript
/// is never affected — only this reconstructed outgoing array.
pub(crate) fn fit_conversation<'a>(
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
pub(crate) fn trim_conversation_to_budget(
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
