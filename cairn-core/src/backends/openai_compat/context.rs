//! Trim the OUTGOING request to fit the model's real context window: estimate
//! per-message token cost, and collapse the oldest aged tool outputs oldest-first
//! (protecting the system prompt, user turns, assistant tool-call decisions, and
//! the most recent exchanges). Never touches the stored transcript.

use super::wire::{ChatContent, ChatMessage};
use crate::backends::http_loop::{CHARS_PER_TOKEN, PROTECT_RECENT_MESSAGES};
// The budget policy is shared by every HTTP protocol family; re-exported here
// for this module's callers and tests.
pub(crate) use crate::backends::http_loop::context_fit_budget;
use std::borrow::Cow;
use std::collections::HashMap;

/// Rough token estimate for a single message: content, any tool-call name and
/// arguments, and serialized reasoning, divided by the char heuristic plus a
/// small per-message framing overhead.
fn estimate_message_tokens(message: &ChatMessage) -> i64 {
    let mut chars = message
        .content
        .as_ref()
        .map(ChatContent::estimated_chars)
        .unwrap_or(0);
    if let Some(calls) = &message.tool_calls {
        for call in calls {
            chars += call.function.name.len() + call.function.arguments.len();
        }
    }
    if let Some(details) = &message.reasoning_details {
        chars += details.to_string().len();
    }
    (chars as i64) / CHARS_PER_TOKEN + 4
}

pub(crate) fn estimate_conversation_tokens(messages: &[ChatMessage]) -> i64 {
    messages.iter().map(estimate_message_tokens).sum()
}

/// Return a view of `messages` that fits under the model's real context window,
/// trimming aged tool outputs only when necessary. Borrows the input untouched
/// when the window is unknown or the request already fits; otherwise returns an
/// owned, trimmed copy. The stored transcript is never affected — only this
/// reconstructed outgoing array.
pub(crate) fn fit_conversation<'a>(
    messages: &'a [ChatMessage],
    context_window: Option<i64>,
) -> Cow<'a, [ChatMessage]> {
    let Some(window) = context_window.filter(|window| *window > 0) else {
        return Cow::Borrowed(messages);
    };
    let budget = context_fit_budget(window);
    if estimate_conversation_tokens(messages) <= budget {
        return Cow::Borrowed(messages);
    }
    Cow::Owned(trim_conversation_to_budget(messages, budget))
}

/// Collapse the oldest tool-result messages to short markers until the estimated
/// request fits under `budget`. The system prompt, user turns, and assistant
/// reasoning / tool-call decisions are never touched (only `tool` messages are
/// eligible), and the most recent exchanges' tool outputs are protected. When
/// even collapsing every eligible tool output cannot reach budget, the request
/// is left as small as possible and the read timeout backstops the over-limit
/// send.
pub(crate) fn trim_conversation_to_budget(
    messages: &[ChatMessage],
    budget: i64,
) -> Vec<ChatMessage> {
    let mut trimmed = messages.to_vec();
    // Map each tool_call id to its tool name so a collapsed marker can name the
    // source (`read`/`write`/`run`). Borrows `messages`, not `trimmed`.
    let mut tool_names: HashMap<&str, &str> = HashMap::new();
    for message in messages {
        if let Some(calls) = &message.tool_calls {
            for call in calls {
                tool_names.insert(call.id.as_str(), call.function.name.as_str());
            }
        }
    }

    let protect_from = trimmed.len().saturating_sub(PROTECT_RECENT_MESSAGES);
    let mut estimate = estimate_conversation_tokens(messages);
    for message in trimmed.iter_mut().take(protect_from) {
        if estimate <= budget {
            break;
        }
        if message.role != "tool" {
            continue;
        }
        let marker = {
            let Some(content) = message.content.as_ref() else {
                continue;
            };
            let Some(text) = content.as_text() else {
                continue;
            };
            let lines = text.lines().count().max(1);
            let name = message
                .tool_call_id
                .as_deref()
                .and_then(|id| tool_names.get(id).copied())
                .unwrap_or("tool");
            format!("[{name} output elided — {lines} lines]")
        };
        let before = estimate_message_tokens(message);
        message.content = Some(ChatContent::Text(marker));
        let after = estimate_message_tokens(message);
        estimate -= before - after;
    }
    trimmed
}
