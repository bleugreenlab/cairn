//! Trim the OUTGOING request to fit the model's real context window: estimate
//! per-message token cost, and collapse the oldest aged tool outputs oldest-first
//! (protecting the system prompt, user turns, assistant tool-call decisions, and
//! the most recent exchanges). Never touches the stored transcript.

use super::wire::ChatMessage;
use std::borrow::Cow;
use std::collections::HashMap;

/// Rough chars-per-token ratio for the trim heuristic. English and code average
/// ~3.5–4 characters per token; 4 deliberately under-counts token volume, with
/// the slack absorbed by trimming to a fraction of the window
/// (`CONTEXT_FIT_FRACTION`) rather than to its exact edge. Phase 1 only needs to
/// stay under the limit, not account perfectly.
const CHARS_PER_TOKEN: i64 = 4;

/// Trim the outgoing request down to this fraction of the model's real context
/// window. The remaining headroom covers the response (including reasoning
/// tokens) plus slack for the approximate char-per-token estimate.
const CONTEXT_FIT_FRACTION: f64 = 0.75;

/// The most recent messages whose tool outputs are never collapsed, so the model
/// keeps full fidelity on the exchange it is actively reasoning about. Older tool
/// outputs are collapsed oldest-first when the request would exceed budget.
const PROTECT_RECENT_MESSAGES: usize = 8;

/// Rough token estimate for a single message: content, any tool-call name and
/// arguments, and serialized reasoning, divided by the char heuristic plus a
/// small per-message framing overhead.
fn estimate_message_tokens(message: &ChatMessage) -> i64 {
    let mut chars = message.content.as_ref().map(String::len).unwrap_or(0);
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

pub(super) fn estimate_conversation_tokens(messages: &[ChatMessage]) -> i64 {
    messages.iter().map(estimate_message_tokens).sum()
}

/// The token budget the outgoing request is trimmed to fit under.
pub(super) fn context_fit_budget(context_window: i64) -> i64 {
    ((context_window as f64) * CONTEXT_FIT_FRACTION) as i64
}

/// Return a view of `messages` that fits under the model's real context window,
/// trimming aged tool outputs only when necessary. Borrows the input untouched
/// when the window is unknown or the request already fits; otherwise returns an
/// owned, trimmed copy. The stored transcript is never affected — only this
/// reconstructed outgoing array.
pub(super) fn fit_conversation<'a>(
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
pub(super) fn trim_conversation_to_budget(
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
            let lines = content.lines().count().max(1);
            let name = message
                .tool_call_id
                .as_deref()
                .and_then(|id| tool_names.get(id).copied())
                .unwrap_or("tool");
            format!("[{name} output elided — {lines} lines]")
        };
        let before = estimate_message_tokens(message);
        message.content = Some(marker);
        let after = estimate_message_tokens(message);
        estimate -= before - after;
    }
    trimmed
}
