//! Anthropic Messages response -> neutral [`Generation`].
//!
//! Tool-name canonicalization happens here, BEFORE anything is stored, pushed
//! into the conversation, or executed, so execution, the stored transcript, and
//! any replay all reference the dispatched verb. Otherwise a successful call
//! replays under an invalid name and reinforces it in the model-facing history.

use super::wire::{parse_cost, ContentBlock, MessagesMessage, MessagesResponse};
use crate::backends::http_loop::{repair, Generation, TurnToolCall};
use serde_json::Value;

pub(crate) fn into_generation(
    response: MessagesResponse,
    provider_name: &str,
) -> Result<Generation<MessagesMessage>, String> {
    let MessagesResponse {
        id,
        model,
        content,
        stop_reason,
        usage,
        cost,
        streamed_text,
        raw_tool_input,
    } = response;
    let cost = parse_cost(cost.as_ref());

    // A block this build cannot describe is dropped rather than echoed: replaying
    // a block Cairn could not parse would corrupt the conversation it is trying
    // to preserve.
    let mut content: Vec<ContentBlock> = content
        .into_iter()
        .filter(ContentBlock::is_replayable)
        .collect();

    for block in content.iter_mut() {
        let ContentBlock::ToolUse { name, .. } = block else {
            continue;
        };
        if let Some(verb) = repair::normalize_tool_name(name) {
            if verb != name.as_str() {
                log::warn!("{provider_name} normalized tool name {name:?} -> {verb:?}");
                *name = verb.to_string();
            }
        }
    }

    let assistant_text = content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();

    let tool_calls = content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolUse { id, name, input } => Some(TurnToolCall {
                id: id.clone(),
                name: name.clone(),
                // Prefer the raw streamed argument text: a truncated payload has
                // to reach the repair path as the model actually emitted it, not
                // as the empty object a failed parse leaves behind.
                arguments: raw_tool_input
                    .get(id)
                    .cloned()
                    .unwrap_or_else(|| input.to_string()),
            }),
            _ => None,
        })
        .collect();

    // Reasoning is persisted as this protocol's own block array, so a resumed
    // turn replays thinking (and its signature) verbatim instead of translating
    // it through a neutral shape that cannot carry a signature.
    let reasoning: Vec<Value> = content
        .iter()
        .filter(|block| {
            matches!(
                block,
                ContentBlock::Thinking { .. } | ContentBlock::RedactedThinking { .. }
            )
        })
        .filter_map(|block| serde_json::to_value(block).ok())
        .collect();

    Ok(Generation {
        assistant_text,
        tool_calls,
        reasoning_details: (!reasoning.is_empty()).then_some(Value::Array(reasoning)),
        usage: usage.map(|usage| usage.into_turn_usage(cost)),
        finish_reason: stop_reason.map(neutral_finish_reason),
        generation_id: id,
        response_model: model,
        streamed_text,
        assistant_message: MessagesMessage::assistant(content),
    })
}

/// Anthropic's `stop_reason` in the turn loop's vocabulary.
///
/// The loop reads `"length"` as "the output was cut off at the token cap" and
/// refuses to dispatch a side-effecting tool call that may be truncated.
/// Anthropic spells that same condition `max_tokens`, so it is translated rather
/// than passed through — leaving it untranslated would silently disarm the
/// truncation guard. Every other reason passes through as itself, because
/// pretending `end_turn` is a chat-completions `stop` would claim an equivalence
/// this protocol does not have.
fn neutral_finish_reason(stop_reason: String) -> String {
    match stop_reason.as_str() {
        "max_tokens" => "length".to_string(),
        _ => stop_reason,
    }
}
