//! OpenAI Responses response -> neutral [`Generation`].
//!
//! Tool-name canonicalization happens here, BEFORE anything is stored, pushed
//! into the conversation, or executed, so execution, the stored transcript, and
//! any replay all reference the dispatched verb.

use super::wire::{ContentPart, ResponsesItem, ResponsesResponse, ResponsesTurn};
use crate::backends::http_loop::{repair, Generation, TurnToolCall};
use serde_json::Value;

pub(crate) fn into_generation(
    response: ResponsesResponse,
    provider_name: &str,
) -> Result<Generation<ResponsesTurn>, String> {
    let ResponsesResponse {
        id,
        model,
        status,
        incomplete_reason,
        output,
        usage,
        cost,
        streamed_text,
    } = response;

    // An item this build cannot describe is dropped rather than echoed: replaying
    // an item Cairn could not parse would corrupt the conversation it is trying
    // to preserve.
    let mut output: Vec<ResponsesItem> = output
        .into_iter()
        .filter(ResponsesItem::is_replayable)
        .collect();

    for item in output.iter_mut() {
        let ResponsesItem::FunctionCall { name, .. } = item else {
            continue;
        };
        if let Some(verb) = repair::normalize_tool_name(name) {
            if verb != name.as_str() {
                log::warn!("{provider_name} normalized tool name {name:?} -> {verb:?}");
                *name = verb.to_string();
            }
        }
    }

    let assistant_text = output
        .iter()
        .filter_map(|item| match item {
            ResponsesItem::Message { content, .. } => Some(
                content
                    .iter()
                    .filter_map(ContentPart::text)
                    .collect::<String>(),
            ),
            _ => None,
        })
        .collect::<String>();

    let tool_calls: Vec<TurnToolCall> = output
        .iter()
        .filter_map(|item| match item {
            ResponsesItem::FunctionCall {
                call_id,
                name,
                arguments,
                ..
            } => Some(TurnToolCall {
                // `call_id`, not the item id: a `function_call_output` that
                // quotes the wrong one orphans the result.
                id: call_id.clone(),
                name: name.clone(),
                arguments: arguments.clone(),
            }),
            _ => None,
        })
        .collect();

    let reasoning: Vec<Value> = output
        .iter()
        .filter(|item| matches!(item, ResponsesItem::Reasoning { .. }))
        .filter_map(|item| serde_json::to_value(item).ok())
        .collect();

    Ok(Generation {
        assistant_text,
        finish_reason: Some(finish_reason(
            status.as_deref(),
            incomplete_reason.as_deref(),
            !tool_calls.is_empty(),
        )),
        tool_calls,
        reasoning_details: (!reasoning.is_empty()).then_some(Value::Array(reasoning)),
        usage: usage.map(|usage| usage.into_turn_usage(cost)),
        generation_id: id,
        response_model: model,
        streamed_text,
        assistant_message: ResponsesTurn { items: output },
    })
}

/// This protocol's outcome in the turn loop's vocabulary.
///
/// The loop reads `"length"` as "the output was cut off at the token cap" and
/// refuses to dispatch a side-effecting tool call that may be truncated.
/// Responses reports that as `status: "incomplete"` with
/// `incomplete_details.reason = "max_output_tokens"`, so it is translated —
/// leaving it untranslated would silently disarm the truncation guard.
fn finish_reason(status: Option<&str>, incomplete_reason: Option<&str>, has_calls: bool) -> String {
    if status == Some("incomplete") {
        return match incomplete_reason {
            Some("max_output_tokens") => "length".to_string(),
            Some(reason) => reason.to_string(),
            None => "incomplete".to_string(),
        };
    }
    if has_calls {
        return "tool_calls".to_string();
    }
    "stop".to_string()
}
