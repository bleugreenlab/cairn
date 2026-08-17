//! Rebuild the OpenAI Responses input a turn sends, and trim it to fit.
//!
//! Cairn replays an EXPLICIT item list every turn rather than leaning on the
//! provider-retained `previous_response_id`. That is deliberate: a Cairn session
//! resumes from its own persisted transcript days later, across restarts and
//! across gateways that need not retain anything, so a conversation that only
//! exists on the provider's side is a conversation Cairn cannot resume.
//!
//! Unlike a role-keyed message array, this protocol is a FLAT ordered item list.
//! A call and its output are siblings, so pairing them is a matter of item order
//! rather than message grouping.

use super::wire::{ContentPart, ResponsesItem, ResponsesTurn};
use crate::agent_process::stream::TranscriptEvent;
use crate::backends::http_loop::transcript::{
    load_prior_rows, stored_reasoning_details, ReplayRow, EMPTY_TOOL_RESULT,
    INTERRUPTED_TOOL_RESULT,
};
use crate::backends::http_loop::{context_fit_budget, CHARS_PER_TOKEN, PROTECT_RECENT_MESSAGES};
use crate::backends::{SessionConfig, SessionStart};
use crate::orchestrator::Orchestrator;
use base64::Engine;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;

/// Why a wholly empty turn is refused instead of assembled.
const EMPTY_USER_MESSAGE: &str = "Refusing to start a turn with an empty user message: an empty input item is rejected by the provider and would poison every later replay of this conversation.";

pub(crate) fn build_conversation_messages(
    orch: &Orchestrator,
    config: &SessionConfig,
    session_id: &str,
    system_prompt: &str,
) -> Result<Vec<ResponsesTurn>, String> {
    if config.message_content.is_blank() {
        return Err(EMPTY_USER_MESSAGE.to_string());
    }
    let mut items = vec![ResponsesItem::Instructions {
        text: system_prompt.to_string(),
    }];
    if !matches!(config.session_start, SessionStart::New { .. }) {
        items.extend(load_prior_items(
            orch,
            session_id,
            &config.run_id,
            &config.project_id,
            &config.project_key,
        )?);
    }
    items.push(user_item(&config.message_content));
    // Each replayed item is its own turn; only a fresh generation groups several
    // items (reasoning + message + calls) under one.
    Ok(items.into_iter().map(ResponsesTurn::one).collect())
}

/// A user turn's text and images as one input message.
///
/// Never emits an empty text part beside images: the provider rejects one, and a
/// persisted empty part makes every later replay fail the same way (CAIRN-3263).
pub(crate) fn user_item(content: &crate::agent_process::stdin::MessageContent) -> ResponsesItem {
    let mut parts = Vec::with_capacity(content.images.len() + 1);
    if !content.text.trim().is_empty() {
        parts.push(ContentPart::InputText {
            text: content.text.clone(),
        });
    }
    parts.extend(content.images.iter().map(|image| {
        let encoded = base64::engine::general_purpose::STANDARD.encode(&image.bytes);
        ContentPart::InputImage {
            image_url: format!("data:{};base64,{encoded}", image.mime_type),
        }
    }));
    ResponsesItem::Message {
        id: None,
        role: "user".to_string(),
        content: parts,
    }
}

fn load_prior_items(
    orch: &Orchestrator,
    session_id: &str,
    current_run_id: &str,
    project_id: &str,
    project_key: &str,
) -> Result<Vec<ResponsesItem>, String> {
    let rows = load_prior_rows(orch, session_id, current_run_id, project_id, project_key)?;
    let mut items = Vec::new();
    for row in rows {
        match row {
            ReplayRow::User(content) => items.push(user_item(&content)),
            ReplayRow::Event { event_type, event } => {
                items.extend(transcript_event_to_items(&event_type, *event))
            }
        }
    }
    Ok(normalize_call_pairs(items))
}

/// Map one persisted transcript event into its Responses items.
///
/// An assistant turn can produce SEVERAL items — reasoning, a message, and one
/// item per tool call — because this protocol has no single message that can
/// carry all three.
pub(crate) fn transcript_event_to_items(
    event_type: &str,
    event: TranscriptEvent,
) -> Vec<ResponsesItem> {
    match event_type {
        "assistant" => {
            let mut items = Vec::new();
            // Reasoning leads the turn it belongs to, and replays with its
            // opaque `encrypted_content` intact: the provider validates it, and
            // a summary alone will not stand in for it.
            if let Some(details) = stored_reasoning_details(&event) {
                items.extend(replayable_reasoning(details));
            }
            if let Some(text) = event.content.filter(|text| !text.trim().is_empty()) {
                items.push(ResponsesItem::assistant_text(text));
            }
            for tool in event.tool_uses.unwrap_or_default() {
                items.push(ResponsesItem::FunctionCall {
                    id: None,
                    call_id: tool.id,
                    name: tool.name,
                    arguments: serde_json::to_string(&tool.input)
                        .unwrap_or_else(|_| "{}".to_string()),
                });
            }
            items
        }
        "tool_result" => event
            .tool_use_id
            .zip(event.tool_result)
            .map(|(call_id, output)| {
                let output = if output.trim().is_empty() {
                    EMPTY_TOOL_RESULT.to_string()
                } else {
                    output
                };
                vec![ResponsesItem::FunctionCallOutput { call_id, output }]
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// The reasoning items a stored assistant event can legally replay.
///
/// Reasoning is persisted as this protocol's own items, so this is a round-trip
/// rather than a translation; anything that is not a reasoning item (or does not
/// parse) is dropped rather than guessed at.
fn replayable_reasoning(details: Value) -> Vec<ResponsesItem> {
    serde_json::from_value::<Vec<ResponsesItem>>(details)
        .unwrap_or_default()
        .into_iter()
        .filter(|item| matches!(item, ResponsesItem::Reasoning { .. }))
        .collect()
}

/// Reconstruct valid call/output pairing from persisted history.
///
/// Results may be stored after unrelated events when a foreground prompt
/// suspends a turn, so association is by call id rather than adjacency: each
/// output is lifted out of wherever it sat and re-emitted directly after its
/// call. A call whose output never landed gets a stated placeholder, because an
/// unanswered `function_call` is rejected.
pub(crate) fn normalize_call_pairs(items: Vec<ResponsesItem>) -> Vec<ResponsesItem> {
    let mut outputs: HashMap<String, ResponsesItem> = HashMap::new();
    let mut duplicates = Vec::new();
    let mut remaining = Vec::with_capacity(items.len());
    for item in items {
        match item {
            ResponsesItem::FunctionCallOutput { ref call_id, .. } => {
                if outputs.insert(call_id.clone(), item.clone()).is_some() {
                    duplicates.push(call_id.clone());
                }
            }
            ResponsesItem::Unknown => {}
            item => remaining.push(item),
        }
    }

    let mut synthesized = Vec::new();
    let mut out = Vec::with_capacity(remaining.len());
    for item in remaining {
        let call_id = match &item {
            ResponsesItem::FunctionCall { call_id, .. } => Some(call_id.clone()),
            _ => None,
        };
        out.push(item);
        let Some(call_id) = call_id else {
            continue;
        };
        let output = outputs.remove(&call_id).unwrap_or_else(|| {
            synthesized.push(call_id.clone());
            ResponsesItem::FunctionCallOutput {
                call_id,
                output: INTERRUPTED_TOOL_RESULT.to_string(),
            }
        });
        out.push(output);
    }

    if !synthesized.is_empty() || !duplicates.is_empty() || !outputs.is_empty() {
        log::warn!(
            "Repaired OpenAI Responses call history: synthesized={:?}, duplicates={:?}, orphans={:?}",
            synthesized,
            duplicates,
            outputs.keys().collect::<Vec<_>>()
        );
    }
    out
}

// === Outgoing context trimming ===

fn estimate_turn_tokens(turn: &ResponsesTurn) -> i64 {
    (turn.estimated_chars() as i64) / CHARS_PER_TOKEN + 4
}

pub(crate) fn estimate_conversation_tokens(turns: &[ResponsesTurn]) -> i64 {
    turns.iter().map(estimate_turn_tokens).sum()
}

/// Return a view of `items` that fits under the model's real context window,
/// collapsing aged tool outputs only when necessary. Borrows the input untouched
/// when the window is unknown or the request already fits.
pub(crate) fn fit_conversation<'a>(
    turns: &'a [ResponsesTurn],
    context_window: Option<i64>,
) -> Cow<'a, [ResponsesTurn]> {
    let Some(window) = context_window.filter(|window| *window > 0) else {
        return Cow::Borrowed(turns);
    };
    let budget = context_fit_budget(window);
    if estimate_conversation_tokens(turns) <= budget {
        return Cow::Borrowed(turns);
    }
    Cow::Owned(trim_conversation_to_budget(turns, budget))
}

/// Collapse the oldest function-call outputs to short markers until the
/// estimated request fits under `budget`. Instructions, user turns, and the
/// assistant's call decisions are never touched, and recent items are protected.
pub(crate) fn trim_conversation_to_budget(
    turns: &[ResponsesTurn],
    budget: i64,
) -> Vec<ResponsesTurn> {
    let mut trimmed = turns.to_vec();
    let mut tool_names: HashMap<&str, &str> = HashMap::new();
    for turn in turns {
        for item in &turn.items {
            if let ResponsesItem::FunctionCall { call_id, name, .. } = item {
                tool_names.insert(call_id.as_str(), name.as_str());
            }
        }
    }

    let protect_from = trimmed.len().saturating_sub(PROTECT_RECENT_MESSAGES);
    let mut estimate = estimate_conversation_tokens(turns);
    for turn in trimmed.iter_mut().take(protect_from) {
        if estimate <= budget {
            break;
        }
        let before = estimate_turn_tokens(turn);
        for item in turn.items.iter_mut() {
            let ResponsesItem::FunctionCallOutput { call_id, output } = item else {
                continue;
            };
            let lines = output.lines().count().max(1);
            let name = tool_names.get(call_id.as_str()).copied().unwrap_or("tool");
            *output = format!("[{name} output elided — {lines} lines]");
        }
        estimate -= before - estimate_turn_tokens(turn);
    }
    trimmed
}
