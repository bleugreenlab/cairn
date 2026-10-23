//! Application adapter for the OpenAI Chat Completions protocol in `cairn-models`.
//! Core supplies credentials, model selection, history loading, and persistence.

pub(crate) mod conversation;
pub(crate) mod http;
pub(crate) use cairn_models::wire::openai_compat::wire;

use crate::backends::{completion_generation, CompletionError, CompletionRequest, CompletionRole};
use cairn_models::turn::ChatFamily;
use serde_json::Value;
use wire::{ChatContent, ChatMessage};

/// The request body for a one-shot, tool-free chat completion on `family`.
///
/// Built by the wire's [`http::build_body`] from the provider's own family, the
/// same builder and the same provider data as a session turn. So the caller's
/// reasoning effort reaches the provider in that provider's spelling, and the
/// keys the provider adds to every body (a custom server's sampling,
/// OpenRouter's routing object) ride on a completion exactly as on a turn.
/// Deliberately toolless: a completion is a single call that returns text, not
/// a turn loop, so offering tools would advertise a capability nothing here can
/// service.
///
/// The one place a completion departs from its provider's session dialect is
/// the output schema, which it always asks for natively. A session may leave
/// `response_format` off because a strict JSON reply would foreclose its tool
/// calls; a completion has no tools, and the schema is its whole contract.
pub(crate) fn completion_body(
    request: &CompletionRequest,
    family: &ChatFamily,
) -> Result<Value, CompletionError> {
    if request.messages.is_empty() {
        return Err(CompletionError::InvalidRequest(
            "at least one message is required".to_string(),
        ));
    }
    let mut messages = Vec::with_capacity(request.messages.len() + 1);
    if let Some(system) = &request.system {
        messages.push(ChatMessage::system(system.clone()));
    }
    messages.extend(request.messages.iter().map(|message| match message.role {
        CompletionRole::User => ChatMessage::user(message.content.clone()),
        CompletionRole::Assistant => ChatMessage {
            role: "assistant".to_string(),
            content: Some(ChatContent::Text(message.content.clone())),
            tool_call_id: None,
            tool_calls: None,
            reasoning_details: None,
            reasoning: None,
        },
    }));
    let dialect = http::ChatDialect {
        response_format: true,
        ..family.dialect
    };
    Ok(http::build_body(
        &dialect,
        &request.model,
        &messages,
        &completion_generation(request),
        &family.extras,
        false,
    ))
}
