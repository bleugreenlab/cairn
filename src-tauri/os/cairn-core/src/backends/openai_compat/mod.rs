//! Application adapter for the OpenAI Chat Completions protocol in `cairn-llm`.
//! Core supplies credentials, model selection, history loading, and persistence.

pub(crate) mod conversation;
pub(crate) mod http;
pub(crate) use cairn_llm::wire::openai_compat::wire;

use crate::backends::{CompletionError, CompletionRequest, CompletionRole};
use serde_json::{json, Value};

/// The request body for a one-shot, tool-free chat completion.
///
/// Deliberately toolless: a completion is a single call that returns text, not
/// a turn loop, so offering tools would advertise a capability nothing here can
/// service. Shared by every provider on this wire rather than reimplemented per
/// adapter, so a schema-constrained call means the same thing on all of them.
pub(crate) fn completion_body(request: &CompletionRequest) -> Result<Value, CompletionError> {
    if request.messages.is_empty() {
        return Err(CompletionError::InvalidRequest(
            "at least one message is required".to_string(),
        ));
    }
    let mut messages = Vec::with_capacity(request.messages.len() + 1);
    if let Some(system) = &request.system {
        messages.push(json!({"role": "system", "content": system}));
    }
    messages.extend(request.messages.iter().map(|message| {
        let role = match message.role {
            CompletionRole::User => "user",
            CompletionRole::Assistant => "assistant",
        };
        json!({"role": role, "content": message.content})
    }));
    let mut body = json!({
        "model": request.model,
        "messages": messages,
        "stream": false,
    });
    if let Some(extras) = request.extras.as_object() {
        body.as_object_mut()
            .expect("completion body is an object")
            .extend(extras.clone());
    } else if !request.extras.is_null() {
        return Err(CompletionError::InvalidRequest(
            "extras must be an object or null".to_string(),
        ));
    }
    http::apply_output_schema(&mut body, request.output_schema.as_ref());
    Ok(body)
}
