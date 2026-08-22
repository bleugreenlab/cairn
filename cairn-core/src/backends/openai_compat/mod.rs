//! Provider-neutral OpenAI-compatible chat-completions protocol.
//!
//! This module owns wire DTOs, conversation replay and repair, context trimming,
//! and the streaming SSE transport. Provider adapters supply an endpoint and
//! provider-specific request-body additions.
//!
//! It is one of three protocol siblings behind [`crate::backends::http_loop`],
//! beside `anthropic_messages` and `openai_responses`. The families share the
//! neutral turn loop and the neutral tool definitions, and nothing else: no
//! Messages content block or Responses output item is ever fed through the DTOs
//! here.

pub(crate) mod context;
pub(crate) mod conversation;
pub(crate) mod generation;
pub(crate) mod http;
pub(crate) mod wire;

use crate::backends::http_loop::cairn_tool_definitions;
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

/// Cairn's tools in the chat-completions wrapper: a `function` object nested
/// under a `type` discriminator, with the argument schema named `parameters`.
/// The definitions themselves are neutral (`http_loop::tool_defs`), so this
/// family, Messages, and Responses cannot end up describing different tools.
pub(crate) fn tool_schemas() -> Vec<Value> {
    cairn_tool_definitions()
        .into_iter()
        .map(|tool| {
            json!({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.input_schema,
                }
            })
        })
        .collect()
}
