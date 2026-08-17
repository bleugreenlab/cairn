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
use serde_json::{json, Value};

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
