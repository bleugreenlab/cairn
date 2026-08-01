//! Provider-neutral OpenAI-compatible chat-completions protocol.
//!
//! This module owns wire DTOs, conversation replay and repair, context trimming,
//! and the streaming SSE transport. Provider adapters supply an endpoint and
//! provider-specific request-body additions.

pub(crate) mod context;
pub(crate) mod conversation;
pub(crate) mod http;
pub(crate) mod wire;

use serde_json::{json, Value};

pub(crate) fn tool_schemas() -> Vec<Value> {
    vec![
        json!({
            "type": "function",
            "function": {
                "name": "read",
                "description": "Read one or more file, Cairn resource, web, or PDF targets. Prefer paths[] for batch reads.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "paths": { "type": "array", "items": { "type": "string" }, "minItems": 1 },
                        "path": { "type": "string" },
                        "offset": { "type": "integer" },
                        "limit": { "type": "integer" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "write",
                "description": "Apply ordered file/resource mutations. Include commit_msg when touching files.",
                "parameters": {
                    "type": "object",
                    "required": ["changes"],
                    "properties": {
                        "changes": { "type": "array", "items": { "type": "object" }, "minItems": 1 },
                        "commit_msg": { "type": "string" },
                        "preview": { "type": "boolean" },
                        "atomic": { "type": "boolean" }
                    }
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "run",
                "description": "Execute shell commands, inline code, or skill scripts.",
                "parameters": {
                    "type": "object",
                    "required": ["commands"],
                    "properties": {
                        "commands": { "type": "array", "items": { "type": "object" }, "minItems": 1 },
                        "commit_msg": { "type": "string" },
                        "sequential": { "type": "boolean" },
                        "stop_on_error": { "type": "boolean" }
                    }
                }
            }
        }),
    ]
}
