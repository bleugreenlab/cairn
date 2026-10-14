//! Tool definitions in a protocol-neutral shape, and Cairn's own three verbs.
//!
//! What a tool is named and what arguments it accepts is ONE fact about the
//! host that dispatches it, not three facts about three wire protocols. Each
//! protocol module serializes its own wrapper around a list of these
//! (chat/completions nests `{"type":"function","function":{...}}`, Anthropic
//! Messages names the schema `input_schema`, OpenAI Responses flattens
//! name/parameters beside `strict`), so a change to a tool's schema reaches
//! every family at once and the three cannot drift into describing different
//! tools to different models.
//!
//! The list a request advertises is the caller's, through
//! [`crate::GenerationConfig::tools`]. Cairn passes
//! [`cairn_tool_definitions`]; a host outside Cairn passes its own.

use serde_json::{json, Value};

/// One tool as the model sees it: the dispatched name, the sentence that tells
/// the model what it is for, and the JSON Schema for its arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

impl ToolDefinition {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema,
        }
    }
}

/// Cairn's three verbs, in the order every protocol advertises them. An agent
/// run is nothing but `read`/`write`/`run` calls.
pub fn cairn_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition::new(
            "read",
            "Read one or more file, Cairn resource, web, or PDF targets. Prefer paths[] for batch reads.",
            json!({
                "type": "object",
                "properties": {
                    "paths": { "type": "array", "items": { "type": "string" }, "minItems": 1 },
                    "path": { "type": "string" },
                    "offset": { "type": "integer" },
                    "limit": { "type": "integer" }
                }
            }),
        ),
        ToolDefinition::new(
            "write",
            "Apply ordered file/resource mutations. Include commit_msg when touching files.",
            json!({
                "type": "object",
                "required": ["changes"],
                "properties": {
                    "changes": { "type": "array", "items": { "type": "object" }, "minItems": 1 },
                    "commit_msg": { "type": "string" },
                    "preview": { "type": "boolean" },
                    "atomic": { "type": "boolean" }
                }
            }),
        ),
        ToolDefinition::new(
            "run",
            "Execute shell commands, inline code, or skill scripts.",
            json!({
                "type": "object",
                "required": ["commands"],
                "properties": {
                    "commands": { "type": "array", "items": { "type": "object" }, "minItems": 1 },
                    "commit_msg": { "type": "string" },
                    "sequential": { "type": "boolean" },
                    "stop_on_error": { "type": "boolean" }
                }
            }),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::cairn_tool_definitions;

    #[test]
    fn every_protocol_advertises_the_same_three_verbs() {
        let definitions = cairn_tool_definitions();
        let names: Vec<&str> = definitions.iter().map(|tool| tool.name.as_str()).collect();
        assert_eq!(names, vec!["read", "write", "run"]);
    }

    #[test]
    fn the_side_effecting_verbs_state_their_required_argument() {
        // A protocol wrapper only renames the schema field around this object,
        // so `required` living here is what keeps the three families demanding
        // the same payload.
        let definitions = cairn_tool_definitions();
        let required = |name: &str| -> Vec<String> {
            definitions
                .iter()
                .find(|tool| tool.name == name)
                .and_then(|tool| tool.input_schema.get("required").cloned())
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default()
        };
        assert_eq!(required("write"), vec!["changes".to_string()]);
        assert_eq!(required("run"), vec!["commands".to_string()]);
        // `read` accepts either `paths` or `path`, so it requires neither.
        assert!(required("read").is_empty());
    }
}
