//! Adapter form 2: a provider that calls a tool on an MCP server its own pack
//! declares.
//!
//! ```yaml
//! adapter:
//!   mcp:
//!     server: bmd                         # a server in this pack's mcp.yaml
//!     tool: fetch
//!     arguments: { source: "${target}" }
//!     result: text
//! ```
//!
//! The server's own connection authenticates the call, so a binding names no
//! credential and may not interpolate `${credential}`. `arguments` uses the
//! same JSON template as an HTTP body, with the slot's variables and the
//! provider's options. `result: text` is the tool result's text blocks,
//! concatenated; images and structured content have no place in a markdown
//! slot. The host resolves the server through the readiness-gated MCP layer at
//! call time, so a server the user defines under the same name in settings
//! shadows the pack's, as it would for any agent.

use std::collections::BTreeSet;

use serde::Deserialize;
use serde_json::Value;

use super::http::{self, Inputs, JsonTemplate};
use super::{ProviderDecl, Slot, NOT_SUPPORTED_YET};
use crate::provider_options::ProviderOption;

/// A parsed, validated MCP-tool binding.
#[derive(Debug, Clone, PartialEq)]
pub struct McpBinding {
    /// The server's name in the pack's `mcp.yaml`, which is also the name it is
    /// resolved and authorized under.
    pub server: String,
    pub tool: String,
    arguments: JsonTemplate,
    pub result: McpResult,
}

/// What a binding reads from the tool result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpResult {
    /// The result's text blocks, concatenated.
    Text,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingFile {
    server: String,
    tool: String,
    #[serde(default)]
    arguments: Option<serde_yaml::Value>,
    result: String,
}

pub(super) fn parse(
    slot: Slot,
    options: &[ProviderOption],
    servers: &BTreeSet<String>,
    body: serde_yaml::Value,
) -> Result<McpBinding, String> {
    // A tool's text is markdown; search has no contract mapping it to ranked
    // results, so a search binding would install, show ready, and never run.
    if matches!(slot, Slot::WebSearch) {
        return Err(format!(
            "adapter `mcp` for `web-search` is {NOT_SUPPORTED_YET}: it serves the markdown slots"
        ));
    }
    let file: BindingFile =
        serde_yaml::from_value(body).map_err(|e| format!("adapter `mcp`: {e}"))?;
    if !servers.contains(&file.server) {
        return Err(format!(
            "adapter `mcp` names server `{}`, which this pack's `mcp.yaml` does not declare",
            file.server
        ));
    }
    if file.tool.trim().is_empty() {
        return Err("adapter `mcp` needs a `tool`".to_string());
    }
    let arguments = match file.arguments {
        Some(value @ serde_yaml::Value::Mapping(_)) => http::json_template(slot, options, value)?,
        Some(_) => return Err("adapter `mcp`: `arguments` must be a mapping".to_string()),
        None => JsonTemplate::empty_object(),
    };
    let result = match file.result.as_str() {
        "text" => McpResult::Text,
        form @ "structured" => {
            return Err(format!(
                "adapter `mcp`: `result: {form}` is {NOT_SUPPORTED_YET}"
            ))
        }
        other => return Err(format!("adapter `mcp`: unknown `result: {other}` (text)")),
    };
    Ok(McpBinding {
        server: file.server,
        tool: file.tool,
        arguments,
        result,
    })
}

/// The tool arguments for one call. Pure, like [`http::prepare`].
pub fn arguments(provider: &ProviderDecl, binding: &McpBinding, inputs: &Inputs<'_>) -> Value {
    http::resolve_json(&provider.options, &binding.arguments, inputs)
}
