//! The `system`/`init` frame: what a Claude CLI declares about itself once it
//! has started a conversation (its version, the session and model it runs,
//! the tools it offers the model, and the MCP servers it attached), and the
//! predicates Cairn branches on over it.

use serde_json::Value;

use super::event::ClaudeEvent;

/// The three core verbs: the entire working Cairn surface. A launch whose
/// `init` lacks any of them cannot do its work, so it is refused at startup.
/// They are the tool names Cairn's MCP server registers (`#[tool] async fn
/// read/write/run` in cairn-cmd, as `mcp__cairn__<verb>`).
pub const CORE_VERBS: [&str; 3] = ["mcp__cairn__read", "mcp__cairn__write", "mcp__cairn__run"];

/// The CLI-internal tool through which a `--json-schema` answer arrives. It is
/// declared only under `--json-schema`, and `--tools ""` rightly leaves it.
pub const STRUCTURED_OUTPUT: &str = "StructuredOutput";

/// One MCP server an `init` reports, and the state the CLI gave it
/// (`connected`, `failed`, `pending`, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerStatus {
    pub name: String,
    pub status: Option<String>,
}

/// A `system`/`init` frame, read leniently: a field the CLI left out or
/// shaped unexpectedly reads as absent, and a tool entry that is not a string
/// is not a tool.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Init {
    pub session_id: Option<String>,
    pub model: Option<String>,
    pub claude_code_version: Option<String>,
    pub tools: Vec<String>,
    pub mcp_servers: Vec<McpServerStatus>,
}

impl Init {
    /// The `init` this event is, if it is one.
    pub fn of(event: &ClaudeEvent) -> Option<Self> {
        match event {
            ClaudeEvent::System {
                subtype,
                session_id,
                data,
            } if subtype == "init" => {
                let mut init = Self::from_data(data);
                if !session_id.is_empty() {
                    init.session_id = Some(session_id.clone());
                }
                Some(init)
            }
            _ => None,
        }
    }

    /// An `init` from its fields: the whole raw frame, or the `data` a parsed
    /// [`ClaudeEvent::System`] keeps beside its subtype and session id.
    pub fn from_data(data: &Value) -> Self {
        let text = |name: &str| {
            data.get(name)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        };
        Self {
            session_id: text("session_id"),
            model: text("model"),
            claude_code_version: text("claude_code_version"),
            tools: data
                .get("tools")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
            mcp_servers: data
                .get("mcp_servers")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|server| {
                    Some(McpServerStatus {
                        name: server.get("name")?.as_str()?.to_string(),
                        status: server
                            .get("status")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                    })
                })
                .collect(),
        }
    }

    /// The status of every server reported under `name` (normally one).
    pub fn mcp_server_statuses(&self, name: &str) -> Vec<Option<&str>> {
        self.mcp_servers
            .iter()
            .filter(|server| server.name == name)
            .map(|server| server.status.as_deref())
            .collect()
    }
}

/// The [`CORE_VERBS`] an `init` does not declare, in their canonical order.
/// Empty means the launch carries Cairn's whole working surface.
pub fn missing_core_verbs(init: &Init) -> Vec<&'static str> {
    CORE_VERBS
        .into_iter()
        .filter(|verb| !init.tools.iter().any(|tool| tool == verb))
        .collect()
}

/// Every tool an `init` declares that sits outside Cairn's surface: a bare
/// (non-`mcp__`) name, which can only be a CLI built-in. Every tool Cairn
/// offers is an MCP tool, and every launch passes `--tools ""`, so this is
/// empty unless the CLI broke that contract. [`STRUCTURED_OUTPUT`] is the
/// CLI's own answer channel under `--json-schema`, not a built-in.
pub fn builtin_tools(init: &Init) -> Vec<&str> {
    init.tools
        .iter()
        .map(String::as_str)
        .filter(|tool| !tool.starts_with("mcp__") && *tool != STRUCTURED_OUTPUT)
        .collect()
}
