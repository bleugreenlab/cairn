//! Import external MCP server definitions from other CLIs' config files.
//!
//! Both Claude Code (`~/.claude.json`, JSON) and Codex (`~/.codex/config.toml`,
//! TOML) declare MCP servers in a shape close to Cairn's [`McpServerConfig`].
//! These parsers map both into [`McpServerConfig`] so a user can import servers
//! they have already configured elsewhere instead of re-entering them.
//!
//! Parsing is deliberately tolerant: a missing or malformed file yields an
//! empty map, and a single malformed server entry is skipped rather than
//! failing the whole import.

use std::collections::HashMap;

use serde::Deserialize;

use super::mcp_servers::McpServerConfig;

/// Raw server entry as written by Claude Code / Codex. Captures the union of
/// stdio and remote fields; unknown fields (e.g. Codex's `env_vars`,
/// `startup_timeout_ms`, or Claude's per-entry metadata) are ignored.
#[derive(Debug, Deserialize)]
struct RawImportedServer {
    #[serde(rename = "type")]
    transport: Option<String>,
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    url: Option<String>,
    #[serde(default)]
    headers: HashMap<String, String>,
}

impl RawImportedServer {
    /// Map an external entry into Cairn's `McpServerConfig`. Honors an explicit
    /// `type`; otherwise infers a remote transport from a `url` with no
    /// `command`, and falls back to `stdio`. Imported servers are enabled.
    fn into_config(self) -> McpServerConfig {
        let transport = self.transport.unwrap_or_else(|| {
            if self.url.is_some() && self.command.is_none() {
                "http".to_string()
            } else {
                "stdio".to_string()
            }
        });
        McpServerConfig {
            transport,
            command: self.command,
            args: self.args,
            env: self.env,
            url: self.url,
            headers: self.headers,
            enabled: true,
            // Imported servers carry no OAuth config; authorize it later in
            // Settings if the remote endpoint requires it.
            oauth: None,
        }
    }
}

/// Parse the `mcpServers` block from a Claude Code `~/.claude.json` document.
///
/// Returns an empty map when the JSON is malformed or has no `mcpServers`
/// object. Individual malformed entries are skipped.
pub fn parse_claude_mcp_servers(content: &str) -> HashMap<String, McpServerConfig> {
    let Ok(root) = serde_json::from_str::<serde_json::Value>(content) else {
        return HashMap::new();
    };
    let Some(servers) = root.get("mcpServers").and_then(|v| v.as_object()) else {
        return HashMap::new();
    };
    servers
        .iter()
        .filter_map(|(name, value)| {
            serde_json::from_value::<RawImportedServer>(value.clone())
                .ok()
                .map(|raw| (name.clone(), raw.into_config()))
        })
        .collect()
}

/// Parse the `[mcp_servers.*]` tables from a Codex `config.toml` document.
///
/// Returns an empty map when the TOML is malformed or has no `mcp_servers`
/// table. Individual malformed entries are skipped.
pub fn parse_codex_mcp_servers(content: &str) -> HashMap<String, McpServerConfig> {
    let Ok(root) = toml::from_str::<toml::Value>(content) else {
        return HashMap::new();
    };
    let Some(servers) = root.get("mcp_servers").and_then(|v| v.as_table()) else {
        return HashMap::new();
    };
    servers
        .iter()
        .filter_map(|(name, value)| {
            RawImportedServer::deserialize(value.clone())
                .ok()
                .map(|raw| (name.clone(), raw.into_config()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_parses_stdio_and_remote_entries() {
        let json = r#"{
          "numStartups": 5,
          "mcpServers": {
            "playwright": {
              "command": "npx",
              "args": ["-y", "@playwright/mcp@latest"],
              "env": { "FOO": "bar" }
            },
            "remote": {
              "type": "http",
              "url": "https://mcp.example.com/mcp",
              "headers": { "Authorization": "Bearer x" }
            }
          }
        }"#;
        let servers = parse_claude_mcp_servers(json);
        assert_eq!(servers.len(), 2);

        let pw = &servers["playwright"];
        assert_eq!(pw.transport, "stdio");
        assert_eq!(pw.command.as_deref(), Some("npx"));
        assert_eq!(pw.args, vec!["-y", "@playwright/mcp@latest"]);
        assert_eq!(pw.env.get("FOO").map(String::as_str), Some("bar"));
        assert!(pw.enabled);

        let remote = &servers["remote"];
        assert_eq!(remote.transport, "http");
        assert_eq!(remote.url.as_deref(), Some("https://mcp.example.com/mcp"));
        assert_eq!(
            remote.headers.get("Authorization").map(String::as_str),
            Some("Bearer x")
        );
    }

    #[test]
    fn claude_infers_http_from_url_without_command() {
        let json = r#"{ "mcpServers": { "r": { "url": "https://x.example/mcp" } } }"#;
        let servers = parse_claude_mcp_servers(json);
        assert_eq!(servers["r"].transport, "http");
    }

    #[test]
    fn claude_skips_malformed_entry_but_keeps_others() {
        // `args` as a string (not an array) makes that one entry invalid; the
        // valid sibling still imports.
        let json = r#"{
          "mcpServers": {
            "good": { "command": "npx" },
            "bad": { "command": "npx", "args": "not-an-array" }
          }
        }"#;
        let servers = parse_claude_mcp_servers(json);
        assert_eq!(servers.len(), 1);
        assert!(servers.contains_key("good"));
    }

    #[test]
    fn claude_missing_or_malformed_yields_empty() {
        assert!(parse_claude_mcp_servers("").is_empty());
        assert!(parse_claude_mcp_servers("not json").is_empty());
        assert!(parse_claude_mcp_servers(r#"{ "other": 1 }"#).is_empty());
        assert!(parse_claude_mcp_servers(r#"{ "mcpServers": {} }"#).is_empty());
    }

    #[test]
    fn codex_parses_stdio_and_remote_entries() {
        let toml = r#"
model = "gpt-5"

[mcp_servers.playwright]
command = "npx"
args = ["-y", "@playwright/mcp@latest"]
env = { FOO = "bar" }
env_vars = ["PATH"]

[mcp_servers.remote]
url = "https://mcp.example.com/mcp"
"#;
        let servers = parse_codex_mcp_servers(toml);
        assert_eq!(servers.len(), 2);

        let pw = &servers["playwright"];
        assert_eq!(pw.transport, "stdio");
        assert_eq!(pw.command.as_deref(), Some("npx"));
        assert_eq!(pw.args, vec!["-y", "@playwright/mcp@latest"]);
        assert_eq!(pw.env.get("FOO").map(String::as_str), Some("bar"));

        // A bare `url` with no command infers a remote transport.
        assert_eq!(servers["remote"].transport, "http");
        assert_eq!(
            servers["remote"].url.as_deref(),
            Some("https://mcp.example.com/mcp")
        );
    }

    #[test]
    fn codex_skips_malformed_entry_but_keeps_others() {
        let toml = r#"
[mcp_servers.good]
command = "npx"

[mcp_servers.bad]
args = "not-an-array"
"#;
        let servers = parse_codex_mcp_servers(toml);
        assert_eq!(servers.len(), 1);
        assert!(servers.contains_key("good"));
    }

    #[test]
    fn codex_missing_or_malformed_yields_empty() {
        assert!(parse_codex_mcp_servers("").is_empty());
        assert!(parse_codex_mcp_servers("= = not toml").is_empty());
        assert!(parse_codex_mcp_servers("model = \"gpt-5\"\n").is_empty());
    }
}
