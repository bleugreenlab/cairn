//! The MCP servers a spawned agent's CLI is given, rendered into each CLI's own
//! configuration syntax.
//!
//! A session is given exactly the servers in its [`McpServers`] and nothing
//! else: Claude through `--strict-mcp-config`, Codex through a clean
//! `CODEX_HOME` the host prepares. Each [`McpServer`] is named, and reached
//! over one of two transports:
//!
//! - [`McpTransport::Stdio`]: the CLI launches a command and speaks MCP over
//!   its stdin and stdout.
//! - [`McpTransport::Http`]: the CLI speaks streamable HTTP to a URL,
//!   optionally presenting a bearer it reads from the environment.
//!
//! Cairn's own server, `cairn`, is built by [`McpServer::cairn_stdio`] (the
//! CLI launches `cairn-cmd mcp`, which forwards each call to the runner's
//! loopback `/api/mcp` callback with the machine secret) or
//! [`McpServer::cairn_http`] (the CLI reaches the orchestrator's `/mcp`
//! endpoint with the run's bearer from [`MCP_TOKEN_ENV`]). A host outside
//! Cairn builds its own servers and need not include `cairn` at all.
//!
//! # Secrets never render inline
//!
//! A rendered config travels in process arguments (`--mcp-config <json>`,
//! `-c <toml>`), which other local processes can read. So a stdio server's
//! `env` holds non-secret values only. A secret is supplied to the CLI's own
//! environment through the session's `SecretEnv`, and the server names it
//! instead: in `inherit_env` for a stdio server, or as `bearer_env` for an
//! HTTP server. Claude passes its environment to stdio servers and expands
//! `${NAME}` in an HTTP header itself; Codex is told the names through
//! `env_vars` and `bearer_token_env_var` (verified against Claude Code
//! 2.1.281 and Codex 0.153.3).

use std::collections::{BTreeMap, BTreeSet};

/// The environment variable Cairn's HTTP attachment reads its bearer from.
pub const MCP_TOKEN_ENV: &str = "CAIRN_MCP_TOKEN";

/// The name Cairn's own server is attached under.
pub const CAIRN_SERVER_NAME: &str = "cairn";

/// The tools Cairn's server must advertise, sorted.
pub const CAIRN_TOOLS: [&str; 3] = ["read", "run", "write"];

/// What `cairn-cmd` inherits from the agent process rather than receiving in
/// its server entry: the run identity, and the machine secret, which must never
/// appear in process arguments or on disk.
const CAIRN_STDIO_INHERITED_ENV: [&str; 2] = ["CAIRN_RUN_ID", "CAIRN_MCP_SECRET"];

/// Codex's per-server MCP tool-call timeout. The host owns execution: a
/// Cairn `run` answers within its grace window, but a blocking `write` append
/// to a node's tasks or questions awaits a sub-agent or a user answer with no
/// host-side deadline. Codex's default would abandon that call while the host
/// keeps working. Seven days is an effective "never" that still bounds a wedged
/// socket, and sits above `cairn-cmd`'s own callback ceiling. (Claude's
/// equivalent is the `MCP_TOOL_TIMEOUT` process environment variable.)
const CODEX_TOOL_TIMEOUT_SECS: u64 = 604_800;

/// The set of MCP servers a session or a Codex app-server is given.
///
/// Names are validated at construction: non-empty, unique, and made only of
/// ASCII letters, digits, `_`, and `-`, because a name becomes a TOML key in
/// Codex's `-c mcp_servers.<name>=...` and the `mcp__<name>__<tool>` prefix of
/// a Claude tool name. The set may be empty: a tool-using session with no MCP
/// server at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServers(Vec<McpServer>);

/// One named MCP server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServer {
    pub name: String,
    pub transport: McpTransport,
    /// The exact tool set a Codex thread must report for this server before
    /// the session is considered attached. Empty checks only that the server
    /// is present.
    pub required_tools: Vec<String>,
}

/// How the CLI reaches one server. See the module docs for the secret rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpTransport {
    /// Streamable HTTP. `bearer_env` names the environment variable the CLI
    /// reads its bearer from; `None` sends no `Authorization` header.
    Http {
        url: String,
        bearer_env: Option<String>,
    },
    /// A command the CLI launches. `env` is non-secret configuration that is
    /// rendered into the config; `inherit_env` names variables the server
    /// takes from the CLI's own environment, which is where secrets go.
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        inherit_env: Vec<String>,
    },
}

impl McpServer {
    /// Cairn's server over stdio: `cairn-cmd mcp`. `env` is the child's
    /// non-secret configuration (callback URL, home URI, log level, and so on).
    pub fn cairn_stdio(binary: impl Into<String>, env: BTreeMap<String, String>) -> Self {
        Self {
            name: CAIRN_SERVER_NAME.to_string(),
            transport: McpTransport::Stdio {
                command: binary.into(),
                args: vec!["mcp".to_string()],
                env,
                inherit_env: CAIRN_STDIO_INHERITED_ENV
                    .iter()
                    .map(|name| name.to_string())
                    .collect(),
            },
            required_tools: cairn_tools(),
        }
    }

    /// Cairn's server over streamable HTTP, authenticated by the run bearer in
    /// [`MCP_TOKEN_ENV`].
    pub fn cairn_http(url: impl Into<String>) -> Self {
        Self {
            name: CAIRN_SERVER_NAME.to_string(),
            transport: McpTransport::Http {
                url: url.into(),
                bearer_env: Some(MCP_TOKEN_ENV.to_string()),
            },
            required_tools: cairn_tools(),
        }
    }

    /// This server's `mcpServers` entry for Claude Code. An HTTP bearer is
    /// named as `${NAME}`, which Claude expands from its own environment.
    fn claude_entry(&self) -> serde_json::Value {
        match &self.transport {
            McpTransport::Stdio {
                command, args, env, ..
            } => serde_json::json!({
                "type": "stdio",
                "command": command,
                "args": args,
                "env": env,
            }),
            McpTransport::Http { url, bearer_env } => {
                let mut entry = serde_json::json!({ "type": "http", "url": url });
                if let Some(name) = bearer_env {
                    entry["headers"] =
                        serde_json::json!({ "Authorization": format!("Bearer ${{{name}}}") });
                }
                entry
            }
        }
    }

    /// The `-c` value that defines this server for one Codex process.
    fn codex_override(&self) -> String {
        let entry = match &self.transport {
            McpTransport::Stdio {
                command,
                args,
                env,
                inherit_env,
            } => {
                let env = env
                    .iter()
                    .map(|(key, value)| format!("{key}={}", toml_string(value)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "command={}, args=[{}], env={{{env}}}, env_vars=[{}]",
                    toml_string(command),
                    toml_list(args),
                    toml_list(inherit_env)
                )
            }
            McpTransport::Http { url, bearer_env } => match bearer_env {
                Some(name) => format!(
                    "url={}, bearer_token_env_var={}",
                    toml_string(url),
                    toml_string(name)
                ),
                None => format!("url={}", toml_string(url)),
            },
        };
        format!(
            "mcp_servers.{}={{{entry}, tool_timeout_sec={CODEX_TOOL_TIMEOUT_SECS}}}",
            self.name
        )
    }
}

fn cairn_tools() -> Vec<String> {
    CAIRN_TOOLS.iter().map(|tool| tool.to_string()).collect()
}

impl McpServers {
    /// Validate and hold `servers`, in order.
    pub fn new(servers: Vec<McpServer>) -> Result<Self, String> {
        let mut seen = BTreeSet::new();
        for server in &servers {
            let valid = !server.name.is_empty()
                && server
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
            if !valid {
                return Err(format!(
                    "MCP server name {:?} must be non-empty ASCII letters, digits, '_' or '-'",
                    server.name
                ));
            }
            if !seen.insert(server.name.as_str()) {
                return Err(format!("MCP server name {:?} is used twice", server.name));
            }
        }
        Ok(Self(servers))
    }

    pub fn iter(&self) -> std::slice::Iter<'_, McpServer> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn names(&self) -> Vec<String> {
        self.0.iter().map(|server| server.name.clone()).collect()
    }

    /// The inline `--mcp-config` JSON for Claude Code: one `mcpServers` key
    /// per server.
    pub fn claude_mcp_config(&self) -> String {
        let servers: serde_json::Map<String, serde_json::Value> = self
            .0
            .iter()
            .map(|server| (server.name.clone(), server.claude_entry()))
            .collect();
        serde_json::to_string(&serde_json::json!({ "mcpServers": servers }))
            .expect("an MCP server entry always serializes")
    }

    /// One `-c` value per server, defining it for one Codex process.
    ///
    /// Passed as `codex app-server -c <value>` rather than written into
    /// `CODEX_HOME/config.toml`, which every Codex process shares: each process
    /// can then carry its own servers without racing its siblings for the file.
    pub fn codex_config_overrides(&self) -> Vec<String> {
        self.0.iter().map(McpServer::codex_override).collect()
    }
}

impl<'a> IntoIterator for &'a McpServers {
    type Item = &'a McpServer;
    type IntoIter = std::slice::Iter<'a, McpServer>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// The per-thread config key that switches one server a Codex app-server was
/// started with off for one thread. A thread's `config` map is applied as
/// dotted overrides after the process's own `-c` flags, so this wins over
/// [`McpServers::codex_config_overrides`] on that thread only.
pub fn codex_thread_disable_key(server: &str) -> String {
    format!("mcp_servers.{server}.enabled")
}

/// The `--mcp-config` a tool-free Claude session is given: no servers. With
/// `--strict-mcp-config` this also keeps the user's own servers away.
pub const CLAUDE_NO_MCP_CONFIG: &str = r#"{"mcpServers":{}}"#;

fn toml_string(value: &str) -> String {
    toml::Value::String(value.to_string()).to_string()
}

fn toml_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| toml_string(value))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stdio() -> McpServers {
        McpServers::new(vec![McpServer::cairn_stdio(
            "/Applications/Cairn \"dev\".app/cairn-cmd",
            BTreeMap::from([
                (
                    "CAIRN_CALLBACK_URL".to_string(),
                    "http://127.0.0.1:3849/api/mcp".to_string(),
                ),
                (
                    "CAIRN_HOME_URI".to_string(),
                    "cairn://p/CAIRN/1/1/builder".to_string(),
                ),
            ]),
        )])
        .unwrap()
    }

    fn http() -> McpServers {
        McpServers::new(vec![McpServer::cairn_http(
            "https://orch.tail1234.ts.net/mcp",
        )])
        .unwrap()
    }

    /// Parse a `-c key=value` override the way Codex does: the value is TOML.
    fn codex_entry(servers: &McpServers, name: &str) -> toml::Table {
        let prefix = format!("mcp_servers.{name}=");
        let rendered = servers
            .codex_config_overrides()
            .into_iter()
            .find(|value| value.starts_with(&prefix))
            .unwrap_or_else(|| panic!("no override defines mcp_servers.{name}"));
        let value = rendered.strip_prefix(&prefix).unwrap();
        let parsed: toml::Table = toml::from_str(&format!("server = {value}")).unwrap();
        parsed["server"].as_table().unwrap().clone()
    }

    #[test]
    fn claude_stdio_entry_launches_cairn_cmd_mcp() {
        let config: serde_json::Value = serde_json::from_str(&stdio().claude_mcp_config()).unwrap();
        let cairn = &config["mcpServers"]["cairn"];
        assert_eq!(cairn["type"], "stdio");
        assert_eq!(cairn["args"], serde_json::json!(["mcp"]));
        assert_eq!(
            cairn["env"]["CAIRN_CALLBACK_URL"],
            "http://127.0.0.1:3849/api/mcp"
        );
        assert_eq!(config["mcpServers"].as_object().unwrap().len(), 1);
    }

    #[test]
    fn claude_http_entry_names_the_bearer_by_variable_only() {
        let rendered = http().claude_mcp_config();
        let config: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(
            config,
            serde_json::json!({"mcpServers": {"cairn": {
                "type": "http",
                "url": "https://orch.tail1234.ts.net/mcp",
                "headers": {"Authorization": "Bearer ${CAIRN_MCP_TOKEN}"}
            }}})
        );
    }

    #[test]
    fn codex_stdio_override_inherits_secrets_and_keeps_the_long_timeout() {
        let entry = codex_entry(&stdio(), "cairn");
        assert_eq!(
            entry["command"].as_str(),
            Some("/Applications/Cairn \"dev\".app/cairn-cmd")
        );
        assert_eq!(entry["args"].as_array().unwrap()[0].as_str(), Some("mcp"));
        assert_eq!(
            entry["env"]["CAIRN_HOME_URI"].as_str(),
            Some("cairn://p/CAIRN/1/1/builder")
        );
        let inherited: Vec<&str> = entry["env_vars"]
            .as_array()
            .unwrap()
            .iter()
            .map(|name| name.as_str().unwrap())
            .collect();
        assert_eq!(inherited, ["CAIRN_RUN_ID", "CAIRN_MCP_SECRET"]);
        assert!(entry["env"].get("CAIRN_MCP_SECRET").is_none());
        assert_eq!(entry["tool_timeout_sec"].as_integer(), Some(604_800));
    }

    #[test]
    fn codex_http_override_reads_the_bearer_from_the_environment() {
        let entry = codex_entry(&http(), "cairn");
        assert_eq!(
            entry["url"].as_str(),
            Some("https://orch.tail1234.ts.net/mcp")
        );
        assert_eq!(entry["bearer_token_env_var"].as_str(), Some(MCP_TOKEN_ENV));
        assert!(entry.get("command").is_none());
        assert_eq!(entry["tool_timeout_sec"].as_integer(), Some(604_800));
    }

    /// The exact strings Cairn's processes have always been launched with.
    #[test]
    fn cairn_servers_render_byte_for_byte_as_before() {
        let stdio = McpServers::new(vec![McpServer::cairn_stdio(
            "/bin/cairn-cmd",
            BTreeMap::from([("CAIRN_ENV".to_string(), "dev".to_string())]),
        )])
        .unwrap();
        assert_eq!(
            stdio.claude_mcp_config(),
            r#"{"mcpServers":{"cairn":{"args":["mcp"],"command":"/bin/cairn-cmd","env":{"CAIRN_ENV":"dev"},"type":"stdio"}}}"#
        );
        assert_eq!(
            stdio.codex_config_overrides(),
            [
                r#"mcp_servers.cairn={command="/bin/cairn-cmd", args=["mcp"], env={CAIRN_ENV="dev"}, env_vars=["CAIRN_RUN_ID", "CAIRN_MCP_SECRET"], tool_timeout_sec=604800}"#
            ]
        );
        assert_eq!(
            McpServers::new(vec![McpServer::cairn_http("https://o/mcp")])
                .unwrap()
                .codex_config_overrides(),
            [
                r#"mcp_servers.cairn={url="https://o/mcp", bearer_token_env_var="CAIRN_MCP_TOKEN", tool_timeout_sec=604800}"#
            ]
        );
    }

    #[test]
    fn every_server_renders_under_its_own_name() {
        let servers = McpServers::new(vec![
            McpServer {
                name: "tm".to_string(),
                transport: McpTransport::Stdio {
                    command: "/bin/tm-mcp".to_string(),
                    args: vec!["--seat".to_string(), "2".to_string()],
                    env: BTreeMap::new(),
                    inherit_env: vec!["TM_TOKEN".to_string()],
                },
                required_tools: Vec::new(),
            },
            McpServer {
                name: "deck".to_string(),
                transport: McpTransport::Http {
                    url: "http://127.0.0.1:9/mcp".to_string(),
                    bearer_env: None,
                },
                required_tools: Vec::new(),
            },
        ])
        .unwrap();
        let claude: serde_json::Value = serde_json::from_str(&servers.claude_mcp_config()).unwrap();
        assert_eq!(
            claude["mcpServers"]["tm"]["args"],
            serde_json::json!(["--seat", "2"])
        );
        assert_eq!(
            claude["mcpServers"]["deck"],
            serde_json::json!({"type": "http", "url": "http://127.0.0.1:9/mcp"})
        );
        assert_eq!(servers.codex_config_overrides().len(), 2);
        let tm = codex_entry(&servers, "tm");
        assert_eq!(
            tm["env_vars"].as_array().unwrap()[0].as_str(),
            Some("TM_TOKEN")
        );
        let deck = codex_entry(&servers, "deck");
        assert!(deck.get("bearer_token_env_var").is_none());
    }

    #[test]
    fn names_are_validated() {
        let named = |name: &str| McpServer {
            name: name.to_string(),
            ..McpServer::cairn_http("http://x")
        };
        assert!(McpServers::new(vec![named("ok_name-2")]).is_ok());
        assert!(McpServers::new(Vec::new()).unwrap().is_empty());
        for bad in ["", "has space", "dotted.name", "quote\""] {
            assert!(McpServers::new(vec![named(bad)]).is_err(), "{bad:?}");
        }
        assert!(McpServers::new(vec![named("a"), named("a")]).is_err());
    }
}
