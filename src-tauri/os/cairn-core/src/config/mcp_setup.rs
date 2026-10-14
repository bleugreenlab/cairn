//! The stdio MCP attachment: how a locally spawned agent reaches Cairn through
//! `cairn-cmd mcp`.

use std::collections::BTreeMap;

use cairn_llm::session::mcp::McpServer;

/// The stdio attachment for one run: the agent's CLI launches `cairn-cmd mcp`,
/// which calls back to `/api/mcp` on this machine's loopback.
///
/// Built per run and passed inline (Claude's `--mcp-config <json>`, Codex's
/// `-c`), never shared on disk. A shared config file raced under concurrent
/// session starts: the run-specific `CAIRN_HOME_URI` was overwritten by the last
/// writer, so parallel agents (e.g. sibling delegated tasks) all resolved
/// `cairn:~` to the same node.
///
/// The child's environment carries:
/// - `CAIRN_CALLBACK_URL`: the host HTTP port serving `/api/mcp` (the runner
///   transport port locally, or the server port in headless deployments).
/// - `CAIRN_ENV` ("dev"/"prod"), always, so the child (built `--release`)
///   resolves the same Cairn home as the host that spawned it rather than
///   keying on its own build profile.
/// - `CAIRN_LOG_LEVEL`, so the child's `logging::init` matches the app's
///   file-log verbosity.
/// - `CAIRN_HOME_URI`, when given, the run's home node.
/// - `CAIRN_HOME`, when given, so the child resolves the instance's home
///   directory (and its on-disk MCP secret) on the disk-fallback path rather
///   than the base `~/.cairn[-dev]`.
pub(crate) fn stdio_attachment(
    mcp_binary_path: &str,
    callback_port: u16,
    home_uri: Option<&str>,
    cairn_env: &str,
    cairn_home: Option<&str>,
    log_level: &str,
) -> McpServer {
    let mut env = BTreeMap::from([
        (
            "CAIRN_CALLBACK_URL".to_string(),
            format!("http://127.0.0.1:{callback_port}/api/mcp"),
        ),
        ("CAIRN_ENV".to_string(), cairn_env.to_string()),
        ("CAIRN_LOG_LEVEL".to_string(), log_level.to_string()),
    ]);
    if let Some(uri) = home_uri {
        env.insert("CAIRN_HOME_URI".to_string(), uri.to_string());
    }
    if let Some(home) = cairn_home {
        env.insert("CAIRN_HOME".to_string(), home.to_string());
    }
    McpServer::cairn_stdio(mcp_binary_path, env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::cairn_servers;

    fn claude_config(attachment: &McpServer) -> serde_json::Value {
        serde_json::from_str(&cairn_servers(attachment).claude_mcp_config()).unwrap()
    }

    /// The stdio entry Claude receives is exactly the one it has always
    /// received, so moving to a typed attachment changed nothing on this path.
    #[test]
    fn the_stdio_claude_config_is_unchanged() {
        let attachment = stdio_attachment(
            "/usr/bin/cairn-cmd",
            3849,
            Some("cairn://p/cairn/42/1/builder"),
            "dev",
            Some("/Users/me/.cairn-dev-feature-x"),
            "verbose",
        );
        let golden: serde_json::Value = serde_json::from_str(
            r#"{"mcpServers":{"cairn":{"type":"stdio","command":"/usr/bin/cairn-cmd","args":["mcp"],"env":{"CAIRN_CALLBACK_URL":"http://127.0.0.1:3849/api/mcp","CAIRN_ENV":"dev","CAIRN_LOG_LEVEL":"verbose","CAIRN_HOME_URI":"cairn://p/cairn/42/1/builder","CAIRN_HOME":"/Users/me/.cairn-dev-feature-x"}}}}"#,
        )
        .unwrap();
        assert_eq!(claude_config(&attachment), golden);
    }

    #[test]
    fn optional_keys_are_absent_when_not_given() {
        let config = claude_config(&stdio_attachment(
            "/usr/bin/cairn-cmd",
            3849,
            None,
            "prod",
            None,
            "standard",
        ));
        let env = &config["mcpServers"]["cairn"]["env"];
        assert_eq!(env["CAIRN_ENV"], "prod");
        assert_eq!(env["CAIRN_LOG_LEVEL"], "standard");
        assert!(env.get("CAIRN_HOME_URI").is_none());
        assert!(env.get("CAIRN_HOME").is_none());
    }

    #[test]
    fn each_run_gets_its_own_home_uri() {
        let for_run = |home: &str| {
            stdio_attachment(
                "/usr/bin/cairn-cmd",
                3849,
                Some(home),
                "prod",
                None,
                "standard",
            )
        };
        let a = for_run("cairn://p/cairn/1/1/planner/task/agent-spawn");
        let b = for_run("cairn://p/cairn/1/1/planner/task/planbuild-recipe");
        assert_eq!(
            claude_config(&a)["mcpServers"]["cairn"]["env"]["CAIRN_HOME_URI"],
            "cairn://p/cairn/1/1/planner/task/agent-spawn"
        );
        assert_ne!(a, b);
        assert_ne!(
            cairn_servers(&a).codex_config_overrides(),
            cairn_servers(&b).codex_config_overrides()
        );
    }
}
