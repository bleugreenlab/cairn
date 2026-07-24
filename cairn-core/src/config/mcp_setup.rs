//! MCP config file generation for Claude CLI.
//!
//! Generates the JSON config that tells Claude CLI where to find the cairn-cmd binary
//! and how to connect to the callback server.

/// Build MCP config JSON for Claude CLI.
///
/// This is the pure function for generating the config content.
///
/// If `available_agents` is provided, adds `--agents <json>`.
/// If `home_uri` is provided, sets `CAIRN_HOME_URI` in the MCP server env.
/// If `cairn_home` is provided, sets `CAIRN_HOME` in the MCP server env so the
/// child resolves the instance's home directory (and its on-disk MCP secret) on
/// the disk-fallback path rather than the base `~/.cairn[-dev]`.
///
/// `cairn_env` ("dev"/"prod") is always set as `CAIRN_ENV` in the MCP server env
/// so the child (built `--release`) resolves the same Cairn home as the host
/// that spawned it, rather than keying on its own build profile. The callback
/// endpoint itself is passed through `CAIRN_CALLBACK_URL` below.
///
/// `log_level` (a `LogLevel` name: "quiet"/"standard"/"verbose") is set as
/// `CAIRN_LOG_LEVEL` so the child's `logging::init` resolves the same file-log
/// verbosity as the app that spawned it.
fn build_mcp_config_json(
    cairn_cmd_path: &str,
    callback_url: &str,
    available_agents: Option<&str>,
    home_uri: Option<&str>,
    cairn_env: &str,
    cairn_home: Option<&str>,
    log_level: &str,
) -> serde_json::Value {
    let mut args: Vec<&str> = Vec::new();
    if let Some(agents_json) = available_agents {
        args.push("--agents");
        args.push(agents_json);
    }
    let mut env = serde_json::json!({
        "CAIRN_CALLBACK_URL": callback_url,
        "CAIRN_ENV": cairn_env,
        "CAIRN_LOG_LEVEL": log_level
    });
    if let Some(uri) = home_uri {
        env["CAIRN_HOME_URI"] = serde_json::Value::String(uri.to_string());
    }
    if let Some(home) = cairn_home {
        env["CAIRN_HOME"] = serde_json::Value::String(home.to_string());
    }

    let servers = serde_json::json!({
        "cairn": {
            "type": "stdio",
            "command": cairn_cmd_path,
            "args": args,
            "env": env
        }
    });

    serde_json::json!({ "mcpServers": servers })
}

/// Build the MCP config as a self-contained JSON string.
///
/// Claude CLI's `--mcp-config` accepts the config inline as a string, so each
/// run passes its own config directly rather than sharing a file on disk.
/// Sharing a single `mcp-config.json` raced under concurrent session starts:
/// the run-specific `CAIRN_HOME_URI` and the project-scoped `--agents` payload
/// were overwritten by the last writer, so parallel agents (e.g. sibling
/// delegated tasks) all resolved `cairn:~` to the same node. Passing the config
/// inline removes the shared mutable file entirely.
///
/// `mcp_binary_path` is the path to the cairn-cmd binary.
/// `callback_port` is the host HTTP port serving `/api/mcp` (the runner
/// transport port locally, or the server port in headless deployments).
pub(crate) fn build_mcp_config_string(
    mcp_binary_path: &str,
    callback_port: u16,
    available_agents: Option<&str>,
    home_uri: Option<&str>,
    cairn_env: &str,
    cairn_home: Option<&str>,
    log_level: &str,
) -> String {
    let callback_url = format!("http://127.0.0.1:{}/api/mcp", callback_port);
    let config = build_mcp_config_json(
        mcp_binary_path,
        &callback_url,
        available_agents,
        home_uri,
        cairn_env,
        cairn_home,
        log_level,
    );
    serde_json::to_string(&config).expect("MCP config JSON always serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_mcp_config_json_basic() {
        let config = build_mcp_config_json(
            "/usr/bin/cairn-cmd",
            "http://127.0.0.1:3849/api/mcp",
            None,
            None,
            "prod",
            None,
            "standard",
        );

        let servers = config.get("mcpServers").unwrap();
        let cairn = servers.get("cairn").unwrap();
        assert_eq!(
            cairn.get("command").unwrap().as_str().unwrap(),
            "/usr/bin/cairn-cmd"
        );
        let env = cairn.get("env").unwrap();
        assert_eq!(
            env.get("CAIRN_CALLBACK_URL").unwrap().as_str().unwrap(),
            "http://127.0.0.1:3849/api/mcp"
        );
        // CAIRN_ENV is always propagated to the child.
        assert_eq!(env.get("CAIRN_ENV").unwrap().as_str().unwrap(), "prod");
        // CAIRN_LOG_LEVEL is always propagated so the child matches app verbosity.
        assert_eq!(
            env.get("CAIRN_LOG_LEVEL").unwrap().as_str().unwrap(),
            "standard"
        );
        // No home URI / home provided — keys must be absent
        assert!(env.get("CAIRN_HOME_URI").is_none());
        assert!(env.get("CAIRN_HOME").is_none());
    }

    #[test]
    fn test_build_mcp_config_json_with_cairn_home() {
        let config = build_mcp_config_json(
            "/usr/bin/cairn-cmd",
            "http://127.0.0.1:3861/api/mcp",
            None,
            None,
            "dev",
            Some("/Users/me/.cairn-dev-feature-x"),
            "standard",
        );

        let env = &config["mcpServers"]["cairn"]["env"];
        assert_eq!(
            env["CAIRN_HOME"].as_str().unwrap(),
            "/Users/me/.cairn-dev-feature-x"
        );
    }

    #[test]
    fn test_build_mcp_config_json_with_home_uri() {
        let config = build_mcp_config_json(
            "/usr/bin/cairn-cmd",
            "http://127.0.0.1:3849/api/mcp",
            None,
            Some("cairn://p/CAIRN/42/1/builder"),
            "dev",
            None,
            "verbose",
        );

        let env = &config["mcpServers"]["cairn"]["env"];
        assert_eq!(
            env["CAIRN_HOME_URI"].as_str().unwrap(),
            "cairn://p/CAIRN/42/1/builder"
        );
        assert_eq!(env["CAIRN_ENV"].as_str().unwrap(), "dev");
    }

    #[test]
    fn test_build_mcp_config_string_is_parseable_and_run_specific() {
        let a = build_mcp_config_string(
            "/usr/bin/cairn-cmd",
            3849,
            None,
            Some("cairn://p/CAIRN/1/1/planner/task/agent-spawn"),
            "prod",
            None,
            "standard",
        );
        let b = build_mcp_config_string(
            "/usr/bin/cairn-cmd",
            3849,
            None,
            Some("cairn://p/CAIRN/1/1/planner/task/planbuild-recipe"),
            "prod",
            None,
            "standard",
        );

        // Each run gets its own home URI baked in — no shared file to clobber.
        let parsed: serde_json::Value = serde_json::from_str(&a).unwrap();
        assert_eq!(
            parsed["mcpServers"]["cairn"]["env"]["CAIRN_HOME_URI"]
                .as_str()
                .unwrap(),
            "cairn://p/CAIRN/1/1/planner/task/agent-spawn"
        );
        assert_ne!(a, b);
    }

    #[test]
    fn test_build_mcp_config_json_with_agents() {
        let config = build_mcp_config_json(
            "/usr/bin/cairn-cmd",
            "http://127.0.0.1:3849/api/mcp",
            Some("[{\"name\":\"Explore\",\"description\":\"x\"}]"),
            None,
            "prod",
            None,
            "standard",
        );

        let args = config["mcpServers"]["cairn"]["args"].as_array().unwrap();
        assert!(args.contains(&serde_json::json!("--agents")));
    }
}
