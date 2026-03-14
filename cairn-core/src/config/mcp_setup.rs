//! MCP config file generation for Claude CLI.
//!
//! Generates the JSON config that tells Claude CLI where to find the cairn-mcp binary
//! and how to connect to the callback server.

use std::path::{Path, PathBuf};

/// Build MCP config JSON for Claude CLI.
///
/// This is the pure function for generating the config content.
///
/// If `schema_path` is provided, adds `--schema <path>` args to cairn-mcp.
/// If `tool_name` is provided, adds `--tool-name <name>`.
/// If `tool_description` is provided, adds `--tool-description <desc>`.
/// If `available_agents` is provided, adds `--agents <json>`.
/// If `available_skills` is provided, adds `--skills <json>`.
/// If `available_tools` is provided, adds `--tools <json>`.
#[allow(clippy::too_many_arguments)]
pub fn build_mcp_config_json(
    cairn_mcp_path: &str,
    callback_url: &str,
    schema_path: Option<&str>,
    tool_name: Option<&str>,
    tool_description: Option<&str>,
    available_agents: Option<&str>,
    available_skills: Option<&str>,
    available_tools: Option<&str>,
) -> serde_json::Value {
    let mut args: Vec<&str> = Vec::new();
    if let Some(path) = schema_path {
        args.push("--schema");
        args.push(path);
    }
    if let Some(name) = tool_name {
        args.push("--tool-name");
        args.push(name);
    }
    if let Some(desc) = tool_description {
        args.push("--tool-description");
        args.push(desc);
    }
    if let Some(agents_json) = available_agents {
        args.push("--agents");
        args.push(agents_json);
    }
    if let Some(skills_json) = available_skills {
        args.push("--skills");
        args.push(skills_json);
    }
    if let Some(tools_json) = available_tools {
        args.push("--tools");
        args.push(tools_json);
    }

    let servers = serde_json::json!({
        "cairn": {
            "type": "stdio",
            "command": cairn_mcp_path,
            "args": args,
            "env": {
                "CAIRN_CALLBACK_URL": callback_url
            }
        }
    });

    serde_json::json!({ "mcpServers": servers })
}

/// Ensure the MCP config file exists and return its path.
///
/// `config_dir` is where the config file is written (host-specific).
/// `mcp_binary_path` is the path to the cairn-mcp binary.
/// `callback_port` is the MCP callback server port.
#[allow(clippy::too_many_arguments)]
pub fn ensure_mcp_config(
    config_dir: &Path,
    mcp_binary_path: &str,
    callback_port: u16,
    schema_path: Option<&str>,
    tool_name: Option<&str>,
    tool_description: Option<&str>,
    available_agents: Option<&str>,
    available_skills: Option<&str>,
    available_tools: Option<&str>,
) -> Result<PathBuf, String> {
    let config_path = config_dir.join("mcp-config.json");

    // Ensure directory exists
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create config dir: {}", e))?;
    }

    let callback_url = format!("http://127.0.0.1:{}/api/mcp", callback_port);
    let config = build_mcp_config_json(
        mcp_binary_path,
        &callback_url,
        schema_path,
        tool_name,
        tool_description,
        available_agents,
        available_skills,
        available_tools,
    );

    std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap())
        .map_err(|e| format!("Failed to write MCP config: {}", e))?;

    log::info!("MCP config written to: {}", config_path.display());
    if let Some(path) = schema_path {
        log::info!("MCP config includes schema: {}", path);
    }

    Ok(config_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_mcp_config_json_basic() {
        let config = build_mcp_config_json(
            "/usr/bin/cairn-mcp",
            "http://127.0.0.1:3847/api/mcp",
            None,
            None,
            None,
            None,
            None,
            None,
        );

        let servers = config.get("mcpServers").unwrap();
        let cairn = servers.get("cairn").unwrap();
        assert_eq!(
            cairn.get("command").unwrap().as_str().unwrap(),
            "/usr/bin/cairn-mcp"
        );
        assert_eq!(
            cairn
                .get("env")
                .unwrap()
                .get("CAIRN_CALLBACK_URL")
                .unwrap()
                .as_str()
                .unwrap(),
            "http://127.0.0.1:3847/api/mcp"
        );
    }

    #[test]
    fn test_build_mcp_config_json_with_schema() {
        let config = build_mcp_config_json(
            "/usr/bin/cairn-mcp",
            "http://127.0.0.1:3847/api/mcp",
            Some("/tmp/schema.json"),
            Some("submit"),
            Some("Submit output"),
            None,
            None,
            None,
        );

        let args = config["mcpServers"]["cairn"]["args"].as_array().unwrap();
        assert!(args.contains(&serde_json::json!("--schema")));
        assert!(args.contains(&serde_json::json!("/tmp/schema.json")));
        assert!(args.contains(&serde_json::json!("--tool-name")));
        assert!(args.contains(&serde_json::json!("submit")));
    }
}
