//! Read-side handler for the external MCP gateway family (`cairn://mcp/...`).
//!
//! Discovery and resource reads route here:
//! - `cairn://mcp` — list configured servers with a one-line tool summary.
//! - `cairn://mcp/<server>` — that server's tools (as compact `run`-style
//!   invocation contracts) plus its resources.
//! - `cairn://mcp/<server>/<resource-uri>` — proxied `resources/read`.
//!
//! Tool *invocation* never happens here — every `tools/call` goes through `run`
//! (a tool may mutate). This handler is read-only by contract.

use crate::config::mcp_servers::{load_project_mcp_servers, resolve_mcp_servers, McpServerConfig};
use crate::mcp::gateway::McpToolDef;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use std::collections::HashMap;

/// Handle `read cairn://mcp...`.
pub async fn handle_mcp_read(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    server: Option<String>,
    resource: Option<String>,
) -> String {
    let Some(gateway) = orch.mcp_gateway() else {
        return "MCP gateway is not available in this host".to_string();
    };

    // Resolve the run context for the project overlay and the per-session
    // connection key. Reads tolerate a missing context (workspace-only servers).
    let run_context = super::run_context::lookup_run(&orch.db.local, request)
        .await
        .ok();
    let session_key = run_context
        .as_ref()
        .map(|c| c.job_id.clone())
        .unwrap_or_else(|| request.cwd.clone());
    let project_path = match run_context.as_ref() {
        Some(ctx) => crate::config::get_project_path(&orch.db.local, &ctx.project_id)
            .await
            .ok(),
        None => None,
    };

    let project_servers = project_path
        .as_deref()
        .map(load_project_mcp_servers)
        .unwrap_or_default();
    let servers = resolve_mcp_servers(&orch.config_dir, project_path.as_deref());

    match (server, resource) {
        // List all configured servers (summary: names + one-line tool list).
        (None, _) => {
            list_servers(
                gateway.as_ref(),
                &session_key,
                &servers,
                project_path.as_deref(),
                &project_servers,
            )
            .await
        }
        // One server: full tool inputSchemas + resources.
        (Some(server), None) => {
            let Some(config) = servers.get(&server) else {
                return unknown_server(&server, &servers);
            };
            let credential_key =
                mcp_credential_key(&server, project_path.as_deref(), &project_servers);
            describe_server(
                gateway.as_ref(),
                &session_key,
                &server,
                &credential_key,
                &config.expanded(&credential_key),
            )
            .await
        }
        // Proxied resources/read.
        (Some(server), Some(uri)) => {
            let Some(config) = servers.get(&server) else {
                return unknown_server(&server, &servers);
            };
            let credential_key =
                mcp_credential_key(&server, project_path.as_deref(), &project_servers);
            match gateway
                .read_resource(
                    &session_key,
                    &credential_key,
                    &config.expanded(&credential_key),
                    &uri,
                )
                .await
            {
                Ok(body) => body,
                Err(e) => format!("Failed to read {uri} from MCP server '{server}': {e}"),
            }
        }
    }
}

fn mcp_credential_key(
    server: &str,
    project_path: Option<&std::path::Path>,
    project_servers: &HashMap<String, McpServerConfig>,
) -> String {
    let project_scope = project_path.filter(|_| project_servers.contains_key(server));
    crate::config::secrets::credential_key(server, project_scope)
}

async fn list_servers(
    gateway: &dyn crate::mcp::gateway::McpGateway,
    session_key: &str,
    servers: &HashMap<String, McpServerConfig>,
    project_path: Option<&std::path::Path>,
    project_servers: &HashMap<String, McpServerConfig>,
) -> String {
    if servers.is_empty() {
        let mut out = String::from(
            "No external MCP servers configured. Add one with \
             write {target:\"cairn://mcp\", mode:\"create\", payload:{name, command, args}} \
             (or via Settings).\n",
        );
        out.push_str(&mutations_affordance());
        return out;
    }

    let mut names: Vec<&String> = servers.keys().collect();
    names.sort();

    let mut out = String::from("# Configured MCP servers\n");
    for name in names {
        let credential_key = mcp_credential_key(name, project_path, project_servers);
        let config = servers[name].expanded(&credential_key);
        out.push_str(&format!("\n## {name} ({})\n", config.transport));
        match gateway
            .list_tools(session_key, &credential_key, &config)
            .await
        {
            Ok(tools) => {
                if tools.is_empty() {
                    out.push_str("  (no tools advertised)\n");
                } else {
                    for tool in &tools {
                        out.push_str(&format!("  - {}{}\n", tool.name, one_line_desc(tool)));
                    }
                }
            }
            Err(e) => out.push_str(&format!("  (failed to list tools: {e})\n")),
        }
    }
    out.push_str(
        "\nRead cairn://mcp/<server> for each tool's argument contract. Invoke a tool with \
         run {target:\"cairn://mcp/<server>/<tool>\", payload:{args_json:{...}}}.\n",
    );
    out.push_str(&mutations_affordance());
    out
}

/// Affordance block advertising the `write cairn://mcp` registry CRUD. Mirrors
/// the contract-table mutations so discovery teaches the write surface, not just
/// reads. Workspace writes edit ~/.cairn/settings.yaml and are gated by the
/// worktree fence; project writes edit the run's .cairn/config.yaml in place.
fn mutations_affordance() -> String {
    String::from(
        "\n## Manage the server registry (write)\n\
         - add: write {target:\"cairn://mcp\", mode:\"create\", payload:{name, command?, args?, env?, type?, url?, headers?, enabled?, oauth?, scope?}}\n\
         - edit: write {target:\"cairn://mcp/<server>\", mode:\"patch\", payload:{...same fields...}}\n\
         - remove: write {target:\"cairn://mcp/<server>\", mode:\"delete\"}\n\
         scope defaults to workspace (~/.cairn/settings.yaml, gated by the worktree fence like any \
         out-of-worktree write); scope:\"project\" edits the run's .cairn/config.yaml in place. \
         Set non-secret config only — reference secrets with ${VAR}, not plaintext.\n",
    )
}

async fn describe_server(
    gateway: &dyn crate::mcp::gateway::McpGateway,
    session_key: &str,
    server: &str,
    credential_key: &str,
    config: &McpServerConfig,
) -> String {
    let mut out = format!("# MCP server: {server} ({})\n", config.transport);

    out.push_str("\n## Tools\n");
    match gateway
        .list_tools(session_key, credential_key, config)
        .await
    {
        Ok(tools) => {
            if tools.is_empty() {
                out.push_str("(no tools advertised)\n");
            } else {
                for tool in &tools {
                    out.push_str(&render_tool_contract(server, tool));
                }
            }
        }
        Err(e) => out.push_str(&format!("(failed to list tools: {e})\n")),
    }

    out.push_str("\n## Resources\n");
    match gateway
        .list_resources(session_key, credential_key, config)
        .await
    {
        Ok(resources) if resources.is_empty() => out.push_str("(no resources advertised)\n"),
        Ok(resources) => {
            for r in resources {
                let name = r.name.as_deref().unwrap_or("");
                let desc = r.description.as_deref().unwrap_or("");
                out.push_str(&format!(
                    "  - {} {}{}\n    read: cairn://mcp/{server}/{}\n",
                    r.uri,
                    name,
                    if desc.is_empty() {
                        String::new()
                    } else {
                        format!("— {desc}")
                    },
                    r.uri,
                ));
            }
        }
        Err(e) => out.push_str(&format!("(failed to list resources: {e})\n")),
    }

    out
}

/// Render a tool as a compact, scannable `run`-style invocation contract:
///
/// ```text
/// ### look — Observe Axon's current surface...
/// run {target:"cairn://mcp/axon/look", payload:{args_json:{
///   app?: string    — Bundle id, pid, app name...
///   depth?: number  — Maximum tree depth...
/// }}}
/// ```
///
/// Each property renders as `name(?): type — description` (a trailing `?` marks
/// an optional argument). The schema is flattened one level; a property whose
/// type cannot be expressed simply (oneOf/anyOf, nested object) falls back to
/// compact inline JSON while keeping its description.
fn render_tool_contract(server: &str, tool: &McpToolDef) -> String {
    let mut out = format!("\n### {}{}\n", tool.name, one_line_desc(tool));
    let target = format!("cairn://mcp/{server}/{}", tool.name);

    let props = tool
        .input_schema
        .get("properties")
        .and_then(|v| v.as_object());
    let required: std::collections::HashSet<&str> = tool
        .input_schema
        .get("required")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
        .unwrap_or_default();

    match props {
        Some(props) if !props.is_empty() => {
            out.push_str(&format!(
                "run {{target:\"{target}\", payload:{{args_json:{{\n"
            ));
            // Build each property's `name(?): type` head, then pad to align the
            // description column.
            let rows: Vec<(String, String)> = props
                .iter()
                .map(|(name, spec)| {
                    let opt = if required.contains(name.as_str()) {
                        ""
                    } else {
                        "?"
                    };
                    let head = format!("{name}{opt}: {}", property_type(spec));
                    (head, property_desc(spec))
                })
                .collect();
            let width = rows.iter().map(|(h, _)| h.len()).max().unwrap_or(0);
            for (head, desc) in rows {
                if desc.is_empty() {
                    out.push_str(&format!("  {head}\n"));
                } else {
                    out.push_str(&format!("  {head:<width$}  — {desc}\n"));
                }
            }
            out.push_str("}}}\n");
        }
        _ => {
            // Argless tool.
            out.push_str(&format!(
                "run {{target:\"{target}\", payload:{{args_json:{{}}}}}}\n"
            ));
        }
    }
    out
}

/// The displayed type for a schema property. A simple `type` (string or array of
/// strings joined by `|`) is used directly; anything else (oneOf/anyOf, nested
/// object, no declared type) falls back to compact inline JSON of the property
/// with its description stripped.
fn property_type(spec: &serde_json::Value) -> String {
    match spec.get("type") {
        Some(serde_json::Value::String(t)) => return t.clone(),
        Some(serde_json::Value::Array(arr)) => {
            let parts: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            if !parts.is_empty() {
                return parts.join("|");
            }
        }
        _ => {}
    }
    // Fallback: compact JSON of the property, minus its (separately rendered)
    // description.
    let mut compact = spec.clone();
    if let Some(obj) = compact.as_object_mut() {
        obj.remove("description");
    }
    serde_json::to_string(&compact).unwrap_or_else(|_| spec.to_string())
}

/// A property's description collapsed to a single scannable line (full text
/// retained, internal whitespace runs flattened to single spaces).
fn property_desc(spec: &serde_json::Value) -> String {
    spec.get("description")
        .and_then(|v| v.as_str())
        .map(|d| d.split_whitespace().collect::<Vec<_>>().join(" "))
        .unwrap_or_default()
}

/// Render a tool as a single-line `name(arg?: type, ...)` signature for the
/// agent orientation affordance block — awareness plus a cheap contract, with the
/// full argument schema a `read cairn://mcp/<server>` away. A trailing `?` on an
/// argument marks it optional; the type uses the same flattening as the full
/// contract (`property_type`), falling back to compact inline JSON for shapes
/// that have no simple type.
pub fn terse_tool_signature(tool: &McpToolDef) -> String {
    let props = tool
        .input_schema
        .get("properties")
        .and_then(|v| v.as_object());
    let required: std::collections::HashSet<&str> = tool
        .input_schema
        .get("required")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
        .unwrap_or_default();
    let args = match props {
        Some(props) if !props.is_empty() => props
            .iter()
            .map(|(name, spec)| {
                let opt = if required.contains(name.as_str()) {
                    ""
                } else {
                    "?"
                };
                format!("{name}{opt}: {}", property_type(spec))
            })
            .collect::<Vec<_>>()
            .join(", "),
        _ => String::new(),
    };
    format!("{}({})", tool.name, args)
}

/// Render the compact MCP affordance block for the agent orientation prompt:
/// each configured (enabled) server with its tools as terse one-line contracts.
/// Tool lists come from the persisted tool store (`config::mcp_tools`), captured
/// when a server was saved in Settings; a server absent from the store renders a
/// `read cairn://mcp/<server>` pointer instead, so awareness never blocks on
/// spawning a server inline. Returns `None` when no servers are configured.
pub fn render_mcp_affordance_block(
    servers: &HashMap<String, McpServerConfig>,
    tools_by_server: &HashMap<String, Vec<McpToolDef>>,
) -> Option<String> {
    if servers.is_empty() {
        return None;
    }
    let mut names: Vec<&String> = servers.keys().collect();
    names.sort();

    let mut out = String::from(
        "## MCP Servers\n\n\
         External MCP servers reachable through the `cairn://mcp` gateway. Invoke a tool with \
         run {target:\"cairn://mcp/<server>/<tool>\", payload:{args_json:{...}}}; read \
         `cairn://mcp/<server>` for full argument schemas.\n\n",
    );
    for name in names {
        let transport = &servers[name].transport;
        match tools_by_server.get(name) {
            Some(tools) if !tools.is_empty() => {
                out.push_str(&format!("- **{name}** ({transport})\n"));
                for tool in tools {
                    out.push_str(&format!(
                        "  - `{}`{}\n",
                        terse_tool_signature(tool),
                        one_line_desc(tool)
                    ));
                }
            }
            Some(_) => {
                out.push_str(&format!(
                    "- **{name}** ({transport}) — no tools advertised\n"
                ));
            }
            None => {
                out.push_str(&format!(
                    "- **{name}** ({transport}) — read `cairn://mcp/{name}` for its tools\n"
                ));
            }
        }
    }
    Some(out)
}

fn one_line_desc(tool: &McpToolDef) -> String {
    match tool.description.as_deref() {
        Some(d) if !d.is_empty() => {
            let first = d.lines().next().unwrap_or("").trim();
            format!(" — {first}")
        }
        _ => String::new(),
    }
}

fn unknown_server(server: &str, servers: &HashMap<String, McpServerConfig>) -> String {
    let mut names: Vec<&str> = servers.keys().map(|s| s.as_str()).collect();
    names.sort_unstable();
    let configured = if names.is_empty() {
        "(none configured)".to_string()
    } else {
        names.join(", ")
    };
    format!(
        "Unknown MCP server '{server}'. Configured servers: {configured}. \
         Add it under `mcpServers` in ~/.cairn/settings.yaml or the project's .cairn/config.yaml."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::gateway::McpToolDef;

    fn tool(name: &str, schema: serde_json::Value) -> McpToolDef {
        McpToolDef {
            name: name.to_string(),
            description: Some("Observe Axon's current surface.\nMore detail.".to_string()),
            input_schema: schema,
        }
    }

    #[test]
    fn contract_renders_compact_invocation_with_descriptions() {
        let t = tool(
            "look",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "app": { "type": "string", "description": "Bundle id, pid, app name." },
                    "depth": { "type": "number", "description": "Maximum tree depth." }
                },
                "required": ["app"]
            }),
        );
        let out = render_tool_contract("axon", &t);
        // Header carries the first line of the description.
        assert!(out.contains("### look — Observe Axon's current surface."));
        // Invocation shape teaches the payload.args_json structure.
        assert!(out.contains("run {target:\"cairn://mcp/axon/look\", payload:{args_json:{"));
        // Required arg has no `?`; optional arg does. Descriptions retained.
        assert!(out.contains("app: string"));
        assert!(!out.contains("app?: string"));
        assert!(out.contains("depth?: number"));
        assert!(out.contains("— Bundle id, pid, app name."));
        assert!(out.contains("— Maximum tree depth."));
        assert!(out.trim_end().ends_with("}}}"));
        // No raw JSON schema dump.
        assert!(!out.contains("inputSchema"));
    }

    #[test]
    fn contract_renders_argless_tool() {
        let t = tool(
            "snapshot",
            serde_json::json!({ "type": "object", "properties": {} }),
        );
        let out = render_tool_contract("axon", &t);
        assert!(out.contains("run {target:\"cairn://mcp/axon/snapshot\", payload:{args_json:{}}}"));
    }

    #[test]
    fn property_type_falls_back_to_inline_json_for_complex_shapes() {
        // anyOf / no simple `type`: fall back to compact inline JSON, minus the
        // description (which is rendered separately).
        let spec = serde_json::json!({
            "anyOf": [ { "type": "string" }, { "type": "object" } ],
            "description": "Snapshot handle or locator."
        });
        let ty = property_type(&spec);
        assert!(ty.contains("anyOf"));
        assert!(!ty.contains("description"));
        assert_eq!(property_desc(&spec), "Snapshot handle or locator.");
    }

    #[test]
    fn property_type_joins_union_types() {
        let spec = serde_json::json!({ "type": ["string", "number"] });
        assert_eq!(property_type(&spec), "string|number");
    }

    #[test]
    fn terse_signature_marks_optionals_and_argless() {
        let t = tool(
            "look",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "app": { "type": "string" },
                    "depth": { "type": "number" }
                },
                "required": ["app"]
            }),
        );
        let sig = terse_tool_signature(&t);
        // Required arg has no `?`; optional arg does. Whole thing is one line.
        assert!(sig.contains("app: string"));
        assert!(!sig.contains("app?: string"));
        assert!(sig.contains("depth?: number"));
        assert!(sig.starts_with("look("));
        assert!(sig.ends_with(")"));
        assert!(!sig.contains('\n'));

        let argless = tool(
            "snapshot",
            serde_json::json!({ "type": "object", "properties": {} }),
        );
        assert_eq!(terse_tool_signature(&argless), "snapshot()");
    }

    #[test]
    fn affordance_block_renders_stored_contracts_and_missing_pointer() {
        let mut servers: HashMap<String, McpServerConfig> = HashMap::new();
        servers.insert(
            "axon".to_string(),
            McpServerConfig {
                transport: "stdio".to_string(),
                command: Some("axon".to_string()),
                args: vec![],
                env: HashMap::new(),
                url: None,
                headers: HashMap::new(),
                enabled: true,
                oauth: None,
            },
        );
        servers.insert(
            "playwright".to_string(),
            McpServerConfig {
                transport: "stdio".to_string(),
                command: Some("npx".to_string()),
                args: vec![],
                env: HashMap::new(),
                url: None,
                headers: HashMap::new(),
                enabled: true,
                oauth: None,
            },
        );

        // Only axon has stored tools; playwright has never listed successfully.
        let mut tools_by_server: HashMap<String, Vec<McpToolDef>> = HashMap::new();
        tools_by_server.insert(
            "axon".to_string(),
            vec![tool(
                "look",
                serde_json::json!({
                    "type": "object",
                    "properties": { "app": { "type": "string" } },
                    "required": []
                }),
            )],
        );

        let block =
            render_mcp_affordance_block(&servers, &tools_by_server).expect("servers present");
        // Heading + gateway invocation guidance, no raw inputSchema dump.
        assert!(block.contains("## MCP Servers"));
        assert!(block.contains("cairn://mcp/<server>/<tool>"));
        assert!(!block.contains("inputSchema"));
        // Stored server: terse per-tool contract with description.
        assert!(block.contains("- **axon** (stdio)"));
        assert!(block.contains("`look(app?: string)`"));
        assert!(block.contains("Observe Axon's current surface"));
        // Server with no stored tools: a read pointer, not a spawn.
        assert!(block
            .contains("- **playwright** (stdio) — read `cairn://mcp/playwright` for its tools"));
    }

    #[test]
    fn affordance_block_none_when_no_servers() {
        let servers: HashMap<String, McpServerConfig> = HashMap::new();
        let tools_by_server: HashMap<String, Vec<McpToolDef>> = HashMap::new();
        assert!(render_mcp_affordance_block(&servers, &tools_by_server).is_none());
    }
}
