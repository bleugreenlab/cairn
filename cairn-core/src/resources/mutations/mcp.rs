//! `write cairn://mcp` CRUD over the external MCP server registry.
//!
//! This is an ergonomic, schema-validated surface over the SAME settings the
//! Settings UI edits — not a new capability. A workspace-scope write edits
//! `~/.cairn/settings.yaml` and is routed through the worktree fence (raised in
//! the change handler before this runs, exactly like any out-of-worktree file
//! write); a project-scope write edits the run's `.cairn/config.yaml` in place.
//!
//! Secret VALUES never land in YAML: `env`/`headers` values may carry `${VAR}`
//! references (expanded at connect time), but the URI write sets non-secret
//! config only. Keychain-backed secrets are managed elsewhere.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::config::mcp_servers::{
    self, delete_project_mcp_server, delete_workspace_mcp_server, upsert_project_mcp_server,
    upsert_workspace_mcp_server, McpServerConfig, OAuthServerConfig,
};
use crate::config::mcp_tools::{self, ToolScope};
use crate::config::project_settings::load_project_settings;
use crate::mcp::handlers::skills_resources;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{parse_uri, CairnResource};

/// Where a `cairn://mcp` mutation lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum McpWriteScope {
    /// `~/.cairn/settings.yaml` — gated by the worktree fence.
    Workspace,
    /// The run's `[project]/.cairn/config.yaml` — in-place, no extra gate.
    Project,
}

/// Accepted config field names (for the field-list rejection on an unknown key).
const CONFIG_FIELDS: &str =
    "type, command, args, env, url, headers, enabled, oauth (plus name on create, and scope)";

/// Parse the optional `scope` field. Absent/null -> workspace (the default,
/// matching the Settings UI's scope).
fn parse_mcp_scope(payload: Option<&serde_json::Value>) -> Result<McpWriteScope, String> {
    match payload.and_then(|p| p.get("scope")) {
        None | Some(serde_json::Value::Null) => Ok(McpWriteScope::Workspace),
        Some(serde_json::Value::String(s)) => match s.as_str() {
            "workspace" => Ok(McpWriteScope::Workspace),
            "project" => Ok(McpWriteScope::Project),
            other => Err(format!(
                "invalid scope '{other}'; expected 'workspace' or 'project'"
            )),
        },
        Some(_) => Err("payload.scope must be a string ('workspace' or 'project')".to_string()),
    }
}

/// True when `item` is a workspace-scoped `cairn://mcp` create/patch/delete —
/// the out-of-worktree `settings.yaml` write the worktree fence must gate.
///
/// An item whose `scope` is malformed returns false here on purpose: it is
/// rejected with the scope error by the dispatcher before any side effect, so
/// it never reaches the settings file. Detection and dispatch both call
/// [`parse_mcp_scope`], so they agree on what "workspace" means.
pub(crate) fn is_workspace_mcp_mutation(item: &ChangeItem) -> bool {
    if !matches!(
        item.mode,
        ChangeMode::Create | ChangeMode::Patch | ChangeMode::Delete
    ) {
        return false;
    }
    if !matches!(parse_uri(&item.target), Some(CairnResource::Mcp { .. })) {
        return false;
    }
    matches!(
        parse_mcp_scope(item.payload.as_ref()),
        Ok(McpWriteScope::Workspace)
    )
}

fn base_config() -> McpServerConfig {
    McpServerConfig {
        transport: "stdio".to_string(),
        command: None,
        args: Vec::new(),
        env: HashMap::new(),
        url: None,
        headers: HashMap::new(),
        enabled: true,
        oauth: None,
    }
}

fn as_opt_str(name: &str, value: &serde_json::Value) -> Result<Option<String>, String> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) => Ok(Some(s.clone())),
        _ => Err(format!("payload.{name} must be a string")),
    }
}

fn as_str_array(name: &str, value: &serde_json::Value) -> Result<Vec<String>, String> {
    let arr = value
        .as_array()
        .ok_or_else(|| format!("payload.{name} must be an array of strings"))?;
    arr.iter()
        .map(|v| {
            v.as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| format!("payload.{name} must be an array of strings"))
        })
        .collect()
}

fn as_str_map(name: &str, value: &serde_json::Value) -> Result<HashMap<String, String>, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| format!("payload.{name} must be an object of string values"))?;
    obj.iter()
        .map(|(k, v)| {
            v.as_str()
                .map(|s| (k.clone(), s.to_string()))
                .ok_or_else(|| format!("payload.{name} values must be strings"))
        })
        .collect()
}

/// Apply the config fields present in `payload` onto `base` (default config for
/// create, the existing config for patch). Rejects unknown keys with the field
/// list so a typo surfaces immediately rather than being silently dropped.
fn apply_payload_fields(
    mut config: McpServerConfig,
    payload: &serde_json::Value,
) -> Result<McpServerConfig, String> {
    let obj = payload
        .as_object()
        .ok_or_else(|| format!("payload must be an object with fields: {CONFIG_FIELDS}"))?;
    for (key, value) in obj {
        match key.as_str() {
            // Routing keys handled by the caller, not config fields.
            "name" | "scope" => {}
            "type" => {
                config.transport = value
                    .as_str()
                    .map(ToOwned::to_owned)
                    .ok_or("payload.type must be a string")?
            }
            "command" => config.command = as_opt_str("command", value)?,
            "args" => config.args = as_str_array("args", value)?,
            "env" => config.env = as_str_map("env", value)?,
            "url" => config.url = as_opt_str("url", value)?,
            "headers" => config.headers = as_str_map("headers", value)?,
            "enabled" => {
                config.enabled = value.as_bool().ok_or("payload.enabled must be a boolean")?
            }
            // Non-secret OAuth block only (clientId/scopes). Tokens and any
            // client secret live in the keychain, never set via the URI.
            "oauth" => {
                config.oauth = match value {
                    serde_json::Value::Null => None,
                    other => Some(
                        serde_json::from_value::<OAuthServerConfig>(other.clone()).map_err(
                            |e| format!("payload.oauth must be {{clientId?, scopes?}}: {e}"),
                        )?,
                    ),
                }
            }
            other => {
                return Err(format!(
                    "unknown field '{other}'. Accepted fields: {CONFIG_FIELDS}"
                ))
            }
        }
    }
    Ok(config)
}

/// Transport-specific shape check: stdio needs a command, http/sse need a url.
fn validate_config(config: &McpServerConfig) -> Result<(), String> {
    match config.transport.as_str() {
        "stdio" => {
            if config
                .command
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
            {
                return Err(
                    "stdio server requires a non-empty 'command'. Accepted fields: ".to_string()
                        + CONFIG_FIELDS,
                );
            }
        }
        "http" | "sse" => {
            if config
                .url
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
            {
                return Err(format!(
                    "{} server requires a non-empty 'url'. Accepted fields: {CONFIG_FIELDS}",
                    config.transport
                ));
            }
        }
        other => {
            return Err(format!(
                "invalid type '{other}'; expected 'stdio', 'http', or 'sse'"
            ));
        }
    }
    Ok(())
}

fn require_name(payload: &serde_json::Value) -> Result<String, String> {
    super::payload_trimmed_non_empty_str(payload, "name", &[])
        .map(ToOwned::to_owned)
        .ok_or_else(|| "payload.name is required and must be a non-empty string".to_string())
}

fn scope_label(scope: McpWriteScope) -> &'static str {
    match scope {
        McpWriteScope::Workspace => "workspace",
        McpWriteScope::Project => "project",
    }
}

/// Resolve the project repo path for a project-scope write: the current run's
/// project. Errors when there is no resolvable project context.
async fn resolve_project_path(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<PathBuf, String> {
    skills_resources::current_run_project(orch, request)
        .await
        .and_then(|(_, path)| path)
        .ok_or_else(|| {
            "project scope requires an active run with a resolvable project path".to_string()
        })
}

/// Load the existing servers for a scope (full registry, including disabled).
async fn existing_servers(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    scope: McpWriteScope,
) -> Result<HashMap<String, McpServerConfig>, String> {
    match scope {
        McpWriteScope::Workspace => Ok(mcp_servers::load_workspace_mcp_servers(&orch.config_dir)),
        McpWriteScope::Project => {
            let project_path = resolve_project_path(orch, request).await?;
            Ok(load_project_settings(&project_path)
                .mcp_servers
                .unwrap_or_default())
        }
    }
}

async fn write_server(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    scope: McpWriteScope,
    name: &str,
    config: &McpServerConfig,
) -> Result<(), String> {
    match scope {
        McpWriteScope::Workspace => upsert_workspace_mcp_server(&orch.config_dir, name, config),
        McpWriteScope::Project => {
            let project_path = resolve_project_path(orch, request).await?;
            upsert_project_mcp_server(&project_path, name, config)
        }
    }
}

/// Remove a server from its scope's config AND purge its captured tool list
/// (CAIRN-1334's per-server tool store). The tool purge is best-effort cleanup:
/// the server is already gone from config, so a purge failure does not fail the
/// delete. `${VAR}`-referenced secrets live in the keychain/environment, not the
/// config, so there is nothing secret to remove here.
async fn remove_server(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    scope: McpWriteScope,
    name: &str,
) -> Result<(), String> {
    match scope {
        McpWriteScope::Workspace => {
            delete_workspace_mcp_server(&orch.config_dir, name)?;
            let _ = mcp_tools::remove_server(&orch.config_dir, ToolScope::Workspace, name);
        }
        McpWriteScope::Project => {
            let project_path = resolve_project_path(orch, request).await?;
            delete_project_mcp_server(&project_path, name)?;
            let _ =
                mcp_tools::remove_server(&orch.config_dir, ToolScope::Project(&project_path), name);
        }
    }
    Ok(())
}

/// `write cairn://mcp` create — add a new server to the registry.
pub(super) async fn apply_mcp_create(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    let name = require_name(payload)?;
    let scope = parse_mcp_scope(Some(payload))?;
    let config = apply_payload_fields(base_config(), payload)?;
    validate_config(&config)?;

    if existing_servers(orch, request, scope)
        .await?
        .contains_key(&name)
    {
        return Err(format!(
            "MCP server already exists: {name}. Use mode=patch to edit it."
        ));
    }

    if dry_run {
        return Ok(format!(
            "Would add {} MCP server '{name}'",
            scope_label(scope)
        ));
    }
    write_server(orch, request, scope, &name, &config).await?;
    Ok(format!("Added {} MCP server '{name}'", scope_label(scope)))
}

/// `write cairn://mcp/<server>` patch — edit one server in place.
pub(super) async fn apply_mcp_patch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    server: &str,
    dry_run: bool,
) -> Result<String, String> {
    let scope = parse_mcp_scope(Some(payload))?;
    let existing = existing_servers(orch, request, scope).await?;
    let base = existing.get(server).cloned().ok_or_else(|| {
        format!(
            "MCP server not found in {} scope: {server}",
            scope_label(scope)
        )
    })?;
    let config = apply_payload_fields(base, payload)?;
    validate_config(&config)?;

    if dry_run {
        return Ok(format!(
            "Would edit {} MCP server '{server}'",
            scope_label(scope)
        ));
    }
    write_server(orch, request, scope, server, &config).await?;
    Ok(format!(
        "Updated {} MCP server '{server}'",
        scope_label(scope)
    ))
}

/// `write cairn://mcp/<server>` delete — remove one server and purge its
/// captured tool list. `${VAR}`-referenced secrets live in the keychain, not the
/// config, so nothing secret is stored to purge here.
pub(super) async fn apply_mcp_delete(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: Option<&serde_json::Value>,
    server: &str,
    dry_run: bool,
) -> Result<String, String> {
    let scope = parse_mcp_scope(payload)?;
    let existing = existing_servers(orch, request, scope).await?;
    if !existing.contains_key(server) {
        return Err(format!(
            "MCP server not found in {} scope: {server}",
            scope_label(scope)
        ));
    }

    if dry_run {
        return Ok(format!(
            "Would remove {} MCP server '{server}'",
            scope_label(scope)
        ));
    }
    remove_server(orch, request, scope, server).await?;
    Ok(format!(
        "Removed {} MCP server '{server}'",
        scope_label(scope)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::types::ChangeItem;

    fn item(target: &str, mode: ChangeMode, payload: serde_json::Value) -> ChangeItem {
        ChangeItem {
            target: target.to_string(),
            mode,
            payload: Some(payload),
        }
    }

    #[test]
    fn scope_defaults_to_workspace() {
        assert_eq!(parse_mcp_scope(None).unwrap(), McpWriteScope::Workspace);
        assert_eq!(
            parse_mcp_scope(Some(&serde_json::json!({}))).unwrap(),
            McpWriteScope::Workspace
        );
        assert_eq!(
            parse_mcp_scope(Some(&serde_json::json!({"scope": "project"}))).unwrap(),
            McpWriteScope::Project
        );
    }

    #[test]
    fn scope_rejects_unknown_value() {
        let err = parse_mcp_scope(Some(&serde_json::json!({"scope": "global"}))).unwrap_err();
        assert!(err.contains("invalid scope 'global'"), "got: {err}");
    }

    #[test]
    fn detects_workspace_mcp_mutations_only() {
        // Workspace-scoped create/patch/delete on cairn://mcp -> fenced.
        assert!(is_workspace_mcp_mutation(&item(
            "cairn://mcp",
            ChangeMode::Create,
            serde_json::json!({"name": "x", "command": "npx"})
        )));
        assert!(is_workspace_mcp_mutation(&item(
            "cairn://mcp/playwright",
            ChangeMode::Delete,
            serde_json::json!({})
        )));
        // Project scope is in-worktree -> not fenced.
        assert!(!is_workspace_mcp_mutation(&item(
            "cairn://mcp",
            ChangeMode::Create,
            serde_json::json!({"name": "x", "command": "npx", "scope": "project"})
        )));
        // A non-mcp target is not an mcp mutation.
        assert!(!is_workspace_mcp_mutation(&item(
            "cairn://skills",
            ChangeMode::Create,
            serde_json::json!({})
        )));
        // Malformed scope -> not treated as workspace (dispatch rejects it).
        assert!(!is_workspace_mcp_mutation(&item(
            "cairn://mcp",
            ChangeMode::Create,
            serde_json::json!({"name": "x", "scope": "bogus"})
        )));
    }

    #[test]
    fn build_config_validates_stdio_requires_command() {
        let cfg = apply_payload_fields(
            base_config(),
            &serde_json::json!({"name": "x", "args": ["--foo"]}),
        )
        .unwrap();
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.contains("requires a non-empty 'command'"), "got: {err}");
    }

    #[test]
    fn build_config_http_requires_url() {
        let cfg = apply_payload_fields(
            base_config(),
            &serde_json::json!({"name": "x", "type": "http"}),
        )
        .unwrap();
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.contains("requires a non-empty 'url'"), "got: {err}");
    }

    #[test]
    fn build_config_rejects_unknown_field() {
        let err = apply_payload_fields(
            base_config(),
            &serde_json::json!({"name": "x", "commnd": "npx"}),
        )
        .unwrap_err();
        assert!(err.contains("unknown field 'commnd'"), "got: {err}");
        assert!(err.contains("Accepted fields"), "got: {err}");
    }

    #[test]
    fn build_config_rejects_bad_type_and_enabled() {
        assert!(
            apply_payload_fields(base_config(), &serde_json::json!({"args": "not-an-array"}))
                .unwrap_err()
                .contains("args must be an array")
        );
        assert!(
            apply_payload_fields(base_config(), &serde_json::json!({"enabled": "yes"}))
                .unwrap_err()
                .contains("enabled must be a boolean")
        );
    }

    #[test]
    fn build_config_accepts_full_stdio_shape() {
        let cfg = apply_payload_fields(
            base_config(),
            &serde_json::json!({
                "name": "playwright",
                "command": "npx",
                "args": ["@playwright/mcp@latest"],
                "env": {"TOKEN": "${MY_TOKEN}"},
                "enabled": false
            }),
        )
        .unwrap();
        validate_config(&cfg).unwrap();
        assert_eq!(cfg.transport, "stdio");
        assert_eq!(cfg.command.as_deref(), Some("npx"));
        assert_eq!(cfg.args, vec!["@playwright/mcp@latest"]);
        assert_eq!(
            cfg.env.get("TOKEN").map(String::as_str),
            Some("${MY_TOKEN}")
        );
        assert!(!cfg.enabled);
    }
}
