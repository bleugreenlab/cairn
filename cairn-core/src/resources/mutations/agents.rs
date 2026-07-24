//! Agent resource mutations (create / patch / delete via `write`).
//!
//! File-backed via `config::agents`. Structured payloads mirror the agent
//! markdown frontmatter (name, description, prompt, tools, tier, ...). Every
//! write is validated by rendering the agent markdown and re-parsing it through
//! `parse_agent_markdown` — the exact loader path — so malformed agents are
//! rejected rather than written.

use std::path::PathBuf;

use crate::agents::{agent_to_markdown, parse_agent_markdown, AgentExportData};
use crate::config::agents::{self as config_agents, FileAgent};
use crate::config::slugify;
use crate::mcp::types::McpCallbackRequest;
use crate::models::{Fence, LegacyOnEscape, LegacySandbox, Model};
use crate::orchestrator::Orchestrator;

fn payload_string_array(
    payload: &serde_json::Value,
    key: &str,
    alias: Option<&str>,
) -> Result<Option<Vec<String>>, String> {
    let aliases = alias.into_iter().collect::<Vec<_>>();
    let value = super::payload_value(payload, key, &aliases);
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Array(values)) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                let entry = value
                    .as_str()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| {
                        format!("payload.{key} must be an array of non-empty strings")
                    })?;
                out.push(entry.to_string());
            }
            Ok(Some(out))
        }
        Some(_) => Err(format!("payload.{key} must be an array of strings")),
    }
}

fn parse_enum<T: serde::de::DeserializeOwned>(
    payload: &serde_json::Value,
    key: &str,
    alias: Option<&str>,
) -> Result<Option<T>, String> {
    let aliases = alias.into_iter().collect::<Vec<_>>();
    let value = super::payload_value(payload, key, &aliases);
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => serde_json::from_value::<T>(value.clone())
            .map(Some)
            .map_err(|e| format!("payload.{key} is invalid: {e}")),
    }
}

/// Apply present payload fields onto an agent. Used by both create and patch.
fn apply_fields(agent: &mut FileAgent, payload: &serde_json::Value) -> Result<(), String> {
    if let Some(description) = super::payload_str(payload, "description", &[]) {
        agent.description = description.to_string();
    }
    if let Some(prompt) = super::payload_str(payload, "prompt", &[]) {
        agent.prompt = prompt.to_string();
    }
    if let Some(tools) = payload_string_array(payload, "tools", None)? {
        agent.tools = tools;
    }
    if let Some(tier) = super::payload_str(payload, "tier", &["model"]) {
        let parsed: Model = tier
            .parse()
            .map_err(|e| format!("payload.tier is invalid: {e}"))?;
        agent.tier = Some(parsed);
    }
    if let Some(backend) = super::payload_str(payload, "backend", &[]) {
        agent.backend_preference = Some(backend.to_string());
    }
    if let Some(icon) = super::payload_str(payload, "icon", &[]) {
        agent.icon = Some(icon.to_string());
    }
    if let Some(fence) = parse_enum::<Fence>(payload, "fence", None)? {
        agent.fence = Some(fence);
    } else {
        let legacy_sandbox = parse_enum::<LegacySandbox>(payload, "sandbox", None)?;
        let legacy_on_escape =
            parse_enum::<LegacyOnEscape>(payload, "onEscape", Some("on_escape"))?;
        if legacy_sandbox.is_some() || legacy_on_escape.is_some() {
            agent.fence = Some(Fence::from_legacy(legacy_sandbox, legacy_on_escape));
        }
    }
    if let Some(disallowed) =
        payload_string_array(payload, "disallowedTools", Some("disallowed_tools"))?
    {
        agent.disallowed_tools = Some(disallowed);
    }
    if let Some(skills) = payload_string_array(payload, "skills", None)? {
        agent.skills = Some(skills);
    }
    if let Some(hooks) = payload.get("hooks") {
        if !hooks.is_null() {
            agent.hooks = Some(hooks.clone());
        }
    }
    Ok(())
}

/// Render the agent to markdown and re-parse it to validate loader parity.
fn validate_roundtrip(agent: &FileAgent) -> Result<(), String> {
    let markdown = agent_to_markdown(AgentExportData {
        id: &agent.id,
        name: &agent.name,
        description: &agent.description,
        tools: &agent.tools,
        tier: agent.tier.as_ref().map(|m| m.to_string()).as_deref(),
        prompt: &agent.prompt,
        fence: agent.fence,
        disallowed_tools: agent.disallowed_tools.as_deref(),
        skills: agent.skills.as_deref(),
        hooks: agent.hooks.as_ref(),
        backend_preference: agent.backend_preference.as_deref(),
        icon: agent.icon.as_deref(),
        bundles: &agent.bundles,
    });
    parse_agent_markdown(&markdown).map(|_| ())
}

fn agent_file_exists(
    orch: &Orchestrator,
    id: &str,
    is_project_scoped: bool,
    project_path: Option<&std::path::Path>,
) -> bool {
    let dir = if is_project_scoped {
        match project_path {
            Some(pp) => pp.join(".cairn").join("agents"),
            None => return false,
        }
    } else {
        orch.config_dir.join("agents")
    };
    dir.join(format!("{id}.md")).exists()
}

pub(super) async fn apply_agent_create(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let name = super::payload_trimmed_non_empty_str(payload, "name", &[])
        .ok_or("payload.name is required and must be a non-empty string")?;
    let id = slugify(name);
    if id.is_empty() {
        return Err("Could not derive an agent id from payload.name".to_string());
    }

    let is_project_scoped = explicit_project.is_some();
    let project_path = super::scope_project_path(orch, request, explicit_project).await?;
    if is_project_scoped && project_path.is_none() {
        return Err("Project path required for a project-scoped agent".to_string());
    }
    if agent_file_exists(orch, &id, is_project_scoped, project_path.as_deref()) {
        return Err(format!("Agent already exists: {id}"));
    }

    let mut agent = FileAgent {
        id: id.clone(),
        name: name.to_string(),
        description: String::new(),
        prompt: String::new(),
        tools: Vec::new(),
        tier: None,
        fence: None,
        disallowed_tools: None,
        skills: None,
        hooks: None,
        backend_preference: None,
        icon: None,
        bundles: Vec::new(),
        is_project_scoped,
        file_path: PathBuf::new(),
    };
    apply_fields(&mut agent, payload)?;
    validate_roundtrip(&agent)?;

    let path = config_agents::save_agent(&orch.config_dir, &agent, project_path.as_deref())?;
    crate::config::commit_config_paths(
        std::slice::from_ref(&path),
        &format!("cairn: create agent {id}"),
    );
    Ok(format!("Created agent '{id}' at {}", path.display()))
}

pub(super) async fn apply_agent_patch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    agent_id: &str,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let project_path = super::scope_project_path(orch, request, explicit_project).await?;
    let mut agent = config_agents::get_agent(&orch.config_dir, agent_id, project_path.as_deref())?
        .filter(|a| explicit_project.is_none() || a.is_project_scoped)
        .ok_or_else(|| not_found(agent_id, explicit_project))?;

    if let Some(name) = super::payload_trimmed_non_empty_str(payload, "name", &[]) {
        agent.name = name.to_string();
    }
    apply_fields(&mut agent, payload)?;
    validate_roundtrip(&agent)?;

    let path = config_agents::save_agent(&orch.config_dir, &agent, project_path.as_deref())?;
    crate::config::commit_config_paths(
        std::slice::from_ref(&path),
        &format!("cairn: update agent {agent_id}"),
    );
    Ok(format!("Updated agent '{agent_id}'"))
}

pub(super) async fn apply_agent_delete(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    agent_id: &str,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let project_path = super::scope_project_path(orch, request, explicit_project).await?;
    let Some(agent) =
        config_agents::get_agent(&orch.config_dir, agent_id, project_path.as_deref())?
            .filter(|a| explicit_project.is_none() || a.is_project_scoped)
    else {
        return Err(not_found(agent_id, explicit_project));
    };
    config_agents::delete_agent(&orch.config_dir, agent_id, project_path.as_deref())?;
    crate::config::commit_config_paths(
        std::slice::from_ref(&agent.file_path),
        &format!("cairn: delete agent {agent_id}"),
    );
    Ok(format!("Deleted agent '{agent_id}'"))
}

fn not_found(agent_id: &str, explicit_project: Option<&str>) -> String {
    match explicit_project {
        Some(project) => format!(
            "Agent not found in project {}: {agent_id}",
            project.to_uppercase()
        ),
        None => format!("Agent not found: {agent_id}"),
    }
}
