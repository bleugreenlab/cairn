//! Agent resource mutations (create / patch / delete via `write`).
//!
//! File-backed via `config::agents`. Structured payloads mirror the agent
//! markdown frontmatter (name, description, prompt, tools, tier, ...). Every
//! write is validated by rendering the agent markdown and re-parsing it through
//! `parse_agent_markdown` — the exact loader path — so malformed agents are
//! rejected rather than written.

use cairn_config::library::{LibraryKind, LibraryRoots, Origin};
use std::path::PathBuf;

use cairn_common::uri::{build_agent_uri, build_project_agent_uri};

use crate::agents::{agent_to_markdown, parse_agent_markdown, AgentExportData};
use crate::config::agents::{self as config_agents, FileAgent};
use crate::config::slugify;
use crate::mcp::types::McpCallbackRequest;
use crate::models::{Fence, LegacyOnEscape, LegacySandbox};
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
    if let Some(models) = payload_string_array(payload, "models", None)? {
        agent.models = cairn_config::allotment::parse_allotment(&models)
            .map_err(|e| format!("payload.models is invalid: {e}"))?;
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
        models: &agent.models,
        prompt: &agent.prompt,
        fence: agent.fence,
        disallowed_tools: agent.disallowed_tools.as_deref(),
        skills: agent.skills.as_deref(),
        hooks: agent.hooks.as_ref(),
        icon: agent.icon.as_deref(),
        bundles: &agent.bundles,
        seed_budget_tokens: None,
        seed_eras: None,
    });
    parse_agent_markdown(&markdown).map(|_| ())
}

fn agent_file_exists(roots: &LibraryRoots, id: &str, layer: &Origin) -> bool {
    roots
        .layer_dir(LibraryKind::Agent, layer)
        .is_some_and(|dir| dir.join(format!("{id}.md")).exists())
}

/// The agent a write addresses: the project's own for an explicit project,
/// else whatever the name resolves to.
fn addressed_agent(
    roots: &LibraryRoots,
    agent_id: &str,
    explicit_project: Option<&str>,
) -> Result<FileAgent, String> {
    let resolved = match (explicit_project, roots.project_origin()) {
        (Some(_), Some(layer)) => {
            config_agents::resolve_agent(roots, agent_id).and_then(|resolved| {
                if resolved.origin == layer {
                    Ok(resolved)
                } else {
                    Err(cairn_config::library::ResolveError::NotFound {
                        kind: LibraryKind::Agent,
                        name: agent_id.to_string(),
                    })
                }
            })
        }
        _ => config_agents::resolve_agent(roots, agent_id),
    };
    match resolved {
        Ok(resolved) => Ok(resolved.item),
        Err(error) if error.is_not_found() => Err(not_found(agent_id, explicit_project)),
        Err(error) => Err(error.to_string()),
    }
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

    let project_path = super::scope_project_path(orch, request, explicit_project).await?;
    let roots = LibraryRoots::at(&orch.config_dir, project_path.as_deref());
    let layer = super::write_layer(payload, &roots, explicit_project)?.unwrap_or(Origin::Workspace);
    if agent_file_exists(&roots, &id, &layer) {
        return Err(format!("Agent already exists: {id}"));
    }

    let mut agent = FileAgent {
        id: id.clone(),
        name: name.to_string(),
        description: String::new(),
        prompt: String::new(),
        tools: Vec::new(),
        models: Vec::new(),
        fence: None,
        disallowed_tools: None,
        skills: None,
        hooks: None,
        icon: None,
        bundles: Vec::new(),
        origin: layer,
        file_path: PathBuf::new(),
        seed_budget_tokens: None,
        seed_eras: None,
    };
    apply_fields(&mut agent, payload)?;
    validate_roundtrip(&agent)?;

    let path = config_agents::save_agent(&roots, &agent, None)?;
    crate::config::commit_config_paths(
        &config_agents::agent_stage_paths(&roots, &path),
        &format!("cairn: create agent {id}"),
    );
    let agent_uri = match explicit_project {
        Some(project) => build_project_agent_uri(project, &id),
        None => build_agent_uri(&id),
    };
    Ok(super::created_resource_ack("agent", &id, &agent_uri))
}

pub(super) async fn apply_agent_patch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    payload: &serde_json::Value,
    agent_id: &str,
    explicit_project: Option<&str>,
) -> Result<String, String> {
    let project_path = super::scope_project_path(orch, request, explicit_project).await?;
    let roots = LibraryRoots::at(&orch.config_dir, project_path.as_deref());
    let mut agent = addressed_agent(&roots, agent_id, explicit_project)?;
    let layer = super::write_layer(payload, &roots, explicit_project)?
        .filter(|layer| *layer != agent.origin);

    if let Some(name) = super::payload_trimmed_non_empty_str(payload, "name", &[]) {
        agent.name = name.to_string();
    }
    apply_fields(&mut agent, payload)?;
    validate_roundtrip(&agent)?;

    let path = config_agents::save_agent(&roots, &agent, layer.as_ref())?;
    crate::config::commit_config_paths(
        &config_agents::agent_stage_paths(&roots, &path),
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
    let roots = LibraryRoots::at(&orch.config_dir, project_path.as_deref());
    let agent = addressed_agent(&roots, agent_id, explicit_project)?;
    let changed = config_agents::delete_agent(&roots, agent_id, Some(&agent.origin))?
        .ok_or_else(|| not_found(agent_id, explicit_project))?;
    crate::config::commit_config_paths(
        &config_agents::agent_stage_paths(&roots, &changed),
        &format!("cairn: delete agent {agent_id}"),
    );
    Ok(match agent.origin.pack_id() {
        Some(pack) => format!("Removed agent '{agent_id}' from pack {pack}"),
        None => format!("Deleted agent '{agent_id}'"),
    })
}

fn not_found(agent_id: &str, explicit_project: Option<&str>) -> String {
    match explicit_project {
        Some(project) => format!(
            "Agent not found in project {}: {agent_id}",
            cairn_common::uri::canonical_project(project)
        ),
        None => format!("Agent not found: {agent_id}"),
    }
}
