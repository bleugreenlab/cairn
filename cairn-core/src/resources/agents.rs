//! Agent resource reads: the `cairn://agents` collection and single agents.
//!
//! Mirrors the recipes read surface, file-backed via `config::agents`. The
//! contextual collection lists workspace + current-project agents (project
//! shadows workspace by id); an explicit project collection is project-only.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::config::agents::{self as config_agents, FileAgent};
use crate::config::ConfigResult;
use crate::mcp::handlers::skills_resources::{current_run_project, project_path_by_key};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{build_agent_uri, build_project_agent_uri};

fn scope_label(agent: &FileAgent) -> &'static str {
    if agent.is_project_scoped {
        "project"
    } else {
        "workspace"
    }
}

/// Canonical URI for an agent: project-scoped when it lives in a project, else workspace.
fn agent_link(agent: &FileAgent, project_key: Option<&str>) -> String {
    if agent.is_project_scoped {
        match project_key {
            Some(project) => build_project_agent_uri(project, &agent.id),
            None => build_agent_uri(&agent.id),
        }
    } else {
        build_agent_uri(&agent.id)
    }
}

/// Resolve the project key + repo path for the requested scope. Mirrors recipes.
async fn resolve_scope(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit_project: Option<&str>,
) -> Result<(Option<String>, Option<PathBuf>), String> {
    if let Some(project) = explicit_project {
        let path = project_path_by_key(orch, project).await?;
        Ok((Some(project.to_uppercase()), Some(path)))
    } else {
        match current_run_project(orch, request).await {
            Some((key, path)) => Ok((Some(key), path)),
            None => Ok((None, None)),
        }
    }
}

/// Render the agents collection (workspace + project, project shadows workspace by id).
pub(crate) async fn read_agents_collection(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit_project: Option<&str>,
) -> String {
    let (project_key, project_path) = match resolve_scope(orch, request, explicit_project).await {
        Ok(scope) => scope,
        Err(e) => return e,
    };

    let agents = match config_agents::list_agents(&orch.config_dir, project_path.as_deref()) {
        Ok(agents) => agents,
        Err(e) => return format!("Error listing agents: {e}"),
    };

    let mut by_id: BTreeMap<String, FileAgent> = BTreeMap::new();
    for result in agents {
        if let ConfigResult::Ok(agent) = result {
            // config_root_subdirs yields project first, so keep the first
            // occurrence for each id to let project agents shadow workspace.
            by_id.entry(agent.id.clone()).or_insert(agent);
        }
    }

    let header = match project_key.as_deref() {
        Some(key) => format!("# Agents — {key} context\n\n"),
        None => "# Agents — workspace\n\n".to_string(),
    };
    let mut out = header;
    out.push_str(&format!("{} agent(s)\n\n", by_id.len()));

    if by_id.is_empty() {
        out.push_str("No agents found.\n\n");
    } else {
        for agent in by_id.values() {
            out.push_str(&format!(
                "- [{}]({}) [{}] — {}\n",
                agent.id,
                agent_link(agent, project_key.as_deref()),
                scope_label(agent),
                agent.name,
            ));
        }
        out.push('\n');
    }

    out
}

/// Render a single agent: id, name, description, tier, tools, and actions.
pub(crate) async fn read_agent(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    agent_id: &str,
    explicit_project: Option<&str>,
) -> String {
    let (_, project_path) = match resolve_scope(orch, request, explicit_project).await {
        Ok(scope) => scope,
        Err(e) => return e,
    };

    let agent = match config_agents::get_agent(&orch.config_dir, agent_id, project_path.as_deref())
    {
        Ok(Some(agent)) => agent,
        Ok(None) => return not_found(agent_id, explicit_project),
        Err(e) => return format!("Error loading agent: {e}"),
    };

    // Explicit project scope is project-only: never resolve a shared workspace
    // agent behind an explicit project URI.
    if explicit_project.is_some() && !agent.is_project_scoped {
        return not_found(agent_id, explicit_project);
    }

    let mut out = format!(
        "# Agent `{}` — {}\n\n[{}]\n\n",
        agent.id,
        agent.name,
        scope_label(&agent),
    );
    if !agent.description.is_empty() {
        out.push_str(&format!("{}\n\n", agent.description));
    }
    if let Some(tier) = &agent.tier {
        out.push_str(&format!("- tier: {}\n", tier));
    }
    if let Some(backend) = &agent.backend_preference {
        out.push_str(&format!("- backend: {}\n", backend));
    }
    out.push_str(&format!("- tools: {}\n", agent.tools.join(", ")));
    if let Some(skills) = agent.skills.as_ref().filter(|s| !s.is_empty()) {
        out.push_str(&format!("- skills: {}\n", skills.join(", ")));
    }
    out.push('\n');
    out.push_str("## prompt\n");
    out.push_str(&agent.prompt);
    out.push_str("\n\n");

    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    fn write_agent(root: &Path, id: &str, name: &str) {
        let agents_dir = root.join("agents");
        std::fs::create_dir_all(&agents_dir).unwrap();
        std::fs::write(
            agents_dir.join(format!("{id}.md")),
            format!(
                "---\nname: {name}\ndescription: Description for {id}\ntools:\n  - Read\n---\n\nPrompt for {id}.\n"
            ),
        )
        .unwrap();
    }

    fn write_project_agent(project_dir: &Path, id: &str, name: &str) {
        let agents_dir = project_dir.join(".cairn").join("agents");
        std::fs::create_dir_all(&agents_dir).unwrap();
        std::fs::write(
            agents_dir.join(format!("{id}.md")),
            format!(
                "---\nname: {name}\ndescription: Project description for {id}\ntools:\n  - Read\n---\n\nProject prompt for {id}.\n"
            ),
        )
        .unwrap();
    }

    /// Render the collection directly from loaded agents, bypassing run-context
    /// resolution (which needs a DB). Mirrors `read_agents_collection`'s body.
    fn render_collection(config_dir: &Path, project_path: Option<&Path>) -> String {
        let agents = config_agents::list_agents(config_dir, project_path).unwrap();
        let mut by_id: BTreeMap<String, FileAgent> = BTreeMap::new();
        for result in agents {
            if let ConfigResult::Ok(agent) = result {
                by_id.entry(agent.id.clone()).or_insert(agent);
            }
        }

        let mut out = String::new();
        for agent in by_id.values() {
            out.push_str(&format!(
                "- [{}]({}) [{}] — {}\n",
                agent.id,
                agent_link(agent, Some("CAIRN")),
                scope_label(agent),
                agent.name,
            ));
        }
        out
    }

    #[test]
    fn project_agent_shadows_workspace_by_id() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");
        write_agent(&config_dir, "shared", "Workspace Version");
        write_project_agent(&project_dir, "shared", "Project Version");

        let rendered = render_collection(&config_dir, Some(&project_dir));
        // Project version wins and links project-scoped.
        assert!(rendered.contains("cairn://p/CAIRN/agents/shared"));
        assert!(rendered.contains("[project] — Project Version"));
        assert!(!rendered.contains("Workspace Version"));
    }
}
