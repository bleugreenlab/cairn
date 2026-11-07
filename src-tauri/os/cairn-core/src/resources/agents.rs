//! Agent resource reads: the `cairn://agents` collection and single agents.
//!
//! Mirrors the recipes read surface, file-backed via `config::agents`. The
//! contextual collection lists every agent the resolver sees across the
//! current project, the workspace and installed packs, each with the layer it
//! resolves from and what it shadows; a project definition that is skipped
//! (untrusted or unconfirmed) is listed with why. An explicit project
//! collection is project-only.

use cairn_config::library::{self, LibraryKind, LibraryRoots, Listed};
use std::path::PathBuf;

use super::library_listing::{origin_lines, render_collection, Links};
use crate::config::agents::{self as config_agents, FileAgent};
use crate::mcp::handlers::skills_resources::{current_run_project, project_path_by_key};
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use cairn_common::uri::{build_agent_uri, build_project_agent_uri};

const LINKS: Links = Links {
    bare: build_agent_uri,
    project: build_project_agent_uri,
};

/// Resolve the project key + repo path for the requested scope. Mirrors recipes.
async fn resolve_scope(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    explicit_project: Option<&str>,
) -> Result<(Option<String>, Option<PathBuf>), String> {
    if let Some(project) = explicit_project {
        let path = project_path_by_key(orch, project).await?;
        Ok((
            Some(cairn_common::uri::canonical_project(project)),
            Some(path),
        ))
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

    let rows = match config_agents::list_agent_rows(&LibraryRoots::at(
        &orch.config_dir,
        project_path.as_deref(),
    )) {
        Ok(rows) => rows,
        Err(e) => return format!("Error listing agents: {e}"),
    };
    render_rows(rows, project_key.as_deref(), explicit_project.is_some())
}

fn render_rows(
    rows: Vec<Listed<FileAgent>>,
    project_key: Option<&str>,
    project_only: bool,
) -> String {
    render_collection(
        "Agents",
        "agent",
        LibraryKind::Agent,
        rows,
        project_key,
        project_only,
        &LINKS,
        |agent: &FileAgent| agent.name.clone(),
    )
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

    let resolved = match config_agents::resolve_agent(
        &LibraryRoots::at(&orch.config_dir, project_path.as_deref()),
        agent_id,
    ) {
        Ok(resolved) => resolved,
        Err(error) if error.is_not_found() => return not_found(agent_id, explicit_project),
        // Untrusted, unconfirmed, colliding or unreadable: the refusal names
        // the definition and what the operator does about it.
        Err(error) => return format!("{error}\n"),
    };

    // Explicit project scope is project-only: never resolve a shared workspace
    // agent behind an explicit project URI.
    if explicit_project.is_some() && !resolved.origin.is_project() {
        return not_found(agent_id, explicit_project);
    }
    render_agent(&resolved)
}

fn render_agent(resolved: &library::Resolved<FileAgent>) -> String {
    let agent = &resolved.item;
    let mut out = format!("# Agent `{}` — {}\n\n", agent.id, agent.name);
    out.push_str(&origin_lines(
        LibraryKind::Agent,
        &agent.id,
        &resolved.origin,
        &resolved.shadows,
    ));
    out.push('\n');
    if !agent.description.is_empty() {
        out.push_str(&format!("{}\n\n", agent.description));
    }
    if !agent.models.is_empty() {
        let entries: Vec<String> = agent.models.iter().map(ToString::to_string).collect();
        out.push_str(&format!("- models: [{}]\n", entries.join(", ")));
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
            cairn_common::uri::canonical_project(project)
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

    fn render(roots: &LibraryRoots) -> String {
        render_rows(
            config_agents::list_agent_rows(roots).unwrap(),
            Some("cairn"),
            false,
        )
    }

    #[test]
    fn a_trusted_project_agent_shadows_the_workspace_s_and_says_so() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");
        write_agent(&config_dir, "shared", "Workspace Version");
        write_project_agent(&project_dir, "shared", "Project Version");

        let rendered = render(&crate::config::trusted_roots(&config_dir, &project_dir));
        assert!(
            rendered.contains("cairn://p/cairn/agents/shared"),
            "{rendered}"
        );
        assert!(
            rendered.contains("[@, shadows ~] — Project Version"),
            "{rendered}"
        );
        assert!(!rendered.contains("Workspace Version"), "{rendered}");
    }

    #[test]
    fn an_untrusted_project_agent_is_listed_as_skipped_and_the_workspace_s_is_in_force() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");
        write_agent(&config_dir, "shared", "Workspace Version");
        write_project_agent(&project_dir, "shared", "Project Version");

        let rendered = render(&LibraryRoots::workspace(&config_dir).with_project(
            library::test_support::project(&project_dir, "cairn", library::ProjectTrust::Untrusted),
        ));
        let (in_force, skipped) = rendered.split_once("## Not in force").unwrap();
        assert!(in_force.contains("[~] — Workspace Version"), "{rendered}");
        assert!(
            skipped.contains("[@, untrusted: skipped] — Project Version"),
            "{rendered}"
        );
        assert!(skipped.contains(library::TRUST_SETTINGS_PLACE));
    }

    #[test]
    fn a_pack_agent_links_its_pack_address_and_a_workspace_one_in_front_names_it() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        library::test_support::install_pack_lock(&config_dir, "core", &[]);
        write_agent(
            &library::test_support::pack_root(&config_dir, "core"),
            "build",
            "Pack Build",
        );
        write_agent(&config_dir, "mine", "Mine");
        let rendered = render(&LibraryRoots::workspace(&config_dir));
        assert!(
            rendered.contains("(cairn://packs/core/agents/build) [pack:core] — Pack Build"),
            "{rendered}"
        );

        write_agent(&config_dir, "build", "My Build");
        let roots = LibraryRoots::workspace(&config_dir);
        let rendered = render(&roots);
        assert!(
            rendered.contains("[~, shadows pack:core] — My Build"),
            "{rendered}"
        );
        let detail = render_agent(&config_agents::resolve_agent(&roots, "build").unwrap());
        assert!(detail.contains("Origin: ~\n"), "{detail}");
        assert!(
            detail.contains("Shadows: pack:core (cairn://packs/core/agents/build)"),
            "{detail}"
        );
    }
}
