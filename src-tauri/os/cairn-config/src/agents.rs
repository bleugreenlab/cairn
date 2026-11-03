//! Agent configuration file reading.
//!
//! Agents are stored as markdown files with YAML frontmatter.
//! - Workspace-scoped: `~/.cairn/agents/`
//! - Project-scoped: `[project-dir]/.cairn/agents/`

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::agent_markdown::{agent_to_markdown, parse_agent_markdown, AgentExportData};
use crate::agent_pins::{self, AgentPin};
use crate::allotment::AllotmentEntry;
use crate::models::Fence;

use super::{id_from_path, ConfigResult};

/// Agent loaded from a file
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileAgent {
    pub id: String,
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub tools: Vec<String>,
    /// The pools and models this agent selects within; empty means the
    /// workspace's default allotment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<AllotmentEntry>,
    pub fence: Option<Fence>,
    pub disallowed_tools: Option<Vec<String>>,
    pub skills: Option<Vec<String>>,
    pub hooks: Option<serde_json::Value>,
    /// Optional lucide icon name (kebab-case) for the agent's visual identity.
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub bundles: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed_budget_tokens: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed_eras: Option<i32>,
    /// Whether this agent is project-scoped (vs workspace-scoped)
    pub is_project_scoped: bool,
    /// Path to the source file
    pub file_path: PathBuf,
}

impl FileAgent {
    /// This agent as the runtime config a job launches with in `project_id`,
    /// with no model chosen yet.
    pub fn agent_config(&self, project_id: &str) -> crate::models::AgentConfig {
        crate::models::AgentConfig {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            prompt: self.prompt.clone(),
            tools: self.tools.clone(),
            models: self.models.clone(),
            workspace_id: (!self.is_project_scoped).then(|| "workspace".to_string()),
            project_id: self.is_project_scoped.then(|| project_id.to_string()),
            created_at: 0,
            updated_at: 0,
            disallowed_tools: self.disallowed_tools.clone(),
            skills: self.skills.clone(),
            fence: self.fence,
            icon: self.icon.clone(),
            selection: None,
            extras: None,
        }
    }

    /// This agent as an execution snapshot freezes it: its allotment and
    /// definition, with no model chosen. A node running it selects at its own
    /// launch; only an operator's pin sets `selection`.
    pub fn snapshot(&self) -> crate::models::AgentSnapshot {
        crate::models::AgentSnapshot {
            edited_at: None,
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            prompt: self.prompt.clone(),
            tools: self.tools.clone(),
            models: self.models.clone(),
            selection: None,
            provider_account: None,
            disallowed_tools: self.disallowed_tools.clone(),
            skills: self.skills.clone(),
            seed_budget_tokens: self.seed_budget_tokens,
            seed_eras: self.seed_eras,
            fence: self.fence,
            sandbox: None,
            on_escape: None,
            extras: None,
            model: None,
            resolved_backend: None,
        }
    }
}

/// List all agents from files
///
/// Reads workspace-scoped agents from `~/.cairn/agents/`.
/// If project_path is Some, also includes agents from `[project_path]/.cairn/agents/`.
pub fn list_agents(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileAgent>>, String> {
    let mut results = vec![];
    let pins = agent_pins::load_pins(config_dir);

    for (dir, is_project_scoped) in super::config_root_subdirs(config_dir, project_path, "agents") {
        if !dir.exists() || !dir.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(&dir)
            .map_err(|e| format!("Failed to read agents directory: {}", e))?
        {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();
            if path.is_dir() || path.extension().and_then(|e| e.to_str()) != Some("md") {
                continue;
            }
            let mut result = load_agent_file(&path, is_project_scoped);
            if let ConfigResult::Ok(agent) = &mut result {
                apply_workspace_pin(&pins, agent);
            }
            results.push(result);
        }
    }

    Ok(results)
}

/// Overlay the workspace pin for a workspace-scoped agent. Project agents are
/// never pack-owned, so the workspace pin document does not reach them.
fn apply_workspace_pin(pins: &agent_pins::AgentPins, agent: &mut FileAgent) {
    if agent.is_project_scoped {
        return;
    }
    if let Some(pin) = pins.get(&agent.id) {
        pin.apply(agent);
    }
}

/// Get a specific agent by ID
///
/// Checks project-scoped first (if project_path provided), then workspace-scoped.
pub fn get_agent(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<FileAgent>, String> {
    for (dir, is_project_scoped) in super::config_root_subdirs(config_dir, project_path, "agents") {
        let path = dir.join(format!("{}.md", id));
        if path.exists() {
            return match load_agent_file(&path, is_project_scoped) {
                ConfigResult::Ok(mut agent) => {
                    apply_workspace_pin(&agent_pins::load_pins(config_dir), &mut agent);
                    Ok(Some(agent))
                }
                ConfigResult::Err { error, .. } => Err(error),
            };
        }
    }

    Ok(None)
}

/// Save an agent.
///
/// Uses the agent's file_path if set, otherwise determines path from is_project_scoped.
/// For project-scoped agents without file_path, project_path must be provided.
///
/// For a workspace agent an installed pack ships, the pinnable keys (`icon`,
/// `models`) are recorded in the pin document rather than the file, and the
/// file is rewritten only when something else changed. Setting an allotment
/// on a shipped agent therefore leaves its file byte-identical and pack-owned,
/// and the pin survives both pack updates and a reset to the pack version.
/// Returns the agent file's path; callers stage [`agent_stage_paths`].
pub fn save_agent(
    config_dir: &Path,
    agent: &FileAgent,
    project_path: Option<&Path>,
) -> Result<PathBuf, String> {
    let path = resolve_save_path(config_dir, agent, project_path)?;
    if agent.is_project_scoped {
        write_agent_file(&path, agent)?;
        return Ok(path);
    }

    let mut pins = agent_pins::load_pins(config_dir);
    let pack_owned = path == config_dir.join("agents").join(format!("{}.md", agent.id))
        && super::pack::installed_packs(config_dir).iter().any(|lock| {
            lock.item(super::pack::PackItemKind::Agent, &agent.id)
                .is_some()
        });
    let on_disk = match pack_owned.then(|| load_agent_file(&path, false)) {
        Some(ConfigResult::Ok(on_disk)) => Some(on_disk),
        _ => None,
    };

    let Some(on_disk) = on_disk else {
        // Not something a pack ships: the file carries every key, and any pin
        // left from when it was (a retired pack item) would only shadow it.
        write_agent_file(&path, agent)?;
        if pins.get(&agent.id).is_some() {
            pins.set(&agent.id, AgentPin::default());
            agent_pins::save_pins(config_dir, &pins)?;
        }
        return Ok(path);
    };

    let (file_agent, pin) = split_pins(agent, &on_disk);
    if !same_definition(&file_agent, &on_disk) {
        write_agent_file(&path, &file_agent)?;
    }
    if pins.get(&agent.id).cloned().unwrap_or_default() != pin {
        pins.set(&agent.id, pin);
        agent_pins::save_pins(config_dir, &pins)?;
    }
    Ok(path)
}

/// Every path a save or delete of the agent at `primary_path` may have changed:
/// the file itself and, for a workspace agent, the pin document beside it.
pub fn agent_stage_paths(primary_path: &Path) -> Vec<PathBuf> {
    let mut paths = vec![primary_path.to_path_buf()];
    if let Some(config_dir) = primary_path.parent().and_then(Path::parent) {
        let pins = agent_pins::pins_path(config_dir);
        // Only an existing document: staging an absent, untracked path would
        // fail the whole commit.
        if pins.exists() {
            paths.push(pins);
        }
    }
    paths
}

/// Split a desired agent into what its file should hold and what it pins.
///
/// A pinnable key equal to the file's value pins nothing. A key the user
/// cleared while the file sets it cannot be expressed as a pin (absence means
/// "use the file"), so that one change is written to the file instead.
fn split_pins(desired: &FileAgent, on_disk: &FileAgent) -> (FileAgent, AgentPin) {
    fn pin<T: Clone + PartialEq>(
        desired: &Option<T>,
        file: &Option<T>,
        file_slot: &mut Option<T>,
    ) -> Option<T> {
        match (desired, file) {
            (desired, file) if desired == file => None,
            (None, Some(_)) => {
                *file_slot = None;
                None
            }
            (Some(value), _) => Some(value.clone()),
            (None, None) => None,
        }
    }

    // An empty allotment is the absent key: "use the workspace default".
    let allotment = |agent: &FileAgent| (!agent.models.is_empty()).then(|| agent.models.clone());

    let mut file_agent = desired.clone();
    file_agent.icon = on_disk.icon.clone();
    file_agent.models = on_disk.models.clone();
    let file_models = allotment(on_disk);
    let mut models_slot = file_models.clone();

    let pin = AgentPin {
        icon: pin(&desired.icon, &on_disk.icon, &mut file_agent.icon),
        models: pin(&allotment(desired), &file_models, &mut models_slot),
        ..AgentPin::default()
    };
    if models_slot.is_none() {
        file_agent.models.clear();
    }
    (file_agent, pin)
}

/// Whether two agents define the same thing, ignoring formatting the editors
/// normalize away (surrounding whitespace, an explicit default fence, an empty
/// versus absent deny-list). Anything this calls different forks a pack file.
fn same_definition(a: &FileAgent, b: &FileAgent) -> bool {
    let denied = |agent: &FileAgent| agent.disallowed_tools.clone().unwrap_or_default();
    a.name == b.name
        && a.description.trim() == b.description.trim()
        && a.prompt.trim() == b.prompt.trim()
        && a.tools == b.tools
        && a.models == b.models
        && a.fence.unwrap_or_default() == b.fence.unwrap_or_default()
        && denied(a) == denied(b)
        && a.skills == b.skills
        && a.hooks == b.hooks
        && a.icon == b.icon
        && a.bundles == b.bundles
        && a.seed_budget_tokens == b.seed_budget_tokens
        && a.seed_eras == b.seed_eras
}

fn resolve_save_path(
    config_dir: &Path,
    agent: &FileAgent,
    project_path: Option<&Path>,
) -> Result<PathBuf, String> {
    // Determine target path
    let path = if agent.file_path.as_os_str().is_empty() {
        // No existing path - determine from scope
        if agent.is_project_scoped {
            let proj_path = project_path.ok_or("Project path required for project-scoped agent")?;
            let dir = proj_path.join(".cairn").join("agents");
            std::fs::create_dir_all(&dir)
                .map_err(|e| format!("Failed to create project agents directory: {}", e))?;
            dir.join(format!("{}.md", agent.id))
        } else {
            let dir = config_dir.join("agents");
            std::fs::create_dir_all(&dir)
                .map_err(|e| format!("Failed to create agents directory: {}", e))?;
            dir.join(format!("{}.md", agent.id))
        }
    } else {
        // Use existing path, ensure parent directory exists
        if let Some(parent) = agent.file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create directory: {}", e))?;
        }
        agent.file_path.clone()
    };
    Ok(path)
}

fn write_agent_file(path: &Path, agent: &FileAgent) -> Result<(), String> {
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
        seed_budget_tokens: agent.seed_budget_tokens,
        seed_eras: agent.seed_eras,
    });

    std::fs::write(path, markdown).map_err(|e| format!("Failed to write agent file: {}", e))
}

/// Delete an agent file
pub fn delete_agent(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<(), String> {
    // Try project-scoped first if project specified
    if let Some(proj_path) = project_path {
        let path = proj_path
            .join(".cairn")
            .join("agents")
            .join(format!("{}.md", id));
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| format!("Failed to delete agent file: {}", e))?;
            return Ok(());
        }
    }

    // Try workspace-scoped
    let path = config_dir.join("agents").join(format!("{}.md", id));
    if path.exists() {
        std::fs::remove_file(&path).map_err(|e| format!("Failed to delete agent file: {}", e))?;
        super::pack::note_removed_item(config_dir, super::pack::PackItemKind::Agent, id);
        // Deleting is the user's decision about the whole agent; a pin left
        // behind would silently re-apply if the item were ever restored.
        let mut pins = agent_pins::load_pins(config_dir);
        if pins.get(id).is_some() {
            pins.set(id, AgentPin::default());
            agent_pins::save_pins(config_dir, &pins)?;
        }
    }

    Ok(())
}

/// List agents from Claude Code directories (for import)
///
/// Reads user-level agents from `~/.claude/agents/`.
/// If project_path is Some, also includes agents from `[project_path]/.claude/agents/`.
#[allow(dead_code)]
pub fn list_claude_agents(
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileAgent>>, String> {
    let mut results = vec![];

    // User-level: ~/.claude/agents/
    if let Some(home) = dirs::home_dir() {
        let user_dir = home.join(".claude").join("agents");
        if user_dir.exists() && user_dir.is_dir() {
            for entry in std::fs::read_dir(&user_dir)
                .map_err(|e| format!("Failed to read ~/.claude/agents: {}", e))?
            {
                let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
                let path = entry.path();

                // Skip directories and non-.md files
                if path.is_dir() || path.extension().and_then(|e| e.to_str()) != Some("md") {
                    continue;
                }

                results.push(load_agent_file(&path, false));
            }
        }
    }

    // Project-level: [project]/.claude/agents/
    if let Some(proj_path) = project_path {
        let proj_dir = proj_path.join(".claude").join("agents");
        if proj_dir.exists() && proj_dir.is_dir() {
            for entry in std::fs::read_dir(&proj_dir)
                .map_err(|e| format!("Failed to read project .claude/agents: {}", e))?
            {
                let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
                let path = entry.path();

                if path.extension().and_then(|e| e.to_str()) != Some("md") {
                    continue;
                }

                results.push(load_agent_file(&path, true));
            }
        }
    }

    Ok(results)
}

/// Load a single agent file
fn load_agent_file(path: &Path, is_project_scoped: bool) -> ConfigResult<FileAgent> {
    let id = match id_from_path(path) {
        Some(id) => id,
        None => {
            return ConfigResult::Err {
                path: path.to_path_buf(),
                error: "Could not determine agent ID from filename".to_string(),
            }
        }
    };

    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            return ConfigResult::Err {
                path: path.to_path_buf(),
                error: format!("Failed to read file: {}", e),
            }
        }
    };

    match parse_agent_markdown(&content) {
        Ok(parsed) => ConfigResult::Ok(FileAgent {
            id,
            name: parsed.name,
            description: parsed.description,
            prompt: parsed.prompt,
            tools: parsed.tools,
            models: parsed.models,
            fence: parsed.fence,
            disallowed_tools: parsed.disallowed_tools,
            skills: parsed.skills,
            hooks: parsed.hooks,
            icon: parsed.icon,
            bundles: parsed.bundles,
            seed_budget_tokens: parsed.seed_budget_tokens,
            seed_eras: parsed.seed_eras,
            is_project_scoped,
            file_path: path.to_path_buf(),
        }),
        Err(e) => ConfigResult::Err {
            path: path.to_path_buf(),
            error: e,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn get_agent_walks_project_then_workspace() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(project_dir.join(".cairn/agents")).unwrap();

        std::fs::write(
            config_dir.join("agents/shared.md"),
            "---\nname: Workspace\ndescription: workspace agent\ntools:
  - Read\n---\n\nWorkspace prompt.",
        )
        .unwrap();
        std::fs::write(
            project_dir.join(".cairn/agents/shared.md"),
            "---\nname: Project\ndescription: project agent\ntools:
  - Read\n---\n\nProject prompt.",
        )
        .unwrap();
        std::fs::write(
            config_dir.join("agents/workspace-only.md"),
            "---\nname: Workspace Only\ndescription: workspace only\ntools:
  - Read\n---\n\nWorkspace only prompt.",
        )
        .unwrap();

        let shared = get_agent(&config_dir, "shared", Some(&project_dir))
            .unwrap()
            .unwrap();
        assert_eq!(shared.name, "Project");
        assert!(shared.is_project_scoped);

        let workspace = get_agent(&config_dir, "workspace-only", Some(&project_dir))
            .unwrap()
            .unwrap();
        assert_eq!(workspace.name, "Workspace Only");
        assert!(!workspace.is_project_scoped);

        assert!(get_agent(&config_dir, "missing", Some(&project_dir))
            .unwrap()
            .is_none());
    }

    /// An agent no pack claims keeps every key in its own file, and saving it
    /// clears any pin left from when a pack shipped it.
    #[test]
    fn an_unclaimed_agent_keeps_its_pins_in_the_file() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path();
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::write(
            config_dir.join("agents/mine.md"),
            "---\nname: Mine\ndescription: d\ntools: Read\n---\n\nPrompt.",
        )
        .unwrap();
        let mut stale = agent_pins::AgentPins::default();
        stale.set(
            "mine",
            AgentPin {
                icon: Some("old".into()),
                ..Default::default()
            },
        );
        agent_pins::save_pins(config_dir, &stale).unwrap();

        let mut agent = get_agent(config_dir, "mine", None).unwrap().unwrap();
        assert_eq!(
            agent.icon.as_deref(),
            Some("old"),
            "the loader applies pins"
        );
        agent.models = vec![AllotmentEntry::Pool("lg".into())];
        agent.icon = None;
        save_agent(config_dir, &agent, None).unwrap();

        let text = std::fs::read_to_string(config_dir.join("agents/mine.md")).unwrap();
        assert!(text.contains("models: [lg]"), "{text}");
        assert!(agent_pins::load_pins(config_dir).get("mine").is_none());
        let reloaded = get_agent(config_dir, "mine", None).unwrap().unwrap();
        assert_eq!(reloaded.icon, None);
    }

    #[test]
    fn test_load_agent_roundtrip() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("test-agent.md");

        let content = r#"---
name: Test Agent
description: A test agent
tools:
  - Read
  - Write
models: [md, claude/sonnet]
---

You are a test agent.
"#;

        std::fs::write(&path, content).unwrap();

        let result = load_agent_file(&path, false);
        match result {
            ConfigResult::Ok(agent) => {
                assert_eq!(agent.id, "test-agent");
                assert_eq!(agent.name, "Test Agent");
                assert_eq!(agent.description, "A test agent");
                assert_eq!(agent.tools, vec!["Read", "Write"]);
                assert_eq!(
                    agent.models,
                    vec![
                        AllotmentEntry::Pool("md".into()),
                        AllotmentEntry::parse("claude/sonnet").unwrap()
                    ]
                );
                assert!(!agent.is_project_scoped);
                assert!(agent.prompt.contains("test agent"));
            }
            ConfigResult::Err { error, .. } => panic!("Failed to load: {}", error),
        }
    }
}
