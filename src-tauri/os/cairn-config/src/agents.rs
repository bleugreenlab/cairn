//! Agent configuration file reading.
//!
//! Agents are markdown files with YAML frontmatter, one per layer directory
//! (`agents/<id>.md` in a project's `.cairn/`, the workspace, or an installed
//! pack), resolved nearest-first through [`crate::library`].

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};

use crate::agent_markdown::{agent_to_markdown, parse_agent_markdown, AgentExportData};
use crate::agent_pins::{self, AgentPin};
use crate::allotment::AllotmentEntry;
use crate::library::{
    self, LibraryItem, LibraryKind, LibraryRef, LibraryRoots, Listed, Origin, ResolveError,
    Resolved,
};
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
    /// The layer this agent was read from, or the layer a new one is for.
    pub origin: Origin,
    /// Path to the source file
    pub file_path: PathBuf,
}

impl LibraryItem for FileAgent {
    fn library_id(&self) -> &str {
        &self.id
    }

    /// An agent's tool grant is always authority, even when empty, so every
    /// project agent is confirmed before it runs. Every field is named below,
    /// so one added to `FileAgent` does not compile until it is classified.
    fn authority(&self) -> Map<String, Value> {
        let FileAgent {
            // The name resolution keys it by.
            id: _,
            // Prose.
            name: _,
            description: _,
            prompt: _,
            tools,
            models,
            fence,
            disallowed_tools,
            skills,
            hooks,
            // Its picture in the UI.
            icon: _,
            // Which bundled sets list it: catalog metadata.
            bundles: _,
            seed_budget_tokens,
            seed_eras,
            origin: _,
            file_path: _,
        } = self;
        let mut fields = Map::new();
        fields.insert("tools".into(), json!(tools));
        if let Some(denied) = disallowed_tools {
            fields.insert("disallowedTools".into(), json!(denied));
        }
        if let Some(fence) = fence {
            fields.insert(
                "fence".into(),
                serde_json::to_value(fence).unwrap_or(Value::Null),
            );
        }
        if let Some(skills) = skills {
            fields.insert("skills".into(), json!(skills));
        }
        if !models.is_empty() {
            fields.insert(
                "models".into(),
                serde_json::to_value(models).unwrap_or(Value::Null),
            );
        }
        if let Some(hooks) = hooks {
            fields.insert("hooks".into(), hooks.clone());
        }
        // How much earlier history is seeded into its context: what it spends.
        if let Some(tokens) = seed_budget_tokens {
            fields.insert("seedBudgetTokens".into(), json!(tokens));
        }
        if let Some(eras) = seed_eras {
            fields.insert("seedEras".into(), json!(eras));
        }
        fields
    }
}

impl FileAgent {
    pub fn is_project_scoped(&self) -> bool {
        self.origin.is_project()
    }

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
            workspace_id: (!self.origin.is_project()).then(|| "workspace".to_string()),
            project_id: self.origin.is_project().then(|| project_id.to_string()),
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

fn load_agent_in(dir: &Path, id: &str, origin: &Origin) -> Option<ConfigResult<FileAgent>> {
    let path = dir.join(format!("{id}.md"));
    path.is_file()
        .then(|| load_agent_file(&path, origin.clone()))
}

fn enumerate_agents(
    dir: &Path,
    origin: &Origin,
) -> Result<Vec<(String, ConfigResult<FileAgent>)>, String> {
    let mut results = Vec::new();
    for entry in
        std::fs::read_dir(dir).map_err(|e| format!("Failed to read agents directory: {e}"))?
    {
        let path = entry
            .map_err(|e| format!("Failed to read directory entry: {e}"))?
            .path();
        if path.is_dir() || path.extension().and_then(|e| e.to_str()) != Some("md") {
            continue;
        }
        if let Some(id) = id_from_path(&path) {
            results.push((id, load_agent_file(&path, origin.clone())));
        }
    }
    Ok(results)
}

/// Every agent definition across the layers, with its origin and what it
/// shadows, the user's pins applied (tenet 1133: over whichever layer).
pub fn list_agent_rows(roots: &LibraryRoots) -> Result<Vec<Listed<FileAgent>>, String> {
    let pins = agent_pins::load_pins(&roots.workspace);
    Ok(roots
        .list(LibraryKind::Agent, enumerate_agents)?
        .into_iter()
        .map(|row| {
            row.map(|mut agent| {
                apply_pin(&pins, &mut agent);
                agent
            })
        })
        .collect())
}

/// The agents in force, one per name.
pub fn list_agents(roots: &LibraryRoots) -> Result<Vec<ConfigResult<FileAgent>>, String> {
    Ok(library::active(list_agent_rows(roots)?))
}

fn apply_pin(pins: &agent_pins::AgentPins, agent: &mut FileAgent) {
    if let Some(pin) = pins.get(&agent.id) {
        pin.apply(agent);
    }
}

/// Resolve an agent reference (a bare id or `cairn://packs/<pack>/agents/<id>`)
/// nearest-first, with the user's pins applied.
pub fn resolve_agent(
    roots: &LibraryRoots,
    reference: &str,
) -> Result<Resolved<FileAgent>, ResolveError> {
    let reference = LibraryRef::parse(LibraryKind::Agent, reference).map_err(|error| {
        ResolveError::Invalid {
            path: PathBuf::new(),
            error,
            origin: None,
        }
    })?;
    let mut resolved = roots.resolve(LibraryKind::Agent, &reference, load_agent_in)?;
    apply_pin(&agent_pins::load_pins(&roots.workspace), &mut resolved.item);
    Ok(resolved)
}

/// The agent a reference resolves to, `None` when nothing defines it. Any
/// other failure (untrusted, unconfirmed, a collision) is an error naming why.
pub fn get_agent(roots: &LibraryRoots, id: &str) -> Result<Option<FileAgent>, String> {
    match resolve_agent(roots, id) {
        Ok(resolved) => Ok(Some(resolved.item)),
        Err(error) if error.is_not_found() => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// A pack agent loaded as its pack ships it, with no pins.
fn pack_version(roots: &LibraryRoots, pack: &str, id: &str) -> Result<FileAgent, String> {
    let reference = LibraryRef::Pack {
        pack: pack.to_string(),
        name: id.to_string(),
    };
    Ok(roots
        .resolve(LibraryKind::Agent, &reference, load_agent_in)?
        .item)
}

/// Save an agent into a definite layer.
///
/// `layer` names where to write; `None` writes back where the agent was read
/// from (`agent.origin`). A pack's own agent is never edited: saving one with
/// no layer may change only its pins (`icon`, `models`), which go to the pin
/// document, and anything else is refused. Saving one into the workspace or a
/// project writes a definition there, in front of the pack's, and keeps its
/// pinnable keys in the pin document. A workspace or project agent's file
/// carries every key, and any pin for it is cleared so the file is what
/// applies. Returns the path written; callers stage [`agent_stage_paths`].
pub fn save_agent(
    roots: &LibraryRoots,
    agent: &FileAgent,
    layer: Option<&Origin>,
) -> Result<PathBuf, String> {
    let target = layer.unwrap_or(&agent.origin);
    let mut pins = agent_pins::load_pins(&roots.workspace);

    if let Origin::Pack { id: pack } = target {
        let shipped = pack_version(roots, pack, &agent.id)?;
        let (file_agent, pin) = split_pins(agent, &shipped);
        if !same_definition(&file_agent, &shipped) {
            return Err(library::pack_edit_refusal(
                LibraryKind::Agent,
                &agent.id,
                pack,
            ));
        }
        if pins.get(&agent.id).cloned().unwrap_or_default() != pin {
            pins.set(&agent.id, pin);
        }
        return agent_pins::save_pins(&roots.workspace, &pins);
    }

    // Written back where it was read from unless a layer is named.
    let path = if layer.is_none() && !agent.file_path.as_os_str().is_empty() {
        agent.file_path.clone()
    } else {
        roots
            .layer_dir(LibraryKind::Agent, target)
            .ok_or("Project path required for project-scoped agent")?
            .join(format!("{}.md", agent.id))
    };
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("Failed to create agents directory: {e}"))?;
    }

    if let Origin::Pack { id: pack } = &agent.origin {
        // A definition placed in front of a pack's: its pins stay pins.
        let shipped = pack_version(roots, pack, &agent.id)?;
        let (mut file_agent, pin) = split_pins(agent, &shipped);
        file_agent.origin = target.clone();
        write_agent_file(&path, &file_agent)?;
        if pins.get(&agent.id).cloned().unwrap_or_default() != pin {
            pins.set(&agent.id, pin);
            agent_pins::save_pins(&roots.workspace, &pins)?;
        }
        return Ok(path);
    }

    write_agent_file(&path, agent)?;
    if pins.get(&agent.id).is_some() {
        pins.set(&agent.id, AgentPin::default());
        agent_pins::save_pins(&roots.workspace, &pins)?;
    }
    Ok(path)
}

/// Every path a save or delete of an agent may have changed: the file (or the
/// lock recording a pack removal) and the workspace pin document.
pub fn agent_stage_paths(roots: &LibraryRoots, primary_path: &Path) -> Vec<PathBuf> {
    let mut paths = vec![primary_path.to_path_buf()];
    let pins = agent_pins::pins_path(&roots.workspace);
    // Only an existing document: staging an absent, untracked path would fail
    // the whole commit.
    if pins.exists() && pins != primary_path {
        paths.push(pins);
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
/// versus absent deny-list). Anything this calls different is more than a pin.
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

/// Delete the agent `id` resolves to, or the one in `layer` when named.
///
/// A workspace or project definition is deleted outright and nothing else is
/// recorded; whatever it shadowed resolves again. A pack's own agent is
/// recorded as removed from its pack (tenet 1121), since there is no local
/// file to delete. Either way the user's pin for the agent is cleared. Returns
/// the path that changed (`None` when nothing defined it), for the commit.
pub fn delete_agent(
    roots: &LibraryRoots,
    id: &str,
    layer: Option<&Origin>,
) -> Result<Option<PathBuf>, String> {
    let (origin, path) = match layer {
        Some(origin) => {
            let Some(dir) = roots.layer_dir(LibraryKind::Agent, origin) else {
                return Err(format!("No {origin} layer to delete agent `{id}` from"));
            };
            let path = dir.join(format!("{id}.md"));
            if !path.is_file() {
                return Ok(None);
            }
            (origin.clone(), path)
        }
        None => match resolve_agent(roots, id) {
            Ok(resolved) => (resolved.origin, resolved.item.file_path),
            Err(ResolveError::Invalid {
                path,
                origin: Some(origin),
                ..
            }) => (origin, path),
            Err(error) if error.is_not_found() => return Ok(None),
            Err(error) => return Err(error.to_string()),
        },
    };
    let changed = library::remove_definition(roots, LibraryKind::Agent, id, &origin, &path)?;
    // Deleting is the user's decision about the whole agent; a pin left
    // behind would silently re-apply to whatever resolves next.
    let mut pins = agent_pins::load_pins(&roots.workspace);
    if pins.get(id).is_some() {
        pins.set(id, AgentPin::default());
        agent_pins::save_pins(&roots.workspace, &pins)?;
    }
    Ok(Some(changed))
}

/// Load a single agent file
fn load_agent_file(path: &Path, origin: Origin) -> ConfigResult<FileAgent> {
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
            origin,
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
    use crate::library::test_support::*;
    use tempfile::tempdir;

    fn agent_body(name: &str, tools: &str) -> String {
        format!("---\nname: {name}\ndescription: d\ntools: {tools}\n---\n\n{name} prompt.")
    }

    #[test]
    fn get_agent_walks_project_then_workspace() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path().join("config");
        let project_dir = temp.path().join("project");
        std::fs::create_dir_all(&config_dir).unwrap();
        write_item(
            &config_dir,
            LibraryKind::Agent,
            "shared",
            &agent_body("Workspace", "Read"),
        );
        let project_file = write_item(
            &project_root(&project_dir),
            LibraryKind::Agent,
            "shared",
            &agent_body("Project", "Read"),
        );
        write_item(
            &config_dir,
            LibraryKind::Agent,
            "workspace-only",
            &agent_body("Workspace Only", "Read"),
        );
        let shared_tools = get_agent(
            &LibraryRoots::workspace(&config_dir).with_project(project(
                &project_dir,
                "demo",
                trusted(&[]),
            )),
            "shared",
        )
        .unwrap_err();
        assert!(
            shared_tools.contains("whose fence and tools the operator has not confirmed"),
            "{shared_tools}"
        );

        let ConfigResult::Ok(defined) = load_agent_file(&project_file, Origin::Workspace) else {
            panic!("the project agent parses");
        };
        let confirmed = defined.authority();
        assert!(confirmed.contains_key("tools") && confirmed.contains_key("fence"));
        let roots = LibraryRoots::workspace(&config_dir).with_project(project(
            &project_dir,
            "demo",
            trusted(&[(LibraryKind::Agent, "shared", confirmed)]),
        ));

        let shared = get_agent(&roots, "shared").unwrap().unwrap();
        assert_eq!(shared.name, "Project");
        assert!(shared.is_project_scoped());

        let workspace = get_agent(&roots, "workspace-only").unwrap().unwrap();
        assert_eq!(workspace.name, "Workspace Only");
        assert_eq!(workspace.origin, Origin::Workspace);

        assert!(get_agent(&roots, "missing").unwrap().is_none());
    }

    /// A workspace agent keeps every key in its own file, and saving it clears
    /// any pin so the file is what applies.
    #[test]
    fn a_workspace_agent_keeps_its_pins_in_the_file() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path();
        write_item(
            config_dir,
            LibraryKind::Agent,
            "mine",
            &agent_body("Mine", "Read"),
        );
        let mut stale = agent_pins::AgentPins::default();
        stale.set(
            "mine",
            AgentPin {
                icon: Some("old".into()),
                ..Default::default()
            },
        );
        agent_pins::save_pins(config_dir, &stale).unwrap();
        let roots = LibraryRoots::workspace(config_dir);

        let mut agent = get_agent(&roots, "mine").unwrap().unwrap();
        assert_eq!(
            agent.icon.as_deref(),
            Some("old"),
            "the loader applies pins"
        );
        agent.models = vec![AllotmentEntry::Pool("lg".into())];
        agent.icon = None;
        save_agent(&roots, &agent, None).unwrap();

        let text = std::fs::read_to_string(config_dir.join("agents/mine.md")).unwrap();
        assert!(text.contains("models: [lg]"), "{text}");
        assert!(agent_pins::load_pins(config_dir).get("mine").is_none());
        let reloaded = get_agent(&roots, "mine").unwrap().unwrap();
        assert_eq!(reloaded.icon, None);
    }

    /// Editing a pack's agent: a pin-only change touches only the pin
    /// document; anything more is refused unless it names a layer, which puts
    /// a definition in front of the pack's and leaves the pack's file alone.
    #[test]
    fn a_pack_agent_is_pinned_or_shadowed_but_never_edited() {
        let temp = tempdir().unwrap();
        let config_dir = temp.path();
        install_pack_lock(config_dir, "core", &[]);
        let shipped = write_item(
            &pack_root(config_dir, "core"),
            LibraryKind::Agent,
            "build",
            &agent_body("Build", "Read"),
        );
        let before = std::fs::read(&shipped).unwrap();
        let roots = LibraryRoots::workspace(config_dir);

        let mut agent = get_agent(&roots, "build").unwrap().unwrap();
        assert_eq!(agent.origin, Origin::Pack { id: "core".into() });
        agent.icon = Some("hammer".into());
        save_agent(&roots, &agent, None).unwrap();
        assert_eq!(std::fs::read(&shipped).unwrap(), before);
        assert_eq!(
            get_agent(&roots, "build").unwrap().unwrap().icon.as_deref(),
            Some("hammer")
        );

        agent.prompt = "Changed.".into();
        let refusal = save_agent(&roots, &agent, None).unwrap_err();
        assert!(refusal.contains("pack items are never edited"), "{refusal}");

        let written = save_agent(&roots, &agent, Some(&Origin::Workspace)).unwrap();
        assert_eq!(written, config_dir.join("agents/build.md"));
        assert_eq!(std::fs::read(&shipped).unwrap(), before);
        let resolved = resolve_agent(&roots, "build").unwrap();
        assert_eq!(resolved.origin, Origin::Workspace);
        assert_eq!(resolved.shadows, vec![Origin::Pack { id: "core".into() }]);
        assert_eq!(resolved.item.prompt, "Changed.");
        assert_eq!(
            resolved.item.icon.as_deref(),
            Some("hammer"),
            "pins still apply"
        );

        // Deleting the workspace definition is plain deletion: the pack's
        // agent resolves again and nothing is recorded against the pack.
        delete_agent(&roots, "build", None).unwrap();
        assert!(!config_dir.join("agents/build.md").exists());
        let roots = LibraryRoots::workspace(config_dir);
        assert_eq!(
            resolve_agent(&roots, "build").unwrap().origin,
            Origin::Pack { id: "core".into() }
        );
        assert!(crate::pack::lock::read_lock(config_dir, "core")
            .unwrap()
            .removed
            .is_empty());
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

        let result = load_agent_file(&path, Origin::Workspace);
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
                assert_eq!(agent.origin, Origin::Workspace);
                assert!(agent.prompt.contains("test agent"));
            }
            ConfigResult::Err { error, .. } => panic!("Failed to load: {}", error),
        }
    }
}
