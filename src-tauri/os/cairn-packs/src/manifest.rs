//! Pack manifests: the source-side description of an installable resource pack.
//!
//! A **pack** is a directory holding Cairn resources — skills, agents, recipes,
//! workflows, response templates, machine services, and MCP server definitions
//! — plus an optional manifest naming it. Packs are the unit of *offering and installing*; they are
//! deliberately NOT a namespace. Installation copies a pack's contents into the
//! flat `~/.cairn` layout (`agents/`, `recipes/`, `skills/`, …) that discovery
//! and global ids already assume, so repacking the shipped tree is a pure source
//! reorganization that existing workspaces never see.
//!
//! ## Manifest resolution
//!
//! [`load`] resolves a directory into a [`PackManifest`] by trying, in order:
//!
//! 1. `cairn-pack.yaml` — the native manifest.
//! 2. `.claude-plugin/plugin.json` — a Claude Code plugin, normalized. Cairn
//!    adopts that ecosystem's *directory conventions* verbatim (`skills/<name>/
//!    SKILL.md`, `agents/*.md`, an `mcpServers` map) but keeps its own manifest,
//!    because recipes, workflows, and OAuth config have no slot in their schema
//!    and that schema is version-gated and actively churning.
//! 3. `SKILL.md` at the root — the degenerate single-skill pack.
//! 4. Conventional content directories with no manifest at all.
//!
//! Unrecognized manifest keys are ignored rather than rejected, matching Claude
//! Code's own tolerance, so one directory can carry both manifests and satisfy
//! both tools.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::lock::PackSourceKind;

/// The native manifest filename, at the pack root.
pub const MANIFEST_FILE: &str = "cairn-pack.yaml";
/// Where a pack's machine services live, in its source and in the workspace.
pub const SERVICES_DIR: &str = "services";
/// The one file a pack's service directory holds: its declaration.
pub const SERVICE_MANIFEST_FILE: &str = "service.yaml";
/// Claude Code's plugin manifest, relative to the pack root.
pub const CLAUDE_PLUGIN_MANIFEST: &str = ".claude-plugin/plugin.json";
/// A pack's MCP server definitions, native form.
pub const PACK_MCP_FILE: &str = "mcp.yaml";
/// A pack's MCP server definitions, Claude Code form.
pub const CLAUDE_MCP_FILE: &str = ".mcp.json";

/// Which manifest shape a pack directory was read from. Recorded on the install
/// lock so a later update knows how to re-read the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PackFormat {
    /// `cairn-pack.yaml` at the root.
    Cairn,
    /// Agent Plugins 1.0.0 `plugin.json` at the root.
    AgentPlugin,
    /// `.claude-plugin/plugin.json` at the root.
    ClaudeCode,
    /// A bare `SKILL.md` at the root — one skill, no manifest.
    BareSkill,
    /// Conventional content directories with no manifest.
    Conventional,
}

/// The kinds of content a pack can carry. Every kind but [`PackItemKind::Mcp`]
/// is a file or directory discovered by convention under the pack root and
/// installed to the same relative path in the workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PackItemKind {
    Agent,
    Credential,
    Mcp,
    Provider,
    Recipe,
    Response,
    Route,
    Service,
    Skill,
    Workflow,
}

impl PackItemKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PackItemKind::Agent => "agent",
            PackItemKind::Credential => "credential",
            PackItemKind::Mcp => "mcp",
            PackItemKind::Provider => "provider",
            PackItemKind::Recipe => "recipe",
            PackItemKind::Response => "response",
            PackItemKind::Route => "route",
            PackItemKind::Service => "service",
            PackItemKind::Skill => "skill",
            PackItemKind::Workflow => "workflow",
        }
    }

    /// The inverse of [`Self::as_str`], for a kind that arrives as text from a
    /// resource payload or a settings invoke.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "agent" => Some(PackItemKind::Agent),
            "credential" => Some(PackItemKind::Credential),
            "mcp" => Some(PackItemKind::Mcp),
            "provider" => Some(PackItemKind::Provider),
            "recipe" => Some(PackItemKind::Recipe),
            "response" => Some(PackItemKind::Response),
            "route" => Some(PackItemKind::Route),
            "service" => Some(PackItemKind::Service),
            "skill" => Some(PackItemKind::Skill),
            "workflow" => Some(PackItemKind::Workflow),
            _ => None,
        }
    }

    /// Every kind, for error text that enumerates the alternatives.
    pub const ALL: [PackItemKind; 10] = [
        PackItemKind::Agent,
        PackItemKind::Credential,
        PackItemKind::Mcp,
        PackItemKind::Provider,
        PackItemKind::Recipe,
        PackItemKind::Response,
        PackItemKind::Route,
        PackItemKind::Service,
        PackItemKind::Skill,
        PackItemKind::Workflow,
    ];

    /// Whether this kind is installed into the pack's own `packs/<id>/` layer
    /// rather than the flat workspace. Every provider pack ships the same file
    /// names (`providers/web-search.yaml`, `credentials/api-key.yaml`), so the
    /// pack id is their namespace, as it is for `packs/<id>/mcp.yaml`.
    pub fn in_pack_layer(self) -> bool {
        matches!(self, PackItemKind::Credential | PackItemKind::Provider)
    }
}

/// What installing an item lets happen on this machine.
///
/// Derived from the item, never declared by it, so a library index cannot
/// describe a stdio server as inert. The Library shows it, and a library index
/// update that changes an [`ItemRuns::Endpoint`] or [`ItemRuns::Here`] item waits
/// for the operator's click instead of being applied in the background.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum ItemRuns {
    /// Text an agent reads: skills, agents, recipes, responses, routes. A
    /// skill's `scripts/` stay content, because running them is an agent act.
    #[default]
    Content,
    /// Something that reaches an external host: a remote MCP server
    /// (`http`/`sse`) the workspace connects to, or a web-service provider and
    /// its credential, which send a stored secret to the host they name.
    Endpoint,
    /// Something that executes on this machine: a stdio MCP server, a workflow,
    /// whose `main.ts` runs, or a machine service.
    Here,
}

impl ItemRuns {
    /// `mcp` is an MCP item's own definition, or the server a provider binds.
    /// An HTTP provider binds none and sends its credential to the host it
    /// names; a provider bound to a stdio server spawns that server.
    pub fn classify(
        kind: PackItemKind,
        mcp: Option<&crate::server_config::McpServerConfig>,
    ) -> Self {
        let remote = |config: &crate::server_config::McpServerConfig| {
            matches!(config.transport.as_str(), "http" | "sse")
        };
        match kind {
            PackItemKind::Workflow | PackItemKind::Service => ItemRuns::Here,
            PackItemKind::Credential => ItemRuns::Endpoint,
            PackItemKind::Provider => match mcp {
                Some(server) if !remote(server) => ItemRuns::Here,
                _ => ItemRuns::Endpoint,
            },
            // An MCP item with no readable definition is assumed to spawn.
            PackItemKind::Mcp => match mcp {
                Some(server) if remote(server) => ItemRuns::Endpoint,
                _ => ItemRuns::Here,
            },
            PackItemKind::Agent
            | PackItemKind::Recipe
            | PackItemKind::Response
            | PackItemKind::Route
            | PackItemKind::Skill => ItemRuns::Content,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ItemRuns::Content => "content",
            ItemRuns::Endpoint => "endpoint",
            ItemRuns::Here => "here",
        }
    }

    /// Whether a change to an item of this class changes what runs.
    pub fn is_runnable(self) -> bool {
        self != ItemRuns::Content
    }
}

/// Why a pack from anywhere but the app bundle does not bring its machine
/// services with it. A service declaration names artifact URLs, hashes and a
/// command, and only the app's own bundle is trusted to declare one, so
/// materializing a remote one would be a catalog row no machine can ever run.
pub const NON_BUNDLED_SERVICES_NOTE: &str =
    "machine services from a remote or local source are not supported yet";

/// One installable unit inside a pack.
///
/// `path` is the workspace-relative destination (`skills/matlab`,
/// `agents/build.md`) and, for every file-backed kind, is also the pack-relative
/// source path — that identity is what lets the sync copy a pack subtree into
/// the flat workspace layout with no rewriting. An MCP item has no such path:
/// its definition lives in the pack's `mcp.yaml`, so `path` is `None` and `id`
/// is the server name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackItem {
    pub kind: PackItemKind,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

impl PackItem {
    pub fn file(kind: PackItemKind, id: impl Into<String>, path: impl Into<String>) -> Self {
        PackItem {
            kind,
            id: id.into(),
            path: Some(path.into()),
        }
    }

    pub fn mcp(name: impl Into<String>) -> Self {
        PackItem {
            kind: PackItemKind::Mcp,
            id: name.into(),
            path: None,
        }
    }
}

/// Author metadata. Cairn's native form is an object; Claude Code allows either
/// a bare string or an object, and both normalize to this.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackAuthor {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// A resolved pack: its identity plus the root it was read from. Contents are
/// discovered from the root by convention ([`PackManifest::items`]) rather than
/// being indexed in the manifest, so a manifest-less directory installs.
#[derive(Debug, Clone, PartialEq)]
pub struct PackManifest {
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: Option<PackAuthor>,
    pub homepage: Option<String>,
    pub license: Option<String>,
    pub keywords: Vec<String>,
    /// Whether this pack installs on a fresh workspace.
    pub default: bool,
    /// Workspace-relative paths this pack no longer ships. The sync walks the
    /// SOURCE tree, so deleting a file from a pack leaves every existing
    /// workspace holding a stale copy; listing it here removes it when it is
    /// still pack-owned.
    pub retired: Vec<String>,
    /// Machine services this pack ships, by id. Contents are otherwise
    /// discovered, but a service only ever runs from the app bundle's own
    /// declaration, so a pack names the ones it ships in its manifest, which
    /// every build carries: a build whose bundle left a declaration out then
    /// says so ([`Self::missing_services`]) instead of reading as a pack that
    /// never had it.
    pub services: Vec<String>,
    pub format: PackFormat,
    /// Ingest warnings surfaced on the catalog — components of a foreign pack
    /// format that Cairn has no equivalent for and skipped.
    pub notes: Vec<String>,
    pub root: PathBuf,
}

/// The native manifest as written on disk. Unknown keys are ignored.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawManifest {
    id: Option<String>,
    name: Option<String>,
    version: Option<String>,
    description: Option<String>,
    author: Option<PackAuthor>,
    homepage: Option<String>,
    license: Option<String>,
    #[serde(default)]
    keywords: Vec<String>,
    #[serde(default)]
    default: bool,
    #[serde(default)]
    retired: Vec<String>,
    #[serde(default)]
    services: Vec<String>,
}

/// Claude Code plugin components Cairn has no equivalent for. Their presence is
/// recorded as a note so a user installing a plugin can see what was skipped
/// rather than silently losing it.
const CLAUDE_UNSUPPORTED: &[&str] = &[
    "commands",
    "hooks",
    "output-styles",
    "themes",
    "monitors",
    "bin",
];

/// Resolve `dir` into a pack manifest. Errors only when the directory is not a
/// pack at all — no manifest and no conventional content.
pub fn load(dir: &Path) -> Result<PackManifest, String> {
    let fallback_id = dir_id(dir)?;

    let native = dir.join(MANIFEST_FILE);
    if native.is_file() {
        let text = std::fs::read_to_string(&native)
            .map_err(|e| format!("Failed to read {MANIFEST_FILE} in {dir:?}: {e}"))?;
        let raw: RawManifest = serde_yaml::from_str(&text)
            .map_err(|e| format!("Failed to parse {MANIFEST_FILE} in {dir:?}: {e}"))?;
        return Ok(from_raw(
            raw,
            &fallback_id,
            PackFormat::Cairn,
            Vec::new(),
            dir,
        ));
    }

    let claude = dir.join(CLAUDE_PLUGIN_MANIFEST);
    if claude.is_file() {
        return load_claude_plugin(dir, &claude, &fallback_id);
    }

    if dir.join("SKILL.md").is_file() {
        return Ok(from_raw(
            RawManifest::default(),
            &fallback_id,
            PackFormat::BareSkill,
            Vec::new(),
            dir,
        ));
    }

    if has_conventional_content(dir) {
        return Ok(from_raw(
            RawManifest::default(),
            &fallback_id,
            PackFormat::Conventional,
            Vec::new(),
            dir,
        ));
    }

    Err(format!(
        "{dir:?} is not a pack: expected {MANIFEST_FILE}, {CLAUDE_PLUGIN_MANIFEST}, a SKILL.md, \
         or one of the conventional content directories (agents, recipes, responses, routes, \
         services, skills, workflows)"
    ))
}

fn load_claude_plugin(
    dir: &Path,
    manifest_path: &Path,
    fallback_id: &str,
) -> Result<PackManifest, String> {
    let text = std::fs::read_to_string(manifest_path)
        .map_err(|e| format!("Failed to read {CLAUDE_PLUGIN_MANIFEST} in {dir:?}: {e}"))?;
    let value: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| format!("Failed to parse {CLAUDE_PLUGIN_MANIFEST} in {dir:?}: {e}"))?;

    let string = |key: &str| {
        value
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };

    let raw = RawManifest {
        id: string("name"),
        // Claude Code's `displayName` is the human label; `name` is the id.
        name: string("displayName").or_else(|| string("name")),
        version: string("version"),
        description: string("description"),
        author: claude_author(value.get("author")),
        homepage: string("homepage").or_else(|| string("repository")),
        license: string("license"),
        keywords: value
            .get("keywords")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        // A foreign pack is never part of Cairn's default set.
        default: false,
        retired: Vec::new(),
        services: Vec::new(),
    };

    let mut notes = Vec::new();
    for component in CLAUDE_UNSUPPORTED {
        if dir.join(component).exists() || dir.join(format!("{component}.json")).is_file() {
            notes.push(format!(
                "ignored claude-code component: {component} (no Cairn equivalent)"
            ));
        }
    }

    Ok(from_raw(
        raw,
        fallback_id,
        PackFormat::ClaudeCode,
        notes,
        dir,
    ))
}

/// Claude Code's `author` is either a bare string or an object.
fn claude_author(value: Option<&serde_json::Value>) -> Option<PackAuthor> {
    match value {
        Some(serde_json::Value::String(name)) => Some(PackAuthor {
            name: Some(name.clone()),
            ..Default::default()
        }),
        Some(object @ serde_json::Value::Object(_)) => serde_json::from_value(object.clone()).ok(),
        _ => None,
    }
}

fn from_raw(
    raw: RawManifest,
    fallback_id: &str,
    format: PackFormat,
    notes: Vec<String>,
    root: &Path,
) -> PackManifest {
    let id = raw
        .id
        .map(|value| normalize_id(&value))
        .unwrap_or_else(|| fallback_id.to_string());
    let name = raw.name.unwrap_or_else(|| id.clone());
    PackManifest {
        name,
        version: raw.version.unwrap_or_else(|| "0.0.0".to_string()),
        description: raw.description.unwrap_or_default(),
        author: raw.author,
        homepage: raw.homepage,
        license: raw.license,
        keywords: raw.keywords,
        default: raw.default,
        retired: raw.retired,
        services: raw.services,
        format,
        notes,
        root: root.to_path_buf(),
        id,
    }
}

/// Pack ids are kebab-case so they can double as `bundles:` names (which the
/// contextual-selection config normalizes the same way).
pub fn normalize_id(value: &str) -> String {
    let lowered = value.trim().to_ascii_lowercase();
    let mapped: String = lowered
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                c
            } else {
                '-'
            }
        })
        .collect();
    mapped.trim_matches('-').to_string()
}

fn dir_id(dir: &Path) -> Result<String, String> {
    let raw = dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("Cannot derive a pack id from {dir:?}"))?;
    let id = normalize_id(raw);
    if id.is_empty() {
        return Err(format!("Cannot derive a pack id from {dir:?}"));
    }
    Ok(id)
}

fn has_conventional_content(dir: &Path) -> bool {
    super::CONTENT_DIRS
        .iter()
        .chain(super::PACK_LAYER_DIRS.iter())
        .any(|name| dir.join(name).is_dir())
        || dir.join(PACK_MCP_FILE).is_file()
        || dir.join(CLAUDE_MCP_FILE).is_file()
}

impl PackManifest {
    /// The pack's installable contents, discovered from its root by convention.
    ///
    /// A bare-skill pack has no `skills/` directory — its root *is* the skill —
    /// so it reports the single skill it holds. Fetch stages such a tree into
    /// the conventional layout before installing, keeping the sync's
    /// source-path-equals-destination-path invariant intact everywhere else.
    pub fn items(&self) -> Vec<PackItem> {
        if self.format == PackFormat::BareSkill {
            return vec![PackItem::file(
                PackItemKind::Skill,
                &self.id,
                format!("skills/{}", self.id),
            )];
        }

        let mut items = discover_items(&self.root);
        items.extend(self.mcp_server_names().into_iter().map(PackItem::mcp));
        items
    }

    /// The services [`Self::services`] names that this tree carries no
    /// declaration for: a build whose bundle omitted the file.
    pub fn missing_services(&self) -> Vec<String> {
        self.services
            .iter()
            .filter(|id| {
                !self
                    .root
                    .join(SERVICES_DIR)
                    .join(id.as_str())
                    .join(SERVICE_MANIFEST_FILE)
                    .is_file()
            })
            .cloned()
            .collect()
    }

    /// What installing this pack from a source of `kind` puts in the workspace:
    /// [`Self::items`], less the machine services only the app bundle may carry.
    pub fn items_from(&self, kind: PackSourceKind) -> Vec<PackItem> {
        let items = self.items();
        if kind == PackSourceKind::Bundled {
            return items;
        }
        items
            .into_iter()
            .filter(|item| item.kind != PackItemKind::Service)
            .collect()
    }

    /// [`Self::notes`] for an install from a source of `kind`, saying what
    /// [`Self::items_from`] left out.
    pub fn notes_from(&self, kind: PackSourceKind) -> Vec<String> {
        let mut notes = self.notes.clone();
        if kind != PackSourceKind::Bundled {
            let skipped: Vec<String> = self
                .items()
                .into_iter()
                .filter(|item| item.kind == PackItemKind::Service)
                .map(|item| item.id)
                .collect();
            if !skipped.is_empty() {
                notes.push(format!(
                    "{NON_BUNDLED_SERVICES_NOTE}; not installed: {}",
                    skipped.join(", ")
                ));
            }
        }
        notes
    }

    /// Path to this pack's MCP definitions in its source tree, if it ships any.
    pub fn mcp_source(&self) -> Option<PathBuf> {
        let native = self.root.join(PACK_MCP_FILE);
        if native.is_file() {
            return Some(native);
        }
        let claude = self.root.join(CLAUDE_MCP_FILE);
        if claude.is_file() {
            return Some(claude);
        }
        None
    }

    /// Server names declared by this pack, sorted. Both the native `mcp.yaml`
    /// and Claude Code's `.mcp.json` carry the same `mcpServers` map, and
    /// `serde_yaml` reads JSON, so one parse covers both.
    pub fn mcp_server_names(&self) -> Vec<String> {
        let Some(path) = self.mcp_source() else {
            return Vec::new();
        };
        match super::mcp::parse_pack_mcp_file(&path) {
            Ok(file) => {
                let mut names: Vec<String> = file.servers.into_keys().collect();
                names.sort();
                names
            }
            Err(error) => {
                log::warn!("Pack `{}` has an unreadable {path:?}: {error}", self.id);
                Vec::new()
            }
        }
    }
}

/// Walk a pack root's conventional content directories and report what it ships.
/// File-backed items only; MCP servers come from the manifest's `mcp.yaml`.
pub fn discover_items(root: &Path) -> Vec<PackItem> {
    let mut items = Vec::new();
    for dir_name in super::CONTENT_DIRS
        .into_iter()
        .chain(super::PACK_LAYER_DIRS)
    {
        let dir = root.join(dir_name);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        let mut paths: Vec<PathBuf> = entries
            .filter_map(|entry| entry.ok())
            .map(|e| e.path())
            .collect();
        paths.sort();
        for path in paths {
            let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let rel = format!("{dir_name}/{file_name}");
            match dir_name {
                "skills" => {
                    if path.is_dir() && path.join("SKILL.md").is_file() {
                        items.push(PackItem::file(PackItemKind::Skill, file_name, rel));
                    }
                }
                "workflows" => {
                    if path.is_dir() && path.join("workflow.yaml").is_file() {
                        items.push(PackItem::file(PackItemKind::Workflow, file_name, rel));
                    }
                }
                "services" => {
                    if path.is_dir() && path.join(SERVICE_MANIFEST_FILE).is_file() {
                        items.push(PackItem::file(PackItemKind::Service, file_name, rel));
                    }
                }
                "agents" | "credentials" | "providers" | "recipes" | "responses" | "routes" => {
                    if !path.is_file() {
                        continue;
                    }
                    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
                    let (kind, matches) = match dir_name {
                        "agents" => (PackItemKind::Agent, ext == "md"),
                        "credentials" => (PackItemKind::Credential, ext == "yaml"),
                        "providers" => (PackItemKind::Provider, ext == "yaml"),
                        "responses" => (PackItemKind::Response, ext == "md"),
                        // The route loader reads `.yaml` only, so a `.yml` route
                        // would install and then never load.
                        "routes" => (PackItemKind::Route, ext == "yaml"),
                        _ => (PackItemKind::Recipe, ext == "yaml" || ext == "yml"),
                    };
                    if !matches {
                        continue;
                    }
                    let id = path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or(file_name)
                        .to_string();
                    items.push(PackItem::file(kind, id, rel));
                }
                _ => {}
            }
        }
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn a_service_needs_its_manifest_and_a_services_only_directory_is_a_pack() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("kit");
        write(&root.join("services/axon/service.yaml"), "name: Axon\n");
        write(&root.join("services/notes/README.md"), "not a service\n");

        let manifest = load(&root).unwrap();
        assert_eq!(manifest.format, PackFormat::Conventional);
        assert_eq!(
            manifest.items(),
            vec![PackItem::file(
                PackItemKind::Service,
                "axon",
                "services/axon"
            )]
        );
        assert_eq!(PackItemKind::parse("service"), Some(PackItemKind::Service));
    }

    #[test]
    fn a_service_the_manifest_names_and_the_tree_lacks_is_missing() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("kit");
        write(
            &root.join(MANIFEST_FILE),
            "id: kit\nname: Kit\nversion: 1.0.0\nservices: [axon, gone]\n",
        );
        write(&root.join("services/axon/service.yaml"), "name: Axon\n");
        std::fs::create_dir_all(root.join("services/gone")).unwrap();

        let manifest = load(&root).unwrap();
        assert_eq!(manifest.services, ["axon", "gone"]);
        assert_eq!(manifest.missing_services(), ["gone"]);
    }

    #[test]
    fn a_machine_service_classifies_as_running_here() {
        let runs = ItemRuns::classify(PackItemKind::Service, None);
        assert_eq!(runs, ItemRuns::Here);
        assert!(runs.is_runnable());
    }

    #[test]
    fn only_the_bundle_carries_a_service_and_anything_else_says_it_did_not() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("kit");
        write(&root.join("services/axon/service.yaml"), "name: Axon\n");
        write(&root.join("agents/a.md"), "agent\n");
        let manifest = load(&root).unwrap();

        assert_eq!(manifest.items_from(PackSourceKind::Bundled).len(), 2);
        assert!(manifest.notes_from(PackSourceKind::Bundled).is_empty());
        for kind in [PackSourceKind::Url, PackSourceKind::Local] {
            assert_eq!(
                manifest.items_from(kind),
                vec![PackItem::file(PackItemKind::Agent, "a", "agents/a.md")]
            );
            assert_eq!(
                manifest.notes_from(kind),
                vec![format!("{NON_BUNDLED_SERVICES_NOTE}; not installed: axon")]
            );
        }
    }

    #[test]
    fn native_manifest_wins_and_contents_are_discovered() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("matlab");
        write(
            &root.join(MANIFEST_FILE),
            "cairnVersion: 1\nid: matlab\nname: MATLAB\nversion: 1.2.3\ndescription: Numerics.\ndefault: false\nkeywords: [matlab]\n",
        );
        write(
            &root.join("skills/matlab/SKILL.md"),
            "---\nname: matlab\n---\n",
        );
        write(&root.join("agents/analyst.md"), "# analyst\n");
        write(&root.join("recipes/run.yaml"), "name: run\n");
        write(&root.join("responses/tone.md"), "tone\n");
        write(&root.join("routes/ping.yaml"), "name: ping\n");
        write(&root.join("routes/ignored.yml"), "name: ignored\n");
        write(&root.join("workflows/sweep/workflow.yaml"), "name: sweep\n");
        write(
            &root.join(PACK_MCP_FILE),
            "mcpServers:\n  matlab:\n    type: stdio\n    command: ${MATLAB_MCP_SERVER}\n",
        );

        let manifest = load(&root).unwrap();
        assert_eq!(manifest.id, "matlab");
        assert_eq!(manifest.name, "MATLAB");
        assert_eq!(manifest.version, "1.2.3");
        assert_eq!(manifest.format, PackFormat::Cairn);
        assert!(!manifest.default);

        let items = manifest.items();
        assert!(items.contains(&PackItem::file(
            PackItemKind::Skill,
            "matlab",
            "skills/matlab"
        )));
        assert!(items.contains(&PackItem::file(
            PackItemKind::Agent,
            "analyst",
            "agents/analyst.md"
        )));
        assert!(items.contains(&PackItem::file(
            PackItemKind::Recipe,
            "run",
            "recipes/run.yaml"
        )));
        assert!(items.contains(&PackItem::file(
            PackItemKind::Response,
            "tone",
            "responses/tone.md"
        )));
        assert!(items.contains(&PackItem::file(
            PackItemKind::Route,
            "ping",
            "routes/ping.yaml"
        )));
        assert!(
            !items.iter().any(|item| item.id == "ignored"),
            "a `.yml` route is not discovered, because the route loader would never read it"
        );
        assert!(items.contains(&PackItem::file(
            PackItemKind::Workflow,
            "sweep",
            "workflows/sweep"
        )));
        assert!(items.contains(&PackItem::mcp("matlab")));
    }

    #[test]
    fn claude_plugin_normalizes_and_records_skipped_components() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("acme-tools");
        write(
            &root.join(CLAUDE_PLUGIN_MANIFEST),
            r#"{"name":"acme-tools","displayName":"Acme Tools","version":"2.0.0","description":"d","author":"Acme","keywords":["acme"],"unknownFutureField":true}"#,
        );
        write(&root.join("skills/acme/SKILL.md"), "---\nname: acme\n---\n");
        write(&root.join("agents/acme.md"), "# acme\n");
        write(
            &root.join(CLAUDE_MCP_FILE),
            r#"{"mcpServers":{"acme":{"type":"http","url":"https://acme.test/mcp"}}}"#,
        );
        write(&root.join("commands/thing.md"), "cmd\n");
        write(&root.join("hooks/hooks.json"), "{}\n");

        let manifest = load(&root).unwrap();
        assert_eq!(manifest.format, PackFormat::ClaudeCode);
        assert_eq!(manifest.id, "acme-tools");
        assert_eq!(manifest.name, "Acme Tools");
        assert_eq!(manifest.version, "2.0.0");
        assert_eq!(
            manifest.author,
            Some(PackAuthor {
                name: Some("Acme".into()),
                ..Default::default()
            })
        );
        assert!(
            !manifest.default,
            "a foreign pack never joins the default set"
        );

        let items = manifest.items();
        assert!(items.contains(&PackItem::file(PackItemKind::Skill, "acme", "skills/acme")));
        assert!(items.contains(&PackItem::file(
            PackItemKind::Agent,
            "acme",
            "agents/acme.md"
        )));
        assert!(
            items.contains(&PackItem::mcp("acme")),
            "`.mcp.json` carries the same mcpServers map as mcp.yaml"
        );

        assert!(manifest.notes.iter().any(|n| n.contains("commands")));
        assert!(manifest.notes.iter().any(|n| n.contains("hooks")));
    }

    #[test]
    fn bare_skill_directory_is_a_degenerate_pack() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("pdf-forms");
        write(&root.join("SKILL.md"), "---\nname: pdf-forms\n---\n");
        write(&root.join("scripts/fill.py"), "print()\n");

        let manifest = load(&root).unwrap();
        assert_eq!(manifest.format, PackFormat::BareSkill);
        assert_eq!(manifest.id, "pdf-forms");
        assert_eq!(
            manifest.items(),
            vec![PackItem::file(
                PackItemKind::Skill,
                "pdf-forms",
                "skills/pdf-forms"
            )]
        );
    }

    #[test]
    fn manifest_less_conventional_directory_installs() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("my-stuff");
        write(&root.join("recipes/mine.yaml"), "name: mine\n");

        let manifest = load(&root).unwrap();
        assert_eq!(manifest.format, PackFormat::Conventional);
        assert_eq!(manifest.id, "my-stuff");
        assert_eq!(manifest.version, "0.0.0");
        assert_eq!(
            manifest.items(),
            vec![PackItem::file(
                PackItemKind::Recipe,
                "mine",
                "recipes/mine.yaml"
            )]
        );
    }

    #[test]
    fn what_an_item_runs_is_derived_from_its_kind_and_transport() {
        let remote: crate::server_config::McpServerConfig =
            serde_yaml::from_str("type: http\nurl: https://mcp.example/mcp\n").unwrap();
        let sse: crate::server_config::McpServerConfig =
            serde_yaml::from_str("type: sse\nurl: https://mcp.example/sse\n").unwrap();
        let stdio: crate::server_config::McpServerConfig =
            serde_yaml::from_str("command: npx\n").unwrap();
        assert_eq!(
            ItemRuns::classify(PackItemKind::Mcp, Some(&remote)),
            ItemRuns::Endpoint
        );
        assert_eq!(
            ItemRuns::classify(PackItemKind::Mcp, Some(&sse)),
            ItemRuns::Endpoint
        );
        assert_eq!(
            ItemRuns::classify(PackItemKind::Mcp, Some(&stdio)),
            ItemRuns::Here,
            "`type` defaults to stdio, which spawns a process"
        );
        assert_eq!(ItemRuns::classify(PackItemKind::Mcp, None), ItemRuns::Here);
        for kind in [PackItemKind::Credential, PackItemKind::Provider] {
            assert_eq!(
                ItemRuns::classify(kind, None),
                ItemRuns::Endpoint,
                "{kind:?} sends a stored secret to an external host"
            );
        }
        // A provider bound to an MCP server runs what that server runs.
        assert_eq!(
            ItemRuns::classify(PackItemKind::Provider, Some(&remote)),
            ItemRuns::Endpoint
        );
        assert_eq!(
            ItemRuns::classify(PackItemKind::Provider, Some(&stdio)),
            ItemRuns::Here,
            "a provider bound to a stdio server spawns it"
        );
        assert_eq!(
            ItemRuns::classify(PackItemKind::Workflow, None),
            ItemRuns::Here
        );
        for kind in [
            PackItemKind::Agent,
            PackItemKind::Recipe,
            PackItemKind::Response,
            PackItemKind::Route,
            PackItemKind::Skill,
        ] {
            assert_eq!(ItemRuns::classify(kind, None), ItemRuns::Content);
        }
        assert!(ItemRuns::Here > ItemRuns::Endpoint && ItemRuns::Endpoint > ItemRuns::Content);
    }

    #[test]
    fn providers_and_credentials_are_discovered_and_live_in_the_pack_layer() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("search");
        write(&root.join("providers/web-search.yaml"), "label: S\n");
        write(&root.join("credentials/api-key.yaml"), "label: K\n");
        write(&root.join("providers/notes.md"), "not a provider\n");

        let manifest = load(&root).unwrap();
        assert_eq!(manifest.format, PackFormat::Conventional);
        let items = manifest.items();
        assert_eq!(
            items,
            vec![
                PackItem::file(
                    PackItemKind::Credential,
                    "api-key",
                    "credentials/api-key.yaml"
                ),
                PackItem::file(
                    PackItemKind::Provider,
                    "web-search",
                    "providers/web-search.yaml"
                ),
            ]
        );
        assert!(items.iter().all(|item| item.kind.in_pack_layer()));
        assert_eq!(
            PackItemKind::parse("provider"),
            Some(PackItemKind::Provider)
        );
        assert_eq!(
            PackItemKind::parse("credential"),
            Some(PackItemKind::Credential)
        );
    }

    #[test]
    fn a_directory_with_no_pack_content_is_rejected() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("empty");
        std::fs::create_dir_all(root.join("docs")).unwrap();
        assert!(load(&root).is_err());
    }
}
