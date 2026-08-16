//! Pack manifests: the source-side description of an installable resource pack.
//!
//! A **pack** is a directory holding Cairn resources — skills, agents, recipes,
//! workflows, response templates, and MCP server definitions — plus an optional
//! manifest naming it. Packs are the unit of *offering and installing*; they are
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

/// The native manifest filename, at the pack root.
pub const MANIFEST_FILE: &str = "cairn-pack.yaml";
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
    Mcp,
    Recipe,
    Response,
    Skill,
    Workflow,
}

impl PackItemKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PackItemKind::Agent => "agent",
            PackItemKind::Mcp => "mcp",
            PackItemKind::Recipe => "recipe",
            PackItemKind::Response => "response",
            PackItemKind::Skill => "skill",
            PackItemKind::Workflow => "workflow",
        }
    }

    /// The inverse of [`Self::as_str`], for a kind that arrives as text from a
    /// resource payload or a settings invoke.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "agent" => Some(PackItemKind::Agent),
            "mcp" => Some(PackItemKind::Mcp),
            "recipe" => Some(PackItemKind::Recipe),
            "response" => Some(PackItemKind::Response),
            "skill" => Some(PackItemKind::Skill),
            "workflow" => Some(PackItemKind::Workflow),
            _ => None,
        }
    }

    /// Every kind, for error text that enumerates the alternatives.
    pub const ALL: [PackItemKind; 6] = [
        PackItemKind::Agent,
        PackItemKind::Mcp,
        PackItemKind::Recipe,
        PackItemKind::Response,
        PackItemKind::Skill,
        PackItemKind::Workflow,
    ];
}

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
         or one of the conventional content directories (agents, recipes, responses, skills, workflows)"
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
            Ok(servers) => {
                let mut names: Vec<String> = servers.into_keys().collect();
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
    for dir_name in super::CONTENT_DIRS {
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
                "agents" | "recipes" | "responses" => {
                    if !path.is_file() {
                        continue;
                    }
                    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
                    let (kind, matches) = match dir_name {
                        "agents" => (PackItemKind::Agent, ext == "md"),
                        "responses" => (PackItemKind::Response, ext == "md"),
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
    fn a_directory_with_no_pack_content_is_rejected() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("empty");
        std::fs::create_dir_all(root.join("docs")).unwrap();
        assert!(load(&root).is_err());
    }
}
