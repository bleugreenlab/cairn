//! Persisted store of each external MCP server's advertised tool metadata.
//!
//! The agent orientation prompt renders a terse one-line contract per MCP tool
//! (see [`crate::mcp::handlers::mcp_resources::render_mcp_affordance_block`]).
//! Listing those tools means connecting to (and often spawning) the server, far
//! too costly to do inline on every prompt build. This store captures the tool
//! metadata once — when a server is added/edited/saved in Settings — and the
//! prompt builder reads it back *synchronously*. The result is deterministic:
//! a server whose tools listed successfully shows its contracts in the very next
//! spawned agent, and they survive an app restart.
//!
//! It is derived data, not user config, so it lives in its own sidecar JSON
//! (`~/.cairn/mcp_tools.json`) rather than `settings.yaml`.
//!
//! ## Scope keying
//!
//! Servers resolve per run as workspace entries overlaid by a project's
//! `.cairn/config.yaml` (project wins on a name collision — see
//! [`crate::config::mcp_servers::resolve_mcp_servers`]). Keying tools by name
//! alone would let a project's `foo` render the workspace `foo`'s contracts.
//! The store therefore keeps workspace and per-project entries separate, and
//! [`resolve_tools`] overlays them the same way server resolution does.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::mcp::gateway::McpToolDef;

const STORE_FILENAME: &str = "mcp_tools.json";

/// The scope a captured tool list belongs to, mirroring server resolution.
#[derive(Debug, Clone, Copy)]
pub enum ToolScope<'a> {
    /// Workspace-level server (`~/.cairn/settings.yaml`). The settings UI only
    /// edits this scope, so it is the common capture target.
    Workspace,
    /// Project-level server (`[project]/.cairn/config.yaml`), keyed by repo path.
    Project(&'a Path),
}

/// On-disk shape of the tool store. Workspace and project entries are kept
/// apart so same-named servers across scopes never collide.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct McpToolStore {
    /// Workspace-scoped servers, keyed by server name.
    #[serde(default)]
    workspace: HashMap<String, Vec<McpToolDef>>,
    /// Project-scoped servers, keyed by canonical project path then server name.
    #[serde(default)]
    projects: HashMap<String, HashMap<String, Vec<McpToolDef>>>,
}

fn store_path(config_dir: &Path) -> PathBuf {
    config_dir.join(STORE_FILENAME)
}

fn project_key(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

/// Load the store, returning an empty one if the file is missing or unreadable.
/// Best-effort by design: a corrupt sidecar must never break prompt assembly —
/// the affordance block just falls back to per-server `read` pointers.
fn load(config_dir: &Path) -> McpToolStore {
    let path = store_path(config_dir);
    let Ok(content) = std::fs::read_to_string(&path) else {
        return McpToolStore::default();
    };
    serde_json::from_str(&content).unwrap_or_default()
}

fn save(config_dir: &Path, store: &McpToolStore) -> Result<(), String> {
    let path = store_path(config_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create config directory: {e}"))?;
    }
    let json = serde_json::to_string_pretty(store)
        .map_err(|e| format!("Failed to serialize MCP tool store: {e}"))?;
    std::fs::write(&path, json).map_err(|e| format!("Failed to write MCP tool store: {e}"))
}

/// Record (replace) the tool list captured for `server` in the given scope.
/// Called after a successful `list_tools` on save/edit. An empty list is stored
/// faithfully ("server answered, no tools") — distinct from a *missing* entry,
/// which renders the `read cairn://mcp/<server>` pointer.
pub fn record_tools(
    config_dir: &Path,
    scope: ToolScope<'_>,
    server: &str,
    tools: &[McpToolDef],
) -> Result<(), String> {
    let mut store = load(config_dir);
    match scope {
        ToolScope::Workspace => {
            store.workspace.insert(server.to_string(), tools.to_vec());
        }
        ToolScope::Project(path) => {
            store
                .projects
                .entry(project_key(path))
                .or_default()
                .insert(server.to_string(), tools.to_vec());
        }
    }
    save(config_dir, &store)
}

/// Drop any captured tools for `server` in the given scope (e.g. on delete).
/// Succeeds even if no entry existed.
pub fn remove_server(config_dir: &Path, scope: ToolScope<'_>, server: &str) -> Result<(), String> {
    let mut store = load(config_dir);
    let changed = match scope {
        ToolScope::Workspace => store.workspace.remove(server).is_some(),
        ToolScope::Project(path) => store
            .projects
            .get_mut(&project_key(path))
            .map(|servers| servers.remove(server).is_some())
            .unwrap_or(false),
    };
    if changed {
        save(config_dir, &store)
    } else {
        Ok(())
    }
}

/// Resolve the captured tools for a run context, keyed by server name. Mirrors
/// [`crate::config::mcp_servers::resolve_mcp_servers`]: workspace tools overlaid
/// by the project's, with the project entry winning on a name collision. The
/// prompt builder passes the result straight to the affordance renderer.
pub fn resolve_tools(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> HashMap<String, Vec<McpToolDef>> {
    let store = load(config_dir);
    let mut tools = store.workspace;
    if let Some(path) = project_path {
        if let Some(project_tools) = store.projects.get(&project_key(path)) {
            for (name, defs) in project_tools {
                tools.insert(name.clone(), defs.clone());
            }
        }
    }
    tools
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tool(name: &str) -> McpToolDef {
        McpToolDef {
            name: name.to_string(),
            description: Some(format!("{name} desc")),
            input_schema: serde_json::json!({ "type": "object", "properties": {} }),
        }
    }

    #[test]
    fn record_then_resolve_workspace_roundtrips() {
        let dir = TempDir::new().unwrap();
        assert!(resolve_tools(dir.path(), None).is_empty());

        record_tools(dir.path(), ToolScope::Workspace, "axon", &[tool("look")]).unwrap();
        let resolved = resolve_tools(dir.path(), None);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved["axon"].len(), 1);
        assert_eq!(resolved["axon"][0].name, "look");
        // It is a real file on disk, so it survives a fresh load (i.e. restart).
        assert!(store_path(dir.path()).exists());
    }

    #[test]
    fn project_overlays_workspace_without_colliding() {
        let dir = TempDir::new().unwrap();
        let proj = Path::new("/repo/alpha");
        record_tools(dir.path(), ToolScope::Workspace, "foo", &[tool("ws_tool")]).unwrap();
        record_tools(
            dir.path(),
            ToolScope::Project(proj),
            "foo",
            &[tool("proj_tool")],
        )
        .unwrap();

        // Without a project path, the workspace entry is what resolves.
        let ws = resolve_tools(dir.path(), None);
        assert_eq!(ws["foo"][0].name, "ws_tool");

        // With the project path, its entry wins for the same name.
        let scoped = resolve_tools(dir.path(), Some(proj));
        assert_eq!(scoped["foo"][0].name, "proj_tool");

        // A different project does not see the first project's override.
        let other = resolve_tools(dir.path(), Some(Path::new("/repo/beta")));
        assert_eq!(other["foo"][0].name, "ws_tool");
    }

    #[test]
    fn empty_tool_list_is_stored_distinct_from_missing() {
        let dir = TempDir::new().unwrap();
        record_tools(dir.path(), ToolScope::Workspace, "toolless", &[]).unwrap();
        let resolved = resolve_tools(dir.path(), None);
        // Present with an empty list — not absent.
        assert!(resolved.contains_key("toolless"));
        assert!(resolved["toolless"].is_empty());
    }

    #[test]
    fn remove_server_drops_entry() {
        let dir = TempDir::new().unwrap();
        record_tools(dir.path(), ToolScope::Workspace, "axon", &[tool("look")]).unwrap();
        remove_server(dir.path(), ToolScope::Workspace, "axon").unwrap();
        assert!(resolve_tools(dir.path(), None).is_empty());
        // Removing a missing server is a no-op, not an error.
        remove_server(dir.path(), ToolScope::Workspace, "nope").unwrap();
    }

    #[test]
    fn corrupt_store_loads_as_empty() {
        let dir = TempDir::new().unwrap();
        std::fs::write(store_path(dir.path()), "{ not valid json").unwrap();
        assert!(resolve_tools(dir.path(), None).is_empty());
    }
}
