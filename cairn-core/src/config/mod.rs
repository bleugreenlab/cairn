//! File-based configuration reading.
//!
//! This module reads recipes, agents, and skills directly from files.
//! Files are the source of truth - the database only stores runtime state (executions, jobs, etc.).
//!
//! ## File Structure
//!
//! Workspace-level configs (shared across all projects):
//! ```text
//! ~/.cairn/
//! ├── recipes/
//! │   └── my-recipe.yaml
//! ├── agents/
//! │   └── my-agent.md
//! └── skills/
//!     └── my-skill.md
//! ```
//!
//! Project-level configs (stored in project directory, can be version-controlled):
//! ```text
//! [project-dir]/.cairn/
//! ├── recipes/
//! │   └── project-recipe.yaml
//! ├── agents/
//! │   └── project-agent.md
//! └── skills/
//!     └── project-skill.md
//! ```
//!
//! ## ID = Filename
//!
//! - `explore.md` → ID is `explore`
//! - Config in project's `.cairn/agents/custom.md` → ID is `custom` (project-scoped)

pub mod agents;
pub mod build_services;
pub mod dev_commands;
pub mod keybinds;
pub mod mcp_import;
pub mod mcp_servers;
pub mod mcp_setup;
pub mod mcp_tools;
pub mod pdf;
pub mod presets;
pub mod project_settings;
pub mod provider_options;
pub mod recipes;
pub mod secrets;
pub mod settings;
pub mod skill_fetch;
pub mod skills;
pub mod web_fetch;
pub mod web_search;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::storage::{DbError, LocalDb, RowExt};

/// Get the base config directory (~/.cairn/ for prod, ~/.cairn-dev/ for dev).
///
/// Delegates to the shared `cairn_common::paths` resolver so home resolution is
/// canonical across all crates. The `Result` signature is retained for the ~12
/// existing callers; resolution itself is now infallible (falls back to `/tmp`).
pub fn get_config_dir() -> Result<PathBuf, String> {
    Ok(cairn_common::paths::cairn_home())
}

/// Get project repo path from project ID
pub async fn get_project_path(db: &LocalDb, project_id: &str) -> Result<PathBuf, String> {
    let project_id = project_id.to_string();
    let repo_path = db
        .read(|conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT repo_path FROM projects WHERE id = ?1",
                        (project_id.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("project not found".to_string()))?;
                row.text(0)
            })
        })
        .await
        .map_err(|e| format!("Project not found: {e}"))?;

    Ok(PathBuf::from(repo_path))
}

/// Commit project-scoped config changes in a project's **canonical checkout**.
///
/// Project resource mutations (`write cairn://p/PROJECT/skills/...` and friends)
/// and read-path config migrations rewrite files under `[project]/.cairn/`. Those
/// edits land in the user's canonical repo — not an agent worktree — so without
/// committing them they float as uncommitted dirty state in the user's repo.
///
/// Stages only the `.cairn` tree (leaving unrelated dirty state untouched) and
/// commits it with Cairn's inline identity. The commit is **best effort**: the
/// file write has already succeeded and is never rolled back, so a failure here
/// is logged as a warning rather than surfaced as a mutation error. Returns the
/// new commit sha when a commit was actually made; returns `None` when
/// `project_path` is not inside a git work tree (e.g. a workspace `~/.cairn`
/// directory), when there was nothing to commit, or when staging/committing
/// failed.
pub(crate) fn commit_project_config_change(project_path: &Path, msg: &str) -> Option<String> {
    // Only operate inside a real git work tree. Workspace config lives in
    // ~/.cairn, which is not a repo, so this guard makes the helper a safe no-op
    // for workspace-scoped writes.
    let inside = crate::env::git()
        .args(["rev-parse", "--is-inside-work-tree"])
        .current_dir(project_path)
        .output()
        .ok()?;
    if !inside.status.success() || String::from_utf8_lossy(&inside.stdout).trim() != "true" {
        return None;
    }

    // Stage only the .cairn config tree so unrelated dirty state in the canonical
    // checkout is left alone.
    match crate::env::git()
        .args(["add", "--", ".cairn"])
        .current_dir(project_path)
        .output()
    {
        Ok(out) if out.status.success() => {}
        Ok(out) => {
            log::warn!(
                "Failed to stage project config in {:?}: {}",
                project_path,
                String::from_utf8_lossy(&out.stderr).trim()
            );
            return None;
        }
        Err(e) => {
            log::warn!("Failed to stage project config in {:?}: {e}", project_path);
            return None;
        }
    }

    git_commit_with_identity(project_path, msg)
}

/// Commit the currently-staged index in `repo_root` with Cairn's inline
/// identity, returning the new commit sha. A "nothing to commit" result is the
/// expected no-op when staging produced no change, not a failure.
fn git_commit_with_identity(repo_root: &Path, msg: &str) -> Option<String> {
    match crate::env::git()
        .args([
            "-c",
            "user.name=Cairn",
            "-c",
            "user.email=cairn@local.invalid",
            "commit",
            "-m",
            msg,
        ])
        .current_dir(repo_root)
        .output()
    {
        Ok(out) if out.status.success() => {}
        Ok(out) => {
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            if !combined.contains("nothing to commit") {
                log::warn!(
                    "Failed to commit config in {:?}: {}",
                    repo_root,
                    combined.trim()
                );
            }
            return None;
        }
        Err(e) => {
            log::warn!("Failed to commit config in {:?}: {e}", repo_root);
            return None;
        }
    }

    let head = crate::env::git()
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_root)
        .output()
        .ok()?;
    head.status
        .success()
        .then(|| String::from_utf8_lossy(&head.stdout).trim().to_string())
}

/// Commit specific config file/directory paths to whichever git work tree
/// contains them, staging ONLY those paths so unrelated dirty state in the
/// repository is left untouched.
///
/// Both the shared `ConfigResource` mutation path (the Settings UI) and the MCP
/// resource mutation handlers funnel their post-write commits through here, for
/// workspace scope (`~/.cairn`, a repo since CAIRN-1468) and project scope (the
/// project checkout) alike. Paths are grouped by their containing repository, so
/// a scope change that moves a config from one repo to another commits each side
/// once.
///
/// Best effort: the file write has already succeeded and is never rolled back,
/// so a git failure is logged rather than surfaced as a mutation error. A path
/// not inside any git work tree (an older install where `~/.cairn` is not a
/// repo) is silently skipped. Returns the shas of the commits actually made.
pub(crate) fn commit_config_paths(paths: &[PathBuf], msg: &str) -> Vec<String> {
    use std::collections::BTreeMap;

    // Map each repo's work-tree root to a directory inside it that we can run
    // `git commit` from. Staging happens per path (relative to its parent dir,
    // sidestepping symlinked-temp-dir pathspec mismatches); the commit happens
    // once per repo.
    let mut repos: BTreeMap<PathBuf, PathBuf> = BTreeMap::new();
    for path in paths {
        let Some(parent) = path.parent() else {
            continue;
        };
        if !parent.exists() {
            continue;
        }
        let Some(root) = git_work_tree_root(parent) else {
            continue;
        };
        let Some(name) = path.file_name() else {
            continue;
        };
        match crate::env::git()
            .arg("add")
            .arg("--")
            .arg(name)
            .current_dir(parent)
            .output()
        {
            Ok(out) if out.status.success() => {
                repos.entry(root).or_insert_with(|| parent.to_path_buf());
            }
            Ok(out) => log::warn!(
                "Failed to stage config path {:?}: {}",
                path,
                String::from_utf8_lossy(&out.stderr).trim()
            ),
            Err(e) => log::warn!("Failed to stage config path {:?}: {e}", path),
        }
    }

    let mut shas = Vec::new();
    for (_root, dir) in repos {
        if let Some(sha) = git_commit_with_identity(&dir, msg) {
            shas.push(sha);
        }
    }
    shas
}

/// Resolve the git work-tree root that contains `dir`, or None when it is not
/// inside one (an older install where `~/.cairn` is not a git repo).
fn git_work_tree_root(dir: &Path) -> Option<PathBuf> {
    let out = crate::env::git()
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(dir)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let root = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!root.is_empty()).then(|| PathBuf::from(root))
}

/// Ensure config directories exist
pub(crate) fn config_root_subdirs(
    config_dir: &Path,
    project_path: Option<&Path>,
    subdir: &str,
) -> Vec<(PathBuf, bool)> {
    let mut roots = Vec::new();
    if let Some(project_path) = project_path {
        roots.push((project_path.join(".cairn").join(subdir), true));
    }
    roots.push((config_dir.join(subdir), false));
    roots
}

pub fn ensure_config_dirs(config_dir: &Path) -> Result<(), String> {
    for dir in ["recipes", "agents", "skills"] {
        let path = config_dir.join(dir);
        std::fs::create_dir_all(&path)
            .map_err(|e| format!("Failed to create directory {:?}: {}", path, e))?;
    }

    Ok(())
}

pub(crate) const BUNDLE_RESOURCE_DIRS: [&str; 3] = ["agents", "recipes", "skills"];

/// Recursively mirror `source_dir` into `dest_dir`, replacing any existing destination tree.
pub fn mirror_dir(source_dir: &Path, dest_dir: &Path) -> Result<(), String> {
    let fs = crate::services::RealFileSystem;
    mirror_dir_with_fs(&fs, source_dir, dest_dir)
}

/// Mirror bundled agents, recipes, and skills into `target_dir`.
///
/// Only those three managed subtrees are deleted and replaced; sibling user-local
/// files such as `.gitignore`, `settings.yaml`, and `AGENTS.md` are left alone.
pub fn mirror_bundle_resources(resource_dir: &Path, target_dir: &Path) -> Result<(), String> {
    let fs = crate::services::RealFileSystem;
    mirror_bundle_resources_with_fs(&fs, resource_dir, target_dir)
}

pub(crate) fn mirror_bundle_resources_with_fs(
    fs: &dyn crate::services::FileSystem,
    resource_dir: &Path,
    target_dir: &Path,
) -> Result<(), String> {
    fs.create_dir_all(target_dir)?;
    for dir_name in BUNDLE_RESOURCE_DIRS {
        let source = resource_dir.join(dir_name);
        let dest = target_dir.join(dir_name);
        if fs.exists(&dest) {
            fs.remove_dir_all(&dest)?;
        }
        if fs.exists(&source) {
            mirror_dir_with_fs(fs, &source, &dest)?;
        } else {
            fs.create_dir_all(&dest)?;
            log::debug!(
                "Bundled {} directory not found at {:?}; leaving empty managed tree",
                dir_name,
                source
            );
        }
    }
    Ok(())
}

pub(crate) fn mirror_dir_with_fs(
    fs: &dyn crate::services::FileSystem,
    source_dir: &Path,
    dest_dir: &Path,
) -> Result<(), String> {
    if fs.exists(dest_dir) {
        fs.remove_dir_all(dest_dir)?;
    }
    fs.copy_dir_recursive(source_dir, dest_dir)
}

/// Return IDs restored by mirroring a bundled resource subtree.
pub fn bundled_resource_ids(resource_dir: &Path, dir_name: &str) -> Result<Vec<String>, String> {
    let dir = resource_dir.join(dir_name);
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut ids = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(|e| {
        format!(
            "Failed to read bundled {} directory {:?}: {}",
            dir_name, dir, e
        )
    })? {
        let entry = entry.map_err(|e| format!("Failed to read bundled entry: {}", e))?;
        let path = entry.path();
        if dir_name == "skills" {
            if path.is_dir() && path.join("SKILL.md").exists() {
                if let Some(id) = path.file_name().and_then(|name| name.to_str()) {
                    ids.push(id.to_string());
                }
            }
        } else if path.is_file() {
            let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            let matches = match dir_name {
                "agents" => ext == "md",
                "recipes" => ext == "yaml" || ext == "yml",
                _ => false,
            };
            if matches {
                if let Some(id) = path.file_stem().and_then(|stem| stem.to_str()) {
                    ids.push(id.to_string());
                }
            }
        }
    }
    ids.sort();
    Ok(ids)
}

/// Restore one bundled config artifact (agent | recipe | skill) over its
/// workspace copy and commit the change. The per-artifact analog of
/// [`mirror_bundle_resources`], for resetting a single diverged workspace
/// artifact back to its shipped default. Errors if the id has no bundled
/// counterpart (a user-created artifact has no default to restore).
///
/// `entity_type` is the singular form used across the config commands
/// (`agent` | `recipe` | `skill`). `resource_dir` is the app bundle's resource
/// directory; `target_dir` is the workspace config dir (`~/.cairn`).
pub fn restore_bundled_config_entry(
    resource_dir: &Path,
    target_dir: &Path,
    entity_type: &str,
    id: &str,
) -> Result<(), String> {
    let subdir = match entity_type {
        "agent" => "agents",
        "recipe" => "recipes",
        "skill" => "skills",
        other => return Err(format!("No bundled defaults for entity type '{other}'")),
    };

    let dest = if subdir == "skills" {
        let source = resource_dir.join(subdir).join(id);
        if !source.join("SKILL.md").exists() {
            return Err(format!("No bundled default for skill '{id}'"));
        }
        let dest = target_dir.join(subdir).join(id);
        let fs = crate::services::RealFileSystem;
        mirror_dir_with_fs(&fs, &source, &dest)?;
        dest
    } else {
        let extensions: &[&str] = if subdir == "agents" {
            &["md"]
        } else {
            &["yaml", "yml"]
        };
        let source = extensions
            .iter()
            .map(|ext| resource_dir.join(subdir).join(format!("{id}.{ext}")))
            .find(|path| path.exists())
            .ok_or_else(|| format!("No bundled default for {entity_type} '{id}'"))?;
        let ext = source.extension().and_then(|e| e.to_str()).unwrap_or("");
        let dest = target_dir.join(subdir).join(format!("{id}.{ext}"));
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create {subdir} directory: {e}"))?;
        }
        std::fs::copy(&source, &dest)
            .map_err(|e| format!("Failed to copy bundled {entity_type} '{id}': {e}"))?;
        dest
    };

    commit_config_paths(
        std::slice::from_ref(&dest),
        &format!("cairn: reset {entity_type} {id}"),
    );
    Ok(())
}

/// Result type for config file loading - includes both successful loads and parse errors
#[derive(Debug, Clone)]
pub enum ConfigResult<T> {
    /// Successfully parsed config
    Ok(T),
    /// Failed to parse - shows error in UI
    Err { path: PathBuf, error: String },
}

/// Extract ID from a file path (filename without extension)
pub fn id_from_path(path: &std::path::Path) -> Option<String> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
}

/// Generate a slug from a name (lowercase, hyphens, no special chars).
///
/// Unicode-aware: preserves letters and digits from any script. Used for agent,
/// skill, and recipe identifiers where names may be non-ASCII.
pub fn slugify(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// ASCII-only slugify for cairn:// URI segments.
///
/// Mirrors `slugifyResourceSegment` in `packages/ui/src/uri.ts` exactly so that
/// URIs emitted by the backend and the browser are byte-identical for any
/// given input.
pub fn slugify_resource_segment(value: &str) -> String {
    value
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Derive a unique URI-safe slug for a delegated task.
///
/// This is the **single source of truth** for delegated task slugs. The
/// backend computes the slug once at materialization time and stores it in
/// `jobs.node_name`; the frontend reads that stored value rather than
/// recomputing.
///
/// Algorithm:
/// 1. Slugify the description (ASCII-only, URI-safe) and split on `-` into words.
/// 2. Start with the first `min(2, N)` words joined by `-`.
/// 3. While the candidate collides with the reserved set, append the next
///    word until either it is unique or words are exhausted.
/// 4. If still colliding, append `-2`, `-3`, … to the full word join.
/// 5. Empty / non-alphanumeric descriptions fall back to `task` with the same
///    numeric suffix disambiguation.
///
/// The reserved set must be prepopulated with the already-assigned slugs
/// (normalized through `slugify_resource_segment`) for sibling task jobs.
pub fn derive_unique_task_slug(description: &str, reserved: &HashSet<String>) -> String {
    let slugged = slugify_resource_segment(description);
    let words: Vec<&str> = slugged.split('-').filter(|s| !s.is_empty()).collect();

    if words.is_empty() {
        return next_available("task", reserved);
    }

    let initial = 2.min(words.len());
    for count in initial..=words.len() {
        let candidate = words[..count].join("-");
        if !reserved.contains(&candidate) {
            return candidate;
        }
    }

    next_available(&words.join("-"), reserved)
}

fn next_available(base: &str, reserved: &HashSet<String>) -> String {
    if !reserved.contains(base) {
        return base.to_string();
    }
    let mut n: u32 = 2;
    loop {
        let candidate = format!("{}-{}", base, n);
        if !reserved.contains(&candidate) {
            return candidate;
        }
        n += 1;
    }
}

/// Copy files from source to dest directory, skipping files that already exist.
pub fn copy_dir_contents(
    source_dir: &Path,
    dest_dir: &Path,
    extension: &str,
) -> Result<(), String> {
    let entries = std::fs::read_dir(source_dir)
        .map_err(|e| format!("Failed to read directory {:?}: {}", source_dir, e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let path = entry.path();

        // Skip directories and files with wrong extension
        if path.is_dir() {
            continue;
        }

        let file_ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if file_ext != extension && !(extension == "yaml" && file_ext == "yml") {
            continue;
        }

        // Get filename and determine destination
        let filename = match path.file_name() {
            Some(name) => name,
            None => continue,
        };

        let dest_path = dest_dir.join(filename);

        // Only copy if destination doesn't exist
        if !dest_path.exists() {
            std::fs::copy(&path, &dest_path)
                .map_err(|e| format!("Failed to copy {:?} to {:?}: {}", path, dest_path, e))?;
            log::info!("Copied bundled default: {:?}", dest_path);
        }
    }

    Ok(())
}

/// Copy files from source to dest directory, always overwriting.
/// Collects restored filenames into the provided list.
pub fn force_copy_dir_contents(
    source_dir: &Path,
    dest_dir: &Path,
    extension: &str,
    restored: &mut Vec<String>,
) -> Result<(), String> {
    let entries = std::fs::read_dir(source_dir)
        .map_err(|e| format!("Failed to read directory {:?}: {}", source_dir, e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let path = entry.path();

        // Skip directories and files with wrong extension
        if path.is_dir() {
            continue;
        }

        let file_ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if file_ext != extension && !(extension == "yaml" && file_ext == "yml") {
            continue;
        }

        // Get filename and determine destination
        let filename = match path.file_name() {
            Some(name) => name,
            None => continue,
        };

        let dest_path = dest_dir.join(filename);

        // Always copy (overwrite if exists)
        std::fs::copy(&path, &dest_path)
            .map_err(|e| format!("Failed to copy {:?} to {:?}: {}", path, dest_path, e))?;

        // Track the restored file (without extension)
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            restored.push(stem.to_string());
        }

        log::info!("Restored bundled default: {:?}", dest_path);
    }

    Ok(())
}

/// Result of restoring bundled defaults
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreResult {
    pub recipes_restored: Vec<String>,
    pub agents_restored: Vec<String>,
    pub skills_restored: Vec<String>,
}

/// Get a recipe by ID from files (for use by execution and snapshot builders).
///
/// This loads recipes from YAML files rather than the database, since files
/// are the source of truth for recipe configuration.
pub fn get_recipe_from_files(
    config_dir: &std::path::Path,
    project_path: Option<&std::path::Path>,
    recipe_id: &str,
) -> Result<crate::models::Recipe, String> {
    log::info!(
        "get_recipe_from_files: loading recipe_id={} from project_path={:?}",
        recipe_id,
        project_path
    );

    let file_recipe = recipes::get_recipe(config_dir, recipe_id, project_path)?
        .ok_or_else(|| format!("Recipe not found: {}", recipe_id))?;

    log::info!(
        "get_recipe_from_files: loaded file_recipe with {} nodes, {} edges, file_path={:?}",
        file_recipe.recipe.nodes.len(),
        file_recipe.recipe.edges.len(),
        file_recipe.file_path
    );

    // Determine workspace_id and project_id based on scope
    let (ws_id, proj_id) = if file_recipe.is_project_scoped {
        (None, None) // Project ID will be set by caller if needed
    } else {
        (Some("default".to_string()), None)
    };

    let recipe = file_recipe_to_recipe(file_recipe, ws_id, proj_id);
    log::info!(
        "get_recipe_from_files: after conversion, recipe has {} nodes, {} edges",
        recipe.nodes.len(),
        recipe.edges.len()
    );

    Ok(recipe)
}

/// Convert a FileRecipe to a Recipe with scope metadata
pub fn file_recipe_to_recipe(
    file_recipe: recipes::FileRecipe,
    workspace_id: Option<String>,
    project_id: Option<String>,
) -> crate::models::Recipe {
    let mut recipe = file_recipe.recipe;
    // Set workspace_id to "default" for workspace-scoped recipes if not provided
    recipe.workspace_id = if !file_recipe.is_project_scoped {
        workspace_id.or_else(|| Some("default".to_string()))
    } else {
        None
    };
    recipe.project_id = project_id;
    recipe
}

/// Get a recipe as a snapshot format, along with all agents referenced by the recipe.
///
/// This loads a recipe from files and converts it to RecipeSnapshot format.
/// Used by the SnapshotRecipeEditorDialog for customizing recipes before execution,
/// and by cairn-server for the same purpose.
pub async fn get_recipe_as_snapshot(
    db: &LocalDb,
    config_dir: &Path,
    recipe_id: &str,
    project_id: &str,
) -> Result<
    (
        crate::models::RecipeSnapshot,
        std::collections::HashMap<String, crate::models::AgentSnapshot>,
    ),
    String,
> {
    use crate::models::{AgentSnapshot, RecipeNodeType, RecipeSnapshot};
    use std::collections::HashMap;

    // Get project path for file-based config lookup
    let project_path = get_project_path(db, project_id).await?;

    // Load the recipe from files
    let recipe = get_recipe_from_files(config_dir, Some(&project_path), recipe_id)?;

    // Build recipe snapshot
    let recipe_snapshot = RecipeSnapshot {
        id: recipe.id.clone(),
        name: recipe.name,
        description: recipe.description,
        trigger: recipe.trigger,
        nodes: recipe.nodes.clone(),
        edges: recipe.edges,
    };

    // Find all agent IDs referenced by agent nodes
    let agent_ids: Vec<String> = recipe
        .nodes
        .iter()
        .filter(|n| n.node_type == RecipeNodeType::Agent)
        .filter_map(|n| {
            n.agent_config
                .as_ref()
                .and_then(|cfg| cfg.agent_config_id.clone())
        })
        .collect();

    // Load presets for agent resolution
    let presets_config = presets::load_effective_presets(config_dir, Some(&project_path));

    // Load agents
    let mut agents: HashMap<String, AgentSnapshot> = HashMap::new();
    for agent_id in agent_ids {
        if agents.contains_key(&agent_id) {
            continue;
        }

        if let Ok(Some(file_agent)) = agents::get_agent(config_dir, &agent_id, Some(&project_path))
        {
            let snapshot = presets::resolve_agent_snapshot(&file_agent, None, &presets_config)?;
            agents.insert(agent_id.clone(), snapshot);
        }
    }

    Ok((recipe_snapshot, agents))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("My Agent"), "my-agent");
        assert_eq!(slugify("test--skill"), "test-skill");
        assert_eq!(slugify("Recipe Name!"), "recipe-name");
    }

    #[test]
    fn test_id_from_path() {
        let path = PathBuf::from("/home/user/.cairn/agents/my-agent.md");
        assert_eq!(id_from_path(&path), Some("my-agent".to_string()));

        let path = PathBuf::from("/home/user/.cairn/recipes/test-recipe.yaml");
        assert_eq!(id_from_path(&path), Some("test-recipe".to_string()));
    }

    #[test]
    fn slugify_resource_segment_matches_frontend_rules() {
        assert_eq!(slugify_resource_segment("Survey Search"), "survey-search");
        assert_eq!(slugify_resource_segment("Map Tauri"), "map-tauri");
        // Non-ASCII is stripped, mirroring JS `/[^a-z0-9]+/g`.
        assert_eq!(slugify_resource_segment("Caf\u{00e9}"), "caf");
        // Collapses runs of non-alphanumerics and trims ends.
        assert_eq!(
            slugify_resource_segment("  Fix bug: off-by-one!  "),
            "fix-bug-off-by-one"
        );
    }

    #[test]
    fn derive_unique_task_slug_takes_first_two_words() {
        let reserved = HashSet::new();
        assert_eq!(
            derive_unique_task_slug("Survey Search usage for migration complexity", &reserved),
            "survey-search"
        );
    }

    #[test]
    fn derive_unique_task_slug_grows_one_word_on_collision() {
        let mut reserved = HashSet::new();
        reserved.insert("survey-search".to_string());
        assert_eq!(
            derive_unique_task_slug("Survey Search usage for migration complexity", &reserved),
            "survey-search-usage"
        );
    }

    #[test]
    fn derive_unique_task_slug_grows_through_word_exhaustion() {
        let mut reserved = HashSet::new();
        reserved.insert("map-tauri".to_string());
        assert_eq!(
            derive_unique_task_slug("Map Tauri deployment", &reserved),
            "map-tauri-deployment"
        );
    }

    #[test]
    fn derive_unique_task_slug_appends_numeric_suffix_when_words_exhausted() {
        let mut reserved = HashSet::new();
        reserved.insert("map-tauri".to_string());
        reserved.insert("map-tauri-deployment".to_string());
        assert_eq!(
            derive_unique_task_slug("Map Tauri deployment", &reserved),
            "map-tauri-deployment-2"
        );
    }

    #[test]
    fn derive_unique_task_slug_single_word_no_collision() {
        let reserved = HashSet::new();
        assert_eq!(derive_unique_task_slug("Explore", &reserved), "explore");
    }

    #[test]
    fn derive_unique_task_slug_single_word_collision_appends_suffix() {
        let mut reserved = HashSet::new();
        reserved.insert("explore".to_string());
        assert_eq!(derive_unique_task_slug("Explore", &reserved), "explore-2");
    }

    #[test]
    fn derive_unique_task_slug_keeps_incrementing_suffix() {
        let mut reserved = HashSet::new();
        reserved.insert("explore".to_string());
        reserved.insert("explore-2".to_string());
        reserved.insert("explore-3".to_string());
        assert_eq!(derive_unique_task_slug("Explore", &reserved), "explore-4");
    }

    #[test]
    fn derive_unique_task_slug_empty_input_falls_back_to_task() {
        let reserved = HashSet::new();
        assert_eq!(derive_unique_task_slug("", &reserved), "task");
        assert_eq!(derive_unique_task_slug("   ", &reserved), "task");
        assert_eq!(derive_unique_task_slug("!!!???", &reserved), "task");
    }

    #[test]
    fn derive_unique_task_slug_empty_input_numeric_suffix() {
        let mut reserved = HashSet::new();
        reserved.insert("task".to_string());
        reserved.insert("task-2".to_string());
        assert_eq!(derive_unique_task_slug("", &reserved), "task-3");
    }

    #[test]
    fn derive_unique_task_slug_numeric_input() {
        let reserved = HashSet::new();
        assert_eq!(derive_unique_task_slug("123", &reserved), "123");
    }

    #[test]
    fn derive_unique_task_slug_punctuation_and_spaces() {
        let reserved = HashSet::new();
        assert_eq!(
            derive_unique_task_slug("  Fix bug: off-by-one!", &reserved),
            "fix-bug"
        );
    }

    #[test]
    fn derive_unique_task_slug_long_description_takes_two_words_no_collision() {
        let reserved = HashSet::new();
        let desc = "one two three four five six seven eight nine ten eleven twelve";
        assert_eq!(derive_unique_task_slug(desc, &reserved), "one-two");
    }

    fn init_git_repo(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn git_status(path: &Path) -> String {
        let out = crate::env::git()
            .args(["status", "--porcelain"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn git_head_subject(path: &Path) -> String {
        let out = crate::env::git()
            .args(["log", "-1", "--pretty=%s"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[test]
    fn commit_project_config_change_commits_cairn_tree() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        init_git_repo(repo);
        std::fs::create_dir_all(repo.join(".cairn")).unwrap();
        std::fs::write(
            repo.join(".cairn").join("config.yaml"),
            "setupCommands: []\n",
        )
        .unwrap();

        let sha = commit_project_config_change(repo, "cairn: test commit");
        assert!(sha.is_some(), "expected a commit to be made");
        assert!(git_status(repo).is_empty(), "repo left dirty");
        assert_eq!(git_head_subject(repo), "cairn: test commit");
    }

    #[test]
    fn commit_project_config_change_no_op_outside_repo() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        std::fs::create_dir_all(dir.join(".cairn")).unwrap();
        std::fs::write(dir.join(".cairn").join("config.yaml"), "x: 1\n").unwrap();
        assert!(commit_project_config_change(dir, "cairn: test").is_none());
    }

    #[test]
    fn commit_project_config_change_no_op_when_nothing_changed() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        init_git_repo(repo);
        assert!(commit_project_config_change(repo, "cairn: nothing").is_none());
    }

    #[test]
    fn commit_config_paths_stages_only_touched_path() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        init_git_repo(repo);
        std::fs::create_dir_all(repo.join("agents")).unwrap();
        std::fs::write(repo.join("agents").join("planner.md"), "agent").unwrap();
        // Unrelated dirty state in the same repo must be left untouched.
        std::fs::write(repo.join("unrelated.txt"), "dirty").unwrap();

        let shas = commit_config_paths(
            &[repo.join("agents").join("planner.md")],
            "cairn: update agent planner",
        );
        assert_eq!(shas.len(), 1, "expected exactly one commit");
        assert_eq!(git_head_subject(repo), "cairn: update agent planner");

        let status = git_status(repo);
        assert!(
            status.contains("unrelated.txt"),
            "unrelated file should remain dirty: {status:?}"
        );
        assert!(
            !status.contains("planner.md"),
            "planner.md should have been committed: {status:?}"
        );
    }

    #[test]
    fn commit_config_paths_stages_a_deletion() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        init_git_repo(repo);
        let file = repo.join("agents").join("planner.md");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, "agent").unwrap();
        commit_config_paths(&[file.clone()], "cairn: create agent planner");

        std::fs::remove_file(&file).unwrap();
        let shas = commit_config_paths(&[file.clone()], "cairn: delete agent planner");
        assert_eq!(shas.len(), 1, "deletion should produce a commit");
        assert_eq!(git_head_subject(repo), "cairn: delete agent planner");
        assert!(
            git_status(repo).is_empty(),
            "repo should be clean after committing the deletion"
        );
    }

    #[test]
    fn commit_config_paths_no_op_outside_repo() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        std::fs::create_dir_all(dir.join("agents")).unwrap();
        let file = dir.join("agents").join("planner.md");
        std::fs::write(&file, "agent").unwrap();
        assert!(
            commit_config_paths(&[file], "cairn: create agent planner").is_empty(),
            "a path outside any git work tree must be a no-op"
        );
    }

    #[test]
    fn commit_config_paths_commits_each_repo_once() {
        let temp = tempfile::tempdir().unwrap();
        let repo_a = temp.path().join("a");
        let repo_b = temp.path().join("b");
        std::fs::create_dir_all(repo_a.join("agents")).unwrap();
        std::fs::create_dir_all(repo_b.join("recipes")).unwrap();
        init_git_repo(&repo_a);
        init_git_repo(&repo_b);
        let a_file = repo_a.join("agents").join("planner.md");
        let b_file = repo_b.join("recipes").join("build.yaml");
        std::fs::write(&a_file, "agent").unwrap();
        std::fs::write(&b_file, "recipe").unwrap();

        let shas = commit_config_paths(&[a_file, b_file], "cairn: bulk");
        assert_eq!(shas.len(), 2, "each repo should get its own commit");
        assert_eq!(git_head_subject(&repo_a), "cairn: bulk");
        assert_eq!(git_head_subject(&repo_b), "cairn: bulk");
    }
}
