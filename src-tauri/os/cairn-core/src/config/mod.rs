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
pub mod check_exemption;
pub mod contextual_packages;
pub mod credential_witness;
pub mod dev_commands;
pub mod keybinds;
pub mod mcp_import;
pub mod mcp_servers;
pub mod mcp_setup;
pub mod mcp_tools;
pub mod model_routing;
pub mod pack;
pub mod pdf;
pub mod presets;
pub mod project_settings;
pub mod provider_options;
pub mod recipes;
pub mod responses;
pub mod routes;
pub mod secrets;
pub mod settings;
pub mod skill_archive;
pub mod skill_fetch;
pub mod skills;
pub mod snapshot_migrate;
pub mod web_fetch;
pub mod web_search;
pub mod workflows;
pub mod yaml_edit;
pub use cairn_config::files::*;

#[cfg(test)]
use cairn_packs::restore::mirror_bundle_resources_with;
pub use cairn_packs::{bundled_resource_ids, mirror_bundle_resources, RestoreResult};
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
/// edits land in the user's canonical repo — not the runner logical head — so without
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
fn commit_project_config_change(project_path: &Path, msg: &str) -> Option<String> {
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

/// Ensure config directories exist
pub fn ensure_config_dirs(config_dir: &Path) -> Result<(), String> {
    for dir in ["recipes", "agents", "responses", "routes", "skills"] {
        let path = config_dir.join(dir);
        std::fs::create_dir_all(&path)
            .map_err(|e| format!("Failed to create directory {:?}: {}", path, e))?;
    }

    Ok(())
}

/// Generate a slug from a name, preserving Unicode letters and digits.
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

/// Get a recipe by ID from files (for use by execution and snapshot builders).
///
/// This loads recipes from YAML files rather than the database, since files
/// are the source of truth for recipe configuration.
pub(crate) fn get_recipe_from_files(
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
fn file_recipe_to_recipe(
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

pub(crate) use cairn_config::slugify_resource_segment;

#[cfg(test)]
mod tests {
    use super::*;

    /// Total restore replaces shipped workflow packages while preserving
    /// unrelated user-authored packages.
    #[test]
    fn restore_mirror_replaces_shipped_workflows_only() {
        use crate::services::{RealFileSystem, RealGitClient};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let resource_dir = temp.path().join("resources");
        let home = temp.path().join("home");

        // The default `core` pack ships an agent and a workflow.
        let core = resource_dir.join("packs/core");
        std::fs::create_dir_all(core.join("agents")).unwrap();
        std::fs::write(
            core.join("cairn-pack.yaml"),
            "id: core\nname: Core\nversion: 1.0.0\ndefault: true\n",
        )
        .unwrap();
        std::fs::write(core.join("agents/explore.md"), "bundle\n").unwrap();
        std::fs::create_dir_all(core.join("workflows/fan-out")).unwrap();
        std::fs::write(
            core.join("workflows/fan-out/workflow.yaml"),
            "name: Fan Out\ndescription: d\n",
        )
        .unwrap();

        // Workspace has a user-authored workflow and an edited fan-out copy.
        std::fs::create_dir_all(home.join("workflows/my-flow")).unwrap();
        std::fs::write(
            home.join("workflows/my-flow/workflow.yaml"),
            "name: Mine\ndescription: user\n",
        )
        .unwrap();
        std::fs::create_dir_all(home.join("workflows/fan-out")).unwrap();
        std::fs::write(home.join("workflows/fan-out/main.ts"), "// user edit\n").unwrap();

        mirror_bundle_resources_with(&RealGitClient, &RealFileSystem, &resource_dir, &home)
            .unwrap();

        // Agents were reset to the bundle…
        assert_eq!(
            std::fs::read_to_string(home.join("agents/explore.md")).unwrap(),
            "bundle\n"
        );
        // The unrelated user-authored package remains, while the shipped package
        // is deliberately reset as part of the confirmed total restore.
        assert!(home.join("workflows/my-flow/workflow.yaml").exists());
        assert_eq!(
            std::fs::read_to_string(home.join("workflows/fan-out/workflow.yaml")).unwrap(),
            "name: Fan Out\ndescription: d\n"
        );
        assert!(!home.join("workflows/fan-out/main.ts").exists());

        assert_eq!(
            bundled_resource_ids(&resource_dir, &home, "agents").unwrap(),
            vec!["explore".to_string()]
        );
    }

    /// A restore brings back what this workspace INSTALLED, not everything the
    /// app ships. An optional pack the user never installed must not appear.
    /// "Restore bundled defaults" is a deliberate click that supersedes an
    /// earlier uninstall of a DEFAULT pack. What it must never do is put the
    /// files back while leaving the pack uninstalled: those files would be
    /// claimed by no pack, tracked by nothing, and skipped by every later sync.
    #[test]
    fn restore_after_uninstalling_everything_reinstalls_coherently() {
        use crate::services::{RealFileSystem, RealGitClient};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let resource_dir = temp.path().join("resources");
        let home = temp.path().join("home");
        let core = resource_dir.join("packs/core");
        std::fs::create_dir_all(core.join("agents")).unwrap();
        std::fs::write(
            core.join("cairn-pack.yaml"),
            "id: core\nname: Core\nversion: 1.0.0\ndefault: true\n",
        )
        .unwrap();
        std::fs::write(core.join("agents/explore.md"), "shipped\n").unwrap();

        let git = RealGitClient;
        let fs = RealFileSystem;
        crate::workspace::bundle::sync_workspace_packs(&git, &fs, &resource_dir, &home, "1.0.0")
            .unwrap();
        crate::workspace::bundle::uninstall_pack(&git, &fs, &home, "core").unwrap();
        assert!(!home.join("agents/explore.md").exists());

        mirror_bundle_resources_with(&git, &fs, &resource_dir, &home).unwrap();

        assert!(home.join("agents/explore.md").exists());
        assert!(
            pack::lock::read_lock(&home, "core").is_some(),
            "restored files must be claimed by their pack"
        );
        assert!(
            !pack::lock::is_uninstalled(&home, "core"),
            "restoring supersedes the earlier uninstall rather than contradicting it"
        );

        // And the next ordinary sync keeps it, rather than skipping a pack it
        // still considers removed.
        crate::workspace::bundle::sync_workspace_packs(&git, &fs, &resource_dir, &home, "1.0.0")
            .unwrap();
        assert!(pack::lock::read_lock(&home, "core").is_some());
        assert!(home.join("agents/explore.md").exists());

        let lock = pack::lock::read_lock(&home, "core").unwrap();
        assert!(lock
            .item(pack::PackItemKind::Agent, "explore")
            .is_some_and(|item| item.content_hash.is_some() && !item.forked));
    }

    /// A restore overrides every user decision within its scope, uniformly. An
    /// item removal is not an exception: undoing one otherwise has no route a
    /// user can reach without driving Cairn through an agent, and re-removing
    /// it afterwards is a single click.
    #[test]
    fn restore_undoes_an_item_removal_too() {
        use crate::services::{RealFileSystem, RealGitClient};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let resource_dir = temp.path().join("resources");
        let home = temp.path().join("home");
        let core = resource_dir.join("packs/core");
        std::fs::create_dir_all(core.join("agents")).unwrap();
        std::fs::write(
            core.join("cairn-pack.yaml"),
            "id: core\nname: Core\nversion: 1.0.0\ndefault: true\n",
        )
        .unwrap();
        std::fs::write(core.join("agents/explore.md"), "shipped\n").unwrap();
        std::fs::write(core.join("agents/plan.md"), "shipped\n").unwrap();

        let git = RealGitClient;
        let fs = RealFileSystem;
        crate::workspace::bundle::sync_workspace_packs(&git, &fs, &resource_dir, &home, "1.0.0")
            .unwrap();

        crate::config::agents::delete_agent(&home, "plan", None).unwrap();
        assert!(!home.join("agents/plan.md").exists());
        assert!(pack::lock::read_lock(&home, "core")
            .unwrap()
            .is_removed(pack::PackItemKind::Agent, "plan"));
        mirror_bundle_resources_with(&git, &fs, &resource_dir, &home).unwrap();

        assert!(home.join("agents/plan.md").exists());
        assert!(
            pack::lock::read_lock(&home, "core")
                .unwrap()
                .removed
                .is_empty(),
            "the removal record is cleared, so the next sync keeps the item"
        );
        crate::workspace::bundle::sync_workspace_packs(&git, &fs, &resource_dir, &home, "1.0.0")
            .unwrap();
        assert!(home.join("agents/plan.md").exists());
    }

    #[test]
    fn restore_mirror_only_reinstates_installed_packs() {
        use crate::services::{RealFileSystem, RealGitClient};
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let resource_dir = temp.path().join("resources");
        let home = temp.path().join("home");

        for (id, default) in [("core", true), ("matlab", false)] {
            let root = resource_dir.join("packs").join(id);
            std::fs::create_dir_all(root.join("agents")).unwrap();
            std::fs::write(
                root.join("cairn-pack.yaml"),
                format!("id: {id}\nname: {id}\nversion: 1.0.0\ndefault: {default}\n"),
            )
            .unwrap();
            std::fs::write(root.join(format!("agents/{id}.md")), "shipped\n").unwrap();
        }

        // Only `core` is locked as installed.
        std::fs::create_dir_all(home.join("packs/core")).unwrap();
        std::fs::write(
            home.join("packs/core/pack.yaml"),
            "cairnVersion: 1\nid: core\nname: Core\nversion: 1.0.0\ninstalledAt: now\ncontentHash: h\nsource:\n  kind: bundled\n",
        )
        .unwrap();

        mirror_bundle_resources_with(&RealGitClient, &RealFileSystem, &resource_dir, &home)
            .unwrap();

        assert!(home.join("agents/core.md").exists());
        assert!(
            !home.join("agents/matlab.md").exists(),
            "a restore must not install a pack the user never chose"
        );
        assert_eq!(
            bundled_resource_ids(&resource_dir, &home, "agents").unwrap(),
            vec!["core".to_string()]
        );
    }

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

    #[cfg(unix)]
    #[test]
    fn commit_config_paths_handles_symlinked_checkout_and_preserves_leaf_identity() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        init_git_repo(&repo);
        let alias = temp.path().join("alias");
        symlink(&repo, &alias).unwrap();
        let outside = temp.path().join("outside");
        std::fs::write(&outside, "outside the repository").unwrap();
        let file = alias.join("settings.yaml");
        let link = alias.join("instructions.md");
        std::fs::write(&file, "before").unwrap();
        symlink(&outside, &link).unwrap();

        let paths = [file.clone(), link.clone()];
        assert_eq!(commit_config_paths(&paths, "seed through alias").len(), 1);
        assert!(git_status(&repo).is_empty());
        let tree = crate::env::git()
            .args(["ls-tree", "HEAD", "instructions.md"])
            .current_dir(&repo)
            .output()
            .unwrap();
        assert!(tree.status.success());
        assert!(String::from_utf8_lossy(&tree.stdout).starts_with("120000"));

        std::fs::remove_file(&file).unwrap();
        std::fs::remove_file(&link).unwrap();
        symlink("another-target", &link).unwrap();
        assert_eq!(
            commit_config_paths(&paths, "delete and retarget through alias").len(),
            1
        );
        assert!(git_status(&repo).is_empty());
        assert_eq!(
            std::fs::read_to_string(outside).unwrap(),
            "outside the repository"
        );
    }

    #[test]
    fn commit_config_paths_stages_only_touched_path() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        init_git_repo(repo);
        std::fs::create_dir_all(repo.join("agents")).unwrap();
        std::fs::write(repo.join("agents").join("planner.md"), "agent").unwrap();
        // Unrelated user work in both states an app-authored commit could
        // disturb: already staged, and merely present. Neither may enter the
        // commit, and neither may change state because of it.
        std::fs::write(repo.join("staged.txt"), "staged").unwrap();
        assert!(crate::env::git()
            .args(["add", "--", "staged.txt"])
            .current_dir(repo)
            .status()
            .unwrap()
            .success());
        std::fs::write(repo.join("untracked.txt"), "dirty").unwrap();

        let shas = commit_config_paths(
            &[repo.join("agents").join("planner.md")],
            "cairn: update agent planner",
        );
        assert_eq!(shas.len(), 1, "expected exactly one commit");
        assert_eq!(git_head_subject(repo), "cairn: update agent planner");

        let status = git_status(repo);
        assert!(
            status.contains("A  staged.txt"),
            "pre-staged user work should stay staged: {status:?}"
        );
        assert!(
            status.contains("?? untracked.txt"),
            "untracked user work should stay untracked: {status:?}"
        );
        let committed_files = crate::env::git()
            .args(["show", "--pretty=", "--name-only", "HEAD"])
            .current_dir(repo)
            .output()
            .unwrap();
        let committed = String::from_utf8_lossy(&committed_files.stdout);
        assert!(
            !committed.contains("staged.txt") && !committed.contains("untracked.txt"),
            "only the named path belongs in the commit: {committed:?}"
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
        commit_config_paths(std::slice::from_ref(&file), "cairn: create agent planner");

        std::fs::remove_file(&file).unwrap();
        let shas = commit_config_paths(std::slice::from_ref(&file), "cairn: delete agent planner");
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

    fn init_bare_origin(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "--bare", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn set_origin(repo: &Path, origin: &Path) {
        assert!(crate::env::git()
            .args(["remote", "add", "origin"])
            .arg(origin)
            .current_dir(repo)
            .status()
            .unwrap()
            .success());
    }

    fn git_rev(repo: &Path, rev: &str) -> String {
        let out = crate::env::git()
            .args(["rev-parse", rev])
            .current_dir(repo)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn current_branch(repo: &Path) -> String {
        let out = crate::env::git()
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .current_dir(repo)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// Build a work repo whose `origin` is a fresh bare repo. Returns both roots.
    fn repo_with_bare_origin(tmp: &Path) -> (PathBuf, PathBuf) {
        let origin = tmp.join("origin.git");
        std::fs::create_dir_all(&origin).unwrap();
        init_bare_origin(&origin);
        let repo = tmp.join("ws");
        std::fs::create_dir_all(&repo).unwrap();
        init_git_repo(&repo);
        set_origin(&repo, &origin);
        (repo, origin)
    }

    #[test]
    fn push_workspace_repo_best_effort_advances_origin() {
        let temp = tempfile::tempdir().unwrap();
        let (repo, origin) = repo_with_bare_origin(temp.path());

        std::fs::write(repo.join("settings.yaml"), "a: 1\n").unwrap();
        let commits = commit_config_paths(&[repo.join("settings.yaml")], "cairn: update settings");
        assert_eq!(commits.len(), 1);

        push_workspace_repo_best_effort(&repo);

        let branch = current_branch(&repo);
        assert_eq!(
            git_rev(&origin, &format!("refs/heads/{branch}")),
            git_rev(&repo, "HEAD"),
            "origin branch should advance to the pushed commit"
        );
    }

    #[test]
    fn push_workspace_repo_best_effort_no_remote_is_noop() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        init_git_repo(repo);
        std::fs::write(repo.join("settings.yaml"), "a: 1\n").unwrap();
        commit_config_paths(&[repo.join("settings.yaml")], "cairn: update settings");
        // No origin configured: must return silently (nothing to assert but no panic).
        push_workspace_repo_best_effort(repo);
    }

    #[test]
    fn push_workspace_repo_best_effort_never_force_pushes() {
        let temp = tempfile::tempdir().unwrap();
        let (repo, origin) = repo_with_bare_origin(temp.path());

        std::fs::write(repo.join("settings.yaml"), "a: 1\n").unwrap();
        commit_config_paths(&[repo.join("settings.yaml")], "cairn: c1");
        let c1 = git_rev(&repo, "HEAD");
        push_workspace_repo_best_effort(&repo);

        std::fs::write(repo.join("settings.yaml"), "a: 2\n").unwrap();
        commit_config_paths(&[repo.join("settings.yaml")], "cairn: c2");
        push_workspace_repo_best_effort(&repo);

        let branch = current_branch(&repo);
        let origin_tip = git_rev(&origin, &format!("refs/heads/{branch}"));
        assert_eq!(
            origin_tip,
            git_rev(&repo, "HEAD"),
            "origin at c2 before rewind"
        );

        // Rewind the work repo behind origin so any push would move origin
        // backwards — a non-fast-forward git rejects unless forced.
        assert!(crate::env::git()
            .args(["reset", "--hard"])
            .arg(&c1)
            .current_dir(&repo)
            .status()
            .unwrap()
            .success());

        push_workspace_repo_best_effort(&repo);

        assert_eq!(
            git_rev(&origin, &format!("refs/heads/{branch}")),
            origin_tip,
            "origin tip must be unchanged; best-effort push must never force"
        );
    }

    #[test]
    fn commit_and_maybe_push_commits_only_touched_path() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        init_git_repo(repo);
        std::fs::write(repo.join("settings.yaml"), "a: 1\n").unwrap();
        // Unrelated staged state in the same repo must be left out of the
        // app-authored commit and remain staged for the user's next commit.
        std::fs::write(repo.join("unrelated.txt"), "dirty").unwrap();
        assert!(crate::env::git()
            .args(["add", "--", "unrelated.txt"])
            .current_dir(repo)
            .status()
            .unwrap()
            .success());

        // push_root = None: commit only, no push attempted.
        commit_and_maybe_push(
            &[repo.join("settings.yaml")],
            "cairn: update settings",
            None,
        );

        assert_eq!(git_head_subject(repo), "cairn: update settings");
        let status = git_status(repo);
        assert!(
            status.contains("unrelated.txt"),
            "unrelated file should remain dirty: {status:?}"
        );
        assert!(
            !status.contains("settings.yaml"),
            "settings.yaml should be committed: {status:?}"
        );
    }
}
