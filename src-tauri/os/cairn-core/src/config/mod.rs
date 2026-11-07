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
pub mod keychain_read;
pub mod machine_services;
pub mod mcp_import;
pub mod mcp_servers;
pub mod mcp_setup;
pub mod mcp_tools;
pub mod pack;
pub mod pdf;
pub mod project_settings;
pub mod recipes;
pub mod responses;
pub mod routes;
pub mod secrets;
pub mod settings;
pub mod skill_archive;
pub mod skill_package;
pub mod skills;
pub mod snapshot_migrate;
pub mod workflows;
pub mod yaml_edit;
pub use cairn_config::files::*;
pub use cairn_config::library;

use cairn_config::library::LibraryRoots;
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

    let file_recipe = recipes::get_recipe(&LibraryRoots::at(config_dir, project_path), recipe_id)?
        .ok_or_else(|| format!("Recipe not found: {}", recipe_id))?;

    log::info!(
        "get_recipe_from_files: loaded file_recipe with {} nodes, {} edges, file_path={:?}",
        file_recipe.recipe.nodes.len(),
        file_recipe.recipe.edges.len(),
        file_recipe.file_path
    );

    // Determine workspace_id and project_id based on scope
    let (ws_id, proj_id) = if file_recipe.is_project_scoped() {
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
    let project_scoped = file_recipe.is_project_scoped();
    let mut recipe = file_recipe.recipe;
    // Set workspace_id to "default" for workspace-scoped recipes if not provided
    recipe.workspace_id = if !project_scoped {
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

    // Load agents
    let mut agents: HashMap<String, AgentSnapshot> = HashMap::new();
    for agent_id in agent_ids {
        if agents.contains_key(&agent_id) {
            continue;
        }

        if let Ok(Some(file_agent)) = agents::get_agent(
            &LibraryRoots::at(config_dir, Some(&project_path)),
            &agent_id,
        ) {
            agents.insert(agent_id.clone(), file_agent.snapshot());
        }
    }

    Ok((recipe_snapshot, agents))
}

pub(crate) use cairn_config::slugify_resource_segment;

/// What every bare name of every library kind resolves to across `roots`:
/// the origin in force, or the collision that refuses it. A project row that
/// is skipped or unconfirmed is left out, since it is in force for nothing.
pub(crate) fn library_index(
    roots: &library::LibraryRoots,
) -> std::collections::BTreeMap<
    (library::LibraryKind, String),
    (library::Origin, library::ListedStatus),
> {
    use library::{LibraryKind, ListedStatus};
    fn rows<T>(
        kind: LibraryKind,
        listed: Result<Vec<library::Listed<T>>, String>,
    ) -> Vec<((LibraryKind, String), (library::Origin, ListedStatus))> {
        listed
            .unwrap_or_default()
            .into_iter()
            .filter(|row| {
                matches!(
                    row.status,
                    ListedStatus::Active | ListedStatus::Collision { .. }
                )
            })
            .map(|row| ((kind, row.name), (row.origin, row.status)))
            .collect()
    }
    let mut index = std::collections::BTreeMap::new();
    index.extend(rows(LibraryKind::Agent, agents::list_agent_rows(roots)));
    index.extend(rows(LibraryKind::Recipe, recipes::list_recipe_rows(roots)));
    index.extend(rows(LibraryKind::Skill, skills::list_skill_rows(roots)));
    index.extend(rows(
        LibraryKind::Response,
        responses::list_response_rows(roots),
    ));
    index.extend(rows(LibraryKind::Route, routes::list_route_rows(roots)));
    index.extend(rows(
        LibraryKind::Workflow,
        workflows::list_workflow_rows(roots),
    ));
    index
}

/// One definition in a project's `.cairn/`, as the operator reviews it before
/// trusting the project or confirming the definition's authority.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectDefinition {
    pub kind: library::LibraryKind,
    pub name: String,
    /// The definition's authority projection as it stands, or `None` when the
    /// file does not parse (and so cannot be confirmed).
    pub authority: Option<serde_json::Map<String, serde_json::Value>>,
    /// SHA-256 of `authority`'s canonical JSON: what a confirmation binds.
    pub digest: Option<String>,
    /// Why the file did not parse.
    pub error: Option<String>,
    /// Whether the definition is in force, skipped as untrusted, or waiting on
    /// a confirmation of the named fields.
    pub status: &'static str,
    pub unconfirmed_fields: Vec<String>,
}

/// Every definition the project in `roots` holds, of every kind, whatever its
/// trust: the list the operator reviews.
pub fn project_definitions(roots: &library::LibraryRoots) -> Vec<ProjectDefinition> {
    use library::{LibraryItem, LibraryKind, ListedStatus};
    fn rows<T: LibraryItem>(
        kind: LibraryKind,
        listed: Result<Vec<library::Listed<T>>, String>,
    ) -> Vec<ProjectDefinition> {
        listed
            .unwrap_or_default()
            .into_iter()
            .filter(|row| row.origin.is_project())
            .map(|row| {
                let (authority, error) = match &row.entry {
                    ConfigResult::Ok(item) => (Some(item.authority()), None),
                    ConfigResult::Err { error, .. } => (None, Some(error.clone())),
                };
                ProjectDefinition {
                    kind,
                    name: row.name,
                    digest: authority.as_ref().map(library::authority_digest),
                    authority,
                    error,
                    status: row.status.as_str(),
                    unconfirmed_fields: match row.status {
                        ListedStatus::Unconfirmed { fields } => fields,
                        _ => Vec::new(),
                    },
                }
            })
            .collect()
    }
    let mut all = Vec::new();
    all.extend(rows(LibraryKind::Agent, agents::list_agent_rows(roots)));
    all.extend(rows(LibraryKind::Recipe, recipes::list_recipe_rows(roots)));
    all.extend(rows(LibraryKind::Skill, skills::list_skill_rows(roots)));
    all.extend(rows(
        LibraryKind::Response,
        responses::list_response_rows(roots),
    ));
    all.extend(rows(LibraryKind::Route, routes::list_route_rows(roots)));
    all.extend(rows(
        LibraryKind::Workflow,
        workflows::list_workflow_rows(roots),
    ));
    all
}

/// Roots for tests about layering rather than trust: the project trusted, and
/// every definition it holds today confirmed exactly as it stands.
#[cfg(test)]
pub(crate) fn trusted_roots(config_dir: &Path, project: &Path) -> library::LibraryRoots {
    use library::ProjectTrust;
    let layer = library::test_support::project(project, "demo", ProjectTrust::Untrusted);
    let roots = library::LibraryRoots::workspace(config_dir).with_project(layer);
    let confirmed = project_definitions(&roots)
        .into_iter()
        .filter_map(|definition| Some(((definition.kind, definition.name), definition.authority?)))
        .collect();
    library::LibraryRoots::workspace(config_dir).with_project(library::test_support::project(
        project,
        "demo",
        ProjectTrust::Trusted { confirmed },
    ))
}

/// Record in the runner's trust registry that the project checked out at
/// `project` is trusted with every definition it holds now confirmed, for tests
/// that exercise layering through the orchestrator rather than trust itself.
/// Call it after writing the definitions.
#[cfg(test)]
pub(crate) fn trust_for_test(config_dir: &Path, project: &Path, key: &str) {
    if let Some(layer) = trusted_roots(config_dir, project).project {
        library::trust::set(project, key, layer.trust);
    }
}

#[cfg(test)]
mod authority_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use cairn_config::library::{LibraryRoots, Origin};

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
            .args(["init", "-b", "main", "-q"])
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
            .args(["init", "-b", "main", "--bare", "-q"])
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
