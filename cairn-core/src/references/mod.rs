//! Project reference management — external git repos and local directories.
//!
//! References are configured per-project in `.cairn/config.yaml` but clones are
//! stored globally at `~/.cairn/references/{name}/`. Multiple projects can share
//! the same clone. Removing a reference from a project only updates config — the
//! clone persists for other projects.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// External project reference (git repo or local directory).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectReference {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// Status of a resolved reference.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceStatus {
    pub name: String,
    pub description: String,
    pub reference_type: ReferenceType,
    pub resolved_path: Option<String>,
    pub exists: bool,
}

/// A globally cloned reference (discovered by scanning the references directory).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalReference {
    pub name: String,
    pub path: String,
    /// Git remote URL, if this is a git clone.
    pub git_url: Option<String>,
    /// Description from contents.yaml.
    pub description: Option<String>,
}

/// Contents of `references/contents.yaml` — maps reference name to description.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ContentsFile(BTreeMap<String, String>);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ReferenceType {
    Git,
    Local,
}

/// Get the global references directory: `{config_dir}/references/`
pub fn get_references_dir(config_dir: &Path) -> PathBuf {
    config_dir.join("references")
}

/// Path to the global reference metadata file.
fn contents_path(config_dir: &Path) -> PathBuf {
    get_references_dir(config_dir).join("contents.yaml")
}

/// Load the contents.yaml metadata file.
fn load_contents(config_dir: &Path) -> ContentsFile {
    let path = contents_path(config_dir);
    if !path.exists() {
        return ContentsFile::default();
    }
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_yaml::from_str(&s).ok())
        .unwrap_or_default()
}

/// Save the contents.yaml metadata file.
fn save_contents(config_dir: &Path, contents: &ContentsFile) -> Result<(), String> {
    let dir = get_references_dir(config_dir);
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("Failed to create references directory: {}", e))?;
    let yaml = serde_yaml::to_string(contents)
        .map_err(|e| format!("Failed to serialize contents: {}", e))?;
    std::fs::write(contents_path(config_dir), yaml)
        .map_err(|e| format!("Failed to write contents.yaml: {}", e))
}

/// Save a reference's description to the global contents.yaml.
pub fn save_reference_description(
    config_dir: &Path,
    name: &str,
    description: &str,
) -> Result<(), String> {
    let mut contents = load_contents(config_dir);
    if description.is_empty() {
        contents.0.remove(name);
    } else {
        contents.0.insert(name.to_string(), description.to_string());
    }
    save_contents(config_dir, &contents)
}

/// Resolve a reference to its absolute path on disk.
/// - Git references: `{config_dir}/references/{name}/`
/// - Local references: expanded `~` path
///
/// Returns `None` if the path doesn't exist.
pub fn resolve_reference_path(config_dir: &Path, reference: &ProjectReference) -> Option<PathBuf> {
    let path = if reference.git.is_some() {
        get_references_dir(config_dir).join(&reference.name)
    } else if let Some(ref local_path) = reference.path {
        expand_tilde(local_path)
    } else {
        return None;
    };

    if path.exists() {
        Some(path)
    } else {
        None
    }
}

/// Clone a git reference to the global directory.
/// Uses `--depth 1` for speed. If the clone already exists, returns it as-is.
pub fn clone_reference(config_dir: &Path, reference: &ProjectReference) -> Result<PathBuf, String> {
    let git_url = reference
        .git
        .as_ref()
        .ok_or_else(|| "Reference has no git URL".to_string())?;

    let clone_dir = get_references_dir(config_dir).join(&reference.name);

    // Create parent directory
    if let Some(parent) = clone_dir.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create references directory: {}", e))?;
    }

    // Don't clone if already exists
    if clone_dir.exists() {
        return Ok(clone_dir);
    }

    let mut args = vec!["clone", "--depth", "1"];

    if let Some(ref branch) = reference.branch {
        args.push("--branch");
        args.push(branch);
    }

    args.push(git_url);
    args.push(clone_dir.to_str().unwrap_or_default());

    let output = std::process::Command::new("git")
        .args(&args)
        .output()
        .map_err(|e| format!("Failed to run git clone: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git clone failed: {}", stderr.trim()));
    }

    log::info!("Cloned reference '{}' to {:?}", reference.name, clone_dir);
    Ok(clone_dir)
}

/// Refresh a git reference by pulling latest changes.
pub fn refresh_reference(config_dir: &Path, reference: &ProjectReference) -> Result<(), String> {
    if reference.git.is_none() {
        return Err("Cannot refresh a local reference".to_string());
    }

    let clone_dir = get_references_dir(config_dir).join(&reference.name);

    if !clone_dir.exists() {
        return Err(format!("Reference '{}' not cloned yet", reference.name));
    }

    let output = std::process::Command::new("git")
        .args(["pull"])
        .current_dir(&clone_dir)
        .output()
        .map_err(|e| format!("Failed to run git pull: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git pull failed: {}", stderr.trim()));
    }

    log::info!("Refreshed reference '{}'", reference.name);
    Ok(())
}

/// Remove a global clone directory. Only call for cleanup — removing a reference
/// from a project should NOT delete the clone (other projects may use it).
pub fn remove_reference_clone(config_dir: &Path, name: &str) -> Result<(), String> {
    let clone_dir = get_references_dir(config_dir).join(name);

    if clone_dir.exists() {
        std::fs::remove_dir_all(&clone_dir)
            .map_err(|e| format!("Failed to remove reference clone: {}", e))?;
        log::info!("Removed reference clone '{}'", name);
    }

    Ok(())
}

/// Get status for all configured references.
/// Description is resolved from: per-project config > contents.yaml > empty.
pub fn list_reference_status(
    config_dir: &Path,
    references: &[ProjectReference],
) -> Vec<ReferenceStatus> {
    let contents = load_contents(config_dir);
    references
        .iter()
        .map(|r| {
            let resolved = resolve_reference_path(config_dir, r);
            let description = r
                .description
                .clone()
                .or_else(|| contents.0.get(&r.name).cloned())
                .unwrap_or_default();
            ReferenceStatus {
                name: r.name.clone(),
                description,
                reference_type: if r.git.is_some() {
                    ReferenceType::Git
                } else {
                    ReferenceType::Local
                },
                resolved_path: resolved.as_ref().map(|p| p.to_string_lossy().to_string()),
                exists: resolved.is_some(),
            }
        })
        .collect()
}

/// List all globally cloned references by scanning `{config_dir}/references/`.
/// Descriptions are read from `contents.yaml`.
pub fn list_global_references(config_dir: &Path) -> Vec<GlobalReference> {
    let references_dir = get_references_dir(config_dir);
    if !references_dir.exists() {
        return Vec::new();
    }

    let contents = load_contents(config_dir);

    let entries = match std::fs::read_dir(&references_dir) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };

    let mut results = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };

        // Try to get git remote URL
        let git_url = std::process::Command::new("git")
            .args(["remote", "get-url", "origin"])
            .current_dir(&path)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());

        let description = contents.0.get(&name).cloned();

        results.push(GlobalReference {
            name,
            path: path.to_string_lossy().to_string(),
            git_url,
            description,
        });
    }

    results.sort_by(|a, b| a.name.cmp(&b.name));
    results
}

/// Build the "Project References" prompt section for agent injection.
/// Returns empty string if no references are available.
pub fn build_references_prompt(config_dir: &Path, references: &[ProjectReference]) -> String {
    let contents = load_contents(config_dir);
    let mut lines = Vec::new();
    for reference in references {
        if let Some(resolved) = resolve_reference_path(config_dir, reference) {
            let desc = reference
                .description
                .as_deref()
                .or_else(|| contents.0.get(&reference.name).map(|s| s.as_str()))
                .unwrap_or(&reference.name);
            lines.push(format!(
                "- **{}** (`{}`): {}",
                reference.name,
                resolved.display(),
                desc
            ));
        } else {
            log::warn!(
                "Reference '{}' path does not exist, omitting from prompt",
                reference.name
            );
        }
    }

    if lines.is_empty() {
        return String::new();
    }

    let mut section = String::from("## Project References\n\n");
    section.push_str(
        "The following reference directories are available. \
         Use absolute paths with Read, Glob, Grep, or Bash to search and read them.\n\n",
    );
    section.push_str(&lines.join("\n"));
    section
}

/// Expand `~` to the user's home directory.
fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn git_reference(name: &str, git: &str, description: Option<&str>) -> ProjectReference {
        ProjectReference {
            name: name.to_string(),
            git: Some(git.to_string()),
            path: None,
            description: description.map(str::to_string),
            branch: None,
        }
    }

    fn example_git_reference(name: &str, description: Option<&str>) -> ProjectReference {
        git_reference(
            name,
            &format!("https://example.com/{name}.git"),
            description,
        )
    }

    fn local_reference(
        name: &str,
        path: impl Into<String>,
        description: Option<&str>,
    ) -> ProjectReference {
        ProjectReference {
            name: name.to_string(),
            git: None,
            path: Some(path.into()),
            description: description.map(str::to_string),
            branch: None,
        }
    }

    fn invalid_reference(name: &str, description: Option<&str>) -> ProjectReference {
        ProjectReference {
            name: name.to_string(),
            git: None,
            path: None,
            description: description.map(str::to_string),
            branch: None,
        }
    }

    fn create_reference_clone(config_dir: &Path, name: &str) -> PathBuf {
        let clone_dir = get_references_dir(config_dir).join(name);
        std::fs::create_dir_all(&clone_dir).unwrap();
        clone_dir
    }

    fn create_child_dir(parent: &Path, name: &str) -> PathBuf {
        let dir = parent.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_get_references_dir() {
        let config_dir = Path::new("/home/user/.cairn");
        let result = get_references_dir(config_dir);
        assert_eq!(result, PathBuf::from("/home/user/.cairn/references"));
    }

    #[test]
    fn test_resolve_git_reference_missing() {
        let temp = TempDir::new().unwrap();
        let reference = git_reference(
            "openpnp",
            "https://github.com/openpnp/openpnp.git",
            Some("OpenPnP source"),
        );

        let result = resolve_reference_path(temp.path(), &reference);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_git_reference_exists() {
        let temp = TempDir::new().unwrap();
        let clone_dir = create_reference_clone(temp.path(), "openpnp");

        let reference = git_reference(
            "openpnp",
            "https://github.com/openpnp/openpnp.git",
            Some("OpenPnP source"),
        );

        let result = resolve_reference_path(temp.path(), &reference);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), clone_dir);
    }

    #[test]
    fn test_resolve_local_reference() {
        let temp = TempDir::new().unwrap();
        let local_dir = create_child_dir(temp.path(), "specs");

        let reference = local_reference(
            "specs",
            local_dir.to_string_lossy().to_string(),
            Some("Specs"),
        );

        let result = resolve_reference_path(temp.path(), &reference);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), local_dir);
    }

    #[test]
    fn test_resolve_local_reference_missing() {
        let temp = TempDir::new().unwrap();
        let reference = local_reference("specs", "/nonexistent/path", Some("Specs"));

        let result = resolve_reference_path(temp.path(), &reference);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_reference_no_git_no_path() {
        let temp = TempDir::new().unwrap();
        let reference = invalid_reference("bad", Some("Invalid"));

        let result = resolve_reference_path(temp.path(), &reference);
        assert!(result.is_none());
    }

    #[test]
    fn test_list_reference_status() {
        let temp = TempDir::new().unwrap();
        create_reference_clone(temp.path(), "existing");

        let references = vec![
            git_reference("existing", "https://example.com/repo.git", Some("Exists")),
            git_reference(
                "missing",
                "https://example.com/other.git",
                Some("Does not exist"),
            ),
        ];

        let statuses = list_reference_status(temp.path(), &references);
        assert_eq!(statuses.len(), 2);
        assert!(statuses[0].exists);
        assert!(statuses[0].resolved_path.is_some());
        assert!(!statuses[1].exists);
        assert!(statuses[1].resolved_path.is_none());
    }

    #[test]
    fn test_list_global_references() {
        let temp = TempDir::new().unwrap();
        let references_dir = get_references_dir(temp.path());

        // Create some fake reference dirs
        create_reference_clone(temp.path(), "alpha");
        create_reference_clone(temp.path(), "beta");
        // Create a file (should be ignored)
        std::fs::write(references_dir.join("not-a-dir.txt"), "").unwrap();

        let globals = list_global_references(temp.path());
        assert_eq!(globals.len(), 2);
        assert_eq!(globals[0].name, "alpha");
        assert_eq!(globals[1].name, "beta");
        // No git remote since these aren't real git repos
        assert!(globals[0].git_url.is_none());
    }

    #[test]
    fn test_list_global_references_empty() {
        let temp = TempDir::new().unwrap();
        let globals = list_global_references(temp.path());
        assert!(globals.is_empty());
    }

    #[test]
    fn test_build_references_prompt_with_existing() {
        let temp = TempDir::new().unwrap();
        let clone_dir = create_reference_clone(temp.path(), "docs");

        let references = vec![example_git_reference("docs", Some("Project documentation"))];

        let prompt = build_references_prompt(temp.path(), &references);
        assert!(prompt.contains("## Project References"));
        assert!(prompt.contains("**docs**"));
        assert!(prompt.contains("Project documentation"));
        assert!(prompt.contains(&clone_dir.to_string_lossy().to_string()));
    }

    #[test]
    fn test_build_references_prompt_empty_when_none_exist() {
        let temp = TempDir::new().unwrap();
        let references = vec![example_git_reference("missing", Some("Missing reference"))];

        let prompt = build_references_prompt(temp.path(), &references);
        assert!(prompt.is_empty());
    }

    #[test]
    fn test_remove_reference_clone() {
        let temp = TempDir::new().unwrap();
        let clone_dir = create_reference_clone(temp.path(), "repo");
        assert!(clone_dir.exists());

        remove_reference_clone(temp.path(), "repo").unwrap();
        assert!(!clone_dir.exists());
    }

    #[test]
    fn test_remove_reference_clone_nonexistent() {
        let temp = TempDir::new().unwrap();
        remove_reference_clone(temp.path(), "nonexistent").unwrap();
    }

    #[test]
    fn test_expand_tilde() {
        let result = expand_tilde("/absolute/path");
        assert_eq!(result, PathBuf::from("/absolute/path"));

        if let Some(home) = dirs::home_dir() {
            let result = expand_tilde("~/Documents/specs");
            assert_eq!(result, home.join("Documents/specs"));
        }
    }

    #[test]
    fn test_clone_reference_errors_without_git_url() {
        let temp = TempDir::new().unwrap();
        let reference = local_reference("local-only", "/some/path", Some("No git URL"));

        let result = clone_reference(temp.path(), &reference);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no git URL"));
    }

    #[test]
    fn test_clone_reference_returns_existing_without_recloning() {
        let temp = TempDir::new().unwrap();
        let clone_dir = create_reference_clone(temp.path(), "repo");
        // Place a marker file so we can verify it wasn't wiped
        std::fs::write(clone_dir.join("marker.txt"), "exists").unwrap();

        let reference = example_git_reference("repo", Some("Already cloned"));

        let result = clone_reference(temp.path(), &reference);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), clone_dir);
        // Marker file should still be there (no re-clone happened)
        assert!(clone_dir.join("marker.txt").exists());
    }

    #[test]
    fn test_refresh_reference_errors_for_local_reference() {
        let temp = TempDir::new().unwrap();
        let reference = local_reference("local", "/some/path", Some("Local dir"));

        let result = refresh_reference(temp.path(), &reference);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Cannot refresh a local reference"));
    }

    #[test]
    fn test_refresh_reference_errors_when_not_cloned() {
        let temp = TempDir::new().unwrap();
        let reference = git_reference(
            "not-cloned",
            "https://example.com/repo.git",
            Some("Missing clone"),
        );

        let result = refresh_reference(temp.path(), &reference);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not cloned yet"));
    }

    #[test]
    fn test_build_references_prompt_filters_missing() {
        let temp = TempDir::new().unwrap();
        // Create only the "docs" reference dir, not "missing"
        create_reference_clone(temp.path(), "docs");

        let references = vec![
            example_git_reference("docs", Some("Documentation")),
            example_git_reference("missing", Some("Should be filtered")),
        ];

        let prompt = build_references_prompt(temp.path(), &references);
        assert!(prompt.contains("**docs**"));
        assert!(prompt.contains("Documentation"));
        assert!(!prompt.contains("missing"));
        assert!(!prompt.contains("Should be filtered"));
    }

    #[test]
    fn test_list_reference_status_fields() {
        let temp = TempDir::new().unwrap();
        create_reference_clone(temp.path(), "repo");

        let local_dir = create_child_dir(temp.path(), "local-specs");

        let references = vec![
            example_git_reference("repo", Some("A git repo")),
            local_reference(
                "local-specs",
                local_dir.to_string_lossy().to_string(),
                Some("Local specs"),
            ),
        ];

        let statuses = list_reference_status(temp.path(), &references);
        assert_eq!(statuses.len(), 2);

        // Git reference
        assert_eq!(statuses[0].name, "repo");
        assert_eq!(statuses[0].description, "A git repo");
        assert!(matches!(statuses[0].reference_type, ReferenceType::Git));
        assert!(statuses[0].exists);

        // Local reference
        assert_eq!(statuses[1].name, "local-specs");
        assert_eq!(statuses[1].description, "Local specs");
        assert!(matches!(statuses[1].reference_type, ReferenceType::Local));
        assert!(statuses[1].exists);
    }

    #[test]
    fn test_list_reference_status_description_fallback() {
        let temp = TempDir::new().unwrap();
        create_reference_clone(temp.path(), "repo");

        // Save a description in contents.yaml
        save_reference_description(temp.path(), "repo", "From contents.yaml").unwrap();

        // ProjectReference without description — should fall back to contents.yaml
        let references = vec![example_git_reference("repo", None)];

        let statuses = list_reference_status(temp.path(), &references);
        assert_eq!(statuses[0].description, "From contents.yaml");
    }

    #[test]
    fn test_list_reference_status_description_config_takes_priority() {
        let temp = TempDir::new().unwrap();
        create_reference_clone(temp.path(), "repo");

        // Save a description in contents.yaml
        save_reference_description(temp.path(), "repo", "From contents.yaml").unwrap();

        // ProjectReference WITH description — config should win over contents.yaml
        let references = vec![example_git_reference("repo", Some("From config"))];

        let statuses = list_reference_status(temp.path(), &references);
        assert_eq!(statuses[0].description, "From config");
    }

    #[test]
    fn test_build_references_prompt_falls_back_to_name_without_description() {
        let temp = TempDir::new().unwrap();
        create_reference_clone(temp.path(), "my-repo");

        let references = vec![git_reference(
            "my-repo",
            "https://example.com/repo.git",
            None,
        )];

        let prompt = build_references_prompt(temp.path(), &references);
        assert!(prompt.contains("**my-repo**"));
        // Falls back to using the name as description
        assert!(prompt.contains("my-repo"));
    }

    #[test]
    fn test_build_references_prompt_empty_list() {
        let temp = TempDir::new().unwrap();
        let prompt = build_references_prompt(temp.path(), &[]);
        assert!(prompt.is_empty());
    }

    #[test]
    fn test_save_reference_description_empty_removes() {
        let temp = TempDir::new().unwrap();

        // Save then remove
        save_reference_description(temp.path(), "repo", "Some desc").unwrap();
        let contents = load_contents(temp.path());
        assert_eq!(contents.0.get("repo").unwrap(), "Some desc");

        save_reference_description(temp.path(), "repo", "").unwrap();
        let contents = load_contents(temp.path());
        assert!(!contents.0.contains_key("repo"));
    }

    #[test]
    fn test_list_global_references_includes_descriptions() {
        let temp = TempDir::new().unwrap();
        create_reference_clone(temp.path(), "myrepo");

        save_reference_description(temp.path(), "myrepo", "My repo description").unwrap();

        let globals = list_global_references(temp.path());
        assert_eq!(globals.len(), 1);
        assert_eq!(
            globals[0].description.as_deref(),
            Some("My repo description")
        );
    }
}
