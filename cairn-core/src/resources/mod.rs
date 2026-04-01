//! Project resource management — external git repos and local directories.
//!
//! Resources are configured per-project in `.cairn/config.yaml` but clones are
//! stored globally at `~/.cairn/resources/{name}/`. Multiple projects can share
//! the same clone. Removing a resource from a project only updates config — the
//! clone persists for other projects.

use crate::config::project_settings::Resource;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Status of a resolved resource.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceStatus {
    pub name: String,
    pub description: String,
    pub resource_type: ResourceType,
    pub resolved_path: Option<String>,
    pub exists: bool,
}

/// A globally cloned resource (discovered by scanning the resources directory).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalResource {
    pub name: String,
    pub path: String,
    /// Git remote URL, if this is a git clone.
    pub git_url: Option<String>,
    /// Description from contents.yaml.
    pub description: Option<String>,
}

/// Contents of `resources/contents.yaml` — maps resource name to description.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ContentsFile(BTreeMap<String, String>);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ResourceType {
    Git,
    Local,
}

/// Get the global resources directory: `{config_dir}/resources/`
pub fn get_resources_dir(config_dir: &Path) -> PathBuf {
    config_dir.join("resources")
}

/// Path to the global resource metadata file.
fn contents_path(config_dir: &Path) -> PathBuf {
    get_resources_dir(config_dir).join("contents.yaml")
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
    let dir = get_resources_dir(config_dir);
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("Failed to create resources directory: {}", e))?;
    let yaml = serde_yaml::to_string(contents)
        .map_err(|e| format!("Failed to serialize contents: {}", e))?;
    std::fs::write(contents_path(config_dir), yaml)
        .map_err(|e| format!("Failed to write contents.yaml: {}", e))
}

/// Save a resource's description to the global contents.yaml.
pub fn save_resource_description(
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

/// Resolve a resource to its absolute path on disk.
/// - Git resources: `{config_dir}/resources/{name}/`
/// - Local resources: expanded `~` path
///
/// Returns `None` if the path doesn't exist.
pub fn resolve_resource_path(config_dir: &Path, resource: &Resource) -> Option<PathBuf> {
    let path = if resource.git.is_some() {
        get_resources_dir(config_dir).join(&resource.name)
    } else if let Some(ref local_path) = resource.path {
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

/// Clone a git resource to the global directory.
/// Uses `--depth 1` for speed. If the clone already exists, returns it as-is.
pub fn clone_resource(config_dir: &Path, resource: &Resource) -> Result<PathBuf, String> {
    let git_url = resource
        .git
        .as_ref()
        .ok_or_else(|| "Resource has no git URL".to_string())?;

    let clone_dir = get_resources_dir(config_dir).join(&resource.name);

    // Create parent directory
    if let Some(parent) = clone_dir.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create resources directory: {}", e))?;
    }

    // Don't clone if already exists
    if clone_dir.exists() {
        return Ok(clone_dir);
    }

    let mut args = vec!["clone", "--depth", "1"];

    if let Some(ref branch) = resource.branch {
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

    log::info!("Cloned resource '{}' to {:?}", resource.name, clone_dir);
    Ok(clone_dir)
}

/// Refresh a git resource by pulling latest changes.
pub fn refresh_resource(config_dir: &Path, resource: &Resource) -> Result<(), String> {
    if resource.git.is_none() {
        return Err("Cannot refresh a local resource".to_string());
    }

    let clone_dir = get_resources_dir(config_dir).join(&resource.name);

    if !clone_dir.exists() {
        return Err(format!("Resource '{}' not cloned yet", resource.name));
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

    log::info!("Refreshed resource '{}'", resource.name);
    Ok(())
}

/// Remove a global clone directory. Only call for cleanup — removing a resource
/// from a project should NOT delete the clone (other projects may use it).
pub fn remove_resource_clone(config_dir: &Path, name: &str) -> Result<(), String> {
    let clone_dir = get_resources_dir(config_dir).join(name);

    if clone_dir.exists() {
        std::fs::remove_dir_all(&clone_dir)
            .map_err(|e| format!("Failed to remove resource clone: {}", e))?;
        log::info!("Removed resource clone '{}'", name);
    }

    Ok(())
}

/// Get status for all configured resources.
/// Description is resolved from: per-project config > contents.yaml > empty.
pub fn list_resource_status(config_dir: &Path, resources: &[Resource]) -> Vec<ResourceStatus> {
    let contents = load_contents(config_dir);
    resources
        .iter()
        .map(|r| {
            let resolved = resolve_resource_path(config_dir, r);
            let description = r
                .description
                .clone()
                .or_else(|| contents.0.get(&r.name).cloned())
                .unwrap_or_default();
            ResourceStatus {
                name: r.name.clone(),
                description,
                resource_type: if r.git.is_some() {
                    ResourceType::Git
                } else {
                    ResourceType::Local
                },
                resolved_path: resolved.as_ref().map(|p| p.to_string_lossy().to_string()),
                exists: resolved.is_some(),
            }
        })
        .collect()
}

/// List all globally cloned resources by scanning `{config_dir}/resources/`.
/// Descriptions are read from `contents.yaml`.
pub fn list_global_resources(config_dir: &Path) -> Vec<GlobalResource> {
    let resources_dir = get_resources_dir(config_dir);
    if !resources_dir.exists() {
        return Vec::new();
    }

    let contents = load_contents(config_dir);

    let entries = match std::fs::read_dir(&resources_dir) {
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

        results.push(GlobalResource {
            name,
            path: path.to_string_lossy().to_string(),
            git_url,
            description,
        });
    }

    results.sort_by(|a, b| a.name.cmp(&b.name));
    results
}

/// Build the "Project Resources" prompt section for agent injection.
/// Returns empty string if no resources are available.
pub fn build_resources_prompt(config_dir: &Path, resources: &[Resource]) -> String {
    let contents = load_contents(config_dir);
    let mut lines = Vec::new();
    for resource in resources {
        if let Some(resolved) = resolve_resource_path(config_dir, resource) {
            let desc = resource
                .description
                .as_deref()
                .or_else(|| contents.0.get(&resource.name).map(|s| s.as_str()))
                .unwrap_or(&resource.name);
            lines.push(format!(
                "- **{}** (`{}`): {}",
                resource.name,
                resolved.display(),
                desc
            ));
        } else {
            log::warn!(
                "Resource '{}' path does not exist, omitting from prompt",
                resource.name
            );
        }
    }

    if lines.is_empty() {
        return String::new();
    }

    let mut section = String::from("## Project Resources\n\n");
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

    #[test]
    fn test_get_resources_dir() {
        let config_dir = Path::new("/home/user/.cairn");
        let result = get_resources_dir(config_dir);
        assert_eq!(result, PathBuf::from("/home/user/.cairn/resources"));
    }

    #[test]
    fn test_resolve_git_resource_missing() {
        let temp = TempDir::new().unwrap();
        let resource = Resource {
            name: "openpnp".to_string(),
            git: Some("https://github.com/openpnp/openpnp.git".to_string()),
            path: None,
            description: Some("OpenPnP source".to_string()),
            branch: None,
        };

        let result = resolve_resource_path(temp.path(), &resource);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_git_resource_exists() {
        let temp = TempDir::new().unwrap();
        let clone_dir = get_resources_dir(temp.path()).join("openpnp");
        std::fs::create_dir_all(&clone_dir).unwrap();

        let resource = Resource {
            name: "openpnp".to_string(),
            git: Some("https://github.com/openpnp/openpnp.git".to_string()),
            path: None,
            description: Some("OpenPnP source".to_string()),
            branch: None,
        };

        let result = resolve_resource_path(temp.path(), &resource);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), clone_dir);
    }

    #[test]
    fn test_resolve_local_resource() {
        let temp = TempDir::new().unwrap();
        let local_dir = temp.path().join("specs");
        std::fs::create_dir_all(&local_dir).unwrap();

        let resource = Resource {
            name: "specs".to_string(),
            git: None,
            path: Some(local_dir.to_string_lossy().to_string()),
            description: Some("Specs".to_string()),
            branch: None,
        };

        let result = resolve_resource_path(temp.path(), &resource);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), local_dir);
    }

    #[test]
    fn test_resolve_local_resource_missing() {
        let temp = TempDir::new().unwrap();
        let resource = Resource {
            name: "specs".to_string(),
            git: None,
            path: Some("/nonexistent/path".to_string()),
            description: Some("Specs".to_string()),
            branch: None,
        };

        let result = resolve_resource_path(temp.path(), &resource);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_resource_no_git_no_path() {
        let temp = TempDir::new().unwrap();
        let resource = Resource {
            name: "bad".to_string(),
            git: None,
            path: None,
            description: Some("Invalid".to_string()),
            branch: None,
        };

        let result = resolve_resource_path(temp.path(), &resource);
        assert!(result.is_none());
    }

    #[test]
    fn test_list_resource_status() {
        let temp = TempDir::new().unwrap();
        let clone_dir = get_resources_dir(temp.path()).join("existing");
        std::fs::create_dir_all(&clone_dir).unwrap();

        let resources = vec![
            Resource {
                name: "existing".to_string(),
                git: Some("https://example.com/repo.git".to_string()),
                path: None,
                description: Some("Exists".to_string()),
                branch: None,
            },
            Resource {
                name: "missing".to_string(),
                git: Some("https://example.com/other.git".to_string()),
                path: None,
                description: Some("Does not exist".to_string()),
                branch: None,
            },
        ];

        let statuses = list_resource_status(temp.path(), &resources);
        assert_eq!(statuses.len(), 2);
        assert!(statuses[0].exists);
        assert!(statuses[0].resolved_path.is_some());
        assert!(!statuses[1].exists);
        assert!(statuses[1].resolved_path.is_none());
    }

    #[test]
    fn test_list_global_resources() {
        let temp = TempDir::new().unwrap();
        let resources_dir = get_resources_dir(temp.path());

        // Create some fake resource dirs
        std::fs::create_dir_all(resources_dir.join("alpha")).unwrap();
        std::fs::create_dir_all(resources_dir.join("beta")).unwrap();
        // Create a file (should be ignored)
        std::fs::write(resources_dir.join("not-a-dir.txt"), "").unwrap();

        let globals = list_global_resources(temp.path());
        assert_eq!(globals.len(), 2);
        assert_eq!(globals[0].name, "alpha");
        assert_eq!(globals[1].name, "beta");
        // No git remote since these aren't real git repos
        assert!(globals[0].git_url.is_none());
    }

    #[test]
    fn test_list_global_resources_empty() {
        let temp = TempDir::new().unwrap();
        let globals = list_global_resources(temp.path());
        assert!(globals.is_empty());
    }

    #[test]
    fn test_build_resources_prompt_with_existing() {
        let temp = TempDir::new().unwrap();
        let clone_dir = get_resources_dir(temp.path()).join("docs");
        std::fs::create_dir_all(&clone_dir).unwrap();

        let resources = vec![Resource {
            name: "docs".to_string(),
            git: Some("https://example.com/docs.git".to_string()),
            path: None,
            description: Some("Project documentation".to_string()),
            branch: None,
        }];

        let prompt = build_resources_prompt(temp.path(), &resources);
        assert!(prompt.contains("## Project Resources"));
        assert!(prompt.contains("**docs**"));
        assert!(prompt.contains("Project documentation"));
        assert!(prompt.contains(&clone_dir.to_string_lossy().to_string()));
    }

    #[test]
    fn test_build_resources_prompt_empty_when_none_exist() {
        let temp = TempDir::new().unwrap();
        let resources = vec![Resource {
            name: "missing".to_string(),
            git: Some("https://example.com/missing.git".to_string()),
            path: None,
            description: Some("Missing resource".to_string()),
            branch: None,
        }];

        let prompt = build_resources_prompt(temp.path(), &resources);
        assert!(prompt.is_empty());
    }

    #[test]
    fn test_remove_resource_clone() {
        let temp = TempDir::new().unwrap();
        let clone_dir = get_resources_dir(temp.path()).join("repo");
        std::fs::create_dir_all(&clone_dir).unwrap();
        assert!(clone_dir.exists());

        remove_resource_clone(temp.path(), "repo").unwrap();
        assert!(!clone_dir.exists());
    }

    #[test]
    fn test_remove_resource_clone_nonexistent() {
        let temp = TempDir::new().unwrap();
        remove_resource_clone(temp.path(), "nonexistent").unwrap();
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
    fn test_clone_resource_errors_without_git_url() {
        let temp = TempDir::new().unwrap();
        let resource = Resource {
            name: "local-only".to_string(),
            git: None,
            path: Some("/some/path".to_string()),
            description: Some("No git URL".to_string()),
            branch: None,
        };

        let result = clone_resource(temp.path(), &resource);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no git URL"));
    }

    #[test]
    fn test_clone_resource_returns_existing_without_recloning() {
        let temp = TempDir::new().unwrap();
        let clone_dir = get_resources_dir(temp.path()).join("repo");
        std::fs::create_dir_all(&clone_dir).unwrap();
        // Place a marker file so we can verify it wasn't wiped
        std::fs::write(clone_dir.join("marker.txt"), "exists").unwrap();

        let resource = Resource {
            name: "repo".to_string(),
            git: Some("https://example.com/repo.git".to_string()),
            path: None,
            description: Some("Already cloned".to_string()),
            branch: None,
        };

        let result = clone_resource(temp.path(), &resource);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), clone_dir);
        // Marker file should still be there (no re-clone happened)
        assert!(clone_dir.join("marker.txt").exists());
    }

    #[test]
    fn test_refresh_resource_errors_for_local_resource() {
        let temp = TempDir::new().unwrap();
        let resource = Resource {
            name: "local".to_string(),
            git: None,
            path: Some("/some/path".to_string()),
            description: Some("Local dir".to_string()),
            branch: None,
        };

        let result = refresh_resource(temp.path(), &resource);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Cannot refresh a local resource"));
    }

    #[test]
    fn test_refresh_resource_errors_when_not_cloned() {
        let temp = TempDir::new().unwrap();
        let resource = Resource {
            name: "not-cloned".to_string(),
            git: Some("https://example.com/repo.git".to_string()),
            path: None,
            description: Some("Missing clone".to_string()),
            branch: None,
        };

        let result = refresh_resource(temp.path(), &resource);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not cloned yet"));
    }

    #[test]
    fn test_build_resources_prompt_filters_missing() {
        let temp = TempDir::new().unwrap();
        // Create only the "docs" resource dir, not "missing"
        let docs_dir = get_resources_dir(temp.path()).join("docs");
        std::fs::create_dir_all(&docs_dir).unwrap();

        let resources = vec![
            Resource {
                name: "docs".to_string(),
                git: Some("https://example.com/docs.git".to_string()),
                path: None,
                description: Some("Documentation".to_string()),
                branch: None,
            },
            Resource {
                name: "missing".to_string(),
                git: Some("https://example.com/missing.git".to_string()),
                path: None,
                description: Some("Should be filtered".to_string()),
                branch: None,
            },
        ];

        let prompt = build_resources_prompt(temp.path(), &resources);
        assert!(prompt.contains("**docs**"));
        assert!(prompt.contains("Documentation"));
        assert!(!prompt.contains("missing"));
        assert!(!prompt.contains("Should be filtered"));
    }

    #[test]
    fn test_list_resource_status_fields() {
        let temp = TempDir::new().unwrap();
        let clone_dir = get_resources_dir(temp.path()).join("repo");
        std::fs::create_dir_all(&clone_dir).unwrap();

        let local_dir = temp.path().join("local-specs");
        std::fs::create_dir_all(&local_dir).unwrap();

        let resources = vec![
            Resource {
                name: "repo".to_string(),
                git: Some("https://example.com/repo.git".to_string()),
                path: None,
                description: Some("A git repo".to_string()),
                branch: None,
            },
            Resource {
                name: "local-specs".to_string(),
                git: None,
                path: Some(local_dir.to_string_lossy().to_string()),
                description: Some("Local specs".to_string()),
                branch: None,
            },
        ];

        let statuses = list_resource_status(temp.path(), &resources);
        assert_eq!(statuses.len(), 2);

        // Git resource
        assert_eq!(statuses[0].name, "repo");
        assert_eq!(statuses[0].description, "A git repo");
        assert!(matches!(statuses[0].resource_type, ResourceType::Git));
        assert!(statuses[0].exists);

        // Local resource
        assert_eq!(statuses[1].name, "local-specs");
        assert_eq!(statuses[1].description, "Local specs");
        assert!(matches!(statuses[1].resource_type, ResourceType::Local));
        assert!(statuses[1].exists);
    }

    #[test]
    fn test_list_resource_status_description_fallback() {
        let temp = TempDir::new().unwrap();
        let clone_dir = get_resources_dir(temp.path()).join("repo");
        std::fs::create_dir_all(&clone_dir).unwrap();

        // Save a description in contents.yaml
        save_resource_description(temp.path(), "repo", "From contents.yaml").unwrap();

        // Resource without description — should fall back to contents.yaml
        let resources = vec![Resource {
            name: "repo".to_string(),
            git: Some("https://example.com/repo.git".to_string()),
            path: None,
            description: None,
            branch: None,
        }];

        let statuses = list_resource_status(temp.path(), &resources);
        assert_eq!(statuses[0].description, "From contents.yaml");
    }

    #[test]
    fn test_list_resource_status_description_config_takes_priority() {
        let temp = TempDir::new().unwrap();
        let clone_dir = get_resources_dir(temp.path()).join("repo");
        std::fs::create_dir_all(&clone_dir).unwrap();

        // Save a description in contents.yaml
        save_resource_description(temp.path(), "repo", "From contents.yaml").unwrap();

        // Resource WITH description — config should win over contents.yaml
        let resources = vec![Resource {
            name: "repo".to_string(),
            git: Some("https://example.com/repo.git".to_string()),
            path: None,
            description: Some("From config".to_string()),
            branch: None,
        }];

        let statuses = list_resource_status(temp.path(), &resources);
        assert_eq!(statuses[0].description, "From config");
    }

    #[test]
    fn test_build_resources_prompt_falls_back_to_name_without_description() {
        let temp = TempDir::new().unwrap();
        let clone_dir = get_resources_dir(temp.path()).join("my-repo");
        std::fs::create_dir_all(&clone_dir).unwrap();

        let resources = vec![Resource {
            name: "my-repo".to_string(),
            git: Some("https://example.com/repo.git".to_string()),
            path: None,
            description: None,
            branch: None,
        }];

        let prompt = build_resources_prompt(temp.path(), &resources);
        assert!(prompt.contains("**my-repo**"));
        // Falls back to using the name as description
        assert!(prompt.contains("my-repo"));
    }

    #[test]
    fn test_build_resources_prompt_empty_list() {
        let temp = TempDir::new().unwrap();
        let prompt = build_resources_prompt(temp.path(), &[]);
        assert!(prompt.is_empty());
    }

    #[test]
    fn test_save_resource_description_empty_removes() {
        let temp = TempDir::new().unwrap();

        // Save then remove
        save_resource_description(temp.path(), "repo", "Some desc").unwrap();
        let contents = load_contents(temp.path());
        assert_eq!(contents.0.get("repo").unwrap(), "Some desc");

        save_resource_description(temp.path(), "repo", "").unwrap();
        let contents = load_contents(temp.path());
        assert!(contents.0.get("repo").is_none());
    }

    #[test]
    fn test_list_global_resources_includes_descriptions() {
        let temp = TempDir::new().unwrap();
        let resources_dir = get_resources_dir(temp.path());
        std::fs::create_dir_all(resources_dir.join("myrepo")).unwrap();

        save_resource_description(temp.path(), "myrepo", "My repo description").unwrap();

        let globals = list_global_resources(temp.path());
        assert_eq!(globals.len(), 1);
        assert_eq!(
            globals[0].description.as_deref(),
            Some("My repo description")
        );
    }
}
