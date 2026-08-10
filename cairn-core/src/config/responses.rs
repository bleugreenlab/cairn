//! Response definition file loading.

use super::{id_from_path, ConfigResult};
use crate::responses::{parse_definition, ResponseDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileResponse {
    pub id: String,
    #[serde(flatten)]
    pub definition: ResponseDefinition,
    pub is_project_scoped: bool,
    pub file_path: PathBuf,
}

pub fn list_responses(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileResponse>>, String> {
    let mut results = Vec::new();
    let mut seen = HashSet::new();
    for (dir, is_project_scoped) in
        super::config_root_subdirs(config_dir, project_path, "responses")
    {
        if !dir.is_dir() {
            continue;
        }
        let mut paths = std::fs::read_dir(&dir)
            .map_err(|e| format!("Failed to read responses directory: {e}"))?
            .map(|entry| {
                entry
                    .map(|e| e.path())
                    .map_err(|e| format!("Failed to read directory entry: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        paths.sort();
        for path in paths {
            if path.is_dir() || path.extension().and_then(|e| e.to_str()) != Some("md") {
                continue;
            }
            let Some(id) = id_from_path(&path) else {
                continue;
            };
            if seen.insert(id) {
                results.push(load_response_file(&path, is_project_scoped));
            }
        }
    }
    Ok(results)
}

/// Delete one response file, project scope first when a project path is given.
/// Returns the path that was removed, so the caller can commit it.
///
/// A workspace-scope delete records the removal against whichever installed
/// pack ships the response; without that the next sync copies it back, which is
/// the same copy-when-missing that seeds a fresh workspace.
pub fn delete_response(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<PathBuf>, String> {
    if let Some(project_path) = project_path {
        let path = project_path
            .join(".cairn")
            .join("responses")
            .join(format!("{id}.md"));
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| format!("Failed to delete response file: {e}"))?;
            return Ok(Some(path));
        }
    }

    let path = config_dir.join("responses").join(format!("{id}.md"));
    if path.exists() {
        std::fs::remove_file(&path).map_err(|e| format!("Failed to delete response file: {e}"))?;
        super::pack::note_removed_item(config_dir, super::pack::PackItemKind::Response, id);
        return Ok(Some(path));
    }
    Ok(None)
}

pub fn get_response(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<FileResponse>, String> {
    for (dir, is_project_scoped) in
        super::config_root_subdirs(config_dir, project_path, "responses")
    {
        let path = dir.join(format!("{id}.md"));
        if path.exists() {
            return match load_response_file(&path, is_project_scoped) {
                ConfigResult::Ok(response) => Ok(Some(response)),
                ConfigResult::Err { error, .. } => Err(error),
            };
        }
    }
    Ok(None)
}

fn load_response_file(path: &Path, is_project_scoped: bool) -> ConfigResult<FileResponse> {
    let fail = |error| ConfigResult::Err {
        path: path.to_path_buf(),
        error,
    };
    let Some(id) = id_from_path(path) else {
        return fail("Could not determine response ID from filename".into());
    };
    let content = match std::fs::read_to_string(path) {
        Ok(value) => value,
        Err(e) => return fail(format!("Failed to read file: {e}")),
    };
    match parse_definition(&content) {
        Ok(definition) => ConfigResult::Ok(FileResponse {
            id,
            definition,
            is_project_scoped,
            file_path: path.to_path_buf(),
        }),
        Err(error) => fail(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const VALID: &str = "---\nname: Workspace\ndescription: valid\n---\nPrompt";

    #[test]
    fn project_definition_shadows_workspace_by_id() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        let project = temp.path().join("project");
        std::fs::create_dir_all(config.join("responses")).unwrap();
        std::fs::create_dir_all(project.join(".cairn/responses")).unwrap();
        std::fs::write(config.join("responses/shared.md"), VALID).unwrap();
        std::fs::write(
            project.join(".cairn/responses/shared.md"),
            VALID.replace("Workspace", "Project"),
        )
        .unwrap();
        let listed = list_responses(&config, Some(&project)).unwrap();
        assert_eq!(listed.len(), 1);
        let ConfigResult::Ok(response) = &listed[0] else {
            panic!("expected valid response")
        };
        assert_eq!(response.definition.name, "Project");
        assert!(response.is_project_scoped);
    }

    #[test]
    fn malformed_definition_remains_visible_and_shadows_workspace() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        let project = temp.path().join("project");
        std::fs::create_dir_all(config.join("responses")).unwrap();
        std::fs::create_dir_all(project.join(".cairn/responses")).unwrap();
        std::fs::write(config.join("responses/shared.md"), VALID).unwrap();
        std::fs::write(
            project.join(".cairn/responses/shared.md"),
            "not frontmatter",
        )
        .unwrap();
        let listed = list_responses(&config, Some(&project)).unwrap();
        assert_eq!(listed.len(), 1);
        assert!(matches!(&listed[0], ConfigResult::Err { path, .. } if path.starts_with(&project)));
    }

    #[test]
    fn bundled_conveyor_parses() {
        let parsed =
            parse_definition(include_str!("../../../../packs/core/responses/conveyor.md")).unwrap();
        assert_eq!(parsed.tier.as_deref().unwrap_or("sm"), "sm");
        assert!(parsed
            .render(&serde_json::json!({"text":"Dense prose"}))
            .unwrap()
            .contains("Dense prose"));
    }
}
