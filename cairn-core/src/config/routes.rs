//! Tiered route definition loading. Project definitions shadow workspace
//! definitions, and bundled definitions are the final fallback.

use super::{id_from_path, ConfigResult};
use crate::routes::{parse_definition, FactRegistry, RouteDefinition};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct FileRoute {
    pub id: String,
    pub definition: RouteDefinition,
    pub is_project_scoped: bool,
    pub file_path: Option<PathBuf>,
}

const FOLLOWED_ID: &str = "followed-thread-stream";
const FOLLOWED: &str = include_str!("../../../../routes/followed-thread-stream.yaml");
const GITHUB_MENTION_ID: &str = "github-mention";
const GITHUB_MENTION: &str = include_str!("../../../../routes/github-mention.yaml");

pub fn list_routes(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileRoute>>, String> {
    list_route_dirs(
        super::config_root_subdirs(config_dir, project_path, "routes"),
        true,
    )
}

pub fn list_project_routes(project_path: &Path) -> Result<Vec<ConfigResult<FileRoute>>, String> {
    list_route_dirs(vec![(project_path.join(".cairn/routes"), true)], false)
}

fn list_route_dirs(
    dirs: Vec<(PathBuf, bool)>,
    include_bundled: bool,
) -> Result<Vec<ConfigResult<FileRoute>>, String> {
    let registry = FactRegistry::default();
    let mut results = Vec::new();
    let mut seen = HashSet::new();
    for (dir, project) in dirs {
        if !dir.is_dir() {
            continue;
        }
        let mut paths = std::fs::read_dir(&dir)
            .map_err(|e| format!("Failed to read routes directory: {e}"))?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .collect::<Vec<_>>();
        paths.sort();
        for path in paths {
            if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
                continue;
            }
            let Some(id) = id_from_path(&path) else {
                continue;
            };
            if seen.insert(id.clone()) {
                results.push(load(&path, id, project, &registry));
            }
        }
    }
    if include_bundled && seen.insert(GITHUB_MENTION_ID.into()) {
        results.push(ConfigResult::Ok(FileRoute {
            id: GITHUB_MENTION_ID.into(),
            definition: parse_definition(GITHUB_MENTION, &registry)?,
            is_project_scoped: false,
            file_path: None,
        }));
    }
    if include_bundled && seen.insert(FOLLOWED_ID.into()) {
        results.push(ConfigResult::Ok(FileRoute {
            id: FOLLOWED_ID.into(),
            definition: parse_definition(FOLLOWED, &registry)?,
            is_project_scoped: false,
            file_path: None,
        }));
    }
    Ok(results)
}

pub fn get_route(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<FileRoute>, String> {
    find_route(list_routes(config_dir, project_path)?, id)
}

pub fn get_project_route(project_path: &Path, id: &str) -> Result<Option<FileRoute>, String> {
    find_route(list_project_routes(project_path)?, id)
}

fn find_route(routes: Vec<ConfigResult<FileRoute>>, id: &str) -> Result<Option<FileRoute>, String> {
    for result in routes {
        match result {
            ConfigResult::Ok(route) if route.id == id => return Ok(Some(route)),
            ConfigResult::Err { path, error } if id_from_path(&path).as_deref() == Some(id) => {
                return Err(error)
            }
            _ => {}
        }
    }
    Ok(None)
}

fn load(
    path: &Path,
    id: String,
    project: bool,
    registry: &FactRegistry,
) -> ConfigResult<FileRoute> {
    let fail = |error| ConfigResult::Err {
        path: path.to_owned(),
        error,
    };
    let content = match std::fs::read_to_string(path) {
        Ok(v) => v,
        Err(e) => return fail(e.to_string()),
    };
    match parse_definition(&content, registry) {
        Ok(definition) => ConfigResult::Ok(FileRoute {
            id,
            definition,
            is_project_scoped: project,
            file_path: Some(path.to_owned()),
        }),
        Err(error) => fail(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn bundled_route_is_fallback_and_project_shadows_workspace() {
        let temp = tempdir().unwrap();
        let project = temp.path().join("project");
        std::fs::create_dir_all(project.join(".cairn/routes")).unwrap();
        std::fs::write(
            project.join(".cairn/routes/followed-thread-stream.yaml"),
            FOLLOWED.replace("Followed thread stream", "Project route"),
        )
        .unwrap();
        let routes = list_routes(temp.path(), Some(&project)).unwrap();
        assert_eq!(routes.len(), 2);
        let ConfigResult::Ok(route) = routes
            .iter()
            .find(|route| matches!(route, ConfigResult::Ok(route) if route.id == FOLLOWED_ID))
            .expect("followed route")
        else {
            panic!("valid route")
        };
        assert!(route.is_project_scoped);
        assert_eq!(route.definition.name, "Project route");
    }

    /// The one route that existed before routes were graphs is a file on disk
    /// somewhere, so the loader has to read it — and hand back the graph it
    /// means, not a second shape the rest of the system would have to know.
    #[test]
    fn a_route_file_in_the_older_linear_form_loads_as_a_graph() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        std::fs::create_dir_all(config.join("routes")).unwrap();
        std::fs::write(
            config.join("routes/legacy.yaml"),
            "name: Legacy\ndescription: written before routes were graphs\nwhen:\n  - fact: thread_stream\n    presence: away\ntransforms: []\nto:\n  kind: channel\n  register: notify\n  initiatedBy: operator_subscription\ndedupe: 10m\n",
        )
        .unwrap();

        let route = get_route(&config, "legacy", None).unwrap().unwrap();
        assert_eq!(route.definition.triggers().count(), 1);
        assert_eq!(route.definition.sinks().count(), 1);
        assert_eq!(route.definition.edges.len(), 1);
        assert!(route.definition.dedupe.is_some());
    }

    #[test]
    fn explicit_project_routes_exclude_workspace_and_bundled_tiers() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        let project = temp.path().join("project");
        std::fs::create_dir_all(config.join("routes")).unwrap();
        std::fs::create_dir_all(project.join(".cairn/routes")).unwrap();
        std::fs::write(config.join("routes/workspace.yaml"), FOLLOWED).unwrap();
        std::fs::write(
            project.join(".cairn/routes/project.yaml"),
            FOLLOWED.replace("Followed thread stream", "Project only"),
        )
        .unwrap();

        let routes = list_project_routes(&project).unwrap();
        assert_eq!(routes.len(), 1);
        let ConfigResult::Ok(route) = &routes[0] else {
            panic!("valid route")
        };
        assert_eq!(route.id, "project");
        assert!(route.is_project_scoped);
        assert!(get_project_route(&project, "workspace").unwrap().is_none());
        assert!(get_project_route(&project, FOLLOWED_ID).unwrap().is_none());
    }
}
