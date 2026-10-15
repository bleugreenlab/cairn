//! Tiered route definition loading. Project definitions shadow workspace
//! definitions by filename.
//!
//! The routes Cairn ships are core-pack content (`packs/core/routes/`), which
//! the workspace pack sync materializes into `~/.cairn/routes/`. They are
//! ordinary workspace files from here on: there is no compiled-in tier.

use super::{id_from_path, ConfigResult};
use crate::routes::{parse_definition, FactRegistry, RouteDefinition};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct FileRoute {
    pub id: String,
    pub definition: RouteDefinition,
    pub is_project_scoped: bool,
    pub file_path: PathBuf,
}

pub fn list_routes(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> Result<Vec<ConfigResult<FileRoute>>, String> {
    list_route_dirs(super::config_root_subdirs(
        config_dir,
        project_path,
        "routes",
    ))
}

pub fn list_project_routes(project_path: &Path) -> Result<Vec<ConfigResult<FileRoute>>, String> {
    list_route_dirs(vec![(project_path.join(".cairn/routes"), true)])
}

#[cfg(test)]
thread_local! {
    /// How many times this thread listed route directories, so a test can
    /// prove route YAML is read when it changes rather than once per fact.
    pub(crate) static ROUTE_DIR_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn list_route_dirs(dirs: Vec<(PathBuf, bool)>) -> Result<Vec<ConfigResult<FileRoute>>, String> {
    #[cfg(test)]
    ROUTE_DIR_READS.with(|reads| reads.set(reads.get() + 1));
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
    Ok(results)
}

/// Delete a route from the innermost scope that has it: the project's
/// `.cairn/routes/` when `project_path` holds one, else the workspace's.
/// Returns the path that was removed, so the caller can commit it.
///
/// A workspace-scope delete records the removal against whichever installed
/// pack ships the route; without that the next sync copies it back, which is
/// the same copy-when-missing that seeds a fresh workspace.
pub fn delete_route(
    config_dir: &Path,
    id: &str,
    project_path: Option<&Path>,
) -> Result<Option<PathBuf>, String> {
    if let Some(project_path) = project_path {
        let path = project_path
            .join(".cairn")
            .join("routes")
            .join(format!("{id}.yaml"));
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| format!("Failed to delete route file: {e}"))?;
            return Ok(Some(path));
        }
    }

    let path = config_dir.join("routes").join(format!("{id}.yaml"));
    if path.exists() {
        std::fs::remove_file(&path).map_err(|e| format!("Failed to delete route file: {e}"))?;
        super::pack::note_removed_item(config_dir, super::pack::PackItemKind::Route, id);
        return Ok(Some(path));
    }
    Ok(None)
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
            file_path: path.to_owned(),
        }),
        Err(error) => fail(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{RealFileSystem, RealGitClient};
    use tempfile::tempdir;

    const FOLLOWED_ID: &str = "followed-thread-stream";
    const GITHUB_MENTION_ID: &str = "github-mention";

    /// A workspace config dir synced from the packs this repository ships, so
    /// the routes under test are the real shipped files as the pack sync
    /// materializes them, not a copy that could drift.
    fn synced_workspace(root: &Path) -> PathBuf {
        crate::config::secrets::mock_keychain::install();
        let src_tauri = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("cairn-core manifest is nested under src-tauri");
        let config = root.join("config");
        crate::workspace::bundle::sync_workspace_packs(
            &RealGitClient,
            &RealFileSystem,
            src_tauri,
            &config,
            "1.0.0",
        )
        .unwrap();
        config
    }

    fn ok_route<'a>(routes: &'a [ConfigResult<FileRoute>], id: &str) -> &'a FileRoute {
        routes
            .iter()
            .find_map(|route| match route {
                ConfigResult::Ok(route) if route.id == id => Some(route),
                _ => None,
            })
            .unwrap_or_else(|| panic!("valid route `{id}`"))
    }

    #[test]
    fn shipped_routes_load_from_the_synced_workspace_and_project_shadows_them() {
        let temp = tempdir().unwrap();
        let config = synced_workspace(temp.path());
        let project = temp.path().join("project");
        std::fs::create_dir_all(project.join(".cairn/routes")).unwrap();
        let shipped =
            std::fs::read_to_string(config.join("routes/followed-thread-stream.yaml")).unwrap();
        std::fs::write(
            project.join(".cairn/routes/followed-thread-stream.yaml"),
            shipped.replace("Followed thread stream", "Project route"),
        )
        .unwrap();

        let workspace = list_routes(&config, None).unwrap();
        for id in [FOLLOWED_ID, GITHUB_MENTION_ID] {
            let route = ok_route(&workspace, id);
            assert!(!route.is_project_scoped);
            assert_eq!(
                route.file_path,
                config.join("routes").join(format!("{id}.yaml")),
                "a shipped route is a real workspace file"
            );
        }

        let routes = list_routes(&config, Some(&project)).unwrap();
        assert_eq!(
            routes.len(),
            2,
            "the project copy shadows, never duplicates"
        );
        let route = ok_route(&routes, FOLLOWED_ID);
        assert!(route.is_project_scoped);
        assert_eq!(route.definition.name, "Project route");
        assert!(!ok_route(&routes, GITHUB_MENTION_ID).is_project_scoped);
    }

    /// Deleting a shipped route must hold across the next sync. The sync is
    /// copy-when-missing, so without the removal record it would put the file
    /// straight back.
    #[test]
    fn deleting_a_shipped_route_survives_the_next_pack_sync() {
        let temp = tempdir().unwrap();
        let config = synced_workspace(temp.path());
        let removed = delete_route(&config, GITHUB_MENTION_ID, None).unwrap();
        assert_eq!(
            removed,
            Some(config.join("routes/github-mention.yaml")),
            "the workspace file is what was deleted"
        );
        assert!(super::super::pack::lock::read_lock(&config, "core")
            .unwrap()
            .is_removed(super::super::pack::PackItemKind::Route, GITHUB_MENTION_ID));

        synced_workspace(temp.path());
        assert!(get_route(&config, GITHUB_MENTION_ID, None)
            .unwrap()
            .is_none());
        assert!(get_route(&config, FOLLOWED_ID, None).unwrap().is_some());
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
    fn explicit_project_routes_exclude_the_workspace_tier() {
        let temp = tempdir().unwrap();
        let config = synced_workspace(temp.path());
        let project = temp.path().join("project");
        let shipped =
            std::fs::read_to_string(config.join("routes/followed-thread-stream.yaml")).unwrap();
        std::fs::create_dir_all(project.join(".cairn/routes")).unwrap();
        std::fs::write(config.join("routes/workspace.yaml"), &shipped).unwrap();
        std::fs::write(
            project.join(".cairn/routes/project.yaml"),
            shipped.replace("Followed thread stream", "Project only"),
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
