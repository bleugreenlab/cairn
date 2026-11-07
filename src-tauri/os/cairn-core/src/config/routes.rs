//! Route definition loading: `routes/<id>.yaml` in a project's `.cairn/`, the
//! workspace, or an installed pack, resolved nearest-first through
//! [`cairn_config::library`]. The routes Cairn ships are core-pack content,
//! read where the pack is installed: there is no compiled-in tier.

use super::{id_from_path, ConfigResult};
use crate::routes::{parse_definition, FactRegistry, RouteDefinition, RouteNodeConfig};
use cairn_config::library::{
    self, LibraryItem, LibraryKind, LibraryRef, LibraryRoots, Listed, Origin, ResolveError,
    Resolved,
};
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct FileRoute {
    pub id: String,
    pub definition: RouteDefinition,
    /// The layer this route was read from, or the layer a new one is for.
    pub origin: Origin,
    pub file_path: PathBuf,
}

impl FileRoute {
    pub fn is_project_scoped(&self) -> bool {
        self.origin.is_project()
    }
}

/// A route's authority: whether it runs at all, what fires it and how often,
/// which responses it spends and with what, the edges and decisions that pick
/// a path, and where its sinks deliver. Every field and node kind is named
/// below, so one added to a route does not compile until it is classified.
impl LibraryItem for FileRoute {
    fn library_id(&self) -> &str {
        &self.id
    }

    fn authority(&self) -> Map<String, Value> {
        use crate::models::decision::DecisionNode;
        use crate::routes::{RouteDefinition, RouteNode};

        let FileRoute {
            id: _,
            definition,
            origin: _,
            file_path: _,
        } = self;
        let RouteDefinition {
            // Prose.
            name: _,
            description: _,
            enabled,
            nodes,
            edges,
            dedupe,
        } = definition;
        let mut fields = Map::new();
        fields.insert("enabled".into(), json!(enabled));
        if let Some(window) = dedupe {
            fields.insert("dedupe".into(), json!(window.to_string()));
        }
        if !edges.is_empty() {
            fields.insert("edges".into(), json!(edges));
        }
        for node in nodes {
            let RouteNode {
                id,
                // The operator's label and the canvas layout.
                name: _,
                position: _,
                config,
            } = node;
            let (key, value) = match config {
                RouteNodeConfig::Trigger { when } => ("trigger", json!(when)),
                RouteNodeConfig::Response { response, args } => {
                    ("response", json!({ "response": response, "args": args }))
                }
                RouteNodeConfig::Decision { decision, input } => {
                    let DecisionNode {
                        // Prose put to a model; the edges' `when` clauses
                        // decide where its answers lead.
                        questions: _,
                        models,
                        thresholds,
                    } = decision;
                    (
                        "decision",
                        json!({ "models": models, "thresholds": thresholds, "input": input }),
                    )
                }
                RouteNodeConfig::Sink { sink } => ("sink", json!(sink)),
            };
            fields.insert(format!("{key}:{id}"), value);
        }
        fields
    }
}

#[cfg(test)]
thread_local! {
    /// How many times this thread listed route directories, so a test can
    /// prove route YAML is read when it changes rather than once per fact.
    pub(crate) static ROUTE_DIR_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn load_route_in(dir: &Path, id: &str, origin: &Origin) -> Option<ConfigResult<FileRoute>> {
    let path = dir.join(format!("{id}.yaml"));
    path.is_file().then(|| {
        load(
            &path,
            id.to_string(),
            origin.clone(),
            &FactRegistry::default(),
        )
    })
}

fn enumerate_routes(
    dir: &Path,
    origin: &Origin,
) -> Result<Vec<(String, ConfigResult<FileRoute>)>, String> {
    let registry = FactRegistry::default();
    let mut paths = std::fs::read_dir(dir)
        .map_err(|e| format!("Failed to read routes directory: {e}"))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths
        .into_iter()
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("yaml"))
        .filter_map(|path| {
            let id = id_from_path(&path)?;
            Some((id.clone(), load(&path, id, origin.clone(), &registry)))
        })
        .collect())
}

/// Every route definition across the layers, with its origin and what it
/// shadows.
pub fn list_route_rows(roots: &LibraryRoots) -> Result<Vec<Listed<FileRoute>>, String> {
    #[cfg(test)]
    ROUTE_DIR_READS.with(|reads| reads.set(reads.get() + 1));
    roots.list(LibraryKind::Route, enumerate_routes)
}

/// The routes in force, one per name.
pub fn list_routes(roots: &LibraryRoots) -> Result<Vec<ConfigResult<FileRoute>>, String> {
    Ok(library::active(list_route_rows(roots)?))
}

/// The project's own route definitions, each with its trust status: those in
/// force and those skipped or awaiting the operator's confirmation.
pub fn list_project_routes(roots: &LibraryRoots) -> Result<Vec<Listed<FileRoute>>, String> {
    Ok(list_route_rows(roots)?
        .into_iter()
        .filter(|row| row.origin.is_project())
        .collect())
}

/// Resolve a route reference (an id or `cairn://packs/<pack>/routes/<id>`)
/// nearest-first.
pub fn resolve_route(
    roots: &LibraryRoots,
    reference: &str,
) -> Result<Resolved<FileRoute>, ResolveError> {
    let parsed = LibraryRef::parse(LibraryKind::Route, reference).map_err(|error| {
        ResolveError::Invalid {
            path: PathBuf::new(),
            error,
            origin: None,
        }
    })?;
    roots.resolve(LibraryKind::Route, &parsed, load_route_in)
}

pub fn get_route(roots: &LibraryRoots, id: &str) -> Result<Option<FileRoute>, String> {
    match resolve_route(roots, id) {
        Ok(resolved) => Ok(Some(resolved.item)),
        Err(error) if error.is_not_found() => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// The project's own definition of route `id`, behind the project's trust
/// gate; `None` when the project does not define it.
pub fn get_project_route(roots: &LibraryRoots, id: &str) -> Result<Option<FileRoute>, String> {
    let Some(layer) = roots.project_origin() else {
        return Ok(None);
    };
    match roots.resolve_in(LibraryKind::Route, &layer, id, load_route_in) {
        Ok(resolved) => Ok(Some(resolved.item)),
        Err(error) if error.is_not_found() => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// Delete the route `id` resolves to, or the one in `layer` when named. A
/// workspace or project definition is deleted outright; a pack's own route is
/// recorded as removed from its pack. Returns the path that changed, so the
/// caller can commit it.
pub fn delete_route(
    roots: &LibraryRoots,
    id: &str,
    layer: Option<&Origin>,
) -> Result<Option<PathBuf>, String> {
    let (origin, path) = match layer {
        Some(origin @ (Origin::Project { .. } | Origin::Workspace)) => {
            let path = roots
                .layer_dir(LibraryKind::Route, origin)
                .ok_or_else(|| format!("No {origin} layer to delete route `{id}` from"))?
                .join(format!("{id}.yaml"));
            if !path.is_file() {
                return Ok(None);
            }
            (origin.clone(), path)
        }
        Some(origin) => match roots.resolve_in(LibraryKind::Route, origin, id, load_route_in) {
            Ok(resolved) => (resolved.origin, resolved.item.file_path),
            Err(ResolveError::Invalid {
                path,
                origin: Some(origin),
                ..
            }) => (origin, path),
            Err(error) if error.is_not_found() => return Ok(None),
            Err(error) => return Err(error.to_string()),
        },
        None => match resolve_route(roots, id) {
            Ok(resolved) => (resolved.origin, resolved.item.file_path),
            Err(ResolveError::Invalid {
                path,
                origin: Some(origin),
                ..
            }) => (origin, path),
            Err(error) if error.is_not_found() => return Ok(None),
            Err(error) => return Err(error.to_string()),
        },
    };
    library::remove_definition(roots, LibraryKind::Route, id, &origin, &path).map(Some)
}

fn load(
    path: &Path,
    id: String,
    origin: Origin,
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
            origin,
            file_path: path.to_owned(),
        }),
        Err(error) => fail(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{RealFileSystem, RealGitClient};
    #[allow(unused_imports)]
    use cairn_config::library::{LibraryRoots, Origin};
    use tempfile::tempdir;

    const FOLLOWED_ID: &str = "followed-thread-stream";
    const GITHUB_MENTION_ID: &str = "github-mention";
    const AMBIENT_ID: &str = "ambient-channel";

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
            std::fs::read_to_string(config.join("packs/core/routes/followed-thread-stream.yaml"))
                .unwrap();
        std::fs::write(
            project.join(".cairn/routes/followed-thread-stream.yaml"),
            shipped.replace("Followed thread stream", "Project route"),
        )
        .unwrap();

        let workspace = list_routes(&LibraryRoots::workspace(&config)).unwrap();
        for id in [FOLLOWED_ID, GITHUB_MENTION_ID, AMBIENT_ID] {
            let route = ok_route(&workspace, id);
            assert_eq!(route.origin, Origin::Pack { id: "core".into() });
            assert_eq!(
                route.file_path,
                config.join("packs/core/routes").join(format!("{id}.yaml")),
                "a shipped route is read where its pack installed it"
            );
        }

        let routes = list_routes(&crate::config::trusted_roots(&config, &project)).unwrap();
        assert_eq!(
            routes.len(),
            3,
            "the project copy shadows, never duplicates"
        );
        let route = ok_route(&routes, FOLLOWED_ID);
        assert!(route.is_project_scoped());
        assert_eq!(route.definition.name, "Project route");
        assert!(!ok_route(&routes, GITHUB_MENTION_ID).is_project_scoped());
        assert!(!ok_route(&routes, AMBIENT_ID).is_project_scoped());
        assert_eq!(
            workspace.len(),
            3,
            "one workspace route per shipped filename"
        );
    }

    /// Deleting a shipped route records its removal from the pack, and the
    /// removal holds across the next sync.
    #[test]
    fn deleting_a_shipped_route_survives_the_next_pack_sync() {
        let temp = tempdir().unwrap();
        let config = synced_workspace(temp.path());
        let removed =
            delete_route(&LibraryRoots::workspace(&config), GITHUB_MENTION_ID, None).unwrap();
        assert_eq!(
            removed,
            Some(super::super::pack::lock::lock_path(&config, "core")),
            "a pack's own route is removed by its pack's record"
        );
        assert!(super::super::pack::lock::read_lock(&config, "core")
            .unwrap()
            .is_removed(super::super::pack::PackItemKind::Route, GITHUB_MENTION_ID));

        synced_workspace(temp.path());
        assert!(
            get_route(&LibraryRoots::workspace(&config), GITHUB_MENTION_ID)
                .unwrap()
                .is_none()
        );
        assert!(get_route(&LibraryRoots::workspace(&config), FOLLOWED_ID)
            .unwrap()
            .is_some());
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

        let route = get_route(&LibraryRoots::workspace(&config), "legacy")
            .unwrap()
            .unwrap();
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
            std::fs::read_to_string(config.join("packs/core/routes/followed-thread-stream.yaml"))
                .unwrap();
        std::fs::create_dir_all(project.join(".cairn/routes")).unwrap();
        std::fs::create_dir_all(config.join("routes")).unwrap();
        std::fs::write(config.join("routes/workspace.yaml"), &shipped).unwrap();
        std::fs::write(
            project.join(".cairn/routes/project.yaml"),
            shipped.replace("Followed thread stream", "Project only"),
        )
        .unwrap();

        let roots = crate::config::trusted_roots(&config, &project);
        let routes = list_project_routes(&roots).unwrap();
        assert_eq!(routes.len(), 1);
        let ConfigResult::Ok(route) = &routes[0].entry else {
            panic!("valid route")
        };
        assert_eq!(route.id, "project");
        assert!(route.is_project_scoped());
        assert!(get_project_route(&roots, "workspace").unwrap().is_none());
        assert!(get_project_route(&roots, FOLLOWED_ID).unwrap().is_none());
    }
}
