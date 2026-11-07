//! Response definition file loading: `responses/<id>.md` in a project's
//! `.cairn/`, the workspace, or an installed pack, resolved nearest-first
//! through [`cairn_config::library`].

use super::{id_from_path, ConfigResult};
use crate::responses::{parse_definition, ResponseDefinition};
use cairn_config::library::{
    self, LibraryItem, LibraryKind, LibraryRef, LibraryRoots, Listed, Origin, ResolveError,
    Resolved,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileResponse {
    pub id: String,
    #[serde(flatten)]
    pub definition: ResponseDefinition,
    /// The layer this response was read from, or the layer a new one is for.
    pub origin: Origin,
    pub file_path: PathBuf,
}

impl FileResponse {
    pub fn is_project_scoped(&self) -> bool {
        self.origin.is_project()
    }
}

/// A response's authority is what it spends: the models it completes within,
/// the launch options over them, and how long it may run. Every field is named
/// below, so a field added to a response does not compile until it is
/// classified.
impl LibraryItem for FileResponse {
    fn library_id(&self) -> &str {
        &self.id
    }

    fn authority(&self) -> Map<String, Value> {
        let FileResponse {
            id: _,
            definition,
            origin: _,
            file_path: _,
        } = self;
        let crate::responses::ResponseDefinition {
            // Prose.
            name: _,
            description: _,
            models,
            options,
            // Retired keys: a definition naming one is refused when it loads,
            // so they never select anything.
            tier: _,
            model: _,
            backend: _,
            // The inputs it is filled from, the shape it answers in, worked
            // examples, and the template: what is written, not what is spent.
            variables: _,
            output: _,
            timeout,
            examples: _,
            template: _,
        } = definition;
        let mut fields = Map::new();
        if let Some(models) = models {
            fields.insert("models".into(), json!(models));
        }
        if !options.is_empty() {
            let options: std::collections::BTreeMap<_, _> = options.iter().collect();
            fields.insert("options".into(), json!(options));
        }
        // The default bound is no grant; only a longer or shorter one is.
        if *timeout != crate::responses::default_timeout() {
            fields.insert("timeoutSeconds".into(), json!(timeout.as_secs()));
        }
        fields
    }
}

fn load_response_in(dir: &Path, id: &str, origin: &Origin) -> Option<ConfigResult<FileResponse>> {
    let path = dir.join(format!("{id}.md"));
    path.is_file()
        .then(|| load_response_file(&path, origin.clone()))
}

fn enumerate_responses(
    dir: &Path,
    origin: &Origin,
) -> Result<Vec<(String, ConfigResult<FileResponse>)>, String> {
    let mut paths = std::fs::read_dir(dir)
        .map_err(|e| format!("Failed to read responses directory: {e}"))?
        .map(|entry| {
            entry
                .map(|e| e.path())
                .map_err(|e| format!("Failed to read directory entry: {e}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    Ok(paths
        .into_iter()
        .filter(|path| !path.is_dir() && path.extension().and_then(|e| e.to_str()) == Some("md"))
        .filter_map(|path| {
            let id = id_from_path(&path)?;
            Some((id, load_response_file(&path, origin.clone())))
        })
        .collect())
}

/// Every response definition across the layers, with its origin and what it
/// shadows.
pub fn list_response_rows(roots: &LibraryRoots) -> Result<Vec<Listed<FileResponse>>, String> {
    roots.list(LibraryKind::Response, enumerate_responses)
}

/// The responses in force, one per name.
pub fn list_responses(roots: &LibraryRoots) -> Result<Vec<ConfigResult<FileResponse>>, String> {
    Ok(library::active(list_response_rows(roots)?))
}

/// Resolve a response reference (an id or
/// `cairn://packs/<pack>/responses/<id>`) nearest-first.
pub fn resolve_response(
    roots: &LibraryRoots,
    reference: &str,
) -> Result<Resolved<FileResponse>, ResolveError> {
    let parsed = LibraryRef::parse(LibraryKind::Response, reference).map_err(|error| {
        ResolveError::Invalid {
            path: PathBuf::new(),
            error,
            origin: None,
        }
    })?;
    roots.resolve(LibraryKind::Response, &parsed, load_response_in)
}

/// Resolve response `id` in one layer only (see [`LibraryRoots::resolve_in`]).
pub fn resolve_response_in(
    roots: &LibraryRoots,
    layer: &Origin,
    id: &str,
) -> Result<Resolved<FileResponse>, ResolveError> {
    roots.resolve_in(LibraryKind::Response, layer, id, load_response_in)
}

pub fn get_response(roots: &LibraryRoots, id: &str) -> Result<Option<FileResponse>, String> {
    match resolve_response(roots, id) {
        Ok(resolved) => Ok(Some(resolved.item)),
        Err(error) if error.is_not_found() => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// Delete the response `id` resolves to, or the one in `layer` when named. A
/// workspace or project definition is deleted outright; a pack's own response
/// is recorded as removed from its pack. Returns the path that changed, so
/// the caller can commit it.
pub fn delete_response(
    roots: &LibraryRoots,
    id: &str,
    layer: Option<&Origin>,
) -> Result<Option<PathBuf>, String> {
    let (origin, path) = match layer {
        Some(origin @ (Origin::Project { .. } | Origin::Workspace)) => {
            let path = roots
                .layer_dir(LibraryKind::Response, origin)
                .ok_or_else(|| format!("No {origin} layer to delete response `{id}` from"))?
                .join(format!("{id}.md"));
            if !path.is_file() {
                return Ok(None);
            }
            (origin.clone(), path)
        }
        Some(origin) => match resolve_response_in(roots, origin, id) {
            Ok(resolved) => (resolved.origin, resolved.item.file_path),
            Err(ResolveError::Invalid {
                path,
                origin: Some(origin),
                ..
            }) => (origin, path),
            Err(error) if error.is_not_found() => return Ok(None),
            Err(error) => return Err(error.to_string()),
        },
        None => match resolve_response(roots, id) {
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
    library::remove_definition(roots, LibraryKind::Response, id, &origin, &path).map(Some)
}

fn load_response_file(path: &Path, origin: Origin) -> ConfigResult<FileResponse> {
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
            origin,
            file_path: path.to_path_buf(),
        }),
        Err(error) => fail(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_config::library::test_support::*;
    use tempfile::tempdir;

    const VALID: &str = "---\nname: Workspace\ndescription: valid\n---\nPrompt";

    fn trusted_roots(config: &Path, project_dir: &Path) -> LibraryRoots {
        LibraryRoots::workspace(config).with_project(project(project_dir, "demo", trusted(&[])))
    }

    #[test]
    fn project_definition_shadows_workspace_by_id() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        let project_dir = temp.path().join("project");
        write_item(&config, LibraryKind::Response, "shared", VALID);
        write_item(
            &project_root(&project_dir),
            LibraryKind::Response,
            "shared",
            &VALID.replace("Workspace", "Project"),
        );
        let listed = list_responses(&trusted_roots(&config, &project_dir)).unwrap();
        assert_eq!(listed.len(), 1);
        let ConfigResult::Ok(response) = &listed[0] else {
            panic!("expected valid response")
        };
        assert_eq!(response.definition.name, "Project");
        assert!(response.is_project_scoped());
    }

    #[test]
    fn malformed_definition_remains_visible_and_shadows_workspace() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("config");
        let project_dir = temp.path().join("project");
        write_item(&config, LibraryKind::Response, "shared", VALID);
        write_item(
            &project_root(&project_dir),
            LibraryKind::Response,
            "shared",
            "not frontmatter",
        );
        let listed = list_responses(&trusted_roots(&config, &project_dir)).unwrap();
        assert_eq!(listed.len(), 1);
        assert!(
            matches!(&listed[0], ConfigResult::Err { path, .. } if path.starts_with(&project_dir))
        );
    }

    #[test]
    fn bundled_conveyor_parses() {
        let parsed =
            parse_definition(include_str!("../../../../packs/core/responses/conveyor.md")).unwrap();
        assert_eq!(
            parsed.models,
            Some(vec![cairn_config::allotment::AllotmentEntry::Pool(
                "sm".into()
            )])
        );
        assert!(parsed
            .render(&serde_json::json!({"text":"Dense prose"}))
            .unwrap()
            .contains("Dense prose"));
    }
}
