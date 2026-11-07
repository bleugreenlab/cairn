//! What a page's data may be, and how a write composes onto the stored page.

use serde_json::{json, Map, Value};

/// The `artifact_type` every page row carries.
pub const PAGE_ARTIFACT_TYPE: &str = "page";

/// The entry module a page names when it names none.
pub(crate) const DEFAULT_ENTRY: &str = "index.js";

/// The cap on a page's files, all of them together.
pub const MAX_PAGE_BYTES: usize = 1 << 20;

/// The `output_name` a page is stored under. The prefix keeps a page called
/// `plan` or `review` from colliding with the artifacts of that name.
pub(crate) fn output_name(name: &str) -> String {
    format!("pages/{name}")
}

/// A page name: `^[a-z0-9][a-z0-9-]{0,62}$`. It is a URI segment and a facet
/// label, so it stays short and needs no escaping anywhere.
pub fn validate_page_name(name: &str) -> Result<(), String> {
    let valid = (1..=63).contains(&name.len())
        && name
            .bytes()
            .next()
            .is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(format!(
            "page name `{name}` is not valid: use 1-63 lowercase letters, digits, and '-', starting with a letter or digit"
        ))
    }
}

/// The page (or thread service: the two are written alike) a write leaves
/// behind.
///
/// `create` replaces the whole, defaulting `entry` to `default_entry`. `patch`
/// merges `files` per path, where `null` deletes that file, and replaces every
/// other key it carries (`null` removes an optional key). The generic artifact
/// merge would replace `files` wholesale, which would make editing one file
/// mean resending all of them.
pub(crate) fn compose(
    base: Option<&Value>,
    payload: &Value,
    is_patch: bool,
    default_entry: &str,
) -> Result<Value, String> {
    let changes = payload
        .as_object()
        .ok_or_else(|| "the payload is an object".to_string())?;
    let mut page: Map<String, Value> = if is_patch {
        base.and_then(Value::as_object).cloned().ok_or_else(|| {
            "there is nothing here to patch yet: write it first with mode `create`".to_string()
        })?
    } else {
        Map::new()
    };
    for (key, value) in changes {
        if is_patch && key == "files" {
            let edits = value.as_object().ok_or_else(|| {
                "payload.files must be an object of path -> text, with null deleting a file"
                    .to_string()
            })?;
            let files = page
                .entry("files")
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .ok_or_else(|| "the stored page's files are not an object".to_string())?;
            for (path, content) in edits {
                if content.is_null() {
                    files.remove(path);
                } else {
                    files.insert(path.clone(), content.clone());
                }
            }
        } else if is_patch && value.is_null() {
            page.remove(key);
        } else {
            page.insert(key.clone(), value.clone());
        }
    }
    page.entry("entry")
        .or_insert_with(|| Value::String(default_entry.to_string()));
    Ok(Value::Object(page))
}

/// Every rule a stored page satisfies: the `page` preset's shape, then what a
/// JSON Schema cannot say.
pub(crate) fn validate(page: &Value) -> Result<(), String> {
    // Ahead of the schema, so a malformed entry is refused with the rule it
    // breaks rather than a pattern mismatch.
    if page.pointer("/manifest/network").is_some() {
        return Err(NETWORK_RETIRED.to_string());
    }
    validate_writes(page)?;
    let schema = crate::output_schemas::load_embedded_preset_schema(PAGE_ARTIFACT_TYPE)?;
    crate::mcp::handlers::comments_artifacts::validate_against_schema(&schema, page)
        .map_err(|error| error.replacen("Artifact does not match", "Page does not match", 1))?;

    let files = page
        .get("files")
        .and_then(Value::as_object)
        .ok_or_else(|| "a page carries files".to_string())?;
    let mut total = 0usize;
    for (path, content) in files {
        validate_file_path(path)?;
        total += content.as_str().map_or(0, str::len);
    }
    if total > MAX_PAGE_BYTES {
        return Err(format!(
            "page files total {total} bytes, over the 1 MiB ({MAX_PAGE_BYTES} bytes) cap: keep data in database rows the page reads rather than embedding it"
        ));
    }

    let entry = page
        .get("entry")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_ENTRY);
    if !(entry.ends_with(".js") || entry.ends_with(".mjs")) {
        return Err(format!(
            "entry `{entry}` must be a .js or .mjs module whose default export is the page's React component"
        ));
    }
    if !files.contains_key(entry) {
        let mut paths: Vec<&str> = files.keys().map(String::as_str).collect();
        paths.sort_unstable();
        return Err(format!(
            "entry `{entry}` is not among the page's files ({}); add it to files or set entry to one of them",
            paths.join(", ")
        ));
    }

    validate_services(&service_names(page))?;
    Ok(())
}

/// Why a page may not declare `manifest.network` (tenet #1312).
const NETWORK_RETIRED: &str = "manifest.network is retired: a page reads only database rows, never the network. To show data from an https API (or vcs history, transcripts, anything outside Cairn's databases), declare a service in manifest.services whose method fetches it, on demand or on the service's schedule, and writes rows to the thread's database; the page reads those rows with useRows. Remove manifest.network (a patch's manifest replaces the whole manifest)";

/// The most services one page may declare.
pub(crate) const MAX_PAGE_SERVICES: usize = 16;

/// A service a page's manifest names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeclaredService {
    /// A project service, by its id under `.cairn/services/`.
    Project(String),
    /// A thread's service, by its canonical URI
    /// `cairn://p/<project>/<thread>/services/<name>`.
    Thread {
        project: String,
        thread: String,
        service: String,
    },
}

impl DeclaredService {
    /// One entry of `manifest.services`, or of a frame's call: a service id, or
    /// a thread service's canonical URI.
    pub(crate) fn parse(entry: &str) -> Result<Self, String> {
        let entry = entry.trim();
        if crate::project_services::declaration::is_valid_service_id(entry) {
            return Ok(Self::Project(entry.to_string()));
        }
        let refusal = || {
            format!(
                "manifest.services entry `{entry}` is neither a project service id (the directory \
                 name under .cairn/services/, a lowercase letter then lowercase letters, digits, \
                 or '-') nor a thread service's URI (cairn://p/PROJECT/THREAD/services/NAME)"
            )
        };
        let resource = cairn_uri::parse_uri(entry).ok_or_else(refusal)?;
        match cairn_uri::delegate_thread_descendant(resource).map_err(|_| refusal())? {
            cairn_uri::CairnResource::ThreadService {
                project,
                name,
                service,
            } if crate::project_services::declaration::is_valid_service_id(&service) => {
                Ok(Self::Thread {
                    project,
                    thread: name,
                    service,
                })
            }
            _ => Err(refusal()),
        }
    }

    /// The service's canonical form, so a declaration and a call compare
    /// however either spelled the project key.
    pub(crate) fn canonical(&self) -> String {
        match self {
            Self::Project(id) => id.clone(),
            Self::Thread {
                project,
                thread,
                service,
            } => cairn_uri::build_thread_service_uri(project, thread, service),
        }
    }
}

/// The services a page's manifest declares, as written.
pub(crate) fn service_names(page: &Value) -> Vec<String> {
    page.pointer("/manifest/services")
        .and_then(Value::as_array)
        .map(|services| {
            services
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Declared services are project service ids or thread service URIs, at most
/// [`MAX_PAGE_SERVICES`], each once. Whether a service exists, and whether a
/// thread's service is this page's own thread's to call, are questions for
/// call time: the repository and the thread's services change after the page
/// is written.
fn validate_services(services: &[String]) -> Result<(), String> {
    if services.len() > MAX_PAGE_SERVICES {
        return Err(format!(
            "manifest.services lists {} services; a page may declare at most {MAX_PAGE_SERVICES}",
            services.len()
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for service in services {
        let declared = DeclaredService::parse(service)?;
        if !seen.insert(declared.canonical()) {
            return Err(format!("manifest.services lists `{service}` twice"));
        }
    }
    Ok(())
}

/// A file path: segments of `[A-Za-z0-9._-]` joined by `/`, with no `.` or
/// `..` segment, so every path names exactly one file inside the page (or
/// thread service).
pub(crate) fn validate_file_path(path: &str) -> Result<(), String> {
    let valid = !path.is_empty()
        && path.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        });
    if valid {
        Ok(())
    } else {
        Err(format!(
            "file path `{path}` is not valid: use segments of letters, digits, '.', '_' and '-' joined by '/', with no '.' or '..' segment"
        ))
    }
}

/// The most write scopes a page may declare.
pub(crate) const MAX_WRITE_SCOPES: usize = 32;

/// The thread-database tables a page's manifest declares it writes, as
/// written (`cairn:~/db/<table>`).
pub(crate) fn write_scopes(page: &Value) -> Vec<String> {
    page.pointer("/manifest/writes")
        .and_then(Value::as_array)
        .map(|scopes| {
            scopes
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// The table a write scope names, when it is exactly `cairn:~/db/<table>`.
pub(crate) fn write_scope_table(scope: &str) -> Option<&str> {
    let table = scope.strip_prefix("cairn:~/db/")?;
    let valid = !table.is_empty()
        && table != cairn_uri::RESERVED_TABLE_SEGMENT
        && !table.contains(['/', '?', '#', '*', '%'])
        && !table.chars().any(char::is_whitespace);
    valid.then_some(table)
}

/// `manifest.writes`: at most [`MAX_WRITE_SCOPES`] distinct row collections of
/// the page's own thread database, each exactly `cairn:~/db/<table>`.
fn validate_writes(page: &Value) -> Result<(), String> {
    let Some(scopes) = page.pointer("/manifest/writes") else {
        return Ok(());
    };
    let scopes = scopes.as_array().ok_or_else(|| {
        "manifest.writes is a list of row collections, e.g. [\"cairn:~/db/attempts\"]".to_string()
    })?;
    if scopes.len() > MAX_WRITE_SCOPES {
        return Err(format!(
            "manifest.writes declares {} tables, over the {MAX_WRITE_SCOPES}-table cap",
            scopes.len()
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for scope in scopes {
        let scope = scope
            .as_str()
            .ok_or_else(|| "manifest.writes entries are strings".to_string())?;
        let refuse = |why: &str| {
            Err(format!(
                "manifest.writes entry `{scope}` is refused: {why}. A page may declare writes only to its own thread's tables, as `cairn:~/db/<table>`"
            ))
        };
        let Some(rest) = scope.strip_prefix("cairn:~/db/") else {
            return refuse("it is not a table of the page's own thread database");
        };
        if rest.contains('/') {
            return refuse("declare the table, not one row of it");
        }
        if rest.contains(['?', '#']) {
            return refuse("a write scope carries no query");
        }
        if rest.contains('*') {
            return refuse("a write scope names one table, never a wildcard");
        }
        if rest == cairn_uri::RESERVED_TABLE_SEGMENT {
            return refuse("`schema` is the database's schema listing, not a table");
        }
        if write_scope_table(scope).is_none() {
            return refuse("the table name is not a valid URI segment");
        }
        if !seen.insert(scope) {
            return refuse("it is declared twice");
        }
    }
    Ok(())
}
