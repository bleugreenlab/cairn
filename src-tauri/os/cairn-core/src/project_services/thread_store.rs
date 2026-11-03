//! Thread-owned services: where their versions live, and the second
//! [`ServiceSource`].
//!
//! A thread writes `cairn:~/services/<name>` the way it writes a page: a
//! payload of `title`, `description`, `files`, `entry`, and `manifest`,
//! `create` to replace it and `patch` to revise it (files merge per path). Each
//! write appends a version to `thread_services`, numbered per (thread, name),
//! and the newest version is what the supervisor serves. Only the owning
//! thread writes: its session jobs and the tasks they spawn, the same rule as
//! its pages.
//!
//! A payload becomes a package exactly as a repository directory does. The
//! package's `service.yaml` is not a file the thread writes: it is built from
//! the payload's `title` (the manifest's `name`), `description`, `entry`, and
//! the keys under `manifest` (`start`, `env`, `limits`, `executor`), then
//! parsed by the same validator, so a thread's service has the same declaration
//! rules and the same source hash rule as a project's.

use cairn_db::turso::params;
use serde_json::{Map, Value};

use super::declaration::{
    is_valid_service_id, package_from_files_in, PackageOrigin, ServiceListing, ServicePackage,
    ServiceSource, DEFAULT_ENTRY, MANIFEST_FILE,
};
use super::ServiceOwner;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// The keys a thread service's payload may carry.
const PAYLOAD_KEYS: &[&str] = &["title", "description", "files", "entry", "manifest"];

/// One stored version of a thread's service.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ThreadServiceRow {
    pub name: String,
    pub version: i64,
    pub data: Value,
    pub source_hash: String,
    pub created_at: i64,
}

fn row_of(row: &cairn_db::turso::Row) -> DbResult<ThreadServiceRow> {
    let data = row.text(2)?;
    Ok(ThreadServiceRow {
        name: row.text(0)?,
        version: row.i64(1)?,
        data: serde_json::from_str(&data).map_err(|error| {
            DbError::Row(format!("stored thread service data is not JSON: {error}"))
        })?,
        source_hash: row.text(3)?,
        created_at: row.i64(4)?,
    })
}

const COLUMNS: &str = "name, version, data, source_hash, created_at";

/// The thread a service address names, as the owner of its services.
pub(crate) async fn resolve_owner(
    db: &LocalDb,
    project_key: &str,
    thread: &str,
) -> Result<(String, ServiceOwner), String> {
    let (key, name) = (project_key.to_string(), thread.to_string());
    db.read(move |conn| {
        let (key, name) = (key.clone(), name.clone());
        Box::pin(async move {
            let id = match crate::threads::thread_id_for_coordinate_conn(conn, &key, &name).await {
                Ok(id) => id,
                Err(DbError::Internal(message)) => return Ok(Err(message)),
                Err(error) => return Err(error),
            };
            let mut rows = conn
                .query(
                    "SELECT project_id FROM threads WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            let project_id = match rows.next().await? {
                Some(row) => row.text(0)?,
                None => return Ok(Err(format!("Thread '{name}' not found"))),
            };
            Ok(Ok((project_id, ServiceOwner::Thread { id, name })))
        })
    })
    .await
    .map_err(|error| format!("Failed to resolve the thread: {error}"))?
}

async fn latest_conn(
    conn: &cairn_db::turso::Connection,
    thread_id: &str,
    name: &str,
) -> DbResult<Option<ThreadServiceRow>> {
    let mut rows = conn
        .query(
            &format!(
                "SELECT {COLUMNS} FROM thread_services WHERE thread_id = ?1 AND name = ?2
                 ORDER BY version DESC LIMIT 1"
            ),
            params![thread_id, name],
        )
        .await?;
    rows.next().await?.map(|row| row_of(&row)).transpose()
}

/// The newest version of `name`, or exactly `version`.
pub(crate) async fn load(
    db: &LocalDb,
    thread_id: &str,
    name: &str,
    version: Option<i64>,
) -> Result<Option<ThreadServiceRow>, String> {
    let (thread_id, name) = (thread_id.to_string(), name.to_string());
    db.read(move |conn| {
        let (thread_id, name) = (thread_id.clone(), name.clone());
        Box::pin(async move {
            match version {
                None => latest_conn(conn, &thread_id, &name).await,
                Some(version) => {
                    let mut rows = conn
                        .query(
                            &format!(
                                "SELECT {COLUMNS} FROM thread_services
                                 WHERE thread_id = ?1 AND name = ?2 AND version = ?3"
                            ),
                            params![thread_id.as_str(), name.as_str(), version],
                        )
                        .await?;
                    rows.next().await?.map(|row| row_of(&row)).transpose()
                }
            }
        })
    })
    .await
    .map_err(|error| format!("Failed to load the thread service: {error}"))
}

/// Every version of `name`'s package, newest first, as (version, source hash):
/// what a status read maps a serving hash back to.
pub(crate) async fn versions(
    db: &LocalDb,
    thread_id: &str,
    name: &str,
) -> Result<Vec<(i64, String)>, String> {
    let (thread_id, name) = (thread_id.to_string(), name.to_string());
    db.read(move |conn| {
        let (thread_id, name) = (thread_id.clone(), name.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT version, source_hash FROM thread_services
                     WHERE thread_id = ?1 AND name = ?2 ORDER BY version DESC",
                    params![thread_id.as_str(), name.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.i64(0)?, row.text(1)?));
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("Failed to load the thread service's versions: {error}"))
}

/// The newest version of every service the thread owns, by name.
pub(crate) async fn list_latest(
    db: &LocalDb,
    thread_id: &str,
) -> Result<Vec<ThreadServiceRow>, String> {
    let thread_id = thread_id.to_string();
    db.read(move |conn| {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    &format!(
                        "SELECT {COLUMNS} FROM thread_services WHERE thread_id = ?1
                         ORDER BY name, version DESC"
                    ),
                    params![thread_id.as_str()],
                )
                .await?;
            let mut out: Vec<ThreadServiceRow> = Vec::new();
            while let Some(row) = rows.next().await? {
                let row = row_of(&row)?;
                if out.last().is_some_and(|last| last.name == row.name) {
                    continue;
                }
                out.push(row);
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("Failed to list the thread's services: {error}"))
}

/// Every (project, thread) that owns a service, for the runner's start.
pub(crate) async fn owners(db: &LocalDb) -> Result<Vec<(String, ServiceOwner)>, String> {
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT DISTINCT s.project_id, t.id, t.name FROM thread_services s
                     JOIN threads t ON t.id = s.thread_id",
                    (),
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((
                    row.text(0)?,
                    ServiceOwner::Thread {
                        id: row.text(1)?,
                        name: row.text(2)?,
                    },
                ));
            }
            Ok(out)
        })
    })
    .await
    .map_err(|error| format!("Failed to list thread service owners: {error}"))
}

/// The package a stored (or about-to-be-stored) payload builds, validated by
/// the same rules as a repository package. `uri` names it in errors.
pub(crate) fn package(name: &str, uri: &str, data: &Value) -> Result<ServicePackage, String> {
    let object = data
        .as_object()
        .ok_or_else(|| "a service payload is an object".to_string())?;
    if let Some(key) = object
        .keys()
        .find(|key| !PAYLOAD_KEYS.contains(&key.as_str()))
    {
        return Err(format!(
            "`{key}` is not a service field: a service carries {}",
            PAYLOAD_KEYS.join(", ")
        ));
    }
    let files = object
        .get("files")
        .and_then(Value::as_object)
        .filter(|files| !files.is_empty())
        .ok_or_else(|| {
            "a service carries files: an object of path -> text, its entry among them".to_string()
        })?;
    let mut package_files = Vec::with_capacity(files.len() + 1);
    for (path, content) in files {
        crate::pages::validate_file_path(path)?;
        if path == MANIFEST_FILE {
            return Err(format!(
                "files may not include {MANIFEST_FILE}: a thread's service is declared by its \
                 title, description, entry, and manifest, and {MANIFEST_FILE} is built from them"
            ));
        }
        let text = content
            .as_str()
            .ok_or_else(|| format!("file `{path}` must be text"))?;
        package_files.push((path.clone(), text.as_bytes().to_vec()));
    }

    let mut manifest: Map<String, Value> = match object.get("manifest") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(manifest)) => manifest.clone(),
        Some(_) => {
            return Err(
                "manifest must be an object of start, env, limits, executor, schedule, and \
                 idleStopAfter"
                    .to_string(),
            )
        }
    };
    for key in ["name", "description", "entry"] {
        if manifest.contains_key(key) {
            let top = if key == "name" { "title" } else { key };
            return Err(format!(
                "manifest.{key} is refused: set `{top}` at the top of the payload instead"
            ));
        }
    }
    let text_field = |key: &str| -> Result<Option<String>, String> {
        match object.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(text)) => Ok(Some(text.clone())),
            Some(_) => Err(format!("`{key}` must be a string")),
        }
    };
    let title = text_field("title")?
        .filter(|title| !title.trim().is_empty())
        .unwrap_or_else(|| name.to_string());
    manifest.insert("name".into(), Value::String(title));
    if let Some(description) = text_field("description")? {
        manifest.insert("description".into(), Value::String(description));
    }
    let entry = text_field("entry")?.unwrap_or_else(|| DEFAULT_ENTRY.to_string());
    manifest.insert("entry".into(), Value::String(entry));
    // JSON is YAML, so the one manifest parser reads it.
    let manifest_text = serde_json::to_string_pretty(&Value::Object(manifest))
        .map_err(|error| format!("the manifest does not serialize: {error}"))?;
    package_files.push((MANIFEST_FILE.to_string(), manifest_text.into_bytes()));
    package_from_files_in(&PackageOrigin::thread(uri), name, package_files)
}

/// Who is writing a thread's service: an authenticated run, by its job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceWriter {
    pub job_id: String,
}

/// A version as written, and what it builds.
#[derive(Debug, Clone)]
pub(crate) struct WrittenVersion {
    pub version: i64,
    pub source_hash: String,
}

fn not_the_owner(uri: &str) -> String {
    format!(
        "{uri} is not this run's to write. A thread's service is its own deploy, so only the \
         owning thread (its sessions and their tasks) writes it. Send the source to that thread \
         and let it write the service."
    )
}

/// Whether `writer` is one of the thread's own jobs. A service job is never
/// one: it names its owner in `service_thread_id`, not `thread_id`.
async fn may_write_conn(
    conn: &cairn_db::turso::Connection,
    thread_id: &str,
    writer: &ServiceWriter,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT 1 FROM jobs WHERE id = ?1 AND thread_id = ?2",
            params![writer.job_id.as_str(), thread_id],
        )
        .await?;
    Ok(rows.next().await?.is_some())
}

/// One write's inputs, moved into the transaction that performs it.
#[derive(Clone)]
struct WriteRequest {
    project_id: String,
    thread_id: String,
    name: String,
    uri: String,
    writer: ServiceWriter,
    payload: Value,
    is_patch: bool,
    dry_run: bool,
}

async fn write_conn(
    conn: &cairn_db::turso::Connection,
    request: &WriteRequest,
) -> DbResult<Result<WrittenVersion, String>> {
    if !may_write_conn(conn, &request.thread_id, &request.writer).await? {
        return Ok(Err(not_the_owner(&request.uri)));
    }
    let latest = latest_conn(conn, &request.thread_id, &request.name).await?;
    let composed = match crate::pages::compose(
        latest.as_ref().map(|row| &row.data),
        &request.payload,
        request.is_patch,
        DEFAULT_ENTRY,
    ) {
        Ok(composed) => composed,
        Err(message) => return Ok(Err(message)),
    };
    let built = match package(&request.name, &request.uri, &composed) {
        Ok(built) => built,
        Err(message) => return Ok(Err(message)),
    };
    let version = latest.as_ref().map_or(1, |row| row.version + 1);
    let written = WrittenVersion {
        version,
        source_hash: built.source_hash,
    };
    if request.dry_run {
        return Ok(Ok(written));
    }
    let data = serde_json::to_string(&composed)
        .map_err(|error| DbError::Internal(format!("service data does not serialize: {error}")))?;
    let id = cairn_common::ids::mint_child(&request.thread_id);
    conn.execute(
        "INSERT INTO thread_services (id, project_id, thread_id, name, version, data,
                                      source_hash, written_by_job_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            id.as_str(),
            request.project_id.as_str(),
            request.thread_id.as_str(),
            request.name.as_str(),
            version,
            data.as_str(),
            written.source_hash.as_str(),
            request.writer.job_id.as_str(),
            chrono::Utc::now().timestamp()
        ],
    )
    .await?;
    Ok(Ok(written))
}

/// Whether `job_id` is one of the thread's own jobs: who may write, start,
/// stop, or restart the thread's services.
pub(crate) async fn is_owners_job(
    db: &LocalDb,
    thread_id: &str,
    job_id: &str,
) -> Result<bool, String> {
    let (thread_id, writer) = (
        thread_id.to_string(),
        ServiceWriter {
            job_id: job_id.to_string(),
        },
    );
    db.read(move |conn| {
        let (thread_id, writer) = (thread_id.clone(), writer.clone());
        Box::pin(async move { may_write_conn(conn, &thread_id, &writer).await })
    })
    .await
    .map_err(|error| format!("Failed to check the service's owner: {error}"))
}

/// Compose `payload` onto the newest version, validate the package it builds,
/// and append it as the next version, in one write transaction so the version
/// number and the patch base are the same read. A dry run validates the same
/// way and stores nothing.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write(
    db: &LocalDb,
    project_id: &str,
    owner: &ServiceOwner,
    name: &str,
    uri: &str,
    writer: &ServiceWriter,
    payload: &Value,
    is_patch: bool,
    dry_run: bool,
) -> Result<WrittenVersion, String> {
    let ServiceOwner::Thread { id: thread_id, .. } = owner else {
        return Err("a project service is declared in its repository, not written".to_string());
    };
    if !is_valid_service_id(name) {
        return Err(format!(
            "service name `{name}` is not valid: use a lowercase letter, then up to 62 lowercase letters, digits, or '-'"
        ));
    }
    let request = WriteRequest {
        project_id: project_id.to_string(),
        thread_id: thread_id.clone(),
        name: name.to_string(),
        uri: uri.to_string(),
        writer: writer.clone(),
        payload: payload.clone(),
        is_patch,
        dry_run,
    };
    let outcome = if dry_run {
        db.read(move |conn| {
            let request = request.clone();
            Box::pin(async move { write_conn(conn, &request).await })
        })
        .await
    } else {
        db.write(move |conn| {
            let request = request.clone();
            Box::pin(async move { write_conn(conn, &request).await })
        })
        .await
    };
    outcome.map_err(|error| format!("Failed to store the service: {error}"))?
}

/// A thread's services as they stand now: the second [`ServiceSource`].
///
/// The newest version of each is read up front, so the source is a snapshot
/// the supervisor reads off the async runtime like any other.
pub(crate) struct ThreadSource {
    revision: String,
    packages: Vec<(String, Result<ServicePackage, String>)>,
}

impl ThreadSource {
    pub(crate) async fn load(
        db: &LocalDb,
        project_key: &str,
        owner: &ServiceOwner,
    ) -> Result<Self, String> {
        let ServiceOwner::Thread { id, name } = owner else {
            return Err("a project's services come from its repository".to_string());
        };
        let rows = list_latest(db, id).await?;
        let packages = rows
            .iter()
            .map(|row| {
                let uri = owner.home(project_key, &row.name);
                (row.name.clone(), package(&row.name, &uri, &row.data))
            })
            .collect();
        Ok(Self {
            revision: format!("thread {name}"),
            packages,
        })
    }
}

impl ServiceSource for ThreadSource {
    fn revision(&self) -> &str {
        &self.revision
    }

    fn list(&self) -> Result<Vec<ServiceListing>, String> {
        Ok(self
            .packages
            .iter()
            .map(|(id, package)| ServiceListing {
                id: id.clone(),
                declaration: package
                    .as_ref()
                    .map(|p| p.declaration.clone())
                    .map_err(Clone::clone),
            })
            .collect())
    }

    fn package(&self, id: &str) -> Result<Option<ServicePackage>, String> {
        match self.packages.iter().find(|(name, _)| name == id) {
            None => Ok(None),
            Some((_, package)) => package.clone().map(Some),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const URI: &str = "cairn://p/cairn/spanish/services/tutor";

    #[test]
    fn a_payload_builds_the_package_a_repository_directory_would() {
        let package = package(
            "tutor",
            URI,
            &json!({
                "title": "Spanish tutor",
                "description": "Drills and corrections.",
                "files": {"main.ts": "serve({})", "lib/words.ts": "export const w = 1;"},
                "manifest": {"limits": {"callTimeoutSeconds": 60}, "env": {"LEVEL": "a2"}},
            }),
        )
        .unwrap();
        let declaration = &package.declaration;
        assert_eq!(declaration.id, "tutor");
        assert_eq!(declaration.name, "Spanish tutor");
        assert_eq!(declaration.entry(), Some("main.ts"));
        assert_eq!(declaration.limits.call_timeout_seconds, 60);
        assert_eq!(declaration.env.get("LEVEL").map(String::as_str), Some("a2"));
        let paths: Vec<&str> = package
            .files
            .iter()
            .map(|(path, _)| path.as_str())
            .collect();
        assert_eq!(paths, ["lib/words.ts", "main.ts", "service.yaml"]);
    }

    #[test]
    fn the_title_defaults_to_the_name_and_the_hash_moves_with_the_manifest() {
        let bare = package("tutor", URI, &json!({"files": {"main.ts": "serve({})"}})).unwrap();
        assert_eq!(bare.declaration.name, "tutor");
        let launched = package(
            "tutor",
            URI,
            &json!({"files": {"main.ts": "serve({})"}, "manifest": {"start": "launch"}}),
        )
        .unwrap();
        assert_ne!(bare.source_hash, launched.source_hash);
    }

    #[test]
    fn the_runner_owned_and_misplaced_keys_are_refused_naming_the_service() {
        let refused = |data: Value| package("tutor", URI, &data).unwrap_err();
        let files = json!({"main.ts": "serve({})"});
        assert!(
            refused(json!({"files": files, "manifest": {"env": {"CAIRN_RUN_ID": "x"}}}))
                .contains("CAIRN_*")
        );
        assert!(refused(json!({"files": files, "manifest": {"name": "x"}})).contains("`title`"));
        assert!(refused(json!({"files": files, "promoted": true})).contains("not a service field"));
        assert!(
            refused(json!({"files": {"service.yaml": "name: x", "main.ts": ""}}))
                .contains("built from them")
        );
        let unknown = refused(json!({"files": files, "manifest": {"ports": [80]}}));
        assert!(unknown.starts_with(URI), "{unknown}");
        let missing = refused(json!({"files": {"index.ts": ""}}));
        assert!(
            missing.contains("entry `main.ts` is not in the package (index.ts)"),
            "{missing}"
        );
        assert!(refused(json!({"files": {"../x.ts": ""}})).contains("not valid"));
        assert!(refused(json!({"files": {}})).contains("carries files"));
    }
}
