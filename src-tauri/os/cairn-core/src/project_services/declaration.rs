//! What a project service is, as its repository declares it.
//!
//! A service is a package at `.cairn/services/<id>/`: a `service.yaml`
//! manifest beside a Bun entry built on `@cairn/sdk/service`. This module owns
//! the manifest's schema and validation, the package's source hash (the value a
//! running incarnation is compared against to decide whether it is current),
//! and the [`ServiceSource`] seam through which the supervisor reads packages.
//!
//! The seam exists so that *where* a package comes from stays a property of
//! one implementation. Today that is [`BaseTipSource`], which reads the
//! project's default-branch tip from the object store and never the operator's
//! working copy. Nothing outside this module may assume the base tip.

use cairn_codec::objects::{ObjectReadError, ObjectStore, TreeEntryKind};
use cairn_common::executor_protocol::ExecutorSelector;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use crate::orchestrator::Orchestrator;

/// The directory every project service package lives under.
pub(crate) const SERVICES_DIR: &str = ".cairn/services";
/// The manifest filename inside a service package.
pub(crate) const MANIFEST_FILE: &str = "service.yaml";
/// The entry a manifest that names none runs.
pub(crate) const DEFAULT_ENTRY: &str = "main.ts";

pub(crate) const DEFAULT_MEMORY_MB: u32 = 256;
pub(crate) const DEFAULT_CONCURRENCY: u32 = 8;
pub(crate) const DEFAULT_CALL_TIMEOUT_SECONDS: u32 = 30;
pub(crate) const MAX_CALL_TIMEOUT_SECONDS: u32 = 300;
const MAX_CONCURRENCY: u32 = 64;
const MAX_MEMORY_MB: u32 = 8 * 1024;

/// A scheduled method runs at most this often and at least this rarely: the
/// interval grammar thread automations had, which `schedule` replaces.
pub(crate) const MIN_SCHEDULE_EVERY_MS: i64 = 5 * 60 * 1_000;
pub(crate) const MAX_SCHEDULE_EVERY_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const MAX_SCHEDULE_ENTRIES: usize = 16;
/// How long an `on_demand` incarnation may sit with no call before it is
/// stopped, when its manifest names no `idleStopAfter`.
pub(crate) const DEFAULT_IDLE_STOP_AFTER: Duration = Duration::from_secs(5 * 60);
const MIN_IDLE_STOP_AFTER: Duration = Duration::from_secs(60);
const MAX_IDLE_STOP_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

/// A package is shipped to its executor as runtime assets, so it is bounded
/// well inside the protocol's own asset limits, leaving room for the SDK.
const MAX_PACKAGE_FILES: usize = 128;
const MAX_PACKAGE_BYTES: usize = 8 * 1024 * 1024;

/// When a service's first incarnation starts.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StartPolicy {
    /// On its first call, or an explicit start.
    #[default]
    OnDemand,
    /// When the runner starts, and whenever the declaration appears.
    Launch,
}

impl StartPolicy {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            StartPolicy::OnDemand => "on_demand",
            StartPolicy::Launch => "launch",
        }
    }
}

/// The placement a service may ask for: a machine by name, or a platform,
/// refined by toolchains. Deliberately narrower than [`ExecutorSelector`], whose
/// containment and nested-sandbox fields are not a package's to choose.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceExecutor {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    os: Option<String>,
    #[serde(default)]
    required_toolchains: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceLimitsManifest {
    #[serde(default)]
    memory_mb: Option<u32>,
    #[serde(default)]
    concurrency: Option<u32>,
    #[serde(default)]
    call_timeout_seconds: Option<u32>,
}

/// One `schedule` entry as written.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduleManifestEntry {
    every: String,
    method: String,
    #[serde(default)]
    args: Option<serde_json::Value>,
}

/// `service.yaml` as written.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceManifest {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    entry: Option<String>,
    #[serde(default)]
    start: StartPolicy,
    #[serde(default)]
    executor: Option<ServiceExecutor>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    limits: ServiceLimitsManifest,
    #[serde(default)]
    schedule: Vec<ScheduleManifestEntry>,
    #[serde(default)]
    idle_stop_after: Option<String>,
}

/// A method the supervisor calls on a fixed cadence, with no caller but the
/// service's owner and no turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScheduleEntry {
    /// The interval as written, for display.
    pub every: String,
    pub every_ms: i64,
    pub method: String,
    /// The call's params.
    pub args: serde_json::Value,
}

impl ScheduleEntry {
    /// What identifies the entry across versions of its declaration: method,
    /// interval, and args. Reordering entries keeps each one's cadence;
    /// changing any of the three makes a new entry with a new anchor.
    pub(crate) fn key(&self) -> String {
        let mut hasher = Sha256::new();
        for part in [
            self.method.as_bytes(),
            &self.every_ms.to_be_bytes(),
            self.args.to_string().as_bytes(),
        ] {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        format!("{:x}", hasher.finalize())[..16].to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ServiceLimits {
    pub memory_mb: u32,
    pub concurrency: u32,
    pub call_timeout_seconds: u32,
}

impl Default for ServiceLimits {
    fn default() -> Self {
        Self {
            memory_mb: DEFAULT_MEMORY_MB,
            concurrency: DEFAULT_CONCURRENCY,
            call_timeout_seconds: DEFAULT_CALL_TIMEOUT_SECONDS,
        }
    }
}

impl ServiceLimits {
    /// Calls beyond `concurrency` wait in a queue this deep before the service
    /// answers `busy`.
    pub(crate) fn queue_depth(&self) -> u32 {
        self.concurrency.saturating_mul(2)
    }
}

/// A validated declaration: everything the supervisor needs to place and
/// start a service, with every default resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceDeclaration {
    pub id: String,
    pub name: String,
    pub description: String,
    pub entry: String,
    pub start: StartPolicy,
    /// `None` lets the fleet choose.
    pub executor: Option<ExecutorSelector>,
    pub env: BTreeMap<String, String>,
    pub limits: ServiceLimits,
    /// Methods the supervisor calls on a cadence.
    pub schedule: Vec<ScheduleEntry>,
    /// How long an idle incarnation lives: `Some` for an `on_demand`
    /// service, `None` for a `launch` service, which never idle-stops.
    pub idle_stop_after: Option<Duration>,
    /// The manifest exactly as committed, for the resource read.
    pub manifest_text: String,
}

/// A declaration together with the files it runs, read at one revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServicePackage {
    pub declaration: ServiceDeclaration,
    /// Package-relative paths, sorted, with their bytes.
    pub files: Vec<(String, Vec<u8>)>,
    pub source_hash: String,
}

/// One entry in a source's listing. A malformed declaration is listed with its
/// error rather than hidden, so the index can say why a service will not start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceListing {
    pub id: String,
    pub declaration: Result<ServiceDeclaration, String>,
}

/// Where service packages come from.
///
/// One value describes one revision of one project's packages. Every method is
/// synchronous because the only implementation reads the object store, which
/// is blocking; callers run them off the async runtime.
pub(crate) trait ServiceSource: Send + Sync {
    /// The revision this source reads, as it is shown to a person.
    fn revision(&self) -> &str;
    /// Every declared service, sorted by id.
    fn list(&self) -> Result<Vec<ServiceListing>, String>;
    /// One service's declaration and files; `None` when it is not declared.
    fn package(&self, id: &str) -> Result<Option<ServicePackage>, String>;
}

/// Where a package came from, as its errors name it: a directory in the
/// repository, or a thread-owned service's resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PackageOrigin {
    package: String,
    manifest: String,
}

impl PackageOrigin {
    /// `.cairn/services/<id>` at the base tip.
    pub(crate) fn repository(id: &str) -> Self {
        Self {
            package: format!("{SERVICES_DIR}/{id}"),
            manifest: format!("{SERVICES_DIR}/{id}/{MANIFEST_FILE}"),
        }
    }

    /// A thread-owned service, whose manifest is its payload's `title`,
    /// `description`, `entry`, and `manifest` rather than a file.
    pub(crate) fn thread(uri: &str) -> Self {
        Self {
            package: uri.to_string(),
            manifest: format!("{uri} (title, description, entry, manifest)"),
        }
    }
}

/// Whether `id` may name a service: a lowercase letter, then up to 62
/// lowercase letters, digits, or hyphens.
pub(crate) fn is_valid_service_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// Parse and validate a manifest for the service `id`.
pub(crate) fn parse_service_manifest(id: &str, text: &str) -> Result<ServiceDeclaration, String> {
    parse_service_manifest_in(&PackageOrigin::repository(id), id, text)
}

/// [`parse_service_manifest`], naming `origin` in its errors.
pub(crate) fn parse_service_manifest_in(
    origin: &PackageOrigin,
    id: &str,
    text: &str,
) -> Result<ServiceDeclaration, String> {
    let (package, manifest_place) = (&origin.package, &origin.manifest);
    if !is_valid_service_id(id) {
        return Err(format!(
            "service id `{id}` must match ^[a-z][a-z0-9-]{{0,62}}$ ({package})"
        ));
    }
    let manifest: ServiceManifest =
        serde_yaml::from_str(text).map_err(|error| format!("{manifest_place}: {error}"))?;
    if manifest.name.trim().is_empty() {
        return Err(format!(
            "{manifest_place}: `name` must say what a person calls this service"
        ));
    }

    let entry = manifest
        .entry
        .unwrap_or_else(|| DEFAULT_ENTRY.to_string())
        .trim()
        .to_string();
    validate_entry(&entry).map_err(|error| format!("{package}: {error}"))?;

    for key in manifest.env.keys() {
        validate_env_key(key).map_err(|error| format!("{package}: {error}"))?;
    }

    let executor = match manifest.executor {
        None => None,
        Some(executor) => {
            let selector = ExecutorSelector {
                name: executor.name.filter(|name| !name.trim().is_empty()),
                os: executor.os.filter(|os| !os.trim().is_empty()),
                required_toolchains: executor.required_toolchains,
                ..ExecutorSelector::default()
            };
            selector
                .validate()
                .map_err(|error| format!("{package}: executor: {error}"))?;
            Some(selector)
        }
    };

    let limits =
        resolve_limits(&manifest.limits).map_err(|error| format!("{package}: limits: {error}"))?;
    let schedule =
        resolve_schedule(&manifest.schedule).map_err(|error| format!("{package}: {error}"))?;
    let idle_stop_after = resolve_idle_stop(manifest.start, manifest.idle_stop_after.as_deref())
        .map_err(|error| format!("{package}: {error}"))?;

    Ok(ServiceDeclaration {
        id: id.to_string(),
        name: manifest.name.trim().to_string(),
        description: manifest.description.trim().to_string(),
        entry,
        start: manifest.start,
        executor,
        env: manifest.env,
        limits,
        schedule,
        idle_stop_after,
        manifest_text: text.to_string(),
    })
}

fn resolve_schedule(entries: &[ScheduleManifestEntry]) -> Result<Vec<ScheduleEntry>, String> {
    if entries.len() > MAX_SCHEDULE_ENTRIES {
        return Err(format!(
            "schedule has {} entries; a service may schedule at most {MAX_SCHEDULE_ENTRIES}",
            entries.len()
        ));
    }
    let mut resolved: Vec<ScheduleEntry> = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let at = format!("schedule[{index}]");
        let every = entry.every.trim();
        let every_ms = crate::duration::parse_duration_ms(every)
            .ok()
            .and_then(|ms| i64::try_from(ms).ok())
            .filter(|ms| (MIN_SCHEDULE_EVERY_MS..=MAX_SCHEDULE_EVERY_MS).contains(ms))
            .ok_or_else(|| {
                format!("{at}.every `{every}` must be a duration from 5m to 30d (like 15m, 1h, 1d)")
            })?;
        let method = entry.method.trim();
        if method.is_empty()
            || method.len() > 128
            || method
                .chars()
                .any(|ch| ch.is_whitespace() || ch.is_control())
        {
            return Err(format!(
                "{at}.method must name one of the service's methods (up to 128 characters, no spaces)"
            ));
        }
        let args = entry.args.clone().unwrap_or(serde_json::Value::Null);
        if args.to_string().len() > super::supervisor::MAX_PARAMS_BYTES {
            return Err(format!(
                "{at}.args are larger than a call may carry ({} bytes)",
                super::supervisor::MAX_PARAMS_BYTES
            ));
        }
        let resolved_entry = ScheduleEntry {
            every: every.to_string(),
            every_ms,
            method: method.to_string(),
            args,
        };
        if let Some(first) = resolved
            .iter()
            .position(|earlier| earlier.key() == resolved_entry.key())
        {
            return Err(format!("{at} repeats schedule[{first}]"));
        }
        resolved.push(resolved_entry);
    }
    Ok(resolved)
}

fn resolve_idle_stop(
    start: StartPolicy,
    written: Option<&str>,
) -> Result<Option<Duration>, String> {
    match (start, written) {
        (StartPolicy::Launch, None) => Ok(None),
        (StartPolicy::Launch, Some(_)) => Err(
            "idleStopAfter applies only to `start: on_demand`; a `launch` service runs until it is \
             stopped"
                .to_string(),
        ),
        (StartPolicy::OnDemand, None) => Ok(Some(DEFAULT_IDLE_STOP_AFTER)),
        (StartPolicy::OnDemand, Some(written)) => {
            let written = written.trim();
            crate::duration::parse_duration_ms(written)
                .ok()
                .map(Duration::from_millis)
                .filter(|idle| (MIN_IDLE_STOP_AFTER..=MAX_IDLE_STOP_AFTER).contains(idle))
                .map(Some)
                .ok_or_else(|| {
                    format!("idleStopAfter `{written}` must be a duration from 1m to 24h (like 5m)")
                })
        }
    }
}

fn validate_entry(entry: &str) -> Result<(), String> {
    let is_script = [".ts", ".js", ".mjs"]
        .iter()
        .any(|extension| entry.ends_with(extension));
    if !is_script {
        return Err(format!("entry `{entry}` must be a .ts, .js, or .mjs file"));
    }
    if entry.starts_with('/')
        || entry.contains('\\')
        || entry.chars().any(char::is_control)
        || entry
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(format!(
            "entry `{entry}` must be a plain relative path inside the package"
        ));
    }
    Ok(())
}

/// The runner owns every `CAIRN_*` variable a service sees; a declaration that
/// could set one could impersonate another run or point the SDK elsewhere.
fn validate_env_key(key: &str) -> Result<(), String> {
    let bytes = key.as_bytes();
    let well_formed = !bytes.is_empty()
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_');
    if !well_formed {
        return Err(format!("env key `{key}` must be a shell variable name"));
    }
    if key.to_ascii_uppercase().starts_with("CAIRN_") {
        return Err(format!(
            "env key `{key}` is refused: CAIRN_* variables are set by the runner"
        ));
    }
    Ok(())
}

fn resolve_limits(limits: &ServiceLimitsManifest) -> Result<ServiceLimits, String> {
    let memory_mb = limits.memory_mb.unwrap_or(DEFAULT_MEMORY_MB);
    if !(16..=MAX_MEMORY_MB).contains(&memory_mb) {
        return Err(format!("memoryMb must be between 16 and {MAX_MEMORY_MB}"));
    }
    let concurrency = limits.concurrency.unwrap_or(DEFAULT_CONCURRENCY);
    if !(1..=MAX_CONCURRENCY).contains(&concurrency) {
        return Err(format!(
            "concurrency must be between 1 and {MAX_CONCURRENCY}"
        ));
    }
    let call_timeout_seconds = limits
        .call_timeout_seconds
        .unwrap_or(DEFAULT_CALL_TIMEOUT_SECONDS);
    if !(1..=MAX_CALL_TIMEOUT_SECONDS).contains(&call_timeout_seconds) {
        return Err(format!(
            "callTimeoutSeconds must be between 1 and {MAX_CALL_TIMEOUT_SECONDS}"
        ));
    }
    Ok(ServiceLimits {
        memory_mb,
        concurrency,
        call_timeout_seconds,
    })
}

/// sha256 over the package's (path, bytes) pairs in path order.
///
/// Each field is length-prefixed so no two different packages can frame to the
/// same byte stream, and sorting first makes the hash a property of the
/// package's contents rather than of the order a tree walk produced them in.
pub(crate) fn source_hash(files: &[(String, Vec<u8>)]) -> String {
    let mut sorted: Vec<&(String, Vec<u8>)> = files.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut hasher = Sha256::new();
    for (path, bytes) in sorted {
        hasher.update((path.len() as u64).to_be_bytes());
        hasher.update(path.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    format!("{:x}", hasher.finalize())
}

/// Assemble a package from its files. The manifest must be among them and the
/// entry it names must exist.
pub(crate) fn package_from_files(
    id: &str,
    files: Vec<(String, Vec<u8>)>,
) -> Result<ServicePackage, String> {
    package_from_files_in(&PackageOrigin::repository(id), id, files)
}

/// [`package_from_files`], naming `origin` in its errors.
pub(crate) fn package_from_files_in(
    origin: &PackageOrigin,
    id: &str,
    mut files: Vec<(String, Vec<u8>)>,
) -> Result<ServicePackage, String> {
    let package = &origin.package;
    if files.len() > MAX_PACKAGE_FILES {
        return Err(format!(
            "{package} has {} files; a service package may have at most {MAX_PACKAGE_FILES}",
            files.len()
        ));
    }
    let total: usize = files.iter().map(|(_, bytes)| bytes.len()).sum();
    if total > MAX_PACKAGE_BYTES {
        return Err(format!(
            "{package} is {total} bytes; a service package may be at most {MAX_PACKAGE_BYTES}"
        ));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    let manifest = files
        .iter()
        .find(|(path, _)| path == MANIFEST_FILE)
        .ok_or_else(|| format!("{package} has no {MANIFEST_FILE}"))?;
    let text = std::str::from_utf8(&manifest.1)
        .map_err(|_| format!("{} is not UTF-8", origin.manifest))?;
    let declaration = parse_service_manifest_in(origin, id, text)?;
    if !files.iter().any(|(path, _)| *path == declaration.entry) {
        let mut paths: Vec<&str> = files
            .iter()
            .map(|(path, _)| path.as_str())
            .filter(|path| *path != MANIFEST_FILE)
            .collect();
        paths.sort_unstable();
        return Err(format!(
            "{package}: entry `{}` is not in the package ({})",
            declaration.entry,
            paths.join(", ")
        ));
    }
    let source_hash = source_hash(&files);
    Ok(ServicePackage {
        declaration,
        files,
        source_hash,
    })
}

/// Packages as they stand at the tip of a project's default branch.
///
/// Merging to the base branch is what puts code here, so a running service
/// always runs code that has been through review and the operator's merge
/// policy, and "restart when the source changes" is a hash comparison against
/// this source after each base advance.
pub(crate) struct BaseTipSource {
    repo_path: PathBuf,
    commit: String,
}

impl BaseTipSource {
    /// A source at `commit` in the repository at `repo_path`.
    pub(crate) fn at_commit(repo_path: PathBuf, commit: String) -> Result<Self, String> {
        let source = Self { repo_path, commit };
        source.store()?;
        Ok(source)
    }

    /// Resolve the project's default-branch tip and open a source there.
    pub(crate) async fn resolve(orch: &Orchestrator, project_id: &str) -> Result<Self, String> {
        let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
            .await
            .map_err(|error| error.to_string())?;
        let repo_path = crate::orchestrator::base_advance::load_project_repo_path(&db, project_id)
            .await?
            .filter(|path| !path.is_empty())
            .ok_or_else(|| format!("project {project_id} has no repository"))?;
        let branch =
            crate::orchestrator::base_advance::load_project_default_branch(&db, project_id)
                .await?
                .filter(|branch| !branch.is_empty())
                .ok_or_else(|| format!("project {project_id} has no default branch"))?;
        let repo_root = PathBuf::from(&repo_path);
        // Checked here because git, spawned in a missing directory, reports
        // only "No such file or directory", naming neither the path nor why.
        if !tokio::fs::try_exists(&repo_root).await.unwrap_or(false) {
            return Err(format!(
                "project {project_id}'s repository {repo_path} does not exist"
            ));
        }
        let commit = crate::projects::codemap::head_commit(orch, &repo_root, &branch).await?;
        tokio::task::spawn_blocking(move || Self::at_commit(repo_root, commit))
            .await
            .map_err(|error| format!("service source open task failed: {error}"))?
    }

    /// The object store is opened per read: it is not `Sync`, and a source is
    /// shared with whichever blocking task reads it.
    fn store(&self) -> Result<ObjectStore, String> {
        ObjectStore::new(&self.repo_path, None)
            .map_err(|error| format!("open repository object store: {error}"))
    }

    fn limits() -> cairn_codec::objects::ObjectReadLimits {
        crate::mcp::handlers::read::object_read::serve_limits()
    }
}

impl ServiceSource for BaseTipSource {
    fn revision(&self) -> &str {
        &self.commit
    }

    fn list(&self) -> Result<Vec<ServiceListing>, String> {
        let store = self.store()?;
        let limits = Self::limits();
        let entries = match store.entries_at_commit(&self.commit, SERVICES_DIR, &limits) {
            Ok(entries) => entries,
            Err(ObjectReadError::PathNotFound(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error.to_string()),
        };
        let mut listings = Vec::new();
        for entry in entries {
            if entry.kind != TreeEntryKind::Tree {
                continue;
            }
            let manifest_path = format!("{SERVICES_DIR}/{}/{MANIFEST_FILE}", entry.name);
            let text = match store.read_path_at_commit(&self.commit, &manifest_path, &limits) {
                Ok(bytes) => bytes,
                // A directory without a manifest is not a service.
                Err(ObjectReadError::PathNotFound(_)) => continue,
                Err(error) => return Err(error.to_string()),
            };
            let declaration = match std::str::from_utf8(&text) {
                Ok(text) => parse_service_manifest(&entry.name, text),
                Err(_) => Err(format!("{manifest_path} is not UTF-8")),
            };
            listings.push(ServiceListing {
                id: entry.name,
                declaration,
            });
        }
        listings.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(listings)
    }

    fn package(&self, id: &str) -> Result<Option<ServicePackage>, String> {
        if !is_valid_service_id(id) {
            return Ok(None);
        }
        let prefix = format!("{SERVICES_DIR}/{id}");
        let store = self.store()?;
        let limits = Self::limits();
        let items = match store.walk_commit(&self.commit, &prefix, &limits) {
            Ok(items) => items,
            Err(ObjectReadError::PathNotFound(_)) => return Ok(None),
            Err(error) => return Err(error.to_string()),
        };
        let mut files = Vec::new();
        for item in items {
            if item.kind != TreeEntryKind::Blob {
                continue;
            }
            let relative = item
                .path
                .strip_prefix(&prefix)
                .map(|rest| rest.trim_start_matches('/'))
                .unwrap_or(&item.path)
                .to_string();
            let bytes = store
                .blob(&item.oid, &limits)
                .map_err(|error| error.to_string())?;
            files.push((relative, bytes));
        }
        if !files.iter().any(|(path, _)| path == MANIFEST_FILE) {
            return Ok(None);
        }
        package_from_files(id, files).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ECHO: &str = "name: Echo\ndescription: Says it back.\n";

    fn files(entries: &[(&str, &str)]) -> Vec<(String, Vec<u8>)> {
        entries
            .iter()
            .map(|(path, body)| (path.to_string(), body.as_bytes().to_vec()))
            .collect()
    }

    /// The reference packages this repository ships must stay valid.
    #[test]
    fn the_in_repo_reference_services_parse() {
        for (id, manifest, main) in [
            (
                "echo",
                include_str!("../../../../../.cairn/services/echo/service.yaml"),
                include_str!("../../../../../.cairn/services/echo/main.ts"),
            ),
            (
                "judge",
                include_str!("../../../../../.cairn/services/judge/service.yaml"),
                include_str!("../../../../../.cairn/services/judge/main.ts"),
            ),
        ] {
            let package =
                package_from_files(id, files(&[("service.yaml", manifest), ("main.ts", main)]))
                    .unwrap_or_else(|error| panic!("{id}: {error}"));
            assert_eq!(package.declaration.start, StartPolicy::OnDemand, "{id}");
            assert!(main.contains("@cairn/sdk/service"), "{id}");
        }
    }

    #[test]
    fn a_minimal_manifest_resolves_every_default() {
        let declaration = parse_service_manifest("echo", ECHO).unwrap();
        assert_eq!(declaration.name, "Echo");
        assert_eq!(declaration.entry, DEFAULT_ENTRY);
        assert_eq!(declaration.start, StartPolicy::OnDemand);
        assert_eq!(declaration.executor, None);
        assert_eq!(declaration.limits, ServiceLimits::default());
        assert_eq!(declaration.limits.queue_depth(), 16);
    }

    #[test]
    fn a_full_manifest_parses() {
        let declaration = parse_service_manifest(
            "judge",
            "name: Judge\nentry: src/main.ts\nstart: launch\nexecutor:\n  os: linux\n  requiredToolchains: [uv]\nenv:\n  JUDGE_TIMEOUT_SECONDS: \"20\"\nlimits:\n  memoryMb: 512\n  concurrency: 4\n  callTimeoutSeconds: 60\n",
        )
        .unwrap();
        assert_eq!(declaration.entry, "src/main.ts");
        assert_eq!(declaration.start, StartPolicy::Launch);
        let executor = declaration.executor.unwrap();
        assert_eq!(executor.os.as_deref(), Some("linux"));
        assert_eq!(executor.required_toolchains, vec!["uv".to_string()]);
        assert_eq!(declaration.env["JUDGE_TIMEOUT_SECONDS"], "20");
        assert_eq!(
            declaration.limits,
            ServiceLimits {
                memory_mb: 512,
                concurrency: 4,
                call_timeout_seconds: 60
            }
        );
    }

    #[test]
    fn unknown_keys_are_refused_at_every_level() {
        assert!(parse_service_manifest("echo", "name: Echo\nport: 8080\n").is_err());
        assert!(
            parse_service_manifest("echo", "name: Echo\nexecutor:\n  containment: none\n").is_err()
        );
        assert!(parse_service_manifest("echo", "name: Echo\nlimits:\n  cpu: 2\n").is_err());
    }

    #[test]
    fn the_runner_owns_cairn_env() {
        let error = parse_service_manifest("echo", "name: Echo\nenv:\n  CAIRN_RUN_ID: forged\n")
            .unwrap_err();
        assert!(error.contains("CAIRN_*"), "{error}");
        assert!(parse_service_manifest("echo", "name: Echo\nenv:\n  cairn_home: x\n").is_err());
        assert!(parse_service_manifest("echo", "name: Echo\nenv:\n  NOT-A-NAME: x\n").is_err());
    }

    #[test]
    fn ids_are_lowercase_names() {
        for good in ["echo", "judge-2", "a"] {
            assert!(is_valid_service_id(good), "{good}");
        }
        for bad in [
            "",
            "Echo",
            "2echo",
            "-echo",
            "echo_1",
            "echo.x",
            &"a".repeat(64),
        ] {
            assert!(!is_valid_service_id(bad), "{bad}");
        }
        assert!(parse_service_manifest("Echo", ECHO).is_err());
    }

    #[test]
    fn the_entry_stays_inside_the_package() {
        assert!(
            parse_service_manifest("echo", "name: Echo\nentry: \"a\\bc.ts\"\n").is_err(),
            "a control character in the entry is refused"
        );
        for bad in [
            "../main.ts",
            "/main.ts",
            "src/../../x.ts",
            "./main.ts",
            "main.py",
            "a\\b.ts",
        ] {
            let manifest = format!("name: Echo\nentry: '{bad}'\n");
            assert!(parse_service_manifest("echo", &manifest).is_err(), "{bad}");
        }
    }

    #[test]
    fn limits_are_bounded() {
        assert!(
            parse_service_manifest("echo", "name: Echo\nlimits:\n  callTimeoutSeconds: 301\n")
                .is_err()
        );
        assert!(parse_service_manifest("echo", "name: Echo\nlimits:\n  concurrency: 0\n").is_err());
    }

    #[test]
    fn a_schedule_parses_within_the_interval_grammar() {
        let declaration = parse_service_manifest(
            "board",
            "name: Board\nschedule:\n  - every: 15m\n    method: refresh\n  - every: 1d\n    method: prune\n    args: {keep: 30}\n",
        )
        .unwrap();
        assert_eq!(declaration.schedule.len(), 2);
        assert_eq!(declaration.schedule[0].every_ms, 15 * 60_000);
        assert_eq!(declaration.schedule[0].args, serde_json::Value::Null);
        assert_eq!(declaration.schedule[1].args["keep"], 30);
        assert_ne!(declaration.schedule[0].key(), declaration.schedule[1].key());
        assert_eq!(declaration.idle_stop_after, Some(DEFAULT_IDLE_STOP_AFTER));

        for (bad, says) in [
            (
                "schedule:\n  - every: 1m\n    method: tick\n",
                "from 5m to 30d",
            ),
            (
                "schedule:\n  - every: 31d\n    method: tick\n",
                "from 5m to 30d",
            ),
            (
                "schedule:\n  - every: soon\n    method: tick\n",
                "from 5m to 30d",
            ),
            ("schedule:\n  - every: 5m\n    method: ''\n", "method"),
            (
                "schedule:\n  - every: 5m\n    method: tick\n    cron: x\n",
                "cron",
            ),
            (
                "schedule:\n  - every: 5m\n    method: tick\n  - every: 5m\n    method: tick\n",
                "repeats schedule[0]",
            ),
        ] {
            let error =
                parse_service_manifest("board", &format!("name: Board\n{bad}")).unwrap_err();
            assert!(error.contains(says), "{bad}: {error}");
        }
    }

    #[test]
    fn the_idle_stop_is_on_demand_only_and_bounded() {
        let declaration =
            parse_service_manifest("board", "name: Board\nidleStopAfter: 10m\n").unwrap();
        assert_eq!(declaration.idle_stop_after, Some(Duration::from_secs(600)));
        let launch = parse_service_manifest("board", "name: Board\nstart: launch\n").unwrap();
        assert_eq!(
            launch.idle_stop_after, None,
            "a launch service never idle-stops"
        );
        assert!(
            parse_service_manifest("board", "name: Board\nstart: launch\nidleStopAfter: 5m\n")
                .unwrap_err()
                .contains("on_demand")
        );
        for bad in ["30s", "2d", "five"] {
            assert!(
                parse_service_manifest("board", &format!("name: Board\nidleStopAfter: {bad}\n"))
                    .is_err(),
                "{bad}"
            );
        }
    }

    /// A thread's scheduled service is two keys: one file, one schedule entry.
    #[test]
    fn a_thread_service_schedules_with_two_keys() {
        let package = crate::project_services::thread_store::package(
            "releases",
            "cairn://p/cairn/thread-ux/services/releases",
            &serde_json::json!({
                "files": {"main.ts": "serve({methods: {refresh: async () => null}})"},
                "manifest": {"schedule": [{"every": "15m", "method": "refresh"}]}
            }),
        )
        .unwrap();
        assert_eq!(package.declaration.name, "releases");
        assert_eq!(package.declaration.start, StartPolicy::OnDemand);
        assert_eq!(package.declaration.schedule[0].method, "refresh");
        assert_eq!(package.declaration.schedule[0].every_ms, 15 * 60_000);
    }

    #[test]
    fn a_package_needs_its_entry() {
        let error = package_from_files("echo", files(&[("service.yaml", ECHO)])).unwrap_err();
        assert!(error.contains("main.ts"), "{error}");
        let package = package_from_files(
            "echo",
            files(&[("service.yaml", ECHO), ("main.ts", "serve({})")]),
        )
        .unwrap();
        assert_eq!(package.files[0].0, "main.ts");
    }

    #[test]
    fn the_source_hash_is_order_free_and_byte_exact() {
        let a = files(&[("service.yaml", ECHO), ("main.ts", "serve({})")]);
        let mut reordered = a.clone();
        reordered.reverse();
        assert_eq!(source_hash(&a), source_hash(&reordered));

        let changed = files(&[("service.yaml", ECHO), ("main.ts", "serve({ })")]);
        assert_ne!(source_hash(&a), source_hash(&changed));

        // Moving a byte from a path into the content must not collide.
        let framed_a = files(&[("ab", "c")]);
        let framed_b = files(&[("a", "bc")]);
        assert_ne!(source_hash(&framed_a), source_hash(&framed_b));
    }

    fn git(repo: &std::path::Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(repo)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .unwrap();
        assert!(output.status.success(), "git {args:?}: {output:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn write(repo: &std::path::Path, path: &str, body: &str) {
        let path = repo.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    #[test]
    fn the_base_tip_source_reads_committed_packages_only() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        git(repo, &["init", "-b", "main"]);
        write(repo, ".cairn/services/echo/service.yaml", ECHO);
        write(repo, ".cairn/services/echo/main.ts", "serve({})");
        write(
            repo,
            ".cairn/services/broken/service.yaml",
            "name: Broken\nport: 1\n",
        );
        write(repo, ".cairn/services/broken/main.ts", "");
        write(repo, ".cairn/services/notes/README.md", "not a service");
        git(repo, &["add", "."]);
        git(repo, &["commit", "-m", "services"]);
        let first = git(repo, &["rev-parse", "HEAD"]);

        // Uncommitted edits are invisible to the source.
        write(
            repo,
            ".cairn/services/echo/main.ts",
            "serve({ dirty: true })",
        );

        let source = BaseTipSource::at_commit(repo.to_path_buf(), first.clone()).unwrap();
        assert_eq!(source.revision(), first);
        let listed = source.list().unwrap();
        let ids: Vec<&str> = listed.iter().map(|l| l.id.as_str()).collect();
        assert_eq!(ids, vec!["broken", "echo"]);
        assert!(listed[0].declaration.is_err());
        assert!(listed[1].declaration.is_ok());

        let package = source.package("echo").unwrap().unwrap();
        assert_eq!(
            package.files,
            files(&[("main.ts", "serve({})"), ("service.yaml", ECHO)])
        );
        assert!(source.package("notes").unwrap().is_none());
        assert!(source.package("absent").unwrap().is_none());

        git(repo, &["add", "."]);
        git(repo, &["commit", "-m", "change echo"]);
        let second = git(repo, &["rev-parse", "HEAD"]);
        let advanced = BaseTipSource::at_commit(repo.to_path_buf(), second).unwrap();
        assert_ne!(
            advanced.package("echo").unwrap().unwrap().source_hash,
            package.source_hash
        );
    }

    #[test]
    fn a_repository_without_services_lists_none() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        git(repo, &["init", "-b", "main"]);
        git(repo, &["commit", "--allow-empty", "-m", "initial"]);
        let head = git(repo, &["rev-parse", "HEAD"]);
        let source = BaseTipSource::at_commit(repo.to_path_buf(), head).unwrap();
        assert!(source.list().unwrap().is_empty());
    }
}
