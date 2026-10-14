//! Measured source distance and conservative workspace dependency attribution.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheDistance {
    pub files: u32,
    pub crates: u32,
    pub crates_transitive: u32,
    pub js_workspaces: u32,
}

impl CacheDistance {
    /// Lexicographic (reverse dependency count, file count), reserving MAX for unknown.
    pub fn rank(&self) -> u64 {
        (((self.crates_transitive as u64) << 32) | self.files as u64).min(u64::MAX - 1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MeasuredDistance {
    pub target_commit: String,
    pub distance: CacheDistance,
    pub measured_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DistanceUnknown(pub String);

impl DistanceUnknown {
    pub fn rank(&self) -> u64 {
        u64::MAX
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrateFanout {
    pub direct: Vec<String>,
    /// Includes the directly touched members.
    pub transitive: Vec<String>,
}

/// Minimal projection of `cargo metadata` output. Unknown fields are ignored.
#[derive(Debug, Deserialize, Serialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CargoPackage {
    name: String,
    manifest_path: String,
    #[serde(default)]
    dependencies: Vec<CargoDependency>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CargoDependency {
    name: String,
}

/// Pure core of the crate-graph resolver: parse metadata JSON, map changed files
/// to members, and expand to the reverse-dependency closure. Split from the
/// `cargo` invocation so it can be unit-tested with a fixture without shelling
/// out. Returns `None` on the same uncertainty conditions as
/// the test-target selector.
pub fn affected_crates(
    metadata_json: &str,
    changed_files: &[String],
    repo_root: &Path,
) -> Option<CrateFanout> {
    let metadata: CargoMetadata = serde_json::from_str(metadata_json).ok()?;

    // Member name -> package directory (the manifest's parent).
    let member_dirs: Vec<(String, PathBuf)> = metadata
        .packages
        .iter()
        .filter_map(|pkg| {
            Path::new(&pkg.manifest_path)
                .parent()
                .map(|dir| (pkg.name.clone(), dir.to_path_buf()))
        })
        .collect();
    let member_names: BTreeSet<&str> = metadata.packages.iter().map(|p| p.name.as_str()).collect();

    // Map each changed file to its owning member by longest package-dir prefix
    // (deepest wins, so a file in a nested member is attributed to that member,
    // not an ancestor workspace member). Any unmappable file is uncertain.
    let mut seeds: BTreeSet<String> = BTreeSet::new();
    for file in changed_files {
        let file_abs = repo_root.join(file);
        let owner = member_dirs
            .iter()
            .filter(|(_, dir)| file_abs.starts_with(dir))
            .max_by_key(|(_, dir)| dir.components().count());
        match owner {
            Some((name, _)) => {
                seeds.insert(name.clone());
            }
            None => return None,
        }
    }

    // Reverse edges: member -> members that depend on it (workspace-internal
    // only). Built from each package's declared dependencies filtered to members.
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for pkg in &metadata.packages {
        for dep in &pkg.dependencies {
            if member_names.contains(dep.name.as_str()) {
                dependents
                    .entry(dep.name.as_str())
                    .or_default()
                    .push(pkg.name.as_str());
            }
        }
    }

    // Transitive reverse-dependency closure over the seeds.
    let mut affected: BTreeSet<String> = BTreeSet::new();
    let direct: Vec<String> = seeds.into_iter().collect();
    let mut stack = direct.clone();
    while let Some(crate_name) = stack.pop() {
        if !affected.insert(crate_name.clone()) {
            continue;
        }
        if let Some(rdeps) = dependents.get(crate_name.as_str()) {
            for rdep in rdeps {
                if !affected.contains(*rdep) {
                    stack.push((*rdep).to_string());
                }
            }
        }
    }

    Some(CrateFanout {
        direct,
        transitive: affected.into_iter().collect(),
    })
}

/// Measure against the named target's committed workspace manifests, including in bare repositories.
/// Callers must not hold fleet or queue locks across this subprocess boundary.
pub fn cache_distance(
    repository: &Path,
    from: &str,
    to: &str,
) -> Result<CacheDistance, DistanceUnknown> {
    measure(repository, from, to, &mut command_output)
}

fn command_output(
    program: &str,
    directory: &Path,
    args: &[String],
) -> Result<String, DistanceUnknown> {
    let mut command =
        crate::toolchain_path::command_on_path(program, crate::toolchain_path::user_path());
    command.current_dir(directory).args(args);
    if program == "cargo" && args.first().is_some_and(|arg| arg == "metadata") {
        for ancestor in directory.ancestors() {
            if ancestor.join(".cargo/config").exists()
                || ancestor.join(".cargo/config.toml").exists()
            {
                return Err(DistanceUnknown(
                    "cargo configuration in metadata directory ancestry".into(),
                ));
            }
        }
    }
    let output = command.output().map_err(|error| {
        DistanceUnknown(format!("{program} in {}: {error}", directory.display()))
    })?;
    if !output.status.success() {
        return Err(DistanceUnknown(format!(
            "{program} in {} ({}): {}",
            directory.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| DistanceUnknown(format!("{program} output is not UTF-8: {error}")))
}

// A process sees unbounded commits and repositories over its lifetime. Retain
// only a small recent working set globally, not a growing per-repository budget.
const CACHE_ENTRY_LIMIT: usize = 16;

struct BoundedCache<K, V> {
    entries: VecDeque<(K, V)>,
}

impl<K, V> Default for BoundedCache<K, V> {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }
}

impl<K: Eq, V> BoundedCache<K, V> {
    fn get(&mut self, key: &K) -> Option<&V> {
        let position = self.entries.iter().position(|(stored, _)| stored == key)?;
        let entry = self.entries.remove(position)?;
        self.entries.push_back(entry);
        self.entries.back().map(|(_, value)| value)
    }

    fn insert(&mut self, key: K, value: V) {
        if let Some(position) = self.entries.iter().position(|(stored, _)| stored == &key) {
            self.entries.remove(position);
        } else if self.entries.len() == CACHE_ENTRY_LIMIT {
            self.entries.pop_front();
        }
        self.entries.push_back((key, value));
    }
}

// Source content does not change the graph, but manifests and target discovery
// (the tracked path layout) do. Both are needed alongside the committed lockfile.
type MetadataKey = (PathBuf, [u8; 32]);
static METADATA: OnceLock<Mutex<BoundedCache<MetadataKey, String>>> = OnceLock::new();
type TreeCache = Mutex<BoundedCache<(PathBuf, String), CommitTree>>;
static TREES: OnceLock<TreeCache> = OnceLock::new();

#[derive(Clone)]
struct CommitTree {
    blobs: std::collections::BTreeMap<String, String>,
    nonregular: BTreeSet<String>,
}

impl CommitTree {
    fn read(
        repository: &Path,
        target: &str,
        run: &mut impl FnMut(&str, &Path, &[String]) -> Result<String, DistanceUnknown>,
    ) -> Result<Self, DistanceUnknown> {
        let target = run(
            "git",
            repository,
            &[
                "rev-parse".into(),
                "--verify".into(),
                "--end-of-options".into(),
                format!("{target}^{{commit}}"),
            ],
        )?
        .trim()
        .to_owned();
        let key = (repository.to_path_buf(), target.clone());
        let mut cache = TREES
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| DistanceUnknown("commit tree cache poisoned".into()))?;
        if let Some(tree) = cache.get(&key) {
            return Ok(tree.clone());
        }
        let listing = run(
            "git",
            repository,
            &[
                "ls-tree".into(),
                "-r".into(),
                "-z".into(),
                "--full-tree".into(),
                target,
            ],
        )?;
        let tree = Self::parse(&listing)?;
        cache.insert(key, tree.clone());
        Ok(tree)
    }

    fn parse(listing: &str) -> Result<Self, DistanceUnknown> {
        let mut blobs = std::collections::BTreeMap::new();
        let mut nonregular = BTreeSet::new();
        for entry in listing.split('\0').filter(|entry| !entry.is_empty()) {
            let (header, path) = entry
                .split_once('\t')
                .ok_or_else(|| DistanceUnknown("invalid git tree entry".into()))?;
            let fields: Vec<_> = header.split_whitespace().collect();
            if fields.len() != 3 {
                return Err(DistanceUnknown("invalid git tree header".into()));
            }
            // Apply portable rules even on Unix: this tree may later be measured
            // on Windows, where backslashes and drive prefixes are structural.
            if path.split('/').any(|part| {
                part.is_empty()
                    || part == "."
                    || part == ".."
                    || part.eq_ignore_ascii_case(".git")
                    || part.contains(['\\', ':'])
            }) || Path::new(path)
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
                || !fields[2].bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(DistanceUnknown(format!("unsafe committed path: {path}")));
            }
            if matches!(fields[0], "100644" | "100755") && fields[1] == "blob" {
                blobs.insert(path.to_owned(), fields[2].to_owned());
            } else {
                nonregular.insert(path.to_owned());
            }
        }
        Ok(Self { blobs, nonregular })
    }

    fn contents(
        &self,
        repository: &Path,
        path: &str,
        run: &mut impl FnMut(&str, &Path, &[String]) -> Result<String, DistanceUnknown>,
    ) -> Result<String, DistanceUnknown> {
        let oid = self
            .blobs
            .get(path)
            .ok_or_else(|| DistanceUnknown(format!("missing committed metadata: {path}")))?;
        run(
            "git",
            repository,
            &["cat-file".into(), "blob".into(), oid.clone()],
        )
    }
}

/// This directory is never registered as a worktree and contains no executable
/// source, repository config, hooks or toolchain overrides. Cargo only describes
/// its manifests; no build is run. Cleanup is limited to this uniquely owned tree.
struct MetadataDirectory(PathBuf);

impl MetadataDirectory {
    fn new() -> Result<Self, DistanceUnknown> {
        let path = std::env::temp_dir().join(format!("cairn-distance-{}", uuid::Uuid::new_v4()));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&path)
            .map_err(|error| DistanceUnknown(format!("metadata directory: {error}")))?;
        Ok(Self(path.canonicalize().map_err(|error| {
            DistanceUnknown(format!("metadata directory: {error}"))
        })?))
    }
}

impl Drop for MetadataDirectory {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            tracing::warn!(path = %self.0.display(), %error, "metadata directory cleanup failed");
        }
    }
}

/// Cargo is the only manifest parser. Its trusted user configuration/cache may
/// resolve dependencies, so this is not an OS sandbox or a promise of no host
/// reads. No outside package, dependency, or target path may inform our graph.
fn validate_metadata_paths(
    output: &str,
    root: &Path,
    tree: &CommitTree,
) -> Result<(), DistanceUnknown> {
    let metadata: serde_json::Value = serde_json::from_str(output)
        .map_err(|error| DistanceUnknown(format!("cargo metadata: {error}")))?;
    let validate = |value: &serde_json::Value, file: bool| -> Result<(), DistanceUnknown> {
        let path = value
            .as_str()
            .ok_or_else(|| DistanceUnknown("invalid metadata path".into()))?;
        let path = Path::new(path);
        let mut normalized = PathBuf::new();
        for component in path.components() {
            match component {
                std::path::Component::ParentDir => {
                    normalized.pop();
                }
                std::path::Component::CurDir => {}
                component => normalized.push(component.as_os_str()),
            }
        }
        let relative = normalized
            .strip_prefix(root)
            .map_err(|_| DistanceUnknown("Cargo path escapes committed metadata tree".into()))?;
        let relative = relative
            .to_str()
            .ok_or_else(|| DistanceUnknown("non UTF-8 Cargo path".into()))?
            .replace('\\', "/");
        let exists = if file {
            tree.blobs.contains_key(&relative)
        } else {
            relative.is_empty()
                || tree
                    .blobs
                    .keys()
                    .any(|path| path.starts_with(&format!("{relative}/")))
        };
        if !exists {
            return Err(DistanceUnknown(
                "Cargo path is not regular committed input".into(),
            ));
        }
        Ok(())
    };
    if let Some(workspace) = metadata.get("workspace_root") {
        validate(workspace, false)?;
    }
    for package in metadata
        .get("packages")
        .and_then(|value| value.as_array())
        .ok_or_else(|| DistanceUnknown("missing cargo packages".into()))?
    {
        validate(
            package
                .get("manifest_path")
                .ok_or_else(|| DistanceUnknown("missing package manifest".into()))?,
            true,
        )?;
        for dependency in package
            .get("dependencies")
            .and_then(|value| value.as_array())
            .into_iter()
            .flatten()
        {
            if let Some(path) = dependency.get("path").filter(|value| !value.is_null()) {
                validate(path, false)?;
            }
        }
        for target in package
            .get("targets")
            .and_then(|value| value.as_array())
            .into_iter()
            .flatten()
        {
            validate(
                target
                    .get("src_path")
                    .ok_or_else(|| DistanceUnknown("missing Cargo target path".into()))?,
                true,
            )?;
        }
    }
    Ok(())
}

fn metadata_cached(
    repository: &Path,
    rust_root: &str,
    tree: &CommitTree,
    run: &mut impl FnMut(&str, &Path, &[String]) -> Result<String, DistanceUnknown>,
) -> Result<String, DistanceUnknown> {
    if tree.nonregular.iter().any(|path| {
        path.starts_with(rust_root)
            && Path::new(path)
                .file_name()
                .is_some_and(|name| name == "Cargo.toml" || name == "Cargo.lock")
            || Path::new(path).extension().is_some_and(|ext| ext == "rs")
    }) {
        return Err(DistanceUnknown("nonregular committed Cargo input".into()));
    }
    let lock_path = format!("{rust_root}Cargo.lock");
    let lockfile = tree.contents(repository, &lock_path, run)?;
    let mut identity = Sha256::new();
    identity.update(lockfile.as_bytes());
    for (path, oid) in &tree.blobs {
        identity.update([0]);
        identity.update(path.as_bytes());
        if Path::new(path)
            .file_name()
            .is_some_and(|name| name == "Cargo.toml")
        {
            identity.update([0]);
            identity.update(oid.as_bytes());
        }
    }
    let key = (repository.to_path_buf(), identity.finalize().into());
    let mut cache = METADATA
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| DistanceUnknown("cargo metadata cache poisoned".into()))?;
    if let Some(metadata) = cache.get(&key) {
        return Ok(metadata.clone());
    }
    let directory = MetadataDirectory::new()?;
    for ancestor in directory.0.parent().into_iter().flat_map(Path::ancestors) {
        if ancestor.join("Cargo.toml").exists() {
            return Err(DistanceUnknown(
                "Cargo workspace in metadata directory ancestry".into(),
            ));
        }
    }
    for path in tree.blobs.keys() {
        let name = Path::new(path).file_name();
        if !name.is_some_and(|name| name == "Cargo.toml" || name == "Cargo.lock")
            && !Path::new(path).extension().is_some_and(|ext| ext == "rs")
        {
            continue;
        }
        if path.split('/').any(|part| part == ".cargo")
            || Path::new(path)
                .file_name()
                .is_some_and(|name| name == "rust-toolchain" || name == "rust-toolchain.toml")
        {
            continue;
        }
        let content = if path == &lock_path {
            lockfile.clone()
        } else if Path::new(path)
            .file_name()
            .is_some_and(|name| name == "Cargo.toml")
        {
            tree.contents(repository, path, run)?
        } else {
            String::new()
        };
        let destination = directory.0.join(path);
        std::fs::create_dir_all(destination.parent().expect("validated relative file"))
            .and_then(|()| std::fs::write(destination, content))
            .map_err(|error| DistanceUnknown(format!("materialize metadata: {error}")))?;
    }
    let output = run(
        "cargo",
        &directory.0.join(rust_root),
        &[
            "metadata".into(),
            "--format-version".into(),
            "1".into(),
            "--no-deps".into(),
            "--locked".into(),
            "--offline".into(),
        ],
    )?;
    validate_metadata_paths(&output, &directory.0, tree)?;
    let mut metadata: CargoMetadata = serde_json::from_str(&output)
        .map_err(|error| DistanceUnknown(format!("cargo metadata: {error}")))?;
    for package in &mut metadata.packages {
        let path = Path::new(&package.manifest_path)
            .canonicalize()
            .map_err(|error| DistanceUnknown(format!("metadata manifest: {error}")))?;
        let relative = path
            .strip_prefix(&directory.0)
            .map_err(|_| DistanceUnknown("Cargo member escapes committed metadata tree".into()))?;
        package.manifest_path = repository
            .join(relative)
            .to_str()
            .ok_or_else(|| DistanceUnknown("metadata path is not UTF-8".into()))?
            .to_owned();
    }
    let metadata = serde_json::to_string(&metadata)
        .map_err(|error| DistanceUnknown(format!("cargo metadata: {error}")))?;
    cache.insert(key, metadata.clone());
    Ok(metadata)
}

fn measure(
    repository: &Path,
    from: &str,
    to: &str,
    run: &mut impl FnMut(&str, &Path, &[String]) -> Result<String, DistanceUnknown>,
) -> Result<CacheDistance, DistanceUnknown> {
    if from == to {
        return Ok(CacheDistance::default());
    }
    let repository = repository
        .canonicalize()
        .map_err(|error| DistanceUnknown(format!("repository: {error}")))?;
    // NUL framing preserves whitespace/newlines; no rename folding means both
    // owners of a moved source file contribute to invalidation.
    let changed = run(
        "git",
        &repository,
        &[
            "diff".into(),
            "--name-only".into(),
            "-z".into(),
            "--no-renames".into(),
            format!("{from}..{to}"),
            "--".into(),
        ],
    )?;
    let files: Vec<String> = changed
        .split('\0')
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .collect();
    let tree = CommitTree::read(&repository, to, run)?;
    let rust_root = if tree.blobs.contains_key("src-tauri/Cargo.toml") {
        "src-tauri/"
    } else {
        ""
    };
    let rust_files: Vec<String> = files
        .iter()
        .filter(|file| {
            let path = Path::new(file);
            (!rust_root.is_empty() && file.starts_with(rust_root))
                || path.extension().is_some_and(|ext| ext == "rs")
                || path.file_name().is_some_and(|name| {
                    name == "Cargo.toml"
                        || name == "Cargo.lock"
                        || name == "rust-toolchain"
                        || name == "rust-toolchain.toml"
                })
                || path.components().any(|part| part.as_os_str() == ".cargo")
        })
        .cloned()
        .collect();
    let fanout = if rust_files.is_empty() {
        CrateFanout {
            direct: Vec::new(),
            transitive: Vec::new(),
        }
    } else {
        // Global configuration can affect every crate, even when a root package
        // would otherwise claim it by prefix. Manifest edits can alter the graph
        // without changing the lockfile, so cached metadata cannot establish them.
        if rust_files.iter().any(|file| {
            let path = Path::new(file);
            path.file_name().is_some_and(|name| {
                name == "Cargo.toml"
                    || name == "Cargo.lock"
                    || name == "rust-toolchain"
                    || name == "rust-toolchain.toml"
            }) || path.components().any(|part| part.as_os_str() == ".cargo")
        }) {
            return Err(DistanceUnknown(
                "Rust workspace configuration changed".into(),
            ));
        }
        let metadata = metadata_cached(&repository, rust_root, &tree, run)?;
        affected_crates(&metadata, &rust_files, &repository).ok_or_else(|| {
            DistanceUnknown("changed Rust paths cannot be attributed to workspace members".into())
        })?
    };
    Ok(CacheDistance {
        files: count(files.len())?,
        crates: count(fanout.direct.len())?,
        crates_transitive: count(fanout.transitive.len())?,
        js_workspaces: count(js_workspaces(&repository, &files, &tree, run)?.len())?,
    })
}

fn count(value: usize) -> Result<u32, DistanceUnknown> {
    u32::try_from(value).map_err(|_| DistanceUnknown("distance count exceeds u32".into()))
}

fn js_workspaces(
    repository: &Path,
    files: &[String],
    tree: &CommitTree,
    run: &mut impl FnMut(&str, &Path, &[String]) -> Result<String, DistanceUnknown>,
) -> Result<BTreeSet<PathBuf>, DistanceUnknown> {
    if tree.nonregular.iter().any(|path| {
        path == "package.json"
            || path.strip_suffix("/package.json").is_some_and(|directory| {
                files
                    .iter()
                    .any(|file| Path::new(file).starts_with(directory))
            })
    }) {
        return Err(DistanceUnknown(
            "nonregular committed JS workspace manifest".into(),
        ));
    }
    if !tree.blobs.contains_key("package.json") {
        if files.iter().any(|file| {
            Path::new(file).extension().is_some_and(|ext| {
                matches!(
                    ext.to_str(),
                    Some("js" | "jsx" | "ts" | "tsx" | "mjs" | "cjs")
                )
            })
        }) {
            return Err(DistanceUnknown(
                "missing committed metadata: package.json".into(),
            ));
        }
        return Ok(BTreeSet::new());
    }
    let manifest = tree.contents(repository, "package.json", run)?;
    let manifest: serde_json::Value = serde_json::from_str(&manifest)
        .map_err(|error| DistanceUnknown(format!("package.json: {error}")))?;
    let Some(workspaces) = manifest.get("workspaces") else {
        return Ok(BTreeSet::new());
    };
    let patterns = workspaces
        .as_array()
        .or_else(|| {
            workspaces
                .get("packages")
                .and_then(|value| value.as_array())
        })
        .ok_or_else(|| DistanceUnknown("package.json workspaces must be an array".into()))?;
    let mut directories = BTreeSet::new();
    for pattern in patterns {
        let pattern = pattern
            .as_str()
            .ok_or_else(|| DistanceUnknown("workspace pattern must be a string".into()))?;
        let components: Vec<_> = pattern
            .trim_end_matches('/')
            .split('/')
            .filter(|part| *part != ".")
            .collect();
        if components.is_empty()
            || components.iter().any(|part| {
                part.is_empty()
                    || *part == ".."
                    || (*part != "*" && part.contains(['*', '?', '[', ']', '{', '}', '!', '\\']))
            })
        {
            return Err(DistanceUnknown(format!(
                "unsupported workspace pattern: {pattern}"
            )));
        }
        for path in tree
            .blobs
            .keys()
            .filter_map(|path| path.strip_suffix("/package.json"))
        {
            let parts: Vec<_> = path.split('/').collect();
            if parts.len() == components.len()
                && parts
                    .iter()
                    .zip(&components)
                    .all(|(part, pattern)| *pattern == "*" || part == pattern)
            {
                directories.insert(PathBuf::from(path));
            }
        }
    }
    let mut touched = BTreeSet::new();
    for file in files {
        let path = Path::new(file);
        if let Some(owner) = directories
            .iter()
            .filter(|directory| path.starts_with(directory))
            .max_by_key(|directory| directory.components().count())
        {
            touched.insert(owner.clone());
        }
    }
    Ok(touched)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, path: &str, content: &str) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    fn git(root: &Path, args: &[&str]) -> String {
        command_output(
            "git",
            root,
            &args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>(),
        )
        .unwrap()
    }

    fn commit(root: &Path) -> String {
        git(root, &["add", "."]);
        git(
            root,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "fixture",
            ],
        );
        git(root, &["rev-parse", "HEAD"]).trim().to_owned()
    }

    fn fixture() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        git(root, &["init"]);
        write(
            root,
            "src-tauri/Cargo.toml",
            "[workspace]\nmembers = [\"leaf\", \"middle\", \"top\"]\nresolver = \"2\"\n",
        );
        for (name, dependency) in [
            ("leaf", None),
            ("middle", Some("leaf")),
            ("top", Some("middle")),
        ] {
            let mut manifest =
                format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n");
            if let Some(dep) = dependency {
                manifest.push_str(&format!(
                    "[dependencies]\n{dep} = {{ path = \"../{dep}\" }}\n"
                ));
            }
            write(root, &format!("src-tauri/{name}/Cargo.toml"), &manifest);
            write(
                root,
                &format!("src-tauri/{name}/src/lib.rs"),
                "pub fn value() {}\n",
            );
        }
        command_output(
            "cargo",
            &root.join("src-tauri"),
            &["generate-lockfile".into(), "--offline".into()],
        )
        .unwrap();
        write(
            root,
            "package.json",
            r#"{"workspaces":["packages/*","web"]}"#,
        );
        for name in ["packages/harness", "packages/sdk", "packages/ui", "web"] {
            write(root, &format!("{name}/package.json"), "{}");
        }
        temp
    }

    #[test]
    fn equal_refs_never_invoke_a_subprocess_or_read_the_repository() {
        assert_eq!(
            measure(
                Path::new("/does/not/exist"),
                "same",
                "same",
                &mut |_, _, _| { panic!("equal refs must not spawn a subprocess") }
            )
            .unwrap(),
            CacheDistance::default()
        );
    }

    #[test]
    fn rank_is_lexicographic_and_unknown_is_strictly_last() {
        let files = CacheDistance {
            files: u32::MAX,
            ..Default::default()
        };
        let crate_change = CacheDistance {
            crates_transitive: 1,
            ..Default::default()
        };
        assert!(files.rank() < crate_change.rank());
        let maximum = CacheDistance {
            files: u32::MAX,
            crates_transitive: u32::MAX,
            ..Default::default()
        };
        assert_eq!(maximum.rank(), u64::MAX - 1);
        assert!(maximum.rank() < DistanceUnknown("missing".into()).rank());
    }

    #[test]
    fn fixture_measures_rust_reverse_closure_js_and_missing_from() {
        let temp = fixture();
        let root = temp.path();
        let from = commit(root);
        write(root, "src-tauri/leaf/src/lib.rs", "pub fn changed() {}\n");
        write(root, "packages/sdk/src/index.ts", "export {};\n");
        write(root, "packages/sdk/src/second.ts", "export {};\n");
        write(root, "web/src/index.ts", "export {};\n");
        write(root, "website/not-a-workspace.ts", "export {};\n");
        let to = commit(root);
        assert_eq!(
            cache_distance(root, &from, &to).unwrap(),
            CacheDistance {
                files: 5,
                crates: 1,
                crates_transitive: 3,
                js_workspaces: 2,
            }
        );
        let missing = cache_distance(root, "not-a-real-commit", &to).unwrap_err();
        assert!(missing.0.contains("git"));
        assert_eq!(missing.rank(), u64::MAX);
    }

    #[test]
    fn js_only_changes_do_not_run_cargo_and_preserve_path_framing() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        git(root, &["init"]);
        write(
            root,
            "package.json",
            r#"{"workspaces":["packages/*","web"]}"#,
        );
        write(root, "packages/ui/package.json", "{}");
        write(root, "web/package.json", "{}");
        let from = commit(root);
        for file in [
            "packages/ui/new\nline.ts",
            "web/a.ts",
            "website/a.ts",
            "src/global.ts",
        ] {
            write(root, file, "export {};");
        }
        let to = commit(root);
        let measured = measure(root, &from, &to, &mut |program, directory, args| {
            assert_ne!(program, "cargo");
            command_output(program, directory, args)
        })
        .unwrap();
        assert_eq!(
            measured,
            CacheDistance {
                files: 4,
                js_workspaces: 2,
                ..Default::default()
            }
        );
    }

    #[test]
    fn rust_global_and_unmapped_paths_are_unknown() {
        let temp = fixture();
        let from = commit(temp.path());
        for path in [
            "src-tauri/Cargo.lock",
            "src-tauri/Cargo.toml",
            ".cargo/config.toml",
            "rust-toolchain.toml",
            "src-tauri/shared.rs",
        ] {
            let result = measure(
                temp.path(),
                "prior",
                &from,
                &mut |program, directory, args| {
                    if args.first().is_some_and(|arg| arg == "diff") {
                        Ok(format!("{path}\0"))
                    } else {
                        command_output(program, directory, args)
                    }
                },
            );
            assert!(result.is_err(), "{path} must not become false zero");
        }
    }

    #[test]
    fn metadata_cache_reuses_sources_but_invalidates_manifests_layout_and_lock() {
        let temp = fixture();
        let root = temp.path().canonicalize().unwrap();
        let first = commit(&root);
        let tree = CommitTree::read(&root, &first, &mut command_output).unwrap();
        let mut calls = 0;
        let mut run = |program: &str, directory: &Path, args: &[String]| {
            if program == "cargo" {
                calls += 1;
            }
            command_output(program, directory, args)
        };
        metadata_cached(&root, "src-tauri/", &tree, &mut run).unwrap();
        metadata_cached(&root, "src-tauri/", &tree, &mut run).unwrap();
        write(&root, "src-tauri/middle/src/lib.rs", "pub fn second() {}\n");
        let second = commit(&root);
        let tree = CommitTree::read(&root, &second, &mut command_output).unwrap();
        metadata_cached(&root, "src-tauri/", &tree, &mut run).unwrap();
        let lock_before = std::fs::read(root.join("src-tauri/Cargo.lock")).unwrap();
        let manifest = std::fs::read_to_string(root.join("src-tauri/top/Cargo.toml")).unwrap();
        write(
            &root,
            "src-tauri/top/Cargo.toml",
            manifest.split("[dependencies]").next().unwrap(),
        );
        let third = commit(&root);
        let tree = CommitTree::read(&root, &third, &mut command_output).unwrap();
        let metadata = metadata_cached(&root, "src-tauri/", &tree, &mut run).unwrap();
        assert_eq!(
            affected_crates(&metadata, &["src-tauri/middle/src/lib.rs".into()], &root)
                .unwrap()
                .transitive,
            vec!["middle"]
        );
        write(&root, "src-tauri/top/src/bin/extra.rs", "fn main() {}\n");
        let fourth = commit(&root);
        let tree = CommitTree::read(&root, &fourth, &mut command_output).unwrap();
        metadata_cached(&root, "src-tauri/", &tree, &mut run).unwrap();
        assert_eq!(
            std::fs::read(root.join("src-tauri/Cargo.lock")).unwrap(),
            lock_before
        );
        let mut lock = String::from_utf8(lock_before).unwrap();
        lock.push_str("\n# changed lock identity\n");
        write(&root, "src-tauri/Cargo.lock", &lock);
        let fifth = commit(&root);
        let tree = CommitTree::read(&root, &fifth, &mut command_output).unwrap();
        metadata_cached(&root, "src-tauri/", &tree, &mut run).unwrap();
        assert_eq!(calls, 4);
    }

    #[test]
    fn bare_and_dirty_colocated_measure_identical_committed_rust_and_js() {
        let temp = fixture();
        let root = temp.path();
        write(
            root,
            ".cargo/config.toml",
            "this is deliberately invalid cargo configuration",
        );
        write(root, "rust-toolchain.toml", "invalid toolchain file");
        write(
            root,
            "src-tauri/middle/build.rs",
            "compile_error!(\"must never build\");",
        );
        let from = commit(root);
        write(root, "src-tauri/middle/src/lib.rs", "pub fn changed() {}\n");
        write(root, "packages/sdk/index.ts", "export {};\n");
        let to = commit(root);
        let bare_parent = tempfile::tempdir().unwrap();
        let bare = bare_parent.path().join("repository.git");
        git(
            root,
            &[
                "clone",
                "--bare",
                "--no-hardlinks",
                root.to_str().unwrap(),
                bare.to_str().unwrap(),
            ],
        );
        // Named target is independent of both the checkout HEAD and dirty manifests.
        git(root, &["checkout", "--detach", &from]);
        write(root, "src-tauri/Cargo.toml", "not a manifest");
        write(root, "src-tauri/Cargo.lock", "not a lockfile");
        write(root, "package.json", "not json");
        let status = git(root, &["status", "--porcelain=v1"]);
        let expected = CacheDistance {
            files: 2,
            crates: 1,
            crates_transitive: 2,
            js_workspaces: 1,
        };
        assert_eq!(cache_distance(root, &from, &to).unwrap(), expected);
        assert_eq!(cache_distance(&bare, &from, &to).unwrap(), expected);
        assert_eq!(git(root, &["status", "--porcelain=v1"]), status);
        assert_eq!(git(root, &["rev-parse", "HEAD"]).trim(), from);
        assert_eq!(
            std::fs::read_to_string(root.join("package.json")).unwrap(),
            "not json"
        );
    }

    #[test]
    fn missing_committed_metadata_is_unknown_in_bare_repository() {
        for rust in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            git(temp.path(), &["init"]);
            write(temp.path(), "README", "fixture");
            let from = commit(temp.path());
            write(
                temp.path(),
                if rust {
                    "src/lib.rs"
                } else {
                    "packages/sdk/index.ts"
                },
                "changed",
            );
            let to = commit(temp.path());
            let bare = tempfile::tempdir().unwrap();
            git(
                temp.path(),
                &[
                    "clone",
                    "--bare",
                    temp.path().to_str().unwrap(),
                    bare.path().to_str().unwrap(),
                ],
            );
            let error = cache_distance(bare.path(), &from, &to).unwrap_err();
            assert!(error.0.contains("missing committed metadata"), "{error}");
        }
    }

    #[test]
    fn unrelated_symlink_and_gitlink_do_not_change_bare_parity() {
        let temp = fixture();
        let root = temp.path();
        let from = commit(root);
        write(root, "src-tauri/middle/src/lib.rs", "pub fn changed() {}\n");
        write(root, "packages/sdk/index.ts", "export {};\n");
        git(root, &["add", "."]);
        // Index-only fixtures are portable and never follow either link.
        let oid = git(root, &["rev-parse", &format!("{from}:package.json")]);
        git(
            root,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("120000,{},docs-link", oid.trim()),
            ],
        );
        git(
            root,
            &[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("160000,{from},vendor-link"),
            ],
        );
        git(
            root,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "links",
            ],
        );
        let to = git(root, &["rev-parse", "HEAD"]).trim().to_owned();
        let bare = tempfile::tempdir().unwrap();
        git(
            root,
            &[
                "clone",
                "--bare",
                root.to_str().unwrap(),
                bare.path().to_str().unwrap(),
            ],
        );
        let expected = CacheDistance {
            files: 4,
            crates: 1,
            crates_transitive: 2,
            js_workspaces: 1,
        };
        assert_eq!(cache_distance(root, &from, &to).unwrap(), expected);
        assert_eq!(cache_distance(bare.path(), &from, &to).unwrap(), expected);
    }

    #[test]
    fn unsafe_tree_paths_and_needed_nonregular_metadata_fail_closed() {
        for path in [
            "../Cargo.toml",
            "/Cargo.toml",
            "C:/Cargo.toml",
            "a\\Cargo.toml",
            ".git/config",
        ] {
            assert!(CommitTree::parse(&format!("100644 blob abc\t{path}\0")).is_err());
        }
        let tree = CommitTree::parse("120000 blob abc\tpackage.json\0").unwrap();
        assert!(js_workspaces(
            Path::new("/repo"),
            &["web/a.ts".into()],
            &tree,
            &mut |_, _, _| panic!("must not follow symlink")
        )
        .is_err());
    }

    #[test]
    fn cargo_package_dependency_and_target_escapes_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let tree =
            CommitTree::parse("100644 blob abc\tCargo.toml\x00100644 blob def\tsrc/lib.rs\0")
                .unwrap();
        let valid = serde_json::json!({"packages":[{
            "manifest_path": root.join("Cargo.toml"),
            "dependencies": [], "targets": [{"src_path": root.join("src/lib.rs")}]
        }]});
        assert!(validate_metadata_paths(&valid.to_string(), &root, &tree).is_ok());
        for kind in ["package", "dependency", "target"] {
            let mut value = valid.clone();
            let outside = root.join("../outside/Cargo.toml");
            match kind {
                "package" => value["packages"][0]["manifest_path"] = serde_json::json!(outside),
                "dependency" => {
                    value["packages"][0]["dependencies"] = serde_json::json!([{"path":outside}])
                }
                _ => value["packages"][0]["targets"][0]["src_path"] = serde_json::json!(outside),
            }
            assert!(
                validate_metadata_paths(&value.to_string(), &root, &tree).is_err(),
                "{kind}"
            );
        }
    }

    #[test]
    fn bounded_cache_refreshes_recency_and_replaces_without_growing() {
        let mut cache = BoundedCache::default();
        for key in 0..CACHE_ENTRY_LIMIT {
            cache.insert(key, key);
        }
        assert_eq!(cache.get(&0), Some(&0));
        cache.insert(CACHE_ENTRY_LIMIT, CACHE_ENTRY_LIMIT);
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&0), Some(&0));
        cache.insert(0, 999);
        assert_eq!(cache.get(&0), Some(&999));
        assert_eq!(cache.entries.len(), CACHE_ENTRY_LIMIT);
    }

    #[test]
    fn bounded_cache_concurrent_publication_never_exceeds_global_limit() {
        let cache = std::sync::Arc::new(Mutex::new(BoundedCache::default()));
        std::thread::scope(|scope| {
            for repository in 0..8 {
                let cache = cache.clone();
                scope.spawn(move || {
                    for target in 0..CACHE_ENTRY_LIMIT * 3 {
                        let mut cache = cache.lock().unwrap();
                        let key = (repository, target);
                        if cache.get(&key).is_none() {
                            cache.insert(key, target);
                        }
                        assert!(cache.entries.len() <= CACHE_ENTRY_LIMIT);
                    }
                });
            }
        });
        assert_eq!(cache.lock().unwrap().entries.len(), CACHE_ENTRY_LIMIT);
    }

    #[test]
    fn source_only_target_measurements_evict_old_trees_and_reload_on_demand() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        git(&root, &["init"]);
        write(&root, "package.json", r#"{"workspaces":["web"]}"#);
        write(&root, "web/package.json", "{}");
        write(&root, "web/index.ts", "export const version = 0;\n");
        let from = commit(&root);
        let mut first_target = None;
        let mut tree_loads = 0;
        let mut run = |program: &str, directory: &Path, args: &[String]| {
            assert_ne!(program, "cargo", "source-only JS fixture needs no Cargo");
            if args.first().is_some_and(|arg| arg == "ls-tree") {
                tree_loads += 1;
            }
            command_output(program, directory, args)
        };
        for target in 1..=CACHE_ENTRY_LIMIT + 3 {
            write(
                &root,
                "web/index.ts",
                &format!("export const version = {target};\n"),
            );
            let to = commit(&root);
            first_target.get_or_insert_with(|| to.clone());
            let measured = measure(&root, &from, &to, &mut run).unwrap();
            assert_eq!(
                measured,
                CacheDistance {
                    files: 1,
                    js_workspaces: 1,
                    ..Default::default()
                }
            );
            assert!(TREES.get().unwrap().lock().unwrap().entries.len() <= CACHE_ENTRY_LIMIT);
        }
        let first_target = first_target.unwrap();
        assert!(TREES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .get(&(root.clone(), first_target.clone()))
            .is_none());
        let measured = measure(&root, &from, &first_target, &mut run).unwrap();
        assert_eq!(measured.files, 1);
        assert_eq!(measured.js_workspaces, 1);
        assert_eq!(tree_loads, CACHE_ENTRY_LIMIT + 4);
        assert!(TREES.get().unwrap().lock().unwrap().entries.len() <= CACHE_ENTRY_LIMIT);
    }

    #[test]
    fn shared_fanout_returns_sorted_direct_and_transitive_members() {
        let metadata = r#"{"packages":[
            {"name":"outer","manifest_path":"/repo/Cargo.toml","dependencies":[{"name":"leaf"}]},
            {"name":"leaf","manifest_path":"/repo/nested/Cargo.toml"},
            {"name":"top","manifest_path":"/repo/top/Cargo.toml","dependencies":[{"name":"outer"}]}
        ]}"#;
        assert_eq!(
            affected_crates(metadata, &["nested/src/lib.rs".into()], Path::new("/repo")),
            Some(CrateFanout {
                direct: vec!["leaf".into()],
                transitive: vec!["leaf".into(), "outer".into(), "top".into()],
            })
        );
        assert!(affected_crates("invalid", &[], Path::new("/repo")).is_none());
    }
}
