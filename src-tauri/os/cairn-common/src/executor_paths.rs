//! Where one executor keeps each kind of state.
//!
//! An executor's state splits by what losing it would cost. The **home** holds
//! what the machine needs in order to exist as an executor and cannot
//! regenerate: the generation lock, the owner record, the survivor and
//! cell-epoch ledgers, storage state, and the authorities of work that runs
//! outside any cell (existing checkouts and scratch-only residencies). The
//! **build root** holds derived, rebuildable state that grows without bound: the
//! per-project cell inventories, the managed object cache, and the dependency
//! catalog. Cells share blocks with the object cache by copy-on-write, which
//! only works within one volume, so these move together or not at all. **Cache
//! home** is the directory the external build caches (CI targets, the asset
//! cache, sccache, the shared dev target) are found under, by fixed leaf names.
//!
//! With nothing configured the build root is the home and cache home is the
//! home's parent, which is the user's home directory for every standard layout:
//! today's paths, byte for byte. The accessors here are the only place that
//! layout is spelled, so moving the build root is a change of one value.

use std::path::{Path, PathBuf};

use crate::runtime::BUILD_SLOTS_DIR;

/// The directory, inside an inventory or a project's cell root, that holds cell
/// authorities.
pub const AUTHORITY_DIR: &str = ".authority";
/// The managed object cache's directory under the build root.
pub const MANAGED_OBJECTS_DIR: &str = "managed-objects";
/// The dependency catalog's directory under the build root.
pub const DEPENDENCY_CATALOG_DIR: &str = "dependency-catalog";
/// The tool cache's directory under the build root.
pub const TOOL_CACHE_DIR: &str = "tool-cache";
/// The environment variable naming the tool cache to the processes an executor
/// runs. The confined cell's policy grants exactly the directory it names (see
/// `cairn_sandbox::default_writable_extra`).
pub const TOOL_CACHE_ENV: &str = "CAIRN_TOOL_CACHE";
/// Cache home's directory under a configured build root.
pub const CACHE_HOME_DIR: &str = "cache-home";
/// The environment variable naming cache home to processes Cairn launches.
/// Unset means the user's home directory.
pub const CACHE_HOME_ENV: &str = "CAIRN_CACHE_HOME";
/// The file at a configured build root that identifies it as that build root.
/// A directory without it, or with another id in it, is not the build root:
/// an empty mount point, a different drive mounted at the same path, and a
/// path that does not exist all read the same way, as an absent volume.
pub const BUILD_ROOT_MARKER: &str = ".cairn-build-root";
/// The environment variables a runner starts its colocated executor with to
/// configure a build root apart from the home: its path, and the id its marker
/// must carry.
pub const BUILD_ROOT_ENV: &str = "CAIRN_EXECUTOR_BUILD_ROOT";
pub const BUILD_ROOT_ID_ENV: &str = "CAIRN_EXECUTOR_BUILD_ROOT_ID";

/// A build root configured apart from an executor's home: where it is, and the
/// id its marker must carry for the executor to treat it as present.
///
/// One declaration for every side that handles it: the runner's settings hold
/// it, the runner's supervisor hands it to the colocated executor through
/// [`BUILD_ROOT_ENV`] and [`BUILD_ROOT_ID_ENV`], and the executor probes it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildRootDeclaration {
    pub path: PathBuf,
    pub id: String,
}

impl BuildRootDeclaration {
    /// The declaration a parent runner set in this process's environment, if
    /// it set one. Both variables or neither: one alone is ignored.
    pub fn from_env() -> Option<Self> {
        let path = std::env::var_os(BUILD_ROOT_ENV)?;
        let id = std::env::var(BUILD_ROOT_ID_ENV).ok()?;
        (!path.is_empty() && !id.trim().is_empty()).then(|| Self {
            path: PathBuf::from(path),
            id: id.trim().to_string(),
        })
    }

    /// The variables that hand this declaration to a child executor.
    pub fn launch_env(&self) -> [(&'static str, std::ffi::OsString); 2] {
        [
            (BUILD_ROOT_ENV, self.path.clone().into_os_string()),
            (BUILD_ROOT_ID_ENV, self.id.clone().into()),
        ]
    }

    /// Cache home under this build root.
    pub fn cache_home(&self) -> PathBuf {
        self.path.join(CACHE_HOME_DIR)
    }
}

/// The id in `root`'s build-root marker, trimmed.
pub fn read_build_root_marker(root: &Path) -> std::io::Result<String> {
    std::fs::read_to_string(root.join(BUILD_ROOT_MARKER)).map(|id| id.trim().to_string())
}

/// Write `id` into `root`'s build-root marker. `root` must already exist: the
/// marker is written into a mounted volume, never in place of one.
pub fn write_build_root_marker(root: &Path, id: &str) -> std::io::Result<()> {
    if !root.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("build root {} is not a directory", root.display()),
        ));
    }
    std::fs::write(root.join(BUILD_ROOT_MARKER), format!("{id}\n"))
}

/// The directory holding an executor's machine-wide authority files: the
/// generation lock, the owner record, the ledgers, and the authorities of work
/// outside any cell. Always under the home, whatever the build root, so the
/// handoff between executor generations never depends on a removable volume.
pub fn machine_authority_dir(home: &Path) -> PathBuf {
    home.join(BUILD_SLOTS_DIR).join(AUTHORITY_DIR)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorPaths {
    home: PathBuf,
    build_root: PathBuf,
    cache_home: Option<PathBuf>,
}

impl ExecutorPaths {
    /// The default layout: everything derived under `home`, external caches
    /// under its parent.
    pub fn new(home: impl Into<PathBuf>) -> Self {
        let home = home.into();
        Self {
            cache_home: home.parent().map(Path::to_path_buf),
            build_root: home.clone(),
            home,
        }
    }

    /// Keep derived state under `build_root` instead of the home. The external
    /// caches follow it, to [`CACHE_HOME_DIR`] beneath it, so nothing that grows
    /// with builds stays on the home's volume.
    pub fn with_build_root(mut self, build_root: impl Into<PathBuf>) -> Self {
        self.build_root = build_root.into();
        self.cache_home = Some(self.build_root.join(CACHE_HOME_DIR));
        self
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    pub fn build_root(&self) -> &Path {
        &self.build_root
    }

    /// Whether derived state lives somewhere other than the home, and so on a
    /// volume that may come and go.
    pub fn has_separate_build_root(&self) -> bool {
        self.build_root != self.home
    }

    /// See [`machine_authority_dir`].
    pub fn machine_authority_dir(&self) -> PathBuf {
        machine_authority_dir(&self.home)
    }

    /// The directory holding every project's cell root.
    pub fn cell_inventory(&self) -> PathBuf {
        self.build_root.join(BUILD_SLOTS_DIR)
    }

    /// One project's cell root. `component` must already be a single safe path
    /// component; the executor derives it from the project key.
    pub fn project_cell_root(&self, component: &str) -> PathBuf {
        self.cell_inventory().join(component)
    }

    pub fn managed_objects(&self) -> PathBuf {
        self.build_root.join(MANAGED_OBJECTS_DIR)
    }

    pub fn dependency_catalog(&self) -> PathBuf {
        self.build_root.join(DEPENDENCY_CATALOG_DIR)
    }

    /// Where tools a project's setup builds for itself are cached machine-wide,
    /// keyed by the tool and the revision it was built from (for example the
    /// pinned `tursodb`, `scripts/ensure-tursodb.ts`).
    ///
    /// It is the executor's, not the user's: it sits beside the dependency
    /// catalog rather than in a home-directory cache, so a confined cell can be
    /// granted it without being granted anything a person or another Cairn
    /// component keeps there. `<cairn home>/tools`, in particular, holds the
    /// managed Codex the app runs unconfined.
    pub fn tool_cache(&self) -> PathBuf {
        self.build_root.join(TOOL_CACHE_DIR)
    }

    /// Where the external build caches live, or `None` for a home at a
    /// filesystem root, which has nowhere to keep them.
    pub fn cache_home(&self) -> Option<&Path> {
        self.cache_home.as_deref()
    }

    /// Every directory that holds cell authorities: the machine authority
    /// first, then each project's, in directory order.
    pub fn cell_authority_roots(&self) -> Vec<PathBuf> {
        let mut roots = vec![self.machine_authority_dir()];
        if let Ok(projects) = std::fs::read_dir(self.cell_inventory()) {
            roots.extend(
                projects
                    .flatten()
                    .filter(|entry| entry.path().is_dir() && entry.file_name() != AUTHORITY_DIR)
                    .map(|entry| entry.path().join(AUTHORITY_DIR)),
            );
        }
        roots
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_layout_is_the_home_layout() {
        let paths = ExecutorPaths::new("/Users/u/.cairn");
        assert_eq!(paths.home(), Path::new("/Users/u/.cairn"));
        assert_eq!(paths.build_root(), Path::new("/Users/u/.cairn"));
        assert_eq!(
            paths.machine_authority_dir(),
            Path::new("/Users/u/.cairn/build-slots/.authority")
        );
        assert_eq!(
            paths.cell_inventory(),
            Path::new("/Users/u/.cairn/build-slots")
        );
        assert_eq!(
            paths.project_cell_root("cairn"),
            Path::new("/Users/u/.cairn/build-slots/cairn")
        );
        assert_eq!(
            paths.managed_objects(),
            Path::new("/Users/u/.cairn/managed-objects")
        );
        assert_eq!(
            paths.dependency_catalog(),
            Path::new("/Users/u/.cairn/dependency-catalog")
        );
        assert_eq!(paths.tool_cache(), Path::new("/Users/u/.cairn/tool-cache"));
        assert_eq!(paths.cache_home(), Some(Path::new("/Users/u")));
    }

    #[test]
    fn a_build_root_moves_derived_state_and_leaves_the_machine_authority_home() {
        let paths = ExecutorPaths::new("/Users/u/.cairn").with_build_root("/Volumes/Builds/cairn");
        assert_eq!(
            paths.machine_authority_dir(),
            Path::new("/Users/u/.cairn/build-slots/.authority")
        );
        assert_eq!(
            paths.cell_inventory(),
            Path::new("/Volumes/Builds/cairn/build-slots")
        );
        assert_eq!(
            paths.managed_objects(),
            Path::new("/Volumes/Builds/cairn/managed-objects")
        );
        assert_eq!(
            paths.dependency_catalog(),
            Path::new("/Volumes/Builds/cairn/dependency-catalog")
        );
        assert_eq!(
            paths.tool_cache(),
            Path::new("/Volumes/Builds/cairn/tool-cache")
        );
        assert_eq!(
            paths.cache_home(),
            Some(Path::new("/Volumes/Builds/cairn/cache-home"))
        );
    }

    #[test]
    fn a_marker_is_written_only_into_an_existing_root_and_read_back_trimmed() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("unmounted");
        assert!(write_build_root_marker(&missing, "id").is_err());
        assert!(!missing.exists(), "the marker never creates the root");
        write_build_root_marker(temp.path(), "abc-123").unwrap();
        assert_eq!(read_build_root_marker(temp.path()).unwrap(), "abc-123");
        assert!(!ExecutorPaths::new(temp.path()).has_separate_build_root());
        assert!(ExecutorPaths::new(temp.path())
            .with_build_root(&missing)
            .has_separate_build_root());
    }

    #[test]
    fn authority_roots_are_the_machine_authority_then_each_project() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let build = temp.path().join("build");
        std::fs::create_dir_all(home.join("build-slots/.authority")).unwrap();
        // A project left in the home's inventory is not the build root's.
        std::fs::create_dir_all(home.join("build-slots/stale/.authority")).unwrap();
        std::fs::create_dir_all(build.join("build-slots/cairn/.authority")).unwrap();
        let paths = ExecutorPaths::new(&home).with_build_root(&build);
        assert_eq!(
            paths.cell_authority_roots(),
            vec![
                home.join("build-slots/.authority"),
                build.join("build-slots/cairn/.authority"),
            ]
        );
    }
}
