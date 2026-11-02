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
/// The directory, beside `build-slots/`, that holds every `$TMPDIR` an
/// executor exports, one level down under a fixed-length name (see
/// [`scratch_tree_name`]):
///
/// - a cell's scratch is `tmp/c<hex>` itself (see [`cell_scratch_dir`]);
/// - a node scratch replica is `tmp/n<hex>/`, holding `tree/` (the `$TMPDIR`),
///   `cache.git/`, the replica's own `index`, and `state.json`.
///
/// The name is short, and the scratch's depth fixed, because a pathname unix
/// socket bound under `$TMPDIR` has to fit `sockaddr_un.sun_path`: 108 bytes on
/// Linux and 104 on macOS, the terminating NUL included. The scratch is the one
/// place a fenced job may bind a socket its later spawns can still reach (see
/// `cairn_sandbox::SandboxPolicy::allow_job_sockets_in`), so every byte of its
/// path is a byte the job's socket names cannot use. Nested under a cell's
/// authority or named by a full scratch id it left about 30.
///
/// A tool that canonicalizes its socket directory (tmux does) sees this real
/// path, which is why the scratch lives here rather than behind a short alias.
pub const SCRATCH_TREES_DIR: &str = "tmp";
/// The directory inside a node scratch replica that is the `$TMPDIR` itself.
pub const NODE_SCRATCH_TREE_DIR: &str = "tree";
/// Hex digits of the identity digest in a scratch tree's name.
pub const SCRATCH_TREE_DIGEST_HEX: usize = 11;
/// A scratch tree's name: its kind letter and the digest.
pub const SCRATCH_TREE_NAME_LEN: usize = 1 + SCRATCH_TREE_DIGEST_HEX;

/// What a directory under [`SCRATCH_TREES_DIR`] is. Each kind has its own
/// letter, so one never mistakes the other for its own: a cell's scratch is
/// tenant space, and a tenant may write a `state.json` into it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScratchTreeKind {
    /// A cell's scratch, named by its authority's place under the root.
    Cell,
    /// A node scratch replica, named by its scratch id.
    Node,
}

impl ScratchTreeKind {
    fn letter(self) -> char {
        match self {
            Self::Cell => 'c',
            Self::Node => 'n',
        }
    }
}

/// The name a scratch tree of `kind` whose identity is `identity` has under
/// [`SCRATCH_TREES_DIR`]: the kind letter, then the leading hex digits of a
/// SHA-256 over the kind and the identity. Fixed length whatever the identity,
/// so no project key, cell id, or scratch id can lengthen a `$TMPDIR`.
pub fn scratch_tree_name(kind: ScratchTreeKind, identity: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update([kind.letter() as u8, 0]);
    digest.update(identity);
    let mut name = String::with_capacity(SCRATCH_TREE_NAME_LEN);
    name.push(kind.letter());
    name.push_str(&hex::encode(digest.finalize())[..SCRATCH_TREE_DIGEST_HEX]);
    name
}

/// Whether `name` has the shape [`scratch_tree_name`] gives `kind`. A staged
/// discard beside a tree (`<name>.discard-*`) does not.
pub fn is_scratch_tree_name(kind: ScratchTreeKind, name: &str) -> bool {
    name.len() == SCRATCH_TREE_NAME_LEN
        && name.starts_with(kind.letter())
        && name[1..]
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

/// A cell's scratch, from where its authority lives: `<root>/tmp/c<hex>`,
/// where `<root>` holds the `build-slots` directory the authority is under (the
/// build root for a project's cells, the home for the machine authority's), and
/// the digest is of the authority's path relative to `<root>`.
///
/// Derived, never recorded: the same authority always names the same scratch,
/// on the same volume as its authority, and a build root that moves whole
/// keeps every name. `None` for an authority under no `build-slots` directory.
pub fn cell_scratch_dir(authority: &Path) -> Option<PathBuf> {
    let inventory = authority
        .ancestors()
        .skip(1)
        .find(|ancestor| ancestor.file_name() == Some(std::ffi::OsStr::new(BUILD_SLOTS_DIR)))?;
    let root = inventory.parent()?;
    let relative = authority.strip_prefix(root).ok()?;
    Some(root.join(SCRATCH_TREES_DIR).join(scratch_tree_name(
        ScratchTreeKind::Cell,
        relative.as_os_str().as_encoded_bytes(),
    )))
}

/// The prefix of a per-call directory's name inside a cell's scratch.
pub const PER_CALL_DIR_PREFIX: &str = "pc-";
/// Hex digits of the digest in a per-call directory's name.
pub const PER_CALL_DIR_DIGEST_HEX: usize = 12;

/// The directory a per-call batch (a page call's) runs in beneath its cell's
/// scratch, `pc-<hex>`: its items' cwd, `HOME` and `TMPDIR`.
///
/// Derived from the request and attempt the batch was placed under, never
/// recorded, so everything that holds the batch's ownership names the same
/// directory: the release that removes it once its tree is proven gone, the
/// drain that waits on a survivor in it, the sweep that discards one nothing
/// names, and the executor page that reports it. Fixed length for the reason a
/// scratch tree's name is (see [`SCRATCH_TREES_DIR`]): a socket bound in it
/// has to fit `sun_path`.
pub fn per_call_dir_name(request_id: &str, attempt_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(request_id.as_bytes());
    digest.update([0]);
    digest.update(attempt_id.as_bytes());
    format!(
        "{PER_CALL_DIR_PREFIX}{}",
        &hex::encode(digest.finalize())[..PER_CALL_DIR_DIGEST_HEX]
    )
}

/// Whether `name` has the shape [`per_call_dir_name`] gives. A staged discard
/// beside one (`<name>.discard-*`) does not.
pub fn is_per_call_dir_name(name: &str) -> bool {
    name.strip_prefix(PER_CALL_DIR_PREFIX)
        .is_some_and(|digest| {
            digest.len() == PER_CALL_DIR_DIGEST_HEX
                && digest
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        })
}
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

    /// The build root's [`SCRATCH_TREES_DIR`]: where node scratch replicas
    /// live, beside the scratch of the build root's cells.
    pub fn scratch_trees(&self) -> PathBuf {
        self.build_root.join(SCRATCH_TREES_DIR)
    }

    /// Every [`SCRATCH_TREES_DIR`] this executor's scratch can be under: the
    /// home's, which holds the machine authority's cells, then the build
    /// root's when that is somewhere else.
    pub fn scratch_tree_roots(&self) -> Vec<PathBuf> {
        let mut roots = vec![self.home.join(SCRATCH_TREES_DIR)];
        if self.has_separate_build_root() {
            roots.push(self.scratch_trees());
        }
        roots
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
        assert_eq!(paths.scratch_trees(), Path::new("/Users/u/.cairn/tmp"));
        assert_eq!(
            paths.scratch_tree_roots(),
            vec![PathBuf::from("/Users/u/.cairn/tmp")]
        );
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
        assert_eq!(
            paths.scratch_trees(),
            Path::new("/Volumes/Builds/cairn/tmp")
        );
        assert_eq!(
            paths.scratch_tree_roots(),
            vec![
                PathBuf::from("/Users/u/.cairn/tmp"),
                PathBuf::from("/Volumes/Builds/cairn/tmp"),
            ]
        );
    }

    #[test]
    fn a_scratch_tree_name_is_fixed_length_and_kind_tagged() {
        let long = "x".repeat(4_096);
        for (kind, identity) in [
            (ScratchTreeKind::Cell, "build-slots/p/.authority/slot-1"),
            (
                ScratchTreeKind::Node,
                "a0445167-6499-499f-8aec-96ec56da3742",
            ),
            (ScratchTreeKind::Node, long.as_str()),
        ] {
            let name = scratch_tree_name(kind, identity.as_bytes());
            assert_eq!(name.len(), SCRATCH_TREE_NAME_LEN, "{name}");
            assert!(is_scratch_tree_name(kind, &name), "{name}");
            let other = match kind {
                ScratchTreeKind::Cell => ScratchTreeKind::Node,
                ScratchTreeKind::Node => ScratchTreeKind::Cell,
            };
            assert!(!is_scratch_tree_name(other, &name), "{name}");
            assert_eq!(name, scratch_tree_name(kind, identity.as_bytes()));
        }
        // The same identity under the other kind is another tree.
        assert_ne!(
            scratch_tree_name(ScratchTreeKind::Cell, b"same"),
            scratch_tree_name(ScratchTreeKind::Node, b"same")
        );
        let name = scratch_tree_name(ScratchTreeKind::Cell, b"x");
        assert!(!is_scratch_tree_name(
            ScratchTreeKind::Cell,
            &format!("{name}.discard-1-a")
        ));
        assert!(!is_scratch_tree_name(
            ScratchTreeKind::Cell,
            &name.to_uppercase()
        ));
    }

    #[test]
    fn a_per_call_directory_is_named_by_its_request_and_attempt() {
        let name = per_call_dir_name("request-1", "attempt-1");
        assert_eq!(name, per_call_dir_name("request-1", "attempt-1"));
        assert_eq!(
            name.len(),
            PER_CALL_DIR_PREFIX.len() + PER_CALL_DIR_DIGEST_HEX
        );
        assert!(is_per_call_dir_name(&name), "{name}");
        assert_ne!(name, per_call_dir_name("request-1", "attempt-2"));
        // The separator keeps the two ids from sliding into each other.
        assert_ne!(per_call_dir_name("ab", "c"), per_call_dir_name("a", "bc"));
        assert!(!is_per_call_dir_name(&format!("{name}.discard-1-x")));
        assert!(!is_per_call_dir_name("pc-"));
        assert!(!is_per_call_dir_name("c0123456789a"));
    }

    #[test]
    fn a_cell_scratch_sits_beside_the_inventory_its_authority_is_in() {
        let project = Path::new("/Volumes/Builds/cairn/build-slots/cairn/.authority/slot-1088");
        let scratch = cell_scratch_dir(project).unwrap();
        assert_eq!(
            scratch.parent(),
            Some(Path::new("/Volumes/Builds/cairn/tmp"))
        );
        assert_eq!(
            scratch.file_name().unwrap().to_str().unwrap(),
            scratch_tree_name(
                ScratchTreeKind::Cell,
                b"build-slots/cairn/.authority/slot-1088"
            )
        );
        let machine = Path::new("/Users/u/.cairn/build-slots/.authority/checkout-k");
        assert_eq!(
            cell_scratch_dir(machine).unwrap().parent(),
            Some(Path::new("/Users/u/.cairn/tmp"))
        );
        // A build root that moves whole keeps every name.
        let moved = Path::new("/mnt/b/build-slots/cairn/.authority/slot-1088");
        assert_eq!(
            cell_scratch_dir(moved).unwrap().file_name(),
            scratch.file_name()
        );
        // Another project's cell of the same id is another scratch.
        let sibling = Path::new("/Volumes/Builds/cairn/build-slots/rs/.authority/slot-1088");
        assert_ne!(cell_scratch_dir(sibling).unwrap(), scratch);
        assert_eq!(
            cell_scratch_dir(Path::new("/tmp/x/.authority/slot-1")),
            None
        );
        assert_eq!(cell_scratch_dir(Path::new("/build-slots")), None);
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
