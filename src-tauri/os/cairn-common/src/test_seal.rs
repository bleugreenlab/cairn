//! The test-process seal: a Rust test binary is structurally unable to reach a
//! live Cairn home.
//!
//! An unsealed test process reaches live state two ways:
//!
//! - **Home inheritance.** `HOME` and `CAIRN_HOME` name the live home, so a test
//!   that saves settings, an agent, or a workflow commits into the workspace
//!   repository there, and the workspace save path pushes that commit to the
//!   repository's real origin.
//! - **Repository discovery.** A build slot's scratch, which an agent shell
//!   exports as `TMPDIR`, lives under `<home>/build-slots`, and so may a jj-only
//!   checkout the tests run from. Git run in a directory that is not itself a
//!   repository walks upward until it finds one, and the first one above a slot
//!   is the home's own `.git`: a remote set or a commit made "in a temp repo"
//!   lands in the live workspace repository instead.
//!
//! [`seal_process`] closes both before any test runs. It moves `HOME`,
//! `CAIRN_HOME`, and the temp directory into a fresh root outside every live
//! home, and puts that root and every live home on `GIT_CEILING_DIRECTORIES`, so
//! no git command in the process tree can discover a repository at or above
//! either. The Rust test runner (`scripts/lib/rust-test-scratch.ts`) applies the
//! same seal to every child it starts and marks it with [`SEALED_ENV`]; a test
//! binary started any other way, such as a bare `cargo test` from an agent
//! shell, seals itself. Each test binary calls [`seal_process`] from a
//! constructor that runs before `main`, because a test that resolved a home
//! before the seal moved it would keep the live one.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Names the seal root of an already-sealed process. A process that inherits it
/// leaves its environment alone, so a runner-sealed test, and every child a
/// sealed test spawns, keeps the one seal it was given.
pub const SEALED_ENV: &str = "CAIRN_TEST_SEALED";

/// Set by the executor to a command's directory under the cell's own scratch.
/// When it is set, the inherited `TMPDIR` is a node's travelling scratch and is
/// never used for test leftovers.
const CELL_TMPDIR_ENV: &str = "CAIRN_CELL_TMPDIR";

/// The executor's tool cache, sealed out for the reason the runner gives: a test
/// that builds a fence policy in-process would otherwise grant the machine's
/// real cache to the sandboxes it spawns.
const TOOL_CACHE_ENV: &str = "CAIRN_TOOL_CACHE";

/// One sealed process's layout. `home` serves as both `HOME` and `CAIRN_HOME`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seal {
    pub root: PathBuf,
    pub home: PathBuf,
    pub temp: PathBuf,
}

impl Seal {
    fn at(root: PathBuf) -> Self {
        Self {
            home: root.join("home"),
            temp: root.join("tmp"),
            root,
        }
    }
}

/// Every Cairn home this process could treat as live: an explicit `CAIRN_HOME`,
/// and both default homes under `HOME`. Which default
/// [`cairn_home`](crate::paths::cairn_home) picks depends on the build mode, so
/// both are protected whether or not `CAIRN_HOME` is set.
pub fn live_homes(cairn_home: Option<&Path>, home: Option<&Path>) -> Vec<PathBuf> {
    let mut homes = Vec::new();
    if let Some(explicit) = cairn_home.filter(|path| !path.as_os_str().is_empty()) {
        homes.push(explicit.to_path_buf());
    }
    if let Some(home) = home {
        homes.push(home.join(".cairn"));
        homes.push(home.join(".cairn-dev"));
    }
    homes
}

/// Whether `child` is `parent` or lies beneath it, judged both as written and
/// with symlinks resolved, so neither macOS's `/var` -> `/private/var` nor a
/// symlinked home lets a path inside a live home pass as outside it.
pub fn is_within(parent: &Path, child: &Path) -> bool {
    if child.starts_with(parent) {
        return true;
    }
    match (parent.canonicalize(), child.canonicalize()) {
        (Ok(parent), Ok(child)) => child.starts_with(parent),
        _ => false,
    }
}

/// The directory the seal root is created in: the first candidate outside every
/// live home. Candidates run nearest first, so a temp directory that is already
/// safe is used as is.
pub fn seal_base(candidates: &[PathBuf], live: &[PathBuf]) -> Option<PathBuf> {
    candidates
        .iter()
        .find(|candidate| live.iter().all(|home| !is_within(home, candidate)))
        .cloned()
}

/// `GIT_CEILING_DIRECTORIES` for a sealed process: whatever the caller already
/// set, then the seal root, then every live home. Git never climbs into a
/// ceiling while discovering a repository, so a live home's `.git` cannot be
/// found from anywhere beneath it, including a test's source checkout.
pub fn git_ceilings(
    existing: Option<&std::ffi::OsStr>,
    seal_root: &Path,
    live: &[PathBuf],
) -> OsString {
    let mut ceilings: Vec<PathBuf> = existing
        .map(|value| std::env::split_paths(value).collect())
        .unwrap_or_default();
    ceilings.push(seal_root.to_path_buf());
    ceilings.extend(live.iter().cloned());
    ceilings.retain(|path| !path.as_os_str().is_empty());
    std::env::join_paths(ceilings).unwrap_or_else(|_| seal_root.as_os_str().to_owned())
}

fn temp_candidates() -> Vec<PathBuf> {
    let nearest = std::env::var_os(CELL_TMPDIR_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let mut candidates = vec![nearest];
    if cfg!(windows) {
        let system_root = std::env::var_os("SystemRoot").unwrap_or_else(|| "C:\\Windows".into());
        candidates.push(PathBuf::from(system_root).join("Temp"));
    } else {
        candidates.push(PathBuf::from("/tmp"));
        candidates.push(PathBuf::from("/var/tmp"));
    }
    candidates.retain(|candidate| candidate.is_dir());
    candidates
}

fn create_root(base: &Path) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    for attempt in 0..64u32 {
        let candidate = base.join(format!(
            "cairn-rust-tests-process-{pid}-{nanos:x}-{attempt}"
        ));
        match std::fs::create_dir(&candidate) {
            Ok(()) => return candidate,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("test seal: cannot create {}: {error}", candidate.display()),
        }
    }
    panic!("test seal: no free seal root under {}", base.display())
}

fn set_if_unset(name: &str, value: PathBuf) {
    if std::env::var_os(name).is_none_or(|current| current.is_empty()) {
        std::env::set_var(name, value);
    }
}

/// Seal this process away from every live Cairn home, unless it is already
/// sealed. Returns the seal it applied, or `None` when it inherited one.
///
/// Mutates the process environment, so it must run before any other thread
/// exists: call it from a constructor that runs before `main`.
pub fn seal_process() -> Option<Seal> {
    if std::env::var_os(SEALED_ENV).is_some_and(|value| !value.is_empty()) {
        return None;
    }
    let inherited_home = dirs::home_dir();
    let explicit_home = std::env::var_os("CAIRN_HOME").map(PathBuf::from);
    let live = live_homes(explicit_home.as_deref(), inherited_home.as_deref());
    let base = seal_base(&temp_candidates(), &live).unwrap_or_else(|| {
        panic!(
            "test seal: no temp directory outside the live Cairn homes {}",
            live.iter()
                .map(|home| home.display().to_string())
                .collect::<Vec<_>>()
                .join(", "),
        )
    });
    let seal = Seal::at(create_root(&base));
    for dir in [&seal.home, &seal.temp] {
        std::fs::create_dir_all(dir)
            .unwrap_or_else(|error| panic!("test seal: cannot create {}: {error}", dir.display()));
    }

    // Toolchains stay where the inherited home keeps them, so a test that runs
    // cargo still finds it once HOME has moved.
    if let Some(home) = &inherited_home {
        set_if_unset("CARGO_HOME", home.join(".cargo"));
        set_if_unset("RUSTUP_HOME", home.join(".rustup"));
    }
    let ceilings = git_ceilings(
        std::env::var_os("GIT_CEILING_DIRECTORIES").as_deref(),
        &seal.root,
        &live,
    );
    std::env::remove_var(TOOL_CACHE_ENV);
    std::env::set_var("HOME", &seal.home);
    std::env::set_var("CAIRN_HOME", &seal.home);
    if cfg!(windows) {
        std::env::set_var("USERPROFILE", &seal.home);
    }
    for name in ["TMPDIR", "TMP", "TEMP"] {
        std::env::set_var(name, &seal.temp);
    }
    std::env::set_var("GIT_CEILING_DIRECTORIES", ceilings);
    std::env::set_var(SEALED_ENV, &seal.root);
    remove_at_exit(&seal.root);
    Some(seal)
}

/// Remove the seal root when the process exits. Best effort: a test killed
/// mid-run never reaches exit, and its root is ordinary temp-directory litter
/// outside every live home.
#[cfg(unix)]
fn remove_at_exit(root: &Path) {
    static ROOT: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    extern "C" fn remove_root() {
        if let Some(root) = ROOT.get() {
            let _ = std::fs::remove_dir_all(root);
        }
    }
    if ROOT.set(root.to_path_buf()).is_ok() {
        // SAFETY: `remove_root` is a plain `extern "C" fn()` that touches only a
        // `OnceLock` that was set before registration.
        unsafe {
            libc::atexit(remove_root);
        }
    }
}

#[cfg(not(unix))]
fn remove_at_exit(_root: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_homes_protect_an_explicit_home_and_both_defaults() {
        let homes = live_homes(Some(Path::new("/srv/cairn")), Some(Path::new("/home/op")));
        assert_eq!(
            homes,
            ["/srv/cairn", "/home/op/.cairn", "/home/op/.cairn-dev"].map(PathBuf::from)
        );
        assert_eq!(
            live_homes(Some(Path::new("")), Some(Path::new("/home/op"))),
            ["/home/op/.cairn", "/home/op/.cairn-dev"].map(PathBuf::from)
        );
    }

    /// A build slot's scratch is inside the live home, so a temp directory
    /// there must give way to the system temp roots.
    #[test]
    fn a_temp_directory_inside_a_live_home_gives_way() {
        let live = live_homes(None, Some(Path::new("/home/op")));
        let slot_scratch =
            PathBuf::from("/home/op/.cairn/build-slots/CAIRN/.authority/slot-446/scratch");
        let base = seal_base(&[slot_scratch, PathBuf::from("/tmp")], &live);
        assert_eq!(base, Some(PathBuf::from("/tmp")));

        let safe = PathBuf::from("/var/folders/xy/T");
        assert_eq!(seal_base(std::slice::from_ref(&safe), &live), Some(safe));
        assert_eq!(
            seal_base(&[PathBuf::from("/home/op/.cairn-dev/tmp")], &live),
            None
        );
    }

    #[test]
    fn containment_sees_through_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        std::fs::create_dir_all(home.join("scratch")).unwrap();
        let alias = dir.path().join("alias");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&home, &alias).unwrap();
        #[cfg(unix)]
        assert!(is_within(&home, &alias.join("scratch")));
        assert!(is_within(&home, &home));
        assert!(!is_within(&home.join("scratch"), &home));
    }

    #[test]
    fn ceilings_keep_the_callers_and_add_the_root_and_every_live_home() {
        let live = [PathBuf::from("/home/op/.cairn")];
        let existing = std::env::join_paths(["/work/slot"]).unwrap();
        let ceilings = git_ceilings(Some(&existing), Path::new("/tmp/seal"), &live);
        assert_eq!(
            std::env::split_paths(&ceilings).collect::<Vec<_>>(),
            ["/work/slot", "/tmp/seal", "/home/op/.cairn"].map(PathBuf::from)
        );
        let fresh = git_ceilings(None, Path::new("/tmp/seal"), &[]);
        assert_eq!(fresh, OsString::from("/tmp/seal"));
    }
}
