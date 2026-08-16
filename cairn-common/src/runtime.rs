//! Where the Cairn-owned JavaScript runtime is installed, and why that location
//! is the mechanism rather than an implementation detail.
//!
//! Cairn promises that inline TypeScript/JavaScript can `import { read } from
//! "@cairn/sdk"` in ANY project, including one with no JavaScript toolchain at
//! all, while a project that genuinely depends on the SDK keeps resolving its
//! own copy. Both halves of that promise are satisfied by *placement*: the
//! runner-owned package tree is installed in a Cairn-owned directory that is an
//! ANCESTOR of the checkout, and Node/Bun resolution walks upward from the
//! importer taking the first `node_modules` it meets. A project's own
//! `node_modules` lives inside the checkout and is therefore always nearer than
//! ours, so project-local precedence IS the resolution algorithm rather than
//! code we wrote and could get wrong.
//!
//! ## Why not a resolver hook (measured on Bun 1.3.13)
//!
//! The obvious design is a Bun preload registering `Bun.plugin` with an
//! `onResolve` hook for `@cairn/sdk` that maps the specifier to the runner tree.
//! It does not work. The preload loads and runs, but the `onResolve` callback is
//! never invoked for a bare specifier: not for a static `import`, not for
//! `await import()`, not for `require()`, and not only under `bun -e` but also
//! for an ordinary entry file. Bun's runtime plugins do not participate in
//! bare-specifier resolution. The failure mode is the worst kind, because the
//! hook appears to be installed and the import still fails with Bun's ordinary
//! missing-dependency error, which reads as the user's mistake rather than ours.
//!
//! `NODE_PATH` is likewise ignored by Bun, as
//! [`crate::scratch::link_scratch_dependency_resolution`] already documents.
//! Placement is what remains, and it is also the simplest thing that works.
//!
//! ## The consequence: only a Cairn-owned ancestor will do
//!
//! Cairn materializes checkouts as build-slot cells
//! (`<root>/build-slots/<project>/slot-N`), so a slot's parent is Cairn-owned and
//! shared by every slot of that project. That is where the runtime goes. A run
//! against a user's own repository has no Cairn-owned ancestor, and Cairn does
//! not write there, so no runtime is installed for it and an import of a Cairn
//! package fails with the runtime's ordinary missing-module error. Such a
//! project must depend on `@cairn/sdk` itself. See [`shared_runtime_parent`].

use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};

use crate::executor_protocol::{
    ResidentRuntimeAsset, MAX_RESIDENT_RUNTIME_ASSETS, MAX_RESIDENT_RUNTIME_ASSETS_BYTES,
    MAX_RESIDENT_RUNTIME_ASSET_BYTES,
};

/// Directory name of an installed JavaScript dependency tree.
pub const NODE_MODULES: &str = "node_modules";

/// The path component marking a Cairn-owned cell root. A checkout has a
/// Cairn-owned parent exactly when it lives under one of these.
pub const BUILD_SLOTS_DIR: &str = "build-slots";

/// The npm scope every Cairn-owned package lives under, and therefore the single
/// directory an install swaps. Having exactly one swap unit is what lets the
/// version stamp live inside the thing it describes.
pub const SCOPE_DIR: &str = "@cairn";

/// Records which runtime the shared tree currently holds, so a Cairn upgrade
/// re-syncs it and staleness is detectable without rehashing on every run.
///
/// It lives INSIDE [`SCOPE_DIR`], not beside it. A stamp written as a separate
/// file is a second thing to keep in step with the first, and two installers
/// racing can leave one's stamp describing the other's tree -- which certifies
/// the wrong SDK permanently, because the next run takes the fast path and
/// believes it. Inside the swapped directory the two move as one: a reader sees
/// either the whole previous install or the whole next one.
pub const RUNTIME_MARKER: &str = ".cairn-runtime-version";

/// The Cairn-owned directory the runtime is installed into for `checkout`, or
/// `None` when this checkout has no Cairn-owned ancestor.
///
/// `None` is a real answer rather than a failure to compute one: a run against a
/// user's live checkout genuinely has nowhere Cairn may write. Callers skip the
/// install rather than failing the run, because most work never imports a Cairn
/// package; a run that does import one gets the runtime's ordinary
/// missing-module error.
pub fn shared_runtime_parent(checkout: &Path) -> Option<PathBuf> {
    let parent = checkout.parent()?;
    // The parent must be a strict DESCENDANT of a `build-slots` directory.
    // `build-slots/<project>` is one project's cell parent and a legitimate
    // install root, while `build-slots` itself is the root of every project and
    // belongs to no single one of them. `any` leaves the iterator positioned just
    // past the match, so a remaining component is exactly that strictness.
    let mut components = parent.components();
    let under_build_slots = components
        .any(|component| matches!(component, Component::Normal(name) if name == BUILD_SLOTS_DIR));
    (under_build_slots && components.next().is_some()).then(|| parent.to_path_buf())
}

/// The installed location of one runner-provided package under `parent`.
pub fn installed_package_dir(parent: &Path, scope: &str, name: &str) -> PathBuf {
    parent.join(NODE_MODULES).join(scope).join(name)
}

/// Validate runner-supplied runtime assets against the transport limits and the
/// path policy: relative, non-empty, no parent escape, no duplicates.
///
/// Shared by every carrier of runtime assets, so runner-supplied bytes have one
/// security policy rather than one per call site.
pub fn validate_runtime_assets(assets: &[ResidentRuntimeAsset]) -> Result<(), String> {
    if assets.len() > MAX_RESIDENT_RUNTIME_ASSETS {
        return Err(format!(
            "runtime assets exceed the {MAX_RESIDENT_RUNTIME_ASSETS} file limit"
        ));
    }
    let mut total = 0usize;
    let mut seen = HashSet::new();
    for asset in assets {
        if asset.path.is_empty() {
            return Err("runtime asset path must be non-empty".into());
        }
        let path = Path::new(&asset.path);
        if path.is_absolute()
            || path.components().any(|c| {
                matches!(
                    c,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(format!(
                "runtime asset path {} must be relative and may not escape its root",
                asset.path
            ));
        }
        if asset.data.len() > MAX_RESIDENT_RUNTIME_ASSET_BYTES {
            return Err(format!(
                "runtime asset {} exceeds the {MAX_RESIDENT_RUNTIME_ASSET_BYTES} byte file limit",
                asset.path
            ));
        }
        total = total
            .checked_add(asset.data.len())
            .ok_or_else(|| "runtime asset aggregate size overflow".to_string())?;
        if total > MAX_RESIDENT_RUNTIME_ASSETS_BYTES {
            return Err(format!(
                "runtime assets exceed the {MAX_RESIDENT_RUNTIME_ASSETS_BYTES} byte aggregate limit"
            ));
        }
        if !seen.insert(asset.path.as_str()) {
            return Err(format!("duplicate runtime asset path {}", asset.path));
        }
    }
    Ok(())
}

/// A content identity for one runtime asset set, used as the install stamp.
///
/// Derived from the bytes rather than taken from the build version, so the stamp
/// answers the question the install actually asks -- is what is on disk what I am
/// about to write -- which stays correct across a dev build whose version string
/// does not move while its packages do.
pub fn assets_version(assets: &[ResidentRuntimeAsset]) -> String {
    use sha2::{Digest, Sha256};
    let mut ordered: Vec<&ResidentRuntimeAsset> = assets.iter().collect();
    ordered.sort_by(|a, b| a.path.cmp(&b.path));
    let mut hasher = Sha256::new();
    for asset in ordered {
        hasher.update(asset.path.as_bytes());
        hasher.update([0]);
        hasher.update((asset.data.len() as u64).to_le_bytes());
        hasher.update(&asset.data);
    }
    format!("{:x}", hasher.finalize())
}

/// Install `assets` (paths relative to `<parent>/node_modules`) as the shared
/// runtime for every checkout under `parent`, and report the `node_modules` root.
///
/// Every slot of a project shares this tree and their batches run concurrently,
/// so the install is staged and swapped rather than written in place: a
/// concurrent reader sees either the previous complete tree or the new complete
/// tree, never a partially written one. `version` stamps the result, and a
/// matching stamp makes the call a cheap no-op, so the common path costs one
/// file read and takes no lock at all.
///
/// Concurrency is handled in two independent ways, because the failure they
/// prevent is silent and permanent. The stamp lives inside the swapped directory
/// (see [`RUNTIME_MARKER`]), so no interleaving can leave it describing another
/// installer's tree. The swap itself is then serialized by a lock, so two
/// installers of different versions cannot collide mid-rename and fail a batch
/// that had nothing wrong with it.
pub fn install_shared_runtime(
    parent: &Path,
    assets: &[ResidentRuntimeAsset],
    version: &str,
) -> Result<PathBuf, String> {
    validate_runtime_assets(assets)?;
    // One scope means one swap unit. This is enforced here rather than in
    // `validate_runtime_assets`, which also guards workflow assets that are
    // addressed under their own roots.
    if let Some(stray) = assets
        .iter()
        .find(|asset| !asset.path.starts_with(&format!("{SCOPE_DIR}/")))
    {
        return Err(format!(
            "runtime package path {} must live under {SCOPE_DIR}/",
            stray.path
        ));
    }
    let root = parent.join(NODE_MODULES);
    let scope = root.join(SCOPE_DIR);
    let marker = scope.join(RUNTIME_MARKER);
    if stamped_with(&marker, version) {
        return Ok(root);
    }
    std::fs::create_dir_all(&root)
        .map_err(|error| format!("create runtime root {}: {error}", root.display()))?;

    let _lock = InstallLock::acquire(&root)?;
    // Re-read under the lock. Waiting for the lock is exactly the window in which
    // another installer may have put our own version in place, which turns this
    // call back into the no-op it should have been.
    if stamped_with(&marker, version) {
        return Ok(root);
    }

    // Stage beside the destination so the swap below is a rename within one
    // filesystem. The name carries pid and a nanosecond stamp so a crashed
    // installer's leftovers can never be mistaken for this one's.
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let unique = format!("{}-{stamp}", std::process::id());
    let staging = root.join(format!(".cairn-runtime-stage-{unique}"));
    let _ = std::fs::remove_dir_all(&staging);
    for asset in assets {
        let target = staging.join(&asset.path);
        if let Some(dir) = target.parent() {
            std::fs::create_dir_all(dir)
                .map_err(|error| format!("create runtime asset parent: {error}"))?;
        }
        std::fs::write(&target, &asset.data)
            .map_err(|error| format!("write runtime asset {}: {error}", asset.path))?;
    }
    // Stamp the staged tree BEFORE it is published, so the directory that appears
    // is already self-describing.
    let staged_scope = staging.join(SCOPE_DIR);
    std::fs::write(staged_scope.join(RUNTIME_MARKER), format!("{version}\n"))
        .map_err(|error| format!("stamp runtime version: {error}"))?;

    // A directory rename cannot land on a non-empty directory, so the outgoing
    // tree is moved aside first and deleted after. The window in which the scope
    // is absent is two renames wide and only opens on a version change, where the
    // alternative -- writing in place -- would expose a half-written package for
    // the whole copy instead.
    let retired = root.join(format!(".cairn-runtime-retired-{unique}"));
    let _ = std::fs::remove_dir_all(&retired);
    if scope.exists() && std::fs::rename(&scope, &retired).is_err() {
        let _ = std::fs::remove_dir_all(&scope);
    }
    std::fs::rename(&staged_scope, &scope)
        .map_err(|error| format!("install runtime packages {}: {error}", scope.display()))?;
    let _ = std::fs::remove_dir_all(&retired);
    let _ = std::fs::remove_dir_all(&staging);
    Ok(root)
}

fn stamped_with(marker: &Path, version: &str) -> bool {
    std::fs::read_to_string(marker).is_ok_and(|held| held.trim() == version)
}

/// Serializes installs into one shared tree across every process that shares it.
///
/// Slots of a project run concurrently and all install into the same directory,
/// so without this two installers of different versions can interleave their
/// renames: one retires the tree the other just published, and a batch fails on
/// an install that had nothing wrong with it. The kernel drops the lock when its
/// holder exits, so a crashed installer never wedges the next one.
struct InstallLock(#[allow(dead_code)] std::fs::File);

impl InstallLock {
    fn acquire(root: &Path) -> Result<Self, String> {
        let path = root.join(".cairn-runtime.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| format!("open runtime install lock {}: {error}", path.display()))?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match try_lock_exclusive(&file) {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => return Err(format!("lock {}: {error}", path.display())),
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!(
                    "the Cairn runtime install lock at {} was held by another process for 30s",
                    path.display()
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(Self(file))
    }
}

/// Take the OS's exclusive advisory lock without blocking. `Ok(false)` means
/// another holder has it right now.
///
/// Deliberately implemented per platform rather than left to one of them: Cairn
/// runs executors on Windows as well as Unix, and a lock that is a no-op there
/// is worse than no lock at all, because the code above reads as serialized. On
/// both platforms the lock is released when the handle closes, including on an
/// abnormal exit, so a crashed installer never wedges the next one. A platform
/// that is neither fails to compile here on purpose, so the gap is a build error
/// rather than a silently unserialized install.
#[cfg(unix)]
fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::WouldBlock {
        Ok(false)
    } else {
        Err(error)
    }
}

#[cfg(windows)]
fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<bool> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    // Lock the whole conceivable file rather than a byte range: the file carries
    // no content, it is only ever a lock token.
    let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
    let locked = unsafe {
        LockFileEx(
            file.as_raw_handle() as _,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            u32::MAX,
            u32::MAX,
            &mut overlapped,
        )
    };
    if locked != 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    // `LOCKFILE_FAIL_IMMEDIATELY` reports contention as a lock violation rather
    // than as a would-block, and Rust does not map it to a named ErrorKind.
    if error.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32) {
        Ok(false)
    } else {
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(path: &str, data: &[u8]) -> ResidentRuntimeAsset {
        ResidentRuntimeAsset {
            path: path.to_string(),
            data: data.to_vec(),
        }
    }

    /// The case from the field report: a build-slot checkout installs into the
    /// project directory that holds every slot, which is the nearest ancestor
    /// Cairn owns.
    #[test]
    fn a_build_slot_checkout_installs_into_its_project_directory() {
        assert_eq!(
            shared_runtime_parent(Path::new("/home/dev/.cairn/build-slots/axn/slot-91")),
            Some(PathBuf::from("/home/dev/.cairn/build-slots/axn"))
        );
    }

    /// A live checkout in the user's own repository has no Cairn-owned ancestor.
    /// `None` is the answer that makes the caller say so out loud instead of
    /// writing packages into a repository Cairn does not own.
    #[test]
    fn a_live_checkout_has_nowhere_cairn_may_write() {
        assert_eq!(
            shared_runtime_parent(Path::new("/Users/mitch/projects/axn")),
            None
        );
        assert_eq!(shared_runtime_parent(Path::new("/")), None);
    }

    /// `build-slots` holds every project, so installing there would place one
    /// project's runtime above another's checkout. Only a directory strictly
    /// beneath it is a project's own.
    #[test]
    fn the_build_slots_root_is_not_an_install_root() {
        assert_eq!(
            shared_runtime_parent(Path::new("/home/dev/.cairn/build-slots/axn")),
            None
        );
    }

    /// The install is addressed relative to `node_modules` so that the paths
    /// written are exactly the ones a bare-specifier import walks up to find.
    #[test]
    fn assets_install_under_the_node_modules_root_and_stamp_their_version() {
        let temp = tempfile::tempdir().unwrap();
        let assets = vec![asset("@cairn/sdk/package.json", b"{}")];
        let version = assets_version(&assets);

        let root = install_shared_runtime(temp.path(), &assets, &version).unwrap();

        assert_eq!(root, temp.path().join("node_modules"));
        assert_eq!(
            std::fs::read_to_string(root.join("@cairn/sdk/package.json")).unwrap(),
            "{}"
        );
        assert_eq!(
            std::fs::read_to_string(root.join(SCOPE_DIR).join(RUNTIME_MARKER))
                .unwrap()
                .trim(),
            version,
            "the stamp lives inside the directory it describes"
        );
    }

    /// Packages outside the Cairn scope would make the install more than one
    /// directory, and the single swap unit is what keeps the stamp and the tree
    /// inseparable.
    #[test]
    fn an_unscoped_package_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let assets = vec![asset("lodash/package.json", b"{}")];
        let version = assets_version(&assets);
        assert!(install_shared_runtime(temp.path(), &assets, &version).is_err());
    }

    /// The failure this guards is silent and permanent, so it is worth racing
    /// for real. Installers carrying DIFFERENT contents run concurrently against
    /// one shared tree; whichever wins, the stamp must describe the tree that is
    /// actually on disk. A stamp kept beside the tree instead of inside it lets
    /// one installer's marker outlive another's content, and every later run
    /// takes the fast path and resolves against an SDK it was never told about.
    #[test]
    fn racing_installs_leave_the_stamp_agreeing_with_the_tree() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().to_path_buf();

        let racers: Vec<_> = (0..8)
            .map(|n| {
                let parent = parent.clone();
                std::thread::spawn(move || {
                    let assets = vec![asset(
                        "@cairn/sdk/package.json",
                        format!("{{\"build\":{n}}}").as_bytes(),
                    )];
                    let version = assets_version(&assets);
                    for _ in 0..4 {
                        install_shared_runtime(&parent, &assets, &version)
                            .expect("a contended install must not fail");
                    }
                })
            })
            .collect();
        for racer in racers {
            racer.join().unwrap();
        }

        let root = parent.join(NODE_MODULES);
        let stamped = std::fs::read_to_string(root.join(SCOPE_DIR).join(RUNTIME_MARKER)).unwrap();
        let installed = std::fs::read(root.join("@cairn/sdk/package.json")).unwrap();
        assert_eq!(
            stamped.trim(),
            assets_version(&[asset("@cairn/sdk/package.json", &installed)]),
            "the stamp must describe the tree that is actually installed"
        );
    }

    /// Every batch of every slot of a project reaches this call, so the common
    /// path has to be a cheap no-op rather than a rewrite. A sentinel left inside
    /// the installed tree survives a same-version call and is swept by a
    /// different-version one, which is the observable difference between the two.
    #[test]
    fn a_matching_version_is_a_no_op_and_a_new_one_replaces_the_tree() {
        let temp = tempfile::tempdir().unwrap();
        let assets = vec![asset("@cairn/sdk/package.json", b"{}")];
        let version = assets_version(&assets);
        let root = install_shared_runtime(temp.path(), &assets, &version).unwrap();

        let sentinel = root.join(SCOPE_DIR).join("sdk/sentinel");
        std::fs::write(&sentinel, b"kept").unwrap();
        install_shared_runtime(temp.path(), &assets, &version).unwrap();
        assert!(sentinel.exists(), "a same-version install must not rewrite");

        let next = vec![asset("@cairn/sdk/package.json", b"{\"version\":\"2\"}")];
        let next_version = assets_version(&next);
        assert_ne!(version, next_version);
        install_shared_runtime(temp.path(), &next, &next_version).unwrap();
        assert!(!sentinel.exists(), "a new version must replace the tree");
        assert_eq!(
            std::fs::read_to_string(root.join("@cairn/sdk/package.json")).unwrap(),
            "{\"version\":\"2\"}"
        );
    }

    /// The bytes come from a runner, so the path policy is a security boundary:
    /// a runner may say what to install, never where.
    #[test]
    fn runner_supplied_paths_may_not_escape_their_root() {
        for path in [
            "../outside/package.json",
            "/etc/passwd",
            "@cairn/../../escape.json",
            "",
        ] {
            assert!(
                validate_runtime_assets(&[asset(path, b"{}")]).is_err(),
                "path {path:?} must be rejected"
            );
        }
        assert!(validate_runtime_assets(&[asset("@cairn/sdk/package.json", b"{}")]).is_ok());
    }

    /// A duplicate path makes the installed result depend on iteration order, so
    /// it is refused rather than resolved by last-writer-wins.
    #[test]
    fn duplicate_asset_paths_are_refused() {
        let duplicated = vec![
            asset("@cairn/sdk/package.json", b"{}"),
            asset("@cairn/sdk/package.json", b"{\"other\":true}"),
        ];
        assert!(validate_runtime_assets(&duplicated).is_err());
    }

    /// The stamp answers "is what is on disk what I am about to write", so it has
    /// to move with content and stand still under reordering.
    #[test]
    fn the_version_stamp_tracks_content_not_order() {
        let one = asset("@cairn/sdk/package.json", b"{}");
        let two = asset("@cairn/sdk/src/index.ts", b"export const x = 1;");
        assert_eq!(
            assets_version(&[one.clone(), two.clone()]),
            assets_version(&[two, one.clone()])
        );
        assert_ne!(
            assets_version(&[one]),
            assets_version(&[asset("@cairn/sdk/package.json", b"{\"v\":2}")])
        );
    }
}
