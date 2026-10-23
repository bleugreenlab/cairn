//! Copy-on-write cloning of directory trees, and the measures that make its
//! sharing visible.
//!
//! A clone shares every data extent with its source until one side writes.
//! Neither the logical size nor the allocated blocks (`st_blocks`) a file
//! reports says anything about that sharing: on APFS, XFS and btrfs alike each
//! clone reports its full allocation. [`exclusive_bytes`] is the honest
//! per-file figure, the bytes a deletion would actually free.
//!
//! Whether a volume can clone at all is decided by [`probe`] actually cloning a
//! small file there, never by the platform: APFS, XFS with `reflink=1` and
//! btrfs clone; ext4 and tmpfs do not.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Whether a volume clones files by copy-on-write, and which filesystem it is.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum CowSupport {
    Supported { filesystem: String },
    Unsupported { filesystem: String, reason: String },
}

impl CowSupport {
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Supported { .. })
    }

    pub fn filesystem(&self) -> &str {
        match self {
            Self::Supported { filesystem } | Self::Unsupported { filesystem, .. } => filesystem,
        }
    }

    /// The operator-facing reason a volume cannot clone, naming the filesystem
    /// so the line says what to fix: `build volume does not support
    /// copy-on-write clones (ext4)`. `None` when the volume clones.
    pub fn ineligible_reason(&self) -> Option<String> {
        match self {
            Self::Supported { .. } => None,
            Self::Unsupported { filesystem, reason } => Some(format!("{reason} ({filesystem})")),
        }
    }
}

const UNSUPPORTED_REASON: &str = "build volume does not support copy-on-write clones";
const DIFFERENT_VOLUMES_REASON: &str = "source and destination are on different volumes";

fn probe_cache() -> &'static Mutex<HashMap<u64, CowSupport>> {
    static CACHE: OnceLock<Mutex<HashMap<u64, CowSupport>>> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// Whether the volume holding `dir` clones files. `dir` need not exist yet:
/// its nearest existing ancestor is probed, which is where it would be created.
///
/// The probe writes a small file there, clones it, and removes both. The answer
/// is cached per device for the process lifetime; a probe that could not even
/// write its file is not cached, since that says nothing about the volume.
pub fn probe(dir: &Path) -> CowSupport {
    let existing = match nearest_existing_dir(dir) {
        Ok(existing) => existing,
        Err(error) => {
            return CowSupport::Unsupported {
                filesystem: "unknown".into(),
                reason: format!("cannot read the build volume at {}: {error}", dir.display()),
            }
        }
    };
    let device = device_id(&existing).ok();
    if let Some(device) = device {
        if let Some(cached) = probe_cache().lock().unwrap().get(&device) {
            return cached.clone();
        }
    }
    let filesystem = filesystem_name(&existing);
    let verdict = match probe_clone(&existing) {
        Ok(()) => CowSupport::Supported { filesystem },
        Err(ProbeFailure::CannotWrite(error)) => {
            return CowSupport::Unsupported {
                filesystem,
                reason: format!("cannot write a probe file on the build volume: {error}"),
            }
        }
        Err(ProbeFailure::CloneFailed(error)) => {
            tracing::debug!(
                dir = %existing.display(),
                filesystem = %filesystem,
                %error,
                "copy-on-write probe clone failed"
            );
            CowSupport::Unsupported {
                filesystem,
                reason: UNSUPPORTED_REASON.into(),
            }
        }
    };
    if let Some(device) = device {
        probe_cache()
            .lock()
            .unwrap()
            .insert(device, verdict.clone());
    }
    verdict
}

/// Whether `src` can be cloned to `dst`: both must be on one volume (a clone
/// across volumes fails with `EXDEV`), and that volume must clone.
pub fn probe_pair(src: &Path, dst: &Path) -> CowSupport {
    let devices = nearest_existing_dir(src)
        .and_then(|src| device_id(&src))
        .and_then(|src| Ok((src, device_id(&nearest_existing_dir(dst)?)?)));
    match devices {
        Ok((src_device, dst_device)) => pair_verdict(src_device, dst_device, dst, probe),
        Err(error) => CowSupport::Unsupported {
            filesystem: "unknown".into(),
            reason: format!("cannot read the clone source or destination volume: {error}"),
        },
    }
}

fn pair_verdict(
    src_device: u64,
    dst_device: u64,
    dst: &Path,
    probe: impl FnOnce(&Path) -> CowSupport,
) -> CowSupport {
    if src_device != dst_device {
        return CowSupport::Unsupported {
            filesystem: nearest_existing_dir(dst)
                .map(|dir| filesystem_name(&dir))
                .unwrap_or_else(|_| "unknown".into()),
            reason: DIFFERENT_VOLUMES_REASON.into(),
        };
    }
    probe(dst)
}

enum ProbeFailure {
    CannotWrite(io::Error),
    CloneFailed(io::Error),
}

fn probe_clone(dir: &Path) -> Result<(), ProbeFailure> {
    let token = uuid::Uuid::new_v4();
    let source = dir.join(format!(".cairn-cow-probe-{token}"));
    let clone = dir.join(format!(".cairn-cow-probe-{token}.clone"));
    fs::write(&source, b"cairn copy-on-write probe").map_err(ProbeFailure::CannotWrite)?;
    let result = reflink_copy::reflink(&source, &clone);
    let _ = fs::remove_file(&clone);
    let _ = fs::remove_file(&source);
    result.map_err(ProbeFailure::CloneFailed)
}

fn nearest_existing_dir(path: &Path) -> io::Result<PathBuf> {
    let mut candidate = Some(path);
    while let Some(dir) = candidate {
        if dir.is_dir() {
            return Ok(dir.to_path_buf());
        }
        candidate = dir.parent().filter(|parent| !parent.as_os_str().is_empty());
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("no existing directory at or above {}", path.display()),
    ))
}

#[cfg(unix)]
fn device_id(path: &Path) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt;
    Ok(fs::metadata(path)?.dev())
}

/// Windows has no stable device id in `std`; the volume prefix (`C:`, a UNC
/// share) names the volume well enough to keep a cross-volume clone from being
/// attempted.
#[cfg(not(unix))]
fn device_id(path: &Path) -> io::Result<u64> {
    use std::hash::{Hash, Hasher};
    let canonical = path.canonicalize()?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.components().next().hash(&mut hasher);
    Ok(hasher.finish())
}

#[cfg(target_os = "macos")]
fn filesystem_name(path: &Path) -> String {
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return "unknown".into();
    };
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(path.as_ptr(), &mut stat) } != 0 {
        return "unknown".into();
    }
    unsafe { std::ffi::CStr::from_ptr(stat.f_fstypename.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

#[cfg(target_os = "linux")]
fn filesystem_name(path: &Path) -> String {
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return "unknown".into();
    };
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(path.as_ptr(), &mut stat) } != 0 {
        return "unknown".into();
    }
    // `f_type` is signed on some targets; the magic numbers are 32-bit.
    let magic = (stat.f_type as u64) & 0xffff_ffff;
    match magic {
        0xEF53 => "ext4".into(),
        0x5846_5342 => "xfs".into(),
        0x9123_683E => "btrfs".into(),
        0x0102_1994 => "tmpfs".into(),
        0x794C_7630 => "overlayfs".into(),
        0x2FC1_2FC1 => "zfs".into(),
        0xCA45_1A4E => "bcachefs".into(),
        0xF2F5_2010 => "f2fs".into(),
        0x6969 => "nfs".into(),
        0x6573_5546 => "fuse".into(),
        other => format!("unknown(0x{other:x})"),
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn filesystem_name(_path: &Path) -> String {
    "unknown".into()
}

/// What [`clone_tree`] should leave out and how long it may take.
#[derive(Debug, Clone, Default)]
pub struct CloneOptions {
    /// Paths relative to the source root that are not cloned. An excluded
    /// directory is skipped with everything beneath it.
    pub exclude: Vec<PathBuf>,
    /// The walk stops between files once this passes, removes what it staged,
    /// and fails with [`CloneError::DeadlineExceeded`]. A clone that has
    /// already completed is published regardless.
    pub deadline: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloneReport {
    /// Entries the per-file walk cloned. `None` for a single whole-directory
    /// clone (macOS), which does not enumerate.
    pub entries: Option<u64>,
    pub elapsed: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum CloneError {
    #[error("clone destination already exists: {}", .0.display())]
    DestinationExists(PathBuf),
    #[error("clone deadline exceeded after {cloned} entries")]
    DeadlineExceeded { cloned: u64 },
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: io::Error,
    },
}

impl CloneError {
    fn io(context: impl Into<String>, source: io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}

/// Clone the tree at `src` to `dst` by copy-on-write, strictly: there is no
/// byte-copy fallback, so a volume that cannot clone fails here instead of
/// silently deep-copying a multi-gigabyte build tree.
///
/// `dst` must not exist. The tree is cloned into a sibling staging name and
/// published by a no-replace rename, so `dst` is either absent or complete;
/// staging is removed on every error.
///
/// On macOS this is one `clonefile(2)` of the directory. Elsewhere it is a
/// parallel walk that clones each regular file (FICLONE), recreates
/// directories and symlinks, and copies permission bits and nanosecond
/// modification times. Preserving mtimes is load-bearing: Cargo decides
/// freshness by comparing them, and a file clone copies data, not metadata.
pub fn clone_tree(
    src: &Path,
    dst: &Path,
    options: &CloneOptions,
) -> Result<CloneReport, CloneError> {
    clone_tree_staged(src, dst, options, native_clone_into)
}

fn clone_tree_staged(
    src: &Path,
    dst: &Path,
    options: &CloneOptions,
    clone_into: impl FnOnce(&Path, &Path, &CloneOptions) -> Result<Option<u64>, CloneError>,
) -> Result<CloneReport, CloneError> {
    let started = Instant::now();
    if dst.symlink_metadata().is_ok() {
        return Err(CloneError::DestinationExists(dst.to_path_buf()));
    }
    let parent = dst
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = dst.file_name().ok_or_else(|| {
        CloneError::io(
            format!("clone destination has no file name: {}", dst.display()),
            io::ErrorKind::InvalidInput.into(),
        )
    })?;
    fs::create_dir_all(parent).map_err(|e| CloneError::io("create clone parent", e))?;
    let staging = parent.join(format!(
        ".{}.cairn-clone-{}",
        name.to_string_lossy(),
        uuid::Uuid::new_v4()
    ));
    let discard = |error: CloneError| {
        let _ = remove_any(&staging);
        error
    };
    // A clone that completed is published even if it finished past the
    // deadline: deleting a whole cloned tree here would cost far more than the
    // clone did, and the caller decides what a late clone is worth.
    let entries = clone_into(src, &staging, options).map_err(discard)?;
    rename_noreplace(&staging, dst).map_err(|error| {
        discard(if error.kind() == io::ErrorKind::AlreadyExists {
            CloneError::DestinationExists(dst.to_path_buf())
        } else {
            CloneError::io("publish clone", error)
        })
    })?;
    Ok(CloneReport {
        entries,
        elapsed: started.elapsed(),
    })
}

#[cfg(target_os = "macos")]
fn native_clone_into(
    src: &Path,
    staging: &Path,
    options: &CloneOptions,
) -> Result<Option<u64>, CloneError> {
    use std::os::unix::ffi::OsStrExt;
    unsafe extern "C" {
        fn clonefile(src: *const libc::c_char, dst: *const libc::c_char, flags: u32)
            -> libc::c_int;
    }
    if options
        .deadline
        .is_some_and(|deadline| Instant::now() > deadline)
    {
        return Err(CloneError::DeadlineExceeded { cloned: 0 });
    }
    let invalid = |_| CloneError::io("clone path", io::ErrorKind::InvalidInput.into());
    let from = std::ffi::CString::new(src.as_os_str().as_bytes()).map_err(invalid)?;
    let to = std::ffi::CString::new(staging.as_os_str().as_bytes()).map_err(invalid)?;
    if unsafe { clonefile(from.as_ptr(), to.as_ptr(), 0) } != 0 {
        return Err(CloneError::io(
            format!("clonefile {}", src.display()),
            io::Error::last_os_error(),
        ));
    }
    for excluded in &options.exclude {
        match remove_any(&staging.join(excluded)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(CloneError::io("remove excluded clone entry", error)),
        }
    }
    Ok(None)
}

#[cfg(not(target_os = "macos"))]
fn native_clone_into(
    src: &Path,
    staging: &Path,
    options: &CloneOptions,
) -> Result<Option<u64>, CloneError> {
    walk_clone(src, staging, options, &|from, to| {
        reflink_copy::reflink(from, to)
    })
    .map(Some)
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
enum EntryKind {
    Dir,
    File,
    Symlink(PathBuf),
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
struct WalkEntry {
    relative: PathBuf,
    kind: EntryKind,
    metadata: fs::Metadata,
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
const WALK_THREADS: usize = 8;

/// Clone `src` into a new directory `root` one file at a time with
/// `clone_file`, which must create its destination (it never exists yet).
#[cfg_attr(target_os = "macos", allow(dead_code))]
fn walk_clone(
    src: &Path,
    root: &Path,
    options: &CloneOptions,
    clone_file: &(dyn Fn(&Path, &Path) -> io::Result<()> + Sync),
) -> Result<u64, CloneError> {
    let deadline_passed = || {
        options
            .deadline
            .is_some_and(|deadline| Instant::now() > deadline)
    };
    let root_metadata =
        fs::metadata(src).map_err(|e| CloneError::io(format!("read {}", src.display()), e))?;
    if !root_metadata.is_dir() {
        return Err(CloneError::io(
            format!("clone source is not a directory: {}", src.display()),
            io::ErrorKind::InvalidInput.into(),
        ));
    }
    let entries = collect_entries(src, &options.exclude)?;
    fs::create_dir(root).map_err(|e| CloneError::io("create clone root", e))?;
    for entry in &entries {
        if deadline_passed() {
            return Err(CloneError::DeadlineExceeded { cloned: 0 });
        }
        let to = root.join(&entry.relative);
        match &entry.kind {
            EntryKind::Dir => fs::create_dir(&to),
            EntryKind::Symlink(target) => make_symlink(target, &to, &src.join(&entry.relative)),
            EntryKind::File => continue,
        }
        .map_err(|e| CloneError::io(format!("recreate {}", entry.relative.display()), e))?;
    }

    let files: Vec<&WalkEntry> = entries
        .iter()
        .filter(|entry| matches!(entry.kind, EntryKind::File))
        .collect();
    let next = AtomicUsize::new(0);
    let cloned = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let failure: Mutex<Option<CloneError>> = Mutex::new(None);
    let threads = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .clamp(1, WALK_THREADS)
        .min(files.len().max(1));
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                let fail = |error: CloneError| {
                    stop.store(true, Ordering::SeqCst);
                    failure.lock().unwrap().get_or_insert(error);
                };
                while !stop.load(Ordering::SeqCst) {
                    let index = next.fetch_add(1, Ordering::SeqCst);
                    let Some(entry) = files.get(index) else {
                        return;
                    };
                    if deadline_passed() {
                        return fail(CloneError::DeadlineExceeded {
                            cloned: cloned.load(Ordering::SeqCst),
                        });
                    }
                    let from = src.join(&entry.relative);
                    let to = root.join(&entry.relative);
                    let result = clone_file(&from, &to)
                        .and_then(|()| copy_metadata(&to, &entry.metadata, false));
                    if let Err(error) = result {
                        return fail(CloneError::io(
                            format!("clone {}", entry.relative.display()),
                            error,
                        ));
                    }
                    cloned.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
    });
    if let Some(error) = failure.into_inner().unwrap() {
        return Err(error);
    }

    // Directory metadata last and deepest first: creating entries moves a
    // directory's mtime, and a read-only directory admits no new entries.
    for entry in entries.iter().rev() {
        let to = root.join(&entry.relative);
        let result = match entry.kind {
            EntryKind::Dir => copy_metadata(&to, &entry.metadata, false),
            EntryKind::Symlink(_) => copy_metadata(&to, &entry.metadata, true),
            EntryKind::File => continue,
        };
        result.map_err(|e| CloneError::io(format!("stamp {}", entry.relative.display()), e))?;
    }
    copy_metadata(root, &root_metadata, false)
        .map_err(|e| CloneError::io("stamp clone root", e))?;
    Ok(entries.len() as u64)
}

/// Every entry beneath `src` (not following symlinks), parents before
/// children. Sockets and FIFOs are skipped: a clone cannot carry them.
#[cfg_attr(target_os = "macos", allow(dead_code))]
fn collect_entries(src: &Path, exclude: &[PathBuf]) -> Result<Vec<WalkEntry>, CloneError> {
    let mut entries = Vec::new();
    let mut pending = vec![PathBuf::new()];
    while let Some(relative_dir) = pending.pop() {
        let dir = src.join(&relative_dir);
        let read =
            fs::read_dir(&dir).map_err(|e| CloneError::io(format!("read {}", dir.display()), e))?;
        for item in read {
            let item = item.map_err(|e| CloneError::io(format!("read {}", dir.display()), e))?;
            let relative = relative_dir.join(item.file_name());
            if exclude.iter().any(|excluded| excluded == &relative) {
                continue;
            }
            let metadata = item
                .path()
                .symlink_metadata()
                .map_err(|e| CloneError::io(format!("stat {}", relative.display()), e))?;
            let file_type = metadata.file_type();
            let kind = if file_type.is_symlink() {
                let target = fs::read_link(item.path())
                    .map_err(|e| CloneError::io(format!("readlink {}", relative.display()), e))?;
                EntryKind::Symlink(target)
            } else if file_type.is_dir() {
                pending.push(relative.clone());
                EntryKind::Dir
            } else if file_type.is_file() {
                EntryKind::File
            } else {
                continue;
            };
            entries.push(WalkEntry {
                relative,
                kind,
                metadata,
            });
        }
    }
    Ok(entries)
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
#[cfg(unix)]
fn make_symlink(target: &Path, link: &Path, _original: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
#[cfg(windows)]
fn make_symlink(target: &Path, link: &Path, original: &Path) -> io::Result<()> {
    if fs::metadata(original).is_ok_and(|metadata| metadata.is_dir()) {
        std::os::windows::fs::symlink_dir(target, link)
    } else {
        std::os::windows::fs::symlink_file(target, link)
    }
}

/// Stamp `path` with `metadata`'s permission bits and access and modification
/// times, to the nanosecond.
#[cfg_attr(target_os = "macos", allow(dead_code))]
#[cfg(unix)]
fn copy_metadata(path: &Path, metadata: &fs::Metadata, symlink: bool) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;
    if !symlink {
        fs::set_permissions(path, metadata.permissions())?;
    }
    let times = [
        libc::timespec {
            tv_sec: metadata.atime() as libc::time_t,
            tv_nsec: metadata.atime_nsec() as _,
        },
        libc::timespec {
            tv_sec: metadata.mtime() as libc::time_t,
            tv_nsec: metadata.mtime_nsec() as _,
        },
    ];
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let flags = if symlink {
        libc::AT_SYMLINK_NOFOLLOW
    } else {
        0
    };
    if unsafe { libc::utimensat(libc::AT_FDCWD, path.as_ptr(), times.as_ptr(), flags) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
#[cfg(not(unix))]
fn copy_metadata(path: &Path, metadata: &fs::Metadata, symlink: bool) -> io::Result<()> {
    if symlink {
        return Ok(());
    }
    if metadata.is_file() {
        let file = fs::OpenOptions::new().write(true).open(path)?;
        file.set_times(
            fs::FileTimes::new()
                .set_accessed(metadata.accessed()?)
                .set_modified(metadata.modified()?),
        )?;
    }
    fs::set_permissions(path, metadata.permissions())
}

fn remove_any(path: &Path) -> io::Result<()> {
    let metadata = path.symlink_metadata()?;
    if metadata.is_dir() {
        make_tree_writable(path);
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

/// A clone carries its source's permission bits, including read-only
/// directories that would refuse the removal of their entries.
#[cfg(unix)]
fn make_tree_writable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut pending = vec![path.to_path_buf()];
    while let Some(dir) = pending.pop() {
        if let Ok(metadata) = dir.symlink_metadata() {
            if metadata.is_dir() && metadata.permissions().mode() & 0o200 == 0 {
                let mut permissions = metadata.permissions();
                permissions.set_mode(permissions.mode() | 0o700);
                let _ = fs::set_permissions(&dir, permissions);
            }
        }
        for entry in fs::read_dir(&dir).into_iter().flatten().flatten() {
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                pending.push(entry.path());
            }
        }
    }
}

#[cfg(not(unix))]
fn make_tree_writable(_path: &Path) {}

#[cfg(target_os = "macos")]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    const RENAME_EXCL: u32 = 0x0000_0004;
    let from = std::ffi::CString::new(from.as_os_str().as_bytes())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let to = std::ffi::CString::new(to.as_os_str().as_bytes())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    if unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), RENAME_EXCL) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let from_c = std::ffi::CString::new(from.as_os_str().as_bytes())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let to_c = std::ffi::CString::new(to.as_os_str().as_bytes())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            from_c.as_ptr(),
            libc::AT_FDCWD,
            to_c.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        // A kernel or filesystem without RENAME_NOREPLACE.
        Some(libc::ENOSYS) | Some(libc::EINVAL) => rename_checked(from, to),
        _ => Err(error),
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    rename_checked(from, to)
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
fn rename_checked(from: &Path, to: &Path) -> io::Result<()> {
    if to.symlink_metadata().is_ok() {
        return Err(io::ErrorKind::AlreadyExists.into());
    }
    fs::rename(from, to)
}

/// The blocks a file has allocated: `st_blocks` on Unix, which counts a
/// clone's shared extents in full; the logical length elsewhere.
pub fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        metadata.len()
    }
}

/// The bytes of the regular file at `path` that no other file shares: what
/// deleting it alone would free.
///
/// Linux reads the FIEMAP extent map and sums the extents not flagged shared
/// (XFS and btrfs both flag them). macOS reads APFS's private-size attribute.
/// Anywhere the measure is unavailable, and on any error, this is
/// [`allocated_bytes`], which overcounts a clone but is never below the truth
/// for an unshared file.
pub fn exclusive_bytes(path: &Path, metadata: &fs::Metadata) -> u64 {
    let allocated = allocated_bytes(metadata);
    if !metadata.is_file() {
        return allocated;
    }
    match platform_exclusive_bytes(path) {
        // Compressed extents report their uncompressed length; the allocation
        // bounds what the file can free.
        Ok(Some(exclusive)) => exclusive.min(allocated),
        Ok(None) | Err(_) => allocated,
    }
}

#[cfg(target_os = "linux")]
fn platform_exclusive_bytes(path: &Path) -> io::Result<Option<u64>> {
    use std::os::unix::io::AsRawFd;

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct FiemapExtent {
        fe_logical: u64,
        fe_physical: u64,
        fe_length: u64,
        fe_reserved64: [u64; 2],
        fe_flags: u32,
        fe_reserved: [u32; 3],
    }
    const EXTENTS: usize = 64;
    #[repr(C)]
    struct Fiemap {
        fm_start: u64,
        fm_length: u64,
        fm_flags: u32,
        fm_mapped_extents: u32,
        fm_extent_count: u32,
        fm_reserved: u32,
        fm_extents: [FiemapExtent; EXTENTS],
    }
    // _IOWR('f', 11, struct fiemap), where the header is 32 bytes.
    const FS_IOC_FIEMAP: u64 = 0xC020_660B;
    const FIEMAP_EXTENT_LAST: u32 = 0x0000_0001;
    const FIEMAP_EXTENT_SHARED: u32 = 0x0000_2000;

    let file = fs::File::open(path)?;
    let mut start = 0u64;
    let mut exclusive = 0u64;
    loop {
        let mut request: Fiemap = unsafe { std::mem::zeroed() };
        request.fm_start = start;
        request.fm_length = u64::MAX - start;
        request.fm_extent_count = EXTENTS as u32;
        if unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FIEMAP as _, &mut request) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let mapped = (request.fm_mapped_extents as usize).min(EXTENTS);
        if mapped == 0 {
            return Ok(Some(exclusive));
        }
        let mut last = false;
        for extent in &request.fm_extents[..mapped] {
            if extent.fe_flags & FIEMAP_EXTENT_SHARED == 0 {
                exclusive = exclusive.saturating_add(extent.fe_length);
            }
            last |= extent.fe_flags & FIEMAP_EXTENT_LAST != 0;
            start = extent.fe_logical.saturating_add(extent.fe_length);
        }
        if last || start == u64::MAX {
            return Ok(Some(exclusive));
        }
    }
}

#[cfg(target_os = "macos")]
fn platform_exclusive_bytes(path: &Path) -> io::Result<Option<u64>> {
    use std::os::unix::ffi::OsStrExt;
    const ATTR_BIT_MAP_COUNT: u16 = 5;
    const FSOPT_NOFOLLOW: u32 = 0x0000_0001;
    const FSOPT_ATTR_CMN_EXTENDED: u32 = 0x0000_0020;

    #[repr(C, packed(4))]
    struct PrivateSize {
        length: u32,
        private_size: libc::off_t,
    }
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let mut request = libc::attrlist {
        bitmapcount: ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: 0,
        volattr: 0,
        dirattr: 0,
        fileattr: 0,
        // Extended common attributes ride in the fork field.
        forkattr: libc::ATTR_CMNEXT_PRIVATESIZE,
    };
    let mut reply = PrivateSize {
        length: 0,
        private_size: 0,
    };
    let result = unsafe {
        libc::getattrlist(
            path.as_ptr(),
            (&mut request as *mut libc::attrlist).cast(),
            (&mut reply as *mut PrivateSize).cast(),
            std::mem::size_of::<PrivateSize>(),
            FSOPT_NOFOLLOW | FSOPT_ATTR_CMN_EXTENDED,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // A volume without the attribute (HFS+) returns only the length word.
    if (reply.length as usize) < std::mem::size_of::<PrivateSize>() {
        return Ok(None);
    }
    let private_size = reply.private_size;
    Ok(Some(private_size.max(0) as u64))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn platform_exclusive_bytes(_path: &Path) -> io::Result<Option<u64>> {
    Ok(None)
}

/// A digest of the tree's metadata: relative path, type, length, and
/// nanosecond modification and change times of every entry beneath `root`,
/// except the `exclude`d relative paths and everything beneath them.
///
/// Two equal fingerprints taken around a clone prove no writer touched the
/// tree in between: any write moves ctime, including a metadata-only one.
pub fn tree_fingerprint(root: &Path, exclude: &[PathBuf]) -> io::Result<String> {
    let mut digest = Sha256::new();
    let mut pending = vec![PathBuf::new()];
    while let Some(relative_dir) = pending.pop() {
        let mut items = fs::read_dir(root.join(&relative_dir))?
            .map(|item| item.map(|item| item.file_name()))
            .collect::<io::Result<Vec<_>>>()?;
        items.sort();
        for name in items {
            let relative = relative_dir.join(&name);
            if exclude.iter().any(|excluded| excluded == &relative) {
                continue;
            }
            let metadata = root.join(&relative).symlink_metadata()?;
            let file_type = metadata.file_type();
            if file_type.is_dir() {
                pending.push(relative.clone());
            }
            digest.update(relative.to_string_lossy().as_bytes());
            digest.update([0]);
            digest.update([u8::from(file_type.is_dir()) | (u8::from(file_type.is_symlink()) << 1)]);
            digest.update(metadata.len().to_le_bytes());
            let (mtime, ctime) = metadata_times_ns(&metadata);
            digest.update(mtime.to_le_bytes());
            digest.update(ctime.to_le_bytes());
        }
    }
    Ok(hex::encode(digest.finalize()))
}

#[cfg(unix)]
fn metadata_times_ns(metadata: &fs::Metadata) -> (i128, i128) {
    use std::os::unix::fs::MetadataExt;
    let ns = |seconds: i64, nanos: i64| i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    (
        ns(metadata.mtime(), metadata.mtime_nsec()),
        ns(metadata.ctime(), metadata.ctime_nsec()),
    )
}

#[cfg(not(unix))]
fn metadata_times_ns(metadata: &fs::Metadata) -> (i128, i128) {
    let ns = |time: io::Result<std::time::SystemTime>| {
        time.ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |elapsed| elapsed.as_nanos() as i128)
    };
    (ns(metadata.modified()), ns(metadata.created()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn byte_copy(from: &Path, to: &Path) -> io::Result<()> {
        if to.symlink_metadata().is_ok() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        fs::copy(from, to).map(|_| ())
    }

    fn walk_with(
        cloner: &'static (dyn Fn(&Path, &Path) -> io::Result<()> + Sync),
    ) -> impl FnOnce(&Path, &Path, &CloneOptions) -> Result<Option<u64>, CloneError> {
        move |src, staging, options| walk_clone(src, staging, options, cloner).map(Some)
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[cfg(unix)]
    #[test]
    fn walk_preserves_content_permissions_nanosecond_mtimes_and_symlinks() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("src");
        fs::create_dir_all(src.join("debug/deps")).unwrap();
        fs::create_dir_all(src.join(".git-like")).unwrap();
        fs::write(src.join("debug/deps/lib.rlib"), b"artifact").unwrap();
        fs::write(src.join("run.sh"), b"#!/bin/sh\n").unwrap();
        fs::set_permissions(src.join("run.sh"), fs::Permissions::from_mode(0o751)).unwrap();
        fs::write(src.join(".git"), b"gitdir: elsewhere").unwrap();
        std::os::unix::fs::symlink("debug/deps/lib.rlib", src.join("link")).unwrap();
        let stamped = fs::File::options()
            .write(true)
            .open(src.join("debug/deps/lib.rlib"))
            .unwrap();
        let mtime = std::time::UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
        stamped
            .set_times(fs::FileTimes::new().set_modified(mtime))
            .unwrap();
        drop(stamped);

        let dst = temp.path().join("dst");
        let options = CloneOptions {
            exclude: vec![PathBuf::from(".git")],
            deadline: None,
        };
        let report = clone_tree_staged(&src, &dst, &options, walk_with(&byte_copy)).unwrap();

        assert_eq!(report.entries, Some(6));
        assert_eq!(
            fs::read(dst.join("debug/deps/lib.rlib")).unwrap(),
            b"artifact"
        );
        let cloned = fs::metadata(dst.join("debug/deps/lib.rlib")).unwrap();
        assert_eq!(cloned.mtime(), 1_700_000_000);
        assert_eq!(cloned.mtime_nsec(), 123_456_789);
        assert_eq!(
            fs::metadata(dst.join("run.sh"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o751
        );
        assert_eq!(
            fs::read_link(dst.join("link")).unwrap(),
            PathBuf::from("debug/deps/lib.rlib")
        );
        assert!(!dst.join(".git").exists(), "excluded entry was cloned");
        assert_eq!(names(temp.path()), vec!["dst", "src"]);
    }

    #[test]
    fn refuses_an_existing_destination_without_touching_it() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("src");
        let dst = temp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("new"), b"new").unwrap();
        fs::create_dir_all(&dst).unwrap();
        fs::write(dst.join("existing"), b"existing").unwrap();

        let error = clone_tree_staged(&src, &dst, &CloneOptions::default(), walk_with(&byte_copy))
            .unwrap_err();

        assert!(matches!(error, CloneError::DestinationExists(_)));
        assert_eq!(names(&dst), vec!["existing"]);
        assert_eq!(names(temp.path()), vec!["dst", "src"]);
    }

    #[test]
    fn removes_staging_after_a_mid_walk_error() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("src");
        fs::create_dir_all(src.join("a/b")).unwrap();
        for index in 0..32 {
            fs::write(src.join(format!("a/b/file-{index}")), b"x").unwrap();
        }
        fs::write(src.join("a/poison"), b"x").unwrap();
        let dst = temp.path().join("dst");

        fn poisoned(from: &Path, to: &Path) -> io::Result<()> {
            if from.ends_with("poison") {
                return Err(io::Error::other("injected clone failure"));
            }
            byte_copy(from, to)
        }
        let error = clone_tree_staged(&src, &dst, &CloneOptions::default(), walk_with(&poisoned))
            .unwrap_err();

        assert!(
            error.to_string().contains("injected clone failure"),
            "{error}"
        );
        assert_eq!(names(temp.path()), vec!["src"]);
    }

    #[test]
    fn aborts_at_the_deadline_and_leaves_no_staging() {
        let temp = tempfile::tempdir().unwrap();
        let src = temp.path().join("src");
        fs::create_dir_all(&src).unwrap();
        for index in 0..64 {
            fs::write(src.join(format!("file-{index}")), b"x").unwrap();
        }
        let dst = temp.path().join("dst");
        fn slow(from: &Path, to: &Path) -> io::Result<()> {
            std::thread::sleep(Duration::from_millis(20));
            byte_copy(from, to)
        }
        let options = CloneOptions {
            exclude: Vec::new(),
            deadline: Some(Instant::now() + Duration::from_millis(50)),
        };

        let error = clone_tree_staged(&src, &dst, &options, walk_with(&slow)).unwrap_err();

        assert!(
            matches!(error, CloneError::DeadlineExceeded { .. }),
            "{error}"
        );
        assert_eq!(names(temp.path()), vec!["src"]);
    }

    #[test]
    fn pair_on_different_devices_is_refused_without_probing() {
        let temp = tempfile::tempdir().unwrap();
        let verdict = pair_verdict(1, 2, temp.path(), |_| panic!("must not probe"));
        assert!(!verdict.is_supported());
        assert!(verdict
            .ineligible_reason()
            .unwrap()
            .starts_with("source and destination are on different volumes ("));
        let same = pair_verdict(3, 3, temp.path(), |_| CowSupport::Supported {
            filesystem: "xfs".into(),
        });
        assert!(same.is_supported());
    }

    #[test]
    fn fingerprint_moves_with_any_write_and_ignores_exclusions() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir_all(temp.path().join("scratch")).unwrap();
        fs::write(temp.path().join("file"), b"one").unwrap();
        let exclude = vec![PathBuf::from("scratch")];
        let before = tree_fingerprint(temp.path(), &exclude).unwrap();
        assert_eq!(before, tree_fingerprint(temp.path(), &exclude).unwrap());

        fs::write(temp.path().join("scratch/noise"), b"ignored").unwrap();
        assert_eq!(before, tree_fingerprint(temp.path(), &exclude).unwrap());

        fs::write(temp.path().join("file"), b"two").unwrap();
        assert_ne!(before, tree_fingerprint(temp.path(), &exclude).unwrap());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn apfs_clone_round_trips_and_shares_until_written() {
        use std::io::Write;
        let temp = tempfile::tempdir().unwrap();
        assert!(
            probe(temp.path()).is_supported(),
            "{:?}",
            probe(temp.path())
        );
        let src = temp.path().join("src");
        fs::create_dir_all(src.join("deps")).unwrap();
        let payload = vec![7u8; 4 * 1024 * 1024];
        let mut file = fs::File::create(src.join("deps/big.rlib")).unwrap();
        file.write_all(&payload).unwrap();
        file.sync_all().unwrap();
        drop(file);
        fs::write(src.join(".git"), b"gitdir: elsewhere").unwrap();

        let dst = temp.path().join("dst");
        let options = CloneOptions {
            exclude: vec![PathBuf::from(".git")],
            deadline: None,
        };
        clone_tree(&src, &dst, &options).unwrap();
        assert_eq!(fs::read(dst.join("deps/big.rlib")).unwrap(), payload);
        assert!(!dst.join(".git").exists());
        assert_eq!(names(temp.path()), vec!["dst", "src"]);

        let clone = dst.join("deps/big.rlib");
        let shared = exclusive_bytes(&clone, &fs::metadata(&clone).unwrap());
        assert!(
            shared < 64 * 1024,
            "a fresh clone should own almost nothing, owned {shared}"
        );
        assert!(allocated_bytes(&fs::metadata(&clone).unwrap()) >= payload.len() as u64);

        let mut file = fs::File::create(&clone).unwrap();
        file.write_all(&vec![9u8; payload.len()]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let owned = exclusive_bytes(&clone, &fs::metadata(&clone).unwrap());
        assert!(
            owned >= payload.len() as u64,
            "an overwritten clone owns its blocks, owned {owned}"
        );
    }
}
