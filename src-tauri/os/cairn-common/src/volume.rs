//! How much room the volume holding a path has: the one probe every Cairn
//! process that reclaims disk reads its pressure from.

use std::io;
use std::path::Path;

/// A volume's size and the bytes an unprivileged writer may still use on it,
/// with its inode table where the filesystem has one.
///
/// A filesystem can refuse every create while bytes remain: a tmpfs or ext4
/// sized with a fixed inode table runs out of inodes under many small files.
/// The inode pair is `None` where the filesystem keeps no fixed table (btrfs,
/// ZFS, many network filesystems report zero), which means the volume is not
/// inode-limited, not that it has none left; and on platforms with no such
/// reading at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VolumeSpace {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub total_inodes: Option<u64>,
    pub free_inodes: Option<u64>,
}

/// The capacity of the volume `path` lives on, read through its nearest
/// existing ancestor so a directory that has not been created yet still
/// answers for the volume it will be on.
///
/// Blocking: a `statvfs` on a disk that has stopped completing I/O does not
/// return, so call it off an async runtime's workers.
pub fn space(path: &Path) -> io::Result<VolumeSpace> {
    let existing = path
        .ancestors()
        .find(|candidate| candidate.exists())
        .unwrap_or(path);
    platform_space(existing)
}

#[cfg(unix)]
fn statvfs_value<T: Into<u64>>(value: T) -> u64 {
    value.into()
}

#[cfg(unix)]
fn platform_space(path: &Path) -> io::Result<VolumeSpace> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let value = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "storage path contains NUL"))?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(value.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let stats = unsafe { stats.assume_init() };
    let block = statvfs_value(stats.f_frsize);
    let total_inodes = statvfs_value(stats.f_files);
    let (total_inodes, free_inodes) = if total_inodes == 0 {
        (None, None)
    } else {
        (
            Some(total_inodes),
            Some(statvfs_value(stats.f_favail).min(total_inodes)),
        )
    };
    Ok(VolumeSpace {
        total_bytes: statvfs_value(stats.f_blocks).saturating_mul(block),
        free_bytes: statvfs_value(stats.f_bavail).saturating_mul(block),
        total_inodes,
        free_inodes,
    })
}

#[cfg(windows)]
fn platform_space(path: &Path) -> io::Result<VolumeSpace> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let wide = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut caller_available = 0_u64;
    let mut total = 0_u64;
    if unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut caller_available,
            &mut total,
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(VolumeSpace {
        total_bytes: total,
        free_bytes: caller_available,
        total_inodes: None,
        free_inodes: None,
    })
}

#[cfg(not(any(unix, windows)))]
fn platform_space(_path: &Path) -> io::Result<VolumeSpace> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "volume capacity unavailable",
    ))
}

/// The kind of filesystem `path` lives on (`tmpfs`, `ext4`, `apfs`, ...), for
/// naming a volume to a reader. `None` where the platform does not say or the
/// kind is not one this table knows. Never used to decide anything.
///
/// Blocking, like [`space`].
pub fn filesystem_kind(path: &Path) -> Option<String> {
    let existing = path
        .ancestors()
        .find(|candidate| candidate.exists())
        .unwrap_or(path);
    platform_filesystem_kind(existing)
}

#[cfg(target_os = "linux")]
fn platform_filesystem_kind(path: &Path) -> Option<String> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let value = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    if unsafe { libc::statfs(value.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return None;
    }
    let stats = unsafe { stats.assume_init() };
    // `f_type`'s width and signedness vary by target; every magic is 32 bits.
    #[allow(clippy::unnecessary_cast)]
    let magic = (stats.f_type as u64) & 0xffff_ffff;
    let kind = match magic {
        0x0102_1994 => "tmpfs",
        0xef53 => "ext4",
        0x5846_5342 => "xfs",
        0x9123_683e => "btrfs",
        0x794c_7630 => "overlay",
        0x2fc1_2fc1 => "zfs",
        0xf2f5_2010 => "f2fs",
        0x6969 => "nfs",
        _ => return None,
    };
    Some(kind.to_owned())
}

#[cfg(target_os = "macos")]
fn platform_filesystem_kind(path: &Path) -> Option<String> {
    use std::ffi::{CStr, CString};
    use std::os::unix::ffi::OsStrExt;
    let value = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    if unsafe { libc::statfs(value.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return None;
    }
    let stats = unsafe { stats.assume_init() };
    let name = unsafe { CStr::from_ptr(stats.f_fstypename.as_ptr()) };
    let name = name.to_string_lossy().trim().to_owned();
    (!name.is_empty()).then_some(name)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn platform_filesystem_kind(_path: &Path) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_not_yet_created_answers_for_the_volume_it_will_be_on() {
        let temp = tempfile::tempdir().unwrap();
        let existing = space(temp.path()).unwrap();
        let future = space(&temp.path().join("not/yet/made")).unwrap();
        assert!(existing.total_bytes > 0);
        assert!(existing.free_bytes <= existing.total_bytes);
        assert_eq!(existing.total_bytes, future.total_bytes);
    }

    #[test]
    fn the_inode_pair_is_whole_and_consistent_or_absent() {
        let temp = tempfile::tempdir().unwrap();
        let space = space(temp.path()).unwrap();
        match (space.total_inodes, space.free_inodes) {
            (Some(total), Some(free)) => assert!(free <= total && total > 0, "{space:?}"),
            (None, None) => {}
            partial => panic!("inode reading is half present: {partial:?}"),
        }
    }
}
