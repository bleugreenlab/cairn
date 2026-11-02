//! How much room the volume holding a path has: the one probe every Cairn
//! process that reclaims disk reads its pressure from.

use std::io;
use std::path::Path;

/// A volume's size and the bytes an unprivileged writer may still use on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VolumeSpace {
    pub total_bytes: u64,
    pub free_bytes: u64,
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
    Ok(VolumeSpace {
        total_bytes: statvfs_value(stats.f_blocks).saturating_mul(block),
        free_bytes: statvfs_value(stats.f_bavail).saturating_mul(block),
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
    })
}

#[cfg(not(any(unix, windows)))]
fn platform_space(_path: &Path) -> io::Result<VolumeSpace> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "volume capacity unavailable",
    ))
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
}
