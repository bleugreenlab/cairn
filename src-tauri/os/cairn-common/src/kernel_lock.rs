//! Exclusive kernel advisory locks on files, and the note a holder leaves in one.
//!
//! The kernel releases these locks when the holding handle closes, including on
//! an abnormal exit, so a contended attempt always means a live holder and never
//! a crashed one. That is what makes them usable as fences between processes
//! that never otherwise talk: one runtime installer and the next, or an outgoing
//! executor generation and its successor.

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Take the OS's exclusive advisory lock without blocking. `Ok(false)` means
/// another holder has it right now.
///
/// Deliberately implemented per platform rather than left to one of them: Cairn
/// runs executors on Windows as well as Unix, and a lock that is a no-op there
/// is worse than no lock at all, because the code above it reads as serialized.
/// A platform that is neither fails to compile here on purpose, so the gap is a
/// build error rather than a silently unserialized fence.
#[cfg(unix)]
pub fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<bool> {
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
pub fn try_lock_exclusive(file: &std::fs::File) -> std::io::Result<bool> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    // Lock the whole conceivable file rather than a byte range: what the file
    // carries is a diagnostic note, and the lock is the token.
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

/// What a kernel-held lock file records about the process holding it.
///
/// A contended acquisition is positive evidence that a *live* process holds the
/// file, never that a crashed owner left a claim behind. This record exists so
/// the contention can name that process, rather than reporting the
/// unfalsifiable "owned by another runner" that sent CAIRN-3420 hunting for a
/// corpse to evict. It never authorizes anything: the record is written just
/// after the lock is taken, so a contender reading inside that window sees the
/// previous holder, and a pid alone does not prove which process it names.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockHolderRecord {
    pub pid: u32,
    pub acquired_at_unix_ms: u64,
}

/// Stamp the calling process's identity into the lock file it now owns.
///
/// Best effort by construction: a lock that could not be annotated is still
/// perfectly held, and the only thing lost is a phrase in someone else's log
/// line.
pub fn record_lock_holder(mut file: &std::fs::File, path: &Path) {
    let Ok(encoded) = serde_json::to_vec(&LockHolderRecord {
        pid: std::process::id(),
        acquired_at_unix_ms: unix_time_ms(),
    }) else {
        return;
    };
    let stamped = file
        .set_len(0)
        .and_then(|()| file.seek(SeekFrom::Start(0)).map(|_| ()))
        .and_then(|()| file.write_all(&encoded))
        .and_then(|()| file.flush());
    if let Err(error) = stamped {
        tracing::debug!(path = %path.display(), %error, "could not record kernel lock holder");
    }
}

/// Whatever the lock file at `path` currently claims about its holder. Absent
/// or unreadable degrades to `None`, never to an error, because nothing may
/// decide on it.
pub fn read_lock_holder(path: &Path) -> Option<LockHolderRecord> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

pub(crate) fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::*;

    #[test]
    fn a_second_handle_is_refused_until_the_first_closes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("fence.lock");
        let open = || {
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)
                .unwrap()
        };
        let first = open();
        assert!(try_lock_exclusive(&first).unwrap());
        record_lock_holder(&first, &path);
        let second = open();
        assert!(!try_lock_exclusive(&second).unwrap());
        // Windows refuses reads of a locked range, so the note is only legible
        // to a contender on unix.
        #[cfg(unix)]
        assert_eq!(
            read_lock_holder(&path).map(|record| record.pid),
            Some(std::process::id())
        );
        drop(first);
        assert!(try_lock_exclusive(&second).unwrap());
    }
}
