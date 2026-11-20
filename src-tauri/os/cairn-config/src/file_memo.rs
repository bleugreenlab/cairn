//! Values derived from config files, kept until a file they were read from
//! changes.
//!
//! Config files are edited outside this process as well as through it (by
//! hand, by `git checkout`, by another Cairn writer), so a cache of something
//! derived from one cannot rely on a save hook. It compares what one `stat`
//! says now with what it said when the value was derived, which is cheap next
//! to reading and YAML-parsing the file again.

use std::collections::HashMap;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

/// How old a file's modification time must be before its stamp is trusted to
/// mean the file has stopped changing.
///
/// A filesystem whose timestamps tick coarsely (FAT's two seconds, HFS+'s one,
/// a Linux coarse clock's few milliseconds) gives two rewrites of the same
/// length inside one tick the same stamp. A value read from a file modified
/// that recently is served but not kept, which is git's "racily clean" rule.
pub const STAMP_SETTLE: Duration = Duration::from_secs(2);

/// Enough of a file's metadata to tell whether it has been rewritten or
/// replaced since it was stamped.
///
/// Length and modification time catch an ordinary edit. On Unix the device,
/// inode and change time also catch an atomic replacement and a write that
/// restores an older modification time, since the change time cannot be set
/// back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStamp {
    len: u64,
    modified: Option<SystemTime>,
    #[cfg(unix)]
    identity: (u64, u64, i64, i64),
}

impl FileStamp {
    /// `path`'s stamp, or `None` when it cannot be stat'ed (absent, or
    /// unreadable to this process).
    pub fn of(path: &Path) -> Option<Self> {
        let metadata = std::fs::metadata(path).ok()?;
        Some(Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            identity: {
                use std::os::unix::fs::MetadataExt;
                (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.ctime(),
                    metadata.ctime_nsec(),
                )
            },
        })
    }

    /// Whether the file had stopped changing by `read_at`: it was last
    /// modified at least [`STAMP_SETTLE`] before then.
    fn settled(&self, read_at: SystemTime) -> bool {
        self.modified
            .and_then(|modified| modified.checked_add(STAMP_SETTLE))
            .is_some_and(|settled_at| settled_at <= read_at)
    }
}

/// Values derived from files, each kept under its key until one of the files
/// it was derived from changes: appears, disappears, or is rewritten or
/// replaced.
pub struct FileMemo<K, V> {
    entries: Mutex<HashMap<K, Kept<V>>>,
    fills: AtomicU64,
}

struct Kept<V> {
    stamps: Vec<(PathBuf, Option<FileStamp>)>,
    value: V,
}

impl<K, V> Default for FileMemo<K, V> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            fills: AtomicU64::new(0),
        }
    }
}

impl<K: Eq + Hash, V: Clone> FileMemo<K, V> {
    /// `key`'s value as `fill` derives it from `files`, filled again only when
    /// one of them has changed since the value was kept. A failed fill keeps
    /// nothing, so the next call tries again.
    pub fn get<E>(
        &self,
        key: K,
        files: &[PathBuf],
        fill: impl FnOnce() -> Result<V, E>,
    ) -> Result<V, E> {
        let read_at = SystemTime::now();
        // Stamped before `fill` reads anything, so a change that lands while it
        // runs leaves a stale stamp and shows on the next call.
        let stamps: Vec<_> = files
            .iter()
            .map(|path| (path.clone(), FileStamp::of(path)))
            .collect();
        if let Some(kept) = self.lock().get(&key) {
            if kept.stamps == stamps {
                return Ok(kept.value.clone());
            }
        }
        self.fills.fetch_add(1, Ordering::Relaxed);
        let value = fill()?;
        let settled = stamps
            .iter()
            .all(|(_, stamp)| stamp.as_ref().is_none_or(|stamp| stamp.settled(read_at)));
        if settled {
            self.lock().insert(
                key,
                Kept {
                    stamps,
                    value: value.clone(),
                },
            );
        }
        Ok(value)
    }

    /// How many values have been derived afresh rather than served kept.
    pub fn fills(&self) -> u64 {
        self.fills.load(Ordering::Relaxed)
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<K, Kept<V>>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settle(path: &Path) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(3600))
            .unwrap();
    }

    #[test]
    fn a_value_is_kept_until_a_file_it_came_from_changes() {
        let dir = tempfile::tempdir().unwrap();
        let present = dir.path().join("present.yaml");
        let absent = dir.path().join("absent.yaml");
        std::fs::write(&present, "a").unwrap();
        settle(&present);
        let memo = FileMemo::<(), String>::default();
        let files = [present.clone(), absent.clone()];
        let read = || {
            memo.get((), &files, || {
                Ok::<_, ()>(
                    std::fs::read_to_string(&present).unwrap()
                        + &std::fs::read_to_string(&absent).unwrap_or_default(),
                )
            })
            .unwrap()
        };

        assert_eq!(read(), "a");
        assert_eq!(read(), "a");
        assert_eq!(memo.fills(), 1, "an unchanged file is not read again");

        // Freshly written, the file is read on every call until it settles.
        std::fs::write(&present, "b").unwrap();
        assert_eq!(read(), "b");
        assert_eq!(read(), "b");
        assert_eq!(memo.fills(), 3);
        settle(&present);
        assert_eq!(read(), "b");
        assert_eq!(read(), "b");
        assert_eq!(memo.fills(), 4);

        // A file that appears is a change too.
        std::fs::write(&absent, "c").unwrap();
        settle(&absent);
        assert_eq!(read(), "bc");
        assert_eq!(memo.fills(), 5);
    }
}
