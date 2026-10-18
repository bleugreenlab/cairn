//! Proving a lock file abandoned before anything removes it.
//!
//! Git and jj guard every index and ref write with a `<name>.lock` file, created
//! exclusively and renamed into place when the write commits. A crashed writer
//! leaves its lock behind, and every later writer refuses to start until someone
//! removes it. Removing it is safe only when it really is abandoned: deleting a
//! live writer's lock lets a second writer start beside it, and that is how a
//! ref store or an index gets corrupted.
//!
//! So a removal is earned by a proof with two halves. The lock has outlived
//! [`LOCK_WAIT`], longer than any legitimate write holds one, AND `lsof` reports
//! no live process holding it open. The proof names the exact file it examined
//! by `(dev, ino)`, and [`remove_proven`] re-reads that identity immediately
//! before unlinking, refusing a replacement a live writer minted in the window
//! between the proof and the removal.
//!
//! This module is the one copy of that sequence. The packed-refs patience in
//! `cairn-vcs` and the executor's claim-time slot integrity both build on it;
//! the identity revalidation already needed one review correction, and a second
//! copy would drift on exactly the step that decides whether removal is safe.

use std::path::Path;
#[cfg(unix)]
use std::process::Stdio;
use std::time::Duration;

/// How long a lock must stand before it can be proven abandoned. Longer than
/// any legitimate index or `packed-refs` rewrite holds its lock, and far shorter
/// than the 600s managed-store lease several writers hold while they write.
pub const LOCK_WAIT: Duration = Duration::from_secs(30);

/// Where `lsof` is looked for. It is a system tool, so the system directories
/// come first and are always present, whatever PATH this process inherited: a
/// service manager's minimal environment must not turn "who holds this lock"
/// into "cannot tell" on every machine.
#[cfg(unix)]
const LSOF_SEARCH_PATH: &str = "/usr/sbin:/usr/bin:/sbin:/bin";

/// A live process `lsof` reports holding a lock open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockHolder {
    pub pid: String,
    pub command: String,
}

/// The exact file a proof examined, as opposed to whatever later stands at its
/// path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockIdentity {
    dev: u64,
    ino: u64,
}

/// Who holds `lock` open right now, according to `lsof`: `None` when nobody
/// does. An error means the question could not be answered, which is never
/// read as "nobody".
#[cfg(unix)]
pub fn lock_holder(lock: &Path) -> Result<Option<LockHolder>, String> {
    let inherited = std::env::var("PATH").unwrap_or_default();
    let search_path = if inherited.is_empty() {
        LSOF_SEARCH_PATH.to_string()
    } else {
        format!("{LSOF_SEARCH_PATH}:{inherited}")
    };
    let output = crate::toolchain_path::command_on_path("lsof", &search_path)
        .args(["-F", "pc", "--"])
        .arg(lock)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|error| format!("ask lsof who holds {}: {error}", lock.display()))?;
    match output.status.code() {
        Some(0) => Ok(Some(parse_lsof_holder(&String::from_utf8_lossy(
            &output.stdout,
        )))),
        Some(1) => Ok(None),
        code => Err(format!(
            "ask lsof who holds {}: unexpected exit {code:?}",
            lock.display()
        )),
    }
}

#[cfg(not(unix))]
pub fn lock_holder(_lock: &Path) -> Result<Option<LockHolder>, String> {
    Err("no lsof on this platform to prove the lock unheld".to_string())
}

/// The first process in `lsof -F pc` output: a `p<pid>` line, then `c<command>`.
pub fn parse_lsof_holder(output: &str) -> LockHolder {
    let field = |tag: char| {
        output
            .lines()
            .find_map(|line| line.strip_prefix(tag))
            .map(str::to_string)
            .unwrap_or_else(|| "?".to_string())
    };
    LockHolder {
        pid: field('p'),
        command: field('c'),
    }
}

/// How long ago `lock` was last written.
pub fn lock_age(metadata: &std::fs::Metadata, lock: &Path) -> Result<Duration, String> {
    metadata
        .modified()
        .and_then(|modified| modified.elapsed().map_err(std::io::Error::other))
        .map_err(|error| format!("inspect {}: {error}", lock.display()))
}

/// Prove `lock` abandoned: older than `stale_after` and held by no live
/// process.
///
/// `Ok(Some(identity))` is the proof, naming the exact file that was examined.
/// `Ok(None)` means the lock is not abandoned: it is younger than `stale_after`,
/// or a live process holds it open. `Err` means the question could not be
/// answered — `lsof` is missing or failed, or the platform has no way to ask —
/// and a caller must treat that as "not proven", never as "unheld".
#[cfg(unix)]
pub fn prove_abandoned(lock: &Path, stale_after: Duration) -> Result<Option<LockIdentity>, String> {
    use std::os::unix::fs::MetadataExt;

    let metadata = lock
        .metadata()
        .map_err(|error| format!("inspect {}: {error}", lock.display()))?;
    if lock_age(&metadata, lock)? < stale_after {
        return Ok(None);
    }
    match lock_holder(lock)? {
        Some(_) => Ok(None),
        None => Ok(Some(LockIdentity {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })),
    }
}

#[cfg(not(unix))]
pub fn prove_abandoned(
    lock: &Path,
    _stale_after: Duration,
) -> Result<Option<LockIdentity>, String> {
    lock_holder(lock).map(|_| None)
}

/// Is the file standing at `lock` still the one `identity` proved abandoned?
#[cfg(unix)]
pub fn lock_identity_matches(lock: &Path, identity: LockIdentity) -> Result<bool, String> {
    use std::os::unix::fs::MetadataExt;
    let current = lock
        .metadata()
        .map_err(|error| format!("revalidate {} before removal: {error}", lock.display()))?;
    Ok(current.dev() == identity.dev && current.ino() == identity.ino)
}

#[cfg(not(unix))]
pub fn lock_identity_matches(lock: &Path, _identity: LockIdentity) -> Result<bool, String> {
    Err(format!(
        "cannot revalidate {} before removal on this platform",
        lock.display()
    ))
}

/// Remove a lock [`prove_abandoned`] proved, provided the file at its path is
/// still the proven one.
///
/// `Ok(true)` means it was removed. `Ok(false)` means it was NOT: a different
/// file now stands at the path, which is a live writer that took the lock after
/// the proof, and removing THAT lock is the corruption this whole ceremony
/// exists to avoid. `Err` means the revalidation or the unlink itself failed.
pub fn remove_proven(lock: &Path, identity: LockIdentity) -> Result<bool, String> {
    if !lock_identity_matches(lock, identity)? {
        return Ok(false);
    }
    std::fs::remove_file(lock)
        .map_err(|error| format!("remove proven-stale {}: {error}", lock.display()))?;
    Ok(true)
}

/// Does this text say the operation lost the race for `packed-refs.lock`?
///
/// Narrow on purpose. It covers gix's phrasings, which jj's Git backend
/// surfaces, and git's own `Unable to create '<path>/packed-refs.lock'` — all of
/// which name either the lock file or the packed-ref file. Anything broader
/// would retry genuine failures for thirty seconds before reporting them.
pub fn packed_refs_lock_error(error: &str) -> bool {
    error.contains("packed-refs.lock")
        || error.contains("packed-ref file could not be obtained")
        || error.contains("lock for the packed-ref")
}

/// Does this text say a `git` command lost the race for a lock — an index,
/// a loose ref, `packed-refs`, or the repository config — because another
/// writer holds it?
///
/// The evidence is that the lock already exists: git's `Unable to create
/// '<path>.lock': File exists.` (the exclusive create failed with `EEXIST`), and
/// gix's `The lock for '<path>' could not be obtained`. The same `Unable to
/// create` and `cannot lock ref` prefixes also open permanent failures —
/// `Permission denied`, `Read-only file system`, a ref namespace conflict, a
/// broken ref — which are facts about the repository, not a writer that will
/// finish, so they are deliberately not matched.
///
/// Broader than [`packed_refs_lock_error`] because its callers do not retry on
/// it: they classify a failure as "someone else is writing" rather than "this
/// tree is damaged".
pub fn git_lock_contention(error: &str) -> bool {
    error.contains(".lock': File exists")
        || (error.contains("could not be obtained")
            && (error.contains(".lock") || error.contains("lock for the packed-ref")))
}

/// The lock file a contention diagnostic names, when it names one.
pub fn contended_lock_path(error: &str) -> Option<std::path::PathBuf> {
    ["Unable to create '", "The lock for '"]
        .into_iter()
        .find_map(|open| {
            let rest = &error[error.find(open)? + open.len()..];
            let path = &rest[..rest.find('\'')?];
            path.ends_with(".lock")
                .then(|| std::path::PathBuf::from(path))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[cfg(unix)]
    #[test]
    fn an_old_unheld_lock_is_proven_stale() {
        let temp = TempDir::new().unwrap();
        let lock = temp.path().join("packed-refs.lock");
        std::fs::write(&lock, "orphaned\n").unwrap();
        assert!(prove_abandoned(&lock, Duration::ZERO).unwrap().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn a_lock_younger_than_the_wait_is_not_stale() {
        let temp = TempDir::new().unwrap();
        let lock = temp.path().join("packed-refs.lock");
        std::fs::write(&lock, "fresh\n").unwrap();
        assert!(prove_abandoned(&lock, LOCK_WAIT).unwrap().is_none());
    }

    /// The refusal that makes removal safe at all: a lock old enough to look
    /// abandoned but still held open by a live process must never be removed.
    ///
    /// The holder is the only variable — the same lock, at the same age, is
    /// removable once the descriptor closes — so a regression that read `lsof`'s
    /// exit 0 as "nobody home" fails here and nowhere else.
    #[cfg(unix)]
    #[test]
    fn an_old_lock_a_live_process_holds_open_is_never_removed() {
        let temp = TempDir::new().unwrap();
        let lock = temp.path().join("packed-refs.lock");
        std::fs::write(&lock, "held by a live writer\n").unwrap();

        // An open descriptor is what distinguishes a live writer from an
        // abandoned lock, and it is what `lsof` reports.
        let held = std::fs::File::open(&lock).unwrap();
        assert!(
            prove_abandoned(&lock, Duration::ZERO).unwrap().is_none(),
            "a lock a live process holds open must never be proven stale, however old it looks"
        );
        assert_eq!(
            lock_holder(&lock).unwrap().map(|holder| holder.pid),
            Some(std::process::id().to_string()),
            "names the live holder"
        );

        // Nothing changes but the holder going away.
        drop(held);
        let identity = prove_abandoned(&lock, Duration::ZERO).unwrap().unwrap();
        assert!(remove_proven(&lock, identity).unwrap());
        assert!(!lock.exists(), "a proven-abandoned lock is removed");
    }

    #[cfg(unix)]
    #[test]
    fn replacement_after_stale_proof_is_not_the_proven_lock() {
        let temp = TempDir::new().unwrap();
        let lock = temp.path().join("packed-refs.lock");
        std::fs::write(&lock, "old\n").unwrap();
        let identity = prove_abandoned(&lock, Duration::ZERO).unwrap().unwrap();
        // Hold the original inode open so the filesystem cannot hand its
        // number straight back to the replacement.
        let _original = std::fs::File::open(&lock).unwrap();
        std::fs::remove_file(&lock).unwrap();
        std::fs::write(&lock, "new live writer\n").unwrap();
        assert!(!lock_identity_matches(&lock, identity).unwrap());
        assert!(
            !remove_proven(&lock, identity).unwrap(),
            "a replacement is never removed on the strength of its predecessor's proof"
        );
        assert!(lock.exists());
    }

    #[test]
    fn lsof_field_output_names_the_first_holder() {
        assert_eq!(
            parse_lsof_holder("p4242\ncgit\nf3\np99\ncjj\n"),
            LockHolder {
                pid: "4242".to_string(),
                command: "git".to_string()
            }
        );
        assert_eq!(parse_lsof_holder("").pid, "?");
    }

    #[cfg(not(unix))]
    #[test]
    fn a_platform_without_lsof_never_proves_a_lock_abandoned() {
        let temp = TempDir::new().unwrap();
        let lock = temp.path().join("index.lock");
        std::fs::write(&lock, "orphaned\n").unwrap();
        assert!(prove_abandoned(&lock, Duration::ZERO).is_err());
    }

    /// Verbatim diagnostics from git 2.50: contention is the lock already
    /// existing, and nothing else that shares its opening words.
    #[test]
    fn git_lock_contention_is_the_lock_already_existing() {
        for contended in [
            "discard tracked claim residue: fatal: Unable to create '/r/.git/worktrees/s/index.lock': \
             File exists.\n\nAnother git process seems to be running in this repository",
            "fatal: update_ref failed for ref 'refs/heads/x': cannot lock ref 'refs/heads/x': Unable \
             to create '/r/.git/refs/heads/x.lock': File exists.",
            "error: Unable to create '/r/.git/packed-refs.lock': File exists.",
            "The lock for '/r/.git/packed-refs.lock' could not be obtained",
            "The lock for the packed-ref file could not be obtained",
        ] {
            assert!(git_lock_contention(contended), "{contended}");
        }
        for permanent in [
            "discard tracked claim residue: fatal: Unable to create '/r/.git/index.lock': Permission \
             denied",
            "fatal: Unable to create '/r/.git/worktrees/s/index.lock': Read-only file system",
            "fatal: update_ref failed for ref 'refs/cairn/p': cannot lock ref 'refs/cairn/p': Unable \
             to create '/r/.git/refs/cairn/p.lock': Permission denied",
            "fatal: update_ref failed for ref 'refs/heads/a/b': cannot lock ref 'refs/heads/a/b': \
             'refs/heads/a' exists; cannot create 'refs/heads/a/b'",
            "fatal: update_ref failed for ref 'refs/heads/broken': cannot lock ref \
             'refs/heads/broken': unable to resolve reference 'refs/heads/broken': reference broken",
            "read claim index: fatal: index file corrupt",
            "fatal: Unable to create temporary file: No space left on device",
        ] {
            assert!(!git_lock_contention(permanent), "{permanent}");
        }
    }

    #[test]
    fn the_contended_lock_is_named_from_the_diagnostic() {
        assert_eq!(
            contended_lock_path(
                "fatal: Unable to create '/r/.git/worktrees/s/index.lock': File exists."
            ),
            Some(std::path::PathBuf::from("/r/.git/worktrees/s/index.lock"))
        );
        assert_eq!(
            contended_lock_path("The lock for '/r/.git/packed-refs.lock' could not be obtained"),
            Some(std::path::PathBuf::from("/r/.git/packed-refs.lock"))
        );
        assert_eq!(
            contended_lock_path("The lock for the packed-ref file could not be obtained"),
            None
        );
    }
}
