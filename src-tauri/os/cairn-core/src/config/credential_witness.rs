//! Mutual exclusion and witness identities for MCP credential mutations.
//!
//! A cached MCP catalog is bound to a *credential witness* — a generation that
//! changes whenever the credentials behind a server change — so a catalog
//! fetched under one account can never verify under another. A generation is
//! only a witness if no two committed credentials share one, and the keychain
//! cannot promise that by itself: it offers no compare-and-swap and no lock, so
//! two writers that both read generation `n` would both publish `n + 1`.
//!
//! Every participating credential mutation therefore runs under one
//! cross-process advisory lock, from reading the current witness through
//! publishing the next one. Catalog publication takes the same lock for its
//! final witness check, so no mutation can land between that check and the
//! write. The lock is never held across network I/O.
//!
//! The lock's identity follows the keychain namespace it protects, which is
//! the OS user and the dev/prod mode, not the Cairn home: dev instances run
//! with their own `CAIRN_HOME` but share the dev keychain, so a lock beside each
//! home would not exclude them from one another.
//!
//! Advisory locking only constrains participating writers. A keychain entry
//! deleted by hand outside Cairn is handled separately: a witness that has to be
//! created from nothing gets a fresh random incarnation, so it never restarts at
//! a generation an old catalog still carries.

use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long a mutation waits for another to finish before failing closed.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

thread_local! {
    static HELD: Cell<bool> = const { Cell::new(false) };
}

/// Proof that the caller holds the credential mutation lock.
///
/// Deliberately `!Send`: holding it across an `.await` makes the future
/// non-`Send`, which is how a lock that would span a network fetch becomes a
/// compile error at the call site rather than a review comment.
pub struct CredentialMutationGuard {
    file: File,
    _not_send: PhantomData<*const ()>,
}

impl Drop for CredentialMutationGuard {
    fn drop(&mut self) {
        // Release on the open file description itself, not by closing it: a
        // fork in another thread duplicates the descriptor, and a close would
        // leave that copy holding the lock until the child execs.
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            // SAFETY: `self.file` owns the descriptor for the duration of the
            // call, and `flock` only reads it.
            unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        }
        HELD.with(|held| held.set(false));
    }
}

#[cfg(not(test))]
fn lock_path() -> PathBuf {
    let home = cairn_common::paths::os_home_dir().unwrap_or_else(std::env::temp_dir);
    home.join(format!(
        ".cairn-mcp-credentials-{}.lock",
        cairn_common::paths::env_str()
    ))
}

// Unit tests share one in-memory keychain per process, so one lock per process
// is the matching identity, and tests never touch the user's home.
#[cfg(test)]
fn lock_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "cairn-test-mcp-credentials-{}.lock",
        std::process::id()
    ))
}

/// Take the credential mutation lock, waiting for a concurrent mutation.
///
/// Fails rather than waiting forever, and fails immediately on re-entry from
/// the same thread, which would otherwise wait on itself until the timeout.
pub fn acquire() -> Result<CredentialMutationGuard, String> {
    acquire_at(&lock_path(), ACQUIRE_TIMEOUT)
}

fn acquire_at(path: &Path, timeout: Duration) -> Result<CredentialMutationGuard, String> {
    if HELD.with(Cell::get) {
        return Err("credential mutation lock is already held by this thread".to_string());
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("Failed to open credential lock {}: {e}", path.display()))?;
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(1);
    loop {
        match cairn_common::kernel_lock::try_lock_exclusive(&file) {
            Ok(true) => {
                HELD.with(|held| held.set(true));
                return Ok(CredentialMutationGuard {
                    file,
                    _not_send: PhantomData,
                });
            }
            Ok(false) if Instant::now() < deadline => {
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_millis(50));
            }
            Ok(false) => {
                return Err(
                    "Timed out waiting for another MCP credential change to finish".to_string(),
                )
            }
            Err(e) => return Err(format!("Failed to lock MCP credentials: {e}")),
        }
    }
}

/// A random incarnation for a witness created from nothing.
pub fn new_incarnation() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_holder_waits_and_then_times_out() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.lock");
        let first = acquire_at(&path, Duration::from_millis(10)).unwrap();
        let contender = std::thread::spawn({
            let path = path.clone();
            move || acquire_at(&path, Duration::from_millis(30)).map(drop)
        });
        assert!(contender.join().unwrap().is_err());
        drop(first);
        let contender =
            std::thread::spawn(move || acquire_at(&path, Duration::from_secs(5)).map(drop));
        assert!(contender.join().unwrap().is_ok());
    }

    #[test]
    fn re_entry_on_one_thread_fails_fast_instead_of_waiting_on_itself() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.lock");
        let held = acquire_at(&path, Duration::from_secs(5)).unwrap();
        let started = Instant::now();
        assert!(acquire_at(&path, Duration::from_secs(5)).is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(held);
        assert!(acquire_at(&path, Duration::from_secs(5)).is_ok());
    }

    #[test]
    fn incarnations_are_not_reused() {
        assert_ne!(new_incarnation(), new_incarnation());
    }
}
