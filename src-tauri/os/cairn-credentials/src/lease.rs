//! Cross-process leases over the credentials in the credential store.
//!
//! A rotating OAuth refresh token is a single-use secret: spending it mints a
//! replacement and destroys the original. Exactly one refresher may hold it at a
//! time, and "one" has to mean one on the machine, not one per process. Cairn
//! runs several processes over a single `CAIRN_HOME` — the runner, the desktop
//! app, agent invocations, usage probes — and every one of them resolves the
//! same `credentials.yaml`. A `Mutex` inside any of them is invisible to the rest,
//! so the token gets spent twice and the provider answers `refresh_token_reused`
//! to whichever process lost.
//!
//! The kernel's advisory file lock is the one mutual exclusion primitive all of
//! them can see. It is also self-healing in the way this problem needs: the lock
//! lives on the open file description, so a process that dies mid-refresh
//! releases it immediately rather than wedging the account until someone clears
//! a stale marker by hand.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long a caller polls for a contended lease before giving up.
///
/// The holder is making one HTTP round trip to the provider's token endpoint,
/// so the wait is bounded by that call plus a store write. Waiting is the
/// correct behavior — the winner is about to publish credentials this caller can
/// use — but an unbounded wait would turn a wedged holder into a wedged account.
pub const DEFAULT_LEASE_WAIT: Duration = Duration::from_secs(45);

/// How often a waiter re-tries the non-blocking acquire.
///
/// Polling rather than a blocking `flock` keeps the wait bounded on every
/// platform with one implementation, and the interval is short enough that a
/// waiter reads the winner's freshly published credential promptly.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// An acquired credential lease. Dropping it releases the machine-wide lock.
pub struct CredentialLease {
    file: File,
    /// What this lease covers, for diagnostics. Never a token.
    scope: String,
}

/// Reports the scope and nothing else.
///
/// Written out rather than derived because `Debug for File` resolves the
/// descriptor back to a path on unix, which would print the whole home
/// directory and repeat the account id inside it, in a shape that varies by
/// platform and degrades to a bare descriptor when the path cannot be resolved.
/// None of that is secret — a lock path is not a token — but the scope is the
/// only part a reader of a diagnostic needs, and it reads the same everywhere.
impl std::fmt::Debug for CredentialLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialLease")
            .field("scope", &self.scope)
            .finish()
    }
}

/// Release the lock explicitly rather than by closing the file.
///
/// An `flock` lock lives on the open file description, and `fork` duplicates
/// every descriptor into the child — so between any thread in this process
/// forking and that child reaching `exec`, a copy of this description exists
/// that `close` does not dispose of. Cairn spawns processes constantly, so a
/// lease dropped inside that window would read as held long after its owner
/// finished. `LOCK_UN` releases the lock on the description itself, which every
/// copy shares.
impl Drop for CredentialLease {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            // SAFETY: `self.file` owns the descriptor for the duration of the
            // call, and `flock` only reads it.
            unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Storage::FileSystem::UnlockFile;
            // SAFETY: `self.file` owns the handle for the duration of the call,
            // over the same whole-file range `try_lock_exclusive` took.
            unsafe {
                UnlockFile(self.file.as_raw_handle() as _, 0, 0, u32::MAX, u32::MAX);
            }
        }
    }
}

/// Why a lease could not be taken.
#[derive(Debug)]
pub enum LeaseError {
    /// Another process held the lease for the whole bounded wait. The caller is
    /// a loser, not a failure: the holder is refreshing this very credential,
    /// and re-reading the durable store is the right next move.
    Busy { scope: String, waited: Duration },
    /// The lock file itself could not be opened. Distinct from `Busy` because
    /// serializing is impossible here rather than merely contended, and a
    /// caller must not treat that as "someone else has it".
    Unusable { scope: String, reason: String },
}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::Busy { scope, waited } => write!(
                f,
                "another Cairn process has been refreshing {scope} for {}s",
                waited.as_secs()
            ),
            LeaseError::Unusable { scope, reason } => {
                write!(f, "credential lease for {scope} is unusable: {reason}")
            }
        }
    }
}

/// Where a credential lease lives: beside the store it guards, so every process
/// that resolves this `CAIRN_HOME` resolves the same lock file.
fn lease_path(config_dir: &Path, scope: &str) -> PathBuf {
    config_dir
        .join("credential-leases")
        .join(format!("{}.lock", sanitize_scope(scope)))
}

/// Reduce a scope to something that is safe as a single path component.
///
/// Account ids are Cairn-minted and already tame, but they cross a filesystem
/// boundary here and a scope that escaped into a parent directory would put the
/// lock somewhere other processes never look — which reads as "uncontended"
/// rather than as an error.
fn sanitize_scope(scope: &str) -> String {
    scope
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Take the machine-wide lease for `scope`, waiting up to `wait` for a holder.
pub fn acquire_credential_lease(
    config_dir: &Path,
    scope: &str,
    wait: Duration,
) -> Result<CredentialLease, LeaseError> {
    let path = lease_path(config_dir, scope);
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return Err(LeaseError::Unusable {
                scope: scope.to_string(),
                reason: format!("create {}: {e}", parent.display()),
            });
        }
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|e| LeaseError::Unusable {
            scope: scope.to_string(),
            reason: format!("open {}: {e}", path.display()),
        })?;

    let started = Instant::now();
    loop {
        if try_lock_exclusive(&file) {
            return Ok(CredentialLease {
                file,
                scope: scope.to_string(),
            });
        }
        if started.elapsed() >= wait {
            return Err(LeaseError::Busy {
                scope: scope.to_string(),
                waited: started.elapsed(),
            });
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> bool {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `file` owns the descriptor for the duration of the call, and
    // `flock` only reads it. `LOCK_NB` keeps this a try-lock so the caller owns
    // its own deadline.
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
}

#[cfg(windows)]
fn try_lock_exclusive(file: &File) -> bool {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    // SAFETY: `file` owns the handle for the duration of the call. The zeroed
    // OVERLAPPED requests a lock from offset 0; the maximum byte range covers
    // the whole file, matching what `Drop` unlocks.
    unsafe {
        let mut overlapped: OVERLAPPED = std::mem::zeroed();
        LockFileEx(
            file.as_raw_handle() as _,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            u32::MAX,
            u32::MAX,
            &mut overlapped,
        ) != 0
    }
}

/// No advisory lock on this target. Refusing to refresh would leave the account
/// permanently unusable, which is worse than an unserialized refresh, so the
/// caller proceeds — but says so, because an unserialized rotating token is
/// exactly the hazard this module exists to remove.
#[cfg(not(any(unix, windows)))]
fn try_lock_exclusive(_file: &File) -> bool {
    log::warn!("credential leases are not enforced on this platform");
    true
}

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::*;

    #[test]
    fn a_second_holder_waits_and_then_reports_busy() {
        let dir = tempfile::tempdir().unwrap();
        let held = acquire_credential_lease(dir.path(), "openai/acct-1", DEFAULT_LEASE_WAIT)
            .expect("first acquire succeeds");

        let started = Instant::now();
        let contended =
            acquire_credential_lease(dir.path(), "openai/acct-1", Duration::from_millis(200));

        match contended {
            Err(LeaseError::Busy { scope, .. }) => assert_eq!(scope, "openai/acct-1"),
            other => panic!("expected Busy while held, got {other:?}"),
        }
        assert!(
            started.elapsed() >= Duration::from_millis(200),
            "a contended acquire must wait out its bound rather than fail immediately"
        );
        drop(held);
    }

    /// Two accounts refresh independently: one account's in-flight rotation must
    /// never stall another subscription's.
    #[test]
    fn different_accounts_do_not_contend() {
        let dir = tempfile::tempdir().unwrap();
        let first = acquire_credential_lease(dir.path(), "openai/acct-1", DEFAULT_LEASE_WAIT)
            .expect("first account");
        let second =
            acquire_credential_lease(dir.path(), "openai/acct-2", Duration::from_millis(0))
                .expect("a different account is uncontended");
        assert_eq!(first.scope, "openai/acct-1");
        assert_eq!(second.scope, "openai/acct-2");
    }

    /// A holder that dies (or simply drops) hands the lease straight to the next
    /// caller, so a crashed refresher cannot strand the account.
    #[test]
    fn releasing_hands_the_lease_to_the_next_caller() {
        let dir = tempfile::tempdir().unwrap();
        let held =
            acquire_credential_lease(dir.path(), "openai/acct-1", DEFAULT_LEASE_WAIT).unwrap();
        drop(held);
        acquire_credential_lease(dir.path(), "openai/acct-1", Duration::from_millis(0))
            .expect("a released lease is immediately reusable");
    }

    /// Scopes cross into a path component, so one that tried to escape its
    /// directory must be flattened rather than silently locking elsewhere.
    #[test]
    fn scopes_are_flattened_into_one_path_component() {
        let dir = tempfile::tempdir().unwrap();
        let path = lease_path(dir.path(), "openai/../../acct");
        assert_eq!(path.parent().unwrap(), dir.path().join("credential-leases"));
        assert_eq!(
            path.file_name().unwrap().to_string_lossy(),
            "openai_.._.._acct.lock"
        );
    }
}
