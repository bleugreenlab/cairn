//! The one place Cairn is patient with `packed-refs.lock`.
//!
//! Every runner-owned ref transaction against a project's shared Git repository
//! contends for a single `packed-refs.lock`: `jj git import` when a job is
//! prepared, `jj git export` when a logical head publishes, the `git update-ref`
//! pair that publishes and retires a sealed check commit's reference, and the
//! ref that names a straddled delta. gix waits about a second for that lock and
//! then fails the whole operation, and each of those failures costs an agent
//! something it cannot recover on its own: a cleanly applied patch rolled back,
//! a computed verdict discarded, a job preparation killed outright.
//!
//! The remedy is one policy applied at every one of those sites. Wait out
//! ordinary contention — a routine `git` writer holds the lock for milliseconds,
//! and the only cost of waiting is latency. Then, and only when the lock has
//! outlived the whole wait AND `lsof` proves that no live process holds it,
//! remove it exactly once and try again.
//!
//! The proof matters more than the removal. Deleting a live writer's lock is
//! how a ref store gets corrupted, so the healer re-reads the lock file's
//! `(dev, ino)` immediately before unlinking it and refuses to delete a
//! replacement that a live writer minted in the window between the proof and
//! the removal.
//!
//! This module exists so that sequence is never copied. It was first written
//! for the import path alone, and its identity revalidation already needed one
//! review correction; a second copy would drift on exactly that step, and the
//! step is the one that decides whether healing is safe.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

/// How long a ref transaction waits out contention before it treats the lock as
/// abandoned. Longer than any legitimate `packed-refs` rewrite, and far shorter
/// than the 600s managed-store lease several callers hold while they write.
const LOCK_WAIT: Duration = Duration::from_secs(30);

/// Gap between attempts. gix's own internal wait is about a second, so a caller
/// that is going to win the race usually does so within the first few attempts.
const RETRY_DELAY: Duration = Duration::from_millis(250);

/// Where the contended `packed-refs.lock` lives, named the way each caller
/// already knows its repository.
#[derive(Clone, Copy)]
pub enum PackedRefs<'a> {
    /// A jj store or workspace directory. The lock belongs to the ordinary Git
    /// checkout backing it, reached through the store's `git_target`.
    JjStore(&'a Path),
    /// An ordinary Git working directory, or a linked worktree of one.
    Checkout(&'a Path),
}

impl PackedRefs<'_> {
    /// The lock file this scope contends for, or `None` when the topology does
    /// not resolve one — a store with no backing checkout, an unreadable
    /// worktree pointer. An unresolvable scope is simply never healed, and the
    /// caller's own error is what surfaces.
    fn lock_path(self) -> Option<PathBuf> {
        let checkout = match self {
            Self::JjStore(dir) => super::export::resolve_backing_checkout(dir)?,
            Self::Checkout(dir) => dir.to_path_buf(),
        };
        Some(git_common_dir(&checkout)?.join("packed-refs.lock"))
    }
}

/// The directory holding `packed-refs` for `checkout`: its `.git` directory in
/// the ordinary case, the shared common directory when `checkout` is a linked
/// worktree (whose `.git` is a file pointing into `<common>/worktrees/<name>`),
/// or the directory itself when it is a bare repository.
///
/// Resolved by reading rather than by spawning `git rev-parse`: this runs on the
/// healing path, whose whole premise is that the repository's ref store is
/// already contended.
pub(crate) fn git_common_dir(checkout: &Path) -> Option<PathBuf> {
    let dot_git = checkout.join(".git");
    if dot_git.is_dir() {
        return Some(dot_git);
    }
    if dot_git.is_file() {
        let pointer = std::fs::read_to_string(&dot_git).ok()?;
        let target = PathBuf::from(pointer.trim().strip_prefix("gitdir:")?.trim());
        let target = if target.is_absolute() {
            target
        } else {
            checkout.join(target)
        };
        // A linked worktree's private directory is `<common>/worktrees/<name>`,
        // and `packed-refs` is shared across every worktree, so the lock lives
        // two levels up rather than beside HEAD.
        if target.parent().and_then(Path::file_name) == Some(std::ffi::OsStr::new("worktrees")) {
            return target.parent()?.parent().map(Path::to_path_buf);
        }
        return Some(target);
    }
    checkout
        .join("HEAD")
        .is_file()
        .then(|| checkout.to_path_buf())
}

/// Does this text say the operation lost the race for `packed-refs.lock`?
///
/// Narrow on purpose. It covers gix's phrasings, which jj's Git backend
/// surfaces, and git's own `Unable to create '<path>/packed-refs.lock'` — all of
/// which name either the lock file or the packed-ref file. Anything broader
/// would retry genuine failures for thirty seconds before reporting them.
///
/// Public to the crate because contention does not always arrive as an error:
/// `jj git export` reports a refused ref on stderr and still exits 0, so its
/// caller has to recognize the same phrasing in a "successful" run and hand it
/// back as the failure it is.
pub(crate) fn packed_refs_lock_error(error: &str) -> bool {
    error.contains("packed-refs.lock")
        || error.contains("packed-ref file could not be obtained")
        || error.contains("lock for the packed-ref")
}

/// Prove a lock is abandoned: older than `stale_after` and held by no live
/// process. Returns the lock's `(dev, ino)` identity, which is what makes the
/// proof usable — it names the exact file that was proven, not merely the path.
#[cfg(unix)]
fn stale_unheld_lock(lock: &Path, stale_after: Duration) -> Result<Option<(u64, u64)>, String> {
    use std::os::unix::fs::MetadataExt;

    let metadata = lock
        .metadata()
        .map_err(|error| format!("inspect {}: {error}", lock.display()))?;
    let age = metadata
        .modified()
        .and_then(|modified| modified.elapsed().map_err(std::io::Error::other))
        .map_err(|error| format!("inspect {}: {error}", lock.display()))?;
    if age < stale_after {
        return Ok(None);
    }

    let status = cairn_platform::env::command("lsof")
        .arg("--")
        .arg(lock)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("prove {} is unheld with lsof: {error}", lock.display()))?;
    match status.code() {
        Some(0) => Ok(None),
        Some(1) => Ok(Some((metadata.dev(), metadata.ino()))),
        code => Err(format!(
            "prove {} is unheld with lsof: unexpected exit {code:?}",
            lock.display()
        )),
    }
}

#[cfg(not(unix))]
fn stale_unheld_lock(_lock: &Path, _stale_after: Duration) -> Result<Option<(u64, u64)>, String> {
    Ok(None)
}

#[cfg(unix)]
fn lock_identity_matches(lock: &Path, identity: (u64, u64)) -> Result<bool, String> {
    use std::os::unix::fs::MetadataExt;
    let current = lock
        .metadata()
        .map_err(|error| format!("revalidate {} before removal: {error}", lock.display()))?;
    Ok((current.dev(), current.ino()) == identity)
}

/// Remove a lock that has been proven abandoned, and say so in the log with the
/// detail an operator would otherwise gather by hand: which lock, how long it
/// was waited out, and that `lsof` found no holder.
fn heal_stale_lock(
    scope: PackedRefs<'_>,
    ctx: &str,
    stale_after: Duration,
) -> Result<bool, String> {
    let Some(lock) = scope.lock_path() else {
        return Ok(false);
    };
    let Some(identity) = stale_unheld_lock(&lock, stale_after)? else {
        return Ok(false);
    };
    #[cfg(unix)]
    {
        // Between the lsof proof and this unlink, a live writer may have taken
        // the lock for itself. Removing THAT lock is the corruption this whole
        // ceremony exists to avoid, so the file still standing at the path must
        // be the same file that was proven abandoned.
        if !lock_identity_matches(&lock, identity)? {
            return Err(format!(
                "refused to remove {} because the lock was replaced after stale-lock proof",
                lock.display()
            ));
        }
    }
    std::fs::remove_file(&lock)
        .map_err(|error| format!("remove proven-stale {}: {error}", lock.display()))?;
    log::warn!(
        "{ctx}: removed proven-stale packed-refs lock {} after waiting {:.1}s; lsof found no live holder",
        lock.display(),
        stale_after.as_secs_f64()
    );
    Ok(true)
}

/// What to do after an attempt failed on lock contention.
enum Recovery {
    /// Ordinary contention, still inside the wait: sleep, then try again.
    Wait(Duration),
    /// A proven-stale lock was just removed: try again immediately.
    RetryNow,
}

/// The policy itself, held apart from how a caller sleeps so that the blocking
/// and async wrappers share one decision procedure rather than two copies of it.
struct Patience<'a> {
    scope: PackedRefs<'a>,
    ctx: &'a str,
    started: Instant,
    attempts: u32,
    healed: bool,
}

impl<'a> Patience<'a> {
    fn new(scope: PackedRefs<'a>, ctx: &'a str) -> Self {
        Self {
            scope,
            ctx,
            started: Instant::now(),
            attempts: 0,
            healed: false,
        }
    }

    fn attempting(&mut self) {
        self.attempts += 1;
    }

    fn settled(&self) {
        if self.attempts > 1 {
            log::info!(
                "{}: succeeded after {} attempts and {:.1}s of packed-refs lock contention",
                self.ctx,
                self.attempts,
                self.started.elapsed().as_secs_f64()
            );
        }
    }

    /// Decide what a failed attempt earns. An error that is not lock contention
    /// surfaces untouched, and so does a contention error the wait and a single
    /// healing attempt could not clear — patience never converts a real failure
    /// into a different one.
    fn recover(&mut self, error: String) -> Result<Recovery, String> {
        if !packed_refs_lock_error(&error) {
            return Err(error);
        }
        if self.started.elapsed() < LOCK_WAIT {
            log::warn!(
                "{}: attempt {} hit packed-refs lock contention; retrying",
                self.ctx,
                self.attempts
            );
            return Ok(Recovery::Wait(RETRY_DELAY));
        }
        // Healing is attempted at most once per operation. A second stale lock
        // inside the same call means a live writer is minting them, which is
        // exactly the case where removal must stop rather than escalate.
        if self.healed {
            return Err(error);
        }
        self.healed = true;
        match heal_stale_lock(self.scope, self.ctx, LOCK_WAIT) {
            Ok(true) => Ok(Recovery::RetryNow),
            Ok(false) => Err(error),
            Err(proof_error) => Err(format!(
                "{error}\nStale-lock recovery refused: {proof_error}"
            )),
        }
    }
}

/// Run `op`, waiting out `packed-refs.lock` contention and healing a lock proven
/// abandoned. Blocking; for a caller already inside an async task, use
/// [`with_packed_refs_patience_async`], which sleeps without holding a runtime
/// worker.
///
/// `op` must be safe to repeat: every caller here either writes a ref
/// idempotently or verifies afterwards that the ref landed.
pub fn with_packed_refs_patience<T>(
    scope: PackedRefs<'_>,
    ctx: &str,
    mut op: impl FnMut() -> Result<T, String>,
) -> Result<T, String> {
    let mut patience = Patience::new(scope, ctx);
    loop {
        patience.attempting();
        match op() {
            Ok(value) => {
                patience.settled();
                return Ok(value);
            }
            Err(error) => match patience.recover(error)? {
                Recovery::Wait(delay) => std::thread::sleep(delay),
                Recovery::RetryNow => {}
            },
        }
    }
}

/// [`with_packed_refs_patience`] for an async caller.
///
/// The waiting is what takes seconds; `op` itself is a short `git` or `jj`
/// invocation. So the wait yields to the runtime and the operation runs inline,
/// rather than moving a whole ref transaction onto the blocking pool.
pub async fn with_packed_refs_patience_async<T>(
    scope: PackedRefs<'_>,
    ctx: &str,
    mut op: impl FnMut() -> Result<T, String>,
) -> Result<T, String> {
    let mut patience = Patience::new(scope, ctx);
    loop {
        patience.attempting();
        match op() {
            Ok(value) => {
                patience.settled();
                return Ok(value);
            }
            Err(error) => match patience.recover(error)? {
                Recovery::Wait(delay) => tokio::time::sleep(delay).await,
                Recovery::RetryNow => {}
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tempfile::TempDir;

    #[test]
    fn packed_refs_contention_is_classified_narrowly() {
        assert!(packed_refs_lock_error(
            "The lock for '/repo/.git/packed-refs.lock' could not be obtained"
        ));
        assert!(packed_refs_lock_error(
            "The lock for the packed-ref file could not be obtained"
        ));
        assert!(packed_refs_lock_error(
            "cannot lock ref 'refs/cairn/checks/x': Unable to create \
             '/repo/.git/packed-refs.lock': File exists."
        ));
        assert!(!packed_refs_lock_error("Revision main does not exist"));
    }

    #[cfg(unix)]
    #[test]
    fn an_old_unheld_lock_is_proven_stale() {
        let temp = TempDir::new().unwrap();
        let lock = temp.path().join("packed-refs.lock");
        std::fs::write(&lock, "orphaned\n").unwrap();
        assert!(stale_unheld_lock(&lock, Duration::ZERO).unwrap().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn a_lock_younger_than_the_wait_is_not_stale() {
        let temp = TempDir::new().unwrap();
        let lock = temp.path().join("packed-refs.lock");
        std::fs::write(&lock, "fresh\n").unwrap();
        assert!(stale_unheld_lock(&lock, LOCK_WAIT).unwrap().is_none());
    }

    /// The refusal that makes healing safe at all: a lock old enough to look
    /// abandoned but still held open by a live process must never be removed.
    /// Deleting a live writer's lock is how a ref store gets corrupted.
    ///
    /// The holder is the only variable — the same lock, at the same age, is
    /// removable once the descriptor closes — so a regression that read `lsof`'s
    /// exit 0 as "nobody home" fails here and nowhere else.
    #[cfg(unix)]
    #[test]
    fn an_old_lock_a_live_process_holds_open_is_never_removed() {
        let temp = TempDir::new().unwrap();
        let checkout = temp.path().join("repo");
        std::fs::create_dir_all(checkout.join(".git")).unwrap();
        let lock = checkout.join(".git").join("packed-refs.lock");
        std::fs::write(&lock, "held by a live writer\n").unwrap();

        // An open descriptor is what distinguishes a live writer from an
        // abandoned lock, and it is what `lsof` reports.
        let held = std::fs::File::open(&lock).unwrap();
        assert!(
            stale_unheld_lock(&lock, Duration::ZERO).unwrap().is_none(),
            "a lock a live process holds open must never be proven stale, however old it looks"
        );
        assert!(
            !heal_stale_lock(PackedRefs::Checkout(&checkout), "unit test", Duration::ZERO).unwrap(),
            "healing must decline a lock it could not prove abandoned"
        );
        assert!(
            lock.exists(),
            "the held lock must survive the healing attempt"
        );

        // Nothing changes but the holder going away.
        drop(held);
        assert!(stale_unheld_lock(&lock, Duration::ZERO).unwrap().is_some());
        assert!(
            heal_stale_lock(PackedRefs::Checkout(&checkout), "unit test", Duration::ZERO).unwrap()
        );
        assert!(!lock.exists(), "a proven-abandoned lock is removed");
    }

    #[cfg(unix)]
    #[test]
    fn replacement_after_stale_proof_is_not_the_proven_lock() {
        let temp = TempDir::new().unwrap();
        let lock = temp.path().join("packed-refs.lock");
        std::fs::write(&lock, "old\n").unwrap();
        let identity = stale_unheld_lock(&lock, Duration::ZERO).unwrap().unwrap();
        std::fs::remove_file(&lock).unwrap();
        std::fs::write(&lock, "new live writer\n").unwrap();
        assert!(!lock_identity_matches(&lock, identity).unwrap());
    }

    /// A checkout's lock lives in `.git`; a linked worktree shares the common
    /// directory's one `packed-refs`, two levels above its private directory.
    #[test]
    fn a_linked_worktree_resolves_the_shared_common_directory() {
        let temp = TempDir::new().unwrap();
        let main = temp.path().join("main");
        std::fs::create_dir_all(main.join(".git").join("worktrees").join("slot")).unwrap();
        let slot = temp.path().join("slot");
        std::fs::create_dir_all(&slot).unwrap();
        std::fs::write(
            slot.join(".git"),
            format!(
                "gitdir: {}\n",
                main.join(".git").join("worktrees").join("slot").display()
            ),
        )
        .unwrap();

        assert_eq!(
            PackedRefs::Checkout(&main).lock_path().unwrap(),
            main.join(".git").join("packed-refs.lock")
        );
        assert_eq!(
            PackedRefs::Checkout(&slot).lock_path().unwrap(),
            main.join(".git").join("packed-refs.lock")
        );
    }

    #[test]
    fn a_non_contention_error_is_never_retried() {
        let temp = TempDir::new().unwrap();
        let attempts = AtomicU32::new(0);
        let error =
            with_packed_refs_patience::<()>(PackedRefs::Checkout(temp.path()), "unit test", || {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err("Revision main does not exist".to_string())
            })
            .unwrap_err();
        assert_eq!(error, "Revision main does not exist");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn contention_that_clears_is_waited_out() {
        let temp = TempDir::new().unwrap();
        let attempts = AtomicU32::new(0);
        with_packed_refs_patience(PackedRefs::Checkout(temp.path()), "unit test", || {
            if attempts.fetch_add(1, Ordering::SeqCst) < 3 {
                return Err("The lock for the packed-ref file could not be obtained".to_string());
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn async_contention_that_clears_is_waited_out() {
        let temp = TempDir::new().unwrap();
        let attempts = AtomicU32::new(0);
        with_packed_refs_patience_async(PackedRefs::Checkout(temp.path()), "unit test", || {
            if attempts.fetch_add(1, Ordering::SeqCst) < 3 {
                return Err("The lock for the packed-ref file could not be obtained".to_string());
            }
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }
}
