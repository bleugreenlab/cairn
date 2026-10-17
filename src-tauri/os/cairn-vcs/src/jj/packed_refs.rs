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

/// The leading words of the line a give-up appends to diagnose the lock as it
/// stood when patience ran out. Stable on purpose: the reconcile classifier
/// keys its `ref_lock_contention` and `ref_lock_stale` kinds on them.
pub(crate) const CONTENTION_DIAGNOSIS: &str = "packed-refs lock: contention";
pub(crate) const STALE_DIAGNOSIS: &str = "packed-refs lock: stale";

/// A live process `lsof` reports holding the lock open.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LockHolder {
    pid: String,
    command: String,
}

/// Who holds `lock` open right now, according to `lsof`: `None` when nobody
/// does. An error means the question could not be answered, which is never
/// read as "nobody".
#[cfg(unix)]
fn lock_holder(lock: &Path) -> Result<Option<LockHolder>, String> {
    let output = cairn_platform::env::command("lsof")
        .args(["-F", "pc", "--"])
        .arg(lock)
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
fn lock_holder(_lock: &Path) -> Result<Option<LockHolder>, String> {
    Err("no lsof on this platform to prove the lock unheld".to_string())
}

/// The first process in `lsof -F pc` output: a `p<pid>` line, then `c<command>`.
fn parse_lsof_holder(output: &str) -> LockHolder {
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

fn lock_age(metadata: &std::fs::Metadata, lock: &Path) -> Result<Duration, String> {
    metadata
        .modified()
        .and_then(|modified| modified.elapsed().map_err(std::io::Error::other))
        .map_err(|error| format!("inspect {}: {error}", lock.display()))
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
    if lock_age(&metadata, lock)? < stale_after {
        return Ok(None);
    }
    match lock_holder(lock)? {
        Some(_) => Ok(None),
        None => Ok(Some((metadata.dev(), metadata.ino()))),
    }
}

#[cfg(not(unix))]
fn stale_unheld_lock(_lock: &Path, _stale_after: Duration) -> Result<Option<(u64, u64)>, String> {
    Ok(None)
}

/// One line saying what the lock looked like when patience gave up, so a
/// reader can tell a busy ref store from an abandoned lock without gathering
/// it by hand. `refusal` is why a proven-stale removal did not happen, when it
/// was attempted and declined.
fn give_up_diagnosis(scope: PackedRefs<'_>, refusal: Option<&str>) -> String {
    let waited = LOCK_WAIT.as_secs();
    let burst = |what: &str| {
        format!(
            "{CONTENTION_DIAGNOSIS} — {what} at give-up; lost the race to a burst of writers for \
             {waited}s"
        )
    };
    let Some(lock) = scope.lock_path() else {
        return burst("no resolvable lock path");
    };
    let Ok(metadata) = lock.metadata() else {
        return burst("no lock");
    };
    let old = lock_age(&metadata, &lock).is_ok_and(|age| age >= LOCK_WAIT);
    let stale = |reason: &str| {
        format!(
            "{STALE_DIAGNOSIS} — older than {waited}s with no live holder, not removed: {reason}"
        )
    };
    match lock_holder(&lock) {
        Ok(Some(holder)) => format!(
            "{CONTENTION_DIAGNOSIS} — held by live process {} ({}) after {waited}s",
            holder.pid, holder.command
        ),
        Ok(None) if old => stale(refusal.unwrap_or(
            "it outlived the wait only after removal was considered; the next attempt removes it",
        )),
        Ok(None) => burst("a fresh lock with no live holder"),
        Err(reason) if old => stale(refusal.unwrap_or(&reason)),
        Err(reason) => burst(&format!(
            "a fresh lock whose holder could not be read ({reason})"
        )),
    }
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
            return Err(self.give_up(error, None));
        }
        self.healed = true;
        match heal_stale_lock(self.scope, self.ctx, LOCK_WAIT) {
            Ok(true) => Ok(Recovery::RetryNow),
            Ok(false) => Err(self.give_up(error, None)),
            Err(proof_error) => Err(self.give_up(
                format!("{error}\nStale-lock recovery refused: {proof_error}"),
                Some(&proof_error),
            )),
        }
    }

    /// The contention error, unchanged, plus one line diagnosing the lock as
    /// it stands now: a live holder, a burst of writers, or a stale lock that
    /// could not be removed.
    fn give_up(&self, error: String, refusal: Option<&str>) -> String {
        let diagnosis = give_up_diagnosis(self.scope, refusal);
        log::warn!(
            "{}: gave up after {} attempts and {:.1}s: {diagnosis}",
            self.ctx,
            self.attempts,
            self.started.elapsed().as_secs_f64()
        );
        format!("{error}\n{diagnosis}")
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

/// Run one repeatable `jj` rewrite against the shared store, patient with
/// `packed-refs.lock`.
///
/// For a single `--ignore-working-copy` command against the store, run under
/// the per-store lock, and nothing else. A workspace command snapshots its
/// working copy as an operation of its own, so a repeat is not the same
/// command; `jj git export` has its own leg with its own verification.
///
/// Repeating is safe because a rewrite that loses the lock fails inside its
/// commit write, before the transaction commits (pinned by
/// `a_rewrite_that_loses_the_packed_refs_lock_commits_no_operation`). The
/// guard here does not trust that premise blindly: it compares the operation
/// head after each lost attempt with the one captured before the first. If it
/// moved, the attempt committed and then failed, so the store is restored to
/// the captured operation — exact, because under the per-store lock every
/// local-view operation since is this call's own — before the command repeats.
pub(crate) fn run_store_rewrite(
    jj: &super::JjEnv,
    store: &Path,
    args: &[&str],
    ctx: &str,
) -> Result<String, String> {
    let pre_op = super::operation_id(jj, store)?;
    with_packed_refs_patience(PackedRefs::JjStore(store), ctx, || {
        let error = match jj.run(store, args, ctx) {
            Ok(output) => return Ok(output),
            Err(error) => error,
        };
        if !packed_refs_lock_error(&error) {
            return Err(error);
        }
        let now = super::operation_id(jj, store).map_err(|read| {
            unrepeatable_rewrite(
                ctx,
                &format!(
                    "the operation head could not be re-read to prove the attempt committed \
                     nothing ({})",
                    defuse_lock_phrasing(&read)
                ),
            )
        })?;
        if now == pre_op {
            return Err(error);
        }
        log::warn!(
            "{ctx}: an attempt that lost the packed-refs lock still committed operation {now}; \
             restoring {pre_op} before repeating"
        );
        super::restore_operation(jj, store, &pre_op).map_err(|restore| {
            log::error!(
                "{ctx}: restoring {pre_op} after a committed lost-lock attempt failed: {restore}"
            );
            unrepeatable_rewrite(
                ctx,
                &format!(
                    "it committed operation {now} before failing, and restoring {pre_op} also \
                     failed ({}). The store may hold that partial operation",
                    defuse_lock_phrasing(&restore)
                ),
            )
        })?;
        Err(error)
    })
}

/// The refusal for a lost-lock attempt that cannot safely be repeated. Its
/// text must never read as contention, or patience would repeat exactly what
/// it refuses to repeat and the classifier would file it as routine.
fn unrepeatable_rewrite(ctx: &str, why: &str) -> String {
    super::rollback_incomplete(&format!(
        "{ctx}: lost the ref-store lock and was not repeated, because {why}."
    ))
}

#[cfg(test)]
type StepHook = Box<dyn FnOnce()>;

#[cfg(test)]
thread_local! {
    static STEP_HOOKS: std::cell::RefCell<Vec<(&'static str, StepHook)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Test-only fault injection: run `hook` the next time this thread reaches
/// `step`. It is how a test takes the lock at an exact later stage of a
/// rewrite sequence rather than only before the first command.
#[cfg(test)]
pub(crate) fn at_step(step: &'static str, hook: impl FnOnce() + 'static) {
    STEP_HOOKS.with(|hooks| hooks.borrow_mut().push((step, Box::new(hook))));
}

/// A named point in a store rewrite sequence. Does nothing outside tests.
pub(crate) fn reached_step(step: &str) {
    #[cfg(test)]
    {
        let hook = STEP_HOOKS.with(|hooks| {
            let mut hooks = hooks.borrow_mut();
            let index = hooks.iter().position(|(name, _)| *name == step)?;
            Some(hooks.remove(index).1)
        });
        if let Some(hook) = hook {
            hook();
        }
    }
    #[cfg(not(test))]
    let _ = step;
}

/// Quote an error inside [`unrepeatable_rewrite`] without carrying the lock
/// phrasing [`packed_refs_lock_error`] keys on. The verbatim text goes to the
/// log at the call site.
fn defuse_lock_phrasing(error: &str) -> String {
    if packed_refs_lock_error(error) {
        "it too lost the ref-store lock; the full error is in the log".to_string()
    } else {
        error.to_string()
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

    /// A rewrite that committed and then could not be put back must stop, and
    /// say so in words that neither patience nor the reconcile classifier read
    /// as routine contention — even when the restore's own failure was a lock.
    #[test]
    fn an_unrepeatable_rewrite_never_reads_as_contention() {
        let restore = "jj op restore failed: The lock for the packed-ref file could not be \
                       obtained. The lockfile at '/r/.git/packed-refs.lock' might need manual \
                       deletion.";
        let refusal = unrepeatable_rewrite(
            "jj rebase",
            &format!(
                "restoring op1 also failed ({})",
                defuse_lock_phrasing(restore)
            ),
        );
        assert!(!packed_refs_lock_error(&refusal), "{refusal}");
        assert!(!refusal.contains(CONTENTION_DIAGNOSIS), "{refusal}");
        assert_eq!(
            super::super::reconcile_failure_kind(&refusal),
            super::super::ROLLBACK_FAILED,
            "a store that may hold a partial operation is reported, not quietly retried"
        );
        assert!(refusal.contains("was not repeated"), "{refusal}");
        assert_eq!(
            defuse_lock_phrasing("Revision x does not exist"),
            "Revision x does not exist",
            "an unrelated error is quoted verbatim"
        );
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

    fn checkout_with_lock() -> (TempDir, PathBuf, PathBuf) {
        let temp = TempDir::new().unwrap();
        let checkout = temp.path().join("repo");
        std::fs::create_dir_all(checkout.join(".git")).unwrap();
        let lock = checkout.join(".git").join("packed-refs.lock");
        (temp, checkout, lock)
    }

    /// The give-up line separates the three situations an operator must tell
    /// apart: a burst of writers that left nothing behind, a live holder, and
    /// an abandoned lock nothing removed.
    #[test]
    fn the_give_up_line_says_burst_holder_or_stale() {
        let (_temp, checkout, lock) = checkout_with_lock();
        let scope = PackedRefs::Checkout(&checkout);

        let burst = give_up_diagnosis(scope, None);
        assert!(burst.starts_with(CONTENTION_DIAGNOSIS), "{burst}");
        assert!(burst.contains("no lock at give-up"), "{burst}");

        std::fs::write(&lock, "fresh\n").unwrap();
        let fresh = give_up_diagnosis(scope, None);
        assert!(fresh.starts_with(CONTENTION_DIAGNOSIS), "{fresh}");

        let old = std::time::SystemTime::now() - Duration::from_secs(120);
        std::fs::File::options()
            .write(true)
            .open(&lock)
            .unwrap()
            .set_modified(old)
            .unwrap();
        #[cfg(unix)]
        {
            let held = std::fs::File::open(&lock).unwrap();
            let live = give_up_diagnosis(scope, None);
            assert!(live.starts_with(CONTENTION_DIAGNOSIS), "{live}");
            assert!(
                live.contains(&format!("held by live process {}", std::process::id())),
                "names the holder: {live}"
            );
            drop(held);
        }
        let stale = give_up_diagnosis(scope, Some("the lock was replaced after proof"));
        assert!(stale.starts_with(STALE_DIAGNOSIS), "{stale}");
        assert!(
            stale.contains("not removed: the lock was replaced after proof"),
            "{stale}"
        );
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
