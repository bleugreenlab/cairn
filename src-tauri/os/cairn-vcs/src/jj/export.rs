//! HEAD-preserving, VERIFIED `jj git export`, and the publication boundary that
//! rests on it.
//!
//! Two independent jobs live here because both need the same fact — which
//! ordinary git checkout backs this store or workspace:
//!
//! 1. Repairing the checkout HEAD an export detaches (the original concern,
//!    documented below).
//! 2. Proving the export actually reached `refs/heads/*`, and repairing it when
//!    it did not. `jj git export` does NOT fail loudly when a ref moved outside
//!    jj: it refuses that one ref, reports it on stderr, and exits 0. The jj
//!    bookmark advances while the git ref freezes, so every later push carries a
//!    stale ref — how a PR half-landed on a tree that was never pushed. Nothing
//!    read that stderr, and nothing compared the resulting ref to the bookmark.
//!    [`export_git_verified`] does both, and [`verified_publish_target`] puts
//!    the check in front of every push.
//!
//! This module READS the coupling between the store's git backend and the
//! project checkout's `.git` in order to detect when it fails. It adds no new
//! write through that coupling beyond the one export repair jj itself performs.
//!
//! The shared jj store's git backend IS the project's `.git`, so any
//! `jj git export` that moves the branch the project checkout's HEAD is a symref
//! to (normally `refs/heads/main`) DETACHES that HEAD: jj cannot leave HEAD a
//! symref to a branch it is moving, so it pins HEAD at the pre-move commit (which
//! keeps the working tree clean). Left alone, the user's checkout sits in
//! detached HEAD after the export.
//!
//! [`export_git_preserving_checkout`] wraps the export and, when it can PROVE the
//! export itself caused the detach — HEAD was attached to `B` before, `B` moved
//! during the export, and HEAD is detached after — re-attaches HEAD to `B` and
//! fast-forwards the clean working tree. This is the one canonical repair,
//! invoked synchronously at the export choke point rather than in a deferred,
//! success-path-only cleanup. The pre/post attribution is what makes it safe to
//! run on EVERY export: it only ever repairs a detach the export caused, works
//! for non-default branches, and never touches a user-deliberate detached HEAD.
use super::*;
use crate::checkout::{reattach_checkout_head, CheckoutRepairPolicy};
use cairn_platform::services::{GitClient, RealGitClient};
use std::path::{Path, PathBuf};

/// The project checkout's HEAD attachment, snapshotted before an export. Only
/// captured when HEAD was ATTACHED (a pre-existing detached HEAD is the user's
/// own choice and is never repaired), so `branch` is always non-empty.
struct CheckoutHeadBefore {
    branch: String,
    branch_commit: String,
}

/// Run `jj git export` in `export_cwd` and repair the project checkout's HEAD if
/// the export detached it by moving the branch HEAD was attached to.
///
/// The export's own result semantics are preserved verbatim (callers propagate or
/// swallow it exactly as before); the HEAD repair is a best-effort side effect
/// that logs a warning on failure and never changes the returned result.
///
/// This form asserts nothing about WHICH bookmarks the export was meant to
/// publish, so it cannot detect a frozen ref. A caller that is advancing a
/// specific bookmark should call [`export_git_verified`] with that expectation
/// instead; callers with nothing specific to assert keep this behavior exactly.
pub fn export_git_preserving_checkout(
    jj: &JjEnv,
    export_cwd: &Path,
    ignore_working_copy: bool,
    ctx: &str,
) -> Result<(), String> {
    export_git_verified(jj, export_cwd, ignore_working_copy, ctx, &[])
}

/// [`export_git_preserving_checkout`] plus a proof that the export landed.
///
/// `expect` names the `(branch, commit)` pairs this export is supposed to have
/// written into the backing checkout's `refs/heads/*`. Each is re-read with
/// `git rev-parse` afterwards and compared. A disagreement is the export freeze:
/// it is logged at error with both commits and jj's own export stderr (the one
/// place jj reports the refusal, on an exit-0 run), then repaired, then
/// re-verified. An unrepairable freeze returns a typed error rather than `Ok`.
///
/// An empty `expect` is exactly today's behavior, which is what keeps the eight
/// existing export sites unchanged instead of fabricating expectations they do
/// not have.
pub fn export_git_verified(
    jj: &JjEnv,
    export_cwd: &Path,
    ignore_working_copy: bool,
    ctx: &str,
    expect: &[(&str, &str)],
) -> Result<(), String> {
    let checkout = resolve_backing_checkout(export_cwd);
    let git = RealGitClient;
    let before = checkout
        .as_deref()
        .and_then(|repo| snapshot_checkout_head(&git, repo));

    let exported = run_export(jj, export_cwd, ignore_working_copy, ctx);

    if let (Some(repo), Some(before)) = (checkout.as_deref(), before.as_ref()) {
        repair_export_detach(&git, repo, before, jj.checkout_repair_policy);
    }
    let export_stderr = exported?;

    if expect.is_empty() {
        return Ok(());
    }
    let Some(repo) = checkout.as_deref() else {
        // A caller that named an expectation asked to be PROVEN right. Without a
        // resolvable backing checkout there is no `refs/heads/*` to compare
        // against, so verification cannot run — and "could not check" must not be
        // reported as "checked and fine". Fail closed: publishing something we
        // cannot prove we exported is the failure this exists to prevent.
        return Err(format!(
            "{EXPORT_FREEZE_MSG} ({ctx}): cannot verify the exported ref(s) — no backing git \
             checkout resolved for {}",
            export_cwd.display()
        ));
    };
    verify_exported_refs(
        jj,
        &git,
        export_cwd,
        repo,
        ignore_working_copy,
        ctx,
        expect,
        &export_stderr,
    )
}

/// Run the export itself, returning jj's stderr on success. jj reports a refused
/// ref there while still exiting 0, so the stderr is the only in-band evidence
/// of a freeze and is carried into the verifier's diagnostics.
fn run_export(
    jj: &JjEnv,
    export_cwd: &Path,
    ignore_working_copy: bool,
    ctx: &str,
) -> Result<String, String> {
    let args: &[&str] = if ignore_working_copy {
        &["git", "export", "--ignore-working-copy"]
    } else {
        &["git", "export"]
    };
    // This is the leg that moves `refs/heads/<branch>` in the backing checkout,
    // so it contends for the same `packed-refs.lock` as every other
    // runner-owned ref transaction. Losing that race here is the expensive
    // failure: the store transaction has already committed, and
    // `publish_logical_head_exported` may not report a publication as committed
    // unless this leg succeeded — so an impatient export rolls back a patch that
    // already applied cleanly. Repeating is safe: `jj git export` is idempotent,
    // and `export_git_verified` re-reads the refs afterwards to prove it landed.
    super::with_packed_refs_patience(super::PackedRefs::JjStore(export_cwd), ctx, || {
        let stderr = jj
            .run_capturing_stderr(export_cwd, args, ctx)
            .map(|(_, stderr)| stderr)?;
        // Contention does not arrive here as an error. `jj git export` reports a
        // ref it could not write on stderr and still exits 0, so a lost race for
        // `packed-refs.lock` reaches this point looking like success while
        // `refs/heads/<branch>` never moved. Hand it back as the failure it is,
        // or the patience above has nothing to retry — and the export sites that
        // name no expectation would return `Ok` having exported nothing at all.
        if super::packed_refs_lock_error(&stderr) {
            return Err(stderr);
        }
        Ok(stderr)
    })
}

/// The commit `refs/heads/<branch>` currently holds in `repo`, or `None` when the
/// ref does not exist.
fn git_ref_commit(git: &dyn GitClient, repo: &Path, branch: &str) -> Option<String> {
    git.rev_parse(repo, vec![format!("refs/heads/{branch}")])
        .ok()
        .map(|commit| commit.trim().to_string())
        .filter(|commit| !commit.is_empty())
}

/// Compare each expectation against the backing checkout and repair a
/// disagreement. Returns the typed freeze error when a repair cannot close it.
#[allow(clippy::too_many_arguments)]
fn verify_exported_refs(
    jj: &JjEnv,
    git: &dyn GitClient,
    export_cwd: &Path,
    repo: &Path,
    ignore_working_copy: bool,
    ctx: &str,
    expect: &[(&str, &str)],
    export_stderr: &str,
) -> Result<(), String> {
    let frozen: Vec<(&str, &str, Option<String>)> = expect
        .iter()
        .filter_map(|(branch, commit)| {
            let actual = git_ref_commit(git, repo, branch);
            (actual.as_deref() != Some(*commit)).then_some((*branch, *commit, actual))
        })
        .collect();
    if frozen.is_empty() {
        return Ok(());
    }

    for (branch, expected, actual) in &frozen {
        log::error!(
            "{ctx}: jj→git export did not reach the backing checkout. bookmark `{branch}` is at \
             {expected}, but {}/refs/heads/{branch} is at {}. jj export stderr: {}",
            repo.display(),
            actual.as_deref().unwrap_or("<absent>"),
            if export_stderr.is_empty() {
                "<empty>"
            } else {
                export_stderr
            }
        );
    }

    repair_frozen_export(jj, export_cwd, ignore_working_copy, ctx, &frozen);

    let still_frozen: Vec<String> = frozen
        .iter()
        .filter_map(|(branch, expected, _)| {
            let actual = git_ref_commit(git, repo, branch);
            (actual.as_deref() != Some(*expected)).then(|| {
                format!(
                    "`{branch}` bookmark {expected} vs git ref {}",
                    actual.as_deref().unwrap_or("<absent>")
                )
            })
        })
        .collect();
    if still_frozen.is_empty() {
        log::info!(
            "{ctx}: repaired {} frozen git ref(s) after a silent jj export refusal",
            frozen.len()
        );
        return Ok(());
    }
    Err(format!(
        "{EXPORT_FREEZE_MSG} ({ctx}): {}",
        still_frozen.join("; ")
    ))
}

/// The proven repair for a frozen ref, in the order that actually works.
///
/// Re-running `jj git export` alone does NOT clear this state — measured on jj
/// 0.42, a ref moved outside jj stays refused indefinitely, because jj's `@git`
/// view still records the content it last wrote. The repair must first `jj git
/// import` so `@git` observes the ref where it really is (which turns the freeze
/// into a conflicted bookmark), then re-point the bookmark at the commit it is
/// supposed to publish, then export. Best-effort throughout: the verifier
/// re-reads the refs afterwards and is the sole judge of whether it worked.
fn repair_frozen_export(
    jj: &JjEnv,
    export_cwd: &Path,
    ignore_working_copy: bool,
    ctx: &str,
    frozen: &[(&str, &str, Option<String>)],
) {
    if let Err(error) = import_git(jj, export_cwd) {
        log::warn!("{ctx}: import while repairing a frozen export failed: {error}");
    }
    for (branch, expected, _) in frozen {
        if let Err(error) = jj.run(
            export_cwd,
            &[
                "bookmark",
                "set",
                branch,
                "-r",
                expected,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "jj bookmark set (frozen export repair)",
        ) {
            log::warn!("{ctx}: re-pointing `{branch}` to {expected} during repair failed: {error}");
        }
    }
    if let Err(error) = run_export(jj, export_cwd, ignore_working_copy, ctx) {
        log::warn!("{ctx}: re-export while repairing a frozen ref failed: {error}");
    }
}

/// Export after advancing `branch`, verified against the commit the store now
/// says that bookmark is.
///
/// The canonical call for "I just moved a bookmark; prove it reached git." Every
/// such site has this invariant available, so none of them should be exporting
/// blind: a frozen ref there is invisible precisely because jj exits 0, and the
/// stale ref does NOT self-heal on the next export — it stays refused until
/// something repairs jj's view of the git side.
///
/// FAILS CLOSED when the bookmark does not resolve to exactly one commit. Every
/// caller reaches this immediately after moving that specific bookmark, so an
/// absent bookmark, a conflicted name, or a resolver error all mean the same
/// thing: the postcondition this operation is contracted to establish cannot be
/// proven. Downgrading to an unverified export there would let a load-bearing
/// fold report success without proving its integration ref, precisely when
/// bookmark resolution is unhealthy — and deferring to "the reconciler or the
/// push will catch it" does not preserve THIS operation's contract, nor do all
/// callers publish. The resolver's own error is carried through, since "it is
/// conflicted", "it is gone", and "jj failed" are three different diagnoses.
pub(crate) fn export_bookmark_advance(
    jj: &JjEnv,
    cwd: &Path,
    ignore_working_copy: bool,
    branch: &str,
    ctx: &str,
) -> Result<(), String> {
    let commit = bookmark_commit_checked(jj, cwd, branch)
        .and_then(|commit| {
            commit.ok_or_else(|| {
                format!("bookmark `{branch}` does not exist in the store after being advanced")
            })
        })
        .map_err(|error| {
            format!(
                "{EXPORT_FREEZE_MSG} ({ctx}): cannot verify the export of `{branch}`, because the \
                 bookmark this operation just advanced does not resolve to a single commit: {error}"
            )
        })?;
    export_git_verified(
        jj,
        cwd,
        ignore_working_copy,
        ctx,
        &[(branch, commit.as_str())],
    )
}

/// The commit a push of `branch` is about to publish, with the backing git ref
/// PROVEN to match it first.
///
/// This is the check that would have caught the half-landed PR: the tests ran on
/// a tree the push never carried, because the git ref had frozen behind the
/// bookmark and nothing compared them. Returns `None` when there is no bookmark
/// to publish (the caller's push is then jj's own concern); refuses outright when
/// the bookmark NAME is conflicted, since "the commit to publish" is not a
/// single answer in that state and pushing would publish an arbitrary side.
pub(crate) fn verified_publish_target(
    jj: &JjEnv,
    cwd: &Path,
    branch: &str,
) -> Result<Option<String>, String> {
    if bookmark_name_is_conflicted(jj, cwd, branch)? {
        return Err(format!(
            "refusing to push `{branch}`: its bookmark name is conflicted in the store, so the \
             commit to publish is ambiguous. Reconcile the bookmark against its remote first."
        ));
    }
    // `Ok(None)` is the one benign answer: the bookmark is genuinely absent, so
    // there is nothing to publish. A resolver FAILURE is propagated rather than
    // read as absence, or a push would proceed unverified and skip its post-push
    // origin confirmation on exactly the runs where the store cannot answer.
    let Some(commit) = bookmark_commit_checked(jj, cwd, branch)? else {
        return Ok(None);
    };
    export_git_verified(
        jj,
        cwd,
        true,
        &format!("publish `{branch}`: verify exported ref"),
        &[(branch, commit.as_str())],
    )?;
    Ok(Some(commit))
}

/// Confirm origin's tip for `branch` equals `expected` after a push.
///
/// The push-side half of the same question: a push that reported success while
/// origin gained no such branch is exactly the phantom-PR specimen.
///
/// Fails closed in every direction, including when the probe itself cannot run.
/// This is a publication path, and "could not confirm" is not "confirmed" — a
/// caller that treats an unverified publish as a successful one is back to
/// trusting an exit code, which is the thing that produced a create-pr artifact
/// for a pull request that did not exist. Note the probe follows a push that just
/// succeeded over the same network to the same remote, so being unable to reach
/// origin here is genuinely anomalous rather than routine.
pub(crate) fn confirm_origin_tip(cwd: &Path, branch: &str, expected: &str) -> Result<(), String> {
    let Some(repo) = resolve_backing_checkout(cwd) else {
        return Err(format!(
            "cannot confirm the published tip of `{branch}`: no backing git checkout resolved for {}",
            cwd.display()
        ));
    };
    let git = RealGitClient;
    let output = git
        .run(
            &repo,
            vec![
                "ls-remote".to_string(),
                "--heads".to_string(),
                "origin".to_string(),
                format!("refs/heads/{branch}"),
            ],
        )
        .map_err(|error| {
            format!(
                "cannot confirm the published tip of `{branch}`: ls-remote failed to run: {error}"
            )
        })?;
    if !output.success {
        return Err(format!(
            "cannot confirm the published tip of `{branch}`: ls-remote could not reach origin: {}",
            output.stderr.trim()
        ));
    }
    let actual = output
        .stdout
        .lines()
        .find_map(|line| line.split_whitespace().next())
        .map(str::to_string);
    match actual {
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => Err(format!(
            "push of `{branch}` reported success but origin's tip is {actual}, not the {expected} \
             that was published"
        )),
        None => Err(format!(
            "push of `{branch}` reported success but origin has no `{branch}` branch"
        )),
    }
}

/// Resolve the primary project checkout backing the jj store/workspace at
/// `export_cwd`, or `None` when the topology cannot be resolved (best-effort:
/// an unresolvable checkout simply skips the HEAD repair).
///
/// A jj store's `.jj/repo` is the repo directory itself; a workspace's `.jj/repo`
/// is a file naming the shared store's repo directory (relative to that file).
/// The store's git backend records the project's `.git` in `store/git_target`,
/// and Cairn always inits the store against the project's MAIN checkout, so the
/// checkout is the worktree that owns that `.git` — its parent directory.
pub(crate) fn resolve_backing_checkout(export_cwd: &Path) -> Option<PathBuf> {
    let repo_pointer = export_cwd.join(".jj").join("repo");
    let store_repo = if repo_pointer.is_dir() {
        repo_pointer
    } else {
        let target = std::fs::read_to_string(&repo_pointer).ok()?;
        let target = PathBuf::from(target.trim());
        if target.is_absolute() {
            target
        } else {
            repo_pointer.parent()?.join(target)
        }
    };

    let git_target_file = store_repo.join("store").join("git_target");
    let raw = std::fs::read_to_string(&git_target_file).ok()?;
    let git_dir = PathBuf::from(raw.trim());
    let git_dir = if git_dir.is_absolute() {
        git_dir
    } else {
        git_target_file.parent()?.join(git_dir)
    };

    let checkout = git_dir.parent()?.to_path_buf();
    // Guard against a topology that isn't a `<worktree>/.git` layout (e.g. a bare
    // backend): only a real worktree can carry a detached HEAD to repair.
    git_dir.exists().then_some(checkout)
}

/// Snapshot the checkout's HEAD attachment before the export. Returns `None` when
/// HEAD is already detached (the user's own choice, never repaired) or when the
/// probe fails (best-effort: skip the repair).
fn snapshot_checkout_head(git: &dyn GitClient, repo: &Path) -> Option<CheckoutHeadBefore> {
    // Read from disk first: every export runs this under the store lock, and on
    // a loaded host each `git` spawn costs about a second. The subprocess probe
    // stays as the fallback for a layout the reader does not understand.
    let branch = match read_checkout_head(repo) {
        CheckoutHead::Detached => return None,
        CheckoutHead::Attached(branch) => branch,
        CheckoutHead::Unreadable => {
            // `git branch --show-current` is empty exactly when HEAD is detached.
            let branch = git.current_branch(repo).ok()?;
            if branch.is_empty() {
                return None;
            }
            branch
        }
    };
    let branch_commit = match read_branch_commit(repo, &branch) {
        Some(commit) => commit,
        None => git
            .rev_parse(repo, vec![format!("refs/heads/{branch}")])
            .ok()?
            .trim()
            .to_string(),
    };
    Some(CheckoutHeadBefore {
        branch,
        branch_commit,
    })
}

/// What a checkout's `HEAD` says, read from disk.
#[derive(Debug, PartialEq, Eq)]
enum CheckoutHead {
    /// A symref to `refs/heads/<branch>`.
    Attached(String),
    /// An object id.
    Detached,
    /// Not an ordinary `.git` directory, or not a form read here (a symref
    /// outside `refs/heads/`, a reftable repository): ask git instead.
    Unreadable,
}

fn read_checkout_head(repo: &Path) -> CheckoutHead {
    let dot_git = repo.join(".git");
    if !dot_git.is_dir() || dot_git.join("reftable").exists() {
        return CheckoutHead::Unreadable;
    }
    let Ok(text) = std::fs::read_to_string(dot_git.join("HEAD")) else {
        return CheckoutHead::Unreadable;
    };
    let text = text.trim();
    match text.strip_prefix("ref:") {
        Some(target) => match target.trim().strip_prefix("refs/heads/") {
            Some(branch) if !branch.is_empty() => CheckoutHead::Attached(branch.to_string()),
            _ => CheckoutHead::Unreadable,
        },
        None if is_object_id(text) => CheckoutHead::Detached,
        None => CheckoutHead::Unreadable,
    }
}

fn is_object_id(text: &str) -> bool {
    matches!(text.len(), 40 | 64) && text.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// The commit `refs/heads/<branch>` holds, read from the loose ref or, failing
/// that, `packed-refs`. `None` when neither names it, which the caller treats
/// as "ask git" rather than "absent".
fn read_branch_commit(repo: &Path, branch: &str) -> Option<String> {
    let dot_git = repo.join(".git");
    let name = format!("refs/heads/{branch}");
    if let Ok(text) = std::fs::read_to_string(dot_git.join(&name)) {
        let text = text.trim();
        return is_object_id(text).then(|| text.to_string());
    }
    let packed = std::fs::read_to_string(dot_git.join("packed-refs")).ok()?;
    packed.lines().find_map(|line| {
        let (id, reference) = line.split_once(' ')?;
        (reference == name && is_object_id(id)).then(|| id.to_string())
    })
}

/// Repair a detach the export caused, and ONLY that: HEAD must be detached now,
/// and the branch it was attached to must have actually moved during the export.
/// Any other post-state (still attached, or the branch did not move) is left
/// untouched. Best-effort throughout — a probe failure logs and returns.
fn repair_export_detach(
    git: &dyn GitClient,
    repo: &Path,
    before: &CheckoutHeadBefore,
    policy: CheckoutRepairPolicy,
) {
    let now = match read_checkout_head(repo) {
        CheckoutHead::Attached(branch) => branch,
        CheckoutHead::Detached => String::new(),
        CheckoutHead::Unreadable => match git.current_branch(repo) {
            Ok(branch) => branch,
            Err(e) => {
                log::warn!(
                    "post-export HEAD check failed for checkout {}: {e}",
                    repo.display()
                );
                return;
            }
        },
    };
    if !now.is_empty() {
        // Still attached: the export did not detach HEAD.
        return;
    }

    let after_commit = match git.rev_parse(repo, vec![format!("refs/heads/{}", before.branch)]) {
        Ok(commit) => commit.trim().to_string(),
        Err(e) => {
            log::warn!(
                "post-export ref read failed for checkout {}: {e}",
                repo.display()
            );
            return;
        }
    };
    if after_commit == before.branch_commit {
        // The branch did not move, so this export did not cause the detach; leave
        // HEAD alone rather than attribute an unrelated detached state to it.
        return;
    }

    if let Err(e) = reattach_checkout_head(git, repo, &before.branch, policy) {
        log::warn!("failed to re-attach checkout HEAD after export detached it: {e}");
    }
}

#[cfg(test)]
mod head_reader_tests {
    use super::*;

    const A: &str = "1111111111111111111111111111111111111111";
    const B: &str = "2222222222222222222222222222222222222222";

    fn checkout() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join(".git/refs/heads/feature")).unwrap();
        temp
    }

    #[test]
    fn head_is_read_attached_detached_or_unreadable() {
        let temp = checkout();
        let head = temp.path().join(".git/HEAD");
        std::fs::write(&head, "ref: refs/heads/main\n").unwrap();
        assert_eq!(
            read_checkout_head(temp.path()),
            CheckoutHead::Attached("main".into())
        );
        std::fs::write(&head, format!("{A}\n")).unwrap();
        assert_eq!(read_checkout_head(temp.path()), CheckoutHead::Detached);
        std::fs::write(&head, "ref: refs/remotes/origin/main\n").unwrap();
        assert_eq!(read_checkout_head(temp.path()), CheckoutHead::Unreadable);
        std::fs::create_dir(temp.path().join(".git/reftable")).unwrap();
        std::fs::write(&head, "ref: refs/heads/main\n").unwrap();
        assert_eq!(
            read_checkout_head(temp.path()),
            CheckoutHead::Unreadable,
            "a reftable repository's refs are not files; ask git"
        );
    }

    #[test]
    fn a_branch_commit_is_read_from_the_loose_ref_before_packed_refs() {
        let temp = checkout();
        let git = temp.path().join(".git");
        std::fs::write(
            git.join("packed-refs"),
            format!("# pack-refs with: peeled fully-peeled sorted \n{A} refs/heads/main\n{B} refs/heads/feature/x\n^{A}\n"),
        )
        .unwrap();
        assert_eq!(read_branch_commit(temp.path(), "main").as_deref(), Some(A));
        assert_eq!(
            read_branch_commit(temp.path(), "feature/x").as_deref(),
            Some(B)
        );
        std::fs::write(git.join("refs/heads/main"), format!("{B}\n")).unwrap();
        assert_eq!(
            read_branch_commit(temp.path(), "main").as_deref(),
            Some(B),
            "a loose ref is newer than its packed copy"
        );
        assert_eq!(read_branch_commit(temp.path(), "absent"), None);
    }

    /// The disk reader and git agree on a real repository.
    #[test]
    fn the_disk_reader_matches_git() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(repo)
                .output()
                .unwrap();
            assert!(output.status.success(), "git {args:?}");
            String::from_utf8(output.stdout).unwrap().trim().to_string()
        };
        git(&["init", "-q", "-b", "main"]);
        git(&[
            "-c",
            "user.email=a@b",
            "-c",
            "user.name=a",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "one",
        ]);
        let head = git(&["rev-parse", "refs/heads/main"]);
        assert_eq!(
            read_checkout_head(repo),
            CheckoutHead::Attached("main".into())
        );
        assert_eq!(read_branch_commit(repo, "main"), Some(head.clone()));
        git(&["pack-refs", "--all"]);
        assert_eq!(read_branch_commit(repo, "main"), Some(head));
        git(&["checkout", "-q", "--detach"]);
        assert_eq!(read_checkout_head(repo), CheckoutHead::Detached);
    }
}
