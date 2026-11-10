//! Exact-lease publication of store refs to origin, atomic across refs.
//!
//! A landing publishes the target (the default or integration branch) and,
//! for a PR landing, the source branch too, all to one landing commit built on
//! the exact target commit `T` that validation judged. Two independent pushes
//! cannot carry that: the first can land while the second is refused, and
//! nothing makes the target's old value `T` rather than whatever origin
//! advertised at push time. This module is the one transport for it.
//!
//! The wire is one `git push --porcelain` with an explicit
//! `--force-with-lease=<ref>:<expected-old>` per ref (and `--atomic` when there
//! is more than one), run in the store's backing checkout. Each lease is an
//! exact compare-and-swap the receiver applies inside its ref transaction, and
//! `--atomic` makes that one transaction: every ref moves or none does. A
//! lease is not an unconditional overwrite: a ref whose origin value differs
//! from the expected old is refused. The target's fast-forward is proven
//! locally before anything is sent, because a matched lease waives git's own
//! fast-forward check.
//!
//! The push goes to origin's resolved push URL, not to the remote name. Pushing
//! to the name makes git update `refs/remotes/origin/<branch>` on success, and
//! the next store import would fold that into the tracked local bookmark:
//! the live branch would move before its owner decided to move it. Sending
//! leaves every local ref alone; recording origin's new value is
//! [`record_leased_push`]'s job alone.
//!
//! The phases are separate so a caller can hold the store lock only where the
//! store is touched:
//! - send (`*_unrecorded`): read-only local preconditions, then network. No
//!   store or ref write, including the probe that resolves an unknown outcome.
//! - [`readback_leased_push`]: network read only; origin still holds the heads.
//! - [`record_leased_push`]: local only, under the store lock; brings the
//!   store's `<branch>@origin` to the accepted commits.
//!
//! [`publish_landing_atomic`], [`push_bookmark_expecting`] and
//! [`push_refs_leased`] compose all three for callers that hold the lock
//! throughout and have already moved the store bookmarks.
//!
//! Measured on git 2.43 against local bare repositories, which the tests pin:
//! a lease mismatch rejects that ref `[rejected] (stale info)` and the others
//! `[rejected] (atomic push failed)`, leaving origin unchanged; a receiver that
//! does not advertise `atomic` fails before any ref command is sent; a
//! receiver-side refusal rejects every ref `[remote rejected]`; a ref already at
//! the new commit reports `=` and passes even with a stale lease, so re-sending
//! an accepted publication is idempotent; an empty lease means "must not
//! exist".
use super::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// One ref to move on origin under an exact lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedRefUpdate {
    /// Branch name, without `refs/heads/`.
    pub branch: String,
    /// The commit origin must hold for this ref to move, or `None` when origin
    /// must not have the branch at all.
    pub expected_old: Option<String>,
    /// The commit the ref moves to, or `None` to delete it (a [`LeasedRefRole::Retire`]).
    pub new: Option<String>,
    pub role: LeasedRefRole,
}

/// What the ref is to the publication, which decides the proof it needs and
/// how a lease mismatch on it is reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeasedRefRole {
    /// The branch a landing advances. `expected_old` is required and `new` must
    /// be proven to satisfy `proof` against it before anything is sent.
    Target(TargetProof),
    /// The PR head. It may move sideways (a squash replaces its history), so
    /// its only guard is the lease.
    Source,
    /// A branch being deleted from origin. `new` is `None`, `expected_old` is
    /// required, and the lease on it is the only guard: origin's copy is
    /// removed only while it is still exactly that commit.
    Retire,
    /// A managed branch the store publishes as it stands (an agent branch, an
    /// integration branch): no landing proof, and `expected_old` may be `None`
    /// for its first publication. Origin's copy only fast-forwards, unless the
    /// send carries accepted evidence and the supersession proof passes.
    Managed,
}

/// The independent local proof that a target update is the landing that was
/// validated, and therefore a fast-forward.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetProof {
    /// The default squash landing: `new` has exactly one parent, the expected
    /// old target, and its tree is the validated tree.
    SquashOnto { tree: String },
    /// `new` descends from the expected old target.
    Descends,
}

/// Heads origin accepted, in request order. This is the durable record a
/// caller keeps to read back and record a publication; a caller that persists
/// it must persist each `expected_old` too, because recording judges every
/// local view against it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedHeads {
    pub refs: Vec<AcceptedRef>,
}

/// One ref origin accepted: it moved from `expected_old` to `commit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedRef {
    pub branch: String,
    pub expected_old: Option<String>,
    /// `None` for a deletion origin accepted.
    pub commit: Option<String>,
}

fn render(commit: &Option<String>) -> &str {
    commit.as_deref().unwrap_or("<absent>")
}

impl AcceptedHeads {
    fn of(updates: &[&LeasedRefUpdate]) -> Self {
        Self {
            refs: updates
                .iter()
                .map(|update| AcceptedRef {
                    branch: update.branch.clone(),
                    expected_old: update.expected_old.clone(),
                    commit: update.new.clone(),
                })
                .collect(),
        }
    }
}

/// A publication origin accepted, from the composed (recording) entry points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeasedPush {
    /// Origin accepted every ref, a read-back confirmed it, and the store
    /// recorded origin's new values.
    Accepted(AcceptedHeads),
    /// Origin accepted every ref, but read-back or recording did not complete.
    /// The publication HAPPENED: keep `heads` and retry
    /// [`confirm_leased_push`]; never treat this as a failure or roll back.
    AcceptedUnconfirmed { heads: AcceptedHeads, error: String },
}

impl LeasedPush {
    pub fn heads(&self) -> &AcceptedHeads {
        match self {
            Self::Accepted(heads) | Self::AcceptedUnconfirmed { heads, .. } => heads,
        }
    }
}

/// A publication origin did not accept, or whose fate is unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeasedPushError {
    /// A local precondition failed; nothing was sent.
    Precondition(String),
    /// The receiver cannot apply several refs atomically; nothing was sent.
    /// There is no fallback to independent pushes.
    AtomicUnsupported(String),
    /// The target's origin value was not the expected old commit. Origin is
    /// unchanged. This is a base move, not a defect of the source: re-prepare
    /// against the new target and validate again. Reported in preference to a
    /// source move when both refs moved.
    TargetMoved {
        branch: String,
        expected: Option<String>,
        /// Origin's value at the post-refusal probe; `None` when the branch is
        /// absent there or the probe could not run.
        observed: Option<String>,
    },
    /// The source's origin value was not the expected old commit (someone
    /// pushed to the PR head). Origin is unchanged.
    SourceMoved {
        branch: String,
        expected: Option<String>,
        observed: Option<String>,
    },
    /// A branch being retired was not the expected commit on origin. Origin is
    /// unchanged: its copy stays.
    RetireMoved {
        branch: String,
        expected: Option<String>,
        observed: Option<String>,
    },
    /// The receiver refused the update for another reason (a hook, branch
    /// protection). Origin is unchanged.
    Rejected(String),
    /// Origin's expected target is not an ancestor of the landing (the store
    /// rewrote the target since origin last took it), and the publication did
    /// not allow a rewrite. Nothing was sent.
    RemoteNotAncestor {
        branch: String,
        remote: String,
        landing: String,
    },
    /// A rewrite of origin's target was allowed, but the supersession proof did
    /// not establish that origin loses only what this store replaced or
    /// dropped. Nothing was sent.
    RemoteRewriteUnproven {
        branch: String,
        remote: String,
        landing: String,
        reason: String,
    },
    /// The push did not report an outcome (transport failure, timeout) and a
    /// probe did not show the publication on origin. Origin may or may not hold
    /// it: read back before deciding.
    Unknown(String),
}

impl std::fmt::Display for LeasedPushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = |value: &Option<String>| value.clone().unwrap_or_else(|| "<absent>".into());
        match self {
            Self::Precondition(error) => write!(f, "leased push refused before sending: {error}"),
            Self::AtomicUnsupported(error) => write!(
                f,
                "origin cannot apply the landing's refs atomically, so nothing was pushed: {error}"
            ),
            Self::TargetMoved {
                branch,
                expected,
                observed,
            } => write!(
                f,
                "target moved: `{branch}` on origin is {}, not the {} this landing was validated \
                 against; nothing was pushed",
                name(observed),
                name(expected)
            ),
            Self::SourceMoved {
                branch,
                expected,
                observed,
            } => write!(
                f,
                "source moved: `{branch}` on origin is {}, not the expected {}; nothing was pushed",
                name(observed),
                name(expected)
            ),
            Self::RetireMoved {
                branch,
                expected,
                observed,
            } => write!(
                f,
                "not retired: `{branch}` on origin is {}, not the expected {}; it was left in place",
                name(observed),
                name(expected)
            ),
            Self::Rejected(error) => write!(f, "origin refused the publication: {error}"),
            Self::RemoteNotAncestor {
                branch,
                remote,
                landing,
            } => write!(
                f,
                "origin's `{branch}` ({remote}) is not an ancestor of the landing {landing}; \
                 publishing it would rewrite origin's history, so nothing was pushed"
            ),
            Self::RemoteRewriteUnproven {
                branch,
                remote,
                landing,
                reason,
            } => write!(
                f,
                "origin's `{branch}` ({remote}) is not proven superseded by the landing {landing}, \
                 so it was not rewritten and nothing was pushed: {reason}"
            ),
            Self::Unknown(error) => write!(
                f,
                "the publication's outcome is unknown (origin may hold it): {error}"
            ),
        }
    }
}

/// A landing: the source and target branches both moved to `landing`.
#[derive(Debug, Clone)]
pub struct LandingPublication {
    pub source_branch: String,
    /// The source's ORIGIN value when the landing was prepared; `None` when
    /// the source was never published.
    pub source_expected_old: Option<String>,
    pub target_branch: String,
    /// The exact target commit `T` the landing was built on and validated
    /// against (on origin, and in the store until the landing is recorded).
    pub target_expected_old: String,
    pub landing: String,
    pub proof: TargetProof,
    /// Whether origin's target may be REWRITTEN rather than fast-forwarded,
    /// and on what evidence. `None` (every default-branch landing) never
    /// rewrites: an origin target the landing does not descend from is
    /// refused. `Some(accepted)` (a managed integration branch the store
    /// rewrote) allows it only when [`origin_copy_is_superseded_with_accepted`]
    /// proves every commit origin's target holds that the prepared target lacks
    /// was replaced or dropped by this store; the push stays leased on exactly
    /// `target_expected_old`. Only [`publish_landing_atomic_unrecorded`], which
    /// knows the prepared target, can carry it.
    pub remote_rewrite: Option<Vec<AcceptedAdvanceDrops>>,
}

/// A remote target rewrite a send is allowed to make: the store's prepared
/// target and the accepted evidence the supersession proof may use.
struct RemoteRewrite<'a> {
    prepared_target: &'a str,
    accepted: &'a [AcceptedAdvanceDrops],
}

/// The bound on one supersession proof; the engine also bounds its walk.
const REMOTE_REWRITE_PROOF_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

impl LandingPublication {
    fn updates(&self) -> [LeasedRefUpdate; 2] {
        [
            LeasedRefUpdate {
                branch: self.source_branch.clone(),
                expected_old: self.source_expected_old.clone(),
                new: Some(self.landing.clone()),
                role: LeasedRefRole::Source,
            },
            LeasedRefUpdate {
                branch: self.target_branch.clone(),
                expected_old: Some(self.target_expected_old.clone()),
                new: Some(self.landing.clone()),
                role: LeasedRefRole::Target(self.proof.clone()),
            },
        ]
    }
}

/// What the store's local bookmark must hold for a send to proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LocalExpectation {
    /// Already moved to the commit being published, with the backing git ref
    /// proven to match (the composed, recording entry points); for a deletion,
    /// already deleted.
    AtNew,
    /// Still at this commit: nothing local moves until origin accepts.
    At(String),
}

// ── Send (network; no local writes) ─────────────────────────────────────────

/// Fast-forward origin's `bookmark` from exactly `expected_old` to `new`,
/// touching nothing local.
///
/// Preconditions, read-only and checked before sending: the store bookmark
/// still names exactly `expected_old`; `new` is a full commit id present in the
/// backing repository; `new` descends from `expected_old`. On `Ok`, origin
/// accepted: move the store bookmark, then [`record_leased_push`] under the
/// store lock (and [`readback_leased_push`] off it when confirmation is
/// wanted).
pub fn push_commit_expecting_unrecorded(
    jj: &JjEnv,
    store: &Path,
    bookmark: &str,
    expected_old: &str,
    new: &str,
) -> Result<AcceptedHeads, LeasedPushError> {
    send_leased(
        jj,
        store,
        &[(
            LeasedRefUpdate {
                branch: bookmark.to_string(),
                expected_old: Some(expected_old.to_string()),
                new: Some(new.to_string()),
                role: LeasedRefRole::Target(TargetProof::Descends),
            },
            LocalExpectation::At(expected_old.to_string()),
        )],
        None,
    )
}

/// Delete origin's `branch` only while it is exactly `expected_old`, touching
/// nothing local.
///
/// The store's own bookmark must already be deleted (the caller removes it
/// first, under the store lock, by compare-and-swap), so the branch cannot be
/// recreated by this store between the decision and the push. On `Ok` origin
/// no longer has the branch: [`readback_leased_push`] off the store lock
/// confirms its absence, and [`record_leased_push`] under it retires the
/// store's record of origin's copy. A retry after origin already deleted it is
/// accepted.
pub fn push_managed_branch_unrecorded(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    expected_old: Option<&str>,
    new: &str,
    accepted: Option<&[AcceptedAdvanceDrops]>,
) -> Result<AcceptedHeads, LeasedPushError> {
    send_leased(
        jj,
        store,
        &[(
            LeasedRefUpdate {
                branch: branch.to_string(),
                expected_old: expected_old.map(str::to_string),
                new: Some(new.to_string()),
                role: LeasedRefRole::Managed,
            },
            LocalExpectation::At(new.to_string()),
        )],
        accepted.map(|accepted| RemoteRewrite {
            prepared_target: new,
            accepted,
        }),
    )
}

pub fn retire_remote_branch_unrecorded(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    expected_old: &str,
) -> Result<AcceptedHeads, LeasedPushError> {
    send_leased(
        jj,
        store,
        &[(
            LeasedRefUpdate {
                branch: branch.to_string(),
                expected_old: Some(expected_old.to_string()),
                new: None,
                role: LeasedRefRole::Retire,
            },
            LocalExpectation::AtNew,
        )],
        None,
    )
}

/// [`publish_landing_atomic`]'s send phase: source and target in one atomic
/// push, touching nothing local.
///
/// The store's views and origin's are separate expectations. The store's
/// source bookmark must still be at `source_local_expected` and its target at
/// `target_local_expected` (the prepared target `T`, which may be ahead of
/// origin, or the store's own rewrite of what origin holds); the landing's
/// proof is judged against `T`. Origin is leased on exactly
/// `landing.source_expected_old` and `landing.target_expected_old` (`O`), and
/// `O` must be an ancestor of the landing, so origin's target only ever
/// fast-forwards. For a default-branch landing `T` and `O` are the same commit.
pub fn publish_landing_atomic_unrecorded(
    jj: &JjEnv,
    store: &Path,
    landing: &LandingPublication,
    source_local_expected: &str,
    target_local_expected: &str,
) -> Result<AcceptedHeads, LeasedPushError> {
    let [source, target] = landing.updates();
    send_leased(
        jj,
        store,
        &[
            (
                source,
                LocalExpectation::At(source_local_expected.to_string()),
            ),
            (
                target,
                LocalExpectation::At(target_local_expected.to_string()),
            ),
        ],
        landing
            .remote_rewrite
            .as_deref()
            .map(|accepted| RemoteRewrite {
                prepared_target: target_local_expected,
                accepted,
            }),
    )
}

fn send_leased(
    jj: &JjEnv,
    store: &Path,
    updates: &[(LeasedRefUpdate, LocalExpectation)],
    rewrite: Option<RemoteRewrite<'_>>,
) -> Result<AcceptedHeads, LeasedPushError> {
    let repo = backing_checkout(store).map_err(LeasedPushError::Precondition)?;
    check_preconditions(jj, store, &repo, updates).map_err(LeasedPushError::Precondition)?;
    let updates: Vec<&LeasedRefUpdate> = updates.iter().map(|(update, _)| update).collect();
    refuse_remote_rewrite(jj, store, &repo, &updates, rewrite)?;
    send_refs(jj, &repo, &updates)
}

/// The remote half of a target's proof, independent of the store's: origin's
/// expected target must be an ancestor of the landing, so the leased update is
/// a fast-forward of origin's history whatever the store's target is, unless
/// the publication allowed a rewrite and the supersession proof establishes
/// that origin loses only what this store itself replaced or dropped.
fn refuse_remote_rewrite(
    jj: &JjEnv,
    store: &Path,
    repo: &Path,
    updates: &[&LeasedRefUpdate],
    rewrite: Option<RemoteRewrite<'_>>,
) -> Result<(), LeasedPushError> {
    for update in updates {
        if !matches!(
            update.role,
            LeasedRefRole::Target(_) | LeasedRefRole::Managed
        ) {
            continue;
        }
        let (Some(remote), Some(new)) = (update.expected_old.as_deref(), update.new.as_deref())
        else {
            continue;
        };
        if remote == new || git_is_ancestor(jj, repo, remote, new) {
            continue;
        }
        let Some(rewrite) = &rewrite else {
            return Err(LeasedPushError::RemoteNotAncestor {
                branch: update.branch.clone(),
                remote: remote.to_string(),
                landing: new.to_string(),
            });
        };
        let unproven = |reason: String| LeasedPushError::RemoteRewriteUnproven {
            branch: update.branch.clone(),
            remote: remote.to_string(),
            landing: new.to_string(),
            reason,
        };
        match origin_copy_is_superseded_with_accepted(
            jj,
            store,
            &update.branch,
            rewrite.prepared_target,
            remote,
            rewrite.accepted,
            REMOTE_REWRITE_PROOF_BUDGET,
        ) {
            Ok(true) => {}
            Ok(false) => {
                return Err(unproven(
                    "the store's tracked copy of origin's target is not the expected commit".into(),
                ))
            }
            Err(error) => return Err(unproven(error.to_string())),
        }
    }
    Ok(())
}

/// The one network core: push the updates and classify the outcome. Writes no
/// local ref; every probe only reads origin.
fn send_refs(
    jj: &JjEnv,
    repo: &Path,
    updates: &[&LeasedRefUpdate],
) -> Result<AcceptedHeads, LeasedPushError> {
    let heads = AcceptedHeads::of(updates);
    let url = run_git(
        jj,
        repo,
        &["remote", "get-url", "--push", "origin"],
        "git remote get-url --push origin",
    )
    .map_err(LeasedPushError::Precondition)?;
    let mut args = vec![
        "push".to_string(),
        "--porcelain".into(),
        // The store's pushes never run the user checkout's hooks (jj's own push
        // does not), and tags never ride along with a landing.
        "--no-verify".into(),
        "--no-follow-tags".into(),
    ];
    if updates.len() > 1 {
        args.push("--atomic".into());
    }
    for update in updates {
        args.push(format!(
            "--force-with-lease=refs/heads/{}:{}",
            update.branch,
            update.expected_old.as_deref().unwrap_or("")
        ));
    }
    args.push(url);
    for update in updates {
        args.push(format!(
            "{}:refs/heads/{}",
            update.new.as_deref().unwrap_or(""),
            update.branch
        ));
    }
    reached_step("leased push: preconditions proven");

    let output = match jj.command_output(
        cairn_platform::env::git().args(&args).current_dir(repo),
        JJ_NETWORK_TIMEOUT,
        "git push (leased)",
    ) {
        Ok(output) => output,
        Err(error) => return resolve_unknown(jj, repo, heads, error),
    };
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.contains("does not support --atomic") {
        return Err(LeasedPushError::AtomicUnsupported(stderr));
    }
    let statuses = parse_porcelain(&stdout);
    let status_of = |branch: &str| {
        let full = format!("refs/heads/{branch}");
        statuses.iter().find(|status| status.dst == full)
    };
    if output.status.success()
        && updates
            .iter()
            .all(|update| status_of(&update.branch).is_some_and(PorcelainStatus::accepted))
    {
        reached_step("leased push: accepted");
        return Ok(heads);
    }
    let rejections: Vec<&PorcelainStatus> = statuses
        .iter()
        .filter(|status| status.flag == '!')
        .collect();
    if rejections.is_empty() {
        let error = format!(
            "git push exited {} without reporting every ref: {}",
            output.status,
            if stderr.is_empty() {
                stdout.trim()
            } else {
                &stderr
            }
        );
        return resolve_unknown(jj, repo, heads, error);
    }
    classify_rejection(jj, repo, updates, &rejections, &stderr)
}

/// A push that reported no outcome. When origin already holds every head the
/// publication was accepted (by this push or an earlier one); otherwise its
/// state is not known to be either. Read-only.
fn resolve_unknown(
    jj: &JjEnv,
    repo: &Path,
    heads: AcceptedHeads,
    error: String,
) -> Result<AcceptedHeads, LeasedPushError> {
    match observe_origin_heads(jj, repo, &heads) {
        Ok(observed) if holds_every_head(&observed, &heads) => Ok(heads),
        Ok(observed) => Err(LeasedPushError::Unknown(format!(
            "{error}; origin after the push: {observed:?}; expected {:?}",
            heads.refs
        ))),
        Err(probe) => Err(LeasedPushError::Unknown(format!(
            "{error}; origin could not be probed afterwards: {probe}"
        ))),
    }
}

fn classify_rejection(
    jj: &JjEnv,
    repo: &Path,
    updates: &[&LeasedRefUpdate],
    rejections: &[&PorcelainStatus],
    stderr: &str,
) -> Result<AcceptedHeads, LeasedPushError> {
    // A lease can also lose on the receiver after a matching advertisement
    // (`[remote rejected]`), and only origin's value now says whether that ref
    // moved. Read-only.
    let heads = AcceptedHeads::of(updates);
    let observed = observe_origin_heads(jj, repo, &heads).ok();
    // Origin already in exactly the requested state (a retry after an earlier
    // acceptance; git refuses to delete what is already gone) is the
    // postcondition, not a refusal.
    if observed
        .as_ref()
        .is_some_and(|observed| holds_every_head(observed, &heads))
    {
        return Ok(heads);
    }
    let moved = |update: &&LeasedRefUpdate| {
        let stale = rejections.iter().any(|status| {
            status.dst == format!("refs/heads/{}", update.branch)
                && status.summary.contains("stale info")
        });
        let now = observed
            .as_ref()
            .and_then(|observed| observed.get(update.branch.as_str()).cloned());
        let differs = observed.is_some() && now != update.expected_old;
        (stale || differs).then(|| (update.branch.clone(), update.expected_old.clone(), now))
    };
    let target = updates
        .iter()
        .filter(|update| {
            matches!(
                update.role,
                LeasedRefRole::Target(_) | LeasedRefRole::Managed
            )
        })
        .find_map(moved);
    if let Some((branch, expected, observed)) = target {
        return Err(LeasedPushError::TargetMoved {
            branch,
            expected,
            observed,
        });
    }
    let source = updates
        .iter()
        .filter(|update| update.role == LeasedRefRole::Source)
        .find_map(moved);
    if let Some((branch, expected, observed)) = source {
        return Err(LeasedPushError::SourceMoved {
            branch,
            expected,
            observed,
        });
    }
    let retired = updates
        .iter()
        .filter(|update| update.role == LeasedRefRole::Retire)
        .find_map(moved);
    if let Some((branch, expected, observed)) = retired {
        return Err(LeasedPushError::RetireMoved {
            branch,
            expected,
            observed,
        });
    }
    let reasons: Vec<String> = rejections
        .iter()
        .filter(|status| !status.summary.contains("atomic push failed"))
        .map(|status| format!("{} {}", status.dst, status.summary))
        .collect();
    Err(LeasedPushError::Rejected(if stderr.is_empty() {
        reasons.join("; ")
    } else {
        format!("{} ({stderr})", reasons.join("; "))
    }))
}

// ── Read back (network read) and record (local) ─────────────────────────────

/// Confirm origin holds every accepted head, or a descendant of it (a later
/// publication that carries it), and no longer has any branch accepted as
/// deleted. A network READ only; run it without the store lock.
pub fn readback_leased_push(jj: &JjEnv, store: &Path, heads: &AcceptedHeads) -> Result<(), String> {
    readback_leased_push_exact(jj, store, heads).map(|_| ())
}

/// [`readback_leased_push`] that also says whether origin holds EXACTLY every
/// accepted head (`Ok(true)`) or has since moved at least one past it to a
/// descendant (`Ok(false)`, still confirmed). One network read; run it without
/// the store lock.
pub fn readback_leased_push_exact(
    jj: &JjEnv,
    store: &Path,
    heads: &AcceptedHeads,
) -> Result<bool, String> {
    if let Some(error) = take_injected_confirmation_failure() {
        return Err(error);
    }
    let repo = backing_checkout(store)?;
    let observed = observe_origin_heads(jj, &repo, heads)?;
    let mut exact = true;
    for AcceptedRef { branch, commit, .. } in &heads.refs {
        let Some(commit) = commit else {
            if let Some(actual) = observed.get(branch) {
                return Err(format!(
                    "origin accepted the deletion of `{branch}` but now holds it at {actual}"
                ));
            }
            continue;
        };
        match observed.get(branch) {
            Some(actual) if actual == commit => {}
            Some(actual) if git_is_ancestor(jj, &repo, commit, actual) => exact = false,
            Some(actual) => {
                return Err(format!(
                    "origin accepted `{branch}` at {commit} but now holds {actual}, which does not \
                     carry it"
                ))
            }
            None => {
                return Err(format!(
                    "origin accepted `{branch}` at {commit} but now has no such branch"
                ))
            }
        }
    }
    Ok(exact)
}

/// What recording one accepted ref would do, judged before anything is written.
#[derive(Debug, PartialEq, Eq)]
enum RecordStep {
    /// The store already records the accepted commit.
    Done,
    /// A newer observation already supersedes the accepted commit (it carries
    /// it, or the tracking ref was retired); writing would rewind it.
    Superseded { by: String },
    /// Record it: move the tracking ref from exactly `tracking`, and the
    /// store's `<branch>@origin` from exactly `recorded`, to the commit.
    Write {
        tracking: Option<String>,
        recorded: Option<String>,
    },
}

/// Record accepted heads as the store's `<branch>@origin`. Local only, no
/// network: run it under the store lock, after origin accepted and after the
/// store bookmarks were moved to the accepted commits. Idempotent, and safe to
/// retry late.
///
/// Every ref is judged before anything is written, so a refusal writes
/// nothing. Each of the store's views of the branch (the tracking ref and the
/// recorded `<branch>@origin`) must be the expected old commit, the accepted
/// commit, or older than the accepted commit. A view that already carries the
/// accepted commit is newer evidence, and that ref is skipped. Any other view
/// is an intervening observation the record refuses to overwrite. The tracking
/// ref is then moved by compare-and-swap against the value judged, and only
/// then imported, so the import folds into the local bookmark exactly the move
/// that was judged.
pub fn record_leased_push(jj: &JjEnv, store: &Path, heads: &AcceptedHeads) -> Result<(), String> {
    let repo = backing_checkout(store)?;
    let mut writes = Vec::new();
    for accepted in &heads.refs {
        match judge_record(jj, store, &repo, accepted)? {
            RecordStep::Done => {}
            RecordStep::Superseded { by } => log::info!(
                "leased push record: `{}` at {} is superseded by {by}; left as it is",
                accepted.branch,
                render(&accepted.commit)
            ),
            RecordStep::Write { tracking, recorded } => writes.push((accepted, tracking, recorded)),
        }
    }
    if writes.is_empty() {
        return Ok(());
    }
    let git_writes: Vec<GitRefWrite> = writes
        .iter()
        .map(|(accepted, tracking, _)| GitRefWrite {
            name: format!("refs/remotes/origin/{}", accepted.branch),
            prior: tracking.clone(),
            written: accepted.commit.clone(),
            by_transaction: false,
        })
        .collect();
    let records: Vec<crate::OriginRecord> = writes
        .iter()
        .map(|(accepted, _, recorded)| crate::OriginRecord {
            branch: accepted.branch.clone(),
            expected: recorded.clone(),
            target: accepted.commit.clone(),
            advance_local_from: None,
        })
        .collect();
    record_with_rollback(
        jj,
        &repo,
        store,
        &git_writes,
        &records,
        "record origin's accepted heads",
    )
    .and_then(committed_or_unverified)
}

/// One git ref a record moves, and the value it held before.
#[derive(Debug, Clone)]
pub(crate) struct GitRefWrite {
    pub(crate) name: String,
    pub(crate) prior: Option<String>,
    pub(crate) written: Option<String>,
    /// Moved by the jj transaction's own export rather than before it.
    pub(crate) by_transaction: bool,
}

/// The one way a record crosses git and the jj store; see [`RecordOutcome`].
/// A failure before the commit restores git; an ambiguous commit is settled
/// by re-reading the store; anything after the commit is committed.
///
/// The two cannot be written atomically together, so the order and the
/// failure contract are fixed here. First every pre-written git ref moves by
/// compare-and-swap. Then the one refs-only jj transaction
/// ([`crate::record_origin_bookmarks`]) commits, and the result is verified.
/// If any step before the commit fails, every git ref already moved (including
/// one the transaction's own export moved) is put back by compare-and-swap.
/// A ref found at its prior value counts as restored. So a failed record
/// leaves git and the store as they were, and says so. Only when the restore
/// fails too is the error marked `PARTIAL`: git is then ahead of the store's
/// record, and the caller must retry this same record (it is idempotent and
/// re-reads every value) before anything imports. The caller holds the store
/// lock throughout.
pub(crate) fn record_with_rollback(
    jj: &JjEnv,
    repo: &Path,
    store: &Path,
    git_writes: &[GitRefWrite],
    records: &[crate::OriginRecord],
    description: &str,
) -> Result<RecordOutcome, String> {
    let pre_writes: Vec<&GitRefWrite> = git_writes.iter().filter(|w| !w.by_transaction).collect();
    for (done, write) in pre_writes.iter().enumerate() {
        if let Err(error) = cas_git_ref(jj, repo, &write.name, &write.written, &write.prior) {
            return Err(restore_after(jj, repo, &pre_writes[..done], error));
        }
    }
    let moved: Vec<&GitRefWrite> = git_writes.iter().collect();
    let committed = match take_injected_record_failure() {
        Some(error) => Err(crate::OriginRecordError::BeforeCommit(error)),
        None => crate::record_origin_bookmarks(store, records, description),
    };
    let mut note = None;
    match committed {
        Ok(()) => {}
        Err(crate::OriginRecordError::BeforeCommit(error)) => {
            return Err(restore_after(jj, repo, &moved, error))
        }
        // The commit may have landed: only the settled store says which, and
        // restoring git under a committed view would recreate the very
        // disagreement an import folds into the local bookmarks.
        Err(crate::OriginRecordError::AtCommit(error)) => match settled_records(store, records) {
            Ok(Settled::Targets) => {
                note = Some(format!(
                    "the commit reported an error but the store shows it committed: {error}"
                ))
            }
            Ok(Settled::Priors) => return Err(restore_after(jj, repo, &moved, error)),
            Ok(Settled::Mixed(view)) => {
                return Err(format!(
                    "PARTIAL RECORD: {error}; the store shows {view}, neither the record nor its \
                     prior, so the git refs were left as written. Retry this same record (it is \
                     idempotent) before anything imports"
                ))
            }
            Err(read) => {
                return Err(format!(
                    "PARTIAL RECORD: {error}; the store could not be re-read ({read}), so the git \
                     refs were left as written. Retry this same record (it is idempotent) before \
                     anything imports"
                ))
            }
        },
    }
    // Committed from here on: nothing is restored, and a failure to verify is
    // reported as committed-but-unverified, never as a refusal.
    if let Some(error) = take_injected_verify_failure() {
        return Ok(RecordOutcome::CommittedUnverified { note: error });
    }
    for record in records {
        match crate::recorded_origin_bookmark(store, &record.branch) {
            Ok((recorded, tracked))
                if recorded == record.target && (recorded.is_none() || tracked) => {}
            Ok((recorded, tracked)) => {
                return Ok(RecordOutcome::CommittedUnverified {
                    note: format!(
                        "origin holds `{}` at {}, but the store recorded `{}@origin` as {}{}",
                        record.branch,
                        render(&record.target),
                        record.branch,
                        render(&recorded),
                        if tracked { "" } else { " (untracked)" }
                    ),
                })
            }
            Err(read) => {
                return Ok(RecordOutcome::CommittedUnverified {
                    note: format!("the committed record could not be read back: {read}"),
                })
            }
        }
    }
    if let Some(note) = note {
        log::warn!("{description}: {note}");
    }
    Ok(RecordOutcome::Committed)
}

/// A record whose jj transaction committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecordOutcome {
    Committed,
    /// Committed, but reading it back failed or disagreed. The store did move;
    /// callers report it as done with a warning, never as a refusal.
    CommittedUnverified {
        note: String,
    },
}

/// The stable prefix a record function's error carries when its transaction
/// committed but could not be verified: the store DID record it.
pub const RECORD_COMMITTED_UNVERIFIED: &str = "COMMITTED (unverified)";

/// A record function's answer for callers that read it as done-or-not.
fn committed_or_unverified(outcome: RecordOutcome) -> Result<(), String> {
    match outcome {
        RecordOutcome::Committed => Ok(()),
        RecordOutcome::CommittedUnverified { note } => {
            Err(format!("{RECORD_COMMITTED_UNVERIFIED}: {note}"))
        }
    }
}

enum Settled {
    Targets,
    Priors,
    Mixed(String),
}

/// Where the store settled after an ambiguous commit: every record (and its
/// local bookmark, when the record advances it) at its target, at its prior,
/// or a mixture.
fn settled_records(store: &Path, records: &[crate::OriginRecord]) -> Result<Settled, String> {
    let (mut at_target, mut at_prior, mut views) = (true, true, Vec::new());
    for record in records {
        let (recorded, _) = crate::recorded_origin_bookmark(store, &record.branch)?;
        let local = match record.advance_local_from {
            Some(_) => Some(crate::local_bookmark_targets(store, &record.branch)?),
            None => None,
        };
        let local_at = |want: &Option<String>| match (&local, want) {
            (None, _) => true,
            (Some(local), Some(want)) => local.as_slice() == std::slice::from_ref(want),
            (Some(local), None) => local.is_empty(),
        };
        at_target &= recorded == record.target && local_at(&record.target);
        at_prior &= recorded == record.expected && local_at(&record.advance_local_from);
        views.push(format!(
            "`{}@origin` at {}{}",
            record.branch,
            render(&recorded),
            local
                .map(|local| format!(" and the bookmark at {}", local.join(" and ")))
                .unwrap_or_default()
        ));
    }
    Ok(if at_target {
        Settled::Targets
    } else if at_prior {
        Settled::Priors
    } else {
        Settled::Mixed(views.join(", "))
    })
}

/// Move `name` from exactly `prior` to `written` (`None` deletes; a `None`
/// prior means it must not exist), within the store's packed-refs patience.
fn cas_git_ref(
    jj: &JjEnv,
    repo: &Path,
    name: &str,
    written: &Option<String>,
    prior: &Option<String>,
) -> Result<(), String> {
    super::packed_refs::with_packed_refs_patience_for(
        jj,
        PackedRefs::Checkout(repo),
        "git update-ref (record)",
        || {
            match (written, prior) {
                (Some(written), prior) => run_git(
                    jj,
                    repo,
                    &["update-ref", name, written, prior.as_deref().unwrap_or("")],
                    "git update-ref (record)",
                ),
                (None, Some(prior)) => run_git(
                    jj,
                    repo,
                    &["update-ref", "-d", name, prior],
                    "git update-ref -d (record)",
                ),
                (None, None) => Ok(String::new()),
            }
            .map(|_| ())
        },
    )
}

/// Put back git refs a failed record moved, and say truthfully what is left.
fn restore_after(jj: &JjEnv, repo: &Path, moved: &[&GitRefWrite], error: String) -> String {
    let mut stuck = Vec::new();
    for write in moved.iter().rev() {
        let restored = match git_ref(jj, repo, &write.name) {
            Ok(current) if current == write.prior => Ok(()),
            Ok(current) if current == write.written => {
                cas_git_ref(jj, repo, &write.name, &write.prior, &write.written)
            }
            Ok(current) => Err(format!(
                "it is at {}, neither the written {} nor the prior {}",
                render(&current),
                render(&write.written),
                render(&write.prior)
            )),
            Err(read) => Err(read),
        };
        if let Err(why) = restored {
            stuck.push(format!("{} ({why})", write.name));
        }
    }
    if stuck.is_empty() {
        format!("{error}; the git refs it had moved were restored, so nothing is recorded")
    } else {
        format!(
            "PARTIAL RECORD: {error}; restoring {} failed, so git is ahead of the store's record. \
             Retry this same record (it is idempotent) before anything imports",
            stuck.join(", ")
        )
    }
}

/// Record what origin was just observed to hold for `branch` (fetched into the
/// backing repository, e.g. under `refs/cairn/origin-observed/`) as the
/// store's tracked `<branch>@origin`, without importing and without touching
/// the local bookmark, which must be exactly `local_expected`. Local only, no
/// network: run it under the store lock. The tracking ref moves by
/// compare-and-swap from the value read here.
pub fn record_origin_observation(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    observed: &str,
    local_expected: &str,
) -> Result<(), String> {
    let repo = backing_checkout(store)?;
    let local = crate::local_bookmark_targets(store, branch)?;
    if local.as_slice() != [local_expected] {
        return Err(format!(
            "refusing to record origin's `{branch}`: the store bookmark is {}, not the expected \
             {local_expected}",
            if local.is_empty() {
                "<absent>".to_string()
            } else {
                local.join(" and ")
            }
        ));
    }
    run_git(
        jj,
        &repo,
        &["cat-file", "-e", &format!("{observed}^{{commit}}")],
        "git cat-file -e (origin observation)",
    )
    .map_err(|error| {
        format!("origin's observed {observed} is not in the backing repository: {error}")
    })?;
    let (recorded, _) = crate::recorded_origin_bookmark(store, branch)?;
    let tracking = git_ref(jj, &repo, &format!("refs/remotes/origin/{branch}"))?;
    record_with_rollback(
        jj,
        &repo,
        store,
        &[GitRefWrite {
            name: format!("refs/remotes/origin/{branch}"),
            prior: tracking,
            written: Some(observed.to_string()),
            by_transaction: false,
        }],
        &[crate::OriginRecord {
            branch: branch.to_string(),
            expected: recorded,
            target: Some(observed.to_string()),
            advance_local_from: None,
        }],
        "record origin's observed head",
    )
    .and_then(committed_or_unverified)
}

fn judge_record(
    jj: &JjEnv,
    store: &Path,
    repo: &Path,
    accepted: &AcceptedRef,
) -> Result<RecordStep, String> {
    let AcceptedRef {
        branch,
        expected_old,
        commit,
    } = accepted;
    let tracking = git_ref(jj, repo, &format!("refs/remotes/origin/{branch}"))?;
    let (recorded, _) = crate::recorded_origin_bookmark(store, branch)?;
    let Some(commit) = commit else {
        return judge_retire_record(store, branch, expected_old, tracking, recorded);
    };
    if tracking.as_ref() == Some(commit) && recorded.as_ref() == Some(commit) {
        return Ok(RecordStep::Done);
    }
    for view in [&tracking, &recorded].into_iter().flatten() {
        if view != commit && git_is_ancestor(jj, repo, commit, view) {
            return Ok(RecordStep::Superseded { by: view.clone() });
        }
    }
    if tracking.is_none() && expected_old.is_some() {
        return Ok(RecordStep::Superseded {
            by: "<retired tracking ref>".into(),
        });
    }
    let admissible = |view: &Option<String>| match view {
        None => true,
        Some(view) => {
            view == commit
                || Some(view) == expected_old.as_ref()
                || git_is_ancestor(jj, repo, view, commit)
        }
    };
    for (name, view) in [("tracking ref", &tracking), ("recorded copy", &recorded)] {
        if !admissible(view) {
            return Err(format!(
                "refusing to record `{branch}@origin` at {commit}: its {name} is {}, an intervening \
                 observation that is neither the expected {} nor older than the accepted commit",
                view.as_deref().unwrap_or("<absent>"),
                expected_old.as_deref().unwrap_or("<absent>")
            ));
        }
    }
    let local = crate::local_bookmark_targets(store, branch)?;
    if local.as_slice() != std::slice::from_ref(commit) {
        return Err(format!(
            "refusing to record `{branch}@origin` at {commit}: the store bookmark is {}, not the \
             accepted commit; move it first so the import has nothing to fold into it",
            if local.is_empty() {
                "<absent>".to_string()
            } else {
                local.join(" and ")
            }
        ));
    }
    Ok(RecordStep::Write { tracking, recorded })
}

/// Recording an accepted deletion. Every view must still be the expected old
/// commit or already gone; a branch recreated since (a local bookmark, or a
/// tracking or recorded copy at another commit) belongs to whoever recreated
/// it, so the record refuses and writes nothing.
fn judge_retire_record(
    store: &Path,
    branch: &str,
    expected_old: &Option<String>,
    tracking: Option<String>,
    recorded: Option<String>,
) -> Result<RecordStep, String> {
    for (name, view) in [("tracking ref", &tracking), ("recorded copy", &recorded)] {
        if view.is_some() && view != expected_old {
            return Err(format!(
                "refusing to record the deletion of `{branch}@origin`: its {name} is {}, not the \
                 retired {}; the branch was recreated, and is left as it is",
                render(view),
                render(expected_old)
            ));
        }
    }
    let local = crate::local_bookmark_targets(store, branch)?;
    if !local.is_empty() {
        return Err(format!(
            "refusing to record the deletion of `{branch}@origin`: the store bookmark exists \
             again at {}; it is left as it is",
            local.join(" and ")
        ));
    }
    if tracking.is_none() && recorded.is_none() {
        return Ok(RecordStep::Done);
    }
    Ok(RecordStep::Write { tracking, recorded })
}

fn git_ref(jj: &JjEnv, repo: &Path, name: &str) -> Result<Option<String>, String> {
    let output = jj.command_output(
        cairn_platform::env::git()
            .args([
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("{name}^{{commit}}"),
            ])
            .current_dir(repo),
        JJ_DEFAULT_TIMEOUT,
        "git rev-parse (leased push record)",
    )?;
    let commit = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok((output.status.success() && !commit.is_empty()).then_some(commit))
}

/// [`readback_leased_push`] then [`record_leased_push`], for a caller holding
/// the store lock across both. Idempotent.
pub fn confirm_leased_push(jj: &JjEnv, store: &Path, heads: &AcceptedHeads) -> Result<(), String> {
    readback_leased_push(jj, store, heads)?;
    record_leased_push(jj, store, heads)
}

// ── Composed entry points (caller holds the store lock throughout) ──────────

/// Publish a landing whose store bookmarks already name `landing`: source and
/// target in one atomic, exactly leased push, then confirm and record.
pub fn publish_landing_atomic(
    jj: &JjEnv,
    store: &Path,
    landing: &LandingPublication,
) -> Result<LeasedPush, LeasedPushError> {
    if landing.remote_rewrite.is_some() {
        return Err(LeasedPushError::Precondition(
            "a rewrite of origin's target is published only through \
             publish_landing_atomic_unrecorded, which knows the prepared target"
                .into(),
        ));
    }
    push_refs_leased(jj, store, &landing.updates())
}

/// Fast-forward one bookmark already moved to `new` on origin from exactly
/// `expected_old`, then confirm and record.
pub fn push_bookmark_expecting(
    jj: &JjEnv,
    store: &Path,
    bookmark: &str,
    expected_old: &str,
    new: &str,
) -> Result<LeasedPush, LeasedPushError> {
    push_refs_leased(
        jj,
        store,
        &[LeasedRefUpdate {
            branch: bookmark.to_string(),
            expected_old: Some(expected_old.to_string()),
            new: Some(new.to_string()),
            role: LeasedRefRole::Target(TargetProof::Descends),
        }],
    )
}

/// The composed leased push for bookmarks already moved to their new commits.
/// A single update is sent without `--atomic` (one ref is one transaction, and
/// a receiver without the capability can still take it).
pub fn push_refs_leased(
    jj: &JjEnv,
    store: &Path,
    updates: &[LeasedRefUpdate],
) -> Result<LeasedPush, LeasedPushError> {
    let updates: Vec<(LeasedRefUpdate, LocalExpectation)> = updates
        .iter()
        .map(|update| (update.clone(), LocalExpectation::AtNew))
        .collect();
    let heads = send_leased(jj, store, &updates, None)?;
    Ok(match confirm_leased_push(jj, store, &heads) {
        Ok(()) => LeasedPush::Accepted(heads),
        Err(error) => {
            log::error!(
                "leased push: origin accepted {:?} but confirmation failed; the publication stands \
                 and must be confirmed again: {error}",
                heads.refs
            );
            LeasedPush::AcceptedUnconfirmed { heads, error }
        }
    })
}

// ── Preconditions (read-only) ───────────────────────────────────────────────

fn check_preconditions(
    jj: &JjEnv,
    store: &Path,
    repo: &Path,
    updates: &[(LeasedRefUpdate, LocalExpectation)],
) -> Result<(), String> {
    if updates.is_empty() {
        return Err("no refs to publish".into());
    }
    let mut seen = std::collections::HashSet::new();
    for (update, local) in updates {
        if update.branch.is_empty() || !seen.insert(update.branch.as_str()) {
            return Err(format!(
                "branch `{}` is empty or named twice",
                update.branch
            ));
        }
        match (&update.role, &update.new, &update.expected_old) {
            (LeasedRefRole::Retire, None, Some(_)) => {}
            (LeasedRefRole::Retire, _, _) => {
                return Err(format!(
                    "retiring `{}` needs no new commit and an exact expected old one",
                    update.branch
                ))
            }
            (_, None, _) => {
                return Err(format!(
                    "`{}` has no commit to publish; only a retirement deletes",
                    update.branch
                ))
            }
            _ => {}
        }
        let local_commit = match local {
            LocalExpectation::At(commit) => Some(commit),
            LocalExpectation::AtNew => None,
        };
        for commit in update
            .new
            .iter()
            .chain(update.expected_old.iter())
            .chain(local_commit)
        {
            if commit.len() < 40 || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(format!(
                    "`{}`: `{commit}` is not a full commit id; a lease must be exact",
                    update.branch
                ));
            }
        }
        let Some(new) = update.new.as_deref() else {
            // A retirement: the store's bookmark must already be gone, so this
            // store cannot recreate the branch behind the deletion.
            let local = crate::local_bookmark_targets(store, &update.branch)?;
            if !local.is_empty() || local_commit.is_some() {
                return Err(format!(
                    "the store bookmark `{}` still exists at {}; delete it before retiring \
                     origin's copy",
                    update.branch,
                    local.join(" and ")
                ));
            }
            continue;
        };
        match local {
            // The bookmark is what the store believes it published; this also
            // proves the backing git ref carries it (the export freeze check).
            LocalExpectation::AtNew => {
                let published = verified_publish_target(jj, store, &update.branch)?;
                if published.as_deref() != Some(new) {
                    return Err(format!(
                        "the store bookmark `{}` is {}, not the {new} to publish",
                        update.branch,
                        published.as_deref().unwrap_or("<absent>"),
                    ));
                }
            }
            LocalExpectation::At(expected) => {
                let targets = crate::local_bookmark_targets(store, &update.branch)?;
                if targets.as_slice() != std::slice::from_ref(expected) {
                    return Err(format!(
                        "the store bookmark `{}` is {}, not the {expected} this publication was \
                         prepared from",
                        update.branch,
                        if targets.is_empty() {
                            "<absent>".to_string()
                        } else {
                            targets.join(" and ")
                        }
                    ));
                }
                run_git(
                    jj,
                    repo,
                    &["cat-file", "-e", &format!("{new}^{{commit}}")],
                    "git cat-file -e (leased push object)",
                )
                .map_err(|error| {
                    format!(
                        "`{}`: the commit {new} to publish is not in the backing repository: \
                         {error}",
                        update.branch
                    )
                })?;
            }
        }
        if let LeasedRefRole::Target(proof) = &update.role {
            let Some(remote) = update.expected_old.as_deref() else {
                return Err(format!(
                    "target `{}` has no expected old commit to fast-forward from",
                    update.branch
                ));
            };
            // The landing is proven against the target it was prepared on: the
            // store's, when that is named, else origin's (the same commit for a
            // caller whose bookmark already moved to the landing).
            let prepared = match local {
                LocalExpectation::At(commit) => commit.as_str(),
                LocalExpectation::AtNew => remote,
            };
            prove_target(jj, repo, &update.branch, prepared, new, proof)?;
        }
    }
    Ok(())
}

fn prove_target(
    jj: &JjEnv,
    repo: &Path,
    branch: &str,
    old: &str,
    new: &str,
    proof: &TargetProof,
) -> Result<(), String> {
    match proof {
        TargetProof::Descends => {
            if !git_is_ancestor(jj, repo, old, new) {
                return Err(format!(
                    "`{new}` does not descend from `{branch}`'s expected {old}; that is not a \
                     fast-forward"
                ));
            }
        }
        TargetProof::SquashOnto { tree } => {
            let parents = run_git(
                jj,
                repo,
                &["rev-list", "--parents", "-n", "1", new],
                "git rev-list --parents",
            )?;
            let parents: Vec<&str> = parents.split_whitespace().skip(1).collect();
            if parents != [old] {
                return Err(format!(
                    "the squash landing {new} has parents {parents:?}, not exactly `{branch}`'s \
                     validated {old}"
                ));
            }
            let actual = run_git(
                jj,
                repo,
                &["rev-parse", &format!("{new}^{{tree}}")],
                "git rev-parse tree",
            )?;
            if actual != *tree {
                return Err(format!(
                    "the squash landing {new} carries tree {actual}, not the validated {tree}"
                ));
            }
        }
    }
    Ok(())
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn backing_checkout(store: &Path) -> Result<PathBuf, String> {
    super::export::resolve_backing_checkout(store)
        .ok_or_else(|| format!("no backing git checkout resolved for {}", store.display()))
}

fn holds_every_head(observed: &HashMap<String, String>, heads: &AcceptedHeads) -> bool {
    heads
        .refs
        .iter()
        .all(|accepted| observed.get(&accepted.branch) == accepted.commit.as_ref())
}

/// Origin's current commit for each head's branch (absent branches omitted).
/// A network read; writes nothing.
fn observe_origin_heads(
    jj: &JjEnv,
    repo: &Path,
    heads: &AcceptedHeads,
) -> Result<HashMap<String, String>, String> {
    let branches: Vec<&str> = heads
        .refs
        .iter()
        .map(|accepted| accepted.branch.as_str())
        .collect();
    let mut args = vec!["ls-remote".to_string(), "--heads".into(), "origin".into()];
    args.extend(branches.iter().map(|branch| format!("refs/heads/{branch}")));
    let output = jj.command_output(
        cairn_platform::env::git().args(&args).current_dir(repo),
        JJ_NETWORK_TIMEOUT,
        "git ls-remote (leased push)",
    )?;
    if !output.status.success() {
        return Err(format!(
            "ls-remote could not reach origin: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let (commit, name) = line.split_once('\t')?;
            let branch = name.trim().strip_prefix("refs/heads/")?;
            branches
                .contains(&branch)
                .then(|| (branch.to_string(), commit.trim().to_string()))
        })
        .collect())
}

fn run_git(jj: &JjEnv, repo: &Path, args: &[&str], ctx: &str) -> Result<String, String> {
    let output = jj.command_output(
        cairn_platform::env::git().args(args).current_dir(repo),
        JJ_DEFAULT_TIMEOUT,
        ctx,
    )?;
    if !output.status.success() {
        return Err(format!(
            "{ctx}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// One `git push --porcelain` ref line: `<flag>\t<src>:<dst>\t<summary>`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PorcelainStatus {
    flag: char,
    dst: String,
    summary: String,
}

impl PorcelainStatus {
    /// Fast-forward, forced, new, or already up to date: origin holds `new`.
    /// Fast-forward, forced, new, deleted, or already up to date.
    fn accepted(&self) -> bool {
        matches!(self.flag, ' ' | '+' | '*' | '-' | '=')
    }
}

fn parse_porcelain(stdout: &str) -> Vec<PorcelainStatus> {
    stdout
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, '\t');
            let flag = fields.next()?;
            let refspec = fields.next()?;
            let summary = fields.next().unwrap_or("");
            let mut chars = flag.chars();
            let flag = chars.next()?;
            if chars.next().is_some() {
                return None;
            }
            let (_, dst) = refspec.split_once(':')?;
            Some(PorcelainStatus {
                flag,
                dst: dst.to_string(),
                summary: summary.trim().to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
static INJECTED_CONFIRMATION_FAILURE: std::sync::Mutex<Option<String>> =
    std::sync::Mutex::new(None);

/// Fail the next read-back with `error`, after origin has accepted.
#[cfg(test)]
pub(crate) fn fail_next_confirmation(error: &str) {
    *INJECTED_CONFIRMATION_FAILURE.lock().unwrap() = Some(error.to_string());
}

#[cfg(test)]
fn take_injected_confirmation_failure() -> Option<String> {
    INJECTED_CONFIRMATION_FAILURE.lock().unwrap().take()
}

#[cfg(not(test))]
fn take_injected_confirmation_failure() -> Option<String> {
    None
}

#[cfg(test)]
static INJECTED_RECORD_FAILURE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Fail the next record's jj transaction with `error`, after its git refs
/// have moved: the window between the two stores.
#[cfg(test)]
pub(crate) fn fail_next_record_transaction(error: &str) {
    *INJECTED_RECORD_FAILURE.lock().unwrap() = Some(error.to_string());
}

#[cfg(test)]
fn take_injected_record_failure() -> Option<String> {
    INJECTED_RECORD_FAILURE.lock().unwrap().take()
}

#[cfg(not(test))]
fn take_injected_record_failure() -> Option<String> {
    None
}

#[cfg(test)]
static INJECTED_VERIFY_FAILURE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

#[cfg(test)]
fn take_injected_verify_failure() -> Option<String> {
    INJECTED_VERIFY_FAILURE.lock().unwrap().take()
}

#[cfg(not(test))]
fn take_injected_verify_failure() -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jj::testing::{git, git_stdout, init_project, jj_bin};
    use tempfile::TempDir;

    #[test]
    fn porcelain_lines_parse_into_flag_ref_and_summary() {
        let parsed = parse_porcelain(
            "To ../origin.git\n!\tabc:refs/heads/main\t[rejected] (stale info)\n=\tabc:refs/heads/src\t[up to date]\n \tabc:refs/heads/x\te6..82\nDone\n",
        );
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].flag, '!');
        assert_eq!(parsed[0].dst, "refs/heads/main");
        assert!(parsed[0].summary.contains("stale info"));
        assert!(parsed[1].accepted() && parsed[2].accepted());
    }

    /// A store whose `source` branch is published with real work and then
    /// squashed onto `main` locally: both store bookmarks name the landing,
    /// origin still holds the pre-landing values.
    struct Landing {
        _home: TempDir,
        project: TempDir,
        origin: TempDir,
        _workspaces: TempDir,
        jj: JjEnv,
        store: PathBuf,
        source: String,
        source_old: String,
        target_old: String,
        landing: String,
        tree: String,
    }

    impl Landing {
        fn publication(&self) -> LandingPublication {
            LandingPublication {
                source_branch: self.source.clone(),
                source_expected_old: Some(self.source_old.clone()),
                target_branch: "main".into(),
                target_expected_old: self.target_old.clone(),
                landing: self.landing.clone(),
                proof: TargetProof::SquashOnto {
                    tree: self.tree.clone(),
                },
                remote_rewrite: None,
            }
        }

        fn origin_head(&self, branch: &str) -> String {
            git_stdout(
                self.origin.path(),
                &["rev-parse", &format!("refs/heads/{branch}")],
            )
        }

        fn tracking_ref(&self, branch: &str) -> String {
            git_stdout(
                self.project.path(),
                &["rev-parse", &format!("refs/remotes/origin/{branch}")],
            )
        }

        fn set_bookmark(&self, branch: &str, commit: &str) {
            self.jj
                .run(
                    &self.store,
                    &[
                        "bookmark",
                        "set",
                        branch,
                        "-r",
                        commit,
                        "--allow-backwards",
                        "--ignore-working-copy",
                    ],
                    "test: set bookmark",
                )
                .unwrap();
        }

        /// The two-phase shape: the landing commit exists, but the store
        /// bookmarks are back at their pre-landing commits.
        fn unmove_bookmarks(&self) {
            self.set_bookmark("main", &self.target_old);
            self.set_bookmark(&self.source, &self.source_old);
        }

        /// Move origin's `branch` to a new commit on top of it, outside the
        /// store, as another landing would between observation and push.
        fn advance_origin(&self, branch: &str) -> String {
            let old = self.origin_head(branch);
            let tree = git_stdout(
                self.origin.path(),
                &["rev-parse", &format!("{old}^{{tree}}")],
            );
            let moved = git_stdout(
                self.origin.path(),
                &[
                    "-c",
                    "user.name=O",
                    "-c",
                    "user.email=o@e.com",
                    "commit-tree",
                    "-p",
                    &old,
                    "-m",
                    "outside",
                    &tree,
                ],
            );
            git(
                self.origin.path(),
                &["update-ref", &format!("refs/heads/{branch}"), &moved],
            );
            moved
        }
    }

    fn landing_fixture() -> Option<Landing> {
        let bin = jj_bin()?;
        let home = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        let origin = TempDir::new().unwrap();
        let workspaces = TempDir::new().unwrap();
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(project.path());
        git(
            project.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(project.path(), &["push", "-q", "origin", "main"]);
        let jj = JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("landing");
        ensure_project_store(&jj, &store, project.path()).unwrap();
        let source = "agent/cairn-5387-builder-1".to_string();
        let workspace = workspaces.path().join("builder");
        add_workspace(&jj, &store, &workspace, &source, "main", None).unwrap();
        std::fs::write(workspace.join("one.txt"), "one\n").unwrap();
        seal(&jj, &workspace, "first change", None).unwrap();
        std::fs::write(workspace.join("two.txt"), "two\n").unwrap();
        seal(&jj, &workspace, "second change", None).unwrap();
        export_bookmark_advance(&jj, &store, true, &source, "landing fixture").unwrap();
        let _ = track_bookmark(&jj, &store, &source);
        push_store_bookmark(&jj, &store, &source).unwrap();
        let _ = track_bookmark(&jj, &store, "main");

        let source_old = bookmark_commit(&jj, &store, &source).unwrap();
        let target_old = bookmark_commit(&jj, &store, "main").unwrap();
        squash_branch_onto(&jj, &store, &source, &source_old, "main", "Land the change").unwrap();
        merge_into_bookmark(&jj, &store, "main", &source).unwrap();
        let landing = bookmark_commit(&jj, &store, &source).unwrap();
        assert_eq!(bookmark_commit(&jj, &store, "main"), Some(landing.clone()));
        let tree = git_stdout(
            project.path(),
            &["rev-parse", &format!("{landing}^{{tree}}")],
        );
        Some(Landing {
            _home: home,
            project,
            origin,
            _workspaces: workspaces,
            jj,
            store,
            source,
            source_old,
            target_old,
            landing,
            tree,
        })
    }

    /// Create an empty child of `parent` in the store without moving anything,
    /// and return its commit id.
    fn new_child(fx: &Landing, parent: &str, message: &str) -> String {
        let children = || -> std::collections::HashSet<String> {
            fx.jj
                .run(
                    &fx.store,
                    &[
                        "log",
                        "-r",
                        &format!("children({parent})"),
                        "--no-graph",
                        "-T",
                        "commit_id ++ \"\\n\"",
                        "--ignore-working-copy",
                    ],
                    "test: children",
                )
                .unwrap()
                .lines()
                .map(str::to_string)
                .collect()
        };
        let before = children();
        fx.jj
            .run(
                &fx.store,
                &[
                    "new",
                    "--no-edit",
                    "-r",
                    parent,
                    "-m",
                    message,
                    "--ignore-working-copy",
                ],
                "test: new child",
            )
            .unwrap();
        let added: Vec<String> = children().difference(&before).cloned().collect();
        assert_eq!(added.len(), 1, "{added:?}");
        export_git_preserving_checkout(&fx.jj, &fx.store, true, "test: new child").unwrap();
        added.into_iter().next().unwrap()
    }

    fn tracked_origin(fx: &Landing, branch: &str) -> Option<String> {
        revset_commit_checked(
            &fx.jj,
            &fx.store,
            &format!("tracked_remote_bookmarks(exact:{branch:?}, remote=exact:\"origin\")"),
        )
        .unwrap()
    }

    /// The fixture's source branch is the managed rewrite case: origin holds
    /// the published head `H` (tracked), and the store rewrote the branch to a
    /// commit that does not carry `H`.
    ///
    /// The fixture's squash `L` records `H` and its parent as predecessor
    /// receipts, which already prove `L` supersedes origin's copy. The sends
    /// therefore use a rewrite `R` with no receipts, so only accepted evidence
    /// can account for `H`.
    #[test]
    #[serial_test::serial(jj)]
    fn a_managed_rewrite_publishes_only_on_a_proof_and_never_over_a_moved_origin() {
        let Some(fx) = landing_fixture() else { return };
        // Keep `H` visible, so only evidence can account for it.
        fx.set_bookmark("keep-published-head", &fx.source_old);
        let first = git_stdout(
            fx.project.path(),
            &["rev-parse", &format!("{}^", fx.source_old)],
        );
        assert!(
            origin_copy_is_superseded_with_accepted(
                &fx.jj,
                &fx.store,
                &fx.source,
                &fx.landing,
                &fx.source_old,
                &[],
                REMOTE_REWRITE_PROOF_BUDGET,
            )
            .unwrap(),
            "the squash's predecessor receipts are proof"
        );
        let rewrite = new_child(&fx, &fx.target_old, "rewrite without receipts");
        fx.set_bookmark(&fx.source, &rewrite);
        let send = |accepted: Option<&[AcceptedAdvanceDrops]>| {
            push_managed_branch_unrecorded(
                &fx.jj,
                &fx.store,
                &fx.source,
                Some(&fx.source_old),
                &rewrite,
                accepted,
            )
        };
        assert!(
            matches!(
                send(None).unwrap_err(),
                LeasedPushError::RemoteNotAncestor { .. }
            ),
            "no evidence offered: only fast-forwards"
        );
        assert!(
            matches!(
                send(Some(&[])).unwrap_err(),
                LeasedPushError::RemoteRewriteUnproven { .. }
            ),
            "origin's visible commits are unaccounted"
        );
        let evidence = [AcceptedAdvanceDrops {
            head_after: rewrite.clone(),
            dropped_commit_ids: vec![fx.source_old.clone(), first],
        }];
        let moved = fx.advance_origin(&fx.source);
        let error = send(Some(&evidence)).unwrap_err();
        assert!(
            matches!(&error, LeasedPushError::TargetMoved { observed: Some(o), .. } if *o == moved),
            "an origin that moved is never overwritten: {error:?}"
        );
        git(
            fx.origin.path(),
            &[
                "update-ref",
                &format!("refs/heads/{}", fx.source),
                &fx.source_old,
            ],
        );
        assert_eq!(fx.origin_head(&fx.source), fx.source_old);
        let heads = send(Some(&evidence)).unwrap();
        assert_eq!(fx.origin_head(&fx.source), rewrite);
        assert_eq!(fx.tracking_ref(&fx.source), fx.source_old, "nothing local");
        record_leased_push(&fx.jj, &fx.store, &heads).unwrap();
        assert_eq!(tracked_origin(&fx, &fx.source), Some(rewrite.clone()));
    }

    /// The record touches only the accepted refs: a tracking ref another
    /// writer moved in git, and one jj has never seen, are neither imported
    /// nor folded into their local bookmarks.
    #[test]
    #[serial_test::serial(jj)]
    fn a_record_writes_only_its_own_refs_and_never_merges_a_local_bookmark() {
        let Some(fx) = landing_fixture() else { return };
        fx.unmove_bookmarks();
        let heads = push_commit_expecting_unrecorded(
            &fx.jj,
            &fx.store,
            "main",
            &fx.target_old,
            &fx.landing,
        )
        .unwrap();
        // Concurrent writes the record must not pick up.
        git(
            fx.project.path(),
            &[
                "update-ref",
                &format!("refs/remotes/origin/{}", fx.source),
                &fx.landing,
            ],
        );
        git(
            fx.project.path(),
            &[
                "update-ref",
                "refs/remotes/origin/unrelated",
                &fx.target_old,
            ],
        );
        fx.set_bookmark("main", &fx.landing);
        record_leased_push(&fx.jj, &fx.store, &heads).unwrap();
        assert_eq!(recorded_origin(&fx, "main"), Some(fx.landing.clone()));
        assert_eq!(tracked_origin(&fx, "main"), Some(fx.landing.clone()));
        assert_eq!(
            recorded_origin(&fx, &fx.source),
            Some(fx.source_old.clone()),
            "another branch's moved tracking ref was not imported"
        );
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, &fx.source),
            Some(fx.source_old.clone()),
            "nor folded into its local bookmark"
        );
        assert_eq!(recorded_origin(&fx, "unrelated"), None);
        assert_eq!(bookmark_commit(&fx.jj, &fx.store, "unrelated"), None);
        // jj's record of the git ref matches, so an ordinary import of `main`
        // later finds nothing to fold in.
        import_git(&fx.jj, &fx.store).unwrap();
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, "main"),
            Some(fx.landing.clone())
        );
    }

    /// The window between the two stores: the git tracking ref has moved and
    /// the jj transaction then fails. The record restores git, so a whole
    /// import afterwards finds nothing to fold into the diverged local branch,
    /// and a retry records it.
    #[test]
    #[serial_test::serial(jj)]
    fn a_record_whose_transaction_fails_restores_git_so_no_import_can_fold_it_in() {
        let Some(fx) = landing_fixture() else { return };
        let moved = fx.advance_origin(&fx.source);
        // The store's own observation fetch, which leaves the tracking ref alone.
        assert_eq!(
            fetch_origin_observation(&fx.store, &fx.source).unwrap(),
            Some(moved.clone())
        );
        let before = fx.tracking_ref(&fx.source);
        fail_next_record_transaction("injected: the jj transaction did not commit");
        let error = record_origin_observation(&fx.jj, &fx.store, &fx.source, &moved, &fx.landing)
            .unwrap_err();
        assert!(
            error.contains("injected") && error.contains("restored"),
            "{error}"
        );
        assert_eq!(fx.tracking_ref(&fx.source), before, "git was restored");
        assert_eq!(
            recorded_origin(&fx, &fx.source),
            Some(fx.source_old.clone())
        );
        import_git(&fx.jj, &fx.store).unwrap();
        assert_eq!(
            crate::local_bookmark_targets(&fx.store, &fx.source).unwrap(),
            vec![fx.landing.clone()],
            "a whole import afterwards folds nothing into the local branch"
        );
        record_origin_observation(&fx.jj, &fx.store, &fx.source, &moved, &fx.landing).unwrap();
        assert_eq!(recorded_origin(&fx, &fx.source), Some(moved));
    }

    /// An origin observation ready to record: origin's source moved, and the
    /// store fetched it the way it does, leaving the tracking ref alone.
    fn observed_source_move(fx: &Landing) -> String {
        let moved = fx.advance_origin(&fx.source);
        assert_eq!(
            fetch_origin_observation(&fx.store, &fx.source).unwrap(),
            Some(moved.clone())
        );
        moved
    }

    /// The ambiguous commit that did land: git is not restored, the record is
    /// reported done, and a whole import afterwards moves nothing.
    #[test]
    #[serial_test::serial(jj)]
    fn a_commit_that_reports_an_error_after_landing_is_kept_not_rolled_back() {
        let Some(fx) = landing_fixture() else { return };
        let moved = observed_source_move(&fx);
        crate::inject_origin_record_commit_failure(true, "injected: publishing the op head failed");
        record_origin_observation(&fx.jj, &fx.store, &fx.source, &moved, &fx.landing).unwrap();
        assert_eq!(fx.tracking_ref(&fx.source), moved, "git was kept");
        assert_eq!(recorded_origin(&fx, &fx.source), Some(moved.clone()));
        import_git(&fx.jj, &fx.store).unwrap();
        assert_eq!(
            crate::local_bookmark_targets(&fx.store, &fx.source).unwrap(),
            vec![fx.landing.clone()]
        );
        assert_eq!(recorded_origin(&fx, &fx.source), Some(moved));
    }

    fn delete_bookmark(fx: &Landing, branch: &str) {
        fx.jj
            .run(
                &fx.store,
                &["bookmark", "delete", branch, "--ignore-working-copy"],
                "test: delete bookmark",
            )
            .unwrap();
    }

    /// A landed source branch: published by an accepted landing, then deleted
    /// from the store, so origin's copy is what is left to retire.
    fn landed_and_deleted_locally(fx: &Landing) {
        let pushed = publish_landing_atomic(&fx.jj, &fx.store, &fx.publication()).unwrap();
        assert!(matches!(pushed, LeasedPush::Accepted(_)), "{pushed:?}");
        let error = retire_remote_branch_unrecorded(&fx.jj, &fx.store, &fx.source, &fx.landing)
            .unwrap_err();
        assert!(
            matches!(&error, LeasedPushError::Precondition(e) if e.contains("delete it before")),
            "a branch the store still has is never retired: {error:?}"
        );
        delete_bookmark(fx, &fx.source);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_late_retirement_record_never_erases_a_recreated_branch() {
        let Some(fx) = landing_fixture() else { return };
        landed_and_deleted_locally(&fx);
        let heads =
            retire_remote_branch_unrecorded(&fx.jj, &fx.store, &fx.source, &fx.landing).unwrap();
        // The branch is recreated before the deletion is recorded.
        let recreated = fx.target_old.clone();
        fx.set_bookmark(&fx.source, &recreated);
        git(
            fx.project.path(),
            &[
                "update-ref",
                &format!("refs/remotes/origin/{}", fx.source),
                &recreated,
            ],
        );
        let error = record_leased_push(&fx.jj, &fx.store, &heads).unwrap_err();
        assert!(error.contains("recreated"), "{error}");
        assert_eq!(fx.tracking_ref(&fx.source), recreated);
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, &fx.source),
            Some(recreated)
        );
    }

    fn recorded_origin(fx: &Landing, branch: &str) -> Option<String> {
        let revset = format!("remote_bookmarks(exact:{branch:?}, remote=exact:\"origin\")");
        revset_commit_checked(&fx.jj, &fx.store, &revset).unwrap()
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_target_moved_after_observation_rejects_both_refs_and_preserves_the_source() {
        let Some(fx) = landing_fixture() else { return };
        let moved = fx.advance_origin("main");
        let error = publish_landing_atomic(&fx.jj, &fx.store, &fx.publication()).unwrap_err();
        assert_eq!(
            error,
            LeasedPushError::TargetMoved {
                branch: "main".into(),
                expected: Some(fx.target_old.clone()),
                observed: Some(moved.clone()),
            }
        );
        assert_eq!(
            fx.origin_head("main"),
            moved,
            "origin's target is untouched"
        );
        assert_eq!(
            fx.origin_head(&fx.source),
            fx.source_old,
            "the atomic refusal left origin's source ref where it was"
        );
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, &fx.source),
            Some(fx.landing.clone())
        );
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_confirmation_failure_after_acceptance_is_accepted_unconfirmed_and_recoverable() {
        let Some(fx) = landing_fixture() else { return };
        fail_next_confirmation("injected: origin unreachable after the push");
        let pushed = publish_landing_atomic(&fx.jj, &fx.store, &fx.publication()).unwrap();
        let LeasedPush::AcceptedUnconfirmed { heads, error } = pushed else {
            panic!("expected AcceptedUnconfirmed, got {pushed:?}");
        };
        assert!(error.contains("injected"), "{error}");
        assert_eq!(fx.origin_head("main"), fx.landing, "the acceptance stands");
        assert_eq!(fx.origin_head(&fx.source), fx.landing);
        confirm_leased_push(&fx.jj, &fx.store, &heads).unwrap();
        confirm_leased_push(&fx.jj, &fx.store, &heads).unwrap();
    }

    #[test]
    #[serial_test::serial(jj)]
    fn preconditions_refuse_before_sending() {
        let Some(fx) = landing_fixture() else { return };
        let mut wrong_tree = fx.publication();
        wrong_tree.proof = TargetProof::SquashOnto {
            tree: "0".repeat(40),
        };
        let error = publish_landing_atomic(&fx.jj, &fx.store, &wrong_tree).unwrap_err();
        assert!(
            matches!(error, LeasedPushError::Precondition(_)),
            "{error:?}"
        );

        let mut not_ff = fx.publication();
        not_ff.target_expected_old = fx.source_old.clone();
        let error = publish_landing_atomic(&fx.jj, &fx.store, &not_ff).unwrap_err();
        assert!(
            matches!(error, LeasedPushError::Precondition(_)),
            "{error:?}"
        );

        let error =
            push_bookmark_expecting(&fx.jj, &fx.store, "main", &fx.target_old, &fx.source_old)
                .unwrap_err();
        assert!(
            matches!(&error, LeasedPushError::Precondition(e) if e.contains("store bookmark")),
            "{error:?}"
        );
        assert_eq!(fx.origin_head("main"), fx.target_old);
        assert_eq!(fx.origin_head(&fx.source), fx.source_old);
    }

    #[test]
    #[serial_test::serial(jj)]
    fn an_unrecorded_atomic_landing_moves_both_refs_on_origin_only() {
        let Some(fx) = landing_fixture() else { return };
        fx.unmove_bookmarks();
        let heads = publish_landing_atomic_unrecorded(
            &fx.jj,
            &fx.store,
            &fx.publication(),
            &fx.source_old,
            &fx.target_old,
        )
        .unwrap();
        assert_eq!(fx.origin_head("main"), fx.landing);
        assert_eq!(fx.origin_head(&fx.source), fx.landing);
        assert_eq!(fx.tracking_ref("main"), fx.target_old);
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, &fx.source),
            Some(fx.source_old.clone())
        );
        fx.set_bookmark("main", &fx.landing);
        fx.set_bookmark(&fx.source, &fx.landing);
        confirm_leased_push(&fx.jj, &fx.store, &heads).unwrap();
        assert_eq!(recorded_origin(&fx, &fx.source), Some(fx.landing.clone()));
    }

    #[test]
    #[serial_test::serial(jj)]
    fn a_record_refuses_an_unmoved_bookmark_or_an_intervening_observation_and_writes_nothing() {
        let Some(fx) = landing_fixture() else { return };
        fx.unmove_bookmarks();
        let heads = push_commit_expecting_unrecorded(
            &fx.jj,
            &fx.store,
            "main",
            &fx.target_old,
            &fx.landing,
        )
        .unwrap();
        let error = record_leased_push(&fx.jj, &fx.store, &heads).unwrap_err();
        assert!(error.contains("move it first"), "{error}");
        assert_eq!(fx.tracking_ref("main"), fx.target_old);

        fx.set_bookmark("main", &fx.landing);
        let tree = git_stdout(
            fx.project.path(),
            &["rev-parse", &format!("{}^{{tree}}", fx.target_old)],
        );
        let sideways = git_stdout(
            fx.project.path(),
            &["commit-tree", "-p", &fx.target_old, "-m", "sideways", &tree],
        );
        git(
            fx.project.path(),
            &["update-ref", "refs/remotes/origin/main", &sideways],
        );
        let error = record_leased_push(&fx.jj, &fx.store, &heads).unwrap_err();
        assert!(error.contains("intervening"), "{error}");
        assert_eq!(fx.tracking_ref("main"), sideways, "nothing was overwritten");
    }
}
