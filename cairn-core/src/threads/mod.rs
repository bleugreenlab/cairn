//! Threads: durable, non-terminal sessions (CAIRN-3377).
//!
//! A thread orchestrates children and converses indefinitely instead of
//! completing an objective. Two consequences of that live here, and both are
//! keyed on the single `issues.kind` lookup in [`identity_for_job`]:
//!
//! - Its session must stay cheap to run for weeks. [`compaction`] is the
//!   mechanism that makes that true: it replaces concluded history with a table
//!   of contents whose lines are re-readable at their URIs, and keeps the
//!   thread's authored arc and a recency window verbatim.
//! - It owns no branch, so it writes no tracked files.
//!   [`commit_refusal_for_job`] is the refusal both commit verbs take before
//!   they act on a `commit_msg`.

pub mod compaction;

use crate::issues::crud::IssueIdentity;
use crate::models::IssueKind;
use crate::storage::LocalDb;

/// The issue a job runs for, resolved through the canonical identity helper.
///
/// The one query behind every kind-keyed rule a running job is subject to, so
/// "is this a thread?" is asked of `issues.kind` in exactly one place and no
/// second discriminator can drift from the column.
async fn identity_for_job(db: &LocalDb, job_id: &str) -> Option<IssueIdentity> {
    let issue_id = db
        .query_opt_text(
            "SELECT issue_id FROM jobs WHERE id = ?1",
            cairn_db::turso::params![job_id.to_string()],
        )
        .await
        .ok()
        .flatten()?;
    crate::issues::crud::identity(db, &issue_id).await.ok()?
}

/// Whether the rolling compaction path applies to a job's session.
///
/// Every compaction API takes this from its caller rather than deciding for
/// itself, so there is exactly one place in the codebase that answers "is this a
/// thread?" and no second discriminator can drift from `issues.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadCompaction {
    Enabled,
    Disabled,
}

impl ThreadCompaction {
    pub fn is_enabled(self) -> bool {
        self == ThreadCompaction::Enabled
    }
}

/// Resolve whether `job_id` runs a thread session, from the canonical
/// `issues.kind` discriminator (CAIRN-3387).
///
/// Every compaction API takes the answer as a parameter instead of re-deriving
/// it, over the one kind lookup in [`identity_for_job`], so no second source can
/// drift from the column. Do not add a flag, an env override, or a per-agent
/// opt-in beside it: a capability with two sources is a capability that
/// disagrees with itself.
///
/// A job Cairn cannot resolve reads as `Disabled`. That is the safe direction:
/// an unresolvable job keeps the ordinary full-digest reseed, which is correct
/// for every issue and merely expensive for a thread.
pub(crate) async fn compaction_capability_for_job(db: &LocalDb, job_id: &str) -> ThreadCompaction {
    match identity_for_job(db, job_id).await {
        Some(identity) if identity.kind == IssueKind::Thread => ThreadCompaction::Enabled,
        _ => ThreadCompaction::Disabled,
    }
}

/// Why a tool batch from `job_id` may not carry a `commit_msg`, or `None` when
/// it may.
///
/// A thread owns no branch. Its execution resolves to the `base` branch target,
/// which rewrites every agent node to [`crate::models::BranchMode::None`], and a
/// job without a branch of its own reads and writes its *base branch*. So a
/// `commit_msg` from a thread does not land on a reviewable side branch to be
/// squashed or abandoned later — it lands on the project's default branch, with
/// no pull request and no review surface. External state depends on this
/// refusal, which is why it is taken up front, before the batch runs, rather
/// than discovered after a tree has been dirtied.
///
/// `commit_msg` is the entire surface to close, on both commit verbs. A `write`
/// cannot touch a tracked file without one — `change_validation` rejects a
/// file-target change that lacks it, and `mode:"apply"` carries its own — and a
/// `run` that dirties the tree without one is already restored to HEAD by the
/// commit barrier. Refusing the field therefore refuses every tracked write, and
/// leaves reads, operational commands, and scratch dirt to the machinery that
/// already handles them.
///
/// A job that cannot be resolved to an issue is treated as ordinary. That is the
/// same safe direction [`compaction_capability_for_job`] takes, for the same
/// reason: an unresolvable job must keep the behaviour every issue has.
pub(crate) async fn commit_refusal_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let identity = identity_for_job(db, job_id).await?;
    if identity.kind != IssueKind::Thread {
        return None;
    }
    Some(format!(
        "Refusing to commit from {}-{}: it is a thread. A thread owns no branch, so this batch is running ON the project's base branch — a commit_msg here lands on the default branch directly, with no pull request and no review surface. Threads orchestrate and converse; work that changes the repository belongs in a child issue, which owns a branch, ships its own PR, and merges to the base branch. Re-send this batch without commit_msg: reading, running commands, and scratch files are all yours, and anything the batch dirties is restored to HEAD for you.",
        identity.project_key, identity.number
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn seed(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('p','default','P','PRJ','/tmp/p',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, kind, created_at, updated_at)
               VALUES ('i-issue','p',1,'An ordinary issue','active','none','issue',1,1);
             INSERT INTO issues(id, project_id, number, title, status, attention, kind, created_at, updated_at)
               VALUES ('i-thread','p',2,'A thread','active','none','thread',1,1);
             INSERT INTO jobs(id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('j-issue','i-issue','p','running','builder','builder',1,1);
             INSERT INTO jobs(id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
               VALUES ('j-thread','i-thread','p','running','thread','thread',1,1);",
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn a_thread_job_compacts_and_an_ordinary_issue_job_does_not() {
        // Zero regression for ordinary issues is the acceptance criterion of the
        // whole slice: an issue node must keep reseeding from its full digest,
        // and only `kind = 'thread'` may take the compaction path.
        let db = seed("thread-capability-by-kind.db").await;

        assert_eq!(
            compaction_capability_for_job(&db, "j-thread").await,
            ThreadCompaction::Enabled
        );
        assert_eq!(
            compaction_capability_for_job(&db, "j-issue").await,
            ThreadCompaction::Disabled
        );
    }

    #[tokio::test]
    async fn a_row_predating_the_discriminator_is_not_a_thread() {
        // The column defaults to `issue` for every pre-migration row, so an
        // untouched database must not start compacting after an upgrade.
        let db = seed("thread-capability-legacy.db").await;
        db.execute(
            "UPDATE issues SET kind = NULL WHERE id = 'i-issue'",
            cairn_db::turso::params![],
        )
        .await
        .unwrap();

        assert_eq!(
            compaction_capability_for_job(&db, "j-issue").await,
            ThreadCompaction::Disabled
        );
    }

    #[tokio::test]
    async fn an_unresolvable_job_keeps_the_ordinary_path() {
        let db = seed("thread-capability-unknown.db").await;

        assert_eq!(
            compaction_capability_for_job(&db, "no-such-job").await,
            ThreadCompaction::Disabled
        );
    }

    /// The tracked-writes half of the thread posture. A thread runs on the base
    /// branch, so its commit has nowhere to land but the project's default
    /// branch; an ordinary issue's job owns a branch and is untouched.
    #[tokio::test]
    async fn a_thread_job_is_refused_a_commit_msg_and_an_ordinary_issue_job_is_not() {
        let db = seed("thread-commit-refusal-by-kind.db").await;

        let refusal = commit_refusal_for_job(&db, "j-thread")
            .await
            .expect("a thread job must be refused a commit_msg");
        assert!(
            refusal.contains("PRJ-2"),
            "the refusal names the thread it is speaking about: {refusal}"
        );
        assert!(
            refusal.contains("it is a thread") && refusal.contains("owns no branch"),
            "the refusal states the posture, not just the rejection: {refusal}"
        );
        assert!(
            refusal.contains("child issue"),
            "the refusal points at where durable changes do belong: {refusal}"
        );

        assert_eq!(
            commit_refusal_for_job(&db, "j-issue").await,
            None,
            "every ordinary issue commits exactly as it did before"
        );
    }

    /// Both safe directions, together: an upgraded database whose rows predate
    /// the discriminator, and a job Cairn cannot resolve to an issue at all,
    /// keep the commit behaviour every issue has. A guard that failed the other
    /// way would strand ordinary work the moment a lookup went sideways.
    #[tokio::test]
    async fn a_job_that_does_not_resolve_to_a_thread_commits_as_an_ordinary_issue() {
        let db = seed("thread-commit-refusal-unresolvable.db").await;
        db.execute(
            "UPDATE issues SET kind = NULL WHERE id = 'i-issue'",
            cairn_db::turso::params![],
        )
        .await
        .unwrap();

        assert_eq!(commit_refusal_for_job(&db, "j-issue").await, None);
        assert_eq!(commit_refusal_for_job(&db, "no-such-job").await, None);
    }
}
