//! Held publications: a routed batch's sealed delta that reached this machine's
//! project store but not its branch.
//!
//! A routed batch seals its changes as a delta commit where it ran, and the
//! runner installs that commit's objects in the project repository before it
//! tries to advance the branch. If the branch transaction then fails (lock
//! contention that outlasts patience, a store error), the commit is real and
//! the store holds everything needed to land it later. Dropping it would lose
//! the agent's committed work, and telling the agent nothing was committed
//! would invite it to redo edits a later batch may already have absorbed.
//!
//! So the delta is held: a ref under `refs/cairn/stranded/` keeps its objects
//! alive, and a `held_publications` row schedules it. Every later commit
//! barrier on the same branch (a `write`, a `run` with a commit_msg, and the PR
//! artifact's create, publish, refresh, and merge) drains the branch's held
//! rows first, oldest first, through the same publication funnel a batch's own
//! delta uses. A held delta that conflicts with what the branch now holds is
//! stranded, exactly like a batch that straddled a conflicting move. A merge is
//! refused while anything on the branch is still held.
//!
//! A held delta and a later delta that absorbed its content (a resident
//! execution environment keeps its unpublished HEAD and seals the next batch on
//! top of it) need no special handling: draining first integrates the held
//! commit and the later one adds only its own paths, and publishing the later
//! one first makes the held commit resolve as already landed.

use super::{
    climb_landing_rungs, publish_available_delta, straddle_inspection_commands, DeltaPublication,
    LandingRungs, PublishFailure, RoutedLanding, SlotPublicationOutcome,
};
use crate::orchestrator::{JjStoreOperationTrace, Orchestrator};
use crate::storage::{LocalDb, RowExt};
use cairn_vcs::PublicationIdentity;
use std::path::{Path, PathBuf};

/// One ref transaction against the project repository: `update-ref <args>`.
///
/// It contends for the repository's one `packed-refs.lock` like every other
/// ref writer, and losing that race is exactly how a delta comes to be held,
/// so it waits out contention rather than failing on it.
async fn held_ref_transaction(
    repository: &Path,
    context: &str,
    args: &[&str],
) -> Result<(), String> {
    crate::jj::with_packed_refs_patience_async(
        crate::jj::PackedRefs::Checkout(repository),
        context,
        || {
            let mut command = vec!["update-ref"];
            command.extend_from_slice(args);
            super::git_output(repository, &command, context).map(|_| ())
        },
    )
    .await
}

/// What [`hold_delta`] needs: where the delta is, where it was headed, and the
/// commit it should become.
pub(crate) struct Hold<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) store_dir: &'a Path,
    pub(crate) repository: &'a Path,
    pub(crate) branch: &'a str,
    pub(crate) job_id: Option<&'a str>,
    pub(crate) attempt_id: &'a str,
    pub(crate) base_commit: &'a str,
    pub(crate) delta_commit: &'a str,
    /// The batch's commit_msg, `^` preserved: an amend republished onto the
    /// head it was sealed against still amends it.
    pub(crate) message: &'a str,
    pub(crate) author: Option<&'a PublicationIdentity>,
    /// Why publication failed.
    pub(crate) error: &'a str,
}

/// A delta that was held rather than published, and how well it is held.
#[derive(Debug)]
pub(crate) struct HeldDelta {
    pub(crate) delta_commit: String,
    pub(crate) branch: String,
    pub(crate) error: String,
    /// The ref keeping the commit alive, or why writing it failed.
    pub(crate) reference: Result<String, String>,
    /// Whether the row scheduling republication was written, or why not.
    pub(crate) recorded: Result<(), String>,
}

impl HeldDelta {
    /// What the agent is told. The work is committed and will land; the one
    /// thing the agent must not do is redo it.
    pub(crate) fn message(&self) -> String {
        let delta = &self.delta_commit;
        let mut text = format!(
            "This batch's changes were committed as {delta} but could not be published to `{}`: \
             {}. The branch is unchanged, so `read` does not show them yet. ",
            self.branch, self.error
        );
        match (&self.recorded, &self.reference) {
            (Ok(()), reference) => {
                match reference {
                    Ok(reference) => {
                        text.push_str(&format!("The commit is held at `{reference}` "))
                    }
                    Err(_) => text.push_str("The commit is held "),
                }
                text.push_str(
                    "and is published, ahead of your own changes, by your next `write` or `run` \
                     with a commit_msg on this branch. To publish it now, `run` a trivial command \
                     (for example `true`) with any commit_msg. Do not redo these edits.",
                );
                if let Err(error) = reference {
                    text.push_str(&format!(
                        " Holding it under a ref also failed ({error}); `git show {delta}` shows \
                         the changes if they need re-applying."
                    ));
                }
            }
            (Err(error), Ok(reference)) => text.push_str(&format!(
                "Recording it for republication failed ({error}), so it will not be published \
                 on its own. It is kept at `{reference}`, and `git show {delta}` shows the \
                 changes; re-apply them and commit as usual."
            )),
            (Err(error), Err(ref_error)) => text.push_str(&format!(
                "Recording it for republication failed ({error}) and so did keeping it under a \
                 ref ({ref_error}). `git show {delta}` shows the changes for as long as they \
                 last; re-apply them and commit as usual."
            )),
        }
        text
    }
}

/// What the agent is told when its batch's delta never reached the project
/// store, so there is nothing here to hold or republish.
pub(crate) fn not_in_store_message(delta: &str, error: &str) -> String {
    format!(
        "This batch's changes were committed as {delta} where the batch ran, but never reached \
         the project store: {error}. Nothing was published or held. Re-apply the edits."
    )
}

fn unix_now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Hold a delta whose publication failed after the store took its objects.
///
/// The caller still pins the objects, so the ref takes over protecting them
/// before that pin is released. Never fails: each leg that does not complete is
/// reported in the returned value, and the message says what that means.
pub(crate) async fn hold_delta(orch: &Orchestrator, hold: Hold<'_>) -> HeldDelta {
    // The stranded namespace: the ref-store sweep retires a job-named ref there
    // 30 days after the job's issue resolves, which is the lifecycle a held
    // delta needs too.
    let reference = super::stranded_ref_name(hold.job_id, hold.attempt_id);
    let reference = held_ref_transaction(
        hold.repository,
        "hold the unpublished delta",
        &[&reference, hold.delta_commit],
    )
    .await
    .map(|()| reference);
    if let Err(error) = &reference {
        log::warn!(
            "held delta {} on `{}` has no protecting ref: {error}",
            hold.delta_commit,
            hold.branch
        );
    }
    let recorded = record_hold(&orch.db.local, &hold, reference.as_ref().ok()).await;
    match &recorded {
        Ok(()) => log::warn!(
            "held delta {} for `{}` (attempt {}): {}",
            hold.delta_commit,
            hold.branch,
            hold.attempt_id,
            hold.error
        ),
        Err(error) => log::error!(
            "delta {} for `{}` could not be recorded for republication: {error}",
            hold.delta_commit,
            hold.branch
        ),
    }
    HeldDelta {
        delta_commit: hold.delta_commit.to_string(),
        branch: hold.branch.to_string(),
        error: hold.error.to_string(),
        reference,
        recorded,
    }
}

async fn record_hold(
    db: &LocalDb,
    hold: &Hold<'_>,
    reference: Option<&String>,
) -> Result<(), String> {
    let now = unix_now();
    db.execute(
        "INSERT INTO held_publications
              (attempt_id, project_id, store_path, repository_path, branch, job_id, base_commit,
               delta_commit, commit_message, author_name, author_email, reference, status,
               last_error, attempts, created_at, updated_at)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 'held', ?13, 1, ?14, ?14)
         ON CONFLICT(attempt_id) DO UPDATE SET
              status = 'held',
              last_error = excluded.last_error,
              attempts = held_publications.attempts + 1,
              updated_at = excluded.updated_at",
        cairn_db::turso::params![
            hold.attempt_id,
            hold.project_id,
            hold.store_dir.to_string_lossy().into_owned(),
            hold.repository.to_string_lossy().into_owned(),
            hold.branch,
            hold.job_id,
            hold.base_commit,
            hold.delta_commit,
            hold.message,
            hold.author.map(|author| author.name.as_str()),
            hold.author.map(|author| author.email.as_str()),
            reference.map(String::as_str),
            hold.error,
            now
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("record the held delta: {error}"))
}

/// One held delta, as recorded.
#[derive(Debug, Clone)]
pub(crate) struct HeldRow {
    pub(crate) attempt_id: String,
    pub(crate) project_id: String,
    pub(crate) store_path: PathBuf,
    pub(crate) repository_path: PathBuf,
    pub(crate) branch: String,
    pub(crate) job_id: Option<String>,
    pub(crate) base_commit: String,
    pub(crate) delta_commit: String,
    pub(crate) commit_message: String,
    author_name: Option<String>,
    author_email: Option<String>,
    pub(crate) reference: Option<String>,
    pub(crate) last_error: String,
    /// `held`, or `unpublished` once it landed here but did not reach the
    /// remote an open pull request reads.
    pub(crate) status: String,
    /// The commit it landed as, once it has.
    pub(crate) landed_head: Option<String>,
}

impl HeldRow {
    fn author(&self) -> Option<PublicationIdentity> {
        match (&self.author_name, &self.author_email) {
            (Some(name), Some(email)) => Some(PublicationIdentity::new(name, email)),
            _ => None,
        }
    }

    /// Which batch this was, in the agent's terms: the commit message it gave.
    pub(crate) fn origin(&self) -> String {
        let first_line = self.commit_message.lines().next().unwrap_or("").trim();
        match first_line {
            "^" => "from an earlier batch that amended the previous commit".to_string(),
            "" => "from an earlier batch".to_string(),
            line => format!("from an earlier batch: \"{line}\""),
        }
    }
}

/// The branch's held deltas, oldest first.
pub(crate) async fn held_rows(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<HeldRow>, String> {
    rows_with_status(db, project_id, branch, false).await
}

/// Everything on the branch a merge must wait for: held deltas, and ones that
/// landed here but never reached the remote.
pub(crate) async fn blocking_rows(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
) -> Result<Vec<HeldRow>, String> {
    rows_with_status(db, project_id, branch, true).await
}

async fn rows_with_status(
    db: &LocalDb,
    project_id: &str,
    branch: &str,
    include_unpublished: bool,
) -> Result<Vec<HeldRow>, String> {
    db.query_all(
        "SELECT attempt_id, project_id, store_path, repository_path, branch, job_id, base_commit,
                delta_commit, commit_message, author_name, author_email, reference, last_error,
                status, landed_head
           FROM held_publications
          WHERE project_id = ?1 AND branch = ?2
            AND (status = 'held' OR (?3 AND status = 'unpublished'))
          ORDER BY created_at, rowid",
        (
            project_id.to_string(),
            branch.to_string(),
            i64::from(include_unpublished),
        ),
        |row| {
            Ok(HeldRow {
                attempt_id: row.text(0)?,
                project_id: row.text(1)?,
                store_path: PathBuf::from(row.text(2)?),
                repository_path: PathBuf::from(row.text(3)?),
                branch: row.text(4)?,
                job_id: row.opt_text(5)?,
                base_commit: row.text(6)?,
                delta_commit: row.text(7)?,
                commit_message: row.text(8)?,
                author_name: row.opt_text(9)?,
                author_email: row.opt_text(10)?,
                reference: row.opt_text(11)?,
                last_error: row.text(12)?,
                status: row.text(13)?,
                landed_head: row.opt_text(14)?,
            })
        },
    )
    .await
    .map_err(|error| format!("read held commits: {error}"))
}

async fn settle_row(db: &LocalDb, attempt_id: &str, status: &str, landed_head: Option<&str>) {
    if let Err(error) = db
        .execute(
            "UPDATE held_publications
                SET status = ?2, landed_head = ?3, updated_at = ?4
              WHERE attempt_id = ?1",
            (attempt_id, status, landed_head, unix_now()),
        )
        .await
    {
        // Self-healing: a row left `held` resolves as already landed, or as a
        // conflict, at the next drain.
        log::warn!("held publication {attempt_id} was not marked {status}: {error}");
    }
}

/// Mark a landed delta as not having reached the remote, so a merge keeps
/// waiting for it however many barriers pass before a push succeeds.
async fn record_unpublished(db: &LocalDb, attempt_id: &str, error: &str) {
    if let Err(write_error) = db
        .execute(
            "UPDATE held_publications
                SET status = 'unpublished', last_error = ?2, updated_at = ?3
              WHERE attempt_id = ?1",
            (attempt_id, error, unix_now()),
        )
        .await
    {
        log::error!(
            "held publication {attempt_id} landed but did not reach the remote, and that was \
             not recorded: {write_error}"
        );
    }
}

async fn record_retry(db: &LocalDb, attempt_id: &str, error: &str) {
    if let Err(write_error) = db
        .execute(
            "UPDATE held_publications
                SET attempts = attempts + 1, last_error = ?2, updated_at = ?3
              WHERE attempt_id = ?1",
            (attempt_id, error, unix_now()),
        )
        .await
    {
        log::warn!("held publication {attempt_id} retry was not recorded: {write_error}");
    }
}

/// One held delta a drain dealt with.
#[derive(Debug)]
pub(crate) enum Drained {
    /// Landed on the branch. Its remaining publication rungs (export check,
    /// origin push, dev instances, cloud pack) are climbed by
    /// [`settle_drained`] once the store lock is released.
    Landed(Box<LandedHeld>),
    /// Settled entirely under the lock: already on the branch, stranded, or
    /// still held. The text is what the agent is told.
    Note(String),
}

#[derive(Debug)]
pub(crate) struct LandedHeld {
    row: HeldRow,
    head: String,
    export: Result<(), String>,
    /// The branch already carried it, so this drain moved nothing. Its
    /// publication obligation still has to be discharged: local content
    /// equality says nothing about the remote.
    already: bool,
}

/// Whether a drain moved the branch.
pub(crate) fn any_landed(drained: &[Drained]) -> bool {
    drained
        .iter()
        .any(|entry| matches!(entry, Drained::Landed(landed) if !landed.already))
}

/// Record that a held delta is on the branch here, before the store lock that
/// landed it is released, while its push is still owed.
///
/// This is the fail-closed half of the merge gate. The row stays blocking
/// (`unpublished`) until [`settle_landed`] sees the remaining rungs succeed, so
/// a merge that runs in between, a runner that stops before settling, or a
/// write here that fails all leave the gate shut rather than open.
async fn record_landed_here(db: &LocalDb, attempt_id: &str, head: &str) {
    if let Err(error) = db
        .execute(
            "UPDATE held_publications
                SET status = 'unpublished', landed_head = ?2,
                    last_error = 'landed on the branch here; the push to the remote has not \
                                  completed', updated_at = ?3
              WHERE attempt_id = ?1",
            (attempt_id, head, unix_now()),
        )
        .await
    {
        // Still blocking: the row stays `held`, and the next drain finds the
        // branch already carrying it and records this again.
        log::warn!("held publication {attempt_id} landed, and that was not recorded: {error}");
    }
}

/// Republish the branch's held deltas, oldest first.
///
/// Caller holds the store lock. Stops at the first delta that still cannot be
/// published, so held commits land in the order they were made. Never fails
/// the barrier that called it: everything it could not do is a note.
pub(crate) async fn drain_held_publications(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
    trace: Option<&JjStoreOperationTrace>,
) -> Vec<Drained> {
    let rows = match held_rows(&orch.db.local, project_id, branch).await {
        Ok(rows) => rows,
        Err(error) => {
            return vec![Drained::Note(format!(
                "⚠️ Could not check this branch for held commits, so none were published: {error}."
            ))]
        }
    };
    let mut drained = Vec::new();
    for row in rows {
        match republish(orch, &row, trace).await {
            Ok(SlotPublicationOutcome::Published { landed, export, .. }) => {
                record_landed_here(&orch.db.local, &row.attempt_id, &landed.head).await;
                release_ref(&row).await;
                drained.push(Drained::Landed(Box::new(LandedHeld {
                    row,
                    head: landed.head,
                    export,
                    already: false,
                })));
            }
            // Already on the branch here, perhaps landed by a drain that never
            // settled. Whether the remote has it is still unknown, so it owes
            // the same rungs a fresh landing does.
            Ok(SlotPublicationOutcome::AlreadyLanded { head }) => {
                record_landed_here(&orch.db.local, &row.attempt_id, &head).await;
                release_ref(&row).await;
                drained.push(Drained::Landed(Box::new(LandedHeld {
                    row,
                    head,
                    export: Ok(()),
                    already: true,
                })));
            }
            Err(PublishFailure::Conflicted { head, paths }) => {
                // The ref stays: it is the agent's route back to the work.
                settle_row(&orch.db.local, &row.attempt_id, "stranded", None).await;
                drained.push(Drained::Note(stranded_held_message(&row, &head, &paths)));
            }
            Err(PublishFailure::Failed(error)) => {
                record_retry(&orch.db.local, &row.attempt_id, &error).await;
                drained.push(Drained::Note(format!(
                    "⚠️ Held commit {} ({}) still could not be published: {error}. It stays held.",
                    row.delta_commit,
                    row.origin()
                )));
                break;
            }
        }
    }
    drained
}

async fn republish(
    orch: &Orchestrator,
    row: &HeldRow,
    trace: Option<&JjStoreOperationTrace>,
) -> Result<SlotPublicationOutcome, PublishFailure> {
    // The objects were installed when the delta was held; this re-pins them
    // for the transaction the way a fresh install is pinned.
    let _pin = crate::jj::pin_validated_delta(
        &row.repository_path,
        &row.base_commit,
        &row.delta_commit,
        None,
    )?;
    let author = row.author();
    publish_available_delta(
        orch,
        &DeltaPublication {
            repository: &row.repository_path,
            store_dir: &row.store_path,
            project_id: &row.project_id,
            branch: &row.branch,
            base_commit: &row.base_commit,
            delta_commit: &row.delta_commit,
            message: &row.commit_message,
            author: author.as_ref(),
            job_id: row.job_id.as_deref(),
            trace,
        },
        None,
    )
    .await
}

async fn release_ref(row: &HeldRow) {
    let Some(reference) = row.reference.as_deref() else {
        return;
    };
    if let Err(error) = held_ref_transaction(
        &row.repository_path,
        "release the published held delta",
        &["-d", reference, &row.delta_commit],
    )
    .await
    {
        // Harmless: the ref-store sweep retires it with the job's issue.
        log::warn!("held delta ref {reference} was not released after landing: {error}");
    }
}

/// A held delta that can no longer be merged onto its branch.
fn stranded_held_message(row: &HeldRow, head: &str, paths: &[String]) -> String {
    let delta = &row.delta_commit;
    let [show, diff] = straddle_inspection_commands(head, delta);
    format!(
        "⚠️ Held commit {delta} ({}) can no longer be published: it conflicts with what the \
         branch now holds at {head} in {}. Nothing from it was committed. `{show}` shows its \
         changes, and `{diff}` compares them with what the branch holds now. Re-apply them on \
         top of the branch's version, editing as you normally would, and commit as usual. \
         Running again without re-applying them will not pick them up.",
        row.origin(),
        paths.join(", "),
    )
}

/// Climb the remaining publication rungs for each held delta the drain landed,
/// and render everything the drain did. Must run without the store lock.
pub(crate) async fn settle_drained(orch: &Orchestrator, drained: Vec<Drained>) -> Vec<String> {
    let mut notes = Vec::with_capacity(drained.len());
    for entry in drained {
        notes.push(match entry {
            Drained::Note(note) => note,
            Drained::Landed(landed) => settle_landed(orch, *landed).await,
        });
    }
    notes
}

async fn settle_landed(orch: &Orchestrator, landed: LandedHeld) -> String {
    let LandedHeld {
        row,
        head,
        export,
        already,
    } = landed;
    let db = match row.job_id.as_deref() {
        Some(job_id) => crate::execution::routing::owning_db_for_job(&orch.db, job_id)
            .await
            .ok(),
        None => None,
    };
    let report = climb_landing_rungs(
        orch,
        &LandingRungs {
            project_id: &row.project_id,
            object_repository: &row.repository_path,
            store_dir: &row.store_path,
            branch: &row.branch,
            routed: match (row.job_id.as_deref(), db.as_deref()) {
                (Some(job_id), Some(db)) => Some(RoutedLanding {
                    job_id,
                    project_id: &row.project_id,
                    db,
                }),
                _ => None,
            },
        },
        &head,
        export,
    )
    .await;
    match report.published {
        Ok(()) => {
            // Only now is nothing owed. A failure to record it leaves the row
            // blocking, and the next PR barrier's push retry settles it.
            settle_row(&orch.db.local, &row.attempt_id, "landed", Some(&head)).await;
            if already {
                format!(
                    "Held commit {} ({}) was already on the branch; released{}.",
                    row.delta_commit,
                    row.origin(),
                    report.warnings
                )
            } else {
                format!(
                    "✓ Published held commit {head} ({}){}.",
                    row.origin(),
                    report.warnings
                )
            }
        }
        Err(error) => {
            record_unpublished(&orch.db.local, &row.attempt_id, &error).await;
            format!(
                "⚠️ Republished held commit ({}): {}{}",
                row.origin(),
                super::super::unpublished_commit_message(&head, &error),
                report.warnings
            )
        }
    }
}

/// What a commit barrier outside a batch (a PR artifact action) did about the
/// branch's held deltas.
#[derive(Debug)]
pub(crate) struct PrBarrier {
    pub(crate) notes: Vec<String>,
    /// What is still held afterwards. `Err` when that could not be read, which
    /// a merge treats as held.
    pub(crate) still_held: Result<Vec<HeldRow>, String>,
}

impl PrBarrier {
    /// Prefix an action's reply with what the barrier published.
    pub(crate) fn prefix(&self, reply: String) -> String {
        if self.notes.is_empty() {
            return reply;
        }
        format!("{}\n\n{reply}", self.notes.join("\n"))
    }

    /// Append a warning to a reply when the branch still holds an unpublished
    /// commit, so a saved PR artifact cannot read as presenting all the work.
    pub(crate) fn warn_if_held(&self, reply: String) -> String {
        match &self.still_held {
            Ok(rows) if !rows.is_empty() => format!(
                "{reply}\n\n⚠️ This branch still holds {} that has not been published to it, so \
                 the pull request does not carry it yet and merging is refused until it lands. \
                 Any `write`, or a `run` with a commit_msg, on this branch retries it.",
                describe_held(rows)
            ),
            _ => reply,
        }
    }

    /// Refuse a merge while the branch still holds an unpublished commit: the
    /// pull request would ship without work the agent was told is committed.
    pub(crate) fn refuse_merge(&self) -> Result<(), String> {
        let rows = match &self.still_held {
            Ok(rows) if rows.is_empty() => return Ok(()),
            Ok(rows) => rows,
            Err(error) => {
                return Err(self.prefix(format!(
                    "Refusing to merge: whether this branch still holds an unpublished commit \
                     could not be read ({error})."
                )))
            }
        };
        Err(self.prefix(format!(
            "Refusing to merge: the pull request does not carry all of this branch's committed \
             work yet: {}. Any `write`, or a `run` with a commit_msg, on this branch retries a \
             held commit, and patching the PR artifact with action:\"publish\" retries both.",
            describe_held(rows)
        )))
    }
}

/// The warning a rendered PR shows while its source branch holds a commit
/// that has not reached it.
pub(crate) async fn held_warning(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
) -> Option<String> {
    let rows = blocking_rows(&orch.db.local, project_id, branch)
        .await
        .ok()?;
    (!rows.is_empty()).then(|| {
        format!(
            "⚠️ Committed work this pull request does not carry yet: {}. Merging is refused \
             until it is published. Any `write`, or a `run` with a commit_msg, on this branch \
             retries a held commit, and action:\"publish\" retries both.",
            describe_held(&rows)
        )
    })
}

/// Name each held commit with its origin and why it is held.
pub(crate) fn describe_held(rows: &[HeldRow]) -> String {
    rows.iter()
        .map(
            |row| match (row.status.as_str(), row.landed_head.as_deref()) {
                ("unpublished", Some(landed)) => format!(
                    "commit {landed} ({}) is on the branch here but has not reached the remote \
                 (last attempt: {})",
                    row.origin(),
                    row.last_error
                ),
                _ => format!(
                    "commit {} ({}{}) has not been published to the branch (last attempt: {})",
                    row.delta_commit,
                    row.origin(),
                    row.reference
                        .as_deref()
                        .map(|reference| format!(", held at `{reference}`"))
                        .unwrap_or_default(),
                    row.last_error
                ),
            },
        )
        .collect::<Vec<_>>()
        .join("; ")
}

/// A commit barrier for a PR artifact action on `branch`: drain its held
/// deltas under the store lock, climb their remaining rungs, and report what is
/// still held.
pub(crate) async fn pr_commit_barrier(
    orch: &Orchestrator,
    project_id: &str,
    branch: &str,
) -> PrBarrier {
    let pending = match blocking_rows(&orch.db.local, project_id, branch).await {
        Ok(rows) => rows,
        Err(error) => {
            return PrBarrier {
                notes: Vec::new(),
                still_held: Err(error),
            }
        }
    };
    let Some(store) = pending.first().map(|row| row.store_path.clone()) else {
        return PrBarrier {
            notes: Vec::new(),
            still_held: Ok(Vec::new()),
        };
    };
    let mut notes = Vec::new();
    // A row that landed here but never reached the remote is carried by any
    // push of the branch, so one retry covers all of them. It runs before the
    // drain: a delta the drain lands now is pushed by its own rungs.
    let unpublished: Vec<&HeldRow> = pending
        .iter()
        .filter(|row| row.status == "unpublished")
        .collect();
    if !unpublished.is_empty() {
        match crate::orchestrator::base_advance::publish_managed_branch(orch, &store, branch).await
        {
            Ok(()) => {
                for row in &unpublished {
                    settle_row(
                        &orch.db.local,
                        &row.attempt_id,
                        "landed",
                        row.landed_head.as_deref(),
                    )
                    .await;
                }
                notes.push(format!(
                    "✓ Published `{branch}` with {} commit(s) that had not reached the remote.",
                    unpublished.len()
                ));
            }
            Err(error) => {
                for row in &unpublished {
                    record_unpublished(&orch.db.local, &row.attempt_id, &error).await;
                }
                notes.push(format!(
                    "⚠️ `{branch}` still could not be published to the remote: {error}"
                ));
            }
        }
    }
    if pending.iter().any(|row| row.status == "held") {
        let drained = match crate::mcp::vcs::acquire_store_lock(
            orch,
            Some(&store),
            crate::orchestrator::StoreLockClass::Foreground,
            "held publication drain",
            crate::mcp::vcs::STORE_LOCK_TIMEOUT,
        )
        .await
        {
            Ok(guard) => {
                let trace = guard.as_ref().map(|guard| guard.trace());
                drain_held_publications(orch, project_id, branch, trace.as_ref()).await
            }
            Err(error) => vec![Drained::Note(format!(
                "⚠️ Held commits on this branch could not be published: {error}"
            ))],
        };
        notes.extend(settle_drained(orch, drained).await);
    }
    PrBarrier {
        notes,
        still_held: blocking_rows(&orch.db.local, project_id, branch).await,
    }
}

/// [`pr_commit_barrier`] for the branch a job commits to.
pub(crate) async fn job_commit_barrier(orch: &Orchestrator, job_id: &str) -> PrBarrier {
    let located = async {
        let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
            .await
            .map_err(|error| error.to_string())?;
        db.query_opt(
            "SELECT project_id, branch FROM jobs WHERE id = ?1",
            (job_id.to_string(),),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .map_err(|error| error.to_string())
    }
    .await;
    match located {
        Ok(Some((project_id, Some(branch)))) => pr_commit_barrier(orch, &project_id, &branch).await,
        // A job with no branch of its own commits nothing that could be held.
        Ok(_) => PrBarrier {
            notes: Vec::new(),
            still_held: Ok(Vec::new()),
        },
        Err(error) => PrBarrier {
            notes: Vec::new(),
            still_held: Err(format!("locate the job's branch: {error}")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(delta: &str, message: &str) -> HeldRow {
        HeldRow {
            attempt_id: format!("attempt-{delta}"),
            project_id: "project".into(),
            store_path: PathBuf::from("/store"),
            repository_path: PathBuf::from("/repo"),
            branch: "feature".into(),
            job_id: Some("job".into()),
            base_commit: "base".into(),
            delta_commit: delta.into(),
            commit_message: message.into(),
            author_name: None,
            author_email: None,
            reference: Some(format!("refs/cairn/stranded/job/attempt-{delta}")),
            last_error: "the lock was held".into(),
            status: "held".into(),
            landed_head: None,
        }
    }

    fn held_delta(reference: Result<String, String>, recorded: Result<(), String>) -> HeldDelta {
        HeldDelta {
            delta_commit: "1111111111111111111111111111111111111111".into(),
            branch: "agent/feature".into(),
            error: "write logical-head child commit: contention".into(),
            reference,
            recorded,
        }
    }

    /// Whatever went wrong while holding, the agent is told what it can rely
    /// on, and "do not redo" only when republication is actually scheduled.
    #[test]
    fn the_held_message_says_exactly_what_the_hold_achieved() {
        let full = held_delta(Ok("refs/cairn/stranded/job/a".into()), Ok(())).message();
        assert!(
            full.contains("held at `refs/cairn/stranded/job/a`"),
            "{full}"
        );
        assert!(full.contains("Do not redo"), "{full}");
        assert!(full.contains("`run` a trivial command"), "{full}");

        let no_ref = held_delta(Err("ref locked".into()), Ok(())).message();
        assert!(no_ref.contains("Do not redo"), "{no_ref}");
        assert!(no_ref.contains("ref locked"), "{no_ref}");
        assert!(no_ref.contains("git show 1111111"), "{no_ref}");

        let unrecorded =
            held_delta(Ok("refs/cairn/stranded/job/a".into()), Err("db".into())).message();
        assert!(!unrecorded.contains("Do not redo"), "{unrecorded}");
        assert!(unrecorded.contains("re-apply"), "{unrecorded}");

        for message in [
            full,
            no_ref,
            unrecorded,
            not_in_store_message("2222", "pack refused"),
        ] {
            assert!(!message.contains("not rerun locally"), "{message}");
            crate::system_prompt::assert_no_substrate_vocabulary("held publication", &message);
        }
    }

    #[test]
    fn a_held_commit_is_named_by_the_message_its_batch_gave() {
        assert_eq!(
            row("d", "feat: roles\n\nbody").origin(),
            "from an earlier batch: \"feat: roles\""
        );
        assert_eq!(
            row("d", "^").origin(),
            "from an earlier batch that amended the previous commit"
        );
    }

    /// A merge is refused while anything is held, and when what is held
    /// cannot be read, because the pull request would ship without it.
    #[test]
    fn a_merge_is_refused_while_the_branch_holds_a_commit() {
        let clear = PrBarrier {
            notes: Vec::new(),
            still_held: Ok(Vec::new()),
        };
        assert!(clear.refuse_merge().is_ok());
        assert_eq!(clear.warn_if_held("saved".into()), "saved");

        let held = PrBarrier {
            notes: vec!["✓ Published held commit abc (from an earlier batch)".into()],
            still_held: Ok(vec![row("deadbeef", "feat: roles")]),
        };
        let refusal = held.refuse_merge().unwrap_err();
        assert!(
            refusal.starts_with("✓ Published held commit abc"),
            "{refusal}"
        );
        assert!(refusal.contains("Refusing to merge"), "{refusal}");
        assert!(refusal.contains("deadbeef"), "{refusal}");
        assert!(
            refusal.contains("refs/cairn/stranded/job/attempt-deadbeef"),
            "{refusal}"
        );
        assert!(held
            .warn_if_held("saved".into())
            .contains("merging is refused"));

        let unknown = PrBarrier {
            notes: Vec::new(),
            still_held: Err("db closed".into()),
        };
        assert!(unknown.refuse_merge().unwrap_err().contains("db closed"));
    }
}
