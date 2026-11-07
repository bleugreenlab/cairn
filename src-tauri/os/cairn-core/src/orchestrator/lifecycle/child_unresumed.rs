//! Parent attention for a child whose turn a host restart cancelled and whose
//! one automatic resume did not take.
//!
//! ## The failure this names
//!
//! cairn/4751's builder stopped amid executor link failures on 2026-09-05. Its
//! queued successor turn was minted with no run, and the boot sweep settled it
//! `cancelled/orphaned_pending`. The job stayed non-terminal, nothing was
//! running, and the work sat invisible until someone happened to message the
//! node twenty-four days later. Its parent coordinator heard nothing.
//!
//! The boot resume (`runs::queries::resume_jobs_interrupted_by_restart`) now
//! closes most of that, and it runs exactly once. What it leaves open is the
//! case where that one attempt does not take. Nobody hears about that, and this
//! module is the fact that says so.
//!
//! ## The predicate is durable rows, not the resume's outcome
//!
//! A resume can fail in three ways: refused after it minted a turn, which leaves
//! a cancelled `launch_refused` row; refused before minting, which leaves only a
//! log line; or never attempted at all, by a pass that skipped selection or a
//! host that predates the resume. Only the first is visible in the resume's
//! summary, and that summary is discarded anyway. All three leave the same row
//! shape: the job's latest work turn, not counting refused launches, is still
//! the one the restart cancelled. Keying on that shape covers them with one rule,
//! holds across any number of boots, and is what a later successful turn retires
//! on its own.
//!
//! ## The sweep waits for this process's pass
//!
//! Saying a resume did not take is false until the resume has had its chance, so
//! the sweep does nothing at all before the pass has finished. A turn an earlier
//! boot settled waits the same few seconds, which costs nothing and keeps one
//! rule. A host that runs no pass, such as an inert server, resumes nothing and so
//! has nothing to report.
//!
//! ## Its own question about motion
//!
//! This asks "is something actually moving this job?" and not watchdog recovery's
//! `PENDING_RESUME_TRIGGER_SQL`, which answers a different question and is too
//! loose for this one. Any non-terminal sub-task satisfies it, and the standard
//! recipe creates a builder's review task at the start of the execution, where it
//! sits `pending` waiting for that builder. cairn/4751's builder had one for the
//! whole of its twenty-four days, so reading that constant would have kept this
//! fact silent for the very specimen it exists for. A sub-task counts here only
//! when it is in motion, and a sub-task the same restart stranded does not, since
//! it is excluded from having a fact of its own and would otherwise silence both.
//! Wake subscriptions do not count either: one may never fire, and a muted one
//! rouses nothing. A parent woken early by one reads a withdrawn or re-derived
//! fact, which is much cheaper than open-ended silence. See cairn/6124 for the
//! recovery side of the same looseness.
//!
//! ## Separate from provider-silence stalls
//!
//! [`super::child_stall`] is about a node holding no session that could do the
//! work, and it enters on a watchdog-closed session. Nothing here looks at
//! sessions. cairn/4751's builder held an open provider session throughout, and
//! an open session is no evidence that orphaned work is progressing.
//!
//! ## Observation, never authorization
//!
//! Nothing here writes to the child's job, turns, sessions or runs. The resume
//! has had its one attempt, and a second mechanism starting turns from a
//! different vantage point would be a retry loop nobody budgeted. The parent is
//! told, and the parent decides whether to resume, replace or close.

use std::collections::HashSet;

use crate::models::TurnEndReason;
use crate::orchestrator::attention_push::delete_pending_by_id;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

use super::child_attention::{tell_child_watchers, wake_recipients, ChildFact};

use cairn_attention::push::CHILD_UNRESUMED_PUSH_PREFIX;

/// The turn the restart cancelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Episode {
    pub(crate) turn_id: String,
    pub(crate) end_reason: String,
    pub(crate) started: bool,
    pub(crate) created_at: i64,
    pub(crate) sequence: i64,
    pub(crate) ended_at: i64,
    pub(crate) run_id: Option<String>,
}

impl Episode {
    /// The same two end reasons the boot resume keys on, because this fact is that
    /// resume's report.
    fn is_restart_cancelled(&self) -> bool {
        self.end_reason == TurnEndReason::OrphanedPending.to_string()
            || self.end_reason == TurnEndReason::HostRestart.to_string()
    }
}

/// The newest run the rows can name as evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RunEvidence {
    pub(crate) id: String,
    pub(crate) status: String,
    pub(crate) exit_reason: Option<String>,
    /// Whether the cancelled turn was bound to this run. A runless turn has only
    /// its job's last run to point at, which is what cairn/4751's crash looked
    /// like from the parent's side.
    pub(crate) bound: bool,
}

/// The refused launches since the episode's turn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Refusals {
    pub(crate) count: i64,
    pub(crate) newest_at: Option<i64>,
}

/// One child node whose restart resume did not take, with the evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnresumedChild {
    pub(crate) issue_id: String,
    pub(crate) issue_uri: String,
    pub(crate) node_uri: String,
    pub(crate) episode: Episode,
    pub(crate) run: Option<RunEvidence>,
    pub(crate) refusals: Refusals,
}

impl UnresumedChild {
    /// One episode is one turn, so repeated sweeps, repeated boots and later
    /// refused launches all keep this value and wake nobody, while a new orphan
    /// after a real recovery is a new turn and wakes the parent again. Nothing
    /// time-derived appears here.
    pub(crate) fn fingerprint(&self) -> String {
        self.episode.turn_id.clone()
    }

    fn key(&self) -> String {
        unresumed_push_key(&self.node_uri)
    }

    /// The fact in words, as the parent reads it. `now` is a parameter so the
    /// rendered spans can be pinned in a test.
    pub(crate) fn render(&self, now: i64) -> String {
        let mut out = format!(
            "Child `{}` has unfinished work that nothing is running, and {} is still open.\n\n{} {}",
            self.node_uri,
            self.issue_uri,
            self.turn_sentence(now),
            self.resume_sentence(now),
        );
        if let Some(run) = &self.run {
            let subject = if run.bound {
                "Its run"
            } else {
                "The turn never had a run; the node's last run"
            };
            out.push_str(&format!(
                " {subject} `{}` reads `{}`{}.",
                run.id,
                run.status,
                run.exit_reason
                    .as_deref()
                    .map(|reason| format!(" (`{reason}`)"))
                    .unwrap_or_default(),
            ));
        }
        out.push_str(
            "\n\nThe work is resumable, and nothing will resume it automatically: send the node \
             a message to continue it, or replace or close the child.",
        );
        out
    }

    fn turn_sentence(&self, now: i64) -> String {
        let episode = &self.episode;
        let ended = crate::clock::host().age(episode.ended_at, now);
        if episode.started {
            format!(
                "Its turn `{}` was interrupted mid-flight by a host restart (`{}`, {ended}), so \
                 the outcome of its last tool call is unknown.",
                episode.turn_id, episode.end_reason
            )
        } else {
            format!(
                "Its queued turn `{}` was cancelled by a host restart before it started (`{}`, \
                 {ended}).",
                episode.turn_id, episode.end_reason
            )
        }
    }

    fn resume_sentence(&self, now: i64) -> String {
        const LEAD: &str = "The one automatic resume after that restart did not take";
        match (self.refusals.count, self.refusals.newest_at) {
            (count, Some(newest_at)) if count > 0 => format!(
                "{LEAD} ({count} refused launch{} since; the newest {}).",
                if count == 1 { "" } else { "es" },
                crate::clock::host().age(newest_at, now),
            ),
            _ => format!("{LEAD}: no resume turn was ever minted."),
        }
    }
}

/// The push key of one child node. Canonical, because the store canonicalizes
/// the keys it writes and the withdrawal below compares against what it stored.
fn unresumed_push_key(node_uri: &str) -> String {
    cairn_uri::canonicalize_uri_identity(&format!("{CHILD_UNRESUMED_PUSH_PREFIX}:{node_uri}"))
}

/// Every node of a child issue that is not over and could hold a stranded turn.
///
/// `parent_job_id IS NULL` scopes this to a child issue's own nodes. A sub-agent
/// task is its parent node's problem, and a job's execution must be the issue's
/// canonical one: a superseded execution can leave a job Pending indefinitely,
/// and telling the parent about work its own re-run replaced would be false.
const CANDIDATE_SQL: &str = "SELECT j.id, j.status, i.id, p.key, i.number, e.seq, j.uri_segment
       FROM jobs j
       JOIN issues i ON i.id = j.issue_id
       JOIN projects p ON p.id = i.project_id
       JOIN executions e ON e.id = j.execution_id
      WHERE j.parent_job_id IS NULL
        AND j.uri_segment IS NOT NULL
        AND j.status NOT IN ('failed', 'cancelled', 'complete')
        AND e.lifecycle = 'canonical'
        AND (i.parent_issue_id IS NOT NULL OR i.parent_thread_id IS NOT NULL)
        AND i.status NOT IN ('merged', 'closed', 'failed')";

/// The job's latest work turn, not counting a refused launch: the same reading
/// `restart_resume_note` makes when it decides what a later launch owes the
/// agent. Any later queued, running or started turn takes this position, and
/// that is how the child's own recovery retires the fact.
async fn episode_turn(db: &LocalDb, job_id: &str) -> Result<Option<Episode>, String> {
    let row = db
        .query_opt(
            "SELECT id, COALESCE(end_reason, ''), started_at IS NOT NULL, created_at, sequence,
                    ended_at, run_id
               FROM turns
              WHERE job_id = ?1
                AND start_reason != 'memory_review'
                AND COALESCE(end_reason, '') != 'launch_refused'
              ORDER BY created_at DESC, sequence DESC
              LIMIT 1",
            (job_id.to_string(),),
            |row| {
                Ok((
                    row.text(0)?,
                    row.text(1)?,
                    row.i64(2)? != 0,
                    row.i64(3)?,
                    row.i64(4)?,
                    row.opt_i64(5)?,
                    row.opt_text(6)?,
                ))
            },
        )
        .await
        .map_err(|error| format!("unresumed-child sweep failed to load a turn: {error}"))?;
    let Some((turn_id, end_reason, started, created_at, sequence, ended_at, run_id)) = row else {
        return Ok(None);
    };
    // A turn that has not ended is not an episode, whatever else it is.
    Ok(ended_at.map(|ended_at| Episode {
        turn_id,
        end_reason,
        started,
        created_at,
        sequence,
        ended_at,
        run_id,
    }))
}

/// Whether a job is running, or has something durable that will start it: a run,
/// a queued message, or a wait such as an unanswered question.
const IN_MOTION_SQL: &str = "SELECT 1 WHERE
       EXISTS (SELECT 1 FROM runs WHERE job_id = ?1 AND status IN ('starting', 'live'))
       OR EXISTS (SELECT 1 FROM queued_messages WHERE job_id = ?1 AND delivered_at IS NULL)
       OR EXISTS (SELECT 1 FROM agent_waits WHERE job_id = ?1 AND state IN ('pending', 'resolving'))
     LIMIT 1";

/// A job's open sub-tasks, each with whether its latest work turn has yet to end.
/// A task that never started has no such turn, and neither does one the restart
/// stranded, whose latest turn has ended.
const OPEN_TASKS_SQL: &str = "SELECT task.id,
            COALESCE((SELECT head.ended_at IS NULL FROM turns head
                       WHERE head.job_id = task.id AND head.start_reason != 'memory_review'
                       ORDER BY head.created_at DESC, head.sequence DESC
                       LIMIT 1), 0)
       FROM jobs task
      WHERE task.parent_job_id = ?1
        AND task.status NOT IN ('failed', 'cancelled', 'complete')";

async fn job_in_motion(db: &LocalDb, job_id: &str) -> Result<bool, String> {
    db.query_opt(IN_MOTION_SQL, (job_id.to_string(),), |_| Ok(()))
        .await
        .map(|row| row.is_some())
        .map_err(|error| format!("unresumed-child sweep failed to read job motion: {error}"))
}

/// Whether anything is actually moving this job, on it or on a sub-task at any
/// depth. A task blocked on its own sub-task holds no run of its own, so the walk
/// has to go all the way down. `parent_job_id` is a tree, so it terminates.
async fn in_motion(db: &LocalDb, job_id: &str, claimed: &HashSet<String>) -> Result<bool, String> {
    if claimed.contains(job_id) || job_in_motion(db, job_id).await? {
        return Ok(true);
    }
    let mut parents = vec![job_id.to_string()];
    while let Some(parent) = parents.pop() {
        let tasks = db
            .query_all(OPEN_TASKS_SQL, (parent,), |row| {
                Ok((row.text(0)?, row.i64(1)? != 0))
            })
            .await
            .map_err(|error| format!("unresumed-child sweep failed to load sub-tasks: {error}"))?;
        for (task, head_unended) in tasks {
            if head_unended || claimed.contains(&task) || job_in_motion(db, &task).await? {
                return Ok(true);
            }
            parents.push(task);
        }
    }
    Ok(false)
}

async fn run_evidence(
    db: &LocalDb,
    job_id: &str,
    episode: &Episode,
) -> Result<Option<RunEvidence>, String> {
    let row = match episode.run_id.as_deref() {
        Some(run_id) => {
            db.query_opt(
                "SELECT id, COALESCE(status, 'unknown'), exit_reason FROM runs WHERE id = ?1",
                (run_id.to_string(),),
                |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)),
            )
            .await
        }
        None => {
            db.query_opt(
                "SELECT id, COALESCE(status, 'unknown'), exit_reason FROM runs
                  WHERE job_id = ?1
                  ORDER BY created_at DESC, rowid DESC
                  LIMIT 1",
                (job_id.to_string(),),
                |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)),
            )
            .await
        }
    }
    .map_err(|error| format!("unresumed-child sweep failed to load a run: {error}"))?;
    Ok(row.map(|(id, status, exit_reason)| RunEvidence {
        id,
        status,
        exit_reason,
        bound: episode.run_id.is_some(),
    }))
}

async fn refusals_since(db: &LocalDb, job_id: &str, episode: &Episode) -> Result<Refusals, String> {
    db.query_opt(
        "SELECT COUNT(*), MAX(created_at) FROM turns
          WHERE job_id = ?1
            AND end_reason = 'launch_refused'
            AND (created_at > ?2 OR (created_at = ?2 AND sequence > ?3))",
        (job_id.to_string(), episode.created_at, episode.sequence),
        |row| {
            Ok(Refusals {
                count: row.i64(0)?,
                newest_at: row.opt_i64(1)?,
            })
        },
    )
    .await
    .map(Option::unwrap_or_default)
    .map_err(|error| format!("unresumed-child sweep failed to count refusals: {error}"))
}

/// One candidate node, before the turn evidence is read.
struct Candidate {
    job_id: String,
    job_status: String,
    issue_id: String,
    issue_uri: String,
    node_uri: String,
}

/// Every child node in `db` whose restart cancelled its turn and which nothing is
/// moving. Whether the resume has had its chance is the callers' gate, since it is
/// one fact about this process and not a row.
///
/// `claimed` is the set of jobs with a launch in flight, which is held in memory
/// and so is passed in rather than read here.
pub(crate) async fn unresumed_children(
    db: &LocalDb,
    claimed: &HashSet<String>,
) -> Result<Vec<UnresumedChild>, String> {
    let candidates = db
        .query_all(CANDIDATE_SQL, (), |row| {
            let project_key = row.text(3)?;
            let number = row.i64(4)? as i32;
            let exec_seq = row.i64(5)? as i32;
            let segment = row.text(6)?;
            Ok(Candidate {
                job_id: row.text(0)?,
                job_status: row.text(1)?,
                issue_id: row.text(2)?,
                issue_uri: cairn_uri::build_issue_uri(&project_key, number),
                node_uri: cairn_uri::build_node_uri(&project_key, number, exec_seq, &segment),
            })
        })
        .await
        .map_err(|error| format!("unresumed-child sweep failed to load candidates: {error}"))?;

    let mut unresumed = Vec::new();
    for candidate in candidates {
        if let Some(child) = evaluate(db, candidate, claimed).await? {
            unresumed.push(child);
        }
    }
    Ok(unresumed)
}

/// Decide one candidate. The cheap disqualifiers run first, and each gate is a
/// distinct claim:
///
/// 1. **The job is not over.** Its parent hears about a terminal job through the
///    resolution path.
/// 2. **Its latest work turn is the one the restart cancelled.**
/// 3. **Nothing is moving it** ([`in_motion`]).
async fn evaluate(
    db: &LocalDb,
    candidate: Candidate,
    claimed: &HashSet<String>,
) -> Result<Option<UnresumedChild>, String> {
    if crate::orchestrator::wakes::job_is_terminal(&candidate.job_status) {
        return Ok(None);
    }
    let Some(episode) = episode_turn(db, &candidate.job_id).await? else {
        return Ok(None);
    };
    if !episode.is_restart_cancelled() || in_motion(db, &candidate.job_id, claimed).await? {
        return Ok(None);
    }
    let run = run_evidence(db, &candidate.job_id, &episode).await?;
    let refusals = refusals_since(db, &candidate.job_id, &episode).await?;
    Ok(Some(UnresumedChild {
        issue_id: candidate.issue_id,
        issue_uri: candidate.issue_uri,
        node_uri: candidate.node_uri,
        episode,
        run,
        refusals,
    }))
}

/// Tell the child's watchers, and hand back the ones a wake should rouse.
async fn tell_unresumed_watchers(
    db: &LocalDb,
    child: &UnresumedChild,
) -> Result<Vec<String>, String> {
    tell_child_watchers(
        db,
        &ChildFact {
            issue_id: &child.issue_id,
            issue_uri: &child.issue_uri,
            node_uri: &child.node_uri,
            key: child.key(),
            fingerprint: child.fingerprint(),
        },
    )
    .await
}

/// Delete every undelivered fact whose node no longer stands, and report how
/// many went.
///
/// An undelivered fact whose episode ended owes nothing: the child's own turns
/// are the record of what happened. `attention_pushes` has no retirement reason
/// for this, and adding one means rebuilding that table, so the row is withdrawn
/// rather than retired. A delivered row is left alone, because the parent has
/// already read it. A standing node whose fingerprint changed needs nothing here,
/// since the push that told it superseded the row in place.
async fn withdraw_settled(db: &LocalDb, standing: &[UnresumedChild]) -> Result<usize, String> {
    let pending = db
        .query_all(
            "SELECT id, key FROM attention_pushes
              WHERE key LIKE ?1 AND delivered_event_id IS NULL AND retired_at IS NULL",
            (format!("{CHILD_UNRESUMED_PUSH_PREFIX}:%"),),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("unresumed-child sweep failed to load its pushes: {error}"))?;
    let standing: HashSet<String> = standing.iter().map(UnresumedChild::key).collect();
    let mut withdrawn = 0;
    for (id, key) in pending {
        if standing.contains(&key) {
            continue;
        }
        delete_pending_by_id(db, &id)
            .await
            .map_err(|error| format!("unresumed-child sweep failed to withdraw {key}: {error}"))?;
        withdrawn += 1;
    }
    Ok(withdrawn)
}

/// What one pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct UnresumedSweep {
    /// Each node that produced a rousing notice, with who it roused.
    pub(crate) told: Vec<(String, Vec<String>)>,
    pub(crate) withdrawn: usize,
}

/// The database half of one pass: tell, then withdraw. A notice that fails for
/// one node is logged and does not cost the others theirs.
pub(crate) async fn sweep_unresumed_in(
    db: &LocalDb,
    claimed: &HashSet<String>,
) -> Result<UnresumedSweep, String> {
    let standing = unresumed_children(db, claimed).await?;
    let mut sweep = UnresumedSweep::default();
    for child in &standing {
        match tell_unresumed_watchers(db, child).await {
            Ok(recipients) if recipients.is_empty() => {}
            Ok(recipients) => sweep.told.push((child.node_uri.clone(), recipients)),
            Err(error) => {
                log::warn!(
                    "unresumed-child notice for {} failed: {error}",
                    child.node_uri
                );
            }
        }
    }
    sweep.withdrawn = withdraw_settled(db, &standing).await?;
    Ok(sweep)
}

/// The periodic pass. This host's private database only, the same scope as the
/// boot resume, because a team replica's jobs are resumed by their owner's host.
/// It neither tells nor withdraws until this process's resume pass has finished.
pub(crate) async fn sweep_unresumed_children(orch: &Orchestrator) -> UnresumedSweep {
    if !orch.restart_resume_pass_over() {
        return UnresumedSweep::default();
    }
    let sweep = match sweep_unresumed_in(&orch.db.local, &orch.claimed_launch_job_ids()).await {
        Ok(sweep) => sweep,
        Err(error) => {
            log::error!("{error}");
            return UnresumedSweep::default();
        }
    };
    for (node_uri, recipients) in &sweep.told {
        log::info!(
            "unresumed child {node_uri}: its restart resume did not take, telling {} watcher(s)",
            recipients.len()
        );
        wake_recipients(orch, recipients, "unresumed-child");
    }
    if sweep.withdrawn > 0 {
        orch.notifier.emit_change("attention_pushes");
        log::info!(
            "withdrew {} unresumed-child notice(s) whose episode ended",
            sweep.withdrawn
        );
    }
    sweep
}

/// The body a `child-unresumed:` push renders at delivery, re-derived from the
/// live rows.
///
/// The sweep withdraws a fact whose episode ended, but delivery can beat it by up
/// to one tick, and a parent told about a node that has moved on should read that
/// rather than spend a turn finding it out. Before this process's pass has
/// finished the rows cannot say either way, so the push falls back to its pointer.
pub(crate) async fn render_unresumed_push_body(
    orch: &Orchestrator,
    recipient_job_id: &str,
    node_uri: &str,
) -> Option<String> {
    if !orch.restart_resume_pass_over() {
        return None;
    }
    let db = crate::execution::routing::owning_db_for_job(&orch.db, recipient_job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let now = chrono::Utc::now().timestamp();
    let standing = unresumed_children(&db, &orch.claimed_launch_job_ids())
        .await
        .ok()?;
    let node_uri = cairn_uri::canonicalize_uri_identity(node_uri);
    match standing
        .into_iter()
        .find(|child| cairn_uri::canonicalize_uri_identity(&child.node_uri) == node_uri)
    {
        Some(child) => Some(child.render(now)),
        None => Some(format!(
            "Child `{node_uri}` was reported with unfinished work nothing was running. It is no \
             longer in that state: it has since started a turn, or something is about to resume \
             it, or its issue has resolved. Nothing is owed here unless you want to check what \
             it did in between."
        )),
    }
}
