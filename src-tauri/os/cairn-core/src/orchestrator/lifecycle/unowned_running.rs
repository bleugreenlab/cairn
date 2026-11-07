//! Running jobs that nothing will move.
//!
//! ## The shape
//!
//! A job reads `running`, but its latest work turn ended without finished work:
//! the provider failed it, or a crash was recorded that the process outlived.
//! Nothing durable owns the job's next transition either: no turn is pending,
//! running or yielded, no launch is in flight, no retry is reserved, and no wait,
//! question, permission or dependency holds it. cairn/5958's planner sat in this
//! shape for 6h45m with its Codex process warm, until an unrelated direct
//! message happened to resume it (cairn/6189).
//!
//! The queued-content idle flush cannot own this. It resumes only for
//! deliverable waking content, which is right for its narrower contract, and
//! pending content is work to drain, not proof that anything will resume the
//! job. Boot recovery owns a host restart and watchdog recovery owns provider
//! silence. This module owns what is left.
//!
//! ## Liveness is read, not inferred
//!
//! What happens depends on whether the provider behind the job's session is
//! alive, dead, or unknowable since a time:
//!
//! - **Alive**: this host holds the session's process, parked warm. The job is
//!   continued on it through the ordinary launch funnel as an automatic retry.
//!   The native conversation is kept, the launch drains whatever is pending for
//!   the job, and the retry chain bounds the attempts.
//! - **Busy**: the process is serving something, which owns the transition.
//! - **Dead**: there is no process and no run claims to be live. The existing
//!   death policy applies, and this module does nothing.
//! - **Unknown since T**: there is no process, yet a run row still reads live.
//!   A missing handle is not evidence of death, so nothing is launched. T is
//!   that run's last recorded update.
//!
//! A live run row or an open session alone is never read as liveness.
//!
//! ## Bounded and reported once
//!
//! A continuation is an automatic retry, so it shares the capacity retry's chain
//! and budget rather than multiplying it, and each episode turn can be claimed
//! once: a successor that never started leaves its predecessor with a successor,
//! which the claim refuses. When no continuation can be made (the budget is
//! spent, the one dispatch did not start, or liveness is unknown) the node's
//! watchers are told with a `child-unowned:` fact whose fingerprint is the
//! episode turn and the cause, so repeated sweeps and boots wake nobody twice.
//! Only a child issue's node has watchers to tell. Any other job stays visible
//! through the operator's agent-failure entry for its turn.
//!
//! ## Scope
//!
//! This host's private database only, the same scope as the restart resume: a
//! team replica's job belongs to the host running it, and this host's lack of a
//! process for it says nothing about that host.

use std::collections::HashSet;
use std::sync::Arc;

use cairn_attention::push::CHILD_UNOWNED_PUSH_PREFIX;

use crate::agent_process::process::AgentProcessState as ProcessState;
use crate::models::TurnEndReason;
use crate::orchestrator::attention_push::delete_pending_by_id;
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, LocalDb, RowExt};

use super::child_attention::{tell_child_watchers, wake_recipients, ChildFact};

/// How long an ended turn stands before the sweep calls it unowned. Every owner
/// that acts on a turn end (a capacity retry's claim, the queued-content flush,
/// a parent's answer) does so well inside this.
pub(crate) const UNOWNED_GRACE_SECS: i64 = 30;

/// Automatic continuations one failure episode may take. The same three the
/// capacity retry takes, counted on the same retry chain.
pub(crate) const CONTINUATION_BUDGET: u32 = 3;

/// Whether the provider behind a job's session is alive, as this host can tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Liveness {
    /// This host holds the session's process, parked warm between turns.
    Alive { run_id: String },
    /// This host holds the session's process and it is serving something.
    Busy,
    /// No process, and no run of the job claims to be live.
    Dead,
    /// No process, yet a run row still reads live; `since` is its last update.
    Unknown { since: i64 },
}

/// The job's latest work turn and what has been tried since it ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Episode {
    pub(crate) turn_id: String,
    pub(crate) state: String,
    pub(crate) end_reason: String,
    pub(crate) ended_at: i64,
    /// Consecutive automatic retries ending at this turn, this turn included.
    pub(crate) retries: u32,
    /// A successor was claimed for this turn and never started.
    pub(crate) dispatch_failed: bool,
}

impl Episode {
    /// A turn that ended without finished work for a reason no owner answers:
    /// a provider failure, or a recorded crash. A user stop, an artifact
    /// handoff, a restart and a supersession each have their own owner.
    fn is_unfinished_failure(&self) -> bool {
        self.state == "interrupted"
            && (self.end_reason == TurnEndReason::ProviderFailure.to_string()
                || self.end_reason == TurnEndReason::Crash.to_string())
    }
}

/// Why a stranded job was reported instead of continued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Cause {
    BudgetSpent,
    DispatchFailed,
    LivenessUnknown { since: i64 },
}

impl Cause {
    /// Stable across sweeps: nothing time-derived, so a standing fact wakes once.
    fn tag(&self) -> &'static str {
        match self {
            Cause::BudgetSpent => "budget_spent",
            Cause::DispatchFailed => "dispatch_failed",
            Cause::LivenessUnknown { .. } => "liveness_unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Something owns the job, or it is not this module's shape.
    Leave,
    /// Continue the job on this warm run.
    Resume { run_id: String },
    /// Tell the job's watchers why it was not continued.
    Report(Cause),
}

/// The whole policy, from facts already read. `launch_claimed` is an in-memory
/// launch reservation for the job.
pub(crate) fn decide(
    episode: &Episode,
    liveness: &Liveness,
    launch_claimed: bool,
    now: i64,
) -> Decision {
    if launch_claimed
        || !episode.is_unfinished_failure()
        || episode.ended_at > now - UNOWNED_GRACE_SECS
    {
        return Decision::Leave;
    }
    match liveness {
        Liveness::Busy | Liveness::Dead => Decision::Leave,
        Liveness::Unknown { since } => Decision::Report(Cause::LivenessUnknown { since: *since }),
        Liveness::Alive { run_id } => {
            if episode.dispatch_failed {
                Decision::Report(Cause::DispatchFailed)
            } else if episode.retries >= CONTINUATION_BUDGET {
                Decision::Report(Cause::BudgetSpent)
            } else {
                Decision::Resume {
                    run_id: run_id.clone(),
                }
            }
        }
    }
}

/// One running job nothing owns, with the evidence and the decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnownedJob {
    pub(crate) job_id: String,
    pub(crate) session_id: String,
    pub(crate) issue_id: String,
    pub(crate) issue_uri: String,
    /// The node URI of a child issue's own node; `None` for a sub-agent task or
    /// a node on an issue with no parent, which have no watchers to tell.
    pub(crate) child_node_uri: Option<String>,
    pub(crate) episode: Episode,
    pub(crate) decision: Decision,
}

impl UnownedJob {
    fn fingerprint(&self, cause: &Cause) -> String {
        format!("{}:{}", self.episode.turn_id, cause.tag())
    }

    /// The fact in words, as the parent reads it. `now` is a parameter so the
    /// rendered spans can be pinned in a test.
    pub(crate) fn render(&self, node_uri: &str, cause: &Cause, now: i64) -> String {
        let clock = crate::clock::host();
        let ended = clock.age(self.episode.ended_at, now);
        let why = match cause {
            Cause::BudgetSpent => format!(
                "Cairn continued it automatically on its warm process {} time{}, and each attempt \
                 ended the same way, so it will not try again on its own.",
                self.episode.retries,
                if self.episode.retries == 1 { "" } else { "s" }
            ),
            Cause::DispatchFailed => {
                "Cairn's automatic continuation on its warm process did not start.".to_string()
            }
            Cause::LivenessUnknown { since } => format!(
                "This host holds no process for it, yet its run still reads live (last updated \
                 {}). Whether its provider is alive is unknown, so Cairn has not launched a \
                 second one beside it.",
                clock.age(*since, now)
            ),
        };
        format!(
            "Child `{node_uri}` is marked running, but nothing is moving it, and {} is still \
             open.\n\nIts turn `{}` ended `{}` {ended} without finished work. {why}\n\nThe work \
             is resumable: send the node a message to continue it, or replace or close the \
             child.",
            self.issue_uri, self.episode.turn_id, self.episode.end_reason,
        )
    }
}

fn unowned_push_key(node_uri: &str) -> String {
    cairn_uri::canonicalize_uri_identity(&format!("{CHILD_UNOWNED_PUSH_PREFIX}:{node_uri}"))
}

/// Every job that reads running on an open session of a canonical execution on
/// an unresolved issue, with no live turn and no pending wait. Workflows are
/// excluded: a workflow run is one-shot and its caller owns its outcome.
const CANDIDATE_SQL: &str = "SELECT j.id, j.current_session_id, i.id, p.key, i.number, e.seq,
            j.uri_segment, j.parent_job_id IS NULL
              AND (i.parent_issue_id IS NOT NULL OR i.parent_thread_id IS NOT NULL)
       FROM jobs j
       JOIN sessions s ON s.id = j.current_session_id AND s.status = 'open'
       JOIN executions e ON e.id = j.execution_id AND e.lifecycle = 'canonical'
       JOIN issues i ON i.id = j.issue_id AND i.status NOT IN ('merged', 'closed', 'failed')
       JOIN projects p ON p.id = i.project_id
      WHERE j.status = 'running'
        AND COALESCE(j.agent_config_id, '') != 'workflow'
        AND NOT EXISTS (
          SELECT 1 FROM turns live
           WHERE live.job_id = j.id AND live.state IN ('pending', 'running', 'yielded')
        )
        AND NOT EXISTS (
          SELECT 1 FROM agent_waits w
           WHERE w.job_id = j.id AND w.state IN ('pending', 'resolving')
        )";

/// The job's latest work turn. A memory review, a refused launch, and an
/// automatic retry that was claimed and abandoned before it started are not
/// work turns: the last of these is instead the evidence that a dispatch for
/// its predecessor failed.
const EPISODE_SQL: &str = "SELECT t.id, t.state, COALESCE(t.end_reason, ''),
            COALESCE(t.ended_at, t.updated_at),
            EXISTS (
              SELECT 1 FROM turns s
               WHERE s.predecessor_id = t.id
                 AND s.start_reason = 'retry'
                 AND s.started_at IS NULL
                 AND s.state IN ('interrupted', 'cancelled')
                 AND COALESCE(s.end_reason, 'launch_refused') = 'launch_refused'
            )
       FROM turns t
      WHERE t.job_id = ?1
        AND t.start_reason != 'memory_review'
        AND COALESCE(t.end_reason, '') != 'launch_refused'
        AND NOT (t.start_reason = 'retry' AND t.started_at IS NULL
                 AND t.end_reason IS NULL AND t.state IN ('interrupted', 'cancelled'))
      ORDER BY t.created_at DESC, t.sequence DESC
      LIMIT 1";

async fn retry_chain(db: &LocalDb, turn_id: &str) -> Result<u32, String> {
    let mut count = 0;
    let mut next = Some(turn_id.to_string());
    while let Some(id) = next {
        let row = db
            .query_opt(
                "SELECT start_reason, predecessor_id FROM turns WHERE id = ?1",
                (id,),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .map_err(|error| error.to_string())?;
        match row {
            Some((reason, predecessor)) if reason == "retry" => {
                count += 1;
                next = predecessor;
            }
            _ => break,
        }
    }
    Ok(count)
}

async fn episode(db: &LocalDb, job_id: &str) -> Result<Option<Episode>, String> {
    let row = db
        .query_opt(EPISODE_SQL, (job_id.to_string(),), |row| {
            Ok((
                row.text(0)?,
                row.text(1)?,
                row.text(2)?,
                row.i64(3)?,
                row.i64(4)? != 0,
            ))
        })
        .await
        .map_err(|error| format!("unowned-running sweep failed to load a turn: {error}"))?;
    let Some((turn_id, state, end_reason, ended_at, dispatch_failed)) = row else {
        return Ok(None);
    };
    let retries = retry_chain(db, &turn_id).await?;
    Ok(Some(Episode {
        turn_id,
        state,
        end_reason,
        ended_at,
        retries,
        dispatch_failed,
    }))
}

async fn liveness(
    processes: &ProcessState,
    db: &LocalDb,
    job_id: &str,
    session_id: &str,
) -> Result<Liveness, String> {
    if let Some(run_id) = processes.find_process_by_session(session_id) {
        return Ok(if processes.is_warm(&run_id) {
            Liveness::Alive { run_id }
        } else {
            Liveness::Busy
        });
    }
    let since = db
        .query_opt(
            "SELECT MAX(updated_at) FROM runs WHERE job_id = ?1 AND status IN ('starting', 'live')",
            (job_id.to_string(),),
            |row| row.opt_i64(0),
        )
        .await
        .map_err(|error| format!("unowned-running sweep failed to read runs: {error}"))?
        .flatten();
    Ok(match since {
        Some(since) => Liveness::Unknown { since },
        None => Liveness::Dead,
    })
}

/// Every job in `db` in this module's shape whose decision is not `Leave`.
/// `claimed` is the set of jobs with a launch reservation in flight.
pub(crate) async fn unowned_jobs(
    processes: &ProcessState,
    db: &LocalDb,
    claimed: &HashSet<String>,
    now: i64,
) -> Result<Vec<UnownedJob>, String> {
    let candidates = db
        .query_all(CANDIDATE_SQL, (), |row| {
            let project_key = row.text(3)?;
            let number = row.i64(4)? as i32;
            let exec_seq = row.i64(5)? as i32;
            let segment = row.opt_text(6)?;
            let child = row.i64(7)? != 0;
            Ok((
                row.text(0)?,
                row.text(1)?,
                row.text(2)?,
                cairn_uri::build_issue_uri(&project_key, number),
                segment.filter(|_| child).map(|segment| {
                    cairn_uri::build_node_uri(&project_key, number, exec_seq, &segment)
                }),
            ))
        })
        .await
        .map_err(|error| format!("unowned-running sweep failed to load candidates: {error}"))?;

    let mut unowned = Vec::new();
    for (job_id, session_id, issue_id, issue_uri, child_node_uri) in candidates {
        let Some(episode) = episode(db, &job_id).await? else {
            continue;
        };
        if !episode.is_unfinished_failure() {
            continue;
        }
        let liveness = liveness(processes, db, &job_id, &session_id).await?;
        let decision = decide(&episode, &liveness, claimed.contains(&job_id), now);
        if decision == Decision::Leave {
            continue;
        }
        unowned.push(UnownedJob {
            job_id,
            session_id,
            issue_id,
            issue_uri,
            child_node_uri,
            episode,
            decision,
        });
    }
    Ok(unowned)
}

/// Continue one job on its warm run, as an automatic retry of its episode turn.
///
/// The retry claim is the fence: it succeeds only while the episode turn is
/// still the job's head, nothing is live, and no successor exists, so a
/// concurrent continuation, a second sweep or a changed session wins cleanly.
/// A launch that does not take abandons the claimed turn, which the next sweep
/// reads as a failed dispatch and reports.
fn resume(orch: &Orchestrator, db: &Arc<LocalDb>, job: &UnownedJob, run_id: &str) -> bool {
    let short = &job.job_id[..job.job_id.len().min(8)];
    let retry_turn_id = match crate::execution::jobs::claim_retry_successor_if_head_matches(
        orch,
        db.clone(),
        &job.job_id,
        &job.session_id,
        &job.episode.turn_id,
    ) {
        Ok(Some(turn_id)) => turn_id,
        Ok(None) => return false,
        Err(error) => {
            log::warn!(
                "unowned-running: could not reserve a continuation for job {short}: {error}"
            );
            return false;
        }
    };
    let attempt = job.episode.retries + 1;
    if let Err(error) = crate::messages::transcript::insert_system_message_sync(
        orch,
        run_id,
        Some(&job.session_id),
        Some(&job.episode.turn_id),
        &format!(
            "The last turn ended `{}` without finishing, and nothing was going to continue it. \
             Continuing on the same process ({attempt}/{CONTINUATION_BUDGET}).",
            job.episode.end_reason
        ),
        serde_json::json!({
            "kind": "unowned_running_continuation",
            "endReason": job.episode.end_reason,
            "attempt": attempt,
            "maxAttempts": CONTINUATION_BUDGET,
        }),
    ) {
        log::warn!("unowned-running: failed to note the continuation for job {short}: {error}");
    }
    match crate::execution::jobs::continue_automatic_retry(orch, &job.job_id, &retry_turn_id) {
        Ok(run) => {
            log::info!(
                "unowned-running: dispatched continuation for job {short} after `{}` turn {} \
                 (run {}, attempt {attempt}/{CONTINUATION_BUDGET}); launch outcome settles on \
                 the run",
                job.episode.end_reason,
                job.episode.turn_id,
                &run.id[..run.id.len().min(8)],
            );
            true
        }
        Err(error) => {
            log::warn!("unowned-running: continuation for job {short} did not launch: {error}");
            if let Err(cleanup) = crate::execution::jobs::abandon_pending_retry_if_head_matches(
                db.clone(),
                &job.job_id,
                &retry_turn_id,
            ) {
                log::warn!(
                    "unowned-running: failed to release unstarted continuation {retry_turn_id}: \
                     {cleanup}"
                );
            }
            false
        }
    }
}

/// Tell a child node's watchers why it was not continued, once per episode and
/// cause. Returns who a wake should rouse.
async fn tell(db: &LocalDb, job: &UnownedJob, cause: &Cause) -> Result<Vec<String>, String> {
    let Some(node_uri) = job.child_node_uri.as_deref() else {
        return Ok(Vec::new());
    };
    tell_child_watchers(
        db,
        &ChildFact {
            issue_id: &job.issue_id,
            issue_uri: &job.issue_uri,
            node_uri,
            key: unowned_push_key(node_uri),
            fingerprint: job.fingerprint(cause),
        },
    )
    .await
}

/// Delete every undelivered fact whose node no longer stands reported.
async fn withdraw_settled(db: &LocalDb, reported: &HashSet<String>) -> Result<usize, String> {
    let pending = db
        .query_all(
            "SELECT id, key FROM attention_pushes
              WHERE key LIKE ?1 AND delivered_event_id IS NULL AND retired_at IS NULL",
            (format!("{CHILD_UNOWNED_PUSH_PREFIX}:%"),),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(|error| format!("unowned-running sweep failed to load its pushes: {error}"))?;
    let mut withdrawn = 0;
    for (id, key) in pending {
        if reported.contains(&key) {
            continue;
        }
        delete_pending_by_id(db, &id)
            .await
            .map_err(|error| format!("unowned-running sweep failed to withdraw {key}: {error}"))?;
        withdrawn += 1;
    }
    Ok(withdrawn)
}

/// What one pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct UnownedSweep {
    pub(crate) resumed: Vec<String>,
    /// Each reported job, with the watchers its fact roused.
    pub(crate) reported: Vec<(String, Vec<String>)>,
    pub(crate) withdrawn: usize,
}

/// One pass over `db`: continue what can be continued on a warm process,
/// report the rest once, and withdraw facts whose node has moved on.
pub(crate) fn sweep_unowned_running_in(
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    now: i64,
) -> Result<UnownedSweep, String> {
    let found = run_db_blocking({
        let processes = orch.process_state.clone();
        let db = db.clone();
        let claimed = orch.claimed_launch_job_ids();
        move || async move { unowned_jobs(&processes, &db, &claimed, now).await }
    })?;
    let mut sweep = UnownedSweep::default();
    let mut reported_keys = HashSet::new();
    for job in &found {
        match &job.decision {
            Decision::Leave => {}
            Decision::Resume { run_id } => {
                if resume(orch, db, job, run_id) {
                    sweep.resumed.push(job.job_id.clone());
                }
            }
            Decision::Report(cause) => {
                if let Some(node_uri) = job.child_node_uri.as_deref() {
                    reported_keys.insert(unowned_push_key(node_uri));
                }
                let told = run_db_blocking({
                    let db = db.clone();
                    let job = job.clone();
                    let cause = cause.clone();
                    move || async move { tell(&db, &job, &cause).await }
                });
                match told {
                    Ok(recipients) if recipients.is_empty() => {}
                    Ok(recipients) => {
                        log::warn!(
                            "unowned-running: job {} is running with nothing moving it ({}), \
                             telling {} watcher(s)",
                            &job.job_id[..job.job_id.len().min(8)],
                            cause.tag(),
                            recipients.len()
                        );
                        wake_recipients(orch, &recipients, "unowned-running");
                        sweep.reported.push((job.job_id.clone(), recipients));
                    }
                    Err(error) => log::warn!(
                        "unowned-running notice for job {} failed: {error}",
                        &job.job_id[..job.job_id.len().min(8)]
                    ),
                }
            }
        }
    }
    sweep.withdrawn = run_db_blocking({
        let db = db.clone();
        move || async move { withdraw_settled(&db, &reported_keys).await }
    })?;
    if sweep.withdrawn > 0 {
        orch.notifier.emit_change("attention_pushes");
    }
    Ok(sweep)
}

/// The periodic pass, over this host's private database.
pub(crate) fn sweep_unowned_running(orch: &Orchestrator) -> UnownedSweep {
    let now = chrono::Utc::now().timestamp();
    match sweep_unowned_running_in(orch, &orch.db.local, now) {
        Ok(sweep) => sweep,
        Err(error) => {
            log::error!("unowned-running sweep failed: {error}");
            UnownedSweep::default()
        }
    }
}

/// The body a `child-unowned:` push renders at delivery, re-derived from the
/// live rows, so a child that has moved on since says so.
pub(crate) async fn render_unowned_push_body(
    orch: &Orchestrator,
    recipient_job_id: &str,
    node_uri: &str,
) -> Option<String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, recipient_job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let now = chrono::Utc::now().timestamp();
    let standing = unowned_jobs(
        &orch.process_state,
        &db,
        &orch.claimed_launch_job_ids(),
        now,
    )
    .await
    .ok()?;
    let node_uri = cairn_uri::canonicalize_uri_identity(node_uri);
    let found = standing.into_iter().find_map(|job| {
        let Decision::Report(cause) = &job.decision else {
            return None;
        };
        let uri = job.child_node_uri.as_deref()?;
        (cairn_uri::canonicalize_uri_identity(uri) == node_uri).then(|| job.render(uri, cause, now))
    });
    Some(found.unwrap_or_else(|| {
        format!(
            "Child `{node_uri}` was reported as running with nothing moving it. It is no longer \
             in that state: it has started a turn, something is about to continue it, or its \
             issue has resolved. Nothing is owed here unless you want to check what it did."
        )
    }))
}
