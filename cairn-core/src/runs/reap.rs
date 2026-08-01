//! Reaping runs that no process stands behind, from inside a live session.
//!
//! A `runs` row is the durable shadow of a process. When the process is gone but
//! the row still reads `starting`/`live`, every downstream reader answers from a
//! lie: [`crate::messages::delivery::latest_run_for_job`] hands that row out as
//! the job's current run, `AgentProcessState::is_active` says false because there
//! is no handle behind it, and so the delivery ladder's "is this recipient busy?"
//! question answers *idle* for as long as the host stays up. The row also never
//! reaches a terminal status, so the node reads as perpetually starting.
//!
//! [`crate::runs::queries::reconcile_stale_runs`] answers this at startup only,
//! and answers it by boot time: every non-terminal run predating this host's boot
//! belonged to a predecessor. A machine that stays up for days never asks again,
//! which is how the incident row sat in `starting` for two hours with no process
//! anywhere (CAIRN-3291). This module asks the same question mid-life, where boot
//! time is no longer the discriminator and process presence is.
//!
//! ## What makes a run stale
//!
//! Three pieces of evidence that must all agree, because the cost of being wrong
//! is crashing a run that is legitimately mid-spawn:
//!
//! 1. **No process handle.** The registry is in-memory and every backend
//!    registers its process before the run can produce anything, so handle
//!    presence ([`crate::agent_process::process::AgentProcessState::has_process`])
//!    is the authoritative "is something behind this row?". Presence, not
//!    occupancy — a warm idle process is very much alive. And *presence*, not
//!    the absence of an answer: a registry that cannot be read at all (a
//!    poisoned lock) proves nothing about any run, so it settles none of them
//!    rather than all of them.
//! 2. **No launch claim.** A cold start holds a
//!    [`crate::orchestrator::JobLaunchClaim`] from `prepare_job` until its process
//!    registers, which is precisely the interval where a healthy launch has a run
//!    row and no handle yet.
//! 3. **Older than [`STALE_RUN_GRACE_SECS`].** The claim is the real cover for
//!    the spawn window; the grace is what keeps a gap in that cover from being
//!    expressed as a crashed run, and makes a just-inserted row un-reapable by
//!    construction rather than by argument.
//!
//! ## Where it fires
//!
//! Two triggers, one implementation.
//!
//! At **continue time** ([`reap_stale_runs_for_job`]), mirroring the turn-level
//! `reconcile_stale_active_turn_for_continue`: the job's launch lock is already
//! held, the resume is about to mint a successor run, and leaving the predecessor
//! row non-terminal is exactly what makes the job misreport itself afterwards.
//! This one runs against the job's *owning* database, because a resume is this
//! host asserting authority over that job either way.
//!
//! On the **periodic process sweep** ([`reap_stale_runs`], every ~30s from
//! `Orchestrator::spawn_process_sweep`), because a job nobody continues again —
//! the common case for a leaked row — would otherwise keep its lie forever. The
//! sweep holds no authority over any particular job, so it is deliberately
//! narrower: it reads only this host's own local database, never a synced team
//! replica whose rows may belong to a teammate's host, and it takes each job's
//! launch lock (non-blocking) before touching that job's rows. A job whose launch
//! is in flight is skipped this tick rather than raced — the resume path holds
//! that lock from run insert through process registration, and a slow one (a
//! digest reseed, a cold spawn) can outlast the grace.

use std::sync::Arc;

use cairn_db::turso::params;

use crate::orchestrator::Orchestrator;
use crate::runs::ownership::run_began_at;
use crate::storage::{run_db_blocking, LocalDb, RowExt};

/// Exit reason for a run this host settled because nothing was behind it.
///
/// Distinct from the startup sweep's `crash` and from the turn reconciler's
/// `stale_continue_recovery` so a row says which recovery claimed it.
pub const STALE_RUN_EXIT_REASON: &str = "stale_run_recovery";

/// How long a non-terminal run with no process and no launch claim behind it is
/// left alone before it is settled.
///
/// Sized for the gap it actually guards. Admission across the spawn window is
/// carried by the launch claim and the launch lock, not by this number, so the
/// grace only has to be long enough that a hole in either of those shows up as a
/// run left alone for another tick rather than as a crashed run. Overshooting
/// costs a stale row one more sweep interval; undershooting invents a crash.
pub const STALE_RUN_GRACE_SECS: i64 = 60;

/// A non-terminal run row and the moment it began, the two facts staleness is
/// decided from.
struct RunCandidate {
    run_id: String,
    began_at: i64,
}

/// Settle every stale run of one job. Returns how many rows were settled.
///
/// **The caller must hold this job's launch lock.** What the lock excludes is a
/// concurrent resume of the same job sitting between its run insert and its
/// process registration — a window with no handle and no claim, which this would
/// otherwise read as stale.
pub fn reap_stale_runs_for_job(orch: &Orchestrator, db: &Arc<LocalDb>, job_id: &str) -> usize {
    let now = chrono::Utc::now().timestamp();
    let candidates = match run_db_blocking({
        let db = db.clone();
        let job_id = job_id.to_string();
        move || async move { load_non_terminal_runs(&db, &job_id).await }
    }) {
        Ok(candidates) => candidates,
        Err(error) => {
            log::warn!("stale-run reaper: could not read runs for job {job_id}: {error}");
            return 0;
        }
    };
    if candidates.is_empty() {
        return 0;
    }

    let claimed_launch = orch.claimed_launch_run(job_id);
    let stale: Vec<String> = candidates
        .into_iter()
        .filter(|candidate| is_stale(orch, claimed_launch.as_deref(), candidate, now))
        .map(|candidate| candidate.run_id)
        .collect();
    if stale.is_empty() {
        return 0;
    }

    let settled = match run_db_blocking({
        let db = db.clone();
        let stale = stale.clone();
        move || async move {
            db.write(move |conn| {
                let stale = stale.clone();
                Box::pin(async move {
                    let mut settled = Vec::new();
                    for run_id in &stale {
                        if let Some(turn_ids) = crate::runs::queries::settle_stale_run(
                            conn,
                            run_id,
                            STALE_RUN_EXIT_REASON,
                            now,
                        )
                        .await?
                        {
                            settled.push((run_id.clone(), turn_ids));
                        }
                    }
                    Ok(settled)
                })
            })
            .await
            .map_err(|error| error.to_string())
        }
    }) {
        Ok(settled) => settled,
        Err(error) => {
            log::warn!("stale-run reaper: could not settle runs for job {job_id}: {error}");
            return 0;
        }
    };

    for (run_id, turn_ids) in &settled {
        log::warn!(
            "Settled stale run {} for job {}: no process behind it and no launch claimed it ({} turn(s) stranded)",
            &run_id[..run_id.len().min(8)],
            &job_id[..job_id.len().min(8)],
            turn_ids.len()
        );
    }
    emit_settled(orch, db, &settled);
    settled.len()
}

/// Settle every stale run this host's own local database knows about.
///
/// The unattended trigger, with no authority over any single job: see the module
/// docs for why it stays on the local database and takes each job's launch lock
/// before touching it.
pub fn reap_stale_runs(orch: &Orchestrator) -> usize {
    let db = orch.db.local.clone();
    let job_ids = match run_db_blocking({
        let db = db.clone();
        move || async move { load_jobs_with_non_terminal_runs(&db).await }
    }) {
        Ok(job_ids) => job_ids,
        Err(error) => {
            log::warn!("stale-run reaper: could not list jobs with non-terminal runs: {error}");
            return 0;
        }
    };

    let mut settled = 0;
    for job_id in job_ids {
        let launch_lock = orch.job_launch_lock(&job_id);
        // A poisoned lock means an earlier launch panicked; what it guards is
        // durable rows this reads from scratch, so recover it rather than skip
        // the job for the rest of the host's life.
        let _launch_guard = match launch_lock.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => continue,
        };
        settled += reap_stale_runs_for_job(orch, &db, &job_id);
    }
    settled
}

/// Whether this host has any evidence that `candidate` is still alive.
///
/// Every uncertain answer resolves to "not stale". Declining to settle costs a
/// leaked row one more sweep interval; settling on a bad answer is permanent.
fn is_stale(
    orch: &Orchestrator,
    claimed_launch: Option<&str>,
    candidate: &RunCandidate,
    now: i64,
) -> bool {
    match orch.process_state.has_process(&candidate.run_id) {
        // A handle is registered — warm or serving, this run is alive.
        Some(true) => return false,
        Some(false) => {}
        // The registry could not be inspected, so nothing here is evidence of
        // anything. See `AgentProcessState::has_process`.
        None => {
            log::warn!(
                "stale-run reaper: process registry unavailable; leaving run {} alone",
                &candidate.run_id[..candidate.run_id.len().min(8)]
            );
            return false;
        }
    }
    if claimed_launch == Some(candidate.run_id.as_str()) {
        return false;
    }
    now.saturating_sub(candidate.began_at) >= STALE_RUN_GRACE_SECS
}

async fn load_non_terminal_runs(db: &LocalDb, job_id: &str) -> Result<Vec<RunCandidate>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, started_at, created_at
                     FROM runs
                     WHERE job_id = ?1
                       AND status IN ('starting', 'live')",
                    params![job_id.as_str()],
                )
                .await?;
            let mut candidates = Vec::new();
            while let Some(row) = rows.next().await? {
                candidates.push(RunCandidate {
                    run_id: row.text(0)?,
                    began_at: run_began_at(row.opt_i64(1)?, row.i64(2)?),
                });
            }
            Ok(candidates)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn load_jobs_with_non_terminal_runs(db: &LocalDb) -> Result<Vec<String>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT DISTINCT job_id
                     FROM runs
                     WHERE status IN ('starting', 'live')
                       AND job_id IS NOT NULL",
                    (),
                )
                .await?;
            let mut job_ids = Vec::new();
            while let Some(row) = rows.next().await? {
                job_ids.push(row.text(0)?);
            }
            Ok(job_ids)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Invalidate the settled rows for every reader. Scoped payloads rather than a
/// bare table invalidation: a reap touches a handful of rows belonging to one
/// job, and the frontend's run and turn views key off those ids.
fn emit_settled(orch: &Orchestrator, db: &Arc<LocalDb>, settled: &[(String, Vec<String>)]) {
    if settled.is_empty() {
        return;
    }
    let payloads = run_db_blocking({
        let db = db.clone();
        let settled = settled.to_vec();
        move || async move {
            let mut payloads = Vec::new();
            for (run_id, turn_ids) in &settled {
                payloads.push(crate::notify::run_db_change_for_id(&db, run_id, "update").await);
                for turn_id in turn_ids {
                    payloads
                        .push(crate::notify::turn_db_change_for_id(&db, turn_id, "update").await);
                }
            }
            Ok::<_, String>(payloads)
        }
    })
    .unwrap_or_default();
    for payload in payloads {
        let _ = orch.services.emitter.emit("db-change", payload);
    }
}
