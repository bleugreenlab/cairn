//! Provider-process lifetime enforcement, independent of memory-pressure trimming.

use super::stop::{teardown_process, TerminalPolicy};
use super::{finalize_run, set_exit_reason};
use crate::agent_process::process::WarmProcess;
use crate::db::DbState;
use crate::execution::routing::routing_db_for_id;
use crate::models::RunStatus;
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, LocalDb, RowExt};
use cairn_models::continuation::{
    prefix_cache_state, prompt_cache_horizon_secs, PrefixCacheState, ResumeBackend,
};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
struct LifetimeFacts {
    session_id: String,
    session_status: String,
    backend: String,
    job_id: String,
    job_status: String,
    /// Work is in this process or about to enter it: a pending turn (a
    /// delivery stamped into it awaits launch), a running one, or a `run` call
    /// held open on its socket (`agent_waits.hold = 'inline'`), which a Codex
    /// exec cell can keep past the provider turn that opened it. Stopping the
    /// process would cut that work, so nothing stops it.
    ///
    /// Nothing else owns the process. A `yielded` turn is terminal and stays
    /// so beside the successor that continued it, so its state is no evidence
    /// (tenet 1421). A parked wait, pending permission, unanswered question,
    /// live delegated child or detached batch continues the job by minting a
    /// successor turn or queueing a notice, never through this process: all it
    /// gains from the process is a warm prefix for native resume, which an open
    /// session inside its horizon keeps anyway and a closed session or
    /// cancelled job cannot use (tenets 1252, 920).
    work_in_process: bool,
    last_provider_request_at: Option<i64>,
}

/// Why an idle provider process was retired, recorded as its run's exit
/// reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetireCause {
    SessionClosed,
    JobCancelled,
    IdlePastHorizon,
    /// The managed CLI the process runs was replaced.
    CliReplaced,
    /// The host's fresh pipes read below the floor (cairn/6440).
    PipePressure,
}

impl RetireCause {
    fn exit_reason(self) -> &'static str {
        match self {
            Self::SessionClosed => "session_closed",
            Self::JobCancelled => "job_cancelled",
            Self::IdlePastHorizon => "cache_horizon_idle",
            Self::CliReplaced => crate::orchestrator::models::MANAGED_CLI_SWAP_EXIT_REASON,
            Self::PipePressure => "pipe_pressure_release",
        }
    }
}

/// The grounds a caller retires an idle provider on. Every one requires the
/// same thing first: the run is warm under the job's launch lock, and facts
/// read under that lock show no work in its process. The grounds then add
/// their own condition and name the cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetireGrounds {
    /// The lifetime sweep: the session closed, the job was cancelled, or the
    /// provider's prefix cache went cold, judged at the sweep's own `now`.
    Lifetime { now: i64 },
    /// A managed-CLI swap: the process runs a CLI that has been replaced.
    CliReplaced,
    /// The fresh-pipe floor's release experiment. Also requires that the warm
    /// collector, reading its metadata now, does not protect the run (live
    /// delegated children, a pending memory review, an unresolved database).
    PipePressure,
}

/// Why a retire did not happen. None of these is an error: each is a fence
/// doing its job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum NotRetired {
    /// The run has no job, or its job could not be read.
    NoJob,
    /// A launch or resume holds the job's launch lock.
    LaunchHeld,
    /// The process is no longer registered idle.
    NotIdle,
    /// Its facts could not be read under the lock: unknown state never
    /// authorizes a stop.
    FactsUnknown,
    /// A turn is pending or running, or a `run` call is held open inline.
    WorkInProcess,
    /// The warm collector protects it.
    Protected,
    /// The lifetime grounds found no cause once re-read under the lock.
    NoCause,
    TeardownFailed(String),
}

impl NotRetired {
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::NoJob => "its run has no readable job".into(),
            Self::LaunchHeld => "a launch or resume holds its job".into(),
            Self::NotIdle => "it is no longer idle".into(),
            Self::FactsUnknown => "its facts could not be read".into(),
            Self::WorkInProcess => "work is in its process".into(),
            Self::Protected => "the warm collector protects it".into(),
            Self::NoCause => "nothing retires it any more".into(),
            Self::TeardownFailed(error) => format!("its teardown failed: {error}"),
        }
    }
}

/// A retired run and what it was.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Retired {
    pub cause: RetireCause,
    pub session_id: String,
    pub job_id: String,
    pub backend: String,
}

fn lifetime_verdict(
    facts: &LifetimeFacts,
    process_idle_secs: u64,
    now: i64,
) -> Option<RetireCause> {
    if facts.work_in_process {
        return None;
    }
    if facts.session_status != "open" {
        return Some(RetireCause::SessionClosed);
    }
    if facts.job_status == "cancelled" {
        return Some(RetireCause::JobCancelled);
    }
    let backend = ResumeBackend::from_key(&facts.backend);
    match prefix_cache_state(backend, facts.last_provider_request_at, now) {
        PrefixCacheState::Cold => Some(RetireCause::IdlePastHorizon),
        PrefixCacheState::Unknown
            if prompt_cache_horizon_secs(backend)
                .is_some_and(|horizon| process_idle_secs >= horizon as u64) =>
        {
            Some(RetireCause::IdlePastHorizon)
        }
        _ => None,
    }
}

/// Read each owning database once. An unresolved route, absent row, or failed
/// lookup supplies no verdict: unknown state never authorizes a stop.
async fn load_lifetime_facts(dbs: &DbState, run_ids: &[String]) -> HashMap<String, LifetimeFacts> {
    let mut groups: Vec<(Arc<LocalDb>, Vec<String>)> = Vec::new();
    for run_id in run_ids {
        let db = match routing_db_for_id(dbs, run_id).await {
            Ok(db) => db,
            Err(error) => {
                log::warn!("Process lifetime sweep: cannot route run {run_id}: {error}");
                continue;
            }
        };
        if let Some((_, runs)) = groups.iter_mut().find(|(owner, _)| Arc::ptr_eq(owner, &db)) {
            runs.push(run_id.clone());
        } else {
            groups.push((db, vec![run_id.clone()]));
        }
    }

    let mut facts = HashMap::new();
    for (db, runs) in groups {
        let ids = serde_json::to_string(&runs).expect("run ids serialize");
        let result = db
            .query_all(
                "WITH candidates AS (
                SELECT r.id, r.session_id, r.job_id FROM runs r
                WHERE r.id IN (SELECT value FROM json_each(?1))
             ), last_requests AS (
                SELECT e.session_id, MAX(e.created_at) AS requested_at FROM events e
                WHERE e.session_id IN (SELECT session_id FROM candidates)
                  AND e.parent_tool_use_id IS NULL
                  AND (e.input_tokens IS NOT NULL OR e.output_tokens IS NOT NULL
                    OR e.cache_read_tokens IS NOT NULL OR e.cache_create_tokens IS NOT NULL)
                GROUP BY e.session_id
             )
             SELECT r.id, s.id, s.status, s.backend, j.id, j.status,
                    EXISTS(SELECT 1 FROM turns t WHERE t.job_id = j.id
                           AND t.state IN ('pending', 'running'))
                    OR EXISTS(SELECT 1 FROM agent_waits w WHERE w.job_id = j.id
                              AND w.state IN ('pending', 'resolving')
                              AND w.hold = 'inline'),
                    p.requested_at
             FROM candidates r
             JOIN sessions s ON s.id = r.session_id
             JOIN jobs j ON j.id = r.job_id
             LEFT JOIN last_requests p ON p.session_id = s.id",
                (ids,),
                |row| {
                    Ok((
                        row.text(0)?,
                        LifetimeFacts {
                            session_id: row.text(1)?,
                            session_status: row.text(2)?,
                            backend: row.text(3)?,
                            job_id: row.text(4)?,
                            job_status: row.text(5)?,
                            work_in_process: row.i64(6)? != 0,
                            last_provider_request_at: row.opt_i64(7)?,
                        },
                    ))
                },
            )
            .await;
        match result {
            Ok(rows) => facts.extend(rows),
            Err(error) => log::warn!(
                "Process lifetime sweep: skipping {} runs after facts lookup failed: {error}",
                runs.len()
            ),
        }
    }
    facts
}

/// Enforce session lifetime and provider cache horizons for registered idle
/// processes only. Background terminals belong to the job, not its provider.
pub(crate) fn sweep_process_lifetimes(orch: &Orchestrator) {
    let warm = orch.process_state.warm_processes();
    let inspected = warm.len();
    let dbs = orch.db.clone();
    let candidates: Vec<String> = warm.iter().map(|process| process.run_id.clone()).collect();
    let facts = run_db_blocking(move || async move {
        Ok::<_, String>(load_lifetime_facts(&dbs, &candidates).await)
    })
    .unwrap_or_else(|error| {
        log::warn!("Process lifetime sweep: failed to load facts: {error}");
        HashMap::new()
    });
    let now = orch.services.clock.now();
    let mut session_closed = 0;
    let mut job_cancelled = 0;
    let mut cache_horizon_idle = 0;
    for process in warm {
        let Some(facts) = facts.get(&process.run_id) else {
            continue;
        };
        if lifetime_verdict(facts, process.seconds_since_activity, now).is_none() {
            continue;
        }
        match retire_unowned_idle(orch, &process.run_id, RetireGrounds::Lifetime { now }) {
            Ok(retired) => match retired.cause {
                RetireCause::SessionClosed => session_closed += 1,
                RetireCause::JobCancelled => job_cancelled += 1,
                RetireCause::IdlePastHorizon => cache_horizon_idle += 1,
                RetireCause::CliReplaced | RetireCause::PipePressure => {}
            },
            Err(NotRetired::TeardownFailed(error)) => log::warn!(
                "Process lifetime sweep: could not stop run {} on session {}: {error}",
                process.run_id,
                facts.session_id
            ),
            Err(_) => {}
        }
    }
    // An all-zero tick is the positive control that the sweep is alive.
    log::info!(
        "Process lifetime sweep: inspected={inspected}, session_closed={session_closed}, job_cancelled={job_cancelled}, cache_horizon_idle={cache_horizon_idle}"
    );
}

/// Retire `run_id`'s idle provider process on `grounds`: the one fenced
/// teardown every caller that stops an idle provider while its job continues
/// shares, so all of them agree on what "retirable" means.
///
/// The job's launch lock is taken without waiting (a job whose launch or
/// resume holds it is left alone, so a process a message is resuming is never
/// torn down mid-turn). Under it the run must still be warm, and its facts are
/// re-read: a turn that started, or a `run` held open inline, since any
/// earlier snapshot keeps the process. Only the provider process is torn down
/// (`TerminalPolicy::Keep`: the job's terminals keep running, its session
/// stays open and its next delivery resumes it), and the run is finalized
/// outside the lock, because finalization can launch the job's next turn and
/// the lock is not reentrant.
pub(crate) fn retire_unowned_idle(
    orch: &Orchestrator,
    run_id: &str,
    grounds: RetireGrounds,
) -> Result<Retired, NotRetired> {
    let job_id = super::common::job_id_for_run(orch, run_id).ok_or(NotRetired::NoJob)?;
    let launch_lock = orch.job_launch_lock(&job_id);
    let Ok(guard) = launch_lock.try_lock() else {
        return Err(NotRetired::LaunchHeld);
    };
    if !orch.process_state.is_warm(run_id) {
        return Err(NotRetired::NotIdle);
    }
    let dbs = orch.db.clone();
    let ids = vec![run_id.to_string()];
    let refreshed =
        run_db_blocking(
            move || async move { Ok::<_, String>(load_lifetime_facts(&dbs, &ids).await) },
        );
    let facts = match refreshed.map(|mut facts| facts.remove(run_id)) {
        Ok(Some(facts)) => facts,
        Ok(None) => return Err(NotRetired::FactsUnknown),
        Err(error) => {
            log::warn!("Cannot revalidate idle run {run_id} before retiring it: {error}");
            return Err(NotRetired::FactsUnknown);
        }
    };
    if facts.work_in_process {
        return Err(NotRetired::WorkInProcess);
    }
    let cause = match grounds {
        RetireGrounds::Lifetime { now } => {
            let idle_secs = orch
                .process_state
                .processes
                .lock()
                .ok()
                .and_then(|processes| {
                    processes.get(run_id).and_then(|handle| {
                        handle
                            .is_warm()
                            .then(|| handle.last_activity.elapsed().as_secs())
                    })
                })
                .ok_or(NotRetired::NotIdle)?;
            lifetime_verdict(&facts, idle_secs, now).ok_or(NotRetired::NoCause)?
        }
        RetireGrounds::CliReplaced => RetireCause::CliReplaced,
        RetireGrounds::PipePressure => {
            if warm_collector_protects(orch, run_id, &job_id) {
                return Err(NotRetired::Protected);
            }
            RetireCause::PipePressure
        }
    };
    if let Err(error) = teardown_process(orch, run_id, TerminalPolicy::Keep) {
        return Err(NotRetired::TeardownFailed(error.to_string()));
    }
    drop(guard);
    if let Err(error) = set_exit_reason(orch, run_id, cause.exit_reason()) {
        log::error!("Failed to record exit for retired run {run_id}: {error}");
    }
    // The physical process is already gone. A failed metadata write must not
    // leave its run live for the reaper to misclassify as a crash; not every
    // provider finalizes an idle run from its EOF callback.
    finalize_run(orch, run_id, RunStatus::Exited);
    Ok(Retired {
        cause,
        session_id: facts.session_id,
        job_id: facts.job_id,
        backend: facts.backend,
    })
}

/// Whether the warm collector protects `run_id`, from metadata it reads now.
/// No collector, or a read that fails, protects: unknown state never
/// authorizes a stop.
fn warm_collector_protects(orch: &Orchestrator, run_id: &str, job_id: &str) -> bool {
    let Some(gc) = orch.warm_gc.clone() else {
        return true;
    };
    let dbs = orch.db.clone();
    let process = WarmProcess {
        run_id: run_id.to_string(),
        pid: None,
        seconds_since_activity: 0,
        job_id: Some(job_id.to_string()),
    };
    run_db_blocking(move || async move { Ok::<_, String>(gc.protects(&dbs, &process).await) })
        .unwrap_or(true)
}

/// Each run's session and backend, for the runs whose facts read. A run
/// whose facts did not read is absent.
pub(crate) fn run_sessions(
    orch: &Orchestrator,
    run_ids: Vec<String>,
) -> HashMap<String, (String, String)> {
    let dbs = orch.db.clone();
    run_db_blocking(
        move || async move { Ok::<_, String>(load_lifetime_facts(&dbs, &run_ids).await) },
    )
    .unwrap_or_default()
    .into_iter()
    .map(|(run_id, facts)| (run_id, (facts.session_id, facts.backend)))
    .collect()
}

/// Whether a launch still waiting on its CLI's install still wants it: its
/// run has not been stopped or settled meanwhile, and the admission claim it
/// launched under, if any, is still current. A run with no row reads as
/// wanted.
pub(crate) fn launch_still_wanted(
    orch: &Orchestrator,
    run_id: &str,
    claim: Option<&(String, i64)>,
) -> bool {
    if let Some((request_id, generation)) = claim {
        if !orch
            .process_state
            .admission_claim_is_current(run_id, request_id, *generation)
        {
            return false;
        }
    }
    !matches!(
        super::common::run_status(orch, run_id).as_deref(),
        Some(status) if status != "starting" && status != "live"
    )
}
