//! What this host can observe of a job's work, as opposed to what its rows say.
//!
//! A job's status and its turn's state are labels written at a transition
//! (tenet 785): they record what was last decided, not what is running now. A
//! job left `running` behind a turn whose process died reads exactly like one
//! mid-turn. Everything that has to know whether work is actually under way
//! (the fleet and focus caps, the census, the node summary, the unowned-running
//! sweep) reads it here, from evidence:
//!
//! - **A process.** This host's registry holds an agent handle for the job's
//!   session, parked warm or serving, or a bound workflow runtime for its run.
//! - **A launch in flight.** A launch owns a job from admission until its
//!   process registers: first through the in-memory launch claim, then, once
//!   the launch is durably enqueued, through its admission row (the reaper's
//!   reading, `crate::runs::reap`).
//! - **An advance in flight.** Before any launch, a job is owned on its way by
//!   this host: an execution started or released this boot whose advance this
//!   process carries, a durable `advance_dag` outbox entry a restart replays,
//!   or a `running` claim advancement made this boot
//!   ([`crate::jobs::queries::claim_predates_boot`]) that prepares it.
//! - **A run row that still reads live.** On its own this is not liveness: with
//!   no process behind it, liveness is unknown since that row's last update.
//!
//! [`Liveness`] is the reading of the first and third; the policy of what to do
//! about it (continue, report, count, render) belongs to each caller.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};

use crate::agent_process::process::AgentProcessState as ProcessState;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

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

/// What this host's process registry holds for a job.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum ProcessReading {
    #[default]
    Absent,
    /// An agent handle for the job's session, parked warm.
    Warm { run_id: String },
    /// An agent handle serving a turn or awaiting the host, or a bound
    /// workflow runtime.
    Serving,
    /// The registry could not be inspected. Not evidence that nothing runs.
    Unreadable,
}

impl ProcessReading {
    pub(crate) fn is_present(&self) -> bool {
        !matches!(self, ProcessReading::Absent)
    }
}

/// The one reading of liveness: a process first, then a run row that still
/// reads live, whose last update is when liveness became unknown.
pub(crate) fn liveness(process: &ProcessReading, live_run_since: Option<i64>) -> Liveness {
    match process {
        ProcessReading::Warm { run_id } => Liveness::Alive {
            run_id: run_id.clone(),
        },
        ProcessReading::Serving | ProcessReading::Unreadable => Liveness::Busy,
        ProcessReading::Absent => match live_run_since {
            Some(since) => Liveness::Unknown { since },
            None => Liveness::Dead,
        },
    }
}

/// The agent handle this host holds for `session_id`.
pub(crate) fn session_process(processes: &ProcessState, session_id: &str) -> ProcessReading {
    match processes.find_process_by_session(session_id) {
        Some(run_id) if processes.is_warm(&run_id) => ProcessReading::Warm { run_id },
        Some(_) => ProcessReading::Serving,
        None => ProcessReading::Absent,
    }
}

/// The last update of job `j`'s newest run that still reads live, or NULL.
/// Expects a `jobs` row aliased `j`; seeks `idx_runs_job_id`. Written as
/// `ORDER BY .. LIMIT 1`, not `MAX`, because the planner flattens an aggregate
/// subquery inside a join into a walk of every run, where this form stays a
/// per-job seek.
macro_rules! live_run_since_sql {
    () => {
        "(SELECT live_run.updated_at FROM runs live_run
           WHERE live_run.job_id = j.id AND live_run.status IN ('starting', 'live')
           ORDER BY live_run.updated_at DESC LIMIT 1)"
    };
}
pub(crate) use live_run_since_sql;

/// The id of job `j`'s most recently updated run that still reads live, or
/// NULL: the run a launch in admission would be launching. Expects a `jobs` row
/// aliased `j`; seeks `idx_runs_job_id`.
macro_rules! live_run_id_sql {
    () => {
        "(SELECT live_run.id FROM runs live_run
           WHERE live_run.job_id = j.id AND live_run.status IN ('starting', 'live')
           ORDER BY live_run.updated_at DESC, live_run.id DESC LIMIT 1)"
    };
}
pub(crate) use live_run_id_sql;

const LIVE_RUN_SINCE_SQL: &str = concat!(
    "SELECT ",
    live_run_since_sql!(),
    " FROM jobs j WHERE j.id = ?1"
);

/// The liveness of `job_id`'s provider on `session_id`, read now.
pub(crate) async fn read_liveness(
    processes: &ProcessState,
    db: &LocalDb,
    job_id: &str,
    session_id: &str,
) -> Result<Liveness, String> {
    let process = session_process(processes, session_id);
    if process.is_present() {
        return Ok(liveness(&process, None));
    }
    let since = db
        .query_opt(LIVE_RUN_SINCE_SQL, (job_id.to_string(),), |row| {
            row.opt_i64(0)
        })
        .await
        .map_err(|error| format!("failed to read the runs of job {job_id}: {error}"))?
        .flatten();
    Ok(liveness(&process, since))
}

/// Executions whose advance is durably owed: an `advance_dag` outbox entry not
/// yet done. Seeks `idx_effect_outbox_kind_dedupe`.
pub(crate) const ADVANCING_EXECUTIONS_SQL: &str = "SELECT DISTINCT dedupe_key FROM effect_outbox
      WHERE kind = 'advance_dag' AND state IN ('pending', 'running')";

/// One observation of the work this host can see, taken once per decision or
/// pass and carried across every job it classifies.
///
/// The launch evidence is captured when the observation is read, the claims
/// before the admission rows; each process is looked up later, as each job is
/// classified. A launch takes each stage before letting go of the one before
/// (its claim until its admission row commits, its row until its process
/// registers), so one read in that order always catches it at some stage and
/// it never reads as free capacity. The owed advances are read before any job
/// row too: an advance claims its jobs before its entry is done, so a row read
/// later shows either the entry's job still pending or the claim it made.
///
/// A job's own launch lock is deliberately not evidence: admission decides a
/// job's start while holding that lock, and would read the job as its own
/// launch.
#[derive(Clone)]
pub(crate) struct WorkObserver {
    processes: Option<Arc<ProcessState>>,
    claimed_jobs: Arc<HashSet<String>>,
    admitted_runs: Arc<HashSet<String>>,
    advancing_executions: Arc<HashSet<String>>,
    boot_at: i64,
}

/// A live, non-owning source for projections whose rebuild outlives a request.
/// It observes afresh on every read without keeping the runner or its caches alive.
pub(crate) struct WorkObserverSource {
    db: Weak<crate::db::DbState>,
    processes: Weak<ProcessState>,
    claims: Weak<Mutex<HashMap<String, String>>>,
    boot_at: i64,
}

impl WorkObserverSource {
    pub(crate) fn new(
        db: Weak<crate::db::DbState>,
        processes: Weak<ProcessState>,
        claims: Weak<Mutex<HashMap<String, String>>>,
        boot_at: i64,
    ) -> Self {
        Self {
            db,
            processes,
            claims,
            boot_at,
        }
    }

    pub(crate) async fn read(&self) -> Result<WorkObserver, String> {
        let db = self.db.upgrade().ok_or("work observer database is gone")?;
        let processes = self
            .processes
            .upgrade()
            .ok_or("work observer processes are gone")?;
        let claims = self
            .claims
            .upgrade()
            .ok_or("work observer launch claims are gone")?;
        let claimed_jobs = claims
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .keys()
            .cloned()
            .collect();
        // One connection for both durable facts: this read runs under the
        // admission lock, where each extra checkout is a wait every decision
        // queued behind it shares.
        let (admitted_runs, advancing_executions) = db
            .local
            .read(|conn| {
                Box::pin(async move {
                    let admitted = crate::storage::pending_launch_run_ids_conn(conn).await?;
                    let mut rows = conn.query(ADVANCING_EXECUTIONS_SQL, ()).await?;
                    let mut advancing = HashSet::new();
                    while let Some(row) = rows.next().await? {
                        advancing.insert(row.text(0)?);
                    }
                    Ok((admitted, advancing))
                })
            })
            .await
            .map_err(|error| format!("failed to read the launches and advances owed: {error}"))?;
        Ok(WorkObserver {
            processes: Some(processes),
            claimed_jobs: Arc::new(claimed_jobs),
            admitted_runs: Arc::new(admitted_runs),
            advancing_executions: Arc::new(advancing_executions),
            boot_at: self.boot_at,
        })
    }
}

impl WorkObserver {
    /// Read this host's launch evidence in acquisition order, then its owed
    /// advances. Admission rows and the outbox are runner-local, so they come
    /// from this host's own database whichever database the jobs live in.
    pub(crate) async fn read(orch: &Orchestrator) -> Result<Self, String> {
        orch.work_observer_source().read().await
    }

    /// An observation that sees no process, no launch and no owed advance.
    #[cfg(test)]
    pub(crate) fn nothing() -> Self {
        Self {
            processes: None,
            claimed_jobs: Arc::default(),
            admitted_runs: Arc::default(),
            advancing_executions: Arc::default(),
            boot_at: 0,
        }
    }

    /// Whether a job with no turn is still owned on its way to a launch:
    ///
    /// - `pending`: its execution's advance is owed in the outbox, or the job
    ///   was minted this boot (`began_at`, when a start or release created it)
    ///   in an execution that has run no turn yet, so this process carries its
    ///   first advance;
    /// - `running`: a claim advancement recorded this boot (`claimed_at` at or
    ///   after the boot) whose launch has not ended without a turn. A claim
    ///   with no recorded time is no evidence of this boot's launch.
    ///   ([`crate::jobs::queries::claim_predates_boot`] reads the other side of
    ///   the same line for display, where a missing time reads as preparing.)
    ///
    /// A predecessor host's claim, a failed launch, a pending node behind
    /// predecessors that ran, and an execution from an earlier boot nothing
    /// will advance are owned by nothing, whatever their label says.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn placed_for_launch(
        &self,
        status: &str,
        claimed_at: Option<i64>,
        launch_ended: bool,
        execution_id: &str,
        execution_unstarted: bool,
        began_at: Option<i64>,
    ) -> bool {
        match status {
            "pending" => {
                self.advancing_executions.contains(execution_id)
                    || (execution_unstarted && began_at.is_some_and(|at| at >= self.boot_at))
            }
            "running" => claimed_at.is_some_and(|at| at >= self.boot_at) && !launch_ended,
            _ => false,
        }
    }

    /// An observation that sees a launch claimed for each of `jobs`.
    #[cfg(test)]
    pub(crate) fn launching<'a>(jobs: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            claimed_jobs: Arc::new(jobs.into_iter().map(str::to_string).collect()),
            ..Self::nothing()
        }
    }

    /// Whether a launch for `job_id` is between admission and its process:
    /// claimed in memory, or durably admitted on one of `runs`.
    pub(crate) fn launch_in_flight(&self, job_id: &str, runs: &[Option<&str>]) -> bool {
        self.claimed_jobs.contains(job_id)
            || runs
                .iter()
                .flatten()
                .any(|run| self.admitted_runs.contains(*run))
    }

    /// The process this host holds for a job: an agent handle for its
    /// session, else any process (agent or workflow runtime) for one of `runs`.
    pub(crate) fn process(
        &self,
        session_id: Option<&str>,
        runs: &[Option<&str>],
    ) -> ProcessReading {
        let Some(processes) = self.processes.as_deref() else {
            return ProcessReading::Absent;
        };
        if let Some(session_id) = session_id {
            let reading = session_process(processes, session_id);
            if reading.is_present() {
                return reading;
            }
        }
        for run in runs.iter().flatten() {
            match processes.has_process(run) {
                Some(true) => return ProcessReading::Serving,
                Some(false) => {}
                None => return ProcessReading::Unreadable,
            }
        }
        ProcessReading::Absent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_process_outranks_a_run_row_and_a_live_row_alone_is_unknown() {
        let warm = ProcessReading::Warm { run_id: "r".into() };
        assert_eq!(
            liveness(&warm, Some(5)),
            Liveness::Alive { run_id: "r".into() }
        );
        assert_eq!(liveness(&ProcessReading::Serving, None), Liveness::Busy);
        assert_eq!(
            liveness(&ProcessReading::Unreadable, None),
            Liveness::Busy,
            "an unreadable registry is not evidence of death"
        );
        assert_eq!(
            liveness(&ProcessReading::Absent, Some(5)),
            Liveness::Unknown { since: 5 }
        );
        assert_eq!(liveness(&ProcessReading::Absent, None), Liveness::Dead);
    }

    #[test]
    fn a_launch_is_in_flight_while_claimed_or_admitted() {
        let observer = WorkObserver {
            admitted_runs: Arc::new(["queued-run".to_string()].into()),
            ..WorkObserver::launching(["claimed"])
        };
        assert!(observer.launch_in_flight("claimed", &[]));
        assert!(observer.launch_in_flight("other", &[None, Some("queued-run")]));
        assert!(!observer.launch_in_flight("other", &[Some("old-run")]));
        assert_eq!(
            observer.process(Some("s"), &[Some("queued-run")]),
            ProcessReading::Absent,
            "an observation without a registry sees no process"
        );
    }

    #[test]
    fn a_turnless_job_is_placed_only_while_something_owns_its_way_to_a_launch() {
        let observer = WorkObserver {
            advancing_executions: Arc::new(["advancing".to_string()].into()),
            boot_at: 100,
            ..WorkObserver::nothing()
        };
        let placed = |status, claimed_at, launch_ended, execution, unstarted, began_at| {
            observer.placed_for_launch(
                status,
                claimed_at,
                launch_ended,
                execution,
                unstarted,
                began_at,
            )
        };
        assert!(placed("pending", None, false, "advancing", false, Some(1)));
        assert!(
            placed("pending", None, false, "idle", true, Some(150)),
            "started this boot, its first advance carried by this process"
        );
        assert!(
            !placed("pending", None, false, "idle", true, Some(50)),
            "started before this boot and never advanced"
        );
        assert!(
            !placed("pending", None, false, "idle", false, Some(150)),
            "a pending node behind predecessors that ran"
        );
        assert!(placed("running", Some(150), false, "idle", false, None));
        assert!(
            !placed("running", None, false, "advancing", true, Some(150)),
            "a claim with no recorded time is no evidence of a launch"
        );
        assert!(
            !placed("running", Some(50), false, "advancing", true, Some(150)),
            "a claim a predecessor host made never launches"
        );
        assert!(
            !placed("running", Some(150), true, "idle", true, Some(150)),
            "a launch that ended without a turn"
        );
        assert!(!placed(
            "blocked",
            None,
            false,
            "advancing",
            true,
            Some(150)
        ));
    }
}
