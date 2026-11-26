//! A dispatched launch that launch admission could not take.
//!
//! Advancement claims a ready job and hands it to the runner, whose first step
//! is [`admit_thread_launch`]: a supply hold or the run queue may take the
//! launch instead. Admission reads the job's database, and when that read
//! fails it refuses with nothing written. A refusal there used to be logged and dropped:
//! the job stayed claimed with no run until a later recompute demoted it to
//! `pending`, and nothing launched it until some unrelated advancement of its
//! execution happened along. A background task sat "preparing" for as long as
//! its parent spawned nothing else.
//!
//! [`admit_dispatched_launch`] is the runner's admission seam instead. A
//! refused launch is recorded here and re-dispatched after a backoff, and a
//! launch refused through every attempt is failed through
//! [`mark_job_start_failed`], which records the reason on a run and answers
//! whoever waits on the job. The record is what the job's node page, its task
//! page, a refused direct message, and a spawn's answer say it is waiting on.
//!
//! The record and timer are in memory. On restart, boot reconciliation recovers
//! the never-launched job (even if recompute already demoted it to pending),
//! reprojects its execution and enqueues a durable advance. The new host admits
//! that launch normally; it does not restore this host's refusal history.
//! See [`crate::runs::queries::reconcile_stale_runs`].
//!
//! [`mark_job_start_failed`]: crate::execution::jobs::mark_job_start_failed

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use crate::orchestrator::Orchestrator;

use super::admit_thread_launch;

/// The pause before each re-dispatch of a refused launch. The budget spans
/// several minutes before the job is failed.
const REDRIVE_BACKOFF: [Duration; 6] = [
    Duration::from_secs(5),
    Duration::from_secs(15),
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(120),
    Duration::from_secs(120),
];

/// Said of a launch failure that is Cairn's own and not the job's work, so a
/// parent can tell it from a task that failed at what it was asked to do.
const CAIRN_FAULT: &str =
    ". This is a Cairn fault, not the job's work; spawning it again may succeed";

/// Where a refused launch stands: when admission first refused it, how many
/// attempts it has refused out of how many it gets, the latest refusal, and
/// when the next attempt is due (`None` while one is under way).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchAdmissionWait {
    pub since_unix: i64,
    pub refusals: u32,
    pub attempts_allowed: u32,
    pub last_refusal: String,
    pub next_attempt_unix: Option<i64>,
}

fn utc(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|at| at.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| unix.to_string())
}

impl LaunchAdmissionWait {
    /// What every surface says about a job whose launch admission refused. It
    /// reaches parents, direct messengers and node readers alike, so it says
    /// "the job" and tells the reader that nothing is asked of them.
    pub fn describe(&self) -> String {
        let next = match self.next_attempt_unix {
            Some(at) => format!("next attempt at {}", utc(at)),
            None => "an attempt is under way".to_string(),
        };
        format!(
            "waiting for Cairn to admit its launch since {}: {} of {} attempts refused, most recently: {}; {next}. \
             Nothing is needed from you: Cairn retries on its own, and the job fails with this reason \
             if all {} are refused. Stop it only to give up early",
            utc(self.since_unix),
            self.refusals,
            self.attempts_allowed,
            self.last_refusal,
            self.attempts_allowed,
        )
    }
}

/// What a recorded refusal leaves for the job.
enum Redrive {
    /// Dispatch it again after this pause.
    After(Duration),
    /// Every attempt was refused; fail it.
    Exhausted(LaunchAdmissionWait),
}

/// Every launch admission refused that has not yet launched or settled, by
/// job id.
pub struct LaunchAdmissionWaits {
    waits: Mutex<HashMap<String, LaunchAdmissionWait>>,
    backoff: Mutex<Vec<Duration>>,
}

impl Default for LaunchAdmissionWaits {
    fn default() -> Self {
        Self {
            waits: Mutex::default(),
            backoff: Mutex::new(REDRIVE_BACKOFF.to_vec()),
        }
    }
}

impl LaunchAdmissionWaits {
    /// The job's refused launch, while it is being re-driven.
    pub fn get(&self, job_id: &str) -> Option<LaunchAdmissionWait> {
        self.waits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(job_id)
            .cloned()
    }

    /// Forget the job's refused launch: it was admitted, held, or settled.
    pub fn settle(&self, job_id: &str) {
        self.waits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(job_id);
    }

    /// The job's re-dispatch has begun: no attempt is pending on a timer.
    fn begin_attempt(&self, job_id: &str) {
        if let Some(wait) = self
            .waits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(job_id)
        {
            wait.next_attempt_unix = None;
        }
    }

    fn record_refusal(&self, job_id: &str, refusal: &str, now: i64) -> Redrive {
        let backoff = self
            .backoff
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut waits = self.waits.lock().unwrap_or_else(PoisonError::into_inner);
        let wait = waits
            .entry(job_id.to_string())
            .or_insert_with(|| LaunchAdmissionWait {
                since_unix: now,
                refusals: 0,
                attempts_allowed: backoff.len() as u32 + 1,
                last_refusal: String::new(),
                next_attempt_unix: None,
            });
        wait.refusals += 1;
        wait.last_refusal = refusal.to_string();
        match backoff.get(wait.refusals as usize - 1) {
            Some(pause) => {
                wait.next_attempt_unix = Some(now.saturating_add_unsigned(pause.as_secs()));
                Redrive::After(*pause)
            }
            None => {
                Redrive::Exhausted(waits.remove(job_id).expect("the refusal was just recorded"))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn set_backoff(&self, backoff: Vec<Duration>) {
        *self.backoff.lock().unwrap_or_else(PoisonError::into_inner) = backoff;
    }
}

/// Admit a launch the runner was handed, or arrange for it to be re-driven.
///
/// `true` means the runner starts the job now. `false` means it starts
/// nothing now: a hold took the launch (and owns its wake), or admission
/// refused it and it is re-dispatched after a pause, or failed with the
/// refusal once every attempt was refused.
pub async fn admit_dispatched_launch(orch: &Orchestrator, job_id: &str) -> bool {
    match admit_thread_launch(orch, job_id).await {
        Ok(admitted) => {
            orch.launch_admission_waits.settle(job_id);
            admitted
        }
        Err(refusal) => {
            redrive_refused_launch(orch, job_id, &refusal);
            false
        }
    }
}

/// Record that admission refused `job_id`'s launch, then re-dispatch it after
/// the next pause, or fail it if that was its last attempt. Both happen on a
/// task of their own, so the dispatch that met the refusal moves on to its
/// other jobs.
pub(crate) fn redrive_refused_launch(orch: &Orchestrator, job_id: &str, refusal: &str) {
    let now = orch.services.clock.now();
    let redrive = orch
        .launch_admission_waits
        .record_refusal(job_id, refusal, now);
    let orch_task = orch.clone();
    let job_id = job_id.to_string();
    match redrive {
        Redrive::After(pause) => {
            log::warn!(
                "Cannot admit launch {job_id}: {refusal}; dispatching it again in {} ms",
                pause.as_millis()
            );
            orch.spawn_detached(async move {
                tokio::time::sleep(pause).await;
                redispatch(&orch_task, &job_id).await;
            });
        }
        Redrive::Exhausted(wait) => {
            log::error!(
                "Cannot admit launch {job_id}: {refusal}; every attempt was refused, failing it"
            );
            orch.spawn_detached(async move {
                let reason = format!(
                    "its launch was never admitted: {} attempts were refused, most recently: {}",
                    wait.refusals, wait.last_refusal
                );
                fail_unadmitted(&orch_task, &job_id, &reason).await;
            });
        }
    }
}

/// Hand `job_id` to the runner again, unless it launched or settled while it
/// waited.
async fn redispatch(orch: &Orchestrator, job_id: &str) {
    orch.launch_admission_waits.begin_attempt(job_id);
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let job = match crate::jobs::queries::get_job(&db, job_id).await {
        Ok(job) => job,
        Err(error) => {
            log::warn!("Not re-dispatching launch {job_id}: {error}");
            orch.launch_admission_waits.settle(job_id);
            return;
        }
    };
    let unstarted = matches!(
        job.status,
        crate::models::JobStatus::Pending | crate::models::JobStatus::Running
    ) && matches!(
        crate::execution::jobs::live_run_for_job(&db, job_id).await,
        Ok(None)
    );
    if !unstarted {
        orch.launch_admission_waits.settle(job_id);
        return;
    }
    let Some(executor) = orch.executor.get() else {
        fail_unadmitted(
            orch,
            job_id,
            &format!(
                "its launch was refused admission, and no runner is configured to dispatch it again{CAIRN_FAULT}"
            ),
        )
        .await;
        return;
    };
    let handed = executor
        .execute(
            orch,
            crate::effects::types::WorkflowEffect::StartAgentJobs {
                jobs: vec![job],
                cause: cairn_workflow::effects::types::WakeCause::System,
            },
        )
        .await;
    if let Err(error) = handed {
        fail_unadmitted(
            orch,
            job_id,
            &format!(
                "its launch was refused admission, and handing it to a runner again failed: {error}{CAIRN_FAULT}"
            ),
        )
        .await;
    }
}

async fn fail_unadmitted(orch: &Orchestrator, job_id: &str, reason: &str) {
    orch.launch_admission_waits.settle(job_id);
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    crate::execution::jobs::mark_job_start_failed(orch, &db, job_id, reason, true).await;
}
