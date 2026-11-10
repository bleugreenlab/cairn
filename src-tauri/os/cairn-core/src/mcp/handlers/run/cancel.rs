//! Cancelling one run batch, addressed by its request id.
//!
//! The cancel adds no second way for a batch to end. It records who cancelled
//! the batch, interrupts its host items, and stops it exactly as its ceiling
//! does ([`super::BatchControl::stop`]): placement halts and the fleet sends the
//! batch's machine `Cancel` for its request, which ends the process tree. The
//! batch's own task then settles it through the ordinary cancelled tail, so
//! whatever is waiting on it hears the result the way it would hear any other:
//! a parked call resumes its turn once through the durable resolver, and a
//! detached batch settles its row and queues its job's notice. Nothing here
//! resolves a wait or writes a row.
//!
//! The settle gate decides whether a cancel can still matter. Before
//! settlement begins it stops the batch, and nothing from it is published.
//! Once settlement has begun the batch may already be writing its commit, so
//! the cancel touches nothing and that result stands; a replay carries the
//! commit like any other.
//!
//! Exactly one batch: only that control and that fleet request are touched,
//! never the session, the turn, the job, or a sibling batch.

use super::{detached, Interruption};
use crate::orchestrator::Orchestrator;

/// How long a cancel waits for its batch to leave this runner and the fleet
/// before answering that it is still stopping.
pub(crate) const CANCEL_SETTLE_WAIT: std::time::Duration = std::time::Duration::from_secs(20);

/// What a cancel did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CancelReport {
    /// It was stopped before settlement began, so it publishes nothing.
    /// `settled` when its task and its fleet request both ended within the
    /// wait, which is when its durable rows release the branch.
    Stopped { settled: bool },
    /// Its commands finished on their own before the cancel reached them, and
    /// it settled with its own result.
    FinishedFirst { verdict: String },
    /// Settlement had already begun: its result, and any commit, stands.
    AlreadySettling,
    /// No task in this runner holds it, but the fleet did: its machine was
    /// told to cancel it, and that machine's report settles it.
    Retracted,
    /// Nothing in this runner or its fleet holds it.
    NotLive,
}

impl CancelReport {
    /// The cancel's answer, for the principal that asked.
    pub(crate) fn sentence(&self, request_id: &str) -> String {
        match self {
            Self::Stopped { settled: true } => format!(
                "Cancelled run batch `{request_id}`: it settled as cancelled and committed nothing, and its machine work has ended, so it no longer holds its branch. A call waiting on it resumes once with that result; a detached batch's result reaches its job as a notice."
            ),
            Self::Stopped { settled: false } => format!(
                "Cancelling run batch `{request_id}`: it was stopped before it began publishing, so it commits nothing, but its machine has not yet confirmed the cancel. It settles as cancelled when it does, and holds its branch until then."
            ),
            Self::FinishedFirst { verdict } => format!(
                "Run batch `{request_id}` finished on its own before the cancel reached it ({verdict}); its own result stands, and any commit it made is on the branch."
            ),
            Self::AlreadySettling => format!(
                "Run batch `{request_id}` was not cancelled: it had already begun publishing its result, so any commit it carries stands, and a replay carries it like any other commit. It releases its branch once it finishes publishing."
            ),
            Self::Retracted => format!(
                "Asked the machine running batch `{request_id}` to cancel it. No task in this runner holds it (an earlier runner placed it), so it settles from that machine's own report."
            ),
            Self::NotLive => format!(
                "Nothing was cancelled: no task in this runner holds run batch `{request_id}` and no machine holds its request."
            ),
        }
    }

    /// Whether the cancel reached a batch at all.
    pub(crate) fn reached(&self) -> bool {
        !matches!(self, Self::NotLive)
    }
}

/// Cancel the one batch placed under `request_id`, on behalf of `by` (a
/// node or task home URI, or `operator`), waiting up to `settle_wait` for it
/// to leave this runner and the fleet.
pub(crate) async fn cancel_batch(
    orch: &Orchestrator,
    request_id: &str,
    by: &str,
    settle_wait: std::time::Duration,
) -> CancelReport {
    let Some(control) = detached::live_control(request_id) else {
        // A batch an earlier runner placed is still the fleet's request: the
        // same `Cancel` ends it, and its machine's report settles its row.
        return match orch.cancel_cell_request(request_id) {
            crate::fleet::CancelOutcome::Unknown => CancelReport::NotLive,
            _ => CancelReport::Retracted,
        };
    };
    if control.settling() {
        return CancelReport::AlreadySettling;
    }
    control.set_cancelled_by(by);
    control.interrupt(Interruption::Cancelled(format!("cancelled by {by}")));
    control.stop(orch);
    log::info!("run batch {request_id} cancelled by {by}");
    let deadline = tokio::time::Instant::now() + settle_wait;
    // Settled once its task and fleet request have ended and its resolver has
    // written what it owes: the wait's resolution or the detached row. Those
    // durable rows are what hold the branch.
    let settled = loop {
        if !detached::batch_is_live(orch, request_id) && !detached::has_live_resolver(request_id) {
            break true;
        }
        if tokio::time::Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    // The process may have finished on its own between the gate read above
    // and the stop reaching it; its verdict then says so.
    match control.settled_verdict() {
        Some(verdict) if settled && !cancelled_verdict(&verdict) => {
            CancelReport::FinishedFirst { verdict }
        }
        _ => CancelReport::Stopped { settled },
    }
}

/// Whether a settled verdict is a cancellation's (or a stop before any
/// machine claimed the batch).
fn cancelled_verdict(verdict: &str) -> bool {
    verdict.starts_with("it was cancelled") || verdict.starts_with("it was never placed")
}
