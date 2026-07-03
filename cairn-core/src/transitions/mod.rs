//! Centralized state machine transitions.
//!
//! This module is the single authority for all status changes in the system.
//! Run and Job transitions are validated and stored. Execution and Issue
//! statuses are recomputed deterministically from the states below them.
//!
//! ## Hierarchy
//!
//! ```text
//! Run (stored)  →  Job (stored)  →  Execution (recomputed)  →  Issue (recomputed)
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! // Transition a run
//! transitions::transition_run(&mut conn, run_id, RunStatus::Live)?;
//!
//! // Transition a job (cascades to execution and issue)
//! transitions::transition_job(&mut conn, &emitter, job_id, JobStatus::Complete)?;
//!
//! // Resolve an issue (merge/close — sets timestamp and recomputes)
//! transitions::resolve_issue(&mut conn, &emitter, issue_id, Resolution::Merged)?;
//! ```

pub mod outcome;
pub mod projection;
mod run;
mod session;
mod status;
pub mod turn;

pub use run::{set_exit_reason, transition_run};
pub use session::transition_session;
pub use status::{
    recompute_execution_status, recompute_issue_status, resolve_issue, transition_job_readiness,
    unresolve_issue, ExecutionStatus, Resolution,
};
pub use turn::{apply_turn_outcome, interrupt_turn, start_turn, yield_turn};

/// Error type for invalid state transitions.
#[derive(Debug, Clone)]
pub struct TransitionError {
    pub entity: &'static str,
    pub id: String,
    pub from: String,
    pub to: String,
    pub reason: String,
}

impl std::fmt::Display for TransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Invalid {} transition for {}: {} → {} ({})",
            self.entity, self.id, self.from, self.to, self.reason
        )
    }
}

impl std::error::Error for TransitionError {}
