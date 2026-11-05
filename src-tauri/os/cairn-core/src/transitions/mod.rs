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
//! // Transition a run (validated against the run state machine)
//! transitions::transition_run(&db, run_id, RunStatus::Live, &emitter).await?;
//!
//! // Move a job between the readiness states (Pending or Running) by hand
//! transitions::transition_job_readiness(&db, &emitter, job_id, JobStatus::Pending).await?;
//!
//! // Recompute the statuses above the jobs from the jobs themselves
//! transitions::recompute_execution_status(&db, execution_id).await;
//! transitions::recompute_issue_status(&db, issue_id).await;
//!
//! // Reopen a resolved issue, lifting the jobs its resolution cancelled
//! transitions::unresolve_issue(&db, &emitter, issue_id).await?;
//! ```
//!
//! Every other job status is derived, not written: the projection in
//! `cairn_workflow::advancement::recompute` settles each job from its facts.
//! A terminal resolution goes through `issues::status::resolve_terminal`.

pub mod outcome;
pub mod projection;
mod run;
mod status;
pub mod turn;

pub use run::{set_exit_reason, transition_run};
pub use status::{
    recompute_execution_status, recompute_issue_status, transition_job_readiness, unresolve_issue,
    ExecutionStatus, Resolution,
};
pub use turn::{apply_turn_outcome, interrupt_turn, start_turn, yield_turn};

pub use cairn_workflow::transitions::TransitionError;
