//! Delegated task runtime.

mod common;
mod results;
mod resume;
mod spawn;

pub(crate) use common::lookup_caller_job_id;
pub(crate) use resume::{is_call_child, resume_suspended_parent_after_task_completion};
pub(crate) use spawn::{spawn_call_packets, spawn_task_packets, spawn_workflow_packets};

// Delegation's two suspension hand-off markers stay with the client that writes
// them; this reach-through exists so the crate-wide property test over EVERY
// hand-off marker (`mcp::handlers::suspension_markers`) can see them too.
#[cfg(test)]
pub(crate) use spawn::{DELEGATED_TASKS_SUSPENDED_PARENT_SUFFIX, DELEGATED_TASKS_SUSPENDED_SUFFIX};
