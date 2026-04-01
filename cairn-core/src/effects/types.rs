//! Typed effects produced by the core transition layer.
//!
//! State transition functions (`apply_step_outcome`, `reduce_dag`) produce
//! `Vec<WorkflowEffect>` instead of executing side effects inline. An effect
//! loop in `run.rs` handles core-internal effects directly and dispatches
//! host-crossing effects to an `EffectExecutor` trait implementation.

use std::path::PathBuf;

use crate::messages::system::JobEvent;
use crate::models::Job;

/// Shared context carried by effects for debugging and correlation.
#[derive(Debug, Clone)]
pub struct EffectContext {
    pub job_id: Option<String>,
    pub run_id: Option<String>,
    pub execution_id: Option<String>,
    pub source: EffectSource,
}

/// Where an effect originated.
#[derive(Debug, Clone)]
pub enum EffectSource {
    /// From `apply_step_outcome`
    StepOutcome,
    /// From `reduce_dag`
    DagAdvancement,
    /// From `reduce_effect_result` after checkpoint
    CheckpointResult,
    /// From `reduce_effect_result` after condition eval
    ConditionResult,
}

/// Effects produced by the core transition layer.
///
/// Split into two categories:
/// - **Core effects**: handled by the effect loop directly (no host involvement)
/// - **Host effects**: dispatched to the `EffectExecutor` trait
pub enum WorkflowEffect {
    // ── Core effects (handled by the effect loop) ──────────────────────
    /// Advance the DAG for an execution.
    /// The effect loop calls `reduce_dag` directly — this never reaches the host.
    AdvanceDag {
        execution_id: String,
        /// If this effect was persisted in the outbox, the specific entry ID.
        /// Used for precise row-level acknowledgement after processing.
        /// `None` for follow-on effects produced by `reduce_dag` / `reduce_effect_result`.
        outbox_entry_id: Option<String>,
    },

    /// Emit a lifecycle system message (job completed/failed/blocked).
    EmitLifecycleMessage {
        job_id: String,
        run_id: Option<String>,
        event: JobEvent,
    },

    /// Wake manager on job failure.
    WakeManager { job_id: String },

    /// Store a condition evaluation result and advance.
    StoreConditionEvaluation {
        execution_id: String,
        node_id: String,
        port: String,
        error_msg: Option<String>,
    },

    /// Apply checkpoint approval (DB transition + follow-on effects).
    ApplyCheckpointApproval { job_id: String },

    /// Apply checkpoint rejection (DB transition + follow-on effects).
    ApplyCheckpointRejection {
        job_id: String,
        reason: Option<String>,
    },

    /// Mark a job as failed (transition + error event).
    MarkJobFailed { job_id: String, error: String },

    /// Mark an action run as failed.
    MarkActionRunFailed {
        action_run_id: String,
        error: String,
    },

    /// Clean up worktrees for completed non-issue executions.
    CleanupWorktrees { execution_id: String },

    // ── Host effects (dispatched to EffectExecutor) ────────────────────
    /// Start ready agent jobs (host prepares worktree + spawns process).
    StartAgentJobs(Vec<Job>),

    /// Execute an action node (built-in or shell command).
    ExecuteAction {
        action_run_id: String,
        execution_id: String,
        node_id: String,
        ctx: EffectContext,
    },

    /// Run a programmatic checkpoint command.
    RunCheckpointCommand {
        job_id: String,
        node_name: String,
        command: String,
        worktree_path: PathBuf,
        cached_pass: bool,
        ctx: EffectContext,
    },

    /// Evaluate a condition node (may involve LLM call).
    EvaluateCondition {
        execution_id: String,
        node_id: String,
        node_name: String,
        /// Lightweight condition spec extracted from the recipe node config.
        condition: ConditionSpec,
        ctx: EffectContext,
    },

    /// Create a worktree for an executor node.
    CreateExecutorWorktree {
        job_id: String,
        execution_id: String,
        project_id: String,
        ctx: EffectContext,
    },
}

/// Lightweight condition spec extracted from DbRecipeNode config.
/// Avoids putting heavy DB structs in the effect.
#[derive(Debug, Clone)]
pub struct ConditionSpec {
    pub condition_type: String,
    pub expression: Option<String>,
    pub question: Option<String>,
    pub ports: Vec<String>,
    pub error_handling: String,
}

/// Results from host effect execution that feed back into the core reducer.
pub enum EffectResult {
    /// Checkpoint command completed.
    CheckpointComplete {
        job_id: String,
        passed: bool,
        error: Option<String>,
    },

    /// Condition evaluated.
    ConditionEvaluated {
        execution_id: String,
        node_id: String,
        port: String,
        error_msg: Option<String>,
    },

    /// Worktree created for executor node.
    WorktreeCreated {
        job_id: String,
        execution_id: String,
    },

    /// Worktree creation failed.
    WorktreeFailed { job_id: String, error: String },

    /// Action completed successfully.
    ActionComplete { execution_id: String },

    /// Action failed.
    ActionFailed {
        action_run_id: String,
        execution_id: String,
        error: String,
    },
}

// WorkflowEffect is not Send because JobEvent contains references to DB types
// that don't implement Send. The effect loop runs on the same thread as the
// orchestrator, so this is fine.
impl std::fmt::Debug for WorkflowEffect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdvanceDag { execution_id, .. } => f
                .debug_struct("AdvanceDag")
                .field("execution_id", execution_id)
                .finish(),
            Self::EmitLifecycleMessage { job_id, .. } => f
                .debug_struct("EmitLifecycleMessage")
                .field("job_id", job_id)
                .finish(),
            Self::WakeManager { job_id } => f
                .debug_struct("WakeManager")
                .field("job_id", job_id)
                .finish(),
            Self::StoreConditionEvaluation {
                execution_id,
                node_id,
                ..
            } => f
                .debug_struct("StoreConditionEvaluation")
                .field("execution_id", execution_id)
                .field("node_id", node_id)
                .finish(),
            Self::ApplyCheckpointApproval { job_id } => f
                .debug_struct("ApplyCheckpointApproval")
                .field("job_id", job_id)
                .finish(),
            Self::ApplyCheckpointRejection { job_id, .. } => f
                .debug_struct("ApplyCheckpointRejection")
                .field("job_id", job_id)
                .finish(),
            Self::MarkJobFailed { job_id, .. } => f
                .debug_struct("MarkJobFailed")
                .field("job_id", job_id)
                .finish(),
            Self::MarkActionRunFailed { action_run_id, .. } => f
                .debug_struct("MarkActionRunFailed")
                .field("action_run_id", action_run_id)
                .finish(),
            Self::CleanupWorktrees { execution_id } => f
                .debug_struct("CleanupWorktrees")
                .field("execution_id", execution_id)
                .finish(),
            Self::StartAgentJobs(jobs) => f
                .debug_struct("StartAgentJobs")
                .field("count", &jobs.len())
                .finish(),
            Self::ExecuteAction {
                action_run_id,
                execution_id,
                ..
            } => f
                .debug_struct("ExecuteAction")
                .field("action_run_id", action_run_id)
                .field("execution_id", execution_id)
                .finish(),
            Self::RunCheckpointCommand {
                job_id, node_name, ..
            } => f
                .debug_struct("RunCheckpointCommand")
                .field("job_id", job_id)
                .field("node_name", node_name)
                .finish(),
            Self::EvaluateCondition {
                execution_id,
                node_name,
                ..
            } => f
                .debug_struct("EvaluateCondition")
                .field("execution_id", execution_id)
                .field("node_name", node_name)
                .finish(),
            Self::CreateExecutorWorktree {
                job_id,
                execution_id,
                ..
            } => f
                .debug_struct("CreateExecutorWorktree")
                .field("job_id", job_id)
                .field("execution_id", execution_id)
                .finish(),
        }
    }
}

impl std::fmt::Debug for EffectResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CheckpointComplete { job_id, passed, .. } => f
                .debug_struct("CheckpointComplete")
                .field("job_id", job_id)
                .field("passed", passed)
                .finish(),
            Self::ConditionEvaluated {
                execution_id,
                node_id,
                port,
                ..
            } => f
                .debug_struct("ConditionEvaluated")
                .field("execution_id", execution_id)
                .field("node_id", node_id)
                .field("port", port)
                .finish(),
            Self::WorktreeCreated {
                job_id,
                execution_id,
            } => f
                .debug_struct("WorktreeCreated")
                .field("job_id", job_id)
                .field("execution_id", execution_id)
                .finish(),
            Self::WorktreeFailed { job_id, .. } => f
                .debug_struct("WorktreeFailed")
                .field("job_id", job_id)
                .finish(),
            Self::ActionComplete { execution_id } => f
                .debug_struct("ActionComplete")
                .field("execution_id", execution_id)
                .finish(),
            Self::ActionFailed {
                action_run_id,
                execution_id,
                ..
            } => f
                .debug_struct("ActionFailed")
                .field("action_run_id", action_run_id)
                .field("execution_id", execution_id)
                .finish(),
        }
    }
}
