//! Host-specific effect executor trait.
//!
//! Only receives effects that require host resources: process spawning,
//! filesystem operations, external commands, LLM calls.
//!
//! Core-internal effects (AdvanceDag, EmitLifecycleMessage,
//! StoreConditionEvaluation, etc.) are handled by the effect loop directly
//! and never reach this trait.

use async_trait::async_trait;

use super::types::{EffectResult, WorkflowEffect};
use crate::orchestrator::Orchestrator;

/// Host-specific effect executor.
///
/// Implemented by each host (Tauri desktop app, cairn-server) to handle
/// effects that cross the host boundary: process spawning, worktree creation,
/// shell command execution, LLM condition evaluation.
///
/// The effect loop calls `execute` for each host effect. The executor returns
/// `Some(result)` if the effect produces a result that feeds back into the
/// core reducer, or `None` for fire-and-forget effects like `StartAgentJobs`.
#[async_trait]
pub trait EffectExecutor: Send + Sync {
    /// Execute a host-crossing effect.
    ///
    /// Returns `Some(result)` if the effect produces feedback for the core
    /// reducer. Returns `None` for fire-and-forget effects.
    async fn execute(
        &self,
        orch: &Orchestrator,
        effect: WorkflowEffect,
    ) -> Result<Option<EffectResult>, String>;
}
