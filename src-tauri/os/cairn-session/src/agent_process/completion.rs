//! Session facts reported to the host after a provider turn ends.
//!
//! These values carry no run/task policy, orchestrator, or database handle.
//! They form the completion boundary for the planned `cairn-session` crate.

use super::stream::TokenCounts;
use cairn_models::turn::{SuspendKind, TurnExit};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionTurnOutcome {
    Completed,
    Interrupted,
    /// Preserve the provider status for the host's diagnostic event.
    Failed {
        status: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLifetime {
    Persistent,
    /// The provider reader releases this session after its terminal event.
    OneShot,
}

#[derive(Debug)]
pub struct SessionTurnCompletion {
    pub outcome: SessionTurnOutcome,
    pub usage: TokenCounts,
    pub session_lifetime: SessionLifetime,
}

/// Claude's Result flag is authoritative even when its subtype says success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaudeTurnOutcome {
    Completed,
    Failed { message: String },
}

/// Exit facts from the in-process HTTP tool loop. These differ from a provider
/// Result: a durable wait can end the loop without a provider failure or success.
/// The loop itself reports a [`TurnExit`]; `Failed` and `Panicked` are the
/// host's containment of its error and of a panic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedTurnOutcome {
    Completed,
    Cancelled,
    Suspended(SuspendKind),
    Failed { message: String },
    Panicked { message: String },
}

impl From<TurnExit> for OwnedTurnOutcome {
    fn from(exit: TurnExit) -> Self {
        match exit {
            TurnExit::Completed => Self::Completed,
            TurnExit::Cancelled => Self::Cancelled,
            TurnExit::Suspended(kind) => Self::Suspended(kind),
        }
    }
}

pub struct ClaudeTurnContext {
    pub terminal_tool_completed: bool,
    pub terminal_interrupt_ack_pending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnFailureDisposition {
    Unhandled,
    /// A specific diagnostic exists, but the host must still fail the run.
    DiagnosticRecorded,
    /// Recovery owns the run transition as well as the diagnostic.
    AlreadyHandled,
}

pub struct CodexTurnContext {
    pub repeat_safety_pause: bool,
    pub failure_disposition: TurnFailureDisposition,
}
