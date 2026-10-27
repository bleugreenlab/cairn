//! Turn types — semantic work units between host interaction points.
//!
//! A Turn represents the period where an agent has the floor — from receiving
//! input to either completing, failing, or yielding back to the host.

use serde::{Deserialize, Serialize};

/// A turn — a semantic unit of agent work within a session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Turn {
    pub id: String,
    pub session_id: String,
    pub run_id: Option<String>,
    pub job_id: Option<String>,
    pub sequence: i32,
    pub predecessor_id: Option<String>,
    pub state: TurnState,
    pub yield_reason: Option<TurnYieldReason>,
    pub end_reason: Option<TurnEndReason>,
    pub start_reason: TurnStartReason,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub ended_at: Option<i64>,
    pub updated_at: i64,
}

/// Turn lifecycle state.
///
/// Transitions are validated by `transitions::turn`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TurnState {
    Pending,
    Running,
    Yielded,
    Complete,
    Failed,
    Interrupted,
    Cancelled,
}

impl TurnState {
    /// Whether the turn is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TurnState::Yielded
                | TurnState::Complete
                | TurnState::Failed
                | TurnState::Interrupted
                | TurnState::Cancelled
        )
    }
}

impl std::fmt::Display for TurnState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurnState::Pending => write!(f, "pending"),
            TurnState::Running => write!(f, "running"),
            TurnState::Yielded => write!(f, "yielded"),
            TurnState::Complete => write!(f, "complete"),
            TurnState::Failed => write!(f, "failed"),
            TurnState::Interrupted => write!(f, "interrupted"),
            TurnState::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for TurnState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(TurnState::Pending),
            "running" => Ok(TurnState::Running),
            "yielded" => Ok(TurnState::Yielded),
            "complete" => Ok(TurnState::Complete),
            "failed" => Ok(TurnState::Failed),
            "interrupted" => Ok(TurnState::Interrupted),
            "cancelled" => Ok(TurnState::Cancelled),
            _ => Err(format!("Unknown turn state: {}", s)),
        }
    }
}

/// Reason a turn yielded back to the host.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TurnYieldReason {
    UserInput,
    Permission,
    DependencyWait,
    Wait,
    SafetyPause,
    ProviderLimit,
}

impl std::fmt::Display for TurnYieldReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurnYieldReason::UserInput => write!(f, "user_input"),
            TurnYieldReason::Permission => write!(f, "permission"),
            TurnYieldReason::DependencyWait => write!(f, "dependency_wait"),
            TurnYieldReason::Wait => write!(f, "wait"),
            TurnYieldReason::SafetyPause => write!(f, "safety_pause"),
            TurnYieldReason::ProviderLimit => write!(f, "provider_limit"),
        }
    }
}

impl std::str::FromStr for TurnYieldReason {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "user_input" => Ok(TurnYieldReason::UserInput),
            "permission" => Ok(TurnYieldReason::Permission),
            "dependency_wait" => Ok(TurnYieldReason::DependencyWait),
            "wait" => Ok(TurnYieldReason::Wait),
            "safety_pause" => Ok(TurnYieldReason::SafetyPause),
            "provider_limit" => Ok(TurnYieldReason::ProviderLimit),
            _ => Err(format!("Unknown yield reason: {}", s)),
        }
    }
}

/// Notable reason a terminal turn ended. `None` means natural completion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TurnEndReason {
    ArtifactHandoff,
    UserStop,
    Crash,
    OrphanedPending,
    /// The provider account serving the turn was depleted. The turn produced
    /// no finished work and resumes on an explicit continuation.
    ProviderDepleted,
    /// A launch created this turn and was then refused before it attached to
    /// a run; the launch guard cancelled it.
    LaunchRefused,
    /// The turn's execution was superseded while the turn was live.
    Superseded,
    /// A permission answer minted this turn to continue the job in, and the
    /// job refused to continue; nothing ever served it.
    PermissionResumeRefused,
    /// The operator compacted the session while the turn was live: its process
    /// was killed and the job continued on a fresh session from the digest.
    OperatorCompact,
    /// The host restarted under the turn: the boot sweep settled the run the
    /// turn was bound to. Marks the job for its one automatic resume after
    /// boot, and tells that resume to warn the agent that any tool call in
    /// flight has an unknown outcome.
    HostRestart,
}

impl std::fmt::Display for TurnEndReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurnEndReason::ArtifactHandoff => write!(f, "artifact_handoff"),
            TurnEndReason::UserStop => write!(f, "user_stop"),
            TurnEndReason::Crash => write!(f, "crash"),
            TurnEndReason::OrphanedPending => write!(f, "orphaned_pending"),
            TurnEndReason::ProviderDepleted => write!(f, "provider_depleted"),
            TurnEndReason::LaunchRefused => write!(f, "launch_refused"),
            TurnEndReason::Superseded => write!(f, "superseded"),
            TurnEndReason::PermissionResumeRefused => write!(f, "permission_resume_refused"),
            TurnEndReason::OperatorCompact => write!(f, "operator_compact"),
            TurnEndReason::HostRestart => write!(f, "host_restart"),
        }
    }
}

impl std::str::FromStr for TurnEndReason {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "artifact_handoff" => Ok(TurnEndReason::ArtifactHandoff),
            "user_stop" => Ok(TurnEndReason::UserStop),
            "crash" => Ok(TurnEndReason::Crash),
            "orphaned_pending" => Ok(TurnEndReason::OrphanedPending),
            "provider_depleted" => Ok(TurnEndReason::ProviderDepleted),
            "launch_refused" => Ok(TurnEndReason::LaunchRefused),
            "superseded" => Ok(TurnEndReason::Superseded),
            "permission_resume_refused" => Ok(TurnEndReason::PermissionResumeRefused),
            "operator_compact" => Ok(TurnEndReason::OperatorCompact),
            "host_restart" => Ok(TurnEndReason::HostRestart),
            _ => Err(format!("Unknown turn end reason: {}", s)),
        }
    }
}

/// Reason a turn was created.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TurnStartReason {
    Initial,
    FollowUp,
    PromptResponse,
    PermissionResponse,
    Retry,
    ManagerWake,
    DependencyUnblock,
    WaitResolved,
    /// The post-completion memory review/reflection turn. It runs after the
    /// job's real work turn has already completed, so the job-status projection
    /// excludes it when gathering facts (the work turn stays the latest fact),
    /// and review completion keys off this turn actually ending.
    MemoryReview,
}

impl std::fmt::Display for TurnStartReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurnStartReason::Initial => write!(f, "initial"),
            TurnStartReason::FollowUp => write!(f, "follow_up"),
            TurnStartReason::PromptResponse => write!(f, "prompt_response"),
            TurnStartReason::PermissionResponse => write!(f, "permission_response"),
            TurnStartReason::Retry => write!(f, "retry"),
            TurnStartReason::ManagerWake => write!(f, "manager_wake"),
            TurnStartReason::DependencyUnblock => write!(f, "dependency_unblock"),
            TurnStartReason::WaitResolved => write!(f, "wait_resolved"),
            TurnStartReason::MemoryReview => write!(f, "memory_review"),
        }
    }
}

impl std::str::FromStr for TurnStartReason {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "initial" => Ok(TurnStartReason::Initial),
            "follow_up" => Ok(TurnStartReason::FollowUp),
            "prompt_response" => Ok(TurnStartReason::PromptResponse),
            "permission_response" => Ok(TurnStartReason::PermissionResponse),
            "retry" => Ok(TurnStartReason::Retry),
            "manager_wake" => Ok(TurnStartReason::ManagerWake),
            "dependency_unblock" => Ok(TurnStartReason::DependencyUnblock),
            "wait_resolved" => Ok(TurnStartReason::WaitResolved),
            "memory_review" => Ok(TurnStartReason::MemoryReview),
            _ => Err(format!("Unknown start reason: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_terminal_covers_all_terminal_states() {
        assert!(TurnState::Yielded.is_terminal());
        assert!(TurnState::Complete.is_terminal());
        assert!(TurnState::Failed.is_terminal());
        assert!(TurnState::Interrupted.is_terminal());
        assert!(TurnState::Cancelled.is_terminal());
    }

    #[test]
    fn is_terminal_excludes_non_terminal() {
        assert!(!TurnState::Pending.is_terminal());
        assert!(!TurnState::Running.is_terminal());
    }

    #[test]
    fn roundtrip_turn_state_display_parse() {
        for state in [
            TurnState::Pending,
            TurnState::Running,
            TurnState::Yielded,
            TurnState::Complete,
            TurnState::Failed,
            TurnState::Interrupted,
            TurnState::Cancelled,
        ] {
            let s = state.to_string();
            let parsed: TurnState = s.parse().unwrap();
            assert_eq!(parsed, state);
        }
    }

    #[test]
    fn provider_limit_yield_serializes_as_its_durable_reason() {
        let reason = TurnYieldReason::ProviderLimit;
        let json = serde_json::to_string(&reason).unwrap();
        assert_eq!(json, "\"provider_limit\"");
        assert_eq!(
            serde_json::from_str::<TurnYieldReason>(&json).unwrap(),
            reason
        );
    }

    #[test]
    fn roundtrip_yield_reason_display_parse() {
        for reason in [
            TurnYieldReason::UserInput,
            TurnYieldReason::Permission,
            TurnYieldReason::DependencyWait,
            TurnYieldReason::Wait,
            TurnYieldReason::SafetyPause,
            TurnYieldReason::ProviderLimit,
        ] {
            let s = reason.to_string();
            let parsed: TurnYieldReason = s.parse().unwrap();
            assert_eq!(parsed, reason);
        }
    }

    #[test]
    fn roundtrip_end_reason_display_parse() {
        for reason in [
            TurnEndReason::ArtifactHandoff,
            TurnEndReason::UserStop,
            TurnEndReason::Crash,
            TurnEndReason::PermissionResumeRefused,
        ] {
            let parsed: TurnEndReason = reason.to_string().parse().unwrap();
            assert_eq!(parsed, reason);
        }
    }

    #[test]
    fn roundtrip_start_reason_display_parse() {
        for reason in [
            TurnStartReason::Initial,
            TurnStartReason::FollowUp,
            TurnStartReason::PromptResponse,
            TurnStartReason::PermissionResponse,
            TurnStartReason::Retry,
            TurnStartReason::ManagerWake,
            TurnStartReason::DependencyUnblock,
            TurnStartReason::MemoryReview,
        ] {
            let s = reason.to_string();
            let parsed: TurnStartReason = s.parse().unwrap();
            assert_eq!(parsed, reason);
        }
    }

    #[test]
    fn unknown_state_parse_errors() {
        assert!("bogus".parse::<TurnState>().is_err());
    }

    #[test]
    fn unknown_yield_reason_parse_errors() {
        assert!("bogus".parse::<TurnYieldReason>().is_err());
    }

    #[test]
    fn unknown_start_reason_parse_errors() {
        assert!("bogus".parse::<TurnStartReason>().is_err());
    }
}
