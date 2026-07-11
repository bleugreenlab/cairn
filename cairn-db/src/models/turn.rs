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
}

impl std::fmt::Display for TurnYieldReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurnYieldReason::UserInput => write!(f, "user_input"),
            TurnYieldReason::Permission => write!(f, "permission"),
            TurnYieldReason::DependencyWait => write!(f, "dependency_wait"),
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
}

impl std::fmt::Display for TurnEndReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TurnEndReason::ArtifactHandoff => write!(f, "artifact_handoff"),
            TurnEndReason::UserStop => write!(f, "user_stop"),
            TurnEndReason::Crash => write!(f, "crash"),
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
            "memory_review" => Ok(TurnStartReason::MemoryReview),
            _ => Err(format!("Unknown start reason: {}", s)),
        }
    }
}

impl From<crate::db_records::DbTurn> for Turn {
    fn from(db: crate::db_records::DbTurn) -> Self {
        Turn {
            id: db.id,
            session_id: db.session_id,
            run_id: db.run_id,
            job_id: db.job_id,
            sequence: db.sequence,
            predecessor_id: db.predecessor_id,
            state: db.state.parse().unwrap_or(TurnState::Pending),
            yield_reason: db.yield_reason.and_then(|r| r.parse().ok()),
            end_reason: db.end_reason.and_then(|r| r.parse().ok()),
            start_reason: db.start_reason.parse().unwrap_or(TurnStartReason::Initial),
            created_at: db.created_at as i64,
            started_at: db.started_at.map(|t| t as i64),
            ended_at: db.ended_at.map(|t| t as i64),
            updated_at: db.updated_at as i64,
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
    fn roundtrip_yield_reason_display_parse() {
        for reason in [
            TurnYieldReason::UserInput,
            TurnYieldReason::Permission,
            TurnYieldReason::DependencyWait,
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

    #[test]
    fn db_turn_conversion_unknown_state_falls_back() {
        let db = crate::db_records::DbTurn {
            id: "t1".into(),
            session_id: "s1".into(),
            run_id: None,
            job_id: None,
            sequence: 1,
            predecessor_id: None,
            state: "GARBAGE".into(),
            yield_reason: None,
            end_reason: None,
            start_reason: "initial".into(),
            created_at: 0,
            started_at: None,
            ended_at: None,
            updated_at: 0,
        };
        let turn: Turn = db.into();
        assert_eq!(turn.state, TurnState::Pending);
    }

    #[test]
    fn db_turn_conversion_unknown_start_reason_falls_back() {
        let db = crate::db_records::DbTurn {
            id: "t1".into(),
            session_id: "s1".into(),
            run_id: None,
            job_id: None,
            sequence: 1,
            predecessor_id: None,
            state: "running".into(),
            yield_reason: None,
            end_reason: None,
            start_reason: "GARBAGE".into(),
            created_at: 0,
            started_at: None,
            ended_at: None,
            updated_at: 0,
        };
        let turn: Turn = db.into();
        assert_eq!(turn.start_reason, TurnStartReason::Initial);
    }

    #[test]
    fn db_turn_conversion_unknown_yield_reason_becomes_none() {
        let db = crate::db_records::DbTurn {
            id: "t1".into(),
            session_id: "s1".into(),
            run_id: None,
            job_id: None,
            sequence: 1,
            predecessor_id: None,
            state: "yielded".into(),
            yield_reason: Some("GARBAGE".into()),
            end_reason: None,
            start_reason: "initial".into(),
            created_at: 0,
            started_at: None,
            ended_at: None,
            updated_at: 0,
        };
        let turn: Turn = db.into();
        assert!(turn.yield_reason.is_none());
    }
}
