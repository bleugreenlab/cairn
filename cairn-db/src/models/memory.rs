//! Memory types for the agent learning system.

use serde::{Deserialize, Serialize};

/// A learned memory intake row.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Memory {
    pub id: String,
    pub name: Option<String>,
    pub project_id: Option<String>,
    pub content: String,
    pub status: MemoryStatus,
    pub scope: MemoryScope,
    pub scope_value: String,
    pub job_id: Option<String>,
    pub node_seq: Option<i64>,
    pub promoted_commit_sha: Option<String>,
    pub reason: Option<String>,
    pub triage_decision: Option<MemoryTriageDecision>,
    pub deferred_scope: Option<MemoryScope>,
    pub deferred_scope_value: Option<String>,
    /// Precise provenance for captured intake: the transcript turn URI of the
    /// write that appended this memory.
    pub provenance_uri: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Triage state for an intake-ledger memory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MemoryStatus {
    Draft,
    Pending,
    Claimed,
    Promoted,
    Discarded,
    Deferred,
}

/// Explicit scope kind for a memory intake row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MemoryScope {
    Project,
    Role,
    Workspace,
}

/// Reasoned decision recorded by the Integrator while a memory is claimed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MemoryTriageDecision {
    Promote,
    Discard,
    Defer,
}

impl std::fmt::Display for MemoryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryStatus::Draft => write!(f, "draft"),
            MemoryStatus::Pending => write!(f, "pending"),
            MemoryStatus::Claimed => write!(f, "claimed"),
            MemoryStatus::Promoted => write!(f, "promoted"),
            MemoryStatus::Discarded => write!(f, "discarded"),
            MemoryStatus::Deferred => write!(f, "deferred"),
        }
    }
}

impl std::str::FromStr for MemoryStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "draft" => Ok(MemoryStatus::Draft),
            "pending" => Ok(MemoryStatus::Pending),
            "claimed" => Ok(MemoryStatus::Claimed),
            "promoted" => Ok(MemoryStatus::Promoted),
            "discarded" => Ok(MemoryStatus::Discarded),
            "deferred" => Ok(MemoryStatus::Deferred),
            _ => Err(format!("Invalid memory status: {}", s)),
        }
    }
}

impl std::fmt::Display for MemoryTriageDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryTriageDecision::Promote => write!(f, "promote"),
            MemoryTriageDecision::Discard => write!(f, "discard"),
            MemoryTriageDecision::Defer => write!(f, "defer"),
        }
    }
}

impl std::str::FromStr for MemoryTriageDecision {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "promote" => Ok(MemoryTriageDecision::Promote),
            "discard" => Ok(MemoryTriageDecision::Discard),
            "defer" => Ok(MemoryTriageDecision::Defer),
            _ => Err(format!("Invalid memory triage decision: {}", s)),
        }
    }
}

impl std::fmt::Display for MemoryScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryScope::Project => write!(f, "project"),
            MemoryScope::Role => write!(f, "role"),
            MemoryScope::Workspace => write!(f, "workspace"),
        }
    }
}

impl std::str::FromStr for MemoryScope {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "project" => Ok(MemoryScope::Project),
            "role" => Ok(MemoryScope::Role),
            "workspace" => Ok(MemoryScope::Workspace),
            _ => Err(format!("Invalid memory scope: {}", s)),
        }
    }
}

/// Input for creating a new memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMemory {
    /// Optional agent-chosen display handle. Not used for identity.
    pub name: Option<String>,
    pub content: String,
    pub project_id: Option<String>,
    pub scope: MemoryScope,
    pub scope_value: String,
    pub job_id: Option<String>,
    pub node_seq: Option<i64>,
    pub provenance_uri: Option<String>,
}

/// Input for updating an existing memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMemory {
    pub id: String,
    pub content: Option<String>,
    pub status: Option<MemoryStatus>,
}

#[cfg(test)]
mod tests {
    use super::{MemoryScope, MemoryStatus, MemoryTriageDecision};

    #[test]
    fn memory_status_round_trips_and_rejects_handled() {
        for status in [
            MemoryStatus::Draft,
            MemoryStatus::Pending,
            MemoryStatus::Claimed,
            MemoryStatus::Promoted,
            MemoryStatus::Discarded,
            MemoryStatus::Deferred,
        ] {
            let raw = status.to_string();
            assert_eq!(raw.parse::<MemoryStatus>().unwrap(), status);
        }
        assert!("handled".parse::<MemoryStatus>().is_err());
    }

    #[test]
    fn memory_triage_decision_round_trips() {
        for decision in [
            MemoryTriageDecision::Promote,
            MemoryTriageDecision::Discard,
            MemoryTriageDecision::Defer,
        ] {
            let raw = decision.to_string();
            assert_eq!(raw.parse::<MemoryTriageDecision>().unwrap(), decision);
        }
        assert!("keep".parse::<MemoryTriageDecision>().is_err());
    }

    #[test]
    fn memory_scope_round_trips() {
        for scope in [
            MemoryScope::Project,
            MemoryScope::Role,
            MemoryScope::Workspace,
        ] {
            let raw = scope.to_string();
            assert_eq!(raw.parse::<MemoryScope>().unwrap(), scope);
        }
    }
}
