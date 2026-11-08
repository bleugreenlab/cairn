//! Install-local operator request wire records. These records are not grants.
use crate::authorization::ActBinding;
use crate::identity::{AppearanceSnapshot, PrincipalRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestKind {
    Rebuild,
    BumpVersion,
    LiftDrain,
}
impl RequestKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rebuild => "rebuild",
            Self::BumpVersion => "bump_version",
            Self::LiftDrain => "lift_drain",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestState {
    Open,
    Accepted,
    Acting,
    Done,
    Failed,
    Declined,
    Withdrawn,
}
impl RequestState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Accepted => "accepted",
            Self::Acting => "acting",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Declined => "declined",
            Self::Withdrawn => "withdrawn",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RequestRequirements {
    #[serde(default)]
    pub commits: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileOperatorRequest {
    pub kind: RequestKind,
    pub target: Option<String>,
    pub rationale: String,
    #[serde(default)]
    pub waiting: Vec<String>,
    #[serde(default)]
    pub requirements: RequestRequirements,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContribution {
    pub id: String,
    pub request_id: String,
    pub origin_uri: String,
    pub run_id: String,
    pub principal: PrincipalRef,
    pub wake_destination: String,
    pub rationale: String,
    pub waiting: Vec<String>,
    pub requirements: RequestRequirements,
    pub minimum_target: Option<String>,
    pub normalized_target: serde_json::Value,
    pub active: bool,
    pub satisfied_by_attempt: Option<String>,
    pub created_at: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestFacts {
    pub version: i64,
    pub schema_version: u32,
    pub revision: i64,
    pub observed_at: i64,
    pub document: serde_json::Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainAcceptance {
    pub executor_id: String,
    pub drain_key: String,
    pub owner: String,
    /// The operator entry's own record generation.
    pub generation: u64,
    /// The machine's retained monotonic revision when the operator accepted.
    /// Distinct from `generation`: any other owner's change advances it, so an
    /// acceptance cannot silently apply to a drain set it never displayed.
    pub machine_revision: u64,
}
/// Mandatory, versioned authority inputs. No field is inferred from prose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestAcceptance {
    pub schema_version: u32,
    pub install_id: String,
    pub workspace_id: String,
    pub cohort_id: String,
    pub request_ids: Vec<String>,
    pub attempt_id: String,
    pub actor_id: String,
    pub kind: RequestKind,
    #[serde(deserialize_with = "required_option")]
    pub target: Option<String>,
    pub target_identity: String,
    pub binding: ActBinding,
    pub facts_version: i64,
    pub revision: i64,
    #[serde(deserialize_with = "required_option")]
    pub candidate_commit: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub base_commit: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub policy_project_id: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub drain: Option<DrainAcceptance>,
    pub operator: PrincipalRef,
    pub appearance: AppearanceSnapshot,
    pub accepted_at: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestAttemptOutcome {
    Done,
    Failed,
    RolledBack,
    Cancelled,
    Inconclusive,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestAttempt {
    pub id: String,
    pub authorization_event_id: String,
    pub acceptance: RequestAcceptance,
    pub state: RequestState,
    pub claim_id: Option<String>,
    pub outcome: Option<RequestAttemptOutcome>,
    pub report: Option<serde_json::Value>,
    pub updated_at: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorRequest {
    pub id: String,
    pub uri: String,
    pub workspace_id: String,
    pub cohort_id: String,
    pub member_request_ids: Vec<String>,
    pub kind: RequestKind,
    pub target: Option<String>,
    pub target_identity: String,
    pub coalescing_key: Option<String>,
    pub revision: i64,
    pub state: RequestState,
    pub created_at: i64,
    pub updated_at: i64,
    pub current_attempt: Option<String>,
    pub fulfilled_by_attempt: Option<String>,
    pub report: Option<serde_json::Value>,
    pub requesters: Vec<RequestContribution>,
    pub facts: Option<RequestFacts>,
    pub attempts: Vec<RequestAttempt>,
    pub available: bool,
    pub unavailable_reason: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct RequestFilter {
    pub status: Option<RequestState>,
    pub kind: Option<RequestKind>,
    pub requester: Option<String>,
    pub waiting: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FiledOperatorRequest {
    pub request: OperatorRequest,
    pub created: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestOutbox {
    pub id: String,
    pub request_id: String,
    pub kind: String,
    pub destination: Option<String>,
    pub payload: serde_json::Value,
    pub created_at: i64,
}

fn required_option<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}
