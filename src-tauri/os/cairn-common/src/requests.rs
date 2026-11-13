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
    FencedCommand,
}
impl RequestKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rebuild => "rebuild",
            Self::BumpVersion => "bump_version",
            Self::LiftDrain => "lift_drain",
            Self::FencedCommand => "fenced_command",
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
/// The assurance an acceptance must carry, set by the kind's definition in
/// code and never by the asker. Served on each request so the desktop knows
/// which ceremony to run before it forwards a decision; the runner checks the
/// acceptance's appearance against it regardless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AcceptancePolicy {
    /// The desktop operator credential, or an owner/admin JWT.
    #[default]
    OperatorCredential,
    /// The desktop operator credential plus a local device-owner check (Touch
    /// ID or the Mac password) performed for this one acceptance.
    LocalUserVerification,
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
    /// A string for the kinds whose target is one address or version, and an
    /// object for a kind that declares a structured target
    /// ([`FencedCommandTarget`]). The kind parses it; nothing else reads it.
    #[serde(default)]
    pub target: Option<serde_json::Value>,
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
/// What an agent supplies when it asks the operator to run one command outside
/// its path fence. The commit, the executor's stable identity and the resolved
/// working directory are Cairn's to fix at filing, never the agent's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FencedCommandTarget {
    /// The executor's public name, as `cairn://executors` lists it.
    pub executor: String,
    /// The exact bytes run under `/bin/sh -c`. Shown to the operator in full.
    pub command: String,
    /// Relative to the repository root; must stay inside the checkout.
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// The act a `fenced_command` request names, frozen at filing. It is stored as
/// the request's canonical target and is the coalescing identity: two filings
/// join only when every field is equal, the requesting node included, because
/// the act brings that node's `$TMPDIR` current and writes it back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencedCommandAct {
    pub executor_id: String,
    pub device_id: String,
    pub project_id: String,
    /// The branch whose head `commit` was when the request was filed.
    pub branch: String,
    pub commit: String,
    /// Repository-relative and normalized; empty for the repository root.
    pub cwd: String,
    pub command: String,
    pub timeout_ms: u64,
    /// The requesting node's home URI: whose scratch the act carries.
    pub node: String,
}

/// The executor's credential floor as measured when the operator accepted.
/// Containment is measured at executor start and replayed, so the act is bound
/// to this exact measurement and connection: any of these moving before the
/// claim is a fresh acceptance, never a silent retarget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutorFloorWitness {
    pub connection_generation: u64,
    pub executor_build_id: String,
    pub boot_generation: String,
    /// The `OperatorCredentialRead` observation's invalidation fingerprint.
    pub floor_fingerprint: String,
    pub floor_measured_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencedCommandAcceptance {
    pub act: FencedCommandAct,
    pub witness: ExecutorFloorWitness,
    /// The head of the requesting node's `$TMPDIR` scratch when the card's
    /// facts were read, or `None` when that node has none or it is empty. The
    /// act brings the scratch current and runs whatever it holds, which the
    /// card cannot show, so a change after acceptance refuses before anything
    /// runs.
    #[serde(default)]
    pub scratch_head: Option<String>,
}

/// The executor facts a `fenced_command` report carries. The executor is a
/// placement fact authenticated by enrolment, never a principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FencedCommandExecutorReport {
    pub executor_id: String,
    pub device_id: String,
    pub name: String,
    pub executor_build_id: String,
    pub connection_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

/// What a `fenced_command` attempt reports, and what a waiting filer receives.
/// A command that ran and exited non-zero is still `ran: true`: the act was
/// performed and the command failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FencedCommandReport {
    pub ran: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Scrubbed and capped like a run result.
    pub output: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<FencedCommandExecutorReport>,
    pub cell_request_id: String,
    pub cell_attempt_id: String,
    /// Why the act did not run, or what the scratch write-back said.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
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
    /// Present exactly for `fenced_command`. Omitted when absent so every
    /// earlier kind's frozen acceptance encodes byte-for-byte as it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fenced_command: Option<FencedCommandAcceptance>,
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
    /// What the kind's definition requires of an acceptance.
    #[serde(default)]
    pub acceptance_policy: AcceptancePolicy,
    /// The newest live headline the asking thread's project holds about this
    /// request, its origin, or the origin's issue. Supplied by the runner when
    /// it serves the request; absent when no thread wrote one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headline: Option<crate::headlines::HeadlineAbout>,
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
