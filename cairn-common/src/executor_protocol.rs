//! Versioned wire contract between the runner and enrolled executors.
//!
//! Build-slot requests are immutable. Cancellation is deliberately represented
//! as a separate control message so dropping a runner-side waiter cannot mutate
//! or ambiguously replay an admitted request.

use serde::{Deserialize, Serialize};

pub const EXECUTOR_PROTOCOL_VERSION: u32 = 4;
pub const MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS: u64 = 60;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementConstraints {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub os: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arch: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_toolchains: Vec<String>,
}

impl PlacementConstraints {
    pub fn is_empty(&self) -> bool {
        self.executor_id.is_none()
            && self.device_id.is_none()
            && self.os.is_none()
            && self.arch.is_none()
            && self.required_toolchains.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorIdentity {
    pub device_id: String,
    pub executor_id: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorCapabilities {
    pub os: String,
    pub arch: String,
    pub logical_cores: usize,
    #[serde(default)]
    pub toolchains: Vec<String>,
    #[serde(default)]
    pub projects_served: Vec<String>,
    pub slot_capacity: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_budget_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_budget_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum GitObjectFormat {
    Sha1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryIdentity {
    pub project_id: String,
    pub repository_id: String,
    pub object_format: GitObjectFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RepositoryLocator {
    ColocatedPath {
        project_id: String,
        repository_id: String,
        absolute_path: String,
    },
    ManagedObjects {
        project_id: String,
        repository_id: String,
        object_format: GitObjectFormat,
    },
}

impl RepositoryLocator {
    pub fn identity(&self) -> RepositoryIdentity {
        match self {
            Self::ColocatedPath {
                project_id,
                repository_id,
                ..
            } => RepositoryIdentity {
                project_id: project_id.clone(),
                repository_id: repository_id.clone(),
                object_format: GitObjectFormat::Sha1,
            },
            Self::ManagedObjects {
                project_id,
                repository_id,
                object_format,
            } => RepositoryIdentity {
                project_id: project_id.clone(),
                repository_id: repository_id.clone(),
                object_format: *object_format,
            },
        }
    }

    pub fn project_id(&self) -> &str {
        match self {
            Self::ColocatedPath { project_id, .. } | Self::ManagedObjects { project_id, .. } => {
                project_id
            }
        }
    }

    pub fn repository_id(&self) -> &str {
        match self {
            Self::ColocatedPath { repository_id, .. }
            | Self::ManagedObjects { repository_id, .. } => repository_id,
        }
    }

    pub fn colocated_path(&self) -> Option<&str> {
        match self {
            Self::ColocatedPath { absolute_path, .. } => Some(absolute_path),
            Self::ManagedObjects { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedWarmRoot {
    pub repository: RepositoryIdentity,
    pub commit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorAdvertisement {
    pub identity: ExecutorIdentity,
    pub capabilities: ExecutorCapabilities,
    pub current_load: usize,
    #[serde(default)]
    pub warm_roots: Vec<VerifiedWarmRoot>,
    pub observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum ExecutorEnrollmentIdentity {
    Colocated,
    Grant {
        token: String,
        expected_runner_device_id: String,
    },
    Credential {
        credential: String,
        expected_runner_device_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum EnrollmentRejectionReason {
    Unenrolled,
    Expired,
    Revoked,
    IdentityMismatch,
    RunnerIdentityMismatch,
    MalformedAdvertisement,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum BuildSlotPriority {
    ReviewCheck,
    WriteCheck,
    AgentInteractive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MutationPolicy {
    PureVerdict,
    AllowDelta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildSlotRequest {
    pub request_id: String,
    pub attempt_id: String,
    pub project_id: String,
    pub repository: RepositoryLocator,
    pub base_commit: String,
    pub command: String,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    pub priority: BuildSlotPriority,
    pub deadline_unix_ms: u64,
    pub timeout_ms: u32,
    pub mutation_policy: MutationPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraints: Option<PlacementConstraints>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectChannelCredential {
    pub base_url: String,
    pub bearer_token: String,
    pub expires_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectTransferCoordinate {
    pub repository: RepositoryIdentity,
    pub request_id: String,
    pub attempt_id: String,
    pub executor_id: String,
    pub connection_generation: u64,
}

impl ObjectTransferCoordinate {
    pub fn matches_execution(
        &self,
        request: &BuildSlotRequest,
        executor_id: &str,
        connection_generation: u64,
    ) -> bool {
        self.repository == request.repository.identity()
            && self.request_id == request.request_id
            && self.attempt_id == request.attempt_id
            && self.executor_id == executor_id
            && self.connection_generation == connection_generation
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeltaUploadReceipt {
    pub receipt_id: String,
    pub coordinate: ObjectTransferCoordinate,
    pub base_commit: String,
    pub delta_commit: String,
    pub content_hash: String,
    pub pack_checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MutationDelta {
    pub base_commit: String,
    pub delta_commit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_receipt: Option<DeltaUploadReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildSlotExecutionMeta {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_device_id: String,
    #[serde(default)]
    pub executor_connection_generation: u64,
    pub slot_id: String,
    pub lease_epoch: u64,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ObjectInfrastructureStage {
    FetchInterrupted,
    IntegrityFailure,
    IncompleteClosure,
    InstallFailure,
    UploadFailure,
    ExpiredReceipt,
    StaleReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum BuildSlotUnavailableReason {
    NoCapacity,
    Deadline,
    Provisioning,
    Checkout,
    Spawn,
    Preparation,
    ExecutorUnavailable,
    NoMatchingExecutor,
    ObjectInfrastructure(ObjectInfrastructureStage),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum BuildSlotOutcome {
    Completed {
        request_id: String,
        attempt_id: String,
        exit_code: Option<i32>,
        output: String,
        timed_out: bool,
        metadata: BuildSlotExecutionMeta,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mutation_delta: Option<Box<MutationDelta>>,
    },
    Unavailable {
        reason: BuildSlotUnavailableReason,
        diagnostic: String,
    },
    FailedAfterExecution {
        request_id: String,
        attempt_id: String,
        diagnostic: String,
    },
    Cancelled {
        request_id: String,
        attempt_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PersistentSlotLifecycle {
    Provisioning,
    Idle,
    Queued,
    Running,
    Recovering,
    Retired,
    Quarantined,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ActiveBuildSlotRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    pub request_id: String,
    pub attempt_id: String,
    pub command: String,
    pub priority: BuildSlotPriority,
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    pub queued_at_unix_ms: u64,
    pub started_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SlotMaterializationKind {
    #[default]
    JujutsuWorkspace,
    DetachedGitWorktree,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PersistentBuildSlotState {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_display_name: Option<String>,
    pub project_id: String,
    pub slot_id: String,
    pub path: String,
    #[serde(default)]
    pub workspace_name: String,
    pub repository: String,
    #[serde(default)]
    pub materialization_kind: SlotMaterializationKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_common_dir: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub authority_path: String,
    pub lifecycle: PersistentSlotLifecycle,
    pub lease_epoch: u64,
    pub last_sealed_commit: Option<String>,
    pub last_used_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_affinity_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preparation_fingerprint: Option<String>,
    pub active_request: Option<ActiveBuildSlotRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueuedBuildSlotRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    pub request_id: String,
    pub attempt_id: String,
    pub project_id: String,
    pub command: String,
    pub priority: BuildSlotPriority,
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    pub queued_at_unix_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildSlotPoolSnapshot {
    pub slots: Vec<PersistentBuildSlotState>,
    pub queued_requests: Vec<QueuedBuildSlotRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorConfig {
    pub project_id: String,
    pub capacity: usize,
    pub acquisition_deadline_seconds: u64,
    pub default_timeout_seconds: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ProcessSandboxMode {
    Unconfined,
    Confined,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessBatch {
    pub sequential: bool,
    pub stop_on_error: bool,
    pub sandbox_mode: ProcessSandboxMode,
    pub items: Vec<ProcessBatchItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_context_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessBatchItem {
    pub header: String,
    pub stream_id: String,
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<String>,
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ExecutorMessage {
    Hello {
        protocol_version: u32,
        advertisement: ExecutorAdvertisement,
        enrollment: ExecutorEnrollmentIdentity,
    },
    Ready {
        protocol_version: u32,
        identity: ExecutorIdentity,
        runner_device_id: String,
        generation: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        issued_credential: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        object_channel: Option<ObjectChannelCredential>,
    },
    ObjectChannelUpdated {
        credential: ObjectChannelCredential,
        executor_id: String,
        generation: u64,
    },
    EnrollmentCredentialUpdated {
        credential: String,
        expires_at_unix_ms: u64,
        runner_device_id: String,
        executor_id: String,
        generation: u64,
    },
    EnrollmentCredentialAccepted {
        credential: String,
        runner_device_id: String,
        executor_id: String,
        generation: u64,
    },
    EnrollmentRejected {
        reason: EnrollmentRejectionReason,
        diagnostic: String,
    },
    Heartbeat {
        advertisement: ExecutorAdvertisement,
    },
    AdvertisementUpdated {
        advertisement: ExecutorAdvertisement,
    },
    ProtocolIncompatible {
        expected: u32,
        received: u32,
    },
    Configure {
        config: ExecutorConfig,
    },
    Submit {
        request: BuildSlotRequest,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        batch: Option<ProcessBatch>,
    },
    Result {
        request_id: String,
        attempt_id: String,
        outcome: BuildSlotOutcome,
    },
    Cancel {
        request_id: String,
        attempt_id: String,
    },
    CancelJob {
        job_id: String,
    },
    SnapshotRequest {
        correlation_id: String,
    },
    SnapshotResponse {
        correlation_id: String,
        snapshot: BuildSlotPoolSnapshot,
    },
    SnapshotUpdated {
        snapshot: BuildSlotPoolSnapshot,
    },
    Shutdown,
    CallbackRequest {
        correlation_id: String,
        callback: RunnerCallback,
    },
    CallbackResponse {
        correlation_id: String,
        result: RunnerCallbackResult,
    },
    InfrastructureDiagnostic {
        diagnostic: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "scope", content = "path", rename_all = "camelCase")]
pub enum SandboxDenial {
    Path(String),
    Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RunnerCallback {
    SandboxDenied {
        runner_context_id: String,
        command: String,
        cwd: String,
        denial: SandboxDenial,
    },
    CacheCheckpoint {
        runner_context_id: String,
        command: String,
        cwd: String,
        exit_code: Option<i32>,
    },
    ProcessEvent {
        runner_context_id: String,
        stream_id: String,
        payload: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum RunnerCallbackResult {
    Allowed,
    Rejected { diagnostic: String },
    Suspended,
    Completed,
    Failed { diagnostic: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_message_variant_round_trips() {
        let request = sample_request();
        let outcome = sample_outcome();
        let snapshot = BuildSlotPoolSnapshot::default();
        let advertisement = sample_advertisement();
        let messages = vec![
            ExecutorMessage::Hello {
                protocol_version: EXECUTOR_PROTOCOL_VERSION,
                advertisement: advertisement.clone(),
                enrollment: ExecutorEnrollmentIdentity::Colocated,
            },
            ExecutorMessage::Ready {
                protocol_version: EXECUTOR_PROTOCOL_VERSION,
                identity: advertisement.identity.clone(),
                runner_device_id: "runner".into(),
                generation: 2,
                issued_credential: None,
                object_channel: None,
            },
            ExecutorMessage::ObjectChannelUpdated {
                credential: ObjectChannelCredential {
                    base_url: "https://runner.example/api/executor/objects".into(),
                    bearer_token: "rotated".into(),
                    expires_at_unix_ms: 10,
                },
                executor_id: "e".into(),
                generation: 2,
            },
            ExecutorMessage::EnrollmentCredentialUpdated {
                credential: "replacement".into(),
                expires_at_unix_ms: 10,
                runner_device_id: "runner".into(),
                executor_id: "e".into(),
                generation: 2,
            },
            ExecutorMessage::EnrollmentCredentialAccepted {
                credential: "replacement".into(),
                runner_device_id: "runner".into(),
                executor_id: "e".into(),
                generation: 2,
            },
            ExecutorMessage::EnrollmentRejected {
                reason: EnrollmentRejectionReason::Revoked,
                diagnostic: "revoked".into(),
            },
            ExecutorMessage::Heartbeat {
                advertisement: advertisement.clone(),
            },
            ExecutorMessage::AdvertisementUpdated { advertisement },
            ExecutorMessage::ProtocolIncompatible {
                expected: 1,
                received: 2,
            },
            ExecutorMessage::Configure {
                config: ExecutorConfig {
                    project_id: "p".into(),
                    capacity: 2,
                    acquisition_deadline_seconds: 20,
                    default_timeout_seconds: 30,
                },
            },
            ExecutorMessage::Submit {
                request: request.clone(),
                batch: None,
            },
            ExecutorMessage::Result {
                request_id: "r".into(),
                attempt_id: "a".into(),
                outcome,
            },
            ExecutorMessage::Cancel {
                request_id: "r".into(),
                attempt_id: "a".into(),
            },
            ExecutorMessage::CancelJob { job_id: "j".into() },
            ExecutorMessage::SnapshotRequest {
                correlation_id: "c".into(),
            },
            ExecutorMessage::SnapshotResponse {
                correlation_id: "c".into(),
                snapshot: snapshot.clone(),
            },
            ExecutorMessage::SnapshotUpdated { snapshot },
            ExecutorMessage::Shutdown,
            ExecutorMessage::CallbackRequest {
                correlation_id: "denial".into(),
                callback: RunnerCallback::SandboxDenied {
                    runner_context_id: "ctx".into(),
                    command: "touch /outside".into(),
                    cwd: "/tmp/worktree".into(),
                    denial: SandboxDenial::Path("/outside".into()),
                },
            },
            ExecutorMessage::CallbackRequest {
                correlation_id: "c".into(),
                callback: RunnerCallback::CacheCheckpoint {
                    runner_context_id: "ctx".into(),
                    command: "echo ok".into(),
                    cwd: "/tmp/worktree".into(),
                    exit_code: Some(0),
                },
            },
            ExecutorMessage::CallbackResponse {
                correlation_id: "c".into(),
                result: RunnerCallbackResult::Completed,
            },
            ExecutorMessage::InfrastructureDiagnostic {
                diagnostic: "lost".into(),
            },
        ];
        for message in messages {
            let json = serde_json::to_string(&message).unwrap();
            assert_eq!(
                serde_json::from_str::<ExecutorMessage>(&json).unwrap(),
                message
            );
        }
    }

    #[test]
    fn request_and_delta_round_trip_and_cancellation_is_separate() {
        let request = sample_request();
        let json = serde_json::to_value(&request).unwrap();
        assert!(json.get("cancelled").is_none());
        assert!(json.get("cancellation").is_none());
        assert_eq!(
            serde_json::from_value::<BuildSlotRequest>(json).unwrap(),
            request
        );
        let outcome = sample_outcome();
        let json = serde_json::to_value(&outcome).unwrap();
        assert_eq!(
            json.get("mutation_delta"),
            Some(&serde_json::json!({
                "baseCommit": "b",
                "deltaCommit": "d"
            }))
        );
        assert_eq!(
            serde_json::from_value::<BuildSlotOutcome>(json).unwrap(),
            outcome
        );
    }

    fn sample_request() -> BuildSlotRequest {
        BuildSlotRequest {
            request_id: "r".into(),
            attempt_id: "a".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repo".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "b".into(),
            command: "true".into(),
            cwd: String::new(),
            env: Vec::new(),
            priority: BuildSlotPriority::ReviewCheck,
            deadline_unix_ms: 1,
            timeout_ms: 2,
            mutation_policy: MutationPolicy::AllowDelta,
            requesting_job_id: Some("j".into()),
            affinity_key: Some("affinity".into()),
            constraints: Some(PlacementConstraints {
                os: Some("linux".into()),
                required_toolchains: vec!["rust".into()],
                ..PlacementConstraints::default()
            }),
        }
    }

    fn sample_advertisement() -> ExecutorAdvertisement {
        ExecutorAdvertisement {
            identity: ExecutorIdentity {
                device_id: "d".into(),
                executor_id: "e".into(),
                display_name: "Executor".into(),
            },
            capabilities: ExecutorCapabilities {
                os: "linux".into(),
                arch: "x86_64".into(),
                logical_cores: 8,
                toolchains: vec!["rust".into()],
                projects_served: vec!["p".into()],
                slot_capacity: 2,
                disk_budget_bytes: Some(10),
                memory_budget_bytes: None,
            },
            current_load: 1,
            warm_roots: vec![VerifiedWarmRoot {
                repository: RepositoryIdentity {
                    project_id: "p".into(),
                    repository_id: "repo".into(),
                    object_format: GitObjectFormat::Sha1,
                },
                commit: "b".into(),
            }],
            observed_at_unix_ms: 4,
        }
    }

    #[test]
    fn transfer_coordinate_is_bound_to_the_exact_execution_and_generation() {
        let request = sample_request();
        let coordinate = ObjectTransferCoordinate {
            repository: request.repository.identity(),
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            executor_id: "executor".into(),
            connection_generation: 7,
        };
        assert!(coordinate.matches_execution(&request, "executor", 7));

        let mut another_attempt = request.clone();
        another_attempt.attempt_id = "another-attempt".into();
        assert!(!coordinate.matches_execution(&another_attempt, "executor", 7));
        assert!(!coordinate.matches_execution(&request, "another-executor", 7));
        assert!(!coordinate.matches_execution(&request, "executor", 8));

        let mut another_repository = request;
        another_repository.repository = RepositoryLocator::ManagedObjects {
            project_id: "p".into(),
            repository_id: "another-repository".into(),
            object_format: GitObjectFormat::Sha1,
        };
        assert!(!coordinate.matches_execution(&another_repository, "executor", 7));
    }

    #[test]
    fn omitted_constraints_remain_backward_compatible() {
        let mut value = serde_json::to_value(sample_request()).unwrap();
        value.as_object_mut().unwrap().remove("constraints");
        assert_eq!(
            serde_json::from_value::<BuildSlotRequest>(value)
                .unwrap()
                .constraints,
            None
        );
    }

    fn sample_outcome() -> BuildSlotOutcome {
        BuildSlotOutcome::Completed {
            request_id: "r".into(),
            attempt_id: "a".into(),
            exit_code: Some(1),
            output: "failed".into(),
            timed_out: false,
            metadata: BuildSlotExecutionMeta {
                executor_id: "e".into(),
                executor_device_id: "d".into(),
                executor_connection_generation: 1,
                slot_id: "s".into(),
                lease_epoch: 2,
                started_at_unix_ms: 3,
                finished_at_unix_ms: 4,
            },
            mutation_delta: Some(Box::new(MutationDelta {
                base_commit: "b".into(),
                delta_commit: "d".into(),
                upload_receipt: None,
            })),
        }
    }
}
