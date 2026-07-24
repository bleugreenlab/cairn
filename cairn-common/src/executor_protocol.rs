//! Versioned wire contract between the runner and enrolled executors.
//!
//! Build-cell requests are immutable. Cancellation is deliberately represented
//! as a separate control message so dropping a runner-side waiter cannot mutate
//! or ambiguously replay an admitted request.

use serde::{Deserialize, Serialize};
use std::future::Future;

/// Receives and accepts one executor Ready message before starting work that is
/// explicitly outside the readiness boundary.
pub async fn accept_ready_then<T, R, E, Receive, Accept, PostReady>(
    receive: Receive,
    accept: Accept,
    post_ready: PostReady,
) -> Result<R, E>
where
    Receive: Future<Output = Result<T, E>>,
    Accept: FnOnce(T) -> Result<R, E>,
    PostReady: FnOnce(),
{
    let message = receive.await?;
    let accepted = accept(message)?;
    post_ready();
    Ok(accepted)
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum LifetimeProcessCwdRoot {
    #[default]
    Checkout,
    LeaseScratch,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum LifetimeProcessStream {
    Pty,
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeSandboxPolicy {
    pub worktree: String,
    #[serde(default)]
    pub writable_extra: Vec<String>,
    #[serde(default)]
    pub deny_read: Vec<String>,
    #[serde(default)]
    pub writable_regex: Vec<String>,
    pub worktree_writable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PromotedTerminalProcess {
    pub fence: LifetimeLeaseFence,
    pub slug: String,
    pub uri: String,
    pub wake_subscribed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogPackDescriptor {
    pub catalog_id: String,
    pub content_hash: String,
    pub byte_count: u64,
    pub pack_checksum: String,
    pub base_commit: Option<String>,
    pub tip_commit: String,
    pub grant: CloudObjectGrant,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogFetchResponse {
    pub packs: Vec<CatalogPackDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MutationDeltaUploadRequest {
    pub coordinate: ObjectTransferCoordinate,
    pub base_commit: String,
    pub delta_commit: String,
    pub content_hash: String,
    pub byte_count: u64,
    pub pack_checksum: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellExecutionStage {
    #[serde(rename = "materializing")]
    CheckingOut,
    PreparingSetup,
    Running,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellCommandClass {
    CargoCheck,
    CargoTest,
    CargoClippy,
    Vitest,
    Typecheck,
    Build,
    #[default]
    Other,
}

impl CellCommandClass {
    pub fn classify(command: &str) -> Self {
        let command = command.to_ascii_lowercase();
        if command.contains("cargo clippy") || command.contains("check:rust") {
            Self::CargoClippy
        } else if command.contains("cargo test") || command.contains("test:rust") {
            Self::CargoTest
        } else if command.contains("cargo check") {
            Self::CargoCheck
        } else if command.contains("vitest") || command.contains("test:frontend") {
            Self::Vitest
        } else if command.contains("tsc ") || command.contains("typecheck") {
            Self::Typecheck
        } else if command.contains("vite build") || command.contains("bun run build") {
            Self::Build
        } else {
            Self::Other
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellOwnerRef {
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue_number: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_seq: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_kind: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LearnedResourceEstimate {
    pub sample_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_peak_rss_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_disk_growth_bytes: Option<u64>,
}

pub const EXECUTOR_PROTOCOL_VERSION: u32 = 18;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorDistributionInfo {
    pub protocol_version: u32,
    pub target: String,
    pub distribution_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorArtifact {
    pub target: String,
    pub url: String,
    pub sha256: String,
    pub size: u64,
    pub distribution_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorDistributionManifest {
    pub protocol_version: u32,
    pub artifacts: Vec<ExecutorArtifact>,
}
pub const MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS: u64 = 60;
pub const EXECUTOR_PROGRESS_FRESHNESS_MS: u64 = 75_000;

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
    ExistingCheckout {
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
            }
            | Self::ExistingCheckout {
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
            Self::ColocatedPath { project_id, .. }
            | Self::ExistingCheckout { project_id, .. }
            | Self::ManagedObjects { project_id, .. } => project_id,
        }
    }

    pub fn repository_id(&self) -> &str {
        match self {
            Self::ColocatedPath { repository_id, .. }
            | Self::ExistingCheckout { repository_id, .. }
            | Self::ManagedObjects { repository_id, .. } => repository_id,
        }
    }

    pub fn colocated_path(&self) -> Option<&str> {
        match self {
            Self::ColocatedPath { absolute_path, .. }
            | Self::ExistingCheckout { absolute_path, .. } => Some(absolute_path),
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase")]
pub enum CellPriority {
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

pub const COMMAND_RESOURCE_IDENTITY_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommandResourceIdentity {
    pub version: u32,
    pub key: String,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ResourceReservationSource {
    Learned,
    Declared,
    #[default]
    ZeroKnowledgePrior,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceReservation {
    pub memory_bytes: u64,
    pub disk_growth_bytes: u64,
    #[serde(default = "default_concurrency_units")]
    pub concurrency_units: u32,
    #[serde(default)]
    pub source: ResourceReservationSource,
}

const fn default_concurrency_units() -> u32 {
    1
}

impl Default for ResourceReservation {
    fn default() -> Self {
        Self {
            memory_bytes: 0,
            disk_growth_bytes: 0,
            concurrency_units: default_concurrency_units(),
            source: ResourceReservationSource::ZeroKnowledgePrior,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellRequest {
    pub request_id: String,
    pub attempt_id: String,
    pub project_id: String,
    pub repository: RepositoryLocator,
    pub base_commit: String,
    pub command: String,
    #[serde(default)]
    pub command_class: CellCommandClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CellOwnerRef>,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    pub priority: CellPriority,
    pub deadline_unix_ms: u64,
    pub timeout_ms: u32,
    pub mutation_policy: MutationPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub constraints: Option<PlacementConstraints>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_resource_identity: Option<CommandResourceIdentity>,
    #[serde(default)]
    pub resource_reservation: ResourceReservation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectChannelCredential {
    pub base_url: String,
    pub bearer_token: String,
    pub expires_at_unix_ms: u64,
}

pub const CLOUD_OBJECT_GRANT_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CloudObjectOperation {
    Get,
    Put,
}

/// A transient exact-object bearer grant. Callers must never persist or log `url`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CloudObjectGrant {
    pub version: u16,
    pub content_hash: String,
    pub operation: CloudObjectOperation,
    pub url: String,
    pub method: String,
    pub expires_at: String,
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CloudObjectGrantRequest {
    pub coordinate: ObjectTransferCoordinate,
    pub content_hash: String,
    pub operation: CloudObjectOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_count: Option<u64>,
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
        request: &CellRequest,
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

/// Tracked repository content written by a pure-verdict command and discarded
/// before the command result is published.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TrackedModificationEvidence {
    pub paths: Vec<String>,
    pub files_changed: usize,
    pub lines_added: u64,
    pub lines_deleted: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellExecutionMeta {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_device_id: String,
    #[serde(default)]
    pub executor_connection_generation: u64,
    #[serde(rename = "slotId")]
    pub cell_id: String,
    pub lease_epoch: u64,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_rss_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_physical_footprint_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_delta_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurement_quality: Option<ExecutionMeasurementQuality>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionMeasurementQuality {
    pub duration: MeasurementQuality,
    pub memory: MeasurementQuality,
    pub disk: MeasurementQuality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_platform: Option<String>,
    pub disk_boundary: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum MeasurementQuality {
    Authoritative,
    Sampled,
    Approximate,
    Unavailable,
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
pub enum AdmissionRejectionReason {
    QueueFull,
    RequestTooLarge,
    StorageCleanupFailed,
    Draining,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StorageFailureStage {
    PreAdmissionPressure,
    #[serde(rename = "provisioningMaterialization")]
    ProvisioningCheckout,
    StatePersistence,
    CommandSeal,
    DeltaUpload,
    Recovery,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StorageFailureKind {
    NoSpace,
    QuotaExceeded,
    CleanupFailed,
    AccountingUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum HostPressureCondition {
    MemoryAvailable {
        available_bytes: u64,
        floor_bytes: u64,
    },
    DiskFree {
        free_bytes: u64,
        floor_bytes: u64,
    },
    LifetimeOccupancy {
        lease_count: usize,
        reservation: ResourceReservation,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HostPressureEvidence {
    pub conditions: Vec<HostPressureCondition>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ExecutorSubstrateState {
    SupervisorSpawning,
    SupervisorRespawning,
    ProtocolAttaching,
    InitialStorageSweep,
    StorageAccounting,
    DispatchPreparing,
    SlotAdoption,
    CapacityBusy,
    ExecutionRunning,
    ConnectedStalled,
    Draining,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorSubstrateEvidence {
    pub state: ExecutorSubstrateState,
    pub since_unix_ms: u64,
    pub last_progress_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_position: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "activeSlotCount")]
    pub active_cell_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_running_started_at_unix_ms: Option<u64>,
}

impl ExecutorSubstrateEvidence {
    pub fn without_queue(
        state: ExecutorSubstrateState,
        since_unix_ms: u64,
        last_progress_unix_ms: u64,
    ) -> Self {
        Self {
            state,
            since_unix_ms,
            last_progress_unix_ms,
            diagnostic: None,
            queue_depth: None,
            queue_position: None,
            active_cell_count: None,
            oldest_running_started_at_unix_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellUnavailableReason {
    Deadline {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_pressure: Option<HostPressureEvidence>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        substrate: Option<ExecutorSubstrateEvidence>,
    },
    Provisioning,
    Checkout,
    Spawn,
    Preparation,
    ExecutorUnavailable,
    NoMatchingExecutor,
    AdmissionRejected {
        reason: AdmissionRejectionReason,
    },
    ObjectInfrastructure(ObjectInfrastructureStage),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
// This wire enum intentionally keeps completed metadata inline for protocol
// compatibility; additive optional measurements make that variant larger.
#[allow(clippy::large_enum_variant)]
pub enum CellOutcome {
    Completed {
        request_id: String,
        attempt_id: String,
        exit_code: Option<i32>,
        output: String,
        timed_out: bool,
        metadata: CellExecutionMeta,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mutation_delta: Option<Box<MutationDelta>>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        sandbox_denials: Vec<SandboxDenialEvidence>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tracked_modifications: Option<TrackedModificationEvidence>,
    },
    Unavailable {
        reason: CellUnavailableReason,
        diagnostic: String,
    },
    FailedAfterExecution {
        request_id: String,
        attempt_id: String,
        diagnostic: String,
    },
    StorageFailure {
        request_id: String,
        attempt_id: String,
        stage: StorageFailureStage,
        kind: StorageFailureKind,
        diagnostic: String,
        #[serde(default)]
        slot_retired: bool,
    },
    Cancelled {
        request_id: String,
        attempt_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PersistentCellLifecycle {
    Provisioning,
    Idle,
    Queued,
    Running,
    AwaitingReclaim,
    Releasing,
    Recovering,
    Retired,
    Quarantined,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ActiveCellRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    pub request_id: String,
    pub attempt_id: String,
    pub command: String,
    #[serde(default)]
    pub command_class: CellCommandClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CellOwnerRef>,
    pub priority: CellPriority,
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    pub queued_at_unix_ms: u64,
    pub started_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<CellExecutionStage>,
    #[serde(default)]
    pub resource_reservation: ResourceReservation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
    #[serde(default = "default_subscriber_count")]
    pub subscriber_count: usize,
}

fn default_subscriber_count() -> usize {
    1
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum LifetimeLeaseOwnerKind {
    DevInstance,
    Terminal,
    Repl,
    Workflow,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeLeaseOwner {
    pub kind: LifetimeLeaseOwnerKind,
    pub owner_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeOwnerDeathPolicy {
    pub heartbeat_timeout_ms: u64,
    pub reclaim_grace_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeLeaseDeclaration {
    pub lease_id: String,
    pub owner: LifetimeLeaseOwner,
    /// Human-facing owner identity. This is descriptive only: lease authority
    /// remains the stable `(owner, name)` pair above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_ref: Option<CellOwnerRef>,
    pub name: String,
    pub purpose: String,
    pub repository: RepositoryLocator,
    pub initial_base_commit: String,
    pub resource_reservation: ResourceReservation,
    pub owner_death_policy: LifetimeOwnerDeathPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeProcessSpec {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub cwd_root: LifetimeProcessCwdRoot,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    #[serde(default)]
    pub sandbox_mode: ProcessSandboxMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_policy: Option<LifetimeSandboxPolicy>,
    /// Files supplied by the runner that are not part of the repository checkout.
    /// The executor validates and materializes these beneath its lease-owned scratch
    /// directory and exposes that root through `CAIRN_RUNTIME_ASSETS`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_assets: Vec<LifetimeRuntimeAsset>,
    #[serde(default)]
    pub io: LifetimeProcessIoMode,
}

pub const MAX_LIFETIME_RUNTIME_ASSET_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_LIFETIME_RUNTIME_ASSETS_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_LIFETIME_RUNTIME_ASSETS: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeRuntimeAsset {
    pub path: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum LifetimeProcessIoMode {
    #[default]
    Supervised,
    /// Transport stdin, stdout, and stderr over the fenced lifetime-process
    /// protocol. The executor keeps stdin open until process teardown.
    Pipe,
    Pty {
        size: LifetimePtySize,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimePtySize {
    pub rows: u16,
    pub cols: u16,
    #[serde(default)]
    pub pixel_width: u16,
    #[serde(default)]
    pub pixel_height: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "camelCase")]
pub enum LifetimeProcessEventKind {
    Output {
        sequence: u64,
        stream: LifetimeProcessStream,
        data: Vec<u8>,
    },
    State {
        status: LifetimeProcessStatus,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeProcessEvent {
    pub lease_id: String,
    pub incarnation_id: String,
    pub lease_epoch: u64,
    pub process_key: String,
    pub process_generation: u64,
    pub event: LifetimeProcessEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum LifetimeProcessStatus {
    Stopped,
    Starting,
    Running {
        started_at_unix_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        process_group_id: Option<u32>,
    },
    Exited {
        finished_at_unix_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        restartable: bool,
        executor_lost: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeProcessState {
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<LifetimeProcessSpec>,
    pub status: LifetimeProcessStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum LifetimeLeasePhase {
    Active,
    AwaitingReclaim,
    Releasing,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LifetimeLeaseEventKind {
    Acquired,
    Renewed,
    AwaitingReclaim,
    Reclaimed,
    ProcessStarting {
        process_key: String,
        generation: u64,
    },
    ProcessRunning {
        process_key: String,
        generation: u64,
    },
    ProcessExited {
        process_key: String,
        generation: u64,
        restartable: bool,
        executor_lost: bool,
    },
    #[serde(rename = "materializationRefreshed")]
    CheckoutRefreshed {
        base_commit: String,
    },
    Releasing,
    Released,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeLeaseEvent {
    pub revision: u64,
    pub occurred_at_unix_ms: u64,
    pub event: LifetimeLeaseEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeLeaseState {
    pub declaration: LifetimeLeaseDeclaration,
    #[serde(default)]
    pub incarnation_id: String,
    pub current_base_commit: String,
    pub phase: LifetimeLeasePhase,
    pub last_heartbeat_unix_ms: u64,
    pub reclaim_deadline_unix_ms: u64,
    pub state_revision: u64,
    /// Promoted command groups retain their command reservation until the batch
    /// has settled; declared lifetime leases begin settled.
    #[serde(default = "default_true")]
    pub command_settled: bool,
    #[serde(default)]
    pub processes: std::collections::BTreeMap<String, LifetimeProcessState>,
    #[serde(default)]
    pub events: Vec<LifetimeLeaseEvent>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellOccupant {
    Command(ActiveCellRequest),
    Lifetime(LifetimeLeaseState),
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "state", rename_all = "camelCase")]
enum TaggedCellOccupant {
    Command(ActiveCellRequest),
    Lifetime(LifetimeLeaseState),
}

impl Serialize for CellOccupant {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Command(state) => TaggedCellOccupant::Command(state.clone()),
            Self::Lifetime(state) => TaggedCellOccupant::Lifetime(state.clone()),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CellOccupant {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.get("kind").is_some() {
            return serde_json::from_value::<TaggedCellOccupant>(value)
                .map(|occupant| match occupant {
                    TaggedCellOccupant::Command(state) => Self::Command(state),
                    TaggedCellOccupant::Lifetime(state) => Self::Lifetime(state),
                })
                .map_err(serde::de::Error::custom);
        }
        serde_json::from_value::<ActiveCellRequest>(value)
            .map(Self::Command)
            .map_err(serde::de::Error::custom)
    }
}

impl CellOccupant {
    pub fn command(&self) -> Option<&ActiveCellRequest> {
        match self {
            Self::Command(command) => Some(command),
            Self::Lifetime(_) => None,
        }
    }

    pub fn lifetime(&self) -> Option<&LifetimeLeaseState> {
        match self {
            Self::Command(_) => None,
            Self::Lifetime(lease) => Some(lease),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellCheckoutKind {
    #[default]
    JujutsuWorkspace,
    DetachedGitWorktree,
    /// A checkout owned outside the build fabric. The executor may host a
    /// lifetime process in it, but must never reset, clean, or delete it.
    ExistingCheckout,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PersistentCellState {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_display_name: Option<String>,
    pub project_id: String,
    #[serde(rename = "slotId")]
    pub cell_id: String,
    pub path: String,
    #[serde(default)]
    pub workspace_name: String,
    pub repository: String,
    #[serde(default)]
    #[serde(rename = "materializationKind")]
    pub checkout_kind: CellCheckoutKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_common_dir: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub authority_path: String,
    pub lifecycle: PersistentCellLifecycle,
    pub lease_epoch: u64,
    pub last_sealed_commit: Option<String>,
    pub last_used_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_affinity_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preparation_fingerprint: Option<String>,
    #[serde(
        default,
        rename = "occupant",
        // Added 2026-07-13. Keep until an explicit migration rewrites both
        // authority identity.json and state.json files: executor startup can
        // adopt an old lifetime cell while rewriting only state.json.
        alias = "activeRequest",
        skip_serializing_if = "Option::is_none"
    )]
    pub occupant: Option<CellOccupant>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellOccupantKind {
    #[default]
    Command,
    Lifetime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueuedCellRequest {
    #[serde(default)]
    pub occupant_kind: CellOccupantKind,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    pub request_id: String,
    pub attempt_id: String,
    pub project_id: String,
    pub command: String,
    #[serde(default)]
    pub command_class: CellCommandClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CellOwnerRef>,
    pub priority: CellPriority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_priority: Option<CellPriority>,
    pub requesting_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity_key: Option<String>,
    pub queued_at_unix_ms: u64,
    #[serde(default)]
    pub resource_reservation: ResourceReservation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
    #[serde(default = "default_subscriber_count")]
    pub subscriber_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub substrate_hold: Option<ExecutorSubstrateEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeOccupancyEvidence {
    pub lease_count: usize,
    pub reservation: ResourceReservation,
}

impl Default for LifetimeOccupancyEvidence {
    fn default() -> Self {
        Self {
            lease_count: 0,
            reservation: ResourceReservation {
                concurrency_units: 0,
                ..ResourceReservation::default()
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellOutputEvent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    #[serde(rename = "slotId")]
    pub cell_id: String,
    pub request_id: String,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stream_id: String,
    pub chunk: String,
    pub emitted_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutingCellRequest {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[serde(rename = "slotId")]
    pub cell_id: String,
    pub request_id: String,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CellOwnerRef>,
    #[serde(default)]
    pub command_class: CellCommandClass,
    #[serde(default)]
    pub command: String,
    pub started_at_unix_ms: u64,
    pub process_ids: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<CellPriority>,
    #[serde(default = "default_subscriber_count")]
    pub subscriber_count: usize,
    #[serde(default)]
    pub resource_reservation: ResourceReservation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum CellCompletionVerdict {
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellCompletion {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub executor_id: String,
    pub request_id: String,
    pub attempt_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CellOwnerRef>,
    pub command_class: CellCommandClass,
    pub command: String,
    pub priority: CellPriority,
    pub queued_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<u64>,
    pub finished_at_unix_ms: u64,
    pub duration_ms: u64,
    pub verdict: CellCompletionVerdict,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_reservation: Option<ResourceReservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_estimate: Option<LearnedResourceEstimate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actuals: Option<CellExecutionMeta>,
    #[serde(default)]
    pub cached: bool,
    #[serde(default = "default_subscriber_count")]
    pub subscriber_count: usize,
    pub served_at_unix_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FleetSnapshot {
    #[serde(rename = "slots")]
    pub cells: Vec<PersistentCellState>,
    pub queued_requests: Vec<QueuedCellRequest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub executing_requests: Vec<ExecutingCellRequest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_completions: Vec<CellCompletion>,
    #[serde(
        default,
        rename = "lifetimeOccupancy",
        skip_serializing_if = "Option::is_none"
    )]
    pub lifetime_cell_occupancy: Option<LifetimeOccupancyEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub substrate_state: Option<ExecutorSubstrateEvidence>,
}

pub const SUBSTRATE_HEALTH_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum SubstrateHealthStatus {
    Healthy,
    Degraded,
    Blocked,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ExecutorHealthStatus {
    Online,
    Stale,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DiskHealthStatus {
    Ok,
    Pressured,
    Full,
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StorageSweepStatus {
    #[default]
    NotStarted,
    InFlight,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum SubstrateHealthReason {
    NoExecutors,
    StaleExecutor {
        executor_id: String,
    },
    HostPressure {
        executor_id: String,
    },
    DiskPressured {
        executor_id: String,
    },
    DiskFull {
        executor_id: String,
    },
    AdmissionSaturated {
        executor_id: String,
    },
    StorageCleanupFailed {
        executor_id: String,
    },
    DiskAccountingPartial {
        executor_id: String,
        skipped_entries: usize,
    },
    ReadingUnavailable {
        section: String,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BoundedDurationSummary {
    pub sample_count: u64,
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub max_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueueClassHealth {
    pub priority: CellPriority,
    pub depth: usize,
    pub oldest_age_ms: Option<u64>,
    pub waits: BoundedDurationSummary,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionHealth {
    pub concurrency_capacity: Option<u32>,
    pub memory_capacity_bytes: Option<u64>,
    pub disk_growth_capacity_bytes: Option<u64>,
    pub active_reservation: ResourceReservation,
    pub queued_reservation_bytes: u64,
    pub accepted_count: u64,
    pub rejected_count: u64,
    pub timed_out_count: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HostHealth {
    pub pressure: Option<HostPressureEvidence>,
    pub available_memory_bytes: Option<u64>,
    pub process_rss_bytes: Option<u64>,
    pub process_physical_footprint_bytes: Option<u64>,
    pub cpu_load_one: Option<f64>,
    pub logical_cores: Option<usize>,
    pub tokio_worker_count: Option<usize>,
    pub tokio_alive_tasks: Option<usize>,
    pub tokio_global_queue_depth: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiskCategoryAccounting {
    pub managed_objects_bytes: u64,
    #[serde(rename = "liveSlotsBytes")]
    pub live_cells_bytes: u64,
    pub warm_caches_bytes: u64,
    pub quarantines_bytes: u64,
    pub temporary_other_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiskHealth {
    pub total_bytes: Option<u64>,
    pub free_bytes: Option<u64>,
    pub budget_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub categories: Option<DiskCategoryAccounting>,
    #[serde(default)]
    pub accounting_measured_at_unix_ms: Option<u64>,
    #[serde(default)]
    pub accounting_skipped_entries: Option<usize>,
    pub status: DiskHealthStatus,
    pub sweep_status: StorageSweepStatus,
    pub sweep_generation: u64,
    pub cleanup_blocked: bool,
    pub cleanup_last_error: Option<String>,
    pub cleanup_failing_path: Option<String>,
    pub cleanup_skipped_entries: Option<usize>,
}

impl Default for DiskHealth {
    fn default() -> Self {
        Self {
            total_bytes: None,
            free_bytes: None,
            budget_bytes: None,
            used_bytes: None,
            categories: None,
            accounting_measured_at_unix_ms: None,
            accounting_skipped_entries: None,
            status: DiskHealthStatus::Unknown,
            sweep_status: StorageSweepStatus::NotStarted,
            sweep_generation: 0,
            cleanup_blocked: false,
            cleanup_last_error: None,
            cleanup_failing_path: None,
            cleanup_skipped_entries: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorRuntimePolicy {
    pub memory_budget_bytes: Option<u64>,
    pub disk_growth_budget_bytes: Option<u64>,
    pub free_disk_watermark_bytes: u64,
    pub concurrency_units: u32,
    pub maximum_queue_depth: usize,
    #[serde(default = "default_maximum_idle_cells_per_project")]
    pub maximum_idle_cells_per_project: usize,
}

const fn default_maximum_idle_cells_per_project() -> usize {
    1
}

impl Default for ExecutorRuntimePolicy {
    fn default() -> Self {
        Self {
            memory_budget_bytes: None,
            disk_growth_budget_bytes: None,
            free_disk_watermark_bytes: 2 * 1024 * 1024 * 1024,
            concurrency_units: u32::MAX,
            maximum_queue_depth: 512,
            maximum_idle_cells_per_project: default_maximum_idle_cells_per_project(),
        }
    }
}

impl ExecutorRuntimePolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.concurrency_units == 0 {
            return Err("executor concurrency units must be greater than zero".into());
        }
        if self.maximum_queue_depth == 0 {
            return Err("executor maximum queue depth must be greater than zero".into());
        }
        if self.maximum_idle_cells_per_project == 0 {
            return Err("executor maximum idle cells per project must be greater than zero".into());
        }
        if self.free_disk_watermark_bytes == 0 {
            return Err("executor free-disk watermark must be greater than zero".into());
        }
        if self.memory_budget_bytes == Some(0) {
            return Err("executor memory budget must be greater than zero when configured".into());
        }
        if self.disk_growth_budget_bytes == Some(0) {
            return Err(
                "executor disk-growth budget must be greater than zero when configured".into(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BuildSkew {
    pub runner_build_id: String,
    pub executor_build_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorSubstrateReport {
    pub admission: AdmissionHealth,
    pub queues: Vec<QueueClassHealth>,
    pub host: HostHealth,
    pub disk: DiskHealth,
    #[serde(default)]
    pub inventory: CellInventoryHealth,
    #[serde(default)]
    pub applied_policy: ExecutorRuntimePolicy,
    #[serde(default)]
    pub drain_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorHealthSnapshot {
    pub identity: ExecutorIdentity,
    /// True for the executor the runner supervises inside its own process tree.
    /// Everything else attached to this fleet is an enrolled executor, so work
    /// placed there is attributed to it rather than read as ambient local work.
    #[serde(default)]
    pub colocated: bool,
    pub status: ExecutorHealthStatus,
    pub heartbeat_age_ms: u64,
    pub advertisement: ExecutorAdvertisement,
    pub admission: AdmissionHealth,
    pub queues: Vec<QueueClassHealth>,
    pub host: HostHealth,
    pub disk: DiskHealth,
    #[serde(default)]
    pub inventory: CellInventoryHealth,
    #[serde(default)]
    pub connection_generation: u64,
    #[serde(default)]
    pub applied_policy: ExecutorRuntimePolicy,
    #[serde(default)]
    pub drain_mode: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_skew: Option<BuildSkew>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum InventoryAuthorityState {
    Authoritative,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellInventoryHealth {
    pub authority: InventoryAuthorityState,
    #[serde(rename = "materializedCount")]
    pub checked_out_count: usize,
    pub idle_count: usize,
    #[serde(default)]
    pub idle_limit_per_project: usize,
    #[serde(default)]
    pub excess_idle_count: usize,
    pub transient_occupancy: usize,
    #[serde(rename = "lifetimeOccupancy")]
    pub lifetime_cell_occupancy: usize,
    pub active_transient_reservation: ResourceReservation,
    pub active_lifetime_reservation: ResourceReservation,
    pub retirement_in_progress: bool,
    pub sweep_status: StorageSweepStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "lastReclaimedSlotId")]
    pub last_reclaimed_cell_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reclaimed_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reclaimed_bytes: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CellOccupancy {
    pub total: usize,
    pub provisioning: usize,
    pub idle: usize,
    pub queued: usize,
    pub running: usize,
    pub recovering: usize,
    pub retired: usize,
    pub quarantined: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StoreLockHealth {
    pub store: String,
    pub waiter_count: usize,
    pub waits: BoundedDurationSummary,
    pub holds: BoundedDurationSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SubstrateHealthSnapshot {
    pub schema_version: u32,
    pub captured_at_unix_ms: u64,
    pub status: SubstrateHealthStatus,
    pub reasons: Vec<SubstrateHealthReason>,
    pub executors: Vec<ExecutorHealthSnapshot>,
    pub occupancy: CellOccupancy,
    #[serde(default)]
    pub inventory: CellInventoryHealth,
    #[serde(rename = "buildSlots")]
    pub fleet: FleetSnapshot,
    pub store_locks: Vec<StoreLockHealth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeLeaseAcquireRequest {
    pub declaration: LifetimeLeaseDeclaration,
    pub priority: CellPriority,
    pub deadline_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LifetimeLeaseFence {
    pub lease_id: String,
    pub owner: LifetimeLeaseOwner,
    #[serde(default)]
    pub incarnation_id: String,
    pub lease_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "camelCase")]
pub enum LifetimeLeaseOperation {
    Acquire {
        request: LifetimeLeaseAcquireRequest,
    },
    Reclaim {
        fence: LifetimeLeaseFence,
    },
    Renew {
        fence: LifetimeLeaseFence,
    },
    Release {
        fence: LifetimeLeaseFence,
    },
    StartProcess {
        fence: LifetimeLeaseFence,
        process_key: String,
        process: LifetimeProcessSpec,
    },
    StopProcess {
        fence: LifetimeLeaseFence,
        process_key: String,
    },
    WriteProcessInput {
        fence: LifetimeLeaseFence,
        process_key: String,
        process_generation: u64,
        data: Vec<u8>,
    },
    ResizePty {
        fence: LifetimeLeaseFence,
        process_key: String,
        process_generation: u64,
        size: LifetimePtySize,
    },
    #[serde(rename = "refreshMaterialization")]
    RefreshCheckout {
        fence: LifetimeLeaseFence,
        base_commit: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum LifetimeLeaseFailureKind {
    InvalidDeclaration,
    ConflictingDeclaration,
    NotFound,
    /// The runner cannot currently route the operation to the executor that may
    /// still retain the lease. Unlike `NotFound`, this is not proof of lease death.
    Unavailable,
    WrongOwner,
    StaleEpoch,
    InvalidState,
    Admission,
    Process,
    Cleanup,
    Persistence,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum LifetimeLeaseResult {
    State {
        #[serde(rename = "slot")]
        cell: PersistentCellState,
    },
    Released {
        lease_id: String,
        lease_epoch: u64,
    },
    Failed {
        kind: LifetimeLeaseFailureKind,
        diagnostic: String,
        #[serde(
            default,
            rename = "buildSlotOutcome",
            skip_serializing_if = "Option::is_none"
        )]
        cell_outcome: Option<Box<CellOutcome>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorConfig {
    pub project_id: String,
    /// Human-readable project key used only for executor-owned presentation paths.
    /// Stable protocol and repository identity remains `project_id`.
    pub project_key: String,
    pub acquisition_deadline_seconds: u64,
    pub default_timeout_seconds: u64,
    #[serde(default)]
    pub setup_commands: Vec<String>,
    #[serde(default)]
    pub populate: cairn_worktree::PopulateConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub population_source_root: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ProcessSandboxMode {
    #[default]
    Unconfined,
    Confined,
    /// The externally owned checkout stays readable but is never writable,
    /// including after a fence grant. Temp and toolchain cache roots remain writable.
    ReadOnlyCheckout,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessBatch {
    pub sequential: bool,
    pub stop_on_error: bool,
    /// Timed-out items transfer their live child into a terminal lifetime lease
    /// instead of being killed. Verifier/check batches leave this false.
    #[serde(default)]
    pub promote_timeouts: bool,
    pub sandbox_mode: ProcessSandboxMode,
    pub items: Vec<ProcessBatchItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_context_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ProcessBatchExecution {
    #[default]
    Direct,
    NativeShell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessBatchItem {
    pub header: String,
    pub stream_id: String,
    #[serde(default)]
    pub execution: ProcessBatchExecution,
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<String>,
    pub timeout_ms: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_resource_identity: Option<CommandResourceIdentity>,
}

/// Typed result for one command in a build-cell process batch.
///
/// The runner-facing presentation fields remain stable while verifier callers can
/// consume command verdict and measurement fields without decoding an opaque body.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessBatchItemOutcome {
    pub header: String,
    pub body: String,
    pub succeeded: bool,
    pub suspended: bool,
    #[serde(default)]
    pub images: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promoted_terminal: Option<PromotedTerminalProcess>,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_rss_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_delta_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sandbox_denials: Vec<SandboxDenialEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracked_modifications: Option<TrackedModificationEvidence>,
}

// The protocol keeps request payloads inline so serde preserves the established
// wire shape; boxing would be an in-memory optimization with a broad API cost.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ExecutorMessage {
    Hello {
        protocol_version: u32,
        advertisement: ExecutorAdvertisement,
        enrollment: ExecutorEnrollmentIdentity,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        executor_build_id: Option<String>,
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
        health: ExecutorSubstrateReport,
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
    RuntimePolicyRequest {
        correlation_id: String,
        policy: ExecutorRuntimePolicy,
    },
    RuntimePolicyResponse {
        correlation_id: String,
        result: Result<ExecutorRuntimePolicy, String>,
    },
    DrainModeRequest {
        correlation_id: String,
        enabled: bool,
    },
    DrainModeResponse {
        correlation_id: String,
        result: Result<bool, String>,
    },
    Submit {
        request: CellRequest,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        batch: Option<ProcessBatch>,
    },
    Result {
        request_id: String,
        attempt_id: String,
        outcome: CellOutcome,
    },
    #[serde(rename = "buildSlotOutput")]
    CellOutput {
        event: CellOutputEvent,
    },
    Cancel {
        request_id: String,
        attempt_id: String,
    },
    CancelJob {
        job_id: String,
    },
    LifetimeLeaseRequest {
        correlation_id: String,
        operation: LifetimeLeaseOperation,
    },
    LifetimeLeaseResponse {
        correlation_id: String,
        result: LifetimeLeaseResult,
    },
    LifetimeProcessEvent {
        event: LifetimeProcessEvent,
    },
    SnapshotRequest {
        correlation_id: String,
    },
    SnapshotResponse {
        correlation_id: String,
        snapshot: FleetSnapshot,
        health: ExecutorSubstrateReport,
    },
    SnapshotUpdated {
        snapshot: FleetSnapshot,
        health: ExecutorSubstrateReport,
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

/// A sandbox denial observed while executing one concrete subprocess.
///
/// Pure-verdict callers keep this evidence without replacing the subprocess's
/// own exit-code verdict. Interactive callers continue to adjudicate `denial`
/// through the runner callback.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SandboxDenialEvidence {
    pub denial: SandboxDenial,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    pub command: String,
    pub stream_id: String,
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
    ProcessItemStarted {
        runner_context_id: String,
        stream_id: String,
    },
    ProcessItemCompleted {
        runner_context_id: String,
        stream_id: String,
        succeeded: bool,
        exit_code: Option<i32>,
        timed_out: bool,
        duration_ms: u64,
    },
    ActivatePromotedTerminal {
        runner_context_id: String,
        fence: LifetimeLeaseFence,
        process_key: String,
        command: String,
        output: Vec<u8>,
        process_generation: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum RunnerCallbackResult {
    Allowed,
    Rejected { diagnostic: String },
    Suspended,
    Completed,
    Promoted { terminal: PromotedTerminalProcess },
    Failed { diagnostic: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_config_defaults_omitted_population_fields() {
        let config: ExecutorConfig = serde_json::from_value(serde_json::json!({
            "projectId": "p",
            "projectKey": "P",
            "acquisitionDeadlineSeconds": 5,
            "defaultTimeoutSeconds": 30,
            "setupCommands": []
        }))
        .unwrap();
        assert!(config.populate.is_empty());
        assert!(config.population_source_root.is_none());
    }

    #[test]
    fn every_message_variant_round_trips() {
        let request = sample_request();
        let outcome = sample_outcome();
        let snapshot = FleetSnapshot::default();
        let advertisement = sample_advertisement();
        let messages = vec![
            ExecutorMessage::Hello {
                protocol_version: EXECUTOR_PROTOCOL_VERSION,
                advertisement: advertisement.clone(),
                enrollment: ExecutorEnrollmentIdentity::Colocated,
                executor_build_id: Some("executor-build".into()),
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
                health: ExecutorSubstrateReport::default(),
            },
            ExecutorMessage::AdvertisementUpdated { advertisement },
            ExecutorMessage::ProtocolIncompatible {
                expected: 1,
                received: 2,
            },
            ExecutorMessage::Configure {
                config: ExecutorConfig {
                    project_id: "p".into(),
                    project_key: "p".into(),
                    acquisition_deadline_seconds: 20,
                    default_timeout_seconds: 30,
                    setup_commands: vec!["bun install".into()],
                    populate: Default::default(),
                    population_source_root: None,
                },
            },
            ExecutorMessage::LifetimeProcessEvent {
                event: LifetimeProcessEvent {
                    lease_id: "lease".into(),
                    incarnation_id: "incarnation".into(),
                    lease_epoch: 3,
                    process_key: "main".into(),
                    process_generation: 4,
                    event: LifetimeProcessEventKind::Output {
                        sequence: 5,
                        stream: LifetimeProcessStream::Stdout,
                        data: b"hello".to_vec(),
                    },
                },
            },
            ExecutorMessage::RuntimePolicyRequest {
                correlation_id: "policy".into(),
                policy: ExecutorRuntimePolicy::default(),
            },
            ExecutorMessage::RuntimePolicyResponse {
                correlation_id: "policy".into(),
                result: Ok(ExecutorRuntimePolicy::default()),
            },
            ExecutorMessage::DrainModeRequest {
                correlation_id: "drain".into(),
                enabled: true,
            },
            ExecutorMessage::DrainModeResponse {
                correlation_id: "drain".into(),
                result: Ok(true),
            },
            ExecutorMessage::CellOutput {
                event: CellOutputEvent {
                    executor_id: "e".into(),
                    cell_id: "slot".into(),
                    request_id: "r".into(),
                    attempt_id: "a".into(),
                    stream_id: "stdout".into(),
                    chunk: "hello".into(),
                    emitted_at_unix_ms: 1,
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
            ExecutorMessage::LifetimeLeaseRequest {
                correlation_id: "lease-request".into(),
                operation: LifetimeLeaseOperation::Acquire {
                    request: LifetimeLeaseAcquireRequest {
                        declaration: sample_lifetime_declaration(),
                        priority: CellPriority::AgentInteractive,
                        deadline_unix_ms: 10,
                    },
                },
            },
            ExecutorMessage::LifetimeLeaseResponse {
                correlation_id: "lease-response".into(),
                result: LifetimeLeaseResult::Released {
                    lease_id: "lease".into(),
                    lease_epoch: 3,
                },
            },
            ExecutorMessage::SnapshotRequest {
                correlation_id: "c".into(),
            },
            ExecutorMessage::SnapshotResponse {
                correlation_id: "c".into(),
                snapshot: snapshot.clone(),
                health: ExecutorSubstrateReport::default(),
            },
            ExecutorMessage::SnapshotUpdated {
                snapshot,
                health: ExecutorSubstrateReport::default(),
            },
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
    fn tagged_occupants_round_trip_and_legacy_active_request_migrates_to_command() {
        let command = ActiveCellRequest {
            executor_id: "executor".into(),
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            command: "true".into(),
            command_class: CellCommandClass::Other,
            owner: None,
            priority: CellPriority::ReviewCheck,
            requesting_job_id: None,
            affinity_key: None,
            queued_at_unix_ms: 1,
            started_at_unix_ms: Some(2),
            stage: Some(CellExecutionStage::Running),
            resource_reservation: ResourceReservation::default(),
            learned_estimate: None,
            subscriber_count: 1,
        };
        let tagged = CellOccupant::Command(command.clone());
        let value = serde_json::to_value(&tagged).unwrap();
        assert_eq!(value.get("kind"), Some(&serde_json::json!("command")));
        assert_eq!(
            serde_json::from_value::<CellOccupant>(value).unwrap(),
            tagged
        );
        assert_eq!(
            serde_json::from_value::<CellOccupant>(serde_json::to_value(command).unwrap()).unwrap(),
            tagged
        );

        let lifetime = CellOccupant::Lifetime(LifetimeLeaseState {
            declaration: sample_lifetime_declaration(),
            incarnation_id: "incarnation".into(),
            current_base_commit: "b".into(),
            phase: LifetimeLeasePhase::Active,
            last_heartbeat_unix_ms: 1,
            reclaim_deadline_unix_ms: 41_000,
            state_revision: 1,
            command_settled: true,
            processes: std::collections::BTreeMap::new(),
            events: vec![LifetimeLeaseEvent {
                revision: 1,
                occurred_at_unix_ms: 1,
                event: LifetimeLeaseEventKind::Acquired,
            }],
        });
        let value = serde_json::to_value(&lifetime).unwrap();
        assert_eq!(value.get("kind"), Some(&serde_json::json!("lifetime")));
        assert_eq!(
            serde_json::from_value::<CellOccupant>(value).unwrap(),
            lifetime
        );
    }

    #[test]
    fn request_and_delta_round_trip_and_cancellation_is_separate() {
        let request = sample_request();
        let json = serde_json::to_value(&request).unwrap();
        assert!(json.get("cancelled").is_none());
        assert!(json.get("cancellation").is_none());
        assert_eq!(
            serde_json::from_value::<CellRequest>(json).unwrap(),
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
            serde_json::from_value::<CellOutcome>(json).unwrap(),
            outcome
        );
    }

    fn sample_request() -> CellRequest {
        CellRequest {
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
            command_class: CellCommandClass::Other,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
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
            command_resource_identity: Some(CommandResourceIdentity {
                version: COMMAND_RESOURCE_IDENTITY_VERSION,
                key: "check-result-key".into(),
            }),
            resource_reservation: ResourceReservation {
                memory_bytes: 10,
                disk_growth_bytes: 20,
                concurrency_units: 1,
                source: ResourceReservationSource::ZeroKnowledgePrior,
            },
            learned_estimate: None,
        }
    }

    fn sample_lifetime_declaration() -> LifetimeLeaseDeclaration {
        LifetimeLeaseDeclaration {
            lease_id: "lease".into(),
            owner: LifetimeLeaseOwner {
                kind: LifetimeLeaseOwnerKind::DevInstance,
                owner_id: "launcher".into(),
            },
            owner_ref: Some(CellOwnerRef {
                project_id: "project".into(),
                project_key: Some("CAIRN".into()),
                issue_number: Some(2873),
                job_id: Some("job".into()),
                execution_seq: Some(1),
                node_kind: Some("builder".into()),
            }),
            name: "dev-instance:feature".into(),
            purpose: "serve the committed feature branch".into(),
            repository: sample_request().repository,
            initial_base_commit: "b".into(),
            resource_reservation: ResourceReservation {
                memory_bytes: 10,
                disk_growth_bytes: 20,
                concurrency_units: 1,
                source: ResourceReservationSource::Declared,
            },
            owner_death_policy: LifetimeOwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
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
    fn omitted_subscriber_counts_default_to_one() {
        let active: ActiveCellRequest = serde_json::from_value(serde_json::json!({
            "requestId": "r",
            "attemptId": "a",
            "command": "true",
            "priority": "reviewCheck",
            "requestingJobId": null,
            "queuedAtUnixMs": 1,
            "startedAtUnixMs": null
        }))
        .unwrap();
        assert_eq!(active.subscriber_count, 1);

        let queued: QueuedCellRequest = serde_json::from_value(serde_json::json!({
            "requestId": "r",
            "attemptId": "a",
            "projectId": "p",
            "command": "true",
            "priority": "reviewCheck",
            "requestingJobId": null,
            "queuedAtUnixMs": 1
        }))
        .unwrap();
        assert_eq!(queued.subscriber_count, 1);
    }

    #[test]
    fn omitted_constraints_remain_backward_compatible() {
        let mut value = serde_json::to_value(sample_request()).unwrap();
        value.as_object_mut().unwrap().remove("constraints");
        assert_eq!(
            serde_json::from_value::<CellRequest>(value)
                .unwrap()
                .constraints,
            None
        );
    }

    #[test]
    fn substrate_health_round_trip_preserves_contract_and_nulls() {
        let snapshot = SubstrateHealthSnapshot {
            schema_version: SUBSTRATE_HEALTH_SCHEMA_VERSION,
            captured_at_unix_ms: 42,
            status: SubstrateHealthStatus::Degraded,
            reasons: vec![
                SubstrateHealthReason::ReadingUnavailable {
                    section: "host.cpu".into(),
                },
                SubstrateHealthReason::DiskAccountingPartial {
                    executor_id: "executor-1".into(),
                    skipped_entries: 2,
                },
            ],
            executors: vec![],
            occupancy: CellOccupancy::default(),
            inventory: CellInventoryHealth::default(),
            fleet: FleetSnapshot::default(),
            store_locks: vec![StoreLockHealth {
                store: "/tmp/store".into(),
                waiter_count: 0,
                waits: BoundedDurationSummary {
                    sample_count: 1,
                    p50_ms: Some(0),
                    p95_ms: Some(0),
                    max_ms: Some(0),
                },
                holds: BoundedDurationSummary::default(),
            }],
        };
        let value = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["capturedAtUnixMs"], 42);
        assert_eq!(value["status"], "degraded");
        assert_eq!(
            value["reasons"][1]["diskAccountingPartial"]["executorId"],
            "executor-1"
        );
        assert_eq!(
            value["reasons"][1]["diskAccountingPartial"]["skippedEntries"],
            2
        );
        assert_eq!(value["storeLocks"][0]["waits"]["p50Ms"], 0);
        assert!(value["storeLocks"][0]["holds"]["p50Ms"].is_null());
        assert_eq!(
            serde_json::from_value::<SubstrateHealthSnapshot>(value).unwrap(),
            snapshot
        );
    }

    #[test]
    fn unsupported_host_and_disk_readings_are_null_not_zero() {
        let host = serde_json::to_value(HostHealth::default()).unwrap();
        assert!(host["availableMemoryBytes"].is_null());
        assert!(host["processRssBytes"].is_null());
        let disk = serde_json::to_value(DiskHealth::default()).unwrap();
        assert_eq!(disk["status"], "unknown");
        assert!(disk["totalBytes"].is_null());
        assert!(disk["categories"].is_null());
        assert!(disk["accountingMeasuredAtUnixMs"].is_null());
        assert!(disk["accountingSkippedEntries"].is_null());

        let mut legacy_disk = disk;
        legacy_disk
            .as_object_mut()
            .unwrap()
            .remove("accountingMeasuredAtUnixMs");
        legacy_disk
            .as_object_mut()
            .unwrap()
            .remove("accountingSkippedEntries");
        let legacy_disk = serde_json::from_value::<DiskHealth>(legacy_disk).unwrap();
        assert_eq!(legacy_disk.accounting_measured_at_unix_ms, None);
        assert_eq!(legacy_disk.accounting_skipped_entries, None);
    }

    #[test]
    fn cell_state_preserves_legacy_persistent_shape_and_alias() {
        let legacy = serde_json::json!({
            "projectId": "p",
            "slotId": "slot-7",
            "path": "/tmp/slot-7",
            "workspaceName": "slot-7",
            "repository": "/tmp/repo",
            "materializationKind": "jujutsuWorkspace",
            "lifecycle": "running",
            "leaseEpoch": 3,
            "lastSealedCommit": null,
            "lastUsedUnixMs": 4,
            "activeRequest": {
                "requestId": "r",
                "attemptId": "a",
                "command": "true",
                "priority": "reviewCheck",
                "requestingJobId": null,
                "queuedAtUnixMs": 1,
                "startedAtUnixMs": null,
                "resourceReservation": {
                    "memoryBytes": 0,
                    "diskGrowthBytes": 0,
                    "concurrencyUnits": 0,
                    "source": "zeroKnowledgePrior"
                }
            }
        });
        let state: PersistentCellState = serde_json::from_value(legacy).unwrap();
        assert_eq!(state.cell_id, "slot-7");
        assert!(matches!(state.occupant, Some(CellOccupant::Command(_))));

        let value = serde_json::to_value(state).unwrap();
        assert_eq!(value["slotId"], "slot-7");
        assert_eq!(value["materializationKind"], "jujutsuWorkspace");
        assert_eq!(value["occupant"]["kind"], "command");
        assert!(value.get("activeRequest").is_none());
        assert!(value.get("cellId").is_none());
        assert!(value.get("checkoutKind").is_none());
    }

    #[test]
    fn renamed_occupancy_fields_preserve_wire_keys() {
        let fleet = FleetSnapshot {
            lifetime_cell_occupancy: Some(LifetimeOccupancyEvidence {
                lease_count: 2,
                ..LifetimeOccupancyEvidence::default()
            }),
            ..FleetSnapshot::default()
        };
        let fleet_value = serde_json::to_value(&fleet).unwrap();
        assert_eq!(fleet_value["lifetimeOccupancy"]["leaseCount"], 2);
        assert!(fleet_value.get("lifetimeCellOccupancy").is_none());
        let fleet_round_trip: FleetSnapshot = serde_json::from_value(fleet_value).unwrap();
        assert_eq!(
            fleet_round_trip
                .lifetime_cell_occupancy
                .unwrap()
                .lease_count,
            2
        );

        let inventory = CellInventoryHealth {
            lifetime_cell_occupancy: 3,
            ..CellInventoryHealth::default()
        };
        let inventory_value = serde_json::to_value(&inventory).unwrap();
        assert_eq!(inventory_value["lifetimeOccupancy"], 3);
        assert!(inventory_value.get("lifetimeCellOccupancy").is_none());
        let inventory_round_trip: CellInventoryHealth =
            serde_json::from_value(inventory_value).unwrap();
        assert_eq!(inventory_round_trip.lifetime_cell_occupancy, 3);
    }

    #[test]
    fn renamed_checkout_variants_preserve_wire_tags() {
        assert_eq!(
            serde_json::to_value(CellExecutionStage::CheckingOut).unwrap(),
            "materializing"
        );
        assert_eq!(
            serde_json::to_value(StorageFailureStage::ProvisioningCheckout).unwrap(),
            "provisioningMaterialization"
        );
        let operation = LifetimeLeaseOperation::RefreshCheckout {
            fence: LifetimeLeaseFence {
                lease_id: "lease".into(),
                owner: LifetimeLeaseOwner {
                    kind: LifetimeLeaseOwnerKind::Other,
                    owner_id: "owner".into(),
                },
                incarnation_id: "incarnation".into(),
                lease_epoch: 1,
            },
            base_commit: "base".into(),
        };
        assert_eq!(
            serde_json::to_value(operation).unwrap()["operation"],
            "refreshMaterialization"
        );
    }

    #[test]
    fn runtime_policy_validation_rejects_zero_without_weakening_optional_budgets() {
        let valid = ExecutorRuntimePolicy::default();
        assert!(valid.validate().is_ok());
        assert!(ExecutorRuntimePolicy {
            concurrency_units: 0,
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            maximum_idle_cells_per_project: 0,
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            maximum_queue_depth: 0,
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            free_disk_watermark_bytes: 0,
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            memory_budget_bytes: Some(0),
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ExecutorRuntimePolicy {
            disk_growth_budget_bytes: Some(0),
            ..valid
        }
        .validate()
        .is_err());
    }

    #[test]
    fn legacy_health_defaults_new_operator_fields() {
        let mut value = serde_json::to_value(ExecutorSubstrateReport::default()).unwrap();
        value.as_object_mut().unwrap().remove("appliedPolicy");
        value.as_object_mut().unwrap().remove("drainMode");
        let report: ExecutorSubstrateReport = serde_json::from_value(value).unwrap();
        assert_eq!(report.applied_policy, ExecutorRuntimePolicy::default());
        assert!(!report.drain_mode);
    }

    fn sample_outcome() -> CellOutcome {
        CellOutcome::Completed {
            request_id: "r".into(),
            attempt_id: "a".into(),
            exit_code: Some(1),
            output: "failed".into(),
            timed_out: false,
            metadata: CellExecutionMeta {
                executor_id: "e".into(),
                executor_device_id: "d".into(),
                executor_connection_generation: 1,
                cell_id: "s".into(),
                lease_epoch: 2,
                started_at_unix_ms: 3,
                finished_at_unix_ms: 4,
                duration_ms: None,
                peak_rss_bytes: None,
                peak_physical_footprint_bytes: None,
                disk_delta_bytes: None,
                measurement_quality: None,
            },
            mutation_delta: Some(Box::new(MutationDelta {
                base_commit: "b".into(),
                delta_commit: "d".into(),
                upload_receipt: None,
            })),
            sandbox_denials: Vec::new(),
            tracked_modifications: None,
        }
    }
}

#[cfg(test)]
mod lifetime_pipe_protocol_tests {
    use super::*;

    #[test]
    fn lifetime_pipe_runtime_shape_round_trips_with_stream_tags() {
        let process = LifetimeProcessSpec {
            program: "bun".into(),
            args: vec!["main.ts".into()],
            cwd: "package".into(),
            cwd_root: LifetimeProcessCwdRoot::LeaseScratch,
            env: Vec::new(),
            sandbox_mode: ProcessSandboxMode::Confined,
            sandbox_policy: None,
            runtime_assets: vec![LifetimeRuntimeAsset {
                path: "package/main.ts".into(),
                data: b"console.log('ok')".to_vec(),
            }],
            io: LifetimeProcessIoMode::Pipe,
        };
        let value = serde_json::to_value(&process).unwrap();
        assert_eq!(value["cwdRoot"], "leaseScratch");
        assert_eq!(value["io"]["mode"], "pipe");
        assert_eq!(value["runtimeAssets"][0]["path"], "package/main.ts");
        assert_eq!(
            serde_json::from_value::<LifetimeProcessSpec>(value).unwrap(),
            process
        );
        assert_eq!(
            serde_json::to_value(LifetimeLeaseOwnerKind::Workflow).unwrap(),
            "workflow"
        );
        let output = LifetimeProcessEventKind::Output {
            sequence: 7,
            stream: LifetimeProcessStream::Stderr,
            data: b"diagnostic".to_vec(),
        };
        let output_value = serde_json::to_value(output).unwrap();
        assert_eq!(output_value["event"], "output");
        assert_eq!(output_value["stream"], "stderr");
    }
}
