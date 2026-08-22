//! Runner-side fleet-placement facade for supervised and enrolled executors.
//!
//! Core owns request construction, settings resolution, result correlation, and
//! the cached UI snapshot. Scheduling, workspaces, processes, cancellation, and
//! mutation sealing exist only in the executor process.

pub mod desktop;
pub mod management;
pub(crate) mod occupancy;
pub(crate) mod output_guard;
pub(crate) mod placement;
pub(crate) mod purpose;
pub(crate) mod residency;
mod resource_profiles;
pub(crate) mod service_placement;
pub mod telemetry;

use crate::mcp::handlers::run::{ResolvedRunBatch, RunSpec};
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::{
    aged_priority, CacheWarmthEvidence, CellCheckoutKind, CellCommandClass, CellResidency,
    ContentionEstimate, CpuAdmissionState, DurationEstimate, DurationFallback, EnrolledRemote,
    ExecutionLoadContext, ExecutionWarmth, ExecutorAdvertisement, ExecutorCapabilities,
    ExecutorConfig, ExecutorHealthSnapshot, ExecutorHealthStatus, ExecutorIdentity,
    ExecutorInspection, ExecutorMessage, ExecutorSelector, ExecutorSubstrateEvidence,
    ExecutorSubstrateReport, ExecutorSubstrateState, InventoryAuthorityState, MachineMeasurement,
    MaterializationReadFailureKind, MaterializationReadRequest, MaterializationReadResult,
    ObjectTransferCoordinate, ObservationReuse, PlacementCounterfactualWinner, PlacementDecision,
    PlacementMobility, PlacementOutcome, PlacementPolicyEvidence, PlacementPrediction,
    PlacementReadings, PlacementReason, PlacementRejection, PlacementRejectionReason,
    PlacementSelection, PlacementSyncCost, PlacementTieBreak, PreparationForecast, ProcessBatch,
    ProcessBatchExecution, ProcessBatchFile, ProcessBatchItem, ProcessSandboxMode, QueueForecast,
    QueueUnknownReason, QueuedPlacementEvidence, QueuedPlacementRejection, QueuedPlacementTarget,
    RemoteAttachAttempt, RemoteConnectionTransition, RemoteLinkState, RepositoryLocator,
    ReservationFallback, ReservationRationale, ResidencyAcquireRequest, ResidencyFailureKind,
    ResidencyFence, ResidencyHolder, ResidencyOperation, ResidencyResult, ResidencyRuntimeConfig,
    ResidentProcessEvent, ResidentProcessEventKind, RunnerCallback, RunnerCallbackResult,
    WarmthUnknownReason, CPU_ADMISSION_BLIND_AFTER_MS, EXECUTOR_HEARTBEAT_INTERVAL_MS,
    EXECUTOR_LINK_STALL_REMEDIATION_MS, EXECUTOR_PROGRESS_FRESHNESS_MS,
    EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS, LOCAL_EXECUTOR_NAME,
    MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS, RESIDENCY_ACQUIRE_ATTEMPT_ID,
};
use cairn_common::executor_protocol::{
    executor_names_match, normalize_executor_name, EXECUTOR_NAME_RULE,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::Instant;

pub use cairn_common::executor_protocol::{
    ActiveCellRequest, CellExecutionMeta, CellOutcome, CellPriority, CellRequest,
    CellUnavailableReason, CommandResourceIdentity, ExecutingCellRequest, FleetSnapshot,
    MutationDelta, MutationPolicy, PersistentCellLifecycle, PersistentCellState,
    PlacementWorkClass, QueuedCellRequest, ResourceReservation, ResourceReservationSource,
};

pub const DEFAULT_PLACEMENT_PROFILE: &str = "default";
pub const INTERACTIVE_PLACEMENT_PROFILE: &str = "interactive";
pub const LOW_POWER_PLACEMENT_PROFILE: &str = "low-power";
pub const MAX_PREFERENCE_DELAY_SECONDS: u64 = 60 * 60;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum PlacementStance {
    LocalFirst,
    RemoteFirst,
    RemoteOnly,
    #[default]
    Any,
}

fn residency_runtime_config_request(residency: &CellResidency) -> CellRequest {
    residency_placement_request(&ResidencyAcquireRequest {
        holder: residency.holder.clone(),
        repository: residency.repository.clone(),
        executor: None,
        owner_ref: residency.owner_ref.clone(),
        selector: residency.selector.clone(),
        initial_base_commit: residency.current_base_commit.clone(),
        footprint: residency.footprint,
        death_policy: residency.death_policy.clone(),
        priority: CellPriority::AgentInteractive,
        wait_horizon_unix_ms: unix_time_ms().saturating_add(30_000),
        waiting_since_unix_ms: unix_time_ms(),
    })
}

/// One executor admission transition, recorded with the placement survey that
/// led work to this machine. The executor owns the capacity decision while the
/// runner owns the fleet refusal set; joining them here is the only point where
/// an operator can later answer both "why here?" and "why still waiting?".
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AdmissionParkedEvidence {
    executor_id: String,
    executor_name: String,
    queued: cairn_common::executor_protocol::QueuedCellRequest,
    admission: cairn_common::executor_protocol::AdmissionHealth,
    machine: cairn_common::executor_protocol::MachineTelemetry,
    placement: PlacementDecision,
}

fn queued_placement_evidence(decision: &PlacementDecision) -> QueuedPlacementEvidence {
    let selected = match &decision.outcome {
        PlacementOutcome::Selected(selection) => Some(QueuedPlacementTarget {
            executor_name: selection.executor_name.clone(),
            executor_id: selection.executor_id.clone(),
            available_memory_bytes: selection
                .readings
                .memory
                .value()
                .map(|value| value.available_bytes),
            free_volume_bytes: selection
                .readings
                .volume
                .value()
                .map(|value| value.free_bytes),
            reservation: selection.reservation.clone(),
        }),
        PlacementOutcome::Refused { .. } => None,
    };
    QueuedPlacementEvidence {
        decided_at_unix_ms: decision.decided_at_unix_ms,
        mobility: decision.mobility,
        pinned_executor_id: decision.pinned_executor_id.clone(),
        selected,
        rejected: decision
            .rejected
            .iter()
            .map(|rejection| QueuedPlacementRejection {
                executor_name: rejection.executor_name.clone(),
                executor_id: rejection.executor_id.clone(),
                reason: rejection.reason.clone(),
            })
            .collect(),
    }
}

#[cfg(test)]
fn placement_prediction(
    entry: &ExecutorConnectionState,
    warmth: CacheWarmthEvidence,
    queue: QueueForecast,
    run: DurationEstimate,
    sync_cost: SyncCost,
) -> PlacementPrediction {
    placement_prediction_with_contention(
        entry,
        warmth,
        queue,
        run,
        resource_profiles::contention_prior(&candidate_load(entry)),
        sync_cost,
    )
}

fn candidate_load(entry: &ExecutorConnectionState) -> ExecutionLoadContext {
    let (compile, light) =
        entry
            .snapshot
            .executing_requests
            .iter()
            .fold((0_u32, 0_u32), |counts, request| {
                if request.command_class.is_compile_class() {
                    (counts.0.saturating_add(1), counts.1)
                } else {
                    (counts.0, counts.1.saturating_add(1))
                }
            });
    ExecutionLoadContext {
        co_resident_compile_jobs: compile,
        co_resident_light_jobs: light,
        cpu_utilization_millis: entry
            .health
            .host
            .cpu_admission
            .utilization
            .map(|value| (value.clamp(0.0, 1.0) * 1_000.0).round() as u32),
    }
}

fn default_active_placement_profile() -> String {
    DEFAULT_PLACEMENT_PROFILE.to_string()
}

fn built_in_profile(name: &str) -> Option<&'static PlacementProfile> {
    use std::sync::OnceLock;
    static BUILT_INS: OnceLock<BTreeMap<String, PlacementProfile>> = OnceLock::new();
    BUILT_INS.get_or_init(built_in_placement_profiles).get(name)
}

fn is_built_in_profile(name: &str) -> bool {
    built_in_profile(name).is_some()
}

fn validate_custom_profile_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() || name.trim() != name {
        return Err(
            "placement profile name must be nonblank and have no surrounding whitespace".into(),
        );
    }

    if is_built_in_profile(name) {
        return Err(format!(
            "built-in placement profile {name} cannot be shadowed"
        ));
    }
    Ok(())
}

fn validate_profile_executor_id(
    executor_id: &str,
    known_executor_ids: &HashSet<&str>,
) -> Result<(), String> {
    if !known_executor_ids.contains(executor_id) {
        return Err(format!(
            "placement profile names unknown executor ID {executor_id}"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlacementProfile {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub machine_priority: Vec<String>,
    pub routes: BTreeMap<PlacementWorkClass, PlacementStance>,
    #[serde(default)]
    pub max_preference_delay_seconds: u64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub executor_policy_overrides:
        HashMap<String, cairn_common::executor_protocol::ExecutorRuntimePolicy>,
}

impl PlacementProfile {
    pub fn stance(&self, work_class: PlacementWorkClass) -> PlacementStance {
        self.routes
            .get(&work_class)
            .copied()
            .unwrap_or(PlacementStance::Any)
    }

    fn validate(&self, known_executor_ids: &HashSet<&str>) -> Result<(), String> {
        for class in placement_work_classes() {
            if !self.routes.contains_key(&class) {
                return Err(format!("placement profile is missing route {class:?}"));
            }
        }
        if self.routes.len() != placement_work_classes().len() {
            return Err("placement profile contains an unknown work-class route".into());
        }
        if self.max_preference_delay_seconds > MAX_PREFERENCE_DELAY_SECONDS {
            return Err(format!(
                "maxPreferenceDelaySeconds must not exceed {MAX_PREFERENCE_DELAY_SECONDS}"
            ));
        }
        let mut priority = HashSet::new();
        for executor_id in &self.machine_priority {
            validate_profile_executor_id(executor_id, known_executor_ids)?;
            if !priority.insert(executor_id) {
                return Err(format!(
                    "placement profile repeats machinePriority executor {executor_id}"
                ));
            }
        }
        for executor_id in self.executor_policy_overrides.keys() {
            validate_profile_executor_id(executor_id, known_executor_ids)?;
        }
        Ok(())
    }
}

fn placement_work_classes() -> [PlacementWorkClass; 5] {
    [
        PlacementWorkClass::ReviewChecks,
        PlacementWorkClass::WriteChecks,
        PlacementWorkClass::AgentSessions,
        PlacementWorkClass::DevInstances,
        PlacementWorkClass::Services,
    ]
}

fn profile_with_routes(
    max_preference_delay_seconds: u64,
    route: impl Fn(PlacementWorkClass) -> PlacementStance,
) -> PlacementProfile {
    PlacementProfile {
        machine_priority: Vec::new(),
        routes: placement_work_classes()
            .into_iter()
            .map(|class| (class, route(class)))
            .collect(),
        max_preference_delay_seconds,
        executor_policy_overrides: HashMap::new(),
    }
}

pub fn built_in_placement_profiles() -> BTreeMap<String, PlacementProfile> {
    [
        (
            DEFAULT_PLACEMENT_PROFILE.to_string(),
            profile_with_routes(0, |_| PlacementStance::Any),
        ),
        (INTERACTIVE_PLACEMENT_PROFILE.to_string(), {
            let mut profile = profile_with_routes(30, |class| match class {
                PlacementWorkClass::ReviewChecks | PlacementWorkClass::WriteChecks => {
                    PlacementStance::RemoteFirst
                }
                PlacementWorkClass::AgentSessions => PlacementStance::LocalFirst,
                PlacementWorkClass::DevInstances | PlacementWorkClass::Services => {
                    PlacementStance::Any
                }
            });
            profile.executor_policy_overrides.insert(
                LOCAL_EXECUTOR_NAME.to_string(),
                cairn_common::executor_protocol::ExecutorRuntimePolicy {
                    cpu_admission: cairn_common::executor_protocol::CpuAdmissionPolicy {
                        entry_utilization: 0.75,
                        clear_utilization: 0.60,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            );
            profile
        }),
        (
            LOW_POWER_PLACEMENT_PROFILE.to_string(),
            profile_with_routes(120, |_| PlacementStance::RemoteFirst),
        ),
    ]
    .into_iter()
    .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FleetConfig {
    #[serde(default = "default_active_placement_profile")]
    pub active_placement_profile: String,
    /// Operator-authored profiles only. Code-owned built-ins are merged through
    /// [`FleetConfig::resolved_placement_profiles`] and never persisted here.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub placement_profiles: BTreeMap<String, PlacementProfile>,
    /// How long a caller with no tighter answer of its own is willing to wait
    /// for a machine that is merely busy.
    ///
    /// This is a wait horizon, not a queue budget. It is not a bound on one
    /// attempt — there are no attempts — and nothing about it is refreshed while
    /// a request waits: it is the requester's answer to "when does this result
    /// stop being wanted", carried onto the queue entry and honoured there.
    ///
    /// Read it through [`FleetConfig::capacity_wait_horizon_ms`], never
    /// directly: a value below [`MIN_CAPACITY_WAIT_HORIZON_SECONDS`] is not a
    /// policy anyone chose, and that accessor is where it is refused.
    ///
    /// The retired spelling `acquisitionDeadlineSeconds` is deliberately NOT
    /// aliased. It named a different quantity — twenty seconds of per-attempt
    /// queue budget that a ten-minute executor-side pause then quietly
    /// compensated for — and CAIRN-3268 changed the meaning by a factor of
    /// thirty. An alias migrates a spelling; it cannot migrate a semantics. So
    /// the retired key is ignored and the default applies, because inheriting
    /// the old number under the new name is how a fleet came to abandon every
    /// check after twenty seconds while its settings file looked deliberate
    /// (CAIRN-3429).
    #[serde(default = "default_capacity_wait_horizon_seconds")]
    pub(crate) capacity_wait_horizon_seconds: u64,
    #[serde(default = "default_timeout_seconds")]
    pub(crate) default_timeout_seconds: u64,
    /// Workspace fallback used when a machine has no complete runtime-policy
    /// override. Profile and machine-specific resolution can layer above this
    /// single baseline without putting policy into executor inventory.
    #[serde(default)]
    pub cpu_admission: cairn_common::executor_protocol::CpuAdmissionPolicy,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub executor_policies: HashMap<String, cairn_common::executor_protocol::ExecutorRuntimePolicy>,
    /// Runner-owned SSH executor declarations, keyed by stable executor ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub remote_executors: BTreeMap<String, RemoteExecutorConfig>,
    /// Stable operating-system identities learned during SSH enrollment, keyed
    /// by executor ID. Kept outside the public declaration because callers name
    /// a host; only the host itself can authoritatively identify it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub remote_host_identities: BTreeMap<String, String>,
    #[serde(default)]
    pub desktop_automation: DesktopAutomationConfig,
    /// Machines an operator has drained, by the key
    /// [`FleetConfig::drain_key`] gives them.
    ///
    /// Draining is an admission-refusal fact the RUNNER holds, not a setting on
    /// the machine: it says this runner will not place new work there. That is
    /// why it lives here rather than only in the executor's memory. A drain is
    /// reached for precisely when a machine is in no condition to agree to it,
    /// and a flag that died with the connection let a crushed machine be
    /// dogpiled again the moment it came back (CAIRN-4360).
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub drained_machines: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DesktopAutomationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub machines: HashMap<String, MachineDesktopAutomation>,
}

impl Default for DesktopAutomationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            machines: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MachineDesktopAutomation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDesktopAutomation {
    pub enabled: bool,
    pub binary: String,
}

fn default_true() -> bool {
    true
}

impl DesktopAutomationConfig {
    /// Carry a machine's desktop configuration with it when it is renamed.
    ///
    /// This map is keyed by public name rather than by executor identity,
    /// because the colocated machine has no remote declaration to hang it off.
    /// That makes a rename a move: leaving the entry under the old name would
    /// silently strip the machine of the binary it was configured with, and the
    /// only symptom would be desktop automation quietly resolving the platform
    /// default instead.
    pub fn rename_machine(&mut self, from: &str, to: &str) {
        let (Some(from), Some(to)) = (normalize_executor_name(from), normalize_executor_name(to))
        else {
            return;
        };
        if from == to {
            return;
        }
        if let Some(machine) = self.machines.remove(&from) {
            self.machines.insert(to, machine);
        }
    }

    /// Drop a removed machine's desktop configuration along with it, so removal
    /// leaves no entry addressing a machine that no longer exists.
    pub fn forget_machine(&mut self, name: &str) {
        if let Some(name) = normalize_executor_name(name) {
            self.machines.remove(&name);
        }
    }

    pub fn resolve(&self, machine_name: &str, os: &str) -> ResolvedDesktopAutomation {
        let machine =
            normalize_executor_name(machine_name).and_then(|name| self.machines.get(&name));
        ResolvedDesktopAutomation {
            enabled: machine
                .and_then(|config| config.enabled)
                .unwrap_or(self.enabled),
            binary: machine
                .and_then(|config| config.binary.clone())
                .unwrap_or_else(|| match os {
                    "windows" => "axon-win.exe".to_string(),
                    "linux" => "axon-linux".to_string(),
                    _ => "axon".to_string(),
                }),
        }
    }
}

/// A machine the runner can enroll, identified by the executor artifact it can
/// run. Every variant names one published target triple: a platform with no
/// artifact is not a platform this fleet can reach, so the enum and
/// `PUBLISHED_TARGETS` in `scripts/build-executor-manifest.ts` describe the same
/// set from the two ends of the distribution.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RemotePlatform {
    #[default]
    LinuxX86_64,
    LinuxArm64,
    WindowsX86_64,
    DarwinArm64,
}

impl RemotePlatform {
    pub fn os(self) -> &'static str {
        match self {
            Self::LinuxX86_64 | Self::LinuxArm64 => "linux",
            Self::WindowsX86_64 => "windows",
            Self::DarwinArm64 => "macos",
        }
    }

    /// The architecture this machine will report once it is attached.
    ///
    /// Spelled the way `std::env::consts::ARCH` spells it, because that is what
    /// the executor itself advertises in [`ExecutorCapabilities`]. These two
    /// descriptions of one machine meet in the fleet projection —
    /// `unattached_enrolled_remotes` hands over to the executor projection the
    /// moment a link comes up — and a machine that changed architecture on
    /// attach would be reporting a fact about the enum rather than the host.
    pub fn arch(self) -> &'static str {
        match self {
            Self::LinuxX86_64 | Self::WindowsX86_64 => "x86_64",
            Self::LinuxArm64 | Self::DarwinArm64 => "aarch64",
        }
    }

    pub fn target(self) -> &'static str {
        match self {
            Self::LinuxX86_64 => "x86_64-unknown-linux-gnu",
            Self::LinuxArm64 => "aarch64-unknown-linux-gnu",
            Self::WindowsX86_64 => "x86_64-pc-windows-msvc",
            Self::DarwinArm64 => "aarch64-apple-darwin",
        }
    }

    fn is_absolute(self, path: &str) -> bool {
        match self {
            Self::LinuxX86_64 | Self::LinuxArm64 | Self::DarwinArm64 => path.starts_with('/'),
            Self::WindowsX86_64 => {
                let bytes = path.as_bytes();
                (bytes.len() >= 3
                    && bytes[0].is_ascii_alphabetic()
                    && bytes[1] == b':'
                    && matches!(bytes[2], b'\\' | b'/'))
                    || path.starts_with(r"\\")
            }
        }
    }
}

fn missing_wait_reason_is_new(previous: &FleetSnapshot, queued: &QueuedCellRequest) -> bool {
    !previous.queued_requests.iter().any(|prior| {
        prior.request_id == queued.request_id
            && prior.attempt_id == queued.attempt_id
            && prior.substrate_hold.is_none()
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoteExecutorDeclaration {
    pub host: String,
    pub ssh_user: String,
    pub binary_path: Option<String>,
    pub cairn_home: Option<String>,
    pub executor_id: String,
    pub device_id: String,
    pub display_name: String,
    pub project_ids: Vec<String>,
    pub tunnel_port: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_ssh_args: Vec<String>,
}

impl RemoteExecutorDeclaration {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("host", self.host.as_str()),
            ("sshUser", self.ssh_user.as_str()),
            ("executorId", self.executor_id.as_str()),
            ("deviceId", self.device_id.as_str()),
            ("displayName", self.display_name.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("remote executor {name} must not be blank"));
            }
        }
        if self.host.starts_with('-') || self.ssh_user.starts_with('-') {
            return Err("remote executor host and sshUser must not begin with '-'".into());
        }
        if self.executor_id == COLOCATED_EXECUTOR_ID || !is_safe_executor_id(&self.executor_id) {
            return Err("remote executor executorId is unsafe".into());
        }
        validate_public_executor_name(&self.display_name)?;
        if self.tunnel_port == 0 {
            return Err("remote executor tunnelPort must be nonzero".into());
        }
        for (name, path) in [
            ("binaryPath", &self.binary_path),
            ("cairnHome", &self.cairn_home),
        ] {
            if path.as_deref().is_some_and(|value| value.trim().is_empty()) {
                return Err(format!("remote executor {name} must not be blank"));
            }
        }
        let mut projects = HashSet::new();
        for project_id in &self.project_ids {
            uuid::Uuid::parse_str(project_id)
                .map_err(|_| format!("remote executor project ID is not a UUID: {project_id}"))?;
            if !projects.insert(project_id) {
                return Err(format!("remote executor repeats project UUID {project_id}"));
            }
        }
        validate_extra_ssh_args(&self.extra_ssh_args)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoteExecutorConfig {
    pub host: String,
    pub ssh_user: String,
    #[serde(default)]
    pub platform: RemotePlatform,
    pub binary_path: String,
    pub cairn_home: String,
    pub executor_id: String,
    pub device_id: String,
    pub display_name: String,
    pub project_ids: Vec<String>,
    pub tunnel_port: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_ssh_args: Vec<String>,
}

impl RemoteExecutorConfig {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("host", self.host.as_str()),
            ("sshUser", self.ssh_user.as_str()),
            ("executorId", self.executor_id.as_str()),
            ("deviceId", self.device_id.as_str()),
            ("displayName", self.display_name.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("remote executor {name} must not be blank"));
            }
        }
        if self.host.starts_with('-') || self.ssh_user.starts_with('-') {
            return Err("remote executor host and sshUser must not begin with '-'".into());
        }
        for (name, path) in [
            ("binaryPath", &self.binary_path),
            ("cairnHome", &self.cairn_home),
        ] {
            if path.trim().is_empty() || !self.platform.is_absolute(path) {
                return Err(format!("remote executor {name} must be an absolute path"));
            }
        }
        if self.executor_id == COLOCATED_EXECUTOR_ID {
            return Err("remote executor cannot reuse the colocated executor identity".into());
        }
        if !is_safe_executor_id(&self.executor_id) {
            return Err("remote executor executorId must start with an ASCII letter or digit and contain only ASCII letters, digits, '.', '_', or '-'".into());
        }
        validate_public_executor_name(&self.display_name)?;
        if self.tunnel_port == 0 {
            return Err("remote executor tunnelPort must be nonzero".into());
        }
        let mut project_ids = HashSet::new();
        for project_id in &self.project_ids {
            uuid::Uuid::parse_str(project_id)
                .map_err(|_| format!("remote executor project ID is not a UUID: {project_id}"))?;
            if !project_ids.insert(project_id) {
                return Err(format!("remote executor repeats project UUID {project_id}"));
            }
        }
        validate_extra_ssh_args(&self.extra_ssh_args)
    }
}

fn validate_extra_ssh_args(args: &[String]) -> Result<(), String> {
    for argument in args {
        if !matches!(argument.as_str(), "-4" | "-6") {
            return Err(format!(
                "remote executor extra SSH argument is not an allowed transport selector (-4 or -6): {argument}"
            ));
        }
    }
    Ok(())
}

/// Whether an operator-supplied label can serve as this machine's public
/// address.
///
/// A remote executor may not claim the reserved local name: agents read that
/// name as "the machine the runner is on", and a remote answering to it would
/// send work that must be local somewhere else entirely.
fn validate_public_executor_name(display_name: &str) -> Result<(), String> {
    let Some(name) = normalize_executor_name(display_name) else {
        return Err(format!(
            "remote executor displayName {display_name:?} yields no public name: {EXECUTOR_NAME_RULE}"
        ));
    };
    if name == LOCAL_EXECUTOR_NAME {
        return Err(format!(
            "remote executor displayName {display_name:?} claims the reserved name {LOCAL_EXECUTOR_NAME}, which addresses the runner's own executor"
        ));
    }
    Ok(())
}

fn is_safe_executor_id(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric())
        && characters.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        })
}

impl FleetConfig {
    pub fn resolved_placement_profiles(&self) -> BTreeMap<String, PlacementProfile> {
        let mut profiles = built_in_placement_profiles();
        profiles.extend(self.placement_profiles.clone());
        profiles
    }

    pub fn active_placement_profile(&self) -> &PlacementProfile {
        self.placement_profiles
            .get(&self.active_placement_profile)
            .or_else(|| built_in_profile(&self.active_placement_profile))
            .expect("FleetConfig is healed at load and validated before save")
    }

    pub fn effective_executor_policy(
        &self,
        executor_id: &str,
    ) -> cairn_common::executor_protocol::ExecutorRuntimePolicy {
        self.active_placement_profile()
            .executor_policy_overrides
            .get(executor_id)
            .cloned()
            .unwrap_or_else(|| self.resolve_executor_policy(executor_id))
    }

    pub fn save_placement_profile(
        &mut self,
        name: String,
        profile: PlacementProfile,
    ) -> Result<(), String> {
        validate_custom_profile_name(&name)?;
        profile.validate(&self.known_profile_executor_ids())?;
        self.placement_profiles.insert(name, profile);
        Ok(())
    }

    pub fn delete_placement_profile(&mut self, name: &str) -> Result<PlacementProfile, String> {
        if is_built_in_profile(name) {
            return Err(format!(
                "built-in placement profile {name} cannot be deleted"
            ));
        }
        if self.active_placement_profile == name {
            return Err(format!("active placement profile {name} cannot be deleted"));
        }
        self.placement_profiles
            .remove(name)
            .ok_or_else(|| format!("placement profile {name} does not exist"))
    }

    pub fn activate_placement_profile(&mut self, name: &str) -> Result<PlacementProfile, String> {
        let profile = self
            .placement_profiles
            .get(name)
            .cloned()
            .or_else(|| built_in_profile(name).cloned())
            .ok_or_else(|| format!("placement profile {name} does not exist"))?;
        self.active_placement_profile = name.to_string();
        Ok(profile)
    }

    fn known_profile_executor_ids(&self) -> HashSet<&str> {
        std::iter::once(LOCAL_EXECUTOR_NAME)
            .chain(self.remote_executors.keys().map(String::as_str))
            .collect()
    }

    /// How a machine is named in durable fleet state.
    ///
    /// The colocated executor has no enrollment to hang a key off, so it is
    /// keyed by the name that addresses it; every other machine is keyed by its
    /// stable identity, which a rename does not move. This is the same key
    /// convention `executor_policies` uses, and drain must not invent a second
    /// one: two spellings of "this machine" is how a drain comes to be held for
    /// a machine nobody can clear it from.
    pub fn drain_key(executor_id: &str) -> &str {
        if executor_id == COLOCATED_EXECUTOR_ID {
            LOCAL_EXECUTOR_NAME
        } else {
            executor_id
        }
    }

    /// Whether this runner is holding a machine drained.
    pub fn is_machine_drained(&self, executor_id: &str) -> bool {
        self.drained_machines.contains(Self::drain_key(executor_id))
    }

    /// Record or clear a drain. Returns whether the stored state changed.
    pub fn set_machine_drained(&mut self, executor_id: &str, drained: bool) -> bool {
        let key = Self::drain_key(executor_id).to_string();
        if drained {
            self.drained_machines.insert(key)
        } else {
            self.drained_machines.remove(&key)
        }
    }

    /// Drop a removed machine's drain along with it, so a name that is enrolled
    /// again does not inherit a refusal nobody asked for.
    pub fn forget_drain(&mut self, executor_id: &str) {
        self.drained_machines.remove(Self::drain_key(executor_id));
    }

    /// Every machine this runner is holding drained, by executor identity.
    pub fn drained_executor_ids(&self) -> Vec<String> {
        self.drained_machines
            .iter()
            .map(|key| {
                if key == LOCAL_EXECUTOR_NAME {
                    COLOCATED_EXECUTOR_ID.to_string()
                } else {
                    key.clone()
                }
            })
            .collect()
    }

    pub fn resolve_executor_policy(
        &self,
        executor_id: &str,
    ) -> cairn_common::executor_protocol::ExecutorRuntimePolicy {
        self.executor_policies
            .get(executor_id)
            .cloned()
            .unwrap_or_else(|| cairn_common::executor_protocol::ExecutorRuntimePolicy {
                cpu_admission: self.cpu_admission,
                ..Default::default()
            })
    }

    /// Replace a wait horizon that is a leftover rather than a policy, at the
    /// one moment the config is read from disk.
    ///
    /// Healing on load rather than at each use, so that every consumer — the
    /// scheduler, the settings API, the Fleet editor the operator is looking at
    /// — sees one effective number. A value corrected only where it is spent
    /// would leave the operator reading 20 in a form whose save button then
    /// refuses it.
    ///
    /// Raising rather than refusing, because this is state that already exists
    /// on disk: refusing would take the fleet down over a number nobody chose,
    /// and honouring it would keep the fleet abandoning work in twenty seconds.
    /// The log line is how the operator learns their file says something they
    /// did not mean; [`Self::validate`] is what keeps a new one from being
    /// written, and the file itself heals on the next fleet-settings save.
    pub(crate) fn healed(mut self) -> Self {
        if self.capacity_wait_horizon_seconds < MIN_CAPACITY_WAIT_HORIZON_SECONDS {
            log::warn!(
                "settings.yaml buildSlots.capacityWaitHorizonSeconds is {}, below the {MIN_CAPACITY_WAIT_HORIZON_SECONDS}s floor; \
                 using {}s instead. A value this small is usually a pre-cairn-3268 acquisitionDeadlineSeconds \
                 carried across the rename, where it meant a per-attempt queue budget rather than a total wait.",
                self.capacity_wait_horizon_seconds,
                default_capacity_wait_horizon_seconds(),
            );
            self.capacity_wait_horizon_seconds = default_capacity_wait_horizon_seconds();
        }
        self.placement_profiles
            .retain(|name, _| !is_built_in_profile(name));
        if !is_built_in_profile(&self.active_placement_profile)
            && !self
                .placement_profiles
                .contains_key(&self.active_placement_profile)
        {
            log::warn!(
                "settings.yaml names missing placement profile {}; using {DEFAULT_PLACEMENT_PROFILE}",
                self.active_placement_profile
            );
            self.active_placement_profile = DEFAULT_PLACEMENT_PROFILE.to_string();
        }
        self
    }

    /// The horizon a caller with no tighter answer of its own waits on.
    pub(crate) fn capacity_wait_horizon_ms(&self) -> u64 {
        self.capacity_wait_horizon_seconds.saturating_mul(1_000)
    }

    pub fn validate(&self) -> Result<(), String> {
        self.cpu_admission.validate()?;
        for (executor_id, policy) in &self.executor_policies {
            policy
                .validate()
                .map_err(|error| format!("executor policy {executor_id}: {error}"))?;
        }
        if self.capacity_wait_horizon_seconds < MIN_CAPACITY_WAIT_HORIZON_SECONDS {
            return Err(format!(
                "capacityWaitHorizonSeconds must be at least {MIN_CAPACITY_WAIT_HORIZON_SECONDS}: \
                 a shorter horizon cannot outlast one ordinary unit of work finishing, so it refuses \
                 every request the machine is merely busy for"
            ));
        }
        let mut device_ids = HashSet::new();
        let mut names: HashMap<String, String> = HashMap::new();
        for (executor_id, remote) in &self.remote_executors {
            remote.validate()?;
            if executor_id != &remote.executor_id {
                return Err(format!(
                    "remote executor inventory key {executor_id} does not match executorId {}",
                    remote.executor_id
                ));
            }
            if !device_ids.insert(&remote.device_id) {
                return Err(format!(
                    "remote executors must not share device identity {}",
                    remote.device_id
                ));
            }
            let name = normalize_executor_name(&remote.display_name)
                .expect("validate() rejects a displayName with no public name");
            if let Some(existing) = names.insert(name.clone(), remote.executor_id.clone()) {
                return Err(format!(
                    "remote executors {existing} and {} both address the public name {name}; rename one with `cairn executor rename`",
                    remote.executor_id
                ));
            }
        }
        if !is_built_in_profile(&self.active_placement_profile)
            && !self
                .placement_profiles
                .contains_key(&self.active_placement_profile)
        {
            return Err(format!(
                "active placement profile {} does not exist",
                self.active_placement_profile
            ));
        }
        let known = self.known_profile_executor_ids();
        for (name, profile) in &self.placement_profiles {
            validate_custom_profile_name(name)?;
            profile.validate(&known)?;
        }
        Ok(())
    }
}

fn deadline_evidence(
    now_unix_ms: u64,
    authoritative_last_progress_unix_ms: u64,
    evidence: ExecutorSubstrateEvidence,
) -> ExecutorSubstrateEvidence {
    if now_unix_ms.saturating_sub(authoritative_last_progress_unix_ms)
        <= EXECUTOR_PROGRESS_FRESHNESS_MS
    {
        evidence
    } else {
        ExecutorSubstrateEvidence {
            state: ExecutorSubstrateState::ConnectedStalled,
            since_unix_ms: authoritative_last_progress_unix_ms,
            last_progress_unix_ms: authoritative_last_progress_unix_ms,
            ..evidence
        }
    }
}

fn check_index_from_stream_id(stream_id: &str) -> Option<usize> {
    stream_id
        .rsplit_once(":check-")
        .and_then(|(_, index)| index.parse().ok())
}

fn format_duration_annotation(duration_ms: u64) -> String {
    if duration_ms >= 1_000 {
        format!("{:.1}s", duration_ms as f64 / 1_000.0)
    } else {
        format!("{duration_ms}ms")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorDisconnectOrigin {
    RunnerInitiated,
    PeerOrIo,
}

/// What a colocated link's observed silence says about whether it can be left
/// alone. `ConnectedStalled` is only a classification; this is the verdict the
/// supervisor acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkRemediation {
    Healthy,
    Bounce {
        /// The connection the verdict was formed about. Carrying it fences the
        /// subsequent teardown against a reattach that lands in between.
        executor_id: String,
        generation: u64,
        cause: LinkStallCause,
    },
}

/// Why a link that never closed has stopped being usable.
///
/// Named rather than collapsed into one duration because the two failures look
/// nothing alike to whoever reads the log or the machine: one went quiet, the
/// other never missed a beat and has been republishing the same history for
/// minutes. An operator told "heartbeat lost" about a machine whose heartbeats
/// all arrived goes looking for the wrong failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkStallCause {
    /// Nothing at all has been recorded on this link.
    Silence {
        /// How long the runner has recorded no progress at all on this link.
        silence_ms: u64,
        /// How long since the socket pump last completed a loop iteration.
        /// Fresh here while `silence_ms` is stale means the executor went quiet;
        /// stale in both means the runner's own pump is wedged. This is the
        /// field that tells those two apart in the log after the fact, and it
        /// only can because the pump stamps it on a timer as well as on each
        /// frame — a clock driven purely by inbound traffic reads identically
        /// for a silent executor and a wedged pump.
        pump_silence_ms: u64,
    },
    /// Beats keep arriving on time; what they carry stopped being recomputed.
    TelemetryFrozen {
        /// How long ago the executor last recomputed the facts its beats carry,
        /// by its own stamp.
        facts_age_ms: u64,
    },
}

impl LinkStallCause {
    /// One line an operator can act on.
    pub fn describe(&self) -> String {
        match self {
            Self::Silence {
                silence_ms,
                pump_silence_ms,
            } => format!(
                "the link recorded nothing for {silence_ms}ms (socket pump last turned over {pump_silence_ms}ms ago)"
            ),
            Self::TelemetryFrozen { facts_age_ms } => format!(
                "heartbeats kept arriving but the facts they carry were last measured {facts_age_ms}ms ago"
            ),
        }
    }

    /// Whether this cause has to wait its turn behind another machine's
    /// recovery. Only frozen telemetry does: it is the failure that arrives on
    /// many machines at once.
    pub fn is_paced(&self) -> bool {
        matches!(self, Self::TelemetryFrozen { .. })
    }
}

/// The bounds a link's two stall verdicts are judged against.
///
/// Injected rather than read from the constants so a verdict is testable
/// without wall-clock sleeps, following [`deadline_evidence`]. They travel as
/// one value because they are judged together and in a fixed order, and because
/// two bare `u64` parameters are two parameters a caller can transpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkStallBounds {
    /// How long total silence is tolerated before the link is bounced.
    pub silence_ms: u64,
    /// How long a link that keeps beating may carry facts that stopped being
    /// recomputed before it is bounced.
    pub telemetry_ms: u64,
}

impl Default for LinkStallBounds {
    fn default() -> Self {
        Self {
            silence_ms: EXECUTOR_LINK_STALL_REMEDIATION_MS,
            telemetry_ms: EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS,
        }
    }
}

struct CoalescedLeaderCompletionGuard {
    pool: Fleet,
    leader: RequestIdentity,
    result_identities: Vec<CheckResultIdentity>,
    runner_context_id: Option<String>,
    armed: bool,
}

impl CoalescedLeaderCompletionGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CoalescedLeaderCompletionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.pool
            .cancelled_leaders
            .lock()
            .unwrap()
            .remove(&self.leader);
        self.pool
            .coalesced_leaders
            .lock()
            .unwrap()
            .remove(&self.leader);
        self.pool
            .preparing_leaders
            .lock()
            .unwrap()
            .remove(&self.leader);
        if let Some(id) = &self.runner_context_id {
            self.pool.runner_contexts.lock().unwrap().remove(id);
        }
        let outcome = CellOutcome::Unavailable {
            reason: CellUnavailableReason::ExecutorUnavailable,
            diagnostic: "coalesced cell leader ended without publishing a terminal outcome".into(),
        };
        for result_identity in &self.result_identities {
            self.pool
                .complete_coalesced_for_leader(result_identity, &self.leader, outcome.clone());
        }
    }
}

impl Default for FleetConfig {
    fn default() -> Self {
        Self {
            active_placement_profile: default_active_placement_profile(),
            placement_profiles: BTreeMap::new(),
            capacity_wait_horizon_seconds: default_capacity_wait_horizon_seconds(),
            default_timeout_seconds: default_timeout_seconds(),
            cpu_admission: Default::default(),
            executor_policies: HashMap::new(),
            remote_executors: BTreeMap::new(),
            remote_host_identities: BTreeMap::new(),
            desktop_automation: DesktopAutomationConfig::default(),
            drained_machines: BTreeSet::new(),
        }
    }
}

fn default_capacity_wait_horizon_seconds() -> u64 {
    10 * 60
}

/// The shortest wait horizon that can be a policy rather than a leftover.
///
/// Below this the number stops describing patience and starts guaranteeing
/// refusal: the fleet's own placement, provisioning, and admission round trip
/// takes seconds on an idle machine, so a horizon under a minute cannot outlast
/// even one ordinary unit of work finishing. Every caller that reaches this
/// default is one with no tighter answer of its own — a terminal, a REPL, a dev
/// instance — and none of them means "give up before anything could plausibly
/// free".
///
/// It exists because a persisted twenty, carried across CAIRN-3268's rename
/// from a field that meant something else, is indistinguishable afterwards from
/// a number an operator typed. This floor is what tells them apart, and
/// [`FleetConfig::validate`] is what stops a new one being written.
pub(crate) const MIN_CAPACITY_WAIT_HORIZON_SECONDS: u64 = 60;
fn default_timeout_seconds() -> u64 {
    // Cold Rust builds in managed cells routinely cross ten minutes. Thirty
    // minutes clears that ordinary floor while keeping setup, residency, and
    // check infrastructure failures bounded and operator-configurable.
    30 * 60
}

/// The wait horizon for a caller with no tighter answer of its own.
///
/// A terminal, a REPL, a workflow, a dev instance: none of these has a
/// principled bound on how long it will wait for a busy machine, only on how
/// long it will wait for a broken one — and silence, not elapsed time, is what
/// says broken. So they declare the machine-wide default and let the liveness
/// report be what frees the queue slot if their caller goes away.
///
/// A batch that stated its own bound does not come here. `run` derives a horizon
/// from the item timeouts the agent declared, because a batch that bounded all of
/// its work said what that work is worth.
pub(crate) fn default_wait_horizon_unix_ms(config: &FleetConfig) -> u64 {
    unix_time_ms().saturating_add(config.capacity_wait_horizon_ms())
}

type RequestIdentity = (String, String);
pub(crate) const COLOCATED_EXECUTOR_ID: &str = "colocated";
const MIN_REQUEST_WATCHDOG_SLACK: Duration = Duration::from_millis(100);
const MAX_REQUEST_WATCHDOG_SLACK: Duration = Duration::from_secs(5);

struct PendingResult {
    executor_id: String,
    generation: u64,
    requesting_job_id: Option<String>,
    waiter: oneshot::Sender<CellOutcome>,
}
type PendingResults = HashMap<RequestIdentity, PendingResult>;

/// What a reattached connection still owes an accounting for: the generation
/// that owes it, and the executions carried across the gap into it.
type HeldExecutions = (u64, HashSet<RequestIdentity>);

struct PendingLifetimeResult {
    executor_id: String,
    generation: u64,
    waiter: oneshot::Sender<ResidencyResult>,
    /// The executor-side queue entry this operation occupies, when it takes one.
    ///
    /// Only an acquisition queues; every other residency operation is bounded by
    /// the work it names and never enters admission. Naming the entry is what
    /// lets the runner report that it is still waiting for this acquisition
    /// specifically, so a long horizon on it does not read as a phantom.
    queue_entry_id: Option<String>,
}
type PendingResidencyResults = HashMap<String, PendingLifetimeResult>;

struct PendingMaterializationRead {
    executor_id: String,
    generation: u64,
    waiter: oneshot::Sender<MaterializationReadResult>,
}

type PendingMaterializationReads = HashMap<String, PendingMaterializationRead>;

struct PendingPolicyResult {
    executor_id: String,
    generation: u64,
    waiter: oneshot::Sender<Result<cairn_common::executor_protocol::ExecutorRuntimePolicy, String>>,
}

struct PendingDrainResult {
    executor_id: String,
    generation: u64,
    waiter: oneshot::Sender<Result<bool, String>>,
}

use crate::execution::cache::CheckResultIdentity;

struct CoalescedSubscriber {
    waiter: oneshot::Sender<CoalescedCellOutcome>,
    priority: CellPriority,
    requesting_job_id: Option<String>,
}

struct InFlightExecution {
    leader: RequestIdentity,
    subscribers: HashMap<RequestIdentity, CoalescedSubscriber>,
    publication: PublicationCoordination,
}

#[derive(Default)]
struct InFlightRegistry {
    by_key: HashMap<CheckResultIdentity, InFlightExecution>,
    subscriber_keys: HashMap<RequestIdentity, CheckResultIdentity>,
}

#[derive(Clone, Debug)]
pub(crate) struct PublicationCoordination {
    state: Arc<PublicationState>,
}

#[derive(Debug)]
struct PublicationState {
    claimed: AtomicBool,
    published: AtomicBool,
    /// What the publisher recorded, so every coalesced sibling can name the same
    /// observation. Written before `published` is set and read after it is
    /// observed, so the release/acquire pair also publishes this value.
    observation: std::sync::Mutex<Option<crate::execution::cache::RecordedCheckObservation>>,
    notify: Notify,
}

pub(crate) struct PublicationGuard {
    coordination: PublicationCoordination,
    published: bool,
}

pub(crate) enum PublicationRole {
    Publisher(PublicationGuard),
    /// A sibling already recorded this verdict, and named the observation it
    /// wrote. `None` means it recorded nothing — the write failed, and the
    /// verdict stands without a durable row behind it.
    Published(Option<crate::execution::cache::RecordedCheckObservation>),
}

pub(crate) struct CoalescedCellOutcome {
    pub outcome: CellOutcome,
    pub publication: PublicationCoordination,
}

pub(crate) struct PureVerdictBatchItem {
    pub result_identity: CheckResultIdentity,
    pub process: ProcessBatchItem,
}

impl PublicationCoordination {
    fn new() -> Self {
        Self {
            state: Arc::new(PublicationState {
                claimed: AtomicBool::new(false),
                published: AtomicBool::new(false),
                observation: std::sync::Mutex::new(None),
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) async fn acquire(&self) -> PublicationRole {
        loop {
            if self.state.published.load(Ordering::Acquire) {
                return PublicationRole::Published(self.state.observation.lock().unwrap().clone());
            }
            if self
                .state
                .claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return PublicationRole::Publisher(PublicationGuard {
                    coordination: self.clone(),
                    published: false,
                });
            }
            self.state.notify.notified().await;
        }
    }
}

impl PublicationGuard {
    /// Declare this verdict recorded, naming the observation written for it so a
    /// coalesced sibling reports the same row rather than none.
    pub(crate) fn published(
        mut self,
        observation: Option<crate::execution::cache::RecordedCheckObservation>,
    ) {
        *self.coordination.state.observation.lock().unwrap() = observation;
        self.coordination
            .state
            .published
            .store(true, Ordering::Release);
        self.coordination.state.notify.notify_waiters();
        self.published = true;
    }
}

impl Drop for PublicationGuard {
    fn drop(&mut self) {
        if !self.published {
            self.coordination
                .state
                .claimed
                .store(false, Ordering::Release);
            self.coordination.state.notify.notify_waiters();
        }
    }
}

type ResidentProcessSubscriber = Arc<dyn Fn(ResidentProcessEvent) + Send + Sync>;

/// What the runner knows about an enrolled machine independently of whether it
/// is attached: who it is, and the account of the last time the runner tried to
/// bring it up.
///
/// This outlives every connection to the machine on purpose. A record that only
/// existed while the link was up could not describe a link that is down, which
/// is the state the fleet most needs described.
#[derive(Debug, Clone)]
struct EnrolledRemoteRecord {
    name: String,
    os: String,
    arch: String,
    link: RemoteLinkState,
    last_attempt: Option<RemoteAttachAttempt>,
    last_seen_unix_ms: Option<u64>,
    connection_timeline: VecDeque<RemoteConnectionTransition>,
}

const REMOTE_CONNECTION_TIMELINE_LIMIT: usize = 32;

const EXPLORATION_INTERVAL: usize = 4;

#[derive(Debug, Clone)]
struct ExplorationState {
    decisions_since_exploration: usize,
    /// Least recently explored first. Executors not present have never received
    /// a bootstrap attempt and precede every entry here.
    explored_executors: VecDeque<String>,
    /// Least recently selected first. Missing executors have never won and sort
    /// ahead of every executor present here when all authoritative keys tie.
    selected_executors: VecDeque<String>,
}

impl Default for ExplorationState {
    fn default() -> Self {
        Self {
            decisions_since_exploration: EXPLORATION_INTERVAL - 1,
            explored_executors: VecDeque::new(),
            selected_executors: VecDeque::new(),
        }
    }
}

impl ExplorationState {
    fn record(&mut self, explored_executor_id: Option<&str>) {
        match explored_executor_id {
            Some(executor_id) => {
                self.decisions_since_exploration = 0;
                self.explored_executors.retain(|known| known != executor_id);
                self.explored_executors.push_back(executor_id.to_string());
            }
            None => {
                self.decisions_since_exploration = self
                    .decisions_since_exploration
                    .saturating_add(1)
                    .min(EXPLORATION_INTERVAL - 1);
            }
        }
    }

    fn record_selection(&mut self, executor_id: &str) {
        self.selected_executors.retain(|known| known != executor_id);
        self.selected_executors.push_back(executor_id.to_string());
    }
}

/// What a drain write reached.
///
/// A drain is durable runner state either way: the runner has stopped placing
/// work on the machine the moment it is recorded. This says only whether the
/// machine itself was told, which is a courtesy the runner extends when it can
/// and reports honestly when it cannot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "application")]
pub enum DrainApplication {
    /// The executor acknowledged the change on its own admission path too.
    Applied,
    /// The runner holds the drain and the machine does not know yet.
    HeldWithoutMachine { reason: String },
}

/// How long a successor waits for a displaced predecessor to confirm death
/// before dispatching anyway.
///
/// An escape hatch, not a schedule. An ordinary retirement resolves in the time
/// a message takes to cross the link plus the time the item takes to reach its
/// next phase transition. This budget exists so that a predecessor which can
/// never report — its machine gone, its link severed beyond the watchdog's
/// reach — cannot silently stop a project from ever being attested again.
/// Reaching it is worth a log line, because at that point the guarantee has
/// genuinely lapsed and the overlap it prevents may be real.
const RETIREMENT_CONFIRMATION_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

#[derive(Clone, Default)]
pub struct Fleet {
    connections: Arc<Mutex<HashMap<String, ExecutorConnectionState>>>,
    connection_generations: Arc<Mutex<HashMap<String, u64>>>,
    disconnect_origins: Arc<Mutex<HashMap<(String, u64), ExecutorDisconnectOrigin>>>,
    connection_ready: Arc<tokio::sync::Notify>,
    pending: Arc<Mutex<PendingResults>>,
    /// Executions carried across a link gap that the reattached connection has
    /// not yet accounted for, with the generation that owes the accounting.
    ///
    /// Emptied by the first snapshot of that generation, which is the machine's
    /// own statement of what it still has.
    unreconciled_executions: Arc<Mutex<HashMap<String, HeldExecutions>>>,
    pending_residency: Arc<Mutex<PendingResidencyResults>>,
    pending_materialization_reads: Arc<Mutex<PendingMaterializationReads>>,
    pending_policy: Arc<Mutex<HashMap<String, PendingPolicyResult>>>,
    desired_runtime_policies:
        Arc<Mutex<HashMap<String, cairn_common::executor_protocol::ExecutorRuntimePolicy>>>,
    pending_drain: Arc<Mutex<HashMap<String, PendingDrainResult>>>,
    /// Machines this runner is holding drained, by executor identity.
    ///
    /// The in-memory face of [`FleetConfig::drained_machines`], hydrated when
    /// the runner reads its fleet settings and updated by every drain write.
    /// Placement and attach both consult it, so neither has to read disk and
    /// neither depends on the machine being reachable enough to be asked.
    drained: Arc<Mutex<HashSet<String>>>,
    residency_routes: Arc<Mutex<HashMap<(String, String), ResidencyRoute>>>,
    residency_route_path: Arc<Option<PathBuf>>,
    residency_route_store_error: Arc<Mutex<Option<String>>>,
    /// Environments this runner rebuilt because the executor session holding the
    /// previous one ended, keyed by holder and drained by the surface that
    /// reports it. Held in memory on purpose: it is a fact about this runner's
    /// current session, and an unreported one is worth less than a stale one
    /// resurfacing days later.
    environment_evictions: Arc<Mutex<HashMap<String, EnvironmentEviction>>>,
    /// When each absent executor was first observed to be absent, which is what
    /// separates a link that flapped from a machine that left. Stamped when a
    /// connection goes away and when an acquisition first finds a route with no
    /// connection behind it; cleared when the executor attaches again.
    link_absence_since: Arc<Mutex<HashMap<String, u64>>>,
    /// When the runner last bounced a link for publishing frozen facts.
    ///
    /// Facts freezing on several machines within one window is the specimen
    /// this remediation exists for, and it is also the case where recycling all
    /// of them together is worst. The trigger is shared, so a simultaneous
    /// bounce reruns whatever caused it on the whole fleet at once, drops every
    /// in-flight attempt in one window, and hands the survivors the entire
    /// load. One machine at a time, spaced, recovers capacity just as fast in
    /// the case that matters and leaves a legible sequence behind instead of a
    /// cliff.
    ///
    /// Silence bounces are deliberately not paced through here: a link nobody
    /// has heard from is already gone, and making its replacement wait behind
    /// another machine's recovery would only extend the outage.
    last_telemetry_bounce_unix_ms: Arc<Mutex<u64>>,
    /// One acquisition flight per execution environment, keyed by holder.
    /// [`Fleet::residency_acquire_flight`] owns both the keying and the pruning.
    residency_acquisitions: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    resident_process_subscribers: Arc<Mutex<Vec<ResidentProcessSubscriber>>>,
    /// At most one live work item per question. The single chokepoint fact that
    /// makes duplicate answers unrepresentable rather than merely suppressed;
    /// see [`purpose`].
    purposes: Arc<Mutex<purpose::PurposeRegistry>>,
    /// Woken whenever a terminal outcome clears a purpose debt, so a successor
    /// waiting on its predecessor's death learns of it rather than polling.
    purpose_quiesced: Arc<tokio::sync::Notify>,
    cancelled_leaders: Arc<Mutex<HashSet<RequestIdentity>>>,
    coalesced_leaders: Arc<Mutex<HashSet<RequestIdentity>>>,
    preparing_leaders: Arc<Mutex<HashMap<RequestIdentity, LeaderPreparation>>>,
    in_flight: Arc<Mutex<InFlightRegistry>>,
    runner_contexts: Arc<Mutex<HashMap<String, RunnerContextEntry>>>,
    /// Streaming scrubbers for resident-process output, one per live stream.
    ///
    /// Terminals, REPLs, workflows, and dev instances all reach their consumers
    /// through one fan-out, and all of them are handed this runner's own
    /// `CAIRN_MCP_SECRET`. An executor cannot scrub for a credential it was
    /// never told about, so this is the pass that covers them.
    resident_output: output_guard::SharedRelayedOutput,
    recent_cached_completions:
        Arc<Mutex<VecDeque<cairn_common::executor_protocol::CellCompletion>>>,
    expected_executor_build_ids: Arc<Mutex<HashMap<String, String>>>,
    /// The placement decisions this runner most recently took, newest last.
    recent_placements: Arc<Mutex<VecDeque<PlacementDecision>>>,
    /// Request attempts whose transition into executor admission has been
    /// journaled. Missing placement evidence is retried on later snapshots; it
    /// is never made durable as a partial record.
    journaled_admission_parks: Arc<Mutex<HashSet<RequestIdentity>>>,
    /// Atomic cadence accounting, separate from the operator ledger because a
    /// placement must consume its exploration slot before concurrent ranking can
    /// spend the same slot.
    exploration_by_work_class: Arc<Mutex<HashMap<PlacementWorkClass, ExplorationState>>>,
    /// Rate limit for the pushed telemetry frame. One per runner rather than
    /// one per connection: an N-machine fleet beating independently would
    /// otherwise emit at N times the intended cadence, each connection
    /// believing it was within its own budget.
    telemetry_cadence: Arc<telemetry::TelemetryCadence>,
    colocated_substrate_state: Arc<Mutex<Option<ExecutorSubstrateEvidence>>>,
    /// Every machine this runner is enrolled with, keyed by executor id and
    /// kept whether or not the machine is attached. Always locked AFTER
    /// `connections` where both are needed, so the two orders cannot deadlock.
    enrolled_remotes: Arc<Mutex<HashMap<String, EnrolledRemoteRecord>>>,
    /// Who manages this fleet and what enrollments are in flight. Held here
    /// because management is fleet state: the same projection that answers what
    /// machines exist has to answer what is currently becoming one.
    management: Arc<management::ExecutorManagementState>,
}

/// A live acquisition flight for one execution environment. Dropping it hands
/// the gate to the next acquirer of the SAME environment and, when there is no
/// next one, takes the map entry out with it — so a runner that has served a
/// thousand jobs holds one entry per live acquisition rather than one per job it
/// ever ran.
struct ResidencyAcquireFlight {
    flights: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    key: String,
    gate: Arc<tokio::sync::Mutex<()>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for ResidencyAcquireFlight {
    fn drop(&mut self) {
        drop(self.guard.take());
        let mut flights = self.flights.lock().unwrap();
        // Two strong references — the map's and this flight's — mean nobody else
        // holds or waits on this gate, so the entry is spent. Every other
        // acquirer takes its clone under this same lock, so the count cannot
        // change underneath the check.
        if Arc::strong_count(&self.gate) == 2 {
            flights.remove(&self.key);
        }
    }
}

/// How long the runner waits in SILENCE for an executor to answer a residency
/// operation.
///
/// It bounds the link, not the work. The budget is renewed for every interval in
/// which the executor reports progress on this operation, so an acquisition that
/// spends ten minutes provisioning a cold cell is waited out in full; what this
/// bounds is an executor that stopped answering at all.
///
/// It is also what lets the executor's own answer win the race. When a queued
/// acquisition reaches its wait horizon the executor answers with the substrate
/// evidence it collected, and at that instant the runner has just observed the
/// entry in the queue — so it still holds a full budget of patience and receives
/// the diagnosis rather than manufacturing its own.
const RESIDENCY_RESPONSE_FLOOR_MS: u64 = 30_000;

/// What a wait bounded by silence rather than by elapsed time ended up doing.
enum SilenceWatchdog<T> {
    /// The other side answered.
    Answered(T),
    /// The response channel closed without an answer.
    Dropped,
    /// Nothing reported progress for a whole silence budget.
    Silent,
}

#[derive(Clone)]
struct RunnerCallbackContext {
    request: Option<crate::mcp::types::McpCallbackRequest>,
    run_context: Option<crate::mcp::handlers::RunContext>,
    check_status_board: Option<crate::execution::checks::CheckStatusBoard>,
    /// Whether this batch runs in the project's externally owned live checkout.
    ///
    /// Stated from the submitted repository locator, never inferred from the
    /// filesystem: a cell checkout is a plain detached git checkout carrying no
    /// `.jj` marker, so a marker test calls every cell the live checkout and
    /// answers a cell's sandbox denial with a read-only-checkout explanation
    /// instead of the fence prompt the agent's dial asked for.
    live_checkout: bool,
    /// The only executor session allowed to exercise this short-lived capability.
    /// Populated after placement and before the process batch is submitted.
    executor_binding: Option<RunnerContextExecutorBinding>,
    /// Streaming scrubbers for this batch's relayed output, one per item stream.
    ///
    /// Shared with the submitting future, which drains them at the batch's
    /// terminal outcome. It has to be shared rather than owned here, because
    /// this entry is often revoked *before* that future returns — the result
    /// message revokes the context on its way in — and a scrubber dropped
    /// without a flush silently truncates the tail of the stream.
    output: output_guard::SharedRelayedOutput,
}

/// One live runner callback context and the registration that hides its
/// capability.
///
/// The key of this map *is* the batch's relay capability: the executor injects
/// it into every item as `CAIRN_MCP_SECRET`, so a batch that dumps its own
/// environment echoes it straight back. Registering it inside the entry means
/// its scrub lifetime is exactly its authorization lifetime — batch completion,
/// `revoke_runner_contexts_for_request`, `revoke_mcp_relay_contexts_for_executor`
/// and the coalesced-leader guard all remove from this map, so none of them can
/// forget to release it. That is the CAIRN-3385 property held by construction
/// rather than by four call sites agreeing.
struct RunnerContextEntry {
    context: RunnerCallbackContext,
    /// Held, never read: dropping it is the whole contract.
    _capability: Option<crate::security::SecretGuard<'static>>,
}

impl RunnerContextEntry {
    fn new(runner_context_id: &str, batch_label: &str, context: RunnerCallbackContext) -> Self {
        use crate::security::{registry, SecretCategory, SecretId, SecretMaterial};

        // Non-secret metadata: the id reaches logs and detection reports, so it
        // names the batch and never the capability. Deriving it from the value
        // would publish the capability to exactly the places the registry exists
        // to keep it out of.
        //
        // The counter is what makes it *unique*, and that is load-bearing rather
        // than tidy. Registering an id that already exists takes a second hold
        // and REPLACES the stored forms, so two live contexts spelling one id
        // would leave the first capability unregistered-for while it is still
        // minted, injected, and authorized. Batch identity alone does not
        // guarantee distinctness — the coalesced path names a request with no
        // attempt — so the id carries a process-local sequence instead of
        // relying on callers never colliding.
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = SecretId::new(format!("batch-capability:{batch_label}:{sequence}"));
        let capability = match registry().register(
            id.clone(),
            SecretCategory::BatchCapability,
            "per-batch executor relay capability",
            SecretMaterial::from_string(runner_context_id.to_string()),
        ) {
            Ok(guard) => Some(guard),
            Err(error) => {
                // Loud: the capability is minted and sent either way, so a
                // producer that believes it registered and did not believes in a
                // guarantee it does not have.
                log::warn!(
                    "per-batch relay capability {id} was not registered for scrubbing: {error}"
                );
                None
            }
        };
        Self {
            context,
            _capability: capability,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RunnerContextExecutorBinding {
    executor_id: String,
    generation: u64,
    request_id: String,
    attempt_id: String,
}

/// Scrub the free-form text an executor reports with a terminal outcome.
///
/// This is the model-visible copy of a batch's output, and unlike the live
/// stream it arrives whole, so one exact pass covers it completely with no
/// chunk boundaries to reason about. The diagnostics are included because a
/// spawn failure, a preparation timeout, or a storage error quotes the command
/// and whatever it managed to print.
fn scrub_cell_outcome(outcome: &mut CellOutcome) {
    use crate::security::{Crossing, DetectionReport, Sanitizer};

    let mut sanitizer = Sanitizer::exact();
    if sanitizer.is_noop() {
        return;
    }
    match outcome {
        CellOutcome::Completed { output, .. } => sanitizer.text_in_place(output),
        CellOutcome::Unavailable { diagnostic, .. }
        | CellOutcome::FailedAfterExecution { diagnostic, .. }
        | CellOutcome::StorageFailure { diagnostic, .. } => sanitizer.text_in_place(diagnostic),
        CellOutcome::Cancelled { .. } => {}
    }
    for report in DetectionReport::from_detections(
        &sanitizer.into_detections(),
        Crossing::ProcessOutput,
        None,
        None,
    ) {
        report.log();
    }
}

/// Whether a submitted batch runs in the project's externally owned live
/// checkout, as opposed to a cell the executor materialized.
///
/// The locator states it. Nothing on disk does: a cell checkout is a plain
/// detached git checkout, indistinguishable from the user's own, so a
/// `.jj`-marker test answers "live checkout" for every cell in the fleet.
fn runs_in_live_checkout(repository: &RepositoryLocator) -> bool {
    matches!(repository, RepositoryLocator::ExistingCheckout { .. })
}

struct PreparedExecution {
    executor_config: ExecutorConfig,
    object_plane: Arc<crate::orchestrator::object_plane::ObjectPlaneState>,
    db: Arc<cairn_db::storage::LocalDb>,
    /// This machine's build-service client env, or empty when this batch must
    /// not be pointed at the supervised daemon. Resolved here because the
    /// runner owns the service configuration; applied only for a colocated
    /// placement, because the daemon answers on loopback and is named by this
    /// machine's paths. See [`cell_build_service_env`].
    cell_client_env: Vec<(String, String)>,
    placement_policy: ActivePlacementPolicy,
}

#[derive(Clone)]
struct ActivePlacementPolicy {
    name: String,
    profile: PlacementProfile,
}

impl ActivePlacementPolicy {
    #[cfg(test)]
    fn default_profile() -> Self {
        Self {
            name: DEFAULT_PLACEMENT_PROFILE.to_string(),
            profile: built_in_profile(DEFAULT_PLACEMENT_PROFILE).unwrap().clone(),
        }
    }
}

/// The build-service client env a cell batch should carry.
///
/// A cell the executor materializes builds inside `{cairnHome}/build-slots`,
/// which is a managed build root: the daemon's writable grant covers its
/// `target/` tree, so its compiles belong on the shared compile cache. A batch
/// running in the project's live checkout does not, and this is not a question
/// of losing a cache hit — the daemon runs each cache-miss compile itself, so a
/// build whose `target/` its sandbox does not cover fails outright with
/// `Operation not permitted`. That is the split the Cairn-specific sccache port
/// exists to keep: the developer's own checkout starts its own unconfined server
/// on sccache's default port instead.
fn cell_build_service_env(
    orch: &Orchestrator,
    repository: &RepositoryLocator,
) -> Vec<(String, String)> {
    if runs_in_live_checkout(repository) {
        return Vec::new();
    }
    let mut env: Vec<(String, String)> = orch.cell_build_service_client_env().into_iter().collect();
    env.sort();
    env
}

/// Add the machine's build-service client env to every item of a batch bound for
/// a colocated cell. An item that already names a variable keeps its own value:
/// a caller that stated it meant it.
fn with_cell_client_env(mut batch: ProcessBatch, env: &[(String, String)]) -> ProcessBatch {
    for item in &mut batch.items {
        for (key, value) in env {
            if !item.env.iter().any(|(existing, _)| existing == key) {
                item.env.push((key.clone(), value.clone()));
            }
        }
    }
    batch
}

#[derive(Clone, Copy)]
struct LeaderPreparation {
    since_unix_ms: u64,
    last_progress_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ResidencyRoute {
    holder: ResidencyHolder,
    repository: RepositoryLocator,
    executor_id: String,
    pending: bool,
}

/// How long an execution environment outlives its executor's link being down
/// before the runner concludes the environment is gone and rebuilds it.
///
/// This is the whole distance between a link that flapped and a machine that
/// left, and both mistakes it sits between are real. Too short and a reconnect
/// window costs an agent its uncommitted files and resident processes; too long
/// and a job is parked on a machine that is never coming back — which is the
/// failure this bound was introduced to end, and which ran for hours.
///
/// Five minutes, matching the grace an executor gives an owner that stopped
/// answering before reclaiming its cell: the two are the same judgement made
/// from opposite sides of the link, and they should not disagree about how long
/// silence takes to mean absence.
const ENVIRONMENT_LEASE_GRACE_MS: u64 = 5 * 60 * 1000;

/// An execution environment the runner rebuilt because the executor session
/// holding the previous one ended.
///
/// Recorded so the loss is stated rather than absorbed. A rebuilt environment is
/// a fresh checkout at the state the durable side holds, so committed work is
/// intact and everything only the old environment had — uncommitted files,
/// installed packages, resident shells — is not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EnvironmentEviction {
    /// The machine the lost environment was resident on, by the name a
    /// placement request would address it with.
    pub(crate) executor: String,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistentResidencyRoutes {
    #[serde(default)]
    routes: Vec<ResidencyRoute>,
}

impl Fleet {
    /// Authorize one MCP request relayed by an executor-local callback endpoint.
    ///
    /// The context token is a capability, but possession is not sufficient: it
    /// is bound to the executor session selected for the originating process
    /// batch, and the independently carried run identity must still match.
    pub fn authorize_mcp_relay(
        &self,
        executor_id: &str,
        generation: u64,
        runner_context_id: &str,
        request: &cairn_common::protocol::CallbackRequest,
    ) -> Result<(), String> {
        if request.thread_id.is_some() {
            return Err("relayed MCP requests cannot select a thread identity".into());
        }
        let contexts = self.runner_contexts.lock().unwrap();
        let context = contexts
            .get(runner_context_id)
            .map(|entry| &entry.context)
            .ok_or_else(|| "unknown or expired runner callback context".to_string())?;
        let binding = context.executor_binding.as_ref().ok_or_else(|| {
            "runner callback context is not bound to an active process batch".to_string()
        })?;
        if binding.executor_id != executor_id || binding.generation != generation {
            return Err("runner callback context belongs to a different executor session".into());
        }
        let expected_run_id = context
            .request
            .as_ref()
            .and_then(|request| request.run_id.as_deref())
            .or_else(|| {
                context
                    .run_context
                    .as_ref()
                    .map(|context| context.run_id.as_str())
            });
        if request.run_id.as_deref() != expected_run_id || expected_run_id.is_none() {
            return Err("relayed MCP request run identity does not match its process batch".into());
        }
        Ok(())
    }

    fn bind_runner_context(
        &self,
        batch: Option<&ProcessBatch>,
        request: &CellRequest,
        executor_id: &str,
        generation: u64,
    ) -> Result<(), String> {
        let Some(context_id) = batch.and_then(|batch| batch.runner_context_id.as_deref()) else {
            return Ok(());
        };
        let mut contexts = self.runner_contexts.lock().unwrap();
        let context = contexts
            .get_mut(context_id)
            .map(|entry| &mut entry.context)
            .ok_or_else(|| "process batch names an unknown runner callback context".to_string())?;
        context.executor_binding = Some(RunnerContextExecutorBinding {
            executor_id: executor_id.to_string(),
            generation,
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
        });
        Ok(())
    }

    pub fn revoke_runner_contexts_for_request(
        &self,
        request_id: &str,
        attempt_id: &str,
    ) -> Vec<String> {
        let mut revoked = Vec::new();
        self.runner_contexts.lock().unwrap().retain(|id, entry| {
            let matches = entry
                .context
                .executor_binding
                .as_ref()
                .is_some_and(|binding| {
                    binding.request_id == request_id && binding.attempt_id == attempt_id
                });
            if matches {
                revoked.push(id.clone());
            }
            !matches
        });
        revoked
    }

    pub fn revoke_mcp_relay_contexts_for_executor(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> Vec<String> {
        let mut revoked = Vec::new();
        self.runner_contexts.lock().unwrap().retain(|id, entry| {
            let matches = entry
                .context
                .executor_binding
                .as_ref()
                .is_some_and(|binding| {
                    binding.executor_id == executor_id && binding.generation == generation
                });
            if matches {
                revoked.push(id.clone());
            }
            !matches
        });
        // A link that dies mid-stream never delivers the exits that would have
        // drained its resident processes, so their scrubbers are dropped here
        // rather than left to accumulate.
        self.resident_output
            .lock()
            .unwrap()
            .forget_executor(executor_id);
        revoked
    }

    /// Scrub one resident-process event, and drain the process when it exits.
    ///
    /// Returns what should reach subscribers: usually the one event, scrubbed;
    /// on exit, the withheld tail of each of the process's streams *first*, so
    /// the last bytes land in the scrollback the exit is about to seal. An
    /// output event whose content was entirely withheld yields nothing at all,
    /// since a chunk holding no bytes is not yet a chunk.
    fn settle_resident_event(
        &self,
        executor_id: &str,
        mut event: ResidentProcessEvent,
    ) -> Vec<ResidentProcessEvent> {
        let holder_key = event.holder.storage_key();
        match &mut event.event {
            ResidentProcessEventKind::Output { stream, data, .. } => {
                let key = output_guard::resident_stream_key(
                    executor_id,
                    &holder_key,
                    &event.process_key,
                    event.process_generation,
                    *stream,
                );
                let settled = self
                    .resident_output
                    .lock()
                    .unwrap()
                    .settle_bytes(&key, data);
                if settled.is_empty() {
                    return Vec::new();
                }
                *data = settled;
                vec![event]
            }
            ResidentProcessEventKind::State {
                status: cairn_common::executor_protocol::ResidentProcessStatus::Exited { .. },
            } => {
                let tails = output_guard::finish_resident_process(
                    &self.resident_output,
                    executor_id,
                    &holder_key,
                    &event.process_key,
                    event.process_generation,
                );
                let mut sequence = u64::MAX - tails.len() as u64;
                let mut settled: Vec<ResidentProcessEvent> = tails
                    .into_iter()
                    .map(|(stream, data)| {
                        sequence += 1;
                        ResidentProcessEvent {
                            // The tail carries the exiting process's own
                            // identity, so a consumer keyed on the process does
                            // not have to special-case it.
                            event: ResidentProcessEventKind::Output {
                                sequence,
                                stream,
                                data,
                            },
                            ..event.clone()
                        }
                    })
                    .collect();
                settled.push(event);
                settled
            }
            ResidentProcessEventKind::State { .. } => vec![event],
        }
    }

    /// Whether a resident-process event still describes live work.
    ///
    /// The cached cell snapshot cannot answer that question by existence. A
    /// snapshot and a process event are two streams out of one executor with no
    /// ordering between them — the executor's protocol writer selects over its
    /// control channel and its event channel — so an event routinely reaches
    /// the runner before the snapshot that would vouch for it. Requiring the
    /// snapshot to already name the process at that generation made delivery a
    /// coin flip for processes whose whole life is shorter than that skew, and
    /// the event most often lost is the exit, which is the one a caller is
    /// parked on (CAIRN-3444).
    ///
    /// So the snapshot is consulted for what it can answer: contradiction, not
    /// existence. An event is refused when the runner knows something strictly
    /// newer at the same address — a later cell epoch for that holder, or a
    /// later generation at that process key — and admitted otherwise. Every
    /// subscriber matches the exact fence and generation it holds, so precision
    /// lives there; this gate exists to keep a superseded or foreign link's
    /// events from reaching any of them.
    fn resident_event_is_current(
        &self,
        executor_id: &str,
        generation: u64,
        event: &ResidentProcessEvent,
    ) -> bool {
        let connections = self.connections.lock().unwrap();
        let Some(connection) = connections
            .get(executor_id)
            .filter(|connection| connection.generation == generation)
        else {
            return false;
        };
        !connection.snapshot.cells.iter().any(|cell| {
            let holds_event = cell
                .residency
                .as_ref()
                .is_some_and(|residency| residency.holder == event.holder);
            if !holds_event {
                return false;
            }
            if cell.cell_epoch > event.cell_epoch {
                return true;
            }
            cell.cell_epoch == event.cell_epoch
                && cell
                    .occupancy
                    .processes
                    .get(&event.process_key)
                    .is_some_and(|process| process.generation > event.process_generation)
        })
    }

    fn residency_silence_diagnostic(&self, executor_id: &str, generation: u64) -> String {
        let now = unix_time_ms();
        let connections = self.connections.lock().unwrap();
        let Some(entry) = connections
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
        else {
            return "residency operation lost its executor connection generation".into();
        };
        let pump_silence_ms = now.saturating_sub(entry.pump_tick.load(Ordering::Relaxed));
        let report_silence_ms = now.saturating_sub(entry.last_progress_unix_ms);
        if pump_silence_ms > EXECUTOR_PROGRESS_FRESHNESS_MS {
            format!(
                "runner socket pump stopped reporting residency progress ({pump_silence_ms}ms silent)"
            )
        } else if report_silence_ms > EXECUTOR_PROGRESS_FRESHNESS_MS {
            format!(
                "executor snapshot stream stopped reporting residency progress ({report_silence_ms}ms silent)"
            )
        } else {
            "executor admission queue did not publish residency operation progress".into()
        }
    }

    pub(crate) fn subscribe_resident_process_events(
        &self,
        subscriber: impl Fn(ResidentProcessEvent) + Send + Sync + 'static,
    ) {
        self.resident_process_subscribers
            .lock()
            .unwrap()
            .push(Arc::new(subscriber));
    }

    pub(crate) fn with_residency_route_path(path: PathBuf) -> Self {
        // Routes recorded under the lease shape name owners this build cannot
        // address, and the cells behind them are retired at adoption anyway.
        // Remove the file rather than carrying a second parser for it.
        if let Some(parent) = path.parent() {
            let _ = std::fs::remove_file(parent.join("build-slot-lifetime-routes.json"));
        }
        let pool = Self {
            residency_route_path: Arc::new(Some(path.clone())),
            ..Self::default()
        };
        match load_residency_routes(&path) {
            Ok(routes) => *pool.residency_routes.lock().unwrap() = routes,
            Err(error) => *pool.residency_route_store_error.lock().unwrap() = Some(error),
        }
        pool
    }

    fn update_residency_routes<R>(
        &self,
        mutation: impl FnOnce(&mut HashMap<(String, String), ResidencyRoute>) -> R,
    ) -> Result<R, String> {
        let mut routes = self.residency_routes.lock().unwrap();
        let previous = routes.clone();
        let result = mutation(&mut routes);
        if *routes == previous {
            return Ok(result);
        }
        if let Some(path) = self.residency_route_path.as_ref() {
            if let Err(error) = persist_residency_routes(path, &routes) {
                *routes = previous;
                *self.residency_route_store_error.lock().unwrap() = Some(error.clone());
                return Err(error);
            }
        }
        *self.residency_route_store_error.lock().unwrap() = None;
        Ok(result)
    }

    fn ensure_residency_route_store_available(&self) -> Result<(), String> {
        if self.residency_route_store_error.lock().unwrap().is_none() {
            return Ok(());
        }

        let Some(path) = self.residency_route_path.as_ref() else {
            *self.residency_route_store_error.lock().unwrap() = None;
            return Ok(());
        };
        let mut routes = self.residency_routes.lock().unwrap();
        if self.residency_route_store_error.lock().unwrap().is_none() {
            return Ok(());
        }

        let recovered = load_residency_routes(path)
            .and_then(|recovered| persist_residency_routes(path, &recovered).map(|()| recovered));
        match recovered {
            Ok(recovered) => {
                *routes = recovered;
                *self.residency_route_store_error.lock().unwrap() = None;
                Ok(())
            }
            Err(error) => {
                *self.residency_route_store_error.lock().unwrap() = Some(error.clone());
                Err(error)
            }
        }
    }
}

fn load_residency_routes(path: &Path) -> Result<HashMap<(String, String), ResidencyRoute>, String> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let bytes = std::fs::read(path)
        .map_err(|error| format!("read residency route authority {}: {error}", path.display()))?;
    let persisted: PersistentResidencyRoutes = serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "parse residency route authority {}: {error}",
            path.display()
        )
    })?;
    let mut routes = HashMap::new();
    for route in persisted.routes {
        let key = (route.executor_id.clone(), route.holder.storage_key());
        if routes.insert(key, route).is_some() {
            return Err(format!(
                "residency route authority {} contains duplicate routes",
                path.display()
            ));
        }
    }
    Ok(routes)
}

fn persist_residency_routes(
    path: &Path,
    routes: &HashMap<(String, String), ResidencyRoute>,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            format!(
                "create residency route authority directory {}: {error}",
                parent.display()
            )
        })?;
    }
    let mut persisted = PersistentResidencyRoutes {
        routes: routes.values().cloned().collect(),
    };

    persisted.routes.sort_by(|a, b| {
        (&a.executor_id, a.holder.storage_key()).cmp(&(&b.executor_id, b.holder.storage_key()))
    });
    let bytes = serde_json::to_vec_pretty(&persisted)
        .map_err(|error| format!("serialize residency route authority: {error}"))?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(|error| {
        format!(
            "open residency route authority {}: {error}",
            temporary.display()
        )
    })?;
    file.write_all(&bytes).map_err(|error| {
        format!(
            "write residency route authority {}: {error}",
            temporary.display()
        )
    })?;
    file.sync_all().map_err(|error| {
        format!(
            "sync residency route authority {}: {error}",
            temporary.display()
        )
    })?;
    std::fs::rename(&temporary, path).map_err(|error| {
        format!(
            "publish residency route authority {}: {error}",
            path.display()
        )
    })
}

#[derive(Clone)]
struct ExecutorConnectionState {
    identity: ExecutorIdentity,
    advertisement: ExecutorAdvertisement,
    generation: u64,
    sender: mpsc::UnboundedSender<ExecutorMessage>,
    snapshot: FleetSnapshot,
    last_progress_unix_ms: u64,
    health: ExecutorSubstrateReport,
    executor_build_id: Option<String>,
    colocated: bool,
    /// Whether the runner is holding this machine drained. Stamped from the
    /// durable set when the connection is made and whenever a drain is written,
    /// so the placement gate reads one field on the candidate rather than
    /// reaching for fleet state it would have to lock separately.
    drained: bool,
    /// When the socket pump serving this connection last dequeued an inbound
    /// message, stamped by the transport rather than by anything in core. A
    /// wedged pump cannot advance `last_progress_unix_ms` either, so the two
    /// clocks diverge only when the executor itself has gone quiet — which is
    /// what makes this the deciding diagnostic when a link stalls.
    pump_tick: Arc<AtomicU64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MaterializationReadCandidate {
    pub executor_id: String,
    pub generation: u64,
    pub cell_id: String,
    pub materialization_generation: Option<String>,
    pub fence: ResidencyFence,
}

#[derive(Debug)]
struct SelectedExecutor {
    executor_id: String,
    device_id: String,
    generation: u64,
    sender: mpsc::UnboundedSender<ExecutorMessage>,
    colocated: bool,
    capabilities: ExecutorCapabilities,
}

/// A machine chosen for one request, with the demand resolved for it and the
/// complete account of how it was chosen.
#[derive(Debug)]
struct Placement {
    selected: SelectedExecutor,
    reservation: Option<resource_profiles::ResolvedResourceProfile>,
    decision: PlacementDecision,
}

/// A request that could be placed nowhere, carrying the same candidate
/// evaluation a successful placement would have.
#[derive(Debug)]
struct RefusedPlacement {
    decision: PlacementDecision,
    diagnostic: String,
}

fn refused_placement(
    request: &CellRequest,
    decided_at_unix_ms: u64,
    policy: &ActivePlacementPolicy,
    unpinned_winner: Option<PlacementCounterfactualWinner>,
    diagnostic: String,
) -> RefusedPlacement {
    RefusedPlacement {
        decision: placement_decision(
            request,
            decided_at_unix_ms,
            Some(policy_evidence(request, policy, false)),
            unpinned_winner,
            PlacementOutcome::Refused {
                diagnostic: diagnostic.clone(),
            },
            Vec::new(),
        ),
        diagnostic,
    }
}

fn placement_decision(
    request: &CellRequest,
    decided_at_unix_ms: u64,
    policy: Option<PlacementPolicyEvidence>,
    unpinned_winner: Option<PlacementCounterfactualWinner>,
    outcome: PlacementOutcome,
    rejected: Vec<PlacementRejection>,
) -> PlacementDecision {
    PlacementDecision {
        request_id: request.request_id.clone(),
        attempt_id: request.attempt_id.clone(),
        decided_at_unix_ms,
        mobility: request.placement_mobility,
        selector: request
            .executor
            .as_ref()
            .filter(|selector| !selector.is_empty())
            .cloned(),
        pinned_executor_id: request.pinned_executor_id.clone(),
        unpinned_winner,
        policy,
        outcome,
        rejected,
    }
}

/// How many placement decisions the runner keeps. Bounded because this is a
/// window onto what the fleet is doing now, not an audit log.
const RECENT_PLACEMENT_DECISIONS: usize = 32;

struct CoalescedSubscriberDropGuard {
    pool: Fleet,
    identity: RequestIdentity,
    armed: bool,
}

impl CoalescedSubscriberDropGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CoalescedSubscriberDropGuard {
    fn drop(&mut self) {
        if self.armed {
            self.pool.detach_coalesced_subscriber(&self.identity);
        }
    }
}

/// What the dispatch gate decided about an item that has won its question.
enum PurposeGate {
    /// The question owes no unconfirmed deaths; this item may dispatch.
    Clear,
    /// A newer answer took the question while this item waited at the gate.
    /// It never dispatched, so it has been settled and must not proceed.
    Displaced,
    /// The debt did not clear within the budget.
    Owed {
        outstanding: Option<(RequestIdentity, u64)>,
    },
}

/// What detaching one subscriber did to the execution it was waiting on.
enum Detachment {
    /// The last subscriber let go, so the execution itself is retired.
    Retired { leader: RequestIdentity },
    /// Interest released; the execution continues for the subscribers left.
    Released { remaining: usize },
    /// No such subscriber.
    Unknown,
}

/// What a retraction did, in terms a surface can state without guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", tag = "outcome")]
pub enum CancelOutcome {
    /// Nobody else wanted this answer, so the work has been told to stop. Its
    /// row clears when the machine reports that it did; a delivered instruction
    /// is not yet a stopped process.
    Stopping,
    /// This requester let go and the work continues for the ones that remain.
    Released { remaining: usize },
    /// Nothing live goes by that name: it finished, or was already retired,
    /// between the row rendering and the click landing.
    Unknown,
}

struct SubmitDropGuard {
    pool: Fleet,
    request_id: String,
    attempt_id: String,
    executor_id: String,
    generation: u64,
    armed: bool,
}

impl SubmitDropGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for SubmitDropGuard {
    fn drop(&mut self) {
        if self.armed {
            self.pool
                .pending
                .lock()
                .unwrap()
                .remove(&(self.request_id.clone(), self.attempt_id.clone()));
            let _ = self.pool.send_to(
                &self.executor_id,
                self.generation,
                ExecutorMessage::Cancel {
                    request_id: self.request_id.clone(),
                    attempt_id: self.attempt_id.clone(),
                },
            );
        }
    }
}

impl Fleet {
    pub fn attach_executor(&self, sender: mpsc::UnboundedSender<ExecutorMessage>) -> u64 {
        let advertisement = ExecutorAdvertisement {
            identity: ExecutorIdentity {
                device_id: "local-device".into(),
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                display_name: "Local executor".into(),
            },
            capabilities: ExecutorCapabilities {
                os: std::env::consts::OS.into(),
                arch: std::env::consts::ARCH.into(),
                logical_cores: 1,
                concurrency_capacity: None,
                toolchains: Vec::new(),
                projects_served: Vec::new(),
                disk_budget_bytes: None,
                memory_budget_bytes: None,
                sandbox: None,
                toolchain_detection: None,
                build_prerequisites: None,
            },
            current_load: 0,
            warm_roots: Vec::new(),
            observed_at_unix_ms: unix_time_ms(),
            liveness_observed_at_unix_ms: None,
            facts_stale_refresh_blocked: None,
        };
        self.attach_advertised_executor(advertisement, sender, true, None)
    }

    pub fn attach_advertised_executor(
        &self,
        advertisement: ExecutorAdvertisement,
        sender: mpsc::UnboundedSender<ExecutorMessage>,
        colocated: bool,
        executor_build_id: Option<String>,
    ) -> u64 {
        let executor_id = advertisement.identity.executor_id.clone();
        if let Some(policy) = self
            .desired_runtime_policies
            .lock()
            .unwrap()
            .get(&executor_id)
            .cloned()
        {
            let _ = sender.send(ExecutorMessage::RuntimePolicyRequest {
                correlation_id: uuid::Uuid::new_v4().to_string(),
                policy,
            });
        }
        // Attaching is where the two accounts of this machine's drain are
        // reconciled, so the runner states its own on every connection rather
        // than only when it holds one. The runner's refusal is what enforces a
        // drain; this is what keeps the executor's admission from disagreeing
        // with it in either direction.
        //
        // Sending only `true` would be assuming a new connection means a new
        // process with default state, and drain mode is process state: an
        // executor that keeps running across a link bounce keeps whatever it
        // was last told. A drain cleared while the machine was detached — the
        // path this change exists to allow — would then leave the executor
        // refusing work the runner believes it is admitting, each side correct
        // about itself and opposite about the fleet.
        let drained = self.drained.lock().unwrap().contains(&executor_id);
        let _ = sender.send(ExecutorMessage::DrainModeRequest {
            correlation_id: uuid::Uuid::new_v4().to_string(),
            enabled: drained,
        });
        let generation = {
            let mut generations = self.connection_generations.lock().unwrap();
            let generation = generations
                .get(&executor_id)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .expect("executor connection generation exhausted");
            generations.insert(executor_id.clone(), generation);
            generation
        };
        self.link_absence_since.lock().unwrap().remove(&executor_id);
        let replaced = {
            let mut connections = self.connections.lock().unwrap();
            connections
                .insert(
                    executor_id.clone(),
                    ExecutorConnectionState {
                        identity: advertisement.identity.clone(),
                        advertisement,
                        generation,
                        sender,
                        snapshot: FleetSnapshot::default(),
                        last_progress_unix_ms: unix_time_ms(),
                        health: ExecutorSubstrateReport::default(),
                        executor_build_id,
                        colocated,
                        drained,
                        pump_tick: Arc::new(AtomicU64::new(unix_time_ms())),
                    },
                )
                .is_some()
        };
        if replaced {
            self.fail_for_executor(
                &executor_id,
                "executor connection was replaced before returning a result",
            );
        }
        // Work this machine was running when its last link went away is still
        // its work. Carrying it onto this connection is what lets the result of
        // a command that outlived the gap be delivered to whoever asked for it.
        let carried = self.rebind_held_executions(&executor_id, generation);
        if !carried.is_empty() {
            log::info!(
                "executor_id={executor_id} reattached at generation {generation} carrying {} held execution(s)",
                carried.len()
            );
            self.unreconciled_executions
                .lock()
                .unwrap()
                .insert(executor_id.clone(), (generation, carried));
        }
        self.connection_ready.notify_waiters();
        generation
    }

    pub async fn wait_for_named_executor(&self, executor_name: &str) {
        loop {
            let notified = self.connection_ready.notified();
            if self.named_executor_is_connected(executor_name) {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn named_executor_is_connected(&self, executor_name: &str) -> bool {
        self.connections.lock().unwrap().values().any(|entry| {
            !entry.sender.is_closed()
                && executor_names_match(&executor_public_name(entry), executor_name)
        })
    }

    pub fn disconnect_advertised_executor(&self, executor_id: &str, generation: u64) -> bool {
        self.disconnect_advertised_executor_with_origin(
            executor_id,
            generation,
            ExecutorDisconnectOrigin::PeerOrIo,
        )
    }

    pub fn disconnect_advertised_executor_with_origin(
        &self,
        executor_id: &str,
        generation: u64,
        origin: ExecutorDisconnectOrigin,
    ) -> bool {
        let disconnected = {
            let mut connections = self.connections.lock().unwrap();
            if connections
                .get(executor_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                connections.remove(executor_id);
                true
            } else {
                false
            }
        };
        if disconnected {
            // The link going down is the last moment this machine was seen, and
            // the only moment the fact can be captured: once the connection is
            // gone there is nothing left holding its heartbeat.
            if let Some(record) = self.enrolled_remotes.lock().unwrap().get_mut(executor_id) {
                record.last_seen_unix_ms = Some(unix_time_ms());
            }
            self.link_absence_since
                .lock()
                .unwrap()
                .entry(executor_id.to_string())
                .or_insert_with(unix_time_ms);
            if executor_id == COLOCATED_EXECUTOR_ID {
                self.disconnect_origins
                    .lock()
                    .unwrap()
                    .insert((executor_id.to_string(), generation), origin);
            }
            // Who ended the link decides what it says about the work. A runner
            // that closed this connection is recycling the process behind it,
            // so the work really is gone; a link that simply went away says
            // nothing about a machine that is still running and still coming
            // back.
            match origin {
                ExecutorDisconnectOrigin::RunnerInitiated => self.fail_for_executor(
                    executor_id,
                    "executor connection closed before returning a result",
                ),
                ExecutorDisconnectOrigin::PeerOrIo => {
                    self.hold_executions_across_link_gap(executor_id);
                }
            }
            self.connection_ready.notify_waiters();
        }
        disconnected
    }

    /// Declare a machine this runner is enrolled with.
    ///
    /// Called for every configured remote as the runner starts and on every
    /// successful add — *before* any attempt is made on it. That ordering is the
    /// point: a machine becomes visible when it is enrolled, not when it first
    /// succeeds, so the state worth surfacing (nothing has worked yet) is not
    /// the one state that produces no row.
    ///
    /// Re-declaring refreshes the enrollment facts and keeps the attempt
    /// history, so a rename does not erase the explanation for a machine's
    /// current state.
    pub fn declare_enrolled_remote(&self, executor_id: &str, name: &str, os: &str, arch: &str) {
        let mut enrolled = self.enrolled_remotes.lock().unwrap();
        let record =
            enrolled
                .entry(executor_id.to_string())
                .or_insert_with(|| EnrolledRemoteRecord {
                    name: name.to_string(),
                    os: os.to_string(),
                    arch: arch.to_string(),
                    link: RemoteLinkState::Pending,
                    last_attempt: None,
                    last_seen_unix_ms: None,
                    connection_timeline: VecDeque::new(),
                });
        record.name = name.to_string();
        record.os = os.to_string();
        record.arch = arch.to_string();
    }

    /// Record what the runner's most recent attempt on a machine did.
    ///
    /// The caller decides which state the attempt proved, because only the
    /// caller knows whether the host answered. Recording an attempt against a
    /// machine that is no longer enrolled is a no-op rather than a resurrection.
    pub fn record_remote_attach_attempt(
        &self,
        executor_id: &str,
        link: RemoteLinkState,
        attempt: RemoteAttachAttempt,
    ) {
        if let Some(record) = self.enrolled_remotes.lock().unwrap().get_mut(executor_id) {
            record.link = link;
            record.last_attempt = Some(attempt);
        }
    }

    pub fn record_remote_connection_transition(
        &self,
        executor_id: &str,
        transition: RemoteConnectionTransition,
    ) {
        if let Some(record) = self.enrolled_remotes.lock().unwrap().get_mut(executor_id) {
            record.connection_timeline.push_back(transition);
            while record.connection_timeline.len() > REMOTE_CONNECTION_TIMELINE_LIMIT {
                record.connection_timeline.pop_front();
            }
        }
    }

    /// Drop an enrollment, so a removed machine stops being a fleet member
    /// rather than becoming a permanently failing one.
    /// Fleet management: the installed lifecycle implementation, enrollment
    /// operations in flight, and whether machine-local callers may manage it.
    pub fn management(&self) -> &management::ExecutorManagementState {
        &self.management
    }

    pub fn forget_enrolled_remote(&self, executor_id: &str) {
        self.enrolled_remotes.lock().unwrap().remove(executor_id);
    }

    /// Every enrolled machine that is not attached right now, by name.
    ///
    /// An attached machine is deliberately absent: it is already described in
    /// full by the executor projections, and listing it twice would invite the
    /// two descriptions to disagree.
    pub fn unattached_enrolled_remotes(&self) -> Vec<EnrolledRemote> {
        // Snapshot the attached identities and release that lock before taking
        // the enrollment one, so this read never holds both at once.
        let attached: HashSet<String> = self.connections.lock().unwrap().keys().cloned().collect();
        let drained: HashSet<String> = self.drained.lock().unwrap().clone();
        let mut values: Vec<_> = self
            .enrolled_remotes
            .lock()
            .unwrap()
            .iter()
            .filter(|(executor_id, _)| !attached.contains(*executor_id))
            .map(|(executor_id, record)| EnrolledRemote {
                drained: drained.contains(executor_id),
                name: record.name.clone(),
                os: record.os.clone(),
                arch: record.arch.clone(),
                link: record.link,
                last_attempt: record.last_attempt.clone(),
                last_seen_unix_ms: record.last_seen_unix_ms,
                connection_timeline: record.connection_timeline.iter().rev().cloned().collect(),
            })
            .collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));
        values
    }

    pub fn take_disconnect_origin(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> Option<ExecutorDisconnectOrigin> {
        self.disconnect_origins
            .lock()
            .unwrap()
            .remove(&(executor_id.to_string(), generation))
    }

    pub fn clear_disconnect_origins(&self, executor_id: &str) {
        self.disconnect_origins
            .lock()
            .unwrap()
            .retain(|(id, _), _| id != executor_id);
    }

    pub fn declare_colocated_substrate(&self, state: ExecutorSubstrateState) {
        let now = unix_time_ms();
        *self.colocated_substrate_state.lock().unwrap() =
            Some(ExecutorSubstrateEvidence::without_queue(state, now, now));
        self.connection_ready.notify_waiters();
    }

    pub fn declare_colocated_substrate_failure(&self, diagnostic: String) {
        self.declare_colocated_substrate_failure_state(
            ExecutorSubstrateState::SupervisorRespawning,
            diagnostic,
        );
    }

    pub fn declare_colocated_permanent_refusal(&self, diagnostic: String) {
        self.declare_colocated_substrate_failure_state(
            ExecutorSubstrateState::PermanentRefusal,
            diagnostic,
        );
    }

    fn declare_colocated_substrate_failure_state(
        &self,
        state: ExecutorSubstrateState,
        diagnostic: String,
    ) {
        let now = unix_time_ms();
        let mut evidence = ExecutorSubstrateEvidence::without_queue(state, now, now);
        evidence.diagnostic = Some(diagnostic);
        *self.colocated_substrate_state.lock().unwrap() = Some(evidence);
        self.connection_ready.notify_waiters();
    }

    pub fn clear_colocated_substrate(&self) {
        self.colocated_substrate_state.lock().unwrap().take();
        self.connection_ready.notify_waiters();
    }

    pub fn colocated_substrate(&self) -> Option<ExecutorSubstrateEvidence> {
        self.colocated_substrate_state.lock().unwrap().clone()
    }

    /// Whether the runner is actively rebuilding the colocated environment.
    ///
    /// This is the fact [`placement::classify_unavailable`] cannot know: a lost
    /// link means "there is no machine" or "the machine is restarting" depending
    /// entirely on what the supervisor is doing about it, and only the runner is
    /// holding that.
    ///
    /// Read from the supervisor's own declaration, which is trustworthy in both
    /// directions because the supervisor clears it the moment a link attaches
    /// healthily. Three conditions have to hold, and each rules out a way this
    /// could park an agent on a machine that is never coming back: the state must
    /// be one of the recovery states, the declaration must carry no failure of its
    /// own (a recovery that is failing has a diagnostic, and that diagnostic is
    /// the actionable thing to tell the caller), and it must be fresh by the same
    /// progress rule every other substrate hold is judged by.
    pub(crate) fn link_restoration(&self) -> placement::LinkRestoration {
        let Some(evidence) = self.colocated_substrate() else {
            return placement::LinkRestoration::NotRestoring;
        };
        let recovering = matches!(
            evidence.state,
            ExecutorSubstrateState::SupervisorSpawning
                | ExecutorSubstrateState::SupervisorRespawning
                | ExecutorSubstrateState::ProtocolAttaching
        );
        let fresh = unix_time_ms().saturating_sub(evidence.last_progress_unix_ms)
            <= EXECUTOR_PROGRESS_FRESHNESS_MS;
        if recovering && fresh && evidence.diagnostic.is_none() {
            placement::LinkRestoration::Restoring
        } else {
            placement::LinkRestoration::NotRestoring
        }
    }

    pub fn executor_generation(&self) -> Option<u64> {
        self.connections
            .lock()
            .unwrap()
            .values()
            .find(|entry| entry.colocated && !entry.sender.is_closed())
            .map(|entry| entry.generation)
    }

    /// The clock the socket pump stamps on every inbound message it dequeues.
    /// Fetched once after attach and stored lock-free per message, so a wedged
    /// pump stays distinguishable from a silent executor after the fact.
    pub fn pump_clock(&self, executor_id: &str, generation: u64) -> Option<Arc<AtomicU64>> {
        self.connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
            .map(|entry| entry.pump_tick.clone())
    }

    /// Runner-observed silence for one exact managed connection generation.
    pub fn managed_link_silence_ms(
        &self,
        executor_id: &str,
        generation: u64,
        now_unix_ms: u64,
    ) -> Option<u64> {
        self.connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
            .map(|entry| now_unix_ms.saturating_sub(entry.last_progress_unix_ms))
    }

    /// Whether the colocated link has stopped being worth waiting on.
    ///
    /// `now_unix_ms` is a parameter rather than read from the clock so the
    /// decision is testable without wall-clock sleeps, following
    /// [`deadline_evidence`]. Returns [`LinkRemediation::Healthy`] when no
    /// colocated connection is attached: the supervisor's spawn and readiness
    /// paths already own that case, and a link that was never established is not
    /// a link to bounce.
    ///
    /// Neither criterion is duration. Any inbound message — heartbeat, snapshot,
    /// result — bumps `last_progress_unix_ms`, and every beat carries a fresh
    /// recompute stamp, so an executor grinding through an hour-long check keeps
    /// reporting `Healthy` throughout.
    ///
    /// Silence is judged first because it is the stronger statement: a link
    /// nobody has heard from cannot also be diagnosed as having a telemetry
    /// problem, and reporting it as one would send an operator after the wrong
    /// failure.
    ///
    /// A fact freeze the executor explains is not remediated here at all; see
    /// [`unexplained_facts_age_ms`].
    pub fn assess_colocated_link(
        &self,
        now_unix_ms: u64,
        bounds: LinkStallBounds,
    ) -> LinkRemediation {
        let observed = self
            .connections
            .lock()
            .unwrap()
            .values()
            .find(|entry| entry.colocated)
            .map(|entry| {
                (
                    entry.identity.executor_id.clone(),
                    entry.generation,
                    entry.last_progress_unix_ms,
                    entry.pump_tick.load(Ordering::Relaxed),
                    unexplained_facts_age_ms(entry, now_unix_ms),
                )
            });
        let Some((executor_id, generation, last_progress_unix_ms, pump_tick, facts_age_ms)) =
            observed
        else {
            return LinkRemediation::Healthy;
        };
        let silence_ms = now_unix_ms.saturating_sub(last_progress_unix_ms);
        if silence_ms > bounds.silence_ms {
            return LinkRemediation::Bounce {
                executor_id,
                generation,
                cause: LinkStallCause::Silence {
                    silence_ms,
                    pump_silence_ms: now_unix_ms.saturating_sub(pump_tick),
                },
            };
        }
        match facts_age_ms {
            Some(facts_age_ms) if facts_age_ms > bounds.telemetry_ms => LinkRemediation::Bounce {
                executor_id,
                generation,
                cause: LinkStallCause::TelemetryFrozen { facts_age_ms },
            },
            _ => LinkRemediation::Healthy,
        }
    }

    /// How long this exact connection generation has been publishing facts that
    /// stopped being recomputed, for a link that is otherwise beating normally.
    ///
    /// `None` covers four different "nothing to remediate" answers on purpose:
    /// the generation is not attached, the link is silent past its own bound so
    /// silence remediation owns it, the executor publishes no recompute stamp
    /// at all, or it publishes one that is frozen *and says why* — see
    /// [`unexplained_facts_age_ms`]. A clock that was never claimed is not a
    /// stalled clock, no machine may be bounced for a fact it never offered,
    /// and a machine reporting its own wedge has already done the thing the
    /// bounce exists to provoke.
    pub fn managed_telemetry_stall_ms(
        &self,
        executor_id: &str,
        generation: u64,
        now_unix_ms: u64,
    ) -> Option<u64> {
        let connections = self.connections.lock().unwrap();
        let entry = connections
            .get(executor_id)
            .filter(|entry| entry.generation == generation)?;
        if now_unix_ms.saturating_sub(entry.last_progress_unix_ms)
            > EXECUTOR_LINK_STALL_REMEDIATION_MS
        {
            return None;
        }
        unexplained_facts_age_ms(entry, now_unix_ms)
    }

    /// Take the fleet's single telemetry-bounce turn, or decline.
    ///
    /// Callers must hold a turn before recycling a link for frozen telemetry;
    /// see [`Fleet::last_telemetry_bounce_unix_ms`] for why that failure is the
    /// one that must not be answered fleet-simultaneously. Declining is not an
    /// error: the next poll re-assesses, and a machine whose facts are still
    /// frozen a minute later takes the turn then.
    pub fn claim_telemetry_stall_bounce(&self, now_unix_ms: u64) -> bool {
        let mut last = self.last_telemetry_bounce_unix_ms.lock().unwrap();
        if *last != 0 && now_unix_ms.saturating_sub(*last) < TELEMETRY_STALL_BOUNCE_SPACING_MS {
            return false;
        }
        *last = now_unix_ms;
        true
    }

    /// Abandon a colocated link that has gone silent, so the supervisor can
    /// replace the process behind it.
    ///
    /// Retires the generation, which discards anything a wedged pump writes if it
    /// later resumes; resolves every attempt this connection owned to a typed
    /// retryable outcome; and parks the surface in `SupervisorRespawning` so
    /// subscribers that are still waiting pause their deadlines rather than
    /// expiring against an environment that is coming back.
    ///
    /// Returns false without any side effect when `generation` no longer owns the
    /// link — a reattach that landed between assessment and action owns it now,
    /// and bouncing that would strand the healthy connection that replaced the
    /// sick one.
    pub fn abandon_stalled_colocated_link(
        &self,
        executor_id: &str,
        generation: u64,
        cause: LinkStallCause,
    ) -> bool {
        if !self
            .connections
            .lock()
            .unwrap()
            .get(executor_id)
            .is_some_and(|entry| entry.generation == generation)
        {
            return false;
        }
        // Captured before the disconnect, which empties `pending` on its way
        // through `fail_for_executor`. These identities are how an in-flight
        // coalesced execution is attributed to this connection: `InFlightExecution`
        // itself carries no executor identity, and a remote executor's leaders
        // must survive a colocated bounce untouched.
        let abandoned_leaders: Vec<RequestIdentity> = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, entry)| entry.executor_id == executor_id && entry.generation == generation)
            .map(|(key, _)| key.clone())
            .collect();
        let last_known_state = self
            .colocated_substrate()
            .map(|evidence| evidence.state)
            .or_else(|| {
                self.connections
                    .lock()
                    .unwrap()
                    .get(executor_id)
                    .and_then(|entry| entry.snapshot.substrate_state.clone())
                    .map(|evidence| evidence.state)
            });
        // Declared first: a waiter that observes the teardown must already see a
        // recovering environment rather than expire against a failing one.
        self.declare_colocated_substrate(ExecutorSubstrateState::SupervisorRespawning);
        let disconnected = self.disconnect_advertised_executor_with_origin(
            executor_id,
            generation,
            ExecutorDisconnectOrigin::RunnerInitiated,
        );
        let abandoned_in_flight = self.abandon_in_flight_for_leaders(&abandoned_leaders);
        log::warn!(
            "abandoned stalled executor link executor_id={executor_id} generation={generation} \
             cause={cause} disconnected={disconnected} \
             abandoned_attempts={} abandoned_in_flight={abandoned_in_flight} \
             last_known_state={last_known_state:?}",
            abandoned_leaders.len(),
            cause = cause.describe(),
        );
        disconnected
    }

    /// Resolve every coalesced execution led by one of `leaders` to a typed
    /// retryable outcome.
    ///
    /// The disconnect already resolved each leader's own waiter, and a leader
    /// that is still being polled will publish that outcome to its subscribers
    /// itself. This closes the gap by construction instead: a subscriber whose
    /// execution had already started has no deadline left to expire against
    /// (`await_coalesced` stops evaluating one once `ExecutionRunning` is
    /// observed), so leaving its resolution to a chain of collaborating drop
    /// guards is what turns a link reset into an indefinite hold.
    fn abandon_in_flight_for_leaders(&self, leaders: &[RequestIdentity]) -> usize {
        let outcome = CellOutcome::Unavailable {
            reason: CellUnavailableReason::ExecutorUnavailable,
            diagnostic: "the build environment was reset while this attempt was in flight; \
                         it is being restored and a retry runs normally"
                .into(),
        };
        let mut resolved = 0;
        for leader in leaders {
            let led: Vec<CheckResultIdentity> = self
                .in_flight
                .lock()
                .unwrap()
                .by_key
                .iter()
                .filter(|(_, execution)| &execution.leader == leader)
                .map(|(result_identity, _)| result_identity.clone())
                .collect();
            for result_identity in led {
                // Leader-fenced, so whichever of this and the leader's own
                // publication runs second is a no-op rather than a double send.
                if self.complete_coalesced_for_leader(&result_identity, leader, outcome.clone()) {
                    resolved += 1;
                }
            }
        }
        resolved
    }

    pub fn managed_generation(&self, executor_id: &str, device_id: &str) -> Option<u64> {
        self.connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| {
                !entry.colocated
                    && entry.identity.device_id == device_id
                    && !entry.sender.is_closed()
            })
            .map(|entry| entry.generation)
    }

    /// End a managed executor's link deliberately, telling it why.
    ///
    /// The reason travels to the machine rather than staying in this runner's
    /// log. An executor whose link simply dies reads a socket reset and
    /// records that its runner dropped it; one that is told records the
    /// runner's own account of the decision, so the two logs of a single event
    /// agree instead of accusing each other.
    pub fn shutdown_advertised_executor(
        &self,
        executor_id: &str,
        device_id: &str,
        generation: u64,
        reason: impl Into<String>,
    ) -> bool {
        let sender = self
            .connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| entry.generation == generation && entry.identity.device_id == device_id)
            .map(|entry| entry.sender.clone());
        let Some(sender) = sender else { return false };
        let _ = sender.send(ExecutorMessage::Shutdown {
            reason: reason.into(),
        });
        self.disconnect_advertised_executor_with_origin(
            executor_id,
            generation,
            ExecutorDisconnectOrigin::RunnerInitiated,
        );
        true
    }

    /// Stop accepting colocated work and fail its outstanding requests before the
    /// runner begins waiting for transport connections to drain. Managed peers are
    /// not owned by this process and must survive a local daemon replacement.
    pub fn begin_colocated_shutdown(&self) -> bool {
        let target = self
            .connections
            .lock()
            .unwrap()
            .iter()
            .find(|(_, entry)| entry.colocated)
            .map(|(executor_id, entry)| {
                (executor_id.clone(), entry.generation, entry.sender.clone())
            });
        let Some((executor_id, generation, sender)) = target else {
            return false;
        };
        let _ = sender.send(ExecutorMessage::Shutdown {
            reason: "this runner is shutting down".into(),
        });
        self.disconnect_advertised_executor_with_origin(
            &executor_id,
            generation,
            ExecutorDisconnectOrigin::RunnerInitiated,
        )
    }

    fn update_advertisement(
        &self,
        executor_id: &str,
        generation: u64,
        advertisement: ExecutorAdvertisement,
    ) -> bool {
        self.update_advertisement_with(executor_id, generation, advertisement, |_| {})
    }

    fn update_advertisement_and_health(
        &self,
        executor_id: &str,
        generation: u64,
        advertisement: ExecutorAdvertisement,
        health: ExecutorSubstrateReport,
    ) -> bool {
        self.update_advertisement_with(executor_id, generation, advertisement, |entry| {
            entry.health = health
        })
    }

    fn update_advertisement_with(
        &self,
        executor_id: &str,
        generation: u64,
        advertisement: ExecutorAdvertisement,
        update: impl FnOnce(&mut ExecutorConnectionState),
    ) -> bool {
        let mut connections = self.connections.lock().unwrap();
        let Some(entry) = connections.get_mut(executor_id) else {
            return false;
        };
        if entry.generation != generation || advertisement.identity != entry.identity {
            return false;
        }
        entry.advertisement = advertisement;
        entry.last_progress_unix_ms = unix_time_ms();
        update(entry);
        self.connection_ready.notify_waiters();
        true
    }

    pub fn handle_executor_message(
        &self,
        executor_id: &str,
        generation: u64,
        message: ExecutorMessage,
    ) -> bool {
        if self
            .connections
            .lock()
            .unwrap()
            .get(executor_id)
            .is_none_or(|entry| entry.generation != generation)
        {
            return false;
        }
        match message {
            ExecutorMessage::Result {
                request_id,
                attempt_id,
                mut outcome,
            } => {
                if !outcome_matches(&outcome, &request_id, &attempt_id) {
                    return false;
                }
                let key = (request_id, attempt_id);
                let pending = self.pending.lock().unwrap().remove(&key);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending.lock().unwrap().insert(key, pending);
                        return false;
                    }
                    let _ = self.revoke_runner_contexts_for_request(&key.0, &key.1);
                    scrub_cell_outcome(&mut outcome);
                    if let CellOutcome::Completed { metadata, .. } = &mut outcome {
                        let canonical = self
                            .connections
                            .lock()
                            .unwrap()
                            .get(executor_id)
                            .filter(|connection| connection.generation == generation)
                            .map(|connection| {
                                (
                                    connection.identity.executor_id.clone(),
                                    connection.identity.device_id.clone(),
                                    connection.generation,
                                )
                            });
                        let Some((canonical_id, device_id, canonical_generation)) = canonical
                        else {
                            self.pending.lock().unwrap().insert(key, pending);
                            return false;
                        };
                        metadata.executor_id = canonical_id;
                        metadata.executor_device_id = device_id;
                        metadata.executor_connection_generation = canonical_generation;
                    }
                    let _ = pending.waiter.send(outcome);
                }
                false
            }
            ExecutorMessage::ResidencyResponse {
                correlation_id,
                result,
            } => {
                let pending = self
                    .pending_residency
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending_residency
                            .lock()
                            .unwrap()
                            .insert(correlation_id, pending);
                        return false;
                    }
                    let _ = pending.waiter.send(result);
                }
                false
            }
            ExecutorMessage::MaterializationReadResponse {
                correlation_id,
                result,
            } => {
                let pending = self
                    .pending_materialization_reads
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending_materialization_reads
                            .lock()
                            .unwrap()
                            .insert(correlation_id, pending);
                        return false;
                    }
                    let _ = pending.waiter.send(result);
                }
                false
            }
            ExecutorMessage::ResidentProcessEvent { event } => {
                if self.resident_event_is_current(executor_id, generation, &event) {
                    for event in self.settle_resident_event(executor_id, event) {
                        for subscriber in self.resident_process_subscribers.lock().unwrap().iter() {
                            subscriber(event.clone());
                        }
                    }
                }
                false
            }
            ExecutorMessage::RuntimePolicyResponse {
                correlation_id,
                result,
            } => {
                let pending = self.pending_policy.lock().unwrap().remove(&correlation_id);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending_policy
                            .lock()
                            .unwrap()
                            .insert(correlation_id, pending);
                        return false;
                    }
                    let _ = pending.waiter.send(result);
                }
                false
            }
            ExecutorMessage::DrainModeResponse {
                correlation_id,
                result,
            } => {
                let pending = self.pending_drain.lock().unwrap().remove(&correlation_id);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending_drain
                            .lock()
                            .unwrap()
                            .insert(correlation_id, pending);
                        return false;
                    }
                    let _ = pending.waiter.send(result);
                }
                false
            }
            ExecutorMessage::SnapshotResponse {
                snapshot, health, ..
            }
            | ExecutorMessage::SnapshotUpdated { snapshot, health } => {
                self.reconcile_held_executions(executor_id, generation, &snapshot);
                self.set_executor_snapshot(executor_id, generation, snapshot, health)
            }
            ExecutorMessage::Heartbeat {
                advertisement,
                health,
            } => {
                // Answer every beat with who is still waiting. Pacing the report
                // off the executor's own beat rather than a runner-side timer is
                // what lets both sides size the reap window from one constant.
                self.report_waiting_requests(executor_id, generation);
                self.update_advertisement_and_health(executor_id, generation, advertisement, health)
            }
            ExecutorMessage::AdvertisementUpdated { advertisement } => {
                self.update_advertisement(executor_id, generation, advertisement)
            }
            ExecutorMessage::InfrastructureDiagnostic { diagnostic } => {
                self.fail_for_executor(executor_id, &diagnostic);
                false
            }
            _ => false,
        }
    }

    /// Apply a snapshot from the current executor generation. `false` means the
    /// public cell snapshot did not change (or the generation is stale); health is
    /// still refreshed for a current connection.
    pub fn set_executor_snapshot(
        &self,
        executor_id: &str,
        generation: u64,
        mut snapshot: FleetSnapshot,
        health: ExecutorSubstrateReport,
    ) -> bool {
        let mut connections = self.connections.lock().unwrap();
        let Some(entry) = connections.get_mut(executor_id) else {
            return false;
        };
        if entry.generation != generation {
            return false;
        }
        for cell in &mut snapshot.cells {
            cell.executor_id = executor_id.to_string();
            cell.executor_display_name = Some(entry.identity.display_name.clone());
            if let Some(active) = cell.occupancy.command.as_mut() {
                active.executor_id = executor_id.to_string();
            }
        }
        let placements = self.recent_placements.lock().unwrap();
        let mut journaled = self.journaled_admission_parks.lock().unwrap();
        let current_queue: HashSet<_> = snapshot
            .queued_requests
            .iter()
            .map(|queued| (queued.request_id.clone(), queued.attempt_id.clone()))
            .collect();
        for queued in &entry.snapshot.queued_requests {
            let key = (queued.request_id.clone(), queued.attempt_id.clone());
            if !current_queue.contains(&key) {
                journaled.remove(&key);
            }
        }
        let mut newly_parked = Vec::new();
        for queued in &mut snapshot.queued_requests {
            queued.executor_id = executor_id.to_string();
            let key = (queued.request_id.clone(), queued.attempt_id.clone());
            let placement = placements.iter().rev().find(|decision| {
                decision.request_id == queued.request_id && decision.attempt_id == queued.attempt_id
            });
            match placement {
                Some(placement) if !journaled.contains(&key) => {
                    newly_parked.push(AdmissionParkedEvidence {
                        executor_id: executor_id.to_string(),
                        executor_name: executor_public_name(entry),
                        queued: queued.clone(),
                        admission: health.admission.clone(),
                        machine: health.machine.clone(),
                        placement: placement.clone(),
                    });
                    journaled.insert(key);
                }
                _ => {}
            }
            if queued.substrate_hold.is_none()
                && missing_wait_reason_is_new(&entry.snapshot, queued)
            {
                log::warn!(
                    "executor {executor_id} reported queued request {} attempt {} without a wait reason",
                    queued.request_id,
                    queued.attempt_id,
                );
            }
        }
        drop(journaled);
        drop(placements);
        for parked in newly_parked {
            match serde_json::to_string(&parked) {
                Ok(evidence) => log::warn!(
                    target: "cairn::fleet::admission",
                    "admission parked request_id={} attempt_id={} evidence={}",
                    parked.queued.request_id,
                    parked.queued.attempt_id,
                    evidence,
                ),
                Err(error) => log::warn!(
                    target: "cairn::fleet::admission",
                    "admission parked request_id={} attempt_id={} serialization_error={}",
                    parked.queued.request_id,
                    parked.queued.attempt_id,
                    error,
                ),
            }
        }
        for execution in &mut snapshot.executing_requests {
            execution.executor_id = executor_id.to_string();
        }
        let snapshot_changed = entry.snapshot != snapshot;
        if snapshot_changed {
            entry.last_progress_unix_ms = unix_time_ms();
        }
        let reconciled_process_events = snapshot
            .cells
            .iter()
            .filter_map(|cell| {
                let residency = cell.residency.as_ref()?;
                Some(
                    cell.occupancy
                        .processes
                        .iter()
                        .filter(move |(_, process)| {
                            matches!(
                                process.status,
                                cairn_common::executor_protocol::ResidentProcessStatus::Exited {
                                    executor_lost: true,
                                    ..
                                }
                            )
                        })
                        .map(move |(process_key, process)| ResidentProcessEvent {
                            holder: residency.holder.clone(),
                            incarnation_id: residency.incarnation_id.clone(),
                            cell_epoch: cell.cell_epoch,
                            process_key: process_key.clone(),
                            process_generation: process.generation,
                            event: ResidentProcessEventKind::State {
                                status: process.status.clone(),
                            },
                        }),
                )
            })
            .flatten()
            .collect::<Vec<_>>();
        let routes = snapshot
            .cells
            .iter()
            .filter_map(|cell| {
                cell.residency.as_ref().map(|residency| {
                    (
                        ResidencyRoute {
                            holder: residency.holder.clone(),
                            repository: residency.repository.clone(),
                            executor_id: executor_id.to_string(),
                            pending: false,
                        },
                        ResidencyFence {
                            holder: residency.holder.clone(),
                            incarnation_id: residency.incarnation_id.clone(),
                            cell_epoch: cell.cell_epoch,
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        entry.snapshot = snapshot;
        entry.health = health;
        let connected = connections.keys().cloned().collect::<HashSet<_>>();
        drop(connections);
        let mut superseded = Vec::new();
        if let Err(error) = self.update_residency_routes(|known| {
            known.retain(|(route_executor, _), route| {
                route_executor != executor_id || route.pending
            });
            for (route, fence) in routes {
                // One owner, one environment. A machine coming back with a cell
                // for an owner that another live machine now hosts is reporting a
                // leftover from before it went away: recording it would leave the
                // owner's next acquisition facing two environments and unable to
                // choose, which is the ambiguity re-placement exists to avoid. The
                // leftover is released instead of recorded.
                if known.values().any(|held| {
                    !held.pending
                        && held.holder == route.holder
                        && held.executor_id != *executor_id
                        && connected.contains(&held.executor_id)
                }) {
                    superseded.push(fence);
                    continue;
                }
                known.insert(
                    (route.executor_id.clone(), route.holder.storage_key()),
                    route,
                );
            }
        }) {
            tracing::error!(%error, "persist executor residency route snapshot failed");
        }
        for fence in superseded {
            self.release_superseded_residency(executor_id, generation, fence);
        }
        for event in reconciled_process_events {
            for subscriber in self.resident_process_subscribers.lock().unwrap().iter() {
                subscriber(event.clone());
            }
        }
        self.connection_ready.notify_waiters();
        snapshot_changed
    }

    /// Every executor-side queue entry this runner still has a live waiter for.
    ///
    /// Liveness is read from the response channels, not inferred: a `oneshot`
    /// sender whose receiver has been dropped is exact evidence that the caller
    /// went away, and coalescing means one request keeps its place while ANY
    /// subscriber is still holding on. A residency acquisition is named by the
    /// entry id the executor mints for it, which is deterministic from the
    /// holder, so a queued acquisition is nameable too.
    ///
    /// The set is deliberately complete rather than incremental. An id it omits
    /// is a statement that nobody is waiting, which is the whole point: it is
    /// what lets the executor stop guessing from elapsed time.
    ///
    /// Scoped to one link, because a queue entry belongs to one. Request ids are
    /// not globally unique — an acquisition's entry id is derived from its holder
    /// (`residency-acquire:job:…`), so two executors routing work for the same job
    /// mint the identical string — and an unscoped report would then assert
    /// liveness on one executor for a waiter that belongs to another, holding a
    /// slot open for work nobody is waiting for. The generation is part of the
    /// scope for the same reason: a waiter recorded against a link that has since
    /// bounced says nothing about the link that replaced it.
    fn waiting_request_ids(&self, executor_id: &str, generation: u64) -> Vec<String> {
        let owned = |entry_executor: &str, entry_generation: u64| {
            entry_executor == executor_id && entry_generation == generation
        };
        let mut ids: Vec<String> = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, entry)| {
                owned(&entry.executor_id, entry.generation) && !entry.waiter.is_closed()
            })
            .map(|((request_id, _), _)| request_id.clone())
            .collect();
        ids.extend(
            self.pending_residency
                .lock()
                .unwrap()
                .values()
                .filter(|entry| {
                    owned(&entry.executor_id, entry.generation) && !entry.waiter.is_closed()
                })
                .filter_map(|entry| entry.queue_entry_id.clone()),
        );
        ids.sort();
        ids.dedup();
        ids
    }

    /// Tell one executor which of its queue entries still have a live waiter.
    ///
    /// Driven by that executor's own heartbeat and by nothing else. A newly
    /// attached executor needs no report before its first beat: its queue is
    /// empty by construction, because losing a link drains the queue rather than
    /// leaving entries to be re-confirmed by whoever attaches next.
    fn report_waiting_requests(&self, executor_id: &str, generation: u64) {
        let request_ids = self.waiting_request_ids(executor_id, generation);
        let _ = self.send_to(
            executor_id,
            generation,
            ExecutorMessage::WaitingRequests { request_ids },
        );
    }

    /// Await one response, bounding SILENCE rather than elapsed duration.
    ///
    /// The budget is renewed for every interval in which `progress` reports that
    /// the executor is working on this operation, so the wait lasts as long as
    /// the work does and expires only when the reporting stops. This is the
    /// difference between "the machine has not finished yet" and "the machine
    /// stopped answering", and a duration bound cannot tell them apart: it turns
    /// a slow cold checkout into a failure and calls a wedged link patience.
    ///
    /// One implementation, two callers — a submitted batch and a residency
    /// operation. They must not drift: a batch waited out on progress while an
    /// acquisition of the very cell it needs was cut off at a flat timeout is the
    /// same contradiction seen twice.
    async fn await_bounding_silence<T>(
        &self,
        mut rx: oneshot::Receiver<T>,
        silence_budget: Duration,
        progress: impl Fn() -> bool,
    ) -> SilenceWatchdog<T> {
        let mut watchdog_deadline = Instant::now() + silence_budget;
        let mut last_observed_at = Instant::now();
        loop {
            let notified = self.connection_ready.notified();
            let now = Instant::now();
            if progress() {
                watchdog_deadline += now.saturating_duration_since(last_observed_at);
            }
            last_observed_at = now;
            let remaining = watchdog_deadline.saturating_duration_since(now);
            if remaining.is_zero() {
                return SilenceWatchdog::Silent;
            }
            tokio::select! {
                result = &mut rx => {
                    return match result {
                        Ok(value) => SilenceWatchdog::Answered(value),
                        Err(_) => SilenceWatchdog::Dropped,
                    };
                }
                _ = tokio::time::sleep(remaining.min(Duration::from_millis(250))) => {}
                _ = notified => {}
            }
        }
    }

    fn request_substrate_hold(
        &self,
        executor_id: &str,
        generation: u64,
        request_id: &str,
        attempt_id: &str,
    ) -> Option<ExecutorSubstrateEvidence> {
        let connections = self.connections.lock().unwrap();
        let entry = connections
            .get(executor_id)
            .filter(|entry| entry.generation == generation)?;
        if let Some(execution) = entry.snapshot.executing_requests.iter().find(|execution| {
            execution.request_id == request_id && execution.attempt_id == attempt_id
        }) {
            // A live child process is a kernel fact, not inferred progress. Once
            // execution begins, acquisition deadlines no longer govern the waiter.
            return Some(ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::ExecutionRunning,
                execution.started_at_unix_ms,
                entry.last_progress_unix_ms,
            ));
        }
        if unix_time_ms().saturating_sub(entry.last_progress_unix_ms)
            > EXECUTOR_PROGRESS_FRESHNESS_MS
        {
            return None;
        }
        // Executor substrate state is level-reported, so late and concurrent
        // waiters share the executor's epoch rather than inventing request epochs.
        entry.snapshot.substrate_state.clone().or_else(|| {
            entry
                .snapshot
                .queued_requests
                .iter()
                .find(|queued| queued.request_id == request_id)
                .map(|queued| {
                    queued.substrate_hold.clone().unwrap_or_else(|| {
                        let queue_position = entry
                            .snapshot
                            .queued_requests
                            .iter()
                            .position(|candidate| candidate.request_id == request_id)
                            .map(|position| position + 1);
                        ExecutorSubstrateEvidence {
                            state: ExecutorSubstrateState::CapacityBusy,
                            since_unix_ms: queued.queued_at_unix_ms,
                            last_progress_unix_ms: entry.last_progress_unix_ms,
                            diagnostic: Some(
                                "executor reported queue membership before its admission reason"
                                    .into(),
                            ),
                            queue_depth: Some(entry.snapshot.queued_requests.len()),
                            queue_position,
                            active_cell_count: Some(entry.snapshot.executing_requests.len()),
                            oldest_running_started_at_unix_ms: entry
                                .snapshot
                                .executing_requests
                                .iter()
                                .map(|request| request.started_at_unix_ms)
                                .min(),
                        }
                    })
                })
        })
    }

    fn executor_deadline_evidence(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> ExecutorSubstrateEvidence {
        let entry = self
            .connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
            .map(|entry| {
                (
                    entry.snapshot.substrate_state.clone(),
                    entry.last_progress_unix_ms,
                )
            });
        let Some((reported, last_progress_unix_ms)) = entry else {
            let now = unix_time_ms();
            return ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::ConnectedStalled,
                now,
                now,
            );
        };
        let evidence = reported.unwrap_or_else(|| {
            ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::ConnectedStalled,
                last_progress_unix_ms,
                last_progress_unix_ms,
            )
        });
        deadline_evidence(unix_time_ms(), last_progress_unix_ms, evidence)
    }

    fn coalesced_leader(&self, identity: &RequestIdentity) -> Option<RequestIdentity> {
        let registry = self.in_flight.lock().unwrap();
        let result_identity = registry.subscriber_keys.get(identity)?;
        registry
            .by_key
            .get(result_identity)
            .map(|execution| execution.leader.clone())
    }

    fn leader_substrate_hold(
        &self,
        identity: &RequestIdentity,
    ) -> Option<ExecutorSubstrateEvidence> {
        let leader = self.coalesced_leader(identity)?;
        let owner = self
            .pending
            .lock()
            .unwrap()
            .get(&leader)
            .map(|pending| (pending.executor_id.clone(), pending.generation));
        if let Some((executor_id, generation)) = owner {
            if let Some(hold) =
                self.request_substrate_hold(&executor_id, generation, &leader.0, &leader.1)
            {
                return Some(hold);
            }
        }
        if let Some(preparing) = self
            .preparing_leaders
            .lock()
            .unwrap()
            .get(&leader)
            .copied()
            .filter(|preparing| {
                unix_time_ms().saturating_sub(preparing.last_progress_unix_ms)
                    <= EXECUTOR_PROGRESS_FRESHNESS_MS
            })
        {
            return Some(ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::DispatchPreparing,
                preparing.since_unix_ms,
                preparing.last_progress_unix_ms,
            ));
        }
        self.colocated_substrate().filter(|evidence| {
            unix_time_ms().saturating_sub(evidence.last_progress_unix_ms)
                <= EXECUTOR_PROGRESS_FRESHNESS_MS
        })
    }

    fn leader_deadline_evidence(&self, identity: &RequestIdentity) -> ExecutorSubstrateEvidence {
        let Some(leader) = self.coalesced_leader(identity) else {
            let now = unix_time_ms();
            return ExecutorSubstrateEvidence::without_queue(
                ExecutorSubstrateState::ConnectedStalled,
                now,
                now,
            );
        };
        let owner = self
            .pending
            .lock()
            .unwrap()
            .get(&leader)
            .map(|pending| (pending.executor_id.clone(), pending.generation));
        let Some((executor_id, generation)) = owner else {
            return self.colocated_substrate().map_or_else(
                || {
                    let now = unix_time_ms();
                    ExecutorSubstrateEvidence::without_queue(
                        ExecutorSubstrateState::ConnectedStalled,
                        now,
                        now,
                    )
                },
                |evidence| {
                    deadline_evidence(unix_time_ms(), evidence.last_progress_unix_ms, evidence)
                },
            );
        };
        let connections = self.connections.lock().unwrap();
        let Some(entry) = connections
            .get(&executor_id)
            .filter(|entry| entry.generation == generation)
        else {
            drop(connections);
            return self.executor_deadline_evidence(&executor_id, generation);
        };
        let queued = entry
            .snapshot
            .queued_requests
            .iter()
            .find(|queued| queued.request_id == leader.0);
        let evidence = queued
            .and_then(|queued| queued.substrate_hold.clone())
            .unwrap_or_else(|| {
                let queue_position = entry
                    .snapshot
                    .queued_requests
                    .iter()
                    .position(|queued| queued.request_id == leader.0)
                    .map(|position| position + 1);
                let oldest_running_started_at_unix_ms = entry
                    .snapshot
                    .executing_requests
                    .iter()
                    .map(|request| request.started_at_unix_ms)
                    .min();
                ExecutorSubstrateEvidence {
                    state: ExecutorSubstrateState::CapacityBusy,
                    since_unix_ms: oldest_running_started_at_unix_ms
                        .or_else(|| queued.map(|queued| queued.queued_at_unix_ms))
                        .unwrap_or(entry.last_progress_unix_ms),
                    last_progress_unix_ms: entry.last_progress_unix_ms,
                    diagnostic: None,
                    queue_depth: Some(entry.snapshot.queued_requests.len()),
                    queue_position,
                    active_cell_count: Some(entry.snapshot.executing_requests.len()),
                    oldest_running_started_at_unix_ms,
                }
            });
        deadline_evidence(unix_time_ms(), entry.last_progress_unix_ms, evidence)
    }

    pub(crate) fn record_cached_completion(
        &self,
        project_id: &str,
        job_id: &str,
        executor_id: Option<&str>,
        command: &str,
        priority: CellPriority,
        passed: bool,
    ) {
        let served_at_unix_ms = unix_time_ms();
        let mut recent = self.recent_cached_completions.lock().unwrap();
        recent.push_front(cairn_common::executor_protocol::CellCompletion {
            executor_id: executor_id.unwrap_or("cache").to_string(),
            request_id: format!("cache:{}", uuid::Uuid::new_v4()),
            attempt_id: "cached".into(),
            owner: Some(cairn_common::executor_protocol::CellOwnerRef {
                project_id: project_id.to_string(),
                project_key: None,
                issue_number: None,
                job_id: Some(job_id.to_string()),
                execution_seq: None,
                node_kind: None,
            }),
            command_class: cairn_common::executor_protocol::CellCommandClass::classify(command),
            command: command.to_string(),
            priority,
            queued_at_unix_ms: served_at_unix_ms,
            started_at_unix_ms: Some(served_at_unix_ms),
            finished_at_unix_ms: served_at_unix_ms,
            duration_ms: 0,
            verdict: if passed {
                cairn_common::executor_protocol::CellCompletionVerdict::Succeeded
            } else {
                cairn_common::executor_protocol::CellCompletionVerdict::Failed
            },
            failure_output_tail: None,
            transcript_event_uri: None,
            resource_reservation: None,
            learned_estimate: None,
            actuals: None,
            cached: true,
            subscriber_count: 1,
            served_at_unix_ms,
        });
        recent.truncate(32);
    }

    /// Select an already-live resident materialization authorized for a run and
    /// matching its exact repository coordinate. Snapshot order never affects the
    /// result; no lease is acquired, renewed, pinned, or refreshed.
    pub(crate) fn select_materialization_read_candidate(
        &self,
        run_id: &str,
        job_id: &str,
        project_id: &str,
        repository: &cairn_common::executor_protocol::RepositoryIdentity,
        base_commit: &str,
    ) -> Result<MaterializationReadCandidate, MaterializationReadFailureKind> {
        let connections = self.connections.lock().unwrap();
        let mut candidates = Vec::new();
        for (executor_id, connection) in connections.iter() {
            for cell in &connection.snapshot.cells {
                if cell.project_id != project_id
                    || cell.lifecycle
                        != cairn_common::executor_protocol::PersistentCellLifecycle::Running
                {
                    continue;
                }
                let Some(residency) = cell.residency.as_ref() else {
                    continue;
                };
                if residency.phase != cairn_common::executor_protocol::ResidencyPhase::Active
                    || residency.current_base_commit != base_commit
                    || residency.repository.identity() != *repository
                {
                    continue;
                }
                // A job's own environment answers before an environment merely
                // serving the same project, and a workflow's before a dev
                // instance's, so the read lands on the most specific holder.
                let holder_rank = match residency.holder {
                    ResidencyHolder::Job { .. } => 0u8,
                    ResidencyHolder::ProjectTerminals { .. } => 1,
                    ResidencyHolder::Workflow { .. } => 2,
                    ResidencyHolder::DevInstance { .. } => 3,
                    ResidencyHolder::Service { .. } => 4,
                };
                let owner_ref_matches = residency.owner_ref.as_ref().is_some_and(|owner| {
                    owner.project_id == project_id && owner.job_id.as_deref() == Some(job_id)
                });
                let holder_id = match &residency.holder {
                    ResidencyHolder::Job { job_id } => job_id.clone(),
                    ResidencyHolder::DevInstance { instance_id } => instance_id.clone(),
                    ResidencyHolder::ProjectTerminals { project_id } => project_id.clone(),
                    ResidencyHolder::Workflow { run_id } => run_id.clone(),
                    ResidencyHolder::Service { service_id } => service_id.clone(),
                };
                let holder_id_matches =
                    residency.owner_ref.is_none() && (holder_id == job_id || holder_id == run_id);
                if !owner_ref_matches && !holder_id_matches {
                    continue;
                }
                let specificity = if owner_ref_matches { 0u8 } else { 1u8 };
                candidates.push((
                    specificity,
                    holder_rank,
                    holder_id,
                    residency.holder.storage_key(),
                    executor_id.clone(),
                    cell.cell_id.clone(),
                    residency.incarnation_id.clone(),
                    cell.cell_epoch,
                    cell.preparation_fingerprint.clone(),
                    connection.generation,
                ));
            }
        }
        candidates.sort();
        let Some(selected) = candidates.first() else {
            return Err(MaterializationReadFailureKind::NoActiveMaterializationLease);
        };
        if candidates.get(1).is_some_and(|next| {
            next.0 == selected.0
                && next.1 == selected.1
                && next.2 == selected.2
                && next.3 == selected.3
                && next.4 == selected.4
                && next.5 == selected.5
                && next.6 == selected.6
                && next.7 == selected.7
                && next.8 == selected.8
        }) {
            return Err(MaterializationReadFailureKind::MaterializationUnavailable);
        }
        Ok(MaterializationReadCandidate {
            executor_id: selected.4.clone(),
            generation: selected.9,
            cell_id: selected.5.clone(),
            materialization_generation: selected.8.clone(),
            fence: ResidencyFence {
                holder: connections[&selected.4]
                    .snapshot
                    .cells
                    .iter()
                    .find(|cell| cell.cell_id == selected.5)
                    .and_then(|cell| cell.residency.as_ref())
                    .expect("selected residency remains in the locked snapshot")
                    .holder
                    .clone(),
                incarnation_id: selected.6.clone(),
                cell_epoch: selected.7,
            },
        })
    }

    /// What Cairn's own running work says about when the machines a batch could
    /// use will have room.
    ///
    /// Read per machine and then combined, rather than from the aggregate
    /// snapshot, because the aggregate deliberately loses executor attribution:
    /// "something somewhere finishes in ninety seconds" is not an answer to
    /// "when will THIS batch's machine have room".
    ///
    /// Eligibility is [`candidate_rejection`], the same relation placement
    /// itself applies — never a subset of it. A machine that matches the
    /// selector but does not serve the project, or whose link has closed, is one
    /// the work will never reach, and a forecast that read it would hand the
    /// batch a shorter wait than its real blocker needs, surface a refusal while
    /// that blocker was still finite, and print the wrong occupant's name on the
    /// row that went red. That is the failure this policy exists to end, so it
    /// must not be reintroduced by approximating where work can go.
    ///
    /// This reads placement state and takes no reservation. Which request is
    /// admitted, and when, remains the executor's try_admit alone (CAIRN-3268);
    /// a forecast only tells a caller how long its own patience is worth.
    pub(crate) fn occupancy_for(&self, scope: PlacementScope<'_>) -> occupancy::MachineOccupancy {
        self.occupancy_for_at(scope, unix_time_ms())
    }

    fn occupancy_for_at(
        &self,
        scope: PlacementScope<'_>,
        now_unix_ms: u64,
    ) -> occupancy::MachineOccupancy {
        let connections = self.connections.lock().unwrap();
        connections
            .values()
            .filter(|connection| candidate_rejection(connection, scope, now_unix_ms).is_none())
            .map(|connection| {
                occupancy::MachineOccupancy::read(
                    &connection.snapshot.executing_requests,
                    now_unix_ms,
                )
            })
            .reduce(occupancy::MachineOccupancy::or_earlier)
            .unwrap_or(occupancy::MachineOccupancy::Unforecastable)
    }

    pub fn snapshot(&self) -> FleetSnapshot {
        let connections = self.connections.lock().unwrap();
        let mut ids: Vec<_> = connections.keys().cloned().collect();
        ids.sort();
        let mut aggregate = FleetSnapshot::default();
        for id in ids {
            let snapshot = &connections[&id].snapshot;
            aggregate.cells.extend(snapshot.cells.clone());
            aggregate
                .queued_requests
                .extend(snapshot.queued_requests.clone());
            aggregate
                .executing_requests
                .extend(snapshot.executing_requests.clone());
            aggregate
                .recent_completions
                .extend(
                    snapshot
                        .recent_completions
                        .iter()
                        .cloned()
                        .map(|mut completion| {
                            completion.executor_id = id.clone();
                            completion
                        }),
                );
            if let Some(occupancy) = &snapshot.resident_occupancy {
                let aggregate_occupancy = aggregate
                    .resident_occupancy
                    .get_or_insert_with(Default::default);
                aggregate_occupancy.process_count += occupancy.process_count;
                aggregate_occupancy.reservation.memory_bytes = aggregate_occupancy
                    .reservation
                    .memory_bytes
                    .saturating_add(occupancy.reservation.memory_bytes);
                aggregate_occupancy.reservation.disk_growth_bytes = aggregate_occupancy
                    .reservation
                    .disk_growth_bytes
                    .saturating_add(occupancy.reservation.disk_growth_bytes);
                aggregate_occupancy.reservation.concurrency_units = aggregate_occupancy
                    .reservation
                    .concurrency_units
                    .saturating_add(occupancy.reservation.concurrency_units);
            }
        }
        aggregate.recent_completions.extend(
            self.recent_cached_completions
                .lock()
                .unwrap()
                .iter()
                .cloned(),
        );
        aggregate
            .cells
            .sort_by(|a, b| (&a.executor_id, &a.cell_id).cmp(&(&b.executor_id, &b.cell_id)));
        aggregate
            .executing_requests
            .sort_by(|a, b| (&a.request_id, &a.attempt_id).cmp(&(&b.request_id, &b.attempt_id)));
        aggregate.recent_completions.sort_by(|a, b| {
            b.served_at_unix_ms
                .cmp(&a.served_at_unix_ms)
                .then_with(|| a.request_id.cmp(&b.request_id))
        });
        aggregate.recent_completions.truncate(32);
        aggregate.queued_requests.sort_by(|a, b| {
            (a.queued_at_unix_ms, &a.executor_id, &a.request_id).cmp(&(
                b.queued_at_unix_ms,
                &b.executor_id,
                &b.request_id,
            ))
        });
        let counts: HashMap<_, _> = self
            .in_flight
            .lock()
            .unwrap()
            .by_key
            .values()
            .map(|execution| (execution.leader.clone(), execution.subscribers.len()))
            .collect();
        for cell in &mut aggregate.cells {
            if let Some(active) = cell.occupancy.command.as_mut() {
                active.subscriber_count = counts
                    .get(&(active.request_id.clone(), active.attempt_id.clone()))
                    .copied()
                    .unwrap_or(1);
            }
        }
        let placements = self.recent_placements.lock().unwrap();
        for queued in &mut aggregate.queued_requests {
            queued.subscriber_count = counts
                .get(&(queued.request_id.clone(), queued.attempt_id.clone()))
                .copied()
                .unwrap_or(1);
            queued.placement = placements
                .iter()
                .rev()
                .find(|decision| {
                    decision.request_id == queued.request_id
                        && decision.attempt_id == queued.attempt_id
                })
                .map(queued_placement_evidence);
        }
        for completion in &mut aggregate.recent_completions {
            completion.subscriber_count = counts
                .get(&(completion.request_id.clone(), completion.attempt_id.clone()))
                .copied()
                .unwrap_or(completion.subscriber_count.max(1));
        }
        if aggregate.substrate_state.is_none() {
            aggregate.substrate_state = self.colocated_substrate();
        }
        aggregate
    }

    pub fn set_expected_executor_build_id(&self, executor_id: impl Into<String>, build_id: String) {
        self.expected_executor_build_ids
            .lock()
            .unwrap()
            .insert(executor_id.into(), build_id);
    }

    pub fn executor_health(&self, captured_at_unix_ms: u64) -> Vec<ExecutorHealthSnapshot> {
        let connections = self.connections.lock().unwrap();
        let expected_build_ids = self.expected_executor_build_ids.lock().unwrap();
        let mut values: Vec<_> = connections
            .values()
            .map(|entry| executor_health_snapshot(entry, captured_at_unix_ms, &expected_build_ids))
            .collect();
        values.sort_by(|a, b| a.identity.executor_id.cmp(&b.identity.executor_id));
        values
    }

    /// Every executor as an agent inspects it, addressed by public name.
    ///
    /// The whole projection is taken under ONE acquisition of the connections
    /// lock, so an executor's link state, its telemetry, and the work resident
    /// on it always come from the same connection generation. Reading them
    /// through separate calls would let a reconnect land in between and describe
    /// a machine that never existed — healthy link, previous incarnation's
    /// occupancy.
    ///
    /// Cached state only. This answers from what the runner already holds and
    /// never probes an executor or samples the fleet, so reading it costs a
    /// clone rather than a round trip to every machine.
    pub fn inspect_executors(&self, captured_at_unix_ms: u64) -> Vec<ExecutorInspection> {
        let connections = self.connections.lock().unwrap();
        let expected_build_ids = self.expected_executor_build_ids.lock().unwrap();
        let enrolled_remotes = self.enrolled_remotes.lock().unwrap();
        let mut values: Vec<_> = connections
            .values()
            .map(|entry| ExecutorInspection {
                name: executor_public_name(entry),
                recent_placements: self.placements_naming(&entry.identity.executor_id),
                colocated: entry.colocated,
                health: executor_health_snapshot(entry, captured_at_unix_ms, &expected_build_ids),
                executor_build_id: entry.executor_build_id.clone(),
                occupancy: entry.snapshot.clone(),
                captured_at_unix_ms,
                connection_timeline: enrolled_remotes
                    .get(&entry.identity.executor_id)
                    .map(|record| record.connection_timeline.iter().rev().cloned().collect())
                    .unwrap_or_default(),
            })
            .collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));
        values
    }

    /// The coarse frame a live surface subscribes to instead of polling
    /// `cairn://executors`.
    ///
    /// Built from [`Self::inspect_executors`] rather than from its own pass over
    /// the connection table, so the pushed frame and the read resource are two
    /// renderings of one projection and cannot come to disagree.
    pub fn telemetry_frame(&self, captured_at_unix_ms: u64) -> telemetry::FleetTelemetryFrame {
        telemetry::FleetTelemetryFrame::from_inspections(
            &self.inspect_executors(captured_at_unix_ms),
            captured_at_unix_ms,
        )
    }

    /// [`Self::telemetry_frame`] stamped with the current instant.
    pub fn telemetry_frame_now(&self) -> telemetry::FleetTelemetryFrame {
        self.telemetry_frame(unix_time_ms())
    }

    /// Admit one fleet-state change and learn when its frame may go out.
    ///
    /// The clock stays inside the fleet rather than being passed in, so no
    /// caller can rate-limit against a different notion of now than the frame
    /// it ends up sending was stamped with.
    pub fn admit_telemetry(&self) -> telemetry::TelemetryEmit {
        self.telemetry_cadence.admit(unix_time_ms())
    }

    /// The deferred trailing frame a [`telemetry::TelemetryEmit::After`] asked
    /// for, with the cadence reopened before the fleet is read.
    ///
    /// Producing the frame and reopening the cadence are one operation because
    /// their order is load-bearing; see [`telemetry::TelemetryCadence::capture_trailing`].
    pub fn trailing_telemetry_frame(&self) -> telemetry::FleetTelemetryFrame {
        let now = unix_time_ms();
        self.telemetry_cadence
            .capture_trailing(now, || self.telemetry_frame(now))
    }

    /// Keep one placement decision, evicting the oldest past the bound.
    fn record_placement_decision(&self, decision: PlacementDecision) {
        for rejection in &decision.rejected {
            // The in-memory ledger is deliberately bounded. Emit each rejection
            // independently so the runner journal remains a durable witness,
            // including the typed reason's capacity numbers.
            match serde_json::to_string(rejection) {
                Ok(rejection) => log::warn!(
                    target: "cairn::fleet::placement",
                    "placement rejection request_id={} attempt_id={} rejection={}",
                    decision.request_id,
                    decision.attempt_id,
                    rejection
                ),
                Err(error) => log::warn!(
                    target: "cairn::fleet::placement",
                    "placement rejection request_id={} attempt_id={} serialization_error={}",
                    decision.request_id,
                    decision.attempt_id,
                    error
                ),
            }
        }
        let mut recent = self.recent_placements.lock().unwrap();
        if recent.len() >= RECENT_PLACEMENT_DECISIONS {
            recent.pop_front();
        }
        recent.push_back(decision);
    }

    /// Every placement decision this machine took part in, newest first.
    fn placements_naming(&self, executor_id: &str) -> Vec<PlacementDecision> {
        self.recent_placements
            .lock()
            .unwrap()
            .iter()
            .rev()
            .filter(|decision| decision.mentions_executor(executor_id))
            .cloned()
            .collect()
    }

    /// The public address of an attached executor, by internal identity.
    pub fn executor_public_name(&self, executor_id: &str) -> Option<String> {
        self.connections
            .lock()
            .unwrap()
            .get(executor_id)
            .map(executor_public_name)
    }

    pub async fn set_executor_runtime_policy(
        &self,
        executor_id: &str,
        expected_generation: u64,
        policy: cairn_common::executor_protocol::ExecutorRuntimePolicy,
    ) -> Result<cairn_common::executor_protocol::ExecutorRuntimePolicy, String> {
        policy.validate()?;
        let sender = self
            .connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| entry.generation == expected_generation)
            .map(|entry| entry.sender.clone())
            .ok_or_else(|| "executor connection generation is stale".to_string())?;
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_policy.lock().unwrap().insert(
            correlation_id.clone(),
            PendingPolicyResult {
                executor_id: executor_id.to_string(),
                generation: expected_generation,
                waiter: tx,
            },
        );
        if sender
            .send(ExecutorMessage::RuntimePolicyRequest {
                correlation_id: correlation_id.clone(),
                policy,
            })
            .is_err()
        {
            self.pending_policy.lock().unwrap().remove(&correlation_id);
            return Err("executor disconnected while applying runtime policy".into());
        }
        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("executor dropped the runtime-policy response".into()),
            Err(_) => {
                self.pending_policy.lock().unwrap().remove(&correlation_id);
                Err("executor runtime-policy update timed out".into())
            }
        }
    }

    /// Hold or release a drain on a machine.
    ///
    /// Unlike every other executor control, this one is not fenced on the
    /// connection generation and does not fail when the machine is unreachable.
    /// A drain is the runner's own refusal to place work, so it needs nothing
    /// from the machine to be true; the generation fence exists to stop config
    /// landing on the wrong process incarnation, and a drain is
    /// incarnation-independent by nature. The moment an operator reaches for
    /// this lever is, almost by definition, the moment the machine has stopped
    /// answering (CAIRN-4360).
    ///
    /// The live push is therefore best effort and its outcome is reported, not
    /// returned as failure: the runner has already stopped placing work either
    /// way, and a machine that is not listening will be told when it attaches.
    pub async fn set_executor_drain_mode(
        &self,
        executor_id: &str,
        enabled: bool,
    ) -> DrainApplication {
        {
            let mut drained = self.drained.lock().unwrap();
            if enabled {
                drained.insert(executor_id.to_string());
            } else {
                drained.remove(executor_id);
            }
        }
        let live = {
            let mut connections = self.connections.lock().unwrap();
            connections.get_mut(executor_id).map(|entry| {
                entry.drained = enabled;
                (entry.sender.clone(), entry.generation)
            })
        };
        let Some((sender, generation)) = live else {
            return DrainApplication::HeldWithoutMachine {
                reason: "it is not attached".into(),
            };
        };
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_drain.lock().unwrap().insert(
            correlation_id.clone(),
            PendingDrainResult {
                executor_id: executor_id.to_string(),
                generation,
                waiter: tx,
            },
        );
        if sender
            .send(ExecutorMessage::DrainModeRequest {
                correlation_id: correlation_id.clone(),
                enabled,
            })
            .is_err()
        {
            self.pending_drain.lock().unwrap().remove(&correlation_id);
            return DrainApplication::HeldWithoutMachine {
                reason: "its link closed before the request was sent".into(),
            };
        }
        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(Ok(_))) => DrainApplication::Applied,
            Ok(Ok(Err(error))) => DrainApplication::HeldWithoutMachine { reason: error },
            Ok(Err(_)) => DrainApplication::HeldWithoutMachine {
                reason: "it dropped the drain-mode response".into(),
            },
            Err(_) => {
                self.pending_drain.lock().unwrap().remove(&correlation_id);
                DrainApplication::HeldWithoutMachine {
                    reason: "it did not answer within ten seconds".into(),
                }
            }
        }
    }

    /// Whether this runner is holding a machine drained.
    pub fn is_drained(&self, executor_id: &str) -> bool {
        self.drained.lock().unwrap().contains(executor_id)
    }

    /// Adopt the durable set of drained machines, stamping any connection that
    /// is already up.
    ///
    /// Called when the runner reads its fleet settings. Stamping live
    /// connections is what makes the order irrelevant: the colocated executor
    /// can attach before settings are read, and a drain that only took effect
    /// on the next attach would leave the fleet placing work on a machine an
    /// operator has already disabled.
    pub fn hydrate_drained(&self, executor_ids: impl IntoIterator<Item = String>) {
        let adopted: HashSet<String> = executor_ids.into_iter().collect();
        let mut connections = self.connections.lock().unwrap();
        for (executor_id, entry) in connections.iter_mut() {
            let drained = adopted.contains(executor_id);
            // Stated to every live connection, in both directions, for the same
            // reason attaching states it: an executor process can outlive the
            // runner that last told it something, so "the settings say clear"
            // is not evidence that the machine is not still draining itself.
            let _ = entry.sender.send(ExecutorMessage::DrainModeRequest {
                correlation_id: uuid::Uuid::new_v4().to_string(),
                enabled: drained,
            });
            entry.drained = drained;
        }
        *self.drained.lock().unwrap() = adopted;
    }

    pub fn set_desired_executor_runtime_policy(
        &self,
        executor_id: &str,
        policy: cairn_common::executor_protocol::ExecutorRuntimePolicy,
    ) {
        self.desired_runtime_policies
            .lock()
            .unwrap()
            .insert(executor_id.to_string(), policy);
    }

    /// Retract interest in one dispatched request, and report what that did.
    ///
    /// This is what the operator's ✗ reaches, and what it may honestly claim
    /// depends on a fact only the model holds: whether anyone else still wants
    /// the same answer. Check work coalesces, so one execution routinely serves
    /// several requesters, and a click on one of their rows releases that
    /// requester without stopping anything. Returning a bare `true` for that
    /// case reported success for an action that changed nothing an operator
    /// could see — the row stayed, fed by executor occupancy, because the work
    /// was still running for everyone else.
    pub(crate) fn cancel_request(&self, request_id: &str) -> CancelOutcome {
        let subscriber = self
            .in_flight
            .lock()
            .unwrap()
            .subscriber_keys
            .keys()
            .find(|(id, _)| id == request_id)
            .cloned();
        if let Some(identity) = subscriber {
            return match self.detach_coalesced_subscriber(&identity) {
                Detachment::Retired { leader } => {
                    // Retiring, not retired: the key stays owed until this
                    // leader's terminal outcome lands, so a fresh answer to the
                    // same question waits for the death rather than starting
                    // beside it.
                    self.purposes
                        .lock()
                        .unwrap()
                        .retire(&leader, unix_time_ms());
                    CancelOutcome::Stopping
                }
                Detachment::Released { remaining } => CancelOutcome::Released { remaining },
                Detachment::Unknown => CancelOutcome::Unknown,
            };
        }
        let owner = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .find(|((id, _), _)| id == request_id)
            .map(|((_, attempt), pending)| {
                (
                    attempt.clone(),
                    pending.executor_id.clone(),
                    pending.generation,
                )
            });
        let Some((attempt_id, executor_id, generation)) = owner else {
            return CancelOutcome::Unknown;
        };
        let identity = (request_id.to_string(), attempt_id.clone());
        self.purposes
            .lock()
            .unwrap()
            .retire(&identity, unix_time_ms());
        // Delivery is what this can honestly claim. An executor built from this
        // tree binds the request identity when its Submit is received, so a
        // Cancel naming it is found whatever phase it has reached; an older one
        // can still lose a retirement that lands in the instant between handling
        // a Submit and its task starting. That window is narrow and getting
        // narrower — the pre-submit gate above means a retirement known before
        // dispatch never produces a submit to race — and it is not worth
        // grounding every deployed executor to close, so it is stated here
        // rather than hidden behind a version bump.
        if self
            .send_to(
                &executor_id,
                generation,
                ExecutorMessage::Cancel {
                    request_id: request_id.into(),
                    attempt_id,
                },
            )
            .is_ok()
        {
            // The honest word is "stopping". A delivered instruction is not a
            // stopped process, and the row stays until the machine reports the
            // terminal outcome — which is also what releases this question for
            // its next answer. Reporting "retired" here would be the same
            // mistake the registry used to make, told to the operator instead.
            CancelOutcome::Stopping
        } else {
            CancelOutcome::Unknown
        }
    }

    fn complete_coalesced_for_leader(
        &self,
        result_identity: &CheckResultIdentity,
        expected_leader: &RequestIdentity,
        outcome: CellOutcome,
    ) -> bool {
        let execution = {
            let mut registry = self.in_flight.lock().unwrap();
            if registry
                .by_key
                .get(result_identity)
                .is_none_or(|execution| &execution.leader != expected_leader)
            {
                return false;
            }
            let execution = registry
                .by_key
                .remove(result_identity)
                .expect("leader-fenced coalesced execution disappeared while locked");
            for identity in execution.subscribers.keys() {
                registry.subscriber_keys.remove(identity);
            }
            execution
        };
        let leader_still_active = self
            .in_flight
            .lock()
            .unwrap()
            .by_key
            .values()
            .any(|candidate| candidate.leader == execution.leader);
        if !leader_still_active {
            self.coalesced_leaders
                .lock()
                .unwrap()
                .remove(&execution.leader);
        }
        for (identity, subscriber) in execution.subscribers {
            let _ = subscriber.waiter.send(CoalescedCellOutcome {
                outcome: restamp_outcome(&outcome, &identity),
                publication: execution.publication.clone(),
            });
        }
        true
    }

    /// Terminate a work item the chokepoint has displaced or released.
    ///
    /// Retirement is recorded before it is delivered, because the item may not
    /// be on the wire yet: an item still being placed is stopped by the record
    /// alone, at the pre-submit gate in [`Fleet::execute_prepared`], and never
    /// reaches an executor. An item already dispatched is told, and an executor
    /// built from this tree observes that at every phase transition it has left,
    /// because it binds the request identity when the `Submit` is received. An
    /// executor older than that binding can still lose a retirement that lands
    /// in the instant between handling a `Submit` and its task starting; the
    /// reasoning for tolerating that rather than moving the protocol version is
    /// recorded at `Fleet::bind_request_identity` in the executor crate.
    ///
    /// The last clause of "terminated means dead" — that the item's process
    /// tree is reaped and its checkout quiescent before the cell is reused — is
    /// deliberately not enforced here, because it is already enforced where the
    /// tree actually is. The executor takes one `TreeQuiescent` proof for the
    /// length of a batch hold and every reset on the cancellation path consumes
    /// it, so a retirement delivered from here cannot reach a tree reset that
    /// skipped the reaper: the type system on the far side refuses to compile
    /// one. This path composes with that gate rather than duplicating it, and
    /// duplicating it would be worse than redundant — a second liveness opinion
    /// formed a network hop away from the processes it describes is exactly the
    /// stale copy of a remote truth this design exists to abolish.
    /// Wait until this question owes no unconfirmed deaths.
    ///
    /// The gate that separates "one holder is recorded" from "one work item
    /// exists". Winning a purpose key establishes which answer is newest; it
    /// says nothing about whether the answer it displaced has stopped consuming
    /// a cell. Dispatching on the strength of the record alone is what left an
    /// operator watching several attestations of one project at once, and a
    /// registry that showed a single item throughout would have made that
    /// overlap harder to see rather than impossible.
    ///
    /// The predecessor's own terminal outcome is the only evidence accepted
    /// here, because it is the only one that is downstream of the work actually
    /// stopping. The executor's quiescence proof guards reuse of the
    /// predecessor's own tree; it says nothing about a successor placed on a
    /// different cell or a different machine, which is exactly the overlap this
    /// gate exists to prevent.
    async fn await_purpose_quiescent(
        &self,
        key: &purpose::PurposeKey,
        me: &RequestIdentity,
    ) -> PurposeGate {
        let deadline = tokio::time::Instant::now() + RETIREMENT_CONFIRMATION_BUDGET;
        loop {
            // Arm the wakeup before reading the registry. A debt cleared in the
            // gap between reading and awaiting would otherwise be a wakeup this
            // waiter never hears, and the successor would sit out the whole
            // budget for a predecessor that had already died.
            let quiesced = self.purpose_quiesced.notified();
            let outstanding = {
                let mut registry = self.purposes.lock().unwrap();
                // Have I been displaced while waiting here? A burst of merges
                // makes this ordinary: this item took the question from its
                // predecessor, and a newer tip took it from this one before the
                // predecessor had finished dying.
                //
                // Being displaced is what puts an item into the debt set, so
                // without this an item would wait for a set that now contains
                // itself, and could only leave it by completing work the gate is
                // still holding it back from starting. Settling it here is sound
                // precisely because this gate sits upstream of everything: an
                // item displaced at this point has not been coalesced, placed,
                // or submitted, so there is provably nothing running to confirm
                // dead. Waiting for a terminal outcome nothing will produce is
                // what deadlocked the burst.
                if !registry.is_live_holder(key, me) {
                    registry.finish(me);
                    drop(registry);
                    self.purpose_quiesced.notify_waiters();
                    return PurposeGate::Displaced;
                }
                if registry.pending_retirements(key) == 0 {
                    return PurposeGate::Clear;
                }
                registry.oldest_pending_retirement(key)
            };
            if tokio::time::timeout_at(deadline, quiesced).await.is_err() {
                return PurposeGate::Owed { outstanding };
            }
        }
    }

    fn retire_dispatched_item(&self, request: &RequestIdentity) {
        self.cancelled_leaders
            .lock()
            .unwrap()
            .insert(request.clone());
        let owner = self
            .pending
            .lock()
            .unwrap()
            .get(request)
            .map(|pending| (pending.executor_id.clone(), pending.generation));
        if let Some((executor_id, generation)) = owner {
            let _ = self.send_to(
                &executor_id,
                generation,
                ExecutorMessage::Cancel {
                    request_id: request.0.clone(),
                    attempt_id: request.1.clone(),
                },
            );
        }
    }

    fn detach_coalesced_subscriber(&self, identity: &RequestIdentity) -> Detachment {
        let remaining;
        let leader_to_cancel = {
            let mut registry = self.in_flight.lock().unwrap();
            let Some(result_identity) = registry.subscriber_keys.remove(identity) else {
                return Detachment::Unknown;
            };
            let Some(execution) = registry.by_key.get_mut(&result_identity) else {
                return Detachment::Unknown;
            };
            execution.subscribers.remove(identity);
            remaining = execution.subscribers.len();
            if execution.subscribers.is_empty() {
                let leader = execution.leader.clone();
                // Keep the empty execution as a tombstone until the leader publishes its
                // terminal outcome. Cancellation is asynchronous at the executor, so removing
                // the result key here would let an immediate cadence retry become a second
                // leader while the first command is still queued, running, or recovering.
                let group_still_consumed = registry.by_key.values().any(|candidate| {
                    candidate.leader == leader && !candidate.subscribers.is_empty()
                });
                if group_still_consumed {
                    None
                } else {
                    self.cancelled_leaders
                        .lock()
                        .unwrap()
                        .insert(leader.clone());
                    Some(leader)
                }
            } else {
                None
            }
        };
        let Some(leader) = leader_to_cancel else {
            return Detachment::Released { remaining };
        };
        let owner = self.pending.lock().unwrap().get(&leader).map(|pending| {
            (
                pending.executor_id.clone(),
                pending.generation,
                leader.clone(),
            )
        });
        if let Some((executor_id, generation, (request_id, attempt_id))) = owner {
            let _ = self.send_to(
                &executor_id,
                generation,
                ExecutorMessage::Cancel {
                    request_id,
                    attempt_id,
                },
            );
        }
        Detachment::Retired { leader }
    }

    pub(crate) fn cancel_job_requests(&self, job_id: &str) -> usize {
        let subscribers: Vec<_> = self
            .in_flight
            .lock()
            .unwrap()
            .by_key
            .values()
            .flat_map(|execution| execution.subscribers.iter())
            .filter(|(_, subscriber)| subscriber.requesting_job_id.as_deref() == Some(job_id))
            .map(|(identity, _)| identity.clone())
            .collect();
        let subscriber_count = subscribers.len();
        for identity in subscribers {
            self.detach_coalesced_subscriber(&identity);
        }

        let coalesced_leaders = self.coalesced_leaders.lock().unwrap().clone();
        let pending: Vec<_> = self
            .pending
            .lock()
            .unwrap()
            .iter()
            .filter(|(identity, pending)| {
                pending.requesting_job_id.as_deref() == Some(job_id)
                    && !coalesced_leaders.contains(*identity)
            })
            .map(|((request_id, attempt_id), pending)| {
                (
                    request_id.clone(),
                    attempt_id.clone(),
                    pending.executor_id.clone(),
                    pending.generation,
                )
            })
            .collect();
        subscriber_count
            + pending
                .into_iter()
                .filter(|(request_id, attempt_id, executor_id, generation)| {
                    self.send_to(
                        executor_id,
                        *generation,
                        ExecutorMessage::Cancel {
                            request_id: request_id.clone(),
                            attempt_id: attempt_id.clone(),
                        },
                    )
                    .is_ok()
                })
                .count()
    }

    pub async fn submit(&self, orch: &Orchestrator, request: CellRequest) -> CellOutcome {
        self.submit_execution(orch, request, None).await
    }

    /// Dispatch a bounded read to the exact executor generation selected from an
    /// authoritative fleet snapshot. This method never performs placement or any
    /// residency operation.
    pub async fn read_resident_materialization(
        &self,
        executor_id: &str,
        generation: u64,
        request: MaterializationReadRequest,
    ) -> MaterializationReadResult {
        let fail = |kind, diagnostic: &str| MaterializationReadResult::Failed {
            kind,
            diagnostic: diagnostic.to_string(),
        };
        let sender = {
            let connections = self.connections.lock().unwrap();
            let Some(connection) = connections
                .get(executor_id)
                .filter(|connection| connection.generation == generation)
            else {
                return fail(
                    MaterializationReadFailureKind::MaterializationUnavailable,
                    "selected executor generation is unavailable",
                );
            };
            connection.sender.clone()
        };
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_materialization_reads.lock().unwrap().insert(
            correlation_id.clone(),
            PendingMaterializationRead {
                executor_id: executor_id.to_string(),
                generation,
                waiter: tx,
            },
        );
        if sender
            .send(ExecutorMessage::MaterializationReadRequest {
                correlation_id: correlation_id.clone(),
                request: request.clone(),
            })
            .is_err()
        {
            self.pending_materialization_reads
                .lock()
                .unwrap()
                .remove(&correlation_id);
            return fail(
                MaterializationReadFailureKind::MaterializationUnavailable,
                "executor connection closed during materialization read dispatch",
            );
        }
        let timeout = Duration::from_millis(
            request
                .deadline_unix_ms
                .saturating_sub(unix_time_ms())
                .max(1),
        );
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => fail(
                MaterializationReadFailureKind::MaterializationUnavailable,
                "executor dropped the materialization read response",
            ),
            Err(_) => {
                self.pending_materialization_reads
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                fail(
                    MaterializationReadFailureKind::DeadlineExceeded,
                    "materialization read response deadline elapsed",
                )
            }
        }
    }

    pub async fn operate_residency(
        &self,
        orch: &Orchestrator,
        mut operation: ResidencyOperation,
    ) -> ResidencyResult {
        // Acquisition is a single flight per EXECUTION ENVIRONMENT. Route
        // resolution, pending-route reservation, and dispatch have to be
        // indivisible for one holder — two acquirers that both resolve "no
        // route" can place onto two executors and leave the route authority
        // naming two environments for one owner — and have nothing to agree
        // about across holders. A gate spanning the fleet made every job on the
        // machine wait behind one job's placement, which itself waits for as
        // long as the chosen executor keeps reporting progress.
        // Correlation-keyed response waiters are independent.
        let acquire_flight = match &operation {
            // The horizon is NOT re-based across this wait. It is an instant the
            // requester chose, and moving it forward because the operation queued
            // behind another would make the executor hold the entry past what
            // anybody agreed to. Waiting here no longer eats a budget the way it
            // ate a deadline: the horizon is the requester's whole patience, and
            // what a wait spends is that patience, honestly.
            ResidencyOperation::Acquire { request } => {
                Some(self.residency_acquire_flight(&request.holder).await)
            }
            _ => None,
        };
        let mut pending_acquire_route = None;
        let (selected, executor_config, object_request, object_plane) = match &mut operation {
            ResidencyOperation::Acquire {
                request: acquisition,
            } => {
                if let Some(selected) = match self.resolve_residency_acquire_route(acquisition) {
                    Ok(selected) => selected,
                    Err(failure) => return failure,
                } {
                    // The persisted route is repository-address authority. Resolve
                    // it before preparing configuration so a retry whose locator
                    // has the same identity but a stale coordinate loads policy
                    // from the address the residency actually holds.
                    let placement = residency_placement_request(acquisition);
                    let prepared = match self.prepare_execution(orch, &placement).await {
                        Ok(prepared) => prepared,
                        Err(outcome) => {
                            return residency_core_failure(
                                ResidencyFailureKind::Admission,
                                "prepare execution environment reattachment",
                                Some(outcome),
                            )
                        }
                    };
                    (
                        selected,
                        ResidencyRuntimeConfig::Install(prepared.executor_config),
                        None,
                        None,
                    )
                } else {
                    let mut placement = residency_placement_request(acquisition);
                    let prepared = match self.prepare_execution(orch, &placement).await {
                        Ok(prepared) => prepared,
                        Err(outcome) => {
                            return residency_core_failure(
                                ResidencyFailureKind::Admission,
                                "prepare execution environment placement",
                                Some(outcome),
                            )
                        }
                    };
                    if let Err(diagnostic) =
                        require_colocated_population(&mut placement, &prepared.executor_config)
                    {
                        return residency_core_failure(
                            ResidencyFailureKind::Admission,
                            diagnostic,
                            None,
                        );
                    }
                    // A residency carries its own declared footprint, so there
                    // is no per-candidate demand to resolve for it.
                    let selected = match self
                        .select_executor(&placement, None, &prepared.placement_policy)
                        .await
                    {
                        Ok(placed) => {
                            self.record_placement_decision(placed.decision);
                            placed.selected
                        }
                        Err(outcome) => {
                            return residency_core_failure(
                                ResidencyFailureKind::Admission,
                                "select execution environment executor",
                                Some(outcome),
                            )
                        }
                    };
                    if !selected.colocated
                        && !matches!(
                            acquisition.repository,
                            RepositoryLocator::ScratchOnly { .. }
                        )
                    {
                        let identity = acquisition.repository.identity();
                        acquisition.repository = RepositoryLocator::ManagedObjects {
                            project_id: identity.project_id,
                            repository_id: identity.repository_id,
                            object_format: identity.object_format,
                        };
                        prepared.object_plane.authorize_request(
                            &placement,
                            &selected.executor_id,
                            selected.generation,
                        );
                    }
                    pending_acquire_route = Some(ResidencyRoute {
                        holder: acquisition.holder.clone(),
                        repository: acquisition.repository.clone(),
                        executor_id: selected.executor_id.clone(),
                        pending: true,
                    });
                    (
                        selected,
                        ResidencyRuntimeConfig::Install(prepared.executor_config),
                        Some(placement),
                        Some(prepared.object_plane),
                    )
                }
            }
            _ => {
                let Some(holder) = residency_operation_holder(&operation) else {
                    return residency_core_failure(
                        ResidencyFailureKind::InvalidDeclaration,
                        "this operation names no execution environment",
                        None,
                    );
                };
                let routed = {
                    let connections = self.connections.lock().unwrap();
                    connections.iter().find_map(|(executor_id, connection)| {
                        connection
                            .snapshot
                            .cells
                            .iter()
                            .find_map(|cell| {
                                cell.residency
                                    .as_ref()
                                    .filter(|residency| residency.holder == *holder)
                                    .cloned()
                            })
                            .map(|residency| {
                                (
                                    SelectedExecutor {
                                        executor_id: executor_id.clone(),
                                        device_id: connection.identity.device_id.clone(),
                                        generation: connection.generation,
                                        sender: connection.sender.clone(),
                                        colocated: connection.colocated,
                                        capabilities: connection.advertisement.capabilities.clone(),
                                    },
                                    residency,
                                )
                            })
                    })
                };
                let Some((selected, residency)) = routed else {
                    return residency_core_failure(
                        ResidencyFailureKind::Unavailable,
                        "no connected executor reports this execution environment",
                        None,
                    );
                };
                let executor_config = if matches!(
                    operation,
                    ResidencyOperation::StartProcess { .. }
                        | ResidencyOperation::RefreshCheckout { .. }
                        | ResidencyOperation::MaterializeConflict { .. }
                ) {
                    let config_request = residency_runtime_config_request(&residency);
                    match self.prepare_execution(orch, &config_request).await {
                        Ok(prepared) => ResidencyRuntimeConfig::Install(prepared.executor_config),
                        Err(outcome) => {
                            return residency_core_failure(
                                ResidencyFailureKind::Admission,
                                "prepare execution environment operation",
                                Some(outcome),
                            )
                        }
                    }
                } else {
                    ResidencyRuntimeConfig::NotRequired
                };
                if !selected.colocated {
                    if let ResidencyOperation::RefreshCheckout {
                        fence, base_commit, ..
                    } = &operation
                    {
                        let request = residency_refresh_request(&residency, fence, base_commit);
                        orch.object_plane.authorize_request(
                            &request,
                            &selected.executor_id,
                            selected.generation,
                        );
                        (
                            selected,
                            executor_config,
                            Some(request),
                            Some(orch.object_plane.clone()),
                        )
                    } else {
                        (selected, executor_config, None, None)
                    }
                } else {
                    (selected, executor_config, None, None)
                }
            }
        };
        let dispatch_error = {
            let connections = self.connections.lock().unwrap();
            selected_link_dispatch_error(&connections, &selected, unix_time_ms())
        };
        if let Some(diagnostic) = dispatch_error {
            if let (Some(request), Some(object_plane)) = (&object_request, &object_plane) {
                object_plane.revoke_request(
                    &request.request_id,
                    &request.attempt_id,
                    &selected.executor_id,
                    selected.generation,
                );
            }
            return residency_core_failure(ResidencyFailureKind::Admission, diagnostic, None);
        }
        if let Some(route) = pending_acquire_route.as_ref() {
            if let Err(error) = self.reserve_pending_residency_route(route.clone()) {
                return residency_core_failure(
                    ResidencyFailureKind::Persistence,
                    format!("persist pending residency route authority: {error}"),
                    None,
                );
            }
        }
        // Read before the operation is handed to the link: this is the bound the
        // executor itself honors, and the runner's own wait below is sized from
        // it so the two cannot disagree about whether this operation is alive.
        // Only an acquisition takes a queue entry, so only an acquisition is
        // nameable in the liveness report. Every other operation is bounded by
        // the work it names and never enters admission.
        let acquire_queue_entry_id = match &operation {
            ResidencyOperation::Acquire { request } => {
                Some(residency_queue_entry_id(&request.holder))
            }
            _ => None,
        };
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_residency.lock().unwrap().insert(
            correlation_id.clone(),
            PendingLifetimeResult {
                executor_id: selected.executor_id.clone(),
                generation: selected.generation,
                waiter: tx,
                queue_entry_id: acquire_queue_entry_id.clone(),
            },
        );
        let sent = selected
            .sender
            .send(ExecutorMessage::ResidencyRequest {
                correlation_id: correlation_id.clone(),
                config: executor_config,
                operation,
            })
            .is_ok();
        if !sent {
            self.pending_residency
                .lock()
                .unwrap()
                .remove(&correlation_id);
            if let Some(route) = pending_acquire_route.as_ref() {
                self.clear_pending_residency_route(route);
            }
            return residency_core_failure(
                ResidencyFailureKind::Admission,
                "executor connection closed while sending residency operation",
                None,
            );
        }
        drop(acquire_flight);
        // Silence, not duration. Acquiring an execution environment is a cell
        // placement, so it waits exactly the way a submitted batch waits: for as
        // long as the executor keeps reporting that it is working on this
        // operation. The number this used to be derived from was a claim about
        // what provisioning costs, and it was not true — a cold cell is a
        // detached checkout plus the project's setup commands, which legitimately
        // runs into minutes, and cutting it off at tens of seconds refused an
        // executor that was working correctly the whole time.
        //
        // The progress probe is the request probe, unchanged: the acquisition
        // occupies an ordinary queue entry, under an id both sides derive from
        // the holder, so "is this operation alive" has one answer for both paths.
        let progress_key = acquire_queue_entry_id.clone();
        let result = match self
            .await_bounding_silence(
                rx,
                Duration::from_millis(RESIDENCY_RESPONSE_FLOOR_MS),
                || match progress_key.as_deref() {
                    Some(request_id) => self
                        .request_substrate_hold(
                            &selected.executor_id,
                            selected.generation,
                            request_id,
                            RESIDENCY_ACQUIRE_ATTEMPT_ID,
                        )
                        .is_some(),
                    // A non-acquiring operation never queues, so there is nothing
                    // request-specific to observe; the link's own freshness is the
                    // only evidence there is.
                    None => self
                        .connections
                        .lock()
                        .unwrap()
                        .get(&selected.executor_id)
                        .filter(|entry| entry.generation == selected.generation)
                        .is_some_and(|entry| {
                            unix_time_ms().saturating_sub(entry.last_progress_unix_ms)
                                <= EXECUTOR_PROGRESS_FRESHNESS_MS
                        }),
                },
            )
            .await
        {
            SilenceWatchdog::Answered(result) => result,
            SilenceWatchdog::Dropped => residency_core_failure(
                ResidencyFailureKind::Admission,
                "executor dropped the residency operation response",
                None,
            ),
            SilenceWatchdog::Silent => {
                self.pending_residency
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                // The executor stopped reporting. Whether it is contended or
                // wedged is not something this wait can tell on its own, so it
                // carries the executor's own substrate evidence and lets the
                // caller classify: a busy machine is worth presenting to again, a
                // stalled one is not. Without this the failure would be untyped,
                // and an untyped placement failure is a refusal by construction.
                let substrate =
                    self.executor_deadline_evidence(&selected.executor_id, selected.generation);
                let diagnostic =
                    self.residency_silence_diagnostic(&selected.executor_id, selected.generation);
                residency_core_failure(
                    ResidencyFailureKind::Admission,
                    diagnostic.clone(),
                    Some(CellOutcome::Unavailable {
                        reason: CellUnavailableReason::Deadline {
                            host_pressure: None,
                            substrate: Some(substrate),
                        },
                        diagnostic,
                    }),
                )
            }
        };
        match &result {
            ResidencyResult::State { cell } => {
                if let Some(residency) = cell.residency.as_ref() {
                    if let Err(error) = self.update_residency_routes(|routes| {
                        routes.insert(
                            (selected.executor_id.clone(), residency.holder.storage_key()),
                            ResidencyRoute {
                                holder: residency.holder.clone(),
                                repository: residency.repository.clone(),
                                executor_id: selected.executor_id.clone(),
                                pending: false,
                            },
                        );
                    }) {
                        tracing::error!(%error, "persist authoritative residency route failed");
                    }
                }
            }
            ResidencyResult::Released { holder, .. } => {
                let released = holder.storage_key();
                if let Err(error) = self.update_residency_routes(|routes| {
                    routes.retain(|(_, known_holder), _| known_holder != &released);
                }) {
                    tracing::error!(%error, "persist released residency route removal failed");
                }
            }
            // A materialization touches only working-tree files, so it neither
            // establishes nor retires a residency route.
            ResidencyResult::ConflictMaterialized { .. } => {}
            ResidencyResult::Failed { .. } => {
                if let Some(route) = pending_acquire_route.as_ref() {
                    self.clear_pending_residency_route(route);
                }
            }
        }
        if let (Some(request), Some(object_plane)) = (object_request, object_plane) {
            object_plane.revoke_request(
                &request.request_id,
                &request.attempt_id,
                &selected.executor_id,
                selected.generation,
            );
        }
        result
    }

    /// The single-flight gate for one execution environment.
    ///
    /// Keyed by holder, because that is the identity acquisition is idempotent
    /// over: two acquirers of the same environment must agree on one route and
    /// one cell, and two acquirers of different environments have nothing to
    /// agree about.
    async fn residency_acquire_flight(&self, holder: &ResidencyHolder) -> ResidencyAcquireFlight {
        let key = holder.storage_key();
        let gate = self
            .residency_acquisitions
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .clone();
        let guard = gate.clone().lock_owned().await;
        ResidencyAcquireFlight {
            flights: self.residency_acquisitions.clone(),
            key,
            gate,
            guard: Some(guard),
        }
    }

    fn reserve_pending_residency_route(&self, route: ResidencyRoute) -> Result<(), String> {
        debug_assert!(route.pending);
        self.update_residency_routes(|routes| {
            routes.insert(
                (route.executor_id.clone(), route.holder.storage_key()),
                route,
            );
        })
    }

    fn clear_pending_residency_route(&self, route: &ResidencyRoute) {
        if let Err(error) = self.update_residency_routes(|routes| {
            routes.retain(|key, known| {
                key != &(route.executor_id.clone(), route.holder.storage_key())
                    || !known.pending
                    || known.holder != route.holder
            });
        }) {
            tracing::error!(%error, "persist pending residency route removal failed");
        }
    }

    #[allow(clippy::result_large_err)]
    /// The live fence for a residency as the fleet currently observes it. This
    /// lets a caller that knows only the holder — job teardown, say — address it
    /// without carrying an incarnation and epoch of its own.
    pub fn residency_fence(&self, holder: &ResidencyHolder) -> Option<ResidencyFence> {
        let connections = self.connections.lock().unwrap();
        connections.values().find_map(|connection| {
            connection.snapshot.cells.iter().find_map(|cell| {
                cell.residency
                    .as_ref()
                    .filter(|residency| residency.holder == *holder)
                    .map(|residency| ResidencyFence {
                        holder: residency.holder.clone(),
                        incarnation_id: residency.incarnation_id.clone(),
                        cell_epoch: cell.cell_epoch,
                    })
            })
        })
    }

    /// The executor currently hosting a residency, if any. Placement for a batch
    /// bound to that residency is not a choice: it must land on the executor that
    /// owns the cell.
    pub(crate) fn residency_route_executor(&self, holder: &ResidencyHolder) -> Option<String> {
        self.residency_routes
            .lock()
            .unwrap()
            .values()
            .find(|route| route.holder == *holder)
            .map(|route| route.executor_id.clone())
    }

    /// The executor a job home occupies, or will occupy before it has been routed.
    ///
    /// Job residencies are untargeted and immobile, so an unrouted home can only
    /// land on the colocated executor. A route naming an executor that is no
    /// longer connected names nothing this job can occupy: the cell it recorded
    /// died with that executor session, and the next acquisition rebuilds the
    /// environment wherever placement lands
    /// ([`Self::resolve_residency_acquire_route`]). Reading it as occupancy is
    /// how a batch came to be measured for compatibility against a machine that
    /// was gone.
    pub(crate) fn job_home_executor(&self, holder: &ResidencyHolder) -> Option<String> {
        self.live_residency_route_executor(holder).or_else(|| {
            self.connections
                .lock()
                .unwrap()
                .values()
                .find(|entry| entry.colocated && !entry.sender.is_closed())
                .map(|entry| entry.identity.executor_id.clone())
        })
    }

    /// The executor a residency is routed to, only while that executor holds a
    /// live connection.
    fn live_residency_route_executor(&self, holder: &ResidencyHolder) -> Option<String> {
        let routed = self
            .residency_routes
            .lock()
            .unwrap()
            .values()
            .filter(|route| route.holder == *holder)
            .map(|route| route.executor_id.clone())
            .collect::<Vec<_>>();
        let connections = self.connections.lock().unwrap();
        routed
            .into_iter()
            .find(|executor_id| connections.contains_key(executor_id))
    }

    /// What to call an executor in text a person or an agent reads, whether or
    /// not it is still connected. A lost environment has to name the machine it
    /// was on, and that machine is by definition not connected.
    pub(crate) fn executor_name_for_text(&self, executor_id: &str) -> String {
        if let Some(name) = self.executor_public_name(executor_id) {
            return name;
        }
        if executor_id == COLOCATED_EXECUTOR_ID {
            return LOCAL_EXECUTOR_NAME.to_string();
        }
        self.enrolled_remotes
            .lock()
            .unwrap()
            .get(executor_id)
            .map(|record| record.name.clone())
            .unwrap_or_else(|| executor_id.to_string())
    }

    /// When this runner first observed that an executor was not connected.
    ///
    /// Stamped on disconnect, and stamped here for an executor this process has
    /// never seen — a route loaded from the authority file after a runner
    /// restart names a machine whose absence starts being counted the first time
    /// anyone asks for it, not at some instant nobody observed.
    fn link_absent_since(&self, executor_id: &str, now_unix_ms: u64) -> u64 {
        *self
            .link_absence_since
            .lock()
            .unwrap()
            .entry(executor_id.to_string())
            .or_insert(now_unix_ms)
    }

    /// Whether the link a residency failure came from is one the runner is still
    /// trying to restore.
    ///
    /// The environment's own executor answers this, not the colocated one: a job
    /// whose home is on a remote is neither helped nor bound by what the
    /// supervisor is doing about a machine it never asked for. A machine still
    /// inside its lease grace counts as restoring, because the runner has
    /// neither given up on it nor rebuilt the environment — and a caller that
    /// waits rather than refuses is precisely what that grace is for.
    pub(crate) fn residency_link_restoration(
        &self,
        holder: &ResidencyHolder,
    ) -> placement::LinkRestoration {
        let routed = self
            .residency_routes
            .lock()
            .unwrap()
            .values()
            .filter(|route| route.holder == *holder)
            .map(|route| route.executor_id.clone())
            .collect::<Vec<_>>();
        let now = unix_time_ms();
        let absent = {
            let connections = self.connections.lock().unwrap();
            routed
                .iter()
                .filter(|executor_id| !connections.contains_key(*executor_id))
                .cloned()
                .collect::<Vec<_>>()
        };
        let within_grace = absent.iter().any(|executor_id| {
            now.saturating_sub(self.link_absent_since(executor_id, now))
                < ENVIRONMENT_LEASE_GRACE_MS
        });
        if within_grace {
            return placement::LinkRestoration::Restoring;
        }
        self.link_restoration()
    }

    /// Whether the runner has positive evidence it is bringing one executor's
    /// link back.
    ///
    /// Only the colocated executor is supervised by this runner, so only it can
    /// produce that evidence. A remote that stopped answering is read as gone
    /// until it attaches again, which is the reading that acts rather than the
    /// one that waits — the same default [`placement`] takes everywhere else.
    fn executor_link_restoration(&self, executor_id: &str) -> placement::LinkRestoration {
        if executor_id == COLOCATED_EXECUTOR_ID {
            self.link_restoration()
        } else {
            placement::LinkRestoration::NotRestoring
        }
    }

    /// Report, once, that an owner's execution environment was rebuilt because
    /// the executor session holding the previous one ended.
    pub(crate) fn take_environment_eviction(
        &self,
        holder: &ResidencyHolder,
    ) -> Option<EnvironmentEviction> {
        self.environment_evictions
            .lock()
            .unwrap()
            .remove(&holder.storage_key())
    }

    /// Whether one live executor satisfies the caller's complete selector.
    ///
    /// A missing result means the executor is not in the live fleet snapshot;
    /// callers must not interpret missing knowledge as selector compatibility.
    pub(crate) fn executor_satisfies_selector(
        &self,
        executor_id: &str,
        selector: &ExecutorSelector,
    ) -> Option<bool> {
        self.connections
            .lock()
            .unwrap()
            .get(executor_id)
            .map(|entry| matches_selector(entry, selector))
    }

    /// Where an acquisition for an owner that already has an environment must
    /// go, or `None` when it is free to be placed.
    ///
    /// A route is a **lease against a live executor session**, not a permanent
    /// binding. The cell it names lives inside that session, so a session that
    /// ended took the environment with it and the route that survives names
    /// nothing. Reading such a route as authority anyway is what stranded jobs
    /// indefinitely (CAIRN-4332): every batch resolved the dead binding and was
    /// refused structurally, while nothing ever removed the route, because route
    /// reconciliation rewrites an executor's routes from that executor's own
    /// snapshots and a disconnected executor sends none. The refusal was
    /// therefore permanent, on a fleet with live machines that could have
    /// rebuilt the environment in seconds.
    ///
    /// So a route with no live session behind it is evicted here and the
    /// acquisition falls through to ordinary placement, which materializes the
    /// environment again at the state the durable side holds. The one exception
    /// is a link the runner is actively restoring: that environment is not gone,
    /// it is seconds away, so the failure is capacity-shaped and the caller
    /// waits for it rather than moving.
    #[allow(clippy::result_large_err)]
    fn resolve_residency_acquire_route(
        &self,
        request: &mut ResidencyAcquireRequest,
    ) -> Result<Option<SelectedExecutor>, ResidencyResult> {
        if let Err(error) = self.ensure_residency_route_store_available() {
            return Err(residency_core_failure(
                ResidencyFailureKind::Persistence,
                format!("residency route authority is unavailable: {error}"),
                None,
            ));
        }
        let existing = {
            let known = self.residency_routes.lock().unwrap();
            known
                .values()
                .filter(|route| route.holder == request.holder)
                .cloned()
                .collect::<Vec<_>>()
        };
        if existing.is_empty() {
            return Ok(None);
        }
        let mut live = {
            let connections = self.connections.lock().unwrap();
            existing
                .iter()
                .filter_map(|route| {
                    connections
                        .get(&route.executor_id)
                        .map(|entry| (route.clone(), selected_executor(entry)))
                })
                .collect::<Vec<_>>()
        };
        // Two machines both claiming to hold one owner's environment right now is
        // a contradiction only they can resolve, and picking one would be picking
        // which live cell to orphan.
        if live.len() > 1 {
            return Err(residency_core_failure(
                ResidencyFailureKind::ConflictingDeclaration,
                "multiple executors report the same execution environment",
                None,
            ));
        }
        let Some((route, selected)) = live.pop() else {
            self.evict_lost_environment(&request.holder, &existing)?;
            return Ok(None);
        };
        if route.repository.identity() != request.repository.identity() {
            return Err(residency_core_failure(
                ResidencyFailureKind::ConflictingDeclaration,
                "this owner's execution environment already holds another repository",
                None,
            ));
        }
        // The route carries the coordinate the executor actually holds, which is
        // where an acquirer's own repository locator can legitimately differ
        // (a colocated path resolved differently, say). The identity above is
        // what makes them the same environment; this is the address.
        request.repository = route.repository.clone();
        // One live session answers for this environment, so a route left over
        // from a session that ended is stale by construction. Dropping it now
        // keeps a later reconnection from resurrecting an ambiguity that no
        // acquisition could resolve.
        self.forget_superseded_routes(&request.holder, &route.executor_id);
        Ok(Some(selected))
    }

    /// Answer for an environment whose every route names an executor this runner
    /// has no live connection to, and leave the route authority in the state that
    /// answer implies.
    ///
    /// Absence from the connection map is not death. A transport that dropped
    /// leaves the executor process running and its cells intact, so treating the
    /// first missed connection as proof the environment is gone would turn every
    /// network blip into lost uncommitted files, packages, and resident
    /// processes. What separates the two is time: a link that flapped comes back,
    /// and a machine that left does not. So the environment is held for a grace
    /// period during which the failure is capacity-shaped and the caller waits,
    /// and only a machine still unreachable at the end of it is treated as gone.
    ///
    /// Evicting the routes is what makes the next step placement rather than
    /// refusal, and it is done before anything is placed so the two cannot
    /// disagree about how many environments this owner has.
    #[allow(clippy::result_large_err)]
    fn evict_lost_environment(
        &self,
        holder: &ResidencyHolder,
        lost: &[ResidencyRoute],
    ) -> Result<(), ResidencyResult> {
        if let Some(restoring) = lost.iter().find(|route| {
            self.executor_link_restoration(&route.executor_id)
                == placement::LinkRestoration::Restoring
        }) {
            // Not lost, just away: the supervisor says it is bringing this link
            // back, and rebuilding the environment somewhere else would abandon a
            // cell that is about to answer again. Carrying the placement evidence
            // is what makes this a wait rather than a refusal.
            let diagnostic = format!(
                "the machine holding this execution environment ({}) is reconnecting",
                self.executor_name_for_text(&restoring.executor_id)
            );
            return Err(residency_core_failure(
                ResidencyFailureKind::Admission,
                diagnostic.clone(),
                Some(CellOutcome::Unavailable {
                    reason: CellUnavailableReason::ExecutorUnavailable,
                    diagnostic,
                }),
            ));
        }
        let now = unix_time_ms();
        if let Some((route, absent_ms)) = lost
            .iter()
            .map(|route| {
                (
                    route,
                    now.saturating_sub(self.link_absent_since(&route.executor_id, now)),
                )
            })
            // The most recently seen machine decides: while any of this owner's
            // routes still has a machine that might answer, the environment is
            // not yet gone.
            .min_by_key(|(_, absent_ms)| *absent_ms)
            .filter(|(_, absent_ms)| *absent_ms < ENVIRONMENT_LEASE_GRACE_MS)
        {
            let diagnostic = format!(
                "the machine holding this execution environment ({}) has not answered for {}s",
                self.executor_name_for_text(&route.executor_id),
                absent_ms / 1_000
            );
            return Err(residency_core_failure(
                ResidencyFailureKind::Admission,
                diagnostic.clone(),
                Some(CellOutcome::Unavailable {
                    reason: CellUnavailableReason::ExecutorUnavailable,
                    diagnostic,
                }),
            ));
        }
        // Named in a stable order, because this reaches an agent as prose and
        // the map these came from has no order of its own.
        let mut names = lost
            .iter()
            .map(|route| self.executor_name_for_text(&route.executor_id))
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        let executor = names.join(", ");
        if let Err(error) = self.update_residency_routes(|routes| {
            routes.retain(|_, route| route.holder != *holder);
        }) {
            return Err(residency_core_failure(
                ResidencyFailureKind::Persistence,
                format!("release the lost execution environment route: {error}"),
                None,
            ));
        }
        tracing::warn!(
            %holder,
            %executor,
            "execution environment lost with its executor session; rebuilding it through placement"
        );
        self.environment_evictions
            .lock()
            .unwrap()
            .insert(holder.storage_key(), EnvironmentEviction { executor });
        Ok(())
    }

    /// Tell an executor to let go of a cell whose owner now lives somewhere
    /// else.
    ///
    /// Deliberately fire-and-forget: the runner has no waiter for the response,
    /// and what matters is that the leftover stops answering for this owner,
    /// which is the executor's to reach. A lost message costs nothing that the
    /// cell's own death policy does not eventually reclaim.
    fn release_superseded_residency(
        &self,
        executor_id: &str,
        generation: u64,
        fence: ResidencyFence,
    ) {
        tracing::warn!(
            executor = %executor_id,
            holder = %fence.holder,
            "releasing an execution environment that was superseded while its executor was away"
        );
        let _ = self.send_to(
            executor_id,
            generation,
            ExecutorMessage::ResidencyRequest {
                correlation_id: uuid::Uuid::new_v4().to_string(),
                config: ResidencyRuntimeConfig::NotRequired,
                operation: ResidencyOperation::Release { fence },
            },
        );
    }

    /// Forget every route for one owner except the one a live executor answers
    /// for.
    fn forget_superseded_routes(&self, holder: &ResidencyHolder, live_executor_id: &str) {
        if let Err(error) = self.update_residency_routes(|routes| {
            routes.retain(|_, route| {
                route.holder != *holder || route.executor_id == live_executor_id
            });
        }) {
            tracing::error!(%error, "persist superseded residency route removal failed");
        }
    }

    pub(crate) async fn submit_pure_verdict(
        &self,
        orch: &Orchestrator,
        result_identity: CheckResultIdentity,
        request: CellRequest,
    ) -> Result<CoalescedCellOutcome, CellOutcome> {
        if request.mutation_policy != MutationPolicy::PureVerdict {
            return Err(CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                diagnostic: "coalesced submission requires pure-verdict mutation policy".into(),
            });
        }
        let prepared = self.prepare_execution(orch, &request).await?;
        let public_identity = (request.request_id.clone(), request.attempt_id.clone());
        let wait_horizon_unix_ms = request.wait_horizon_unix_ms;
        let (tx, rx) = oneshot::channel();
        let mut leader_request = None;
        {
            let mut registry = self.in_flight.lock().unwrap();
            if registry.subscriber_keys.contains_key(&public_identity) {
                return Err(executor_unavailable(
                    "duplicate coalesced subscriber identity".into(),
                ));
            }
            if let Some(execution) = registry.by_key.get_mut(&result_identity) {
                execution.subscribers.insert(
                    public_identity.clone(),
                    CoalescedSubscriber {
                        waiter: tx,
                        priority: request.priority,
                        requesting_job_id: request.requesting_job_id.clone(),
                    },
                );
            } else {
                let publication = PublicationCoordination::new();
                let mut subscribers = HashMap::new();
                subscribers.insert(
                    public_identity.clone(),
                    CoalescedSubscriber {
                        waiter: tx,
                        priority: request.priority,
                        requesting_job_id: request.requesting_job_id.clone(),
                    },
                );
                registry.by_key.insert(
                    result_identity.clone(),
                    InFlightExecution {
                        leader: public_identity.clone(),
                        subscribers,
                        publication,
                    },
                );
                self.coalesced_leaders
                    .lock()
                    .unwrap()
                    .insert(public_identity.clone());
                leader_request = Some(request);
            }
            registry
                .subscriber_keys
                .insert(public_identity.clone(), result_identity.clone());
        }
        if let Some(mut request) = leader_request {
            if let Some(priority) = self
                .in_flight
                .lock()
                .unwrap()
                .by_key
                .get(&result_identity)
                .and_then(|execution| {
                    execution
                        .subscribers
                        .values()
                        .map(|subscriber| subscriber.priority)
                        .max()
                })
            {
                // Executor protocol has no queued priority update. Subscribers that arrive
                // after this send inherit the admitted priority until that protocol grows one.
                request.priority = priority;
            }
            let pool = self.clone();
            let completion_guard = CoalescedLeaderCompletionGuard {
                pool: pool.clone(),
                leader: (request.request_id.clone(), request.attempt_id.clone()),
                result_identities: vec![result_identity.clone()],
                runner_context_id: None,
                armed: true,
            };
            tokio::spawn(async move {
                let mut completion_guard = completion_guard;
                if !pool
                    .in_flight
                    .lock()
                    .unwrap()
                    .by_key
                    .contains_key(&result_identity)
                {
                    let identity = (request.request_id.clone(), request.attempt_id.clone());
                    pool.cancelled_leaders.lock().unwrap().remove(&identity);
                    pool.coalesced_leaders.lock().unwrap().remove(&identity);
                    completion_guard.disarm();
                    return;
                }
                let leader = (request.request_id.clone(), request.attempt_id.clone());
                let outcome = pool.execute_prepared(request, None, prepared).await;
                pool.cancelled_leaders.lock().unwrap().remove(&leader);
                pool.coalesced_leaders.lock().unwrap().remove(&leader);
                pool.complete_coalesced_for_leader(&result_identity, &leader, outcome);
                completion_guard.disarm();
            });
        }
        self.await_coalesced(public_identity, wait_horizon_unix_ms, rx)
            .await
    }

    async fn await_coalesced(
        &self,
        identity: RequestIdentity,
        wait_horizon_unix_ms: u64,
        mut rx: oneshot::Receiver<CoalescedCellOutcome>,
    ) -> Result<CoalescedCellOutcome, CellOutcome> {
        let mut guard = CoalescedSubscriberDropGuard {
            pool: self.clone(),
            identity: identity.clone(),
            armed: true,
        };
        let mut execution_started = false;
        loop {
            let now = unix_time_ms();
            if self
                .leader_substrate_hold(&identity)
                .is_some_and(|hold| hold.state == ExecutorSubstrateState::ExecutionRunning)
            {
                execution_started = true;
            }
            let remaining = wait_horizon_unix_ms.saturating_sub(now);
            if !execution_started && remaining == 0 {
                let substrate = self.leader_deadline_evidence(&identity);
                self.detach_coalesced_subscriber(&identity);
                guard.disarm();
                return Err(CellOutcome::Unavailable {
                    diagnostic: format!(
                        "cell subscriber deadline elapsed with {:?}; last progress at {}",
                        substrate.state, substrate.last_progress_unix_ms
                    ),
                    reason: CellUnavailableReason::Deadline {
                        host_pressure: None,
                        substrate: Some(substrate),
                    },
                });
            }
            let wait = if execution_started {
                Duration::from_millis(250)
            } else {
                Duration::from_millis(remaining.clamp(1, 250))
            };
            match tokio::time::timeout(wait, &mut rx).await {
                Ok(Ok(outcome)) => {
                    guard.disarm();
                    return Ok(outcome);
                }
                Ok(Err(_)) => {
                    guard.disarm();
                    return Err(executor_unavailable(
                        "coalesced cell result channel closed".into(),
                    ));
                }
                Err(_) => {}
            }
        }
    }

    /// The OS-confinement regime for a project-check batch, at either cadence.
    ///
    /// Declared checks run **unconfined, with host permissions**. A check command
    /// is not agent input that has to earn trust: it comes from the `checks`
    /// contract in the project's live main checkout
    /// (`execution::checks::load_live_project_checks`), which a branch cannot edit
    /// for its own run. That is the same trust decision
    /// [`crate::config::check_exemption`] makes when an agent types a declared
    /// check into `run`; there the command must be matched back to the contract,
    /// while a cadence holds the declaration itself.
    ///
    /// Confining these batches does not contain them, it breaks them. macOS
    /// sandboxes do not nest, so a confined suite exits 71 the moment a test
    /// spawns its own `sandbox-exec`, and the `CAIRN_SANDBOXED=1` that rides
    /// along with a policy makes fence-sensitive tests self-skip — a lane that is
    /// structurally red on every branch and says nothing about the tree
    /// (CAIRN-3124). Containment of what a batch may *publish* is a separate
    /// knob: [`MutationPolicy`] owns it, and review stays `PureVerdict`.
    const CHECK_CADENCE_SANDBOX_MODE: ProcessSandboxMode = ProcessSandboxMode::Unconfined;

    /// The single `ProcessBatch` shape both check cadences submit, so the
    /// confinement and ordering contract has one home rather than one copy per
    /// cadence. Checks run sequentially and every item runs even after a red, so
    /// one failing check never hides the verdicts behind it.
    fn check_process_batch(
        mut items: Vec<ProcessBatchItem>,
        runner_context_id: Option<String>,
    ) -> ProcessBatch {
        let mut files: Vec<ProcessBatchFile> = Vec::new();
        for item in &mut items {
            // Zero is the executor protocol's unbounded-process sentinel. Check
            // duration estimates inform placement, but elapsed time cannot turn a
            // live check into a failure.
            item.timeout_ms = 0;
            for (content_env, file_env) in [
                (
                    "CAIRN_CHECK_CHANGED_FILES_CONTENT",
                    "CAIRN_CHECK_CHANGED_FILES",
                ),
                (
                    "CAIRN_CHECK_BASE_CONTENT_CONTENT",
                    "CAIRN_CHECK_BASE_CONTENT",
                ),
            ] {
                if let Some(index) = item.env.iter().position(|(key, _)| key == content_env) {
                    let (_, contents) = item.env.remove(index);
                    if !files.iter().any(|file| file.env == file_env) {
                        files.push(ProcessBatchFile {
                            env: file_env.to_string(),
                            contents,
                        });
                    }
                }
            }
        }
        ProcessBatch {
            files,
            runtime_packages: Vec::new(),
            sequential: true,
            stop_on_error: false,
            sandbox_mode: Self::CHECK_CADENCE_SANDBOX_MODE,
            timeout_bases: Vec::new(),
            items,
            runner_context_id,
            execution_residency: None,
        }
    }

    pub(crate) async fn submit_pure_verdict_batch(
        &self,
        orch: &Orchestrator,
        request: CellRequest,
        items: Vec<PureVerdictBatchItem>,
        run_context: Option<crate::mcp::handlers::RunContext>,
        purpose: Option<purpose::PurposeKey>,
    ) -> Vec<Result<CoalescedCellOutcome, CellOutcome>> {
        if request.mutation_policy != MutationPolicy::PureVerdict {
            let mut outcomes = Vec::with_capacity(items.len());
            for _ in items {
                outcomes.push(Err(executor_unavailable(
                    "coalesced batch submission requires pure-verdict mutation policy".into(),
                )));
            }
            return outcomes;
        }
        let leader = (request.request_id.clone(), request.attempt_id.clone());
        // The chokepoint. An item that names a purpose holds that purpose alone:
        // admitting it is, in the same locked transition, the retirement of
        // whatever older item was answering the same question, and no phase of
        // the older one is exempt from being displaced.
        //
        // Winning the key is only half of it. The successor then waits for the
        // predecessor's terminal outcome before dispatching, because retirement
        // is something the displaced work does rather than something the record
        // says about it.
        if let Some(key) = &purpose {
            let admission =
                self.purposes
                    .lock()
                    .unwrap()
                    .admit(key.clone(), leader.clone(), unix_time_ms());
            match admission {
                purpose::Admission::Admitted => {}
                purpose::Admission::Replaced { retire, phase } => {
                    log::info!(
                        "purpose {key} moves to {leader:?}, retiring {retire:?} from {phase:?}"
                    );
                    self.retire_dispatched_item(&retire);
                }
                purpose::Admission::Superseded { holder } => {
                    // Newest-wins is decided here rather than by which item
                    // reaches an executor first, so a stale one never dispatches
                    // at all and never takes an occupancy row.
                    log::info!(
                        "purpose {key} is already held by {holder:?}; {leader:?} is stale and will not dispatch"
                    );
                    let mut refused = Vec::with_capacity(items.len());
                    for _ in &items {
                        refused.push(Err(CellOutcome::Cancelled {
                            request_id: leader.0.clone(),
                            attempt_id: leader.1.clone(),
                        }));
                    }
                    return refused;
                }
            }
            // Nothing below this line may run while a displaced answer to the
            // same question is still alive somewhere in the fleet.
            match self.await_purpose_quiescent(key, &leader).await {
                PurposeGate::Clear => {}
                PurposeGate::Displaced => {
                    log::info!(
                        "purpose {key} moved on while {leader:?} waited to dispatch; it never ran"
                    );
                    let mut refused = Vec::with_capacity(items.len());
                    for _ in &items {
                        refused.push(Err(CellOutcome::Cancelled {
                            request_id: leader.0.clone(),
                            attempt_id: leader.1.clone(),
                        }));
                    }
                    return refused;
                }
                PurposeGate::Owed { outstanding } => {
                    // Refusing is the whole point. Dispatching here would
                    // knowingly create the overlap this gate exists to prevent,
                    // and a log line is a record rather than a decision anyone
                    // made. A refused wave is visible, is retried by the next
                    // cadence, and leaves the operator a red they can act on;
                    // a silent second attestation leaves them the symptom that
                    // opened this issue.
                    let diagnostic = format!(
                        "purpose {key} still owes a confirmed death after {}s (outstanding \
                         {outstanding:?}); refusing to dispatch a second answer to one question",
                        RETIREMENT_CONFIRMATION_BUDGET.as_secs()
                    );
                    log::warn!("{diagnostic}");
                    self.purposes.lock().unwrap().finish(&leader);
                    self.purpose_quiesced.notify_waiters();
                    let mut refused = Vec::with_capacity(items.len());
                    for _ in &items {
                        refused.push(Err(executor_unavailable(diagnostic.clone())));
                    }
                    return refused;
                }
            }
        }
        let wait_horizon_unix_ms = request.wait_horizon_unix_ms;
        let mut receivers = Vec::with_capacity(items.len());
        let mut newly_claimed = Vec::new();
        {
            let mut registry = self.in_flight.lock().unwrap();
            for (index, item) in items.into_iter().enumerate() {
                let result_identity = item.result_identity.clone();
                let public_identity = (
                    format!("{}:check-{index}", request.request_id),
                    format!("{}:check-{index}", request.attempt_id),
                );
                let (tx, rx) = oneshot::channel();
                if let Some(execution) = registry.by_key.get_mut(&result_identity) {
                    execution.subscribers.insert(
                        public_identity.clone(),
                        CoalescedSubscriber {
                            waiter: tx,
                            priority: request.priority,
                            requesting_job_id: request.requesting_job_id.clone(),
                        },
                    );
                } else {
                    let publication = PublicationCoordination::new();
                    registry.by_key.insert(
                        result_identity.clone(),
                        InFlightExecution {
                            leader: leader.clone(),
                            subscribers: HashMap::from([(
                                public_identity.clone(),
                                CoalescedSubscriber {
                                    waiter: tx,
                                    priority: request.priority,
                                    requesting_job_id: request.requesting_job_id.clone(),
                                },
                            )]),
                            publication,
                        },
                    );
                    newly_claimed.push(item);
                }
                registry
                    .subscriber_keys
                    .insert(public_identity.clone(), result_identity);
                receivers.push((public_identity, rx));
            }
        }
        if !newly_claimed.is_empty() {
            self.coalesced_leaders
                .lock()
                .unwrap()
                .insert(leader.clone());
            let now = unix_time_ms();
            self.preparing_leaders.lock().unwrap().insert(
                leader.clone(),
                LeaderPreparation {
                    since_unix_ms: now,
                    last_progress_unix_ms: now,
                },
            );
            let keys: Vec<_> = newly_claimed
                .iter()
                .map(|item| item.result_identity.clone())
                .collect();
            let coalesced_output = output_guard::RelayedOutput::shared();
            let flushed_context = run_context.clone();
            let runner_context_id = run_context.map(|run_context| {
                let id = uuid::Uuid::new_v4().to_string();
                self.runner_contexts.lock().unwrap().insert(
                    id.clone(),
                    RunnerContextEntry::new(
                        &id,
                        &request.request_id,
                        RunnerCallbackContext {
                            request: None,
                            run_context: Some(run_context),
                            check_status_board: None,
                            live_checkout: false,
                            executor_binding: None,
                            output: coalesced_output.clone(),
                        },
                    ),
                );
                id
            });
            let batch = Self::check_process_batch(
                newly_claimed.into_iter().map(|item| item.process).collect(),
                runner_context_id.clone(),
            );
            let pool = self.clone();
            let orch = orch.clone();
            let completion_guard = CoalescedLeaderCompletionGuard {
                pool: pool.clone(),
                leader: leader.clone(),
                result_identities: keys.clone(),
                runner_context_id: runner_context_id.clone(),
                armed: true,
            };
            tokio::spawn(async move {
                let mut completion_guard = completion_guard;
                let outcome = match pool.prepare_execution(&orch, &request).await {
                    Ok(prepared) => {
                        if let Some(preparing) =
                            pool.preparing_leaders.lock().unwrap().get_mut(&leader)
                        {
                            preparing.last_progress_unix_ms = unix_time_ms();
                        }
                        pool.execute_prepared(request, Some(batch), prepared).await
                    }
                    Err(outcome) => outcome,
                };
                pool.preparing_leaders.lock().unwrap().remove(&leader);
                if let Some(id) = runner_context_id {
                    output_guard::flush_batch_output(
                        &orch,
                        &coalesced_output,
                        flushed_context.as_ref(),
                    );
                    pool.runner_contexts.lock().unwrap().remove(&id);
                }
                pool.cancelled_leaders.lock().unwrap().remove(&leader);
                // The terminal outcome is what actually retires an item, and
                // this is where it lands. Fenced by identity inside the
                // registry, so a displaced item clears only its own debt and
                // never the entry its replacement now holds; the wakeup then
                // releases whichever successor was waiting on this death.
                pool.purposes.lock().unwrap().finish(&leader);
                pool.purpose_quiesced.notify_waiters();
                match &outcome {
                    CellOutcome::Completed {
                        output,
                        metadata,
                        mutation_delta: None,
                        tracked_modifications: None,
                        ..
                    } => match serde_json::from_str::<
                        Vec<cairn_common::executor_protocol::ProcessBatchItemOutcome>,
                    >(output)
                    {
                        Ok(results) if results.len() == keys.len() => {
                            for (key, result) in keys.iter().zip(results) {
                                let mut item_meta = metadata.clone();
                                item_meta.started_at_unix_ms = result.started_at_unix_ms;
                                item_meta.finished_at_unix_ms = result.finished_at_unix_ms;
                                item_meta.duration_ms = Some(result.duration_ms);
                                item_meta.peak_rss_bytes = result.peak_rss_bytes;
                                item_meta.disk_delta_bytes = result.disk_delta_bytes;
                                item_meta.environment_fingerprint = result.environment_fingerprint;
                                pool.complete_coalesced_for_leader(
                                    key,
                                    &leader,
                                    CellOutcome::Completed {
                                        request_id: leader.0.clone(),
                                        attempt_id: leader.1.clone(),
                                        exit_code: result.exit_code,
                                        output: result.body,
                                        timed_out: result.timed_out,
                                        metadata: item_meta,
                                        mutation_delta: None,
                                        sandbox_denials: result.sandbox_denials,
                                        tracked_modifications: result.tracked_modifications,
                                    },
                                );
                            }
                        }
                        Ok(results) => {
                            let failure = CellOutcome::FailedAfterExecution {
                                request_id: leader.0.clone(),
                                attempt_id: leader.1.clone(),
                                diagnostic: format!(
                                    "executor returned {} item outcomes for {} claimed checks",
                                    results.len(),
                                    keys.len()
                                ),
                            };
                            for key in &keys {
                                pool.complete_coalesced_for_leader(key, &leader, failure.clone());
                            }
                        }
                        Err(error) => {
                            let failure = CellOutcome::FailedAfterExecution {
                                request_id: leader.0.clone(),
                                attempt_id: leader.1.clone(),
                                diagnostic: format!("decode typed check batch outcomes: {error}"),
                            };
                            for key in &keys {
                                pool.complete_coalesced_for_leader(key, &leader, failure.clone());
                            }
                        }
                    },
                    CellOutcome::Completed {
                        tracked_modifications: Some(_),
                        ..
                    } => {
                        let failure = CellOutcome::FailedAfterExecution {
                            request_id: leader.0.clone(),
                            attempt_id: leader.1.clone(),
                            diagnostic:
                                "executor returned unattributed batch-level mutation evidence"
                                    .into(),
                        };
                        for key in &keys {
                            pool.complete_coalesced_for_leader(key, &leader, failure.clone());
                        }
                    }
                    _ => {
                        for key in &keys {
                            pool.complete_coalesced_for_leader(key, &leader, outcome.clone());
                        }
                    }
                }
                completion_guard.disarm();
            });
        }
        futures_util::future::join_all(
            receivers
                .into_iter()
                .map(|(identity, rx)| self.await_coalesced(identity, wait_horizon_unix_ms, rx)),
        )
        .await
    }

    /// The confinement a batch runs under.
    ///
    /// The agent's fence dial is the only gate on whether a profile exists: an
    /// `allow` agent's batch is unconfined wherever it runs, including in the
    /// project's live checkout, and a batch with no resolvable run identity is
    /// nobody's agent operation. The repository shape only picks which profile a
    /// *fenced* batch gets — an externally owned live checkout stays readable
    /// with its writes kernel-denied, a cell confines writes to itself.
    fn batch_sandbox_mode(
        fence: Option<crate::models::Fence>,
        repository: &RepositoryLocator,
    ) -> ProcessSandboxMode {
        match fence {
            Some(fence) if crate::services::sandbox::sandbox_applies(fence) => {
                if runs_in_live_checkout(repository) {
                    ProcessSandboxMode::ReadOnlyCheckout
                } else {
                    ProcessSandboxMode::Confined
                }
            }
            _ => ProcessSandboxMode::Unconfined,
        }
    }

    pub(crate) async fn submit_run_batch(
        &self,
        orch: &Orchestrator,
        request: CellRequest,
        batch: ResolvedRunBatch,
    ) -> CellOutcome {
        let runner_context_id = uuid::Uuid::new_v4().to_string();
        let output = output_guard::RelayedOutput::shared();
        let run_context = batch.run_context.clone();
        self.runner_contexts.lock().unwrap().insert(
            runner_context_id.clone(),
            RunnerContextEntry::new(
                &runner_context_id,
                &format!("{}:{}", request.request_id, request.attempt_id),
                RunnerCallbackContext {
                    request: Some(batch.request.clone()),
                    run_context: batch.run_context.clone(),
                    check_status_board: None,
                    live_checkout: runs_in_live_checkout(&request.repository),
                    executor_binding: None,
                    output: output.clone(),
                },
            ),
        );
        let resolved_fence =
            match crate::mcp::handlers::fence::resolve_run_fence(orch, &batch.request).await {
                crate::mcp::handlers::fence::RunFenceResolution::Resolved(_, fence) => Some(fence),
                crate::mcp::handlers::fence::RunFenceResolution::Ambient => None,
                crate::mcp::handlers::fence::RunFenceResolution::Unavailable(diagnostic) => {
                    self.runner_contexts
                        .lock()
                        .unwrap()
                        .remove(&runner_context_id);
                    return CellOutcome::Unavailable {
                        reason: CellUnavailableReason::RunnerContext,
                        diagnostic,
                    };
                }
            };
        let sandbox_mode = Self::batch_sandbox_mode(resolved_fence, &request.repository);
        let batch = match serialize_process_batch(
            batch,
            &request.env,
            runner_context_id.clone(),
            sandbox_mode,
        ) {
            Ok(batch) => batch,
            Err(diagnostic) => {
                return CellOutcome::Unavailable {
                    reason: CellUnavailableReason::Spawn,
                    diagnostic,
                }
            }
        };
        let outcome = self.submit_execution(orch, request, Some(batch)).await;
        output_guard::flush_batch_output(orch, &output, run_context.as_ref());
        self.runner_contexts
            .lock()
            .unwrap()
            .remove(&runner_context_id);
        outcome
    }

    /// Submit one cadence-coherent check batch without result coalescing.
    ///
    /// Mutating write checks cannot be coalesced per item: the executor returns a
    /// single end-of-batch delta and every item must observe earlier mutations.
    /// Review checks continue through the pure-verdict submission path.
    pub(crate) async fn submit_write_check_batch(
        &self,
        orch: &Orchestrator,
        request: CellRequest,
        items: Vec<ProcessBatchItem>,
        run_context: Option<crate::mcp::handlers::RunContext>,
        check_status_board: Option<crate::execution::checks::CheckStatusBoard>,
    ) -> CellOutcome {
        if request.mutation_policy != MutationPolicy::AllowDelta {
            return executor_unavailable(
                "write-check batch submission requires allow-delta mutation policy".into(),
            );
        }
        let runner_context_id = uuid::Uuid::new_v4().to_string();
        let output = output_guard::RelayedOutput::shared();
        let flushed_context = run_context.clone();
        self.runner_contexts.lock().unwrap().insert(
            runner_context_id.clone(),
            RunnerContextEntry::new(
                &runner_context_id,
                &format!("{}:{}", request.request_id, request.attempt_id),
                RunnerCallbackContext {
                    request: None,
                    run_context,
                    check_status_board,
                    live_checkout: false,
                    executor_binding: None,
                    output: output.clone(),
                },
            ),
        );
        let batch = Self::check_process_batch(items, Some(runner_context_id.clone()));
        let outcome = self.submit_execution(orch, request, Some(batch)).await;
        output_guard::flush_batch_output(orch, &output, flushed_context.as_ref());
        self.runner_contexts
            .lock()
            .unwrap()
            .remove(&runner_context_id);
        outcome
    }

    /// Publish process output without entering the serialized core task lane.
    /// A run request itself waits for the executor result on that lane, so routing
    /// output callbacks through it deadlocks the producer behind its own consumer.
    pub fn handle_process_output(
        &self,
        orch: &Orchestrator,
        context_id: &str,
        stream_id: &str,
        payload: String,
    ) -> RunnerCallbackResult {
        let Some(context) = self
            .runner_contexts
            .lock()
            .unwrap()
            .get(context_id)
            .map(|entry| entry.context.clone())
        else {
            return RunnerCallbackResult::Failed {
                diagnostic: "unknown or expired runner callback context".into(),
            };
        };
        // Scrubbed before it is published, against this runner's registry rather
        // than the executor's. See `output_guard` for why both passes exist and
        // why neither subsumes the other. The withheld tail is released by
        // `flush_batch_output` when the batch settles.
        let payload = context
            .output
            .lock()
            .unwrap()
            .settle_text(stream_id, &payload);
        if let Some(run_context) = context.run_context {
            if let Some(board) = context.check_status_board {
                if let Some(index) = stream_id
                    .rsplit(":check-")
                    .next()
                    .and_then(|value| value.parse().ok())
                {
                    board.transition(index, "running", None);
                }
            }
            // A chunk that was entirely withheld is not a chunk yet. Publishing
            // an empty one would render as a flicker with nothing behind it.
            if !payload.is_empty() {
                let _ = orch.services.emitter.emit(
                    "run-output",
                    serde_json::json!({
                        "runId": run_context.run_id,
                        "toolUseId": stream_id,
                        "chunk": payload,
                        "stream": "stdout",
                    }),
                );
            }
        }
        RunnerCallbackResult::Completed
    }

    pub async fn handle_runner_callback(
        &self,
        orch: &Orchestrator,
        callback: RunnerCallback,
    ) -> RunnerCallbackResult {
        if let RunnerCallback::ProcessEvent {
            runner_context_id,
            stream_id,
            payload,
        } = callback
        {
            return self.handle_process_output(orch, &runner_context_id, &stream_id, payload);
        }
        let context_id = match &callback {
            RunnerCallback::SandboxDenied {
                runner_context_id, ..
            }
            | RunnerCallback::CacheCheckpoint {
                runner_context_id, ..
            }
            | RunnerCallback::ProcessItemStarted {
                runner_context_id, ..
            }
            | RunnerCallback::ProcessItemCompleted {
                runner_context_id, ..
            } => runner_context_id,
            RunnerCallback::ProcessEvent { .. } => unreachable!("handled above"),
        };
        let Some(context) = self
            .runner_contexts
            .lock()
            .unwrap()
            .get(context_id)
            .map(|entry| entry.context.clone())
        else {
            return RunnerCallbackResult::Failed {
                diagnostic: "unknown or expired runner callback context".into(),
            };
        };
        match callback {
            RunnerCallback::ProcessItemStarted { stream_id, .. } => {
                if let Some(board) = context.check_status_board {
                    if let Some(index) = check_index_from_stream_id(&stream_id) {
                        board.transition(index, "running", None);
                    }
                }
                RunnerCallbackResult::Completed
            }
            RunnerCallback::ProcessItemCompleted {
                stream_id,
                succeeded,
                exit_code,
                timed_out,
                duration_ms,
                ..
            } => {
                if let Some(board) = context.check_status_board {
                    if let Some(index) = check_index_from_stream_id(&stream_id) {
                        let annotation = if succeeded {
                            Some(format_duration_annotation(duration_ms))
                        } else {
                            Some(match exit_code {
                                Some(code) => format!("exit {code}"),
                                None if timed_out => "timed out".into(),
                                None => "failed".into(),
                            })
                        };
                        board.transition(
                            index,
                            if succeeded { "passed" } else { "failed" },
                            annotation,
                        );
                    }
                }
                RunnerCallbackResult::Completed
            }
            RunnerCallback::SandboxDenied {
                command, denial, ..
            } => {
                if context.live_checkout {
                    return RunnerCallbackResult::Rejected {
                        diagnostic: crate::mcp::handlers::run::READ_ONLY_CHECKOUT_DENIAL.into(),
                    };
                }
                use crate::mcp::handlers::fence::{self, FenceDecision};
                let Some(request) = context.request.as_ref() else {
                    return RunnerCallbackResult::Rejected {
                        diagnostic:
                            "check batch sandbox denial cannot be interactively adjudicated".into(),
                    };
                };
                let (run_id, mode) = match fence::resolve_run_fence(orch, request).await {
                    fence::RunFenceResolution::Resolved(run_id, mode) => (run_id, mode),
                    fence::RunFenceResolution::Unavailable(diagnostic) => return RunnerCallbackResult::Failed { diagnostic },
                    fence::RunFenceResolution::Ambient => return RunnerCallbackResult::Failed {
                        diagnostic: "permission context unavailable: sandbox denial has no authenticated originating run".into(),
                    },
                };
                let crossing = match denial {
                    cairn_common::executor_protocol::SandboxDenial::Path(path) => {
                        let path = std::path::PathBuf::from(path);
                        fence::Crossing::shell_path(&path, &path.display().to_string())
                    }
                    cairn_common::executor_protocol::SandboxDenial::Command => {
                        fence::Crossing::shell_command(
                            format!("command blocked by the executor worktree sandbox: {command}"),
                            &command,
                        )
                    }
                };
                match fence::raise_fence(orch, &run_id, mode, request, crossing).await {
                    FenceDecision::Allow => RunnerCallbackResult::Allowed,
                    FenceDecision::Deny(diagnostic) => {
                        RunnerCallbackResult::Rejected { diagnostic }
                    }
                    FenceDecision::Unavailable(diagnostic) => {
                        RunnerCallbackResult::Failed { diagnostic }
                    }
                    FenceDecision::Suspended => RunnerCallbackResult::Suspended,
                }
            }
            RunnerCallback::CacheCheckpoint {
                command,
                cwd,
                exit_code,
                ..
            } => {
                if let Some(run_context) = context.run_context {
                    crate::mcp::handlers::run::cache_checkpoint_callback(
                        orch,
                        &run_context.job_id,
                        &command,
                        &cwd,
                        exit_code,
                    )
                    .await;
                }
                RunnerCallbackResult::Completed
            }
            RunnerCallback::ProcessEvent { .. } => unreachable!("handled above"),
        }
    }

    async fn submit_execution(
        &self,
        orch: &Orchestrator,
        request: CellRequest,
        batch: Option<ProcessBatch>,
    ) -> CellOutcome {
        let prepared = match self.prepare_execution(orch, &request).await {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        self.execute_prepared(request, batch, prepared).await
    }

    async fn prepare_execution(
        &self,
        orch: &Orchestrator,
        request: &CellRequest,
    ) -> Result<PreparedExecution, CellOutcome> {
        let config = crate::config::settings::load_settings_file(&orch.config_dir)
            .map_err(|error| CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                diagnostic: format!("load cell settings: {error}"),
            })?
            .fleet
            .unwrap_or_default();
        if matches!(request.repository, RepositoryLocator::ScratchOnly { .. }) {
            return Ok(PreparedExecution {
                cell_client_env: Vec::new(),
                placement_policy: ActivePlacementPolicy {
                    name: config.active_placement_profile.clone(),
                    profile: config.active_placement_profile().clone(),
                },
                executor_config: ExecutorConfig {
                    project_id: request.project_id.clone(),
                    project_key: request.project_id.clone(),
                    default_timeout_seconds: config.default_timeout_seconds,
                    setup_commands: Vec::new(),
                    populate: cairn_cell::PopulateConfig::default(),
                    population_source_root: None,
                },
                object_plane: orch.object_plane.clone(),
                db: orch.db.local.clone(),
            });
        }
        let (local_repo_path, project_key) =
            crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, &request.project_id)
                .await
                .map_err(|error| CellOutcome::Unavailable {
                    reason: CellUnavailableReason::ExecutorUnavailable,
                    diagnostic: format!("resolve cell project key: {error}"),
                })?;
        let project_path = local_repo_path.as_deref().or(match &request.repository {
            RepositoryLocator::ColocatedPath { absolute_path, .. }
            | RepositoryLocator::ExistingCheckout { absolute_path, .. } => {
                Some(absolute_path.as_str())
            }
            RepositoryLocator::ManagedObjects { .. } | RepositoryLocator::ScratchOnly { .. } => {
                None
            }
        });
        let project_path = project_path.ok_or_else(|| CellOutcome::Unavailable {
            reason: CellUnavailableReason::Preparation,
            diagnostic: "resolve canonical project setup: no local project checkout".into(),
        })?;
        // Resolve setup from the primary checkout, from the canonical project configuration;
        // the requested commit may contain an older project configuration. This hot path is
        // deliberately fallible and side-effect free: it must neither migrate config nor run
        // a command after invalid setup policy was defaulted away.
        let project_policy =
            crate::config::project_settings::load_execution_project_policy(Path::new(project_path))
                .map_err(|error| CellOutcome::Unavailable {
                    reason: CellUnavailableReason::Preparation,
                    diagnostic: format!("load canonical project execution policy: {error}"),
                })?;
        Ok(PreparedExecution {
            cell_client_env: cell_build_service_env(orch, &request.repository),
            placement_policy: ActivePlacementPolicy {
                name: config.active_placement_profile.clone(),
                profile: config.active_placement_profile().clone(),
            },
            executor_config: ExecutorConfig {
                project_id: request.project_id.clone(),
                project_key,
                default_timeout_seconds: config.default_timeout_seconds,
                setup_commands: project_policy.setup_commands,
                populate: project_policy.populate,
                population_source_root: Some(project_path.to_string()),
            },
            object_plane: orch.object_plane.clone(),
            db: orch.db.local.clone(),
        })
    }

    async fn execute_prepared(
        &self,
        request: CellRequest,
        batch: Option<ProcessBatch>,
        prepared: PreparedExecution,
    ) -> CellOutcome {
        let PreparedExecution {
            executor_config,
            object_plane,
            db,
            cell_client_env,
            placement_policy,
        } = prepared;
        let mut request = request;
        if let Err(diagnostic) = require_colocated_population(&mut request, &executor_config) {
            return CellOutcome::Unavailable {
                reason: CellUnavailableReason::NoMatchingExecutor,
                diagnostic,
            };
        }
        let plan = ReservationPlan::new(db.clone(), &request, batch.as_ref());
        let Placement {
            selected,
            reservation,
            mut decision,
        } = match self
            .select_executor(&request, Some(&plan), &placement_policy)
            .await
        {
            Ok(placement) => placement,
            Err(outcome) => return outcome,
        };
        // Only now is the machine known. The compile daemon is this machine's,
        // reachable on loopback and named by its paths, so a batch placed on a
        // remote executor keeps whatever cache that host configured for itself.
        let batch = match batch {
            Some(batch) if selected.colocated && !cell_client_env.is_empty() => {
                Some(with_cell_client_env(batch, &cell_client_env))
            }
            batch => batch,
        };
        let profile_context = ReservationPlan::profile_context(&selected);
        let batch_profile_identities = plan.batch_identities.clone();
        if let Some(resolved) = reservation {
            request.resource_reservation = resolved.reservation;
            request.learned_estimate = resolved.learned_estimate;
        }
        let profile_identity = request.command_resource_identity.clone();
        if !selected.colocated {
            let identity = request.repository.identity();
            request.repository = RepositoryLocator::ManagedObjects {
                project_id: identity.project_id,
                repository_id: identity.repository_id,
                object_format: identity.object_format,
            };
            object_plane.authorize_request(&request, &selected.executor_id, selected.generation);
            // The coordinate the objects travel under is only knowable once the
            // machine is chosen, and it is what makes a remote object refusal
            // actionable from the decision record alone.
            if let PlacementOutcome::Selected(selection) = &mut decision.outcome {
                selection.object_transfer = Some(ObjectTransferCoordinate {
                    repository: request.repository.identity(),
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    executor_id: selected.executor_id.clone(),
                    connection_generation: selected.generation,
                });
            }
        }
        self.record_placement_decision(decision);
        let key = (request.request_id.clone(), request.attempt_id.clone());
        let (tx, rx) = oneshot::channel();
        if self
            .pending
            .lock()
            .unwrap()
            .insert(
                key.clone(),
                PendingResult {
                    executor_id: selected.executor_id.clone(),
                    generation: selected.generation,
                    requesting_job_id: request.requesting_job_id.clone(),
                    waiter: tx,
                },
            )
            .is_some()
        {
            return executor_unavailable("duplicate cell request identity".into());
        }
        self.preparing_leaders.lock().unwrap().remove(&key);
        let mut guard = SubmitDropGuard {
            pool: self.clone(),
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
            executor_id: selected.executor_id.clone(),
            generation: selected.generation,
            armed: true,
        };
        let watchdog = request_watchdog_duration(
            &request,
            batch.as_ref(),
            &executor_config,
            selected.colocated,
        );
        if let Err(diagnostic) = self.bind_runner_context(
            batch.as_ref(),
            &request,
            &selected.executor_id,
            selected.generation,
        ) {
            self.pending.lock().unwrap().remove(&key);
            guard.disarm();
            return executor_unavailable(diagnostic);
        }
        // A retirement that landed while this item was being placed stops it
        // here. Submitting anyway and chasing it with a Cancel is what made
        // retirement a race the work usually won: the submitted item took an
        // occupancy row, and the cancel behind it could be handled before the
        // executor had bound the identity it named.
        if self.cancelled_leaders.lock().unwrap().remove(&key) {
            self.pending.lock().unwrap().remove(&key);
            if !selected.colocated {
                object_plane.revoke_request(
                    &key.0,
                    &key.1,
                    &selected.executor_id,
                    selected.generation,
                );
            }
            guard.disarm();
            return CellOutcome::Cancelled {
                request_id: key.0,
                attempt_id: key.1,
            };
        }
        let sent = selected
            .sender
            .send(ExecutorMessage::Submit {
                config: executor_config,
                request,
                batch,
            })
            .is_ok();
        if sent {
            self.purposes
                .lock()
                .unwrap()
                .advance(&key, purpose::ItemPhase::Dispatched);
        }
        // Retirement can still land in the instant between the gate above and
        // this send, so the delivered form remains.
        let cancelled_before_correlation = self.cancelled_leaders.lock().unwrap().remove(&key);
        if cancelled_before_correlation {
            let _ = self.send_to(
                &selected.executor_id,
                selected.generation,
                ExecutorMessage::Cancel {
                    request_id: key.0.clone(),
                    attempt_id: key.1.clone(),
                },
            );
        }
        if !sent {
            self.pending.lock().unwrap().remove(&key);
            if !selected.colocated {
                object_plane.revoke_request(
                    &key.0,
                    &key.1,
                    &selected.executor_id,
                    selected.generation,
                );
            }
            guard.disarm();
            return executor_unavailable(
                "executor connection closed while submitting request".into(),
            );
        }
        let (outcome, watchdog_expired) = match self
            .await_bounding_silence(rx, watchdog, || {
                self.request_substrate_hold(
                    &selected.executor_id,
                    selected.generation,
                    &key.0,
                    &key.1,
                )
                .is_some()
            })
            .await
        {
            SilenceWatchdog::Answered(outcome) => (outcome, false),
            SilenceWatchdog::Dropped => (
                executor_unavailable("executor result channel closed".into()),
                false,
            ),
            SilenceWatchdog::Silent => {
                let substrate =
                    self.executor_deadline_evidence(&selected.executor_id, selected.generation);
                (
                    CellOutcome::Unavailable {
                        reason: CellUnavailableReason::Deadline {
                            host_pressure: None,
                            substrate: Some(substrate.clone()),
                        },
                        diagnostic: format!(
                            "executor did not return request {} attempt {} within the end-to-end watchdog budget; waiting on {:?} since {} with last progress at {}; the in-flight attempt was cancelled",
                            key.0,
                            key.1,
                            substrate.state,
                            substrate.since_unix_ms,
                            substrate.last_progress_unix_ms,
                        ),
                    },
                    true,
                )
            }
        };
        if !selected.colocated {
            object_plane.revoke_request(&key.0, &key.1, &selected.executor_id, selected.generation);
        }
        // The executor names the job, request, and commit it could not
        // materialize; only the runner knows which machine it chose. Completing
        // the coordinate here is what makes a remote refusal actionable without
        // a second lookup: the operator reading it is looking at one failed
        // placement among several enrolled machines.
        let outcome = if selected.colocated {
            outcome
        } else {
            name_placement_in_object_refusal(outcome, &selected.executor_id, selected.generation)
        };
        if watchdog_expired {
            return outcome;
        }
        if let CellOutcome::Completed {
            output, metadata, ..
        } = &outcome
        {
            if batch_profile_identities.is_empty() {
                resource_profiles::observe_completed(
                    db,
                    profile_identity.as_ref(),
                    &profile_context,
                    metadata,
                )
                .await;
            } else if let Ok(items) = serde_json::from_str::<
                Vec<cairn_common::executor_protocol::ProcessBatchItemOutcome>,
            >(output)
            {
                for (identity, item) in batch_profile_identities.iter().zip(items) {
                    let mut item_meta = metadata.clone();
                    item_meta.started_at_unix_ms = item.started_at_unix_ms;
                    item_meta.finished_at_unix_ms = item.finished_at_unix_ms;
                    item_meta.duration_ms = Some(item.duration_ms);
                    item_meta.peak_rss_bytes = item.peak_rss_bytes;
                    item_meta.disk_delta_bytes = item.disk_delta_bytes;
                    resource_profiles::observe_completed(
                        db.clone(),
                        Some(identity),
                        &profile_context,
                        &item_meta,
                    )
                    .await;
                }
            }
        }
        guard.disarm();
        outcome
    }

    /// Wait for an executor this request can be placed on, bounded by the
    /// requester's horizon.
    ///
    /// `request` is immutable on purpose. Selection used to push the horizon
    /// forward for every interval it spent watching a supervisor come back,
    /// which is the deadline-pause model surviving in a second place: it made
    /// the number this function hands to the executor later than the instant the
    /// requester actually declared, and under a supervisor that keeps declaring
    /// itself fresh it advanced at wall-clock rate, so a batch that had stated
    /// its whole willingness to wait could be held past it without bound.
    ///
    /// Nothing pauses a horizon. A wait here ends when an executor appears or
    /// when the requester's own instant arrives, and a machine with nothing
    /// being supervised at all still fails immediately rather than waiting.
    async fn select_executor(
        &self,
        request: &CellRequest,
        plan: Option<&ReservationPlan>,
        policy: &ActivePlacementPolicy,
    ) -> Result<Placement, CellOutcome> {
        loop {
            let notified = self.connection_ready.notified();
            let selection = self
                .select_executor_once_with(request, plan, policy, repository_sync_cost)
                .await;
            match selection {
                Ok(Some(placement)) => return Ok(placement),
                Err(refused) => {
                    // A refusal is a decision with the same evidence attached as
                    // a success, and it is recorded as one.
                    self.record_placement_decision(refused.decision);
                    return Err(CellOutcome::Unavailable {
                        reason: CellUnavailableReason::NoMatchingExecutor,
                        diagnostic: refused.diagnostic,
                    });
                }
                Ok(None) => {}
            }
            let now = unix_time_ms();
            let transient = self.colocated_substrate().filter(|evidence| {
                now.saturating_sub(evidence.last_progress_unix_ms) <= EXECUTOR_PROGRESS_FRESHNESS_MS
            });
            let targeted = request.pinned_executor_id.is_some()
                || request
                    .executor
                    .as_ref()
                    .is_some_and(|selector| !selector.is_empty());
            if transient.is_none() && !targeted {
                return Err(executor_unavailable(
                    "no colocated executor is configured, enrolled, or being supervised".into(),
                ));
            }
            let remaining = request.wait_horizon_unix_ms.saturating_sub(now);
            if remaining == 0 {
                // An untargeted request reaches here whenever a live substrate
                // never freed capacity within the horizon, so the diagnostic
                // cannot assume a selector was stated. When one was, the caller
                // is owed both what it asked for and what exists.
                let connections = self.connections.lock().unwrap().clone();
                return Err(CellOutcome::Unavailable {
                    reason: CellUnavailableReason::NoMatchingExecutor,
                    diagnostic: if targeted {
                        format!(
                            "no executor satisfying {} became usable before this request's wait horizon. Known executors: {}. Read cairn://executors for live state.",
                            request
                                .executor
                                .as_ref()
                                .filter(|selector| !selector.is_empty())
                                .map(|selector| selector.describe())
                                .unwrap_or_else(|| "this job's execution home".to_string()),
                            known_executor_inventory(&connections)
                        )
                    } else if !request.verdict_platforms.is_empty() {
                        // Trust is why nothing here was usable, so trust is what
                        // the diagnostic has to name. An operator reading "no
                        // executor became usable" beside a fleet of idle
                        // machines would have no way to tell that they were
                        // passed over deliberately.
                        format!(
                            "no executor on {} — the platform(s) this verdict counts from — became usable before this request's wait horizon. Known executors: {}. Read cairn://executors for live state.",
                            request.verdict_platforms.join(", "),
                            known_executor_inventory(&connections)
                        )
                    } else {
                        "no executor became usable before this request's wait horizon".to_string()
                    },
                });
            }
            let _ = tokio::time::timeout(Duration::from_millis(remaining.clamp(1, 250)), notified)
                .await;
        }
    }

    async fn select_executor_once_with(
        &self,
        request: &CellRequest,
        plan: Option<&ReservationPlan>,
        policy: &ActivePlacementPolicy,
        estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
    ) -> Result<Option<Placement>, RefusedPlacement> {
        // Placement can inspect the local repository to estimate transfer cost
        // and the profile store to estimate demand. Snapshot the bounded executor
        // metadata first so that neither ever holds the connection lock needed by
        // transport-side heartbeat and snapshot processing.
        let connections = self.connections.lock().unwrap().clone();
        let now = unix_time_ms();
        // Stage one: which machines could structurally take this at all. Keep the
        // actual survey's refusal while the shadow is ranked independently: an
        // unavailable home is the most important case for the counterfactual.
        let actual_survey = survey_candidates(&connections, request, now);
        let shadow_request = hermetic_shadow_request(request);
        let shadow_survey = shadow_request
            .as_ref()
            .and_then(|shadow| survey_candidates(&connections, shadow, now).ok());
        // Stage two: what it would cost on each of them. Resource profiles are
        // executor-context keyed, so this is a per-candidate question -- and it
        // is asked only of candidates, so a request waiting for an executor to
        // attach asks nothing at all. Pure with respect to placement: nothing
        // here reserves anything.
        let mut reservations = HashMap::new();
        let mut predictions = HashMap::new();
        let mut sync_costs = HashMap::new();
        let oracle = DurationOracle {
            db: plan.map(|plan| plan.db.clone()),
        };
        let mut estimation_entries = actual_survey
            .as_ref()
            .map(|survey| survey.usable.clone())
            .unwrap_or_default();
        if let Some(shadow) = shadow_survey.as_ref() {
            for entry in &shadow.usable {
                if !estimation_entries
                    .iter()
                    .any(|candidate| candidate.identity.executor_id == entry.identity.executor_id)
                {
                    estimation_entries.push(entry);
                }
            }
        }
        for entry in &estimation_entries {
            let context = ReservationPlan::context_for(
                &entry.identity.device_id,
                &entry.identity.executor_id,
                &entry.advertisement.capabilities,
            );
            let warmth = candidate_warmth(entry, request, now);
            let duration_context = resource_profiles::DurationContext {
                class: request.command_class,
                warmth: warmth.for_lookup(),
                now_unix_ms: now,
            };
            let run = match plan {
                Some(plan) => {
                    let resolved = plan
                        .resolve_for(
                            request,
                            &entry.identity.device_id,
                            &entry.identity.executor_id,
                            &entry.advertisement.capabilities,
                            duration_context,
                        )
                        .await;
                    let run = resolved.duration.clone();
                    reservations.insert(entry.identity.executor_id.clone(), resolved);
                    run
                }
                None => {
                    oracle
                        .predict(
                            request.command_resource_identity.as_ref(),
                            &context,
                            warmth.for_lookup(),
                            request.command_class,
                            now,
                        )
                        .await
                }
            };
            let load = candidate_load(entry);
            let contention = match &oracle.db {
                Some(db) => {
                    resource_profiles::resolve_contention(db.clone(), &context, &load, now).await
                }
                None => resource_profiles::contention_prior(&load),
            };
            let prices = QueuePrices::resolve(&oracle, entry, &context, now).await;
            let queue = forecast_queue_wait(entry, &prices, request, now);
            let sync_cost = estimate(request, entry);
            sync_costs.insert(entry.identity.executor_id.clone(), sync_cost);
            predictions.insert(
                entry.identity.executor_id.clone(),
                placement_prediction_with_contention(
                    entry, warmth, queue, run, contention, sync_cost,
                ),
            );
        }
        // Stage three: rank on when each machine is predicted to answer. Budget
        // inspection and consumption share one short critical section, after all
        // asynchronous prediction work, so concurrent waves cannot spend the
        // same exploration slot.
        let mut exploration_by_work_class = self.exploration_by_work_class.lock().unwrap();
        let exploration = exploration_by_work_class
            .entry(request.placement_work_class)
            .or_default();
        let unpinned_winner = match (shadow_request.as_ref(), shadow_survey) {
            (Some(shadow_request), Some(shadow_survey)) => rank_survey(
                shadow_request,
                shadow_survey,
                SurveyRankingContext {
                    reservations: &reservations,
                    predictions: &predictions,
                    sync_costs: &sync_costs,
                    policy,
                    now_unix_ms: now,
                    exploration,
                },
            )
            .ok()
            .and_then(|draft| draft.selected)
            .map(|(_, selection)| PlacementCounterfactualWinner {
                executor_name: selection.executor_name,
                executor_id: selection.executor_id,
                reason: selection.reason,
                prediction: selection.prediction,
            }),
            _ => None,
        };
        let survey = match actual_survey {
            Ok(survey) => survey,
            Err(diagnostic) => {
                return Err(refused_placement(
                    request,
                    now,
                    policy,
                    unpinned_winner,
                    diagnostic,
                ));
            }
        };
        let draft = match rank_survey(
            request,
            survey,
            SurveyRankingContext {
                reservations: &reservations,
                predictions: &predictions,
                sync_costs: &sync_costs,
                policy,
                now_unix_ms: now,
                exploration,
            },
        ) {
            Ok(draft) => draft,
            Err(diagnostic) => {
                return Err(refused_placement(
                    request,
                    now,
                    policy,
                    unpinned_winner,
                    diagnostic,
                ));
            }
        };
        let Some((selected, selection)) = draft.selected else {
            return Ok(None);
        };
        let is_current = self
            .connections
            .lock()
            .unwrap()
            .get(&selected.executor_id)
            .is_some_and(|entry| {
                entry.generation == selected.generation
                    && entry.sender.same_channel(&selected.sender)
                    && !entry.sender.is_closed()
            });
        if !is_current {
            // A reconnect can replace the selected generation while repository
            // and demand estimation are in flight. Let the outer placement loop
            // rank the fresh connection instead of dispatching through a retired
            // sender.
            return Ok(None);
        }
        if request.placement_mobility.may_spill() && !placement_settled_by_caller(request) {
            exploration.record(
                (selection.reason == PlacementReason::BootstrapExploration)
                    .then_some(selection.executor_id.as_str()),
            );
            exploration.record_selection(selection.executor_id.as_str());
        }
        drop(exploration_by_work_class);
        let reservation = reservations.remove(&selected.executor_id);
        let decision = placement_decision(
            request,
            now,
            Some(draft.policy),
            unpinned_winner,
            PlacementOutcome::Selected(Box::new(selection)),
            draft.rejected,
        );
        Ok(Some(Placement {
            selected,
            reservation,
            decision,
        }))
    }

    #[cfg(test)]
    async fn wait_for_executor(
        &self,
        wait_horizon_unix_ms: u64,
    ) -> Result<mpsc::UnboundedSender<ExecutorMessage>, String> {
        let request = CellRequest {
            request_id: String::new(),
            attempt_id: String::new(),
            project_id: String::new(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: String::new(),
                repository_id: String::new(),
                absolute_path: String::new(),
            },
            base_commit: String::new(),
            command: String::new(),
            command_class: cairn_common::executor_protocol::CellCommandClass::Other,
            placement_work_class:
                cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            wait_horizon_unix_ms,
            waiting_since_unix_ms: 0,
            timeout_ms: 0,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        };
        self.select_executor(&request, None, &ActivePlacementPolicy::default_profile())
            .await
            .map(|placement| placement.selected.sender)
            .map_err(|outcome| match outcome {
                CellOutcome::Unavailable { diagnostic, .. } => diagnostic,
                _ => "executor unavailable".into(),
            })
    }

    fn send_to(
        &self,
        executor_id: &str,
        generation: u64,
        message: ExecutorMessage,
    ) -> Result<(), String> {
        let sender = self
            .connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| entry.generation == generation)
            .map(|entry| entry.sender.clone())
            .ok_or_else(|| "executor is not connected at the selected generation".to_string())?;
        sender
            .send(message)
            .map_err(|_| "executor connection is closed".to_string())
    }

    /// Hold this executor's running work across a link gap.
    ///
    /// A link going away is a fact about a socket, not about the machine behind
    /// it. A `cairn-executor` keeps its cells running through a gap and dials
    /// back on its own, so the commands it is running are still running and
    /// their results are still coming. Resolving them the instant a transport
    /// blip lands is what made every blip cost whatever the machine was doing:
    /// one check lane died sixteen minutes into its run that way, and another
    /// died part-way through writing objects that a different node was then
    /// wedged without.
    ///
    /// Held work is bounded from three directions, none of them a clock that
    /// only knows how long something has been quiet: the reattached connection
    /// accounts for it against its first snapshot
    /// ([`Fleet::reconcile_held_executions`]), the runner abandons it as soon as
    /// it knows the process is gone ([`Fleet::abandon_held_executions`]), and
    /// each submitter's own silence watchdog remains underneath.
    fn hold_executions_across_link_gap(&self, executor_id: &str) -> usize {
        self.fail_calls_for_executor(
            executor_id,
            "executor connection closed before answering this call",
        );
        let held = self
            .pending
            .lock()
            .unwrap()
            .values()
            .filter(|entry| entry.executor_id == executor_id)
            .count();
        if held > 0 {
            log::info!(
                "holding {held} in-flight execution(s) for executor_id={executor_id} across a link gap"
            );
        }
        held
    }

    /// Re-bind work held across a gap to the connection that came back.
    ///
    /// What identifies an execution is what it is on the machine, not the link
    /// that carried the order, so a result computed during the gap arrives on
    /// the new generation and has to be accepted there.
    fn rebind_held_executions(
        &self,
        executor_id: &str,
        generation: u64,
    ) -> HashSet<RequestIdentity> {
        let mut pending = self.pending.lock().unwrap();
        let mut carried = HashSet::new();
        for (key, entry) in pending.iter_mut() {
            if entry.executor_id == executor_id && entry.generation != generation {
                entry.generation = generation;
                carried.insert(key.clone());
            }
        }
        carried
    }

    /// Settle work held across a gap against what the machine says it has.
    ///
    /// The first snapshot of the new connection is the machine's own account of
    /// what it is running. Anything held that does not appear in it did not
    /// survive — the process behind the link was replaced while the link was
    /// down — and is resolved now rather than left waiting for a result nobody
    /// is going to send.
    fn reconcile_held_executions(
        &self,
        executor_id: &str,
        generation: u64,
        snapshot: &FleetSnapshot,
    ) {
        let held = {
            let mut carried = self.unreconciled_executions.lock().unwrap();
            match carried.get(executor_id) {
                Some((held_generation, _)) if *held_generation == generation => {
                    carried.remove(executor_id).map(|(_, keys)| keys)
                }
                _ => None,
            }
        };
        let Some(held) = held else { return };
        let identify =
            |request_id: &String, attempt_id: &String| (request_id.clone(), attempt_id.clone());
        let accounted: HashSet<RequestIdentity> = snapshot
            .executing_requests
            .iter()
            .map(|request| identify(&request.request_id, &request.attempt_id))
            .chain(
                snapshot
                    .queued_requests
                    .iter()
                    .map(|request| identify(&request.request_id, &request.attempt_id)),
            )
            .chain(
                snapshot
                    .recent_completions
                    .iter()
                    .map(|completion| identify(&completion.request_id, &completion.attempt_id)),
            )
            .collect();
        for key in held.difference(&accounted) {
            let entry = {
                let mut pending = self.pending.lock().unwrap();
                match pending.get(key) {
                    Some(entry)
                        if entry.executor_id == executor_id && entry.generation == generation =>
                    {
                        pending.remove(key)
                    }
                    _ => None,
                }
            };
            let Some(entry) = entry else { continue };
            log::warn!(
                "executor_id={executor_id} reattached without request {} attempt {}",
                key.0,
                key.1
            );
            let _ = entry.waiter.send(executor_unavailable(format!(
                "the executor reattached without request {} attempt {}: it is no longer running or queued on that machine",
                key.0, key.1
            )));
        }
    }

    /// Give up on work held across a gap, because the process running it is
    /// gone.
    ///
    /// Only the runner's own supervisor knows this: it is what kills a remote
    /// process group, and what watches the carrier that process lives under.
    pub fn abandon_held_executions(&self, executor_id: &str, diagnostic: &str) {
        self.unreconciled_executions
            .lock()
            .unwrap()
            .remove(executor_id);
        self.fail_executions_for_executor(executor_id, diagnostic);
    }

    /// Resolve everything this executor owed, because none of it is coming.
    fn fail_for_executor(&self, executor_id: &str, diagnostic: &str) {
        self.fail_executions_for_executor(executor_id, diagnostic);
        self.fail_calls_for_executor(executor_id, diagnostic);
    }

    /// Resolve the cell executions this executor was running.
    ///
    /// Separate from the calls below because only this half has work behind it.
    /// An execution is a command that is running on a machine right now, and
    /// deciding it is lost is a decision about that machine's process — not one
    /// a socket is entitled to make on its own.
    fn fail_executions_for_executor(&self, executor_id: &str, diagnostic: &str) {
        let mut pending = self.pending.lock().unwrap();
        let keys: Vec<_> = pending
            .iter()
            .filter(|(_, entry)| entry.executor_id == executor_id)
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            if let Some(entry) = pending.remove(&key) {
                let _ = entry
                    .waiter
                    .send(executor_unavailable(diagnostic.to_string()));
            }
        }
    }

    /// Resolve the calls whose answers were in flight when the link went.
    ///
    /// A call has nothing running behind it worth preserving: it is a question
    /// that is cheap to ask again, and the answer it was waiting for described
    /// a moment that has already passed. These fail on any link loss.
    fn fail_calls_for_executor(&self, executor_id: &str, diagnostic: &str) {
        let mut lifetime = self.pending_residency.lock().unwrap();
        let correlations: Vec<_> = lifetime
            .iter()
            .filter(|(_, entry)| entry.executor_id == executor_id)
            .map(|(correlation, _)| correlation.clone())
            .collect();
        for correlation in correlations {
            if let Some(entry) = lifetime.remove(&correlation) {
                let _ = entry.waiter.send(residency_core_failure(
                    ResidencyFailureKind::Admission,
                    diagnostic.to_string(),
                    None,
                ));
            }
        }
        drop(lifetime);
        let mut reads = self.pending_materialization_reads.lock().unwrap();
        let correlations: Vec<_> = reads
            .iter()
            .filter(|(_, entry)| entry.executor_id == executor_id)
            .map(|(correlation, _)| correlation.clone())
            .collect();
        for correlation in correlations {
            if let Some(entry) = reads.remove(&correlation) {
                let _ = entry.waiter.send(MaterializationReadResult::Failed {
                    kind: MaterializationReadFailureKind::MaterializationUnavailable,
                    diagnostic: diagnostic.to_string(),
                });
            }
        }
    }
}

/// The id of the executor-side queue entry an acquisition for `holder` occupies.
///
/// Deterministic, and the same string the executor mints, which is what lets the
/// runner name a queued acquisition in its liveness report.
pub(crate) fn residency_queue_entry_id(holder: &ResidencyHolder) -> String {
    format!("residency-acquire:{}", holder.storage_key())
}

#[cfg(test)]
pub(crate) fn residency_placement_request_for_test(
    request: &ResidencyAcquireRequest,
) -> CellRequest {
    residency_placement_request(request)
}

/// The queue entry an acquisition creates, named deterministically so the runner
/// can report that it is still waiting for this exact entry.
fn residency_placement_request(acquisition: &ResidencyAcquireRequest) -> CellRequest {
    CellRequest {
        request_id: residency_queue_entry_id(&acquisition.holder),
        attempt_id: RESIDENCY_ACQUIRE_ATTEMPT_ID.into(),
        project_id: acquisition.repository.project_id().to_string(),
        repository: if matches!(acquisition.holder, ResidencyHolder::DevInstance { .. }) {
            acquisition.repository.clone()
        } else {
            match &acquisition.repository {
                RepositoryLocator::ColocatedPath { .. } => {
                    let identity = acquisition.repository.identity();
                    RepositoryLocator::ManagedObjects {
                        project_id: identity.project_id,
                        repository_id: identity.repository_id,
                        object_format: identity.object_format,
                    }
                }
                repository => repository.clone(),
            }
        },
        base_commit: acquisition.initial_base_commit.clone(),
        command: acquisition.holder.storage_key(),
        command_class: cairn_common::executor_protocol::CellCommandClass::Other,
        placement_work_class: match acquisition.holder {
            ResidencyHolder::Service { .. } => PlacementWorkClass::Services,
            ResidencyHolder::DevInstance { .. } => PlacementWorkClass::DevInstances,
            ResidencyHolder::Job { .. }
            | ResidencyHolder::ProjectTerminals { .. }
            | ResidencyHolder::Workflow { .. } => PlacementWorkClass::AgentSessions,
        },
        owner: acquisition.owner_ref.clone().or_else(|| {
            Some(cairn_common::executor_protocol::CellOwnerRef {
                project_id: acquisition.repository.project_id().to_string(),
                project_key: None,
                issue_number: None,
                job_id: None,
                execution_seq: None,
                node_kind: None,
            })
        }),
        cwd: String::new(),
        env: Vec::new(),
        priority: acquisition.priority,
        wait_horizon_unix_ms: acquisition.wait_horizon_unix_ms,
        waiting_since_unix_ms: acquisition.waiting_since_unix_ms,
        timeout_ms: 0,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: Some(format!("residency:{}", acquisition.holder.storage_key())),
        executor: acquisition.executor.clone(),
        // A dev instance serves the operator's own machine: its ports, its
        // browser, its localhost. It has to run where they are.
        pinned_executor_id: matches!(acquisition.holder, ResidencyHolder::DevInstance { .. })
            .then(|| COLOCATED_EXECUTOR_ID.to_string()),
        // A residency stays on the executor that acquires it, but an unpinned
        // acquisition may choose any compatible live executor. Dev instances are
        // pinned above because their localhost surface is inherently colocated.
        placement_mobility: if matches!(acquisition.holder, ResidencyHolder::DevInstance { .. }) {
            PlacementMobility::PinnedOrColocated
        } else {
            PlacementMobility::SpillEligible
        },
        verdict_platforms: Vec::new(),
        command_resource_identity: None,
        resource_reservation: acquisition.footprint.reservation(),
        learned_estimate: None,
    }
}

// Refresh transfers use a commit-specific attempt so concurrent requests cannot revoke each other.
fn residency_refresh_request(
    residency: &CellResidency,
    fence: &ResidencyFence,
    base_commit: &str,
) -> CellRequest {
    let mut request = residency_placement_request(&ResidencyAcquireRequest {
        holder: residency.holder.clone(),
        repository: residency.repository.clone(),
        executor: None,
        owner_ref: residency.owner_ref.clone(),
        selector: residency.selector.clone(),
        initial_base_commit: residency.current_base_commit.clone(),
        footprint: residency.footprint,
        death_policy: residency.death_policy.clone(),
        priority: CellPriority::AgentInteractive,
        // A refresh is an operation on an environment that already exists, not a
        // wait for one to be created: it never enters admission, so this bounds
        // only the object-plane authorization it derives.
        wait_horizon_unix_ms: unix_time_ms().saturating_add(30_000),
        waiting_since_unix_ms: unix_time_ms(),
    });
    request.request_id = format!("residency-refresh:{}", residency.holder.storage_key());
    request.attempt_id = format!(
        "{}:{}:{}",
        fence.incarnation_id, fence.cell_epoch, base_commit
    );
    request.base_commit = base_commit.to_string();
    request
}

fn residency_operation_holder(operation: &ResidencyOperation) -> Option<&ResidencyHolder> {
    match operation {
        ResidencyOperation::Acquire { .. } => None,
        ResidencyOperation::Reclaim { fence }
        | ResidencyOperation::Renew { fence }
        | ResidencyOperation::Release { fence }
        | ResidencyOperation::StartProcess { fence, .. }
        | ResidencyOperation::StopProcess { fence, .. }
        | ResidencyOperation::WriteProcessInput { fence, .. }
        | ResidencyOperation::ResizePty { fence, .. }
        | ResidencyOperation::MaterializeConflict { fence, .. }
        | ResidencyOperation::RefreshCheckout { fence, .. } => Some(&fence.holder),
    }
}

fn residency_core_failure(
    kind: ResidencyFailureKind,
    diagnostic: impl Into<String>,
    outcome: Option<CellOutcome>,
) -> ResidencyResult {
    ResidencyResult::Failed {
        kind,
        diagnostic: diagnostic.into(),
        cell_outcome: outcome.map(Box::new),
    }
}

fn restamp_outcome(outcome: &CellOutcome, identity: &RequestIdentity) -> CellOutcome {
    match outcome {
        CellOutcome::Completed {
            exit_code,
            output,
            timed_out,
            metadata,
            mutation_delta,
            sandbox_denials,
            tracked_modifications,
            ..
        } => CellOutcome::Completed {
            request_id: identity.0.clone(),
            attempt_id: identity.1.clone(),
            exit_code: *exit_code,
            output: output.clone(),
            timed_out: *timed_out,
            metadata: metadata.clone(),
            mutation_delta: mutation_delta.clone(),
            sandbox_denials: sandbox_denials.clone(),
            tracked_modifications: tracked_modifications.clone(),
        },
        CellOutcome::FailedAfterExecution { diagnostic, .. } => CellOutcome::FailedAfterExecution {
            request_id: identity.0.clone(),
            attempt_id: identity.1.clone(),
            diagnostic: diagnostic.clone(),
        },
        CellOutcome::StorageFailure {
            stage,
            kind,
            diagnostic,
            slot_retired,
            ..
        } => CellOutcome::StorageFailure {
            request_id: identity.0.clone(),
            attempt_id: identity.1.clone(),
            stage: *stage,
            kind: *kind,
            diagnostic: diagnostic.clone(),
            slot_retired: *slot_retired,
        },
        CellOutcome::Cancelled { .. } => CellOutcome::Cancelled {
            request_id: identity.0.clone(),
            attempt_id: identity.1.clone(),
        },
        CellOutcome::Unavailable { reason, diagnostic } => CellOutcome::Unavailable {
            reason: reason.clone(),
            diagnostic: diagnostic.clone(),
        },
    }
}

fn require_colocated_population(
    request: &mut CellRequest,
    config: &ExecutorConfig,
) -> Result<(), String> {
    if config.populate.is_empty()
        || matches!(
            request.repository,
            RepositoryLocator::ExistingCheckout { .. } | RepositoryLocator::ScratchOnly { .. }
        )
    {
        return Ok(());
    }
    if request
        .executor
        .as_ref()
        .and_then(|selector| selector.name.as_deref())
        .is_some_and(|name| !executor_names_match(name, LOCAL_EXECUTOR_NAME))
    {
        return Err(
            "worktree population requires the local executor because ignored project content is available only in the runner's live primary checkout; remove the executor selector or run without worktree population"
                .into(),
        );
    }
    request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
    // The pin already settles placement, and stating the mobility alongside it
    // keeps the decision record from claiming this batch was free to move.
    request.placement_mobility = PlacementMobility::PinnedOrColocated;
    Ok(())
}

#[cfg(test)]
fn choose_executor(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
) -> Result<Option<SelectedExecutor>, String> {
    Ok(choose_executor_with(
        connections,
        request,
        &HashMap::new(),
        repository_sync_cost,
        // The fixture clock, not the wall clock. Every fixture machine stamps
        // its last progress at `NOW`, and placement rejects a connection that
        // has been silent past the freshness bound — so reading the real clock
        // here aged every fixture out of the fleet and refused these placements
        // for a reason none of them are about.
        NOW,
    )?
    .selected
    .map(|(selected, _)| selected))
}

/// Predict every usable candidate with no profile store behind it.
///
/// This is exactly what production does for a placement that carries no resource
/// plan: labeled class priors for every duration, and a real queue forecast
/// built from the facts each machine published. It is the sync path, so tests
/// exercise the same ordering, capacity model, and honesty rules production
/// does rather than a second implementation of them.
#[cfg(test)]
fn prior_predictions(
    usable: &[&ExecutorConnectionState],
    request: &CellRequest,
    estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
    now_unix_ms: u64,
) -> (
    HashMap<String, PlacementPrediction>,
    HashMap<String, SyncCost>,
) {
    let mut predictions = HashMap::new();
    let mut sync_costs = HashMap::new();
    for entry in usable {
        let context = ReservationPlan::context_for(
            &entry.identity.device_id,
            &entry.identity.executor_id,
            &entry.advertisement.capabilities,
        );
        let warmth = candidate_warmth(entry, request, now_unix_ms);
        let prices = QueuePrices::from_priors(entry, &context);
        let queue = forecast_queue_wait(entry, &prices, request, now_unix_ms);
        let run = resource_profiles::unmeasured_duration(
            request.command_class,
            &context,
            request.command_resource_identity.as_ref(),
            warmth.for_lookup(),
            DurationFallback::NoProfileStore,
        );
        let sync_cost = estimate(request, entry);
        sync_costs.insert(entry.identity.executor_id.clone(), sync_cost);
        predictions.insert(
            entry.identity.executor_id.clone(),
            placement_prediction_with_contention(
                entry,
                warmth,
                queue,
                run,
                resource_profiles::contention_prior(&candidate_load(entry)),
                sync_cost,
            ),
        );
    }
    (predictions, sync_costs)
}

/// What one pass of placement concluded: the machine it chose, and every machine
/// it passed over with the reason.
///
/// A draft rather than the decision, because two facts are only knowable after
/// selection: the object-transfer coordinate a remote execution travels under,
/// and the instant the decision was taken.
#[derive(Debug)]
struct PlacementDraft {
    selected: Option<(SelectedExecutor, PlacementSelection)>,
    rejected: Vec<PlacementRejection>,
    policy: PlacementPolicyEvidence,
}

/// Both placement stages back to back, for tests that have no demand to resolve
/// between them.
///
/// Production keeps them apart because estimating what the work costs is
/// asynchronous and belongs strictly between the two: `survey_candidates` names
/// what could take the work, the estimate is resolved for exactly those
/// machines, and `rank_survey` decides among them.
#[cfg(test)]
fn choose_executor_with(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
    reservations: &HashMap<String, resource_profiles::ResolvedResourceProfile>,
    estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
    now_unix_ms: u64,
) -> Result<PlacementDraft, String> {
    choose_executor_with_policy(
        connections,
        request,
        reservations,
        &ActivePlacementPolicy::default_profile(),
        estimate,
        now_unix_ms,
    )
}

#[cfg(test)]
fn choose_executor_with_policy(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
    reservations: &HashMap<String, resource_profiles::ResolvedResourceProfile>,
    policy: &ActivePlacementPolicy,
    estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
    now_unix_ms: u64,
) -> Result<PlacementDraft, String> {
    let survey = survey_candidates(connections, request, now_unix_ms)?;
    let (predictions, sync_costs) =
        prior_predictions(&survey.usable, request, &estimate, now_unix_ms);
    rank_survey(
        request,
        survey,
        SurveyRankingContext {
            reservations,
            predictions: &predictions,
            sync_costs: &sync_costs,
            policy,
            now_unix_ms,
            exploration: &ExplorationState::default(),
        },
    )
}

/// Whether the caller settled placement itself, leaving policy nothing to
/// decide among.
///
/// True for a residency pin, and for a selector that names one machine. False
/// for a platform or toolchain constraint, which narrows the fleet and then
/// leaves the choice open — that is still policy choosing, and it is held to the
/// same evidence as an unconstrained request.
fn placement_settled_by_caller(request: &CellRequest) -> bool {
    request.pinned_executor_id.is_some()
        || request
            .executor
            .as_ref()
            .is_some_and(|selector| selector.name.is_some())
}

/// The machines that survived the structural filters, and the ones that did not
/// with the reason each was passed over.
struct CandidateSurvey<'a> {
    usable: Vec<&'a ExecutorConnectionState>,
    rejected: Vec<PlacementRejection>,
}

/// The facts about a piece of work that decide which machines could
/// structurally take it.
///
/// Borrowed from a [`CellRequest`] at placement, and stated directly by a caller
/// that asks the same question before a request exists. An occupancy forecast is
/// exactly that caller: it must read only the machines the work could land on,
/// and "could land on" has to be the one relation placement uses or the two
/// answers drift — a forecast scoped more loosely than placement predicts relief
/// from a machine the work will never reach (CAIRN-3429).
#[derive(Clone, Copy)]
pub(crate) struct PlacementScope<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) repository: &'a RepositoryLocator,
    pub(crate) selector: Option<&'a ExecutorSelector>,
    pub(crate) pinned_executor_id: Option<&'a str>,
    pub(crate) mobility: PlacementMobility,
    pub(crate) verdict_platforms: &'a [String],
}

/// Recast a residency-pinned request as the hermetic request that will replace it.
///
/// All three machine-binding gates move together. Clearing only the pin would
/// leave the shadow request immobile and its checkout non-transferable, making
/// the counterfactual silently choose the colocated executor every time.
fn hermetic_shadow_request(request: &CellRequest) -> Option<CellRequest> {
    request.pinned_executor_id.as_ref()?;
    let identity = request.repository.identity();
    let mut shadow = request.clone();
    shadow.pinned_executor_id = None;
    shadow.placement_mobility = PlacementMobility::SpillEligible;
    shadow.repository = RepositoryLocator::ManagedObjects {
        project_id: identity.project_id,
        repository_id: identity.repository_id,
        object_format: identity.object_format,
    };
    Some(shadow)
}

impl<'a> PlacementScope<'a> {
    fn of(request: &'a CellRequest) -> Self {
        Self {
            project_id: &request.project_id,
            repository: &request.repository,
            selector: request
                .executor
                .as_ref()
                .filter(|selector| !selector.is_empty()),
            pinned_executor_id: request.pinned_executor_id.as_deref(),
            mobility: request.placement_mobility,
            verdict_platforms: &request.verdict_platforms,
        }
    }

    /// Whether the work named where it must run, by machine or by platform.
    /// Targeted work is not held to the colocated default, because naming a
    /// machine IS the permission to leave home.
    fn targeted(&self) -> bool {
        self.selector.is_some() || self.pinned_executor_id.is_some()
    }
}

fn selected_link_dispatch_error(
    connections: &HashMap<String, ExecutorConnectionState>,
    selected: &SelectedExecutor,
    now_unix_ms: u64,
) -> Option<String> {
    let Some(current) = connections.get(&selected.executor_id) else {
        return Some(format!(
            "selected executor {} disconnected before residency dispatch (selected connection generation {})",
            selected.executor_id, selected.generation
        ));
    };
    if current.generation != selected.generation {
        return Some(format!(
            "selected executor {} reconnected before residency dispatch (selected connection generation {}, current generation {}); the old generation is fenced",
            selected.executor_id, selected.generation, current.generation
        ));
    }
    if current.sender.is_closed() {
        return Some(format!(
            "selected executor {} closed connection generation {} before residency dispatch",
            selected.executor_id, selected.generation
        ));
    }
    let silence_ms = now_unix_ms.saturating_sub(current.last_progress_unix_ms);
    (silence_ms > EXECUTOR_PROGRESS_FRESHNESS_MS).then(|| {
        format!(
            "selected executor {} connection generation {} became stale before residency dispatch: no progress for {}ms (liveness bound {}ms)",
            selected.executor_id,
            selected.generation,
            silence_ms,
            EXECUTOR_PROGRESS_FRESHNESS_MS
        )
    })
}

/// Why one machine cannot structurally take this work, or `None` if it could.
///
/// The filters that are facts rather than judgement: a closed link, a pin that
/// points elsewhere, a project this machine does not serve, a selector it does
/// not satisfy, a platform whose answer this request does not count,
/// conservative work on a machine that is not the home, or a repository that
/// cannot be recreated from objects. Ranking never reaches a machine this
/// rejects.
///
/// Verdict-platform trust is a filter here rather than a ranking key, and that
/// is the point: it decides eligibility, so no amount of idleness, cache warmth,
/// or predicted time-to-verdict can outweigh it downstream.
///
/// This is THE eligibility relation. Anything that needs to know where work
/// could go asks here rather than approximating with a subset of these clauses
/// — a forecast that checked only the selector would happily read a machine that
/// does not serve the project.
fn candidate_rejection(
    entry: &ExecutorConnectionState,
    scope: PlacementScope<'_>,
    now_unix_ms: u64,
) -> Option<PlacementRejectionReason> {
    if entry.drained && scope.pinned_executor_id != Some(entry.identity.executor_id.as_str()) {
        // The runner owns this refusal rather than asking the machine. A
        // residency pin is the exception: that work is already on the machine
        // and must be allowed to finish so the drain can empty it. Selectors and
        // unpinned requests are new placement and remain refused.
        Some(PlacementRejectionReason::Drained)
    } else if entry.sender.is_closed() {
        Some(PlacementRejectionReason::ConnectionClosed)
    } else if now_unix_ms.saturating_sub(entry.last_progress_unix_ms)
        > EXECUTOR_PROGRESS_FRESHNESS_MS
    {
        Some(PlacementRejectionReason::ConnectionStale {
            silence_ms: now_unix_ms.saturating_sub(entry.last_progress_unix_ms),
            stale_after_ms: EXECUTOR_PROGRESS_FRESHNESS_MS,
        })
    } else if scope
        .pinned_executor_id
        .is_some_and(|id| id != entry.identity.executor_id)
    {
        Some(PlacementRejectionReason::PinMismatch {
            pinned_executor_id: scope.pinned_executor_id.unwrap_or_default().to_string(),
        })
    } else if !serves_project(entry, scope.project_id) {
        Some(PlacementRejectionReason::ProjectUnavailable {
            project_id: scope.project_id.to_string(),
        })
    } else if scope
        .selector
        .is_some_and(|selector| !matches_selector(entry, selector))
    {
        Some(PlacementRejectionReason::SelectorMismatch {
            requested: scope
                .selector
                .map(|selector| selector.describe())
                .unwrap_or_default(),
        })
    } else if !scope.verdict_platforms.is_empty()
        && !scope
            .verdict_platforms
            .iter()
            .any(|platform| platform.eq_ignore_ascii_case(&entry.advertisement.capabilities.os))
    {
        Some(PlacementRejectionReason::UntrustedVerdictPlatform {
            os: entry.advertisement.capabilities.os.clone(),
            trusted: scope.verdict_platforms.to_vec(),
        })
    } else if !scope.targeted() && !entry.colocated && !scope.mobility.may_spill() {
        // Untargeted work that has not been declared mobile stays on the
        // machine holding the runner's own checkout. Absence of a selector is
        // not permission to move.
        Some(PlacementRejectionReason::NotColocated)
    } else if !entry.colocated && !repository_is_transferable(scope.repository) {
        // A checkout that already exists on one machine cannot be recreated
        // from managed objects on another.
        Some(PlacementRejectionReason::RepositoryNotTransferable {
            locator: repository_locator_name(scope.repository).into(),
        })
    } else {
        None
    }
}

/// Every machine that could structurally take this work, and every one that
/// could not with the reason.
///
/// Separate from ranking so that estimating what the work costs only ever
/// happens for machines that could actually take it. A request waiting for an
/// executor to attach surveys an empty fleet and queries nothing.
fn survey_candidates<'a>(
    connections: &'a HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
    now_unix_ms: u64,
) -> Result<CandidateSurvey<'a>, String> {
    let scope = PlacementScope::of(request);
    let targeted = scope.targeted();

    // Deterministic order in, deterministic rejections out: an operator reading
    // two decisions about the same fleet must not see the machines reshuffle.
    let mut ordered: Vec<_> = connections.values().collect();
    ordered.sort_by(|a, b| a.identity.executor_id.cmp(&b.identity.executor_id));

    let mut usable = Vec::new();
    let mut rejected = Vec::new();
    for entry in ordered {
        match candidate_rejection(entry, scope, now_unix_ms) {
            Some(reason) => rejected.push(PlacementRejection {
                prediction: None,
                executor_name: executor_public_name(entry),
                executor_id: entry.identity.executor_id.clone(),
                reason,
            }),
            None => usable.push(entry),
        }
    }

    // A caller that named a machine or a platform is owed an immediate answer
    // naming what exists. An untargeted request with nothing usable is instead
    // waiting for its own executor to attach, which its horizon already bounds.
    if usable.is_empty() && targeted {
        return Err(no_matching_executor_diagnostic(
            connections,
            request,
            &rejected,
        ));
    }
    // Waiting is the right answer for a mobile request that trust left with
    // nothing: a machine on a platform this verdict counts from may still
    // attach, and the horizon bounds how long that hope lasts. It is the wrong
    // answer for a request that cannot move at all, because its one possible
    // machine is attached right now and its platform will not change while the
    // horizon runs down. That combination is a contradiction between what the
    // check declared and the cadence it runs at, and saying so immediately is
    // the difference between a legible config error and a suite that silently
    // stalls out its horizon on every commit.
    if usable.is_empty() && !request.placement_mobility.may_spill() {
        if let Some(rejection) = rejected.iter().find(|rejection| {
            matches!(
                rejection.reason,
                PlacementRejectionReason::UntrustedVerdictPlatform { .. }
            )
        }) {
            return Err(untrusted_verdict_platform_diagnostic(request, rejection));
        }
    }
    Ok(CandidateSurvey { usable, rejected })
}

/// The refusal for work that can only run on the machine holding the runner's
/// checkout, whose platform is not one this verdict counts from.
fn untrusted_verdict_platform_diagnostic(
    request: &CellRequest,
    rejection: &PlacementRejection,
) -> String {
    format!(
        "this request cannot leave {} ({}), and its verdict counts only from {}. Nothing was run. Either widen the platforms this verdict counts from or move the work to a cadence that can be placed elsewhere.",
        rejection.executor_name,
        rejection.reason.describe(),
        request.verdict_platforms.join(", ")
    )
}

struct SurveyRankingContext<'a> {
    reservations: &'a HashMap<String, resource_profiles::ResolvedResourceProfile>,
    predictions: &'a HashMap<String, PlacementPrediction>,
    sync_costs: &'a HashMap<String, SyncCost>,
    policy: &'a ActivePlacementPolicy,
    now_unix_ms: u64,
    exploration: &'a ExplorationState,
}

/// Rank what survived the survey, on the estimates and readings each machine
/// carries.
fn rank_survey(
    request: &CellRequest,
    survey: CandidateSurvey<'_>,
    context: SurveyRankingContext<'_>,
) -> Result<PlacementDraft, String> {
    let SurveyRankingContext {
        reservations,
        predictions,
        sync_costs,
        policy,
        now_unix_ms,
        exploration,
    } = context;
    let CandidateSurvey {
        usable,
        mut rejected,
    } = survey;
    if usable.is_empty() {
        return Ok(PlacementDraft {
            selected: None,
            rejected,
            policy: policy_evidence(request, policy, false),
        });
    }
    let targeted = request.pinned_executor_id.is_some()
        || request
            .executor
            .as_ref()
            .is_some_and(|selector| !selector.is_empty());
    let pinned = request.pinned_executor_id.as_deref();
    let settled = placement_settled_by_caller(request);
    let exercising_spill = request.placement_mobility.may_spill() && !settled;

    let only_candidate = usable.len() == 1;
    let scored: Vec<_> = usable
        .into_iter()
        .map(|entry| {
            let sync_cost = sync_costs
                .get(&entry.identity.executor_id)
                .copied()
                .unwrap_or(SyncCost::Unknown);
            ScoredCandidate::new(
                entry,
                sync_cost,
                reservations.get(&entry.identity.executor_id),
                &request.resource_reservation,
                predictions
                    .get(&entry.identity.executor_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        // Every usable candidate is predicted for above; this is
                        // the shape of an entry that was surveyed and somehow not
                        // predicted, and it says so rather than scoring zero.
                        unpredicted_candidate(entry, request, sync_cost)
                    }),
                now_unix_ms,
            )
        })
        .collect();

    let (winner, ranked, changed_earliest_winner, bootstrap_exploration, tie_break) =
        rank_candidates(
            &scored,
            settled,
            exercising_spill,
            request,
            policy,
            exploration,
        );
    let Some(winner) = winner else {
        // Every usable machine was measured-blind and none of them was a
        // machine policy is allowed to spill onto. Say so with the evidence.
        rejected.extend(ranked.into_iter().map(|candidate| candidate.rejection()));
        return Err(no_measurable_executor_diagnostic(request, &rejected));
    };

    // Blindness is named ahead of `OnlyCandidate` deliberately. A lone
    // measured-blind home winning by default is precisely the local-degradation
    // case this record exists to expose, and "it was the only one" would describe
    // it as a choice that was made rather than one that was impossible. When the
    // caller settled placement there was nothing to measure for, so blindness is
    // not the story and the ordinary reasons apply.
    let reason = if pinned.is_some() {
        PlacementReason::Pinned
    } else if winner.blindness.is_some() && !settled {
        PlacementReason::MeasuredBlindFleet
    } else if bootstrap_exploration {
        PlacementReason::BootstrapExploration
    } else if only_candidate {
        PlacementReason::OnlyCandidate
    } else if !targeted && !request.placement_mobility.may_spill() {
        PlacementReason::ColocatedHome
    } else {
        PlacementReason::PredictedEarliestVerdict
    };
    let winner_name = executor_public_name(winner.entry);
    rejected.extend(
        ranked
            .into_iter()
            .filter(|candidate| {
                candidate.entry.identity.executor_id != winner.entry.identity.executor_id
            })
            .map(|candidate| candidate.outranked_rejection(&winner_name)),
    );

    let selection = PlacementSelection {
        executor_name: winner_name,
        executor_id: winner.entry.identity.executor_id.clone(),
        colocated: winner.entry.colocated,
        reason,
        tie_break,
        readings: placement_readings(&winner.entry.health.machine),
        reservation: winner
            .reservation
            .map(|resolved| resolved.reservation.clone())
            .unwrap_or_else(|| request.resource_reservation.clone()),
        reservation_rationale: winner
            .reservation
            .map(|resolved| resolved.rationale.clone())
            .unwrap_or_else(|| unresolved_rationale(&request.resource_reservation)),
        sync_cost: match winner.sync_cost {
            SyncCost::Known(bytes) => PlacementSyncCost::Known { bytes },
            SyncCost::Unknown => PlacementSyncCost::Unknown,
        },
        object_transfer: None,
        observation_reuse: if winner.entry.colocated {
            ObservationReuse::Colocated
        } else {
            ObservationReuse::UntrustedRemoteEnvironment
        },
        prediction: Some(winner.prediction.clone()),
    };
    Ok(PlacementDraft {
        selected: Some((selected_executor(winner.entry), selection)),
        rejected,
        policy: policy_evidence(request, policy, changed_earliest_winner),
    })
}

fn policy_evidence(
    request: &CellRequest,
    policy: &ActivePlacementPolicy,
    changed_earliest_winner: bool,
) -> PlacementPolicyEvidence {
    let stance = policy.profile.stance(request.placement_work_class);
    PlacementPolicyEvidence {
        profile_name: policy.name.clone(),
        work_class: request.placement_work_class,
        stance: match stance {
            PlacementStance::LocalFirst => "localFirst",
            PlacementStance::RemoteFirst => "remoteFirst",
            PlacementStance::RemoteOnly => "remoteOnly",
            PlacementStance::Any => "any",
        }
        .to_string(),
        max_preference_delay_seconds: policy.profile.max_preference_delay_seconds,
        changed_earliest_winner,
        constrained_by_mobility: !request.placement_mobility.may_spill()
            && !matches!(stance, PlacementStance::Any | PlacementStance::LocalFirst),
    }
}

/// One usable machine, with everything the ranking decides on.
struct ScoredCandidate<'a> {
    entry: &'a ExecutorConnectionState,
    sync_cost: SyncCost,
    reservation: Option<&'a resource_profiles::ResolvedResourceProfile>,
    /// Why this machine's readings cannot be decided on, when they cannot.
    blindness: Option<PlacementRejectionReason>,
    /// Set when the machine is measured and the resolved demand does not fit.
    misfit: Option<PlacementRejectionReason>,
    /// Whether executor admission can retain and fit this request right now.
    admission_accepts: bool,
    /// Normalized proximity to the entry bar, used only after a candidate can
    /// admit and has an otherwise-comparable verdict prediction.
    cpu_headroom_risk: Option<f64>,
    /// When this machine is predicted to answer, and on what evidence. The
    /// ranking key, and the explanation, are the same object.
    prediction: PlacementPrediction,
}

impl<'a> ScoredCandidate<'a> {
    fn new(
        entry: &'a ExecutorConnectionState,
        sync_cost: SyncCost,
        reservation: Option<&'a resource_profiles::ResolvedResourceProfile>,
        declared_reservation: &ResourceReservation,
        mut prediction: PlacementPrediction,
        now_unix_ms: u64,
    ) -> Self {
        let machine = &entry.health.machine;
        let blindness = placement_blindness(machine, now_unix_ms);
        // Memory and volume remain eligibility evidence: a machine that cannot
        // hold the work cannot be the fastest at it. They are no longer ranking
        // keys, because how much a machine has left says nothing about how long
        // it takes.
        let available_memory_bytes = machine
            .memory
            .value()
            .map_or(0, |memory| memory.available_bytes);
        let free_volume_bytes = machine.volume.value().map_or(0, |volume| volume.free_bytes);
        let misfit = blindness
            .is_none()
            .then_some(reservation)
            .flatten()
            .and_then(|resolved| {
                let demand = &resolved.reservation;
                if machine.memory.value().is_some() && available_memory_bytes < demand.memory_bytes
                {
                    Some(PlacementRejectionReason::InsufficientMemory {
                        required_bytes: demand.memory_bytes,
                        available_bytes: available_memory_bytes,
                    })
                } else if machine.volume.value().is_some()
                    && free_volume_bytes < demand.disk_growth_bytes
                {
                    Some(PlacementRejectionReason::InsufficientVolume {
                        required_bytes: demand.disk_growth_bytes,
                        free_bytes: free_volume_bytes,
                    })
                } else {
                    None
                }
            });
        let demand = reservation
            .map(|resolved| &resolved.reservation)
            .unwrap_or(declared_reservation);
        let admission = &entry.health.admission;
        let active = &admission.active_reservation;
        let queue_depth = entry
            .health
            .queues
            .iter()
            .map(|queue| queue.depth)
            .sum::<usize>();
        let cpu_admission = &entry.health.host.cpu_admission;
        // A verdict is trusted for exactly as long as it is not blind. Two
        // different bounds here — one interval to trust it, three to call it
        // blind — left a ten-second gap where a machine ranked as neither
        // pressured nor blind, which is to say as idle.
        let cpu_admission_fresh = cpu_admission
            .measured_at_unix_ms
            .is_some_and(|measured_at| {
                now_unix_ms.saturating_sub(measured_at) <= CPU_ADMISSION_BLIND_AFTER_MS
            });
        let cpu_pressured =
            cpu_admission_fresh && cpu_admission.state == CpuAdmissionState::Pressured;
        // A machine that measured its own cores and stopped is not a machine
        // with headroom, and ranking it as one is how work keeps arriving at a
        // host nobody can see into. Derived from the reading's age rather than
        // from any state the executor sent, because the executor could only set
        // such a state from the sampler whose silence is the condition itself.
        let cpu_blind = cpu_admission
            .measured_at_unix_ms
            .is_some_and(|measured_at| {
                now_unix_ms.saturating_sub(measured_at) > CPU_ADMISSION_BLIND_AFTER_MS
            });
        let cpu_headroom_risk = (cpu_admission_fresh
            && cpu_admission.state == CpuAdmissionState::Accepting)
            .then(|| {
                let utilization = cpu_admission.utilization?;
                let policy = entry.health.applied_policy.cpu_admission;
                let span = policy.entry_utilization - policy.clear_utilization;
                (span > 0.0)
                    .then(|| ((utilization - policy.clear_utilization) / span).clamp(0.0, 1.0))
            })
            .flatten();
        if cpu_pressured {
            prediction.queue = QueueForecast::Unknown {
                reason: QueueUnknownReason::MeasuredCpuPressure,
            };
            prediction.predicted_verdict_ms = prediction.adjusted_run_ms;
        }
        let admission_accepts = !cpu_pressured
            && !cpu_blind
            && queue_depth < entry.health.applied_policy.maximum_queue_depth
            && admission.memory_capacity_bytes.is_none_or(|capacity| {
                active.memory_bytes.saturating_add(demand.memory_bytes) <= capacity
            })
            && admission.disk_growth_capacity_bytes.is_none_or(|capacity| {
                active
                    .disk_growth_bytes
                    .saturating_add(demand.disk_growth_bytes)
                    <= capacity
            });
        Self {
            entry,
            sync_cost,
            reservation,
            blindness,
            misfit,
            admission_accepts,
            cpu_headroom_risk,
            prediction,
        }
    }

    fn rejection(&self) -> PlacementRejection {
        PlacementRejection {
            prediction: Some(self.prediction.clone()),
            executor_name: executor_public_name(self.entry),
            executor_id: self.entry.identity.executor_id.clone(),
            reason: self
                .blindness
                .clone()
                .or_else(|| self.misfit.clone())
                .unwrap_or(PlacementRejectionReason::NotColocated),
        }
    }

    fn outranked_rejection(&self, winner_name: &str) -> PlacementRejection {
        // A machine that could not be measured, or that measurably could not fit,
        // has a more useful thing to say than "it lost".
        let reason = self
            .blindness
            .clone()
            .or_else(|| self.misfit.clone())
            .unwrap_or_else(|| PlacementRejectionReason::OutrankedBy {
                executor_name: winner_name.to_string(),
            });
        PlacementRejection {
            // A machine that was ranked and lost owes the operator its numbers,
            // not just the name of whoever beat it.
            prediction: Some(self.prediction.clone()),
            executor_name: executor_public_name(self.entry),
            executor_id: self.entry.identity.executor_id.clone(),
            reason,
        }
    }
}

/// Order the usable machines and name the winner.
///
/// A measured machine always beats a measured-blind one, and a machine that fits
/// the resolved demand always beats one that does not. Fit is a ranking key
/// rather than a hard filter on purpose: reserving capacity and queueing for it
/// belong to the executor's admission, and placement refusing work the executor
/// would have queued would be a second, quieter queue.
///
/// **Placement will not choose a machine it cannot see.** A blind candidate is
/// selectable only when nothing is being chosen: the caller settled placement
/// itself (a pin, or a selector naming one machine), or the candidate is the
/// colocated home, where refusing to run at all is a worse failure than running
/// where the work already is. Otherwise a blind machine is excluded and, if that
/// leaves nothing, the request is refused with the evidence rather than shipped
/// somewhere unexamined.
///
/// Constraining the fleet is not settling placement. `executor: {os: "linux"}`
/// narrows the candidate set and leaves policy to pick among what is left, so
/// absent placement evidence is exactly as disqualifying there as it is for an
/// unconstrained request. A gap is never read as no load.
///
/// Among the machines that survive those gates, a known queue forecast always
/// precedes an unknown one: there is no numeric total to compare when one summand
/// is absent. Within that comparable set, the order is predicted time to a
/// verdict, ascending. Evidence quality breaks ties but never leads: a machine
/// confidently predicted to be slow is still slow, and preferring it because the
/// prediction was well-evidenced would rank on the measurement rather than on
/// what was measured.
fn rank_candidates<'a, 'b>(
    scored: &'b [ScoredCandidate<'a>],
    settled_by_caller: bool,
    exercising_spill: bool,
    request: &CellRequest,
    policy: &ActivePlacementPolicy,
    exploration: &ExplorationState,
) -> (
    Option<&'b ScoredCandidate<'a>>,
    Vec<&'b ScoredCandidate<'a>>,
    bool,
    bool,
    Option<PlacementTieBreak>,
) {
    let hard_order = |a: &&ScoredCandidate<'a>, b: &&ScoredCandidate<'a>| {
        a.blindness
            .is_some()
            .cmp(&b.blindness.is_some())
            .then_with(|| a.misfit.is_some().cmp(&b.misfit.is_some()))
            .then_with(|| b.admission_accepts.cmp(&a.admission_accepts))
            .then_with(|| {
                a.prediction
                    .queue
                    .predicted_ms()
                    .is_none()
                    .cmp(&b.prediction.queue.predicted_ms().is_none())
            })
    };
    let forecast_order = |a: &&ScoredCandidate<'a>, b: &&ScoredCandidate<'a>| {
        a.prediction
            .predicted_verdict_ms
            .cmp(&b.prediction.predicted_verdict_ms)
            .then_with(|| {
                a.cpu_headroom_risk
                    .partial_cmp(&b.cpu_headroom_risk)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| {
                a.prediction
                    .evidence_rank()
                    .cmp(&b.prediction.evidence_rank())
            })
            .then_with(|| sync_cost_key(a.sync_cost).cmp(&sync_cost_key(b.sync_cost)))
    };
    let recent_selection_order = |a: &&ScoredCandidate<'a>, b: &&ScoredCandidate<'a>| {
        let age = |candidate: &&ScoredCandidate<'a>| {
            exploration
                .selected_executors
                .iter()
                .position(|executor_id| executor_id == &candidate.entry.identity.executor_id)
        };
        match (age(a), age(b)) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a_age), Some(b_age)) => a_age.cmp(&b_age),
        }
    };
    let stable_order = |a: &&ScoredCandidate<'a>, b: &&ScoredCandidate<'a>| {
        recent_selection_order(a, b).then_with(|| {
            a.entry
                .identity
                .executor_id
                .cmp(&b.entry.identity.executor_id)
        })
    };
    let selectable = |candidate: &&ScoredCandidate<'a>| {
        (candidate.blindness.is_none()
            || settled_by_caller
            || candidate.entry.colocated
            || !exercising_spill)
            && !(exercising_spill
                && matches!(
                    policy.profile.stance(request.placement_work_class),
                    PlacementStance::RemoteOnly
                )
                && candidate.entry.colocated)
    };

    let mut baseline: Vec<&ScoredCandidate<'a>> = scored.iter().collect();
    baseline.sort_by(|a, b| {
        hard_order(a, b)
            .then_with(|| forecast_order(a, b))
            .then_with(|| stable_order(a, b))
    });
    let earliest = baseline.iter().copied().find(selectable);
    let Some(earliest) = earliest else {
        return (None, baseline, false, false, None);
    };
    if settled_by_caller || !exercising_spill {
        return (Some(earliest), baseline, false, false, None);
    }

    // A prior is not evidence about a machine, and pure exploitation can never
    // turn it into evidence. One in every four spill decisions for this work
    // class may spend otherwise-idle, admission-ready capacity acquiring it. The
    // other three decisions remain pure earliest-verdict placements, so a machine
    // that repeatedly fails before completion or reports unusable measurements
    // cannot monopolize work. Candidates rotate by their most recent exploration,
    // which gives every fresh machine a bounded turn instead of retrying the first
    // executor forever. Failed object transfer never reaches `observe_completed`,
    // so those attempts consume budget but never become duration samples.
    let exploration_due = exploration.decisions_since_exploration >= EXPLORATION_INTERVAL - 1;
    let last_explored = |candidate: &&ScoredCandidate<'a>| {
        exploration
            .explored_executors
            .iter()
            .position(|executor_id| executor_id == &candidate.entry.identity.executor_id)
    };
    let bootstrap_eligible = |candidate: &&ScoredCandidate<'a>| {
        selectable(candidate)
            && candidate.misfit.is_none()
            && candidate.admission_accepts
            && candidate.prediction.queue.predicted_ms() == Some(0)
            && candidate.prediction.run.profile_key.is_some()
            && matches!(
                candidate.prediction.run.fallback,
                Some(
                    DurationFallback::NoProfileRecorded
                        | DurationFallback::BelowConfidenceFloor
                        | DurationFallback::ProfileTooOld
                )
            )
    };
    let exploration = exploration_due
        .then(|| {
            baseline
                .iter()
                .copied()
                .filter(bootstrap_eligible)
                .min_by(|a, b| match (last_explored(a), last_explored(b)) {
                    (None, None) => forecast_order(a, b).then_with(|| stable_order(a, b)),
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (Some(a_age), Some(b_age)) => a_age
                        .cmp(&b_age)
                        .then_with(|| forecast_order(a, b))
                        .then_with(|| stable_order(a, b)),
                })
        })
        .flatten();
    if let Some(exploration) = exploration {
        let changed = exploration.entry.identity.executor_id != earliest.entry.identity.executor_id;
        let bootstrap_candidates = scored
            .iter()
            .filter(bootstrap_eligible)
            .map(|candidate| executor_public_name(candidate.entry))
            .collect::<Vec<_>>();
        let exploration_age_decided = scored
            .iter()
            .filter(bootstrap_eligible)
            .any(|candidate| last_explored(&exploration) != last_explored(&candidate));
        let tied_forecasts = scored
            .iter()
            .filter(bootstrap_eligible)
            .filter(|candidate| forecast_order(&exploration, candidate).is_eq())
            .count();
        let deciding_reason =
            exploration_age_decided
                .then_some("least recently explored bootstrap candidate")
                .or_else(|| {
                    (tied_forecasts > 1).then(|| {
                        if scored.iter().filter(bootstrap_eligible).any(|candidate| {
                            recent_selection_order(&exploration, &candidate).is_ne()
                        }) {
                            "least recently selected among equal bootstrap candidates"
                        } else {
                            "stable executor identity seeded recent-selection history"
                        }
                    })
                });
        return (
            Some(exploration),
            baseline,
            changed,
            true,
            deciding_reason.map(|reason| PlacementTieBreak {
                candidates: bootstrap_candidates,
                deciding_reason: reason.into(),
            }),
        );
    }

    let deadline = earliest.prediction.predicted_verdict_ms.saturating_add(
        policy
            .profile
            .max_preference_delay_seconds
            .saturating_mul(1_000),
    );
    let same_hard_group = |candidate: &&ScoredCandidate<'a>| {
        hard_order(&earliest, candidate).is_eq()
            && candidate.prediction.predicted_verdict_ms <= deadline
            && selectable(candidate)
    };
    let stance = policy.profile.stance(request.placement_work_class);
    let preference = |candidate: &&ScoredCandidate<'a>| match stance {
        PlacementStance::LocalFirst => !candidate.entry.colocated,
        PlacementStance::RemoteFirst | PlacementStance::RemoteOnly => candidate.entry.colocated,
        PlacementStance::Any => false,
    };
    let priority = |candidate: &&ScoredCandidate<'a>| {
        policy
            .profile
            .machine_priority
            .iter()
            .position(|id| id == &candidate.entry.identity.executor_id)
            .unwrap_or(usize::MAX)
    };
    let mut ranked = baseline;
    ranked.sort_by(|a, b| {
        hard_order(a, b)
            .then_with(|| same_hard_group(b).cmp(&same_hard_group(a)))
            .then_with(|| {
                if same_hard_group(a) && same_hard_group(b) {
                    preference(a)
                        .cmp(&preference(b))
                        .then_with(|| priority(a).cmp(&priority(b)))
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .then_with(|| forecast_order(a, b))
            .then_with(|| stable_order(a, b))
    });
    let winner = ranked.iter().copied().find(selectable);
    let changed = winner.is_some_and(|winner| {
        winner.entry.identity.executor_id != earliest.entry.identity.executor_id
    });
    let tie_break = winner.and_then(|winner| {
        let tied = ranked
            .iter()
            .copied()
            .filter(|candidate| {
                hard_order(&winner, candidate).is_eq()
                    && forecast_order(&winner, candidate).is_eq()
                    && same_hard_group(candidate)
                    && preference(&winner) == preference(candidate)
                    && priority(&winner) == priority(candidate)
            })
            .map(|candidate| executor_public_name(candidate.entry))
            .collect::<Vec<_>>();
        let recent_selection_decided = ranked.iter().copied().any(|candidate| {
            hard_order(&winner, &candidate).is_eq()
                && forecast_order(&winner, &candidate).is_eq()
                && same_hard_group(&candidate)
                && preference(&winner) == preference(&candidate)
                && priority(&winner) == priority(&candidate)
                && recent_selection_order(&winner, &candidate).is_ne()
        });
        (tied.len() > 1).then(|| PlacementTieBreak {
            candidates: tied,
            deciding_reason: if recent_selection_decided {
                "least recently selected among candidates equal on every earlier placement key"
            } else {
                "stable executor identity seeded recent-selection history"
            }
            .into(),
        })
    });
    (winner, ranked, changed, false, tie_break)
}

/// What this machine already holds for this request, read off the facts it
/// published about its own cells and verified objects.
///
/// The runner classifies; the executor states facts. Nothing here is a score and
/// nothing here is a path: where a cell lives is that machine's business, and
/// comparing paths across machines would be meaningless anyway. What is
/// comparable is the project a cell serves and the work classes that have
/// completed in it.
///
/// An externally owned checkout never counts as warm. The executor supplies
/// process authority over such a tree and does not prepare, fingerprint, or
/// normalize it, so it cannot honestly claim anything about what it contains.
fn candidate_warmth(
    entry: &ExecutorConnectionState,
    request: &CellRequest,
    now_unix_ms: u64,
) -> CacheWarmthEvidence {
    let observed_at = entry
        .advertisement
        .liveness_observed_at_unix_ms
        .unwrap_or(entry.advertisement.observed_at_unix_ms);
    if now_unix_ms.saturating_sub(observed_at) > EXECUTOR_TELEMETRY_STALE_AFTER_MS {
        return CacheWarmthEvidence::Unknown {
            reason: WarmthUnknownReason::FactsStale,
        };
    }
    // A machine that does not claim authority over its own inventory cannot have
    // the absence of a matching cell read as evidence that none exists. Cold and
    // "we cannot tell" are different answers and must not collapse into one.
    if entry.health.inventory.authority != InventoryAuthorityState::Authoritative {
        return CacheWarmthEvidence::Unknown {
            reason: WarmthUnknownReason::InventoryNotAuthoritative,
        };
    }
    let prepared = entry.snapshot.cells.iter().any(|cell| {
        cell.project_id == request.project_id
            && cell.checkout_kind != CellCheckoutKind::ExistingCheckout
            && cell.is_warm_for(request.command_class)
    });
    let warmth = if prepared {
        ExecutionWarmth::PreparedWarmSlot
    } else if warm_root_holds_commit(entry, request) {
        // Objects present is not a build cache. A machine that merely holds the
        // commit still compiles from scratch, and saying otherwise is how a cold
        // remote came to look as ready as one that had just built the tree.
        ExecutionWarmth::RepositoryOnly
    } else {
        ExecutionWarmth::Cold
    };
    CacheWarmthEvidence::Observed {
        warmth,
        observed_at_unix_ms: observed_at,
    }
}

/// Resolves "how long would this take" for any command identity, on any
/// machine, with or without a profile store behind it.
///
/// A residency placement carries no resource plan and therefore no store. That
/// is a different fact from having looked and found nothing, and this is what
/// keeps the two apart on the record instead of blaming a machine for a lookup
/// nobody attempted.
struct DurationOracle {
    db: Option<Arc<cairn_db::storage::LocalDb>>,
}

impl DurationOracle {
    async fn predict(
        &self,
        identity: Option<&CommandResourceIdentity>,
        context: &resource_profiles::ProfileContext,
        warmth: ExecutionWarmth,
        class: cairn_common::executor_protocol::CellCommandClass,
        now_unix_ms: u64,
    ) -> DurationEstimate {
        match &self.db {
            Some(db) => {
                resource_profiles::resolve_duration(
                    db.clone(),
                    identity,
                    context,
                    warmth,
                    class,
                    now_unix_ms,
                )
                .await
            }
            None => resource_profiles::unmeasured_duration(
                class,
                context,
                identity,
                warmth,
                DurationFallback::NoProfileStore,
            ),
        }
    }
}

/// How long this request would wait before a machine could start it.
///
/// Advisory and read-only, in the strongest sense: it enqueues nothing, reserves
/// nothing, refuses nothing, and binds nobody. The executor's waiting room stays
/// the only authority over what actually runs when, and after placement picks a
/// machine that machine may still queue or refuse the request on its own live
/// state. This exists so a caller waiting on a verdict is not sent to a host that
/// will start its work in ten minutes just because that host looked idle.
///
/// The wait is a fluid approximation: the work ahead is summed as unit
/// milliseconds — each item's predicted duration times the concurrency it holds
/// — and the machine is assumed to drain it at its advertised capacity. It does
/// not simulate a scheduler, because the fidelity of such a simulation could not
/// be verified against a queue the runner does not own.
/// Every distinct thing a forecast must price on one machine, so each is
/// resolved once however much work shares an identity.
fn queue_price_keys(
    entry: &ExecutorConnectionState,
) -> Vec<(Option<CommandResourceIdentity>, CellCommandClass)> {
    let mut keys: Vec<(Option<CommandResourceIdentity>, CellCommandClass)> = Vec::new();
    let mut push = |identity: Option<CommandResourceIdentity>, class| {
        if !keys
            .iter()
            .any(|(known, known_class)| *known == identity && *known_class == class)
        {
            keys.push((identity, class));
        }
    };
    for queued in &entry.snapshot.queued_requests {
        push(
            queued.command_resource_identity.clone(),
            queued.command_class,
        );
    }
    for running in &entry.snapshot.executing_requests {
        push(
            running.command_resource_identity.clone(),
            running.command_class,
        );
    }
    keys
}

/// What each piece of work on a machine is predicted to take.
///
/// Resolving these is the only asynchronous part of a forecast, which is why it
/// is separated from the arithmetic: the ordering, the capacity model, and the
/// honesty rules are then a pure function that can be reasoned about and tested
/// without a database behind it.
#[derive(Default)]
struct QueuePrices {
    by_key: Vec<(
        (Option<CommandResourceIdentity>, CellCommandClass),
        DurationEstimate,
    )>,
}

impl QueuePrices {
    /// Queued work has not run, so nothing is known about the state it will
    /// find. Everything ahead is priced cold, which predicts the longer wait and
    /// therefore biases placement away from busy machines rather than toward
    /// them.
    async fn resolve(
        oracle: &DurationOracle,
        entry: &ExecutorConnectionState,
        context: &resource_profiles::ProfileContext,
        now_unix_ms: u64,
    ) -> Self {
        let mut by_key = Vec::new();
        for (identity, class) in queue_price_keys(entry) {
            let estimate = oracle
                .predict(
                    identity.as_ref(),
                    context,
                    ExecutionWarmth::Cold,
                    class,
                    now_unix_ms,
                )
                .await;
            by_key.push(((identity, class), estimate));
        }
        Self { by_key }
    }

    /// The labeled class priors alone, synchronously.
    ///
    /// Production reaches the same answer through [`Self::resolve`] against a
    /// [`DurationOracle`] with no store behind it; this exists only so the
    /// synchronous test entry points exercise the real forecast arithmetic
    /// rather than a stand-in for it.
    #[cfg(test)]
    fn from_priors(
        entry: &ExecutorConnectionState,
        context: &resource_profiles::ProfileContext,
    ) -> Self {
        Self {
            by_key: queue_price_keys(entry)
                .into_iter()
                .map(|(identity, class)| {
                    let estimate = resource_profiles::unmeasured_duration(
                        class,
                        context,
                        identity.as_ref(),
                        ExecutionWarmth::Cold,
                        DurationFallback::NoProfileStore,
                    );
                    ((identity, class), estimate)
                })
                .collect(),
        }
    }

    fn get(
        &self,
        identity: Option<&CommandResourceIdentity>,
        class: CellCommandClass,
    ) -> Option<&DurationEstimate> {
        self.by_key
            .iter()
            .find(|((known, known_class), _)| known.as_ref() == identity && *known_class == class)
            .map(|(_, estimate)| estimate)
    }
}

fn forecast_queue_wait(
    entry: &ExecutorConnectionState,
    prices: &QueuePrices,
    request: &CellRequest,
    now_unix_ms: u64,
) -> QueueForecast {
    let observed_at = entry
        .advertisement
        .liveness_observed_at_unix_ms
        .unwrap_or(entry.advertisement.observed_at_unix_ms);
    if now_unix_ms.saturating_sub(observed_at) > EXECUTOR_TELEMETRY_STALE_AFTER_MS {
        return QueueForecast::Unknown {
            reason: QueueUnknownReason::FactsStale,
        };
    }
    let capacity = entry
        .health
        .admission
        .concurrency_capacity
        .filter(|capacity| *capacity > 0);
    let Some(_capacity) = capacity else {
        return QueueForecast::Unknown {
            reason: QueueUnknownReason::NoAdmissionCapacity,
        };
    };

    // Seniority is a property of the wait, not of the enqueue, so the
    // hypothetical request ranks by when its requester began waiting -- exactly
    // the key the executor's own admission uses.
    let waiting_since = if request.waiting_since_unix_ms == 0 {
        now_unix_ms
    } else {
        request.waiting_since_unix_ms
    };
    let ours = (
        aged_priority(request.priority, waiting_since, now_unix_ms),
        std::cmp::Reverse(waiting_since),
    );

    let mut queued_ms_ahead = 0_u64;
    let mut fully_measured = true;
    let mut requests_ahead = 0_usize;
    for queued in &entry.snapshot.queued_requests {
        let theirs = (
            aged_priority(queued.priority, queued.queued_at_unix_ms, now_unix_ms),
            std::cmp::Reverse(queued.queued_at_unix_ms),
        );
        if theirs <= ours {
            continue;
        }
        requests_ahead += 1;
        let Some(estimate) = prices.get(
            queued.command_resource_identity.as_ref(),
            queued.command_class,
        ) else {
            // Something ahead could not be priced at all. Summing it as zero
            // would report an empty queue, so the whole forecast declines
            // instead.
            return QueueForecast::Unknown {
                reason: QueueUnknownReason::FactsStale,
            };
        };
        fully_measured &= estimate.is_learned();
        queued_ms_ahead = queued_ms_ahead.saturating_add(estimate.predicted_ms);
    }

    let mut running_ahead = 0_usize;
    for running in &entry.snapshot.executing_requests {
        running_ahead += 1;
        let Some(estimate) = prices.get(
            running.command_resource_identity.as_ref(),
            running.command_class,
        ) else {
            return QueueForecast::Unknown {
                reason: QueueUnknownReason::FactsStale,
            };
        };
        let elapsed = now_unix_ms.saturating_sub(running.started_at_unix_ms);
        let remaining = estimate.predicted_ms.saturating_sub(elapsed);
        // An execution past its estimate has demonstrably outrun the prediction.
        // Its remaining time floors at zero because negative time is not a thing,
        // but the forecast stops calling itself measured: the one number it had
        // for this item has already been proven wrong.
        if remaining == 0 {
            fully_measured = false;
        }
        fully_measured &= estimate.is_learned();
        queued_ms_ahead = queued_ms_ahead.saturating_add(remaining);
    }

    QueueForecast::Forecast {
        // CPU demand never gates admission. Memory pressure can still serialize
        // these occupants, so summing their durations is the conservative bound.
        predicted_ms: queued_ms_ahead,
        requests_ahead,
        running_ahead,
        fully_measured,
        observed_at_unix_ms: observed_at,
    }
}

/// Assemble one machine's predicted time to a verdict from its legs.
///
/// The total is queue wait plus run duration and nothing else. Preparation rides
/// along as evidence because no transfer history exists to turn missing object
/// bytes into milliseconds, and a fabricated bytes-per-second constant would be
/// exactly the kind of fiction this ranking replaced. An unknown queue wait is
/// not summed as zero either: it contributes nothing to the number and says so
/// on the record, so a machine whose queue could not be read never wins by
/// looking empty.
fn placement_prediction_with_contention(
    entry: &ExecutorConnectionState,
    warmth: CacheWarmthEvidence,
    queue: QueueForecast,
    run: DurationEstimate,
    contention: ContentionEstimate,
    sync_cost: SyncCost,
) -> PlacementPrediction {
    let preparation = match sync_cost {
        SyncCost::Known(0) => PreparationForecast::ObjectsPresent,
        SyncCost::Known(bytes) => PreparationForecast::TransferPending { bytes },
        SyncCost::Unknown => PreparationForecast::Unknown,
    };
    let base_run_ms = run.predicted_ms;
    let adjusted_run_ms = contention.adjusted_ms(base_run_ms);
    PlacementPrediction {
        executor_name: executor_public_name(entry),
        executor_id: entry.identity.executor_id.clone(),
        predicted_verdict_ms: queue
            .predicted_ms()
            .unwrap_or(0)
            .saturating_add(adjusted_run_ms),
        queue,
        preparation,
        run,
        base_run_ms,
        adjusted_run_ms,
        contention,
        warmth,
    }
}

/// The prediction for a candidate that reached ranking without one.
///
/// Structurally unreachable today, and shaped so that if it ever does happen the
/// machine sorts last on an explicitly unknown queue and a labeled prior rather
/// than winning on an accidental zero.
fn unpredicted_candidate(
    entry: &ExecutorConnectionState,
    request: &CellRequest,
    sync_cost: SyncCost,
) -> PlacementPrediction {
    let context = ReservationPlan::context_for(
        &entry.identity.device_id,
        &entry.identity.executor_id,
        &entry.advertisement.capabilities,
    );
    placement_prediction_with_contention(
        entry,
        CacheWarmthEvidence::Unknown {
            reason: WarmthUnknownReason::FactsStale,
        },
        QueueForecast::Unknown {
            reason: QueueUnknownReason::FactsStale,
        },
        resource_profiles::unmeasured_duration(
            request.command_class,
            &context,
            request.command_resource_identity.as_ref(),
            ExecutionWarmth::Cold,
            DurationFallback::NoProfileStore,
        ),
        resource_profiles::contention_prior(&candidate_load(entry)),
        sync_cost,
    )
}

/// Whether this machine has already verified the exact commit the request names.
fn warm_root_holds_commit(entry: &ExecutorConnectionState, request: &CellRequest) -> bool {
    let repository = request.repository.identity();
    entry
        .advertisement
        .warm_roots
        .iter()
        .any(|root| root.repository == repository && root.commit == request.base_commit)
}

/// The three readings placement decides on, as this machine last reported them.
fn placement_readings(
    machine: &cairn_common::executor_protocol::MachineTelemetry,
) -> PlacementReadings {
    PlacementReadings {
        cpu: machine.cpu.clone(),
        memory: machine.memory.clone(),
        volume: machine.volume.clone(),
    }
}

/// Why this machine's readings cannot be decided on, or `None` when they can.
///
/// A gap comes first because it is the stronger statement: a reading that does
/// not exist cannot also be judged fresh.
fn placement_blindness(
    machine: &cairn_common::executor_protocol::MachineTelemetry,
    now_unix_ms: u64,
) -> Option<PlacementRejectionReason> {
    if let Some((measurement, gap)) = machine.placement_gaps().into_iter().next() {
        return Some(PlacementRejectionReason::TelemetryGap { measurement, gap });
    }
    [
        (MachineMeasurement::Cpu, machine.cpu.measured_at_unix_ms),
        (
            MachineMeasurement::Memory,
            machine.memory.measured_at_unix_ms,
        ),
        (
            MachineMeasurement::Volume,
            machine.volume.measured_at_unix_ms,
        ),
    ]
    .into_iter()
    .find_map(|(measurement, measured_at)| {
        let age_ms = now_unix_ms.saturating_sub(measured_at);
        (age_ms > EXECUTOR_TELEMETRY_STALE_AFTER_MS).then_some(
            PlacementRejectionReason::TelemetryStale {
                measurement,
                age_ms,
                stale_after_ms: EXECUTOR_TELEMETRY_STALE_AFTER_MS,
            },
        )
    })
}

/// Whether this repository can be recreated on a machine that does not already
/// hold it. A colocated path is addressable through the object plane; a checkout
/// that already exists somewhere is that machine's, and nowhere else's.
fn repository_is_transferable(repository: &RepositoryLocator) -> bool {
    !matches!(repository, RepositoryLocator::ExistingCheckout { .. })
}

fn repository_locator_name(repository: &RepositoryLocator) -> &'static str {
    match repository {
        RepositoryLocator::ColocatedPath { .. } => "a colocated checkout",
        RepositoryLocator::ExistingCheckout { .. } => "an existing checkout",
        RepositoryLocator::ManagedObjects { .. } => "managed objects",
        RepositoryLocator::ScratchOnly { .. } => "scratch-only storage",
    }
}

/// The refusal for work that could be placed nowhere policy is allowed to see.
///
/// Named rather than degraded: running this locally without saying so is exactly
/// how a fleet of broken remotes stays invisible.
fn no_measurable_executor_diagnostic(
    request: &CellRequest,
    rejected: &[PlacementRejection],
) -> String {
    let evaluated = rejected
        .iter()
        .map(|rejection| {
            format!(
                "{} ({})",
                rejection.executor_name,
                rejection.reason.describe()
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "no executor could take this {} request: {evaluated}. Read cairn://executors for live state.",
        request.placement_mobility.as_str()
    )
}

/// The rationale for a reservation placement did not resolve, because the caller
/// stated one and nothing overruled it.
fn unresolved_rationale(stated: &ResourceReservation) -> ReservationRationale {
    ReservationRationale {
        declared_concurrency_units: Some(stated.concurrency_units),
        profile_key: None,
        profile_context: String::new(),
        sample_count: 0,
        upper_peak_rss_bytes: None,
        upper_disk_growth_bytes: None,
        upper_duration_ms: None,
        prior: stated.clone(),
        headroom_percent: 0,
        fallback: Some(ReservationFallback::CallerDeclared),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncCost {
    Known(u64),
    Unknown,
}

fn sync_cost_key(cost: SyncCost) -> (bool, u64) {
    match cost {
        SyncCost::Known(bytes) => (false, bytes),
        SyncCost::Unknown => (true, 0),
    }
}

fn repository_sync_cost(request: &CellRequest, entry: &ExecutorConnectionState) -> SyncCost {
    if entry.colocated {
        return SyncCost::Known(0);
    }

    let repository = request.repository.identity();
    let warm_root_commits: Vec<_> = entry
        .advertisement
        .warm_roots
        .iter()
        .filter(|root| root.repository == repository)
        .map(|root| root.commit.clone())
        .collect();
    if warm_root_commits
        .iter()
        .any(|commit| commit == &request.base_commit)
    {
        return SyncCost::Known(0);
    }

    let Some(repository_path) = request.repository.colocated_path() else {
        return SyncCost::Unknown;
    };
    missing_reachable_object_bytes(repository_path, &request.base_commit, &warm_root_commits)
        .map(SyncCost::Known)
        .unwrap_or(SyncCost::Unknown)
}

// Canonical object bytes are a stable placement approximation, not predicted
// compressed wire bytes. Pack deltas, Git LFS, submodules, shallow/promisor
// history, and stale advertised roots can all make the eventual transfer differ.
fn missing_reachable_object_bytes(
    repository: &str,
    base_commit: &str,
    warm_root_commits: &[String],
) -> Result<u64, String> {
    let mut revision_args = vec!["rev-list", "--objects", "--no-object-names", base_commit];
    let exclusions: Vec<_> = warm_root_commits
        .iter()
        .map(|commit| format!("^{commit}"))
        .collect();
    revision_args.extend(exclusions.iter().map(String::as_str));

    let objects = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(revision_args)
        .output()
        .map_err(|error| format!("failed to enumerate repository objects: {error}"))?;
    if !objects.status.success() {
        return Err(String::from_utf8_lossy(&objects.stderr).trim().to_string());
    }
    if objects.stdout.is_empty() {
        return Ok(0);
    }

    inspect_object_sizes(repository, objects.stdout)
}

fn inspect_object_sizes(repository: &str, object_ids: Vec<u8>) -> Result<u64, String> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["cat-file", "--batch-check=%(objectsize)"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to inspect repository objects: {error}"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "git cat-file stdin was unavailable".to_string())?;
    // cat-file writes one response per input line. Feeding all object IDs before
    // reading its piped stdout deadlocks once both OS pipe buffers fill, which is
    // routine for a clone-sized repository. Feed stdin concurrently while
    // wait_with_output drains stdout and stderr.
    let writer = std::thread::spawn(move || stdin.write_all(&object_ids));
    let sizes = child
        .wait_with_output()
        .map_err(|error| format!("failed to read repository object sizes: {error}"))?;
    writer
        .join()
        .map_err(|_| "git object input writer panicked".to_string())?
        .map_err(|error| format!("failed to send repository objects to git: {error}"))?;
    if !sizes.status.success() {
        return Err(String::from_utf8_lossy(&sizes.stderr).trim().to_string());
    }

    String::from_utf8(sizes.stdout)
        .map_err(|error| format!("git returned non-UTF-8 object sizes: {error}"))?
        .lines()
        .try_fold(0_u64, |total, size| {
            let size = size
                .parse::<u64>()
                .map_err(|error| format!("git returned invalid object size {size:?}: {error}"))?;
            total
                .checked_add(size)
                .ok_or_else(|| "repository object size total overflowed u64".to_string())
        })
}

fn aggregate_batch_learned_estimates(
    items: &[Option<cairn_common::executor_protocol::LearnedResourceEstimate>],
) -> Option<cairn_common::executor_protocol::LearnedResourceEstimate> {
    let mut items = items.iter();
    let mut aggregate = items.next()?.clone()?;
    for item in items {
        let item = item.as_ref()?;
        aggregate.sample_count = aggregate.sample_count.min(item.sample_count);
        aggregate.upper_duration_ms = match (aggregate.upper_duration_ms, item.upper_duration_ms) {
            (Some(total), Some(value)) => Some(total.saturating_add(value)),
            _ => None,
        };
        aggregate.upper_peak_rss_bytes =
            match (aggregate.upper_peak_rss_bytes, item.upper_peak_rss_bytes) {
                (Some(current), Some(value)) => Some(current.max(value)),
                _ => None,
            };
        aggregate.upper_disk_growth_bytes = match (
            aggregate.upper_disk_growth_bytes,
            item.upper_disk_growth_bytes,
        ) {
            (Some(current), Some(value)) => Some(current.max(value)),
            _ => None,
        };
    }
    Some(aggregate)
}

/// Resolve a declared concurrency demand against the executor that will run it.
///
/// Whole-machine demand is declared as saturation because a submitter cannot
/// know which executor will be chosen, so the clamp is what makes such a
/// declaration schedulable at all. It resolves to the executor's entire
/// admission budget — the same number that executor admits against, via
/// [`ExecutorCapabilities::admission_concurrency_budget`] — so the lane both
/// always fits and leaves no headroom beside it. Deriving the budget here
/// independently is what previously let an exclusive lane share a small host.
fn clamp_declared_concurrency(declared: u32, capabilities: &ExecutorCapabilities) -> u32 {
    declared.min(capabilities.admission_concurrency_budget())
}

/// What a request would cost, resolved per candidate machine before any machine
/// is chosen.
///
/// Resource profiles are keyed by executor context (class, OS, architecture,
/// toolchains), so "what does this cost" has a different answer on every
/// candidate, and placement cannot rank on fit without asking each of them. This
/// splits that into two pure stages: derive the estimates, then rank. Nothing
/// here touches admission state; reserving capacity remains the executor's.
struct ReservationPlan {
    db: Arc<cairn_db::storage::LocalDb>,
    command_class: cairn_common::executor_protocol::CellCommandClass,
    /// One identity per batch item, empty for a single-command request.
    batch_identities: Vec<CommandResourceIdentity>,
    request_identity: Option<CommandResourceIdentity>,
    /// Present when the caller declared its own concurrency demand, which covers
    /// concurrency and nothing else.
    declared_concurrency: Option<u32>,
    /// False when the caller stated a complete reservation placement must not
    /// overrule.
    resolves: bool,
    stated: ResourceReservation,
}

impl ReservationPlan {
    fn new(
        db: Arc<cairn_db::storage::LocalDb>,
        request: &CellRequest,
        batch: Option<&ProcessBatch>,
    ) -> Self {
        // A submitter can honestly declare its CONCURRENCY demand -- it knows
        // whether its command drives its own machine-wide job server -- while
        // still knowing nothing about memory or disk, which are learned per
        // command identity from observed runs. Treat a declaration as covering
        // only concurrency: resolve the learned profile exactly as for an
        // undeclared request, then re-apply the declaration over the result.
        // Without this, declaring demand would silently opt a command out of
        // every memory and disk estimate the fleet had learned about it.
        let declared_concurrency = (request.resource_reservation.source
            == ResourceReservationSource::Declared)
            .then_some(request.resource_reservation.concurrency_units);
        Self {
            db,
            command_class: request.command_class,
            batch_identities: batch
                .map(|batch| {
                    batch
                        .items
                        .iter()
                        .filter_map(|item| item.command_resource_identity.clone())
                        .collect()
                })
                .unwrap_or_default(),
            request_identity: request.command_resource_identity.clone(),
            declared_concurrency,
            resolves: declared_concurrency.is_some()
                || request.resource_reservation == ResourceReservation::default(),
            stated: request.resource_reservation.clone(),
        }
    }

    fn profile_context(selected: &SelectedExecutor) -> resource_profiles::ProfileContext {
        Self::context_for(
            &selected.device_id,
            &selected.executor_id,
            &selected.capabilities,
        )
    }

    fn context_for(
        device_id: &str,
        executor_id: &str,
        capabilities: &ExecutorCapabilities,
    ) -> resource_profiles::ProfileContext {
        let mut toolchains = capabilities.toolchains.clone();
        toolchains.sort();
        resource_profiles::ProfileContext {
            executor_class: format!("{device_id}:{executor_id}"),
            os: capabilities.os.clone(),
            arch: capabilities.arch.clone(),
            toolchain_fingerprint: toolchains.join("\u{1f}"),
        }
    }

    /// What this work would be charged on one machine, and how long it would
    /// take there.
    ///
    /// Warmth is a per-candidate fact, so the duration half of this answer can
    /// differ between machines even where the reservation half does not: the
    /// same suite against a populated target directory and against an empty one
    /// is exactly the difference this ranking turns on.
    async fn resolve_for(
        &self,
        request: &CellRequest,
        device_id: &str,
        executor_id: &str,
        capabilities: &ExecutorCapabilities,
        duration_context: resource_profiles::DurationContext,
    ) -> resource_profiles::ResolvedResourceProfile {
        let context = Self::context_for(device_id, executor_id, capabilities);
        if !self.resolves {
            // A caller-stated reservation is not a caller-stated duration. The
            // prediction still comes from this machine's own history, because a
            // submitter that knows its own memory demand still knows nothing
            // about how fast any particular host is.
            return resource_profiles::ResolvedResourceProfile {
                reservation: self.stated.clone(),
                learned_estimate: request.learned_estimate.clone(),
                rationale: resource_profiles::declared_rationale(&context, self.stated.clone()),
                duration: self.predict_duration(&context, duration_context).await,
                item_durations: Vec::new(),
            };
        }
        // Whether the caller declared concurrency is decided here, before the
        // learned lookup, because the two answers must both survive onto the
        // record. Replacing one explanation with the other is how a
        // caller-declared whole-machine charge came to read as "learned from 1
        // observation, fell back because belowConfidenceFloor" (CAIRN-3345):
        // the learning it named cannot produce a concurrency number at all.
        let prior = resource_profiles::cold_start_prior(self.command_class, capabilities);
        let mut resolved = if self.batch_identities.is_empty() {
            resource_profiles::resolve_reservation(
                self.db.clone(),
                self.request_identity.as_ref(),
                &context,
                prior,
                capabilities.memory_budget_bytes,
                duration_context,
            )
            .await
        } else {
            self.resolve_batch(
                &context,
                prior,
                capabilities.memory_budget_bytes,
                duration_context,
            )
            .await
        };
        // Whole-machine demand is declared as saturation because the submitter
        // cannot know which executor would be chosen. Clamp it to this executor's
        // capacity: a reservation larger than the host's budget can never fit, so
        // leaving it unclamped would queue the request until its deadline rather
        // than running it.
        if let Some(units) = self.declared_concurrency {
            let clamped = clamp_declared_concurrency(units, capabilities);
            resolved.reservation.concurrency_units = clamped;
            resolved.reservation.source = ResourceReservationSource::Declared;
            resolved.rationale.declared_concurrency_units = Some(clamped);
        }
        resolved
    }

    /// The duration half alone, for the paths that do not resolve a reservation.
    async fn predict_duration(
        &self,
        context: &resource_profiles::ProfileContext,
        duration_context: resource_profiles::DurationContext,
    ) -> cairn_common::executor_protocol::DurationEstimate {
        if self.batch_identities.is_empty() {
            return resource_profiles::resolve_duration(
                self.db.clone(),
                self.request_identity.as_ref(),
                context,
                duration_context.warmth,
                duration_context.class,
                duration_context.now_unix_ms,
            )
            .await;
        }
        self.predict_batch_duration(context, duration_context).await
    }

    /// A batch's predicted run time is the SUM of its items, not the maximum.
    ///
    /// The items run one after another in the same cell, so the caller waits for
    /// all of them. This is the one place where duration and reservation compose
    /// in opposite directions: a batch is CHARGED for its heaviest item, because
    /// that is the peak it must fit under, and TAKES as long as all of them.
    async fn predict_batch_duration(
        &self,
        context: &resource_profiles::ProfileContext,
        duration_context: resource_profiles::DurationContext,
    ) -> cairn_common::executor_protocol::DurationEstimate {
        let mut total: Option<cairn_common::executor_protocol::DurationEstimate> = None;
        for identity in &self.batch_identities {
            let item = resource_profiles::resolve_duration(
                self.db.clone(),
                Some(identity),
                context,
                duration_context.warmth,
                duration_context.class,
                duration_context.now_unix_ms,
            )
            .await;
            total = Some(match total {
                None => item,
                Some(mut running) => {
                    running.predicted_ms = running.predicted_ms.saturating_add(item.predicted_ms);
                    // A sum is only as measured as its least-measured leg, and
                    // only as confident as its thinnest one. Anything else would
                    // let one well-observed item dress up a batch nothing else
                    // in it has been seen doing.
                    if !item.is_learned() {
                        running.source = item.source;
                        running.fallback = item.fallback;
                    }
                    running.sample_count = running.sample_count.min(item.sample_count);
                    running.updated_at_unix_ms =
                        match (running.updated_at_unix_ms, item.updated_at_unix_ms) {
                            (Some(left), Some(right)) => Some(left.min(right)),
                            _ => None,
                        };
                    running
                }
            });
        }
        total.unwrap_or_else(|| {
            resource_profiles::unmeasured_duration(
                duration_context.class,
                context,
                None,
                duration_context.warmth,
                cairn_common::executor_protocol::DurationFallback::NoCommandIdentity,
            )
        })
    }

    async fn resolve_batch(
        &self,
        context: &resource_profiles::ProfileContext,
        prior: ResourceReservation,
        memory_budget_bytes: Option<u64>,
        duration_context: resource_profiles::DurationContext,
    ) -> resource_profiles::ResolvedResourceProfile {
        let mut reservation = ResourceReservation::default();
        let mut learned_estimates = Vec::with_capacity(self.batch_identities.len());
        let mut item_durations = Vec::with_capacity(self.batch_identities.len());
        // No item has spoken yet, so there is no rationale to seed: an unconsulted
        // batch must not carry a stand-in explanation that could survive as one.
        let mut rationale: Option<ReservationRationale> = None;
        for identity in &self.batch_identities {
            let item = resource_profiles::resolve_reservation(
                self.db.clone(),
                Some(identity),
                context,
                prior.clone(),
                memory_budget_bytes,
                duration_context,
            )
            .await;
            reservation.memory_bytes = reservation.memory_bytes.max(item.reservation.memory_bytes);
            reservation.disk_growth_bytes = reservation
                .disk_growth_bytes
                .max(item.reservation.disk_growth_bytes);
            reservation.concurrency_units = reservation
                .concurrency_units
                .max(item.reservation.concurrency_units);
            reservation.source = match (reservation.source, item.reservation.source) {
                (ResourceReservationSource::Learned, _)
                | (_, ResourceReservationSource::Learned) => ResourceReservationSource::Learned,
                _ => ResourceReservationSource::Unmeasured,
            };
            // The batch is charged for its heaviest item, so that is the item
            // whose rationale explains the number.
            if rationale.is_none() || item.reservation.memory_bytes >= reservation.memory_bytes {
                rationale = Some(item.rationale.clone());
            }
            learned_estimates.push(item.learned_estimate);
            item_durations.push(item.duration);
        }
        resource_profiles::ResolvedResourceProfile {
            reservation,
            learned_estimate: aggregate_batch_learned_estimates(&learned_estimates),
            rationale: rationale
                .unwrap_or_else(|| resource_profiles::declared_rationale(context, prior)),
            duration: self.predict_batch_duration(context, duration_context).await,
            item_durations,
        }
    }
}

/// Three missed 30-second executor heartbeats make the live connection stale.
/// This bound describes the *link*, and nothing else: an executor beating on
/// time is reachable whatever the facts it carries look like.
const EXECUTOR_LINK_STALE_AFTER_MS: u64 = 90_000;

/// An executor emits beats from one task and computes what they carry on
/// another, so a link that is demonstrably alive can still be shipping facts
/// that stopped moving. The payload refreshes on the same interval the beat
/// does, so the same three-cycle allowance applies: past it, the pressure, disk,
/// queue, and warm-root numbers on this snapshot are history, and presenting
/// them as current is the failure this bound exists to prevent.
///
/// It is reported separately from the link, because they call for different
/// responses and every surface has to say which one happened. Folding aged facts
/// into the connection status is how a healthy machine came to be labelled
/// "heartbeat stale" while its heartbeats were arriving on time. This is a
/// health verdict only — link stall remediation is silence, and silence alone.
const EXECUTOR_TELEMETRY_STALE_AFTER_MS: u64 = EXECUTOR_LINK_STALE_AFTER_MS;

/// The minimum gap between two telemetry-stall bounces anywhere in the fleet.
///
/// Two heartbeat intervals: long enough for the machine that just went to
/// reattach and publish a beat or two, so the next candidate is judged against
/// a fleet that has already absorbed the previous recovery rather than against
/// the moment of maximum disruption.
const TELEMETRY_STALL_BOUNCE_SPACING_MS: u64 = 2 * EXECUTOR_HEARTBEAT_INTERVAL_MS;

/// How long ago this connection's executor last recomputed the facts its
/// heartbeats carry, by the executor's own stamp.
///
/// `None` when the executor publishes no such stamp. The beat's own
/// `observed_at_unix_ms` is deliberately not substituted for it: that would
/// answer a freshness question about the facts with a timestamp taken when they
/// were shipped, which is precisely the conflation the split stamp exists to
/// prevent.
fn published_facts_age_ms(entry: &ExecutorConnectionState, now_unix_ms: u64) -> Option<u64> {
    entry
        .advertisement
        .liveness_observed_at_unix_ms
        .map(|observed_at| now_unix_ms.saturating_sub(observed_at))
}

/// How long this connection has been publishing facts that stopped being
/// recomputed *and has not said why*.
///
/// Stall remediation reads this rather than the raw age, because the two
/// freezes are not the same fault. An executor whose refresh pass has overrun
/// its bound names the phase it is stuck in on every beat: that is a machine
/// reporting its own defect while still running its work and still answering
/// its link, and recycling that link answers the report by destroying the
/// reporter along with every batch it holds. A freeze with no explanation is
/// the case the watchdog was written for and reaches it unchanged — including
/// from an executor too old to explain itself, which claims nothing here and is
/// judged exactly as before.
///
/// The facts are equally stale either way, and nothing here makes them
/// trustworthy: placement, warmth, queue forecasting and `telemetry_stale` all
/// keep reading [`published_facts_age_ms`] and keep refusing them. What the
/// explanation buys is the difference between a degraded machine and a dead
/// link.
fn unexplained_facts_age_ms(entry: &ExecutorConnectionState, now_unix_ms: u64) -> Option<u64> {
    if entry.advertisement.facts_stale_refresh_blocked.is_some() {
        return None;
    }
    published_facts_age_ms(entry, now_unix_ms)
}

fn executor_health_snapshot(
    entry: &ExecutorConnectionState,
    captured_at_unix_ms: u64,
    expected_build_ids: &HashMap<String, String>,
) -> ExecutorHealthSnapshot {
    let heartbeat_age_ms =
        captured_at_unix_ms.saturating_sub(entry.advertisement.observed_at_unix_ms);
    let liveness_age_ms = published_facts_age_ms(entry, captured_at_unix_ms);
    let telemetry_stale =
        liveness_age_ms.is_some_and(|age| age > EXECUTOR_TELEMETRY_STALE_AFTER_MS);
    ExecutorHealthSnapshot {
        identity: entry.identity.clone(),
        public_name: executor_public_name(entry),
        colocated: entry.colocated,
        status: if heartbeat_age_ms > EXECUTOR_LINK_STALE_AFTER_MS {
            ExecutorHealthStatus::Stale
        } else {
            ExecutorHealthStatus::Online
        },
        heartbeat_age_ms,
        liveness_age_ms,
        telemetry_stale,
        advertisement: entry.advertisement.clone(),
        admission: entry.health.admission.clone(),
        queues: entry.health.queues.clone(),
        host: entry.health.host.clone(),
        disk: entry.health.disk.clone(),
        machine: entry.health.machine.clone(),
        inventory: entry.health.inventory.clone(),
        connection_generation: entry.generation,
        applied_policy: entry.health.applied_policy.clone(),
        // One fact with two witnesses: the drain this runner is holding, and
        // the one the executor reports. A machine that has not yet been told
        // about a drain is still drained, so the surfaces read the union.
        drain_mode: entry.drained || entry.health.drain_mode,
        resident_processes: entry.health.resident_processes.clone(),
        command_processes: entry.health.command_processes.clone(),
        evicted_cells: entry.health.evicted_cells.clone(),
        build_skew: expected_build_ids
            .get(&entry.identity.executor_id)
            .zip(entry.executor_build_id.as_ref())
            .filter(|(expected, running)| expected != running)
            .map(
                |(expected, running)| cairn_common::executor_protocol::BuildSkew {
                    runner_build_id: expected.clone(),
                    executor_build_id: running.clone(),
                },
            ),
    }
}

fn selected_executor(entry: &ExecutorConnectionState) -> SelectedExecutor {
    SelectedExecutor {
        executor_id: entry.identity.executor_id.clone(),
        device_id: entry.identity.device_id.clone(),
        generation: entry.generation,
        sender: entry.sender.clone(),
        colocated: entry.colocated,
        capabilities: entry.advertisement.capabilities.clone(),
    }
}

// Colocated and unrestricted enrolled executors share the runner's project routing authority.
fn serves_project(entry: &ExecutorConnectionState, project_id: &str) -> bool {
    entry.colocated
        || projects_serve(
            &entry.advertisement.capabilities.projects_served,
            project_id,
        )
}

fn projects_serve(projects_served: &[String], project_id: &str) -> bool {
    projects_served.is_empty() || projects_served.iter().any(|project| project == project_id)
}

/// The public address of a connected executor.
///
/// Derived rather than stored on the connection so one rule decides it
/// everywhere: the runner's own executor answers to the reserved name, and every
/// enrolled machine answers to the normalization of the label it advertises. An
/// advertisement whose label normalizes to nothing falls back to its identity,
/// so a machine always has an address rather than vanishing from the fleet.
fn executor_public_name(entry: &ExecutorConnectionState) -> String {
    if entry.colocated {
        return LOCAL_EXECUTOR_NAME.to_string();
    }
    normalize_executor_name(&entry.identity.display_name)
        .or_else(|| normalize_executor_name(&entry.identity.executor_id))
        .unwrap_or_else(|| entry.identity.executor_id.clone())
}

fn matches_selector(entry: &ExecutorConnectionState, selector: &ExecutorSelector) -> bool {
    selector
        .name
        .as_deref()
        .is_none_or(|value| executor_names_match(value, &executor_public_name(entry)))
        && selector
            .os
            .as_ref()
            .is_none_or(|value| value.eq_ignore_ascii_case(&entry.advertisement.capabilities.os))
        && selector.required_toolchains.iter().all(|required| {
            entry
                .advertisement
                .capabilities
                .toolchains
                .iter()
                .any(|available| available == required)
        })
        && matches_sandbox_capability(&entry.advertisement.capabilities, selector)
}

fn matches_sandbox_capability(
    capabilities: &ExecutorCapabilities,
    selector: &ExecutorSelector,
) -> bool {
    !selector.requires_macos_nested_sandbox_write
        || !capabilities.os.eq_ignore_ascii_case("macos")
        || capabilities
            .sandbox
            .as_ref()
            .is_some_and(|evidence| evidence.nested_sandbox_write_available)
}

/// The fleet as a refusal has to describe it: every live machine by the name a
/// caller could have asked for, with what it runs and what it can build.
///
/// A refusal that names only what was wanted leaves the caller to guess what
/// exists, and guessing is what opaque identities forced. Naming both closes the
/// loop against `cairn://executors`, which is the same list from the same cache.
fn known_executor_inventory(connections: &HashMap<String, ExecutorConnectionState>) -> String {
    let mut rows: Vec<String> = connections
        .values()
        .filter(|entry| !entry.sender.is_closed())
        .map(|entry| {
            let capabilities = &entry.advertisement.capabilities;
            let toolchains = if capabilities.toolchains.is_empty() {
                "no advertised toolchains".to_string()
            } else {
                format!("toolchains {}", capabilities.toolchains.join(", "))
            };
            let sandbox = capabilities.sandbox.as_ref().map_or_else(
                || "nested Seatbelt write capability not reported".to_string(),
                |evidence| {
                    format!(
                        "CAIRN_SANDBOXED={}, nested Seatbelt write={} ({})",
                        if evidence.cairn_sandboxed_marker {
                            "set"
                        } else {
                            "unset"
                        },
                        if evidence.nested_sandbox_write_available {
                            "available"
                        } else {
                            "unavailable"
                        },
                        evidence.nested_sandbox_write_detail
                    )
                },
            );
            format!(
                "{} ({}, {toolchains}, {sandbox})",
                executor_public_name(entry),
                capabilities.os
            )
        })
        .collect();
    rows.sort();
    if rows.is_empty() {
        "no executor is currently attached".to_string()
    } else {
        rows.join("; ")
    }
}

/// Why nothing in the fleet can take this request, in the terms the caller used
/// plus the terms it could have used instead.
fn no_matching_executor_diagnostic(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
    rejected: &[PlacementRejection],
) -> String {
    if let Some(pinned) = request.pinned_executor_id.as_deref() {
        if let Some(rejection) = rejected
            .iter()
            .find(|rejection| rejection.executor_id == pinned)
        {
            return format!(
                "the executor holding this job's execution home ({}) cannot run its resident work: {}. Read cairn://executors for live state.",
                rejection.executor_name,
                rejection.reason.describe()
            );
        }
    }

    let drained_target = rejected.iter().find(|rejection| {
        matches!(rejection.reason, PlacementRejectionReason::Drained)
            && connections
                .get(&rejection.executor_id)
                .is_some_and(|entry| {
                    request
                        .executor
                        .as_ref()
                        .filter(|selector| !selector.is_empty())
                        .is_none_or(|selector| matches_selector(entry, selector))
                        && serves_project(entry, &request.project_id)
                })
    });
    if let Some(rejection) = drained_target {
        return format!(
            "{} is drained by an operator, so the runner is refusing new work there. Only an operator can clear the drain; read cairn://executors for live state.",
            rejection.executor_name
        );
    }

    // A pin and a selector are different halves of the same failure, and a
    // refusal reporting only the pin tells an agent its batch was misplaced when
    // what actually failed is the request it wrote. The caller's own words
    // appear whenever the caller supplied any.
    let pinned = request.pinned_executor_id.as_deref().map(|pinned| {
        // The caller never chose the pin, so it is owed the public name rather
        // than the identity placement happened to use.
        match connections
            .values()
            .find(|entry| entry.identity.executor_id == pinned)
            .map(executor_public_name)
        {
            Some(name) => format!("the executor holding this job's execution home ({name})"),
            None => "the executor holding this job's execution home".to_string(),
        }
    });
    let asked = request
        .executor
        .as_ref()
        .filter(|selector| !selector.is_empty())
        .map(|selector| selector.describe());
    let wanted = match (pinned, asked) {
        (Some(pinned), Some(asked)) => format!("{pinned}, which must also satisfy {asked}"),
        (Some(pinned), None) => pinned,
        (None, Some(asked)) => asked,
        (None, None) => "any executor".to_string(),
    };
    // Trust narrows the same fleet the selector does, and a refusal that reports
    // only half of what was applied sends the reader looking for a machine that
    // was passed over for the other half.
    let wanted = if request.verdict_platforms.is_empty() {
        wanted
    } else {
        format!(
            "{wanted} on {} (the platform(s) this verdict counts from)",
            request.verdict_platforms.join(", ")
        )
    };
    format!(
        "no live enrolled executor satisfies {wanted} for project {}. Known executors: {}. Read cairn://executors for live state.",
        request.project_id,
        known_executor_inventory(connections)
    )
}

/// Whether this batch runs JavaScript, and so needs the Cairn packages to travel
/// with it.
///
/// A direct item names its program, which is how an inline TypeScript item
/// arrives. A shell item carries its whole command line in `args` instead, so
/// `bun script.ts` written as a shell command has exactly the same import to
/// resolve; answering only the inline form would leave the shell form as a
/// second, worse answer to one question. The scan is deliberately generous --
/// a command that merely mentions a runtime carries a few unread kilobytes,
/// while a missed one is the failure this whole mechanism exists to remove.
fn batch_runs_javascript(items: &[ProcessBatchItem]) -> bool {
    const RUNTIMES: [&str; 4] = ["bun", "bunx", "node", "npx"];
    items.iter().any(|item| match item.execution {
        ProcessBatchExecution::NativeShell => item.args.iter().any(|command| {
            command
                .split(|c: char| !c.is_alphanumeric())
                .any(|word| RUNTIMES.contains(&word))
        }),
        _ => RUNTIMES.contains(&item.program.as_str()),
    })
}

fn serialize_process_batch(
    batch: ResolvedRunBatch,
    env: &[(String, String)],
    runner_context_id: String,
    sandbox_mode: ProcessSandboxMode,
) -> Result<ProcessBatch, String> {
    let mut items = Vec::with_capacity(batch.resolved.len());
    for (index, (header, spec)) in batch.resolved.into_iter().enumerate() {
        let spec = spec.map_err(|error| format!("resolve process item {header}: {error}"))?;
        let (execution, program, args, stdin, timeout) = match spec {
            RunSpec::Shell { command, timeout } => (
                ProcessBatchExecution::NativeShell,
                String::new(),
                vec![command],
                None,
                timeout,
            ),
            RunSpec::Script {
                program,
                args,
                timeout,
                stdin,
            } => (ProcessBatchExecution::Direct, program, args, stdin, timeout),
            RunSpec::McpCall(_)
            | RunSpec::ExecutorAction(_)
            | RunSpec::ResponseCall { .. }
            | RunSpec::ReplSend { .. } => {
                return Err(format!(
                    "{header} is not process-backed and cannot use a build cell"
                ))
            }
        };
        items.push(ProcessBatchItem {
            header,
            stream_id: format!("{}:{index}", batch.tool_use_id),
            execution,
            program,
            args,
            env: env.to_vec(),
            stdin,
            // The one clamp: an omitted bound runs to completion under the batch
            // ceiling, an explicit one is honored up to it. This layer used to
            // apply no bound of its own while the socket above it applied a
            // smaller one, which is how a suite's output was lost.
            timeout_ms: crate::mcp::handlers::run::clamp_run_item_timeout_ms(timeout).into(),
            command_resource_identity: None,
            verdict_environment_names: Vec::new(),
        });
    }
    // A batch that runs bun carries the version-matched SDK with it, so inline
    // TypeScript resolves `@cairn/sdk` on whichever machine takes the batch --
    // including a project with no JavaScript toolchain of its own. Batches that
    // run no JavaScript carry nothing, so shell, Python, and MATLAB work is not
    // taxed with runtime bytes it will never read.
    //
    // A runner that cannot produce its own SDK fails the batch here, naming the
    // installation, rather than letting bun report a missing project dependency
    // and sending the reader to look for a mistake in their own project.
    let runtime_packages = if batch_runs_javascript(&items) {
        crate::runtime::sdk_packages()
            .map_err(|error| format!("Cairn cannot provide @cairn/sdk to this batch: {error}"))?
    } else {
        Vec::new()
    };
    Ok(ProcessBatch {
        files: Vec::new(),
        runtime_packages,
        sequential: batch.originally_sequential,
        stop_on_error: batch.stop_on_error,
        sandbox_mode,
        timeout_bases: Vec::new(),
        items,
        runner_context_id: Some(runner_context_id),
        execution_residency: batch.execution_residency,
    })
}

fn outcome_matches(outcome: &CellOutcome, request_id: &str, attempt_id: &str) -> bool {
    match outcome {
        CellOutcome::Completed {
            request_id: r,
            attempt_id: a,
            ..
        }
        | CellOutcome::FailedAfterExecution {
            request_id: r,
            attempt_id: a,
            ..
        }
        | CellOutcome::StorageFailure {
            request_id: r,
            attempt_id: a,
            ..
        }
        | CellOutcome::Cancelled {
            request_id: r,
            attempt_id: a,
        } => r == request_id && a == attempt_id,
        CellOutcome::Unavailable { .. } => true,
    }
}

fn request_watchdog_duration(
    request: &CellRequest,
    batch: Option<&ProcessBatch>,
    executor_config: &ExecutorConfig,
    colocated: bool,
) -> Duration {
    let acquisition =
        Duration::from_millis(request.wait_horizon_unix_ms.saturating_sub(unix_time_ms()));
    let phase_budget = Duration::from_secs(executor_config.default_timeout_seconds);
    // Provisioning/checkout and preparation are distinct executor phases.
    let infrastructure = phase_budget.saturating_mul(2);
    // Managed fetch and post-command delta upload each use the executor's bounded
    // whole-request HTTP deadline. Colocated execution performs neither transfer.
    let object_transfer = if colocated {
        Duration::ZERO
    } else {
        Duration::from_secs(MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS * 2)
    };
    let execution = match batch {
        None => Duration::from_millis(request.timeout_ms),
        Some(batch) if batch.sequential => {
            batch.items.iter().fold(Duration::ZERO, |total, item| {
                total.saturating_add(Duration::from_millis(item.timeout_ms))
            })
        }
        Some(batch) => batch
            .items
            .iter()
            .map(|item| Duration::from_millis(item.timeout_ms))
            .max()
            .unwrap_or(Duration::ZERO),
    };
    let end_to_end_budget = acquisition
        .saturating_add(infrastructure)
        .saturating_add(object_transfer)
        .saturating_add(execution);
    // Declared acquisition holds extend this deadline dynamically from executor
    // snapshots. Keep the 2806 phase arithmetic static so held time is added once.
    let proportional_slack = end_to_end_budget / 10;
    end_to_end_budget.saturating_add(
        proportional_slack.clamp(MIN_REQUEST_WATCHDOG_SLACK, MAX_REQUEST_WATCHDOG_SLACK),
    )
}

fn executor_unavailable(diagnostic: String) -> CellOutcome {
    CellOutcome::Unavailable {
        reason: CellUnavailableReason::ExecutorUnavailable,
        diagnostic,
    }
}

/// Prefix a remote object-materialization refusal with the placement it came
/// from, leaving the executor's own coordinate and the low-level cause intact
/// as the tail. Every other outcome passes through untouched: this narrows a
/// diagnostic, it never reclassifies one.
fn name_placement_in_object_refusal(
    outcome: CellOutcome,
    executor_id: &str,
    generation: u64,
) -> CellOutcome {
    match outcome {
        CellOutcome::Unavailable {
            reason: CellUnavailableReason::ObjectInfrastructure(stage),
            diagnostic,
        } => CellOutcome::Unavailable {
            reason: CellUnavailableReason::ObjectInfrastructure(stage),
            diagnostic: format!("on executor {executor_id} generation {generation}: {diagnostic}"),
        },
        other => other,
    }
}

pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The instant every fleet fixture is stamped at, so a test's machines, their
/// telemetry, and the placement clock reading them all agree.
#[cfg(test)]
const NOW: u64 = 1_000_000_000;

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{
        LivenessRefreshBlocked, LivenessRefreshPhase, RemoteConnectionPhase,
        LIVENESS_REFRESH_BOUND_MS,
    };

    /// The claim the whole design rests on, exercised as composition rather
    /// than as bookkeeping: a successor that has won a question does not get to
    /// run until the answer it displaced is confirmed dead.
    ///
    /// Asserting only that the registry shows one holder would pass against the
    /// exact defect this guards. The registry showing one item is compatible
    /// with two items running — that is precisely what a fire-and-forget cancel
    /// produces — so what is asserted here is that the gate is still closed
    /// while the death is unconfirmed, and opens on the terminal outcome and
    /// nothing else.
    #[tokio::test]
    async fn a_successor_does_not_run_until_what_it_displaced_is_confirmed_dead() {
        let fleet = Fleet::default();
        let key = purpose::PurposeKey::main_attestation("project");
        let older: RequestIdentity = ("tip-x".into(), "tip-x-attempt".into());
        let newer: RequestIdentity = ("tip-x1".into(), "tip-x1-attempt".into());

        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), older.clone(), 10);
        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), newer.clone(), 20);

        let mut waiting = {
            let fleet = fleet.clone();
            let (key, newer) = (key.clone(), newer.clone());
            tokio::spawn(async move { fleet.await_purpose_quiescent(&key, &newer).await })
        };

        // Winning the key is not permission to dispatch. This also parks the
        // waiter on its wakeup, so the notification below cannot be delivered
        // before anyone is listening for it.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiting)
                .await
                .is_err(),
            "the successor must not proceed while the item it displaced is still alive"
        );

        // The predecessor's terminal outcome arrives, exactly as the leader
        // task delivers it.
        fleet.purposes.lock().unwrap().finish(&older);
        fleet.purpose_quiesced.notify_waiters();

        let gate = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .expect("a confirmed death must release the successor")
            .expect("the waiting task must not panic");
        assert!(matches!(gate, PurposeGate::Clear));
    }

    /// The burst this issue exists for: three tips in quick succession, where
    /// the middle one is displaced while it is still waiting to dispatch.
    ///
    /// Being displaced is what puts an item into the debt set, so a gate that
    /// only asked "does this question owe anything" would have the middle item
    /// waiting on itself — and it could only leave that set by completing work
    /// the gate was holding it back from starting. The deadlock was real,
    /// reachable from an ordinary merge stream, and cost the newest tip the full
    /// budget on a request that had provably never run.
    #[tokio::test]
    async fn an_item_displaced_while_waiting_to_dispatch_settles_instead_of_deadlocking() {
        let fleet = Fleet::default();
        let key = purpose::PurposeKey::main_attestation("project");
        let a: RequestIdentity = ("tip-a".into(), "a".into());
        let b: RequestIdentity = ("tip-b".into(), "b".into());
        let c: RequestIdentity = ("tip-c".into(), "c".into());

        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), a.clone(), 10);
        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), b.clone(), 20);

        // B holds the question and waits for A's death.
        let mut b_gate = {
            let fleet = fleet.clone();
            let (key, b) = (key.clone(), b.clone());
            tokio::spawn(async move { fleet.await_purpose_quiescent(&key, &b).await })
        };
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut b_gate)
                .await
                .is_err(),
            "B must wait for A"
        );

        // C arrives and takes the question from B, which is still at the gate.
        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), c.clone(), 30);
        fleet.purpose_quiesced.notify_waiters();

        let gate = tokio::time::timeout(std::time::Duration::from_secs(5), b_gate)
            .await
            .expect("a displaced waiter must not wait out the budget on itself")
            .expect("the waiting task must not panic");
        assert!(
            matches!(gate, PurposeGate::Displaced),
            "B never dispatched, so it is settled rather than owed a death"
        );

        // B settled its own debt on the way out, so the question owes only A.
        let registry = fleet.purposes.lock().unwrap();
        assert_eq!(registry.pending_retirements(&key), 1);
        assert!(registry.is_live_holder(&key, &c));
    }

    /// A debt that never clears refuses the successor rather than dispatching
    /// beside it.
    ///
    /// Proceeding would knowingly create the overlap this whole design exists to
    /// prevent, on the reasoning that a logged overlap is better than a stall.
    /// It is not: a warning is a record, not a decision anyone made, and the
    /// operator symptom of that overlap is exactly what opened this issue. A
    /// refusal is visible, is re-armed by the next cadence, and never puts two
    /// answers to one question on the fleet.
    #[tokio::test(start_paused = true)]
    async fn a_debt_that_never_clears_refuses_rather_than_dispatching_beside_it() {
        let fleet = Fleet::default();
        let key = purpose::PurposeKey::main_attestation("project");
        let newer: RequestIdentity = ("tip-x1".into(), "attempt".into());
        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), ("tip-x".into(), "attempt".into()), 10);
        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), newer.clone(), 20);

        // Nothing ever clears the debt; the budget is what returns.
        let gate = fleet.await_purpose_quiescent(&key, &newer).await;
        assert!(
            matches!(gate, PurposeGate::Owed { .. }),
            "an unpaid debt must refuse the successor, not wave it through"
        );
        assert_eq!(
            fleet.purposes.lock().unwrap().pending_retirements(&key),
            1,
            "the debt is still owed; the gate never pretended it was paid"
        );
    }

    /// A question with nothing outstanding does not make its next answer wait.
    #[tokio::test]
    async fn an_uncontested_question_dispatches_without_waiting() {
        let fleet = Fleet::default();
        let key = purpose::PurposeKey::main_attestation("project");
        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), ("tip-x".into(), "attempt".into()), 10);

        let gate = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fleet.await_purpose_quiescent(&key, &("tip-x".into(), "attempt".into())),
        )
        .await
        .expect("a question owing no deaths must not delay its own answer");
        assert!(matches!(gate, PurposeGate::Clear));
    }

    /// The chokepoint's whole promise, composed rather than in pieces: arming a
    /// newer answer to a question retires the older one, and the retirement is
    /// recorded where the pre-submit gate will find it.
    ///
    /// The phase the displaced item had reached is deliberately not part of the
    /// condition. An item still being placed has no executor to tell, so the
    /// record alone must stop it — that is what closes the window in which a
    /// superseded wave used to dispatch anyway and then chase itself with a
    /// cancel it could lose.
    #[test]
    fn arming_a_newer_answer_retires_the_older_before_it_can_dispatch() {
        let fleet = Fleet::default();
        let key = purpose::PurposeKey::main_attestation("project");
        let older: RequestIdentity = ("tip-x".into(), "tip-x-attempt".into());
        let newer: RequestIdentity = ("tip-x1".into(), "tip-x1-attempt".into());

        assert_eq!(
            fleet
                .purposes
                .lock()
                .unwrap()
                .admit(key.clone(), older.clone(), 10),
            purpose::Admission::Admitted
        );
        let admission = fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), newer.clone(), 20);
        let purpose::Admission::Replaced { retire, .. } = admission else {
            panic!("a newer answer to one question must displace the older, got {admission:?}");
        };
        assert_eq!(retire, older);

        fleet.retire_dispatched_item(&retire);
        assert!(
            fleet.cancelled_leaders.lock().unwrap().contains(&older),
            "a retirement must be recorded even with no executor to deliver it to"
        );

        // The gate consumes the record, and consumes it once: a later item that
        // was never retired must not inherit this one's cancellation.
        assert!(fleet.cancelled_leaders.lock().unwrap().remove(&older));
        assert!(!fleet.cancelled_leaders.lock().unwrap().contains(&newer));
    }

    /// A stale item is refused by the registry rather than left to race, so the
    /// key still names the newest answer afterwards.
    #[test]
    fn an_answer_older_than_the_live_one_never_becomes_the_live_one() {
        let fleet = Fleet::default();
        let key = purpose::PurposeKey::main_attestation("project");
        let newer: RequestIdentity = ("newer".into(), "newer-attempt".into());
        let older: RequestIdentity = ("older".into(), "older-attempt".into());

        fleet
            .purposes
            .lock()
            .unwrap()
            .admit(key.clone(), newer.clone(), 50);
        assert_eq!(
            fleet
                .purposes
                .lock()
                .unwrap()
                .admit(key.clone(), older.clone(), 20),
            purpose::Admission::Superseded {
                holder: newer.clone()
            }
        );
        assert!(
            !fleet.cancelled_leaders.lock().unwrap().contains(&newer),
            "refusing a stale arrival must not retire the live item it lost to"
        );
    }

    /// A machine's desktop configuration is keyed by the name it answers to, so
    /// the two operations that change which names exist have to move it.
    #[test]
    fn desktop_configuration_follows_a_machine_through_rename_and_removal() {
        let mut config = DesktopAutomationConfig {
            enabled: true,
            machines: HashMap::from([(
                "bglab-win".to_string(),
                MachineDesktopAutomation {
                    enabled: None,
                    binary: Some(r"C:\axon\axon-win.exe".to_string()),
                },
            )]),
        };

        config.rename_machine("BGLab Win", "bglab-windows");
        assert_eq!(
            config.resolve("bglab-windows", "windows").binary,
            r"C:\axon\axon-win.exe",
            "a renamed machine keeps the binary it was configured with"
        );
        assert!(
            !config.machines.contains_key("bglab-win"),
            "the old name is not left behind addressing nothing"
        );

        config.forget_machine("bglab-windows");
        assert!(config.machines.is_empty());
        assert_eq!(
            config.resolve("bglab-windows", "windows").binary,
            "axon-win.exe",
            "a forgotten machine falls back to the platform default"
        );
    }

    #[test]
    fn desktop_automation_resolves_inheritance_binary_and_normalized_machine_name() {
        let mut config = DesktopAutomationConfig {
            enabled: false,
            ..Default::default()
        };
        config.machines.insert(
            "bglab-win".into(),
            MachineDesktopAutomation {
                enabled: Some(true),
                binary: Some(r"C:\\Axon\\axon.exe".into()),
            },
        );

        assert_eq!(
            config.resolve("BGLAB WIN", "windows"),
            ResolvedDesktopAutomation {
                enabled: true,
                binary: r"C:\\Axon\\axon.exe".into(),
            }
        );
        assert_eq!(
            DesktopAutomationConfig::default().resolve("local", "linux"),
            ResolvedDesktopAutomation {
                enabled: true,
                binary: "axon-linux".into(),
            }
        );
    }
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use cairn_codec::testutil::{commit_all, init_repo, write_file};
    use cairn_common::executor_protocol::{
        CellAdmissionKind, CellOccupancy, GitObjectFormat, MachineMemory, Measurement,
        ResidencyFence, ResidentOccupancyEvidence, VerifiedWarmRoot,
        EXECUTOR_LINK_STALL_REMEDIATION_MS,
    };

    fn relay_request(run_id: Option<&str>) -> cairn_common::protocol::CallbackRequest {
        cairn_common::protocol::CallbackRequest {
            cwd: "/tmp/worktree".into(),
            run_id: run_id.map(str::to_string),
            tool: "read".into(),
            payload: serde_json::json!({"paths": ["cairn:~/todos"]}),
            ..Default::default()
        }
    }

    fn insert_bound_relay_context(pool: &Fleet) {
        pool.runner_contexts.lock().unwrap().insert(
            "context".into(),
            RunnerContextEntry::new(
                "context",
                "request-1:attempt-1",
                RunnerCallbackContext {
                    request: Some(relay_request(Some("run-1"))),
                    run_context: None,
                    check_status_board: None,
                    live_checkout: false,
                    executor_binding: Some(RunnerContextExecutorBinding {
                        executor_id: "executor-1".into(),
                        generation: 7,
                        request_id: "request-1".into(),
                        attempt_id: "attempt-1".into(),
                    }),
                    output: output_guard::RelayedOutput::shared(),
                },
            ),
        );
    }

    #[test]
    fn mcp_relay_authorization_binds_context_executor_generation_and_run() {
        let pool = Fleet::default();
        insert_bound_relay_context(&pool);

        assert!(pool
            .authorize_mcp_relay("executor-1", 7, "context", &relay_request(Some("run-1")))
            .is_ok());
        for (executor, generation, context, run_id) in [
            ("executor-2", 7, "context", Some("run-1")),
            ("executor-1", 8, "context", Some("run-1")),
            ("executor-1", 7, "unknown", Some("run-1")),
            ("executor-1", 7, "context", Some("run-2")),
            ("executor-1", 7, "context", None),
        ] {
            assert!(pool
                .authorize_mcp_relay(executor, generation, context, &relay_request(run_id))
                .is_err());
        }

        let mut foreign_thread = relay_request(Some("run-1"));
        foreign_thread.thread_id = Some("thread-owned-by-another-run".into());
        assert!(pool
            .authorize_mcp_relay("executor-1", 7, "context", &foreign_thread)
            .unwrap_err()
            .contains("thread identity"));
    }

    #[test]
    fn terminal_result_revocation_prevents_replay() {
        let pool = Fleet::default();
        insert_bound_relay_context(&pool);
        pool.revoke_runner_contexts_for_request("request-1", "attempt-1");

        assert!(pool
            .authorize_mcp_relay("executor-1", 7, "context", &relay_request(Some("run-1")))
            .is_err());
    }

    #[test]
    fn object_size_exchange_drains_output_while_feeding_clone_sized_input() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        write_file(repo.path(), "base.txt", b"base");
        let commit = commit_all(repo.path(), "base");
        let repository = repo.path().to_str().unwrap();
        let one = inspect_object_sizes(repository, format!("{commit}\n").into_bytes()).unwrap();
        let count = 100_000_u64;
        let object_ids = format!("{commit}\n").repeat(count as usize).into_bytes();

        assert_eq!(
            inspect_object_sizes(repository, object_ids).unwrap(),
            one * count
        );
    }

    /// The acceptance case for the whole issue: a command that outlives the
    /// link that ordered it is delivered to whoever asked for it.
    ///
    /// The executor keeps its cells running across a link gap, so the result of
    /// a check that was sixteen minutes into its run when the socket reset is
    /// still coming. It arrives on the connection that came back, under a new
    /// generation, and the waiter it belongs to has to still be there.
    #[tokio::test]
    async fn work_running_on_a_machine_outlives_the_link_that_ordered_it() {
        let pool = Fleet::default();
        let (sender, _executor) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(sender);
        let key = ("request".to_string(), "attempt".to_string());
        let (result_tx, result_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            key.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: Some("job".into()),
                waiter: result_tx,
            },
        );

        // Nobody decided this. A socket reset is not a statement about the
        // process on the other end of it.
        assert!(pool.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, generation));
        assert!(
            pool.pending.lock().unwrap().contains_key(&key),
            "a link that went away on its own must not resolve the work it was carrying"
        );

        let (sender, _executor) = mpsc::unbounded_channel();
        let reattached = pool.attach_executor(sender);
        assert_ne!(reattached, generation);

        let finished = CellOutcome::Cancelled {
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
        };
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            reattached,
            ExecutorMessage::Result {
                request_id: key.0.clone(),
                attempt_id: key.1.clone(),
                outcome: finished.clone(),
            },
        );
        assert_eq!(
            result_rx.await.unwrap(),
            finished,
            "the result computed across the gap must reach the waiter that asked for it"
        );
    }

    /// The machine's own account of what it is running is what keeps held work
    /// held, and the result then arrives on the new connection like any other.
    #[tokio::test]
    async fn work_a_reattached_machine_says_it_still_has_stays_held() {
        let pool = Fleet::default();
        let (sender, _executor) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(sender);
        let key = ("request".to_string(), "attempt".to_string());
        let (result_tx, result_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            key.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: None,
                waiter: result_tx,
            },
        );
        assert!(pool.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, generation));

        let (sender, _executor) = mpsc::unbounded_channel();
        let reattached = pool.attach_executor(sender);
        let executing: cairn_common::executor_protocol::ExecutingCellRequest =
            serde_json::from_value(serde_json::json!({
                "requestId": key.0,
                "attemptId": key.1,
                "startedAtUnixMs": 0,
                "processIds": [],
            }))
            .unwrap();
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            reattached,
            ExecutorMessage::SnapshotUpdated {
                snapshot: FleetSnapshot {
                    executing_requests: vec![executing],
                    ..FleetSnapshot::default()
                },
                health: ExecutorSubstrateReport::default(),
            },
        );
        assert!(
            pool.pending.lock().unwrap().contains_key(&key),
            "work the machine says it is still running stays held"
        );

        let finished = CellOutcome::Cancelled {
            request_id: key.0.clone(),
            attempt_id: key.1.clone(),
        };
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            reattached,
            ExecutorMessage::Result {
                request_id: key.0.clone(),
                attempt_id: key.1.clone(),
                outcome: finished.clone(),
            },
        );
        assert_eq!(result_rx.await.unwrap(), finished);
    }

    /// Holding work is not hoping for it. A machine that comes back without the
    /// work says so in the first snapshot of the new connection, and that is the
    /// answer — waiting out a watchdog for a result nobody is going to send is
    /// the other way to lose a lane.
    #[tokio::test]
    async fn a_machine_that_reattaches_without_the_work_resolves_it_at_once() {
        let pool = Fleet::default();
        let (sender, _executor) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(sender);
        let key = ("request".to_string(), "attempt".to_string());
        let (result_tx, result_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            key.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: None,
                waiter: result_tx,
            },
        );
        assert!(pool.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, generation));

        let (sender, _executor) = mpsc::unbounded_channel();
        let reattached = pool.attach_executor(sender);

        // A first snapshot without it means the process behind the link was
        // replaced while the link was down.
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            reattached,
            ExecutorMessage::SnapshotUpdated {
                snapshot: FleetSnapshot::default(),
                health: ExecutorSubstrateReport::default(),
            },
        );
        assert!(matches!(
            result_rx.await.unwrap(),
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                ref diagnostic,
            } if diagnostic.contains("no longer running or queued")
        ));
    }

    /// The supervisor is the only thing that knows a machine's process is gone,
    /// because killing it is what the supervisor does. Its verdict resolves held
    /// work immediately rather than leaving it to a clock.
    #[tokio::test]
    async fn the_supervisors_verdict_that_a_machine_is_gone_resolves_its_held_work() {
        let pool = Fleet::default();
        let (sender, _executor) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(sender);
        let key = ("request".to_string(), "attempt".to_string());
        let (result_tx, result_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            key,
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: None,
                waiter: result_tx,
            },
        );
        assert!(pool.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, generation));

        pool.abandon_held_executions(
            COLOCATED_EXECUTOR_ID,
            "the runner recycled this machine's executor: telemetry stalled",
        );
        assert!(matches!(
            result_rx.await.unwrap(),
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                ref diagnostic,
            } if diagnostic.contains("telemetry stalled")
        ));
        assert!(pool.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn colocated_shutdown_is_immediate_retryable_infrastructure() {
        let pool = Fleet::default();
        let (sender, mut executor) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(sender);
        assert!(matches!(
            executor.recv().await,
            Some(ExecutorMessage::DrainModeRequest { enabled: false, .. })
        ));
        let (result_tx, result_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            ("request".into(), "attempt".into()),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: Some("job".into()),
                waiter: result_tx,
            },
        );

        let started = std::time::Instant::now();
        assert!(pool.begin_colocated_shutdown());
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(matches!(
            executor.recv().await,
            Some(ExecutorMessage::Shutdown { .. })
        ));
        assert!(matches!(
            result_rx.await.unwrap(),
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                ref diagnostic,
            } if diagnostic.contains("connection closed")
        ));
        assert_eq!(
            pool.take_disconnect_origin(COLOCATED_EXECUTOR_ID, generation),
            Some(ExecutorDisconnectOrigin::RunnerInitiated)
        );
    }

    #[test]
    fn check_batch_populates_the_existing_learned_estimate_field() {
        let aggregate = aggregate_batch_learned_estimates(&[Some(
            cairn_common::executor_protocol::LearnedResourceEstimate {
                sample_count: 2,
                upper_duration_ms: Some(600),
                upper_peak_rss_bytes: Some(400),
                upper_disk_growth_bytes: None,
            },
        )]);
        let estimate = aggregate.expect("a profiled check must produce a snapshot estimate");
        assert_eq!(estimate.sample_count, 2);
        assert_eq!(estimate.upper_duration_ms, Some(600));
        assert_eq!(estimate.upper_peak_rss_bytes, Some(400));
        assert_eq!(estimate.upper_disk_growth_bytes, None);
    }

    #[test]
    fn check_batch_with_partial_profile_coverage_has_no_estimate() {
        let aggregate = aggregate_batch_learned_estimates(&[
            Some(cairn_common::executor_protocol::LearnedResourceEstimate {
                sample_count: 2,
                upper_duration_ms: Some(600),
                upper_peak_rss_bytes: Some(400),
                upper_disk_growth_bytes: None,
            }),
            None,
        ]);
        assert_eq!(aggregate, None);
    }

    async fn test_orchestrator(config_dir: &Path) -> Orchestrator {
        let local = LocalDb::open(config_dir.join("build-slots.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.to_path_buf(),
        )
        .build()
    }

    fn cache_batch_item(env: Vec<(String, String)>) -> ProcessBatchItem {
        ProcessBatchItem {
            header: "item".into(),
            stream_id: "stream:0".into(),
            execution: ProcessBatchExecution::NativeShell,
            program: String::new(),
            args: vec!["cargo build".into()],
            env,
            stdin: None,
            timeout_ms: 1_000,
            command_resource_identity: None,
            verdict_environment_names: Vec::new(),
        }
    }

    /// The request's env reaches every command in the batch.
    ///
    /// This is the second half of the run-identity seam: `placed_batch_env`
    /// composes what a batch carries, and this is where that vector becomes the
    /// env of each process the executor spawns. A composer that states the run
    /// and a serializer that drops it would leave every agent shell anonymous
    /// while both halves looked correct in isolation (CAIRN-3381), so assert the
    /// crossing rather than either side of it.
    #[test]
    fn a_placed_batch_hands_its_env_to_every_command_it_carries() {
        let env = vec![
            ("CAIRN_RUN_ID".to_string(), "run-7".to_string()),
            (
                "CAIRN_WORKTREE_BRANCH".to_string(),
                "agent/cairn-3381-builder-0".to_string(),
            ),
        ];
        let batch = serialize_process_batch(
            ResolvedRunBatch {
                request: Default::default(),
                run_context: None,
                resolved: vec![
                    (
                        "cairn check run rust-fmt".to_string(),
                        Ok(RunSpec::Shell {
                            command: "cairn check run rust-fmt".to_string(),
                            timeout: None,
                        }),
                    ),
                    (
                        "cairn read cairn:~/plan".to_string(),
                        Ok(RunSpec::Shell {
                            command: "cairn read cairn:~/plan".to_string(),
                            timeout: None,
                        }),
                    ),
                ],
                tool_use_id: "tool-use".to_string(),
                stop_on_error: true,
                originally_sequential: true,
                execution_residency: None,
            },
            &env,
            "runner-context".to_string(),
            ProcessSandboxMode::Unconfined,
        )
        .expect("a shell batch serializes");

        assert_eq!(batch.items.len(), 2);
        for item in &batch.items {
            assert_eq!(
                item.env, env,
                "every command in a placed batch runs with the batch's env"
            );
        }
    }

    /// The SDK travels with a batch that can run JavaScript and with no other,
    /// so shell, Python, and MATLAB work is not taxed with bytes it never reads
    /// -- and so neither way of reaching bun is left out.
    #[test]
    fn the_sdk_travels_with_javascript_batches_by_either_route() {
        let item = |execution, program: &str, args: Vec<&str>| ProcessBatchItem {
            header: "item".into(),
            stream_id: "item".into(),
            execution,
            program: program.into(),
            args: args.into_iter().map(String::from).collect(),
            env: Vec::new(),
            stdin: None,
            timeout_ms: 1_000,
            command_resource_identity: None,
            verdict_environment_names: Vec::new(),
        };

        // An inline TypeScript item resolves to a direct `bun` program.
        assert!(batch_runs_javascript(&[item(
            ProcessBatchExecution::Direct,
            "bun",
            vec!["-e", "import {read} from '@cairn/sdk'"]
        )]));
        // A shell item carries its command line in `args` instead.
        assert!(batch_runs_javascript(&[item(
            ProcessBatchExecution::NativeShell,
            "",
            vec!["bun run scripts/report.ts"]
        )]));
        assert!(batch_runs_javascript(&[item(
            ProcessBatchExecution::NativeShell,
            "",
            vec!["bunx --bun vite build"]
        )]));
        // A batch with no JavaScript in it carries nothing.
        assert!(!batch_runs_javascript(&[
            item(
                ProcessBatchExecution::Direct,
                "python3",
                vec!["-c", "print(1)"]
            ),
            item(
                ProcessBatchExecution::NativeShell,
                "",
                vec!["cargo test --workspace"]
            ),
        ]));
        // A program whose name merely CONTAINS a runtime name is not that
        // runtime, so the direct match is exact rather than a substring.
        assert!(!batch_runs_javascript(&[item(
            ProcessBatchExecution::Direct,
            "nodemon",
            Vec::new()
        )]));
    }

    #[test]
    fn every_item_of_a_cell_batch_is_pointed_at_the_compile_cache() {
        let injected = vec![("SCCACHE_SERVER_PORT".to_string(), "4227".to_string())];
        let batch = with_cell_client_env(
            ProcessBatch {
                files: Vec::new(),
                runtime_packages: Vec::new(),
                sequential: false,
                stop_on_error: true,
                sandbox_mode: ProcessSandboxMode::Unconfined,
                timeout_bases: Vec::new(),
                items: vec![
                    cache_batch_item(Vec::new()),
                    cache_batch_item(vec![(
                        "SCCACHE_SERVER_PORT".to_string(),
                        "4300".to_string(),
                    )]),
                ],
                runner_context_id: None,
                execution_residency: None,
            },
            &injected,
        );

        assert_eq!(batch.items[0].env, injected);
        // A caller that named the variable itself meant it.
        assert_eq!(
            batch.items[1].env,
            vec![("SCCACHE_SERVER_PORT".to_string(), "4300".to_string())]
        );
    }

    /// A cell builds where the daemon's grant reaches; the project's live
    /// checkout does not. That difference is not a lost cache hit — the daemon
    /// runs each cache-miss compile itself, so a live-checkout build pointed at
    /// it fails outright with `Operation not permitted`.
    #[tokio::test]
    async fn configured_build_service_env_is_limited_to_cells() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;

        let cell = cell_build_service_env(
            &orch,
            &RepositoryLocator::ManagedObjects {
                project_id: "p".into(),
                repository_id: "r".into(),
                object_format: cairn_common::executor_protocol::GitObjectFormat::Sha1,
            },
        );
        assert!(
            cell.is_empty(),
            "without an explicit buildServices setting, cells receive no daemon env: {cell:?}"
        );

        let live = cell_build_service_env(
            &orch,
            &RepositoryLocator::ExistingCheckout {
                project_id: "p".into(),
                repository_id: "r".into(),
                absolute_path: "/home/u/projects/cairn".into(),
            },
        );
        assert!(
            live.is_empty(),
            "a batch in the developer's own checkout must keep its own cache: {live:?}"
        );
    }

    #[tokio::test]
    async fn disconnected_lifetime_lease_is_unavailable_not_not_found() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let result = orch
            .fleet
            .operate_residency(
                &orch,
                ResidencyOperation::RefreshCheckout {
                    fence: ResidencyFence {
                        holder: ResidencyHolder::Job {
                            job_id: "retained-on-disconnected-executor".into(),
                        },
                        incarnation_id: "incarnation".into(),
                        cell_epoch: 1,
                    },
                    base_commit: "new-head".into(),
                    require_clean: false,
                },
            )
            .await;

        assert!(matches!(
            result,
            ResidencyResult::Failed {
                kind: ResidencyFailureKind::Unavailable,
                ..
            }
        ));
    }

    /// A deadline fixed before an operation queues measures the queue. The
    /// budget belongs to the operation's own waiting, so it has to start where
    /// the queueing ended — otherwise an acquisition that waited out its turn
    /// arrives already expired and fails for the waiting rather than for
    /// anything about the environment it asked for.
    /// The liveness report is what makes a long wait horizon safe, so it has to
    /// be honest in both directions: a request whose caller was dropped must fall
    /// out of it, and a request with even one live subscriber must stay in.
    ///
    /// Reporting a live request as absent evicts work somebody is waiting for.
    /// Reporting a dead one as present reintroduces the phantom queue slot the
    /// short deadline used to prevent.
    #[test]
    fn the_waiting_report_names_exactly_the_requests_with_a_live_waiter() {
        let pool = Fleet::default();
        let (live_tx, _live_rx) = oneshot::channel::<CellOutcome>();
        let (abandoned_tx, abandoned_rx) = oneshot::channel::<CellOutcome>();
        drop(abandoned_rx);
        {
            let mut pending = pool.pending.lock().unwrap();
            for (request_id, waiter) in [("live", live_tx), ("abandoned", abandoned_tx)] {
                pending.insert(
                    (request_id.to_string(), "attempt".to_string()),
                    PendingResult {
                        executor_id: COLOCATED_EXECUTOR_ID.into(),
                        generation: 1,
                        requesting_job_id: None,
                        waiter,
                    },
                );
            }
        }
        assert_eq!(
            pool.waiting_request_ids(COLOCATED_EXECUTOR_ID, 1),
            vec!["live".to_string()]
        );

        // A queued acquisition is nameable too, by the entry id both sides derive
        // from the holder rather than by a correlation only the runner knows.
        let holder = ResidencyHolder::Job {
            job_id: "job-1".into(),
        };
        let (acquire_tx, _acquire_rx) = oneshot::channel::<ResidencyResult>();
        pool.pending_residency.lock().unwrap().insert(
            "correlation".into(),
            PendingLifetimeResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation: 1,
                waiter: acquire_tx,
                queue_entry_id: Some(residency_queue_entry_id(&holder)),
            },
        );
        assert_eq!(
            pool.waiting_request_ids(COLOCATED_EXECUTOR_ID, 1),
            vec!["live".to_string(), residency_queue_entry_id(&holder)]
        );

        // An empty report is a legitimate statement, not a missing one: it is how
        // an idle runner frees every slot it was holding.
        pool.pending.lock().unwrap().clear();
        pool.pending_residency.lock().unwrap().clear();
        assert!(pool
            .waiting_request_ids(COLOCATED_EXECUTOR_ID, 1)
            .is_empty());
    }

    /// A report tells one link about its own waiters and nobody else's.
    ///
    /// Request ids are not globally unique. An acquisition's queue entry id is
    /// derived from its holder, so two executors serving the same job mint the
    /// identical string — which is exactly the case a report has to get right,
    /// because asserting liveness for a waiter that belongs to another link keeps
    /// an abandoned entry alive for as long as the other link's waiter lives, and
    /// a slot held for nobody is what the liveness window exists to free.
    ///
    /// The generation is part of the scope for the same reason: a waiter recorded
    /// against a link that has since bounced says nothing about its replacement.
    #[test]
    fn a_waiting_report_never_asserts_liveness_for_another_link() {
        let pool = Fleet::default();
        let holder = ResidencyHolder::Job {
            job_id: "shared-job".into(),
        };
        let shared_entry_id = residency_queue_entry_id(&holder);

        // One live waiter on a remote executor, and one on an older generation of
        // the colocated link. Both name the same queue entry id.
        let (remote_tx, _remote_rx) = oneshot::channel::<ResidencyResult>();
        let (stale_tx, _stale_rx) = oneshot::channel::<ResidencyResult>();
        {
            let mut pending = pool.pending_residency.lock().unwrap();
            pending.insert(
                "remote-correlation".into(),
                PendingLifetimeResult {
                    executor_id: "remote-executor".into(),
                    generation: 1,
                    waiter: remote_tx,
                    queue_entry_id: Some(shared_entry_id.clone()),
                },
            );
            pending.insert(
                "stale-correlation".into(),
                PendingLifetimeResult {
                    executor_id: COLOCATED_EXECUTOR_ID.into(),
                    generation: 1,
                    waiter: stale_tx,
                    queue_entry_id: Some(shared_entry_id.clone()),
                },
            );
        }

        assert_eq!(
            pool.waiting_request_ids("remote-executor", 1),
            vec![shared_entry_id.clone()],
            "the link that owns the waiter is told about it"
        );
        assert!(
            pool.waiting_request_ids(COLOCATED_EXECUTOR_ID, 2)
                .is_empty(),
            "a live waiter on another link, and on a bounced generation of this one, must not \
             assert liveness for an entry queued here"
        );
        assert_eq!(
            pool.waiting_request_ids(COLOCATED_EXECUTOR_ID, 1),
            vec![shared_entry_id],
            "the generation that does own a waiter still hears about it"
        );
    }

    #[tokio::test]
    async fn the_shared_wait_bounds_silence_rather_than_duration() {
        let pool = Fleet::default();
        let budget = Duration::from_millis(50);

        // Progress reported throughout: the answer lands six budgets late and is
        // still received.
        let (tx, rx) = oneshot::channel::<u8>();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = tx.send(7);
        });
        assert!(matches!(
            pool.await_bounding_silence(rx, budget, || true).await,
            SilenceWatchdog::Answered(7)
        ));

        // Nothing reports progress: the same budget expires.
        let (_tx, rx) = oneshot::channel::<u8>();
        assert!(matches!(
            pool.await_bounding_silence(rx, budget, || false).await,
            SilenceWatchdog::Silent
        ));

        // A response channel closed without an answer is neither: the caller has
        // to tell "the executor went away" from "the executor went quiet".
        let (tx, rx) = oneshot::channel::<u8>();
        drop(tx);
        assert!(matches!(
            pool.await_bounding_silence(rx, budget, || true).await,
            SilenceWatchdog::Dropped
        ));
    }

    /// Acquisition is a single flight per execution environment: two holders
    /// never wait on each other, one holder's acquirers do, and the map keeps
    /// nothing once the flights that needed it are gone.
    #[tokio::test]
    async fn acquisition_flights_are_per_environment_and_leave_nothing_behind() {
        let pool = Fleet::default();
        let one = ResidencyHolder::Job {
            job_id: "job-one".into(),
        };
        let two = ResidencyHolder::Job {
            job_id: "job-two".into(),
        };

        let first = pool.residency_acquire_flight(&one).await;
        let second =
            tokio::time::timeout(Duration::from_secs(2), pool.residency_acquire_flight(&two))
                .await
                .expect("a second environment's flight must not wait behind the first");
        assert_eq!(pool.residency_acquisitions.lock().unwrap().len(), 2);

        // The same environment is the one thing that does wait: its second
        // acquirer must see the first's route rather than place beside it.
        let contended = pool.clone();
        let rejoining_holder = one.clone();
        let rejoining =
            tokio::spawn(
                async move { contended.residency_acquire_flight(&rejoining_holder).await },
            );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !rejoining.is_finished(),
            "one environment admits one acquisition flight at a time"
        );
        drop(first);
        let rejoined = tokio::time::timeout(Duration::from_secs(2), rejoining)
            .await
            .expect("the waiting flight is handed the gate")
            .unwrap();

        drop(second);
        drop(rejoined);
        assert!(
            pool.residency_acquisitions.lock().unwrap().is_empty(),
            "a spent flight takes its map entry with it"
        );
    }

    /// One job's acquisition must never gate another's. Placement waits for as
    /// long as the chosen executor keeps reporting progress, so a gate spanning
    /// the fleet turns one job's cold start into every other job's stall — and
    /// the stalled jobs then fail on deadlines the stall itself consumed.
    #[tokio::test]
    async fn acquiring_one_environment_never_gates_another() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let wedged = ResidencyHolder::Job {
            job_id: "wedged".into(),
        };
        let held = orch.fleet.residency_acquire_flight(&wedged).await;

        let unrelated = fleet_residency_request(ResidencyHolder::Job {
            job_id: "unrelated".into(),
        });
        let answered = tokio::time::timeout(
            Duration::from_secs(5),
            orch.fleet
                .operate_residency(&orch, ResidencyOperation::Acquire { request: unrelated }),
        )
        .await
        .expect("an unrelated environment's acquisition must not wait behind a wedged one");
        assert!(matches!(answered, ResidencyResult::Failed { .. }));
        drop(held);
    }

    /// A request whose horizon elapsed while a live substrate kept reporting
    /// progress has to say so. Untargeted requests reach here — a job's
    /// execution environment names no executor — so a diagnostic that assumed
    /// a selector panicked at exactly the point the wait had already run out.
    #[tokio::test]
    async fn an_elapsed_horizon_under_a_live_substrate_diagnoses_rather_than_panicking() {
        let pool = Fleet::default();
        pool.declare_colocated_substrate(ExecutorSubstrateState::CapacityBusy);
        let mut request = targeted_request("linux");
        request.executor = None;
        request.wait_horizon_unix_ms = unix_time_ms().saturating_sub(1);

        let outcome = pool
            .select_executor(&request, None, &ActivePlacementPolicy::default_profile())
            .await
            .unwrap_err();

        let CellOutcome::Unavailable { reason, diagnostic } = outcome else {
            panic!("an elapsed horizon leaves no executor to run on");
        };
        assert!(matches!(reason, CellUnavailableReason::NoMatchingExecutor));
        assert!(
            diagnostic.contains("before this request's wait horizon"),
            "{diagnostic}"
        );
    }

    fn result_identity() -> CheckResultIdentity {
        CheckResultIdentity::new("project", "check", "input")
    }

    fn capabilities_with_cores(logical_cores: usize) -> ExecutorCapabilities {
        ExecutorCapabilities {
            os: "macos".into(),
            arch: "aarch64".into(),
            logical_cores,
            concurrency_capacity: None,
            toolchains: Vec::new(),
            projects_served: Vec::new(),
            disk_budget_bytes: None,
            memory_budget_bytes: None,
            sandbox: None,
            toolchain_detection: None,
            build_prerequisites: None,
        }
    }

    #[test]
    fn measured_nested_sandbox_capability_controls_only_sensitive_placement() {
        let selector = ExecutorSelector {
            requires_macos_nested_sandbox_write: true,
            ..ExecutorSelector::default()
        };
        let mut capabilities = capabilities_with_cores(8);

        assert!(
            !matches_sandbox_capability(&capabilities, &selector),
            "an old or incapable macOS executor must not receive sandbox-sensitive work"
        );
        assert!(
            matches_sandbox_capability(&capabilities, &ExecutorSelector::default()),
            "ordinary work remains placeable"
        );

        capabilities.sandbox = Some(cairn_common::executor_protocol::SandboxCapabilityEvidence {
            cairn_sandboxed_marker: false,
            nested_sandbox_write_available: true,
            nested_sandbox_write_detail: "live probe succeeded".into(),
        });
        assert!(matches_sandbox_capability(&capabilities, &selector));
    }

    /// Whole-machine demand is declared as saturation, because a submitter cannot
    /// know which executor will take the work. Resolving it must yield the
    /// executor's ENTIRE admission budget: anything less leaves headroom, and a
    /// lane that declared the whole machine would run beside something else.
    ///
    /// Asserted against the budget function the executor itself admits against,
    /// so the two cannot drift back apart. A single-core host is the case that
    /// caught the drift — its budget floor is 2, not 1.
    #[test]
    fn whole_machine_demand_resolves_to_the_entire_budget_of_its_executor() {
        let whole_machine = ResourceReservation::WHOLE_MACHINE_CONCURRENCY;
        for logical_cores in [0, 1, 2, 8, 12] {
            let capabilities = capabilities_with_cores(logical_cores);
            let resolved = clamp_declared_concurrency(whole_machine, &capabilities);
            assert_eq!(
                resolved,
                capabilities.admission_concurrency_budget(),
                "a whole-machine lane must consume the complete budget of a \
                 {logical_cores}-core executor, leaving no room beside it"
            );
            assert!(
                resolved >= 2,
                "the budget floor is two, so a one-core host cannot resolve to one"
            );
        }
    }

    #[test]
    fn colocated_whole_machine_demand_uses_advertised_effective_capacity() {
        let mut capabilities = capabilities_with_cores(16);
        capabilities.concurrency_capacity = Some(
            cairn_common::executor_protocol::executor_cpu_budget(16, true),
        );
        assert_eq!(
            clamp_declared_concurrency(
                ResourceReservation::WHOLE_MACHINE_CONCURRENCY,
                &capabilities,
            ),
            14
        );
    }

    /// A reservation has two independent halves, and the record must carry both.
    ///
    /// Concurrency comes from the caller and memory/disk come from the learned
    /// profile, so an explanation that names only one of them describes a number
    /// nobody chose. This is the record that made a caller-declared whole-machine
    /// charge read as "learned from 1 observation, fell back because
    /// belowConfidenceFloor" — an explanation for the memory estimate, attached
    /// to a concurrency figure that learning cannot produce (CAIRN-3345).
    #[tokio::test]
    async fn a_declared_concurrency_and_a_learned_lookup_are_both_on_the_record() {
        let db = Arc::new(crate::storage::migrated_test_db("reservation-provenance.db").await);
        let capabilities = capabilities_with_cores(16);
        let mut request = provenance_request();
        request.resource_reservation = ResourceReservation {
            memory_bytes: 0,
            disk_growth_bytes: 0,
            concurrency_units: 1,
            source: ResourceReservationSource::Declared,
        };
        let resolved = ReservationPlan::new(db.clone(), &request, None)
            .resolve_for(
                &request,
                "device",
                "executor",
                &capabilities,
                resource_profiles::DurationContext {
                    class: request.command_class,
                    warmth: ExecutionWarmth::Cold,
                    now_unix_ms: NOW,
                },
            )
            .await;
        assert_eq!(
            resolved.rationale.declared_concurrency_units,
            Some(1),
            "the declared half says who declared it"
        );
        assert_eq!(resolved.reservation.concurrency_units, 1);
        assert_eq!(
            resolved.rationale.fallback,
            Some(cairn_common::executor_protocol::ReservationFallback::NoProfileRecorded),
            "and the learned half still explains itself in its own terms"
        );

        // An exclusive check declares saturation, and that too is declared
        // provenance rather than a conclusion drawn from a thin profile.
        request.resource_reservation.concurrency_units =
            ResourceReservation::WHOLE_MACHINE_CONCURRENCY;
        let exclusive = ReservationPlan::new(db, &request, None)
            .resolve_for(
                &request,
                "device",
                "executor",
                &capabilities,
                resource_profiles::DurationContext {
                    class: request.command_class,
                    warmth: ExecutionWarmth::Cold,
                    now_unix_ms: NOW,
                },
            )
            .await;
        assert_eq!(
            exclusive.reservation.concurrency_units,
            capabilities.admission_concurrency_budget()
        );
        assert_eq!(
            exclusive.rationale.declared_concurrency_units,
            Some(capabilities.admission_concurrency_budget())
        );
    }

    fn provenance_request() -> CellRequest {
        CellRequest {
            request_id: "provenance".into(),
            attempt_id: "attempt".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "p".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "base".into(),
            command: "cargo test --workspace".into(),
            command_class: cairn_common::executor_protocol::CellCommandClass::CargoTest,
            placement_work_class:
                cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            wait_horizon_unix_ms: unix_time_ms() + 5_000,
            waiting_since_unix_ms: 0,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            command_resource_identity: Some(CommandResourceIdentity {
                version: cairn_common::executor_protocol::COMMAND_RESOURCE_IDENTITY_VERSION,
                key: "check:rust".into(),
            }),
            resource_reservation: Default::default(),
            learned_estimate: None,
        }
    }

    /// A modest declaration is carried through untouched; the clamp is a ceiling,
    /// not a rewrite.
    #[test]
    fn a_declaration_within_capacity_is_left_alone() {
        assert_eq!(
            clamp_declared_concurrency(1, &capabilities_with_cores(12)),
            1
        );
    }

    /// An executor that advertises no cores must still yield a runnable
    /// declaration. Zero is rejected by the runtime policy as an invalid
    /// reservation, which would strand every whole-machine check.
    #[test]
    fn a_coreless_advertisement_still_yields_a_runnable_declaration() {
        assert_eq!(
            clamp_declared_concurrency(
                ResourceReservation::WHOLE_MACHINE_CONCURRENCY,
                &capabilities_with_cores(0)
            ),
            2
        );
    }

    fn resolved_process_batch(
        timeouts: Vec<Option<u32>>,
        sequential: bool,
        stop_on_error: bool,
    ) -> ResolvedRunBatch {
        ResolvedRunBatch {
            execution_residency: None,
            request: crate::mcp::types::McpCallbackRequest {
                thread_id: None,
                cwd: "/tmp".into(),
                run_id: None,
                tool: "run".into(),
                payload: serde_json::Value::Null,
                tool_use_id: None,
            },
            run_context: None,
            resolved: timeouts
                .into_iter()
                .enumerate()
                .map(|(index, timeout)| {
                    (
                        format!("command-{index}"),
                        Ok(RunSpec::Shell {
                            command: "true".into(),
                            timeout,
                        }),
                    )
                })
                .collect(),
            tool_use_id: "tool-use".into(),
            stop_on_error,
            originally_sequential: sequential,
        }
    }

    #[test]
    fn executor_level_hold_is_level_readable_for_late_and_concurrent_waiters() {
        let pool = Fleet::default();
        let (tx, _rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(tx);
        let since = unix_time_ms().saturating_sub(10_000);
        let last_progress = since + 5_000;
        let reported = ExecutorSubstrateEvidence::without_queue(
            ExecutorSubstrateState::InitialStorageSweep,
            since,
            last_progress,
        );
        assert!(pool.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            FleetSnapshot {
                substrate_state: Some(reported.clone()),
                ..FleetSnapshot::default()
            },
            ExecutorSubstrateReport::default(),
        ));

        assert_eq!(
            pool.request_substrate_hold(COLOCATED_EXECUTOR_ID, generation, "late-first", "attempt"),
            Some(reported.clone())
        );
        assert_eq!(
            pool.request_substrate_hold(
                COLOCATED_EXECUTOR_ID,
                generation,
                "late-second",
                "attempt"
            ),
            Some(reported)
        );

        let accounting_reported = ExecutorSubstrateEvidence::without_queue(
            ExecutorSubstrateState::StorageAccounting,
            since + 6_000,
            last_progress + 6_000,
        );
        assert!(pool.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            FleetSnapshot {
                substrate_state: Some(accounting_reported.clone()),
                ..FleetSnapshot::default()
            },
            ExecutorSubstrateReport::default(),
        ));
        assert_eq!(
            pool.request_substrate_hold(
                COLOCATED_EXECUTOR_ID,
                generation,
                "accounting-first",
                "attempt",
            ),
            Some(accounting_reported.clone())
        );
        assert_eq!(
            pool.request_substrate_hold(
                COLOCATED_EXECUTOR_ID,
                generation,
                "accounting-second",
                "attempt",
            ),
            Some(accounting_reported)
        );
    }

    /// What the fixture below hands back: the fleet, the executor generation, the
    /// shared result identity, the leader's identity and result channel, and one
    /// identity/channel pair per coalesced follower.
    type ColocatedLeaderLink = (
        Fleet,
        u64,
        CheckResultIdentity,
        RequestIdentity,
        oneshot::Receiver<CellOutcome>,
        Vec<(RequestIdentity, oneshot::Receiver<CoalescedCellOutcome>)>,
    );

    /// Wire a colocated connection whose leader is executing, with `followers`
    /// extra coalesced subscribers attached to the same result identity.
    ///
    /// This is the incident's shape at the seam: an executor that stays attached
    /// and holds a live child process while the runner stops hearing from it.
    fn colocated_link_with_executing_leader(followers: usize) -> ColocatedLeaderLink {
        let pool = Fleet::default();
        let (sender, _executor) = mpsc::unbounded_channel();
        std::mem::forget(_executor);
        let generation = pool.attach_executor(sender);
        let result_identity = result_identity();
        let leader: RequestIdentity = ("leader-request".into(), "leader-attempt".into());

        let (leader_tx, leader_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            leader.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: Some("job".into()),
                waiter: leader_tx,
            },
        );

        let publication = PublicationCoordination::new();
        let mut subscribers = HashMap::new();
        let mut follower_waiters = Vec::new();
        for index in 0..followers {
            let identity: RequestIdentity = (
                format!("follower-request-{index}"),
                "follower-attempt".into(),
            );
            let (tx, rx) = oneshot::channel();
            subscribers.insert(
                identity.clone(),
                CoalescedSubscriber {
                    waiter: tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: None,
                },
            );
            follower_waiters.push((identity, rx));
        }
        {
            let mut registry = pool.in_flight.lock().unwrap();
            registry.by_key.insert(
                result_identity.clone(),
                InFlightExecution {
                    leader: leader.clone(),
                    subscribers,
                    publication,
                },
            );
            for (identity, _) in &follower_waiters {
                registry
                    .subscriber_keys
                    .insert(identity.clone(), result_identity.clone());
            }
        }
        pool.coalesced_leaders
            .lock()
            .unwrap()
            .insert(leader.clone());

        // A live child process is what latches `execution_started` in
        // `await_coalesced`, which is precisely the state that has no deadline
        // left to expire against.
        assert!(pool.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            FleetSnapshot {
                executing_requests: vec![ExecutingCellRequest {
                    command_resource_identity: None,
                    executor_id: COLOCATED_EXECUTOR_ID.into(),
                    cell_id: "cell-1".into(),
                    request_id: leader.0.clone(),
                    attempt_id: leader.1.clone(),
                    owner: None,
                    command_class: Default::default(),
                    command: "bun run test".into(),
                    started_at_unix_ms: unix_time_ms().saturating_sub(600_000),
                    process_ids: vec![4242],
                    priority: Some(CellPriority::ReviewCheck),
                    subscriber_count: followers,
                    resource_reservation: Default::default(),
                    learned_estimate: None,
                }],
                ..FleetSnapshot::default()
            },
            ExecutorSubstrateReport::default(),
        ));

        (
            pool,
            generation,
            result_identity,
            leader,
            leader_rx,
            follower_waiters,
        )
    }

    /// The acceptance criterion "bound on silence, not on duration". A cell that
    /// has been executing for ten minutes is healthy as long as the link keeps
    /// reporting, and the executor's heartbeat guarantees it does.
    #[test]
    fn a_long_running_cell_on_a_reporting_link_is_never_remediated() {
        let (pool, _generation, _result_identity, _leader, _leader_rx, _followers) =
            colocated_link_with_executing_leader(0);

        assert_eq!(
            pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
            LinkRemediation::Healthy
        );
    }

    /// The two thresholds must stay distinct: subscribers see `ConnectedStalled`
    /// for more than a minute before the supervisor takes the link away, which is
    /// what keeps remediation a last resort rather than a hair trigger.
    #[test]
    fn connected_stalled_is_observable_well_before_remediation_fires() {
        let (pool, generation, ..) = colocated_link_with_executing_leader(0);
        let stalled_at = unix_time_ms() + EXECUTOR_PROGRESS_FRESHNESS_MS + 1;

        assert_eq!(
            deadline_evidence(
                stalled_at,
                pool.connections
                    .lock()
                    .unwrap()
                    .get(COLOCATED_EXECUTOR_ID)
                    .unwrap()
                    .last_progress_unix_ms,
                ExecutorSubstrateEvidence::without_queue(
                    ExecutorSubstrateState::ExecutionRunning,
                    stalled_at,
                    stalled_at,
                ),
            )
            .state,
            ExecutorSubstrateState::ConnectedStalled
        );
        assert_eq!(
            pool.assess_colocated_link(stalled_at, LinkStallBounds::default()),
            LinkRemediation::Healthy
        );
        assert!(matches!(
            pool.assess_colocated_link(
                unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
                LinkStallBounds::default(),
            ),
            LinkRemediation::Bounce {
                generation: bounced,
                cause: LinkStallCause::Silence { .. },
                ..
            } if bounced == generation
        ));
    }

    #[test]
    fn an_unattached_colocated_link_is_not_a_link_to_bounce() {
        let pool = Fleet::default();

        assert_eq!(
            pool.assess_colocated_link(
                unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS * 10,
                LinkStallBounds::default(),
            ),
            LinkRemediation::Healthy
        );
        assert!(!pool.abandon_stalled_colocated_link(
            COLOCATED_EXECUTOR_ID,
            1,
            LinkStallCause::Silence {
                silence_ms: 0,
                pump_silence_ms: 0,
            },
        ));
    }

    /// The regression guard for the whole issue: a subscriber whose execution had
    /// already started has no deadline left to expire against, so if remediation
    /// does not resolve it explicitly the link reset converts a stalled link into
    /// an indefinite hold.
    #[tokio::test]
    async fn abandoning_a_stalled_link_resolves_started_subscribers_typed() {
        let (pool, generation, _result_identity, leader, leader_rx, mut followers) =
            colocated_link_with_executing_leader(1);
        let (follower_identity, follower_rx) = followers.pop().unwrap();

        // Deadline already in the past: only the `execution_started` latch is
        // keeping this subscriber alive, exactly as in the incident.
        let waiting = tokio::spawn({
            let pool = pool.clone();
            let follower_identity = follower_identity.clone();
            async move {
                pool.await_coalesced(follower_identity, unix_time_ms(), follower_rx)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            !waiting.is_finished(),
            "a started subscriber must not expire against its own deadline"
        );

        assert!(pool.abandon_stalled_colocated_link(
            COLOCATED_EXECUTOR_ID,
            generation,
            LinkStallCause::Silence {
                silence_ms: EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
                pump_silence_ms: EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
            },
        ));

        // Resolution arrives as a published coalesced outcome rather than a
        // subscriber-side error: the attempt really was decided, and the verdict
        // it was decided with is retryable.
        let outcome = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("a subscriber must never hold indefinitely across a link reset")
            .unwrap();
        let resolved = match outcome {
            Ok(coalesced) => coalesced.outcome,
            Err(outcome) => outcome,
        };
        assert!(
            matches!(
                resolved,
                CellOutcome::Unavailable {
                    reason: CellUnavailableReason::ExecutorUnavailable,
                    ..
                }
            ),
            "a subscriber must resolve retryable across a link reset, got {resolved:?}"
        );

        // The leader's own attempt resolves retryable through the same teardown,
        // and every registry the connection owned is left empty for it.
        assert!(matches!(
            leader_rx.await.unwrap(),
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                ..
            }
        ));
        assert!(pool.pending.lock().unwrap().is_empty());
        assert!(pool.pending_residency.lock().unwrap().is_empty());
        assert!(pool
            .pending_materialization_reads
            .lock()
            .unwrap()
            .is_empty());
        let registry = pool.in_flight.lock().unwrap();
        assert!(registry.by_key.is_empty());
        assert!(registry.subscriber_keys.is_empty());
        drop(registry);
        assert!(!pool.coalesced_leaders.lock().unwrap().contains(&leader));

        // Waiters see a recovering environment rather than a failing one, and the
        // link is free for the supervisor's replacement generation.
        assert_eq!(
            pool.colocated_substrate().unwrap().state,
            ExecutorSubstrateState::SupervisorRespawning
        );
        assert!(placement::substrate_is_working(
            pool.colocated_substrate().unwrap().state
        ));
        assert_eq!(pool.executor_generation(), None);
    }

    /// A verdict formed about one connection must never be executed against its
    /// replacement. Without the generation fence, a bounce that races a reattach
    /// tears down the healthy link that just arrived.
    #[test]
    fn a_stale_verdict_cannot_bounce_the_replacement_link() {
        let (pool, stale_generation, ..) = colocated_link_with_executing_leader(0);
        assert!(matches!(
            pool.assess_colocated_link(
                unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
                LinkStallBounds::default(),
            ),
            LinkRemediation::Bounce { .. }
        ));

        let (sender, replacement_executor) = mpsc::unbounded_channel();
        let replacement = pool.attach_executor(sender);
        assert!(replacement > stale_generation);

        assert!(!pool.abandon_stalled_colocated_link(
            COLOCATED_EXECUTOR_ID,
            stale_generation,
            LinkStallCause::Silence {
                silence_ms: EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
                pump_silence_ms: EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
            },
        ));
        assert_eq!(pool.executor_generation(), Some(replacement));
        assert_eq!(
            pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
            LinkRemediation::Healthy
        );
        drop(replacement_executor);
    }

    /// Put this connection's published facts that far in the past while leaving
    /// the link itself beating normally: the CAIRN-4362 specimen shape.
    fn facts_last_measured(pool: &Fleet, age_ms: u64) {
        let mut connections = pool.connections.lock().unwrap();
        let entry = connections.get_mut(COLOCATED_EXECUTOR_ID).unwrap();
        entry.last_progress_unix_ms = unix_time_ms();
        entry.advertisement.liveness_observed_at_unix_ms =
            Some(unix_time_ms().saturating_sub(age_ms));
    }

    /// The specimen this remediation exists for: three machines refused for
    /// placement for a quarter of an hour while every heartbeat arrived on
    /// time. Silence remediation cannot see it, because there is no silence.
    #[test]
    fn a_link_whose_facts_froze_while_it_kept_beating_is_bounced() {
        let (pool, generation, ..) = colocated_link_with_executing_leader(0);
        facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1);

        assert!(matches!(
            pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
            LinkRemediation::Bounce {
                generation: bounced,
                cause: LinkStallCause::TelemetryFrozen { .. },
                ..
            } if bounced == generation
        ));
    }

    /// Say that this connection's executor has reported which phase its refresh
    /// pass is stuck in, which is what a machine naming its own wedge looks
    /// like on the wire.
    fn refresh_blocked_in(pool: &Fleet, phase: LivenessRefreshPhase) {
        let mut connections = pool.connections.lock().unwrap();
        let entry = connections.get_mut(COLOCATED_EXECUTOR_ID).unwrap();
        let began = unix_time_ms().saturating_sub(LIVENESS_REFRESH_BOUND_MS);
        entry.advertisement.facts_stale_refresh_blocked = Some(LivenessRefreshBlocked {
            phase,
            pass_started_unix_ms: began,
            phase_entered_unix_ms: began,
        });
    }

    /// The bounce is not free: it ends the executor daemon and every batch
    /// running on the machine (CAIRN-4417). Spending that on a machine that is
    /// telling us exactly what it is stuck in trades a reported fault for an
    /// unreported one, so an explained freeze stays a health verdict and the
    /// link keeps its work.
    #[test]
    fn a_fact_freeze_the_executor_explains_is_not_bounced() {
        let (pool, generation, ..) = colocated_link_with_executing_leader(0);
        facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS * 10);
        refresh_blocked_in(&pool, LivenessRefreshPhase::Snapshot);

        assert_eq!(
            pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
            LinkRemediation::Healthy
        );
        assert_eq!(
            pool.managed_telemetry_stall_ms(COLOCATED_EXECUTOR_ID, generation, unix_time_ms()),
            None,
            "a machine that named its wedge has already done what the bounce exists to provoke"
        );
    }

    /// Explaining a freeze is not a way to be trusted through it. The facts are
    /// exactly as stale as they were, every surface that judges freshness still
    /// says so, and only the remedy changed.
    #[test]
    fn an_explained_freeze_is_still_reported_as_stale_telemetry() {
        let (pool, ..) = colocated_link_with_executing_leader(0);
        facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS * 10);
        refresh_blocked_in(&pool, LivenessRefreshPhase::Snapshot);

        let health = pool.executor_health(unix_time_ms()).remove(0);
        assert!(health.telemetry_stale);
        assert_eq!(health.status, ExecutorHealthStatus::Online);
        assert_eq!(
            health
                .advertisement
                .facts_stale_refresh_blocked
                .map(|blocked| blocked.phase),
            Some(LivenessRefreshPhase::Snapshot),
            "the phase has to survive to the surface an operator reads"
        );
    }

    /// And an explanation buys nothing against silence. A machine that reported
    /// a wedge and then stopped answering is gone, whatever its last beat said,
    /// or the marker becomes a way to be unbounceable.
    #[test]
    fn silence_outranks_an_explained_fact_freeze() {
        let (pool, ..) = colocated_link_with_executing_leader(0);
        facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS * 10);
        refresh_blocked_in(&pool, LivenessRefreshPhase::Snapshot);

        assert!(matches!(
            pool.assess_colocated_link(
                unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
                LinkStallBounds::default(),
            ),
            LinkRemediation::Bounce {
                cause: LinkStallCause::Silence { .. },
                ..
            }
        ));
    }

    /// Frozen facts are a health verdict long before they are grounds to take a
    /// machine away. Inside the bound the link keeps its work.
    #[test]
    fn frozen_facts_inside_the_bound_are_a_verdict_rather_than_a_bounce() {
        let (pool, ..) = colocated_link_with_executing_leader(0);
        facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS - 1);

        assert_eq!(
            pool.assess_colocated_link(unix_time_ms(), LinkStallBounds::default()),
            LinkRemediation::Healthy
        );
    }

    /// Silence is the stronger statement, and the two send an operator to
    /// different places. A link nobody has heard from must never be reported as
    /// a machine with a telemetry problem.
    #[test]
    fn silence_outranks_frozen_facts_when_both_are_past_their_bounds() {
        let (pool, ..) = colocated_link_with_executing_leader(0);
        facts_last_measured(&pool, 0);

        assert!(matches!(
            pool.assess_colocated_link(
                unix_time_ms() + EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1,
                LinkStallBounds::default(),
            ),
            LinkRemediation::Bounce {
                cause: LinkStallCause::Silence { .. },
                ..
            }
        ));
    }

    /// A machine that never claimed a recompute clock cannot have one stop.
    /// `attach_executor` advertises none, which is exactly what an executor
    /// built before the split stamp sends.
    #[test]
    fn a_link_that_publishes_no_recompute_stamp_is_never_bounced_for_it() {
        let (pool, ..) = colocated_link_with_executing_leader(0);

        assert_eq!(
            pool.assess_colocated_link(
                unix_time_ms() + EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS * 10,
                LinkStallBounds {
                    silence_ms: u64::MAX,
                    ..LinkStallBounds::default()
                },
            ),
            LinkRemediation::Healthy
        );
    }

    /// What the SSH carrier's poll reads. The age is the executor's own
    /// recompute stamp, so the carrier and the colocated supervisor judge the
    /// same fact.
    #[test]
    fn a_managed_link_reports_how_long_its_facts_have_been_frozen() {
        let (pool, generation, ..) = colocated_link_with_executing_leader(0);
        facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1);

        let stall = pool
            .managed_telemetry_stall_ms(COLOCATED_EXECUTOR_ID, generation, unix_time_ms())
            .expect("an attached link with a frozen recompute stamp reports its age");
        assert!(stall > EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS);
    }

    /// A link that has also gone silent belongs to silence remediation, which
    /// acts sooner and says something different. Reporting a telemetry stall
    /// here would let the carrier recycle it under the wrong name.
    #[test]
    fn a_silent_managed_link_reports_no_telemetry_stall() {
        let (pool, generation, ..) = colocated_link_with_executing_leader(0);
        facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1);

        assert_eq!(
            pool.managed_telemetry_stall_ms(
                COLOCATED_EXECUTOR_ID,
                generation,
                unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
            ),
            None
        );
    }

    /// The same generation fence the silence path carries: a reattach that
    /// lands between assessment and action owns the link now.
    #[test]
    fn a_superseded_generation_reports_no_telemetry_stall() {
        let (pool, generation, ..) = colocated_link_with_executing_leader(0);
        facts_last_measured(&pool, EXECUTOR_TELEMETRY_STALL_REMEDIATION_MS + 1);

        assert_eq!(
            pool.managed_telemetry_stall_ms(COLOCATED_EXECUTOR_ID, generation + 1, unix_time_ms(),),
            None
        );
    }

    /// The wedge arrives on every attached machine inside one window, so the
    /// cure must not be a fleet-wide recycle. One turn at a time, and a
    /// declined turn is simply retaken on a later poll.
    #[test]
    fn telemetry_bounces_are_taken_one_machine_at_a_time() {
        let pool = Fleet::default();
        let now = unix_time_ms();

        assert!(pool.claim_telemetry_stall_bounce(now));
        assert!(!pool.claim_telemetry_stall_bounce(now));
        assert!(!pool.claim_telemetry_stall_bounce(now + TELEMETRY_STALL_BOUNCE_SPACING_MS - 1));
        assert!(pool.claim_telemetry_stall_bounce(now + TELEMETRY_STALL_BOUNCE_SPACING_MS));
    }

    /// Ordering the two bounds is what keeps their verdicts from collapsing:
    /// telemetry remediation must never fire on a link silence would have
    /// already taken, and neither may fire before the health verdict that warns
    /// about it.
    #[test]
    fn stall_bounds_stay_ordered_against_the_health_verdicts_they_follow() {
        let bounds = LinkStallBounds::default();
        assert!(bounds.telemetry_ms > bounds.silence_ms);
        assert!(bounds.telemetry_ms > EXECUTOR_TELEMETRY_STALE_AFTER_MS);
        assert!(bounds.silence_ms > EXECUTOR_PROGRESS_FRESHNESS_MS);
        assert!(
            TELEMETRY_STALL_BOUNCE_SPACING_MS < bounds.telemetry_ms,
            "pacing must not outlast the condition it paces"
        );
    }

    #[test]
    fn deadline_evidence_preserves_fresh_executor_report() {
        let pool = Fleet::default();
        let (tx, _rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(tx);
        let now = unix_time_ms();
        let reported = ExecutorSubstrateEvidence::without_queue(
            ExecutorSubstrateState::InitialStorageSweep,
            now.saturating_sub(10),
            now,
        );
        assert!(pool.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            FleetSnapshot {
                substrate_state: Some(reported.clone()),
                ..FleetSnapshot::default()
            },
            ExecutorSubstrateReport::default(),
        ));

        assert_eq!(
            pool.executor_deadline_evidence(COLOCATED_EXECUTOR_ID, generation),
            reported
        );
    }

    /// The layer that hands the executor its per-item budget. An explicit bound
    /// must survive here byte-for-byte — including one far above the old
    /// ten-minute cap, which is the exact case whose output the socket used to
    /// discard — an omitted bound must become the batch ceiling rather than any
    /// smaller default, and only the ceiling itself may shorten a request.
    #[test]
    fn process_batch_serialization_preserves_millisecond_timeouts_and_flags() {
        let ceiling = cairn_common::run_contract::RUN_BATCH_CEILING_MS;
        let batch = serialize_process_batch(
            resolved_process_batch(
                vec![Some(3_000), None, Some(3_600_000), Some(u32::MAX)],
                true,
                false,
            ),
            &[(
                "CAIRN_WORKTREE_BRANCH".into(),
                "agent/cairn-2929-builder-0".into(),
            )],
            "runner-context".into(),
            ProcessSandboxMode::Confined,
        )
        .unwrap();

        assert_eq!(batch.items[0].timeout_ms, 3_000);
        assert_eq!(batch.items[1].timeout_ms, u64::from(ceiling));
        assert_eq!(batch.items[2].timeout_ms, 3_600_000);
        assert_eq!(batch.items[3].timeout_ms, u64::from(ceiling));
        assert_eq!(
            batch.items[0].env,
            [(
                "CAIRN_WORKTREE_BRANCH".into(),
                "agent/cairn-2929-builder-0".into()
            )]
        );
        assert_eq!(batch.items[0].execution, ProcessBatchExecution::NativeShell);
        assert!(batch.items[0].program.is_empty());
        assert_eq!(batch.items[0].args, ["true"]);
        assert!(batch.sequential);
        assert!(!batch.stop_on_error);
    }

    #[tokio::test]
    async fn malformed_live_setup_config_prevents_executor_submission() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repo");
        std::fs::create_dir_all(repository.join(".cairn")).unwrap();
        std::fs::write(
            repository.join(".cairn/config.yaml"),
            "setupCommands: [unterminated",
        )
        .unwrap();
        let orch = Arc::new(test_orchestrator(temp.path()).await);
        orch.db
            .local
            .execute_script(&format!(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) \
                 VALUES ('p', 'default', 'Project', 'p', '{}', 1, 1);",
                repository.to_string_lossy()
            ))
            .await
            .unwrap();

        let pool = Fleet::default();
        let (sender, mut executor) = mpsc::unbounded_channel();
        pool.attach_executor(sender);
        let request = CellRequest {
            request_id: "malformed-setup".into(),
            attempt_id: "attempt".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "p".into(),
                absolute_path: repository.to_string_lossy().into_owned(),
            },
            base_commit: "base".into(),
            command: "touch command-ran".into(),
            command_class: cairn_common::executor_protocol::CellCommandClass::Other,
            placement_work_class:
                cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            wait_horizon_unix_ms: unix_time_ms() + 5_000,
            waiting_since_unix_ms: 0,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        };

        let outcome = pool.submit_execution(&orch, request, None).await;
        assert!(matches!(
            outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::Preparation,
                ref diagnostic,
            } if diagnostic.contains("load canonical project execution policy")
        ));
        // Attach states the runner's drain posture, so an empty channel is not
        // the available evidence. What this guards is that a refused request
        // reached no executor: nothing beyond that opening frame was sent, and
        // nothing ran.
        assert!(matches!(
            executor.try_recv(),
            Ok(ExecutorMessage::DrainModeRequest { enabled: false, .. })
        ));
        assert!(
            executor.try_recv().is_err(),
            "a request refused during preparation must never reach an executor"
        );
        assert!(!repository.join("command-ran").exists());
    }

    /// A request presented while the supervisor is still attaching waits for
    /// readiness instead of failing, and it waits on the horizon it declared
    /// rather than on one selection rewrote for it.
    #[tokio::test]
    async fn a_request_waits_out_an_attaching_supervisor_within_its_horizon() {
        let pool = Fleet::default();
        pool.declare_colocated_substrate(ExecutorSubstrateState::ProtocolAttaching);
        let attaching = pool.clone();
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            attaching.attach_executor(tx);
            attaching.clear_colocated_substrate();
        });
        let request = CellRequest {
            request_id: "r".into(),
            attempt_id: "a".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repo".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "base".into(),
            command: "true".into(),
            command_class: cairn_common::executor_protocol::CellCommandClass::Other,
            placement_work_class:
                cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            // Long enough to outlast the attach this test stages. A horizon
            // shorter than the work is a requester saying it does not want the
            // result, and nothing rewrites it into one that does.
            wait_horizon_unix_ms: unix_time_ms() + 5_000,
            waiting_since_unix_ms: 0,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        };
        let config = ExecutorConfig {
            project_id: "p".into(),
            project_key: "p".into(),
            default_timeout_seconds: 1,
            setup_commands: Vec::new(),
            populate: Default::default(),
            population_source_root: None,
        };

        let sender = pool
            .wait_for_executor(request.wait_horizon_unix_ms)
            .await
            .unwrap();
        sender
            .send(ExecutorMessage::Submit {
                config,
                request,
                batch: None,
            })
            .unwrap();
        assert!(matches!(
            rx.recv().await,
            Some(ExecutorMessage::DrainModeRequest { enabled: false, .. })
        ));
        assert!(matches!(
            rx.recv().await,
            Some(ExecutorMessage::Submit { .. })
        ));
    }

    #[tokio::test]
    async fn wedged_submission_times_out_cancels_and_does_not_block_the_next_attempt() {
        let pool = Fleet::default();
        let (sender, mut executor) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(sender);

        let first_key = ("request-1".to_string(), "attempt-1".to_string());
        let (first_tx, first_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            first_key.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: None,
                waiter: first_tx,
            },
        );
        let first_guard = SubmitDropGuard {
            pool: pool.clone(),
            request_id: first_key.0.clone(),
            attempt_id: first_key.1.clone(),
            executor_id: COLOCATED_EXECUTOR_ID.into(),
            generation,
            armed: true,
        };
        let outcome = tokio::time::timeout(Duration::from_millis(120), first_rx)
            .await
            .map(|result| result.unwrap())
            .unwrap_or_else(|_| executor_unavailable("executor request watchdog expired".into()));
        drop(first_guard);
        assert!(matches!(
            outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                ..
            }
        ));
        assert!(!pool.pending.lock().unwrap().contains_key(&first_key));
        // Attaching states the runner's drain posture before any work is sent,
        // so the cancellation is not the first frame. Skipping what attach says
        // rather than asserting on frame position keeps this test about the
        // watchdog cancelling a wedged submission, and keeps a future attach
        // frame from turning it red again.
        let cancellation = loop {
            match executor.recv().await {
                Some(ExecutorMessage::DrainModeRequest { .. }) => continue,
                other => break other,
            }
        };
        assert!(matches!(
            cancellation,
            Some(ExecutorMessage::Cancel { request_id, attempt_id })
                if request_id == first_key.0 && attempt_id == first_key.1
        ));

        let second_key = ("request-2".to_string(), "attempt-2".to_string());
        let (second_tx, second_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            second_key.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: None,
                waiter: second_tx,
            },
        );
        let completed = CellOutcome::Cancelled {
            request_id: second_key.0.clone(),
            attempt_id: second_key.1.clone(),
        };
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::Result {
                request_id: second_key.0,
                attempt_id: second_key.1,
                outcome: completed.clone(),
            },
        );
        assert_eq!(second_rx.await.unwrap(), completed);
    }

    #[tokio::test]
    async fn dropped_coalesced_leader_publishes_terminal_outcome() {
        let pool = Fleet::default();
        let leader = ("leader".to_string(), "attempt".to_string());
        let key = result_identity();
        let (tx, rx) = oneshot::channel();
        {
            let mut registry = pool.in_flight.lock().unwrap();
            registry.subscriber_keys.insert(leader.clone(), key.clone());
            registry.by_key.insert(
                key.clone(),
                InFlightExecution {
                    leader: leader.clone(),
                    subscribers: HashMap::from([(
                        leader.clone(),
                        CoalescedSubscriber {
                            waiter: tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: None,
                        },
                    )]),
                    publication: PublicationCoordination::new(),
                },
            );
        }
        pool.coalesced_leaders
            .lock()
            .unwrap()
            .insert(leader.clone());
        pool.cancelled_leaders
            .lock()
            .unwrap()
            .insert(leader.clone());

        drop(CoalescedLeaderCompletionGuard {
            pool: pool.clone(),
            leader: leader.clone(),
            result_identities: vec![key.clone()],
            runner_context_id: None,
            armed: true,
        });

        let outcome = rx.await.unwrap();
        assert!(matches!(
            outcome.outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                ref diagnostic,
            } if diagnostic.contains("leader ended without publishing")
        ));
        assert!(!pool.in_flight.lock().unwrap().by_key.contains_key(&key));
        assert!(!pool.coalesced_leaders.lock().unwrap().contains(&leader));
        assert!(!pool.cancelled_leaders.lock().unwrap().contains(&leader));
        assert!(!pool.preparing_leaders.lock().unwrap().contains_key(&leader));
    }

    #[tokio::test]
    async fn old_leader_completion_cannot_complete_a_recycled_result_key() {
        let pool = Fleet::default();
        let key = result_identity();
        let leader_a = ("leader-a".to_string(), "attempt-a".to_string());
        let subscriber_a = ("subscriber-a".to_string(), "attempt-a".to_string());
        let (tx_a, _rx_a) = oneshot::channel();
        {
            let mut registry = pool.in_flight.lock().unwrap();
            registry
                .subscriber_keys
                .insert(subscriber_a.clone(), key.clone());
            registry.by_key.insert(
                key.clone(),
                InFlightExecution {
                    leader: leader_a.clone(),
                    subscribers: HashMap::from([(
                        subscriber_a.clone(),
                        CoalescedSubscriber {
                            waiter: tx_a,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: None,
                        },
                    )]),
                    publication: PublicationCoordination::new(),
                },
            );
        }
        pool.coalesced_leaders
            .lock()
            .unwrap()
            .insert(leader_a.clone());
        let guard_a = CoalescedLeaderCompletionGuard {
            pool: pool.clone(),
            leader: leader_a,
            result_identities: vec![key.clone()],
            runner_context_id: None,
            armed: true,
        };

        pool.detach_coalesced_subscriber(&subscriber_a);

        let leader_b = ("leader-b".to_string(), "attempt-b".to_string());
        let subscriber_b = ("subscriber-b".to_string(), "attempt-b".to_string());
        let (tx_b, mut rx_b) = oneshot::channel();
        {
            let mut registry = pool.in_flight.lock().unwrap();
            registry
                .subscriber_keys
                .insert(subscriber_b.clone(), key.clone());
            registry.by_key.insert(
                key.clone(),
                InFlightExecution {
                    leader: leader_b.clone(),
                    subscribers: HashMap::from([(
                        subscriber_b.clone(),
                        CoalescedSubscriber {
                            waiter: tx_b,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: None,
                        },
                    )]),
                    publication: PublicationCoordination::new(),
                },
            );
        }
        pool.coalesced_leaders
            .lock()
            .unwrap()
            .insert(leader_b.clone());

        drop(guard_a);

        assert_eq!(
            pool.in_flight
                .lock()
                .unwrap()
                .by_key
                .get(&key)
                .map(|execution| execution.leader.clone()),
            Some(leader_b.clone())
        );
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut rx_b)
            .await
            .is_err());

        let completed = CellOutcome::Cancelled {
            request_id: leader_b.0.clone(),
            attempt_id: leader_b.1.clone(),
        };
        assert!(pool.complete_coalesced_for_leader(&key, &leader_b, completed.clone()));
        assert_eq!(
            rx_b.await.unwrap().outcome,
            restamp_outcome(&completed, &subscriber_b)
        );
    }

    #[tokio::test]
    async fn a_subscriber_waits_out_its_leaders_preparation() {
        let pool = Fleet::default();
        let leader = ("leader".to_string(), "attempt".to_string());
        let subscriber = ("subscriber".to_string(), "attempt".to_string());
        let key = result_identity();
        let (tx, rx) = oneshot::channel();
        {
            let mut registry = pool.in_flight.lock().unwrap();
            registry
                .subscriber_keys
                .insert(subscriber.clone(), key.clone());
            registry.by_key.insert(
                key.clone(),
                InFlightExecution {
                    leader: leader.clone(),
                    subscribers: HashMap::from([(
                        subscriber.clone(),
                        CoalescedSubscriber {
                            waiter: tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: None,
                        },
                    )]),
                    publication: PublicationCoordination::new(),
                },
            );
        }
        let now = unix_time_ms();
        pool.preparing_leaders.lock().unwrap().insert(
            leader,
            LeaderPreparation {
                since_unix_ms: now,
                last_progress_unix_ms: now,
            },
        );
        let completion_pool = pool.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            completion_pool.complete_coalesced_for_leader(
                &key,
                &("leader".to_string(), "attempt".to_string()),
                CellOutcome::Cancelled {
                    request_id: "leader".into(),
                    attempt_id: "attempt".into(),
                },
            );
        });

        let outcome = pool
            .await_coalesced(subscriber, now + 30_000, rx)
            .await
            .expect("a subscriber must outlive its leader's preparation");
        assert!(matches!(outcome.outcome, CellOutcome::Cancelled { .. }));
    }

    #[tokio::test]
    async fn live_command_process_disables_subscriber_acquisition_deadline() {
        let pool = Fleet::default();
        let (sender, mut executor) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(sender);
        let leader = ("leader".to_string(), "attempt".to_string());
        let subscriber = ("subscriber".to_string(), "attempt".to_string());
        let key = result_identity();
        let (pending_tx, _pending_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            leader.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: None,
                waiter: pending_tx,
            },
        );
        let now = unix_time_ms();
        assert!(pool.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            FleetSnapshot {
                executing_requests: vec![ExecutingCellRequest {
                    command_resource_identity: None,
                    executor_id: COLOCATED_EXECUTOR_ID.into(),
                    cell_id: "slot-1".into(),
                    request_id: leader.0.clone(),
                    attempt_id: leader.1.clone(),
                    owner: None,
                    command_class: cairn_common::executor_protocol::CellCommandClass::Other,
                    command: "check".into(),
                    priority: Some(CellPriority::ReviewCheck),
                    subscriber_count: 1,
                    resource_reservation: ResourceReservation::default(),
                    learned_estimate: None,
                    started_at_unix_ms: now,
                    process_ids: vec![42],
                }],
                ..FleetSnapshot::default()
            },
            ExecutorSubstrateReport::default(),
        ));
        pool.connections
            .lock()
            .unwrap()
            .get_mut(COLOCATED_EXECUTOR_ID)
            .unwrap()
            .last_progress_unix_ms = now.saturating_sub(EXECUTOR_PROGRESS_FRESHNESS_MS + 1);
        assert!(
            pool.request_substrate_hold(
                COLOCATED_EXECUTOR_ID,
                generation,
                &leader.0,
                "different-attempt",
            )
            .is_none(),
            "a recycled request ID must not inherit another attempt's running exemption"
        );
        let (tx, rx) = oneshot::channel();
        {
            let mut registry = pool.in_flight.lock().unwrap();
            registry
                .subscriber_keys
                .insert(subscriber.clone(), key.clone());
            registry.by_key.insert(
                key.clone(),
                InFlightExecution {
                    leader: leader.clone(),
                    subscribers: HashMap::from([(
                        subscriber.clone(),
                        CoalescedSubscriber {
                            waiter: tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: None,
                        },
                    )]),
                    publication: PublicationCoordination::new(),
                },
            );
        }
        let completion_pool = pool.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            completion_pool.complete_coalesced_for_leader(
                &key,
                &leader,
                CellOutcome::Cancelled {
                    request_id: leader.0.clone(),
                    attempt_id: leader.1.clone(),
                },
            );
        });

        let outcome = pool
            .await_coalesced(subscriber, now + 5, rx)
            .await
            .expect("a kernel-live command must outlive the acquisition deadline");
        assert!(matches!(outcome.outcome, CellOutcome::Cancelled { .. }));
        // Attaching states the runner's drain posture, so the channel is not
        // empty to begin with. What this guards is that no *cancellation* went
        // to the machine, which is a different claim from the machine hearing
        // nothing at all.
        assert!(matches!(
            executor.try_recv(),
            Ok(ExecutorMessage::DrainModeRequest { enabled: false, .. })
        ));
        assert!(matches!(
            executor.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    /// A cell is not the live checkout, however much it looks like one on disk.
    /// The marker test that used to answer this called every cell the live
    /// checkout, which turned a fenced cell batch's sandbox denial into a
    /// read-only-checkout refusal instead of the fence prompt an `ask` agent's
    /// dial asked for.
    #[test]
    fn a_cell_is_not_the_live_checkout_even_without_a_jj_marker() {
        let cell_on_disk = tempfile::tempdir().unwrap();
        assert!(
            !crate::jj::is_jj_dir(cell_on_disk.path()),
            "a cell checkout carries no jj marker; that is the trap"
        );

        assert!(!runs_in_live_checkout(&RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "p".into(),
            absolute_path: cell_on_disk.path().to_string_lossy().into_owned(),
        }));
        assert!(!runs_in_live_checkout(&RepositoryLocator::ManagedObjects {
            project_id: "p".into(),
            repository_id: "p".into(),
            object_format: cairn_common::executor_protocol::GitObjectFormat::Sha1,
        }));
        assert!(runs_in_live_checkout(
            &RepositoryLocator::ExistingCheckout {
                project_id: "p".into(),
                repository_id: "p".into(),
                absolute_path: "/live".into(),
            }
        ));
    }

    /// The dial is the whole gate. An externally owned live checkout used to take
    /// a read-only profile structurally, so an `allow` agent's ambient batch was
    /// kernel-denied writing anything outside temp and the toolchain caches —
    /// `~/.cairn/jj-stores`, a slot directory, the checkout's own target dir
    /// (CAIRN-3227). The shape a *fenced* batch gets is unchanged.
    #[test]
    fn batch_confinement_follows_the_fence_dial_on_every_repository_shape() {
        use crate::models::Fence;
        let live = RepositoryLocator::ExistingCheckout {
            project_id: "p".into(),
            repository_id: "p".into(),
            absolute_path: "/live".into(),
        };
        let cell = RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "p".into(),
            absolute_path: "/repo".into(),
        };

        for repository in [&live, &cell] {
            assert_eq!(
                Fleet::batch_sandbox_mode(Some(Fence::Allow), repository),
                ProcessSandboxMode::Unconfined,
                "allow means no Cairn-applied policy, on every repository shape"
            );
            assert_eq!(
                Fleet::batch_sandbox_mode(None, repository),
                ProcessSandboxMode::Unconfined,
                "a batch with no run identity is nobody's agent operation"
            );
        }

        assert_eq!(
            Fleet::batch_sandbox_mode(Some(Fence::Ask), &live),
            ProcessSandboxMode::ReadOnlyCheckout
        );
        assert_eq!(
            Fleet::batch_sandbox_mode(Some(Fence::Deny), &live),
            ProcessSandboxMode::ReadOnlyCheckout
        );
        assert_eq!(
            Fleet::batch_sandbox_mode(Some(Fence::Ask), &cell),
            ProcessSandboxMode::Confined
        );
        assert_eq!(
            Fleet::batch_sandbox_mode(Some(Fence::Deny), &cell),
            ProcessSandboxMode::Confined
        );
    }

    #[test]
    fn check_cadence_batches_run_unconfined() {
        // Both cadences submit project-declared commands sourced from the live
        // main checkout, and docs/checks.md specifies they run with host
        // permissions. Confining them nests a macOS sandbox (exit 71) and sets
        // CAIRN_SANDBOXED, which turns the whole review lane structurally red on
        // every branch — CAIRN-3124. Mutation containment belongs to
        // MutationPolicy, not to this knob.
        let batch = Fleet::check_process_batch(Vec::new(), Some("ctx".into()));
        assert_eq!(batch.sandbox_mode, ProcessSandboxMode::Unconfined);
        assert!(batch.sequential, "checks run in deterministic plan order");
        assert!(
            !batch.stop_on_error,
            "one red check must not hide the verdicts behind it"
        );
    }

    #[test]
    fn check_batches_materialize_content_environment() {
        let item = ProcessBatchItem {
            header: "protocol".into(),
            stream_id: "protocol".into(),
            execution: ProcessBatchExecution::Direct,
            program: "true".into(),
            args: Vec::new(),
            env: vec![
                (
                    "CAIRN_CHECK_CHANGED_FILES_CONTENT".into(),
                    "src/lib.rs".into(),
                ),
                (
                    "CAIRN_CHECK_BASE_CONTENT_CONTENT".into(),
                    "{\"src/lib.rs\":\"base\"}".into(),
                ),
                ("PRESERVED".into(), "yes".into()),
            ],
            stdin: None,
            timeout_ms: 1,
            command_resource_identity: None,
            verdict_environment_names: Vec::new(),
        };

        let batch = Fleet::check_process_batch(vec![item], None);
        assert_eq!(batch.files.len(), 2);
        assert_eq!(batch.files[0].env, "CAIRN_CHECK_CHANGED_FILES");
        assert_eq!(batch.files[0].contents, "src/lib.rs");
        assert_eq!(batch.files[1].env, "CAIRN_CHECK_BASE_CONTENT");
        assert_eq!(batch.files[1].contents, "{\"src/lib.rs\":\"base\"}");
        assert_eq!(batch.items[0].env, vec![("PRESERVED".into(), "yes".into())]);
    }

    #[test]
    fn check_batches_discard_declared_duration_kill_budgets() {
        let item = |header: &str, timeout_ms| ProcessBatchItem {
            header: header.into(),
            stream_id: header.into(),
            execution: ProcessBatchExecution::Direct,
            program: "true".into(),
            args: Vec::new(),
            env: Vec::new(),
            stdin: None,
            timeout_ms,
            command_resource_identity: None,
            verdict_environment_names: Vec::new(),
        };

        let batch = Fleet::check_process_batch(
            vec![item("learned", 180_000), item("unmeasured", 900_000)],
            None,
        );

        assert!(
            batch.items.iter().all(|item| item.timeout_ms == 0),
            "configured and learned duration bounds both lose kill authority"
        );
        assert!(batch.timeout_bases.is_empty());
    }

    #[test]
    fn watchdog_covers_preparation_and_full_process_batch_budget() {
        let mut request = targeted_request(std::env::consts::OS);
        request.wait_horizon_unix_ms = unix_time_ms();
        request.timeout_ms = 500;
        let config = ExecutorConfig {
            project_id: request.project_id.clone(),
            project_key: "cairn".into(),
            default_timeout_seconds: 2,
            setup_commands: Vec::new(),
            populate: Default::default(),
            population_source_root: None,
        };
        let batch = ProcessBatch {
            files: Vec::new(),
            runtime_packages: Vec::new(),
            sequential: true,
            stop_on_error: false,
            sandbox_mode: ProcessSandboxMode::Unconfined,
            timeout_bases: Vec::new(),
            items: vec![
                ProcessBatchItem {
                    header: "one".into(),
                    stream_id: "one".into(),
                    execution: ProcessBatchExecution::Direct,
                    program: "true".into(),
                    args: Vec::new(),
                    env: Vec::new(),
                    stdin: None,
                    timeout_ms: 600,
                    command_resource_identity: None,
                    verdict_environment_names: Vec::new(),
                },
                ProcessBatchItem {
                    header: "two".into(),
                    stream_id: "two".into(),
                    execution: ProcessBatchExecution::Direct,
                    program: "true".into(),
                    args: Vec::new(),
                    env: Vec::new(),
                    stdin: None,
                    timeout_ms: 700,
                    command_resource_identity: None,
                    verdict_environment_names: Vec::new(),
                },
            ],
            runner_context_id: None,
            execution_residency: None,
        };

        let budget = request_watchdog_duration(&request, Some(&batch), &config, true);
        assert!(budget >= Duration::from_millis(5_300));
        assert!(budget > Duration::from_millis(request.timeout_ms));
    }

    #[test]
    fn remote_watchdog_reserves_both_managed_object_transfer_deadlines() {
        let mut request = targeted_request(std::env::consts::OS);
        request.wait_horizon_unix_ms = unix_time_ms();
        request.timeout_ms = 60_000;
        let config = ExecutorConfig {
            project_id: request.project_id.clone(),
            project_key: "cairn".into(),
            default_timeout_seconds: 60,
            setup_commands: Vec::new(),
            populate: Default::default(),
            population_source_root: None,
        };

        let colocated = request_watchdog_duration(&request, None, &config, true);
        let remote = request_watchdog_duration(&request, None, &config, false);
        assert_eq!(
            remote - colocated,
            Duration::from_secs(MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS * 2),
            "remote execution must reserve one whole-request deadline for fetch and one for upload"
        );
    }

    /// An item that omits `timeout` now carries the six-hour ceiling as its
    /// budget, so the end-to-end watchdog has to sit above that. If it does not,
    /// it simply becomes the new premature killer — the same shape of defect as
    /// the HTTP socket that used to die first and discard a running suite's
    /// output.
    #[test]
    fn watchdog_sits_above_a_ceiling_length_item_budget() {
        let ceiling =
            Duration::from_millis(u64::from(cairn_common::run_contract::RUN_BATCH_CEILING_MS));
        let mut request = targeted_request(std::env::consts::OS);
        request.wait_horizon_unix_ms = unix_time_ms();
        request.timeout_ms = u64::from(cairn_common::run_contract::RUN_BATCH_CEILING_MS);
        let config = ExecutorConfig {
            project_id: request.project_id.clone(),
            project_key: "cairn".into(),
            default_timeout_seconds: 1_800,
            setup_commands: Vec::new(),
            populate: Default::default(),
            population_source_root: None,
        };
        let item = |header: &str| ProcessBatchItem {
            header: header.into(),
            stream_id: header.into(),
            execution: ProcessBatchExecution::Direct,
            program: "true".into(),
            args: Vec::new(),
            env: Vec::new(),
            stdin: None,
            timeout_ms: u64::from(cairn_common::run_contract::RUN_BATCH_CEILING_MS),
            command_resource_identity: None,
            verdict_environment_names: Vec::new(),
        };
        let batch = |sequential: bool| ProcessBatch {
            files: Vec::new(),
            runtime_packages: Vec::new(),
            sequential,
            stop_on_error: false,
            sandbox_mode: ProcessSandboxMode::Unconfined,
            timeout_bases: Vec::new(),
            items: vec![item("one"), item("two")],
            runner_context_id: None,
            execution_residency: None,
        };

        // Parallel items overlap, so one ceiling-length item is the budget — and
        // the watchdog must still outlive it.
        let parallel = request_watchdog_duration(&request, Some(&batch(false)), &config, true);
        assert!(
            parallel > ceiling,
            "watchdog {parallel:?} must outlive a ceiling-length item"
        );
        // Sequential items add up, so the watchdog grows with them rather than
        // capping at one item's budget.
        let sequential = request_watchdog_duration(&request, Some(&batch(true)), &config, true);
        assert!(
            sequential > parallel,
            "a sequential batch must get more watchdog than a parallel one: {sequential:?} vs {parallel:?}"
        );
    }

    #[tokio::test]
    async fn absent_executor_fails_fast_with_typed_unavailable() {
        let pool = Fleet::default();
        let request = CellRequest {
            request_id: "r".into(),
            attempt_id: "a".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repo".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "base".into(),
            command: "true".into(),
            command_class: cairn_common::executor_protocol::CellCommandClass::Other,
            placement_work_class:
                cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            wait_horizon_unix_ms: unix_time_ms() + 25,
            waiting_since_unix_ms: 0,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        };
        let started = Instant::now();
        let outcome = pool
            .select_executor(&request, None, &ActivePlacementPolicy::default_profile())
            .await
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(20));
        assert!(matches!(
            outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::ExecutorUnavailable,
                ..
            }
        ));
    }

    fn fleet_entry(
        id: &str,
        os: &str,
        load: usize,
        warm: &[&str],
    ) -> (String, ExecutorConnectionState) {
        let (sender, receiver) = mpsc::unbounded_channel();
        std::mem::forget(receiver);
        let identity = ExecutorIdentity {
            device_id: format!("device-{id}"),
            executor_id: id.into(),
            display_name: id.into(),
        };
        (
            id.into(),
            ExecutorConnectionState {
                drained: false,
                identity: identity.clone(),
                advertisement: ExecutorAdvertisement {
                    identity,
                    capabilities: ExecutorCapabilities {
                        os: os.into(),
                        arch: "x86_64".into(),
                        logical_cores: 8,
                        concurrency_capacity: None,
                        toolchains: vec!["rust".into()],
                        projects_served: vec!["p".into()],
                        disk_budget_bytes: None,
                        memory_budget_bytes: None,
                        sandbox: None,
                        toolchain_detection: None,
                        build_prerequisites: None,
                    },
                    current_load: load,
                    warm_roots: warm
                        .iter()
                        .map(|value| VerifiedWarmRoot {
                            repository: RepositoryLocator::ManagedObjects {
                                project_id: "p".into(),
                                repository_id: "repo".into(),
                                object_format: GitObjectFormat::Sha1,
                            }
                            .identity(),
                            commit: (*value).into(),
                        })
                        .collect(),
                    observed_at_unix_ms: 1,
                    liveness_observed_at_unix_ms: None,
                    facts_stale_refresh_blocked: None,
                },
                generation: 1,
                sender,
                snapshot: FleetSnapshot::default(),
                last_progress_unix_ms: NOW,
                health: ExecutorSubstrateReport {
                    applied_policy: cairn_common::executor_protocol::ExecutorRuntimePolicy {
                        maximum_queue_depth: 64,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                executor_build_id: None,
                colocated: id == COLOCATED_EXECUTOR_ID,
                pump_tick: Arc::new(AtomicU64::new(1)),
            },
        )
    }

    /// A machine with complete, fresh placement readings.
    fn measured(
        entry: &mut ExecutorConnectionState,
        cpu_utilization: f64,
        available_memory_bytes: u64,
        free_volume_bytes: u64,
    ) {
        entry.health.machine = cairn_common::executor_protocol::MachineTelemetry {
            cpu: Measurement::measured(
                NOW,
                cairn_common::executor_protocol::CpuPressure {
                    utilization: cpu_utilization,
                    user: cpu_utilization,
                    system: 0.0,
                    logical_cores: 8,
                },
            ),
            memory: Measurement::measured(
                NOW,
                cairn_common::executor_protocol::MachineMemory {
                    total_bytes: 64 * 1024 * 1024 * 1024,
                    available_bytes: available_memory_bytes,
                },
            ),
            volume: Measurement::measured(
                NOW,
                cairn_common::executor_protocol::MachineVolume {
                    total_bytes: 1024 * 1024 * 1024 * 1024,
                    free_bytes: free_volume_bytes,
                },
            ),
            ..Default::default()
        };
    }

    fn spillable_request() -> CellRequest {
        let mut request = targeted_request("linux");
        request.executor = None;
        request.placement_mobility = PlacementMobility::SpillEligible;
        request
    }

    #[test]
    fn hermetic_shadow_lifts_all_three_execution_home_gates() {
        let mut request = targeted_request("linux");
        request.executor = None;
        request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
        request.placement_mobility = PlacementMobility::PinnedOrColocated;
        request.repository = RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "repo".into(),
            absolute_path: "/repo".into(),
        };

        let shadow = hermetic_shadow_request(&request).expect("a pinned request has a shadow");

        assert_eq!(shadow.pinned_executor_id, None);
        assert_eq!(shadow.placement_mobility, PlacementMobility::SpillEligible);
        assert!(matches!(
            shadow.repository,
            RepositoryLocator::ManagedObjects { .. }
        ));
        assert_eq!(shadow.base_commit, request.base_commit);
    }

    #[tokio::test]
    async fn production_placement_records_shadow_without_changing_dispatch() {
        let pool = Fleet::default();
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(&mut local, 0.95, 8_000_000_000, 8_000_000_000);
        let (remote_id, mut remote) = fleet_entry("remote", "linux", 0, &[]);
        measured(&mut remote, 0.01, 8_000_000_000, 8_000_000_000);
        *pool.connections.lock().unwrap() = HashMap::from([(local_id, local), (remote_id, remote)]);
        let mut request = spillable_request();
        request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
        request.placement_mobility = PlacementMobility::PinnedOrColocated;
        request.repository = RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "repo".into(),
            absolute_path: "/repo".into(),
        };

        let placement = pool
            .select_executor_once_with(
                &request,
                None,
                &ActivePlacementPolicy::default_profile(),
                |_, _| SyncCost::Known(0),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(placement.selected.executor_id, COLOCATED_EXECUTOR_ID);
        assert_eq!(
            placement.decision.unpinned_winner.unwrap().executor_id,
            "remote"
        );
    }

    #[tokio::test]
    async fn unavailable_home_refusal_keeps_the_hermetic_winner() {
        let pool = Fleet::default();
        let (remote_id, mut remote) = fleet_entry("remote", "linux", 0, &[]);
        measured(&mut remote, 0.01, 8_000_000_000, 8_000_000_000);
        *pool.connections.lock().unwrap() = HashMap::from([(remote_id, remote)]);
        let mut request = spillable_request();
        request.pinned_executor_id = Some("missing-home".into());
        request.placement_mobility = PlacementMobility::PinnedOrColocated;
        request.repository = RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "repo".into(),
            absolute_path: "/repo".into(),
        };

        let refusal = pool
            .select_executor_once_with(
                &request,
                None,
                &ActivePlacementPolicy::default_profile(),
                |_, _| SyncCost::Known(0),
            )
            .await
            .unwrap_err();

        assert_eq!(
            refusal.decision.unpinned_winner.unwrap().executor_id,
            "remote"
        );
    }

    #[test]
    fn unpinned_shadow_can_choose_a_remote_machine_without_changing_dispatch() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(&mut local, 0.95, 8_000_000_000, 8_000_000_000);
        let (remote_id, mut remote) = fleet_entry("remote", "linux", 0, &[]);
        measured(&mut remote, 0.01, 8_000_000_000, 8_000_000_000);
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);
        let mut request = spillable_request();
        request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
        request.placement_mobility = PlacementMobility::PinnedOrColocated;
        request.repository = RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "repo".into(),
            absolute_path: "/repo".into(),
        };

        let actual = place(&connections, &request).unwrap();
        let shadow = place(&connections, &hermetic_shadow_request(&request).unwrap()).unwrap();

        assert_eq!(chosen(&actual).executor_id, COLOCATED_EXECUTOR_ID);
        assert_eq!(chosen(&shadow).executor_id, "remote");
    }

    fn place(
        connections: &HashMap<String, ExecutorConnectionState>,
        request: &CellRequest,
    ) -> Result<PlacementDraft, String> {
        choose_executor_with(
            connections,
            request,
            &HashMap::new(),
            |_, _| SyncCost::Known(0),
            NOW,
        )
    }

    fn place_with_profile(
        connections: &HashMap<String, ExecutorConnectionState>,
        request: &CellRequest,
        name: &str,
        profile: PlacementProfile,
    ) -> Result<PlacementDraft, String> {
        choose_executor_with_policy(
            connections,
            request,
            &HashMap::new(),
            &ActivePlacementPolicy {
                name: name.to_string(),
                profile,
            },
            |_, _| SyncCost::Known(0),
            NOW,
        )
    }

    #[test]
    fn remote_first_prefers_a_comparable_remote_and_records_policy_evidence() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        let (remote_id, mut remote) = fleet_entry("remote", "linux", 0, &[]);
        measured(&mut local, 0.1, 8_000_000_000, 8_000_000_000);
        measured(&mut remote, 0.1, 8_000_000_000, 8_000_000_000);
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);
        let request = spillable_request();
        let profile = profile_with_routes(30, |_| PlacementStance::RemoteFirst);

        let draft = place_with_profile(&connections, &request, "interactive", profile).unwrap();

        assert_eq!(chosen(&draft).executor_id, "remote");
        assert_eq!(draft.policy.profile_name, "interactive");
        assert!(draft.policy.changed_earliest_winner);
    }

    #[test]
    fn explicit_selector_leaves_profile_preference_no_choice() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        let (remote_id, mut remote) = fleet_entry("remote", "linux", 0, &[]);
        measured(&mut local, 0.1, 8_000_000_000, 8_000_000_000);
        measured(&mut remote, 0.1, 8_000_000_000, 8_000_000_000);
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);
        let mut request = spillable_request();
        request.placement_work_class = PlacementWorkClass::AgentSessions;
        request.executor = Some(ExecutorSelector {
            name: Some("remote".into()),
            ..ExecutorSelector::default()
        });
        let mut profile = profile_with_routes(30, |_| PlacementStance::LocalFirst);
        profile.machine_priority = vec![LOCAL_EXECUTOR_NAME.into(), "remote".into()];

        let draft = place_with_profile(&connections, &request, "interactive", profile).unwrap();

        assert_eq!(chosen(&draft).executor_id, "remote");
        assert!(!draft.policy.changed_earliest_winner);
    }

    #[test]
    fn machine_priority_breaks_comparable_candidates_without_crossing_hard_gates() {
        let (a_id, mut a) = fleet_entry("a", "linux", 0, &[]);
        let (b_id, mut b) = fleet_entry("b", "linux", 0, &[]);
        measured(&mut a, 0.1, 8_000_000_000, 8_000_000_000);
        measured(&mut b, 0.1, 8_000_000_000, 8_000_000_000);
        let connections = HashMap::from([(a_id, a), (b_id, b)]);
        let mut profile = profile_with_routes(0, |_| PlacementStance::Any);
        profile.machine_priority = vec!["b".into(), "a".into()];

        let draft =
            place_with_profile(&connections, &spillable_request(), "custom", profile).unwrap();

        assert_eq!(chosen(&draft).executor_id, "b");
    }

    /// The labeled class prior, for a fixture whose reservation is the point and
    /// whose duration is not.
    fn test_prior_duration() -> DurationEstimate {
        resource_profiles::unmeasured_duration(
            CellCommandClass::Other,
            &ReservationPlan::context_for(
                "device",
                "executor",
                &ExecutorCapabilities {
                    os: "linux".into(),
                    arch: "x86_64".into(),
                    logical_cores: 8,
                    concurrency_capacity: None,
                    toolchains: Vec::new(),
                    projects_served: Vec::new(),
                    disk_budget_bytes: None,
                    memory_budget_bytes: None,
                    sandbox: None,
                    toolchain_detection: None,
                    build_prerequisites: None,
                },
            ),
            None,
            ExecutionWarmth::Cold,
            DurationFallback::NoProfileStore,
        )
    }

    /// A missing queue reading is not a fast queue. Even when that machine's run
    /// prior is numerically lower, it cannot outrank a candidate whose complete
    /// queue-plus-run prediction is known.
    #[test]
    fn an_unknown_queue_never_wins_by_looking_empty() {
        let (unknown_id, mut unknown) = fleet_entry("unknown-queue", "linux", 0, &[]);
        let (known_id, mut known) = fleet_entry("known-queue", "linux", 0, &[]);
        measured(&mut unknown, 0.1, 8_000_000_000, 8_000_000_000);
        measured(&mut known, 0.1, 8_000_000_000, 8_000_000_000);

        let warmth = CacheWarmthEvidence::Observed {
            warmth: ExecutionWarmth::Cold,
            observed_at_unix_ms: NOW,
        };
        let mut short_run = test_prior_duration();
        short_run.predicted_ms = 10_000;
        let mut longer_run = test_prior_duration();
        longer_run.predicted_ms = 15_000;
        let unknown_prediction = placement_prediction(
            &unknown,
            warmth.clone(),
            QueueForecast::Unknown {
                reason: QueueUnknownReason::NoAdmissionCapacity,
            },
            short_run,
            SyncCost::Known(0),
        );
        let known_prediction = placement_prediction(
            &known,
            warmth,
            QueueForecast::Forecast {
                predicted_ms: 5_000,
                requests_ahead: 1,
                running_ahead: 0,
                fully_measured: true,
                observed_at_unix_ms: NOW,
            },
            longer_run,
            SyncCost::Known(0),
        );
        let scored = vec![
            ScoredCandidate::new(
                &unknown,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                unknown_prediction,
                NOW,
            ),
            ScoredCandidate::new(
                &known,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                known_prediction,
                NOW,
            ),
        ];

        let request = spillable_request();
        let (winner, _, _, _, _) = rank_candidates(
            &scored,
            false,
            true,
            &request,
            &ActivePlacementPolicy::default_profile(),
            &ExplorationState::default(),
        );
        assert_eq!(
            winner
                .expect("one measurable candidate wins")
                .entry
                .identity
                .executor_id,
            known_id
        );
        assert_ne!(unknown_id, known_id);
    }

    #[test]
    fn contention_can_make_an_idle_slower_machine_win() {
        let (_, mut fast_busy) = fleet_entry("fast-busy", "linux", 0, &[]);
        let (idle_id, mut slower_idle) = fleet_entry("slower-idle", "linux", 0, &[]);
        measured(&mut fast_busy, 0.1, 8_000_000_000, 8_000_000_000);
        measured(&mut slower_idle, 0.1, 8_000_000_000, 8_000_000_000);
        let warmth = CacheWarmthEvidence::Observed {
            warmth: ExecutionWarmth::Cold,
            observed_at_unix_ms: NOW,
        };
        let queue = QueueForecast::Forecast {
            predicted_ms: 0,
            requests_ahead: 0,
            running_ahead: 0,
            fully_measured: true,
            observed_at_unix_ms: NOW,
        };
        let mut fast_run = test_prior_duration();
        fast_run.predicted_ms = 5_000;
        let mut slower_run = test_prior_duration();
        slower_run.predicted_ms = 9_000;
        let busy = ContentionEstimate {
            co_resident_compile_jobs: 2,
            co_resident_light_jobs: 0,
            multiplier_millis: 2_200,
            source: cairn_common::executor_protocol::ContentionEvidence::Machine,
            sample_count: 12,
            fallback: None,
        };
        let idle = ContentionEstimate {
            co_resident_compile_jobs: 0,
            co_resident_light_jobs: 0,
            multiplier_millis: 1_000,
            source: cairn_common::executor_protocol::ContentionEvidence::Machine,
            sample_count: 12,
            fallback: None,
        };
        let predictions = [
            placement_prediction_with_contention(
                &fast_busy,
                warmth.clone(),
                queue.clone(),
                fast_run,
                busy,
                SyncCost::Known(0),
            ),
            placement_prediction_with_contention(
                &slower_idle,
                warmth,
                queue,
                slower_run,
                idle,
                SyncCost::Known(0),
            ),
        ];
        assert_eq!(predictions[0].predicted_verdict_ms, 11_000);
        assert_eq!(predictions[1].predicted_verdict_ms, 9_000);
        let scored = vec![
            ScoredCandidate::new(
                &fast_busy,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                predictions[0].clone(),
                NOW,
            ),
            ScoredCandidate::new(
                &slower_idle,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                predictions[1].clone(),
                NOW,
            ),
        ];
        let request = spillable_request();
        let (winner, _, _, _, _) = rank_candidates(
            &scored,
            false,
            true,
            &request,
            &ActivePlacementPolicy::default_profile(),
            &ExplorationState::default(),
        );
        assert_eq!(winner.unwrap().entry.identity.executor_id, idle_id);
    }

    #[test]
    fn idle_unmeasured_capacity_bootstraps_a_profile_before_pure_exploitation() {
        let (favorite_id, mut favorite) = fleet_entry("favorite", "linux", 0, &[]);
        let (fresh_id, mut fresh) = fleet_entry("fresh", "linux", 0, &[]);
        measured(&mut favorite, 0.1, 8_000_000_000, 8_000_000_000);
        measured(&mut fresh, 0.1, 8_000_000_000, 8_000_000_000);

        let warmth = CacheWarmthEvidence::Observed {
            warmth: ExecutionWarmth::Cold,
            observed_at_unix_ms: NOW,
        };
        let mut learned = test_prior_duration();
        learned.predicted_ms = 10_000;
        learned.source = cairn_common::executor_protocol::DurationEvidence::Learned;
        learned.sample_count = 20;
        learned.fallback = None;
        let favorite_prediction = placement_prediction(
            &favorite,
            warmth.clone(),
            QueueForecast::Forecast {
                predicted_ms: 0,
                requests_ahead: 0,
                running_ahead: 0,
                fully_measured: true,
                observed_at_unix_ms: NOW,
            },
            learned,
            SyncCost::Known(0),
        );
        let mut unmeasured = test_prior_duration();
        unmeasured.profile_key = Some("check:rust".into());
        unmeasured.fallback = Some(DurationFallback::NoProfileRecorded);
        let fresh_prediction = placement_prediction(
            &fresh,
            warmth,
            QueueForecast::Forecast {
                predicted_ms: 0,
                requests_ahead: 0,
                running_ahead: 0,
                fully_measured: true,
                observed_at_unix_ms: NOW,
            },
            unmeasured,
            SyncCost::Known(0),
        );
        let scored = vec![
            ScoredCandidate::new(
                &favorite,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                favorite_prediction,
                NOW,
            ),
            ScoredCandidate::new(
                &fresh,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                fresh_prediction,
                NOW,
            ),
        ];
        let request = spillable_request();

        let (winner, _, changed, exploring, _) = rank_candidates(
            &scored,
            false,
            true,
            &request,
            &ActivePlacementPolicy::default_profile(),
            &ExplorationState::default(),
        );

        assert_eq!(winner.unwrap().entry.identity.executor_id, fresh_id);
        assert_ne!(favorite_id, fresh_id);
        assert!(changed);
        assert!(exploring);

        let mut history = ExplorationState::default();
        history.record(Some(&fresh_id));
        let (winner, _, _, exploring, _) = rank_candidates(
            &scored,
            false,
            true,
            &request,
            &ActivePlacementPolicy::default_profile(),
            &history,
        );
        assert_eq!(winner.unwrap().entry.identity.executor_id, favorite_id);
        assert!(!exploring, "the next three waves preserve exploitation");

        for _ in 0..3 {
            history.record(None);
        }
        let (winner, _, _, exploring, _) = rank_candidates(
            &scored,
            false,
            true,
            &request,
            &ActivePlacementPolicy::default_profile(),
            &history,
        );
        assert_eq!(winner.unwrap().entry.identity.executor_id, fresh_id);
        assert!(exploring, "the fourth wave restores the exploration budget");
    }

    #[test]
    fn equal_predictions_rotate_while_a_faster_prediction_remains_authoritative() {
        let (first_id, mut first) = fleet_entry("first", "linux", 0, &[]);
        let (second_id, mut second) = fleet_entry("second", "linux", 0, &[]);
        measured(&mut first, 0.1, 8_000_000_000, 8_000_000_000);
        measured(&mut second, 0.1, 8_000_000_000, 8_000_000_000);
        let warmth = CacheWarmthEvidence::Observed {
            warmth: ExecutionWarmth::Cold,
            observed_at_unix_ms: NOW,
        };
        let queue = QueueForecast::Forecast {
            predicted_ms: 0,
            requests_ahead: 0,
            running_ahead: 0,
            fully_measured: true,
            observed_at_unix_ms: NOW,
        };
        let mut learned = test_prior_duration();
        learned.source = cairn_common::executor_protocol::DurationEvidence::Learned;
        learned.sample_count = 20;
        learned.fallback = None;
        let predictions = [
            placement_prediction(
                &first,
                warmth.clone(),
                queue.clone(),
                learned.clone(),
                SyncCost::Known(0),
            ),
            placement_prediction(&second, warmth, queue, learned, SyncCost::Known(0)),
        ];
        let scored = vec![
            ScoredCandidate::new(
                &first,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                predictions[0].clone(),
                NOW,
            ),
            ScoredCandidate::new(
                &second,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                predictions[1].clone(),
                NOW,
            ),
        ];
        let request = spillable_request();
        let policy = ActivePlacementPolicy::default_profile();
        let mut history = ExplorationState {
            decisions_since_exploration: 0,
            ..Default::default()
        };

        let (winner, _, _, _, tie_break) =
            rank_candidates(&scored, false, true, &request, &policy, &history);
        let first_winner = winner.unwrap().entry.identity.executor_id.clone();
        assert_eq!(
            tie_break.unwrap().deciding_reason,
            "stable executor identity seeded recent-selection history"
        );
        history.record_selection(&first_winner);

        let (winner, _, _, _, tie_break) =
            rank_candidates(&scored, false, true, &request, &policy, &history);
        let second_winner = winner.unwrap().entry.identity.executor_id.clone();
        assert_ne!(first_winner, second_winner);
        assert_eq!(
            tie_break.unwrap().deciding_reason,
            "least recently selected among candidates equal on every earlier placement key"
        );
        assert!([first_id.clone(), second_id.clone()].contains(&second_winner));

        let mut faster = predictions[1].clone();
        faster.predicted_verdict_ms = predictions[0].predicted_verdict_ms - 1;
        faster.adjusted_run_ms = faster.adjusted_run_ms.saturating_sub(1);
        let faster_scored = vec![
            ScoredCandidate::new(
                &first,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                predictions[0].clone(),
                NOW,
            ),
            ScoredCandidate::new(
                &second,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                faster,
                NOW,
            ),
        ];
        history.record_selection(&second_id);
        let (winner, _, _, _, tie_break) =
            rank_candidates(&faster_scored, false, true, &request, &policy, &history);
        assert_eq!(winner.unwrap().entry.identity.executor_id, second_id);
        assert!(tie_break.is_none());

        let mut faster_unmeasured = predictions[0].clone();
        faster_unmeasured.run.profile_key = Some("check:rust".into());
        faster_unmeasured.run.fallback = Some(DurationFallback::NoProfileRecorded);
        let mut slower_unmeasured = predictions[1].clone();
        slower_unmeasured.run.profile_key = Some("check:rust".into());
        slower_unmeasured.run.fallback = Some(DurationFallback::NoProfileRecorded);
        slower_unmeasured.predicted_verdict_ms += 10_000;
        let bootstrap_scored = vec![
            ScoredCandidate::new(
                &first,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                faster_unmeasured,
                NOW,
            ),
            ScoredCandidate::new(
                &second,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                slower_unmeasured,
                NOW,
            ),
        ];
        let mut bootstrap_history = ExplorationState::default();
        bootstrap_history.record(Some(&first_id));
        for _ in 0..3 {
            bootstrap_history.record(None);
        }
        let (winner, _, _, exploring, evidence) = rank_candidates(
            &bootstrap_scored,
            false,
            true,
            &request,
            &policy,
            &bootstrap_history,
        );
        assert_eq!(
            winner.unwrap().entry.identity.executor_id,
            second_id,
            "unexplored bootstrap capacity gets its turn even with a slower prior"
        );
        assert!(exploring);
        let evidence = evidence.unwrap();
        assert_eq!(
            evidence.deciding_reason,
            "least recently explored bootstrap candidate"
        );
        assert_eq!(evidence.candidates.len(), 2);
    }

    #[test]
    fn other_work_classes_cannot_evict_an_exploration_cooldown() {
        let mut states = HashMap::<PlacementWorkClass, ExplorationState>::new();
        states
            .entry(PlacementWorkClass::ReviewChecks)
            .or_default()
            .record(Some("fresh"));

        for _ in 0..(RECENT_PLACEMENT_DECISIONS * 2) {
            states
                .entry(PlacementWorkClass::AgentSessions)
                .or_default()
                .record(None);
        }

        let review = states.get(&PlacementWorkClass::ReviewChecks).unwrap();
        assert_eq!(review.decisions_since_exploration, 0);
        assert_eq!(review.explored_executors, VecDeque::from(["fresh".into()]));
    }

    #[test]
    fn bootstrap_never_queues_behind_work_on_an_unmeasured_machine() {
        let (favorite_id, mut favorite) = fleet_entry("favorite", "linux", 0, &[]);
        let (_fresh_id, mut fresh) = fleet_entry("fresh", "linux", 0, &[]);
        measured(&mut favorite, 0.1, 8_000_000_000, 8_000_000_000);
        measured(&mut fresh, 0.1, 8_000_000_000, 8_000_000_000);
        let warmth = CacheWarmthEvidence::Observed {
            warmth: ExecutionWarmth::Cold,
            observed_at_unix_ms: NOW,
        };
        let mut learned = test_prior_duration();
        learned.predicted_ms = 10_000;
        learned.source = cairn_common::executor_protocol::DurationEvidence::Learned;
        learned.sample_count = 20;
        learned.fallback = None;
        let favorite_prediction = placement_prediction(
            &favorite,
            warmth.clone(),
            QueueForecast::Forecast {
                predicted_ms: 0,
                requests_ahead: 0,
                running_ahead: 0,
                fully_measured: true,
                observed_at_unix_ms: NOW,
            },
            learned,
            SyncCost::Known(0),
        );
        let mut unmeasured = test_prior_duration();
        unmeasured.profile_key = Some("check:rust".into());
        unmeasured.fallback = Some(DurationFallback::NoProfileRecorded);
        let fresh_prediction = placement_prediction(
            &fresh,
            warmth,
            QueueForecast::Forecast {
                predicted_ms: 1_000,
                requests_ahead: 1,
                running_ahead: 0,
                fully_measured: true,
                observed_at_unix_ms: NOW,
            },
            unmeasured,
            SyncCost::Known(0),
        );
        let scored = vec![
            ScoredCandidate::new(
                &favorite,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                favorite_prediction,
                NOW,
            ),
            ScoredCandidate::new(
                &fresh,
                SyncCost::Known(0),
                None,
                &ResourceReservation::default(),
                fresh_prediction,
                NOW,
            ),
        ];
        let request = spillable_request();

        let (winner, _, _, exploring, _) = rank_candidates(
            &scored,
            false,
            true,
            &request,
            &ActivePlacementPolicy::default_profile(),
            &ExplorationState::default(),
        );

        assert_eq!(winner.unwrap().entry.identity.executor_id, favorite_id);
        assert!(!exploring);
    }

    fn chosen(draft: &PlacementDraft) -> &PlacementSelection {
        &draft.selected.as_ref().expect("a machine was chosen").1
    }

    fn rejection_for<'a>(
        draft: &'a PlacementDraft,
        executor_id: &str,
    ) -> &'a PlacementRejectionReason {
        &draft
            .rejected
            .iter()
            .find(|rejection| rejection.executor_id == executor_id)
            .unwrap_or_else(|| panic!("{executor_id} was evaluated"))
            .reason
    }

    /// The whole point of the policy: a check suite that nobody targeted runs on
    /// the machine that is actually idle, not on the one the operator is typing
    /// on. Local competes with interactive use; an enrolled machine mostly does
    /// not.
    #[test]
    fn spill_eligible_work_leaves_a_busy_local_for_a_measured_idle_remote() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.85,
            8 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.03,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        let draft = place(&connections, &spillable_request()).unwrap();
        let selection = chosen(&draft);
        assert_eq!(selection.executor_id, "bglab-ub");
        assert_eq!(selection.reason, PlacementReason::PredictedEarliestVerdict);
        assert_eq!(
            selection.observation_reuse,
            ObservationReuse::UntrustedRemoteEnvironment,
            "a spilled verdict gates its run and seeds no reusable baseline"
        );
        assert_eq!(
            rejection_for(&draft, COLOCATED_EXECUTOR_ID),
            &PlacementRejectionReason::OutrankedBy {
                executor_name: "bglab-ub".into()
            }
        );
    }

    #[test]
    fn spill_eligible_work_uses_remote_when_local_admission_is_full() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.01,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        local.health.admission.concurrency_capacity = Some(16);
        local.health.admission.active_reservation.concurrency_units = 16;
        local.health.applied_policy.maximum_queue_depth = 16;

        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.50,
            16 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        remote.health.admission.concurrency_capacity = Some(16);
        remote.health.applied_policy.maximum_queue_depth = 16;

        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);
        assert_eq!(
            chosen(&place(&connections, &spillable_request()).unwrap()).executor_id,
            "bglab-ub",
            "resident capacity absent from command forecasts still participates in placement"
        );
    }

    #[test]
    fn spill_eligible_work_avoids_a_full_preferred_queue() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.01,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        local.health.admission.concurrency_capacity = Some(16);
        local.health.applied_policy.maximum_queue_depth = 1;
        local.health.queues = vec![cairn_common::executor_protocol::QueueClassHealth {
            priority: CellPriority::ReviewCheck,
            depth: 1,
            oldest_age_ms: Some(1_000),
            waits: Default::default(),
        }];

        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.50,
            16 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        remote.health.admission.concurrency_capacity = Some(16);
        remote.health.applied_policy.maximum_queue_depth = 1;

        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);
        assert_eq!(
            chosen(&place(&connections, &spillable_request()).unwrap()).executor_id,
            "bglab-ub",
            "a preferred executor that cannot retain a queue entry must not win re-placement"
        );
    }

    /// Measured CPU remains observational rather than declared demand, but the
    /// executor's fresh accepting state breaks an otherwise-comparable tie.
    #[test]
    fn measured_cpu_headroom_breaks_only_equal_verdict_predictions() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.02,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.90,
            4 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        local.health.host.cpu_admission = cairn_common::executor_protocol::CpuAdmissionHealth {
            state: CpuAdmissionState::Accepting,
            utilization: Some(0.02),
            state_since_unix_ms: Some(NOW),
            measured_at_unix_ms: Some(NOW),
        };
        remote.health.host.cpu_admission = cairn_common::executor_protocol::CpuAdmissionHealth {
            state: CpuAdmissionState::Accepting,
            utilization: Some(0.89),
            state_since_unix_ms: Some(NOW),
            measured_at_unix_ms: Some(NOW),
        };
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        let selection = place(&connections, &spillable_request()).unwrap();
        let selection = chosen(&selection);
        assert_eq!(
            selection.executor_id, COLOCATED_EXECUTOR_ID,
            "fresh measured headroom breaks an otherwise-equal prediction tie"
        );
        assert_eq!(selection.reason, PlacementReason::PredictedEarliestVerdict);
        assert_eq!(selection.observation_reuse, ObservationReuse::Colocated);
    }

    #[test]
    fn continuing_cpu_pressure_remains_authoritative_after_an_entry_window() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.95,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        local.health.host.cpu_admission = cairn_common::executor_protocol::CpuAdmissionHealth {
            state: CpuAdmissionState::Pressured,
            utilization: Some(0.95),
            state_since_unix_ms: Some(NOW - 20_000),
            measured_at_unix_ms: Some(NOW),
        };

        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.20,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        assert_eq!(
            chosen(&place(&connections, &spillable_request()).unwrap()).executor_id,
            "bglab-ub",
            "a fresh sample must keep an executor rejected after pressure has remained active beyond one sampling interval"
        );
    }

    /// Untargeted is not the same property as free to move. An agent's own batch
    /// states no selector and is still bound to the machine holding its tree.
    #[test]
    fn conservative_untargeted_work_never_leaves_the_colocated_executor() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(&mut local, 0.99, 1024 * 1024 * 1024, 1024 * 1024 * 1024);
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.01,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        let mut request = spillable_request();
        request.placement_mobility = PlacementMobility::PinnedOrColocated;
        let draft = place(&connections, &request).unwrap();
        assert_eq!(chosen(&draft).executor_id, COLOCATED_EXECUTOR_ID);
        assert_eq!(
            rejection_for(&draft, "bglab-ub"),
            &PlacementRejectionReason::NotColocated
        );
    }

    /// A pin is a fact about where the work already lives, and no measurement
    /// overrules it.
    #[test]
    fn a_pinned_request_ignores_a_more_idle_machine() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(&mut local, 0.99, 1024 * 1024 * 1024, 1024 * 1024 * 1024);
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.01,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        let mut request = spillable_request();
        request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());
        let draft = place(&connections, &request).unwrap();
        let selection = chosen(&draft);
        assert_eq!(selection.executor_id, COLOCATED_EXECUTOR_ID);
        assert_eq!(selection.reason, PlacementReason::Pinned);
        assert_eq!(
            rejection_for(&draft, "bglab-ub"),
            &PlacementRejectionReason::PinMismatch {
                pinned_executor_id: COLOCATED_EXECUTOR_ID.into()
            }
        );
    }

    #[test]
    fn unpinned_job_residency_uses_transferable_placement_identity() {
        let acquisition = fleet_residency_request(ResidencyHolder::Job {
            job_id: "job".into(),
        });
        let placement = residency_placement_request(&acquisition);
        assert!(matches!(
            placement.repository,
            RepositoryLocator::ManagedObjects { .. }
        ));
        assert_eq!(
            placement.placement_mobility,
            PlacementMobility::SpillEligible
        );
        assert!(matches!(
            acquisition.repository,
            RepositoryLocator::ColocatedPath { .. }
        ));
    }

    #[test]
    fn dev_instance_residency_remains_colocated_and_nontransferable() {
        let placement = residency_placement_request(&dev_instance_request("dev"));
        assert!(matches!(
            placement.repository,
            RepositoryLocator::ColocatedPath { .. }
        ));
        assert_eq!(
            placement.pinned_executor_id.as_deref(),
            Some(COLOCATED_EXECUTOR_ID)
        );
        assert_eq!(
            placement.placement_mobility,
            PlacementMobility::PinnedOrColocated
        );
    }

    #[test]
    fn residency_dispatch_fences_the_selected_connection_generation() {
        let (id, entry) = fleet_entry("fedora", "linux", 0, &[]);
        let selected = SelectedExecutor {
            executor_id: id.clone(),
            device_id: entry.identity.device_id.clone(),
            generation: entry.generation,
            sender: entry.sender.clone(),
            colocated: false,
            capabilities: entry.advertisement.capabilities.clone(),
        };
        let mut reconnected = entry;
        reconnected.generation += 1;
        let error =
            selected_link_dispatch_error(&HashMap::from([(id, reconnected)]), &selected, NOW)
                .unwrap();
        assert!(error.contains("old generation is fenced"), "{error}");
    }

    #[test]
    fn stale_colocated_link_is_rejected_before_blind_ranking_and_healthy_remote_wins() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        local.last_progress_unix_ms = NOW - EXECUTOR_PROGRESS_FRESHNESS_MS - 1;
        let (remote_id, mut remote) = fleet_entry("fedora", "linux", 0, &[]);
        measured(
            &mut remote,
            0.87,
            8 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        let draft = place(&connections, &spillable_request()).unwrap();
        assert_eq!(chosen(&draft).executor_id, "fedora");
        assert!(matches!(
            rejection_for(&draft, COLOCATED_EXECUTOR_ID),
            PlacementRejectionReason::ConnectionStale { .. }
        ));
    }

    #[test]
    fn pinned_stale_executor_reports_link_unavailability_instead_of_rehoming() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        local.last_progress_unix_ms = NOW - EXECUTOR_PROGRESS_FRESHNESS_MS - 1;
        let (remote_id, remote) = fleet_entry("fedora", "linux", 0, &[]);
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);
        let mut request = spillable_request();
        request.pinned_executor_id = Some(COLOCATED_EXECUTOR_ID.into());

        let error = place(&connections, &request).unwrap_err();
        assert!(error.contains("local"), "{error}");
        assert!(error.contains("published no progress"), "{error}");
    }

    /// A machine whose load cannot be seen is not a machine to ship a tree to.
    /// The gap is named, and it is never read as no load.
    #[test]
    fn a_placement_gap_excludes_a_remote_and_never_reads_as_idle() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.85,
            8 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.01,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        remote.health.machine.volume = Measurement::unavailable(
            NOW,
            cairn_common::executor_protocol::MeasurementGap::SamplingFailed,
        );
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        let draft = place(&connections, &spillable_request()).unwrap();
        assert_eq!(chosen(&draft).executor_id, COLOCATED_EXECUTOR_ID);
        assert_eq!(
            rejection_for(&draft, "bglab-ub"),
            &PlacementRejectionReason::TelemetryGap {
                measurement: MachineMeasurement::Volume,
                gap: cairn_common::executor_protocol::MeasurementGap::SamplingFailed,
            }
        );
    }

    /// A value measured long enough ago is history, and deciding on it would be
    /// deciding on a machine's past.
    #[test]
    fn a_stale_reading_excludes_a_remote_by_its_own_age() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.85,
            8 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.01,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        remote.health.machine.cpu.measured_at_unix_ms = NOW - EXECUTOR_TELEMETRY_STALE_AFTER_MS - 1;
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        let draft = place(&connections, &spillable_request()).unwrap();
        assert_eq!(chosen(&draft).executor_id, COLOCATED_EXECUTOR_ID);
        assert!(matches!(
            rejection_for(&draft, "bglab-ub"),
            PlacementRejectionReason::TelemetryStale {
                measurement: MachineMeasurement::Cpu,
                ..
            }
        ));
    }

    /// The executor's own brake going blind is an earlier, narrower signal than
    /// the telemetry cache going stale: the pressure tracker stops advancing the
    /// moment its sampler does, while the cache is refreshed by any admission
    /// attempt that reads it. A machine in that state is not a machine with
    /// headroom, however good the numbers it last managed to report look.
    #[test]
    fn a_machine_whose_cpu_brake_went_blind_loses_to_one_that_can_still_see() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.85,
            8 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        local.health.host.cpu_admission = cairn_common::executor_protocol::CpuAdmissionHealth {
            state: CpuAdmissionState::Accepting,
            utilization: Some(0.85),
            state_since_unix_ms: Some(NOW),
            measured_at_unix_ms: Some(NOW),
        };

        // Idler, roomier, and its machine telemetry is fresh enough that the
        // placement staleness gate has nothing to say about it. Only its
        // sampler stopped.
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.01,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let went_quiet = NOW - CPU_ADMISSION_BLIND_AFTER_MS - 1;
        remote.health.host.cpu_admission = cairn_common::executor_protocol::CpuAdmissionHealth {
            state: CpuAdmissionState::Accepting,
            utilization: Some(0.01),
            state_since_unix_ms: Some(went_quiet),
            measured_at_unix_ms: Some(went_quiet),
        };
        // The control: with that same machine still sampling, its numbers win.
        // Asserting only the blind case would pass on any fixture that happened
        // to favour the colocated machine for unrelated reasons.
        let mut seeing = remote.clone();
        seeing.health.host.cpu_admission.measured_at_unix_ms = Some(NOW);
        seeing.health.host.cpu_admission.state_since_unix_ms = Some(NOW);
        let control = HashMap::from([
            (local_id.clone(), local.clone()),
            (remote_id.clone(), seeing),
        ]);
        assert_eq!(
            chosen(&place(&control, &spillable_request()).unwrap()).executor_id,
            remote_id,
            "control: a sampling remote with better numbers should win"
        );

        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);
        let draft = place(&connections, &spillable_request()).unwrap();
        assert_eq!(
            chosen(&draft).executor_id,
            COLOCATED_EXECUTOR_ID,
            "a machine that stopped measuring its own load must not outrank one that has not"
        );
    }

    /// A gap that describes the daemon rather than the machine says nothing
    /// about whether the machine can take work, and must not exclude it.
    #[test]
    fn a_diagnostic_gap_does_not_exclude_a_candidate() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.85,
            8 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.01,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        remote.health.machine.process.physical_footprint_bytes = Measurement::unavailable(
            NOW,
            cairn_common::executor_protocol::MeasurementGap::UnsupportedPlatform,
        );
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        assert_eq!(
            chosen(&place(&connections, &spillable_request()).unwrap()).executor_id,
            "bglab-ub"
        );
    }

    /// When nothing in the fleet can be measured, the work stays where it is —
    /// and the record says the fleet was blind rather than implying a
    /// measurement happened.
    #[test]
    fn a_measured_blind_fleet_keeps_work_home_and_says_so() {
        let connections = HashMap::from([
            fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]),
            fleet_entry("bglab-ub", "linux", 0, &[]),
        ]);
        let draft = place(&connections, &spillable_request()).unwrap();
        let selection = chosen(&draft);
        assert_eq!(selection.executor_id, COLOCATED_EXECUTOR_ID);
        assert_eq!(selection.reason, PlacementReason::MeasuredBlindFleet);
        assert!(matches!(
            rejection_for(&draft, "bglab-ub"),
            PlacementRejectionReason::TelemetryGap { .. }
        ));
    }

    /// Constraining the fleet is not settling placement. `os: linux` narrows the
    /// candidate set and leaves policy choosing among what is left, so a machine
    /// whose readings are missing is exactly as disqualified there as it is for
    /// an unconstrained request.
    #[test]
    fn a_platform_constrained_spill_batch_still_refuses_a_blind_remote() {
        let (measured_id, mut measured_remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut measured_remote,
            0.20,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let (blind_id, blind_remote) = fleet_entry("bglab-win", "linux", 0, &[]);
        let connections = HashMap::from([(measured_id, measured_remote), (blind_id, blind_remote)]);

        let mut request = spillable_request();
        request.executor = Some(ExecutorSelector {
            os: Some("linux".into()),
            ..ExecutorSelector::default()
        });
        let draft = place(&connections, &request).unwrap();
        assert_eq!(chosen(&draft).executor_id, "bglab-ub");
        assert!(
            matches!(
                rejection_for(&draft, "bglab-win"),
                PlacementRejectionReason::TelemetryGap { .. }
            ),
            "a constrained set is still a set policy chooses from"
        );
    }

    /// The CAIRN-3452 specimen, as placement sees it.
    ///
    /// A review wave with nowhere idle to go locally found one measured, idle
    /// Windows machine and took it, and the target-gated dead-code findings
    /// clippy only emits there were recorded as the PR's rust-lint verdict.
    /// Every input placement ranks on said bglab-win was the better machine.
    /// The one thing that makes it the wrong machine is not a reading at all:
    /// this project's gate has only ever meant green on macOS, so a Windows
    /// answer is not this check's answer however fast it arrives.
    #[test]
    fn a_verdict_never_travels_to_a_platform_the_project_does_not_gate_on() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
        measured(
            &mut local,
            0.97,
            2 * 1024 * 1024 * 1024,
            100 * 1024 * 1024 * 1024,
        );
        let (windows_id, mut windows) = fleet_entry("bglab-win", "windows", 0, &[]);
        measured(
            &mut windows,
            0.01,
            64 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local), (windows_id, windows)]);

        let mut request = spillable_request();
        request.verdict_platforms = vec!["macos".into()];
        let draft = place(&connections, &request).unwrap();
        assert_eq!(
            chosen(&draft).executor_id,
            COLOCATED_EXECUTOR_ID,
            "a busy machine the project gates on beats an idle one it does not"
        );
        let rejection = rejection_for(&draft, "bglab-win");
        assert!(
            matches!(
                rejection,
                PlacementRejectionReason::UntrustedVerdictPlatform { .. }
            ),
            "the reason must be the trust, not a reading: {rejection:?}"
        );
        assert!(rejection.describe().contains("windows"), "{rejection:?}");
    }

    /// Trust decides eligibility, so a same-platform machine still competes on
    /// its readings. The rule narrows WHERE a verdict may come from; it does not
    /// pin every check back to the runner's own machine, which would trade one
    /// wrong answer for a fleet that never spills at all.
    #[test]
    fn a_gated_platform_still_spills_to_the_idle_machine_on_it() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
        measured(
            &mut local,
            0.95,
            2 * 1024 * 1024 * 1024,
            100 * 1024 * 1024 * 1024,
        );
        let (mac_id, mut mac) = fleet_entry("bglab-mac", "macos", 0, &[]);
        measured(
            &mut mac,
            0.02,
            64 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let (windows_id, mut windows) = fleet_entry("bglab-win", "windows", 0, &[]);
        measured(
            &mut windows,
            0.00,
            96 * 1024 * 1024 * 1024,
            999 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local), (mac_id, mac), (windows_id, windows)]);

        let mut request = spillable_request();
        request.verdict_platforms = vec!["macos".into()];
        let draft = place(&connections, &request).unwrap();
        assert_eq!(chosen(&draft).executor_id, "bglab-mac");
        assert_eq!(
            chosen(&draft).reason,
            PlacementReason::PredictedEarliestVerdict
        );
    }

    /// A batch that cannot move has one possible machine, and waiting cannot
    /// change its platform. That is a contradiction between what a check
    /// declared and the cadence it runs at, and it is answered immediately
    /// rather than by stalling out the horizon on every commit.
    #[test]
    fn work_that_cannot_move_refuses_at_once_when_its_home_is_not_gated_on() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
        measured(
            &mut local,
            0.10,
            8 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local)]);

        let mut request = spillable_request();
        request.placement_mobility = PlacementMobility::PinnedOrColocated;
        request.verdict_platforms = vec!["linux".into()];
        let refusal = place(&connections, &request).unwrap_err();
        assert!(refusal.contains("linux"), "{refusal}");
        assert!(refusal.contains("Nothing was run"), "{refusal}");
    }

    /// A mobile batch waits instead: a machine on a platform this verdict counts
    /// from may still attach, and the requester's horizon already bounds how
    /// long that hope lasts.
    #[test]
    fn mobile_work_waits_when_only_ungated_platforms_are_attached() {
        let (windows_id, mut windows) = fleet_entry("bglab-win", "windows", 0, &[]);
        measured(
            &mut windows,
            0.01,
            64 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(windows_id, windows)]);

        let mut request = spillable_request();
        request.verdict_platforms = vec!["macos".into()];
        let draft = place(&connections, &request).unwrap();
        assert!(draft.selected.is_none(), "{draft:?}");
    }

    /// And when the constraint leaves only blind machines, the answer is the
    /// typed telemetry refusal. Narrowing the fleet cannot make absent evidence
    /// safe to act on.
    #[test]
    fn a_platform_constrained_spill_batch_with_only_blind_remotes_refuses() {
        let connections = HashMap::from([
            fleet_entry("bglab-ub", "linux", 0, &[]),
            fleet_entry("bglab-win", "linux", 0, &[]),
        ]);
        let mut request = spillable_request();
        request.executor = Some(ExecutorSelector {
            os: Some("linux".into()),
            ..ExecutorSelector::default()
        });
        let refusal = place(&connections, &request).unwrap_err();
        assert!(refusal.contains("bglab-ub"), "{refusal}");
        assert!(refusal.contains("bglab-win"), "{refusal}");
        assert!(refusal.contains("unavailable"), "{refusal}");
    }

    /// Naming one machine is different: the caller settled placement, there is
    /// nothing for a measurement to decide, and the work runs where it was sent.
    #[test]
    fn naming_one_machine_runs_there_whether_or_not_it_can_be_measured() {
        let connections = HashMap::from([
            fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]),
            fleet_entry("bglab-ub", "linux", 0, &[]),
        ]);
        let mut request = spillable_request();
        request.executor = Some(ExecutorSelector {
            name: Some("bglab-ub".into()),
            ..ExecutorSelector::default()
        });
        let draft = place(&connections, &request).unwrap();
        let selection = chosen(&draft);
        assert_eq!(selection.executor_id, "bglab-ub");
        assert_eq!(
            selection.reason,
            PlacementReason::OnlyCandidate,
            "nothing was measured for, so the record must not claim the fleet was blind"
        );
    }

    /// The exact local-degradation case: local is the last machine standing and
    /// nothing about it can be seen. "It was the only one" would describe that as
    /// a choice; the record has to say measurement was impossible.
    #[test]
    fn a_lone_blind_home_records_that_measurement_was_impossible() {
        let connections = HashMap::from([fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[])]);
        let draft = place(&connections, &spillable_request()).unwrap();
        let selection = chosen(&draft);
        assert_eq!(selection.executor_id, COLOCATED_EXECUTOR_ID);
        assert_eq!(
            selection.reason,
            PlacementReason::MeasuredBlindFleet,
            "a sole blind home is not a decision that was made, it is one that could not be"
        );
    }

    /// A blind fleet with nowhere to fall back to refuses in words, carrying the
    /// same evidence a success would. Running it somewhere unexamined would be
    /// the silent degradation this policy exists to prevent.
    #[test]
    fn an_unmeasurable_fleet_with_no_home_refuses_with_its_evidence() {
        let connections = HashMap::from([fleet_entry("bglab-ub", "linux", 0, &[])]);
        let refusal = place(&connections, &spillable_request()).unwrap_err();
        assert!(refusal.contains("bglab-ub"), "{refusal}");
        assert!(refusal.contains("unavailable"), "{refusal}");
    }

    /// Fit is the first ranking key, so a machine that measurably cannot hold the
    /// work loses to one that can even when it is the idler of the two.
    #[test]
    fn resolved_demand_that_does_not_fit_loses_to_a_machine_that_holds_it() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.50,
            8 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.01,
            512 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        let demand = resource_profiles::ResolvedResourceProfile {
            duration: test_prior_duration(),
            item_durations: Vec::new(),
            reservation: ResourceReservation {
                memory_bytes: 2 * 1024 * 1024 * 1024,
                disk_growth_bytes: 1024 * 1024 * 1024,
                concurrency_units: 1,
                source: ResourceReservationSource::Learned,
            },
            learned_estimate: None,
            rationale: unresolved_rationale(&ResourceReservation::default()),
        };
        let reservations = HashMap::from([
            (COLOCATED_EXECUTOR_ID.to_string(), demand.clone()),
            ("bglab-ub".to_string(), demand),
        ]);
        let draft = choose_executor_with(
            &connections,
            &spillable_request(),
            &reservations,
            |_, _| SyncCost::Known(0),
            NOW,
        )
        .unwrap();
        assert_eq!(chosen(&draft).executor_id, COLOCATED_EXECUTOR_ID);
        assert_eq!(
            rejection_for(&draft, "bglab-ub"),
            &PlacementRejectionReason::InsufficientMemory {
                required_bytes: 2 * 1024 * 1024 * 1024,
                available_bytes: 512 * 1024 * 1024,
            }
        );
    }

    /// Memory pressure is transient capacity, not a structural placement fault.
    /// When every measured candidate is short right now, placement still chooses
    /// one so executor admission can retain and visibly park the request.
    #[test]
    fn an_all_memory_constrained_fleet_still_places_for_capacity_waiting() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.50,
            512 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.01,
            768 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);
        let demand = resource_profiles::ResolvedResourceProfile {
            duration: test_prior_duration(),
            item_durations: Vec::new(),
            reservation: ResourceReservation {
                memory_bytes: 2 * 1024 * 1024 * 1024,
                disk_growth_bytes: 1024 * 1024 * 1024,
                concurrency_units: 1,
                source: ResourceReservationSource::Learned,
            },
            learned_estimate: None,
            rationale: unresolved_rationale(&ResourceReservation::default()),
        };
        let reservations = HashMap::from([
            (COLOCATED_EXECUTOR_ID.to_string(), demand.clone()),
            ("bglab-ub".to_string(), demand),
        ]);

        let draft = choose_executor_with(
            &connections,
            &spillable_request(),
            &reservations,
            |_, _| SyncCost::Known(0),
            NOW,
        )
        .expect("transient memory pressure must remain placeable for admission waiting");

        assert!(matches!(
            rejection_for(&draft, COLOCATED_EXECUTOR_ID),
            PlacementRejectionReason::InsufficientMemory { .. }
        ));
    }

    /// Equal verdict predictions fall through to transfer cost and then stable
    /// executor identity. Memory and volume are eligibility facts, not ranking
    /// preferences once both machines can fit the work.
    #[test]
    fn equal_verdict_predictions_are_ranked_deterministically() {
        let entry = |id: &str| {
            let (key, mut state) = fleet_entry(id, "linux", 0, &[]);
            measured(
                &mut state,
                0.10,
                40 * 1024 * 1024 * 1024,
                100 * 1024 * 1024 * 1024,
            );
            (key, state)
        };

        let by_sync = HashMap::from([entry(COLOCATED_EXECUTOR_ID), entry("far")]);
        let draft = choose_executor_with(
            &by_sync,
            &spillable_request(),
            &HashMap::new(),
            |_, candidate| {
                if candidate.identity.executor_id == "far" {
                    SyncCost::Known(1_000_000)
                } else {
                    SyncCost::Known(0)
                }
            },
            NOW,
        )
        .unwrap();
        assert_eq!(
            chosen(&draft).executor_id,
            COLOCATED_EXECUTOR_ID,
            "otherwise equal machines separate on what has to be transferred"
        );

        let tied = HashMap::from([entry("aaa"), entry("zzz")]);
        for _ in 0..8 {
            assert_eq!(
                chosen(&place(&tied, &spillable_request()).unwrap()).executor_id,
                "aaa",
                "a completely tied fleet still ranks the same way every time"
            );
        }
    }

    /// A checkout that already exists on one machine cannot be recreated
    /// elsewhere from objects, so spilling it is not an option however idle the
    /// alternative looks.
    #[test]
    fn an_existing_checkout_is_never_spilled() {
        let (local_id, mut local) = fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[]);
        measured(
            &mut local,
            0.95,
            8 * 1024 * 1024 * 1024,
            500 * 1024 * 1024 * 1024,
        );
        let (remote_id, mut remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        measured(
            &mut remote,
            0.01,
            48 * 1024 * 1024 * 1024,
            900 * 1024 * 1024 * 1024,
        );
        let connections = HashMap::from([(local_id, local), (remote_id, remote)]);

        let mut request = spillable_request();
        request.repository = RepositoryLocator::ExistingCheckout {
            project_id: "p".into(),
            repository_id: "repo".into(),
            absolute_path: "/repo".into(),
        };
        let draft = place(&connections, &request).unwrap();
        assert_eq!(chosen(&draft).executor_id, COLOCATED_EXECUTOR_ID);
        assert!(matches!(
            rejection_for(&draft, "bglab-ub"),
            PlacementRejectionReason::RepositoryNotTransferable { .. }
        ));
    }

    /// A machine is addressed by the name it advertises, normalized — which is
    /// the same name `cairn://executors` publishes. Selecting by a label an
    /// operator would actually type has to reach it, or the resource lists
    /// addresses that placement cannot use.
    #[test]
    fn a_named_selector_reaches_the_machine_the_resource_publishes() {
        let mut connections = HashMap::from([
            fleet_entry("remote-a", "linux", 0, &[]),
            fleet_entry("remote-b", "linux", 0, &[]),
        ]);
        connections
            .get_mut("remote-a")
            .unwrap()
            .identity
            .display_name = "BGLab UB".into();
        connections
            .get_mut("remote-b")
            .unwrap()
            .identity
            .display_name = "bglab-mac".into();

        for typed in ["bglab-ub", "BGLab UB", "BGLAB_UB"] {
            let mut request = targeted_request("linux");
            request.executor = Some(ExecutorSelector {
                name: Some(typed.into()),
                ..ExecutorSelector::default()
            });
            assert_eq!(
                choose_executor(&connections, &request)
                    .unwrap()
                    .unwrap()
                    .executor_id,
                "remote-a",
                "selector {typed}"
            );
        }

        // A toolchain the fleet does not advertise narrows a matching name to
        // nothing rather than falling back to the machine that shares its OS.
        let mut request = targeted_request("linux");
        request.executor = Some(ExecutorSelector {
            name: Some("bglab-ub".into()),
            required_toolchains: vec!["msvc".into()],
            ..ExecutorSelector::default()
        });
        assert!(choose_executor(&connections, &request).is_err());
    }

    /// A refusal that names only what was wanted leaves an agent guessing, which
    /// is what opaque identities forced. It names the fleet too, from the same
    /// cache the resource reads, and points at that resource.
    #[test]
    fn a_no_match_refusal_names_the_request_and_every_machine_that_exists() {
        let mut connections = HashMap::from([fleet_entry("remote-a", "linux", 0, &[])]);
        connections
            .get_mut("remote-a")
            .unwrap()
            .identity
            .display_name = "bglab-ub".into();
        let mut request = targeted_request("linux");
        request.executor = Some(ExecutorSelector {
            name: Some("bglab-win".into()),
            ..ExecutorSelector::default()
        });

        let refusal = choose_executor(&connections, &request).unwrap_err();

        assert!(refusal.contains("bglab-win"), "{refusal}");
        assert!(refusal.contains("bglab-ub"), "{refusal}");
        assert!(refusal.contains("linux"), "{refusal}");
        assert!(refusal.contains("rust"), "{refusal}");
        assert!(refusal.contains("cairn://executors"), "{refusal}");
    }

    /// The enrollment outlives every link to the machine. A record that existed
    /// only while the machine was attached could not describe a machine that is
    /// not, which is the state worth describing.
    #[test]
    fn an_enrolled_machine_is_projected_before_it_ever_attaches() {
        let fleet = Fleet::default();
        fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");

        let unattached = fleet.unattached_enrolled_remotes();

        assert_eq!(unattached.len(), 1);
        assert_eq!(unattached[0].name, "bglab-ub");
        assert_eq!(unattached[0].link, RemoteLinkState::Pending);
        assert_eq!(unattached[0].last_attempt, None);
        assert_eq!(
            unattached[0].last_seen_unix_ms, None,
            "a machine that has never attached must not claim to have been seen"
        );
    }

    /// The two down states are recorded as the caller proved them, because only
    /// the caller knows whether the host answered.
    #[test]
    fn an_attach_attempt_records_the_state_it_proved() {
        let fleet = Fleet::default();
        fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");

        fleet.record_remote_attach_attempt(
            "bglab-ub",
            RemoteLinkState::Unreachable,
            RemoteAttachAttempt::bootstrap_failure(4_000, "no route to host"),
        );
        let unreachable = fleet.unattached_enrolled_remotes();
        assert_eq!(unreachable[0].link, RemoteLinkState::Unreachable);

        fleet.record_remote_attach_attempt(
            "bglab-ub",
            RemoteLinkState::AttachFailed,
            RemoteAttachAttempt::bootstrap_failure(
                5_000,
                "executor protocol v28 has no published artifact",
            ),
        );
        let failed = fleet.unattached_enrolled_remotes();
        let attempt = failed[0].last_attempt.as_ref().unwrap();
        assert_eq!(failed[0].link, RemoteLinkState::AttachFailed);
        assert_eq!(attempt.attempted_at_unix_ms, 5_000);
        assert!(attempt.summary.contains("no published artifact"));
    }

    /// The defect this whole seam exists for: a drain must be settable on a
    /// machine that is answering nothing, because that is when an operator
    /// reaches for it. The write records the refusal and says plainly that the
    /// machine has not been told.
    #[tokio::test]
    async fn a_machine_that_is_not_attached_can_still_be_drained() {
        let fleet = Fleet::default();
        fleet.declare_enrolled_remote("bglab-sp1", "bglab-sp1", "linux", "x86_64");

        let application = fleet.set_executor_drain_mode("bglab-sp1", true).await;

        assert!(matches!(
            application,
            DrainApplication::HeldWithoutMachine { .. }
        ));
        assert!(fleet.is_drained("bglab-sp1"));
        assert!(
            fleet.unattached_enrolled_remotes()[0].drained,
            "the fleet surface has to show a drain the runner is holding, or an operator cannot tell it took"
        );
    }

    /// The durability half. A drained machine that comes back — a power cycle,
    /// a supervisor respawn, a new generation — is still drained, and is told
    /// so as part of attaching. Before this, the drain evaporated on reconnect
    /// and the fleet re-dogpiled the machine it had just crushed.
    #[tokio::test]
    async fn a_drain_outlives_the_connection_it_was_set_on() {
        let fleet = Fleet::default();
        let (first_sender, _first) = mpsc::unbounded_channel();
        let first_generation = fleet.attach_executor(first_sender);

        fleet
            .set_executor_drain_mode(COLOCATED_EXECUTOR_ID, true)
            .await;
        fleet.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, first_generation);

        let (sender, mut executor) = mpsc::unbounded_channel();
        let second_generation = fleet.attach_executor(sender);
        assert!(second_generation > first_generation);
        assert_eq!(drain_told(&mut executor), Some(true));
        assert!(fleet.is_drained(COLOCATED_EXECUTOR_ID));
    }

    /// The other direction, and the one that inverts the fleet if it is missed.
    ///
    /// Drain mode is executor-PROCESS state, and a process outlives a link
    /// bounce. Clearing a drain while the machine is detached — the path this
    /// change exists to allow — has to reach that process when its link comes
    /// back, or the executor keeps refusing work the runner has started placing
    /// again: each side correct about itself, opposite about the fleet.
    #[tokio::test]
    async fn clearing_a_drain_while_detached_reaches_the_same_process_on_reconnect() {
        let fleet = Fleet::default();
        // Drained while unreachable, which is the path this change allows.
        fleet
            .set_executor_drain_mode(COLOCATED_EXECUTOR_ID, true)
            .await;

        // Its process was running the whole time; attaching is where it learns.
        let (first_sender, mut first) = mpsc::unbounded_channel();
        let first_generation = fleet.attach_executor(first_sender);
        assert_eq!(drain_told(&mut first), Some(true));

        // The link drops. The process does not.
        fleet.disconnect_advertised_executor(COLOCATED_EXECUTOR_ID, first_generation);
        fleet
            .set_executor_drain_mode(COLOCATED_EXECUTOR_ID, false)
            .await;
        assert!(!fleet.is_drained(COLOCATED_EXECUTOR_ID));

        let (sender, mut executor) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);

        assert_eq!(
            drain_told(&mut executor),
            Some(false),
            "the returning process still holds the drain it was last told about, so the clear has to be stated to it"
        );
        assert!(
            !fleet
                .connections
                .lock()
                .unwrap()
                .get(COLOCATED_EXECUTOR_ID)
                .unwrap()
                .drained
        );
    }

    /// What the runner told an executor about draining on this connection, if
    /// anything. Drains the queue rather than reading one message, because an
    /// attach also carries whatever else the runner has to say.
    fn drain_told(executor: &mut mpsc::UnboundedReceiver<ExecutorMessage>) -> Option<bool> {
        let mut told = None;
        while let Ok(message) = executor.try_recv() {
            if let ExecutorMessage::DrainModeRequest { enabled, .. } = message {
                told = Some(enabled);
            }
        }
        told
    }

    /// Placement is the runner's own decision, so a drained machine is passed
    /// over here rather than by asking the machine to refuse the admission. A
    /// machine too wedged to answer is exactly the one whose refusal cannot be
    /// relied on.
    #[test]
    fn placement_passes_over_a_drained_machine_without_asking_it() {
        let mut connections = HashMap::from([
            fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]),
            fleet_entry("bglab-sp1", "linux", 0, &[]),
        ]);
        connections
            .get_mut(COLOCATED_EXECUTOR_ID)
            .unwrap()
            .colocated = true;
        connections.get_mut("bglab-sp1").unwrap().drained = true;
        let mut request = targeted_request("linux");
        request.executor = Some(ExecutorSelector {
            name: Some("bglab-sp1".into()),
            ..ExecutorSelector::default()
        });

        let refusal = place(&connections, &request)
            .expect_err("a fleet whose only matching machine is drained refuses");
        assert!(refusal.contains("bglab-sp1"), "{refusal}");
        assert!(refusal.contains("drained by an operator"), "{refusal}");
        assert!(refusal.contains("Only an operator can clear"), "{refusal}");

        connections.get_mut("bglab-sp1").unwrap().drained = false;
        assert_eq!(
            place(&connections, &request)
                .unwrap()
                .selected
                .expect("clearing the drain re-admits it")
                .0
                .executor_id,
            "bglab-sp1"
        );
    }

    /// The posts/91 specimen: draining a machine with a live execution home on
    /// it must not strand that job. A pin is residency established before the
    /// drain, not a requester choosing the machine for new work. Letting it run
    /// is what permits the resident work to finish and the machine to empty.
    #[test]
    fn a_drain_admits_work_pinned_to_the_execution_home_it_already_hosts() {
        let (executor_id, mut resident_home) = fleet_entry("bglab-sp1", "linux", 0, &[]);
        resident_home.drained = true;
        let connections = HashMap::from([(executor_id, resident_home)]);
        let mut request = targeted_request("linux");
        request.pinned_executor_id = Some("bglab-sp1".into());

        let selection = place(&connections, &request)
            .expect("resident work crosses the drain gate")
            .selected
            .expect("the execution home remains the only destination")
            .0;

        assert_eq!(selection.executor_id, "bglab-sp1");
        assert_eq!(
            request.pinned_executor_id.as_deref(),
            Some(selection.executor_id.as_str()),
            "admission preserves residency; it does not re-home the job"
        );
    }

    /// A matching residency pin bypasses only the drain refusal. It does not
    /// make a dead link usable, and the diagnostic must preserve that actual
    /// rejection rather than telling an operator to clear an irrelevant drain.
    #[test]
    fn a_pinned_drained_home_with_a_stale_link_reports_liveness_not_drain() {
        let (executor_id, mut resident_home) = fleet_entry("bglab-sp1", "linux", 0, &[]);
        resident_home.drained = true;
        resident_home.last_progress_unix_ms = NOW - EXECUTOR_PROGRESS_FRESHNESS_MS - 1;
        let connections = HashMap::from([(executor_id, resident_home)]);
        let mut request = targeted_request("linux");
        request.pinned_executor_id = Some("bglab-sp1".into());

        let refusal = place(&connections, &request)
            .expect_err("a residency pin cannot cross a stale connection");

        assert!(refusal.contains("execution home (bglab-sp1)"), "{refusal}");
        assert!(refusal.contains("liveness bound"), "{refusal}");
        assert!(!refusal.contains("drained"), "{refusal}");
        assert!(!refusal.contains("clear the drain"), "{refusal}");
    }

    /// Adopting durable state at startup must not depend on winning a race with
    /// the colocated executor's attach, which happens inside the same boot.
    #[tokio::test]
    async fn hydrating_a_drain_reaches_a_machine_that_attached_first() {
        let fleet = Fleet::default();
        let (sender, mut executor) = mpsc::unbounded_channel();
        fleet.attach_executor(sender);
        assert_eq!(
            drain_told(&mut executor),
            Some(false),
            "attaching states this machine's drain either way"
        );

        fleet.hydrate_drained([COLOCATED_EXECUTOR_ID.to_string()]);

        assert_eq!(
            drain_told(&mut executor),
            Some(true),
            "a machine already up is told about the drain it is under"
        );
        assert!(
            fleet
                .connections
                .lock()
                .unwrap()
                .get(COLOCATED_EXECUTOR_ID)
                .unwrap()
                .drained
        );

        // And an adoption holding no drain says so, because a runner restart's
        // settings are not evidence about what a surviving executor process is
        // still doing to its own admission.
        fleet.hydrate_drained(Vec::<String>::new());
        assert_eq!(drain_told(&mut executor), Some(false));
    }

    #[test]
    fn removing_a_remote_generation_leaves_the_colocated_generation_stable() {
        let fleet = Fleet::default();
        let (remote_id, remote) = fleet_entry("bglab-ub", "linux", 0, &[]);
        let (local_id, local) = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]);
        fleet.connections.lock().unwrap().insert(remote_id, remote);
        fleet.connections.lock().unwrap().insert(local_id, local);

        assert_eq!(
            fleet.managed_link_silence_ms("bglab-ub", 1, NOW + 10),
            Some(10)
        );
        assert!(fleet.disconnect_advertised_executor("bglab-ub", 1));
        assert_eq!(fleet.managed_link_silence_ms("bglab-ub", 1, NOW + 10), None);
        assert_eq!(fleet.executor_generation(), Some(1));
    }

    #[test]
    fn remote_connection_history_is_bounded_and_newest_first() {
        let fleet = Fleet::default();
        fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");

        for generation in 1..=REMOTE_CONNECTION_TIMELINE_LIMIT as u64 + 3 {
            fleet.record_remote_connection_transition(
                "bglab-ub",
                RemoteConnectionTransition {
                    occurred_at_unix_ms: generation,
                    phase: RemoteConnectionPhase::ProtocolReady,
                    generation: Some(generation),
                    reason: None,
                    ssh_exit_status: None,
                    remote_process_exit_status: None,
                    last_stderr: None,
                },
            );
        }

        let remote = &fleet.unattached_enrolled_remotes()[0];
        assert_eq!(
            remote.connection_timeline.len(),
            REMOTE_CONNECTION_TIMELINE_LIMIT
        );
        assert_eq!(remote.connection_timeline[0].generation, Some(35));
        assert_eq!(
            remote.connection_timeline.last().unwrap().generation,
            Some(4)
        );
    }

    /// An attached machine is described in full by the executor projections, so
    /// listing it here too would invite two descriptions of one machine to
    /// disagree.
    #[test]
    fn an_attached_machine_is_absent_from_the_unattached_projection() {
        let fleet = Fleet::default();
        fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");
        let (id, entry) = fleet_entry("bglab-ub", "linux", 0, &[]);
        fleet.connections.lock().unwrap().insert(id, entry);

        assert!(fleet.unattached_enrolled_remotes().is_empty());
    }

    /// Removing a machine takes it out of the fleet rather than leaving behind
    /// one that fails to attach forever.
    #[test]
    fn forgetting_an_enrollment_removes_it_rather_than_failing_it() {
        let fleet = Fleet::default();
        fleet.declare_enrolled_remote("bglab-ub", "bglab-ub", "linux", "x86_64");
        fleet.record_remote_attach_attempt(
            "bglab-ub",
            RemoteLinkState::Unreachable,
            RemoteAttachAttempt::bootstrap_failure(4_000, "no route to host"),
        );

        fleet.forget_enrolled_remote("bglab-ub");

        assert!(fleet.unattached_enrolled_remotes().is_empty());
        // A machine nobody is enrolled with cannot accumulate attempts either.
        fleet.record_remote_attach_attempt(
            "bglab-ub",
            RemoteLinkState::AttachFailed,
            RemoteAttachAttempt::bootstrap_failure(6_000, "stale supervisor"),
        );
        assert!(fleet.unattached_enrolled_remotes().is_empty());
    }

    /// An empty fleet says so rather than printing an empty list, and never
    /// leaks an internal identity in place of a name.
    #[test]
    fn a_refusal_against_an_empty_fleet_says_nothing_is_attached() {
        let mut request = targeted_request("linux");
        request.executor = Some(ExecutorSelector {
            name: Some("bglab-ub".into()),
            ..ExecutorSelector::default()
        });
        let refusal = choose_executor(&HashMap::new(), &request).unwrap_err();
        assert!(
            refusal.contains("no executor is currently attached"),
            "{refusal}"
        );
    }

    /// The home pin is the runner's own placement fact, not a selector: it is
    /// honored exactly, and it is not something a requester can state.
    #[test]
    fn a_pinned_batch_reaches_only_the_machine_holding_its_tree() {
        let connections = HashMap::from([
            fleet_entry("remote-a", "linux", 0, &[]),
            fleet_entry("remote-b", "linux", 0, &[]),
        ]);
        let mut request = targeted_request("linux");
        request.executor = None;
        request.pinned_executor_id = Some("remote-b".into());
        assert_eq!(
            choose_executor(&connections, &request)
                .unwrap()
                .unwrap()
                .executor_id,
            "remote-b"
        );

        // A pin to a machine that is gone refuses, and the refusal still lists
        // the fleet an agent could read.
        request.pinned_executor_id = Some("remote-c".into());
        let refusal = choose_executor(&connections, &request).unwrap_err();
        assert!(refusal.contains("execution home"), "{refusal}");
        assert!(refusal.contains("cairn://executors"), "{refusal}");
    }

    /// When a batch is both pinned to its job's home and carrying a selector of
    /// its own, both are why nothing matched. Reporting only the pin tells an
    /// agent its batch was misplaced when what actually failed is the request it
    /// wrote — and leaves the word it typed out of the answer entirely.
    #[test]
    fn a_pinned_batch_that_also_asked_for_something_is_refused_in_its_own_words() {
        let connections = HashMap::from([fleet_entry("remote-a", "linux", 0, &[])]);
        let mut request = targeted_request("plan9");
        request.pinned_executor_id = Some("remote-a".into());

        let refusal = choose_executor(&connections, &request).unwrap_err();

        assert!(refusal.contains("execution home"), "{refusal}");
        assert!(refusal.contains("plan9"), "{refusal}");
        assert!(refusal.contains("remote-a"), "{refusal}");
        assert!(refusal.contains("cairn://executors"), "{refusal}");
    }

    /// Untargeted routing is unchanged by any of this: a batch that names no
    /// machine still takes the colocated compatibility path without consulting a
    /// selector, which is the seam CAIRN-3323 builds policy on.
    #[test]
    fn an_untargeted_batch_still_routes_to_the_colocated_executor() {
        let connections = HashMap::from([
            fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 5, &[]),
            fleet_entry("remote-a", "linux", 0, &[]),
        ]);
        let mut request = targeted_request("linux");
        request.executor = None;
        assert_eq!(
            choose_executor(&connections, &request)
                .unwrap()
                .unwrap()
                .executor_id,
            COLOCATED_EXECUTOR_ID
        );
    }

    /// The projection an agent reads is deterministic, addressed by name, and
    /// carries each executor's own occupancy rather than a fleet aggregate.
    #[test]
    fn the_inspection_projection_is_name_ordered_and_per_machine() {
        let pool = Fleet::default();
        let mut colocated = fleet_entry(COLOCATED_EXECUTOR_ID, "macos", 0, &[]).1;
        colocated.snapshot.executing_requests = vec![ExecutingCellRequest {
            command_resource_identity: None,
            executor_id: COLOCATED_EXECUTOR_ID.into(),
            cell_id: "cell".into(),
            request_id: "r".into(),
            attempt_id: "a".into(),
            owner: None,
            command_class: cairn_common::executor_protocol::CellCommandClass::Other,
            command: "true".into(),
            started_at_unix_ms: 1,
            process_ids: Vec::new(),
            priority: None,
            subscriber_count: 1,
            resource_reservation: ResourceReservation::default(),
            learned_estimate: None,
        }];
        let mut remote = fleet_entry("remote-a", "linux", 0, &[]).1;
        remote.identity.display_name = "BGLab UB".into();
        pool.connections.lock().unwrap().extend([
            (COLOCATED_EXECUTOR_ID.to_string(), colocated),
            ("remote-a".to_string(), remote),
        ]);

        let inspected = pool.inspect_executors(5_000);

        // Sorted by public address, and the runner's own executor answers to the
        // reserved name whatever its label says.
        assert_eq!(
            inspected
                .iter()
                .map(|executor| executor.name.as_str())
                .collect::<Vec<_>>(),
            vec!["bglab-ub", "local"]
        );
        assert!(inspected[1].colocated);
        // Occupancy is attributed to the machine holding it, not summed.
        assert_eq!(inspected[1].occupancy.executing_requests.len(), 1);
        assert!(inspected[0].occupancy.executing_requests.is_empty());
        // Ages derive from the one capture instant handed in.
        assert_eq!(inspected[0].captured_at_unix_ms, 5_000);
        assert_eq!(inspected[0].health.heartbeat_age_ms, 4_999);
        assert_eq!(
            pool.executor_public_name(COLOCATED_EXECUTOR_ID).as_deref(),
            Some("local")
        );
        assert_eq!(pool.executor_public_name("absent"), None);
    }

    /// A public name is an address, so configuration cannot introduce two
    /// machines answering to one, and no remote may claim the reserved local
    /// name.
    #[test]
    fn fleet_configuration_keeps_public_names_unique_and_reserves_local() {
        let remote = |id: &str, display: &str| {
            let mut config = darwin_remote_config();
            config.executor_id = id.into();
            config.device_id = format!("{id}-device");
            config.display_name = display.into();
            (id.to_string(), config)
        };

        let mut config = FleetConfig {
            remote_executors: BTreeMap::from([
                remote("one", "BGLab UB"),
                remote("two", "bglab-ub"),
            ]),
            ..FleetConfig::default()
        };
        let error = config.validate().unwrap_err();
        assert!(error.contains("bglab-ub"), "{error}");
        assert!(error.contains("cairn executor rename"), "{error}");

        config.remote_executors = BTreeMap::from([remote("one", "local")]);
        assert!(config
            .validate()
            .unwrap_err()
            .contains("reserved name local"));

        config.remote_executors = BTreeMap::from([remote("one", "---")]);
        assert!(config.validate().unwrap_err().contains("no public name"));

        config.remote_executors =
            BTreeMap::from([remote("one", "BGLab UB"), remote("two", "bglab-mac")]);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn snapshot_aggregates_lifetime_count_and_reservations_across_executors() {
        let pool = Fleet::default();
        let mut first = fleet_entry("first", "macos", 0, &[]).1;
        first.snapshot.resident_occupancy = Some(ResidentOccupancyEvidence {
            process_count: 2,
            reservation: ResourceReservation {
                memory_bytes: 1_000,
                disk_growth_bytes: 2_000,
                concurrency_units: 3,
                source: ResourceReservationSource::Declared,
            },
        });
        let mut second = fleet_entry("second", "linux", 0, &[]).1;
        second.snapshot.resident_occupancy = Some(ResidentOccupancyEvidence {
            process_count: 1,
            reservation: ResourceReservation {
                memory_bytes: 4_000,
                disk_growth_bytes: 8_000,
                concurrency_units: 5,
                source: ResourceReservationSource::Declared,
            },
        });
        pool.connections
            .lock()
            .unwrap()
            .extend([("first".into(), first), ("second".into(), second)]);

        let occupancy = pool.snapshot().resident_occupancy.unwrap();
        assert_eq!(occupancy.process_count, 3);
        assert_eq!(occupancy.reservation.memory_bytes, 5_000);
        assert_eq!(occupancy.reservation.disk_growth_bytes, 10_000);
        assert_eq!(occupancy.reservation.concurrency_units, 8);
    }

    #[test]
    fn cached_completion_history_is_explicit_bounded_and_newest_first() {
        let pool = Fleet::default();
        for index in 0..40 {
            pool.record_cached_completion(
                "project",
                "job",
                Some("executor"),
                &format!("check-{index}"),
                CellPriority::ReviewCheck,
                true,
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        let recent = pool.snapshot().recent_completions;
        assert_eq!(recent.len(), 32);
        assert_eq!(recent[0].command, "check-39");
        assert!(recent[0].cached);
        assert_eq!(recent[0].duration_ms, 0);
        assert!(recent[0].resource_reservation.is_none());
    }

    #[test]
    fn executor_health_keeps_stale_executor_with_capture_time_age() {
        let pool = Fleet::default();
        let (executor_id, connection) = fleet_entry("stale", "macos", 0, &[]);
        pool.connections
            .lock()
            .unwrap()
            .insert(executor_id, connection);

        let health = pool.executor_health(120_001);
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].identity.executor_id, "stale");
        assert_eq!(health[0].status, ExecutorHealthStatus::Stale);
        assert_eq!(health[0].heartbeat_age_ms, 120_000);
    }

    /// A machine whose readings are fresh but whose link has gone silent is a
    /// connection problem, and the snapshot keeps the last measurements it was
    /// given rather than discarding them.
    #[test]
    fn a_stale_connection_keeps_the_measurements_it_last_carried() {
        let pool = Fleet::default();
        let (executor_id, connection) = fleet_entry("gone-quiet", "macos", 0, &[]);
        let mut advertisement = connection.advertisement.clone();
        pool.connections
            .lock()
            .unwrap()
            .insert(executor_id.clone(), connection);

        let beat_at = 1_000_000;
        advertisement.observed_at_unix_ms = beat_at;
        advertisement.liveness_observed_at_unix_ms = Some(beat_at);
        let mut health = ExecutorSubstrateReport::default();
        health.machine.memory = Measurement::measured(
            beat_at,
            MachineMemory {
                total_bytes: 32_000,
                available_bytes: 12_000,
            },
        );
        assert!(pool.handle_executor_message(
            &executor_id,
            1,
            ExecutorMessage::Heartbeat {
                advertisement,
                health,
            },
        ));

        let silent = pool.executor_health(beat_at + 120_000).remove(0);
        assert_eq!(silent.status, ExecutorHealthStatus::Stale);
        assert!(
            silent.telemetry_stale,
            "a link that stopped delivering also stopped delivering fresh facts"
        );
        assert_eq!(
            silent.machine.memory.value().unwrap().available_bytes,
            12_000,
            "the last measurement is still the last measurement"
        );
        assert_eq!(
            silent.machine.memory.age_ms(beat_at + 120_000),
            120_000,
            "its age is computed from when it was taken, not from the beat"
        );
    }

    #[test]
    fn executor_health_separates_the_runner_own_executor_from_enrolled_ones() {
        let pool = Fleet::default();
        let (colocated_sender, _colocated_receiver) = mpsc::unbounded_channel();
        pool.attach_executor(colocated_sender);
        let (_, managed) = fleet_entry("managed", "linux", 0, &[]);
        pool.attach_advertised_executor(managed.advertisement, managed.sender, false, None);

        let attribution: Vec<_> = pool
            .executor_health(1)
            .into_iter()
            .map(|executor| (executor.identity.executor_id, executor.colocated))
            .collect();
        assert_eq!(
            attribution,
            vec![
                (COLOCATED_EXECUTOR_ID.to_string(), true),
                ("managed".to_string(), false),
            ]
        );
    }

    #[test]
    fn build_skew_compares_the_running_executor_to_the_runner_deployed_artifact() {
        let pool = Fleet::default();
        let (executor_id, mut connection) = fleet_entry("colocated", "macos", 0, &[]);
        connection.executor_build_id = Some("running-build".into());
        pool.connections
            .lock()
            .unwrap()
            .insert(executor_id.clone(), connection);

        assert!(pool.executor_health(1)[0].build_skew.is_none());
        pool.set_expected_executor_build_id(executor_id, "deployed-build".into());
        let skew = pool.executor_health(1)[0].build_skew.clone().unwrap();
        assert_eq!(skew.runner_build_id, "deployed-build");
        assert_eq!(skew.executor_build_id, "running-build");
    }

    #[test]
    fn heartbeat_refreshes_live_executor_health_report() {
        let pool = Fleet::default();
        let (executor_id, connection) = fleet_entry("live", "macos", 0, &[]);
        let mut advertisement = connection.advertisement.clone();
        pool.connections
            .lock()
            .unwrap()
            .insert(executor_id.clone(), connection);

        let mut first = ExecutorSubstrateReport::default();
        first.machine.memory = Measurement::measured(
            1,
            MachineMemory {
                total_bytes: 8_000,
                available_bytes: 4_000,
            },
        );
        assert!(pool.handle_executor_message(
            &executor_id,
            1,
            ExecutorMessage::Heartbeat {
                advertisement: advertisement.clone(),
                health: first,
            },
        ));
        assert_eq!(
            pool.executor_health(1)[0]
                .machine
                .memory
                .value()
                .unwrap()
                .available_bytes,
            4_000
        );

        advertisement.observed_at_unix_ms = 2;
        let mut second = ExecutorSubstrateReport::default();
        second.machine.memory = Measurement::measured(
            2,
            MachineMemory {
                total_bytes: 8_000,
                available_bytes: 2_000,
            },
        );
        second.disk.status = cairn_common::executor_protocol::DiskHealthStatus::Full;
        assert!(pool.handle_executor_message(
            &executor_id,
            1,
            ExecutorMessage::Heartbeat {
                advertisement,
                health: second,
            },
        ));
        let health = pool.executor_health(2);
        assert_eq!(
            health[0].machine.memory.value().unwrap().available_bytes,
            2_000
        );
        assert_eq!(
            health[0].disk.status,
            cairn_common::executor_protocol::DiskHealthStatus::Full
        );
    }

    /// The price of emitting beats from a task that computes nothing is that a
    /// wedged producer keeps beating. A beat arriving on schedule while the
    /// facts it carries were measured ten minutes ago is not evidence of a
    /// healthy executor, and the snapshot has to say so on the payload's age
    /// alone. It must also keep reporting the heartbeat itself as fresh,
    /// because the link genuinely is: conflating the two would make every
    /// healthy executor look stale to the connection-health surfaces.
    #[test]
    fn a_beat_on_schedule_carrying_facts_that_stopped_moving_is_not_online() {
        let pool = Fleet::default();
        let (executor_id, connection) = fleet_entry("wedged", "macos", 0, &[]);
        let mut advertisement = connection.advertisement.clone();
        pool.connections
            .lock()
            .unwrap()
            .insert(executor_id.clone(), connection);

        let beat_at = 1_000_000;
        advertisement.observed_at_unix_ms = beat_at;
        advertisement.liveness_observed_at_unix_ms = Some(beat_at);
        assert!(pool.handle_executor_message(
            &executor_id,
            1,
            ExecutorMessage::Heartbeat {
                advertisement: advertisement.clone(),
                health: ExecutorSubstrateReport::default(),
            },
        ));
        let fresh = pool.executor_health(beat_at).remove(0);
        assert_eq!(fresh.status, ExecutorHealthStatus::Online);
        assert_eq!(fresh.liveness_age_ms, Some(0));

        // The publisher keeps its cadence. The refresher does not.
        let much_later = beat_at + 10 * 60 * 1_000;
        advertisement.observed_at_unix_ms = much_later;
        assert!(pool.handle_executor_message(
            &executor_id,
            1,
            ExecutorMessage::Heartbeat {
                advertisement,
                health: ExecutorSubstrateReport::default(),
            },
        ));
        let wedged = pool.executor_health(much_later).remove(0);
        assert_eq!(
            wedged.heartbeat_age_ms, 0,
            "the link is alive and has to keep reading as alive"
        );
        assert_eq!(wedged.liveness_age_ms, Some(10 * 60 * 1_000));
        assert_eq!(
            wedged.status,
            ExecutorHealthStatus::Online,
            "the connection is not what failed here, and saying it did sends an \
             operator after the wrong problem"
        );
        assert!(
            wedged.telemetry_stale,
            "facts that stopped moving have to be reported as stale facts"
        );
    }

    /// An executor that makes no claim about when its facts were measured is
    /// judged on its heartbeat alone. Absence of the claim is not evidence of
    /// infinite staleness, or every executor predating the field would read as
    /// permanently wedged.
    #[test]
    fn an_executor_that_reports_no_payload_age_is_judged_on_its_heartbeat() {
        let pool = Fleet::default();
        let (executor_id, connection) = fleet_entry("silent-about-age", "macos", 0, &[]);
        let mut advertisement = connection.advertisement.clone();
        pool.connections
            .lock()
            .unwrap()
            .insert(executor_id.clone(), connection);

        let beat_at = 1_000_000;
        advertisement.observed_at_unix_ms = beat_at;
        advertisement.liveness_observed_at_unix_ms = None;
        assert!(pool.handle_executor_message(
            &executor_id,
            1,
            ExecutorMessage::Heartbeat {
                advertisement,
                health: ExecutorSubstrateReport::default(),
            },
        ));
        let health = pool.executor_health(beat_at).remove(0);
        assert_eq!(health.liveness_age_ms, None);
        assert_eq!(health.status, ExecutorHealthStatus::Online);
        assert!(!health.telemetry_stale);
    }

    #[test]
    fn repeated_executor_snapshot_refreshes_health_without_reporting_slot_change() {
        let pool = Fleet::default();
        let (executor_id, connection) = fleet_entry("snapshot", "macos", 0, &[]);
        pool.connections
            .lock()
            .unwrap()
            .insert(executor_id.clone(), connection);

        let snapshot = FleetSnapshot {
            resident_occupancy: Some(ResidentOccupancyEvidence {
                process_count: 1,
                reservation: ResourceReservation::default(),
            }),
            ..FleetSnapshot::default()
        };
        let mut first_health = ExecutorSubstrateReport::default();
        first_health.machine.memory = Measurement::measured(
            1,
            MachineMemory {
                total_bytes: 8_000,
                available_bytes: 4_000,
            },
        );
        assert!(pool.set_executor_snapshot(&executor_id, 1, snapshot.clone(), first_health));

        let mut second_health = ExecutorSubstrateReport::default();
        second_health.machine.memory = Measurement::measured(
            2,
            MachineMemory {
                total_bytes: 8_000,
                available_bytes: 2_000,
            },
        );
        assert!(!pool.set_executor_snapshot(&executor_id, 1, snapshot, second_health));
        assert_eq!(
            pool.executor_health(1)[0]
                .machine
                .memory
                .value()
                .unwrap()
                .available_bytes,
            2_000
        );
    }

    #[test]
    fn empty_first_snapshot_reconciles_stale_persisted_route_without_public_change() {
        let temp = tempfile::tempdir().unwrap();
        let route_path = temp.path().join("lifetime-routes.json");
        let pool = Fleet::with_residency_route_path(route_path.clone());
        let executor_id = "snapshot".to_string();
        let request = dev_instance_request("stale-lease");
        pool.update_residency_routes(|known| {
            known.insert(
                (executor_id.clone(), request.holder.storage_key()),
                ResidencyRoute {
                    holder: request.holder.clone(),
                    repository: request.repository.clone(),
                    executor_id: executor_id.clone(),
                    pending: false,
                },
            );
        })
        .unwrap();

        let (_, connection) = fleet_entry(&executor_id, "macos", 0, &[]);
        let generation = pool.attach_advertised_executor(
            connection.advertisement,
            connection.sender,
            false,
            None,
        );
        assert!(!pool.set_executor_snapshot(
            &executor_id,
            generation,
            FleetSnapshot::default(),
            ExecutorSubstrateReport::default(),
        ));
        assert!(pool.residency_routes.lock().unwrap().is_empty());

        drop(pool);
        assert!(Fleet::with_residency_route_path(route_path)
            .residency_routes
            .lock()
            .unwrap()
            .is_empty());
    }

    fn executing(command: &str, upper_duration_ms: u64) -> ExecutingCellRequest {
        ExecutingCellRequest {
            executor_id: String::new(),
            cell_id: "cell".into(),
            request_id: command.into(),
            attempt_id: "attempt".into(),
            owner: None,
            command_class: cairn_common::executor_protocol::CellCommandClass::CargoTest,
            command: command.into(),
            started_at_unix_ms: NOW,
            process_ids: Vec::new(),
            priority: None,
            subscriber_count: 1,
            resource_reservation: ResourceReservation::default(),
            command_resource_identity: None,
            learned_estimate: Some(cairn_common::executor_protocol::LearnedResourceEstimate {
                sample_count: 20,
                upper_duration_ms: Some(upper_duration_ms),
                upper_peak_rss_bytes: None,
                upper_disk_growth_bytes: None,
            }),
        }
    }

    const FORECAST_PROJECT: &str = "p";

    /// A colocated macOS machine that frees in four seconds, beside a remote
    /// Linux one that does not free for five minutes.
    fn occupied_fleet() -> Fleet {
        let pool = Fleet::default();
        for (id, os, colocated, occupant) in [
            ("local", "macos", true, executing("rust-fmt", 4_000)),
            ("bglab-ub", "linux", false, executing("rust-tests", 300_000)),
        ] {
            let (executor_id, mut connection) = fleet_entry(id, os, 0, &[]);
            connection.colocated = colocated;
            connection.snapshot.executing_requests = vec![occupant];
            pool.connections
                .lock()
                .unwrap()
                .insert(executor_id, connection);
        }
        pool
    }

    /// A colocated check batch's scope: mobile, unpinned, over a colocated
    /// checkout that can be recreated from managed objects elsewhere.
    fn forecast_scope<'a>(
        repository: &'a RepositoryLocator,
        selector: Option<&'a ExecutorSelector>,
        mobility: PlacementMobility,
    ) -> PlacementScope<'a> {
        PlacementScope {
            project_id: FORECAST_PROJECT,
            repository,
            selector,
            pinned_executor_id: None,
            mobility,
            verdict_platforms: &[],
        }
    }

    fn forecast_repository() -> RepositoryLocator {
        RepositoryLocator::ColocatedPath {
            project_id: FORECAST_PROJECT.into(),
            repository_id: FORECAST_PROJECT.into(),
            absolute_path: "/repo".into(),
        }
    }

    /// Forecast fixtures share one synthetic instant for connection freshness,
    /// occupant start time, and occupancy evaluation. Exact relief therefore
    /// identifies which machine spoke without elapsed wall time making the
    /// assertion scheduler-dependent.
    #[track_caller]
    fn assert_relief_near(
        occupancy: &occupancy::MachineOccupancy,
        expected_ms: u64,
        because: &str,
    ) {
        let occupancy::MachineOccupancy::Predicted(forecast) = occupancy else {
            panic!("{because}: expected a prediction, got {occupancy:?}");
        };
        assert_eq!(
            forecast.relief_ms, expected_ms,
            "{because}: expected relief at {expected_ms}ms"
        );
    }

    /// A machine the work cannot land on says nothing about when the work can
    /// start.
    ///
    /// The specimen: a Linux-targeted check whose only eligible machine is busy
    /// for five minutes, beside a colocated macOS machine that frees in four
    /// seconds. Scoping the forecast by mobility alone would hand that check a
    /// four-second reading, clamp its wait to the floor, surface it as
    /// unavailable while its real blocker was still finite, and name the macOS
    /// work on the row — which is the whole failure this policy exists to end.
    #[test]
    fn a_forecast_ignores_machines_the_work_could_never_land_on() {
        let pool = occupied_fleet();
        let repository = forecast_repository();
        let scope =
            |selector| forecast_scope(&repository, selector, PlacementMobility::SpillEligible);
        let linux = ExecutorSelector {
            os: Some("linux".into()),
            ..ExecutorSelector::default()
        };
        assert_relief_near(
            &pool.occupancy_for_at(scope(Some(&linux)), NOW),
            300_000,
            "only the machine that can run it speaks for it",
        );
        assert_relief_near(
            &pool.occupancy_for_at(scope(None), NOW),
            4_000,
            "an unconstrained batch is genuinely relieved by whichever frees first",
        );

        let named = ExecutorSelector {
            name: Some("bglab-ub".into()),
            ..ExecutorSelector::default()
        };
        assert_relief_near(
            &pool.occupancy_for_at(scope(Some(&named)), NOW),
            300_000,
            "naming a machine is asking about that machine",
        );

        let unsatisfiable = ExecutorSelector {
            required_toolchains: vec!["matlab".into()],
            ..ExecutorSelector::default()
        };
        assert_eq!(
            pool.occupancy_for_at(scope(Some(&unsatisfiable)), NOW),
            occupancy::MachineOccupancy::Unforecastable,
            "no eligible machine is no knowledge, never an accidental prediction"
        );
    }

    /// Selector matching is not eligibility, and reading it as though it were is
    /// the same bug in a costume.
    ///
    /// Here the four-second machine satisfies every selector the work could
    /// state and is still ineligible: it does not serve this project, or its
    /// link has closed. Both are facts `candidate_rejection` knows and a
    /// selector check cannot see, and either one alone would have the forecast
    /// predict relief from a machine the work will never reach — clamping the
    /// wait to the floor and printing that machine's occupant on the row.
    /// Two remote machines that satisfy every selector this work could state:
    /// `fast` frees in four seconds, `slow` in five minutes.
    fn two_remotes() -> Fleet {
        let pool = Fleet::default();
        for (id, occupant) in [
            ("fast", executing("rust-fmt", 4_000)),
            ("slow", executing("rust-tests", 300_000)),
        ] {
            let (executor_id, mut connection) = fleet_entry(id, "linux", 0, &[]);
            connection.colocated = false;
            connection.snapshot.executing_requests = vec![occupant];
            pool.connections
                .lock()
                .unwrap()
                .insert(executor_id, connection);
        }
        pool
    }

    /// Selector matching is not eligibility, and reading it as though it were is
    /// the same bug in a costume.
    ///
    /// Here the four-second machine satisfies every selector the work could
    /// state and is still ineligible: it does not serve this project, or its
    /// link has closed. Both are facts `candidate_rejection` knows and a
    /// selector check cannot see, and either one alone would have the forecast
    /// predict relief from a machine the work will never reach — clamping the
    /// wait to the floor and printing that machine's occupant on the row.
    #[test]
    fn a_forecast_reads_only_machines_placement_itself_would_keep() {
        let repository = forecast_repository();
        let scope = forecast_scope(&repository, None, PlacementMobility::SpillEligible);

        assert_relief_near(
            &two_remotes().occupancy_for_at(scope, NOW),
            4_000,
            "with both eligible, the sooner relief is the honest answer",
        );

        let unserved = two_remotes();
        unserved
            .connections
            .lock()
            .unwrap()
            .get_mut("fast")
            .unwrap()
            .advertisement
            .capabilities
            .projects_served = vec!["another-project".into()];
        assert_relief_near(
            &unserved.occupancy_for_at(scope, NOW),
            300_000,
            "a machine that does not serve this project cannot relieve it",
        );

        let disconnected = two_remotes();
        {
            let mut connections = disconnected.connections.lock().unwrap();
            let (sender, receiver) = mpsc::unbounded_channel();
            drop(receiver);
            connections.get_mut("fast").unwrap().sender = sender;
        }
        assert_relief_near(
            &disconnected.occupancy_for_at(scope, NOW),
            300_000,
            "a machine whose link has closed cannot relieve anything",
        );
    }

    /// Mobility follows placement's own rule, which is about being UNtargeted.
    ///
    /// A pinned-or-colocated batch that named nothing stays home, so only the
    /// colocated machine speaks for it. The same batch that named a machine has
    /// said where it goes, and placement lets it leave home — so the forecast
    /// must follow it there rather than answering about a machine it is no
    /// longer headed for.
    #[test]
    fn a_colocated_default_binds_untargeted_work_and_releases_targeted_work() {
        let pool = occupied_fleet();
        let repository = forecast_repository();
        assert_relief_near(
            &pool.occupancy_for_at(
                forecast_scope(&repository, None, PlacementMobility::PinnedOrColocated),
                NOW,
            ),
            4_000,
            "untargeted conservative work stays on the machine holding the checkout",
        );

        let named = ExecutorSelector {
            name: Some("bglab-ub".into()),
            ..ExecutorSelector::default()
        };
        assert_relief_near(
            &pool.occupancy_for_at(
                forecast_scope(
                    &repository,
                    Some(&named),
                    PlacementMobility::PinnedOrColocated,
                ),
                NOW,
            ),
            300_000,
            "naming a machine is the permission to leave home, for placement and forecast alike",
        );
    }

    /// A checkout that exists on exactly one machine cannot be recreated
    /// elsewhere, so no remote machine's occupancy speaks for work that carries
    /// one — however mobile the batch declared itself.
    #[test]
    fn a_forecast_ignores_remotes_for_work_whose_checkout_cannot_travel() {
        let repository = RepositoryLocator::ExistingCheckout {
            project_id: FORECAST_PROJECT.into(),
            repository_id: FORECAST_PROJECT.into(),
            absolute_path: "/repo".into(),
        };
        assert_relief_near(
            &occupied_fleet().occupancy_for_at(
                forecast_scope(&repository, None, PlacementMobility::SpillEligible),
                NOW,
            ),
            4_000,
            "a checkout that cannot travel keeps the forecast at home",
        );
    }

    #[test]
    fn disconnect_is_generation_fenced_for_health_invalidation() {
        let pool = Fleet::default();
        let (executor_id, connection) = fleet_entry("disconnect", "macos", 0, &[]);
        pool.connections
            .lock()
            .unwrap()
            .insert(executor_id.clone(), connection);
        assert!(!pool.disconnect_advertised_executor(&executor_id, 2));
        assert!(pool.disconnect_advertised_executor(&executor_id, 1));
        assert!(!pool.disconnect_advertised_executor(&executor_id, 1));
    }

    fn fleet_residency_request(holder: ResidencyHolder) -> ResidencyAcquireRequest {
        ResidencyAcquireRequest {
            holder,
            executor: None,
            owner_ref: None,
            selector: None,
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repo".into(),
                absolute_path: "/repo".into(),
            },
            initial_base_commit: "base".into(),
            footprint: cairn_common::executor_protocol::ResidencyFootprint::default(),
            death_policy: cairn_common::executor_protocol::OwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
            priority: CellPriority::AgentInteractive,
            wait_horizon_unix_ms: 1,
            waiting_since_unix_ms: 0,
        }
    }

    fn dev_instance_request(instance_id: &str) -> ResidencyAcquireRequest {
        fleet_residency_request(ResidencyHolder::DevInstance {
            instance_id: instance_id.into(),
        })
    }

    fn fleet_residency(
        holder: ResidencyHolder,
        owner_ref: Option<cairn_common::executor_protocol::CellOwnerRef>,
    ) -> CellResidency {
        let request = fleet_residency_request(holder.clone());
        CellResidency {
            holder,
            repository: request.repository,
            owner_ref,
            selector: None,
            incarnation_id: "incarnation".into(),
            current_base_commit: "base".into(),
            phase: cairn_common::executor_protocol::ResidencyPhase::Active,
            last_heartbeat_unix_ms: 1,
            reclaim_deadline_unix_ms: 0,
            death_policy: request.death_policy,
            footprint: request.footprint,
            state_revision: 1,
            events: Vec::new(),
        }
    }

    fn materialization_read_cell(
        cell_id: &str,
        job_id: &str,
    ) -> cairn_common::executor_protocol::PersistentCellState {
        let residency = fleet_residency(
            ResidencyHolder::Job {
                job_id: job_id.into(),
            },
            Some(cairn_common::executor_protocol::CellOwnerRef {
                project_id: "p".into(),
                project_key: Some("cairn".into()),
                issue_number: Some(1),
                job_id: Some(job_id.into()),
                execution_seq: Some(1),
                node_kind: Some("builder".into()),
            }),
        );
        cairn_common::executor_protocol::PersistentCellState {
            warm_command_classes: Vec::new(),
            executor_id: String::new(),
            executor_display_name: None,
            project_id: "p".into(),
            cell_id: cell_id.into(),
            path: format!("/cells/{cell_id}"),
            workspace_name: cell_id.into(),
            repository: "repo".into(),
            checkout_kind: Default::default(),
            git_common_dir: None,
            authority_path: format!("/authority/{cell_id}"),
            lifecycle: cairn_common::executor_protocol::PersistentCellLifecycle::Running,
            cell_epoch: 7,
            last_sealed_commit: Some("base".into()),
            last_used_unix_ms: 1,
            last_affinity_key: None,
            preparation_fingerprint: Some("generation".into()),
            residency: Some(CellResidency {
                incarnation_id: format!("incarnation-{cell_id}"),
                ..residency
            }),
            occupancy: CellOccupancy::default(),
        }
    }

    #[test]
    fn materialization_read_selection_is_stable_across_snapshot_order() {
        let repository = dev_instance_request("identity").repository.identity();
        let select = |reverse: bool| {
            let pool = Fleet::default();
            let mut connection = fleet_entry("executor", "macos", 0, &[]).1;
            connection.snapshot.cells = vec![
                materialization_read_cell("cell-b", "job"),
                materialization_read_cell("cell-a", "job"),
            ];
            if reverse {
                connection.snapshot.cells.reverse();
            }
            pool.connections
                .lock()
                .unwrap()
                .insert("executor".into(), connection);
            pool.select_materialization_read_candidate("run", "job", "p", &repository, "base")
                .unwrap()
                .cell_id
        };
        assert_eq!(select(false), "cell-a");
        assert_eq!(select(true), "cell-a");
    }

    #[test]
    fn materialization_read_selection_rejects_owner_mismatch() {
        let pool = Fleet::default();
        let mut connection = fleet_entry("executor", "macos", 0, &[]).1;
        connection.snapshot.cells = vec![materialization_read_cell("cell", "different-job")];
        let repository = dev_instance_request("identity").repository.identity();
        pool.connections
            .lock()
            .unwrap()
            .insert("executor".into(), connection);
        assert!(matches!(
            pool.select_materialization_read_candidate("run", "job", "p", &repository, "base"),
            Err(MaterializationReadFailureKind::NoActiveMaterializationLease)
        ));
    }

    #[test]
    fn lifetime_route_authority_recovers_after_transient_persistence_failure() {
        let temp = tempfile::tempdir().unwrap();
        let blocked_parent = temp.path().join("blocked");
        std::fs::write(&blocked_parent, "not a directory").unwrap();
        let route_path = blocked_parent.join("lifetime-routes.json");
        let pool = Fleet::with_residency_route_path(route_path.clone());
        let route = ResidencyRoute {
            holder: ResidencyHolder::DevInstance {
                instance_id: "launcher".into(),
            },
            repository: dev_instance_request("launcher").repository,
            executor_id: "first".into(),
            pending: true,
        };

        let initial_error = pool.reserve_pending_residency_route(route).unwrap_err();
        assert!(initial_error.contains("residency route authority"));
        let mut request = dev_instance_request("new-instance");
        assert!(matches!(
            pool.resolve_residency_acquire_route(&mut request),
            Err(ResidencyResult::Failed {
                kind: ResidencyFailureKind::Persistence,
                ..
            })
        ));

        std::fs::remove_file(&blocked_parent).unwrap();
        std::fs::create_dir(&blocked_parent).unwrap();

        assert!(pool
            .resolve_residency_acquire_route(&mut request)
            .unwrap()
            .is_none());
        assert!(route_path.is_file());
        assert!(pool.residency_route_store_error.lock().unwrap().is_none());
    }

    #[test]
    fn fleet_lifetime_retry_routes_to_original_executor() {
        let pool = Fleet::default();
        let first = fleet_entry("first", "linux", 10, &[]);
        let second = fleet_entry("second", "linux", 0, &[]);
        pool.connections.lock().unwrap().extend([first, second]);
        let mut request = dev_instance_request("launcher");
        // The executor holds the same repository by identity, addressed as
        // managed objects because it is not colocated.
        let persisted_repository = RepositoryLocator::ManagedObjects {
            project_id: "p".into(),
            repository_id: "repo".into(),
            object_format: GitObjectFormat::Sha1,
        };
        pool.residency_routes.lock().unwrap().insert(
            ("first".into(), request.holder.storage_key()),
            ResidencyRoute {
                holder: request.holder.clone(),
                repository: persisted_repository.clone(),
                executor_id: "first".into(),
                pending: false,
            },
        );
        let selected = pool
            .resolve_residency_acquire_route(&mut request)
            .unwrap()
            .unwrap();
        assert_eq!(selected.executor_id, "first");
        assert_eq!(request.repository, persisted_repository);
    }

    #[tokio::test]
    async fn executor_restart_reconfigures_a_reacquired_live_residency() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let repository_path = temp.path().join("repository");
        std::fs::create_dir_all(&repository_path).unwrap();
        orch.db
            .local
            .execute_script(&format!(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) \
                 VALUES ('p', 'default', 'Project', 'p', '{}', 1, 1);",
                repository_path.to_string_lossy()
            ))
            .await
            .unwrap();
        let holder = ResidencyHolder::Job {
            job_id: "survives-restart".into(),
        };
        let repository = RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "p".into(),
            absolute_path: repository_path.to_string_lossy().into_owned(),
        };
        let stale_incoming_repository = RepositoryLocator::ExistingCheckout {
            project_id: "p".into(),
            repository_id: "p".into(),
            absolute_path: "/stale/caller/coordinate".into(),
        };
        assert_eq!(repository.identity(), stale_incoming_repository.identity());

        let (first_sender, first_executor) = mpsc::unbounded_channel();
        let first_generation = orch.fleet.attach_executor(first_sender);
        orch.fleet.residency_routes.lock().unwrap().insert(
            (COLOCATED_EXECUTOR_ID.into(), holder.storage_key()),
            ResidencyRoute {
                holder: holder.clone(),
                repository: repository.clone(),
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                pending: false,
            },
        );
        drop(first_executor);

        let (replacement_sender, mut replacement_executor) = mpsc::unbounded_channel();
        let replacement_generation = orch.fleet.attach_executor(replacement_sender);
        assert!(replacement_generation > first_generation);

        let mut cell = materialization_read_cell("resident-cell", "survives-restart");
        cell.project_id = "p".into();
        cell.residency.as_mut().unwrap().repository = repository.clone();
        assert!(orch.fleet.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            replacement_generation,
            FleetSnapshot {
                cells: vec![cell.clone()],
                ..FleetSnapshot::default()
            },
            ExecutorSubstrateReport::default(),
        ));
        let answering_fleet = orch.fleet.clone();
        let answer = tokio::spawn(async move {
            // Attach states the runner's drain posture before any work arrives,
            // so the re-acquire is not necessarily the first frame. Skipping
            // what attach says keeps this test about atomicity of config with
            // re-acquire rather than about frame position.
            let (correlation_id, config) = loop {
                match replacement_executor.recv().await {
                    Some(ExecutorMessage::DrainModeRequest { .. }) => continue,
                    Some(ExecutorMessage::ResidencyRequest {
                        correlation_id,
                        config: ResidencyRuntimeConfig::Install(config),
                        operation: ResidencyOperation::Acquire { .. },
                    }) => break (correlation_id, config),
                    other => panic!(
                        "a restarted executor must receive config atomically with re-acquire, got {other:?}"
                    ),
                }
            };
            assert_eq!(config.project_id, "p");
            assert_eq!(config.project_key, "p");
            answering_fleet.handle_executor_message(
                COLOCATED_EXECUTOR_ID,
                replacement_generation,
                ExecutorMessage::ResidencyResponse {
                    correlation_id,
                    result: ResidencyResult::State { cell },
                },
            );
        });

        let mut request = fleet_residency_request(holder);
        request.repository = stale_incoming_repository;
        request.wait_horizon_unix_ms = unix_time_ms().saturating_add(5_000);
        let result = orch
            .fleet
            .operate_residency(&orch, ResidencyOperation::Acquire { request })
            .await;
        answer.await.unwrap();
        assert!(matches!(result, ResidencyResult::State { .. }));
    }

    /// The incident end to end at the fleet's own entry point: a job whose
    /// environment was on a machine that is no longer connected acquires again,
    /// lands on a live machine, and leaves the route authority naming only that
    /// machine. Before this, the same call returned `Admission` — "the executor
    /// holding this execution environment is disconnected" — on every batch for
    /// the rest of the job's life (CAIRN-4332).
    #[tokio::test]
    async fn a_job_whose_machine_is_gone_acquires_again_on_a_live_one() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let repository_path = temp.path().join("repository");
        std::fs::create_dir_all(&repository_path).unwrap();
        orch.db
            .local
            .execute_script(&format!(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at) \
                 VALUES ('p', 'default', 'Project', 'p', '{}', 1, 1);",
                repository_path.to_string_lossy()
            ))
            .await
            .unwrap();
        let holder = ResidencyHolder::Job {
            job_id: "stranded-by-a-departed-machine".into(),
        };
        let repository = RepositoryLocator::ColocatedPath {
            project_id: "p".into(),
            repository_id: "p".into(),
            absolute_path: repository_path.to_string_lossy().into_owned(),
        };
        orch.fleet.residency_routes.lock().unwrap().insert(
            ("departed".into(), holder.storage_key()),
            ResidencyRoute {
                holder: holder.clone(),
                repository: repository.clone(),
                executor_id: "departed".into(),
                pending: false,
            },
        );
        machine_left(&orch.fleet, "departed");

        let (sender, mut executor) = mpsc::unbounded_channel();
        let generation = orch.fleet.attach_executor(sender);
        assert!(matches!(
            executor.recv().await,
            Some(ExecutorMessage::DrainModeRequest { enabled: false, .. })
        ));
        let mut cell = materialization_read_cell("rebuilt-cell", "stranded-by-a-departed-machine");
        cell.project_id = "p".into();
        cell.residency.as_mut().unwrap().repository = repository.clone();
        let answering_fleet = orch.fleet.clone();
        let answered = cell.clone();
        let answer = tokio::spawn(async move {
            let Some(ExecutorMessage::ResidencyRequest {
                correlation_id,
                operation: ResidencyOperation::Acquire { .. },
                ..
            }) = executor.recv().await
            else {
                panic!("a rebuilt environment must be acquired on the live executor");
            };
            answering_fleet.handle_executor_message(
                COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::ResidencyResponse {
                    correlation_id,
                    result: ResidencyResult::State { cell: answered },
                },
            );
        });

        let mut request = fleet_residency_request(holder.clone());
        request.repository = repository;
        request.wait_horizon_unix_ms = unix_time_ms().saturating_add(5_000);
        let result = orch
            .fleet
            .operate_residency(&orch, ResidencyOperation::Acquire { request })
            .await;
        answer.await.unwrap();

        assert!(matches!(result, ResidencyResult::State { .. }));
        let routes = orch.fleet.residency_routes.lock().unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(
            routes.values().next().unwrap().executor_id,
            COLOCATED_EXECUTOR_ID
        );
        drop(routes);
        assert_eq!(
            orch.fleet.take_environment_eviction(&holder),
            Some(EnvironmentEviction {
                executor: "departed".into()
            })
        );
    }

    #[tokio::test]
    async fn executor_restart_configures_a_cell_in_the_submission_that_consumes_it() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let (first_sender, first_executor) = mpsc::unbounded_channel();
        let first_generation = orch.fleet.attach_executor(first_sender);
        drop(first_executor);

        let (replacement_sender, mut replacement_executor) = mpsc::unbounded_channel();
        let replacement_generation = orch.fleet.attach_executor(replacement_sender);
        assert!(replacement_generation > first_generation);

        let answering_fleet = orch.fleet.clone();
        let answer = tokio::spawn(async move {
            // Attach states the runner's drain posture before any work is sent,
            // so the submission is not necessarily the first frame. Skipping
            // what attach says rather than asserting on frame position keeps
            // this test about the claim it makes — that a submission carries the
            // runtime config for the cell that consumes it — and keeps a future
            // attach frame from turning it red again.
            let (config, request) = loop {
                match replacement_executor.recv().await {
                    Some(ExecutorMessage::DrainModeRequest { .. }) => continue,
                    Some(ExecutorMessage::Submit {
                        config,
                        request,
                        batch: None,
                    }) => break (config, request),
                    other => {
                        panic!("a cell must carry runtime config in its submission, got {other:?}")
                    }
                }
            };
            assert_eq!(config.project_id, request.project_id);
            answering_fleet.handle_executor_message(
                COLOCATED_EXECUTOR_ID,
                replacement_generation,
                ExecutorMessage::Result {
                    request_id: request.request_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    outcome: CellOutcome::Cancelled {
                        request_id: request.request_id,
                        attempt_id: request.attempt_id,
                    },
                },
            );
        });

        let mut request = provenance_request();
        request.repository = RepositoryLocator::ScratchOnly {
            owner_id: request.project_id.clone(),
        };
        let result = orch.fleet.submit(&orch, request).await;
        answer.await.unwrap();
        assert!(matches!(result, CellOutcome::Cancelled { .. }));
    }

    /// A pending route records that an acquisition may have reached an executor
    /// the runner never heard back from, and while that executor answers, the
    /// record is worth honoring: the cell might be there, and placing a second
    /// one would split the owner across two environments.
    ///
    /// Once the connection is gone the record names a session that ended. Holding
    /// the owner to it is what left jobs unable to run anywhere at all, because
    /// nothing else ever removes such a route.
    #[test]
    fn a_pending_route_binds_its_owner_only_while_that_executor_answers() {
        let pool = Fleet::default();
        pool.connections.lock().unwrap().extend([
            fleet_entry("first", "linux", 10, &[]),
            fleet_entry("second", "linux", 0, &[]),
        ]);
        let mut request = dev_instance_request("lease");
        pool.reserve_pending_residency_route(ResidencyRoute {
            holder: request.holder.clone(),
            repository: request.repository.clone(),
            executor_id: "first".into(),
            pending: true,
        })
        .unwrap();
        assert_eq!(
            pool.resolve_residency_acquire_route(&mut request)
                .unwrap()
                .unwrap()
                .executor_id,
            "first"
        );

        // Dispatch was accepted, but neither a response nor an occupant snapshot
        // arrived before the owning connection disappeared for good.
        pool.connections.lock().unwrap().remove("first");
        machine_left(&pool, "first");
        assert!(pool
            .resolve_residency_acquire_route(&mut request)
            .unwrap()
            .is_none());
        assert!(pool.residency_routes.lock().unwrap().is_empty());
    }

    /// Put an executor's link outage far enough in the past that the runner has
    /// stopped reading it as a flap.
    fn machine_left(pool: &Fleet, executor_id: &str) {
        pool.link_absence_since.lock().unwrap().insert(
            executor_id.into(),
            unix_time_ms().saturating_sub(ENVIRONMENT_LEASE_GRACE_MS + 1),
        );
    }

    /// The mistake the grace exists to prevent, and the reason absence alone
    /// cannot decide this: a transport that dropped leaves the executor process
    /// running and its cells intact, so rebuilding on the first missed
    /// connection would spend an agent's uncommitted files on a network blip.
    /// The environment is held and the caller waits; only a machine still
    /// unreachable when the grace runs out is treated as gone.
    #[test]
    fn a_link_that_dropped_is_not_yet_a_machine_that_left() {
        let pool = Fleet::default();
        pool.connections.lock().unwrap().extend([
            fleet_entry("first", "linux", 0, &[]),
            fleet_entry("second", "linux", 0, &[]),
        ]);
        let mut request = fleet_residency_request(ResidencyHolder::Job {
            job_id: "flapping".into(),
        });
        pool.residency_routes.lock().unwrap().insert(
            ("first".into(), request.holder.storage_key()),
            ResidencyRoute {
                holder: request.holder.clone(),
                repository: request.repository.clone(),
                executor_id: "first".into(),
                pending: false,
            },
        );

        // The link goes down the way an ordinary peer or I/O close takes it.
        assert!(pool.disconnect_advertised_executor("first", 1));
        let refusal = pool
            .resolve_residency_acquire_route(&mut request)
            .unwrap_err();
        let ResidencyResult::Failed {
            kind, cell_outcome, ..
        } = &refusal
        else {
            panic!("a machine that may still answer must refuse with a classified failure");
        };
        assert!(placement::classify_residency_failure(
            kind,
            cell_outcome.as_deref(),
            pool.residency_link_restoration(&request.holder)
        )
        .is_capacity());
        assert_eq!(pool.residency_routes.lock().unwrap().len(), 1);
        assert!(pool.take_environment_eviction(&request.holder).is_none());

        // The same outage, once it has outlasted the grace.
        machine_left(&pool, "first");
        assert!(pool
            .resolve_residency_acquire_route(&mut request)
            .unwrap()
            .is_none());
        assert!(pool.residency_routes.lock().unwrap().is_empty());
        assert!(pool.take_environment_eviction(&request.holder).is_some());
    }

    /// A runner restart carries the route authority forward into a process where
    /// nothing is connected yet, which is exactly when a route is a claim about
    /// the past rather than a fact about the present. It binds an owner only
    /// while the machine it names is there to hold the environment, and the
    /// eviction is persisted so the next restart does not resurrect it.
    #[test]
    fn runner_restart_does_not_bind_an_owner_to_a_machine_that_never_returns() {
        let temp = tempfile::tempdir().unwrap();
        let route_path = temp.path().join("lifetime-routes.json");
        let first = Fleet::with_residency_route_path(route_path.clone());
        let request = dev_instance_request("lease-restart");
        first
            .reserve_pending_residency_route(ResidencyRoute {
                holder: request.holder.clone(),
                repository: request.repository.clone(),
                executor_id: "first".into(),
                pending: true,
            })
            .unwrap();
        drop(first);

        let replacement = Fleet::with_residency_route_path(route_path.clone());
        replacement
            .connections
            .lock()
            .unwrap()
            .insert("second".into(), fleet_entry("second", "linux", 0, &[]).1);
        let mut retry = request;
        machine_left(&replacement, "first");
        assert!(replacement
            .resolve_residency_acquire_route(&mut retry)
            .unwrap()
            .is_none());
        assert!(replacement.residency_routes.lock().unwrap().is_empty());
        assert!(Fleet::with_residency_route_path(route_path)
            .residency_routes
            .lock()
            .unwrap()
            .is_empty());
    }

    /// The whole of CAIRN-4332 in its smallest shape: an owner whose environment
    /// was on a machine that is gone is placed again on a machine that is here,
    /// rather than refused for as long as the route survives — which was
    /// forever, since only that machine's own snapshots could have retired it.
    #[test]
    fn an_environment_whose_executor_is_gone_is_rebuilt_rather_than_refused() {
        let pool = Fleet::default();
        pool.connections
            .lock()
            .unwrap()
            .insert("second".into(), fleet_entry("second", "linux", 0, &[]).1);
        let mut request = fleet_residency_request(ResidencyHolder::Job {
            job_id: "stranded".into(),
        });
        pool.residency_routes.lock().unwrap().insert(
            ("first".into(), request.holder.storage_key()),
            ResidencyRoute {
                holder: request.holder.clone(),
                repository: request.repository.clone(),
                executor_id: "first".into(),
                pending: false,
            },
        );
        machine_left(&pool, "first");

        assert!(pool
            .resolve_residency_acquire_route(&mut request)
            .unwrap()
            .is_none());
        assert!(pool.residency_routes.lock().unwrap().is_empty());
        // The loss is a fact the agent is owed, exactly once: its next batch runs
        // in a fresh checkout, and anything only the old environment held is gone.
        assert_eq!(
            pool.take_environment_eviction(&request.holder),
            Some(EnvironmentEviction {
                executor: "first".into()
            })
        );
        assert!(pool.take_environment_eviction(&request.holder).is_none());
    }

    /// A supervised link that is bouncing is not a machine that is gone, and the
    /// difference decides opposite answers: the environment is seconds away, so
    /// the batch waits for it instead of abandoning a cell that is about to
    /// answer again. Only the runner's own restoration evidence buys that wait,
    /// and the moment it stops, the environment is rebuilt like any other.
    #[test]
    fn an_environment_waits_out_a_colocated_link_the_runner_is_restoring() {
        let pool = Fleet::default();
        let mut request = fleet_residency_request(ResidencyHolder::Job {
            job_id: "bouncing".into(),
        });
        pool.residency_routes.lock().unwrap().insert(
            (COLOCATED_EXECUTOR_ID.into(), request.holder.storage_key()),
            ResidencyRoute {
                holder: request.holder.clone(),
                repository: request.repository.clone(),
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                pending: false,
            },
        );
        pool.declare_colocated_substrate(ExecutorSubstrateState::SupervisorRespawning);

        let refusal = pool
            .resolve_residency_acquire_route(&mut request)
            .unwrap_err();
        let ResidencyResult::Failed {
            kind, cell_outcome, ..
        } = &refusal
        else {
            panic!("a restoring link must refuse with a classified residency failure");
        };
        assert!(placement::classify_residency_failure(
            kind,
            cell_outcome.as_deref(),
            pool.residency_link_restoration(&request.holder)
        )
        .is_capacity());
        assert_eq!(pool.residency_routes.lock().unwrap().len(), 1);
        assert!(pool.take_environment_eviction(&request.holder).is_none());

        pool.clear_colocated_substrate();
        machine_left(&pool, COLOCATED_EXECUTOR_ID);
        assert!(pool
            .resolve_residency_acquire_route(&mut request)
            .unwrap()
            .is_none());
        assert!(pool.residency_routes.lock().unwrap().is_empty());
    }

    /// The other half of re-placement: a machine that comes back still holding an
    /// owner's old cell must not become a second home for it. Two routes for one
    /// owner is an ambiguity no acquisition can resolve, so the leftover is
    /// released and the live environment keeps the owner.
    #[test]
    fn a_returning_executor_releases_an_environment_that_moved_while_it_was_away() {
        let pool = Fleet::default();
        pool.connections
            .lock()
            .unwrap()
            .insert("second".into(), fleet_entry("second", "linux", 0, &[]).1);
        let holder = ResidencyHolder::Job {
            job_id: "moved".into(),
        };
        pool.residency_routes.lock().unwrap().insert(
            ("second".into(), holder.storage_key()),
            ResidencyRoute {
                holder: holder.clone(),
                repository: fleet_residency_request(holder.clone()).repository,
                executor_id: "second".into(),
                pending: false,
            },
        );

        let (returning, mut inbox) = mpsc::unbounded_channel();
        let (id, mut entry) = fleet_entry("first", "linux", 0, &[]);
        entry.sender = returning;
        let generation = entry.generation;
        pool.connections.lock().unwrap().insert(id, entry);
        assert!(pool.set_executor_snapshot(
            "first",
            generation,
            FleetSnapshot {
                cells: vec![materialization_read_cell("leftover-cell", "moved")],
                ..FleetSnapshot::default()
            },
            ExecutorSubstrateReport::default(),
        ));

        let routes = pool.residency_routes.lock().unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes.values().next().unwrap().executor_id, "second");
        drop(routes);
        assert!(matches!(
            inbox.try_recv(),
            Ok(ExecutorMessage::ResidencyRequest {
                operation: ResidencyOperation::Release { .. },
                ..
            })
        ));
    }

    /// A job home is where the job's work already is, and a machine that is gone
    /// is not that. Reading a dead route as occupancy is what measured a batch's
    /// selector against a machine nothing could run on.
    #[test]
    fn a_job_home_on_a_departed_machine_is_not_read_as_occupancy() {
        let pool = Fleet::default();
        pool.connections.lock().unwrap().extend([fleet_entry(
            COLOCATED_EXECUTOR_ID,
            "linux",
            0,
            &[],
        )]);
        let holder = ResidencyHolder::Job {
            job_id: "departed".into(),
        };
        pool.residency_routes.lock().unwrap().insert(
            ("first".into(), holder.storage_key()),
            ResidencyRoute {
                holder: holder.clone(),
                repository: fleet_residency_request(holder.clone()).repository,
                executor_id: "first".into(),
                pending: false,
            },
        );
        assert_eq!(
            pool.job_home_executor(&holder).as_deref(),
            Some(COLOCATED_EXECUTOR_ID)
        );
    }

    #[test]
    fn duplicate_fleet_lifetime_snapshots_fail_closed() {
        let pool = Fleet::default();
        pool.connections.lock().unwrap().extend([
            fleet_entry("first", "linux", 0, &[]),
            fleet_entry("second", "linux", 0, &[]),
        ]);
        let mut request = dev_instance_request("lease");
        let mut routes = pool.residency_routes.lock().unwrap();
        for executor_id in ["first", "second"] {
            routes.insert(
                (executor_id.into(), "lease".into()),
                ResidencyRoute {
                    holder: request.holder.clone(),
                    repository: request.repository.clone(),
                    executor_id: executor_id.into(),
                    pending: false,
                },
            );
        }
        drop(routes);
        assert!(matches!(
            pool.resolve_residency_acquire_route(&mut request),
            Err(ResidencyResult::Failed {
                kind: ResidencyFailureKind::ConflictingDeclaration,
                ..
            })
        ));
    }

    fn targeted_request(os: &str) -> CellRequest {
        CellRequest {
            request_id: "r".into(),
            attempt_id: "a".into(),
            project_id: "p".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repo".into(),
                absolute_path: "/repo".into(),
            },
            base_commit: "base".into(),
            command: "true".into(),
            command_class: cairn_common::executor_protocol::CellCommandClass::Other,
            placement_work_class:
                cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            wait_horizon_unix_ms: unix_time_ms() + 1_000,
            waiting_since_unix_ms: 0,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            executor: Some(ExecutorSelector {
                os: Some(os.into()),
                ..ExecutorSelector::default()
            }),
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        }
    }

    #[test]
    fn a_platform_selector_routes_only_to_matching_executor() {
        let connections = HashMap::from([
            fleet_entry("linux", "linux", 0, &[]),
            fleet_entry("windows", "windows", 0, &[]),
        ]);
        let selected = choose_executor(&connections, &targeted_request("windows"))
            .unwrap()
            .unwrap();
        assert_eq!(selected.executor_id, "windows");
    }

    #[test]
    fn transfer_estimation_does_not_hold_executor_connection_lock() {
        let pool = Fleet::default();
        let (id, entry) = fleet_entry("remote", "linux", 0, &[]);
        pool.connections.lock().unwrap().insert(id, entry);
        let request = targeted_request("linux");
        let selecting_pool = pool.clone();
        let (estimation_started_tx, estimation_started_rx) = std::sync::mpsc::channel();
        let (release_estimation_tx, release_estimation_rx) = std::sync::mpsc::channel();
        let selector = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(selecting_pool.select_executor_once_with(
                    &request,
                    None,
                    &ActivePlacementPolicy::default_profile(),
                    |_, _| {
                        estimation_started_tx.send(()).unwrap();
                        release_estimation_rx.recv().unwrap();
                        SyncCost::Unknown
                    },
                ))
                .unwrap()
        });
        estimation_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let updating_pool = pool.clone();
        let (updated_tx, updated_rx) = std::sync::mpsc::channel();
        let updater = std::thread::spawn(move || {
            let changed = updating_pool.set_executor_snapshot(
                "remote",
                1,
                FleetSnapshot::default(),
                ExecutorSubstrateReport::default(),
            );
            updated_tx.send(changed).unwrap();
        });
        let update_completed_while_estimation_blocked =
            updated_rx.recv_timeout(Duration::from_millis(250)).is_ok();
        release_estimation_tx.send(()).unwrap();
        selector.join().unwrap();
        updater.join().unwrap();

        assert!(
            update_completed_while_estimation_blocked,
            "executor WebSocket updates must remain responsive during repository estimation"
        );
    }

    #[test]
    fn transfer_estimation_rejects_a_reconnected_executor_generation() {
        let pool = Fleet::default();
        let (id, entry) = fleet_entry("remote", "linux", 0, &[]);
        pool.connections.lock().unwrap().insert(id, entry);
        let request = targeted_request("linux");
        let selecting_pool = pool.clone();
        let (estimation_started_tx, estimation_started_rx) = std::sync::mpsc::channel();
        let (release_estimation_tx, release_estimation_rx) = std::sync::mpsc::channel();
        let selector = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(selecting_pool.select_executor_once_with(
                    &request,
                    None,
                    &ActivePlacementPolicy::default_profile(),
                    |_, _| {
                        estimation_started_tx.send(()).unwrap();
                        release_estimation_rx.recv().unwrap();
                        SyncCost::Unknown
                    },
                ))
                .unwrap()
        });
        estimation_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let mut replacement = fleet_entry("remote", "linux", 0, &[]).1;
        replacement.generation = 2;
        pool.connections
            .lock()
            .unwrap()
            .insert("remote".into(), replacement);
        release_estimation_tx.send(()).unwrap();

        assert!(
            selector.join().unwrap().is_none(),
            "placement must not return a sender retired during repository estimation"
        );
        let selected = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(pool.select_executor_once_with(
                &targeted_request("linux"),
                None,
                &ActivePlacementPolicy::default_profile(),
                |_, _| SyncCost::Unknown,
            ))
            .unwrap()
            .unwrap()
            .selected;
        assert_eq!(selected.executor_id, "remote");
        assert_eq!(selected.generation, 2);
    }

    #[test]
    fn warm_executor_remains_eligible_as_inventory_grows() {
        let connections = HashMap::from([
            fleet_entry("cold", "linux", 0, &[]),
            fleet_entry("warm", "linux", 2, &["base"]),
        ]);
        assert_eq!(
            choose_executor(&connections, &targeted_request("linux"))
                .unwrap()
                .unwrap()
                .executor_id,
            "warm"
        );
    }

    #[test]
    fn missing_byte_cost_is_scoped_to_the_request_repository() {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        write_file(repo.path(), "base.txt", b"base");
        let warm = commit_all(repo.path(), "warm");
        write_file(repo.path(), "added.txt", b"request-specific bytes");
        let base = commit_all(repo.path(), "base");

        let cold =
            missing_reachable_object_bytes(repo.path().to_str().unwrap(), &base, &[]).unwrap();
        let incremental = missing_reachable_object_bytes(
            repo.path().to_str().unwrap(),
            &base,
            std::slice::from_ref(&warm),
        )
        .unwrap();
        assert!(incremental > 0);
        assert!(incremental < cold);

        let unrelated_repo = tempfile::tempdir().unwrap();
        init_repo(unrelated_repo.path());
        write_file(unrelated_repo.path(), "other.txt", b"other");
        let unrelated = commit_all(unrelated_repo.path(), "other");
        assert!(
            missing_reachable_object_bytes(repo.path().to_str().unwrap(), &base, &[unrelated],)
                .is_err()
        );
    }

    #[test]
    fn warm_root_is_zero_only_for_the_requested_repository() {
        let (_, mut entry) = fleet_entry("warm", "linux", 0, &["base"]);
        let request = targeted_request("linux");
        assert_eq!(repository_sync_cost(&request, &entry), SyncCost::Known(0));

        entry.advertisement.warm_roots[0].repository.repository_id = "other-repo".into();
        assert_eq!(repository_sync_cost(&request, &entry), SyncCost::Unknown);
    }

    #[test]
    fn known_missing_byte_cost_ranks_before_unknown() {
        let connections = HashMap::from([
            fleet_entry("known", "linux", 1, &[]),
            fleet_entry("unknown", "linux", 0, &[]),
        ]);
        let selected = choose_executor_with(
            &connections,
            &targeted_request("linux"),
            &HashMap::new(),
            |_, entry| {
                if entry.identity.executor_id == "known" {
                    SyncCost::Known(10)
                } else {
                    SyncCost::Unknown
                }
            },
            NOW,
        )
        .unwrap()
        .selected
        .unwrap();
        assert_eq!(selected.0.executor_id, "known");
    }

    #[test]
    fn unknown_cost_does_not_exclude_the_only_usable_executor() {
        let connections = HashMap::from([fleet_entry("only", "linux", 0, &[])]);
        let selected = choose_executor_with(
            &connections,
            &targeted_request("linux"),
            &HashMap::new(),
            |_, _| SyncCost::Unknown,
            NOW,
        )
        .unwrap()
        .selected
        .unwrap();
        assert_eq!(selected.0.executor_id, "only");
    }

    #[test]
    fn population_policy_never_routes_runner_local_source_to_remote_executor() {
        let connections = HashMap::from([
            fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 1, &[]),
            fleet_entry("remote", "linux", 0, &[]),
        ]);
        let mut request = targeted_request("linux");
        request.executor.as_mut().unwrap().name = Some("remote".into());
        let config = ExecutorConfig {
            project_id: "p".into(),
            project_key: "p".into(),
            default_timeout_seconds: 5,
            setup_commands: Vec::new(),
            populate: cairn_cell::PopulateConfig {
                copy: vec![".env".into()],
                symlink: Vec::new(),
            },
            population_source_root: Some("/runner/checkout".into()),
        };

        assert!(require_colocated_population(&mut request, &config)
            .unwrap_err()
            .contains("local executor"));
        assert_eq!(
            choose_executor(&connections, &request)
                .unwrap()
                .unwrap()
                .executor_id,
            "remote"
        );
    }

    #[test]
    fn the_reserved_local_name_selects_the_runner_own_executor() {
        let connections = HashMap::from([fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[])]);
        let mut request = targeted_request("linux");
        request.executor.as_mut().unwrap().name = Some(LOCAL_EXECUTOR_NAME.into());
        assert_eq!(
            choose_executor(&connections, &request)
                .unwrap()
                .unwrap()
                .executor_id,
            COLOCATED_EXECUTOR_ID
        );
    }

    #[test]
    fn no_match_is_typed_and_never_uses_colocated() {
        let connections = HashMap::from([fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[])]);
        assert!(choose_executor(&connections, &targeted_request("windows"))
            .unwrap_err()
            .contains("no live enrolled executor"));
    }

    #[tokio::test]
    async fn coalesced_completion_fans_out_and_restamps_public_identities() {
        let pool = Fleet::default();
        let publication = PublicationCoordination::new();
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        let first = ("first".to_string(), "attempt-1".to_string());
        let second = ("second".to_string(), "attempt-2".to_string());
        pool.in_flight.lock().unwrap().by_key.insert(
            result_identity(),
            InFlightExecution {
                leader: first.clone(),
                subscribers: HashMap::from([
                    (
                        first.clone(),
                        CoalescedSubscriber {
                            waiter: first_tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: Some("job-a".into()),
                        },
                    ),
                    (
                        second.clone(),
                        CoalescedSubscriber {
                            waiter: second_tx,
                            priority: CellPriority::WriteCheck,
                            requesting_job_id: Some("job-b".into()),
                        },
                    ),
                ]),
                publication,
            },
        );
        pool.complete_coalesced_for_leader(
            &result_identity(),
            &first,
            CellOutcome::Cancelled {
                request_id: first.0.clone(),
                attempt_id: first.1.clone(),
            },
        );
        assert_eq!(
            first_rx.await.unwrap().outcome,
            CellOutcome::Cancelled {
                request_id: first.0,
                attempt_id: first.1,
            }
        );
        assert_eq!(
            second_rx.await.unwrap().outcome,
            CellOutcome::Cancelled {
                request_id: second.0,
                attempt_id: second.1,
            }
        );
    }

    #[test]
    fn result_identity_preserves_project_and_check_namespaces() {
        assert_ne!(
            CheckResultIdentity::new("project-a", "check", "input"),
            CheckResultIdentity::new("project-b", "check", "input")
        );
        assert_ne!(
            CheckResultIdentity::new("project", "check-a", "input"),
            CheckResultIdentity::new("project", "check-b", "input")
        );
    }

    #[test]
    fn cancelling_one_subscriber_job_keeps_other_jobs_execution() {
        let pool = Fleet::default();
        let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
        // Attaching states the runner's drain posture, so the channel is not
        // empty to begin with. What this test guards is that no *cancellation*
        // is sent while another job still wants the execution, which is a
        // different claim from the machine hearing nothing at all.
        assert!(matches!(
            executor_rx.try_recv(),
            Ok(ExecutorMessage::DrainModeRequest { enabled: false, .. })
        ));
        let first = ("first".to_string(), "attempt-1".to_string());
        let second = ("second".to_string(), "attempt-2".to_string());
        let (first_tx, _) = oneshot::channel();
        let (second_tx, _) = oneshot::channel();
        let key = result_identity();
        let mut registry = pool.in_flight.lock().unwrap();
        registry.subscriber_keys.insert(first.clone(), key.clone());
        registry.subscriber_keys.insert(second.clone(), key.clone());
        registry.by_key.insert(
            key.clone(),
            InFlightExecution {
                leader: first.clone(),
                subscribers: HashMap::from([
                    (
                        first.clone(),
                        CoalescedSubscriber {
                            waiter: first_tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: Some("job-a".into()),
                        },
                    ),
                    (
                        second.clone(),
                        CoalescedSubscriber {
                            waiter: second_tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: Some("job-b".into()),
                        },
                    ),
                ]),
                publication: PublicationCoordination::new(),
            },
        );
        drop(registry);
        pool.coalesced_leaders.lock().unwrap().insert(first.clone());
        let (pending_tx, _pending_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            first.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: Some("job-a".into()),
                waiter: pending_tx,
            },
        );

        assert_eq!(pool.cancel_job_requests("job-a"), 1);
        assert!(executor_rx.try_recv().is_err());
        let registry = pool.in_flight.lock().unwrap();
        assert_eq!(registry.by_key[&key].subscribers.len(), 1);
        assert!(registry.by_key[&key].subscribers.contains_key(&second));
        assert!(!pool.cancelled_leaders.lock().unwrap().contains(&first));
    }

    #[test]
    fn detaching_one_coalesced_subscriber_keeps_the_shared_execution() {
        let pool = Fleet::default();
        let first = ("first".to_string(), "attempt-1".to_string());
        let second = ("second".to_string(), "attempt-2".to_string());
        let (first_tx, _) = oneshot::channel();
        let (second_tx, _) = oneshot::channel();
        let mut registry = pool.in_flight.lock().unwrap();
        registry
            .subscriber_keys
            .insert(first.clone(), result_identity());
        registry
            .subscriber_keys
            .insert(second.clone(), result_identity());
        registry.by_key.insert(
            result_identity(),
            InFlightExecution {
                leader: first.clone(),
                subscribers: HashMap::from([
                    (
                        first.clone(),
                        CoalescedSubscriber {
                            waiter: first_tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: Some("job-a".into()),
                        },
                    ),
                    (
                        second.clone(),
                        CoalescedSubscriber {
                            waiter: second_tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: Some("job-a".into()),
                        },
                    ),
                ]),
                publication: PublicationCoordination::new(),
            },
        );
        drop(registry);
        pool.detach_coalesced_subscriber(&first);
        let registry = pool.in_flight.lock().unwrap();
        assert_eq!(registry.by_key[&result_identity()].subscribers.len(), 1);
        assert!(registry.by_key[&result_identity()]
            .subscribers
            .contains_key(&second));
    }

    #[test]
    fn abandoning_last_subscriber_keeps_the_leader_coalescible_until_terminal_outcome() {
        let pool = Fleet::default();
        let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
        // Attaching states the runner's drain posture on every connection, so
        // that frame is already queued ahead of anything this test provokes.
        // What is asserted below is that the leader is told to cancel, never
        // that the cancel is the first thing the machine ever hears.
        assert!(matches!(
            executor_rx.try_recv(),
            Ok(ExecutorMessage::DrainModeRequest { enabled: false, .. })
        ));
        let leader = ("leader".to_string(), "attempt-1".to_string());
        let resubmitted = ("resubmitted".to_string(), "attempt-2".to_string());
        let key = result_identity();
        let (first_tx, _first_rx) = oneshot::channel();
        let (pending_tx, _pending_rx) = oneshot::channel();
        {
            let mut registry = pool.in_flight.lock().unwrap();
            registry.subscriber_keys.insert(leader.clone(), key.clone());
            registry.by_key.insert(
                key.clone(),
                InFlightExecution {
                    leader: leader.clone(),
                    subscribers: HashMap::from([(
                        leader.clone(),
                        CoalescedSubscriber {
                            waiter: first_tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: Some("job".into()),
                        },
                    )]),
                    publication: PublicationCoordination::new(),
                },
            );
        }
        pool.pending.lock().unwrap().insert(
            leader.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: Some("job".into()),
                waiter: pending_tx,
            },
        );

        pool.detach_coalesced_subscriber(&leader);
        assert!(matches!(
            executor_rx.try_recv(),
            Ok(ExecutorMessage::Cancel { ref request_id, ref attempt_id })
                if request_id == &leader.0 && attempt_id == &leader.1
        ));

        let (resubmit_tx, _resubmit_rx) = oneshot::channel();
        let mut registry = pool.in_flight.lock().unwrap();
        let retained_leader = {
            let execution = registry
                .by_key
                .get_mut(&key)
                .expect("the cancelling leader remains the coalescing authority");
            execution.subscribers.insert(
                resubmitted.clone(),
                CoalescedSubscriber {
                    waiter: resubmit_tx,
                    priority: CellPriority::ReviewCheck,
                    requesting_job_id: Some("job".into()),
                },
            );
            execution.leader.clone()
        };
        registry.subscriber_keys.insert(resubmitted, key);
        assert_eq!(retained_leader, leader);
    }

    fn register_held_subscriber(
        pool: &Fleet,
        generation: u64,
        identity: RequestIdentity,
        waiter: oneshot::Sender<CoalescedCellOutcome>,
        state: ExecutorSubstrateState,
    ) -> CheckResultIdentity {
        let key = result_identity();
        pool.in_flight
            .lock()
            .unwrap()
            .subscriber_keys
            .insert(identity.clone(), key.clone());
        pool.in_flight.lock().unwrap().by_key.insert(
            key.clone(),
            InFlightExecution {
                leader: identity.clone(),
                subscribers: HashMap::from([(
                    identity.clone(),
                    CoalescedSubscriber {
                        waiter,
                        priority: CellPriority::ReviewCheck,
                        requesting_job_id: Some("job".into()),
                    },
                )]),
                publication: PublicationCoordination::new(),
            },
        );
        let (pending_tx, _pending_rx) = oneshot::channel();
        pool.pending.lock().unwrap().insert(
            identity.clone(),
            PendingResult {
                executor_id: COLOCATED_EXECUTOR_ID.into(),
                generation,
                requesting_job_id: Some("job".into()),
                waiter: pending_tx,
            },
        );
        let now = unix_time_ms();
        pool.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            FleetSnapshot {
                queued_requests: vec![QueuedCellRequest {
                    command_resource_identity: None,
                    executor_id: COLOCATED_EXECUTOR_ID.into(),
                    request_id: identity.0,
                    attempt_id: identity.1,
                    project_id: "p".into(),
                    command: "check".into(),
                    command_class: cairn_common::executor_protocol::CellCommandClass::Other,
                    owner: None,
                    priority: CellPriority::ReviewCheck,
                    effective_priority: Some(CellPriority::ReviewCheck),
                    requesting_job_id: Some("job".into()),
                    affinity_key: None,
                    queued_at_unix_ms: now,
                    resource_reservation: Default::default(),
                    learned_estimate: None,
                    admission_kind: CellAdmissionKind::Command,
                    subscriber_count: 1,
                    substrate_hold: Some(ExecutorSubstrateEvidence {
                        state,
                        since_unix_ms: now,
                        last_progress_unix_ms: now,
                        diagnostic: None,
                        queue_depth: Some(3),
                        queue_position: Some(2),
                        active_cell_count: Some(2),
                        oldest_running_started_at_unix_ms: Some(now.saturating_sub(50)),
                    }),
                    placement: None,
                }],
                ..FleetSnapshot::default()
            },
            ExecutorSubstrateReport::default(),
        );
        key
    }

    /// A runner can first see an executor queue after reconnect, before it has
    /// reconstructed the matching placement ledger entry. That incomplete view
    /// must not consume the one durable audit transition: later snapshots retry
    /// the join and journal only once the complete refusal set exists.
    #[test]
    fn parked_admission_audit_retries_until_placement_is_complete() {
        let pool = Fleet::default();
        let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
        let identity = ("reconnected-park".to_string(), "attempt".to_string());
        let (waiter, _receiver) = oneshot::channel();
        register_held_subscriber(
            &pool,
            generation,
            identity.clone(),
            waiter,
            ExecutorSubstrateState::CapacityBusy,
        );
        assert!(
            !pool
                .journaled_admission_parks
                .lock()
                .unwrap()
                .contains(&identity),
            "a reconnect snapshot without placement evidence must remain retryable"
        );

        let mut request = spillable_request();
        request.request_id = identity.0.clone();
        request.attempt_id = identity.1.clone();
        let decision = placement_decision(
            &request,
            unix_time_ms(),
            None,
            None,
            PlacementOutcome::Refused {
                diagnostic: "capacity held".into(),
            },
            vec![PlacementRejection {
                executor_name: "remote".into(),
                executor_id: "remote".into(),
                reason: PlacementRejectionReason::NotColocated,
                prediction: None,
            }],
        );
        pool.record_placement_decision(decision);
        let snapshot = pool
            .connections
            .lock()
            .unwrap()
            .get(COLOCATED_EXECUTOR_ID)
            .unwrap()
            .snapshot
            .clone();
        pool.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            snapshot,
            ExecutorSubstrateReport::default(),
        );
        assert!(
            pool.journaled_admission_parks
                .lock()
                .unwrap()
                .contains(&identity),
            "the unchanged queue snapshot must retry and complete the audit join"
        );
    }

    #[tokio::test]
    async fn a_subscriber_waits_out_capacity_contention_until_its_leader_completes() {
        let pool = Fleet::default();
        let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
        assert!(matches!(
            executor_rx.recv().await,
            Some(ExecutorMessage::DrainModeRequest { enabled: false, .. })
        ));
        let identity = ("leader".to_string(), "attempt".to_string());
        let (tx, rx) = oneshot::channel();
        let key = register_held_subscriber(
            &pool,
            generation,
            identity.clone(),
            tx,
            ExecutorSubstrateState::CapacityBusy,
        );
        let completion_pool = pool.clone();
        let completed_identity = identity.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            completion_pool.complete_coalesced_for_leader(
                &key,
                &completed_identity,
                CellOutcome::Cancelled {
                    request_id: completed_identity.0.clone(),
                    attempt_id: completed_identity.1.clone(),
                },
            );
        });
        let outcome = pool
            .await_coalesced(identity.clone(), unix_time_ms() + 30_000, rx)
            .await
            .expect("a subscriber must outlive capacity contention it declared patience for");
        assert!(matches!(outcome.outcome, CellOutcome::Cancelled { .. }));
        assert!(matches!(
            executor_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn a_subscriber_waits_out_slot_adoption() {
        let pool = Fleet::default();
        let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
        assert!(matches!(
            executor_rx.recv().await,
            Some(ExecutorMessage::DrainModeRequest { enabled: false, .. })
        ));
        let identity = ("leader".to_string(), "attempt".to_string());
        let (tx, rx) = oneshot::channel();
        let key = register_held_subscriber(
            &pool,
            generation,
            identity.clone(),
            tx,
            ExecutorSubstrateState::SlotAdoption,
        );
        let completion_pool = pool.clone();
        let completed_identity = identity.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            completion_pool.complete_coalesced_for_leader(
                &key,
                &completed_identity,
                CellOutcome::Cancelled {
                    request_id: completed_identity.0.clone(),
                    attempt_id: completed_identity.1.clone(),
                },
            );
        });

        let outcome = pool
            .await_coalesced(identity, unix_time_ms() + 30_000, rx)
            .await
            .expect("a subscriber must outlive a cell adoption it declared patience for");
        assert!(matches!(outcome.outcome, CellOutcome::Cancelled { .. }));
        assert!(matches!(
            executor_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn coalesced_subscriber_returns_typed_stall_with_queue_facts() {
        let pool = Fleet::default();
        let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
        let identity = ("leader".to_string(), "attempt".to_string());
        let (tx, rx) = oneshot::channel();
        register_held_subscriber(
            &pool,
            generation,
            identity.clone(),
            tx,
            ExecutorSubstrateState::CapacityBusy,
        );
        pool.connections
            .lock()
            .unwrap()
            .get_mut(COLOCATED_EXECUTOR_ID)
            .unwrap()
            .last_progress_unix_ms = 0;

        let outcome = match pool
            .await_coalesced(identity, unix_time_ms() + 20, rx)
            .await
        {
            Ok(_) => panic!("stalled subscriber unexpectedly completed"),
            Err(outcome) => outcome,
        };
        assert!(matches!(
            outcome,
            CellOutcome::Unavailable {
                reason: CellUnavailableReason::Deadline {
                    substrate: Some(ExecutorSubstrateEvidence {
                        state: ExecutorSubstrateState::ConnectedStalled,
                        queue_depth: Some(3),
                        queue_position: Some(2),
                        active_cell_count: Some(2),
                        ..
                    }),
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn ownerless_preparing_leader_rewrites_stale_capacity_as_connected_stalled() {
        let pool = Fleet::default();
        let leader = ("leader".to_string(), "attempt".to_string());
        let key = result_identity();
        let (tx, _rx) = oneshot::channel();
        {
            let mut registry = pool.in_flight.lock().unwrap();
            registry.subscriber_keys.insert(leader.clone(), key.clone());
            registry.by_key.insert(
                key,
                InFlightExecution {
                    leader: leader.clone(),
                    subscribers: HashMap::from([(
                        leader.clone(),
                        CoalescedSubscriber {
                            waiter: tx,
                            priority: CellPriority::ReviewCheck,
                            requesting_job_id: Some("job".into()),
                        },
                    )]),
                    publication: PublicationCoordination::new(),
                },
            );
        }
        let last_progress_unix_ms =
            unix_time_ms().saturating_sub(EXECUTOR_PROGRESS_FRESHNESS_MS + 1);
        *pool.colocated_substrate_state.lock().unwrap() = Some(ExecutorSubstrateEvidence {
            state: ExecutorSubstrateState::CapacityBusy,
            since_unix_ms: last_progress_unix_ms.saturating_sub(10),
            last_progress_unix_ms,
            diagnostic: None,
            queue_depth: Some(3),
            queue_position: Some(2),
            active_cell_count: Some(2),
            oldest_running_started_at_unix_ms: Some(last_progress_unix_ms.saturating_sub(50)),
        });

        let evidence = pool.leader_deadline_evidence(&leader);
        assert_eq!(evidence.state, ExecutorSubstrateState::ConnectedStalled);
        assert_eq!(evidence.since_unix_ms, last_progress_unix_ms);
        assert_eq!(evidence.last_progress_unix_ms, last_progress_unix_ms);
        assert_eq!(evidence.queue_depth, Some(3));
        assert_eq!(evidence.queue_position, Some(2));
        assert_eq!(evidence.active_cell_count, Some(2));
    }

    #[tokio::test]
    async fn dropped_publication_guard_transfers_ownership() {
        let coordination = PublicationCoordination::new();
        let PublicationRole::Publisher(first) = coordination.acquire().await else {
            panic!("first subscriber should publish");
        };
        drop(first);
        let PublicationRole::Publisher(second) = coordination.acquire().await else {
            panic!("publication ownership should transfer");
        };
        let recorded = crate::execution::cache::RecordedCheckObservation {
            id: "obs-published".to_string(),
            public_handle: "111111111111111111111111".into(),
            ran_at: 1,
            environment_fingerprint: "env".to_string(),
            reusable: true,
        };
        second.published(Some(recorded.clone()));
        let PublicationRole::Published(carried) = coordination.acquire().await else {
            panic!("a published verdict must be reported as published");
        };
        assert_eq!(
            carried,
            Some(recorded),
            "a coalesced sibling must learn which observation the publisher wrote"
        );
    }

    #[test]
    fn mismatched_terminal_identity_is_rejected() {
        let outcome = CellOutcome::Cancelled {
            request_id: "r".into(),
            attempt_id: "old".into(),
        };
        assert!(!outcome_matches(&outcome, "r", "new"));
    }
    #[test]
    fn adopted_executor_lost_process_is_reconciled_to_lifetime_subscribers() {
        let pool = Fleet::default();
        let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
        let received = Arc::new(Mutex::new(Vec::new()));
        let captured = received.clone();
        pool.subscribe_resident_process_events(move |event| {
            captured.lock().unwrap().push(event);
        });
        let residency = fleet_residency(
            ResidencyHolder::Job {
                job_id: "job".into(),
            },
            None,
        );
        let status = cairn_common::executor_protocol::ResidentProcessStatus::Exited {
            finished_at_unix_ms: 42,
            exit_code: None,
            restartable: true,
            executor_lost: true,
        };
        let cell = resident_process_cell(
            CellResidency {
                phase: cairn_common::executor_protocol::ResidencyPhase::AwaitingReclaim,
                reclaim_deadline_unix_ms: 100,
                state_revision: 2,
                ..residency.clone()
            },
            7,
            3,
            status.clone(),
        );

        assert!(pool.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            FleetSnapshot {
                cells: vec![cell],
                ..Default::default()
            },
            ExecutorSubstrateReport::default(),
        ));
        assert_eq!(
            received.lock().unwrap().as_slice(),
            &[ResidentProcessEvent {
                holder: residency.holder.clone(),
                incarnation_id: "incarnation".into(),
                cell_epoch: 7,
                process_key: "main".into(),
                process_generation: 3,
                event: ResidentProcessEventKind::State { status },
            }]
        );
    }

    /// A cell holding one resident process, at a stated cell epoch and process
    /// generation.
    fn resident_process_cell(
        residency: CellResidency,
        cell_epoch: u64,
        process_generation: u64,
        status: cairn_common::executor_protocol::ResidentProcessStatus,
    ) -> PersistentCellState {
        PersistentCellState {
            warm_command_classes: Vec::new(),
            executor_id: String::new(),
            executor_display_name: None,
            project_id: "p".into(),
            cell_id: "slot".into(),
            path: "/slot".into(),
            workspace_name: "slot".into(),
            repository: "/repo".into(),
            checkout_kind: Default::default(),
            git_common_dir: None,
            authority_path: "/slot/.authority".into(),
            lifecycle: PersistentCellLifecycle::Running,
            cell_epoch,
            last_sealed_commit: Some("base".into()),
            last_used_unix_ms: 42,
            last_affinity_key: None,
            preparation_fingerprint: None,
            residency: Some(residency),
            occupancy: CellOccupancy {
                command: None,
                command_ownership: None,
                draining: None,
                processes: std::collections::BTreeMap::from([(
                    "main".into(),
                    cairn_common::executor_protocol::ResidentProcess {
                        generation: process_generation,
                        kind: cairn_common::executor_protocol::ResidentProcessKind::Terminal {
                            slug: "watch".into(),
                        },
                        spec: None,
                        status,
                        reservation: None,
                    },
                )]),
            },
        }
    }

    // ── Ingested output is scrubbed against this runner's registry ─────────
    //
    // Terminals, REPLs, workflows and dev instances are all handed this runner's
    // own `CAIRN_MCP_SECRET`, and the executor that runs them was never told
    // what that value is. These cover the pass that closes that gap.

    const RESIDENT_SECRET: &str = "resident-Qa9Zm2Xp7Lr4";

    fn register_resident_secret(id: &str) -> crate::security::SecretGuard<'static> {
        use crate::security::{registry, SecretCategory, SecretId, SecretMaterial};
        registry()
            .register(
                SecretId::new(id),
                SecretCategory::CallbackCredential,
                "unit test",
                SecretMaterial::from_string(RESIDENT_SECRET.to_string()),
            )
            .expect("registerable")
    }

    /// Collect what reaches resident-process subscribers, in order.
    fn subscribed_events(pool: &Fleet) -> Arc<Mutex<Vec<ResidentProcessEvent>>> {
        let received = Arc::new(Mutex::new(Vec::new()));
        let captured = received.clone();
        pool.subscribe_resident_process_events(move |event| {
            captured.lock().unwrap().push(event);
        });
        received
    }

    fn resident_output_text(events: &[ResidentProcessEvent]) -> String {
        events
            .iter()
            .filter_map(|event| match &event.event {
                ResidentProcessEventKind::Output { data, .. } => {
                    Some(String::from_utf8_lossy(data).into_owned())
                }
                _ => None,
            })
            .collect()
    }

    /// The split that a per-chunk pass cannot catch. Relayed output arrives in
    /// frames whose boundaries fall wherever the executor's coalescer put them,
    /// so the halves of one credential routinely land in two events.
    #[test]
    fn a_credential_split_across_two_resident_events_is_caught() {
        let _guard = register_resident_secret("resident-split");
        for at in 1..RESIDENT_SECRET.len() {
            let pool = Fleet::default();
            let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
            let generation = pool.attach_executor(executor_tx);
            let received = subscribed_events(&pool);
            let residency = fleet_residency(
                ResidencyHolder::Service {
                    service_id: "channel-imessage".into(),
                },
                None,
            );
            let output = |data: &[u8]| ResidentProcessEvent {
                holder: residency.holder.clone(),
                incarnation_id: residency.incarnation_id.clone(),
                cell_epoch: 7,
                process_key: "main".into(),
                process_generation: 2,
                event: ResidentProcessEventKind::Output {
                    sequence: 1,
                    stream: cairn_common::executor_protocol::ResidentProcessStream::Stdout,
                    data: data.to_vec(),
                },
            };
            let full = format!("env: {RESIDENT_SECRET}\n");
            let (left, right) = full.split_at(at);
            for chunk in [left, right] {
                pool.handle_executor_message(
                    COLOCATED_EXECUTOR_ID,
                    generation,
                    ExecutorMessage::ResidentProcessEvent {
                        event: output(chunk.as_bytes()),
                    },
                );
            }
            // The exit is what drains the stream, and asserting the total only
            // after it is deliberate. How much a scrubber withholds mid-stream is
            // a schedule, not a guarantee, and this registry is the process-wide
            // one — any other test in this binary that materializes a callback
            // credential adds needles to it and shifts that schedule. Pinning the
            // schedule would make this test fail depending on what ran beside it;
            // pinning the total is the property it is named for.
            pool.handle_executor_message(
                COLOCATED_EXECUTOR_ID,
                generation,
                ExecutorMessage::ResidentProcessEvent {
                    event: exited(&residency),
                },
            );
            let seen = resident_output_text(&received.lock().unwrap());
            assert!(
                !seen.contains(RESIDENT_SECRET),
                "leaked at split {at}: {seen}"
            );
            assert_eq!(seen, "env: [REDACTED]\n", "split {at}");
        }
    }

    /// The exit event for the fixture's resident process.
    fn exited(residency: &CellResidency) -> ResidentProcessEvent {
        ResidentProcessEvent {
            holder: residency.holder.clone(),
            incarnation_id: residency.incarnation_id.clone(),
            cell_epoch: 7,
            process_key: "main".into(),
            process_generation: 2,
            event: ResidentProcessEventKind::State {
                status: cairn_common::executor_protocol::ResidentProcessStatus::Exited {
                    finished_at_unix_ms: 42,
                    exit_code: Some(0),
                    restartable: false,
                    executor_lost: false,
                },
            },
        }
    }

    /// The withheld tail has to reach the scrollback BEFORE the exit that seals
    /// it. A terminal captures its output tail when it finalizes, so a tail
    /// released after the exit event lands in a buffer nobody reads again.
    #[test]
    fn a_resident_exit_releases_the_withheld_tail_before_the_exit_itself() {
        let _guard = register_resident_secret("resident-exit");
        let pool = Fleet::default();
        let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
        let received = subscribed_events(&pool);
        let residency = fleet_residency(
            ResidencyHolder::Service {
                service_id: "channel-imessage".into(),
            },
            None,
        );
        let event = |kind| ResidentProcessEvent {
            holder: residency.holder.clone(),
            incarnation_id: residency.incarnation_id.clone(),
            cell_epoch: 7,
            process_key: "main".into(),
            process_generation: 2,
            event: kind,
        };
        // Ends on a genuine proper prefix of the registered credential, which is
        // what the scrubber must withhold: at this moment those bytes are
        // indistinguishable from the head of a value whose tail is in the next
        // chunk. Chosen deliberately rather than relying on ordinary text
        // happening to be withheld, so the test does not depend on what else
        // this binary registered into the process-wide registry.
        let head = &RESIDENT_SECRET[..8];
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::ResidentProcessEvent {
                event: event(ResidentProcessEventKind::Output {
                    sequence: 1,
                    stream: cairn_common::executor_protocol::ResidentProcessStream::Stdout,
                    data: format!("done {head}").into_bytes(),
                }),
            },
        );
        assert_eq!(
            resident_output_text(&received.lock().unwrap()),
            "done ",
            "the unsettled tail must wait"
        );
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::ResidentProcessEvent {
                event: event(ResidentProcessEventKind::State {
                    status: cairn_common::executor_protocol::ResidentProcessStatus::Exited {
                        finished_at_unix_ms: 42,
                        exit_code: Some(0),
                        restartable: false,
                        executor_lost: false,
                    },
                }),
            },
        );
        let seen = received.lock().unwrap();
        assert_eq!(
            resident_output_text(&seen),
            format!("done {head}"),
            "the exit owes the stream its tail"
        );
        let last = seen.last().expect("an event");
        assert!(
            matches!(last.event, ResidentProcessEventKind::State { .. }),
            "the tail must arrive before the exit, not after it"
        );
    }

    /// The model-visible copy of a batch's output, and the diagnostics that
    /// quote a failing command's output alongside it.
    #[test]
    fn a_terminal_outcome_is_scrubbed_before_its_waiter_sees_it() {
        let _guard = register_resident_secret("resident-outcome");
        let mut completed = CellOutcome::Completed {
            request_id: "r".into(),
            attempt_id: "a".into(),
            exit_code: Some(0),
            output: format!("CAIRN_MCP_SECRET={RESIDENT_SECRET}"),
            timed_out: false,
            metadata: CellExecutionMeta {
                warmth: None,
                load_context: None,
                executor_id: "executor".into(),
                executor_device_id: "device".into(),
                executor_connection_generation: 1,
                cell_id: "cell".into(),
                cell_epoch: 1,
                started_at_unix_ms: 1,
                finished_at_unix_ms: 2,
                duration_ms: None,
                peak_rss_bytes: None,
                peak_physical_footprint_bytes: None,
                disk_delta_bytes: None,
                measurement_quality: None,
                environment_fingerprint: String::new(),
                verdict_platform: None,
                verdict_arch: None,
                verdict_environment_hash: None,
                sandbox: None,
                toolchain_fingerprint: None,
            },
            mutation_delta: None,
            sandbox_denials: Vec::new(),
            tracked_modifications: None,
        };
        scrub_cell_outcome(&mut completed);
        match &completed {
            CellOutcome::Completed { output, .. } => {
                assert_eq!(output, "CAIRN_MCP_SECRET=[REDACTED]");
            }
            other => panic!("unexpected outcome {other:?}"),
        }

        let mut unavailable = CellOutcome::Unavailable {
            reason: CellUnavailableReason::Spawn,
            diagnostic: format!("spawn failed: {RESIDENT_SECRET}"),
        };
        scrub_cell_outcome(&mut unavailable);
        match &unavailable {
            CellOutcome::Unavailable { diagnostic, .. } => {
                assert_eq!(diagnostic, "spawn failed: [REDACTED]");
            }
            other => panic!("unexpected outcome {other:?}"),
        }
    }

    /// The capability lives exactly as long as the context that authorizes it.
    ///
    /// Every revocation path removes from `runner_contexts`, so this checks the
    /// registration is released by the map entry going away rather than by any
    /// one call site remembering to do it (CAIRN-3385).
    #[test]
    fn revoking_a_runner_context_unregisters_its_capability() {
        use crate::security::registry;

        let capability = "runner-context-capability-3824";
        let label = "request-3824:attempt-3824";
        // Matched by prefix: the id carries a process-local sequence so that two
        // live contexts can never spell the same one and silently replace each
        // other's registered forms.
        let registered = |present: bool| {
            let prefix = format!("batch-capability:{label}:");
            assert_eq!(
                registry()
                    .metadata()
                    .iter()
                    .any(|entry| entry.id.as_str().starts_with(&prefix)),
                present
            );
        };
        let pool = Fleet::default();
        pool.runner_contexts.lock().unwrap().insert(
            capability.into(),
            RunnerContextEntry::new(
                capability,
                label,
                RunnerCallbackContext {
                    request: None,
                    run_context: None,
                    check_status_board: None,
                    live_checkout: false,
                    executor_binding: Some(RunnerContextExecutorBinding {
                        executor_id: "executor-3824".into(),
                        generation: 1,
                        request_id: "request-3824".into(),
                        attempt_id: "attempt-3824".into(),
                    }),
                    output: output_guard::RelayedOutput::shared(),
                },
            ),
        );
        registered(true);
        let mut sanitizer = crate::security::Sanitizer::exact();
        assert_eq!(sanitizer.text(capability), "[REDACTED]");

        pool.revoke_runner_contexts_for_request("request-3824", "attempt-3824");
        registered(false);
    }

    /// An executor's snapshot of what it is running and its events about that
    /// work travel as separate streams, so the event can land first. It has to
    /// reach subscribers anyway: a process that lives a second cannot be made
    /// to wait for the runner's cache to agree about it (CAIRN-3444).
    #[test]
    fn resident_events_survive_a_snapshot_that_has_not_caught_up() {
        let pool = Fleet::default();
        let (executor_tx, _executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
        let received = Arc::new(Mutex::new(Vec::new()));
        let captured = received.clone();
        pool.subscribe_resident_process_events(move |event| {
            captured.lock().unwrap().push(event);
        });
        let residency = fleet_residency(
            ResidencyHolder::Service {
                service_id: "channel-imessage".into(),
            },
            None,
        );
        let event = |cell_epoch: u64, process_generation: u64| ResidentProcessEvent {
            holder: residency.holder.clone(),
            incarnation_id: residency.incarnation_id.clone(),
            cell_epoch,
            process_key: "main".into(),
            process_generation,
            event: ResidentProcessEventKind::Output {
                sequence: 1,
                stream: cairn_common::executor_protocol::ResidentProcessStream::Stdout,
                data: b"guid".to_vec(),
            },
        };

        // Nothing is known about this cell yet: the residency was acquired and
        // the process started since the last beat.
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::ResidentProcessEvent { event: event(7, 2) },
        );
        assert_eq!(received.lock().unwrap().len(), 1);

        // A snapshot that agrees changes nothing, and one that knows a later
        // generation at the same key refuses the process before it.
        pool.set_executor_snapshot(
            COLOCATED_EXECUTOR_ID,
            generation,
            FleetSnapshot {
                cells: vec![resident_process_cell(
                    residency.clone(),
                    7,
                    2,
                    cairn_common::executor_protocol::ResidentProcessStatus::Running {
                        started_at_unix_ms: 42,
                        process_group_id: None,
                        ownership: None,
                    },
                )],
                ..Default::default()
            },
            ExecutorSubstrateReport::default(),
        );
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::ResidentProcessEvent { event: event(7, 2) },
        );
        assert_eq!(received.lock().unwrap().len(), 2);
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::ResidentProcessEvent { event: event(7, 1) },
        );
        assert_eq!(
            received.lock().unwrap().len(),
            2,
            "a generation the runner has already seen superseded is stale"
        );
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation,
            ExecutorMessage::ResidentProcessEvent { event: event(6, 9) },
        );
        assert_eq!(
            received.lock().unwrap().len(),
            2,
            "an epoch the cell has moved past is stale whatever its generation"
        );

        // A link that has since bounced speaks for nobody.
        pool.handle_executor_message(
            COLOCATED_EXECUTOR_ID,
            generation + 1,
            ExecutorMessage::ResidentProcessEvent { event: event(7, 2) },
        );
        assert_eq!(received.lock().unwrap().len(), 2);
    }

    fn darwin_remote_config() -> RemoteExecutorConfig {
        RemoteExecutorConfig {
            host: "bglab-mac.local".into(),
            ssh_user: "dev".into(),
            platform: RemotePlatform::DarwinArm64,
            binary_path: "/Users/dev/.local/bin/cairn-executor".into(),
            cairn_home: "/Users/dev/.cairn-executor".into(),
            executor_id: "bglab-mac".into(),
            device_id: "bglab-mac-device".into(),
            display_name: "bglab-mac".into(),
            project_ids: vec![],
            tunnel_port: 43_851,
            extra_ssh_args: vec![],
        }
    }

    #[test]
    fn darwin_arm64_reports_macos_identity_and_the_apple_silicon_target() {
        assert_eq!(RemotePlatform::DarwinArm64.os(), "macos");
        assert_eq!(RemotePlatform::DarwinArm64.arch(), "aarch64");
        assert_eq!(RemotePlatform::DarwinArm64.target(), "aarch64-apple-darwin");
        // `arch()` was a constant "x86_64" before Darwin existed; the other two
        // platforms must be unaffected by its promotion to a match.
        assert_eq!(RemotePlatform::LinuxX86_64.arch(), "x86_64");
        assert_eq!(RemotePlatform::WindowsX86_64.arch(), "x86_64");
    }

    #[test]
    fn linux_arm64_reports_linux_identity_and_the_gnu_arm_target() {
        assert_eq!(RemotePlatform::LinuxArm64.os(), "linux");
        assert_eq!(RemotePlatform::LinuxArm64.arch(), "aarch64");
        assert_eq!(
            RemotePlatform::LinuxArm64.target(),
            "aarch64-unknown-linux-gnu"
        );
    }

    /// Every enrolled machine is described twice: by this enum before it
    /// attaches, and by its own advertisement after. `std::env::consts::ARCH` is
    /// what the executor advertises, so a variant whose `arch()` disagreed with
    /// it would make one machine appear to change architecture on attach.
    #[test]
    fn arch_agrees_with_what_an_attached_executor_advertises() {
        for platform in [
            RemotePlatform::LinuxX86_64,
            RemotePlatform::LinuxArm64,
            RemotePlatform::WindowsX86_64,
            RemotePlatform::DarwinArm64,
        ] {
            // Rust spells these the same way on every host it supports, so the
            // triple each variant names carries the arch it must report.
            assert!(
                platform.target().starts_with(platform.arch()),
                "{platform:?} reports arch {} but targets {}",
                platform.arch(),
                platform.target()
            );
        }

        // And concretely, against the machine running this test: whichever
        // variant describes this host must report what this host reports.
        let host = if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            Some(RemotePlatform::DarwinArm64)
        } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
            Some(RemotePlatform::LinuxX86_64)
        } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
            Some(RemotePlatform::LinuxArm64)
        } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
            Some(RemotePlatform::WindowsX86_64)
        } else {
            None
        };
        if let Some(host) = host {
            assert_eq!(std::env::consts::ARCH, host.arch());
            assert_eq!(std::env::consts::OS, host.os());
        }
    }

    #[test]
    fn linux_arm64_paths_validate_by_the_posix_absolute_rule() {
        let config = RemoteExecutorConfig {
            platform: RemotePlatform::LinuxArm64,
            binary_path: "/home/dev/.local/bin/cairn-executor".into(),
            cairn_home: "/home/dev/.cairn-executor".into(),
            ..darwin_remote_config()
        };
        config.validate().unwrap();

        let windows_shaped = RemoteExecutorConfig {
            binary_path: r"C:\cairn\cairn-executor".into(),
            ..config.clone()
        };
        assert!(windows_shaped.validate().is_err());

        // The wire spelling settings files carry, and the one
        // `packages/ui/src/api/fleet.ts` mirrors.
        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(encoded["platform"], serde_json::json!("linux-arm64"));
        assert_eq!(
            serde_json::from_value::<RemoteExecutorConfig>(encoded).unwrap(),
            config
        );
    }

    #[test]
    fn darwin_paths_validate_by_the_posix_absolute_rule() {
        darwin_remote_config().validate().unwrap();
        let relative = RemoteExecutorConfig {
            binary_path: ".local/bin/cairn-executor".into(),
            ..darwin_remote_config()
        };
        assert!(relative
            .validate()
            .unwrap_err()
            .contains("binaryPath must be an absolute path"));
        let windows_shaped = RemoteExecutorConfig {
            binary_path: r"C:\cairn\cairn-executor".into(),
            ..darwin_remote_config()
        };
        assert!(windows_shaped.validate().is_err());
    }

    #[test]
    fn platform_round_trips_as_kebab_case_and_defaults_to_linux_when_absent() {
        let encoded = serde_json::to_value(darwin_remote_config()).unwrap();
        assert_eq!(encoded["platform"], serde_json::json!("darwin-arm64"));
        assert_eq!(
            serde_json::from_value::<RemoteExecutorConfig>(encoded).unwrap(),
            darwin_remote_config()
        );

        // Settings files written before Darwin existed carry no `platform` key.
        let mut legacy = serde_json::to_value(darwin_remote_config()).unwrap();
        legacy.as_object_mut().unwrap().remove("platform");
        assert_eq!(
            serde_json::from_value::<RemoteExecutorConfig>(legacy)
                .unwrap()
                .platform,
            RemotePlatform::LinuxX86_64
        );
    }
}
