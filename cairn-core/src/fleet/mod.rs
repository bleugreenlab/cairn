//! Runner-side fleet-placement facade for supervised and enrolled executors.
//!
//! Core owns request construction, settings resolution, result correlation, and
//! the cached UI snapshot. Scheduling, workspaces, processes, cancellation, and
//! mutation sealing exist only in the executor process.

pub mod management;
pub(crate) mod occupancy;
pub(crate) mod placement;
pub(crate) mod residency;
mod resource_profiles;
pub(crate) mod service_placement;

use crate::mcp::handlers::run::{ResolvedRunBatch, RunSpec};
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::{
    aged_priority, CacheWarmthEvidence, CellCheckoutKind, CellCommandClass, CellResidency,
    CpuAdmissionState, DurationEstimate, DurationFallback, EnrolledRemote, ExecutionWarmth,
    ExecutorAdvertisement, ExecutorCapabilities, ExecutorConfig, ExecutorHealthSnapshot,
    ExecutorHealthStatus, ExecutorIdentity, ExecutorInspection, ExecutorMessage, ExecutorSelector,
    ExecutorSubstrateEvidence, ExecutorSubstrateReport, ExecutorSubstrateState,
    InventoryAuthorityState, MachineMeasurement, MaterializationReadFailureKind,
    MaterializationReadRequest, MaterializationReadResult, ObjectTransferCoordinate,
    ObservationReuse, PlacementDecision, PlacementMobility, PlacementOutcome,
    PlacementPolicyEvidence, PlacementPrediction, PlacementReadings, PlacementReason,
    PlacementRejection, PlacementRejectionReason, PlacementSelection, PlacementSyncCost,
    PreparationForecast, ProcessBatch, ProcessBatchExecution, ProcessBatchItem, ProcessSandboxMode,
    QueueForecast, QueueUnknownReason, RemoteAttachAttempt, RemoteLinkState, RepositoryLocator,
    ReservationFallback, ReservationRationale, ResidencyAcquireRequest, ResidencyFailureKind,
    ResidencyFence, ResidencyHolder, ResidencyOperation, ResidencyResult, ResidentProcessEvent,
    ResidentProcessEventKind, RunnerCallback, RunnerCallbackResult, WarmthUnknownReason,
    CPU_ADMISSION_SAMPLE_INTERVAL_MS, EXECUTOR_PROGRESS_FRESHNESS_MS, LOCAL_EXECUTOR_NAME,
    MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS, RESIDENCY_ACQUIRE_ATTEMPT_ID,
};
use cairn_common::executor_protocol::{
    executor_names_match, normalize_executor_name, EXECUTOR_NAME_RULE,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RemotePlatform {
    #[default]
    LinuxX86_64,
    WindowsX86_64,
    DarwinArm64,
}

impl RemotePlatform {
    pub fn os(self) -> &'static str {
        match self {
            Self::LinuxX86_64 => "linux",
            Self::WindowsX86_64 => "windows",
            Self::DarwinArm64 => "macos",
        }
    }

    pub fn arch(self) -> &'static str {
        match self {
            Self::LinuxX86_64 | Self::WindowsX86_64 => "x86_64",
            Self::DarwinArm64 => "arm64",
        }
    }

    pub fn target(self) -> &'static str {
        match self {
            Self::LinuxX86_64 => "x86_64-unknown-linux-gnu",
            Self::WindowsX86_64 => "x86_64-pc-windows-msvc",
            Self::DarwinArm64 => "aarch64-apple-darwin",
        }
    }

    fn is_absolute(self, path: &str) -> bool {
        match self {
            Self::LinuxX86_64 | Self::DarwinArm64 => path.starts_with('/'),
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
                 using {}s instead. A value this small is usually a pre-CAIRN-3268 acquisitionDeadlineSeconds \
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
}

#[derive(Clone, Default)]
pub struct Fleet {
    connections: Arc<Mutex<HashMap<String, ExecutorConnectionState>>>,
    connection_generations: Arc<Mutex<HashMap<String, u64>>>,
    disconnect_origins: Arc<Mutex<HashMap<(String, u64), ExecutorDisconnectOrigin>>>,
    connection_ready: Arc<tokio::sync::Notify>,
    pending: Arc<Mutex<PendingResults>>,
    pending_residency: Arc<Mutex<PendingResidencyResults>>,
    pending_materialization_reads: Arc<Mutex<PendingMaterializationReads>>,
    pending_policy: Arc<Mutex<HashMap<String, PendingPolicyResult>>>,
    pending_drain: Arc<Mutex<HashMap<String, PendingDrainResult>>>,
    residency_routes: Arc<Mutex<HashMap<(String, String), ResidencyRoute>>>,
    residency_route_path: Arc<Option<PathBuf>>,
    residency_route_store_error: Arc<Mutex<Option<String>>>,
    /// One acquisition flight per execution environment, keyed by holder.
    /// [`Fleet::residency_acquire_flight`] owns both the keying and the pruning.
    residency_acquisitions: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    resident_process_subscribers: Arc<Mutex<Vec<ResidentProcessSubscriber>>>,
    cancelled_leaders: Arc<Mutex<HashSet<RequestIdentity>>>,
    coalesced_leaders: Arc<Mutex<HashSet<RequestIdentity>>>,
    preparing_leaders: Arc<Mutex<HashMap<RequestIdentity, LeaderPreparation>>>,
    in_flight: Arc<Mutex<InFlightRegistry>>,
    runner_contexts: Arc<Mutex<HashMap<String, RunnerCallbackContext>>>,
    recent_cached_completions:
        Arc<Mutex<VecDeque<cairn_common::executor_protocol::CellCompletion>>>,
    expected_executor_build_ids: Arc<Mutex<HashMap<String, String>>>,
    /// The placement decisions this runner most recently took, newest last.
    recent_placements: Arc<Mutex<VecDeque<PlacementDecision>>>,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RunnerContextExecutorBinding {
    executor_id: String,
    generation: u64,
    request_id: String,
    attempt_id: String,
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
        self.runner_contexts.lock().unwrap().retain(|id, context| {
            let matches = context.executor_binding.as_ref().is_some_and(|binding| {
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
        self.runner_contexts.lock().unwrap().retain(|id, context| {
            let matches = context.executor_binding.as_ref().is_some_and(|binding| {
                binding.executor_id == executor_id && binding.generation == generation
            });
            if matches {
                revoked.push(id.clone());
            }
            !matches
        });
        revoked
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

fn placement_decision(
    request: &CellRequest,
    decided_at_unix_ms: u64,
    policy: Option<PlacementPolicyEvidence>,
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
                toolchains: Vec::new(),
                projects_served: Vec::new(),
                disk_budget_bytes: None,
                memory_budget_bytes: None,
                toolchain_detection: None,
            },
            current_load: 0,
            warm_roots: Vec::new(),
            observed_at_unix_ms: unix_time_ms(),
            liveness_observed_at_unix_ms: None,
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
            if executor_id == COLOCATED_EXECUTOR_ID {
                self.disconnect_origins
                    .lock()
                    .unwrap()
                    .insert((executor_id.to_string(), generation), origin);
            }
            self.fail_for_executor(
                executor_id,
                "executor connection closed before returning a result",
            );
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
        reason: impl Into<String>,
        attempted_at_unix_ms: u64,
    ) {
        if let Some(record) = self.enrolled_remotes.lock().unwrap().get_mut(executor_id) {
            record.link = link;
            record.last_attempt = Some(RemoteAttachAttempt {
                attempted_at_unix_ms,
                reason: reason.into(),
            });
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
        let mut values: Vec<_> = self
            .enrolled_remotes
            .lock()
            .unwrap()
            .iter()
            .filter(|(executor_id, _)| !attached.contains(*executor_id))
            .map(|(_, record)| EnrolledRemote {
                name: record.name.clone(),
                os: record.os.clone(),
                arch: record.arch.clone(),
                link: record.link,
                last_attempt: record.last_attempt.clone(),
                last_seen_unix_ms: record.last_seen_unix_ms,
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
        let now = unix_time_ms();
        let mut evidence = ExecutorSubstrateEvidence::without_queue(
            ExecutorSubstrateState::SupervisorRespawning,
            now,
            now,
        );
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

    /// Whether the colocated link has gone silent long enough that continuing to
    /// wait on it is no longer the right move.
    ///
    /// `now_unix_ms` is a parameter rather than read from the clock so the
    /// decision is testable without wall-clock sleeps, following
    /// [`deadline_evidence`]. Returns [`LinkRemediation::Healthy`] when no
    /// colocated connection is attached: the supervisor's spawn and readiness
    /// paths already own that case, and a link that was never established is not
    /// a link to bounce.
    ///
    /// The criterion is silence, never duration. Any inbound message — heartbeat,
    /// snapshot, result — bumps `last_progress_unix_ms`, so an executor grinding
    /// through an hour-long check keeps reporting `Healthy` throughout.
    pub fn assess_colocated_link(&self, now_unix_ms: u64, bound_ms: u64) -> LinkRemediation {
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
                )
            });
        let Some((executor_id, generation, last_progress_unix_ms, pump_tick)) = observed else {
            return LinkRemediation::Healthy;
        };
        let silence_ms = now_unix_ms.saturating_sub(last_progress_unix_ms);
        if silence_ms <= bound_ms {
            return LinkRemediation::Healthy;
        }
        LinkRemediation::Bounce {
            executor_id,
            generation,
            silence_ms,
            pump_silence_ms: now_unix_ms.saturating_sub(pump_tick),
        }
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
        silence_ms: u64,
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
             silence_ms={silence_ms} disconnected={disconnected} \
             abandoned_attempts={} abandoned_in_flight={abandoned_in_flight} \
             last_known_state={last_known_state:?}",
            abandoned_leaders.len(),
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

    pub fn shutdown_advertised_executor(
        &self,
        executor_id: &str,
        device_id: &str,
        generation: u64,
    ) -> bool {
        let sender = self
            .connections
            .lock()
            .unwrap()
            .get(executor_id)
            .filter(|entry| entry.generation == generation && entry.identity.device_id == device_id)
            .map(|entry| entry.sender.clone());
        let Some(sender) = sender else { return false };
        let _ = sender.send(ExecutorMessage::Shutdown);
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
        let _ = sender.send(ExecutorMessage::Shutdown);
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
                    for subscriber in self.resident_process_subscribers.lock().unwrap().iter() {
                        subscriber(event.clone());
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
        for queued in &mut snapshot.queued_requests {
            queued.executor_id = executor_id.to_string();
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
                cell.residency.as_ref().map(|residency| ResidencyRoute {
                    holder: residency.holder.clone(),
                    repository: residency.repository.clone(),
                    executor_id: executor_id.to_string(),
                    pending: false,
                })
            })
            .collect::<Vec<_>>();
        entry.snapshot = snapshot;
        entry.health = health;
        drop(connections);
        if let Err(error) = self.update_residency_routes(|known| {
            known.retain(|(route_executor, _), route| {
                route_executor != executor_id || route.pending
            });
            for route in routes {
                known.insert(
                    (route.executor_id.clone(), route.holder.storage_key()),
                    route,
                );
            }
        }) {
            tracing::error!(%error, "persist executor residency route snapshot failed");
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
                .and_then(|queued| queued.substrate_hold.clone())
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
        let now_unix_ms = unix_time_ms();
        let connections = self.connections.lock().unwrap();
        connections
            .values()
            .filter(|connection| candidate_rejection(connection, scope).is_none())
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
        for queued in &mut aggregate.queued_requests {
            queued.subscriber_count = counts
                .get(&(queued.request_id.clone(), queued.attempt_id.clone()))
                .copied()
                .unwrap_or(1);
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
            })
            .collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));
        values
    }

    /// Keep one placement decision, evicting the oldest past the bound.
    fn record_placement_decision(&self, decision: PlacementDecision) {
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

    pub async fn set_executor_drain_mode(
        &self,
        executor_id: &str,
        expected_generation: u64,
        enabled: bool,
    ) -> Result<bool, String> {
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
        self.pending_drain.lock().unwrap().insert(
            correlation_id.clone(),
            PendingDrainResult {
                executor_id: executor_id.to_string(),
                generation: expected_generation,
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
            return Err("executor disconnected while changing drain mode".into());
        }
        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("executor dropped the drain-mode response".into()),
            Err(_) => {
                self.pending_drain.lock().unwrap().remove(&correlation_id);
                Err("executor drain-mode update timed out".into())
            }
        }
    }

    pub(crate) fn cancel_request(&self, request_id: &str) -> bool {
        let subscriber = self
            .in_flight
            .lock()
            .unwrap()
            .subscriber_keys
            .keys()
            .find(|(id, _)| id == request_id)
            .cloned();
        if let Some(identity) = subscriber {
            self.detach_coalesced_subscriber(&identity);
            return true;
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
            return false;
        };
        self.send_to(
            &executor_id,
            generation,
            ExecutorMessage::Cancel {
                request_id: request_id.into(),
                attempt_id,
            },
        )
        .is_ok()
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

    fn detach_coalesced_subscriber(&self, identity: &RequestIdentity) {
        let leader_to_cancel = {
            let mut registry = self.in_flight.lock().unwrap();
            let Some(result_identity) = registry.subscriber_keys.remove(identity) else {
                return;
            };
            let Some(execution) = registry.by_key.get_mut(&result_identity) else {
                return;
            };
            execution.subscribers.remove(identity);
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
            return;
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
                    (selected, None, None, None)
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
                        Some(prepared.executor_config),
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
                let connections = self.connections.lock().unwrap();
                let routed = connections.iter().find_map(|(executor_id, connection)| {
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
                });
                let Some((selected, residency)) = routed else {
                    return residency_core_failure(
                        ResidencyFailureKind::Unavailable,
                        "no connected executor reports this execution environment",
                        None,
                    );
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
                            None,
                            Some(request),
                            Some(orch.object_plane.clone()),
                        )
                    } else {
                        (selected, None, None, None)
                    }
                } else {
                    (selected, None, None, None)
                }
            }
        };
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
        let configured = executor_config.is_none_or(|config| {
            selected
                .sender
                .send(ExecutorMessage::Configure { config })
                .is_ok()
        });
        let sent = configured
            && selected
                .sender
                .send(ExecutorMessage::ResidencyRequest {
                    correlation_id: correlation_id.clone(),
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
                residency_core_failure(
                    ResidencyFailureKind::Admission,
                    "residency operation stopped reporting progress",
                    Some(CellOutcome::Unavailable {
                        reason: CellUnavailableReason::Deadline {
                            host_pressure: None,
                            substrate: Some(substrate),
                        },
                        diagnostic: "residency operation stopped reporting progress".into(),
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
    /// land on the colocated executor.
    pub(crate) fn job_home_executor(&self, holder: &ResidencyHolder) -> Option<String> {
        self.residency_route_executor(holder).or_else(|| {
            self.connections
                .lock()
                .unwrap()
                .values()
                .find(|entry| entry.colocated && !entry.sender.is_closed())
                .map(|entry| entry.identity.executor_id.clone())
        })
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
        if existing.len() > 1 {
            return Err(residency_core_failure(
                ResidencyFailureKind::ConflictingDeclaration,
                "multiple executors report the same execution environment",
                None,
            ));
        }
        let Some(route) = existing.into_iter().next() else {
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
        self.connections
            .lock()
            .unwrap()
            .get(&route.executor_id)
            .map(selected_executor)
            .map(Some)
            .ok_or_else(|| {
                residency_core_failure(
                    ResidencyFailureKind::Admission,
                    "the executor holding this execution environment is disconnected",
                    None,
                )
            })
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
        items: Vec<ProcessBatchItem>,
        runner_context_id: Option<String>,
    ) -> ProcessBatch {
        ProcessBatch {
            sequential: true,
            stop_on_error: false,
            sandbox_mode: Self::CHECK_CADENCE_SANDBOX_MODE,
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
            let runner_context_id = run_context.map(|run_context| {
                let id = uuid::Uuid::new_v4().to_string();
                self.runner_contexts.lock().unwrap().insert(
                    id.clone(),
                    RunnerCallbackContext {
                        request: None,
                        run_context: Some(run_context),
                        check_status_board: None,
                        live_checkout: false,
                        executor_binding: None,
                    },
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
                    pool.runner_contexts.lock().unwrap().remove(&id);
                }
                pool.cancelled_leaders.lock().unwrap().remove(&leader);
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
        self.runner_contexts.lock().unwrap().insert(
            runner_context_id.clone(),
            RunnerCallbackContext {
                request: Some(batch.request.clone()),
                run_context: batch.run_context.clone(),
                check_status_board: None,
                live_checkout: runs_in_live_checkout(&request.repository),
                executor_binding: None,
            },
        );
        let sandbox_mode = Self::batch_sandbox_mode(
            crate::mcp::handlers::fence::resolve_run_fence(orch, &batch.request)
                .await
                .map(|(_, fence)| fence),
            &request.repository,
        );
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
        self.runner_contexts.lock().unwrap().insert(
            runner_context_id.clone(),
            RunnerCallbackContext {
                request: None,
                run_context,
                check_status_board,
                live_checkout: false,
                executor_binding: None,
            },
        );
        let batch = Self::check_process_batch(items, Some(runner_context_id.clone()));
        let outcome = self.submit_execution(orch, request, Some(batch)).await;
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
            .cloned()
        else {
            return RunnerCallbackResult::Failed {
                diagnostic: "unknown or expired runner callback context".into(),
            };
        };
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
            .cloned()
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
                let Some((run_id, mode)) = fence::resolve_run_fence(orch, request).await else {
                    return RunnerCallbackResult::Rejected {
                        diagnostic:
                            "sandbox denial cannot be adjudicated without an originating run fence"
                                .into(),
                    };
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
                    populate: cairn_worktree::PopulateConfig::default(),
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
        // The winning candidate's estimate is the one that is submitted. It was
        // resolved in this machine's own profile context during selection, so
        // there is exactly one number and exactly one rationale behind it.
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
        let configured = selected
            .sender
            .send(ExecutorMessage::Configure {
                config: executor_config,
            })
            .is_ok();
        let sent = configured
            && selected
                .sender
                .send(ExecutorMessage::Submit { request, batch })
                .is_ok();
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
        let refuse = |diagnostic: String| RefusedPlacement {
            decision: placement_decision(
                request,
                now,
                Some(policy_evidence(request, policy, false)),
                PlacementOutcome::Refused {
                    diagnostic: diagnostic.clone(),
                },
                Vec::new(),
            ),
            diagnostic,
        };
        // Stage one: which machines could structurally take this at all.
        let survey = survey_candidates(&connections, request).map_err(refuse)?;
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
        for entry in &survey.usable {
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
            let prices = QueuePrices::resolve(&oracle, entry, &context, now).await;
            let queue = forecast_queue_wait(entry, &prices, request, now);
            let sync_cost = estimate(request, entry);
            sync_costs.insert(entry.identity.executor_id.clone(), sync_cost);
            predictions.insert(
                entry.identity.executor_id.clone(),
                placement_prediction(entry, warmth, queue, run, sync_cost),
            );
        }
        // Stage three: rank on when each machine is predicted to answer.
        let draft = rank_survey(
            request,
            survey,
            &reservations,
            &predictions,
            &sync_costs,
            policy,
            now,
        )
        .map_err(refuse)?;
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
        let reservation = reservations.remove(&selected.executor_id);
        let decision = placement_decision(
            request,
            now,
            Some(draft.policy),
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

    fn fail_for_executor(&self, executor_id: &str, diagnostic: &str) {
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
        drop(pending);
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
        repository: acquisition.repository.clone(),
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
        // A residency is an environment that outlives any one batch. Where it is
        // acquired is where it stays, so policy never gets to choose for it.
        placement_mobility: PlacementMobility::PinnedOrColocated,
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
        unix_time_ms(),
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
            placement_prediction(entry, warmth, queue, run, sync_cost),
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
    let survey = survey_candidates(connections, request)?;
    let (predictions, sync_costs) =
        prior_predictions(&survey.usable, request, &estimate, now_unix_ms);
    rank_survey(
        request,
        survey,
        reservations,
        &predictions,
        &sync_costs,
        policy,
        now_unix_ms,
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
) -> Option<PlacementRejectionReason> {
    if entry.sender.is_closed() {
        Some(PlacementRejectionReason::ConnectionClosed)
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
        match candidate_rejection(entry, scope) {
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
        return Err(no_matching_executor_diagnostic(connections, request));
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

/// Rank what survived the survey, on the estimates and readings each machine
/// carries.
fn rank_survey(
    request: &CellRequest,
    survey: CandidateSurvey<'_>,
    reservations: &HashMap<String, resource_profiles::ResolvedResourceProfile>,
    predictions: &HashMap<String, PlacementPrediction>,
    sync_costs: &HashMap<String, SyncCost>,
    policy: &ActivePlacementPolicy,
    now_unix_ms: u64,
) -> Result<PlacementDraft, String> {
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

    let (winner, ranked, changed_earliest_winner) =
        rank_candidates(&scored, settled, exercising_spill, request, policy);
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
        let cpu_admission_fresh = cpu_admission
            .measured_at_unix_ms
            .is_some_and(|measured_at| {
                now_unix_ms.saturating_sub(measured_at) <= CPU_ADMISSION_SAMPLE_INTERVAL_MS
            });
        let cpu_pressured =
            cpu_admission_fresh && cpu_admission.state == CpuAdmissionState::Pressured;
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
            prediction.predicted_verdict_ms = prediction.run.predicted_ms;
        }
        let admission_accepts = !cpu_pressured
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
) -> (
    Option<&'b ScoredCandidate<'a>>,
    Vec<&'b ScoredCandidate<'a>>,
    bool,
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
            .then_with(|| {
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
    baseline.sort_by(|a, b| hard_order(a, b).then_with(|| forecast_order(a, b)));
    let earliest = baseline.iter().copied().find(selectable);
    let Some(earliest) = earliest else {
        return (None, baseline, false);
    };
    if settled_by_caller || !exercising_spill {
        return (Some(earliest), baseline, false);
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
    });
    let winner = ranked.iter().copied().find(selectable);
    let changed = winner.is_some_and(|winner| {
        winner.entry.identity.executor_id != earliest.entry.identity.executor_id
    });
    (winner, ranked, changed)
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
fn placement_prediction(
    entry: &ExecutorConnectionState,
    warmth: CacheWarmthEvidence,
    queue: QueueForecast,
    run: DurationEstimate,
    sync_cost: SyncCost,
) -> PlacementPrediction {
    let preparation = match sync_cost {
        SyncCost::Known(0) => PreparationForecast::ObjectsPresent,
        SyncCost::Known(bytes) => PreparationForecast::TransferPending { bytes },
        SyncCost::Unknown => PreparationForecast::Unknown,
    };
    PlacementPrediction {
        executor_name: executor_public_name(entry),
        executor_id: entry.identity.executor_id.clone(),
        predicted_verdict_ms: queue
            .predicted_ms()
            .unwrap_or(0)
            .saturating_add(run.predicted_ms),
        queue,
        preparation,
        run,
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
    placement_prediction(
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
                duration_context,
            )
            .await
        } else {
            self.resolve_batch(&context, prior, duration_context).await
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
        duration_context: resource_profiles::DurationContext,
    ) -> resource_profiles::ResolvedResourceProfile {
        let mut reservation = ResourceReservation::default();
        let mut learned_estimates = Vec::with_capacity(self.batch_identities.len());
        // No item has spoken yet, so there is no rationale to seed: an unconsulted
        // batch must not carry a stand-in explanation that could survive as one.
        let mut rationale: Option<ReservationRationale> = None;
        for identity in &self.batch_identities {
            let item = resource_profiles::resolve_reservation(
                self.db.clone(),
                Some(identity),
                context,
                prior.clone(),
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
        }
        resource_profiles::ResolvedResourceProfile {
            reservation,
            learned_estimate: aggregate_batch_learned_estimates(&learned_estimates),
            rationale: rationale
                .unwrap_or_else(|| resource_profiles::declared_rationale(context, prior)),
            duration: self.predict_batch_duration(context, duration_context).await,
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

fn executor_health_snapshot(
    entry: &ExecutorConnectionState,
    captured_at_unix_ms: u64,
    expected_build_ids: &HashMap<String, String>,
) -> ExecutorHealthSnapshot {
    let heartbeat_age_ms =
        captured_at_unix_ms.saturating_sub(entry.advertisement.observed_at_unix_ms);
    let liveness_age_ms = entry
        .advertisement
        .liveness_observed_at_unix_ms
        .map(|observed_at| captured_at_unix_ms.saturating_sub(observed_at));
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
        drain_mode: entry.health.drain_mode,
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
            format!(
                "{} ({}, {toolchains})",
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
) -> String {
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
            RunSpec::McpCall(_) | RunSpec::ReplSend { .. } => {
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
            timeout_ms: crate::mcp::handlers::run::clamp_run_item_timeout_ms(timeout),
            command_resource_identity: None,
            verdict_environment_names: Vec::new(),
        });
    }
    Ok(ProcessBatch {
        sequential: batch.originally_sequential,
        stop_on_error: batch.stop_on_error,
        sandbox_mode,
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
        None => Duration::from_millis(u64::from(request.timeout_ms)),
        Some(batch) if batch.sequential => {
            batch.items.iter().fold(Duration::ZERO, |total, item| {
                total.saturating_add(Duration::from_millis(u64::from(item.timeout_ms)))
            })
        }
        Some(batch) => batch
            .items
            .iter()
            .map(|item| Duration::from_millis(u64::from(item.timeout_ms)))
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

#[cfg(test)]
mod tests {
    use super::*;
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
            },
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

    #[tokio::test]
    async fn colocated_shutdown_is_immediate_retryable_infrastructure() {
        let pool = Fleet::default();
        let (sender, mut executor) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(sender);
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
            Some(ExecutorMessage::Shutdown)
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
                "agent/CAIRN-3381-builder-0".to_string(),
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

    #[test]
    fn every_item_of_a_cell_batch_is_pointed_at_the_compile_cache() {
        let injected = vec![("SCCACHE_SERVER_PORT".to_string(), "4227".to_string())];
        let batch = with_cell_client_env(
            ProcessBatch {
                sequential: false,
                stop_on_error: true,
                sandbox_mode: ProcessSandboxMode::Unconfined,
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
    async fn only_a_cell_batch_is_pointed_at_this_machine_s_compile_cache() {
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
        assert!(cell
            .iter()
            .any(|(key, value)| key == "SCCACHE_SERVER_PORT" && value == "4227"));

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
            toolchains: Vec::new(),
            projects_served: Vec::new(),
            disk_budget_bytes: None,
            memory_budget_bytes: None,
            toolchain_detection: None,
        }
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
            pool.assess_colocated_link(unix_time_ms(), EXECUTOR_LINK_STALL_REMEDIATION_MS),
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
            pool.assess_colocated_link(stalled_at, EXECUTOR_LINK_STALL_REMEDIATION_MS),
            LinkRemediation::Healthy
        );
        assert!(matches!(
            pool.assess_colocated_link(
                unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
                EXECUTOR_LINK_STALL_REMEDIATION_MS,
            ),
            LinkRemediation::Bounce { generation: bounced, .. } if bounced == generation
        ));
    }

    #[test]
    fn an_unattached_colocated_link_is_not_a_link_to_bounce() {
        let pool = Fleet::default();

        assert_eq!(
            pool.assess_colocated_link(
                unix_time_ms() + EXECUTOR_LINK_STALL_REMEDIATION_MS * 10,
                EXECUTOR_LINK_STALL_REMEDIATION_MS,
            ),
            LinkRemediation::Healthy
        );
        assert!(!pool.abandon_stalled_colocated_link(COLOCATED_EXECUTOR_ID, 1, 0));
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
            EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
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
                EXECUTOR_LINK_STALL_REMEDIATION_MS,
            ),
            LinkRemediation::Bounce { .. }
        ));

        let (sender, replacement_executor) = mpsc::unbounded_channel();
        let replacement = pool.attach_executor(sender);
        assert!(replacement > stale_generation);

        assert!(!pool.abandon_stalled_colocated_link(
            COLOCATED_EXECUTOR_ID,
            stale_generation,
            EXECUTOR_LINK_STALL_REMEDIATION_MS + 1,
        ));
        assert_eq!(pool.executor_generation(), Some(replacement));
        assert_eq!(
            pool.assess_colocated_link(unix_time_ms(), EXECUTOR_LINK_STALL_REMEDIATION_MS),
            LinkRemediation::Healthy
        );
        drop(replacement_executor);
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
                "agent/CAIRN-2929-builder-0".into(),
            )],
            "runner-context".into(),
            ProcessSandboxMode::Confined,
        )
        .unwrap();

        assert_eq!(batch.items[0].timeout_ms, 3_000);
        assert_eq!(batch.items[1].timeout_ms, ceiling);
        assert_eq!(batch.items[2].timeout_ms, 3_600_000);
        assert_eq!(batch.items[3].timeout_ms, ceiling);
        assert_eq!(
            batch.items[0].env,
            [(
                "CAIRN_WORKTREE_BRANCH".into(),
                "agent/CAIRN-2929-builder-0".into()
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
                 VALUES ('p', 'default', 'Project', 'P', '{}', 1, 1);",
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
        assert!(executor.try_recv().is_err());
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
        sender.send(ExecutorMessage::Configure { config }).unwrap();
        sender
            .send(ExecutorMessage::Submit {
                request,
                batch: None,
            })
            .unwrap();
        assert!(matches!(
            rx.recv().await,
            Some(ExecutorMessage::Configure { .. })
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
        assert!(matches!(
            executor.recv().await,
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
    fn watchdog_covers_preparation_and_full_process_batch_budget() {
        let mut request = targeted_request(std::env::consts::OS);
        request.wait_horizon_unix_ms = unix_time_ms();
        request.timeout_ms = 500;
        let config = ExecutorConfig {
            project_id: request.project_id.clone(),
            project_key: "CAIRN".into(),
            default_timeout_seconds: 2,
            setup_commands: Vec::new(),
            populate: Default::default(),
            population_source_root: None,
        };
        let batch = ProcessBatch {
            sequential: true,
            stop_on_error: false,
            sandbox_mode: ProcessSandboxMode::Unconfined,
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
        assert!(budget > Duration::from_millis(u64::from(request.timeout_ms)));
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
        request.timeout_ms = cairn_common::run_contract::RUN_BATCH_CEILING_MS;
        let config = ExecutorConfig {
            project_id: request.project_id.clone(),
            project_key: "CAIRN".into(),
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
            timeout_ms: cairn_common::run_contract::RUN_BATCH_CEILING_MS,
            command_resource_identity: None,
            verdict_environment_names: Vec::new(),
        };
        let batch = |sequential: bool| ProcessBatch {
            sequential,
            stop_on_error: false,
            sandbox_mode: ProcessSandboxMode::Unconfined,
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

    /// The instant every placement test derives reading ages from.
    const NOW: u64 = 1_000_000_000;

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
                identity: identity.clone(),
                advertisement: ExecutorAdvertisement {
                    identity,
                    capabilities: ExecutorCapabilities {
                        os: os.into(),
                        arch: "x86_64".into(),
                        logical_cores: 8,
                        toolchains: vec!["rust".into()],
                        projects_served: vec!["p".into()],
                        disk_budget_bytes: None,
                        memory_budget_bytes: None,
                        toolchain_detection: None,
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
                },
                generation: 1,
                sender,
                snapshot: FleetSnapshot::default(),
                last_progress_unix_ms: 1,
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
                    toolchains: Vec::new(),
                    projects_served: Vec::new(),
                    disk_budget_bytes: None,
                    memory_budget_bytes: None,
                    toolchain_detection: None,
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
        let (winner, _, _) = rank_candidates(
            &scored,
            false,
            true,
            &request,
            &ActivePlacementPolicy::default_profile(),
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
            "no route to host",
            4_000,
        );
        let unreachable = fleet.unattached_enrolled_remotes();
        assert_eq!(unreachable[0].link, RemoteLinkState::Unreachable);

        fleet.record_remote_attach_attempt(
            "bglab-ub",
            RemoteLinkState::AttachFailed,
            "executor protocol v28 has no published artifact",
            5_000,
        );
        let failed = fleet.unattached_enrolled_remotes();
        let attempt = failed[0].last_attempt.as_ref().unwrap();
        assert_eq!(failed[0].link, RemoteLinkState::AttachFailed);
        assert_eq!(attempt.attempted_at_unix_ms, 5_000);
        assert!(attempt.reason.contains("no published artifact"));
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
            "no route to host",
            4_000,
        );

        fleet.forget_enrolled_remote("bglab-ub");

        assert!(fleet.unattached_enrolled_remotes().is_empty());
        // A machine nobody is enrolled with cannot accumulate attempts either.
        fleet.record_remote_attach_attempt(
            "bglab-ub",
            RemoteLinkState::AttachFailed,
            "stale supervisor",
            6_000,
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
            started_at_unix_ms: unix_time_ms(),
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

    /// A forecast is taken against the wall clock, so the milliseconds between
    /// placing a fixture's occupant and reading it are real elapsed time and
    /// come off the prediction. The assertion is about WHICH machine spoke, and
    /// the two candidates here are two orders of magnitude apart, so a second of
    /// tolerance distinguishes them without pinning a clock.
    #[track_caller]
    fn assert_relief_near(
        occupancy: &occupancy::MachineOccupancy,
        expected_ms: u64,
        because: &str,
    ) {
        let occupancy::MachineOccupancy::Predicted(forecast) = occupancy else {
            panic!("{because}: expected a prediction, got {occupancy:?}");
        };
        assert!(
            expected_ms.abs_diff(forecast.relief_ms) <= 1_000,
            "{because}: expected relief near {expected_ms}ms, got {}ms",
            forecast.relief_ms
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
            &pool.occupancy_for(scope(Some(&linux))),
            300_000,
            "only the machine that can run it speaks for it",
        );
        assert_relief_near(
            &pool.occupancy_for(scope(None)),
            4_000,
            "an unconstrained batch is genuinely relieved by whichever frees first",
        );

        let named = ExecutorSelector {
            name: Some("bglab-ub".into()),
            ..ExecutorSelector::default()
        };
        assert_relief_near(
            &pool.occupancy_for(scope(Some(&named))),
            300_000,
            "naming a machine is asking about that machine",
        );

        let unsatisfiable = ExecutorSelector {
            required_toolchains: vec!["matlab".into()],
            ..ExecutorSelector::default()
        };
        assert_eq!(
            pool.occupancy_for(scope(Some(&unsatisfiable))),
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
            &two_remotes().occupancy_for(scope),
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
            &unserved.occupancy_for(scope),
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
            &disconnected.occupancy_for(scope),
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
            &pool.occupancy_for(forecast_scope(
                &repository,
                None,
                PlacementMobility::PinnedOrColocated,
            )),
            4_000,
            "untargeted conservative work stays on the machine holding the checkout",
        );

        let named = ExecutorSelector {
            name: Some("bglab-ub".into()),
            ..ExecutorSelector::default()
        };
        assert_relief_near(
            &pool.occupancy_for(forecast_scope(
                &repository,
                Some(&named),
                PlacementMobility::PinnedOrColocated,
            )),
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
            &occupied_fleet().occupancy_for(forecast_scope(
                &repository,
                None,
                PlacementMobility::SpillEligible,
            )),
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
                project_key: Some("CAIRN".into()),
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

    #[test]
    fn lost_acquire_response_keeps_pending_route_on_original_executor() {
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

        // Dispatch was accepted, but neither a response nor an occupant snapshot
        // arrived before the owning connection disappeared.
        pool.connections.lock().unwrap().remove("first");
        assert!(matches!(
            pool.resolve_residency_acquire_route(&mut request),
            Err(ResidencyResult::Failed {
                kind: ResidencyFailureKind::Admission,
                ..
            })
        ));
        assert_eq!(pool.residency_routes.lock().unwrap().len(), 1);
    }

    #[test]
    fn runner_restart_preserves_ambiguous_pending_lifetime_route() {
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

        let replacement = Fleet::with_residency_route_path(route_path);
        replacement
            .connections
            .lock()
            .unwrap()
            .insert("second".into(), fleet_entry("second", "linux", 0, &[]).1);
        let mut retry = request;
        assert!(matches!(
            replacement.resolve_residency_acquire_route(&mut retry),
            Err(ResidencyResult::Failed {
                kind: ResidencyFailureKind::Admission,
                ..
            })
        ));
        let routes = replacement.residency_routes.lock().unwrap();
        assert_eq!(routes.len(), 1);
        assert!(routes.values().next().unwrap().pending);
        assert_eq!(routes.values().next().unwrap().executor_id, "first");
    }

    #[test]
    fn fleet_lifetime_retry_does_not_rehome_disconnected_lease() {
        let pool = Fleet::default();
        pool.connections
            .lock()
            .unwrap()
            .insert("second".into(), fleet_entry("second", "linux", 0, &[]).1);
        let mut request = dev_instance_request("lease");
        pool.residency_routes.lock().unwrap().insert(
            ("first".into(), "lease".into()),
            ResidencyRoute {
                holder: request.holder.clone(),
                repository: request.repository.clone(),
                executor_id: "first".into(),
                pending: false,
            },
        );
        assert!(matches!(
            pool.resolve_residency_acquire_route(&mut request),
            Err(ResidencyResult::Failed {
                kind: ResidencyFailureKind::Admission,
                ..
            })
        ));
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
            project_key: "P".into(),
            default_timeout_seconds: 5,
            setup_commands: Vec::new(),
            populate: cairn_worktree::PopulateConfig {
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
                }],
                ..FleetSnapshot::default()
            },
            ExecutorSubstrateReport::default(),
        );
        key
    }

    #[tokio::test]
    async fn a_subscriber_waits_out_capacity_contention_until_its_leader_completes() {
        let pool = Fleet::default();
        let (executor_tx, mut executor_rx) = mpsc::unbounded_channel();
        let generation = pool.attach_executor(executor_tx);
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
        assert_eq!(RemotePlatform::DarwinArm64.arch(), "arm64");
        assert_eq!(RemotePlatform::DarwinArm64.target(), "aarch64-apple-darwin");
        // `arch()` was a constant "x86_64" before Darwin existed; the other two
        // platforms must be unaffected by its promotion to a match.
        assert_eq!(RemotePlatform::LinuxX86_64.arch(), "x86_64");
        assert_eq!(RemotePlatform::WindowsX86_64.arch(), "x86_64");
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
