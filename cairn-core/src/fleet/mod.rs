//! Runner-side fleet-placement facade for supervised and enrolled executors.
//!
//! Core owns request construction, settings resolution, result correlation, and
//! the cached UI snapshot. Scheduling, workspaces, processes, cancellation, and
//! mutation sealing exist only in the executor process.

pub(crate) mod lifetime;
mod resource_profiles;

use crate::mcp::handlers::run::{ResolvedRunBatch, RunSpec};
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::{
    CellOccupant, ExecutorAdvertisement, ExecutorCapabilities, ExecutorConfig,
    ExecutorHealthSnapshot, ExecutorHealthStatus, ExecutorIdentity, ExecutorMessage,
    ExecutorSubstrateEvidence, ExecutorSubstrateReport, ExecutorSubstrateState,
    LifetimeLeaseAcquireRequest, LifetimeLeaseDeclaration, LifetimeLeaseFailureKind,
    LifetimeLeaseFence, LifetimeLeaseOperation, LifetimeLeaseResult, LifetimeProcessEvent,
    LifetimeProcessEventKind, PlacementConstraints, ProcessBatch, ProcessBatchExecution,
    ProcessBatchItem, ProcessSandboxMode, RepositoryLocator, RunnerCallback, RunnerCallbackResult,
    EXECUTOR_PROGRESS_FRESHNESS_MS, MANAGED_OBJECT_REQUEST_TIMEOUT_SECONDS,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::time::Instant;

pub use cairn_common::executor_protocol::{
    ActiveCellRequest, CellExecutionMeta, CellOutcome, CellPriority, CellRequest,
    CellUnavailableReason, CommandResourceIdentity, ExecutingCellRequest, FleetSnapshot,
    MutationDelta, MutationPolicy, PersistentCellLifecycle, PersistentCellState, QueuedCellRequest,
    ResourceReservation, ResourceReservationSource,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FleetConfig {
    #[serde(default = "default_acquisition_deadline_seconds")]
    pub(crate) acquisition_deadline_seconds: u64,
    #[serde(default = "default_timeout_seconds")]
    pub(crate) default_timeout_seconds: u64,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub executor_policies: HashMap<String, cairn_common::executor_protocol::ExecutorRuntimePolicy>,
    /// Runner-owned SSH executor declarations, keyed by stable executor ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub remote_executors: BTreeMap<String, RemoteExecutorConfig>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RemotePlatform {
    #[default]
    LinuxX86_64,
    WindowsX86_64,
}

impl RemotePlatform {
    pub fn os(self) -> &'static str {
        match self {
            Self::LinuxX86_64 => "linux",
            Self::WindowsX86_64 => "windows",
        }
    }

    pub fn arch(self) -> &'static str {
        "x86_64"
    }

    pub fn target(self) -> &'static str {
        match self {
            Self::LinuxX86_64 => "x86_64-unknown-linux-gnu",
            Self::WindowsX86_64 => "x86_64-pc-windows-msvc",
        }
    }

    fn is_absolute(self, path: &str) -> bool {
        match self {
            Self::LinuxX86_64 => path.starts_with('/'),
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
    pub fn validate(&self) -> Result<(), String> {
        let mut device_ids = HashSet::new();
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
        }
        Ok(())
    }
}

fn pauses_subscriber_deadline(state: ExecutorSubstrateState) -> bool {
    matches!(
        state,
        ExecutorSubstrateState::SupervisorSpawning
            | ExecutorSubstrateState::SupervisorRespawning
            | ExecutorSubstrateState::ProtocolAttaching
            | ExecutorSubstrateState::InitialStorageSweep
            | ExecutorSubstrateState::StorageAccounting
            | ExecutorSubstrateState::DispatchPreparing
            | ExecutorSubstrateState::SlotAdoption
            | ExecutorSubstrateState::CapacityBusy
    )
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
            acquisition_deadline_seconds: default_acquisition_deadline_seconds(),
            default_timeout_seconds: default_timeout_seconds(),
            executor_policies: HashMap::new(),
            remote_executors: BTreeMap::new(),
        }
    }
}

fn default_acquisition_deadline_seconds() -> u64 {
    20
}
fn default_timeout_seconds() -> u64 {
    30 * 60
}

type RequestIdentity = (String, String);
const COLOCATED_EXECUTOR_ID: &str = "colocated";
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
    waiter: oneshot::Sender<LifetimeLeaseResult>,
}
type PendingLifetimeResults = HashMap<String, PendingLifetimeResult>;

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
    notify: Notify,
}

pub(crate) struct PublicationGuard {
    coordination: PublicationCoordination,
    published: bool,
}

pub(crate) enum PublicationRole {
    Publisher(PublicationGuard),
    Published,
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
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) async fn acquire(&self) -> PublicationRole {
        loop {
            if self.state.published.load(Ordering::Acquire) {
                return PublicationRole::Published;
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
    pub(crate) fn published(mut self) {
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

type LifetimeProcessSubscriber = Arc<dyn Fn(LifetimeProcessEvent) + Send + Sync>;

#[derive(Clone, Default)]
pub struct Fleet {
    connections: Arc<Mutex<HashMap<String, ExecutorConnectionState>>>,
    connection_generations: Arc<Mutex<HashMap<String, u64>>>,
    disconnect_origins: Arc<Mutex<HashMap<(String, u64), ExecutorDisconnectOrigin>>>,
    connection_ready: Arc<tokio::sync::Notify>,
    pending: Arc<Mutex<PendingResults>>,
    pending_lifetime: Arc<Mutex<PendingLifetimeResults>>,
    pending_policy: Arc<Mutex<HashMap<String, PendingPolicyResult>>>,
    pending_drain: Arc<Mutex<HashMap<String, PendingDrainResult>>>,
    lifetime_routes: Arc<Mutex<HashMap<(String, String), LifetimeRoute>>>,
    lifetime_route_path: Arc<Option<PathBuf>>,
    lifetime_route_store_error: Arc<Mutex<Option<String>>>,
    lifetime_acquisitions: Arc<tokio::sync::Mutex<()>>,
    lifetime_process_subscribers: Arc<Mutex<Vec<LifetimeProcessSubscriber>>>,
    cancelled_leaders: Arc<Mutex<HashSet<RequestIdentity>>>,
    coalesced_leaders: Arc<Mutex<HashSet<RequestIdentity>>>,
    preparing_leaders: Arc<Mutex<HashMap<RequestIdentity, LeaderPreparation>>>,
    in_flight: Arc<Mutex<InFlightRegistry>>,
    runner_contexts: Arc<Mutex<HashMap<String, RunnerCallbackContext>>>,
    recent_cached_completions:
        Arc<Mutex<VecDeque<cairn_common::executor_protocol::CellCompletion>>>,
    expected_executor_build_ids: Arc<Mutex<HashMap<String, String>>>,
    colocated_substrate_state: Arc<Mutex<Option<ExecutorSubstrateEvidence>>>,
}

#[derive(Clone)]
struct RunnerCallbackContext {
    request: Option<crate::mcp::types::McpCallbackRequest>,
    run_context: Option<crate::mcp::handlers::RunContext>,
    check_status_board: Option<crate::execution::checks::CheckStatusBoard>,
}

struct PreparedExecution {
    executor_config: ExecutorConfig,
    object_plane: Arc<crate::orchestrator::object_plane::ObjectPlaneState>,
    db: Arc<cairn_db::storage::LocalDb>,
}

#[derive(Clone, Copy)]
struct LeaderPreparation {
    since_unix_ms: u64,
    last_progress_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct LifetimeRoute {
    declaration: LifetimeLeaseDeclaration,
    executor_id: String,
    pending: bool,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistentLifetimeRoutes {
    #[serde(default)]
    routes: Vec<LifetimeRoute>,
}

impl Fleet {
    pub(crate) fn subscribe_lifetime_process_events(
        &self,
        subscriber: impl Fn(LifetimeProcessEvent) + Send + Sync + 'static,
    ) {
        self.lifetime_process_subscribers
            .lock()
            .unwrap()
            .push(Arc::new(subscriber));
    }

    pub(crate) fn with_lifetime_route_path(path: PathBuf) -> Self {
        let pool = Self {
            lifetime_route_path: Arc::new(Some(path.clone())),
            ..Self::default()
        };
        match load_lifetime_routes(&path) {
            Ok(routes) => *pool.lifetime_routes.lock().unwrap() = routes,
            Err(error) => *pool.lifetime_route_store_error.lock().unwrap() = Some(error),
        }
        pool
    }

    fn update_lifetime_routes<R>(
        &self,
        mutation: impl FnOnce(&mut HashMap<(String, String), LifetimeRoute>) -> R,
    ) -> Result<R, String> {
        let mut routes = self.lifetime_routes.lock().unwrap();
        let previous = routes.clone();
        let result = mutation(&mut routes);
        if *routes == previous {
            return Ok(result);
        }
        if let Some(path) = self.lifetime_route_path.as_ref() {
            if let Err(error) = persist_lifetime_routes(path, &routes) {
                *routes = previous;
                *self.lifetime_route_store_error.lock().unwrap() = Some(error.clone());
                return Err(error);
            }
        }
        *self.lifetime_route_store_error.lock().unwrap() = None;
        Ok(result)
    }

    fn ensure_lifetime_route_store_available(&self) -> Result<(), String> {
        if self.lifetime_route_store_error.lock().unwrap().is_none() {
            return Ok(());
        }

        let Some(path) = self.lifetime_route_path.as_ref() else {
            *self.lifetime_route_store_error.lock().unwrap() = None;
            return Ok(());
        };
        let mut routes = self.lifetime_routes.lock().unwrap();
        if self.lifetime_route_store_error.lock().unwrap().is_none() {
            return Ok(());
        }

        let recovered = load_lifetime_routes(path)
            .and_then(|recovered| persist_lifetime_routes(path, &recovered).map(|()| recovered));
        match recovered {
            Ok(recovered) => {
                *routes = recovered;
                *self.lifetime_route_store_error.lock().unwrap() = None;
                Ok(())
            }
            Err(error) => {
                *self.lifetime_route_store_error.lock().unwrap() = Some(error.clone());
                Err(error)
            }
        }
    }
}

fn load_lifetime_routes(path: &Path) -> Result<HashMap<(String, String), LifetimeRoute>, String> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let bytes = std::fs::read(path)
        .map_err(|error| format!("read lifetime route authority {}: {error}", path.display()))?;
    let persisted: PersistentLifetimeRoutes = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse lifetime route authority {}: {error}", path.display()))?;
    let mut routes = HashMap::new();
    for route in persisted.routes {
        let key = (
            route.executor_id.clone(),
            route.declaration.lease_id.clone(),
        );
        if routes.insert(key, route).is_some() {
            return Err(format!(
                "lifetime route authority {} contains duplicate routes",
                path.display()
            ));
        }
    }
    Ok(routes)
}

fn persist_lifetime_routes(
    path: &Path,
    routes: &HashMap<(String, String), LifetimeRoute>,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            format!(
                "create lifetime route authority directory {}: {error}",
                parent.display()
            )
        })?;
    }
    let mut persisted = PersistentLifetimeRoutes {
        routes: routes.values().cloned().collect(),
    };

    persisted.routes.sort_by(|a, b| {
        (&a.executor_id, &a.declaration.lease_id).cmp(&(&b.executor_id, &b.declaration.lease_id))
    });
    let bytes = serde_json::to_vec_pretty(&persisted)
        .map_err(|error| format!("serialize lifetime route authority: {error}"))?;
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
            "open lifetime route authority {}: {error}",
            temporary.display()
        )
    })?;
    file.write_all(&bytes).map_err(|error| {
        format!(
            "write lifetime route authority {}: {error}",
            temporary.display()
        )
    })?;
    file.sync_all().map_err(|error| {
        format!(
            "sync lifetime route authority {}: {error}",
            temporary.display()
        )
    })?;
    std::fs::rename(&temporary, path).map_err(|error| {
        format!(
            "publish lifetime route authority {}: {error}",
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
}

#[derive(Clone, Debug)]
struct SelectedExecutor {
    executor_id: String,
    device_id: String,
    generation: u64,
    sender: mpsc::UnboundedSender<ExecutorMessage>,
    colocated: bool,
    capabilities: ExecutorCapabilities,
}

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
            },
            current_load: 0,
            warm_roots: Vec::new(),
            observed_at_unix_ms: unix_time_ms(),
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

    pub fn executor_generation(&self) -> Option<u64> {
        self.connections
            .lock()
            .unwrap()
            .values()
            .find(|entry| entry.colocated && !entry.sender.is_closed())
            .map(|entry| entry.generation)
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
            ExecutorMessage::LifetimeLeaseResponse {
                correlation_id,
                result,
            } => {
                let pending = self
                    .pending_lifetime
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                if let Some(pending) = pending {
                    if pending.executor_id != executor_id || pending.generation != generation {
                        self.pending_lifetime
                            .lock()
                            .unwrap()
                            .insert(correlation_id, pending);
                        return false;
                    }
                    let _ = pending.waiter.send(result);
                }
                false
            }
            ExecutorMessage::LifetimeProcessEvent { event } => {
                let valid = self
                    .connections
                    .lock()
                    .unwrap()
                    .get(executor_id)
                    .filter(|connection| connection.generation == generation)
                    .is_some_and(|connection| {
                        connection.snapshot.cells.iter().any(|cell| {
                            cell.lease_epoch == event.lease_epoch
                                && cell
                                    .occupant
                                    .as_ref()
                                    .and_then(CellOccupant::lifetime)
                                    .is_some_and(|lease| {
                                        lease.declaration.lease_id == event.lease_id
                                            && lease.incarnation_id == event.incarnation_id
                                            && lease.processes.get(&event.process_key).is_some_and(
                                                |process| {
                                                    process.generation == event.process_generation
                                                },
                                            )
                                    })
                        })
                    });
                if valid {
                    for subscriber in self.lifetime_process_subscribers.lock().unwrap().iter() {
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
            if let Some(active) = cell.occupant.as_mut().and_then(|occupant| match occupant {
                cairn_common::executor_protocol::CellOccupant::Command(active) => Some(active),
                cairn_common::executor_protocol::CellOccupant::Lifetime(_) => None,
            }) {
                active.executor_id = executor_id.to_string();
            }
        }
        for queued in &mut snapshot.queued_requests {
            queued.executor_id = executor_id.to_string();
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
                let lease = cell.occupant.as_ref().and_then(CellOccupant::lifetime)?;
                Some(
                    lease
                        .processes
                        .iter()
                        .filter(move |(_, process)| {
                            matches!(
                                process.status,
                                cairn_common::executor_protocol::LifetimeProcessStatus::Exited {
                                    executor_lost: true,
                                    ..
                                }
                            )
                        })
                        .map(move |(process_key, process)| LifetimeProcessEvent {
                            lease_id: lease.declaration.lease_id.clone(),
                            incarnation_id: lease.incarnation_id.clone(),
                            lease_epoch: cell.lease_epoch,
                            process_key: process_key.clone(),
                            process_generation: process.generation,
                            event: LifetimeProcessEventKind::State {
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
                cell.occupant
                    .as_ref()
                    .and_then(CellOccupant::lifetime)
                    .map(|lease| LifetimeRoute {
                        declaration: lease.declaration.clone(),
                        executor_id: executor_id.to_string(),
                        pending: false,
                    })
            })
            .collect::<Vec<_>>();
        entry.snapshot = snapshot;
        entry.health = health;
        drop(connections);
        if let Err(error) = self.update_lifetime_routes(|known| {
            known.retain(|(route_executor, _), route| {
                route_executor != executor_id || route.pending
            });
            for route in routes {
                known.insert(
                    (
                        route.executor_id.clone(),
                        route.declaration.lease_id.clone(),
                    ),
                    route,
                );
            }
        }) {
            tracing::error!(%error, "persist executor lifetime route snapshot failed");
        }
        for event in reconciled_process_events {
            for subscriber in self.lifetime_process_subscribers.lock().unwrap().iter() {
                subscriber(event.clone());
            }
        }
        self.connection_ready.notify_waiters();
        snapshot_changed
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
            if let Some(occupancy) = &snapshot.lifetime_cell_occupancy {
                let aggregate_occupancy = aggregate
                    .lifetime_cell_occupancy
                    .get_or_insert_with(Default::default);
                aggregate_occupancy.lease_count += occupancy.lease_count;
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
            if let Some(cairn_common::executor_protocol::CellOccupant::Command(active)) =
                &mut cell.occupant
            {
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
        // Three missed 30-second executor heartbeats make the live connection stale.
        const STALE_AFTER_MS: u64 = 90_000;
        let connections = self.connections.lock().unwrap();
        let expected_build_ids = self.expected_executor_build_ids.lock().unwrap();
        let mut values: Vec<_> = connections
            .values()
            .map(|entry| {
                let heartbeat_age_ms =
                    captured_at_unix_ms.saturating_sub(entry.advertisement.observed_at_unix_ms);
                ExecutorHealthSnapshot {
                    identity: entry.identity.clone(),
                    colocated: entry.colocated,
                    status: if heartbeat_age_ms > STALE_AFTER_MS {
                        ExecutorHealthStatus::Stale
                    } else {
                        ExecutorHealthStatus::Online
                    },
                    heartbeat_age_ms,
                    advertisement: entry.advertisement.clone(),
                    admission: entry.health.admission.clone(),
                    queues: entry.health.queues.clone(),
                    host: entry.health.host.clone(),
                    disk: entry.health.disk.clone(),
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
            })
            .collect();
        values.sort_by(|a, b| a.identity.executor_id.cmp(&b.identity.executor_id));
        values
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

    pub async fn operate_lifetime_lease(
        &self,
        orch: &Orchestrator,
        mut operation: LifetimeLeaseOperation,
    ) -> LifetimeLeaseResult {
        // Protect route resolution, pending-route reservation, and dispatch against duplicate
        // acquires. Correlation-keyed response waiters are independent.
        let acquire_guard = if matches!(operation, LifetimeLeaseOperation::Acquire { .. }) {
            Some(self.lifetime_acquisitions.lock().await)
        } else {
            None
        };
        let mut pending_acquire_route = None;
        let (selected, executor_config, object_request, object_plane) = match &mut operation {
            LifetimeLeaseOperation::Acquire {
                request: acquisition,
            } => {
                if let Some(selected) =
                    match self.resolve_lifetime_acquire_route(&mut acquisition.declaration) {
                        Ok(selected) => selected,
                        Err(failure) => return failure,
                    }
                {
                    (selected, None, None, None)
                } else {
                    let mut placement = lifetime_placement_request(acquisition);
                    let prepared = match self.prepare_execution(orch, &placement).await {
                        Ok(prepared) => prepared,
                        Err(outcome) => {
                            return lifetime_core_failure(
                                LifetimeLeaseFailureKind::Admission,
                                "prepare lifetime lease placement",
                                Some(outcome),
                            )
                        }
                    };
                    if let Err(diagnostic) =
                        require_colocated_population(&mut placement, &prepared.executor_config)
                    {
                        return lifetime_core_failure(
                            LifetimeLeaseFailureKind::Admission,
                            diagnostic,
                            None,
                        );
                    }
                    let selected = match self.select_executor(&mut placement).await {
                        Ok(selected) => selected,
                        Err(outcome) => {
                            return lifetime_core_failure(
                                LifetimeLeaseFailureKind::Admission,
                                "select lifetime lease executor",
                                Some(outcome),
                            )
                        }
                    };
                    if !selected.colocated {
                        let identity = acquisition.declaration.repository.identity();
                        acquisition.declaration.repository = RepositoryLocator::ManagedObjects {
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
                    pending_acquire_route = Some(LifetimeRoute {
                        declaration: acquisition.declaration.clone(),
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
                let Some(lease_id) = lifetime_operation_lease_id(&operation) else {
                    return lifetime_core_failure(
                        LifetimeLeaseFailureKind::InvalidDeclaration,
                        "lifetime operation has no lease identity",
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
                            cell.occupant
                                .as_ref()
                                .and_then(CellOccupant::lifetime)
                                .filter(|lease| lease.declaration.lease_id == lease_id)
                                .map(|lease| lease.declaration.clone())
                        })
                        .map(|declaration| {
                            (
                                SelectedExecutor {
                                    executor_id: executor_id.clone(),
                                    device_id: connection.identity.device_id.clone(),
                                    generation: connection.generation,
                                    sender: connection.sender.clone(),
                                    colocated: connection.colocated,
                                    capabilities: connection.advertisement.capabilities.clone(),
                                },
                                declaration,
                            )
                        })
                });
                let Some((selected, declaration)) = routed else {
                    return lifetime_core_failure(
                        LifetimeLeaseFailureKind::Unavailable,
                        "no connected executor reports the lifetime lease",
                        None,
                    );
                };
                if !selected.colocated {
                    if let LifetimeLeaseOperation::RefreshCheckout { fence, base_commit } =
                        &operation
                    {
                        let request = lifetime_refresh_request(&declaration, fence, base_commit);
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
            if let Err(error) = self.reserve_pending_lifetime_route(route.clone()) {
                return lifetime_core_failure(
                    LifetimeLeaseFailureKind::Persistence,
                    format!("persist pending lifetime route authority: {error}"),
                    None,
                );
            }
        }
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_lifetime.lock().unwrap().insert(
            correlation_id.clone(),
            PendingLifetimeResult {
                executor_id: selected.executor_id.clone(),
                generation: selected.generation,
                waiter: tx,
            },
        );
        let sent = executor_config
            .map(|config| selected.sender.send(ExecutorMessage::Configure { config }))
            .transpose()
            .and_then(|_| {
                selected.sender.send(ExecutorMessage::LifetimeLeaseRequest {
                    correlation_id: correlation_id.clone(),
                    operation,
                })
            });
        if sent.is_err() {
            self.pending_lifetime
                .lock()
                .unwrap()
                .remove(&correlation_id);
            if let Some(route) = pending_acquire_route.as_ref() {
                self.clear_pending_lifetime_route(route);
            }
            return lifetime_core_failure(
                LifetimeLeaseFailureKind::Admission,
                "executor connection closed while sending lifetime operation",
                None,
            );
        }
        drop(acquire_guard);
        let timeout = object_request
            .as_ref()
            .map(|request| {
                Duration::from_millis(
                    request
                        .deadline_unix_ms
                        .saturating_sub(unix_time_ms())
                        .max(1),
                )
            })
            .unwrap_or(Duration::from_secs(30));
        let result = match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => lifetime_core_failure(
                LifetimeLeaseFailureKind::Admission,
                "executor dropped the lifetime operation response",
                None,
            ),
            Err(_) => {
                self.pending_lifetime
                    .lock()
                    .unwrap()
                    .remove(&correlation_id);
                lifetime_core_failure(
                    LifetimeLeaseFailureKind::Admission,
                    "lifetime operation response deadline elapsed",
                    None,
                )
            }
        };
        match &result {
            LifetimeLeaseResult::State { cell } => {
                if let Some(lease) = cell.occupant.as_ref().and_then(CellOccupant::lifetime) {
                    if let Err(error) = self.update_lifetime_routes(|routes| {
                        routes.insert(
                            (
                                selected.executor_id.clone(),
                                lease.declaration.lease_id.clone(),
                            ),
                            LifetimeRoute {
                                declaration: lease.declaration.clone(),
                                executor_id: selected.executor_id.clone(),
                                pending: false,
                            },
                        );
                    }) {
                        tracing::error!(%error, "persist authoritative lifetime route failed");
                    }
                }
            }
            LifetimeLeaseResult::Released { lease_id, .. } => {
                if let Err(error) = self.update_lifetime_routes(|routes| {
                    routes.retain(|(_, known_lease_id), _| known_lease_id != lease_id);
                }) {
                    tracing::error!(%error, "persist released lifetime route removal failed");
                }
            }
            LifetimeLeaseResult::Failed { .. } => {
                if let Some(route) = pending_acquire_route.as_ref() {
                    self.clear_pending_lifetime_route(route);
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

    fn reserve_pending_lifetime_route(&self, route: LifetimeRoute) -> Result<(), String> {
        debug_assert!(route.pending);
        self.update_lifetime_routes(|routes| {
            routes.insert(
                (
                    route.executor_id.clone(),
                    route.declaration.lease_id.clone(),
                ),
                route,
            );
        })
    }

    fn clear_pending_lifetime_route(&self, route: &LifetimeRoute) {
        if let Err(error) = self.update_lifetime_routes(|routes| {
            routes.retain(|key, known| {
                key != &(
                    route.executor_id.clone(),
                    route.declaration.lease_id.clone(),
                ) || !known.pending
                    || known.declaration != route.declaration
            });
        }) {
            tracing::error!(%error, "persist pending lifetime route removal failed");
        }
    }

    #[allow(clippy::result_large_err)]
    fn resolve_lifetime_acquire_route(
        &self,
        declaration: &mut LifetimeLeaseDeclaration,
    ) -> Result<Option<SelectedExecutor>, LifetimeLeaseResult> {
        if let Err(error) = self.ensure_lifetime_route_store_available() {
            return Err(lifetime_core_failure(
                LifetimeLeaseFailureKind::Persistence,
                format!("lifetime route authority is unavailable: {error}"),
                None,
            ));
        }
        let existing = {
            let known = self.lifetime_routes.lock().unwrap();
            known
                .values()
                .filter(|route| {
                    route.declaration.lease_id == declaration.lease_id
                        || lifetime_declaration_name_matches(&route.declaration, declaration)
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        if existing.len() > 1 {
            return Err(lifetime_core_failure(
                LifetimeLeaseFailureKind::ConflictingDeclaration,
                "multiple executors report the same lifetime lease identity",
                None,
            ));
        }
        let Some(route) = existing.into_iter().next() else {
            return Ok(None);
        };
        // The declaration's initial commit names the first materialization, not
        // the lease's moving head. An idempotent terminal restart may occur after
        // live refreshes advanced it, so compare against the persisted original
        // declaration rather than treating the observed current tip as a new
        // lease identity.
        declaration.initial_base_commit = route.declaration.initial_base_commit.clone();
        if !lifetime_declarations_equivalent(&route.declaration, declaration) {
            return Err(lifetime_core_failure(
                LifetimeLeaseFailureKind::ConflictingDeclaration,
                "lifetime lease name or lease ID is already bound to another declaration",
                None,
            ));
        }
        declaration.repository = route.declaration.repository.clone();
        self.connections
            .lock()
            .unwrap()
            .get(&route.executor_id)
            .map(selected_executor)
            .map(Some)
            .ok_or_else(|| {
                lifetime_core_failure(
                    LifetimeLeaseFailureKind::Admission,
                    "the executor owning this lifetime lease is disconnected",
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
        let deadline_unix_ms = request.deadline_unix_ms;
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
        self.await_coalesced(public_identity, deadline_unix_ms, rx)
            .await
    }

    async fn await_coalesced(
        &self,
        identity: RequestIdentity,
        deadline_unix_ms: u64,
        mut rx: oneshot::Receiver<CoalescedCellOutcome>,
    ) -> Result<CoalescedCellOutcome, CellOutcome> {
        let mut guard = CoalescedSubscriberDropGuard {
            pool: self.clone(),
            identity: identity.clone(),
            armed: true,
        };
        let mut deadline_unix_ms = deadline_unix_ms;
        let subscriber_started_at = unix_time_ms();
        let mut pause_observed_at = None;
        let mut execution_started = false;
        loop {
            let now = unix_time_ms();
            let hold = self.leader_substrate_hold(&identity);
            if hold
                .as_ref()
                .is_some_and(|hold| hold.state == ExecutorSubstrateState::ExecutionRunning)
            {
                execution_started = true;
            }
            if !execution_started {
                if let Some(hold) = hold {
                    if !pauses_subscriber_deadline(hold.state) {
                        if let Some(observed_at) = pause_observed_at.take() {
                            deadline_unix_ms =
                                deadline_unix_ms.saturating_add(now.saturating_sub(observed_at));
                        }
                    } else {
                        let hold_started_at =
                            hold.since_unix_ms.max(subscriber_started_at).min(now);
                        let observed_at = pause_observed_at.replace(now).unwrap_or(hold_started_at);
                        deadline_unix_ms =
                            deadline_unix_ms.saturating_add(now.saturating_sub(observed_at));
                    }
                } else if let Some(observed_at) = pause_observed_at.take() {
                    deadline_unix_ms =
                        deadline_unix_ms.saturating_add(now.saturating_sub(observed_at));
                }
            }
            let remaining = deadline_unix_ms.saturating_sub(now);
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

    pub(crate) async fn submit_pure_verdict_batch(
        &self,
        orch: &Orchestrator,
        request: CellRequest,
        items: Vec<PureVerdictBatchItem>,
        run_context: Option<crate::mcp::handlers::RunContext>,
    ) -> Vec<Result<CoalescedCellOutcome, CellOutcome>> {
        if request.mutation_policy != MutationPolicy::PureVerdict {
            return items
                .into_iter()
                .map(|_| {
                    Err(executor_unavailable(
                        "coalesced batch submission requires pure-verdict mutation policy".into(),
                    ))
                })
                .collect();
        }
        let leader = (request.request_id.clone(), request.attempt_id.clone());
        let deadline_unix_ms = request.deadline_unix_ms;
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
                    },
                );
                id
            });
            let batch = ProcessBatch {
                sequential: true,
                stop_on_error: false,
                promote_timeouts: false,
                sandbox_mode: ProcessSandboxMode::Confined,
                items: newly_claimed.into_iter().map(|item| item.process).collect(),
                runner_context_id: runner_context_id.clone(),
            };
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
                .map(|(identity, rx)| self.await_coalesced(identity, deadline_unix_ms, rx)),
        )
        .await
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
            },
        );
        let sandbox_mode = if matches!(
            request.repository,
            RepositoryLocator::ExistingCheckout { .. }
        ) {
            ProcessSandboxMode::ReadOnlyCheckout
        } else {
            crate::mcp::handlers::fence::resolve_run_fence(orch, &batch.request)
                .await
                .map(|(_, fence)| {
                    if crate::services::sandbox::sandbox_applies(fence) {
                        ProcessSandboxMode::Confined
                    } else {
                        ProcessSandboxMode::Unconfined
                    }
                })
                .unwrap_or(ProcessSandboxMode::Unconfined)
        };
        let batch = match serialize_process_batch(
            batch,
            request.timeout_ms,
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
            },
        );
        let batch = ProcessBatch {
            sequential: true,
            stop_on_error: false,
            sandbox_mode: ProcessSandboxMode::Confined,
            promote_timeouts: false,
            items,
            runner_context_id: Some(runner_context_id.clone()),
        };
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
            | RunnerCallback::ActivatePromotedTerminal {
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
                if context.request.as_ref().is_some_and(|request| {
                    !crate::jj::is_jj_dir(std::path::Path::new(&request.cwd))
                }) {
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
            RunnerCallback::ActivatePromotedTerminal {
                fence,
                process_key,
                command,
                output,
                process_generation,
                ..
            } => {
                let Some(run_context) = context.run_context else {
                    return RunnerCallbackResult::Failed {
                        diagnostic: "timeout promotion has no originating run context".into(),
                    };
                };
                match crate::mcp::handlers::terminal::activate_promoted_executor_terminal(
                    orch,
                    &run_context,
                    fence,
                    process_key,
                    &command,
                    output,
                    process_generation,
                )
                .await
                {
                    Ok(terminal) => RunnerCallbackResult::Promoted { terminal },
                    Err(diagnostic) => RunnerCallbackResult::Failed { diagnostic },
                }
            }
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
            RepositoryLocator::ManagedObjects { .. } => None,
        });
        let project_path = project_path.ok_or_else(|| CellOutcome::Unavailable {
            reason: CellUnavailableReason::Preparation,
            diagnostic: "resolve canonical project setup: no local project checkout".into(),
        })?;
        // Resolve setup from the primary checkout, exactly as job-worktree provisioning does;
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
            executor_config: ExecutorConfig {
                project_id: request.project_id.clone(),
                project_key,
                acquisition_deadline_seconds: config.acquisition_deadline_seconds,
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
        } = prepared;
        let mut request = request;
        if let Err(diagnostic) = require_colocated_population(&mut request, &executor_config) {
            return CellOutcome::Unavailable {
                reason: CellUnavailableReason::NoMatchingExecutor,
                diagnostic,
            };
        }
        let selected = match self.select_executor(&mut request).await {
            Ok(selected) => selected,
            Err(outcome) => return outcome,
        };
        let mut toolchains = selected.capabilities.toolchains.clone();
        toolchains.sort();
        let profile_context = resource_profiles::ProfileContext {
            executor_class: format!("{}:{}", selected.device_id, selected.executor_id),
            os: selected.capabilities.os.clone(),
            arch: selected.capabilities.arch.clone(),
            toolchain_fingerprint: toolchains.join("\u{1f}"),
        };
        let batch_profile_identities: Vec<_> = batch
            .as_ref()
            .map(|batch| {
                batch
                    .items
                    .iter()
                    .filter_map(|item| item.command_resource_identity.clone())
                    .collect()
            })
            .unwrap_or_default();
        if request.resource_reservation == ResourceReservation::default() {
            let prior = zero_knowledge_reservation(&selected.capabilities);
            if batch_profile_identities.is_empty() {
                let resolved = resource_profiles::resolve_reservation(
                    db.clone(),
                    request.command_resource_identity.as_ref(),
                    &profile_context,
                    prior,
                )
                .await;
                request.resource_reservation = resolved.reservation;
                request.learned_estimate = resolved.learned_estimate;
            } else {
                let mut reservation = ResourceReservation::default();
                let mut learned_estimates = Vec::with_capacity(batch_profile_identities.len());
                for identity in &batch_profile_identities {
                    let item = resource_profiles::resolve_reservation(
                        db.clone(),
                        Some(identity),
                        &profile_context,
                        prior.clone(),
                    )
                    .await;
                    reservation.memory_bytes =
                        reservation.memory_bytes.max(item.reservation.memory_bytes);
                    reservation.disk_growth_bytes = reservation
                        .disk_growth_bytes
                        .max(item.reservation.disk_growth_bytes);
                    reservation.concurrency_units = reservation
                        .concurrency_units
                        .max(item.reservation.concurrency_units);
                    reservation.source = match (reservation.source, item.reservation.source) {
                        (ResourceReservationSource::Learned, _)
                        | (_, ResourceReservationSource::Learned) => {
                            ResourceReservationSource::Learned
                        }
                        _ => ResourceReservationSource::ZeroKnowledgePrior,
                    };
                    learned_estimates.push(item.learned_estimate);
                }
                request.resource_reservation = reservation;
                request.learned_estimate = aggregate_batch_learned_estimates(&learned_estimates);
            }
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
        }
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
        let sent = selected
            .sender
            .send(ExecutorMessage::Configure {
                config: executor_config,
            })
            .and_then(|_| {
                selected
                    .sender
                    .send(ExecutorMessage::Submit { request, batch })
            });
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
        if sent.is_err() {
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
        let mut rx = rx;
        let mut watchdog_deadline = Instant::now() + watchdog;
        let mut last_observed_at = Instant::now();
        let (outcome, watchdog_expired) = loop {
            let notified = self.connection_ready.notified();
            let now = Instant::now();
            if self
                .request_substrate_hold(&selected.executor_id, selected.generation, &key.0, &key.1)
                .is_some()
            {
                watchdog_deadline += now.saturating_duration_since(last_observed_at);
            }
            last_observed_at = now;
            let remaining = watchdog_deadline.saturating_duration_since(now);
            if remaining.is_zero() {
                let substrate =
                    self.executor_deadline_evidence(&selected.executor_id, selected.generation);
                break (
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
                );
            }
            tokio::select! {
                result = &mut rx => {
                    break (
                        result.unwrap_or_else(|_| executor_unavailable("executor result channel closed".into())),
                        false,
                    );
                }
                _ = tokio::time::sleep(remaining.min(Duration::from_millis(250))) => {}
                _ = notified => {}
            }
        };
        if !selected.colocated {
            object_plane.revoke_request(&key.0, &key.1, &selected.executor_id, selected.generation);
        }
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

    async fn select_executor(
        &self,
        request: &mut CellRequest,
    ) -> Result<SelectedExecutor, CellOutcome> {
        let mut pause_observed_at = None;
        loop {
            let notified = self.connection_ready.notified();
            let selection = self.select_executor_once_with(request, repository_sync_cost);
            match selection {
                Ok(Some(selected)) => {
                    if let Some(observed_at) = pause_observed_at.take() {
                        request.deadline_unix_ms = request
                            .deadline_unix_ms
                            .saturating_add(unix_time_ms().saturating_sub(observed_at));
                    }
                    return Ok(selected);
                }
                Err(diagnostic) => {
                    return Err(CellOutcome::Unavailable {
                        reason: CellUnavailableReason::NoMatchingExecutor,
                        diagnostic,
                    });
                }
                Ok(None) => {}
            }
            let now = unix_time_ms();
            let transient = self.colocated_substrate().filter(|evidence| {
                now.saturating_sub(evidence.last_progress_unix_ms) <= EXECUTOR_PROGRESS_FRESHNESS_MS
            });
            if transient.is_none()
                && request
                    .constraints
                    .as_ref()
                    .is_none_or(PlacementConstraints::is_empty)
            {
                return Err(executor_unavailable(
                    "no colocated executor is configured, enrolled, or being supervised".into(),
                ));
            }
            if transient.is_some() {
                if let Some(observed_at) = pause_observed_at.replace(now) {
                    request.deadline_unix_ms = request
                        .deadline_unix_ms
                        .saturating_add(now.saturating_sub(observed_at));
                }
            } else if let Some(observed_at) = pause_observed_at.take() {
                request.deadline_unix_ms = request
                    .deadline_unix_ms
                    .saturating_add(now.saturating_sub(observed_at));
            }
            let remaining = request.deadline_unix_ms.saturating_sub(now);
            if remaining == 0 {
                return Err(CellOutcome::Unavailable {
                    reason: CellUnavailableReason::NoMatchingExecutor,
                    diagnostic: format!(
                        "no executor satisfying {} became usable before the acquisition deadline",
                        format_constraints(request.constraints.as_ref().unwrap())
                    ),
                });
            }
            let _ = tokio::time::timeout(Duration::from_millis(remaining.clamp(1, 250)), notified)
                .await;
        }
    }

    fn select_executor_once_with(
        &self,
        request: &CellRequest,
        estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
    ) -> Result<Option<SelectedExecutor>, String> {
        // Placement can inspect the local repository to estimate transfer cost.
        // Snapshot the bounded executor metadata first so that work never holds
        // the connection lock needed by transport-side heartbeat and snapshot
        // processing.
        let connections = self.connections.lock().unwrap().clone();
        let selected = choose_executor_with(&connections, request, estimate)?;
        let Some(selected) = selected else {
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
        if is_current {
            Ok(Some(selected))
        } else {
            // A reconnect can replace the selected generation while repository
            // estimation is in flight. Let the outer placement loop rank the
            // fresh connection instead of dispatching through a retired sender.
            Ok(None)
        }
    }

    #[cfg(test)]
    async fn wait_for_executor(
        &self,
        deadline_unix_ms: u64,
    ) -> Result<mpsc::UnboundedSender<ExecutorMessage>, String> {
        let mut request = CellRequest {
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
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            deadline_unix_ms,
            timeout_ms: 0,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: None,
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        };
        self.select_executor(&mut request)
            .await
            .map(|selected| selected.sender)
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
        let mut lifetime = self.pending_lifetime.lock().unwrap();
        let correlations: Vec<_> = lifetime
            .iter()
            .filter(|(_, entry)| entry.executor_id == executor_id)
            .map(|(correlation, _)| correlation.clone())
            .collect();
        for correlation in correlations {
            if let Some(entry) = lifetime.remove(&correlation) {
                let _ = entry.waiter.send(lifetime_core_failure(
                    LifetimeLeaseFailureKind::Admission,
                    diagnostic.to_string(),
                    None,
                ));
            }
        }
    }
}

fn lifetime_declaration_name_matches(
    left: &LifetimeLeaseDeclaration,
    right: &LifetimeLeaseDeclaration,
) -> bool {
    left.repository.identity().project_id == right.repository.identity().project_id
        && left.owner == right.owner
        && left.owner_ref == right.owner_ref
        && left.name == right.name
}

fn lifetime_declarations_equivalent(
    left: &LifetimeLeaseDeclaration,
    right: &LifetimeLeaseDeclaration,
) -> bool {
    left.lease_id == right.lease_id
        && lifetime_declaration_name_matches(left, right)
        && left.purpose == right.purpose
        && left.repository.identity() == right.repository.identity()
        && left.initial_base_commit == right.initial_base_commit
        && left.resource_reservation == right.resource_reservation
        && left.owner_death_policy == right.owner_death_policy
}

fn lifetime_placement_request(acquisition: &LifetimeLeaseAcquireRequest) -> CellRequest {
    let declaration = &acquisition.declaration;
    CellRequest {
        request_id: format!("lifetime-acquire:{}", declaration.lease_id),
        attempt_id: "acquire".into(),
        project_id: declaration.repository.project_id().to_string(),
        repository: declaration.repository.clone(),
        base_commit: declaration.initial_base_commit.clone(),
        command: declaration.purpose.clone(),
        command_class: cairn_common::executor_protocol::CellCommandClass::Other,
        owner: declaration.owner_ref.clone().or_else(|| {
            Some(cairn_common::executor_protocol::CellOwnerRef {
                project_id: declaration.repository.project_id().to_string(),
                project_key: None,
                issue_number: None,
                job_id: None,
                execution_seq: None,
                node_kind: Some(format!("lifetime:{:?}", declaration.owner.kind)),
            })
        }),
        cwd: String::new(),
        env: Vec::new(),
        priority: acquisition.priority,
        deadline_unix_ms: acquisition.deadline_unix_ms,
        timeout_ms: 0,
        mutation_policy: MutationPolicy::PureVerdict,
        requesting_job_id: None,
        affinity_key: Some(format!(
            "lifetime:{}:{}",
            declaration.owner.owner_id, declaration.name
        )),
        constraints: None,
        command_resource_identity: None,
        resource_reservation: declaration.resource_reservation.clone(),
        learned_estimate: None,
    }
}

// Refresh transfers use a commit-specific attempt so concurrent requests cannot revoke each other.
fn lifetime_refresh_request(
    declaration: &LifetimeLeaseDeclaration,
    fence: &LifetimeLeaseFence,
    base_commit: &str,
) -> CellRequest {
    let mut request = lifetime_placement_request(&LifetimeLeaseAcquireRequest {
        declaration: declaration.clone(),
        priority: CellPriority::AgentInteractive,
        deadline_unix_ms: unix_time_ms().saturating_add(30_000),
    });
    request.request_id = format!("lifetime-refresh:{}", declaration.lease_id);
    request.attempt_id = format!(
        "{}:{}:{}",
        fence.incarnation_id, fence.lease_epoch, base_commit
    );
    request.base_commit = base_commit.to_string();
    request
}

fn lifetime_operation_lease_id(operation: &LifetimeLeaseOperation) -> Option<&str> {
    match operation {
        LifetimeLeaseOperation::Acquire { .. } => None,
        LifetimeLeaseOperation::Reclaim { fence }
        | LifetimeLeaseOperation::Renew { fence }
        | LifetimeLeaseOperation::Release { fence }
        | LifetimeLeaseOperation::StartProcess { fence, .. }
        | LifetimeLeaseOperation::StopProcess { fence, .. }
        | LifetimeLeaseOperation::WriteProcessInput { fence, .. }
        | LifetimeLeaseOperation::ResizePty { fence, .. }
        | LifetimeLeaseOperation::RefreshCheckout { fence, .. } => Some(&fence.lease_id),
    }
}

fn lifetime_core_failure(
    kind: LifetimeLeaseFailureKind,
    diagnostic: impl Into<String>,
    outcome: Option<CellOutcome>,
) -> LifetimeLeaseResult {
    LifetimeLeaseResult::Failed {
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
            RepositoryLocator::ExistingCheckout { .. }
        )
    {
        return Ok(());
    }
    let constraints = request.constraints.get_or_insert_with(Default::default);
    if constraints
        .executor_id
        .as_deref()
        .is_some_and(|executor_id| executor_id != COLOCATED_EXECUTOR_ID)
    {
        return Err(
            "worktree population requires the colocated executor because ignored project content is available only in the runner's live primary checkout"
                .into(),
        );
    }
    constraints.executor_id = Some(COLOCATED_EXECUTOR_ID.into());
    Ok(())
}

#[cfg(test)]
fn choose_executor(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
) -> Result<Option<SelectedExecutor>, String> {
    choose_executor_with(connections, request, repository_sync_cost)
}

fn choose_executor_with(
    connections: &HashMap<String, ExecutorConnectionState>,
    request: &CellRequest,
    estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
) -> Result<Option<SelectedExecutor>, String> {
    let constrained = request
        .constraints
        .as_ref()
        .is_some_and(|constraints| !constraints.is_empty());
    if !constrained {
        return Ok(connections
            .values()
            .find(|entry| entry.colocated && !entry.sender.is_closed())
            .map(selected_executor));
    }
    let constraints = request.constraints.as_ref().unwrap();
    let eligible: Vec<_> = connections
        .values()
        .filter(|entry| !entry.sender.is_closed())
        .filter(|entry| serves_project(entry, &request.project_id))
        .filter(|entry| matches_constraints(entry, constraints))
        .collect();
    if eligible.is_empty() {
        return Err(format!(
            "no live enrolled executor satisfies {} for project {}",
            format_constraints(constraints),
            request.project_id
        ));
    }

    Ok(rank_usable_executors(eligible, request, estimate)
        .first()
        .map(|(entry, _)| selected_executor(entry)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncCost {
    Known(u64),
    Unknown,
}

fn rank_usable_executors<'a>(
    usable: Vec<&'a ExecutorConnectionState>,
    request: &CellRequest,
    estimate: impl Fn(&CellRequest, &ExecutorConnectionState) -> SyncCost,
) -> Vec<(&'a ExecutorConnectionState, SyncCost)> {
    let mut ranked: Vec<_> = usable
        .into_iter()
        .map(|entry| {
            let cost = estimate(request, entry);
            (entry, cost)
        })
        .collect();
    ranked.sort_by(|(a, a_cost), (b, b_cost)| {
        sync_cost_key(*a_cost)
            .cmp(&sync_cost_key(*b_cost))
            .then_with(|| {
                a.advertisement
                    .current_load
                    .cmp(&b.advertisement.current_load)
            })
            .then_with(|| a.identity.executor_id.cmp(&b.identity.executor_id))
    });
    ranked
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

fn zero_knowledge_reservation(capabilities: &ExecutorCapabilities) -> ResourceReservation {
    const MEMORY_FLOOR_BYTES: u64 = 512 * 1024 * 1024;
    const DISK_FLOOR_BYTES: u64 = 1024 * 1024 * 1024;
    let share = |budget: Option<u64>, floor: u64| budget.map_or(floor, |budget| floor.min(budget));
    ResourceReservation {
        memory_bytes: share(capabilities.memory_budget_bytes, MEMORY_FLOOR_BYTES),
        disk_growth_bytes: share(capabilities.disk_budget_bytes, DISK_FLOOR_BYTES),
        concurrency_units: 1,
        source: ResourceReservationSource::ZeroKnowledgePrior,
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

fn matches_constraints(
    entry: &ExecutorConnectionState,
    constraints: &PlacementConstraints,
) -> bool {
    constraints
        .executor_id
        .as_ref()
        .is_none_or(|value| value == &entry.identity.executor_id)
        && constraints
            .device_id
            .as_ref()
            .is_none_or(|value| value == &entry.identity.device_id)
        && constraints
            .os
            .as_ref()
            .is_none_or(|value| value.eq_ignore_ascii_case(&entry.advertisement.capabilities.os))
        && constraints
            .arch
            .as_ref()
            .is_none_or(|value| value.eq_ignore_ascii_case(&entry.advertisement.capabilities.arch))
        && constraints.required_toolchains.iter().all(|required| {
            entry
                .advertisement
                .capabilities
                .toolchains
                .iter()
                .any(|available| available == required)
        })
}

fn format_constraints(constraints: &PlacementConstraints) -> String {
    serde_json::to_string(constraints)
        .unwrap_or_else(|_| "the requested placement constraints".into())
}

fn serialize_process_batch(
    batch: ResolvedRunBatch,
    default_timeout_ms: u32,
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
            timeout_ms: timeout.unwrap_or(default_timeout_ms),
            command_resource_identity: None,
        });
    }
    Ok(ProcessBatch {
        sequential: batch.originally_sequential,
        stop_on_error: batch.stop_on_error,
        promote_timeouts: batch.run_context.is_some(),
        sandbox_mode,
        items,
        runner_context_id: Some(runner_context_id),
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
        Duration::from_millis(request.deadline_unix_ms.saturating_sub(unix_time_ms()));
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
        CellOccupantKind, GitObjectFormat, LifetimeLeaseFence, LifetimeOccupancyEvidence,
        VerifiedWarmRoot,
    };

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

    #[tokio::test]
    async fn disconnected_lifetime_lease_is_unavailable_not_not_found() {
        let temp = tempfile::tempdir().unwrap();
        let orch = test_orchestrator(temp.path()).await;
        let result = orch
            .fleet
            .operate_lifetime_lease(
                &orch,
                LifetimeLeaseOperation::RefreshCheckout {
                    fence: LifetimeLeaseFence {
                        lease_id: "retained-on-disconnected-executor".into(),
                        owner: cairn_common::executor_protocol::LifetimeLeaseOwner {
                            kind: cairn_common::executor_protocol::LifetimeLeaseOwnerKind::Terminal,
                            owner_id: "job".into(),
                        },
                        incarnation_id: "incarnation".into(),
                        lease_epoch: 1,
                    },
                    base_commit: "new-head".into(),
                },
            )
            .await;

        assert!(matches!(
            result,
            LifetimeLeaseResult::Failed {
                kind: LifetimeLeaseFailureKind::Unavailable,
                ..
            }
        ));
    }

    fn result_identity() -> CheckResultIdentity {
        CheckResultIdentity::new("project", "check", "input")
    }

    fn resolved_process_batch(
        timeouts: Vec<Option<u32>>,
        sequential: bool,
        stop_on_error: bool,
    ) -> ResolvedRunBatch {
        ResolvedRunBatch {
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

    #[test]
    fn process_batch_serialization_preserves_millisecond_timeouts_and_flags() {
        let batch = serialize_process_batch(
            resolved_process_batch(vec![Some(3_000), None], true, false),
            1_800_000,
            &[(
                "CAIRN_WORKTREE_BRANCH".into(),
                "agent/CAIRN-2929-builder-0".into(),
            )],
            "runner-context".into(),
            ProcessSandboxMode::Confined,
        )
        .unwrap();

        assert_eq!(batch.items[0].timeout_ms, 3_000);
        assert_eq!(batch.items[1].timeout_ms, 1_800_000);
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
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            deadline_unix_ms: unix_time_ms() + 5_000,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: None,
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

    #[tokio::test]
    async fn declared_attach_pauses_acquisition_deadline_until_executor_readiness() {
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
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            deadline_unix_ms: unix_time_ms() + 10,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: None,
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        };
        let config = ExecutorConfig {
            project_id: "p".into(),
            project_key: "p".into(),
            acquisition_deadline_seconds: 1,
            default_timeout_seconds: 1,
            setup_commands: Vec::new(),
            populate: Default::default(),
            population_source_root: None,
        };

        let sender = pool
            .wait_for_executor(request.deadline_unix_ms)
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
    async fn coalesced_subscriber_deadline_pauses_during_leader_preparation() {
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
            .await_coalesced(subscriber, now + 5, rx)
            .await
            .expect("declared preparation must pause the subscriber deadline");
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

    #[test]
    fn watchdog_covers_preparation_and_full_process_batch_budget() {
        let mut request = constrained_request(std::env::consts::OS);
        request.deadline_unix_ms = unix_time_ms();
        request.timeout_ms = 500;
        let config = ExecutorConfig {
            project_id: request.project_id.clone(),
            project_key: "CAIRN".into(),
            acquisition_deadline_seconds: 1,
            default_timeout_seconds: 2,
            setup_commands: Vec::new(),
            populate: Default::default(),
            population_source_root: None,
        };
        let batch = ProcessBatch {
            sequential: true,
            stop_on_error: false,
            promote_timeouts: false,
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
                },
            ],
            runner_context_id: None,
        };

        let budget = request_watchdog_duration(&request, Some(&batch), &config, true);
        assert!(budget >= Duration::from_millis(5_300));
        assert!(budget > Duration::from_millis(u64::from(request.timeout_ms)));
    }

    #[tokio::test]
    async fn absent_executor_fails_fast_with_typed_unavailable() {
        let pool = Fleet::default();
        let mut request = CellRequest {
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
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            deadline_unix_ms: unix_time_ms() + 25,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: None,
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        };
        let started = Instant::now();
        let outcome = pool.select_executor(&mut request).await.unwrap_err();
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
                },
                generation: 1,
                sender,
                snapshot: FleetSnapshot::default(),
                last_progress_unix_ms: 1,
                health: ExecutorSubstrateReport::default(),
                executor_build_id: None,
                colocated: id == COLOCATED_EXECUTOR_ID,
            },
        )
    }

    #[test]
    fn snapshot_aggregates_lifetime_count_and_reservations_across_executors() {
        let pool = Fleet::default();
        let mut first = fleet_entry("first", "macos", 0, &[]).1;
        first.snapshot.lifetime_cell_occupancy = Some(LifetimeOccupancyEvidence {
            lease_count: 2,
            reservation: ResourceReservation {
                memory_bytes: 1_000,
                disk_growth_bytes: 2_000,
                concurrency_units: 3,
                source: ResourceReservationSource::Declared,
            },
        });
        let mut second = fleet_entry("second", "linux", 0, &[]).1;
        second.snapshot.lifetime_cell_occupancy = Some(LifetimeOccupancyEvidence {
            lease_count: 1,
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

        let occupancy = pool.snapshot().lifetime_cell_occupancy.unwrap();
        assert_eq!(occupancy.lease_count, 3);
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
        first.host.available_memory_bytes = Some(4_000);
        assert!(pool.handle_executor_message(
            &executor_id,
            1,
            ExecutorMessage::Heartbeat {
                advertisement: advertisement.clone(),
                health: first,
            },
        ));
        assert_eq!(
            pool.executor_health(1)[0].host.available_memory_bytes,
            Some(4_000)
        );

        advertisement.observed_at_unix_ms = 2;
        let mut second = ExecutorSubstrateReport::default();
        second.host.available_memory_bytes = Some(2_000);
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
        assert_eq!(health[0].host.available_memory_bytes, Some(2_000));
        assert_eq!(
            health[0].disk.status,
            cairn_common::executor_protocol::DiskHealthStatus::Full
        );
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
            lifetime_cell_occupancy: Some(LifetimeOccupancyEvidence {
                lease_count: 1,
                reservation: ResourceReservation::default(),
            }),
            ..FleetSnapshot::default()
        };
        let mut first_health = ExecutorSubstrateReport::default();
        first_health.host.available_memory_bytes = Some(4_000);
        assert!(pool.set_executor_snapshot(&executor_id, 1, snapshot.clone(), first_health));

        let mut second_health = ExecutorSubstrateReport::default();
        second_health.host.available_memory_bytes = Some(2_000);
        assert!(!pool.set_executor_snapshot(&executor_id, 1, snapshot, second_health));
        assert_eq!(
            pool.executor_health(1)[0].host.available_memory_bytes,
            Some(2_000)
        );
    }

    #[test]
    fn empty_first_snapshot_reconciles_stale_persisted_route_without_public_change() {
        let temp = tempfile::tempdir().unwrap();
        let route_path = temp.path().join("lifetime-routes.json");
        let pool = Fleet::with_lifetime_route_path(route_path.clone());
        let executor_id = "snapshot".to_string();
        let declaration = fleet_lifetime_declaration("stale-lease");
        pool.update_lifetime_routes(|known| {
            known.insert(
                (executor_id.clone(), declaration.lease_id.clone()),
                LifetimeRoute {
                    declaration,
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
        assert!(pool.lifetime_routes.lock().unwrap().is_empty());

        drop(pool);
        assert!(Fleet::with_lifetime_route_path(route_path)
            .lifetime_routes
            .lock()
            .unwrap()
            .is_empty());
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

    fn fleet_lifetime_declaration(lease_id: &str) -> LifetimeLeaseDeclaration {
        LifetimeLeaseDeclaration {
            lease_id: lease_id.into(),
            owner: cairn_common::executor_protocol::LifetimeLeaseOwner {
                kind: cairn_common::executor_protocol::LifetimeLeaseOwnerKind::DevInstance,
                owner_id: "launcher".into(),
            },
            owner_ref: None,
            name: "dev:feature".into(),
            purpose: "serve committed branch".into(),
            repository: RepositoryLocator::ColocatedPath {
                project_id: "p".into(),
                repository_id: "repo".into(),
                absolute_path: "/repo".into(),
            },
            initial_base_commit: "base".into(),
            resource_reservation: ResourceReservation::default(),
            owner_death_policy: cairn_common::executor_protocol::LifetimeOwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
        }
    }

    #[test]
    fn lifetime_route_authority_recovers_after_transient_persistence_failure() {
        let temp = tempfile::tempdir().unwrap();
        let blocked_parent = temp.path().join("blocked");
        std::fs::write(&blocked_parent, "not a directory").unwrap();
        let route_path = blocked_parent.join("lifetime-routes.json");
        let pool = Fleet::with_lifetime_route_path(route_path.clone());
        let route = LifetimeRoute {
            declaration: fleet_lifetime_declaration("lease"),
            executor_id: "first".into(),
            pending: true,
        };

        let initial_error = pool.reserve_pending_lifetime_route(route).unwrap_err();
        assert!(initial_error.contains("lifetime route authority"));
        let mut declaration = fleet_lifetime_declaration("new-lease");
        assert!(matches!(
            pool.resolve_lifetime_acquire_route(&mut declaration),
            Err(LifetimeLeaseResult::Failed {
                kind: LifetimeLeaseFailureKind::Persistence,
                ..
            })
        ));

        std::fs::remove_file(&blocked_parent).unwrap();
        std::fs::create_dir(&blocked_parent).unwrap();

        assert!(pool
            .resolve_lifetime_acquire_route(&mut declaration)
            .unwrap()
            .is_none());
        assert!(route_path.is_file());
        assert!(pool.lifetime_route_store_error.lock().unwrap().is_none());
    }

    #[test]
    fn fleet_lifetime_retry_routes_to_original_executor() {
        let pool = Fleet::default();
        let first = fleet_entry("first", "linux", 10, &[]);
        let second = fleet_entry("second", "linux", 0, &[]);
        pool.connections.lock().unwrap().extend([first, second]);
        let mut declaration = fleet_lifetime_declaration("lease");
        let mut persisted_declaration = declaration.clone();
        persisted_declaration.repository = RepositoryLocator::ManagedObjects {
            project_id: "p".into(),
            repository_id: "repo".into(),
            object_format: GitObjectFormat::Sha1,
        };
        pool.lifetime_routes.lock().unwrap().insert(
            ("first".into(), "lease".into()),
            LifetimeRoute {
                declaration: persisted_declaration.clone(),
                executor_id: "first".into(),
                pending: false,
            },
        );
        let selected = pool
            .resolve_lifetime_acquire_route(&mut declaration)
            .unwrap()
            .unwrap();
        assert_eq!(selected.executor_id, "first");
        assert_eq!(declaration.repository, persisted_declaration.repository);
    }

    #[test]
    fn lost_acquire_response_keeps_pending_route_on_original_executor() {
        let pool = Fleet::default();
        pool.connections.lock().unwrap().extend([
            fleet_entry("first", "linux", 10, &[]),
            fleet_entry("second", "linux", 0, &[]),
        ]);
        let mut declaration = fleet_lifetime_declaration("lease");
        pool.reserve_pending_lifetime_route(LifetimeRoute {
            declaration: declaration.clone(),
            executor_id: "first".into(),
            pending: true,
        })
        .unwrap();

        // Dispatch was accepted, but neither a response nor an occupant snapshot
        // arrived before the owning connection disappeared.
        pool.connections.lock().unwrap().remove("first");
        assert!(matches!(
            pool.resolve_lifetime_acquire_route(&mut declaration),
            Err(LifetimeLeaseResult::Failed {
                kind: LifetimeLeaseFailureKind::Admission,
                ..
            })
        ));
        assert_eq!(pool.lifetime_routes.lock().unwrap().len(), 1);
    }

    #[test]
    fn runner_restart_preserves_ambiguous_pending_lifetime_route() {
        let temp = tempfile::tempdir().unwrap();
        let route_path = temp.path().join("lifetime-routes.json");
        let first = Fleet::with_lifetime_route_path(route_path.clone());
        let declaration = fleet_lifetime_declaration("lease-restart");
        first
            .reserve_pending_lifetime_route(LifetimeRoute {
                declaration: declaration.clone(),
                executor_id: "first".into(),
                pending: true,
            })
            .unwrap();
        drop(first);

        let replacement = Fleet::with_lifetime_route_path(route_path);
        replacement
            .connections
            .lock()
            .unwrap()
            .insert("second".into(), fleet_entry("second", "linux", 0, &[]).1);
        let mut retry = declaration;
        assert!(matches!(
            replacement.resolve_lifetime_acquire_route(&mut retry),
            Err(LifetimeLeaseResult::Failed {
                kind: LifetimeLeaseFailureKind::Admission,
                ..
            })
        ));
        let routes = replacement.lifetime_routes.lock().unwrap();
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
        let mut declaration = fleet_lifetime_declaration("lease");
        pool.lifetime_routes.lock().unwrap().insert(
            ("first".into(), "lease".into()),
            LifetimeRoute {
                declaration: declaration.clone(),
                executor_id: "first".into(),
                pending: false,
            },
        );
        assert!(matches!(
            pool.resolve_lifetime_acquire_route(&mut declaration),
            Err(LifetimeLeaseResult::Failed {
                kind: LifetimeLeaseFailureKind::Admission,
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
        let mut declaration = fleet_lifetime_declaration("lease");
        let mut routes = pool.lifetime_routes.lock().unwrap();
        for executor_id in ["first", "second"] {
            routes.insert(
                (executor_id.into(), "lease".into()),
                LifetimeRoute {
                    declaration: declaration.clone(),
                    executor_id: executor_id.into(),
                    pending: false,
                },
            );
        }
        drop(routes);
        assert!(matches!(
            pool.resolve_lifetime_acquire_route(&mut declaration),
            Err(LifetimeLeaseResult::Failed {
                kind: LifetimeLeaseFailureKind::ConflictingDeclaration,
                ..
            })
        ));
    }

    fn constrained_request(os: &str) -> CellRequest {
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
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            deadline_unix_ms: unix_time_ms() + 1_000,
            timeout_ms: 1_000,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: Some(PlacementConstraints {
                os: Some(os.into()),
                ..PlacementConstraints::default()
            }),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
        }
    }

    #[test]
    fn hard_constraints_route_only_to_matching_executor() {
        let connections = HashMap::from([
            fleet_entry("linux", "linux", 0, &[]),
            fleet_entry("windows", "windows", 0, &[]),
        ]);
        let selected = choose_executor(&connections, &constrained_request("windows"))
            .unwrap()
            .unwrap();
        assert_eq!(selected.executor_id, "windows");
    }

    #[test]
    fn transfer_estimation_does_not_hold_executor_connection_lock() {
        let pool = Fleet::default();
        let (id, entry) = fleet_entry("remote", "linux", 0, &[]);
        pool.connections.lock().unwrap().insert(id, entry);
        let request = constrained_request("linux");
        let selecting_pool = pool.clone();
        let (estimation_started_tx, estimation_started_rx) = std::sync::mpsc::channel();
        let (release_estimation_tx, release_estimation_rx) = std::sync::mpsc::channel();
        let selector = std::thread::spawn(move || {
            selecting_pool
                .select_executor_once_with(&request, |_, _| {
                    estimation_started_tx.send(()).unwrap();
                    release_estimation_rx.recv().unwrap();
                    SyncCost::Unknown
                })
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
        let request = constrained_request("linux");
        let selecting_pool = pool.clone();
        let (estimation_started_tx, estimation_started_rx) = std::sync::mpsc::channel();
        let (release_estimation_tx, release_estimation_rx) = std::sync::mpsc::channel();
        let selector = std::thread::spawn(move || {
            selecting_pool
                .select_executor_once_with(&request, |_, _| {
                    estimation_started_tx.send(()).unwrap();
                    release_estimation_rx.recv().unwrap();
                    SyncCost::Unknown
                })
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
        let selected = pool
            .select_executor_once_with(&constrained_request("linux"), |_, _| SyncCost::Unknown)
            .unwrap()
            .unwrap();
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
            choose_executor(&connections, &constrained_request("linux"))
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
        let request = constrained_request("linux");
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
        let usable: Vec<_> = connections.values().collect();
        let ranked = rank_usable_executors(usable, &constrained_request("linux"), |_, entry| {
            if entry.identity.executor_id == "known" {
                SyncCost::Known(10)
            } else {
                SyncCost::Unknown
            }
        });
        assert_eq!(ranked[0].0.identity.executor_id, "known");
    }

    #[test]
    fn unknown_cost_does_not_exclude_the_only_usable_executor() {
        let connections = HashMap::from([fleet_entry("only", "linux", 0, &[])]);
        let usable: Vec<_> = connections.values().collect();
        let ranked = rank_usable_executors(usable, &constrained_request("linux"), |_, _| {
            SyncCost::Unknown
        });
        assert_eq!(ranked[0].0.identity.executor_id, "only");
    }

    #[test]
    fn population_policy_never_routes_runner_local_source_to_remote_executor() {
        let connections = HashMap::from([
            fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 1, &[]),
            fleet_entry("remote", "linux", 0, &[]),
        ]);
        let mut request = constrained_request("linux");
        request.constraints.as_mut().unwrap().executor_id = Some("remote".into());
        let config = ExecutorConfig {
            project_id: "p".into(),
            project_key: "P".into(),
            acquisition_deadline_seconds: 5,
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
            .contains("colocated executor"));
        assert_eq!(
            choose_executor(&connections, &request)
                .unwrap()
                .unwrap()
                .executor_id,
            "remote"
        );
    }

    #[test]
    fn explicit_constraints_can_select_colocated_executor() {
        let connections = HashMap::from([fleet_entry(COLOCATED_EXECUTOR_ID, "linux", 0, &[])]);
        let mut request = constrained_request("linux");
        request.constraints.as_mut().unwrap().executor_id = Some(COLOCATED_EXECUTOR_ID.into());
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
        assert!(
            choose_executor(&connections, &constrained_request("windows"))
                .unwrap_err()
                .contains("no live enrolled executor")
        );
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
                    occupant_kind: CellOccupantKind::Command,
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
    async fn capacity_busy_pauses_subscriber_deadline_until_leader_completes() {
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
            .await_coalesced(identity.clone(), unix_time_ms() + 5, rx)
            .await
            .expect("fresh capacity contention must pause the subscriber deadline");
        assert!(matches!(outcome.outcome, CellOutcome::Cancelled { .. }));
        assert!(matches!(
            executor_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn slot_adoption_pauses_subscriber_deadline() {
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
            .await_coalesced(identity, unix_time_ms() + 5, rx)
            .await
            .expect("fresh slot adoption must pause the subscriber deadline");
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
        second.published();
        assert!(matches!(
            coordination.acquire().await,
            PublicationRole::Published
        ));
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
        pool.subscribe_lifetime_process_events(move |event| {
            captured.lock().unwrap().push(event);
        });
        let mut declaration = fleet_lifetime_declaration("terminal:job:watch");
        declaration.owner.kind = cairn_common::executor_protocol::LifetimeLeaseOwnerKind::Terminal;
        declaration.owner.owner_id = "job".into();
        declaration.name = "watch".into();
        let status = cairn_common::executor_protocol::LifetimeProcessStatus::Exited {
            finished_at_unix_ms: 42,
            exit_code: None,
            restartable: true,
            executor_lost: true,
        };
        let cell = PersistentCellState {
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
            lease_epoch: 7,
            last_sealed_commit: Some("base".into()),
            last_used_unix_ms: 42,
            last_affinity_key: None,
            preparation_fingerprint: None,
            occupant: Some(CellOccupant::Lifetime(
                cairn_common::executor_protocol::LifetimeLeaseState {
                    declaration,
                    incarnation_id: "incarnation".into(),
                    current_base_commit: "base".into(),
                    phase: cairn_common::executor_protocol::LifetimeLeasePhase::AwaitingReclaim,
                    last_heartbeat_unix_ms: 1,
                    reclaim_deadline_unix_ms: 100,
                    state_revision: 2,
                    command_settled: true,
                    processes: std::collections::BTreeMap::from([(
                        "main".into(),
                        cairn_common::executor_protocol::LifetimeProcessState {
                            generation: 3,
                            spec: None,
                            status: status.clone(),
                        },
                    )]),
                    events: Vec::new(),
                },
            )),
        };

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
            &[LifetimeProcessEvent {
                lease_id: "terminal:job:watch".into(),
                incarnation_id: "incarnation".into(),
                lease_epoch: 7,
                process_key: "main".into(),
                process_generation: 3,
                event: LifetimeProcessEventKind::State { status },
            }]
        );
    }
}
