//! The supervisor: one per runner, holding one cell per (project, owner,
//! service).
//!
//! A cell is a service's lifecycle. It owns at most one serving incarnation
//! (a process, its standing run, its pending calls) plus any being retired,
//! and it moves through `stopped | starting | running | backoff | failed |
//! update_failed`. Every transition writes a lifecycle row to the service log.
//!
//! Two seams keep the mechanics testable and the source replaceable:
//! [`ServiceHost`] places and talks to a process (production: the service
//! lease, `host.rs`), and [`SourceProvider`] opens an owner's packages
//! (production: the base-branch tip for a project, the thread's store for a
//! thread). [`apply_source`] is the one function that turns "the source may
//! have changed" into starts, replacements, and stops, whether the change was
//! a base advance or a thread's write. It also brings each service's
//! `schedule` rows in step with its declaration and keeps one schedule task
//! per scheduled cell, which calls each due method as the owner
//! ([`super::schedule`]).
//!
//! An `on_demand` incarnation with no call in flight for its declaration's
//! `idle_stop_after` is drained and stopped, not stickily: the next call or
//! scheduled tick boots it, so a scheduled method holds no process between
//! runs.

use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch, Notify, Semaphore};

use super::authority::{self, ServiceRun, ServiceRunRole};
use super::declaration::{
    Callback, NativeLaunch, ServiceDeclaration, ServicePackage, ServiceSource, StartPolicy,
    Transport,
};
use super::log::{LogEntry, LogKind};
use super::protocol::{
    CallAuthority, CallerFrame, CallerKind, Line, LineAssembler, RunnerFrame, ServiceFrame,
    ViolationWindow, MAX_LINE_BYTES, PROTOCOL_VERSION,
};
use super::rows;
use super::schedule::{self, ScheduleRow};
use super::ServiceOwner;
use crate::orchestrator::Orchestrator;

/// How long a new incarnation has to answer `hello` with `ready`.
pub(crate) const READY_TIMEOUT: Duration = Duration::from_secs(20);
/// Added to a retiring incarnation's call timeout before it is stopped.
const DRAIN_GRACE: Duration = Duration::from_secs(5);
const CRASH_WINDOW: Duration = Duration::from_secs(600);
const CRASH_LIMIT: usize = 5;
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// The largest params a call may carry.
pub(crate) const MAX_PARAMS_BYTES: usize = 1024 * 1024;
const RECENT_CALLS: usize = 10;
/// How long a call's bearer outlives the call's own deadline, should its
/// revocation at the call's end somehow not run.
const BEARER_GRACE: Duration = Duration::from_secs(30);
/// The longest error text a service's failure carries back to a caller.
const MAX_ERROR_BYTES: usize = 4 * 1024;
/// The longest a schedule task sleeps before reading the wall clock again, so
/// a machine that slept through an occurrence runs it soon after waking.
const SCHEDULE_RECHECK: Duration = Duration::from_secs(60);
/// How many times a scheduled occurrence the placement substrate refused is
/// tried again, each at the schedule's recheck, before it is recorded as
/// failed: a substrate that stays away that long is news to the owner.
pub(crate) const PLACEMENT_RETRIES: u32 = 3;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ServiceState {
    #[default]
    Stopped,
    Starting,
    Running,
    Backoff,
    Failed,
    UpdateFailed,
}

impl ServiceState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ServiceState::Stopped => "stopped",
            ServiceState::Starting => "starting",
            ServiceState::Running => "running",
            ServiceState::Backoff => "backoff",
            ServiceState::Failed => "failed",
            ServiceState::UpdateFailed => "update_failed",
        }
    }
}

/// Why a call or an action did not happen. `code` is the stable word a page
/// or an agent branches on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CallError {
    pub code: String,
    pub message: String,
}

impl CallError {
    pub(crate) fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

/// The code of a call refused because the placement substrate would not grant
/// the service a residency, or could not route its start to the executor
/// holding one. The refusal says nothing about the service, so a scheduled
/// occurrence it refuses is retried rather than counted as the service's
/// failure.
pub(crate) const PLACEMENT_UNAVAILABLE: &str = "placement_unavailable";

/// Why a host did not start a service process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LaunchError {
    /// The placement substrate refused without judging the service: it would
    /// not grant the service a residency, or could not route the start to the
    /// executor holding it. A fact about the substrate, not the service.
    Unplaced(String),
    /// Anything else: the launch itself failed.
    Failed(String),
}

impl From<String> for LaunchError {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
}

impl LaunchError {
    /// The call this refusal ends, classified the same at every step of a
    /// start that places the service.
    fn into_call_error(self) -> CallError {
        let code = match &self {
            Self::Unplaced(_) => PLACEMENT_UNAVAILABLE,
            Self::Failed(_) => "start_failed",
        };
        CallError::new(code, format!("could not place the service: {self}"))
    }
}

impl std::fmt::Display for LaunchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unplaced(error) | Self::Failed(error) => f.write_str(error),
        }
    }
}

/// What a host is asked to start.
#[derive(Debug, Clone)]
pub(crate) struct LaunchSpec {
    pub project_id: String,
    pub project_key: String,
    pub owner: ServiceOwner,
    pub service: String,
    pub generation: u64,
    pub declaration: ServiceDeclaration,
    pub files: Vec<(String, Vec<u8>)>,
    pub env: Vec<(String, String)>,
    /// A native service's executable on this machine; `None` for bun.
    pub native: Option<NativeLaunch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HostEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    /// `executor_lost`: the machine went away, which says nothing about the
    /// service.
    Exited {
        code: Option<i32>,
        detail: String,
        executor_lost: bool,
    },
}

/// What a probe one-shot printed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProbeOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
}

#[async_trait]
pub(crate) trait HostInput: Send + Sync {
    async fn write(&self, bytes: &[u8]) -> Result<(), String>;
    async fn kill(&self);
}

pub(crate) struct HostProcess {
    pub input: Arc<dyn HostInput>,
    pub events: mpsc::UnboundedReceiver<HostEvent>,
    pub executor: String,
}

/// Places service processes and carries their pipes.
#[async_trait]
pub(crate) trait ServiceHost: Send + Sync {
    /// Where the service `spec` names will call back from, decided by placing
    /// it, so its bearers can be bound there before they are minted. A host
    /// that places nothing elsewhere answers this runner's loopback.
    async fn callback(
        &self,
        orch: &Orchestrator,
        _spec: &LaunchSpec,
    ) -> Result<crate::fleet::residency::ResidentCallback, LaunchError> {
        Ok(crate::fleet::residency::ResidentCallback::Direct {
            url: crate::fleet::residency::runner_callback_url(orch),
        })
    }
    async fn launch(
        &self,
        orch: &Orchestrator,
        spec: &LaunchSpec,
    ) -> Result<HostProcess, LaunchError>;
    /// Run `program args` once where `spec` is placed, as a native service's
    /// status probe.
    async fn probe(
        &self,
        _orch: &Orchestrator,
        _spec: &LaunchSpec,
        _program: &str,
        _args: Vec<String>,
        _timeout: Duration,
    ) -> Result<ProbeOutput, String> {
        Err("this host runs no probes".to_string())
    }
    /// The machines attached now, as machine services see them.
    fn machines(&self, orch: &Orchestrator) -> Vec<machine::MachineView> {
        machine::attached_machines(orch)
    }
    /// The service has stopped; give back whatever its placement holds.
    async fn release(
        &self,
        orch: &Orchestrator,
        project_id: &str,
        owner: &ServiceOwner,
        service: &str,
    );
}

/// Opens one owner's service packages.
#[async_trait]
pub(crate) trait SourceProvider: Send + Sync {
    async fn open(
        &self,
        orch: &Orchestrator,
        project_id: &str,
        owner: &ServiceOwner,
    ) -> Result<Arc<dyn ServiceSource>, String>;

    /// Every installed pack's service source, by pack id.
    fn packs(&self, orch: &Orchestrator) -> Vec<(String, Arc<dyn ServiceSource>)> {
        super::pack_source::PackServiceSource::installed(&orch.config_dir)
            .into_iter()
            .map(|source| {
                let pack = source.pack().to_string();
                (pack, Arc::new(source) as Arc<dyn ServiceSource>)
            })
            .collect()
    }
}

/// The production sources: a project's default-branch tip, and a thread's
/// own store.
pub(crate) struct OwnerSources;

#[async_trait]
impl SourceProvider for OwnerSources {
    async fn open(
        &self,
        orch: &Orchestrator,
        project_id: &str,
        owner: &ServiceOwner,
    ) -> Result<Arc<dyn ServiceSource>, String> {
        match owner {
            ServiceOwner::Project => {
                let source = super::declaration::BaseTipSource::resolve(orch, project_id).await?;
                Ok(Arc::new(source))
            }
            ServiceOwner::Thread { .. } => {
                let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
                    .await
                    .map_err(|error| error.to_string())?;
                let key = project_key(orch, project_id).await?;
                let source = super::thread_store::ThreadSource::load(&db, &key, owner).await?;
                Ok(Arc::new(source))
            }
            ServiceOwner::Pack { pack, machine, .. } => {
                machine::open_source(orch, self.packs(orch), pack, machine)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Booting,
    Ready,
    Exited,
}

type Reply = oneshot::Sender<Result<Value, CallError>>;

/// One running process of a service.
struct Incarnation {
    generation: u64,
    /// The standing run, for a service with a Cairn callback; a native
    /// resident has none.
    run_id: Option<String>,
    source_hash: String,
    declaration: ServiceDeclaration,
    /// What its stdio carries, which decides how its stdout is read.
    transport: Transport,
    /// A native service's executable, which its probe runs too.
    launch: Option<NativeLaunch>,
    /// Where an `mcp-stdio` resident's stdout goes: the one gateway
    /// connection attached to it, once one is.
    stdout_sink: StdMutex<Option<mpsc::UnboundedSender<Vec<u8>>>>,
    /// Whether a gateway connection has taken this process's stdio. An MCP
    /// session is one per process, so a second connection gets a fresh one.
    attached: AtomicBool,
    executor: String,
    /// Where this process calls back, as the audience its bearers are bound
    /// to: each call's delegated bearer is minted for it too.
    callback_audience: crate::security::LeaseAudience,
    input: Arc<dyn HostInput>,
    pending: StdMutex<HashMap<String, Reply>>,
    phase: watch::Sender<Phase>,
    drained: AtomicBool,
    drained_notify: Notify,
    gate: Arc<Semaphore>,
    waiting: AtomicUsize,
    /// Set when the runner means for this process to end, so its exit is
    /// not a crash: boot failure, drain, stop, replacement. A violation kill
    /// leaves it unset on purpose, so a misbehaving service backs off and
    /// fails like one that crashed.
    retiring: AtomicBool,
    started_at: Instant,
}

impl Incarnation {
    async fn send(&self, frame: &RunnerFrame) -> Result<(), String> {
        self.input.write(&frame.encode()).await
    }

    fn phase(&self) -> Phase {
        *self.phase.borrow()
    }

    fn serving(&self) -> bool {
        self.phase() == Phase::Ready && !self.retiring.load(Ordering::Acquire)
    }

    fn pending_len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    fn fail_pending(&self, error: &CallError) {
        let pending: Vec<Reply> = self
            .pending
            .lock()
            .unwrap()
            .drain()
            .map(|(_, r)| r)
            .collect();
        for reply in pending {
            let _ = reply.send(Err(error.clone()));
        }
    }
}

/// The last calls, for the service's resource read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CallRecord {
    pub at_ms: i64,
    pub method: String,
    pub caller_kind: String,
    pub acting_for: String,
    pub outcome: String,
    pub millis: u64,
}

#[derive(Default)]
struct CellState {
    status: ServiceState,
    sticky_stop: bool,
    generation: u64,
    current: Option<Arc<Incarnation>>,
    crashes: VecDeque<Instant>,
    restarts: u64,
    backoff_until: Option<Instant>,
    last_error: Option<String>,
    last_source_hash: Option<String>,
    recent: VecDeque<CallRecord>,
}

/// Whether a cell's schedule task runs. `epoch` is bumped, under the same
/// lock, every time the cell's schedule rows may have changed, so a task that
/// found no rows can exit without missing rows written meanwhile.
#[derive(Default)]
struct ScheduleTask {
    running: bool,
    epoch: u64,
}

struct Cell {
    project_id: String,
    project_key: String,
    owner: ServiceOwner,
    service: String,
    /// Serializes start, replace, and stop.
    ops: tokio::sync::Mutex<()>,
    state: StdMutex<CellState>,
    /// Calls admitted and not yet ended, whichever incarnation serves them.
    /// The idle stop reads it under `state`'s lock, and a call counts itself
    /// before it looks for a serving incarnation, so no call can find one the
    /// idle stop is taking.
    in_flight: AtomicUsize,
    /// When a call last began or ended, or an incarnation was installed.
    last_activity: StdMutex<Instant>,
    schedule_task: StdMutex<ScheduleTask>,
    schedule_changed: Notify,
    /// Whether a deferred occurrence has been logged this runner, so a
    /// project service whose schedule waits on the startup catch-up writes
    /// one row rather than one per recheck.
    deferral_noted: AtomicBool,
    /// Scheduled occurrences the placement substrate refused, by entry key:
    /// each is left due and tried again rather than recorded as failed.
    placement_retries: StdMutex<BTreeMap<String, PlacementRetry>>,
    /// Sends a call's first attempt to this incarnation, standing in for the
    /// race where a replacement lands between choosing one and sending.
    #[cfg(test)]
    force_first_pick: StdMutex<Option<Arc<Incarnation>>>,
    /// A machine service's per-machine state: its probe, its facade, and why
    /// it cannot start. `None` for every other owner.
    machine: Option<machine::MachineCell>,
}

impl Cell {
    /// The service's home: its resource, and the home its runs act under.
    fn home(&self) -> String {
        self.owner.home(&self.project_key, &self.service)
    }

    fn serving(&self) -> Option<Arc<Incarnation>> {
        self.state
            .lock()
            .unwrap()
            .current
            .clone()
            .filter(|incarnation| incarnation.serving())
    }
}

/// A scheduled occurrence the placement substrate refused and left due, to be
/// tried again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementRetry {
    /// The occurrence first refused, epoch seconds.
    pub due: i64,
    /// Refusals so far, the first included.
    pub refusals: u32,
    /// The latest refusal.
    pub refusal: String,
}

/// A service's status, for `/status` and the index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceStatus {
    pub state: ServiceState,
    pub sticky_stop: bool,
    pub generation: Option<u64>,
    pub run_id: Option<String>,
    pub executor: Option<String>,
    pub source_hash: Option<String>,
    pub uptime_secs: Option<u64>,
    pub restarts: u64,
    pub last_error: Option<String>,
    pub pending_calls: usize,
    pub backoff_secs: Option<u64>,
    pub recent_calls: Vec<CallRecord>,
    /// The serving incarnation's idle stop; `None` for a `launch` service or
    /// when nothing serves.
    pub idle_stop_after: Option<Duration>,
    /// Scheduled occurrences being retried after a placement refusal, by
    /// entry key. Held by this runner only: a restart finds the occurrence
    /// still due and runs it afresh.
    pub placement_retries: BTreeMap<String, PlacementRetry>,
}

/// A cell's key: (project id, [`ServiceOwner::key`], service).
type CellKey = (String, String, String);

fn cell_key(project_id: &str, owner: &ServiceOwner, service: &str) -> CellKey {
    (
        project_id.to_string(),
        owner.key().to_string(),
        service.to_string(),
    )
}

pub(crate) struct ServiceSupervisor {
    host: RwLock<Arc<dyn ServiceHost>>,
    sources: RwLock<Arc<dyn SourceProvider>>,
    cells: StdMutex<HashMap<CellKey, Arc<Cell>>>,
    /// Set when the project pass of runner start begins, after the startup
    /// catch-up has brought every base tip current. Until then a project
    /// service's scheduled occurrence is deferred, not run from a stale tip.
    projects_current: AtomicBool,
    /// Serializes [`machine::apply_machine_services`] passes.
    machine_apply: tokio::sync::Mutex<()>,
}

impl Default for ServiceSupervisor {
    fn default() -> Self {
        Self {
            host: RwLock::new(Arc::new(super::host::LeaseHost::default())),
            sources: RwLock::new(Arc::new(OwnerSources)),
            cells: StdMutex::default(),
            projects_current: AtomicBool::new(false),
            machine_apply: tokio::sync::Mutex::new(()),
        }
    }
}

impl ServiceSupervisor {
    #[cfg(test)]
    pub(crate) fn install_for_test(
        &self,
        host: Arc<dyn ServiceHost>,
        sources: Arc<dyn SourceProvider>,
    ) {
        *self.host.write().unwrap() = host;
        *self.sources.write().unwrap() = sources;
    }

    fn host(&self) -> Arc<dyn ServiceHost> {
        self.host.read().unwrap().clone()
    }

    fn sources(&self) -> Arc<dyn SourceProvider> {
        self.sources.read().unwrap().clone()
    }

    fn cell(
        &self,
        project_id: &str,
        project_key: &str,
        owner: &ServiceOwner,
        service: &str,
    ) -> Arc<Cell> {
        self.cells
            .lock()
            .unwrap()
            .entry(cell_key(project_id, owner, service))
            .or_insert_with(|| {
                Arc::new(Cell {
                    project_id: project_id.to_string(),
                    project_key: project_key.to_string(),
                    owner: owner.clone(),
                    service: service.to_string(),
                    ops: tokio::sync::Mutex::new(()),
                    state: StdMutex::default(),
                    in_flight: AtomicUsize::new(0),
                    last_activity: StdMutex::new(Instant::now()),
                    schedule_task: StdMutex::default(),
                    schedule_changed: Notify::new(),
                    deferral_noted: AtomicBool::new(false),
                    placement_retries: StdMutex::default(),
                    #[cfg(test)]
                    force_first_pick: StdMutex::default(),
                    machine: machine::MachineCell::for_owner(owner, service),
                })
            })
            .clone()
    }

    fn existing(&self, project_id: &str, owner: &ServiceOwner, service: &str) -> Option<Arc<Cell>> {
        self.cells
            .lock()
            .unwrap()
            .get(&cell_key(project_id, owner, service))
            .cloned()
    }

    fn cells_for(&self, project_id: &str, owner: &ServiceOwner) -> Vec<Arc<Cell>> {
        self.cells
            .lock()
            .unwrap()
            .values()
            .filter(|cell| cell.project_id == project_id && cell.owner.key() == owner.key())
            .cloned()
            .collect()
    }

    /// A service's status, or `None` when this runner has never touched it.
    pub(crate) fn status(
        &self,
        project_id: &str,
        owner: &ServiceOwner,
        service: &str,
    ) -> Option<ServiceStatus> {
        let cell = self.existing(project_id, owner, service)?;
        let placement_retries = cell.placement_retries.lock().unwrap().clone();
        let state = cell.state.lock().unwrap();
        let current = state.current.as_ref();
        Some(ServiceStatus {
            state: state.status,
            sticky_stop: state.sticky_stop,
            generation: current.map(|i| i.generation),
            run_id: current.and_then(|i| i.run_id.clone()),
            executor: current.map(|i| i.executor.clone()),
            source_hash: current
                .map(|i| i.source_hash.clone())
                .or_else(|| state.last_source_hash.clone()),
            uptime_secs: current.map(|i| i.started_at.elapsed().as_secs()),
            restarts: state.restarts,
            last_error: state.last_error.clone(),
            pending_calls: current.map(|i| i.pending_len()).unwrap_or(0),
            backoff_secs: state
                .backoff_until
                .filter(|_| state.status == ServiceState::Backoff)
                .map(|until| until.saturating_duration_since(Instant::now()).as_secs()),
            recent_calls: state.recent.iter().cloned().collect(),
            idle_stop_after: current.and_then(|i| i.declaration.idle_stop_after),
            placement_retries,
        })
    }
}

fn supervisor(orch: &Orchestrator) -> &ServiceSupervisor {
    &orch.project_services.supervisor
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

fn short(hash: &str) -> &str {
    &hash[..hash.len().min(12)]
}

async fn log(
    orch: &Orchestrator,
    cell: &Cell,
    generation: u64,
    kind: LogKind,
    text: &str,
    detail: Value,
) {
    let Ok(db) = cell_db(orch, &cell.project_id).await else {
        return;
    };
    let policy = crate::security::redaction_policy(&orch.config_dir);
    let entry = LogEntry {
        project_id: &cell.project_id,
        owner_thread: cell.owner.key(),
        service: &cell.service,
        generation,
        kind,
        text,
        detail,
    };
    if let Err(error) = super::log::record(&db, &policy, &entry).await {
        tracing::warn!(%error, service = %cell.service, "could not write a service log row");
    }
}

/// The database a cell's rows live in: its project's, or for a machine
/// service, which belongs to no project, this runner's own.
async fn cell_db(
    orch: &Orchestrator,
    project_id: &str,
) -> Result<Arc<crate::storage::LocalDb>, String> {
    if project_id.is_empty() {
        return Ok(orch.db.local.clone());
    }
    crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())
}

async fn lifecycle(orch: &Orchestrator, cell: &Cell, generation: u64, text: &str, actor: &str) {
    log(
        orch,
        cell,
        generation,
        LogKind::Lifecycle,
        text,
        json!({ "actor": actor }),
    )
    .await;
}

/// Mint a bearer for `run_id` acting as `home`, presentable at the callback.
///
/// Returned as the lease, never its value: the value is taken through
/// [`crate::mcp::run_bearer::service_credential`] only where it is written to
/// the service.
fn mint_bearer(
    orch: &Orchestrator,
    run_id: &str,
    home: &str,
    lifetime: Option<Duration>,
    audience: &crate::security::LeaseAudience,
) -> Result<crate::mcp::run_bearer::RunBearer, CallError> {
    let minted = match lifetime {
        Some(lifetime) => orch
            .run_bearers
            .mint_expiring(run_id, home, audience.clone(), lifetime),
        None => orch.run_bearers.mint(run_id, home, audience.clone()),
    };
    minted.map_err(|error| CallError::new("start_failed", error.to_string()))
}

/// Revoke a run's bearers, forget it, and finalize its row: the one way a
/// service run ends.
async fn end_run(orch: &Orchestrator, project_id: &str, run_id: &str, reason: &str) {
    orch.run_bearers.revoke_run(run_id, reason);
    // A batch the call started ends with it, rather than running to its own
    // item timeout after its run's authority is gone (cairn/5564). Stopped
    // after the bearer is revoked, so nothing it does from here reaches Cairn,
    // and before the row is final, so it settles as the call's own.
    let stopped = orch.project_services.runs.stop_batches(orch, run_id);
    if stopped > 0 {
        tracing::info!(
            run_id,
            stopped,
            reason,
            "stopped a service call's in-flight batches"
        );
    }
    let finalized =
        match crate::execution::routing::owning_db_for_project(&orch.db, project_id).await {
            Ok(db) => rows::finalize_run(&db, run_id, reason).await,
            Err(error) => Err(error.to_string()),
        };
    match finalized {
        // Forgotten only once its row is final, so nothing between the two
        // can reach a handler as an ordinary run.
        Ok(()) => {
            orch.project_services.runs.remove(run_id);
        }
        Err(error) => {
            tracing::warn!(%error, run_id, "could not finalize a service run; it stays refused");
            if let Some(mut run) = orch.project_services.runs.get(run_id) {
                run.role = ServiceRunRole::Delegated {
                    caller_kind: super::protocol::CallerKind::Agent,
                    acting_for: run.home(),
                    scope: authority::DelegatedScope::Empty {
                        reason: format!("this run has ended ({reason})"),
                    },
                    // An ended run writes no thread's database either.
                };
                orch.project_services.runs.insert(run_id, run);
            }
        }
    }
}

async fn default_branch(orch: &Orchestrator, project_id: &str) -> String {
    match crate::execution::routing::owning_db_for_project(&orch.db, project_id).await {
        Ok(db) => crate::orchestrator::base_advance::load_project_default_branch(&db, project_id)
            .await
            .ok()
            .flatten()
            .filter(|branch| !branch.is_empty())
            .unwrap_or_else(|| "main".to_string()),
        Err(_) => "main".to_string(),
    }
}

async fn read_package(orch: &Orchestrator, cell: &Cell) -> Result<ServicePackage, CallError> {
    let source = supervisor(orch)
        .sources()
        .open(orch, &cell.project_id, &cell.owner)
        .await
        .map_err(|error| CallError::new("start_failed", error))?;
    let service = cell.service.clone();
    let package = tokio::task::spawn_blocking(move || source.package(&service))
        .await
        .map_err(|error| CallError::new("start_failed", error.to_string()))?
        .map_err(|error| CallError::new("start_failed", error))?;
    package.ok_or_else(|| {
        let message = match &cell.owner {
            ServiceOwner::Project => format!(
                "project {} declares no service `{}` at its base tip",
                cell.project_key, cell.service
            ),
            ServiceOwner::Thread { name, .. } => format!(
                "thread {name} has written no service `{}` ({})",
                cell.service,
                cell.home()
            ),
            ServiceOwner::Pack { pack, machine, .. } => {
                format!("{pack}/{} is not selected on {machine}", cell.service)
            }
        };
        CallError::new("unknown_service", message)
    })
}

type BootFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Arc<Incarnation>, CallError>> + Send + 'a>,
>;

/// Start a new incarnation of `package` and wait for it to be ready. It is not
/// installed as the serving incarnation; the caller decides that.
///
/// Boxed because it is recursive through its own event reader: a crash
/// schedules a restart, which boots again.
fn boot<'a>(
    orch: &'a Orchestrator,
    cell: &'a Arc<Cell>,
    package: &'a ServicePackage,
    actor: &'a str,
) -> BootFuture<'a> {
    Box::pin(boot_incarnation(orch, cell, package, actor))
}

async fn boot_incarnation(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    package: &ServicePackage,
    actor: &str,
) -> Result<Arc<Incarnation>, CallError> {
    let generation = {
        let mut state = cell.state.lock().unwrap();
        state.generation += 1;
        if state.current.is_none() {
            state.status = ServiceState::Starting;
        }
        state.generation
    };
    let mut spec = LaunchSpec {
        project_id: cell.project_id.clone(),
        project_key: cell.project_key.clone(),
        owner: cell.owner.clone(),
        service: cell.service.clone(),
        generation,
        declaration: package.declaration.clone(),
        files: package.files.clone(),
        env: Vec::new(),
        native: package.launch.clone(),
    };
    // A service with no callback (every native one) gets no job, no run, no
    // bearer, and no `CAIRN_*` environment: nothing of Cairn's reaches it.
    let standing = match package.declaration.callback {
        Callback::Cairn => {
            Some(standing_run(orch, cell, generation, package, &mut spec, actor).await?)
        }
        Callback::None => None,
    };

    let process = match supervisor(orch).host().launch(orch, &spec).await {
        Ok(process) => process,
        Err(error) => {
            if let Some(standing) = &standing {
                end_run(orch, &cell.project_id, &standing.run_id, "start failed").await;
            }
            let error = error.into_call_error();
            lifecycle(orch, cell, generation, &error.message, actor).await;
            return Err(error);
        }
    };
    let audience = match &standing {
        Some(standing) => standing.audience.clone(),
        // Never presented: a resident with no callback holds no bearer. The
        // runner's own loopback stands in so the field keeps one shape.
        None => crate::fleet::residency::ResidentCallback::Direct {
            url: crate::fleet::residency::runner_callback_url(orch),
        }
        .audience()
        .map_err(|error| CallError::new("start_failed", error))?,
    };

    let limits = package.declaration.limits;
    let (phase, mut phase_rx) = watch::channel(Phase::Booting);
    let incarnation = Arc::new(Incarnation {
        generation,
        run_id: standing.as_ref().map(|standing| standing.run_id.clone()),
        source_hash: package.source_hash.clone(),
        declaration: package.declaration.clone(),
        transport: package.declaration.transport,
        launch: package.launch.clone(),
        stdout_sink: StdMutex::new(None),
        attached: AtomicBool::new(false),
        executor: process.executor.clone(),
        callback_audience: audience,
        input: process.input.clone(),
        pending: StdMutex::default(),
        phase,
        drained: AtomicBool::new(false),
        drained_notify: Notify::new(),
        gate: Arc::new(Semaphore::new(limits.concurrency as usize)),
        waiting: AtomicUsize::new(0),
        retiring: AtomicBool::new(false),
        started_at: Instant::now(),
    });
    tokio::spawn(read_events(
        orch.clone(),
        cell.clone(),
        incarnation.clone(),
        process.events,
    ));

    // Readiness is the transport's: a `ready` frame for the service protocol,
    // and for a native resident, its probe answering at all, whatever state
    // it reports.
    let outcome = match package.declaration.transport {
        Transport::Ndjson => greet(cell, package, &incarnation, &mut phase_rx).await,
        Transport::McpStdio | Transport::None => {
            machine::first_probe(orch, cell, &spec, &incarnation, &mut phase_rx).await
        }
    };
    if let Err(reason) = outcome {
        incarnation.retiring.store(true, Ordering::Release);
        incarnation.input.kill().await;
        let message = format!(
            "generation {generation} (source {}) failed to start: {reason}; its stderr is in this log",
            short(&package.source_hash)
        );
        lifecycle(orch, cell, generation, &message, actor).await;
        return Err(CallError::new("start_failed", message));
    }
    lifecycle(
        orch,
        cell,
        generation,
        &format!(
            "started generation {generation} (source {}) on {}",
            short(&package.source_hash),
            incarnation.executor
        ),
        actor,
    )
    .await;
    Ok(incarnation)
}

/// Send `hello` and wait for `ready`, within [`READY_TIMEOUT`].
async fn greet(
    cell: &Cell,
    package: &ServicePackage,
    incarnation: &Incarnation,
    phase_rx: &mut watch::Receiver<Phase>,
) -> Result<(), String> {
    let hello = RunnerFrame::Hello {
        protocol: PROTOCOL_VERSION,
        project: cell.project_key.clone(),
        service: cell.service.clone(),
        source: package.source_hash.clone(),
    };
    let ready = async {
        incarnation
            .send(&hello)
            .await
            .map_err(|error| format!("could not greet the service: {error}"))?;
        loop {
            match *phase_rx.borrow_and_update() {
                Phase::Ready => return Ok(()),
                Phase::Exited => return Err("the service exited before it was ready".to_string()),
                Phase::Booting => {}
            }
            if phase_rx.changed().await.is_err() {
                return Err("the service's event stream closed".to_string());
            }
        }
    };
    match tokio::time::timeout(READY_TIMEOUT, ready).await {
        Ok(outcome) => outcome,
        Err(_) => Err(format!(
            "the service did not answer `hello` within {}s",
            READY_TIMEOUT.as_secs()
        )),
    }
}

/// A Cairn-callback service's standing run for one incarnation.
struct Standing {
    run_id: String,
    audience: crate::security::LeaseAudience,
}

/// Mint the standing run and its bearer, and write the `CAIRN_*` environment
/// into `spec`.
async fn standing_run(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    generation: u64,
    package: &ServicePackage,
    spec: &mut LaunchSpec,
    actor: &str,
) -> Result<Standing, CallError> {
    let db = cell_db(orch, &cell.project_id)
        .await
        .map_err(|error| CallError::new("start_failed", error))?;
    let job_id = rows::ensure_service_job(&db, &cell.project_id, &cell.owner, &cell.service)
        .await
        .map_err(|error| CallError::new("start_failed", error))?;
    let run_id = rows::mint_run(&db, &cell.project_id, &job_id)
        .await
        .map_err(|error| CallError::new("start_failed", error))?;
    let home = cell.home();
    // Placed before any bearer exists, because where the service calls back
    // from is what its bearers are bound to: this runner's loopback, or the
    // relay of the one executor it runs on.
    let callback = match supervisor(orch).host().callback(orch, spec).await {
        Ok(callback) => callback,
        Err(error) => {
            end_run(orch, &cell.project_id, &run_id, "start failed").await;
            let error = error.into_call_error();
            lifecycle(orch, cell, generation, &error.message, actor).await;
            return Err(error);
        }
    };
    let audience = match callback.audience() {
        Ok(audience) => audience,
        Err(error) => {
            end_run(orch, &cell.project_id, &run_id, "start failed").await;
            return Err(CallError::new("start_failed", error));
        }
    };
    // Presented here because it is written into the launch environment below.
    let token = match mint_bearer(orch, &run_id, &home, None, &audience).and_then(|bearer| {
        crate::mcp::run_bearer::service_credential(&bearer, &audience)
            .map_err(|error| CallError::new("start_failed", error))
    }) {
        Ok(token) => token,
        Err(error) => {
            end_run(orch, &cell.project_id, &run_id, "start failed").await;
            return Err(error);
        }
    };
    orch.project_services.runs.insert(
        &run_id,
        ServiceRun {
            project_id: cell.project_id.clone(),
            project_key: cell.project_key.clone(),
            owner: cell.owner.clone(),
            service: cell.service.clone(),
            generation,
            default_branch: default_branch(orch, &cell.project_id).await,
            role: ServiceRunRole::Standing,
        },
    );

    let mut env: Vec<(String, String)> = package
        .declaration
        .env
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    env.extend([
        ("CAIRN_RUN_ID".to_string(), run_id.clone()),
        // A run bearer, never the machine secret: it speaks for this run alone
        // and is revoked when the incarnation ends.
        ("CAIRN_MCP_SECRET".to_string(), token),
        (
            "CAIRN_ENV".to_string(),
            cairn_common::paths::env_str().to_string(),
        ),
        ("CAIRN_SERVICE".to_string(), cell.service.clone()),
    ]);
    // A service elsewhere is told its callback by the executor that relays it.
    if let crate::fleet::residency::ResidentCallback::Direct { url } = &callback {
        env.push(("CAIRN_CALLBACK_URL".to_string(), url.clone()));
    }
    spec.env = env;
    Ok(Standing { run_id, audience })
}

fn install(orch: &Orchestrator, cell: &Arc<Cell>, incarnation: Arc<Incarnation>) {
    let idle = incarnation.declaration.idle_stop_after;
    let generation = incarnation.generation;
    *cell.last_activity.lock().unwrap() = Instant::now();
    {
        let mut state = cell.state.lock().unwrap();
        state.last_source_hash = Some(incarnation.source_hash.clone());
        state.current = Some(incarnation);
        state.status = ServiceState::Running;
        state.backoff_until = None;
    }
    if let Some(idle) = idle {
        tokio::spawn(idle_watch(orch.clone(), cell.clone(), generation, idle));
    }
}

/// Stop generation `generation` once it has had no call in flight for
/// `idle`. Not a sticky stop: the next call or scheduled tick starts the
/// service again. Ends without stopping anything once the generation is no
/// longer the one serving.
async fn idle_watch(orch: Orchestrator, cell: Arc<Cell>, generation: u64, idle: Duration) {
    loop {
        let quiet_for = cell.last_activity.lock().unwrap().elapsed();
        if quiet_for < idle {
            tokio::time::sleep(idle - quiet_for).await;
            continue;
        }
        if cell.in_flight.load(Ordering::SeqCst) > 0 {
            // The call's end resets the clock.
            tokio::time::sleep(idle).await;
            continue;
        }
        let _ops = cell.ops.lock().await;
        let taken = {
            let mut state = cell.state.lock().unwrap();
            let ours = state
                .current
                .as_ref()
                .is_some_and(|current| current.generation == generation && current.serving());
            if !ours {
                return;
            }
            let busy = cell.in_flight.load(Ordering::SeqCst) > 0
                || cell.last_activity.lock().unwrap().elapsed() < idle;
            if busy {
                None
            } else {
                state.status = ServiceState::Stopped;
                state.current.take()
            }
        };
        let Some(current) = taken else {
            continue;
        };
        lifecycle(
            &orch,
            &cell,
            generation,
            &format!(
                "no call for {}; stopped generation {generation} (the next call or scheduled tick starts it)",
                schedule::every_text(idle.as_millis() as i64)
            ),
            "runner (idle stop)",
        )
        .await;
        retire(current).await;
        supervisor(&orch)
            .host()
            .release(&orch, &cell.project_id, &cell.owner, &cell.service)
            .await;
        return;
    }
}

/// Drain an incarnation: let its in-flight calls finish, then stop it.
async fn retire(incarnation: Arc<Incarnation>) {
    incarnation.retiring.store(true, Ordering::Release);
    if incarnation.phase() == Phase::Exited {
        return;
    }
    // Only the service protocol has a drain to ask for; anything else is
    // stopped, as a resident always was.
    if incarnation.transport != Transport::Ndjson {
        incarnation.input.kill().await;
        return;
    }
    let _ = incarnation.send(&RunnerFrame::Drain).await;
    let grace = Duration::from_secs(incarnation.declaration.limits.call_timeout_seconds as u64)
        + DRAIN_GRACE;
    let settled = async {
        loop {
            if incarnation.phase() == Phase::Exited {
                return;
            }
            if incarnation.pending_len() == 0 && incarnation.drained.load(Ordering::Acquire) {
                return;
            }
            tokio::select! {
                () = incarnation.drained_notify.notified() => {}
                () = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
        }
    };
    let _ = tokio::time::timeout(grace, settled).await;
    if incarnation.phase() != Phase::Exited {
        incarnation.input.kill().await;
    }
}

/// One incarnation's output, until it exits.
async fn read_events(
    orch: Orchestrator,
    cell: Arc<Cell>,
    incarnation: Arc<Incarnation>,
    mut events: mpsc::UnboundedReceiver<HostEvent>,
) {
    let mut stdout = LineAssembler::new(MAX_LINE_BYTES);
    let mut stderr = LineAssembler::new(super::log::TEXT_MAX_BYTES);
    let mut violations = ViolationWindow::default();
    let generation = incarnation.generation;
    let (code, detail, executor_lost) = loop {
        let Some(event) = events.recv().await else {
            break (None, "its event stream closed".to_string(), false);
        };
        match event {
            HostEvent::Stdout(bytes) if incarnation.transport == Transport::McpStdio => {
                if let Some(sink) = incarnation.stdout_sink.lock().unwrap().as_ref() {
                    let _ = sink.send(bytes);
                }
            }
            HostEvent::Stdout(_) if incarnation.transport == Transport::None => {}
            HostEvent::Stdout(bytes) => {
                for line in stdout.push(&bytes) {
                    let problem = match line {
                        Line::Complete(line) if line.iter().all(u8::is_ascii_whitespace) => None,
                        Line::Complete(line) => handle_frame(&incarnation, &line).err(),
                        Line::Oversized { total, .. } => Some(format!(
                            "discarded a {total}-byte stdout line over the {MAX_LINE_BYTES}-byte limit"
                        )),
                    };
                    if let Some(problem) = problem {
                        let kill = violations.record(Instant::now());
                        let text = if kill {
                            format!("protocol violation: {problem}; three in ten seconds, stopping generation {generation}")
                        } else {
                            format!("protocol violation: {problem}")
                        };
                        log(
                            &orch,
                            &cell,
                            generation,
                            LogKind::Lifecycle,
                            &text,
                            json!({"event": "violation"}),
                        )
                        .await;
                        if kill {
                            incarnation.input.kill().await;
                        }
                    }
                }
            }
            HostEvent::Stderr(bytes) => {
                for line in stderr.push(&bytes) {
                    stderr_row(&orch, &cell, generation, line).await;
                }
            }
            HostEvent::Exited {
                code,
                detail,
                executor_lost,
            } => break (code, detail, executor_lost),
        }
    };
    if let Some(line) = stderr.finish() {
        stderr_row(&orch, &cell, generation, line).await;
    }
    on_exit(&orch, &cell, &incarnation, code, &detail, executor_lost).await;
}

async fn stderr_row(orch: &Orchestrator, cell: &Cell, generation: u64, line: Line) {
    let text = match line {
        Line::Complete(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Line::Oversized { prefix, .. } => String::from_utf8_lossy(&prefix).into_owned(),
    };
    if text.trim().is_empty() {
        return;
    }
    log(orch, cell, generation, LogKind::Stderr, &text, json!({})).await;
}

fn handle_frame(incarnation: &Incarnation, line: &[u8]) -> Result<(), String> {
    match ServiceFrame::decode(line)? {
        ServiceFrame::Ready { .. } => {
            let _ = incarnation.phase.send(Phase::Ready);
        }
        ServiceFrame::Result {
            id,
            ok,
            value,
            error,
        } => {
            let Some(reply) = incarnation.pending.lock().unwrap().remove(&id) else {
                // A late answer to a call the runner already gave up on.
                return Ok(());
            };
            let outcome = if ok {
                Ok(value.unwrap_or(Value::Null))
            } else {
                let error = error.unwrap_or(super::protocol::ServiceError {
                    message: "the service failed the call".into(),
                    code: None,
                });
                Err(CallError::new(
                    error.code.as_deref().unwrap_or("service_error"),
                    error.message,
                ))
            };
            let _ = reply.send(outcome);
        }
        ServiceFrame::Drained => {
            incarnation.drained.store(true, Ordering::Release);
            incarnation.drained_notify.notify_waiters();
        }
    }
    Ok(())
}

async fn on_exit(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    incarnation: &Arc<Incarnation>,
    code: Option<i32>,
    detail: &str,
    executor_lost: bool,
) {
    let booting = incarnation.phase() == Phase::Booting;
    let _ = incarnation.phase.send(Phase::Exited);
    // The attached gateway connection sees its stream end.
    incarnation.stdout_sink.lock().unwrap().take();
    let generation = incarnation.generation;
    let code_text = code.map_or("no exit code".to_string(), |code| {
        format!("exit code {code}")
    });
    incarnation.fail_pending(&CallError::new(
        "service_crashed",
        format!("the service exited ({code_text}, {detail}) while the call was in flight"),
    ));
    // A machine service whose machine went away did nothing wrong: it waits
    // for the machine, and starts again when it attaches.
    let machine_absent = executor_lost && cell.machine.is_some();
    if machine_absent {
        let mut state = cell.state.lock().unwrap();
        if state
            .current
            .as_ref()
            .is_some_and(|current| current.generation == generation)
        {
            state.current = None;
            state.status = ServiceState::Stopped;
        }
    }
    let expected = booting || machine_absent || incarnation.retiring.load(Ordering::Acquire);
    if let Some(run_id) = &incarnation.run_id {
        end_run(
            orch,
            &cell.project_id,
            run_id,
            if expected { "stopped" } else { "crashed" },
        )
        .await;
    }
    if expected {
        lifecycle(
            orch,
            cell,
            generation,
            &format!("generation {generation} exited ({code_text})"),
            "runner",
        )
        .await;
        return;
    }
    crashed(
        orch,
        cell,
        generation,
        &format!("generation {generation} crashed ({code_text}, {detail})"),
    )
    .await;
}

/// Record a crash and decide what follows it: a restart after backoff, or
/// `failed` once crashes come too fast.
///
/// Boxed for the same reason as [`boot`]: a failed restart crashes again.
fn crashed<'a>(
    orch: &'a Orchestrator,
    cell: &'a Arc<Cell>,
    generation: u64,
    what: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(record_crash(orch, cell, generation, what))
}

async fn record_crash(orch: &Orchestrator, cell: &Arc<Cell>, generation: u64, what: &str) {
    let delay = {
        let mut state = cell.state.lock().unwrap();
        if state
            .current
            .as_ref()
            .is_some_and(|current| current.generation == generation)
        {
            state.current = None;
        }
        if state.current.is_some() || state.sticky_stop {
            // A newer generation is serving, or the operator stopped it.
            None
        } else {
            let now = Instant::now();
            state.crashes.push_back(now);
            while state
                .crashes
                .front()
                .is_some_and(|at| now.duration_since(*at) > CRASH_WINDOW)
            {
                state.crashes.pop_front();
            }
            state.last_error = Some(what.to_string());
            if state.crashes.len() >= CRASH_LIMIT {
                state.status = ServiceState::Failed;
                state.backoff_until = None;
                None
            } else {
                let exponent = (state.crashes.len() as u32).saturating_sub(1).min(6);
                let delay = Duration::from_secs(1u64 << exponent).min(MAX_BACKOFF);
                state.status = ServiceState::Backoff;
                state.backoff_until = Some(now + delay);
                Some((delay, state.generation))
            }
        }
    };
    let status = cell.state.lock().unwrap().status;
    let text = match (delay, status) {
        (Some((delay, _)), _) => format!("{what}; restarting in {}s", delay.as_secs()),
        (None, ServiceState::Failed) => format!(
            "{what}; {CRASH_LIMIT} crashes in {} minutes, the service is failed until it is started, restarted, or its source changes",
            CRASH_WINDOW.as_secs() / 60
        ),
        (None, _) => what.to_string(),
    };
    lifecycle(orch, cell, generation, &text, "runner").await;
    if let Some((delay, expected_generation)) = delay {
        let (orch, cell) = (orch.clone(), cell.clone());
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            restart_after_backoff(&orch, &cell, expected_generation).await;
        });
    }
}

async fn restart_after_backoff(orch: &Orchestrator, cell: &Arc<Cell>, expected_generation: u64) {
    let _ops = cell.ops.lock().await;
    {
        let state = cell.state.lock().unwrap();
        if state.status != ServiceState::Backoff
            || state.generation != expected_generation
            || state.sticky_stop
            || state.current.is_some()
        {
            return;
        }
    }
    {
        let mut state = cell.state.lock().unwrap();
        state.restarts += 1;
    }
    let result = match read_package(orch, cell).await {
        Ok(package) => boot(orch, cell, &package, "runner (restart after crash)").await,
        Err(error) => Err(error),
    };
    match result {
        Ok(incarnation) => install(orch, cell, incarnation),
        Err(error) => {
            let generation = cell.state.lock().unwrap().generation;
            crashed(
                orch,
                cell,
                generation,
                &format!("restart failed: {}", error.message),
            )
            .await;
        }
    }
}

/// A serving incarnation, starting one if the service is idle. Refuses a
/// stopped or failed service.
async fn ensure_serving(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    actor: &str,
) -> Result<Arc<Incarnation>, CallError> {
    if let Some(incarnation) = cell.serving() {
        return Ok(incarnation);
    }
    let _ops = cell.ops.lock().await;
    if let Some(incarnation) = cell.serving() {
        return Ok(incarnation);
    }
    {
        let state = cell.state.lock().unwrap();
        if state.sticky_stop {
            return Err(CallError::new(
                "service_stopped",
                format!(
                    "service `{}` is stopped; start or restart it first",
                    cell.service
                ),
            ));
        }
        match state.status {
            ServiceState::Failed => {
                return Err(CallError::new(
                    "service_failed",
                    format!(
                        "service `{}` is failed: {}; start or restart it, or change its source",
                        cell.service,
                        state
                            .last_error
                            .as_deref()
                            .unwrap_or("it crashed repeatedly")
                    ),
                ))
            }
            ServiceState::Backoff => {
                let wait = state
                    .backoff_until
                    .map(|until| until.saturating_duration_since(Instant::now()).as_secs())
                    .unwrap_or(0);
                return Err(CallError::new(
                    "service_restarting",
                    format!("service `{}` crashed and restarts in {wait}s", cell.service),
                ));
            }
            _ => {}
        }
    }
    let package = read_package(orch, cell).await?;
    match boot(orch, cell, &package, actor).await {
        Ok(incarnation) => {
            install(orch, cell, incarnation.clone());
            Ok(incarnation)
        }
        Err(error) => {
            let mut state = cell.state.lock().unwrap();
            if state.current.is_none() {
                state.status = ServiceState::Stopped;
            }
            state.last_error = Some(error.message.clone());
            Err(error)
        }
    }
}

/// Replace the serving incarnation with `package`, or start it if none is
/// serving. The caller holds `cell.ops`.
async fn replace(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    package: &ServicePackage,
    actor: &str,
) -> Result<(), CallError> {
    let old = cell.state.lock().unwrap().current.clone();
    match boot(orch, cell, package, actor).await {
        Ok(incarnation) => {
            let generation = incarnation.generation;
            install(orch, cell, incarnation);
            if let Some(old) = old {
                lifecycle(
                    orch,
                    cell,
                    generation,
                    &format!(
                        "generation {generation} is serving; draining generation {}",
                        old.generation
                    ),
                    actor,
                )
                .await;
                tokio::spawn(retire(old));
            }
            Ok(())
        }
        Err(error) => {
            let mut state = cell.state.lock().unwrap();
            if state.current.is_some() {
                state.status = ServiceState::UpdateFailed;
            } else {
                state.status = ServiceState::Stopped;
            }
            state.last_error = Some(error.message.clone());
            state.last_source_hash = Some(package.source_hash.clone());
            Err(error)
        }
    }
}

async fn project_key(orch: &Orchestrator, project_id: &str) -> Result<String, String> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string())?;
    db.query_opt_text(
        "SELECT key FROM projects WHERE id = ?1",
        cairn_db::turso::params![project_id],
    )
    .await
    .map_err(|error| error.to_string())?
    .ok_or_else(|| format!("project {project_id} not found"))
}

/// Run `work` to completion whatever happens to its caller.
///
/// Every lifecycle action and every call holds state that only its own end
/// releases: a minted run and bearer, a queue slot, a half-booted process.
/// The resource write or bridge request that started it can be dropped at any
/// await (a client timeout, a stopped agent), so the work runs as its own
/// task and the caller only waits for it.
async fn detached<T: Send + 'static>(
    work: impl std::future::Future<Output = Result<T, CallError>> + Send + 'static,
) -> Result<T, CallError> {
    tokio::spawn(work).await.unwrap_or_else(|error| {
        Err(CallError::new(
            "service_unavailable",
            format!("the service task failed: {error}"),
        ))
    })
}

/// Which service an action is for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceRef {
    pub project_id: String,
    pub project_key: String,
    pub owner: ServiceOwner,
    pub service: String,
}

impl ServiceRef {
    /// A service its project declares.
    pub(crate) fn project(project_id: &str, project_key: &str, service: &str) -> Self {
        Self {
            project_id: project_id.to_string(),
            project_key: project_key.to_string(),
            owner: ServiceOwner::Project,
            service: service.to_string(),
        }
    }

    fn cell(&self, orch: &Orchestrator) -> Arc<Cell> {
        supervisor(orch).cell(
            &self.project_id,
            &self.project_key,
            &self.owner,
            &self.service,
        )
    }

    /// This runner's view of the service, or `None` when it has never touched it.
    pub(crate) fn status(&self, orch: &Orchestrator) -> Option<ServiceStatus> {
        supervisor(orch).status(&self.project_id, &self.owner, &self.service)
    }
}

/// Start a service explicitly. Clears a sticky stop and a failure.
pub(crate) async fn start(
    orch: &Orchestrator,
    target: &ServiceRef,
    actor: &str,
) -> Result<(), CallError> {
    let (orch, target, actor) = (orch.clone(), target.clone(), actor.to_string());
    detached(async move { start_attached(&orch, &target, &actor).await }).await
}

async fn start_attached(
    orch: &Orchestrator,
    target: &ServiceRef,
    actor: &str,
) -> Result<(), CallError> {
    let cell = target.cell(orch);
    {
        let mut state = cell.state.lock().unwrap();
        state.sticky_stop = false;
        state.crashes.clear();
        if matches!(state.status, ServiceState::Failed | ServiceState::Backoff) {
            state.status = ServiceState::Stopped;
        }
    }
    ensure_serving(orch, &cell, actor).await.map(|_| ())
}

/// Stop a service. Sticky: calls are refused until a start or restart.
pub(crate) async fn stop(
    orch: &Orchestrator,
    target: &ServiceRef,
    actor: &str,
) -> Result<(), CallError> {
    let (orch, target, actor) = (orch.clone(), target.clone(), actor.to_string());
    detached(async move { stop_attached(&orch, &target, &actor).await }).await
}

async fn stop_attached(
    orch: &Orchestrator,
    target: &ServiceRef,
    actor: &str,
) -> Result<(), CallError> {
    let cell = target.cell(orch);
    let _ops = cell.ops.lock().await;
    let current = {
        let mut state = cell.state.lock().unwrap();
        state.sticky_stop = true;
        state.status = ServiceState::Stopped;
        state.backoff_until = None;
        state.current.take()
    };
    let generation = current.as_ref().map_or(0, |i| i.generation);
    lifecycle(orch, &cell, generation, "stopped", actor).await;
    if let Some(current) = current {
        retire(current).await;
    }
    supervisor(orch)
        .host()
        .release(orch, &target.project_id, &target.owner, &target.service)
        .await;
    Ok(())
}

/// Restart a service: a rolling replace when it is serving, a start otherwise.
pub(crate) async fn restart(
    orch: &Orchestrator,
    target: &ServiceRef,
    actor: &str,
) -> Result<(), CallError> {
    let (orch, target, actor) = (orch.clone(), target.clone(), actor.to_string());
    detached(async move { restart_attached(&orch, &target, &actor).await }).await
}

async fn restart_attached(
    orch: &Orchestrator,
    target: &ServiceRef,
    actor: &str,
) -> Result<(), CallError> {
    let cell = target.cell(orch);
    let _ops = cell.ops.lock().await;
    {
        let mut state = cell.state.lock().unwrap();
        state.sticky_stop = false;
        state.crashes.clear();
    }
    let package = read_package(orch, &cell).await?;
    replace(orch, &cell, &package, actor).await
}

/// Apply an owner's current source to every service it declares or this
/// runner runs for it: the one place a source change becomes starts,
/// replacements, and stops.
///
/// `deploy` names a service whose write this change is. A thread's write is
/// its deploy, so that service starts if it is idle, whatever its start
/// policy, unless it was stopped explicitly. A serving service is replaced
/// either way.
pub(crate) async fn apply_source(
    orch: &Orchestrator,
    project_id: &str,
    owner: &ServiceOwner,
    reason: &str,
    deploy: Option<&str>,
) {
    let source = match supervisor(orch)
        .sources()
        .open(orch, project_id, owner)
        .await
    {
        Ok(source) => source,
        Err(error) => {
            tracing::warn!(%error, project_id, "could not read project services");
            return;
        }
    };
    let key = match project_key(orch, project_id).await {
        Ok(key) => key,
        Err(error) => {
            tracing::warn!(%error, project_id, "could not resolve a project key for services");
            return;
        }
    };
    let listing_source = source.clone();
    let listings = match tokio::task::spawn_blocking(move || listing_source.list()).await {
        Ok(Ok(listings)) => listings,
        Ok(Err(error)) => {
            tracing::warn!(%error, project_id, "could not list project services");
            return;
        }
        Err(error) => {
            tracing::warn!(%error, project_id, "service listing task failed");
            return;
        }
    };
    // (start policy, whether it schedules anything)
    let declared: HashMap<String, (StartPolicy, bool)> = listings
        .iter()
        .filter_map(|listing| {
            listing.declaration.as_ref().ok().map(|declaration| {
                (
                    listing.id.clone(),
                    (declaration.start, !declaration.schedule.is_empty()),
                )
            })
        })
        .collect();
    let db = crate::execution::routing::owning_db_for_project(&orch.db, project_id)
        .await
        .map_err(|error| error.to_string());
    // A listing that failed to parse keeps its rows: an invalid declaration
    // is not a removed one.
    let listed: Vec<String> = listings.iter().map(|listing| listing.id.clone()).collect();
    if let Err(error) = match &db {
        Ok(db) => schedule::retain_services(db, project_id, owner, &listed).await,
        Err(error) => Err(error.clone()),
    } {
        tracing::warn!(%error, project_id, "could not prune service schedules");
    }

    let mut ids: Vec<String> = supervisor(orch)
        .cells_for(project_id, owner)
        .iter()
        .map(|cell| cell.service.clone())
        .collect();
    for (id, (start, scheduled)) in &declared {
        let wanted = *start == StartPolicy::Launch || deploy == Some(id.as_str()) || *scheduled;
        if wanted && !ids.contains(id) {
            ids.push(id.clone());
        }
    }

    for id in ids {
        let cell = supervisor(orch).cell(project_id, &key, owner, &id);
        let _ops = cell.ops.lock().await;
        let Some((start, _)) = declared.get(&id).copied() else {
            // Its rows went with `retain_services`; the task sees none and ends.
            wake_schedule(orch, &cell);
            let current = {
                let mut state = cell.state.lock().unwrap();
                state.status = ServiceState::Stopped;
                state.current.take()
            };
            if let Some(current) = current {
                lifecycle(
                    orch,
                    &cell,
                    current.generation,
                    "declaration removed; stopping",
                    reason,
                )
                .await;
                retire(current).await;
                supervisor(orch)
                    .host()
                    .release(orch, project_id, owner, &id)
                    .await;
            }
            continue;
        };
        let package_source = source.clone();
        let package_id = id.clone();
        let package =
            match tokio::task::spawn_blocking(move || package_source.package(&package_id)).await {
                Ok(Ok(Some(package))) => package,
                Ok(Ok(None)) => continue,
                Ok(Err(error)) => {
                    lifecycle(
                        orch,
                        &cell,
                        0,
                        &format!("could not read the package: {error}"),
                        reason,
                    )
                    .await;
                    continue;
                }
                Err(_) => continue,
            };
        match &db {
            Ok(db) => match schedule::sync(
                db,
                project_id,
                owner,
                &id,
                &package.declaration.schedule,
                now_secs(),
            )
            .await
            {
                Ok(_) => wake_schedule(orch, &cell),
                Err(error) => {
                    lifecycle(orch, &cell, 0, &error, reason).await;
                }
            },
            Err(error) => tracing::warn!(%error, project_id, "could not sync a service schedule"),
        }
        let (serving_hash, last_hash, sticky) = {
            let state = cell.state.lock().unwrap();
            (
                state
                    .current
                    .as_ref()
                    .filter(|current| current.serving())
                    .map(|current| current.source_hash.clone()),
                state.last_source_hash.clone(),
                state.sticky_stop,
            )
        };
        match serving_hash {
            Some(hash) if hash == package.source_hash => {}
            Some(_) => {
                let _ = replace(orch, &cell, &package, reason).await;
            }
            None => {
                if last_hash.as_deref() != Some(package.source_hash.as_str()) {
                    let mut state = cell.state.lock().unwrap();
                    state.crashes.clear();
                    if matches!(
                        state.status,
                        ServiceState::Failed | ServiceState::UpdateFailed | ServiceState::Backoff
                    ) {
                        state.status = ServiceState::Stopped;
                    }
                    state.last_source_hash = Some(package.source_hash.clone());
                }
                let idle = cell.state.lock().unwrap().status == ServiceState::Stopped;
                let wanted = start == StartPolicy::Launch || deploy == Some(id.as_str());
                if wanted && !sticky && idle {
                    match boot(orch, &cell, &package, reason).await {
                        Ok(incarnation) => install(orch, &cell, incarnation),
                        // Not left `starting`: nothing is, and the next call or
                        // start reads the source again.
                        Err(error) => {
                            let mut state = cell.state.lock().unwrap();
                            if state.current.is_none() {
                                state.status = ServiceState::Stopped;
                            }
                            state.last_error = Some(error.message);
                        }
                    }
                }
            }
        }
    }
}

/// The cell's schedule rows may have changed: start its schedule task, or
/// tell the running one to read them again.
fn wake_schedule(orch: &Orchestrator, cell: &Arc<Cell>) {
    let mut task = cell.schedule_task.lock().unwrap();
    task.epoch += 1;
    if task.running {
        cell.schedule_changed.notify_one();
        return;
    }
    task.running = true;
    tokio::spawn(run_schedule(orch.clone(), cell.clone()));
}

/// One cell's cadence: sleep until its next occurrence, run what is due, and
/// end once the cell schedules nothing.
async fn run_schedule(orch: Orchestrator, cell: Arc<Cell>) {
    loop {
        let epoch = cell.schedule_task.lock().unwrap().epoch;
        let rows = match crate::execution::routing::owning_db_for_project(
            &orch.db,
            &cell.project_id,
        )
        .await
        {
            Ok(db) => schedule::list(&db, &cell.project_id, &cell.owner, &cell.service).await,
            Err(error) => Err(error.to_string()),
        };
        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(%error, service = %cell.service, "could not read a service schedule");
                tokio::select! {
                    () = tokio::time::sleep(SCHEDULE_RECHECK) => {}
                    () = cell.schedule_changed.notified() => {}
                }
                continue;
            }
        };
        if rows.is_empty() {
            let mut task = cell.schedule_task.lock().unwrap();
            if task.epoch == epoch {
                task.running = false;
                return;
            }
            continue;
        }
        let now = now_secs();
        if fire_due(&orch, &cell, &rows, now).await > 0 {
            continue;
        }
        let next = rows
            .iter()
            .filter_map(|row| row.next_due_at(now))
            .min()
            .unwrap_or(now + SCHEDULE_RECHECK.as_secs() as i64);
        let wait =
            Duration::from_secs(next.saturating_sub(now).max(1) as u64).min(SCHEDULE_RECHECK);
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            () = cell.schedule_changed.notified() => {}
        }
    }
}

/// Run every occurrence of `rows` due at `now` (epoch seconds), one after
/// another. Answers how many were due and not deferred: a deferred occurrence
/// stays due, and the task waits for a wake or its recheck to try it again.
async fn fire_due(orch: &Orchestrator, cell: &Arc<Cell>, rows: &[ScheduleRow], now: i64) -> usize {
    let mut due_count = 0;
    for row in rows {
        if let Some(due) = row.due_at(now) {
            if fire_occurrence(orch, cell, row, due).await {
                due_count += 1;
            }
        }
    }
    due_count
}

/// An occurrence as a log row names it.
fn occurrence_at(due: i64) -> String {
    chrono::DateTime::from_timestamp(due, 0)
        .map(|at| at.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| due.to_string())
}

/// Claim occurrence `due` of one entry, call its method as the owner, record
/// the outcome, and tell the owning thread if a failure streak begins.
/// Answers false when the occurrence was deferred, unclaimed, to run later:
/// before the project pass, or after a placement refusal.
async fn fire_occurrence(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    row: &ScheduleRow,
    due: i64,
) -> bool {
    if cell.owner == ServiceOwner::Project
        && !supervisor(orch).projects_current.load(Ordering::Acquire)
    {
        if !cell.deferral_noted.swap(true, Ordering::AcqRel) {
            let at = occurrence_at(due);
            lifecycle(
                orch,
                cell,
                0,
                &format!(
                    "scheduled `{}` due {at} deferred: a project's services start after the \
                     runner's startup catch-up brings its base tip current, and it runs then",
                    row.method
                ),
                "schedule",
            )
            .await;
        }
        return false;
    }
    let db = match crate::execution::routing::owning_db_for_project(&orch.db, &cell.project_id)
        .await
    {
        Ok(db) => db,
        Err(error) => {
            tracing::warn!(%error, service = %cell.service, "could not reach a service's schedule");
            return true;
        }
    };
    let (project_id, owner, service) = (&cell.project_id, &cell.owner, &cell.service);
    match schedule::claim(&db, project_id, owner, service, &row.entry_key, due).await {
        Ok(true) => {}
        Ok(false) => return true,
        Err(error) => {
            tracing::warn!(%error, service = %cell.service, "could not claim a scheduled occurrence");
            return true;
        }
    }
    // The occurrence a failure streak's notice names: this one, or the one
    // placement refusals began at when they end in a failure.
    let mut began_at = due;
    let outcome = if cell.state.lock().unwrap().sticky_stop {
        schedule::Outcome::Skipped(
            "the service is stopped; start or restart it to resume its schedule".into(),
        )
    } else {
        let request = CallRequest {
            project_id: project_id.clone(),
            project_key: cell.project_key.clone(),
            owner: owner.clone(),
            service: service.clone(),
            method: row.method.clone(),
            params: row.args.clone(),
            caller: CallerFrame {
                kind: CallerKind::Schedule,
                acting_for: owner.acting_home(&cell.project_key),
                agent: None,
                viewer: None,
                instrument: None,
            },
            timeout: None,
        };
        match call(orch, request).await {
            Ok(_) => schedule::Outcome::Ok,
            Err(error) if error.code == PLACEMENT_UNAVAILABLE => {
                match retry_unplaced(orch, cell, &db, row, due, &error).await {
                    None => return false,
                    Some(first) => {
                        began_at = first;
                        schedule::Outcome::Failed(error.to_string())
                    }
                }
            }
            Err(error) => schedule::Outcome::Failed(error.to_string()),
        }
    };
    // However the occurrence settled, nothing of it is left to retry.
    cell.placement_retries
        .lock()
        .unwrap()
        .remove(&row.entry_key);
    let began = match schedule::settle(
        &db,
        project_id,
        owner,
        service,
        &row.entry_key,
        &outcome,
        now_secs(),
    )
    .await
    {
        Ok(began) => began,
        Err(error) => {
            tracing::warn!(%error, service = %cell.service, "could not record a scheduled outcome");
            false
        }
    };
    if let (true, schedule::Outcome::Failed(error)) = (began, &outcome) {
        let home = cell.home();
        let content = schedule::streak_notice(&home, service, row, began_at, error);
        let key = format!("service-schedule:{home}:{}", row.entry_key);
        if let Err(error) = schedule::notify_owner(orch, &db, owner, &key, &content).await {
            tracing::warn!(%error, service = %cell.service, "could not send a schedule failure notice");
        }
    }
    true
}

/// Occurrence `due` was refused by the placement substrate, which says nothing
/// about the service. Give back its claim, so it stays due and the schedule
/// task's recheck tries it again, and log that it will. Answers the occurrence
/// the refusals began at once they stop being a retry, to be recorded as a
/// failure: the first try and every retry were refused, or the substrate is
/// still refusing when a later occurrence comes due.
async fn retry_unplaced(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    db: &crate::storage::LocalDb,
    row: &ScheduleRow,
    due: i64,
    error: &CallError,
) -> Option<i64> {
    let retry = {
        let mut retries = cell.placement_retries.lock().unwrap();
        let retry = retries
            .entry(row.entry_key.clone())
            .or_insert_with(|| PlacementRetry {
                due,
                refusals: 0,
                refusal: String::new(),
            });
        retry.refusals += 1;
        retry.refusal = error.message.clone();
        retry.clone()
    };
    if retry.due < due || retry.refusals > PLACEMENT_RETRIES {
        return Some(retry.due);
    }
    if let Err(unclaim) = schedule::unclaim(
        db,
        &cell.project_id,
        &cell.owner,
        &cell.service,
        &row.entry_key,
        due,
        row.last_fired_occurrence_at,
    )
    .await
    {
        // Left claimed it would never run: record it rather than lose it.
        tracing::warn!(error = %unclaim, service = %cell.service, "could not release a refused occurrence");
        return Some(retry.due);
    }
    let generation = cell.state.lock().unwrap().generation;
    lifecycle(
        orch,
        cell,
        generation,
        &format!(
            "scheduled `{}` due {} could not be placed ({}); retrying ({} of {PLACEMENT_RETRIES})",
            row.method,
            occurrence_at(due),
            error.message,
            retry.refusals,
        ),
        "schedule",
    )
    .await;
    None
}

/// Run what `target`'s schedule has due at `now` (epoch seconds), as its
/// schedule task would at that instant.
#[cfg(test)]
pub(crate) async fn run_due(orch: &Orchestrator, target: &ServiceRef, now: i64) -> usize {
    let cell = target.cell(orch);
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &cell.project_id)
        .await
        .unwrap();
    let rows = schedule::list(&db, &cell.project_id, &cell.owner, &cell.service)
        .await
        .unwrap();
    fire_due(orch, &cell, &rows, now).await
}

/// A thread has written a new version of `target`: apply its source, with the
/// written service as the one being deployed, and answer the status that
/// leaves. Detached like every lifecycle action, so a caller that goes away
/// mid-boot cannot strand the process it was starting.
pub(crate) async fn deploy(
    orch: &Orchestrator,
    target: &ServiceRef,
    actor: &str,
) -> Option<ServiceStatus> {
    let (task_orch, task_target, actor) = (orch.clone(), target.clone(), actor.to_string());
    let _ = detached(async move {
        apply_source(
            &task_orch,
            &task_target.project_id,
            &task_target.owner,
            &actor,
            Some(&task_target.service),
        )
        .await;
        Ok(())
    })
    .await;
    target.status(orch)
}

/// Every service the owner's source declares, and the revision read.
pub(crate) async fn declarations(
    orch: &Orchestrator,
    project_id: &str,
    owner: &ServiceOwner,
) -> Result<(String, Vec<super::declaration::ServiceListing>), String> {
    let source = supervisor(orch)
        .sources()
        .open(orch, project_id, owner)
        .await?;
    tokio::task::spawn_blocking(move || {
        let revision = source.revision().to_string();
        source.list().map(|listings| (revision, listings))
    })
    .await
    .map_err(|error| error.to_string())?
}

/// The project's base branch advanced; apply its new tip.
pub(crate) fn note_base_advance(orch: &Orchestrator, project_id: &str) {
    let (orch, project_id) = (orch.clone(), project_id.to_string());
    tokio::spawn(async move {
        apply_source(
            &orch,
            &project_id,
            &ServiceOwner::Project,
            "base advance",
            None,
        )
        .await;
    });
}

/// Runner start, at boot and independent of the startup catch-up: finalize
/// what a predecessor left live, resume every schedule whose rows exist, and
/// start every thread's `launch` services. A thread's services come from its
/// own store, which no repository catch-up touches, so nothing here waits on
/// one. Placement is the only precondition, and the colocated executor's
/// pending attach holds a lease request rather than refusing it.
///
/// Schedules resume before any source is applied, and each owner's source is
/// applied on its own task: one owner whose source is slow to read or whose
/// `launch` service is slow to place holds neither another owner's services
/// nor any cadence. Answers once every owner's pass has ended.
pub(crate) async fn start_at_boot(orch: &Orchestrator) {
    let db = orch.db.local.clone();
    if let Err(error) = rows::finalize_orphaned_runs(&db).await {
        tracing::warn!(%error, "could not finalize orphaned service runs");
    }
    resume_schedules(orch).await;
    let owners = match super::thread_store::owners(&db).await {
        Ok(owners) => owners,
        Err(error) => {
            tracing::warn!(%error, "could not list thread-owned services");
            return;
        }
    };
    let passes: Vec<_> = owners
        .into_iter()
        .map(|(project_id, owner)| {
            let orch = orch.clone();
            tokio::spawn(async move { apply_owner_at_boot(&orch, &project_id, &owner).await })
        })
        .collect();
    for pass in passes {
        let _ = pass.await;
    }
}

/// One thread owner's boot-time source pass, logged as it begins and with how
/// long it took, so a slow boot names the owner that held it.
async fn apply_owner_at_boot(orch: &Orchestrator, project_id: &str, owner: &ServiceOwner) {
    let thread = match owner {
        ServiceOwner::Thread { name, .. } => name.as_str(),
        ServiceOwner::Project | ServiceOwner::Pack { .. } => "",
    };
    tracing::info!(project_id, thread, "boot: applying a thread's services");
    let began = Instant::now();
    apply_source(orch, project_id, owner, "runner start", None).await;
    tracing::info!(
        project_id,
        thread,
        duration_ms = began.elapsed().as_millis() as u64,
        "boot: applied a thread's services"
    );
}

/// Start a schedule task for every service with schedule rows, whatever its
/// owner, so a cadence runs from boot even when its source cannot be read
/// yet. A project service's occurrences defer until the project pass.
async fn resume_schedules(orch: &Orchestrator) {
    for (project_id, _) in project_repositories(orch).await {
        let db = match crate::execution::routing::owning_db_for_project(&orch.db, &project_id).await
        {
            Ok(db) => db,
            Err(error) => {
                tracing::warn!(%error, project_id, "could not reach a project's service schedules");
                continue;
            }
        };
        let services = match schedule::scheduled_services(&db, &project_id).await {
            Ok(services) if services.is_empty() => continue,
            Ok(services) => services,
            Err(error) => {
                tracing::warn!(%error, project_id, "could not list scheduled services");
                continue;
            }
        };
        let key = match project_key(orch, &project_id).await {
            Ok(key) => key,
            Err(error) => {
                tracing::warn!(%error, project_id, "could not resolve a project key for services");
                continue;
            }
        };
        for (owner, service) in services {
            let cell = supervisor(orch).cell(&project_id, &key, &owner, &service);
            wake_schedule(orch, &cell);
        }
    }
}

/// Runner start, after the startup catch-up: start every project's `launch`
/// services from the base tip the runner has just brought current, and let
/// project schedules run. Once per runner; a second call does nothing.
pub(crate) async fn start_project_launch_services(orch: &Orchestrator) {
    if supervisor(orch)
        .projects_current
        .swap(true, Ordering::AcqRel)
    {
        return;
    }
    for (project_id, repo_path) in project_repositories(orch).await {
        // A project whose checkout was moved or deleted is an ordinary state,
        // not a fault to report on every boot; its services wait for it.
        if !tokio::fs::try_exists(&repo_path).await.unwrap_or(false) {
            tracing::debug!(
                project_id,
                repo_path,
                "project repository is missing; its services do not start"
            );
            continue;
        }
        apply_source(
            orch,
            &project_id,
            &ServiceOwner::Project,
            "runner start",
            None,
        )
        .await;
    }
    let deferred: Vec<Arc<Cell>> = supervisor(orch)
        .cells
        .lock()
        .unwrap()
        .values()
        .filter(|cell| cell.owner == ServiceOwner::Project)
        .cloned()
        .collect();
    for cell in deferred {
        wake_schedule(orch, &cell);
    }
}

/// Every project with a repository: (id, repository path).
async fn project_repositories(orch: &Orchestrator) -> Vec<(String, String)> {
    orch.db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, repo_path FROM projects
                         WHERE repo_path IS NOT NULL AND repo_path <> ''",
                        (),
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push((
                        crate::storage::RowExt::text(&row, 0)?,
                        crate::storage::RowExt::text(&row, 1)?,
                    ));
                }
                Ok(out)
            })
        })
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(%error, "could not list projects for services");
            Vec::new()
        })
}

/// One call to a service.
pub(crate) struct CallRequest {
    pub project_id: String,
    pub project_key: String,
    pub owner: ServiceOwner,
    pub service: String,
    pub method: String,
    pub params: Value,
    pub caller: CallerFrame,
    /// Shorter than the declared call timeout, never longer.
    pub timeout: Option<Duration>,
}

/// Call a service: start it if it is idle, pass the concurrency gate, mint
/// the call's delegated run, and wait for the answer or the deadline.
pub(crate) async fn call(orch: &Orchestrator, request: CallRequest) -> Result<Value, CallError> {
    call_recording(orch, request).await.result
}

/// A call's answer, and the URIs the service wrote while serving it.
pub(crate) struct CallOutcome {
    pub result: Result<Value, CallError>,
    pub writes: Vec<String>,
    /// Every `run` batch the service started for the call, by request id.
    pub batches: Vec<CallBatch>,
}

/// A `run` batch a call started: its request and the delegated run it ran
/// under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CallBatch {
    pub request: String,
    pub run: String,
    /// Why the batch never ran (a placement refusal names each machine and
    /// the edge or program it lacked), verbatim; `None` when it ran.
    pub refusal: Option<String>,
}

impl CallBatch {
    /// The batch as one clause of a call's log line.
    pub(crate) fn describe(&self) -> String {
        match &self.refusal {
            None => format!("ran batch {} under run {}", self.request, self.run),
            Some(refusal) => format!(
                "batch {} under run {} did not run: {refusal}",
                self.request, self.run
            ),
        }
    }
}

/// What a call touched, for its logs.
#[derive(Debug, Default)]
struct CallTouched {
    writes: Vec<String>,
    batches: Vec<CallBatch>,
}

/// [`call`], also reporting what the call wrote, for a caller that records
/// provenance of its own (a page's call log).
pub(crate) async fn call_recording(orch: &Orchestrator, request: CallRequest) -> CallOutcome {
    let task_orch = orch.clone();
    let outcome =
        detached(async move { Ok::<_, CallError>(call_attached(&task_orch, request).await) }).await;
    match outcome {
        Ok((result, touched)) => CallOutcome {
            result: result.map_err(|error| sanitized(orch, error)),
            writes: touched.writes,
            batches: touched.batches,
        },
        Err(error) => CallOutcome {
            result: Err(sanitized(orch, error)),
            writes: Vec::new(),
            batches: Vec::new(),
        },
    }
}

/// A service's error text is untrusted output headed for a page, an agent's
/// change report, and the call logs: redacted and capped like its values.
fn sanitized(orch: &Orchestrator, error: CallError) -> CallError {
    let policy = crate::security::redaction_policy(&orch.config_dir);
    let mut sanitizer = crate::security::Sanitizer::structural(&policy);
    let (mut code, mut message) = (error.code, error.message);
    sanitizer.text_in_place(&mut code);
    sanitizer.text_in_place(&mut message);
    code.truncate(64.min(code.len()));
    while !code.is_char_boundary(code.len()) {
        code.pop();
    }
    if message.len() > MAX_ERROR_BYTES {
        let mut end = MAX_ERROR_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        message.push_str("… (truncated)");
    }
    CallError { code, message }
}

async fn call_attached(
    orch: &Orchestrator,
    request: CallRequest,
) -> (Result<Value, CallError>, CallTouched) {
    match call_body(orch, request).await {
        Ok(done) => done,
        Err(error) => (Err(error), CallTouched::default()),
    }
}

async fn call_body(
    orch: &Orchestrator,
    request: CallRequest,
) -> Result<(Result<Value, CallError>, CallTouched), CallError> {
    let params_bytes = serde_json::to_vec(&request.params)
        .map(|bytes| bytes.len())
        .unwrap_or(0);
    if params_bytes > MAX_PARAMS_BYTES {
        return Err(CallError::new(
            "params_too_large",
            format!("params are {params_bytes} bytes; a call may carry at most {MAX_PARAMS_BYTES}"),
        ));
    }
    if matches!(request.owner, ServiceOwner::Pack { .. }) {
        return Err(CallError::new(
            "not_callable",
            super::MACHINE_SERVICE_REFUSAL,
        ));
    }
    let cell = supervisor(orch).cell(
        &request.project_id,
        &request.project_key,
        &request.owner,
        &request.service,
    );
    // Who may call is the owner's to decide, and it is decided before the
    // service is started for the call.
    if let Err(refusal) =
        authority::admit_caller(&request.owner, &request.project_key, &request.caller)
    {
        let generation = cell.state.lock().unwrap().generation;
        log(
            orch,
            &cell,
            generation,
            LogKind::Lifecycle,
            &format!(
                "refused `{}` from {} {}: {refusal}",
                request.method,
                request.caller.kind.as_str(),
                request.caller.acting_for
            ),
            json!({"event": "refused", "code": "not_owner"}),
        )
        .await;
        return Err(CallError::new("not_owner", refusal));
    }
    let _in_flight = InFlight::enter(&cell);
    let actor = request.caller.acting_for.clone();
    let started = Instant::now();

    let first = ensure_serving(orch, &cell, &actor).await?;
    let limits = first.declaration.limits;
    let max = Duration::from_secs(limits.call_timeout_seconds as u64);
    let timeout = request.timeout.map_or(max, |timeout| timeout.min(max));
    let deadline = Instant::now() + timeout;

    // Everything slow about a call happens before an incarnation is chosen, so
    // nothing stands between choosing the serving one and sending to it. The
    // delegation is minted for the first incarnation's audience; a retry that
    // lands on one whose lease was re-placed is minted a bearer for its own.
    let mut delegation = match delegate(
        orch,
        &cell,
        &request,
        first.generation,
        timeout,
        &first.callback_audience,
    )
    .await
    {
        Ok(delegation) => delegation,
        Err(error) => {
            let result = Err(error);
            let touched = CallTouched::default();
            record_call(orch, &cell, &first, &request, &result, &touched, started).await;
            return Ok((result, touched));
        }
    };

    let mut served_by = first;
    let mut result = Err(CallError::new(
        "service_unavailable",
        "no generation of the service could take the call",
    ));
    for attempt in 0..2 {
        let incarnation = match pick_incarnation(orch, &cell, &actor, attempt).await {
            Ok(incarnation) => incarnation,
            Err(error) => {
                result = Err(error);
                break;
            }
        };
        served_by = incarnation.clone();
        // A recovered lease moves the service to another machine, whose relay
        // refuses a bearer bound to the old one. The call's run is the same;
        // only where its credential may be presented changes, and every bearer
        // of the run dies with it at `end_run`.
        if incarnation.callback_audience != delegation.audience {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match mint_bearer(
                orch,
                &delegation.run_id,
                &cell.home(),
                Some(remaining + BEARER_GRACE),
                &incarnation.callback_audience,
            ) {
                Ok(bearer) => {
                    delegation.bearer = bearer;
                    delegation.audience = incarnation.callback_audience.clone();
                }
                Err(error) => {
                    result = Err(CallError::new("service_unavailable", error.message));
                    break;
                }
            }
        }
        let permit = match admit(&cell, &incarnation).await {
            Ok(permit) => permit,
            Err(error) => {
                result = Err(error);
                break;
            }
        };
        let outcome = send_call(&incarnation, &request, &delegation, deadline).await;
        drop(permit);
        // A generation that was being retired when the call reached it never
        // ran it: it refused (`draining`), was gone before the frame landed
        // (`service_unavailable`), or exited having drained without it
        // (`service_crashed`). Once, the call goes to the generation now
        // serving. A call a generation accepted is never sent twice.
        let retired_without_running = incarnation.retiring.load(Ordering::Acquire)
            && matches!(&outcome, Err(error) if matches!(
                error.code.as_str(),
                "draining" | "service_unavailable" | "service_crashed"
            ));
        result = outcome;
        if attempt > 0 || !retired_without_running {
            break;
        }
    }
    // The one point a call ends: its delegated credential dies with it, so no
    // new request can act for the call after this.
    end_run(orch, &cell.project_id, &delegation.run_id, "call ended").await;
    // A prompt the call would have raised fails the call, whatever the method
    // made of the refusal: nobody could have answered it, and its owner learns
    // of it through the failure streak rather than a method that swallowed it.
    if let Some(prompt) = orch
        .project_services
        .runs
        .take_prompt_refusal(&delegation.run_id)
    {
        result = Err(CallError::new("prompt_refused", prompt));
    }
    let touched = CallTouched {
        writes: orch.project_services.runs.take_writes(&delegation.run_id),
        batches: orch
            .project_services
            .runs
            .take_batches(&delegation.run_id)
            .into_iter()
            .map(|(request, refusal)| CallBatch {
                request,
                run: delegation.run_id.clone(),
                refusal,
            })
            .collect(),
    };
    record_call(
        orch, &cell, &served_by, &request, &result, &touched, started,
    )
    .await;
    Ok((result, touched))
}

/// A call counted against its cell's idle stop from admission to its end.
struct InFlight(Arc<Cell>);

impl InFlight {
    fn enter(cell: &Arc<Cell>) -> Self {
        cell.in_flight.fetch_add(1, Ordering::SeqCst);
        *cell.last_activity.lock().unwrap() = Instant::now();
        Self(cell.clone())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        // The clock first, so an idle check that sees no call in flight also
        // sees this one's end.
        *self.0.last_activity.lock().unwrap() = Instant::now();
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A call's delegated run, and the bearer the service acts through for it,
/// held as a lease until [`send_call`] writes it into a call frame.
struct Delegation {
    run_id: String,
    bearer: crate::mcp::run_bearer::RunBearer,
    audience: crate::security::LeaseAudience,
}

async fn delegate(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    request: &CallRequest,
    generation: u64,
    timeout: Duration,
    audience: &crate::security::LeaseAudience,
) -> Result<Delegation, CallError> {
    let db = crate::execution::routing::owning_db_for_project(&orch.db, &cell.project_id)
        .await
        .map_err(|error| CallError::new("service_unavailable", error.to_string()))?;
    let job_id = rows::ensure_service_job(&db, &cell.project_id, &cell.owner, &cell.service)
        .await
        .map_err(|error| CallError::new("service_unavailable", error))?;
    let run_id = rows::mint_run(&db, &cell.project_id, &job_id)
        .await
        .map_err(|error| CallError::new("service_unavailable", error))?;
    let home = cell.home();
    let bearer = match mint_bearer(orch, &run_id, &home, Some(timeout + BEARER_GRACE), audience) {
        Ok(bearer) => bearer,
        Err(error) => {
            end_run(orch, &cell.project_id, &run_id, "call ended").await;
            return Err(CallError::new("service_unavailable", error.message));
        }
    };
    orch.project_services.runs.insert(
        &run_id,
        ServiceRun {
            project_id: cell.project_id.clone(),
            project_key: cell.project_key.clone(),
            owner: cell.owner.clone(),
            service: cell.service.clone(),
            generation,
            default_branch: default_branch(orch, &cell.project_id).await,
            role: ServiceRunRole::Delegated {
                caller_kind: request.caller.kind,
                acting_for: request.caller.acting_for.clone(),
                scope: authority::delegation_for(&cell.owner, &request.caller),
            },
        },
    );
    Ok(Delegation {
        run_id,
        bearer,
        audience: audience.clone(),
    })
}

/// The incarnation an attempt goes to: the serving one.
async fn pick_incarnation(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    actor: &str,
    attempt: usize,
) -> Result<Arc<Incarnation>, CallError> {
    #[cfg(test)]
    if attempt == 0 {
        if let Some(forced) = cell.force_first_pick.lock().unwrap().take() {
            return Ok(forced);
        }
    }
    let _ = attempt;
    ensure_serving(orch, cell, actor).await
}

/// A place in the incarnation's concurrency gate, or `busy` when its queue is
/// full. The permit belongs to the incarnation that will run the call.
async fn admit(
    cell: &Cell,
    incarnation: &Arc<Incarnation>,
) -> Result<tokio::sync::OwnedSemaphorePermit, CallError> {
    let limits = incarnation.declaration.limits;
    if let Ok(permit) = incarnation.gate.clone().try_acquire_owned() {
        return Ok(permit);
    }
    if incarnation.waiting.load(Ordering::Acquire) >= limits.queue_depth() as usize {
        return Err(CallError::new(
            "busy",
            format!(
                "service `{}` has {} calls in flight and {} waiting",
                cell.service,
                limits.concurrency,
                limits.queue_depth()
            ),
        ));
    }
    incarnation.waiting.fetch_add(1, Ordering::AcqRel);
    let permit = incarnation.gate.clone().acquire_owned().await;
    incarnation.waiting.fetch_sub(1, Ordering::AcqRel);
    permit.map_err(|_| CallError::new("service_unavailable", "the service closed"))
}

async fn send_call(
    incarnation: &Arc<Incarnation>,
    request: &CallRequest,
    delegation: &Delegation,
    deadline: Instant,
) -> Result<Value, CallError> {
    let timeout = deadline.saturating_duration_since(Instant::now());
    if timeout.is_zero() {
        return Err(CallError::new(
            "deadline",
            "the call's deadline passed before it was sent",
        ));
    }
    // Presented as the frame that carries it is built, once per frame sent.
    let token =
        crate::mcp::run_bearer::service_credential(&delegation.bearer, &delegation.audience)
            .map_err(|error| CallError::new("service_unavailable", error))?;
    let run_id = delegation.run_id.clone();
    let id = format!("c-{}", uuid::Uuid::new_v4().simple());
    let (reply, answer) = oneshot::channel();
    incarnation
        .pending
        .lock()
        .unwrap()
        .insert(id.clone(), reply);
    let frame = RunnerFrame::Call {
        id: id.clone(),
        method: request.method.clone(),
        params: request.params.clone(),
        caller: request.caller.clone(),
        authority: Some(CallAuthority {
            run_id: run_id.clone(),
            token,
        }),
        deadline_ms: (now_ms() + timeout.as_millis() as i64).max(0) as u64,
    };
    let result = match incarnation.send(&frame).await {
        Err(error) => {
            incarnation.pending.lock().unwrap().remove(&id);
            Err(CallError::new(
                "service_unavailable",
                format!("could not reach the service: {error}"),
            ))
        }
        Ok(()) => match tokio::time::timeout(timeout, answer).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err(CallError::new(
                "service_crashed",
                "the service went away while the call was in flight",
            )),
            Err(_) => {
                incarnation.pending.lock().unwrap().remove(&id);
                let _ = incarnation
                    .send(&RunnerFrame::Cancel { id: id.clone() })
                    .await;
                Err(CallError::new(
                    "deadline",
                    format!("the call did not finish within {}s", timeout.as_secs()),
                ))
            }
        },
    };
    result
}

async fn record_call(
    orch: &Orchestrator,
    cell: &Arc<Cell>,
    incarnation: &Incarnation,
    request: &CallRequest,
    result: &Result<Value, CallError>,
    touched: &CallTouched,
    started: Instant,
) {
    let millis = started.elapsed().as_millis() as u64;
    let (outcome, code) = match result {
        Ok(_) => ("ok", None),
        Err(error) => ("error", Some(error.code.clone())),
    };
    let record = CallRecord {
        at_ms: now_ms(),
        method: request.method.clone(),
        caller_kind: request.caller.kind.as_str().to_string(),
        acting_for: request.caller.acting_for.clone(),
        outcome: code.clone().unwrap_or_else(|| outcome.to_string()),
        millis,
    };
    {
        let mut state = cell.state.lock().unwrap();
        state.recent.push_back(record);
        while state.recent.len() > RECENT_CALLS {
            state.recent.pop_front();
        }
    }
    let text = format!(
        "{} from {} {} → {}{} in {millis} ms",
        request.method,
        request.caller.kind.as_str(),
        request.caller.acting_for,
        outcome,
        code.as_deref()
            .map(|code| format!(" ({code})"))
            .unwrap_or_default()
    );
    log(
        orch,
        cell,
        incarnation.generation,
        LogKind::Call,
        &text,
        json!({
            "method": request.method,
            "callerKind": request.caller.kind.as_str(),
            "actingFor": request.caller.acting_for,
            "outcome": outcome,
            "code": code,
            "ms": millis,
            "writes": touched.writes,
            "batches": touched
                .batches
                .iter()
                .map(|batch| json!({"request": batch.request, "run": batch.run, "refusal": batch.refusal}))
                .collect::<Vec<_>>(),
        }),
    )
    .await;
}

#[cfg(test)]
#[path = "supervisor_tests.rs"]
pub(crate) mod tests;

pub(crate) mod machine;
