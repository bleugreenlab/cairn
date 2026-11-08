use crate::fleet::placement::PlacementRefusal;
use crate::fleet::unix_time_ms;
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::{
    CellPriority, ExecutorSelector, OwnerDeathPolicy, ProcessSandboxMode, RepositoryLocator,
    ResidencyAcquireRequest, ResidencyFailureKind, ResidencyFence, ResidencyFootprint,
    ResidencyHolder, ResidencyOperation, ResidencyResult, ResidentProcessCwdRoot,
    ResidentProcessEvent, ResidentProcessEventKind, ResidentProcessIoMode, ResidentProcessKind,
    ResidentProcessSpec, ResidentProcessStatus, ResidentProcessStream, ResidentRuntimeAsset,
    ResidentStreamFraming,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};

const ONE_SHOT_KEY: &str = "service-one-shot";
/// What a one-shot calls itself while it is live. It is short and generic on
/// purpose: the lease runs whatever command the service asks of it, so the only
/// honest thing this process can claim is that it is one.
pub(super) const ONE_SHOT_ROLE: &str = "command";
const ACQUIRE_WAIT: Duration = Duration::from_secs(30);
const RENEW_INTERVAL: Duration = Duration::from_secs(30);
/// How often a lease waiting on its machine checks whether the machine has
/// outlasted the lease grace. Only an outage's first report depends on it.
const LOSS_CHECK_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(20)
} else {
    Duration::from_secs(15)
};

#[derive(Debug, Error)]
pub(crate) enum ServicePlacementError {
    #[error("invalid service placement request: {0}")]
    InvalidRequest(String),
    #[error("service residency is unavailable: {0}")]
    Unavailable(String),
    #[error("service process operation failed: {0}")]
    Operation(String),
    /// The runner could not route the operation to the executor holding the
    /// residency. Unlike `Operation`, it says nothing about the process the
    /// operation was for.
    #[error("service process could not be routed to its executor: {0}")]
    Unroutable(String),
    /// The fleet would not grant the residency for a reason about the fleet:
    /// no executor attached, none satisfying the selector, or none with room.
    /// Like `Unroutable`, it says nothing about the service it was for.
    #[error("service residency was not granted: {0}")]
    Ungranted(String),
    #[error("service process `{process_key}` did not exit within {timeout:?}")]
    Timeout {
        process_key: String,
        timeout: Duration,
    },
    #[error("service process event stream closed before `{process_key}` exited")]
    EventStreamClosed { process_key: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OneShotOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServicePlacementHealth {
    Ready,
    ExecutorOffline,
    ProcessDown {
        process_key: String,
        exit_code: Option<i32>,
    },
}

pub(crate) struct ResidentSubscription {
    process_generation: u64,
    events: mpsc::UnboundedReceiver<ResidentProcessEvent>,
}

/// What a placed process is started as: the command, and the environment the
/// executor builds around it.
///
/// [`ResidentLaunch::command`] is the shape every helper process a lease ran
/// before project services had: scratch cwd, no environment, no assets, and
/// unconfined, because those helpers are Cairn's own binaries. A project
/// service is the one caller that sets the rest, and it always asks for
/// [`ProcessSandboxMode::Confined`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResidentLaunch {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub runtime_assets: Vec<ResidentRuntimeAsset>,
    pub runtime_packages: Vec<ResidentRuntimeAsset>,
    pub sandbox_mode: ProcessSandboxMode,
    pub cwd_root: ResidentProcessCwdRoot,
    /// The relay a process on another machine calls this runner through.
    pub relay: Option<cairn_common::executor_protocol::ResidentRelayGrant>,
    /// `Lines` only for a process whose stdout is a newline-delimited JSON
    /// protocol; see [`ResidentStreamFraming`].
    pub stdout_framing: ResidentStreamFraming,
}

impl ResidentLaunch {
    pub(crate) fn command(program: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            program: program.into(),
            args,
            env: Vec::new(),
            runtime_assets: Vec::new(),
            runtime_packages: Vec::new(),
            sandbox_mode: ProcessSandboxMode::Unconfined,
            cwd_root: ResidentProcessCwdRoot::ResidencyScratch,
            relay: None,
            stdout_framing: ResidentStreamFraming::Bytes,
        }
    }

    pub(crate) fn spec(&self) -> ResidentProcessSpec {
        ResidentProcessSpec {
            stdout_framing: self.stdout_framing,
            runtime_packages: self.runtime_packages.clone(),
            program: self.program.clone(),
            args: self.args.clone(),
            launch_command: None,
            cwd: String::new(),
            cwd_root: self.cwd_root,
            env: self.env.clone(),
            sandbox_mode: self.sandbox_mode,
            sandbox_policy: None,
            runtime_assets: self.runtime_assets.clone(),
            io: ResidentProcessIoMode::Pipe,
            node_scratch: None,
            relay: self.relay.clone(),
        }
    }
}

/// Where one process key's events go, and which start they belong to.
///
/// Generation zero means the start is still in flight: the executor has not
/// named the generation yet, so everything at that key is forwarded and
/// [`ResidentSubscription`] drops whatever turns out not to be this start's.
/// A process that exits before its own start call returns is the case this
/// exists for.
struct ProcessRoute {
    generation: u64,
    events: mpsc::UnboundedSender<ResidentProcessEvent>,
}

type ProcessRoutes = Arc<StdMutex<HashMap<String, ProcessRoute>>>;

impl ResidentSubscription {
    /// The next event of this start, or `None` once its stream is closed —
    /// including when the lease reacquires its residency, because a process
    /// started under the old fence is not running under the new one.
    pub(crate) async fn recv(&mut self) -> Option<ResidentProcessEvent> {
        while let Some(event) = self.events.recv().await {
            if event.process_generation == self.process_generation {
                return Some(event);
            }
        }
        None
    }
}

/// Who a placed service is: the stable id its residency and storage keys are
/// built from, and the words a person reads when one of its processes shows up
/// in a running list.
///
/// These are two different things and the pair keeps them from being confused
/// for one another. `id` is addressing; `label` is identity. A panel handed the
/// id has nothing to call the work but "ambient" (CAIRN-3435).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ServiceIdentity<'a> {
    pub id: &'a str,
    pub label: &'a str,
}

/// Everything a placed service residency is, minus the right to keep it alive.
///
/// Renewal holds this and never the handle below, and the split is not
/// bookkeeping. Recovery legitimately waits forever for a machine that may never
/// come back, so a renewal task that held the lease across that wait would be
/// its last owner for the whole outage — precisely the window in which an
/// abandoned lease must stop contending for its holder rather than keep
/// reacquiring on behalf of a caller that is gone.
pub(crate) struct LeaseCore {
    orch: Orchestrator,
    fence: Arc<RwLock<ResidencyFence>>,
    acquire_request: ResidencyAcquireRequest,
    /// The selector names a machine (`name`) or only a kind of machine. A
    /// pinned lease waits out an outage for that one machine; an unpinned one
    /// is re-placed by the fleet and may come back somewhere else.
    pinned_executor: Option<String>,
    /// Where the residency is placed now. Unpinned leases learn this after
    /// each acquisition, so it is state rather than configuration.
    executor_name: StdMutex<String>,
    service_label: String,
    one_shot: Mutex<()>,
    recovery: Mutex<()>,
    released: AtomicBool,
    release_notify: tokio::sync::Notify,
    health: Arc<StdMutex<ServicePlacementHealth>>,
    routes: ProcessRoutes,
    /// The executor last reported lost under this lease.
    machine_lost: tokio::sync::watch::Sender<Option<String>>,
    /// Whether the current outage has been reported, so a machine that stays
    /// away is one report however long recovery takes.
    loss_reported: AtomicBool,
    /// Keeps [`subscribe_lease_events`] registered for exactly the lease's life.
    _events: super::ResidentProcessSubscription,
}

/// An owner's handle on a placed service residency.
///
/// Lifetime is the whole meaning of this type. Dropping the last handle ends the
/// residency's renewal; every other capability is reached through [`LeaseCore`].
pub(crate) struct ServiceLease {
    core: Arc<LeaseCore>,
}

impl Drop for ServiceLease {
    /// Raise the same signal an explicit release raises.
    ///
    /// A lease nobody holds must stop renewing, and a renewal task parked in
    /// recovery has to be woken to notice — [`wait_for_recovery_or_release`]
    /// already races that park against this notification, so an orphaned lease
    /// and a released one leave by one path rather than two.
    ///
    /// What `Drop` cannot do is hand the cell back, because that is an await.
    /// The executor reclaims it under the death policy the lease was acquired
    /// with, which is the mechanism that already covers a runner exiting without
    /// releasing.
    fn drop(&mut self) {
        self.core.released.store(true, Ordering::Release);
        self.core.release_notify.notify_waiters();
    }
}

impl std::ops::Deref for ServiceLease {
    type Target = LeaseCore;

    fn deref(&self) -> &LeaseCore {
        &self.core
    }
}

/// The selector for a service pinned to one machine by its public name, which
/// is how every helper service that predates project services is placed.
pub(crate) fn named_executor(executor_name: &str) -> ExecutorSelector {
    ExecutorSelector {
        name: Some(executor_name.to_string()),
        ..ExecutorSelector::default()
    }
}

pub(crate) async fn acquire_service_lease(
    orch: &Orchestrator,
    service: ServiceIdentity<'_>,
    selector: ExecutorSelector,
    footprint: ResidencyFootprint,
    death_policy: OwnerDeathPolicy,
) -> Result<ServiceLease, ServicePlacementError> {
    validate_service_identity(service)?;
    // An empty selector lets the fleet choose; anything else must be a
    // selector the protocol accepts.
    if !selector.is_empty() {
        selector
            .validate()
            .map_err(ServicePlacementError::InvalidRequest)?;
    }
    let pinned_executor = selector.name.clone();

    let acquire_request = service_acquire_request(service.id, selector, footprint, death_policy);
    let fence = super::residency::acquire_quietly(orch, acquire_request.clone())
        .await
        .map_err(acquire_refusal)?;
    let executor_name = placed_executor_name(orch, &fence, pinned_executor.as_deref());

    let fence = Arc::new(RwLock::new(fence));
    let routes = ProcessRoutes::default();
    let events = subscribe_lease_events(orch, &fence, &routes);
    Ok(ServiceLease {
        core: Arc::new(LeaseCore {
            orch: orch.clone(),
            fence,
            acquire_request,
            pinned_executor,
            executor_name: StdMutex::new(executor_name),
            service_label: service.label.trim().to_string(),
            one_shot: Mutex::new(()),
            recovery: Mutex::new(()),
            released: AtomicBool::new(false),
            release_notify: tokio::sync::Notify::new(),
            health: Arc::new(StdMutex::new(ServicePlacementHealth::Ready)),
            routes,
            machine_lost: tokio::sync::watch::channel(None).0,
            loss_reported: AtomicBool::new(false),
            _events: events,
        }),
    })
}

/// The machine a residency landed on, as a person would name it. A pinned
/// lease already knows; an unpinned one asks the fleet's residency route.
fn placed_executor_name(
    orch: &Orchestrator,
    fence: &ResidencyFence,
    pinned: Option<&str>,
) -> String {
    pinned
        .map(str::to_string)
        .or_else(|| super::residency::residency_machine_name(orch, &fence.holder))
        .unwrap_or_else(|| "an unnamed executor".to_string())
}

/// Install the lease's single subscription on the fleet's resident-process
/// event stream.
///
/// One per lease, not one per process start: every resident event on the
/// machine is dispatched through every subscriber, and a service's one-shots
/// (a health check a minute is fourteen hundred a day) each want only the
/// routing the lease already does. The lease holds the returned subscription,
/// so it unregisters when the lease's core is dropped.
///
/// The fence is read through the lease's own handle rather than copied, because
/// a lease that reacquired after an executor bounce holds a different fence than
/// the one it started with. A captured copy would match nothing afterwards, and
/// a resident watch fed by it would park in silence rather than fail. The
/// handles are weak because the lease's core owns the subscription, so a strong
/// one would be a cycle.
fn subscribe_lease_events(
    orch: &Orchestrator,
    fence: &Arc<RwLock<ResidencyFence>>,
    routes: &ProcessRoutes,
) -> super::ResidentProcessSubscription {
    let fence = Arc::downgrade(fence);
    let routes = Arc::downgrade(routes);
    orch.fleet.subscribe_resident_process_events(move |event| {
        let (Some(fence), Some(routes)) = (fence.upgrade(), routes.upgrade()) else {
            return;
        };
        if !event_matches(&event, &fence.read().unwrap()) {
            return;
        }
        let routes = routes.lock().unwrap();
        let Some(route) = routes.get(&event.process_key) else {
            return;
        };
        if route.generation != 0 && route.generation != event.process_generation {
            return;
        }
        let _ = route.events.send(event);
    })
}

pub(super) fn service_acquire_request(
    service_id: &str,
    selector: ExecutorSelector,
    footprint: ResidencyFootprint,
    death_policy: OwnerDeathPolicy,
) -> ResidencyAcquireRequest {
    let now = unix_time_ms();
    ResidencyAcquireRequest {
        holder: ResidencyHolder::Service {
            service_id: service_id.to_string(),
        },
        repository: RepositoryLocator::ScratchOnly {
            owner_id: service_id.to_string(),
        },
        executor: (!selector.is_empty()).then_some(selector),
        owner_ref: None,
        selector: None,
        initial_base_commit: String::new(),
        footprint,
        death_policy,
        priority: CellPriority::AgentInteractive,
        wait_horizon_unix_ms: now.saturating_add(ACQUIRE_WAIT.as_millis() as u64),
        waiting_since_unix_ms: now,
    }
}

impl ServiceLease {
    /// Keep this lease's residency alive for as long as an owner holds it.
    ///
    /// The task is handed the core and never a handle, so it cannot become the
    /// lease's last owner. A renewal loop that cloned the lease kept it alive
    /// forever, and because a service holder is a single name, an orphan did not
    /// merely linger: it recovered by reacquiring a holder someone else now
    /// owned, whose own loop recovered by reacquiring in turn. Orphans
    /// accumulated into a herd that filled the executor's admission queue with
    /// `AgentInteractive` residency traffic which never settled (CAIRN-4205).
    ///
    /// Dropping the last handle therefore stops renewal from two directions at
    /// once: the `released` flag this loop checks before every step, and the
    /// notification that wakes it out of an otherwise unbounded recovery wait.
    pub(crate) fn spawn_renewal(&self) {
        let core = Arc::clone(&self.core);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RENEW_INTERVAL);
            interval.tick().await;
            loop {
                interval.tick().await;
                if core.released.load(Ordering::Acquire) {
                    return;
                }
                let fence = core.current_fence();
                let machine = core.executor_id();
                let renewal = super::residency::renew(&core.orch, &fence);
                let Err(error) = core.reporting_loss(machine.as_deref(), renewal).await else {
                    continue;
                };
                tracing::warn!(%error, holder = %fence.holder, "service residency renewal failed; waiting to reacquire");
                *core.health.lock().unwrap() = ServicePlacementHealth::ExecutorOffline;
                loop {
                    if core.released.load(Ordering::Acquire) {
                        return;
                    }
                    let Err(error) = core.recover().await else {
                        break;
                    };
                    if core.released.load(Ordering::Acquire) {
                        return;
                    }
                    tracing::warn!(%error, executor = %core.executor_name(), "service residency reacquisition failed");
                    tokio::time::sleep(RENEW_INTERVAL).await;
                }
            }
        });
    }
}

impl LeaseCore {
    pub(crate) fn executor_name(&self) -> String {
        self.executor_name.lock().unwrap().clone()
    }

    /// The executor the residency is on now, by id, as the fleet routes it.
    pub(crate) fn executor_id(&self) -> Option<String> {
        self.orch
            .fleet
            .residency_route_executor(&self.current_fence().holder)
    }

    pub(crate) fn health(&self) -> ServicePlacementHealth {
        self.health.lock().unwrap().clone()
    }

    pub(crate) fn mark_process_down(&self, process_key: &str, exit_code: Option<i32>) {
        *self.health.lock().unwrap() = ServicePlacementHealth::ProcessDown {
            process_key: process_key.to_string(),
            exit_code,
        };
    }

    fn current_fence(&self) -> ResidencyFence {
        self.fence.read().unwrap().clone()
    }

    /// Told the executor each time one is reported lost under this lease: the
    /// runner's own judgement, past the lease grace, that a machine has stopped
    /// answering. Nothing on the machine can say this, least of all when it is
    /// powered off. A receiver hears only reports made after it subscribed.
    pub(crate) fn machine_lost_signal(&self) -> tokio::sync::watch::Receiver<Option<String>> {
        self.machine_lost.subscribe()
    }

    pub(crate) fn report_machine_lost(&self, executor_id: &str) {
        if !self.loss_reported.swap(true, Ordering::AcqRel) {
            self.machine_lost
                .send_replace(Some(executor_id.to_string()));
        }
    }

    /// Run `work`, reporting `machine` lost if it outlasts the grace meanwhile.
    /// A lease parked for a named machine can wait out a whole outage here, and
    /// nothing else would speak for it until the machine came back.
    async fn reporting_loss<T>(
        &self,
        machine: Option<&str>,
        work: impl std::future::Future<Output = T>,
    ) -> T {
        let Some(machine) = machine else {
            return work.await;
        };
        tokio::pin!(work);
        let mut check = tokio::time::interval(LOSS_CHECK_INTERVAL);
        loop {
            tokio::select! {
                outcome = &mut work => return outcome,
                _ = check.tick() => {
                    if self.orch.fleet.machine_lost_past_grace(machine) {
                        self.report_machine_lost(machine);
                    }
                }
            }
        }
    }

    async fn recover(&self) -> Result<ResidencyFence, ServicePlacementError> {
        let _recovery = self.recovery.lock().await;
        if self.released.load(Ordering::Acquire) {
            return Err(ServicePlacementError::Operation(
                "service residency has been released".into(),
            ));
        }
        if matches!(self.health(), ServicePlacementHealth::Ready) {
            return Ok(self.current_fence());
        }
        // Only a pinned lease has one machine worth waiting for. An unpinned
        // one asks the fleet again at once, and the renewal loop's interval
        // paces the retries while no machine fits.
        let machine = self.executor_id();
        if let Some(pinned) = self.pinned_executor.as_deref() {
            let ready = wait_for_recovery_or_release(
                self.orch.fleet.wait_for_named_executor(pinned),
                &self.released,
                &self.release_notify,
            );
            if !self.reporting_loss(machine.as_deref(), ready).await {
                return Err(ServicePlacementError::Operation(
                    "service residency has been released".into(),
                ));
            }
        }
        let mut request = self.acquire_request.clone();
        let now = unix_time_ms();
        request.waiting_since_unix_ms = now;
        request.wait_horizon_unix_ms = now.saturating_add(ACQUIRE_WAIT.as_millis() as u64);
        let acquired = super::residency::acquire(&self.orch, request).await;
        // An eviction is the fleet acting on the same grace, and may beat the
        // periodic check to it. Reported whether or not a machine was then
        // found, and before the routes drop below, so the owner hears about the
        // loss ahead of the process's end.
        if let (Some(machine), Some(_)) = (
            machine.as_deref(),
            self.orch
                .fleet
                .take_environment_eviction(&self.current_fence().holder),
        ) {
            self.report_machine_lost(machine);
        }
        let fence = acquired.map_err(acquire_refusal)?;
        if self.released.load(Ordering::Acquire) {
            let _ = super::residency::release(&self.orch, &fence).await;
            return Err(ServicePlacementError::Operation(
                "service residency was released during reacquisition".into(),
            ));
        }
        *self.executor_name.lock().unwrap() =
            placed_executor_name(&self.orch, &fence, self.pinned_executor.as_deref());
        *self.fence.write().unwrap() = fence.clone();
        // Nothing routed under the old fence survives it. A readmitted cell
        // keeps only its processes' executor-lost exits, which reach their
        // subscribers whenever a later snapshot happens to re-send them; a
        // fresh cell has a new incarnation no old event will ever match, so a
        // subscriber left routed would park forever. Dropping the routes ends
        // every subscription now, and its owner restarts on the new fence. A
        // start that triggered this recovery routes its process only after
        // recovery returns, so it is not swept here.
        drop(std::mem::take(&mut *self.routes.lock().unwrap()));
        *self.health.lock().unwrap() = ServicePlacementHealth::Ready;
        self.loss_reported.store(false, Ordering::Release);
        Ok(fence)
    }

    pub(crate) async fn release(&self) -> Result<(), ServicePlacementError> {
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_waiters();
        let _recovery = self.recovery.lock().await;
        super::residency::release(&self.orch, &self.current_fence())
            .await
            .map_err(ServicePlacementError::Operation)
    }

    pub(crate) async fn run_one_shot(
        &self,
        program: &str,
        args: Vec<String>,
        timeout: Duration,
    ) -> Result<OneShotOutput, ServicePlacementError> {
        let _single_flight = self.one_shot.lock().await;
        // The bound covers placement as well as output. `start_process` can park
        // indefinitely waiting for an executor that never returns, and it does so
        // holding the single-flight slot — one wedged command would otherwise
        // take every later command on this lease down with it, silently and
        // permanently.
        let attempt = async {
            let mut subscription = self
                .start_process(
                    ONE_SHOT_KEY,
                    ONE_SHOT_ROLE,
                    &ResidentLaunch::command(program, args),
                )
                .await?;
            let generation = subscription.process_generation;
            let mut output = OneShotOutput {
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit_code: None,
            };
            while let Some(event) = subscription.recv().await {
                debug_assert_eq!(event.process_generation, generation);
                match event.event {
                    ResidentProcessEventKind::Output { stream, data, .. } => match stream {
                        ResidentProcessStream::Stdout | ResidentProcessStream::Pty => {
                            output.stdout.extend(data)
                        }
                        ResidentProcessStream::Stderr => output.stderr.extend(data),
                    },
                    ResidentProcessEventKind::State {
                        status:
                            ResidentProcessStatus::Exited {
                                exit_code,
                                executor_lost,
                                ..
                            },
                    } => {
                        if executor_lost {
                            *self.health.lock().unwrap() = ServicePlacementHealth::ExecutorOffline;
                            return Err(ServicePlacementError::Unavailable(format!(
                                "executor {} disconnected while `{program}` was running",
                                self.executor_name()
                            )));
                        }
                        output.exit_code = exit_code;
                        return Ok(output);
                    }
                    ResidentProcessEventKind::State { .. } => {}
                }
            }
            Err(ServicePlacementError::EventStreamClosed {
                process_key: ONE_SHOT_KEY.into(),
            })
        };

        match tokio::time::timeout(timeout, attempt).await {
            Ok(result) => result,
            Err(_) => {
                // Bounded too: a stop that talks to the same unreachable executor
                // must not reintroduce the hang the timeout just escaped.
                let _ = tokio::time::timeout(timeout, self.stop_resident(ONE_SHOT_KEY)).await;
                Err(ServicePlacementError::Timeout {
                    process_key: ONE_SHOT_KEY.into(),
                    timeout,
                })
            }
        }
    }

    /// Starts a long-lived process under this lease. `role` is what the process
    /// does in words — "watch" — which is what a running list shows; `key` is
    /// the lease-internal handle it is stopped and restarted by.
    pub(crate) async fn start_resident(
        &self,
        key: &str,
        role: &str,
        launch: &ResidentLaunch,
    ) -> Result<ResidentSubscription, ServicePlacementError> {
        validate_resident_process(key, role)?;
        self.start_process(key, role, launch).await
    }

    async fn start_process(
        &self,
        key: &str,
        role: &str,
        launch: &ResidentLaunch,
    ) -> Result<ResidentSubscription, ServicePlacementError> {
        if launch.program.is_empty() {
            return Err(ServicePlacementError::InvalidRequest(
                "process program must not be empty".into(),
            ));
        }

        if !matches!(self.health(), ServicePlacementHealth::Ready) {
            self.recover().await?;
        }
        let fence = self.current_fence();
        let process_key = key.to_string();
        let (sender, receiver) = mpsc::unbounded_channel();
        // Routed before the start is asked for, so a process that exits before
        // the executor answers still has its output and its exit waiting in the
        // subscription rather than dropped on the floor.
        self.routes.lock().unwrap().insert(
            process_key.clone(),
            ProcessRoute {
                generation: 0,
                events: sender,
            },
        );

        let result = self
            .orch
            .fleet
            .operate_residency(
                &self.orch,
                ResidencyOperation::StartProcess {
                    fence: fence.clone(),
                    process_key: process_key.clone(),
                    kind: ResidentProcessKind::Service {
                        name: self.service_label.clone(),
                        role: role.trim().to_string(),
                    },
                    reservation: None,
                    process: launch.spec(),
                },
            )
            .await;

        let generation = match process_generation(&result, &process_key) {
            Ok(generation) => generation,
            Err(error) => {
                self.routes.lock().unwrap().remove(&process_key);
                return Err(error);
            }
        };
        if let Some(route) = self.routes.lock().unwrap().get_mut(&process_key) {
            route.generation = generation;
        }
        *self.health.lock().unwrap() = ServicePlacementHealth::Ready;
        Ok(ResidentSubscription {
            process_generation: generation,
            events: receiver,
        })
    }

    pub(crate) async fn write_input(
        &self,
        key: &str,
        data: &[u8],
    ) -> Result<(), ServicePlacementError> {
        let generation = self
            .routes
            .lock()
            .unwrap()
            .get(key)
            .map(|route| route.generation)
            .filter(|generation| *generation != 0)
            .ok_or_else(|| {
                ServicePlacementError::Operation(format!(
                    "process `{key}` is not running on {}",
                    self.executor_name()
                ))
            })?;
        let result = self
            .orch
            .fleet
            .operate_residency(
                &self.orch,
                ResidencyOperation::WriteProcessInput {
                    fence: self.current_fence(),
                    process_key: key.to_string(),
                    process_generation: generation,
                    data: data.to_vec(),
                },
            )
            .await;
        match result {
            ResidencyResult::State { .. } => Ok(()),
            ResidencyResult::Failed {
                kind: ResidencyFailureKind::StaleEpoch,
                diagnostic,
                ..
            } => {
                self.mark_process_down(key, None);
                Err(ServicePlacementError::Operation(format!(
                    "process `{key}` on {} changed generation: {diagnostic}",
                    self.executor_name()
                )))
            }
            ResidencyResult::Failed { diagnostic, .. } => {
                Err(ServicePlacementError::Operation(format!(
                    "could not write to process `{key}` on {}: {diagnostic}",
                    self.executor_name()
                )))
            }
            other => Err(ServicePlacementError::Operation(format!(
                "unexpected input-write result for process `{key}` on {}: {other:?}",
                self.executor_name()
            ))),
        }
    }

    pub(crate) async fn stop_resident(&self, key: &str) -> Result<(), ServicePlacementError> {
        super::residency::stop(&self.orch, &self.current_fence(), key)
            .await
            .map_err(ServicePlacementError::Operation)
    }
}

/// What a lease refuses before it places anything.
///
/// A service that cannot name itself places processes nobody can attribute, and
/// the panel's only recourse is to call the work ambient (CAIRN-3435). Refusing
/// at the declaration fails loudly — the service does not start, and says why —
/// where the alternative fails silently on an operator's screen.
fn validate_service_identity(service: ServiceIdentity<'_>) -> Result<(), ServicePlacementError> {
    if service.id.trim().is_empty() {
        return Err(ServicePlacementError::InvalidRequest(
            "service id must not be empty".into(),
        ));
    }
    if service.label.trim().is_empty() {
        return Err(ServicePlacementError::InvalidRequest(format!(
            "service `{}` must declare what a person calls it",
            service.id
        )));
    }
    Ok(())
}

/// The same rule one level down: a key addresses a process, a role names it,
/// and a resident needs both.
fn validate_resident_process(key: &str, role: &str) -> Result<(), ServicePlacementError> {
    if key.is_empty() || key == ONE_SHOT_KEY {
        return Err(ServicePlacementError::InvalidRequest(format!(
            "resident process key `{key}` is reserved or empty"
        )));
    }
    if role.trim().is_empty() {
        return Err(ServicePlacementError::InvalidRequest(format!(
            "resident process `{key}` must declare what it does"
        )));
    }
    Ok(())
}

async fn wait_for_recovery_or_release(
    executor_ready: impl std::future::Future<Output = ()>,
    released: &AtomicBool,
    release_notify: &tokio::sync::Notify,
) -> bool {
    let release = release_notify.notified();
    tokio::pin!(release);
    if released.load(Ordering::Acquire) {
        return false;
    }
    tokio::select! {
        () = executor_ready => !released.load(Ordering::Acquire),
        () = &mut release => false,
    }
}

fn process_generation(
    result: &ResidencyResult,
    process_key: &str,
) -> Result<u64, ServicePlacementError> {
    match result {
        ResidencyResult::State { cell } => cell
            .occupancy
            .processes
            .get(process_key)
            .map(|process| process.generation)
            .ok_or_else(|| {
                ServicePlacementError::Operation(format!(
                    "executor returned no state for process `{process_key}`"
                ))
            }),
        ResidencyResult::Failed {
            kind, diagnostic, ..
        } => match kind {
            ResidencyFailureKind::Admission | ResidencyFailureKind::NotFound => {
                Err(ServicePlacementError::Unavailable(diagnostic.clone()))
            }
            // The substrate's refusal, not the process's: the runner could not
            // route the start to the environment's cell (its machine is not
            // answering, no longer reports it, has not been granted it, or no
            // route exists), and its diagnostic names which. A service
            // schedule retries a start refused this way instead of counting it
            // as the service's failure.
            ResidencyFailureKind::Unavailable => {
                Err(ServicePlacementError::Unroutable(diagnostic.clone()))
            }
            _ => Err(ServicePlacementError::Operation(diagnostic.clone())),
        },
        other => Err(ServicePlacementError::Operation(format!(
            "unexpected start result: {other:?}"
        ))),
    }
}

/// A refused acquisition as the error a lease reports, decided on the
/// executor's typed kind. A refusal made before any process exists judges the
/// service only when it is the service's declaration being refused; the
/// fleet's own refusals are `Ungranted`, so a service schedule retries them
/// as it retries an `Unroutable` start. cairn/5754 refines `Unavailable` into
/// a snapshot lagging its grant versus a route whose executor is gone; key the
/// substrate's refusals on the kinds it lands.
fn acquire_refusal(refusal: PlacementRefusal) -> ServicePlacementError {
    let PlacementRefusal {
        kind, diagnostic, ..
    } = refusal;
    match kind {
        Some(
            ResidencyFailureKind::Unavailable
            | ResidencyFailureKind::Admission
            | ResidencyFailureKind::NotFound,
        ) => ServicePlacementError::Ungranted(diagnostic),
        Some(
            ResidencyFailureKind::InvalidDeclaration
            | ResidencyFailureKind::ConflictingDeclaration
            | ResidencyFailureKind::StaleEpoch
            | ResidencyFailureKind::InvalidState
            | ResidencyFailureKind::Process
            | ResidencyFailureKind::Cleanup
            | ResidencyFailureKind::Persistence,
        )
        | None => ServicePlacementError::Unavailable(diagnostic),
    }
}

fn event_matches(event: &ResidentProcessEvent, fence: &ResidencyFence) -> bool {
    event.holder == fence.holder
        && event.incarnation_id == fence.incarnation_id
        && event.cell_epoch == fence.cell_epoch
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use cairn_common::executor_protocol::{
        DeclineReason, ExecutorMessage, OfferSummary, ResidencyRuntimeConfig, LOCAL_EXECUTOR_NAME,
    };
    use cairn_exec::blocking::offload;
    use cairn_exec::{ExecutorRuntime, Fleet as ExecutorPool};
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;

    /// An orchestrator with a real executor runtime attached, standing in for
    /// the WebSocket link with the message pump the link would provide.
    ///
    /// Both callbacks are wired exactly as production wires them, so a one-shot
    /// run through this orchestrator meets the real executor's process
    /// supervision and the real runner's event admission. The pump handles each
    /// frame's synchronous pool work on the blocking pool, awaited in arrival
    /// order, as the executor's link-control worker does, so the executor's
    /// blocking guard sees what it sees in production. What it does not
    /// reproduce is the skew between an executor's two streams: in process,
    /// every callback is synchronous and in order.
    pub(crate) async fn attached_orchestrator() -> (Orchestrator, tempfile::TempDir) {
        let (orch, config, _link) = attached_orchestrator_with_link().await;
        (orch, config)
    }

    /// The same orchestrator, plus the connection generation its colocated link
    /// attached at — the fence `disconnect_advertised_executor` matches on, and
    /// therefore what a test needs to take the machine away.
    pub(crate) async fn attached_orchestrator_with_link() -> (Orchestrator, tempfile::TempDir, u64)
    {
        let config = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(config.path().join("cairn.turso.db"))
            .await
            .unwrap();
        let index = SearchIndex::open_or_create(config.path().join("search-index.db")).unwrap();
        let db_state = Arc::new(DbState::new(Arc::new(db), Arc::new(index)));
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = Orchestrator::builder(db_state, services, config.path().to_path_buf()).build();
        let link = attach_executor_runtime(&orch, config.path().join("executor-home"));
        (orch, config, link)
    }

    fn attach_executor_runtime(orch: &Orchestrator, home: PathBuf) -> u64 {
        const EXECUTOR_ID: &str = super::super::COLOCATED_EXECUTOR_ID;
        let fleet = orch.fleet.clone();
        let generation = Arc::new(AtomicU64::new(0));
        let (snapshot_fleet, snapshot_generation) = (fleet.clone(), generation.clone());
        let (event_fleet, event_generation) = (fleet.clone(), generation.clone());
        let runtime = ExecutorRuntime::new(home)
            .with_snapshot_callback(move |_reason, snapshot, health| {
                snapshot_fleet.handle_executor_message(
                    EXECUTOR_ID,
                    snapshot_generation.load(Ordering::Acquire),
                    ExecutorMessage::SnapshotUpdated { snapshot, health },
                );
            })
            .with_resident_process_event_callback(move |event| {
                let (fleet, generation) = (event_fleet.clone(), event_generation.clone());
                Box::pin(async move {
                    fleet.handle_executor_message(
                        EXECUTOR_ID,
                        generation.load(Ordering::Acquire),
                        ExecutorMessage::ResidentProcessEvent { event },
                    );
                })
            })
            // Every placed service is started relayed, and an executor that
            // serves no relay refuses it, as the executor binary always serves
            // one. Nothing here calls back, so the address reaches nothing.
            .with_resident_relay(|_identity| cairn_exec::ResidentRelayLease {
                callback_url: "http://127.0.0.1:9/resident/test/api/mcp".into(),
                registration: Box::new(()),
            });
        let pool = ExecutorPool::new(runtime);
        let (sender, mut receiver) = mpsc::unbounded_channel();
        // A service with no named machine requires bun, which every real
        // executor advertises from its first frame.
        let attached = fleet.attach_executor_with_toolchains(sender, &["bun"]);
        generation.store(attached, Ordering::Release);
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    ExecutorMessage::Offer { entry_id, summary } => {
                        let pool = pool.clone();
                        let answer = offload(move || answer_offer(&pool, entry_id, &summary)).await;
                        fleet.handle_executor_message(EXECUTOR_ID, attached, answer);
                    }
                    ExecutorMessage::Lost { entry_id } => {
                        let pool = pool.clone();
                        offload(move || {
                            if let Some(id) = pool.release_claim(&entry_id) {
                                pool.release_request_identity(&id);
                            }
                        })
                        .await;
                    }
                    ExecutorMessage::Cancel { request_id, .. } => {
                        let pool = pool.clone();
                        offload(move || pool.cancel_request(&request_id)).await;
                    }
                    ExecutorMessage::ResidencyRequest {
                        correlation_id,
                        config,
                        operation,
                        granted_entry_id,
                    } => {
                        if let ResidencyRuntimeConfig::Install(config) = config {
                            let pool = pool.clone();
                            offload(move || pool.configure(config)).await;
                        }
                        let (pool, fleet) = (pool.clone(), fleet.clone());
                        tokio::spawn(async move {
                            let result = pool.operate_residency(operation, granted_entry_id).await;
                            fleet.handle_executor_message(
                                EXECUTOR_ID,
                                attached,
                                ExecutorMessage::ResidencyResponse {
                                    correlation_id,
                                    result,
                                },
                            );
                        });
                    }
                    ExecutorMessage::Shutdown { .. } => break,
                    _ => {}
                }
            }
        });
        attached
    }

    /// Answer an offer in the executor link handler's order: claim, then bind
    /// the request identity. A refused claim has bound nothing, and a claim
    /// whose identity is already live here gives its capacity back without
    /// touching the binding, which belongs to whatever already holds it.
    fn answer_offer(
        pool: &ExecutorPool,
        entry_id: String,
        summary: &OfferSummary,
    ) -> ExecutorMessage {
        match pool.try_claim(&entry_id, summary) {
            Ok(()) if pool.bind_request_identity(&summary.request_id) => {
                ExecutorMessage::Claim { entry_id }
            }
            Ok(()) => {
                pool.release_claim(&entry_id);
                cairn_exec::AdmissionRefusal::new(
                    DeclineReason::Constraint,
                    Some("this machine already holds that request identity".into()),
                )
                .into_decline(entry_id)
            }
            Err(refusal) => refusal.into_decline(entry_id),
        }
    }

    /// A one-shot returns what its command printed and the status it exited
    /// with, twice over at the same process key.
    ///
    /// This is the call every outbound message on a placed channel is made of,
    /// and it failed for two independent reasons at once (CAIRN-3444): a
    /// pipe process announced no exit, and the runner discarded events for a
    /// process its liveness-beat snapshot had not caught up to. Neither is
    /// visible from one side alone, so this drives the real executor runtime
    /// through the runner's real event admission rather than a fake of either.
    #[tokio::test]
    async fn one_shot_returns_its_output_and_exit_status() {
        let (orch, _config) = attached_orchestrator().await;
        let lease = acquire_service_lease(
            &orch,
            ServiceIdentity {
                id: "channel-imessage",
                label: "iMessage channel",
            },
            named_executor(LOCAL_EXECUTOR_NAME),
            ResidencyFootprint {
                memory_bytes: 0,
                disk_growth_bytes: 0,
            },
            OwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
        )
        .await
        .expect("a named colocated executor must place a service residency");

        let sent = lease
            .run_one_shot(
                "/bin/sh",
                vec!["-c".into(), "printf '{\"guid\":\"A-1\"}'".into()],
                Duration::from_secs(10),
            )
            .await
            .expect("a one-shot that exits must be reported as exited");
        assert_eq!(String::from_utf8_lossy(&sent.stdout), "{\"guid\":\"A-1\"}");
        assert_eq!(sent.exit_code, Some(0));

        // The second send reuses the key, on the generation after the first.
        let failed = lease
            .run_one_shot(
                "/bin/sh",
                vec!["-c".into(), "printf 'no chat' >&2; exit 3".into()],
                Duration::from_secs(10),
            )
            .await
            .expect("a one-shot that fails must report its failure, not a timeout");
        assert_eq!(String::from_utf8_lossy(&failed.stderr), "no chat");
        assert_eq!(failed.exit_code, Some(3));

        lease.release().await.unwrap();
    }

    #[tokio::test]
    async fn resident_input_uses_the_started_process_generation() {
        let (orch, _config) = attached_orchestrator().await;
        let lease = acquire_service_lease(
            &orch,
            ServiceIdentity {
                id: "axon/axon@local",
                label: "axon/axon on local",
            },
            named_executor(LOCAL_EXECUTOR_NAME),
            ResidencyFootprint {
                memory_bytes: 0,
                disk_growth_bytes: 0,
            },
            OwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
        )
        .await
        .unwrap();
        let mut subscription = lease
            .start_resident(
                "facade",
                "facade",
                &ResidentLaunch::command(
                    "/bin/sh",
                    vec!["-c".into(), "read line; printf '%s' \"$line\"".into()],
                ),
            )
            .await
            .unwrap();

        lease
            .write_input("facade", b"generation-fenced\n")
            .await
            .unwrap();
        let mut stdout = Vec::new();
        while let Some(event) = subscription.recv().await {
            match event.event {
                ResidentProcessEventKind::Output {
                    stream: ResidentProcessStream::Stdout,
                    data,
                    ..
                } => stdout.extend(data),
                ResidentProcessEventKind::State {
                    status: ResidentProcessStatus::Exited { .. },
                } => break,
                _ => {}
            }
        }
        assert_eq!(stdout, b"generation-fenced");
        lease.release().await.unwrap();
    }

    async fn placed_watch(orch: &Orchestrator) -> (ServiceLease, ResidentSubscription) {
        let lease = acquire_service_lease(
            orch,
            ServiceIdentity {
                id: "channel-imessage",
                label: "iMessage channel",
            },
            named_executor(LOCAL_EXECUTOR_NAME),
            ResidencyFootprint {
                memory_bytes: 0,
                disk_growth_bytes: 0,
            },
            OwnerDeathPolicy {
                heartbeat_timeout_ms: 30_000,
                reclaim_grace_ms: 10_000,
            },
        )
        .await
        .expect("a named colocated executor must place a service residency");
        let watch = lease
            .start_resident(
                "imsg-watch",
                "watch",
                &ResidentLaunch::command("/bin/sh", vec!["-c".into(), "exec sleep 600".into()]),
            )
            .await
            .expect("a resident watch must start on a placed lease");
        (lease, watch)
    }

    /// Whether a subscription ends on its own, draining whatever it already
    /// holds, rather than parking for events that are never coming.
    async fn closes_promptly(subscription: &mut ResidentSubscription) -> bool {
        tokio::time::timeout(Duration::from_secs(5), async {
            while subscription.recv().await.is_some() {}
        })
        .await
        .is_ok()
    }

    /// An executor that stays away past its reclaim grace discards the cell,
    /// and the lease comes back to a fresh one under a new incarnation. No
    /// event will ever match the old watch's fence again, so unless the lease
    /// closes the watch's stream, the watch parks forever while every one-shot
    /// on the new cell reports the channel healthy.
    #[tokio::test]
    async fn reacquiring_a_fresh_cell_closes_the_discarded_cells_subscriptions() {
        let (orch, _config) = attached_orchestrator().await;
        let (lease, mut watch) = placed_watch(&orch).await;
        let discarded = lease.current_fence();

        // The executor's reaper, done by hand: the cell is gone while the
        // lease still holds its fence.
        super::super::residency::release(&orch, &discarded)
            .await
            .expect("the placed residency must release");
        *lease.health.lock().unwrap() = ServicePlacementHealth::ExecutorOffline;
        let reacquired = lease
            .recover()
            .await
            .expect("a lease whose cell was discarded must reacquire a fresh one");

        assert_ne!(
            reacquired.incarnation_id, discarded.incarnation_id,
            "a discarded cell comes back as a new incarnation"
        );
        assert!(
            closes_promptly(&mut watch).await,
            "a watch started on a discarded cell must be told its stream is over"
        );
        lease.release().await.unwrap();
    }

    /// An executor that comes back in time readmits the same cell under a
    /// bumped epoch, and its processes are all exited. Closing on this
    /// reacquisition too means the watch restarts now, not whenever a later
    /// snapshot re-sends the executor-lost exit.
    #[tokio::test]
    async fn reacquiring_a_readmitted_cell_closes_its_subscriptions() {
        let (orch, _config) = attached_orchestrator().await;
        let (lease, mut watch) = placed_watch(&orch).await;
        let current = lease.current_fence();

        // Readmission bumps the cell's epoch under the same incarnation, so
        // the fence the lease held before the restart trails the cell's by one.
        let before_restart = ResidencyFence {
            cell_epoch: current
                .cell_epoch
                .checked_sub(1)
                .expect("a placed cell has been through at least one epoch"),
            ..current.clone()
        };
        *lease.fence.write().unwrap() = before_restart.clone();
        *lease.health.lock().unwrap() = ServicePlacementHealth::ExecutorOffline;
        let reacquired = lease
            .recover()
            .await
            .expect("a lease whose cell is still held must reacquire it");

        assert_eq!(reacquired.incarnation_id, before_restart.incarnation_id);
        assert!(reacquired.cell_epoch > before_restart.cell_epoch);
        assert!(
            closes_promptly(&mut watch).await,
            "a watch started before readmission must be told its stream is over"
        );
        lease.release().await.unwrap();
    }

    fn no_footprint() -> ResidencyFootprint {
        ResidencyFootprint {
            memory_bytes: 0,
            disk_growth_bytes: 0,
        }
    }

    fn death_policy() -> OwnerDeathPolicy {
        OwnerDeathPolicy {
            heartbeat_timeout_ms: 30_000,
            reclaim_grace_ms: 10_000,
        }
    }

    /// A service held to a machine waits through an outage with nothing ending,
    /// so only the runner's own judgement can speak for it. It stays quiet inside
    /// the grace, reports once past it however long the wait goes on, and reports
    /// the next outage too.
    #[tokio::test]
    async fn a_lease_parked_on_its_named_machine_reports_the_loss_once_past_the_grace() {
        const LOCAL: &str = super::super::COLOCATED_EXECUTOR_ID;
        let (orch, _config, link) = attached_orchestrator_with_link().await;
        let lease = acquire_service_lease(
            &orch,
            ServiceIdentity {
                id: "thread:p:t-1:bugs",
                label: "bugs",
            },
            named_executor(LOCAL_EXECUTOR_NAME),
            no_footprint(),
            death_policy(),
        )
        .await
        .unwrap();
        let mut lost = lease.machine_lost_signal();
        let quiet = Duration::from_millis(200);

        assert!(orch.fleet.disconnect_advertised_executor(LOCAL, link));
        *lease.health.lock().unwrap() = ServicePlacementHealth::ExecutorOffline;
        let core = Arc::clone(&lease.core);
        let parked = tokio::spawn(async move { core.recover().await });

        assert!(
            tokio::time::timeout(quiet, lost.changed()).await.is_err(),
            "a machine inside the grace is only away"
        );
        orch.fleet
            .stage_link_absent(LOCAL, super::super::ENVIRONMENT_LEASE_GRACE_MS + 1);
        tokio::time::timeout(Duration::from_secs(5), lost.changed())
            .await
            .expect("a machine past the grace is reported while recovery waits")
            .unwrap();
        assert_eq!(lost.borrow_and_update().as_deref(), Some(LOCAL));
        assert!(
            tokio::time::timeout(quiet, lost.changed()).await.is_err(),
            "one report per outage, however long it lasts"
        );

        lease.loss_reported.store(false, Ordering::Release);
        lease.report_machine_lost(LOCAL);
        assert!(lost.has_changed().unwrap(), "the next outage reports again");

        // Refused, since its machine is still gone. Releasing is what ends the wait.
        let _ = lease.release().await;
        assert!(parked.await.unwrap().is_err());
    }

    /// A service free to move, on a machine that stayed away past the grace: the
    /// real eviction and re-placement, with the fleet choosing the runner's own
    /// machine. The report must arrive before the process's stream closes,
    /// because the supervisor stops reading at the exit that closure becomes.
    #[tokio::test]
    async fn an_unnamed_lease_on_a_lost_machine_reports_the_loss_and_recovers_home() {
        const LOCAL: &str = super::super::COLOCATED_EXECUTOR_ID;
        let (orch, _config) = attached_orchestrator().await;
        let lease = acquire_service_lease(
            &orch,
            ServiceIdentity {
                id: "thread:p:t-1:bugs",
                label: "bugs",
            },
            ExecutorSelector {
                required_toolchains: vec!["bun".into()],
                ..ExecutorSelector::default()
            },
            no_footprint(),
            death_policy(),
        )
        .await
        .unwrap();
        let mut watch = lease
            .start_resident(
                "svc-1",
                "service",
                &ResidentLaunch::command("/bin/sh", vec!["-c".into(), "exec sleep 600".into()]),
            )
            .await
            .unwrap();
        let mut lost = lease.machine_lost_signal();
        assert_eq!(lease.executor_id().as_deref(), Some(LOCAL));

        // The residency's route as it stood on the service box, which has gone.
        let holder = lease.current_fence().holder;
        orch.fleet
            .update_residency_routes(|routes| {
                let route = routes
                    .remove(&(LOCAL.to_string(), holder.storage_key()))
                    .expect("the placed route");
                routes.insert(
                    ("pi1".to_string(), holder.storage_key()),
                    super::super::ResidencyRoute {
                        executor_id: "pi1".into(),
                        ..route
                    },
                );
            })
            .unwrap();
        orch.fleet
            .stage_link_absent("pi1", super::super::ENVIRONMENT_LEASE_GRACE_MS + 1);
        *lease.health.lock().unwrap() = ServicePlacementHealth::ExecutorOffline;

        lease
            .recover()
            .await
            .expect("with the service box gone the fleet places the service at home");

        assert!(
            lost.has_changed().unwrap(),
            "the loss is reported by the time recovery has closed the process's stream"
        );
        assert_eq!(lost.borrow_and_update().as_deref(), Some("pi1"));
        assert_eq!(lease.executor_id().as_deref(), Some(LOCAL));
        assert!(closes_promptly(&mut watch).await);
        lease.release().await.unwrap();
    }

    fn fence() -> ResidencyFence {
        ResidencyFence {
            holder: ResidencyHolder::Service {
                service_id: "channel-imessage".into(),
            },
            incarnation_id: "incarnation".into(),
            cell_epoch: 7,
        }
    }

    fn event(process_key: &str, generation: u64) -> ResidentProcessEvent {
        let fence = fence();
        ResidentProcessEvent {
            holder: fence.holder,
            incarnation_id: fence.incarnation_id,
            cell_epoch: fence.cell_epoch,
            process_key: process_key.into(),
            process_generation: generation,
            event: ResidentProcessEventKind::State {
                status: ResidentProcessStatus::Starting,
            },
        }
    }

    /// A renewal task must not keep its own lease alive — least of all while it
    /// is parked waiting for a machine that is gone.
    ///
    /// This is the whole of CAIRN-4205 in one test, and it is deliberately
    /// staged in the hard state rather than the easy one. Asserting only that a
    /// freshly spawned renewal does not own its lease passes against a task that
    /// borrows weakly and then upgrades across `recover()`, because such a task
    /// owns the lease for the entire duration of an outage — which is exactly
    /// when an abandoned lease must stop reacquiring a holder that someone else
    /// now owns. So the executor is taken away first, recovery is left parked in
    /// `wait_for_named_executor` on a machine that never returns, and only then
    /// is the last handle dropped.
    #[tokio::test]
    async fn a_lease_dropped_while_recovery_is_parked_dies_at_once() {
        let (orch, _config, link) = attached_orchestrator_with_link().await;
        let lease = Arc::new(
            acquire_service_lease(
                &orch,
                ServiceIdentity {
                    id: "axon/axon@local",
                    label: "axon/axon on local",
                },
                named_executor(LOCAL_EXECUTOR_NAME),
                ResidencyFootprint {
                    memory_bytes: 0,
                    disk_growth_bytes: 0,
                },
                OwnerDeathPolicy {
                    heartbeat_timeout_ms: 30_000,
                    reclaim_grace_ms: 10_000,
                },
            )
            .await
            .expect("a named colocated executor must place a service residency"),
        );

        lease.spawn_renewal();
        assert_eq!(
            Arc::strong_count(&lease),
            1,
            "renewal must be handed the lease's state, never a handle to the lease"
        );

        // Take the machine away, so recovery parks for a link that never
        // reattaches instead of completing.
        assert!(
            orch.fleet
                .disconnect_advertised_executor(super::super::COLOCATED_EXECUTOR_ID, link),
            "the test link must be disconnectable at the generation it attached with"
        );
        *lease.health.lock().unwrap() = ServicePlacementHealth::ExecutorOffline;

        let core = Arc::clone(&lease.core);
        let parked = tokio::spawn(async move { core.recover().await });
        // Let recovery reach its wait. Nothing else can advance it: the executor
        // is gone and only the lease's own release signal is racing that park.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let orphaned = Arc::downgrade(&lease);
        drop(lease);

        assert!(
            orphaned.upgrade().is_none(),
            "a parked renewal must not be its lease's last owner"
        );
        let recovered = tokio::time::timeout(Duration::from_secs(5), parked)
            .await
            .expect("dropping the lease must wake a recovery parked on a machine that is gone")
            .expect("the recovery task must not panic");
        assert!(
            recovered.is_err(),
            "an orphaned lease must abandon recovery rather than reacquire its holder"
        );
    }

    #[tokio::test]
    async fn release_wakes_recovery_parked_for_an_offline_executor() {
        let released = AtomicBool::new(false);
        let notify = tokio::sync::Notify::new();
        let wait = wait_for_recovery_or_release(std::future::pending(), &released, &notify);
        tokio::pin!(wait);

        assert!(tokio::time::timeout(Duration::from_millis(10), &mut wait)
            .await
            .is_err());
        released.store(true, Ordering::Release);
        notify.notify_waiters();
        assert!(!tokio::time::timeout(Duration::from_secs(1), &mut wait)
            .await
            .expect("release must wake parked recovery"));
    }

    /// A lease is where a placed process gets its identity, so a service that
    /// declares none is refused here rather than reaching a panel that can only
    /// call it ambient (CAIRN-3435).
    #[test]
    fn a_service_that_cannot_name_itself_is_refused_a_lease() {
        assert!(validate_service_identity(ServiceIdentity {
            id: "channel-imessage",
            label: "iMessage channel",
        })
        .is_ok());
        assert!(validate_service_identity(ServiceIdentity {
            id: "channel-imessage",
            label: "   ",
        })
        .is_err());
        assert!(validate_service_identity(ServiceIdentity {
            id: " ",
            label: "iMessage channel",
        })
        .is_err());
    }

    /// A start the runner could not route is the substrate's refusal and says
    /// so in its variant; a refusal about the process stays an `Operation`.
    #[test]
    fn an_unroutable_start_is_its_own_refusal() {
        let refused = |kind| ResidencyResult::Failed {
            kind,
            diagnostic: "no connected executor reports this execution environment".into(),
            cell_outcome: None,
        };
        assert!(matches!(
            process_generation(&refused(ResidencyFailureKind::Unavailable), "svc-1"),
            Err(ServicePlacementError::Unroutable(_))
        ));
        assert!(matches!(
            process_generation(&refused(ResidencyFailureKind::Process), "svc-1"),
            Err(ServicePlacementError::Operation(_))
        ));
        assert!(matches!(
            process_generation(&refused(ResidencyFailureKind::Admission), "svc-1"),
            Err(ServicePlacementError::Unavailable(_))
        ));
    }

    /// An acquisition the fleet refused for the fleet's reasons is `Ungranted`;
    /// one refusing the service's declaration, or made by the runner itself
    /// without an executor's answer, is not.
    #[test]
    fn only_the_fleets_own_acquire_refusals_are_ungranted() {
        let refused = |kind| PlacementRefusal {
            verdict: crate::fleet::placement::PlacementVerdict::Structural,
            kind,
            diagnostic: "select execution environment executor: no executor is currently attached"
                .into(),
        };
        for kind in [
            ResidencyFailureKind::Unavailable,
            ResidencyFailureKind::Admission,
            ResidencyFailureKind::NotFound,
        ] {
            assert!(
                matches!(
                    acquire_refusal(refused(Some(kind.clone()))),
                    ServicePlacementError::Ungranted(ref diagnostic)
                        if diagnostic.contains("no executor is currently attached")
                ),
                "{kind:?}"
            );
        }
        for kind in [
            Some(ResidencyFailureKind::InvalidDeclaration),
            Some(ResidencyFailureKind::ConflictingDeclaration),
            Some(ResidencyFailureKind::Persistence),
            None,
        ] {
            assert!(
                matches!(
                    acquire_refusal(refused(kind.clone())),
                    ServicePlacementError::Unavailable(_)
                ),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn a_resident_process_needs_both_a_key_and_a_role() {
        assert!(validate_resident_process("imsg-watch", "watch").is_ok());
        assert!(validate_resident_process("imsg-watch", " ").is_err());
        assert!(validate_resident_process("", "watch").is_err());
        // The one-shot key is the lease's own; a caller may not take it.
        assert!(validate_resident_process(ONE_SHOT_KEY, "watch").is_err());
    }

    #[test]
    fn resident_event_filter_is_fenced() {
        let fence = fence();
        assert!(event_matches(&event("watch", 1), &fence));

        let mut stale = event("watch", 1);
        stale.cell_epoch += 1;
        assert!(!event_matches(&stale, &fence));

        let mut foreign = event("watch", 1);
        foreign.incarnation_id = "other-incarnation".into();
        assert!(!event_matches(&foreign, &fence));
    }

    #[tokio::test]
    async fn subscription_drops_stale_process_generations() {
        let (sender, receiver) = mpsc::unbounded_channel();
        sender.send(event("watch", 1)).unwrap();
        sender.send(event("watch", 2)).unwrap();
        drop(sender);

        let mut subscription = ResidentSubscription {
            process_generation: 2,
            events: receiver,
        };
        assert_eq!(subscription.recv().await.unwrap().process_generation, 2);
        assert!(subscription.recv().await.is_none());
    }
}
