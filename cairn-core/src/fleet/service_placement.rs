use crate::fleet::unix_time_ms;
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::{
    CellPriority, ExecutorSelector, OwnerDeathPolicy, ProcessSandboxMode, RepositoryLocator,
    ResidencyAcquireRequest, ResidencyFailureKind, ResidencyFence, ResidencyFootprint,
    ResidencyHolder, ResidencyOperation, ResidencyResult, ResidentProcessCwdRoot,
    ResidentProcessEvent, ResidentProcessEventKind, ResidentProcessIoMode, ResidentProcessKind,
    ResidentProcessSpec, ResidentProcessStatus, ResidentProcessStream,
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
const ONE_SHOT_ROLE: &str = "command";
const ACQUIRE_WAIT: Duration = Duration::from_secs(30);
const RENEW_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub(crate) enum ServicePlacementError {
    #[error("invalid service placement request: {0}")]
    InvalidRequest(String),
    #[error("service residency is unavailable: {0}")]
    Unavailable(String),
    #[error("service process operation failed: {0}")]
    Operation(String),
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
    executor_name: String,
    service_label: String,
    one_shot: Mutex<()>,
    recovery: Mutex<()>,
    released: AtomicBool,
    release_notify: tokio::sync::Notify,
    health: Arc<StdMutex<ServicePlacementHealth>>,
    routes: ProcessRoutes,
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

pub(crate) async fn acquire_service_lease(
    orch: &Orchestrator,
    service: ServiceIdentity<'_>,
    executor_name: &str,
    footprint: ResidencyFootprint,
    death_policy: OwnerDeathPolicy,
) -> Result<ServiceLease, ServicePlacementError> {
    validate_service_identity(service)?;
    let selector = ExecutorSelector {
        name: Some(executor_name.to_string()),
        ..ExecutorSelector::default()
    };
    selector
        .validate()
        .map_err(ServicePlacementError::InvalidRequest)?;

    let acquire_request = service_acquire_request(service.id, selector, footprint, death_policy);
    let fence = super::residency::acquire_quietly(orch, acquire_request.clone())
        .await
        .map_err(|error| ServicePlacementError::Unavailable(error.to_string()))?;

    let fence = Arc::new(RwLock::new(fence));
    let routes = ProcessRoutes::default();
    subscribe_lease_events(orch, &fence, &routes);
    Ok(ServiceLease {
        core: Arc::new(LeaseCore {
            orch: orch.clone(),
            fence,
            acquire_request,
            executor_name: executor_name.to_string(),
            service_label: service.label.trim().to_string(),
            one_shot: Mutex::new(()),
            recovery: Mutex::new(()),
            released: AtomicBool::new(false),
            release_notify: tokio::sync::Notify::new(),
            health: Arc::new(StdMutex::new(ServicePlacementHealth::Ready)),
            routes,
        }),
    })
}

/// Install the lease's single subscription on the fleet's resident-process
/// event stream.
///
/// One per lease, not one per process start. Subscribers are never removed, so
/// subscribing at each start left a closure behind for every one-shot the
/// service ever ran — a health check a minute is fourteen hundred a day — and
/// dispatched every resident event on the machine through all of them.
///
/// The fence is read through the lease's own handle rather than copied, because
/// a lease that reacquired after an executor bounce holds a different fence than
/// the one it started with. A captured copy would match nothing afterwards, and
/// a resident watch fed by it would park in silence rather than fail. Weak
/// handles let a released lease's subscription go inert instead of keeping its
/// state alive.
fn subscribe_lease_events(
    orch: &Orchestrator,
    fence: &Arc<RwLock<ResidencyFence>>,
    routes: &ProcessRoutes,
) {
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
    });
}

fn service_acquire_request(
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
        executor: Some(selector),
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
                let Err(error) = super::residency::renew(&core.orch, &fence).await else {
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
                    tracing::warn!(%error, executor = %core.executor_name, "service residency reacquisition failed");
                    tokio::time::sleep(RENEW_INTERVAL).await;
                }
            }
        });
    }
}

impl LeaseCore {
    pub(crate) fn executor_name(&self) -> &str {
        &self.executor_name
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
        if !wait_for_recovery_or_release(
            self.orch.fleet.wait_for_named_executor(&self.executor_name),
            &self.released,
            &self.release_notify,
        )
        .await
        {
            return Err(ServicePlacementError::Operation(
                "service residency has been released".into(),
            ));
        }
        let mut request = self.acquire_request.clone();
        let now = unix_time_ms();
        request.waiting_since_unix_ms = now;
        request.wait_horizon_unix_ms = now.saturating_add(ACQUIRE_WAIT.as_millis() as u64);
        let fence = super::residency::acquire(&self.orch, request)
            .await
            .map_err(|error| ServicePlacementError::Unavailable(error.to_string()))?;
        if self.released.load(Ordering::Acquire) {
            let _ = super::residency::release(&self.orch, &fence).await;
            return Err(ServicePlacementError::Operation(
                "service residency was released during reacquisition".into(),
            ));
        }
        *self.fence.write().unwrap() = fence.clone();
        *self.health.lock().unwrap() = ServicePlacementHealth::Ready;
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
                .start_process(ONE_SHOT_KEY, ONE_SHOT_ROLE, program, args)
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
                                self.executor_name
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
        program: &str,
        args: Vec<String>,
    ) -> Result<ResidentSubscription, ServicePlacementError> {
        validate_resident_process(key, role)?;
        self.start_process(key, role, program, args).await
    }

    async fn start_process(
        &self,
        key: &str,
        role: &str,
        program: &str,
        args: Vec<String>,
    ) -> Result<ResidentSubscription, ServicePlacementError> {
        if program.is_empty() {
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
                    process: ResidentProcessSpec {
                        runtime_packages: Vec::new(),
                        program: program.to_string(),
                        args,
                        cwd: String::new(),
                        cwd_root: ResidentProcessCwdRoot::ResidencyScratch,
                        env: Vec::new(),
                        sandbox_mode: ProcessSandboxMode::Unconfined,
                        sandbox_policy: None,
                        runtime_assets: Vec::new(),
                        io: ResidentProcessIoMode::Pipe,
                    },
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

    #[allow(dead_code)]
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
                    "desktop process `{key}` is not running on {}",
                    self.executor_name
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
                    "desktop process `{key}` on {} changed generation: {diagnostic}",
                    self.executor_name
                )))
            }
            ResidencyResult::Failed { diagnostic, .. } => {
                Err(ServicePlacementError::Operation(format!(
                    "could not write to desktop process `{key}` on {}: {diagnostic}",
                    self.executor_name
                )))
            }
            other => Err(ServicePlacementError::Operation(format!(
                "unexpected input-write result for desktop process `{key}` on {}: {other:?}",
                self.executor_name
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
            _ => Err(ServicePlacementError::Operation(diagnostic.clone())),
        },
        other => Err(ServicePlacementError::Operation(format!(
            "unexpected start result: {other:?}"
        ))),
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
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use cairn_common::executor_protocol::{
        ExecutorMessage, ResidencyRuntimeConfig, LOCAL_EXECUTOR_NAME,
    };
    use cairn_executor::{ExecutorRuntime, Fleet as ExecutorPool};
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;

    /// An orchestrator with a real executor runtime attached, standing in for
    /// the WebSocket link with the message pump the link would provide.
    ///
    /// Both callbacks are wired exactly as production wires them, so a one-shot
    /// run through this orchestrator meets the real executor's process
    /// supervision and the real runner's event admission. What it does not
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
        let db = LocalDb::open(config.path().join("cairn.turso.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
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
            .with_snapshot_callback(move |snapshot, health| {
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
            });
        let pool = ExecutorPool::new(runtime);
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let attached = fleet.attach_executor(sender);
        generation.store(attached, Ordering::Release);
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                match message {
                    ExecutorMessage::ResidencyRequest {
                        correlation_id,
                        config,
                        operation,
                    } => {
                        if let ResidencyRuntimeConfig::Install(config) = config {
                            pool.configure(config);
                        }
                        let (pool, fleet) = (pool.clone(), fleet.clone());
                        tokio::spawn(async move {
                            let result = pool.operate_residency(operation).await;
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
                    ExecutorMessage::Shutdown => break,
                    _ => {}
                }
            }
        });
        attached
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
            LOCAL_EXECUTOR_NAME,
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
                id: "desktop-automation",
                label: "desktop automation",
            },
            LOCAL_EXECUTOR_NAME,
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
                "/bin/sh",
                vec!["-c".into(), "read line; printf '%s' \"$line\"".into()],
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
                    id: "desktop-automation",
                    label: "desktop automation",
                },
                LOCAL_EXECUTOR_NAME,
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
