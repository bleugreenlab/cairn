use crate::fleet::unix_time_ms;
use crate::orchestrator::Orchestrator;
use cairn_common::executor_protocol::{
    CellPriority, ExecutorSelector, OwnerDeathPolicy, ProcessSandboxMode, RepositoryLocator,
    ResidencyAcquireRequest, ResidencyFailureKind, ResidencyFence, ResidencyFootprint,
    ResidencyHolder, ResidencyOperation, ResidencyResult, ResidentProcessCwdRoot,
    ResidentProcessEvent, ResidentProcessEventKind, ResidentProcessIoMode, ResidentProcessKind,
    ResidentProcessSpec, ResidentProcessStatus, ResidentProcessStream,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};

const ONE_SHOT_KEY: &str = "service-one-shot";
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

pub(crate) struct ServiceLease {
    orch: Orchestrator,
    fence: Arc<RwLock<ResidencyFence>>,
    acquire_request: ResidencyAcquireRequest,
    executor_name: String,
    service_id: String,
    one_shot: Mutex<()>,
    recovery: Mutex<()>,
    released: AtomicBool,
    release_notify: tokio::sync::Notify,
    health: Arc<StdMutex<ServicePlacementHealth>>,
}

pub(crate) async fn acquire_service_lease(
    orch: &Orchestrator,
    service_id: &str,
    executor_name: &str,
    footprint: ResidencyFootprint,
    death_policy: OwnerDeathPolicy,
) -> Result<ServiceLease, ServicePlacementError> {
    if service_id.trim().is_empty() {
        return Err(ServicePlacementError::InvalidRequest(
            "service id must not be empty".into(),
        ));
    }
    let selector = ExecutorSelector {
        name: Some(executor_name.to_string()),
        ..ExecutorSelector::default()
    };
    selector
        .validate()
        .map_err(ServicePlacementError::InvalidRequest)?;

    let acquire_request = service_acquire_request(service_id, selector, footprint, death_policy);
    let fence = super::residency::acquire(orch, acquire_request.clone())
        .await
        .map_err(|error| ServicePlacementError::Unavailable(error.to_string()))?;

    Ok(ServiceLease {
        orch: orch.clone(),
        fence: Arc::new(RwLock::new(fence)),
        acquire_request,
        executor_name: executor_name.to_string(),
        service_id: service_id.to_string(),
        one_shot: Mutex::new(()),
        recovery: Mutex::new(()),
        released: AtomicBool::new(false),
        release_notify: tokio::sync::Notify::new(),
        health: Arc::new(StdMutex::new(ServicePlacementHealth::Ready)),
    })
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

    pub(crate) fn spawn_renewal(self: &Arc<Self>) {
        let lease = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RENEW_INTERVAL);
            interval.tick().await;
            loop {
                interval.tick().await;
                if lease.released.load(Ordering::Acquire) {
                    return;
                }
                let fence = lease.current_fence();
                if let Err(error) = super::residency::renew(&lease.orch, &fence).await {
                    tracing::warn!(%error, holder = %fence.holder, "service residency renewal failed; waiting to reacquire");
                    *lease.health.lock().unwrap() = ServicePlacementHealth::ExecutorOffline;
                    while let Err(error) = lease.recover().await {
                        if lease.released.load(Ordering::Acquire) {
                            return;
                        }
                        tracing::warn!(%error, executor = %lease.executor_name, "service residency reacquisition failed");
                        tokio::time::sleep(RENEW_INTERVAL).await;
                    }
                }
            }
        });
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
        let mut subscription = self.start_process(ONE_SHOT_KEY, program, args).await?;
        let generation = subscription.process_generation;
        let collect = async {
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

        match tokio::time::timeout(timeout, collect).await {
            Ok(result) => result,
            Err(_) => {
                let _ = self.stop_resident(ONE_SHOT_KEY).await;
                Err(ServicePlacementError::Timeout {
                    process_key: ONE_SHOT_KEY.into(),
                    timeout,
                })
            }
        }
    }

    pub(crate) async fn start_resident(
        &self,
        key: &str,
        program: &str,
        args: Vec<String>,
    ) -> Result<ResidentSubscription, ServicePlacementError> {
        if key.is_empty() || key == ONE_SHOT_KEY {
            return Err(ServicePlacementError::InvalidRequest(format!(
                "resident process key `{key}` is reserved or empty"
            )));
        }
        self.start_process(key, program, args).await
    }

    async fn start_process(
        &self,
        key: &str,
        program: &str,
        args: Vec<String>,
    ) -> Result<ResidentSubscription, ServicePlacementError> {
        if program.is_empty() {
            return Err(ServicePlacementError::InvalidRequest(
                "process program must not be empty".into(),
            ));
        }

        let (sender, receiver) = mpsc::unbounded_channel();
        let expected_generation = Arc::new(AtomicU64::new(0));
        let callback_generation = expected_generation.clone();
        if !matches!(self.health(), ServicePlacementHealth::Ready) {
            self.recover().await?;
        }
        let fence = self.current_fence();
        let callback_fence = fence.clone();
        let process_key = key.to_string();
        let callback_key = process_key.clone();
        self.orch
            .fleet
            .subscribe_resident_process_events(move |event| {
                let expected = callback_generation.load(Ordering::Acquire);
                if event_matches(&event, &callback_fence, &callback_key)
                    && (expected == 0 || event.process_generation == expected)
                {
                    let _ = sender.send(event);
                }
            });

        let result = self
            .orch
            .fleet
            .operate_residency(
                &self.orch,
                ResidencyOperation::StartProcess {
                    fence: fence.clone(),
                    process_key: process_key.clone(),
                    kind: ResidentProcessKind::Service {
                        service: self.service_id.clone(),
                    },
                    reservation: None,
                    process: ResidentProcessSpec {
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

        let generation = process_generation(&result, &process_key)?;
        expected_generation.store(generation, Ordering::Release);
        *self.health.lock().unwrap() = ServicePlacementHealth::Ready;
        Ok(ResidentSubscription {
            process_generation: generation,
            events: receiver,
        })
    }

    pub(crate) async fn stop_resident(&self, key: &str) -> Result<(), ServicePlacementError> {
        super::residency::stop(&self.orch, &self.current_fence(), key)
            .await
            .map_err(ServicePlacementError::Operation)
    }
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

fn event_matches(event: &ResidentProcessEvent, fence: &ResidencyFence, key: &str) -> bool {
    event.holder == fence.holder
        && event.incarnation_id == fence.incarnation_id
        && event.cell_epoch == fence.cell_epoch
        && event.process_key == key
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn resident_event_filter_is_fenced_and_keyed() {
        let fence = fence();
        assert!(event_matches(&event("watch", 1), &fence, "watch"));
        assert!(!event_matches(&event("other", 1), &fence, "watch"));

        let mut stale = event("watch", 1);
        stale.cell_epoch += 1;
        assert!(!event_matches(&stale, &fence, "watch"));
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
