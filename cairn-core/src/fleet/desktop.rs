// This module lands with the run-dispatch caller in the same integration branch.
#![allow(dead_code)]

use super::service_placement::{
    acquire_service_lease, ResidentSubscription, ServiceIdentity, ServiceLease,
    ServicePlacementHealth,
};
use crate::mcp::gateway::{DuplexIo, PlacedFacade};
use crate::orchestrator::Orchestrator;
use async_trait::async_trait;
use cairn_common::executor_protocol::{
    OwnerDeathPolicy, ResidencyFootprint, ResidentProcessEvent, ResidentProcessEventKind,
    ResidentProcessStatus, ResidentProcessStream,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio::sync::OnceCell;

const FACADE_PROCESS_KEY: &str = "facade";
const DUPLEX_CAPACITY: usize = 64 * 1024;
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);
const PROBE_CADENCE: Duration = Duration::from_secs(5 * 60);

fn desktop_service_id(executor_name: &str) -> String {
    format!("desktop-automation:{executor_name}")
}

/// Acquire the machine-scoped residency used by desktop run dispatch.
///
/// The returned factory owns its lease for the runner lifetime and opens the
/// resident Axon MCP process lazily when the transport gateway has no pooled
/// connection for this machine.
pub(crate) fn placed_desktop_facade(
    orch: &Orchestrator,
    executor_name: &str,
    binary: impl Into<String>,
) -> Arc<dyn PlacedFacade> {
    desktop_facade(orch, executor_name, binary)
}

fn desktop_facade(
    orch: &Orchestrator,
    executor_name: &str,
    binary: impl Into<String>,
) -> Arc<DesktopFacade> {
    Arc::new(DesktopFacade {
        executor_name: executor_name.to_string(),
        binary: binary.into(),
        orch: orch.clone(),
        lease: OnceCell::new(),
    })
}

struct DesktopFacade {
    executor_name: String,
    binary: String,
    orch: Orchestrator,
    lease: OnceCell<Arc<ServiceLease>>,
}

impl DesktopFacade {
    async fn acquire_lease(&self) -> Result<Arc<ServiceLease>, String> {
        self.lease
            .get_or_try_init(|| async {
                let service_id = desktop_service_id(&self.executor_name);
                let lease = Arc::new(
                    acquire_service_lease(
                        &self.orch,
                        ServiceIdentity {
                            id: &service_id,
                            label: "desktop automation",
                        },
                        &self.executor_name,
                        ResidencyFootprint {
                            memory_bytes: 0,
                            disk_growth_bytes: 0,
                        },
                        OwnerDeathPolicy {
                            heartbeat_timeout_ms: 90_000,
                            reclaim_grace_ms: 30_000,
                        },
                    )
                    .await
                    .map_err(|error| {
                        format!(
                            "could not place desktop automation on {}: {error}",
                            self.executor_name
                        )
                    })?,
                );
                lease.spawn_renewal();
                Ok::<_, String>(lease)
            })
            .await
            .cloned()
    }
}

/// Probe all currently attached machines immediately and then periodically.
/// Reads deliberately never call this function; they only consume its database cache.
pub fn spawn_desktop_probe_service(orch: Orchestrator) {
    tokio::spawn(async move {
        loop {
            let attached = probe_attached_desktops(&orch).await;
            // The transport runtime starts before the colocated executor has
            // necessarily published its first fleet snapshot. Keep watching
            // during that bootstrap window instead of turning one empty scan
            // into a five-minute discovery delay.
            let cadence = if attached == 0 {
                std::time::Duration::from_secs(1)
            } else {
                PROBE_CADENCE
            };
            tokio::time::sleep(cadence).await;
        }
    });
}

pub async fn probe_attached_desktops(orch: &Orchestrator) -> usize {
    let config = crate::config::settings::load_fleet(&orch.config_dir);
    let executors = orch.fleet.inspect_executors(super::unix_time_ms());
    let attached = executors.len();
    for executor in executors {
        let desktop = config.desktop_automation.resolve(
            &executor.name,
            &executor.health.advertisement.capabilities.os,
        );
        if desktop.enabled {
            probe_desktop(orch, &executor.name, desktop.binary).await;
        } else if let Some(gateway) = orch.mcp_gateway() {
            gateway.close_placed(&executor.name).await;
        }
    }
    attached
}

pub async fn probe_desktop(orch: &Orchestrator, name: &str, binary: String) {
    let Some(gateway) = orch.mcp_gateway() else {
        return;
    };
    let facade = desktop_facade(orch, name, binary.clone());
    let result = probe_with_facade(gateway, facade, &binary).await;
    let state = match result {
        Ok((health, Ok(tools))) => cairn_db::storage::ExecutorDesktopAutomation {
            executor_id: name.to_string(),
            probed_at: chrono::Utc::now().timestamp_millis(),
            health_json: Some(health.to_string()),
            verbs_json: serde_json::to_string(&tools).unwrap_or_else(|_| "[]".into()),
            probe_error: None,
        },
        Ok((health, Err(error))) => cairn_db::storage::ExecutorDesktopAutomation {
            executor_id: name.to_string(),
            probed_at: chrono::Utc::now().timestamp_millis(),
            health_json: Some(health.to_string()),
            verbs_json: "[]".into(),
            probe_error: Some(error),
        },
        Err(error) => cairn_db::storage::ExecutorDesktopAutomation {
            executor_id: name.to_string(),
            probed_at: chrono::Utc::now().timestamp_millis(),
            health_json: None,
            verbs_json: "[]".into(),
            probe_error: Some(error),
        },
    };
    if let Err(error) =
        cairn_db::storage::upsert_executor_desktop_automation(&orch.db.local, &state).await
    {
        tracing::warn!(machine = name, %error, "could not cache desktop probe");
    }
}

async fn probe_with_facade(
    gateway: &Arc<dyn crate::mcp::gateway::McpGateway>,
    facade: Arc<DesktopFacade>,
    binary: &str,
) -> Result<
    (
        serde_json::Value,
        Result<Vec<crate::mcp::gateway::McpToolDef>, String>,
    ),
    String,
> {
    let lease = facade.acquire_lease().await?;
    let output = lease
        .run_one_shot(
            binary,
            vec!["status".into(), "--json".into()],
            PROBE_TIMEOUT,
        )
        .await
        .map_err(|e| e.to_string())?;
    if output.exit_code != Some(0) {
        return Err(format!(
            "status exited {:?}: {}",
            output.exit_code,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let health: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("status returned invalid JSON: {error}"))?;
    let ready = health
        .pointer("/daemon/ready")
        .and_then(|v| v.as_bool())
        .or_else(|| health.get("ready").and_then(|v| v.as_bool()))
        .unwrap_or(false);
    let tools = if ready {
        gateway
            .list_placed_tools(facade)
            .await
            .map(|catalog| catalog.tools)
    } else {
        Ok(Vec::new())
    };
    Ok((health, tools))
}

#[async_trait]
impl PlacedFacade for DesktopFacade {
    fn key(&self) -> &str {
        &self.executor_name
    }

    async fn open(&self) -> Result<Box<dyn DuplexIo>, String> {
        let lease = self.acquire_lease().await?;
        match lease.health() {
            ServicePlacementHealth::ExecutorOffline => {
                return Err(format!("{} is offline", self.executor_name));
            }
            ServicePlacementHealth::ProcessDown { exit_code, .. } => {
                tracing::debug!(
                    machine = %self.executor_name,
                    ?exit_code,
                    "restarting desktop facade after process exit"
                );
            }
            ServicePlacementHealth::Ready => {}
        }

        let subscription = lease
            .start_resident(
                FACADE_PROCESS_KEY,
                "desktop automation",
                &self.binary,
                vec!["mcp".to_string()],
            )
            .await
            .map_err(|error| {
                format!(
                    "could not start the desktop facade on {}: {error}",
                    self.executor_name
                )
            })?;
        let (gateway, machine) = tokio::io::duplex(DUPLEX_CAPACITY);
        tokio::spawn(pump_facade(lease, subscription, machine));
        Ok(Box::new(gateway))
    }
}

async fn pump_facade(
    lease: Arc<ServiceLease>,
    mut subscription: ResidentSubscription,
    mut stream: DuplexStream,
) {
    let mut input = vec![0; 16 * 1024];
    loop {
        tokio::select! {
            event = subscription.recv() => {
                let Some(event) = event else { break };
                if !forward_process_event(&lease, &mut stream, event).await {
                    break;
                }
            }
            read = stream.read(&mut input) => {
                match read {
                    Ok(0) => {
                        let _ = lease.stop_resident(FACADE_PROCESS_KEY).await;
                        break;
                    }
                    Ok(count) => {
                        if let Err(error) = lease.write_input(FACADE_PROCESS_KEY, &input[..count]).await {
                            tracing::warn!(machine = %lease.executor_name(), %error, "desktop facade input pump stopped");
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(machine = %lease.executor_name(), %error, "desktop facade duplex read failed");
                        break;
                    }
                }
            }
        }
    }
    // Dropping the machine half is deliberate: rmcp observes EOF immediately
    // when the resident process exits or its lease is lost.
}

async fn forward_process_event(
    lease: &ServiceLease,
    stream: &mut (impl AsyncWrite + Unpin),
    event: ResidentProcessEvent,
) -> bool {
    match event.event {
        ResidentProcessEventKind::Output {
            stream: output,
            data,
            ..
        } => match output {
            ResidentProcessStream::Stdout | ResidentProcessStream::Pty => {
                if let Err(error) = stream.write_all(&data).await {
                    tracing::warn!(machine = %lease.executor_name(), %error, "desktop facade stdout pump failed");
                    return false;
                }
                true
            }
            ResidentProcessStream::Stderr => {
                tracing::warn!(
                    machine = %lease.executor_name(),
                    stderr = %String::from_utf8_lossy(&data).trim(),
                    "desktop facade stderr"
                );
                true
            }
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
                tracing::warn!(machine = %lease.executor_name(), "desktop facade executor disconnected");
            } else {
                tracing::warn!(machine = %lease.executor_name(), ?exit_code, "desktop facade exited");
            }
            lease.mark_process_down(FACADE_PROCESS_KEY, exit_code);
            false
        }
        ResidentProcessEventKind::State { .. } => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desktop_residency_identity_is_machine_scoped() {
        assert_eq!(
            desktop_service_id("bglab-win"),
            "desktop-automation:bglab-win"
        );
        assert_ne!(
            desktop_service_id("bglab-win"),
            desktop_service_id("bglab-mac")
        );
    }
    use cairn_common::executor_protocol::{ResidencyFence, ResidencyHolder};

    fn event(event: ResidentProcessEventKind) -> ResidentProcessEvent {
        let fence = ResidencyFence {
            holder: ResidencyHolder::Service {
                service_id: "desktop-automation".into(),
            },
            incarnation_id: "incarnation".into(),
            cell_epoch: 1,
        };
        ResidentProcessEvent {
            holder: fence.holder,
            incarnation_id: fence.incarnation_id,
            cell_epoch: fence.cell_epoch,
            process_key: FACADE_PROCESS_KEY.into(),
            process_generation: 1,
            event,
        }
    }

    #[tokio::test]
    async fn stdout_enters_duplex_but_stderr_does_not() {
        let (orch, _config) = super::super::service_placement::tests::attached_orchestrator().await;
        let lease = acquire_service_lease(
            &orch,
            ServiceIdentity {
                id: "desktop-automation",
                label: "desktop automation",
            },
            cairn_common::executor_protocol::LOCAL_EXECUTOR_NAME,
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
        let (mut reader, mut writer) = tokio::io::duplex(64);
        assert!(
            forward_process_event(
                &lease,
                &mut writer,
                event(ResidentProcessEventKind::Output {
                    sequence: 1,
                    stream: ResidentProcessStream::Stderr,
                    data: b"warning".to_vec(),
                })
            )
            .await
        );
        assert!(
            forward_process_event(
                &lease,
                &mut writer,
                event(ResidentProcessEventKind::Output {
                    sequence: 2,
                    stream: ResidentProcessStream::Stdout,
                    data: b"json-rpc".to_vec(),
                })
            )
            .await
        );
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"json-rpc");
    }

    #[tokio::test]
    async fn process_exit_closes_the_forwarding_side() {
        let (orch, _config) = super::super::service_placement::tests::attached_orchestrator().await;
        let lease = acquire_service_lease(
            &orch,
            ServiceIdentity {
                id: "desktop-automation",
                label: "desktop automation",
            },
            cairn_common::executor_protocol::LOCAL_EXECUTOR_NAME,
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
        let (_reader, mut writer) = tokio::io::duplex(64);
        assert!(
            !forward_process_event(
                &lease,
                &mut writer,
                event(ResidentProcessEventKind::State {
                    status: ResidentProcessStatus::Exited {
                        finished_at_unix_ms: 1,
                        exit_code: Some(7),
                        restartable: false,
                        executor_lost: false,
                    },
                })
            )
            .await
        );
        assert_eq!(
            lease.health(),
            ServicePlacementHealth::ProcessDown {
                process_key: FACADE_PROCESS_KEY.into(),
                exit_code: Some(7),
            }
        );
    }
}
