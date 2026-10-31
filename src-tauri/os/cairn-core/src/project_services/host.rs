//! The production [`ServiceHost`]: a service process is a resident process
//! under a service lease, one lease per (project, owner, service), reused by
//! every incarnation so a rolling replace runs both generations in one cell.

use async_trait::async_trait;
use cairn_common::executor_protocol::{
    OwnerDeathPolicy, ProcessSandboxMode, ResidencyFootprint, ResidentProcessCwdRoot,
    ResidentProcessEventKind, ResidentProcessStatus, ResidentProcessStream, ResidentRuntimeAsset,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use super::supervisor::{HostEvent, HostInput, HostProcess, LaunchSpec, ServiceHost};
use super::ServiceOwner;
use crate::fleet::service_placement::{
    acquire_service_lease, ResidentLaunch, ServiceIdentity, ServiceLease,
};
use crate::orchestrator::Orchestrator;

/// The role every service process shows in a running list.
const SERVICE_ROLE: &str = "service";

#[derive(Default)]
pub(crate) struct LeaseHost {
    leases: Mutex<HashMap<String, Arc<ServiceLease>>>,
}

/// The lease holder: a thread's service and a project service of the same
/// name are different residencies.
fn holder_id(project_id: &str, owner: &ServiceOwner, service: &str) -> String {
    match owner {
        ServiceOwner::Project => format!("project:{project_id}:{service}"),
        ServiceOwner::Thread { id, .. } => format!("thread:{project_id}:{id}:{service}"),
    }
}

/// What a person calls the residency in a running list.
fn holder_label(spec: &LaunchSpec) -> String {
    match &spec.owner {
        ServiceOwner::Project => format!("{}/{}", spec.project_key, spec.service),
        ServiceOwner::Thread { name, .. } => {
            format!("{}/{name}/{}", spec.project_key, spec.service)
        }
    }
}

/// The package under `service/`, with `@cairn/sdk` beside it so the entry's
/// bare import resolves.
fn runtime_assets(spec: &LaunchSpec) -> Result<Vec<ResidentRuntimeAsset>, String> {
    let mut assets: Vec<ResidentRuntimeAsset> = spec
        .files
        .iter()
        .map(|(path, data)| ResidentRuntimeAsset {
            path: format!("service/{path}"),
            data: data.clone(),
        })
        .collect();
    crate::runtime::collect_package_assets(
        &crate::runtime::runtime_package("sdk")?,
        Path::new("service/node_modules/@cairn/sdk"),
        &mut assets,
    )?;
    cairn_common::runtime::validate_runtime_assets(&assets)
        .map_err(|error| format!("service package and runtime rejected: {error}"))?;
    Ok(assets)
}

/// The service process: `bun` importing the package's entry. Always
/// confined, whatever the service is for.
pub(crate) fn launch_for(spec: &LaunchSpec) -> Result<ResidentLaunch, String> {
    let mut launch = process_for(spec);
    launch.runtime_assets = runtime_assets(spec)?;
    Ok(launch)
}

fn process_for(spec: &LaunchSpec) -> ResidentLaunch {
    let loader = format!(
        "await import(process.env.CAIRN_RUNTIME_ASSETS + \"/service/{}\")",
        spec.declaration.entry.replace('"', "\\\"")
    );
    ResidentLaunch {
        program: "bun".to_string(),
        args: vec!["-e".to_string(), loader],
        env: spec.env.clone(),
        runtime_assets: Vec::new(),
        runtime_packages: Vec::new(),
        sandbox_mode: ProcessSandboxMode::Confined,
        cwd_root: ResidentProcessCwdRoot::ResidencyScratch,
        relay: None,
    }
}

/// Where a service runs: where its declaration's `executor` says, by machine
/// name, platform, and toolchains, exactly as a `run` selector would place it.
/// A declaration that names neither a machine nor a platform runs on the
/// runner's own machine, refined by any toolchains it asks for.
///
/// A service elsewhere reaches Cairn through its executor's resident relay,
/// with bearers bound to that relay, so placement is a choice and not a pin.
fn service_selector(spec: &LaunchSpec) -> cairn_common::executor_protocol::ExecutorSelector {
    use cairn_common::executor_protocol::{ExecutorSelector, LOCAL_EXECUTOR_NAME};
    let declared = spec.declaration.executor.clone().unwrap_or_default();
    if declared.name.is_none() && declared.os.is_none() {
        return ExecutorSelector {
            name: Some(LOCAL_EXECUTOR_NAME.to_string()),
            required_toolchains: declared.required_toolchains,
            ..ExecutorSelector::default()
        };
    }
    ExecutorSelector {
        name: declared.name,
        os: declared.os,
        required_toolchains: declared.required_toolchains,
        ..ExecutorSelector::default()
    }
}

impl LeaseHost {
    async fn lease(
        &self,
        orch: &Orchestrator,
        spec: &LaunchSpec,
    ) -> Result<Arc<ServiceLease>, String> {
        let id = holder_id(&spec.project_id, &spec.owner, &spec.service);
        let mut leases = self.leases.lock().await;
        if let Some(lease) = leases.get(&id) {
            return Ok(lease.clone());
        }
        let label = holder_label(spec);
        let selector = service_selector(spec);
        let lease = Arc::new(
            acquire_service_lease(
                orch,
                ServiceIdentity {
                    id: &id,
                    label: &label,
                },
                selector,
                ResidencyFootprint {
                    memory_bytes: spec.declaration.limits.memory_mb as u64 * 1024 * 1024,
                    disk_growth_bytes: 64 * 1024 * 1024,
                },
                OwnerDeathPolicy {
                    heartbeat_timeout_ms: 90_000,
                    reclaim_grace_ms: 30_000,
                },
            )
            .await
            .map_err(|error| error.to_string())?,
        );
        lease.spawn_renewal();
        leases.insert(id, lease.clone());
        Ok(lease)
    }
}

#[async_trait]
impl ServiceHost for LeaseHost {
    async fn callback(
        &self,
        orch: &Orchestrator,
        spec: &LaunchSpec,
    ) -> Result<crate::fleet::residency::ResidentCallback, String> {
        let lease = self.lease(orch, spec).await?;
        Ok(crate::fleet::residency::resident_callback(
            orch,
            lease.executor_id().as_deref(),
        ))
    }

    async fn launch(&self, orch: &Orchestrator, spec: &LaunchSpec) -> Result<HostProcess, String> {
        let mut launch = launch_for(spec)?;
        let lease = self.lease(orch, spec).await?;
        // Generation-keyed, so a replacement starts beside the generation it
        // replaces rather than over it.
        let key = format!("svc-{}", spec.generation);
        // On another machine the service calls back through its executor's
        // relay, which accepts only bearers of this incarnation's runs.
        if let crate::fleet::residency::ResidentCallback::Relayed { executor_id } =
            crate::fleet::residency::resident_callback(orch, lease.executor_id().as_deref())
        {
            crate::fleet::residency::refuse_runner_credentials(orch, &launch.env)?;
            let holder = cairn_common::executor_protocol::ResidencyHolder::Service {
                service_id: holder_id(&spec.project_id, &spec.owner, &spec.service),
            };
            let minted = orch.fleet.resident_relays().register(
                &executor_id,
                &holder,
                &key,
                crate::fleet::resident_relay::RegisteredAuthority::Service(
                    crate::fleet::resident_relay::ServiceIncarnation {
                        project_id: spec.project_id.clone(),
                        owner_key: spec.owner.key().to_string(),
                        service: spec.service.clone(),
                        generation: spec.generation,
                    },
                ),
            );
            launch.relay = Some(minted.grant);
        }
        let mut subscription = lease
            .start_resident(&key, SERVICE_ROLE, &launch)
            .await
            .map_err(|error| error.to_string())?;
        let (sender, events) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(event) = subscription.recv().await {
                match event.event {
                    ResidentProcessEventKind::Output { stream, data, .. } => {
                        let event = match stream {
                            ResidentProcessStream::Stderr => HostEvent::Stderr(data),
                            ResidentProcessStream::Stdout | ResidentProcessStream::Pty => {
                                HostEvent::Stdout(data)
                            }
                        };
                        let _ = sender.send(event);
                    }
                    ResidentProcessEventKind::State {
                        status:
                            ResidentProcessStatus::Exited {
                                exit_code,
                                executor_lost,
                                ..
                            },
                    } => {
                        let detail = if executor_lost {
                            "its executor disconnected"
                        } else {
                            "the process exited"
                        };
                        let _ = sender.send(HostEvent::Exited {
                            code: exit_code,
                            detail: detail.to_string(),
                        });
                        return;
                    }
                    ResidentProcessEventKind::State { .. } => {}
                }
            }
            let _ = sender.send(HostEvent::Exited {
                code: None,
                detail: "its event stream closed".to_string(),
            });
        });
        Ok(HostProcess {
            executor: lease.executor_name(),
            input: Arc::new(LeaseInput { lease, key }),
            events,
        })
    }

    async fn release(
        &self,
        _orch: &Orchestrator,
        project_id: &str,
        owner: &ServiceOwner,
        service: &str,
    ) {
        let lease = self
            .leases
            .lock()
            .await
            .remove(&holder_id(project_id, owner, service));
        if let Some(lease) = lease {
            if let Err(error) = lease.release().await {
                tracing::warn!(%error, service, "could not release a service lease");
            }
        }
    }
}

struct LeaseInput {
    lease: Arc<ServiceLease>,
    key: String,
}

#[async_trait]
impl HostInput for LeaseInput {
    async fn write(&self, bytes: &[u8]) -> Result<(), String> {
        self.lease
            .write_input(&self.key, bytes)
            .await
            .map_err(|error| error.to_string())
    }

    async fn kill(&self) {
        let _ = self.lease.stop_resident(&self.key).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project_services::supervisor::HostEvent;
    use cairn_common::executor_protocol::ResidentProcessIoMode;
    use std::time::Duration;

    fn echo_spec() -> LaunchSpec {
        let package = crate::project_services::declaration::package_from_files(
            "echo",
            vec![
                (
                    "service.yaml".into(),
                    include_bytes!("../../../../../.cairn/services/echo/service.yaml").to_vec(),
                ),
                (
                    "main.ts".into(),
                    include_bytes!("../../../../../.cairn/services/echo/main.ts").to_vec(),
                ),
            ],
        )
        .unwrap();
        LaunchSpec {
            project_id: "p".into(),
            project_key: "cairn".into(),
            owner: ServiceOwner::Project,
            service: "echo".into(),
            generation: 1,
            declaration: package.declaration,
            files: package.files,
            env: vec![("CAIRN_SERVICE".into(), "echo".into())],
        }
    }

    async fn next_stdout_frame(
        events: &mut tokio::sync::mpsc::UnboundedReceiver<HostEvent>,
        stderr: &mut String,
    ) -> serde_json::Value {
        let mut buffer = Vec::new();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(60), events.recv())
                .await
                .unwrap_or_else(|_| panic!("no frame within 60s; stderr: {stderr}"))
                .unwrap_or_else(|| panic!("events closed; stderr: {stderr}"));
            match event {
                HostEvent::Stdout(bytes) => {
                    buffer.extend(bytes);
                    if let Some(end) = buffer.iter().position(|b| *b == b'\n') {
                        return serde_json::from_slice(&buffer[..end]).unwrap();
                    }
                }
                HostEvent::Stderr(bytes) => stderr.push_str(&String::from_utf8_lossy(&bytes)),
                HostEvent::Exited { code, detail } => {
                    panic!("exited ({code:?}, {detail}) before a frame; stderr: {stderr}")
                }
            }
        }
    }

    /// The whole launch path with a real process: the lease, the confined
    /// resident process, the package and `@cairn/sdk` as runtime assets, the
    /// entry's bare import resolving, and the protocol over the real pipe.
    #[tokio::test]
    async fn the_echo_service_boots_answers_and_drains_as_a_real_process() {
        let (orch, _config) = crate::fleet::service_placement::tests::attached_orchestrator().await;
        let host = LeaseHost::default();
        let spec = echo_spec();
        let mut process = host
            .launch(&orch, &spec)
            .await
            .expect("the service launches");
        let mut stderr = String::new();
        let send = |frame: serde_json::Value| {
            let mut line = serde_json::to_vec(&frame).unwrap();
            line.push(b'\n');
            line
        };
        process
            .input
            .write(&send(serde_json::json!({"type": "hello", "protocol": 1, "project": "cairn", "service": "echo", "source": "s-1"})))
            .await
            .unwrap();
        let ready = next_stdout_frame(&mut process.events, &mut stderr).await;
        assert_eq!(ready["type"], "ready", "{ready}");
        assert_eq!(ready["methods"], serde_json::json!(["echo", "ping"]));

        process
            .input
            .write(&send(serde_json::json!({
                "type": "call", "id": "c-1", "method": "ping", "params": null,
                "caller": {"kind": "agent", "actingFor": "cairn://p/cairn/1/1/builder"},
                "deadlineMs": chrono::Utc::now().timestamp_millis() + 30_000
            })))
            .await
            .unwrap();
        let answer = next_stdout_frame(&mut process.events, &mut stderr).await;
        assert_eq!(answer["id"], "c-1");
        assert_eq!(
            answer["value"],
            serde_json::json!({"pong": true, "source": "s-1"}),
            "{answer}"
        );

        process
            .input
            .write(&send(serde_json::json!({"type": "drain"})))
            .await
            .unwrap();
        assert_eq!(
            next_stdout_frame(&mut process.events, &mut stderr).await["type"],
            "drained"
        );
        host.release(&orch, "p", &ServiceOwner::Project, "echo")
            .await;
    }

    /// The structural half of confinement: the process is a pipe, never a
    /// listener, and always confined.
    #[test]
    fn a_service_process_is_a_confined_pipe() {
        let package = crate::project_services::declaration::package_from_files(
            "echo",
            vec![
                ("service.yaml".into(), b"name: Echo\n".to_vec()),
                ("main.ts".into(), b"serve({})".to_vec()),
            ],
        )
        .unwrap();
        let spec = LaunchSpec {
            project_id: "p".into(),
            project_key: "cairn".into(),
            owner: ServiceOwner::Project,
            service: "echo".into(),
            generation: 1,
            declaration: package.declaration,
            files: package.files,
            env: vec![("CAIRN_SERVICE".into(), "echo".into())],
        };
        let selector = service_selector(&spec);
        assert_eq!(
            selector.name.as_deref(),
            Some(cairn_common::executor_protocol::LOCAL_EXECUTOR_NAME),
            "a declaration that names no machine runs on the runner's"
        );
        let mut remote = spec.clone();
        remote.declaration.executor = Some(cairn_common::executor_protocol::ExecutorSelector {
            os: Some("linux".into()),
            required_toolchains: vec!["uv".into()],
            ..Default::default()
        });
        let selector = service_selector(&remote);
        assert_eq!(selector.name, None);
        assert_eq!(selector.os.as_deref(), Some("linux"));
        assert_eq!(selector.required_toolchains, vec!["uv".to_string()]);
        remote.declaration.executor = Some(cairn_common::executor_protocol::ExecutorSelector {
            name: Some("machine-b".into()),
            ..Default::default()
        });
        assert_eq!(service_selector(&remote).name.as_deref(), Some("machine-b"));
        let launch = process_for(&spec);
        assert_eq!(launch.program, "bun");
        assert_eq!(launch.sandbox_mode, ProcessSandboxMode::Confined);
        assert!(launch.args[1].contains("/service/main.ts"));
        let process = launch.spec();
        assert_eq!(process.io, ResidentProcessIoMode::Pipe);
        assert!(process.sandbox_policy.is_none());
    }

    /// A thread's service is launched by the same path as a project's: the
    /// same confined pipe and the same placement rule. It holds a lease of its own, never its project namesake's.
    #[test]
    fn a_thread_service_process_is_the_same_confined_pipe_on_its_own_lease() {
        let uri = "cairn://p/cairn/spanish/services/echo";
        let package = crate::project_services::thread_store::package(
            "echo",
            uri,
            &serde_json::json!({"files": {"main.ts": "serve({})"}}),
        )
        .unwrap();
        let owner = ServiceOwner::Thread {
            id: "t-1".into(),
            name: "spanish".into(),
        };
        let spec = LaunchSpec {
            project_id: "p".into(),
            project_key: "cairn".into(),
            owner: owner.clone(),
            service: "echo".into(),
            generation: 1,
            declaration: package.declaration,
            files: package.files,
            env: vec![("CAIRN_SERVICE".into(), "echo".into())],
        };
        let launch = process_for(&spec);
        assert_eq!(launch.sandbox_mode, ProcessSandboxMode::Confined);
        assert_eq!(launch.spec().io, ResidentProcessIoMode::Pipe);
        assert!(launch.spec().sandbox_policy.is_none());
        assert_eq!(
            service_selector(&spec).name.as_deref(),
            Some(cairn_common::executor_protocol::LOCAL_EXECUTOR_NAME)
        );
        let elsewhere = crate::project_services::thread_store::package(
            "echo",
            uri,
            &serde_json::json!({"files": {"main.ts": "serve({})"}, "manifest": {"executor": {"os": "linux"}}}),
        )
        .unwrap();
        let mut remote = spec.clone();
        remote.declaration = elsewhere.declaration;
        assert_eq!(service_selector(&remote).os.as_deref(), Some("linux"));
        assert_ne!(
            holder_id("p", &owner, "echo"),
            holder_id("p", &ServiceOwner::Project, "echo")
        );
        assert_eq!(holder_label(&spec), "cairn/spanish/echo");
    }
}
