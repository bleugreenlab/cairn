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
    }
}

/// Where a service may run today: the runner's own machine.
///
/// A service reaches Cairn at the runner's loopback callback, and only batches
/// are relayed from a remote executor, not resident processes. A service on
/// another machine would send its bearer to whatever listens on that port
/// there. So placement is pinned to the colocated executor, refined by any
/// toolchains the declaration asks for, and a declaration that names another
/// machine or a platform refuses to start rather than run unreachable.
fn runner_local_selector(
    spec: &LaunchSpec,
) -> Result<cairn_common::executor_protocol::ExecutorSelector, String> {
    use cairn_common::executor_protocol::{ExecutorSelector, LOCAL_EXECUTOR_NAME};
    let declared = spec.declaration.executor.clone().unwrap_or_default();
    let elsewhere = declared
        .name
        .as_deref()
        .is_some_and(|name| name != LOCAL_EXECUTOR_NAME)
        || declared.os.is_some();
    if elsewhere {
        return Err(format!(
            "service `{}` asks to run on another machine or platform; services run on the \
             runner's own machine until resident processes reach the callback relay \
             (cairn/5562). Remove `executor.name`/`executor.os` from its declaration, and run \
             the heavy part of a method as a `ctx.cairn.run` batch with an `executor` selector \
             (`{{os}}` or `{{name}}`), which runs anywhere today",
            spec.service
        ));
    }
    Ok(ExecutorSelector {
        name: Some(LOCAL_EXECUTOR_NAME.to_string()),
        required_toolchains: declared.required_toolchains,
        ..ExecutorSelector::default()
    })
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
        let selector = runner_local_selector(spec)?;
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
    async fn launch(&self, orch: &Orchestrator, spec: &LaunchSpec) -> Result<HostProcess, String> {
        let launch = launch_for(spec)?;
        let lease = self.lease(orch, spec).await?;
        // Generation-keyed, so a replacement starts beside the generation it
        // replaces rather than over it.
        let key = format!("svc-{}", spec.generation);
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
        let selector = runner_local_selector(&spec).unwrap();
        assert_eq!(
            selector.name.as_deref(),
            Some(cairn_common::executor_protocol::LOCAL_EXECUTOR_NAME)
        );
        let mut remote = spec.clone();
        remote.declaration.executor = Some(cairn_common::executor_protocol::ExecutorSelector {
            os: Some("linux".into()),
            ..Default::default()
        });
        assert!(runner_local_selector(&remote).is_err());
        let launch = process_for(&spec);
        assert_eq!(launch.program, "bun");
        assert_eq!(launch.sandbox_mode, ProcessSandboxMode::Confined);
        assert!(launch.args[1].contains("/service/main.ts"));
        let process = launch.spec();
        assert_eq!(process.io, ResidentProcessIoMode::Pipe);
        assert!(process.sandbox_policy.is_none());
    }

    /// A thread's service is launched by the same path as a project's: the
    /// same confined pipe, the same runner-local pin, whatever its payload
    /// asks for. It holds a lease of its own, never its project namesake's.
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
            runner_local_selector(&spec).unwrap().name.as_deref(),
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
        assert!(runner_local_selector(&remote).is_err());
        assert_ne!(
            holder_id("p", &owner, "echo"),
            holder_id("p", &ServiceOwner::Project, "echo")
        );
        assert_eq!(holder_label(&spec), "cairn/spanish/echo");
    }
}
