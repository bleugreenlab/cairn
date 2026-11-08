//! The production [`ServiceHost`]: a service process is a resident process
//! under a service lease, one lease per (project, owner, service), reused by
//! every incarnation so a rolling replace runs both generations in one cell.

use async_trait::async_trait;
use cairn_common::executor_protocol::{
    OwnerDeathPolicy, ProcessSandboxMode, ResidencyFootprint, ResidentProcessCwdRoot,
    ResidentProcessEventKind, ResidentProcessStatus, ResidentProcessStream, ResidentRuntimeAsset,
    ResidentStreamFraming,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use super::supervisor::{HostEvent, HostInput, HostProcess, LaunchError, LaunchSpec, ServiceHost};
use super::ServiceOwner;
use crate::fleet::service_placement::{
    acquire_service_lease, ResidentLaunch, ServiceIdentity, ServiceLease, ServicePlacementError,
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
        ServiceOwner::Pack { pack, machine, .. } => format!("{pack}/{service}@{machine}"),
    }
}

/// What a person calls the residency in a running list.
fn holder_label(spec: &LaunchSpec) -> String {
    match &spec.owner {
        ServiceOwner::Project => format!("{}/{}", spec.project_key, spec.service),
        ServiceOwner::Thread { name, .. } => {
            format!("{}/{name}/{}", spec.project_key, spec.service)
        }
        ServiceOwner::Pack { pack, machine, .. } => {
            format!("{pack}/{} on {machine}", spec.service)
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
    if let Some(native) = &spec.native {
        return Ok(native_launch(native));
    }
    let mut launch = process_for(spec);
    launch.runtime_assets = runtime_assets(spec)?;
    Ok(launch)
}

/// A machine service's resident: its pinned executable, unconfined like every
/// helper a lease ran before project services, because it exists to reach the
/// machine (Axon talks to the accessibility daemon's socket). It carries no
/// environment of Cairn's and no relay: a native resident holds no bearer.
fn native_launch(native: &super::declaration::NativeLaunch) -> ResidentLaunch {
    ResidentLaunch::command(native.program.clone(), native.args.clone())
}

fn process_for(spec: &LaunchSpec) -> ResidentLaunch {
    let loader = format!(
        "await import(process.env.CAIRN_RUNTIME_ASSETS + \"/service/{}\")",
        spec.declaration
            .entry()
            .unwrap_or(super::declaration::DEFAULT_ENTRY)
            .replace('"', "\\\"")
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
        // The service protocol is one JSON frame per stdout line. That holds by
        // `serve()`'s convention, not by construction: a raw write to fd 1 lands
        // here too, and a multi-line secret in it would be released in pieces
        // (cairn/5852).
        stdout_framing: ResidentStreamFraming::Lines,
    }
}

/// Where a service may run: its declaration's `executor`, by machine name,
/// platform, and toolchains, exactly as a `run` selector would place it.
///
/// A declaration that names a machine is held to it. One that does not leaves
/// the choice to the fleet, which ranks a services-only machine first, then
/// the runner's own, then any other only when the forecast says home is
/// clearly slower (the `services` route of the placement profile). Since the
/// resident is a `bun` process, an unnamed selector also requires the `bun`
/// toolchain, so the fleet never chooses a machine whose probe did not
/// measure one.
///
/// A service elsewhere reaches Cairn through its executor's resident relay,
/// with bearers bound to that relay, so placement is a choice and not a pin.
fn service_selector(spec: &LaunchSpec) -> cairn_common::executor_protocol::ExecutorSelector {
    use cairn_common::executor_protocol::ExecutorSelector;
    if let ServiceOwner::Pack { machine, .. } = &spec.owner {
        return crate::fleet::service_placement::named_executor(machine);
    }
    let declared = spec.declaration.executor.clone().unwrap_or_default();
    let mut required_toolchains = declared.required_toolchains;
    if declared.name.is_none() && !required_toolchains.iter().any(|name| name == "bun") {
        required_toolchains.push("bun".to_string());
    }
    ExecutorSelector {
        name: declared.name,
        os: declared.os,
        required_toolchains,
        ..ExecutorSelector::default()
    }
}

/// How long a service free to run anywhere waits, once in a runner's life, for
/// the colocated executor to attach before placing without it.
const FIRST_COLOCATED_ATTACH_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// What the supervisor is told about a refused placement or start. Only a
/// refusal the substrate made without judging the service is `Unplaced`: a
/// residency the fleet would not grant, or a start it could not route.
fn launch_error(error: ServicePlacementError) -> LaunchError {
    match &error {
        ServicePlacementError::Unroutable(_) | ServicePlacementError::Ungranted(_) => {
            LaunchError::Unplaced(error.to_string())
        }
        _ => LaunchError::Failed(error.to_string()),
    }
}

impl LeaseHost {
    async fn lease(
        &self,
        orch: &Orchestrator,
        spec: &LaunchSpec,
    ) -> Result<Arc<ServiceLease>, LaunchError> {
        let id = holder_id(&spec.project_id, &spec.owner, &spec.service);
        if let Some(lease) = self.leases.lock().await.get(&id) {
            return Ok(lease.clone());
        }
        let selector = service_selector(spec);
        // Held outside the map's lock: a runner whose executor never comes makes
        // each service wait out the bound, not every service in turn.
        if selector.name.is_none() {
            orch.fleet
                .wait_for_first_colocated_attach(FIRST_COLOCATED_ATTACH_WAIT)
                .await;
        }
        let mut leases = self.leases.lock().await;
        if let Some(lease) = leases.get(&id) {
            return Ok(lease.clone());
        }
        let label = holder_label(spec);
        // A machine service claims nothing, as desktop automation did: it is
        // the machine's own program, not work admitted onto the machine.
        let footprint = if spec.native.is_some() {
            ResidencyFootprint {
                memory_bytes: 0,
                disk_growth_bytes: 0,
            }
        } else {
            ResidencyFootprint {
                memory_bytes: spec.declaration.limits.memory_mb as u64 * 1024 * 1024,
                disk_growth_bytes: 64 * 1024 * 1024,
            }
        };
        let lease = Arc::new(
            acquire_service_lease(
                orch,
                ServiceIdentity {
                    id: &id,
                    label: &label,
                },
                selector,
                footprint,
                OwnerDeathPolicy {
                    heartbeat_timeout_ms: 90_000,
                    reclaim_grace_ms: 30_000,
                },
            )
            .await
            .map_err(launch_error)?,
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
    ) -> Result<crate::fleet::residency::ResidentCallback, LaunchError> {
        let lease = self.lease(orch, spec).await?;
        Ok(crate::fleet::residency::resident_callback(
            orch,
            lease.executor_id().as_deref(),
        ))
    }

    async fn launch(
        &self,
        orch: &Orchestrator,
        spec: &LaunchSpec,
    ) -> Result<HostProcess, LaunchError> {
        let mut launch = launch_for(spec)?;
        let lease = self.lease(orch, spec).await?;
        // Generation-keyed, so a replacement starts beside the generation it
        // replaces rather than over it.
        let key = format!("svc-{}", spec.generation);
        // On another machine the service calls back through its executor's
        // relay, which accepts only bearers of this incarnation's runs. A
        // native resident has neither a bearer nor a relay.
        let relayed = match spec.native {
            Some(_) => None,
            None => match crate::fleet::residency::resident_callback(
                orch,
                lease.executor_id().as_deref(),
            ) {
                crate::fleet::residency::ResidentCallback::Relayed { executor_id } => {
                    Some(executor_id)
                }
                // A service reaches Cairn only through its executor's relay,
                // which keeps its bearer out of its launch environment. With
                // no route there is no relay to name, and the start would
                // carry the bearer in its environment to whichever machine
                // took it, so it waits for a route instead.
                crate::fleet::residency::ResidentCallback::Direct { .. } => {
                    return Err(LaunchError::Unplaced(
                        "no executor routes this service's residency, so it has no callback relay"
                            .into(),
                    ));
                }
            },
        };
        if let Some(executor_id) = relayed {
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
            .map_err(launch_error)?;
        let (sender, events) = mpsc::unbounded_channel();
        let mut machine_lost = lease.machine_lost_signal();
        tokio::spawn(async move {
            loop {
                // Biased: a report and the stream's closure can arrive together,
                // and the supervisor stops reading at the exit the closure becomes.
                let event = tokio::select! {
                    biased;
                    Ok(()) = machine_lost.changed() => {
                        if let Some(executor_id) = machine_lost.borrow_and_update().clone() {
                            let _ = sender.send(HostEvent::MachineLost { executor_id });
                        }
                        continue;
                    }
                    event = subscription.recv() => event,
                };
                let Some(event) = event else { break };
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
                            executor_lost,
                        });
                        return;
                    }
                    ResidentProcessEventKind::State { .. } => {}
                }
            }
            let _ = sender.send(HostEvent::Exited {
                code: None,
                detail: "its event stream closed".to_string(),
                executor_lost: false,
            });
        });
        Ok(HostProcess {
            executor: lease.executor_name(),
            input: Arc::new(LeaseInput { lease, key }),
            events,
        })
    }

    async fn probe(
        &self,
        orch: &Orchestrator,
        spec: &LaunchSpec,
        program: &str,
        args: Vec<String>,
        timeout: std::time::Duration,
    ) -> Result<super::supervisor::ProbeOutput, String> {
        let lease = self
            .lease(orch, spec)
            .await
            .map_err(|error| error.to_string())?;
        let output = lease
            .run_one_shot(program, args, timeout)
            .await
            .map_err(|error| error.to_string())?;
        Ok(super::supervisor::ProbeOutput {
            stdout: output.stdout,
            stderr: output.stderr,
            exit_code: output.exit_code,
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
            native: None,
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
                HostEvent::MachineLost { .. } => {}
                HostEvent::Exited { code, detail, .. } => {
                    panic!("exited ({code:?}, {detail}) before a frame; stderr: {stderr}")
                }
            }
        }
    }

    /// The real path from outage to supervisor: a real resident under a real
    /// lease, its link taken away and aged past the grace, and the real renewal
    /// task finding out. The event reaches the process's event stream with no
    /// executor ever saying anything, because a powered-off machine cannot.
    #[tokio::test]
    async fn a_real_outage_reaches_the_supervisor_as_machine_lost() {
        const LOCAL: &str = crate::fleet::COLOCATED_EXECUTOR_ID;
        let (orch, _config, link) =
            crate::fleet::service_placement::tests::attached_orchestrator_with_link().await;
        let host = LeaseHost::default();
        let mut process = host
            .launch(&orch, &echo_spec())
            .await
            .expect("the service launches");

        assert!(orch.fleet.disconnect_advertised_executor(LOCAL, link));
        orch.fleet
            .stage_link_absent(LOCAL, crate::fleet::ENVIRONMENT_LEASE_GRACE_MS + 1);

        let reported = tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(event) = process.events.recv().await {
                if let HostEvent::MachineLost { executor_id } = event {
                    return Some(executor_id);
                }
            }
            None
        })
        .await
        .expect("renewal finds the outage within a renewal interval");
        assert_eq!(reported.as_deref(), Some(LOCAL));
        host.release(&orch, "p", &ServiceOwner::Project, "echo")
            .await;
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

    /// The service protocol is scrubbed line by line. With a registered value
    /// that begins with a newline, a byte-stream pass withholds the `ready`
    /// frame's terminator and the service sends nothing more to release it.
    #[tokio::test]
    async fn a_service_answers_when_a_registered_secret_begins_with_a_newline() {
        use crate::security::{registry, SecretCategory, SecretId, SecretMaterial};
        let _guard = registry()
            .register(
                SecretId::new("service-test:leading-newline"),
                SecretCategory::CallbackCredential,
                "unit test",
                SecretMaterial::from_bytes(b"\nQ7m2Zx9-RedTeam-service"),
            )
            .expect("registerable");
        let (orch, _config) = crate::fleet::service_placement::tests::attached_orchestrator().await;
        let host = LeaseHost::default();
        let spec = echo_spec();
        let mut process = host
            .launch(&orch, &spec)
            .await
            .expect("the service launches");
        let mut stderr = String::new();
        let mut hello = serde_json::to_vec(&serde_json::json!({
            "type": "hello", "protocol": 1, "project": "cairn", "service": "echo", "source": "s-1"
        }))
        .unwrap();
        hello.push(b'\n');
        process.input.write(&hello).await.unwrap();
        let ready = next_stdout_frame(&mut process.events, &mut stderr).await;
        assert_eq!(ready["type"], "ready", "{ready}");
        host.release(&orch, "p", &ServiceOwner::Project, "echo")
            .await;
    }

    /// A machine service's holder carries `/` and `@`. The storage key splits
    /// on its first `:` only, and the executor names the scratch it keys by the
    /// owner id only after `safe_component`, so both take the id as it is.
    #[test]
    fn a_machine_service_holder_round_trips_through_its_storage_key() {
        use cairn_common::executor_protocol::{RepositoryLocator, ResidencyHolder};
        let id = holder_id("", &ServiceOwner::pack("axon", "bgsrv-mac"), "axon");
        assert_eq!(id, "axon/axon@bgsrv-mac");
        let holder = ResidencyHolder::Service {
            service_id: id.clone(),
        };
        assert_eq!(holder.storage_key(), "service:axon/axon@bgsrv-mac");
        assert_eq!(
            ResidencyHolder::parse_storage_key(&holder.storage_key()),
            Some(holder)
        );
        let scratch = RepositoryLocator::ScratchOnly {
            owner_id: id.clone(),
        };
        assert_eq!(scratch.project_id(), id);
        assert_eq!(scratch.repository_id(), id);
    }

    /// Only the substrate's own refusals reach the supervisor as `Unplaced`:
    /// a start it could not route, and a residency it would not grant.
    #[test]
    fn only_the_substrates_refusals_are_unplaced() {
        for unplaced in [
            ServicePlacementError::Unroutable("lagging".into()),
            ServicePlacementError::Ungranted("no executor is currently attached".into()),
        ] {
            assert!(matches!(launch_error(unplaced), LaunchError::Unplaced(_)));
        }
        for refused in [
            ServicePlacementError::Operation("spawn failed".into()),
            ServicePlacementError::Unavailable("no admission".into()),
            ServicePlacementError::InvalidRequest("bad".into()),
        ] {
            assert!(matches!(launch_error(refused), LaunchError::Failed(_)));
        }
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
            native: None,
        };
        let selector = service_selector(&spec);
        assert_eq!(
            selector,
            cairn_common::executor_protocol::ExecutorSelector {
                required_toolchains: vec!["bun".to_string()],
                ..Default::default()
            },
            "a declaration that names no machine leaves the choice to the fleet, among machines with bun"
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
        assert_eq!(
            selector.required_toolchains,
            vec!["uv".to_string(), "bun".to_string()],
            "declared toolchains are kept, and bun is added once"
        );
        remote.declaration.executor = Some(cairn_common::executor_protocol::ExecutorSelector {
            required_toolchains: vec!["bun".into()],
            ..Default::default()
        });
        assert_eq!(
            service_selector(&remote).required_toolchains,
            vec!["bun".to_string()]
        );
        remote.declaration.executor = Some(cairn_common::executor_protocol::ExecutorSelector {
            name: Some("machine-b".into()),
            ..Default::default()
        });
        let named = service_selector(&remote);
        assert_eq!(
            named.name.as_deref(),
            Some("machine-b"),
            "a named machine still wins"
        );
        assert!(named.required_toolchains.is_empty());
        let launch = process_for(&spec);
        assert_eq!(launch.program, "bun");
        assert_eq!(launch.sandbox_mode, ProcessSandboxMode::Confined);
        assert!(launch.args[1].contains("/service/main.ts"));
        let process = launch.spec();
        assert_eq!(process.io, ResidentProcessIoMode::Pipe);
        assert_eq!(process.stdout_framing, ResidentStreamFraming::Lines);
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
            native: None,
        };
        let launch = process_for(&spec);
        assert_eq!(launch.sandbox_mode, ProcessSandboxMode::Confined);
        assert_eq!(launch.spec().io, ResidentProcessIoMode::Pipe);
        assert!(launch.spec().sandbox_policy.is_none());
        assert_eq!(service_selector(&spec).name, None);
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
