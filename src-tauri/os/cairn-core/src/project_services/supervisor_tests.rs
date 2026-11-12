//! The supervisor against a scripted service.
//!
//! The fake host plays a service over the real protocol: it answers `hello`,
//! serves a few methods, drains, crashes, and hangs on request, and records
//! every frame the runner sends. The database, bearers, gate, and log are real.

use super::*;
use crate::mcp::types::McpCallbackRequest;
use crate::project_services::authority::DelegatedScope;
use crate::project_services::declaration::{package_from_files, ServiceListing};
use crate::project_services::protocol::{CallerKind, Instrument};
use crate::project_services::schedule;
use std::sync::atomic::AtomicU64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Boot {
    Ready,
    /// Exit as soon as `hello` arrives.
    Exit,
}

#[derive(Default)]
pub(crate) struct FakeHost {
    launches: AtomicU64,
    envs: StdMutex<Vec<Vec<(String, String)>>>,
    next_boot: StdMutex<Option<Boot>>,
    frames: StdMutex<Vec<Value>>,
    release_slow: Arc<Notify>,
    /// Refusals the next launches answer, in order, before any launch starts.
    refuse: StdMutex<VecDeque<LaunchError>>,
    /// Refusals the next placements answer, in order, before any bearer is
    /// minted, as a lease host refuses an acquisition.
    refuse_placement: StdMutex<VecDeque<LaunchError>>,
    /// Every native resident started, in order.
    native_launches: StdMutex<Vec<NativeLaunchRecord>>,
    /// Ends the latest native resident on a machine.
    native_exits: StdMutex<HashMap<String, NativeExit>>,
    /// Native residents that have exited, by machine.
    native_ended: StdMutex<Vec<String>>,
    /// What the next probes answer: JSON text, or a failure. `None` answers ready.
    probe_answer: StdMutex<Option<Result<String, String>>>,
    probes: AtomicU64,
    /// The machines the supervisor sees, in place of the fleet's.
    machines: StdMutex<Option<Vec<machine::MachineView>>>,
    /// What the next `scripted` calls answer, in order: a failure with the
    /// error code, or `None` for success.
    scripted: StdMutex<VecDeque<Option<&'static str>>>,
}

/// Ends a native resident: (exit code, whether the executor was lost).
type NativeExit = mpsc::UnboundedSender<(Option<i32>, bool)>;

#[derive(Debug, Clone)]
pub(crate) struct NativeLaunchRecord {
    pub machine: String,
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
}

impl FakeHost {
    /// A native resident: echoes its stdin to its stdout, as a stand-in for
    /// an MCP server, until it is killed or told to exit.
    fn launch_native(self: &Arc<Self>, spec: &LaunchSpec) -> Result<HostProcess, LaunchError> {
        let ServiceOwner::Pack { machine, .. } = &spec.owner else {
            panic!("a native launch is a machine service's");
        };
        let native = spec
            .native
            .clone()
            .expect("a native launch names its program");
        self.native_launches
            .lock()
            .unwrap()
            .push(NativeLaunchRecord {
                machine: machine.clone(),
                program: native.program,
                args: native.args,
                env: spec.env.clone(),
            });
        let (lines, mut input) = mpsc::unbounded_channel::<Option<Vec<u8>>>();
        let (exit, mut exits) = mpsc::unbounded_channel::<(Option<i32>, bool)>();
        self.native_exits
            .lock()
            .unwrap()
            .insert(machine.clone(), exit);
        let (events, receiver) = mpsc::unbounded_channel();
        let (host, ended) = (self.clone(), machine.clone());
        tokio::spawn(async move {
            let (code, detail, executor_lost) = loop {
                tokio::select! {
                    line = input.recv() => match line {
                        Some(Some(bytes)) => { let _ = events.send(HostEvent::Stdout(bytes)); }
                        _ => break (None, "killed".to_string(), false),
                    },
                    exit = exits.recv() => {
                        let (code, lost) = exit.unwrap_or((None, false));
                        break (code, "the process exited".to_string(), lost);
                    }
                }
            };
            host.native_ended.lock().unwrap().push(ended);
            let _ = events.send(HostEvent::Exited {
                code,
                detail,
                executor_lost,
            });
        });
        Ok(HostProcess {
            input: Arc::new(FakeInput { lines }),
            events: receiver,
            executor: machine.clone(),
        })
    }
}

impl FakeHost {
    pub(crate) fn frames_of(&self, kind: &str) -> Vec<Value> {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| frame["type"] == kind)
            .cloned()
            .collect()
    }
}

struct FakeInput {
    lines: mpsc::UnboundedSender<Option<Vec<u8>>>,
}

#[async_trait]
impl HostInput for FakeInput {
    async fn write(&self, bytes: &[u8]) -> Result<(), String> {
        self.lines
            .send(Some(bytes.to_vec()))
            .map_err(|_| "the fake process is gone".to_string())
    }

    async fn kill(&self) {
        let _ = self.lines.send(None);
    }
}

fn out(events: &mpsc::UnboundedSender<HostEvent>, frame: Value) {
    let mut line = serde_json::to_vec(&frame).unwrap();
    line.push(b'\n');
    let _ = events.send(HostEvent::Stdout(line));
}

#[async_trait]
impl ServiceHost for Arc<FakeHost> {
    async fn callback(
        &self,
        orch: &Orchestrator,
        _spec: &LaunchSpec,
    ) -> Result<crate::fleet::residency::ResidentCallback, LaunchError> {
        if let Some(refusal) = self.refuse_placement.lock().unwrap().pop_front() {
            return Err(refusal);
        }
        Ok(crate::fleet::residency::ResidentCallback::Direct {
            url: crate::fleet::residency::runner_callback_url(orch),
        })
    }

    async fn launch(
        &self,
        _orch: &Orchestrator,
        spec: &LaunchSpec,
    ) -> Result<HostProcess, LaunchError> {
        if let Some(refusal) = self.refuse.lock().unwrap().pop_front() {
            return Err(refusal);
        }
        if spec.native.is_some() {
            return self.launch_native(spec);
        }
        self.launches.fetch_add(1, Ordering::AcqRel);
        self.envs.lock().unwrap().push(spec.env.clone());
        let boot = self.next_boot.lock().unwrap().take().unwrap_or(Boot::Ready);
        let generation = spec.generation;
        let (lines, mut input) = mpsc::unbounded_channel::<Option<Vec<u8>>>();
        let (events, receiver) = mpsc::unbounded_channel();
        let host = self.clone();
        tokio::spawn(async move {
            let mut slow: Vec<String> = Vec::new();
            // Calls that answer only once they are cancelled, as a service
            // that ignores cancellation and finishes anyway.
            let mut lingering: Vec<Value> = Vec::new();
            let mut draining = false;
            let finish = |events: &mpsc::UnboundedSender<HostEvent>, code| {
                let _ = events.send(HostEvent::Exited {
                    code,
                    detail: "the process exited".into(),
                    executor_lost: false,
                });
            };
            loop {
                tokio::select! {
                    line = input.recv() => {
                        let Some(Some(bytes)) = line else {
                            finish(&events, None);
                            return;
                        };
                        let frame: Value = serde_json::from_slice(&bytes).unwrap();
                        host.frames.lock().unwrap().push(frame.clone());
                        let id = frame["id"].clone();
                        match frame["type"].as_str().unwrap() {
                            "hello" if boot == Boot::Exit => {
                                let _ = events.send(HostEvent::Stderr(b"boot failed: syntax error\n".to_vec()));
                                finish(&events, Some(1));
                                return;
                            }
                            "hello" => out(&events, json!({"type": "ready", "methods": ["echo", "slow", "crash", "hang"]})),
                            // As the SDK does: a draining service refuses new calls.
                            "call" if draining => out(&events, json!({"type": "result", "id": id, "ok": false, "error": {"message": "the service is draining", "code": "draining"}})),
                            "call" => match frame["method"].as_str().unwrap() {
                                "echo" => out(&events, json!({"type": "result", "id": id, "ok": true, "value": {"params": frame["params"], "generation": generation}})),
                                "slow" => slow.push(id.as_str().unwrap().to_string()),
                                "crash" => {
                                    let _ = events.send(HostEvent::Stderr(b"panic: on purpose\n".to_vec()));
                                    finish(&events, Some(3));
                                    return;
                                }
                                "hang" => {}
                                "scripted" => match host.scripted.lock().unwrap().pop_front().flatten() {
                                    None => out(&events, json!({"type": "result", "id": id, "ok": true, "value": {}})),
                                    Some(code) => out(&events, json!({"type": "result", "id": id, "ok": false, "error": {"message": format!("scripted {code}"), "code": code}})),
                                },
                                "linger" => lingering.push(id),
                                // What a real lease delivers when the machine
                                // is lost and the service is placed elsewhere.
                                "lose" => {
                                    let _ = events.send(HostEvent::MachineLost {
                                        executor_id: "pi1".into(),
                                    });
                                    let _ = events.send(HostEvent::Exited {
                                        code: None,
                                        detail: "its event stream closed".into(),
                                        executor_lost: false,
                                    });
                                    return;
                                }
                                other => out(&events, json!({"type": "result", "id": id, "ok": false, "error": {"message": format!("no {other}"), "code": "unknown_method"}})),
                            },
                            "drain" => {
                                draining = true;
                                if slow.is_empty() {
                                    out(&events, json!({"type": "drained"}));
                                    finish(&events, Some(0));
                                    return;
                                }
                            }
                            "cancel" => {
                                if let Some(at) = lingering.iter().position(|call| frame["id"] == *call) {
                                    lingering.remove(at);
                                    out(&events, json!({"type": "result", "id": id, "ok": true, "value": {"late": true}}));
                                }
                            }
                            _ => {}
                        }
                    }
                    () = host.release_slow.notified() => {
                        for id in slow.drain(..) {
                            out(&events, json!({"type": "result", "id": id, "ok": true, "value": {"slow": true, "generation": generation}}));
                        }
                        if draining {
                            out(&events, json!({"type": "drained"}));
                            finish(&events, Some(0));
                            return;
                        }
                    }
                }
            }
        });
        Ok(HostProcess {
            input: Arc::new(FakeInput { lines }),
            events: receiver,
            executor: "fake".into(),
        })
    }

    async fn probe(
        &self,
        _orch: &Orchestrator,
        _spec: &LaunchSpec,
        _program: &str,
        _args: Vec<String>,
        _timeout: Duration,
    ) -> Result<ProbeOutput, String> {
        self.probes.fetch_add(1, Ordering::AcqRel);
        match self.probe_answer.lock().unwrap().clone() {
            None => Ok(ProbeOutput {
                stdout: br#"{"ready": true}"#.to_vec(),
                stderr: Vec::new(),
                exit_code: Some(0),
            }),
            Some(Ok(json)) => Ok(ProbeOutput {
                stdout: json.into_bytes(),
                stderr: Vec::new(),
                exit_code: Some(0),
            }),
            Some(Err(error)) => Err(error),
        }
    }

    fn machines(&self, orch: &Orchestrator) -> Vec<machine::MachineView> {
        self.machines
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| machine::attached_machines(orch))
    }

    async fn release(
        &self,
        _orch: &Orchestrator,
        _project_id: &str,
        _owner: &ServiceOwner,
        _service: &str,
    ) {
    }
}

/// One package's files, by path.
type PackageFiles = Vec<(String, Vec<u8>)>;

/// Packages held in memory, editable mid-test.
#[derive(Default)]
pub(crate) struct FakeSource {
    packages: RwLock<HashMap<String, PackageFiles>>,
    /// An `on_demand` idle stop shorter than a manifest may declare, so a
    /// test can watch one happen.
    idle: StdMutex<Option<Duration>>,
    /// A thread whose source reads wait until the test adds a permit.
    held: StdMutex<Option<(String, Arc<tokio::sync::Semaphore>)>>,
    /// The installed packs, in place of the workspace's.
    packs: StdMutex<Vec<(String, Arc<dyn ServiceSource>)>>,
}

impl FakeSource {
    fn build(&self, id: &str, files: PackageFiles) -> Result<ServicePackage, String> {
        let mut package = package_from_files(id, files)?;
        if let Some(idle) = *self.idle.lock().unwrap() {
            if package.declaration.idle_stop_after.is_some() {
                package.declaration.idle_stop_after = Some(idle);
            }
        }
        Ok(package)
    }
}

impl FakeSource {
    pub(crate) fn put(&self, id: &str, manifest: &str, main: &str) {
        self.packages.write().unwrap().insert(
            id.into(),
            vec![
                ("service.yaml".into(), manifest.as_bytes().to_vec()),
                ("main.ts".into(), main.as_bytes().to_vec()),
            ],
        );
    }
}

impl ServiceSource for FakeSource {
    fn revision(&self) -> &str {
        "fake"
    }

    fn list(&self) -> Result<Vec<ServiceListing>, String> {
        let packages = self.packages.read().unwrap();
        let mut ids: Vec<&String> = packages.keys().collect();
        ids.sort();
        Ok(ids
            .into_iter()
            .map(|id| ServiceListing {
                id: id.clone(),
                declaration: self.build(id, packages[id].clone()).map(|p| p.declaration),
            })
            .collect())
    }

    fn package(&self, id: &str) -> Result<Option<ServicePackage>, String> {
        self.packages
            .read()
            .unwrap()
            .get(id)
            .map(|files| self.build(id, files.clone()))
            .transpose()
    }
}

pub(crate) struct FakeProvider(pub(crate) Arc<FakeSource>);

#[async_trait]
impl SourceProvider for FakeProvider {
    async fn open(
        &self,
        orch: &Orchestrator,
        project_id: &str,
        owner: &ServiceOwner,
    ) -> Result<Arc<dyn ServiceSource>, String> {
        let held = self
            .0
            .held
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(thread, _)| owner.thread_id() == Some(thread.as_str()))
            .map(|(_, gate)| gate.clone());
        if let Some(gate) = held {
            let _ = gate.acquire().await;
        }
        match owner {
            ServiceOwner::Project => Ok(self.0.clone()),
            // A thread's services come from the real store, so a test writes
            // them the way a thread does.
            ServiceOwner::Thread { .. } => OwnerSources.open(orch, project_id, owner).await,
            ServiceOwner::Pack { pack, machine, .. } => {
                machine::open_source(orch, self.packs(orch), pack, machine)
            }
        }
    }

    fn packs(&self, _orch: &Orchestrator) -> Vec<(String, Arc<dyn ServiceSource>)> {
        self.0.packs.lock().unwrap().clone()
    }
}

pub(crate) const ECHO: &str = "name: Echo\nlimits:\n  callTimeoutSeconds: 5\n";

/// Install a scripted host and an in-memory source declaring `echo`.
pub(crate) fn install_fakes(orch: &Orchestrator) -> (Arc<FakeHost>, Arc<FakeSource>) {
    let host = Arc::new(FakeHost::default());
    let source = Arc::new(FakeSource::default());
    source.put("echo", ECHO, "serve({})");
    orch.project_services.supervisor.install_for_test(
        Arc::new(host.clone()),
        Arc::new(FakeProvider(source.clone())),
    );
    (host, source)
}

struct Harness {
    orch: Orchestrator,
    host: Arc<FakeHost>,
    source: Arc<FakeSource>,
    _config: tempfile::TempDir,
}

async fn harness() -> Harness {
    let (orch, config) = crate::fleet::service_placement::tests::attached_orchestrator().await;
    crate::project_services::testing::seed_project(&orch.db.local).await;
    let host = Arc::new(FakeHost::default());
    let source = Arc::new(FakeSource::default());
    source.put("echo", ECHO, "serve({})");
    orch.project_services.supervisor.install_for_test(
        Arc::new(host.clone()),
        Arc::new(FakeProvider(source.clone())),
    );
    Harness {
        orch,
        host,
        source,
        _config: config,
    }
}

fn agent() -> CallerFrame {
    CallerFrame {
        kind: CallerKind::Agent,
        acting_for: "cairn://p/cairn/12/1/builder".into(),
        agent: Some(cairn_common::identity::PrincipalRef::Agent {
            node: "cairn://p/cairn/12/1/builder".into(),
            run_id: Some("caller-run".into()),
        }),
        viewer: None,
        instrument: None,
    }
}

fn page() -> CallerFrame {
    CallerFrame {
        kind: CallerKind::Page,
        acting_for: "cairn://p/cairn/prep".into(),
        agent: None,
        viewer: Some(json!({"kind": "desktop_operator"})),
        instrument: Some(Instrument {
            page: "cairn://p/cairn/prep/pages/ide".into(),
            version: 3,
        }),
    }
}

fn request(service: &str, method: &str, caller: CallerFrame) -> CallRequest {
    CallRequest {
        project_id: "p".into(),
        project_key: "cairn".into(),
        owner: ServiceOwner::Project,
        service: service.into(),
        method: method.into(),
        params: json!({"hi": 1}),
        caller,
        timeout: None,
    }
}

async fn eventually(what: &str, check: impl Fn() -> bool) {
    for _ in 0..500 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

fn status(h: &Harness, service: &str) -> ServiceStatus {
    h.orch
        .project_services
        .supervisor
        .status("p", &ServiceOwner::Project, service)
        .expect("the service has a cell")
}

async fn log_texts(h: &Harness, service: &str) -> Vec<String> {
    crate::project_services::log::list(&h.orch.db.local, "p", "", service, None, None, 200)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.text)
        .collect()
}

fn local() -> crate::security::lease::LeaseAudience {
    crate::mcp::run_bearer::audience_for_host("127.0.0.1")
}

fn authority_of(frame: &Value) -> (String, String) {
    (
        frame["authority"]["runId"].as_str().unwrap().to_string(),
        frame["authority"]["token"].as_str().unwrap().to_string(),
    )
}

#[tokio::test]
async fn a_call_starts_the_service_answers_and_revokes_its_bearer() {
    let h = harness().await;
    let value = call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    assert_eq!(value, json!({"params": {"hi": 1}, "generation": 1}));
    let status = status(&h, "echo");
    assert_eq!(status.state, ServiceState::Running);
    assert_eq!(status.generation, Some(1));
    assert_eq!(status.recent_calls.len(), 1);

    let hello = &h.host.frames_of("hello")[0];
    assert_eq!(hello["protocol"], 1);
    assert_eq!(hello["service"], "echo");
    let call_frame = &h.host.frames_of("call")[0];
    assert_eq!(call_frame["caller"]["agent"]["run_id"], "caller-run");
    let (run_id, token) = authority_of(call_frame);
    assert!(
        matches!(
            h.orch.run_bearers.verify(&token, &local()),
            Err(crate::mcp::run_bearer::BearerRefusal::Revoked { .. })
        ),
        "a call's bearer must be revoked when the call returns"
    );
    assert!(h.orch.project_services.runs.get(&run_id).is_none());

    let log = log_texts(&h, "echo").await;
    assert!(
        log.iter()
            .any(|text| text.starts_with("started generation 1")),
        "{log:?}"
    );
    assert!(
        log.iter().any(|text| text.starts_with("echo from agent")),
        "{log:?}"
    );
}

/// A service's credential speaks for its own standing run and nothing else:
/// never the machine MCP secret, which accepts any run id its holder claims.
#[tokio::test]
async fn a_service_holds_a_bearer_for_its_own_run_never_the_machine_secret() {
    let h = harness().await;
    call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    let env = h.host.envs.lock().unwrap()[0].clone();
    let var = |key: &str| {
        env.iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    };
    let credential = var("CAIRN_MCP_SECRET").expect("the SDK's credential variable is set");
    if let Ok(machine) = h.orch.mcp_auth.get_secret_for_mcp() {
        assert_ne!(credential, machine);
    }
    let standing = status(&h, "echo").run_id.unwrap();
    assert_eq!(var("CAIRN_RUN_ID"), Some(standing.clone()));
    assert_eq!(
        h.orch
            .run_bearers
            .verify(&credential, &local())
            .unwrap()
            .run_id(),
        Some(standing.as_str())
    );
    assert_eq!(var("CAIRN_SERVICE").as_deref(), Some("echo"));
    assert!(
        var("CAIRN_HOME").is_none(),
        "no pointer to the machine secret's directory"
    );

    // Stopping ends the standing run, and its bearer with it.
    stop(
        &h.orch,
        &ServiceRef::project("p", "cairn", "echo"),
        "operator",
    )
    .await
    .unwrap();
    eventually("the standing run to end", || {
        h.orch.project_services.runs.get(&standing).is_none()
    })
    .await;
    assert!(h.orch.run_bearers.verify(&credential, &local()).is_err());
}

/// A caller that goes away mid-call (a client timeout, a stopped agent) must
/// not strand the call's delegated credential: the call runs to its own end.
#[tokio::test]
async fn a_dropped_caller_still_ends_its_call_and_revokes_its_bearer() {
    let h = harness().await;
    let orch = h.orch.clone();
    let caller = tokio::spawn(async move { call(&orch, request("echo", "slow", agent())).await });
    eventually("the slow call", || h.host.frames_of("call").len() == 1).await;
    let (run_id, token) = authority_of(&h.host.frames_of("call")[0]);
    caller.abort();
    let _ = caller.await;
    // Still in flight inside the service, still authorized for its call.
    assert!(h.orch.project_services.runs.get(&run_id).is_some());
    h.host.release_slow.notify_waiters();
    eventually("the call to end", || {
        h.orch.project_services.runs.get(&run_id).is_none()
    })
    .await;
    assert!(matches!(
        h.orch.run_bearers.verify(&token, &local()),
        Err(crate::mcp::run_bearer::BearerRefusal::Revoked { .. })
    ));
    assert_eq!(status(&h, "echo").pending_calls, 0);
}

/// A call's deadline starts once the service is serving, so delegation and
/// admission spend it too: a deadline can pass before the frame is sent
/// (nothing reaches the service) or after (the service is told to cancel).
/// Both answer `deadline`; only the message names the phase.
const DEADLINE_BEFORE_SEND: &str = "the call's deadline passed before it was sent";
const DEADLINE_AFTER_SEND: &str = "did not finish within";

fn calls_of(h: &Harness, method: &str) -> Vec<Value> {
    h.host
        .frames_of("call")
        .into_iter()
        .filter(|frame| frame["method"] == method)
        .collect()
}

/// The `method` call frame as the service received it. A call that ends
/// before any such frame arrives never reached the service, and the panic
/// says which phase its deadline passed in rather than leaving an empty
/// frame list to index.
async fn dispatched(
    h: &Harness,
    method: &str,
    caller: &mut tokio::task::JoinHandle<Result<Value, CallError>>,
) -> Value {
    let began = Instant::now();
    loop {
        if let Some(frame) = calls_of(h, method).into_iter().next() {
            return frame;
        }
        if caller.is_finished() {
            // The host records frames on its own task: look once more.
            tokio::task::yield_now().await;
            if let Some(frame) = calls_of(h, method).into_iter().next() {
                return frame;
            }
            let outcome = caller.await.unwrap();
            panic!(
                "the `{method}` call ended after {:?} without reaching the service \
                 (phase: {}): {outcome:?}; frames seen: {:?}",
                began.elapsed(),
                match &outcome {
                    Err(error) if error.message == DEADLINE_BEFORE_SEND =>
                        "deadline before dispatch",
                    Err(error) if error.message.contains(DEADLINE_AFTER_SEND) =>
                        "deadline after dispatch",
                    _ => "not a deadline",
                },
                h.host.frames.lock().unwrap(),
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Every frame sent to `service` before this returns has been recorded: the
/// fake host reads its input in order, so an answered call comes after them.
async fn flush(h: &Harness, service: &str) {
    call(&h.orch, request(service, "echo", agent()))
        .await
        .unwrap();
}

/// A deadline that passes after the call frame was sent cancels the call in
/// the service and revokes the bearer the frame carried. A service that
/// answers anyway, or calls back with that bearer, is not heard.
#[tokio::test]
async fn a_deadline_after_dispatch_cancels_the_call_and_revokes_its_bearer() {
    let h = harness().await;
    h.source.put(
        "patient",
        "name: Patient\nlimits:\n  callTimeoutSeconds: 30\n",
        "serve({})",
    );
    // Long enough that delegation and admission cannot plausibly spend it,
    // so the frame is sent; `dispatched` fails by phase if it is not.
    let method = "linger";
    let mut linger = request("patient", method, agent());
    linger.timeout = Some(Duration::from_secs(30));
    let orch = h.orch.clone();
    let mut caller = tokio::spawn(async move { call(&orch, linger).await });
    let call_frame = dispatched(&h, method, &mut caller).await;
    let (run_id, token) = authority_of(&call_frame);
    assert!(
        h.orch.run_bearers.verify(&token, &local()).is_ok(),
        "the dispatched call's bearer is live while it runs"
    );

    // The frame was sent, so the call can only end by an answer (`linger`
    // gives none until cancelled) or by its post-send timer. Paused, the clock jumps
    // to that timer once the runtime idles, instead of 30 real seconds on.
    tokio::time::pause();
    let outcome = caller.await.unwrap();
    tokio::time::resume();
    let error = outcome.unwrap_err();
    assert_eq!(error.code, "deadline", "{error:?}");
    assert!(
        error.message.contains(DEADLINE_AFTER_SEND),
        "a dispatched call times out after sending: {error:?}"
    );

    flush(&h, "patient").await;
    let cancels = h.host.frames_of("cancel");
    assert_eq!(cancels.len(), 1, "{cancels:?}");
    assert_eq!(cancels[0]["id"], call_frame["id"]);
    assert_eq!(
        calls_of(&h, method).len(),
        1,
        "a timed-out call is not resent"
    );
    assert!(matches!(
        h.orch.run_bearers.verify(&token, &local()),
        Err(crate::mcp::run_bearer::BearerRefusal::Revoked { .. })
    ));
    assert!(h.orch.project_services.runs.get(&run_id).is_none());
    assert_eq!(
        count(
            &h,
            &format!("SELECT COUNT(*) FROM runs WHERE id = '{run_id}' AND status = 'exited'")
        )
        .await,
        1
    );
}

/// A starved orchestrator fires its timers late while the wall clock runs on.
/// A call still live there keeps a bearer the store accepts, however far past
/// its deadline the clock has gone; ending the call is what ends the bearer.
#[tokio::test]
async fn a_live_call_s_bearer_holds_past_its_deadline_until_its_run_ends() {
    let h = harness().await;
    let orch = h.orch.clone();
    let mut caller =
        tokio::spawn(async move { call(&orch, request("echo", "slow", agent())).await });
    let frame = dispatched(&h, "slow", &mut caller).await;
    let (run_id, token) = authority_of(&frame);

    // Echo's deadline is 5 s; two minutes on is far past it and past the
    // 30 s grace the bearer once expired at, and the call has not ended.
    h.orch
        .run_bearers
        .advance_clock(Duration::from_secs(5 + 120));
    let verified = h
        .orch
        .run_bearers
        .verify(&token, &local())
        .expect("a live call's bearer is good past its deadline");
    assert_eq!(verified.run_id(), Some(run_id.as_str()));

    h.host.release_slow.notify_waiters();
    caller.await.unwrap().unwrap();
    // Ended, and refused at once: the clock is still well inside the backstop.
    assert!(matches!(
        h.orch.run_bearers.verify(&token, &local()),
        Err(crate::mcp::run_bearer::BearerRefusal::Revoked { .. })
    ));
}

/// A call whose end never came is still bounded: past the backstop its
/// bearer is refused as expired, though its run was never ended.
#[tokio::test]
async fn a_call_bearer_past_its_backstop_is_refused_though_its_run_never_ended() {
    let h = harness().await;
    let orch = h.orch.clone();
    let mut caller =
        tokio::spawn(async move { call(&orch, request("echo", "slow", agent())).await });
    let frame = dispatched(&h, "slow", &mut caller).await;
    let (run_id, token) = authority_of(&frame);

    h.orch
        .run_bearers
        .advance_clock(Duration::from_secs(5) + BEARER_BACKSTOP + Duration::from_secs(1));
    assert!(!h.orch.run_bearers.run_is_revoked(&run_id));
    let refusal = h.orch.run_bearers.verify(&token, &local()).unwrap_err();
    assert!(
        matches!(
            &refusal,
            crate::mcp::run_bearer::BearerRefusal::Expired { .. }
        ),
        "{refusal:?}"
    );

    h.host.release_slow.notify_waiters();
    let _ = caller.await;
}

/// A runtime that stalls past a call's deadline wakes to the service's answer
/// before its own timer. The service's write during the stall lands, and the
/// call is recorded as the deadline it overran, not as what the service said.
#[tokio::test]
async fn an_answer_taken_after_the_deadline_is_recorded_as_the_deadline() {
    let h = harness().await;
    let method = "slow";
    let mut brief = request("echo", method, agent());
    brief.timeout = Some(Duration::from_secs(1));
    let orch = h.orch.clone();
    let mut caller = tokio::spawn(async move { call(&orch, brief).await });
    let frame = dispatched(&h, method, &mut caller).await;
    let (_, token) = authority_of(&frame);

    // The service answers, and then the runtime stalls: nothing on it runs,
    // its timers included, while the wall clock passes the deadline.
    h.host.release_slow.notify_waiters();
    std::thread::sleep(Duration::from_millis(1_300));
    // The service's own write, a minute on by the store's clock, lands.
    h.orch.run_bearers.advance_clock(Duration::from_secs(60));
    assert!(h.orch.run_bearers.verify(&token, &local()).is_ok());

    let error = caller.await.unwrap().unwrap_err();
    assert_eq!(error.code, "deadline", "{error:?}");
    assert!(
        error.message.contains("overran its deadline by") && error.message.contains("answered ok"),
        "the overrun is measured and the service's answer kept as detail: {error:?}"
    );
    let recent = status(&h, "echo").recent_calls;
    let last = recent.last().unwrap();
    assert_eq!(last.outcome, "deadline");
    assert!(last.ran_ms >= 1_000, "{last:?}");
    let log = log_texts(&h, "echo").await;
    assert!(
        log.iter()
            .any(|text| text.starts_with("slow from agent") && text.contains("error (deadline)")),
        "{log:?}"
    );
}

#[test]
fn an_answer_is_the_outcome_only_when_taken_by_the_deadline() {
    let deadline = Instant::now();
    let late = deadline + Duration::from_millis(41_000);
    assert_eq!(
        settle_answer(Ok(json!(1)), deadline, deadline).unwrap(),
        json!(1)
    );
    let failed = Err(CallError::new("store", "Cairn write failed (HTTP 403)"));
    assert_eq!(
        settle_answer(failed.clone(), deadline, deadline)
            .unwrap_err()
            .code,
        "store"
    );
    let overran = settle_answer(failed, deadline, late).unwrap_err();
    assert_eq!(overran.code, "deadline");
    assert!(
        overran.message.contains("by 41000 ms")
            && overran
                .message
                .contains("failed `store`: Cairn write failed (HTTP 403)"),
        "{overran:?}"
    );
}

/// A call's row says how long it waited to be served apart from how long it
/// ran, so a slow placement is never read as a deadline that did not fire.
#[tokio::test]
async fn a_call_row_carries_its_wait_and_its_run_apart() {
    let h = harness().await;
    let orch = h.orch.clone();
    let began = Instant::now();
    let mut caller =
        tokio::spawn(async move { call(&orch, request("echo", "slow", agent())).await });
    dispatched(&h, "slow", &mut caller).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    h.host.release_slow.notify_waiters();
    caller.await.unwrap().unwrap();
    let total = began.elapsed().as_millis() as u64;

    let recent = status(&h, "echo").recent_calls;
    let row = recent.last().unwrap();
    assert!(row.ran_ms >= 300, "{row:?}");
    assert!(
        row.waited_ms + row.ran_ms <= total,
        "{row:?} within {total} ms"
    );
    let log = log_texts(&h, "echo").await;
    let line = log
        .iter()
        .find(|text| text.starts_with("slow from agent"))
        .unwrap_or_else(|| panic!("{log:?}"));
    assert!(
        line.contains(&format!("waited {} ms to be served", row.waited_ms))
            && line.contains(&format!("ran {} ms", row.ran_ms)),
        "{line}"
    );
}

#[tokio::test]
async fn a_crash_fails_pending_calls_then_restarts_at_the_next_generation() {
    let h = harness().await;
    call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    let orch = h.orch.clone();
    let pending = tokio::spawn(async move { call(&orch, request("echo", "slow", agent())).await });
    eventually("the slow call", || h.host.frames_of("call").len() == 2).await;

    let crash = call(&h.orch, request("echo", "crash", agent()))
        .await
        .unwrap_err();
    assert_eq!(crash.code, "service_crashed");
    assert_eq!(pending.await.unwrap().unwrap_err().code, "service_crashed");
    assert_eq!(status(&h, "echo").state, ServiceState::Backoff);

    eventually("the restart", || {
        status(&h, "echo").state == ServiceState::Running
    })
    .await;
    assert_eq!(status(&h, "echo").generation, Some(2));
    assert_eq!(status(&h, "echo").restarts, 1);
    let value = call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    assert_eq!(value["generation"], 2);
    let log = log_texts(&h, "echo").await;
    assert!(
        log.iter().any(|text| text.contains("crashed (exit code 3")),
        "{log:?}"
    );
    assert!(
        log.iter().any(|text| text == "panic: on purpose"),
        "{log:?}"
    );
}

#[tokio::test]
async fn five_crashes_inside_the_window_fail_the_service() {
    let h = harness().await;
    let cell =
        h.orch
            .project_services
            .supervisor
            .cell("p", "cairn", &ServiceOwner::Project, "echo");
    for crash in 0..CRASH_LIMIT {
        crashed(&h.orch, &cell, crash as u64, "crashed on purpose").await;
    }
    assert_eq!(status(&h, "echo").state, ServiceState::Failed);
    let error = call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap_err();
    assert_eq!(error.code, "service_failed");

    // A source change clears the failure, so the next call starts fresh.
    h.source.put("echo", ECHO, "serve({ fixed: true })");
    apply_source(&h.orch, "p", &ServiceOwner::Project, "base advance", None).await;
    assert_eq!(status(&h, "echo").state, ServiceState::Stopped);
    call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
}

/// Entering `failed` stops the service and its schedule, so its owning thread
/// is told, once per entry.
#[tokio::test]
async fn entering_failed_wakes_the_owning_thread() {
    let h = thread_harness().await;
    h.orch
        .db
        .local
        .execute(
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
             VALUES ('spanish-run', 'p', 'spanish-session', 'completed', 1, 1)",
            (),
        )
        .await
        .unwrap();
    let cell = h
        .orch
        .project_services
        .supervisor
        .cell("p", "cairn", &spanish(), "tutor");
    const FAILED_NOTICES: &str =
        "SELECT COUNT(*) FROM messages WHERE content LIKE 'Service `tutor` is failed%'";
    for crash in 0..CRASH_LIMIT {
        assert_eq!(count(&h, FAILED_NOTICES).await, 0, "not before it fails");
        crashed(&h.orch, &cell, crash as u64, "crashed on purpose").await;
    }
    assert_eq!(tutor().status(&h.orch).unwrap().state, ServiceState::Failed);
    assert_eq!(count(&h, FAILED_NOTICES).await, 1);
    let notice = h
        .orch
        .db
        .local
        .query_text("SELECT content FROM messages", ())
        .await
        .unwrap()
        .unwrap();
    assert!(notice.contains("crashed on purpose"), "{notice}");
    assert!(notice.contains(&format!("{TUTOR}/status")), "{notice}");
}

#[tokio::test]
async fn a_source_change_replaces_without_dropping_an_in_flight_call() {
    let h = harness().await;
    call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    let first_hash = status(&h, "echo").source_hash.unwrap();
    let orch = h.orch.clone();
    let in_flight =
        tokio::spawn(async move { call(&orch, request("echo", "slow", agent())).await });
    eventually("the slow call", || h.host.frames_of("call").len() == 2).await;

    h.source.put("echo", ECHO, "serve({ v: 2 })");
    apply_source(&h.orch, "p", &ServiceOwner::Project, "base advance", None).await;
    let status_now = status(&h, "echo");
    assert_eq!(status_now.generation, Some(2));
    assert_ne!(status_now.source_hash.unwrap(), first_hash);
    // Retirement runs beside the new generation, so its drain lands shortly.
    eventually("generation 1 to drain", || {
        h.host.frames_of("drain").len() == 1
    })
    .await;

    // New calls go to the new generation while the old one finishes.
    let value = call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    assert_eq!(value["generation"], 2);
    h.host.release_slow.notify_waiters();
    let slow = in_flight.await.unwrap().unwrap();
    assert_eq!(slow, json!({"slow": true, "generation": 1}));

    // An unchanged source is a no-op.
    apply_source(&h.orch, "p", &ServiceOwner::Project, "base advance", None).await;
    assert_eq!(status(&h, "echo").generation, Some(2));
    assert_eq!(h.host.launches.load(Ordering::Acquire), 2);
}

/// The window the rolling replace must not fail a call in: a call chose
/// generation 1, a replacement landed, and only then was the frame sent.
#[tokio::test]
async fn a_call_that_reaches_a_draining_generation_is_answered_by_the_new_one() {
    let h = harness().await;
    call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    let cell =
        h.orch
            .project_services
            .supervisor
            .cell("p", "cairn", &ServiceOwner::Project, "echo");
    let first = cell.serving().unwrap();
    // Keep generation 1 alive and draining past the replacement.
    let orch = h.orch.clone();
    let in_flight =
        tokio::spawn(async move { call(&orch, request("echo", "slow", agent())).await });
    eventually("the slow call", || h.host.frames_of("call").len() == 2).await;
    h.source.put("echo", ECHO, "serve({ v: 2 })");
    apply_source(&h.orch, "p", &ServiceOwner::Project, "base advance", None).await;
    eventually("generation 1 to drain", || {
        h.host.frames_of("drain").len() == 1
    })
    .await;

    *cell.force_first_pick.lock().unwrap() = Some(first);
    let value = call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    assert_eq!(
        value["generation"], 2,
        "re-routed to the serving generation"
    );
    let echoes: Vec<Value> = h
        .host
        .frames_of("call")
        .into_iter()
        .filter(|frame| frame["method"] == "echo")
        .collect();
    assert_eq!(
        echoes.len(),
        3,
        "once to generation 1 (refused), once to generation 2"
    );
    assert_eq!(
        echoes[1]["authority"]["runId"], echoes[2]["authority"]["runId"],
        "one delegated run for the whole call"
    );
    h.host.release_slow.notify_waiters();
    assert!(in_flight.await.unwrap().is_ok());
}

#[tokio::test]
async fn a_replacement_that_never_becomes_ready_keeps_the_old_generation() {
    let h = harness().await;
    call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    *h.host.next_boot.lock().unwrap() = Some(Boot::Exit);
    h.source.put("echo", ECHO, "serve({ broken");
    apply_source(&h.orch, "p", &ServiceOwner::Project, "base advance", None).await;

    let status = status(&h, "echo");
    assert_eq!(status.state, ServiceState::UpdateFailed);
    assert!(status.last_error.unwrap().contains("failed to start"));
    let value = call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    assert_eq!(value["generation"], 1, "generation 1 keeps serving");
    let log = log_texts(&h, "echo").await;
    assert!(
        log.iter().any(|text| text == "boot failed: syntax error"),
        "{log:?}"
    );
}

#[tokio::test]
async fn outside_a_call_a_service_reaches_only_its_home_and_the_refusal_is_logged() {
    let h = harness().await;
    call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    let standing = status(&h, "echo").run_id.unwrap();
    let read = |paths: Value| McpCallbackRequest {
        tool: "read_batch".into(),
        payload: json!({ "paths": paths }),
        run_id: Some(standing.clone()),
        ..Default::default()
    };
    assert!(authority::refuse(&h.orch, &read(json!(["cairn:~/status"])))
        .await
        .is_none());
    let refusal = authority::refuse(&h.orch, &read(json!(["cairn:@/issues"])))
        .await
        .expect("a standing read outside the home is refused");
    assert!(refusal.contains("own account"), "{refusal}");
    let log = log_texts(&h, "echo").await;
    assert!(
        log.iter().any(|text| text.starts_with("refused `read_batch`") && text.contains("cairn:@/issues")),
        "{log:?}"
    );
}

#[tokio::test]
async fn a_page_call_reaches_only_its_threads_database_and_its_refusals_are_logged() {
    let h = harness().await;
    let orch = h.orch.clone();
    let in_flight = tokio::spawn(async move { call(&orch, request("echo", "slow", page())).await });
    eventually("the page call", || h.host.frames_of("call").len() == 1).await;
    let frame = &h.host.frames_of("call")[0];
    assert_eq!(frame["caller"]["kind"], "page");
    assert_eq!(frame["caller"]["instrument"]["version"], 3);
    let (run_id, _) = authority_of(frame);
    let delegated = h.orch.project_services.runs.get(&run_id).unwrap();
    assert!(matches!(
        delegated.role,
        ServiceRunRole::Delegated {
            scope: DelegatedScope::PageCall { .. },
            ..
        }
    ));
    // What `judge` does with a page's submission: a code batch, admitted at
    // the gate (placement then decides where it may run).
    let attempt = |tool: &str, payload: Value| McpCallbackRequest {
        tool: tool.into(),
        payload,
        run_id: Some(run_id.clone()),
        ..Default::default()
    };
    assert_eq!(
        authority::refuse(
            &h.orch,
            &attempt(
                "run",
                json!({"commands": [{"code": "print(1)", "interpreter": "python"}]})
            )
        )
        .await,
        None
    );
    // The same code reaching for a file is refused, naming the page bound.
    let refusal = authority::refuse(
        &h.orch,
        &attempt("read_batch", json!({"paths": ["file:src/lib.rs"]})),
    )
    .await
    .unwrap();
    assert!(
        refusal.contains("serving a page may not read `file:src/lib.rs`")
            && refusal.contains("verdict-only page-call batch"),
        "{refusal}"
    );
    let committing = authority::refuse(
        &h.orch,
        &attempt(
            "run",
            json!({"commands": [{"command": "true"}], "commit_msg": "x"}),
        ),
    )
    .await
    .unwrap();
    assert!(
        committing.contains("serving a page may not commit"),
        "{committing}"
    );
    h.host.release_slow.notify_waiters();
    assert!(in_flight.await.unwrap().is_ok());
    let log = log_texts(&h, "echo").await;
    assert!(
        log.iter()
            .any(|text| text.starts_with("refused `read_batch`")),
        "{log:?}"
    );
    assert!(
        log.iter().any(|text| text.starts_with("refused `run`")),
        "{log:?}"
    );
    assert!(
        log.iter().any(|text| text.starts_with("slow from page")),
        "{log:?}"
    );
}

#[tokio::test]
async fn an_undeclared_service_and_oversized_params_are_refused() {
    let h = harness().await;
    assert_eq!(
        call(&h.orch, request("absent", "echo", agent()))
            .await
            .unwrap_err()
            .code,
        "unknown_service"
    );
    let mut big = request("echo", "echo", agent());
    big.params = json!("x".repeat(MAX_PARAMS_BYTES + 1));
    assert_eq!(
        call(&h.orch, big).await.unwrap_err().code,
        "params_too_large"
    );
    assert_eq!(h.host.launches.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn a_launch_service_starts_with_its_declaration_and_stops_with_its_removal() {
    let h = harness().await;
    h.source
        .put("watcher", "name: Watcher\nstart: launch\n", "serve({})");
    apply_source(&h.orch, "p", &ServiceOwner::Project, "runner start", None).await;
    assert_eq!(status(&h, "watcher").state, ServiceState::Running);
    assert!(
        h.orch
            .project_services
            .supervisor
            .status("p", &ServiceOwner::Project, "echo")
            .is_none(),
        "on_demand waits for a call"
    );

    h.source.packages.write().unwrap().remove("watcher");
    apply_source(&h.orch, "p", &ServiceOwner::Project, "base advance", None).await;
    assert_eq!(status(&h, "watcher").state, ServiceState::Stopped);
    assert_eq!(h.host.frames_of("drain").len(), 1);
}

// ---- Thread-owned services (cairn/5524) ----

const TUTOR: &str = "cairn://p/cairn/spanish/services/tutor";

fn spanish() -> ServiceOwner {
    ServiceOwner::Thread {
        id: "t-1".into(),
        name: "spanish".into(),
    }
}

fn tutor() -> ServiceRef {
    ServiceRef {
        project_id: "p".into(),
        project_key: "cairn".into(),
        owner: spanish(),
        service: "tutor".into(),
    }
}

/// The scripted host with thread `spanish` (id `t-1`) and its session job.
async fn thread_harness() -> Harness {
    let h = harness().await;
    h.orch
        .db
        .local
        .execute_script(
            "INSERT INTO threads (id, project_id, name, created_at, updated_at)
               VALUES ('t-1', 'p', 'spanish', 1, 1);
             INSERT INTO threads (id, project_id, name, created_at, updated_at)
               VALUES ('t-2', 'p', 'french', 1, 1);
             INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status,
                               created_at, updated_at, uri_segment)
               VALUES ('spanish-session', 't-1', 'p', 'thread', 'thread', 'running', 1, 1, 'thread');
             INSERT INTO jobs (id, thread_id, project_id, node_name, agent_config_id, status,
                               created_at, updated_at, uri_segment)
               VALUES ('french-session', 't-2', 'p', 'thread', 'thread', 'running', 1, 1, 'thread');",
        )
        .await
        .unwrap();
    h
}

/// Write a version of `tutor` as `writer_job` would, then deploy it as the
/// resource write does.
async fn write_tutor(
    h: &Harness,
    writer_job: &str,
    payload: Value,
    is_patch: bool,
) -> Result<Option<ServiceStatus>, String> {
    crate::project_services::thread_store::write(
        &h.orch.db.local,
        "p",
        &spanish(),
        "tutor",
        TUTOR,
        &crate::project_services::thread_store::ServiceWriter {
            job_id: writer_job.into(),
        },
        &payload,
        is_patch,
        false,
    )
    .await?;
    Ok(deploy(&h.orch, &tutor(), "cairn://p/cairn/spanish").await)
}

fn main_ts(body: &str) -> Value {
    json!({"title": "Spanish tutor", "files": {"main.ts": body}})
}

fn thread_agent(acting_for: &str) -> CallerFrame {
    CallerFrame {
        kind: CallerKind::Agent,
        acting_for: acting_for.into(),
        agent: Some(cairn_common::identity::PrincipalRef::Agent {
            node: acting_for.into(),
            run_id: Some("caller-run".into()),
        }),
        viewer: None,
        instrument: None,
    }
}

fn thread_page() -> CallerFrame {
    CallerFrame {
        kind: CallerKind::Page,
        acting_for: "cairn://p/cairn/spanish".into(),
        agent: None,
        viewer: Some(json!({"kind": "desktop_operator"})),
        instrument: Some(Instrument {
            page: "cairn://p/cairn/spanish/pages/lesson".into(),
            version: 1,
        }),
    }
}

fn tutor_request(method: &str, caller: CallerFrame) -> CallRequest {
    CallRequest {
        owner: spanish(),
        ..request("tutor", method, caller)
    }
}

async fn tutor_log(h: &Harness) -> Vec<String> {
    crate::project_services::log::list(&h.orch.db.local, "p", "t-1", "tutor", None, None, 200)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.text)
        .collect()
}

/// The write is the deploy: no call, no start, and the service is running.
/// Its standing run acts under its own home beneath the thread.
#[tokio::test]
async fn a_thread_writes_a_service_and_the_write_starts_it() {
    let h = thread_harness().await;
    let status = write_tutor(&h, "spanish-session", main_ts("serve({})"), false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, ServiceState::Running);
    assert_eq!(status.generation, Some(1));
    // An on_demand project service of the same name is untouched.
    assert!(h
        .orch
        .project_services
        .supervisor
        .status("p", &ServiceOwner::Project, "tutor")
        .is_none());

    let standing = status.run_id.unwrap();
    assert_eq!(
        h.orch.project_services.runs.get(&standing).unwrap().home(),
        TUTOR
    );
    let agent =
        crate::mcp::handlers::run_context::authenticated_agent_for_run(&h.orch.db, &standing)
            .await
            .unwrap();
    assert_eq!(
        agent.home, TUTOR,
        "the run's lineage resolves to the thread's service"
    );

    let value = call(
        &h.orch,
        tutor_request("echo", thread_agent("cairn://p/cairn/spanish")),
    )
    .await
    .unwrap();
    assert_eq!(value["generation"], 1);
    let log = tutor_log(&h).await;
    assert!(
        log.iter()
            .any(|text| text.starts_with("started generation 1")),
        "{log:?}"
    );
    assert!(
        crate::project_services::log::list(&h.orch.db.local, "p", "", "tutor", None, None, 10)
            .await
            .unwrap()
            .is_empty(),
        "the thread's service logs under its owner"
    );
}

/// A version that never becomes ready leaves the previous generation
/// serving, with the failure in `/status` and the process's stderr in `/log`.
#[tokio::test]
async fn a_boot_error_keeps_the_previous_generation_serving() {
    let h = thread_harness().await;
    write_tutor(&h, "spanish-session", main_ts("serve({})"), false)
        .await
        .unwrap();
    *h.host.next_boot.lock().unwrap() = Some(Boot::Exit);
    let status = write_tutor(
        &h,
        "spanish-session",
        json!({"files": {"main.ts": "serve({ broken"}}),
        true,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(status.state, ServiceState::UpdateFailed);
    assert_eq!(status.generation, Some(1), "generation 1 keeps serving");
    assert!(status.last_error.unwrap().contains("failed to start"));
    let value = call(
        &h.orch,
        tutor_request("echo", thread_agent("cairn://p/cairn/spanish")),
    )
    .await
    .unwrap();
    assert_eq!(value["generation"], 1);
    let log = tutor_log(&h).await;
    assert!(
        log.iter().any(|text| text == "boot failed: syntax error"),
        "{log:?}"
    );
    assert!(
        log.iter().any(|text| text.contains("failed to start")),
        "{log:?}"
    );

    // The next good write replaces it.
    let fixed = write_tutor(
        &h,
        "spanish-session",
        json!({"files": {"main.ts": "serve({ v: 3 })"}}),
        true,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fixed.state, ServiceState::Running);
    assert_eq!(fixed.generation, Some(3));
}

/// Only the owning thread writes its service, and only callers acting for
/// that thread call it; a call's delegation is bounded to the owner's
/// database, and a page's call runs code only as a page-call batch.
#[tokio::test]
async fn a_thread_service_serves_only_its_thread_and_each_call_acts_for_it() {
    let h = thread_harness().await;
    let stolen = write_tutor(&h, "french-session", main_ts("serve({})"), false)
        .await
        .unwrap_err();
    assert!(stolen.contains("not this run's to write"), "{stolen}");
    crate::project_services::thread_store::write(
        &h.orch.db.local,
        "p",
        &spanish(),
        "tutor",
        TUTOR,
        &crate::project_services::thread_store::ServiceWriter {
            job_id: "spanish-session".into(),
        },
        &main_ts("serve({})"),
        false,
        false,
    )
    .await
    .unwrap();

    for outsider in ["cairn://p/cairn/french", "cairn://p/cairn/12/1/builder"] {
        let refused = call(&h.orch, tutor_request("echo", thread_agent(outsider)))
            .await
            .unwrap_err();
        assert_eq!(refused.code, "not_owner", "{outsider}");
    }
    assert_eq!(
        h.host.launches.load(Ordering::Acquire),
        0,
        "a refused caller never starts the service"
    );
    let log = tutor_log(&h).await;
    assert!(
        log.iter()
            .any(|text| text.starts_with("refused `echo` from agent cairn://p/cairn/french")),
        "{log:?}"
    );

    // A page of the thread: its call reaches the owner's database and nothing else.
    let orch = h.orch.clone();
    let in_flight =
        tokio::spawn(async move { call(&orch, tutor_request("slow", thread_page())).await });
    eventually("the page call", || h.host.frames_of("call").len() == 1).await;
    let (run_id, _) = authority_of(&h.host.frames_of("call")[0]);
    let delegated = h.orch.project_services.runs.get(&run_id).unwrap();
    assert_eq!(delegated.home(), TUTOR);
    assert!(matches!(
        &delegated.role,
        ServiceRunRole::Delegated {
            scope: DelegatedScope::PageCall { thread },
            ..
        } if thread == "spanish"
    ));
    let attempt = |tool: &str, payload: Value| McpCallbackRequest {
        tool: tool.into(),
        payload,
        run_id: Some(run_id.clone()),
        ..Default::default()
    };
    let pinned = authority::refuse(
        &h.orch,
        &attempt(
            "run",
            json!({"commands": [{"code": "print(1)", "interpreter": "python"}], "executor": {"name": "local"}}),
        ),
    )
    .await
    .unwrap();
    assert!(
        pinned.contains("serving a page may not run a batch with `executor`"),
        "{pinned}"
    );
    assert!(authority::refuse(
        &h.orch,
        &attempt(
            "write",
            json!({"changes": [{"target": "cairn://p/cairn/french/db/notes", "mode": "create"}]})
        )
    )
    .await
    .is_some());
    assert!(authority::refuse(
        &h.orch,
        &attempt(
            "write",
            json!({"changes": [{"target": "cairn://p/cairn/spanish/db/notes", "mode": "create"}]})
        )
    )
    .await
    .is_none());
    h.host.release_slow.notify_waiters();
    assert!(in_flight.await.unwrap().is_ok());

    // A task of the thread calls as an agent whose writable thread is the owner.
    let orch = h.orch.clone();
    let in_flight = tokio::spawn(async move {
        call(
            &orch,
            tutor_request("slow", thread_agent("cairn://p/cairn/spanish/task/drill")),
        )
        .await
    });
    eventually("the task call", || h.host.frames_of("call").len() == 2).await;
    let (run_id, _) = authority_of(&h.host.frames_of("call")[1]);
    assert!(matches!(
        &h.orch.project_services.runs.get(&run_id).unwrap().role,
        ServiceRunRole::Delegated {
            scope: DelegatedScope::Owner { thread },
            ..
        } if thread == "spanish"
    ));
    h.host.release_slow.notify_waiters();
    assert!(in_flight.await.unwrap().is_ok());
}

/// An explicit stop outlasts a write: the new version waits for a start.
#[tokio::test]
async fn a_write_does_not_undo_an_explicit_stop() {
    let h = thread_harness().await;
    write_tutor(&h, "spanish-session", main_ts("serve({})"), false)
        .await
        .unwrap();
    stop(&h.orch, &tutor(), "cairn://p/cairn/spanish")
        .await
        .unwrap();
    let status = write_tutor(
        &h,
        "spanish-session",
        json!({"files": {"main.ts": "serve({ v: 2 })"}}),
        true,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(status.sticky_stop);
    assert_eq!(status.state, ServiceState::Stopped);
    assert_eq!(h.host.launches.load(Ordering::Acquire), 1);
    start(&h.orch, &tutor(), "cairn://p/cairn/spanish")
        .await
        .unwrap();
    let value = call(
        &h.orch,
        tutor_request("echo", thread_agent("cairn://p/cairn/spanish")),
    )
    .await
    .unwrap();
    assert_eq!(value["generation"], 2);
}

// ---- Schedules and the idle stop (cairn/5611) ----

/// The smallest scheduled service a thread can write: one file and one
/// schedule entry; title, entry, start, limits, and env all default.
fn scheduled(method: &str) -> Value {
    json!({
        "files": {"main.ts": "serve({})"},
        "manifest": {"schedule": [{"every": "5m", "method": method}]}
    })
}

async fn count(h: &Harness, sql: &str) -> i64 {
    h.orch
        .db
        .local
        .query_text(format!("SELECT CAST(({sql}) AS TEXT)"), ())
        .await
        .unwrap()
        .unwrap()
        .parse()
        .unwrap()
}

/// A scheduled method is a call from its thread, made by the runner: it runs
/// once per occurrence, writes the thread's own database and no other, is
/// logged like any call, and starts no turn and wakes no one.
#[tokio::test]
async fn a_scheduled_method_runs_as_its_thread_with_no_turn() {
    let h = thread_harness().await;
    write_tutor(&h, "spanish-session", scheduled("slow"), false)
        .await
        .unwrap();
    let rows = schedule::list(&h.orch.db.local, "p", &spanish(), "tutor")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the write records the schedule");
    let due = rows[0].anchor_at;
    assert_eq!(rows[0].method, "slow");
    assert_eq!(rows[0].every_ms, 5 * 60_000);
    assert_eq!(
        rows[0].next_due_at(due - 1),
        Some(due),
        "the next due time is one interval after the write"
    );
    assert_eq!(run_due(&h.orch, &tutor(), due - 1).await, 0);

    let orch = h.orch.clone();
    let tick = tokio::spawn(async move { run_due(&orch, &tutor(), due).await });
    eventually("the scheduled call", || h.host.frames_of("call").len() == 1).await;
    let frame = h.host.frames_of("call")[0].clone();
    assert_eq!(frame["method"], "slow");
    assert_eq!(frame["caller"]["kind"], "schedule");
    assert_eq!(frame["caller"]["actingFor"], "cairn://p/cairn/spanish");

    // The call's authority is the owner's: its database, not another thread's.
    let (run_id, _) = authority_of(&frame);
    assert!(matches!(
        &h.orch.project_services.runs.get(&run_id).unwrap().role,
        ServiceRunRole::Delegated {
            scope: DelegatedScope::Owner { thread },
            ..
        } if thread == "spanish"
    ));
    let write_to = |target: &str| McpCallbackRequest {
        tool: "write".into(),
        payload: json!({"changes": [{"target": target, "mode": "create"}]}),
        run_id: Some(run_id.clone()),
        ..Default::default()
    };
    assert!(
        authority::refuse(&h.orch, &write_to("cairn://p/cairn/spanish/db/releases"))
            .await
            .is_none(),
        "a scheduled method writes its owner's table"
    );
    assert!(
        authority::refuse(&h.orch, &write_to("cairn://p/cairn/french/db/releases"))
            .await
            .is_some(),
        "and not another thread's"
    );
    // `cairn:~` inside a service is the service's own home, which has no
    // database: the write is refused, never landed somewhere else.
    let home_relative = authority::refuse(&h.orch, &write_to("cairn:~/db/releases"))
        .await
        .expect("a home-relative database write from a service is refused");
    assert!(
        home_relative.contains("calling thread's database"),
        "{home_relative}"
    );
    // A batch the method starts carries the call's run (`CAIRN_RUN_ID`), so the
    // SDK calls inside it are held to this same delegation. The owning thread's
    // own call runs `code` items and its tool targets (MCP tools, a machine
    // service's included), each logged; a workflow is still refused.
    let run_of = |item: Value| McpCallbackRequest {
        tool: "run".into(),
        payload: json!({"commands": [item]}),
        run_id: Some(run_id.clone()),
        ..Default::default()
    };
    assert!(authority::refuse(
        &h.orch,
        &run_of(json!({"code": "console.log(1)", "interpreter": "typescript"}))
    )
    .await
    .is_none());
    for target in [
        "cairn://mcp/axon/look",
        "cairn://mcp/cairn-bugs/list_reports",
    ] {
        assert!(
            authority::refuse(&h.orch, &run_of(json!({"target": target})))
                .await
                .is_none(),
            "the owning thread's scheduled method invokes {target}"
        );
    }
    let workflow = authority::refuse(
        &h.orch,
        &run_of(json!({"target": "cairn://workflows/release"})),
    )
    .await
    .expect("a workflow is not a tool target");
    assert!(workflow.contains("and nothing else"), "{workflow}");
    let log = tutor_log(&h).await;
    for target in [
        "cairn://mcp/axon/look",
        "cairn://mcp/cairn-bugs/list_reports",
    ] {
        assert!(
            log.iter()
                .any(|text| text == &format!("target `{target}` for cairn://p/cairn/spanish")),
            "every tool target is a log row: {log:?}"
        );
    }
    h.host.release_slow.notify_waiters();
    assert_eq!(tick.await.unwrap(), 1);

    // One occurrence runs once, however often it is asked.
    assert_eq!(run_due(&h.orch, &tutor(), due).await, 0);
    assert_eq!(h.host.frames_of("call").len(), 1);
    let row = &schedule::list(&h.orch.db.local, "p", &spanish(), "tutor")
        .await
        .unwrap()[0];
    assert_eq!(row.last_status.as_deref(), Some("ok"));
    assert_eq!(row.last_fired_occurrence_at, Some(due));
    assert_eq!(row.next_due_at(due), Some(due + 300));
    let log = tutor_log(&h).await;
    assert!(
        log.iter()
            .any(|text| text.starts_with("slow from schedule cairn://p/cairn/spanish → ok")),
        "{log:?}"
    );

    // No turn and no wake: nothing was delivered to the thread, and its
    // session has no run.
    assert_eq!(count(&h, "SELECT COUNT(*) FROM messages").await, 0);
    assert_eq!(count(&h, "SELECT COUNT(*) FROM attention_pushes").await, 0);
    assert_eq!(
        count(
            &h,
            "SELECT COUNT(*) FROM runs WHERE job_id = 'spanish-session'"
        )
        .await,
        0
    );
}

/// A failing schedule tells its thread once, when the streak begins; later
/// failures only log, and the streak's count is on the row `/status` reads.
#[tokio::test]
async fn the_first_failure_of_a_streak_wakes_the_thread_and_the_next_does_not() {
    let h = thread_harness().await;
    h.orch
        .db
        .local
        .execute(
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
             VALUES ('spanish-run', 'p', 'spanish-session', 'completed', 1, 1)",
            (),
        )
        .await
        .unwrap();
    write_tutor(&h, "spanish-session", scheduled("missing"), false)
        .await
        .unwrap();
    let due = schedule::list(&h.orch.db.local, "p", &spanish(), "tutor")
        .await
        .unwrap()[0]
        .anchor_at;
    let notices = "SELECT COUNT(*) FROM messages WHERE content LIKE '%started failing%'";

    assert_eq!(run_due(&h.orch, &tutor(), due).await, 1);
    assert_eq!(count(&h, notices).await, 1, "the first failure is news");
    let notice = h
        .orch
        .db
        .local
        .query_text("SELECT content FROM messages", ())
        .await
        .unwrap()
        .unwrap();
    assert!(notice.contains("`missing` (every 5m)"), "{notice}");
    assert!(
        notice.contains("only notice for this failure streak"),
        "{notice}"
    );
    assert!(notice.contains(&format!("{TUTOR}/status")), "{notice}");

    assert_eq!(run_due(&h.orch, &tutor(), due + 300).await, 1);
    assert_eq!(count(&h, notices).await, 1, "the second failure only logs");
    let row = &schedule::list(&h.orch.db.local, "p", &spanish(), "tutor")
        .await
        .unwrap()[0];
    assert_eq!(row.consecutive_failures, 2);
    assert_eq!(row.last_status.as_deref(), Some("failed"));
    assert!(row
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("unknown_method")));
}

/// `tutor` scheduled to call `scripted`, each occurrence answering the next of
/// `outcomes`, with a session run for its notices to wake. Answers the first
/// due occurrence.
async fn scripted_tutor(h: &Harness, outcomes: &[Option<&'static str>]) -> i64 {
    h.orch
        .db
        .local
        .execute(
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
             VALUES ('spanish-run', 'p', 'spanish-session', 'completed', 1, 1)",
            (),
        )
        .await
        .unwrap();
    h.host.scripted.lock().unwrap().extend(outcomes);
    write_tutor(h, "spanish-session", scheduled("scripted"), false)
        .await
        .unwrap();
    tutor_row(h).await.anchor_at
}

/// Run the occurrences from `first`, one per five-minute interval.
async fn run_occurrences(h: &Harness, first: i64, from: usize, to: usize) {
    for n in from..to {
        assert_eq!(run_due(&h.orch, &tutor(), first + 300 * n as i64).await, 1);
    }
}

async fn streak_notices(h: &Harness) -> Vec<String> {
    use crate::storage::RowExt as _;
    h.orch
        .db
        .local
        .query_all(
            "SELECT content FROM messages WHERE content LIKE '%started failing%' ORDER BY rowid",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
}

/// A known failure alternating with slow successes, as under a substrate
/// incident, wakes the thread once per kind of failure rather than once per
/// streak; the second notice names both kinds; the row counts every streak
/// and every failed occurrence.
#[tokio::test]
async fn streaks_of_a_reported_kind_are_quiet_and_a_new_kind_is_news() {
    let h = thread_harness().await;
    let first = scripted_tutor(
        &h,
        &[
            Some("deadline"),
            None,
            Some("deadline"),
            Some("deadline"),
            None,
            Some("store"),
        ],
    )
    .await;

    run_occurrences(&h, first, 0, 4).await;
    assert_eq!(
        streak_notices(&h).await.len(),
        1,
        "fail, succeed, fail with the same kind is one notice"
    );

    run_occurrences(&h, first, 4, 6).await;
    let notices = streak_notices(&h).await;
    assert_eq!(notices.len(), 2, "a new kind is news: {notices:?}");
    assert!(
        notices[1].contains("not reported before, `store`"),
        "{}",
        notices[1]
    );
    assert!(
        notices[1].contains("(kinds: `deadline`, `store`)"),
        "{}",
        notices[1]
    );
    assert!(
        notices[1].contains("3 streaks, 4 failed occurrences"),
        "{}",
        notices[1]
    );

    let row = tutor_row(&h).await;
    assert_eq!(row.consecutive_failures, 1);
    let window = row.notice_window.unwrap();
    assert_eq!((window.streaks, window.failures), (3, 4));
    assert_eq!(
        tutor_log(&h)
            .await
            .iter()
            .filter(|text| text.contains("scripted"))
            .count(),
        6,
        "every occurrence is still logged"
    );
}

/// The owner acting on the service ends the quiet window: an explicit
/// restart, and a manifest-only rewrite, each make the next streak of a
/// reported kind news again.
#[tokio::test]
async fn an_owner_restart_or_rewrite_makes_the_next_streak_news() {
    let h = thread_harness().await;
    let first = scripted_tutor(
        &h,
        &[
            Some("deadline"),
            None,
            Some("deadline"),
            None,
            Some("deadline"),
            None,
            Some("deadline"),
        ],
    )
    .await;

    run_occurrences(&h, first, 0, 2).await;
    assert_eq!(streak_notices(&h).await.len(), 1);
    restart(&h.orch, &tutor(), "cairn://p/cairn/spanish")
        .await
        .unwrap();
    run_occurrences(&h, first, 2, 4).await;
    let notices = streak_notices(&h).await;
    assert_eq!(notices.len(), 2, "after a restart: {notices:?}");
    assert!(
        !notices[1].contains("The window before"),
        "a one-streak window has nothing to add: {}",
        notices[1]
    );

    // A manifest-only rewrite: the schedule entry, and so its row, is kept.
    let mut rewritten = scheduled("scripted");
    rewritten["description"] = json!("a longer call timeout");
    write_tutor(&h, "spanish-session", rewritten, false)
        .await
        .unwrap();
    run_occurrences(&h, first, 4, 5).await;
    assert_eq!(streak_notices(&h).await.len(), 3, "after a manifest write");

    // With nothing done since, the same kind is quiet again.
    run_occurrences(&h, first, 5, 7).await;
    assert_eq!(streak_notices(&h).await.len(), 3);
}

/// What comes next depends on whether the service is free to move, and the
/// notice says which.
#[test]
fn a_lost_machine_notice_says_what_the_service_does_next() {
    let moves = machine_loss_notice("bugs", "pi1", "cairn://p/cairn/bug-monitor", false);
    assert!(moves.contains("next machine placement prefers"), "{moves}");
    let waits = machine_loss_notice("bugs", "pi1", "cairn://p/cairn/bug-monitor", true);
    assert!(waits.contains("waits for it to come back"), "{waits}");
    assert!(!waits.contains("placement prefers"), "{waits}");
}

/// An explicit stop pauses a schedule rather than failing it.
#[tokio::test]
async fn a_stopped_service_skips_its_schedule() {
    let h = thread_harness().await;
    write_tutor(&h, "spanish-session", scheduled("echo"), false)
        .await
        .unwrap();
    stop(&h.orch, &tutor(), "cairn://p/cairn/spanish")
        .await
        .unwrap();
    let due = schedule::list(&h.orch.db.local, "p", &spanish(), "tutor")
        .await
        .unwrap()[0]
        .anchor_at;
    assert_eq!(run_due(&h.orch, &tutor(), due).await, 1);
    assert!(h.host.frames_of("call").is_empty());
    let row = &schedule::list(&h.orch.db.local, "p", &spanish(), "tutor")
        .await
        .unwrap()[0];
    assert_eq!(row.last_status.as_deref(), Some("skipped"));
    assert_eq!(row.consecutive_failures, 0);
    assert_eq!(count(&h, "SELECT COUNT(*) FROM messages").await, 0);
}

/// Removing the schedule from the declaration removes its rows.
#[tokio::test]
async fn a_rewrite_without_the_schedule_drops_it() {
    let h = thread_harness().await;
    write_tutor(&h, "spanish-session", scheduled("echo"), false)
        .await
        .unwrap();
    write_tutor(&h, "spanish-session", main_ts("serve({})"), false)
        .await
        .unwrap();
    assert!(schedule::list(&h.orch.db.local, "p", &spanish(), "tutor")
        .await
        .unwrap()
        .is_empty());
}

// ---- Placement refusals ----

const STREAK_NOTICES: &str = "SELECT COUNT(*) FROM messages WHERE content LIKE '%started failing%'";

/// The refusal of a start the runner could not route to its executor, as the
/// lease host answers it.
fn unroutable() -> LaunchError {
    LaunchError::Unplaced(
        "service process could not be routed to its executor: no connected executor reports \
         this execution environment"
            .into(),
    )
}

/// `tutor` scheduled to call `echo` every five minutes, written but with no
/// process, so its occurrence's call is what starts it; and a session run, so
/// a streak notice has a run to wake. Answers the due occurrence.
async fn scheduled_tutor_with_no_process(h: &Harness) -> i64 {
    h.orch
        .db
        .local
        .execute(
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
             VALUES ('spanish-run', 'p', 'spanish-session', 'completed', 1, 1)",
            (),
        )
        .await
        .unwrap();
    let payload = scheduled("echo");
    crate::project_services::thread_store::write(
        &h.orch.db.local,
        "p",
        &spanish(),
        "tutor",
        TUTOR,
        &crate::project_services::thread_store::ServiceWriter {
            job_id: "spanish-session".into(),
        },
        &payload,
        false,
        false,
    )
    .await
    .unwrap();
    let entries = crate::project_services::thread_store::package("tutor", TUTOR, &payload)
        .unwrap()
        .declaration
        .schedule;
    schedule::sync(
        &h.orch.db.local,
        "p",
        &spanish(),
        "tutor",
        &entries,
        1_000_000,
    )
    .await
    .unwrap();
    tutor_row(h).await.anchor_at
}

async fn tutor_row(h: &Harness) -> ScheduleRow {
    schedule::list(&h.orch.db.local, "p", &spanish(), "tutor")
        .await
        .unwrap()
        .remove(0)
}

fn retrying_rows(log: &[String]) -> usize {
    log.iter()
        .filter(|text| text.contains("could not be placed") && text.contains("; retrying"))
        .count()
}

/// A start the substrate could not route says nothing about the service: the
/// occurrence stays due, the log says it is retrying, `/status` shows it
/// retrying, and the next recheck runs it, with no failure recorded and
/// nobody woken.
#[tokio::test]
async fn a_placement_refusal_is_retried_and_wakes_no_one() {
    let h = thread_harness().await;
    let due = scheduled_tutor_with_no_process(&h).await;
    h.host.refuse.lock().unwrap().push_back(unroutable());

    assert_eq!(
        run_due(&h.orch, &tutor(), due).await,
        0,
        "a refused occurrence is left due, as a deferred one is"
    );
    let row = tutor_row(&h).await;
    assert_eq!(row.due_at(due), Some(due), "its claim was given back");
    assert_eq!(row.last_status, None, "and nothing was settled");
    assert_eq!(row.consecutive_failures, 0);
    let retry = tutor().status(&h.orch).unwrap().placement_retries[&row.entry_key].clone();
    assert_eq!((retry.due, retry.refusals), (due, 1));
    assert!(retry.refusal.contains("no connected executor"), "{retry:?}");
    let shown = crate::resources::read_cairn_resource(
        &h.orch,
        &McpCallbackRequest::default(),
        &format!("{TUTOR}/status"),
    )
    .await;
    assert!(
        shown.contains(&format!("retrying (1 of {PLACEMENT_RETRIES})")),
        "{shown}"
    );
    let log = tutor_log(&h).await;
    assert_eq!(retrying_rows(&log), 1, "{log:?}");
    assert!(
        log.iter()
            .any(|text| text.starts_with("scheduled `echo` due ")
                && text.contains("no connected executor")),
        "the row names the refusal: {log:?}"
    );

    assert_eq!(run_due(&h.orch, &tutor(), due + 60).await, 1);
    assert_eq!(h.host.frames_of("call").len(), 1, "the retry made the call");
    let row = tutor_row(&h).await;
    assert_eq!(row.last_status.as_deref(), Some("ok"));
    assert_eq!(row.last_fired_occurrence_at, Some(due));
    assert_eq!(row.consecutive_failures, 0);
    assert!(tutor()
        .status(&h.orch)
        .unwrap()
        .placement_retries
        .is_empty());
    assert_eq!(
        retrying_rows(&tutor_log(&h).await),
        1,
        "one refusal, one row"
    );
    assert_eq!(count(&h, "SELECT COUNT(*) FROM messages").await, 0);
    assert_eq!(count(&h, "SELECT COUNT(*) FROM attention_pushes").await, 0);
}

/// A substrate that refuses the first try and every retry is one the owner
/// should hear about: the occurrence fails once, and the streak's one notice
/// names the occurrence the refusals began at.
#[tokio::test]
async fn a_placement_refused_through_every_retry_fails_once_and_wakes_the_thread_once() {
    let h = thread_harness().await;
    let due = scheduled_tutor_with_no_process(&h).await;
    for _ in 0..=PLACEMENT_RETRIES {
        h.host.refuse.lock().unwrap().push_back(unroutable());
    }

    for retry in 0..PLACEMENT_RETRIES as i64 {
        assert_eq!(run_due(&h.orch, &tutor(), due + 60 * retry).await, 0);
        assert_eq!(tutor_row(&h).await.last_status, None);
        assert_eq!(count(&h, STREAK_NOTICES).await, 0, "a retry wakes no one");
    }
    assert_eq!(
        run_due(&h.orch, &tutor(), due + 60 * PLACEMENT_RETRIES as i64).await,
        1
    );
    let row = tutor_row(&h).await;
    assert_eq!(row.last_status.as_deref(), Some("failed"));
    assert_eq!(row.consecutive_failures, 1, "exactly one failed outcome");
    assert_eq!(row.last_fired_occurrence_at, Some(due));
    assert!(
        row.last_error
            .as_deref()
            .is_some_and(|error| error.contains(PLACEMENT_UNAVAILABLE)),
        "{row:?}"
    );
    assert_eq!(count(&h, STREAK_NOTICES).await, 1, "and one notice");
    let notice = h
        .orch
        .db
        .local
        .query_text("SELECT content FROM messages", ())
        .await
        .unwrap()
        .unwrap();
    let began = chrono::DateTime::from_timestamp(due, 0)
        .unwrap()
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string();
    assert!(notice.contains(&began), "{notice}");
    assert!(notice.contains("no connected executor"), "{notice}");
    assert!(tutor()
        .status(&h.orch)
        .unwrap()
        .placement_retries
        .is_empty());
    assert_eq!(
        retrying_rows(&tutor_log(&h).await),
        PLACEMENT_RETRIES as usize
    );
    assert!(h.host.frames_of("call").is_empty());
}

/// A substrate still refusing when the next occurrence comes due has been away
/// a whole interval: that occurrence fails at once rather than retrying anew.
#[tokio::test]
async fn a_placement_still_refused_at_the_next_occurrence_fails_it() {
    let h = thread_harness().await;
    let due = scheduled_tutor_with_no_process(&h).await;
    for _ in 0..2 {
        h.host.refuse.lock().unwrap().push_back(unroutable());
    }
    assert_eq!(run_due(&h.orch, &tutor(), due).await, 0);
    let next = due + 300;
    assert_eq!(run_due(&h.orch, &tutor(), next).await, 1);
    let row = tutor_row(&h).await;
    assert_eq!(row.last_status.as_deref(), Some("failed"));
    assert_eq!(row.consecutive_failures, 1);
    assert_eq!(row.last_fired_occurrence_at, Some(next));
    assert_eq!(count(&h, STREAK_NOTICES).await, 1);
    assert_eq!(retrying_rows(&tutor_log(&h).await), 1);
}

/// A start that failed for any reason but the substrate's is the service's
/// failure, exactly as before: settled at once, and the streak begins.
#[tokio::test]
async fn a_launch_failure_of_the_service_fails_its_occurrence_at_once() {
    let h = thread_harness().await;
    let due = scheduled_tutor_with_no_process(&h).await;
    h.host.refuse.lock().unwrap().push_back(LaunchError::Failed(
        "service package and runtime rejected".into(),
    ));
    assert_eq!(run_due(&h.orch, &tutor(), due).await, 1);
    let row = tutor_row(&h).await;
    assert_eq!(row.last_status.as_deref(), Some("failed"));
    assert_eq!(row.consecutive_failures, 1);
    assert!(
        row.last_error
            .as_deref()
            .is_some_and(|error| error.contains("start_failed")),
        "{row:?}"
    );
    assert_eq!(count(&h, STREAK_NOTICES).await, 1);
    assert_eq!(retrying_rows(&tutor_log(&h).await), 0);
    assert!(tutor()
        .status(&h.orch)
        .unwrap()
        .placement_retries
        .is_empty());
}

/// The fleet's refusal to grant a residency at all, as the lease host answers
/// it when the occurrence fires before the runner's executor has attached.
fn ungranted() -> LaunchError {
    LaunchError::Unplaced(
        "service residency was not granted: select execution environment executor: no live \
         enrolled executor satisfies executor local for project p. Known executors: no \
         executor is currently attached."
            .into(),
    )
}

/// A residency refused before any process exists, for the fleet's reasons, is
/// retried exactly as a start the substrate could not route: no failure, no
/// streak, one retrying row, and the recheck makes the call.
#[tokio::test]
async fn an_ungranted_residency_is_retried_and_wakes_no_one() {
    let h = thread_harness().await;
    let due = scheduled_tutor_with_no_process(&h).await;
    h.host
        .refuse_placement
        .lock()
        .unwrap()
        .push_back(ungranted());

    assert_eq!(run_due(&h.orch, &tutor(), due).await, 0);
    let row = tutor_row(&h).await;
    assert_eq!(row.due_at(due), Some(due), "its claim was given back");
    assert_eq!(row.last_status, None);
    assert_eq!(row.consecutive_failures, 0);
    let retry = tutor().status(&h.orch).unwrap().placement_retries[&row.entry_key].clone();
    assert_eq!((retry.due, retry.refusals), (due, 1));
    assert!(
        retry.refusal.contains("no executor is currently attached"),
        "{retry:?}"
    );
    assert_eq!(retrying_rows(&tutor_log(&h).await), 1);
    assert_eq!(h.host.launches.load(Ordering::Acquire), 0);

    assert_eq!(run_due(&h.orch, &tutor(), due + 60).await, 1);
    assert_eq!(h.host.frames_of("call").len(), 1, "the retry made the call");
    let row = tutor_row(&h).await;
    assert_eq!(row.last_status.as_deref(), Some("ok"));
    assert_eq!(row.last_fired_occurrence_at, Some(due));
    assert_eq!(row.consecutive_failures, 0);
    assert_eq!(
        retrying_rows(&tutor_log(&h).await),
        1,
        "one refusal, one row"
    );
    assert_eq!(count(&h, STREAK_NOTICES).await, 0);
    assert_eq!(count(&h, "SELECT COUNT(*) FROM messages").await, 0);
    assert_eq!(count(&h, "SELECT COUNT(*) FROM attention_pushes").await, 0);
}

/// A placement that refuses the service's own declaration is the service's
/// failure: settled at once as `start_failed`, with no retry.
#[tokio::test]
async fn a_placement_refusing_the_declaration_fails_its_occurrence_at_once() {
    let h = thread_harness().await;
    let due = scheduled_tutor_with_no_process(&h).await;
    h.host
        .refuse_placement
        .lock()
        .unwrap()
        .push_back(LaunchError::Failed(
            "service residency is unavailable: conflicting residency declaration".into(),
        ));
    assert_eq!(run_due(&h.orch, &tutor(), due).await, 1);
    let row = tutor_row(&h).await;
    assert_eq!(row.last_status.as_deref(), Some("failed"));
    assert_eq!(row.consecutive_failures, 1);
    assert!(
        row.last_error
            .as_deref()
            .is_some_and(|error| error.contains("start_failed")),
        "{row:?}"
    );
    assert_eq!(count(&h, STREAK_NOTICES).await, 1);
    assert_eq!(retrying_rows(&tutor_log(&h).await), 0);
    assert!(tutor()
        .status(&h.orch)
        .unwrap()
        .placement_retries
        .is_empty());
}

// ---- Runner start (cairn/5723) ----

/// The schedule row once its occurrence has settled.
async fn settled(h: &Harness, owner: &ServiceOwner, service: &str) -> ScheduleRow {
    for _ in 0..500 {
        let row = schedule::list(&h.orch.db.local, "p", owner, service)
            .await
            .unwrap()
            .remove(0);
        if row.last_status.is_some() {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {service}'s occurrence to settle");
}

/// Record `entries` as a predecessor runner did ten minutes ago, so a
/// five-minute cadence has an occurrence due now.
async fn recorded_earlier(
    h: &Harness,
    owner: &ServiceOwner,
    service: &str,
    entries: &[crate::project_services::declaration::ScheduleEntry],
) {
    schedule::sync(
        &h.orch.db.local,
        "p",
        owner,
        service,
        entries,
        now_secs() - 600,
    )
    .await
    .unwrap();
}

fn started(log: &[String]) -> usize {
    log.iter()
        .filter(|text| text.starts_with("started generation"))
        .count()
}

/// A thread's schedule runs from boot: with no project repository reconciled
/// and no catch-up, the occurrence that fell due while no runner was up
/// fires. The project pass that follows the catch-up leaves it serving.
#[tokio::test]
async fn a_thread_schedule_fires_at_boot_without_the_catch_up() {
    let h = thread_harness().await;
    let payload = scheduled("echo");
    crate::project_services::thread_store::write(
        &h.orch.db.local,
        "p",
        &spanish(),
        "tutor",
        TUTOR,
        &crate::project_services::thread_store::ServiceWriter {
            job_id: "spanish-session".into(),
        },
        &payload,
        false,
        false,
    )
    .await
    .unwrap();
    let entries = crate::project_services::thread_store::package("tutor", TUTOR, &payload)
        .unwrap()
        .declaration
        .schedule;
    recorded_earlier(&h, &spanish(), "tutor", &entries).await;
    assert!(
        tutor().status(&h.orch).is_none(),
        "nothing runs before boot"
    );

    start_at_boot(&h.orch).await;
    assert!(
        !h.orch
            .project_services
            .supervisor
            .projects_current
            .load(Ordering::Acquire),
        "boot does not wait for, or stand in for, the catch-up"
    );
    eventually("the boot-time scheduled call", || {
        h.host.frames_of("call").len() == 1
    })
    .await;
    let frame = h.host.frames_of("call").remove(0);
    assert_eq!(frame["method"], "echo");
    assert_eq!(frame["caller"]["kind"], "schedule");
    let row = settled(&h, &spanish(), "tutor").await;
    assert_eq!(row.last_status.as_deref(), Some("ok"));

    // The project pass after the catch-up finalizes nothing boot started.
    let run_id = tutor().status(&h.orch).unwrap().run_id.unwrap();
    start_project_launch_services(&h.orch).await;
    assert_eq!(
        tutor().status(&h.orch).unwrap().state,
        ServiceState::Running
    );
    assert_eq!(
        count(
            &h,
            &format!("SELECT COUNT(*) FROM runs WHERE id = '{run_id}' AND status = 'exited'")
        )
        .await,
        0
    );
    assert_eq!(started(&tutor_log(&h).await), 1);
}

/// One owner's boot pass cannot finish (its source read never answers), and
/// another owner's due occurrence still fires: boot applies each owner on its
/// own and resumes schedules without waiting for any source. Once the held
/// read answers, its `launch` service starts and boot ends.
#[tokio::test]
async fn a_held_owner_at_boot_holds_neither_another_owners_schedule_nor_its_services() {
    let h = thread_harness().await;
    let french = ServiceOwner::Thread {
        id: "t-2".into(),
        name: "french".into(),
    };
    let write =
        |owner: ServiceOwner, service: &'static str, writer: &'static str, payload: Value| {
            let db = h.orch.db.local.clone();
            async move {
                let uri = owner.home("cairn", service);
                crate::project_services::thread_store::write(
                    &db,
                    "p",
                    &owner,
                    service,
                    &uri,
                    &crate::project_services::thread_store::ServiceWriter {
                        job_id: writer.into(),
                    },
                    &payload,
                    false,
                    false,
                )
                .await
                .unwrap();
            }
        };
    // Written first, so a serial boot would reach it before `spanish`.
    write(
        french.clone(),
        "clock",
        "french-session",
        json!({"files": {"main.ts": "serve({})"}, "manifest": {"start": "launch"}}),
    )
    .await;
    let payload = scheduled("echo");
    write(spanish(), "tutor", "spanish-session", payload.clone()).await;
    let entries = crate::project_services::thread_store::package("tutor", TUTOR, &payload)
        .unwrap()
        .declaration
        .schedule;
    recorded_earlier(&h, &spanish(), "tutor", &entries).await;
    assert_eq!(
        crate::project_services::thread_store::owners(&h.orch.db.local)
            .await
            .unwrap()
            .first()
            .and_then(|(_, owner)| owner.thread_id().map(str::to_string))
            .as_deref(),
        Some("t-2"),
        "the held owner is first in boot order"
    );

    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *h.source.held.lock().unwrap() = Some(("t-2".into(), gate.clone()));
    let orch = h.orch.clone();
    let boot = tokio::spawn(async move { start_at_boot(&orch).await });

    eventually("spanish's due occurrence while french is held", || {
        h.host.frames_of("call").len() == 1
    })
    .await;
    let frame = h.host.frames_of("call").remove(0);
    assert_eq!(frame["caller"]["kind"], "schedule");
    assert_eq!(frame["caller"]["actingFor"], "cairn://p/cairn/spanish");
    assert_eq!(
        settled(&h, &spanish(), "tutor")
            .await
            .last_status
            .as_deref(),
        Some("ok")
    );
    assert!(!boot.is_finished(), "french's pass is still held");
    assert_eq!(
        h.host.launches.load(Ordering::Acquire),
        1,
        "only tutor runs"
    );

    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), boot)
        .await
        .expect("boot ends once the held read answers")
        .unwrap();
    assert_eq!(
        h.orch
            .project_services
            .supervisor
            .status("p", &french, "clock")
            .expect("french's launch service has a cell")
            .state,
        ServiceState::Running
    );
    assert_eq!(h.host.launches.load(Ordering::Acquire), 2);
}

const SCHEDULED_WATCHER: &str =
    "name: Watcher\nstart: launch\nschedule:\n  - every: 5m\n    method: echo\n";

/// A project's schedule runs from boot too, but its due occurrence defers,
/// unclaimed and logged once, until the project pass after the catch-up. That
/// pass starts the `launch` service, runs the occurrence, and runs once.
#[tokio::test]
async fn a_project_schedule_defers_until_the_project_pass_which_runs_once() {
    let h = harness().await;
    let repo = h._config.path().to_string_lossy().to_string();
    h.orch
        .db
        .local
        .execute("UPDATE projects SET repo_path = ?1 WHERE id = 'p'", (repo,))
        .await
        .unwrap();
    h.source.put("watcher", SCHEDULED_WATCHER, "serve({})");
    let entries = h
        .source
        .package("watcher")
        .unwrap()
        .unwrap()
        .declaration
        .schedule;
    recorded_earlier(&h, &ServiceOwner::Project, "watcher", &entries).await;

    start_at_boot(&h.orch).await;
    let deferrals = || async {
        log_texts(&h, "watcher")
            .await
            .iter()
            .filter(|text| text.starts_with("scheduled `echo` due ") && text.contains("deferred"))
            .count()
    };
    for _ in 0..500 {
        if deferrals().await > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        deferrals().await,
        1,
        "boot's schedule task defers the occurrence"
    );
    let watcher = ServiceRef::project("p", "cairn", "watcher");
    assert_eq!(run_due(&h.orch, &watcher, now_secs()).await, 0);
    assert_eq!(deferrals().await, 1, "one row, not one per recheck");
    assert_eq!(h.host.launches.load(Ordering::Acquire), 0);
    assert!(h.host.frames_of("call").is_empty());
    let row = schedule::list(&h.orch.db.local, "p", &ServiceOwner::Project, "watcher")
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        row.last_fired_occurrence_at, None,
        "a deferral claims nothing"
    );
    assert_eq!(
        row.last_status, None,
        "a deferral is neither run nor skipped"
    );
    assert_eq!(row.consecutive_failures, 0);

    start_project_launch_services(&h.orch).await;
    assert_eq!(status(&h, "watcher").state, ServiceState::Running);
    eventually("the deferred occurrence", || {
        h.host.frames_of("call").len() == 1
    })
    .await;
    assert_eq!(h.host.frames_of("call")[0]["caller"]["kind"], "schedule");
    let row = settled(&h, &ServiceOwner::Project, "watcher").await;
    assert_eq!(row.last_status.as_deref(), Some("ok"));

    start_project_launch_services(&h.orch).await;
    assert_eq!(h.host.launches.load(Ordering::Acquire), 1);
    assert_eq!(started(&log_texts(&h, "watcher").await), 1);
}

/// A project whose checkout is gone is skipped by the project pass, and the
/// source that would read it says which path is missing.
#[tokio::test]
async fn a_missing_repository_is_named_and_its_project_skipped() {
    let h = harness().await;
    let missing = h._config.path().join("moved-away");
    let missing_text = missing.to_string_lossy().to_string();
    h.orch
        .db
        .local
        .execute(
            "UPDATE projects SET repo_path = ?1 WHERE id = 'p'",
            (missing_text.clone(),),
        )
        .await
        .unwrap();
    let error = crate::project_services::declaration::BaseTipSource::resolve(&h.orch, "p")
        .await
        .err()
        .expect("a missing repository has no base tip");
    assert!(
        error.contains(&missing_text) && error.contains("does not exist"),
        "{error}"
    );

    h.source
        .put("watcher", "name: Watcher\nstart: launch\n", "serve({})");
    start_project_launch_services(&h.orch).await;
    assert_eq!(h.host.launches.load(Ordering::Acquire), 0);
    assert!(h
        .orch
        .project_services
        .supervisor
        .status("p", &ServiceOwner::Project, "watcher")
        .is_none());
}

/// A `launch` service never idle-stops.
#[tokio::test]
async fn a_launch_service_never_idle_stops() {
    let h = harness().await;
    *h.source.idle.lock().unwrap() = Some(Duration::from_millis(100));
    h.source
        .put("watcher", "name: Watcher\nstart: launch\n", "serve({})");
    apply_source(&h.orch, "p", &ServiceOwner::Project, "test", None).await;
    assert_eq!(status(&h, "watcher").state, ServiceState::Running);
    assert_eq!(status(&h, "watcher").idle_stop_after, None);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(status(&h, "watcher").state, ServiceState::Running);
    assert!(h.host.frames_of("drain").is_empty());
}

/// The owner scope is the owning thread's alone: the same tool target from
/// that thread's page is refused, and another thread is refused at the door.
#[tokio::test]
async fn a_page_of_the_thread_gets_no_tool_targets() {
    let h = thread_harness().await;
    write_tutor(&h, "spanish-session", main_ts("serve({})"), false)
        .await
        .unwrap();
    let orch = h.orch.clone();
    let in_flight =
        tokio::spawn(async move { call(&orch, tutor_request("slow", thread_page())).await });
    eventually("the page call", || h.host.frames_of("call").len() == 1).await;
    let (run_id, _) = authority_of(&h.host.frames_of("call")[0]);
    let refused = authority::refuse(
        &h.orch,
        &McpCallbackRequest {
            tool: "run".into(),
            payload: json!({"commands": [{"target": "cairn://mcp/cairn-bugs/list_reports"}]}),
            run_id: Some(run_id),
            ..Default::default()
        },
    )
    .await;
    assert!(refused.is_some(), "a page's call invokes no tool target");
    h.host.release_slow.notify_waiters();
    assert!(in_flight.await.unwrap().is_ok());

    let outsider = call(
        &h.orch,
        tutor_request("echo", thread_agent("cairn://p/cairn/french")),
    )
    .await
    .unwrap_err();
    assert_eq!(outsider.code, "not_owner");
}

/// A service's tool targets are host batches. The branch a service's batch
/// defaults to is the base tip of a tree it does not have here, so a run of
/// an MCP tool reaches its dispatch instead of being refused for that branch;
/// a branch the service names is still refused, and the removed executor-verb
/// address is refused naming what replaced it.
#[tokio::test]
async fn a_service_tool_target_reaches_dispatch_without_a_branch() {
    let h = thread_harness().await;
    // A configured server, so the target resolves. This orchestrator has no
    // gateway, so a batch that reaches dispatch fails there, by saying so.
    std::fs::write(
        h.orch.config_dir.join("settings.yaml"),
        "mcpServers:\n  cairn-bugs:\n    command: /nonexistent/cairn-bugs-test-server\n",
    )
    .unwrap();
    write_tutor(&h, "spanish-session", main_ts("serve({})"), false)
        .await
        .unwrap();
    let orch = h.orch.clone();
    let in_flight = tokio::spawn(async move {
        call(
            &orch,
            tutor_request("slow", thread_agent("cairn://p/cairn/spanish")),
        )
        .await
    });
    eventually("the agent call", || h.host.frames_of("call").len() == 1).await;
    let (run_id, _) = authority_of(&h.host.frames_of("call")[0]);
    let run = |payload: Value| McpCallbackRequest {
        tool: "run".into(),
        payload,
        run_id: Some(run_id.clone()),
        ..Default::default()
    };

    let mcp = crate::mcp::handlers::run::handle_run(
        &h.orch,
        &run(json!({"commands": [{
            "target": "cairn://mcp/cairn-bugs/list_reports",
            "timeout": 5000,
        }]})),
    )
    .await;
    assert!(!mcp.contains("branch option"), "{mcp}");
    assert!(
        !mcp.contains("Unknown MCP server"),
        "the target resolved to the configured server: {mcp}"
    );
    let at_dispatch = "MCP gateway is not available in this host";
    assert!(
        mcp.contains(at_dispatch),
        "the MCP call reached gateway dispatch: {mcp}"
    );

    let executor = crate::mcp::handlers::run::handle_run(
        &h.orch,
        &run(json!({"commands": [{"target": "cairn://executors/nowhere/look"}]})),
    )
    .await;
    assert!(!executor.contains("branch option"), "{executor}");
    assert!(
        executor.contains("`cairn://executors/nowhere/look` was removed")
            && executor.contains("cairn://mcp/axon/look"),
        "the removed executor verb is refused naming its replacement: {executor}"
    );

    let explicit = crate::mcp::handlers::run::handle_run(
        &h.orch,
        &run(json!({
            "commands": [{"target": "cairn://mcp/cairn-bugs/list_reports"}],
            "branch": "main",
        })),
    )
    .await;
    assert!(
        explicit.contains("The branch option applies only to tree-bound"),
        "a branch the service names on a host batch is refused: {explicit}"
    );

    h.host.release_slow.notify_waiters();
    assert!(in_flight.await.unwrap().is_ok());
}

/// A scheduled method whose work would raise a prompt is refused at once,
/// fails its call even though the method answered, and wakes the thread once
/// per streak.
#[tokio::test]
async fn a_prompt_from_a_scheduled_call_fails_closed_and_wakes_the_thread_once() {
    let h = thread_harness().await;
    h.orch
        .db
        .local
        .execute(
            "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
             VALUES ('spanish-run', 'p', 'spanish-session', 'completed', 1, 1)",
            (),
        )
        .await
        .unwrap();
    write_tutor(&h, "spanish-session", scheduled("slow"), false)
        .await
        .unwrap();
    let due = schedule::list(&h.orch.db.local, "p", &spanish(), "tutor")
        .await
        .unwrap()[0]
        .anchor_at;
    let notices = "SELECT COUNT(*) FROM messages WHERE content LIKE '%started failing%'";

    for (occurrence, expected_notices) in [(due, 1), (due + 300, 1)] {
        let calls_before = h.host.frames_of("call").len();
        let orch = h.orch.clone();
        let tick = tokio::spawn(async move { run_due(&orch, &tutor(), occurrence).await });
        eventually("the scheduled call", || {
            h.host.frames_of("call").len() == calls_before + 1
        })
        .await;
        let (run_id, _) = authority_of(&h.host.frames_of("call")[calls_before]);
        // What the run handler's fence or tool gate would do for a crossing.
        let wait = crate::mcp::handlers::permission::await_permission_decision(
            &h.orch,
            &run_id,
            "tool-1",
            "run",
            &json!({"command": "touch /etc/x"}),
            crate::mcp::handlers::permission::Handoff::Unwatched,
        )
        .await;
        assert!(
            matches!(&wait, crate::mcp::handlers::permission::PermissionWait::Unavailable(why) if why.contains("cannot raise a permission prompt")),
            "a service call never prompts"
        );
        assert_eq!(
            count(&h, "SELECT COUNT(*) FROM permission_requests").await,
            0,
            "no prompt was persisted"
        );
        // The method answers ok; the call fails anyway.
        h.host.release_slow.notify_waiters();
        assert_eq!(tick.await.unwrap(), 1);
        let row = &schedule::list(&h.orch.db.local, "p", &spanish(), "tutor")
            .await
            .unwrap()[0];
        assert_eq!(row.last_status.as_deref(), Some("failed"));
        assert!(row
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("prompt_refused")));
        assert_eq!(count(&h, notices).await, expected_notices);
    }
}

// Machine services: a pack's native service as cells of this supervisor.

const PROBE_SHA: &str = "eb5765e715747caccda94ba30266d6d8616861bc9893ca152700546db938df54";

fn native_manifest(transport: &str) -> String {
    format!(
        "name: Probe\nruntime: native\nplacement: per-machine\ntransport: {transport}\nargs: [serve]\nartifacts:\n  version: 1.0.0\n  prove: [--version]\n  targets:\n    x86_64-unknown-linux-gnu:\n      url: https://example.invalid/probe\n      sha256: {PROBE_SHA}\n      size: 10\n      executable: probe\nprobe:\n  args: [status]\n  every: 5m\n  ready: /ready\n  reason: [/reason]\n  waitingForOperator: [permission-denied]\n"
    )
}

struct MachineHarness {
    h: Harness,
    _pack: tempfile::TempDir,
}

fn view(name: &str) -> machine::MachineView {
    machine::MachineView {
        floors_service_residents: true,
        name: name.to_string(),
        target: Some("x86_64-unknown-linux-gnu".to_string()),
        installed: Vec::new(),
    }
}

/// A pack `kit` declaring the native service `probe` (with `transport`), and
/// the fake host seeing `machines` attached.
async fn machine_harness(transport: &str, machines: &[&str]) -> MachineHarness {
    let h = harness().await;
    let pack = tempfile::tempdir().unwrap();
    let dir = pack.path().join("services/probe");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("service.yaml"), native_manifest(transport)).unwrap();
    h.source.packs.lock().unwrap().push((
        "kit".to_string(),
        Arc::new(
            crate::project_services::pack_source::PackServiceSource::at_root(
                "kit",
                pack.path().to_path_buf(),
            ),
        ),
    ));
    *h.host.machines.lock().unwrap() = Some(machines.iter().map(|name| view(name)).collect());
    MachineHarness { h, _pack: pack }
}

fn probe_key() -> cairn_config::fleet::MachineServiceKey {
    "kit/probe".parse().unwrap()
}

const LINUX: &str = "x86_64-unknown-linux-gnu";

/// [`native_manifest`] placed on the whole fleet: no probe.
fn fleet_manifest() -> String {
    let probed = native_manifest("mcp-stdio");
    probed[..probed.find("probe:\n").unwrap()].replace("placement: per-machine", "placement: fleet")
}

/// A pack `kit` declaring the fleet service `probe`, and the fake host seeing
/// `machines` attached.
async fn fleet_harness(machines: Vec<machine::MachineView>) -> MachineHarness {
    let h = harness().await;
    let pack = tempfile::tempdir().unwrap();
    let dir = pack.path().join("services/probe");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("service.yaml"), fleet_manifest()).unwrap();
    h.source.packs.lock().unwrap().push((
        "kit".to_string(),
        Arc::new(
            crate::project_services::pack_source::PackServiceSource::at_root(
                "kit",
                pack.path().to_path_buf(),
            ),
        ),
    ));
    *h.host.machines.lock().unwrap() = Some(machines);
    MachineHarness { h, _pack: pack }
}

/// `name` on `target`, reporting `outcome` for kit/probe at the declared sha.
fn reporting(
    name: &str,
    target: &str,
    outcome: cairn_common::executor_protocol::MachineServiceInstallOutcome,
) -> machine::MachineView {
    machine::MachineView {
        floors_service_residents: true,
        name: name.to_string(),
        target: Some(target.to_string()),
        installed: vec![cairn_common::executor_protocol::InstalledMachineService {
            pack: "kit".to_string(),
            service: "probe".to_string(),
            sha256: PROBE_SHA.to_string(),
            version: "1.0.0".to_string(),
            outcome,
        }],
    }
}

/// Lay the declared artifact down under the runner's home as the colocated
/// executor would, marker last, and return its executable.
fn install_on_host(h: &Harness) -> std::path::PathBuf {
    let program = cairn_common::executor_protocol::ManagedProgram {
        pack: "kit".to_string(),
        service: "probe".to_string(),
        sha256: PROBE_SHA.to_string(),
        executable: "probe".to_string(),
    };
    let dir = h
        .orch
        .config_dir
        .join(cairn_common::executor_protocol::MACHINE_SERVICE_TOOLS_DIR)
        .join(program.install_dir_name());
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("probe"), b"#!/bin/sh\n").unwrap();
    std::fs::write(
        dir.join(cairn_common::executor_protocol::MACHINE_SERVICE_MARKER),
        PROBE_SHA,
    )
    .unwrap();
    dir.join("probe")
}

fn fleet_instance(h: &Harness, machine: &str) -> machine::MachineServiceInstance {
    machine::instances(&h.orch, &probe_key())
        .into_iter()
        .find(|instance| instance.machine == machine)
        .unwrap_or_else(|| panic!("{machine} has a fleet instance"))
}

/// A fleet service is never a resident: applying starts nothing, so it has no
/// cell and no lease anywhere.
#[tokio::test]
async fn a_fleet_service_starts_no_resident() {
    use cairn_common::executor_protocol::MachineServiceInstallOutcome::Installed;
    let m = fleet_harness(vec![
        reporting("local", LINUX, Installed),
        reporting("fedora", LINUX, Installed),
    ])
    .await;
    install_on_host(&m.h);
    machine::apply_machine_services(&m.h.orch, "test").await;
    assert!(launched_on(&m.h).is_empty());
    assert!(machine_cell(&m.h, "local").is_none());
    assert!(machine_cell(&m.h, "fedora").is_none());
}

/// Each machine's state is its install report, with no cell; only the host
/// serves MCP, and only a ready host carries what it launches.
#[tokio::test]
async fn a_fleet_services_state_on_each_machine_is_its_install_report() {
    use cairn_common::executor_protocol::MachineServiceInstallOutcome as Outcome;
    let m = fleet_harness(vec![
        reporting("local", LINUX, Outcome::Installed),
        reporting("fedora", LINUX, Outcome::Installing),
        reporting(
            "bgsrv-ub",
            LINUX,
            Outcome::NotInstalled {
                reason: "artifact unavailable".to_string(),
            },
        ),
        machine::MachineView {
            floors_service_residents: true,
            name: "bginf-sp1".to_string(),
            target: Some("aarch64-unknown-linux-gnu".to_string()),
            installed: Vec::new(),
        },
        view("quiet"),
    ])
    .await;
    let program = install_on_host(&m.h);

    let local = fleet_instance(&m.h, "local");
    assert_eq!(local.state, machine::InstanceState::Ready);
    assert_eq!(local.sentence(), "installed");
    assert_eq!(local.provenance, "installed 1.0.0 (pack)");
    assert!(local.fleet && local.serves_mcp);
    let launch = local.launch.expect("the host launches what it holds");
    assert_eq!(launch.program, program.to_string_lossy());
    assert_eq!(launch.args, ["serve"]);

    let fedora = fleet_instance(&m.h, "fedora");
    assert_eq!(fedora.state, machine::InstanceState::Installing);
    assert!(!fedora.serves_mcp && fedora.launch.is_none());
    assert_eq!(
        fleet_instance(&m.h, "bgsrv-ub").state,
        machine::InstanceState::NotInstalled {
            reason: "artifact unavailable".to_string()
        }
    );
    assert_eq!(
        fleet_instance(&m.h, "bginf-sp1").state,
        machine::InstanceState::NotInstalled {
            reason: "the pack ships no artifact for this machine".to_string()
        }
    );
    assert_eq!(
        fleet_instance(&m.h, "quiet").state,
        machine::InstanceState::NotInstalled {
            reason: "the machine has not reported it yet".to_string()
        }
    );
    assert!(machine::instances_on(&m.h.orch, "fedora")
        .iter()
        .any(|instance| instance.key == probe_key() && instance.fleet));
    assert!(machine::selected_keys(&m.h.orch).contains(&probe_key()));

    let snapshot = crate::config::machine_services::MachineServiceSnapshot::capture(&m.h.orch);
    assert_eq!(snapshot.ready_on(&probe_key()), ["local"]);
    assert_eq!(
        snapshot
            .host_launch(&probe_key())
            .map(|(_, launch)| launch.program.clone()),
        Some(program.to_string_lossy().into_owned())
    );
}

/// The host is always in the set, and without an install it is not ready,
/// whatever another machine holds: an absent executor, or a directory the
/// marker does not vouch for.
#[tokio::test]
async fn the_host_is_ready_only_with_a_marked_install_or_an_override() {
    use cairn_common::executor_protocol::MachineServiceInstallOutcome::Installed;
    let m = fleet_harness(vec![reporting("fedora", LINUX, Installed)]).await;
    assert_eq!(
        fleet_instance(&m.h, "local").state,
        machine::InstanceState::MachineAbsent
    );

    *m.h.host.machines.lock().unwrap() = Some(vec![
        reporting("local", LINUX, Installed),
        reporting("fedora", LINUX, Installed),
    ]);
    let unmarked = fleet_instance(&m.h, "local");
    let machine::InstanceState::NotInstalled { reason } = &unmarked.state else {
        panic!("an unmarked install is not one: {:?}", unmarked.state);
    };
    assert!(reason.contains("is not installed under"), "{reason}");
    assert!(unmarked.launch.is_none());

    select(&m.h, "local");
    let overridden = fleet_instance(&m.h, "local");
    assert_eq!(overridden.state, machine::InstanceState::Ready);
    assert_eq!(
        overridden.provenance,
        "override /opt/probe/bin/probe (set before attribution was recorded)"
    );
    assert_eq!(
        overridden.launch.map(|launch| launch.program),
        Some("/opt/probe/bin/probe".to_string()),
        "the host's override wins over the managed path"
    );
}

/// Every machine the declaration ships for is served the artifact, with no
/// selection; a machine it does not ship for, or one with an override, is not.
#[tokio::test]
async fn a_fleet_service_is_served_to_every_machine_it_ships_for() {
    let m = fleet_harness(vec![view("fedora")]).await;
    let wanted = crate::fleet::machine_services::wanted_artifacts(&m.h.orch, "fedora", LINUX);
    assert_eq!(wanted.len(), 1);
    assert_eq!(
        (wanted[0].pack.as_str(), wanted[0].service.as_str()),
        ("kit", "probe")
    );
    assert_eq!(wanted[0].sha256, PROBE_SHA);
    assert_eq!(wanted[0].archive, None);
    assert_eq!(wanted[0].prove_args, ["--version"]);
    assert!(crate::fleet::machine_services::wanted_artifacts(
        &m.h.orch,
        "bginf-sp1",
        "aarch64-unknown-linux-gnu"
    )
    .is_empty());

    select(&m.h, "fedora");
    assert!(
        crate::fleet::machine_services::wanted_artifacts(&m.h.orch, "fedora", LINUX).is_empty(),
        "an overridden machine runs its own binary and is served nothing"
    );
    assert_eq!(
        crate::fleet::machine_services::wanted_artifacts(&m.h.orch, "other", LINUX).len(),
        1
    );
}

/// Select `kit/probe` on `machine`, running the operator's binary there.
fn select(h: &Harness, machine: &str) {
    crate::config::settings::mutate_fleet(
        &h.orch.config_dir,
        |config| {
            config.machine_services.select(
                probe_key(),
                machine,
                cairn_config::fleet::MachineSelection::overriding("/opt/probe/bin/probe"),
            )
        },
        |_| {},
    )
    .unwrap();
}

fn unselect(h: &Harness, machine: &str) {
    crate::config::settings::mutate_fleet(
        &h.orch.config_dir,
        |config| Ok(config.machine_services.unselect(&probe_key(), machine)),
        |_| {},
    )
    .unwrap();
}

fn machine_cell(h: &Harness, machine: &str) -> Option<Arc<Cell>> {
    h.orch
        .project_services
        .supervisor
        .existing("", &ServiceOwner::pack("kit", machine), "probe")
}

fn launched_on(h: &Harness) -> Vec<String> {
    let mut machines: Vec<String> = h
        .host
        .native_launches
        .lock()
        .unwrap()
        .iter()
        .map(|launch| launch.machine.clone())
        .collect();
    machines.sort();
    machines
}

fn instance_state(h: &Harness, machine: &str) -> machine::InstanceState {
    machine::instances(&h.orch, &probe_key())
        .into_iter()
        .find(|instance| instance.machine == machine)
        .expect("the machine is selected")
        .state
}

/// One resident per selected machine, and none on a machine the row does
/// not select; and nothing of Cairn's reaches it.
#[tokio::test]
async fn a_selection_of_two_machines_starts_one_resident_on_each() {
    let m = machine_harness("none", &["alpha", "beta", "gamma"]).await;
    select(&m.h, "alpha");
    select(&m.h, "beta");
    machine::apply_machine_services(&m.h.orch, "test").await;
    assert_eq!(launched_on(&m.h), ["alpha", "beta"]);
    assert!(machine_cell(&m.h, "gamma").is_none());

    for launch in m.h.host.native_launches.lock().unwrap().iter() {
        assert_eq!(launch.program, "/opt/probe/bin/probe");
        assert_eq!(launch.args, ["serve"]);
        assert!(
            launch.env.iter().all(|(key, _)| !key.starts_with("CAIRN_")),
            "no CAIRN_* variable reaches a native resident: {:?}",
            launch.env
        );
    }
    assert_eq!(instance_state(&m.h, "alpha"), machine::InstanceState::Ready);
    let alpha = machine::instances(&m.h.orch, &probe_key())
        .into_iter()
        .find(|instance| instance.machine == "alpha")
        .unwrap();
    assert_eq!(
        alpha.provenance,
        "override /opt/probe/bin/probe (set before attribution was recorded)"
    );

    // An unchanged row is a no-op.
    machine::apply_machine_services(&m.h.orch, "test").await;
    assert_eq!(launched_on(&m.h).len(), 2);
}

#[tokio::test]
async fn removing_a_selection_drains_and_forgets_the_cell() {
    let m = machine_harness("none", &["alpha", "beta"]).await;
    select(&m.h, "alpha");
    select(&m.h, "beta");
    machine::apply_machine_services(&m.h.orch, "test").await;
    unselect(&m.h, "beta");
    machine::apply_machine_services(&m.h.orch, "test").await;
    eventually("beta's resident to end", || {
        m.h.host
            .native_ended
            .lock()
            .unwrap()
            .contains(&"beta".to_string())
    })
    .await;
    assert!(machine_cell(&m.h, "beta").is_none(), "its cell is gone");
    assert!(machine_cell(&m.h, "alpha").is_some());
    let left: Vec<String> = machine::instances(&m.h.orch, &probe_key())
        .into_iter()
        .map(|instance| instance.machine)
        .collect();
    assert_eq!(left, ["alpha"]);
}

/// A rename moves a machine's selections, and the next apply drains the old
/// name's cell and starts the new name's; nothing waits for a restart.
#[tokio::test]
async fn a_renamed_machine_drains_its_old_cell_and_serves_under_its_new_name() {
    let m = machine_harness("none", &["alpha"]).await;
    select(&m.h, "alpha");
    machine::apply_machine_services(&m.h.orch, "test").await;
    crate::config::settings::mutate_fleet(
        &m.h.orch.config_dir,
        |config| {
            config.machine_services.rename_machine("alpha", "delta");
            Ok(())
        },
        |_| {},
    )
    .unwrap();
    *m.h.host.machines.lock().unwrap() = Some(vec![view("delta")]);
    machine::apply_machine_services(&m.h.orch, "machine renamed").await;
    eventually("the old name's resident to end", || {
        m.h.host
            .native_ended
            .lock()
            .unwrap()
            .contains(&"alpha".to_string())
    })
    .await;
    assert!(
        machine_cell(&m.h, "alpha").is_none(),
        "the old name's cell is drained"
    );
    let delta = machine_cell(&m.h, "delta").expect("the new name has a cell");
    assert!(delta.serving().is_some(), "and it serves");
    assert_eq!(launched_on(&m.h), ["alpha", "delta"]);
    assert_eq!(instance_state(&m.h, "delta"), machine::InstanceState::Ready);
}

#[tokio::test]
async fn a_selected_machine_that_is_away_starts_when_it_attaches() {
    let m = machine_harness("none", &["alpha"]).await;
    select(&m.h, "gamma");
    machine::apply_machine_services(&m.h.orch, "test").await;
    assert!(launched_on(&m.h).is_empty());
    assert_eq!(
        instance_state(&m.h, "gamma"),
        machine::InstanceState::MachineAbsent
    );
    assert_eq!(
        instance_state(&m.h, "gamma").sentence(),
        "not running: machine absent"
    );

    m.h.host
        .machines
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .push(view("gamma"));
    eventually("the resident to start on attach", || {
        launched_on(&m.h) == ["gamma"]
    })
    .await;

    // The machine going away is not the service's crash.
    let exit = m.h.host.native_exits.lock().unwrap()["gamma"].clone();
    m.h.host.machines.lock().unwrap().as_mut().unwrap().clear();
    exit.send((None, true)).unwrap();
    let cell = machine_cell(&m.h, "gamma").unwrap();
    eventually("the cell to stop", || {
        cell.state.lock().unwrap().status == ServiceState::Stopped
    })
    .await;
    assert!(cell.state.lock().unwrap().crashes.is_empty());
    assert_eq!(
        instance_state(&m.h, "gamma"),
        machine::InstanceState::MachineAbsent
    );
}

#[tokio::test]
async fn a_machine_service_backs_off_and_fails_like_any_service() {
    let m = machine_harness("none", &["alpha"]).await;
    select(&m.h, "alpha");
    machine::apply_machine_services(&m.h.orch, "test").await;
    let cell = machine_cell(&m.h, "alpha").unwrap();
    let exit = m.h.host.native_exits.lock().unwrap()["alpha"].clone();
    exit.send((Some(3), false)).unwrap();
    eventually("the crash to back off", || {
        cell.state.lock().unwrap().status == ServiceState::Backoff
    })
    .await;
    assert!(matches!(
        instance_state(&m.h, "alpha"),
        machine::InstanceState::Degraded { .. }
    ));
    for crash in 1..CRASH_LIMIT {
        crashed(&m.h.orch, &cell, crash as u64 + 10, "crashed on purpose").await;
    }
    assert_eq!(cell.state.lock().unwrap().status, ServiceState::Failed);
    assert!(matches!(
        instance_state(&m.h, "alpha"),
        machine::InstanceState::Failed { .. }
    ));
}

#[tokio::test]
async fn a_probe_that_does_not_answer_degrades_without_a_crash() {
    let m = machine_harness("none", &["alpha"]).await;
    select(&m.h, "alpha");
    machine::apply_machine_services(&m.h.orch, "test").await;
    let cell = machine_cell(&m.h, "alpha").unwrap();
    let incarnation = cell.serving().expect("it booted through its probe");
    assert!(m.h.host.probes.load(Ordering::Acquire) >= 1);

    *m.h.host.probe_answer.lock().unwrap() = Some(Err("timed out".to_string()));
    let report = machine::probe_once(&m.h.orch, &cell, &incarnation).await;
    *cell.machine.as_ref().unwrap().probe.lock().unwrap() = Some(report);
    assert_eq!(
        instance_state(&m.h, "alpha"),
        machine::InstanceState::Degraded {
            reason: "timed out".to_string()
        }
    );
    assert!(cell.state.lock().unwrap().crashes.is_empty());
    assert_eq!(cell.state.lock().unwrap().status, ServiceState::Running);

    *m.h.host.probe_answer.lock().unwrap() = Some(Ok(
        r#"{"ready": false, "reason": "permission-denied"}"#.to_string(),
    ));
    let report = machine::probe_once(&m.h.orch, &cell, &incarnation).await;
    *cell.machine.as_ref().unwrap().probe.lock().unwrap() = Some(report);
    let waiting = instance_state(&m.h, "alpha");
    assert!(
        matches!(waiting, machine::InstanceState::WaitingForOperator { .. }),
        "{waiting:?}"
    );
    assert!(
        waiting.sentence().contains("Accessibility"),
        "{}",
        waiting.sentence()
    );
}

/// Open windows learn of a transition once, and of nothing when nothing moved.
#[tokio::test]
async fn a_state_change_is_announced_once_and_an_unchanged_probe_not_at_all() {
    let m = machine_harness("none", &["alpha"]).await;
    select(&m.h, "alpha");
    machine::apply_machine_services(&m.h.orch, "test").await;
    let cell = machine_cell(&m.h, "alpha").unwrap();
    let incarnation = cell.serving().unwrap();
    // The boot's probe and the first pass's: from here the task sleeps, and
    // cannot announce a change this test means to observe itself.
    eventually("the first pass", || {
        m.h.host.probes.load(Ordering::Acquire) >= 2
    })
    .await;
    let settle = |orch: &Orchestrator, cell: &Arc<Cell>| machine::announce_state(orch, cell);
    settle(&m.h.orch, &cell);
    assert!(
        !settle(&m.h.orch, &cell),
        "nothing moved since the last announcement"
    );

    let probe =
        |answer: &str| *m.h.host.probe_answer.lock().unwrap() = Some(Ok(answer.to_string()));
    probe(r#"{"ready": true}"#);
    let report = machine::probe_once(&m.h.orch, &cell, &incarnation).await;
    *cell.machine.as_ref().unwrap().probe.lock().unwrap() = Some(report);
    assert!(
        !settle(&m.h.orch, &cell),
        "a probe that says the same thing stays silent"
    );

    probe(r#"{"ready": false, "reason": "permission-denied"}"#);
    let report = machine::probe_once(&m.h.orch, &cell, &incarnation).await;
    *cell.machine.as_ref().unwrap().probe.lock().unwrap() = Some(report);
    assert!(settle(&m.h.orch, &cell), "ready to waiting is a change");
    assert!(!settle(&m.h.orch, &cell), "and it is announced once");
}

#[tokio::test]
async fn a_declaration_without_a_size_fails_naming_it() {
    let m = machine_harness("none", &["alpha"]).await;
    let manifest: String = native_manifest("none")
        .lines()
        .filter(|line| !line.trim_start().starts_with("size:"))
        .map(|line| format!("{line}\n"))
        .collect();
    std::fs::write(m._pack.path().join("services/probe/service.yaml"), manifest).unwrap();
    select(&m.h, "alpha");
    let machine::InstanceState::Failed { reason } = instance_state(&m.h, "alpha") else {
        panic!("{:?}", instance_state(&m.h, "alpha"));
    };
    assert!(reason.contains("`size`"), "{reason}");
}

#[tokio::test]
async fn a_native_resident_whose_first_probe_fails_does_not_boot() {
    let m = machine_harness("none", &["alpha"]).await;
    *m.h.host.probe_answer.lock().unwrap() = Some(Ok("not json".to_string()));
    select(&m.h, "alpha");
    machine::apply_machine_services(&m.h.orch, "test").await;
    let cell = machine_cell(&m.h, "alpha").unwrap();
    assert!(cell.serving().is_none());
    assert_eq!(
        cell.state.lock().unwrap().status,
        ServiceState::Backoff,
        "a resident that cannot start backs off rather than retrying hot"
    );
}

#[tokio::test]
async fn a_machine_service_is_not_called_as_a_service() {
    let m = machine_harness("none", &["alpha"]).await;
    let error = call(
        &m.h.orch,
        CallRequest {
            owner: ServiceOwner::pack("kit", "alpha"),
            ..request("probe", "status", agent())
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "not_callable");
    assert!(error.message.contains("over MCP"), "{}", error.message);
}

/// One facade per instance, owned by its cell: a second claimant on one
/// instance's lease is the herd CAIRN-4205 found.
#[tokio::test]
async fn an_instance_keeps_one_facade_and_each_connection_gets_a_fresh_process() {
    let m = machine_harness("mcp-stdio", &["alpha", "beta"]).await;
    select(&m.h, "alpha");
    select(&m.h, "beta");
    machine::apply_machine_services(&m.h.orch, "test").await;
    let alpha = machine_cell(&m.h, "alpha").unwrap();
    let beta = machine_cell(&m.h, "beta").unwrap();
    let probe = machine::facade_for(&m.h.orch, &alpha).unwrap();
    let verb = machine::facade_for(&m.h.orch, &alpha).unwrap();
    assert!(Arc::ptr_eq(&probe, &verb), "one instance, one facade");
    assert_eq!(probe.key(), "kit/probe@alpha");
    assert_eq!(probe.label(), "kit/probe on alpha");
    let elsewhere = machine::facade_for(&m.h.orch, &beta).unwrap();
    assert!(!Arc::ptr_eq(&probe, &elsewhere));

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = probe.open().await.unwrap();
    stream.write_all(b"{\"jsonrpc\":\"2.0\"}\n").await.unwrap();
    let mut echoed = [0u8; 18];
    stream.read_exact(&mut echoed).await.unwrap();
    assert_eq!(
        &echoed, b"{\"jsonrpc\":\"2.0\"}\n",
        "stdin and stdout are the resident's"
    );
    let first = alpha.serving().unwrap().generation;

    // A second connection cannot share the first one's MCP session.
    let _second = probe.open().await.unwrap();
    let second = alpha.serving().unwrap().generation;
    assert_ne!(first, second, "a fresh process for a fresh connection");
    assert!(
        alpha.state.lock().unwrap().crashes.is_empty(),
        "letting a resident go is not a crash"
    );
}

// Machine services as MCP servers: `cairn://mcp/<server>/<tool>` routed to one
// machine's resident by the batch's executor selector.

/// A gateway that answers every placed call at once and records which
/// machine's facade each one went to, with the bound it was handed.
#[derive(Default)]
struct PlacedCalls {
    calls: StdMutex<Vec<(String, String, Option<u32>)>>,
    /// How long each call takes. It holds `slot` meanwhile, as the real
    /// gateway holds a machine's one connection for the whole call.
    delay: StdMutex<std::time::Duration>,
    slot: tokio::sync::Mutex<()>,
    /// Answer with a request for follow-up input instead of a result.
    asks_for_input: std::sync::atomic::AtomicBool,
    /// A call whose future was dropped before it answered, which is what
    /// cancels the request at a real resident.
    abandoned: std::sync::atomic::AtomicBool,
}

impl PlacedCalls {
    fn calls(&self) -> Vec<(String, String, Option<u32>)> {
        self.calls.lock().unwrap().clone()
    }
}

/// Marks its call abandoned when dropped before it is disarmed.
struct Unanswered<'a>(&'a std::sync::atomic::AtomicBool, bool);

impl Drop for Unanswered<'_> {
    fn drop(&mut self) {
        if !self.1 {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[async_trait::async_trait]
impl crate::mcp::gateway::McpGateway for PlacedCalls {
    async fn call_placed_tool_once(
        &self,
        facade: Arc<dyn crate::mcp::gateway::PlacedFacade>,
        tool: &str,
        _: Value,
        timeout_ms: Option<u32>,
    ) -> Result<crate::mcp::gateway::McpCallOutcome, String> {
        let key = facade.key().to_string();
        self.calls
            .lock()
            .unwrap()
            .push((key.clone(), tool.to_string(), timeout_ms));
        if self
            .asks_for_input
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(crate::mcp::gateway::McpCallOutcome::InputRequired {
                input_requests: json!({}),
                request_state: None,
            });
        }
        let mut pending = Unanswered(&self.abandoned, false);
        let _held = self.slot.lock().await;
        let delay = *self.delay.lock().unwrap();
        tokio::time::sleep(delay).await;
        pending.1 = true;
        Ok(crate::mcp::gateway::McpCallOutcome::Complete(
            crate::mcp::gateway::McpToolCallResult::from_text(format!("{tool} answered on {key}")),
        ))
    }
    async fn list_tools(
        &self,
        _: &str,
        _: &str,
        _: &crate::security::BrokeredMcpConfig,
    ) -> Result<crate::mcp::gateway::McpToolCatalog, String> {
        Ok(Default::default())
    }
    async fn list_resources(
        &self,
        _: &str,
        _: &str,
        _: &crate::security::BrokeredMcpConfig,
    ) -> Result<Vec<crate::mcp::gateway::McpResourceDef>, String> {
        Ok(Vec::new())
    }
    async fn read_resource(
        &self,
        _: &str,
        _: &str,
        _: &crate::security::BrokeredMcpConfig,
        _: &str,
    ) -> Result<String, String> {
        Ok(String::new())
    }
    async fn call_tool_once(
        &self,
        _: &str,
        _: &str,
        _: &crate::security::BrokeredMcpConfig,
        _: &str,
        _: Value,
        _: Option<Value>,
        _: Option<String>,
        _: Option<u32>,
        _: Option<&str>,
    ) -> Result<crate::mcp::gateway::McpCallOutcome, String> {
        Err("a machine-service call never reaches a configured server".to_string())
    }
    async fn close_session(&self, _: &str) {}
}

/// The pack `kit`, installed in the workspace from a bundle, shipping the
/// MCP-speaking service `probe` and the machine-service entry `probe-mcp`
/// that reaches it, selected and running on each of `serving`.
async fn served_harness(
    machines: &[&str],
    serving: &[&str],
) -> (MachineHarness, tempfile::TempDir, Arc<PlacedCalls>) {
    let m = machine_harness("mcp-stdio", machines).await;
    let resources = tempfile::tempdir().unwrap();
    let root = resources.path().join("packs/kit");
    std::fs::create_dir_all(root.join("services/probe")).unwrap();
    std::fs::write(
        root.join("cairn-pack.yaml"),
        "id: kit\nname: Kit\nversion: 1.0.0\n",
    )
    .unwrap();
    std::fs::write(
        root.join("mcp.yaml"),
        "mcpServers:\n  probe-mcp:\n    type: machine-service\n    service: probe\n",
    )
    .unwrap();
    std::fs::write(
        root.join("services/probe/service.yaml"),
        native_manifest("mcp-stdio"),
    )
    .unwrap();
    cairn_packs::sync_one_pack(
        &crate::services::RealGitClient,
        &crate::services::RealFileSystem,
        resources.path(),
        &m.h.orch.config_dir,
        "kit",
    )
    .unwrap();
    let gateway = Arc::new(PlacedCalls::default());
    assert!(m.h.orch.set_mcp_gateway(gateway.clone()).is_ok());
    for machine in serving {
        select(&m.h, machine);
    }
    machine::apply_machine_services(&m.h.orch, "test").await;
    for machine in serving {
        assert_eq!(instance_state(&m.h, machine), machine::InstanceState::Ready);
    }
    (m, resources, gateway)
}

/// The text of a `run` call's envelope.
async fn run_batch(h: &Harness, payload: Value) -> String {
    let envelope = crate::mcp::handlers::run::handle_run(
        &h.orch,
        &McpCallbackRequest {
            tool: "run".into(),
            payload,
            ..Default::default()
        },
    )
    .await;
    serde_json::from_str::<Value>(&envelope)
        .ok()
        .and_then(|value| value["text"].as_str().map(str::to_string))
        .unwrap_or(envelope)
}

#[tokio::test]
async fn one_serving_machine_takes_a_call_with_no_selector() {
    let (m, _resources, gateway) = served_harness(&["alpha", "beta"], &["alpha"]).await;
    let out = run_batch(
        &m.h,
        json!({"commands": [{"target": "cairn://mcp/probe-mcp/look", "timeout": 600000}]}),
    )
    .await;
    assert!(out.contains("look answered on kit/probe@alpha"), "{out}");
    // The declared bound reaches the resident unclamped: a machine-service
    // call runs to its own timeout like any MCP-tool item, past the window.
    assert_eq!(
        gateway.calls(),
        [(
            "kit/probe@alpha".to_string(),
            "look".to_string(),
            Some(600_000)
        )]
    );
}

/// Two serving machines and no selector: refused naming both, and nothing
/// reaches either resident, not even the items the refusal would not need.
#[tokio::test]
async fn several_serving_machines_and_no_selector_run_nothing() {
    let (m, _resources, gateway) = served_harness(&["alpha", "beta"], &["alpha", "beta"]).await;
    let out = run_batch(
        &m.h,
        json!({"commands": [
            {"target": "cairn://mcp/probe-mcp/look"},
            {"target": "cairn://mcp/probe-mcp/click"},
        ]}),
    )
    .await;
    assert!(out.contains("No commands ran."), "{out}");
    assert!(
        out.contains("commands[0] (cairn://mcp/probe-mcp/look)"),
        "{out}"
    );
    assert!(
        out.contains("commands[1] (cairn://mcp/probe-mcp/click)"),
        "{out}"
    );
    assert!(
        out.contains("alpha (linux, ready), beta (linux, ready)"),
        "{out}"
    );
    assert!(
        out.contains(
            "run({executor:{name:\"alpha\"}, commands:[{target:\"cairn://mcp/probe-mcp/look\""
        ),
        "{out}"
    );
    assert!(gateway.calls().is_empty(), "{:?}", gateway.calls());

    let chosen = run_batch(
        &m.h,
        json!({
            "executor": {"name": "beta"},
            "commands": [
                {"target": "cairn://mcp/probe-mcp/look"},
                {"target": "cairn://mcp/probe-mcp/click"},
            ],
        }),
    )
    .await;
    assert!(
        chosen.contains("look answered on kit/probe@beta"),
        "{chosen}"
    );
    assert!(
        chosen.contains("cairn://mcp/probe-mcp/look on beta")
            && chosen.contains("cairn://mcp/probe-mcp/click on beta"),
        "each item names the machine that served it: {chosen}"
    );
    let machines: Vec<String> = gateway.calls().into_iter().map(|call| call.0).collect();
    assert_eq!(machines, ["kit/probe@beta", "kit/probe@beta"]);
}

#[tokio::test]
async fn a_selector_naming_a_machine_that_does_not_serve_is_refused_naming_those_that_do() {
    let (m, _resources, gateway) = served_harness(&["alpha", "beta"], &["alpha"]).await;
    let out = run_batch(
        &m.h,
        json!({
            "executor": {"name": "beta"},
            "commands": [{"target": "cairn://mcp/probe-mcp/look"}],
        }),
    )
    .await;
    assert!(
        out.contains("`probe-mcp` is not served by `beta`; it serves on alpha (linux, ready)."),
        "{out}"
    );
    assert!(gateway.calls().is_empty());

    // The selector chooses a machine only for machine-service calls.
    let mixed = run_batch(
        &m.h,
        json!({
            "executor": {"name": "alpha"},
            "commands": [
                {"target": "cairn://mcp/probe-mcp/look"},
                {"target": "cairn://models/decide"},
            ],
        }),
    )
    .await;
    assert!(
        mixed.contains("take no machine: commands[1] (cairn://models/decide)"),
        "{mixed}"
    );
    assert!(gateway.calls().is_empty());
}

/// The read renders each machine with its own state, whether or not one is
/// ready, and a call to a server no machine serves says so by machine.
#[tokio::test]
async fn a_machine_service_with_no_serving_machine_reads_and_refuses_by_machine() {
    let (m, _resources, gateway) = served_harness(&["alpha"], &[]).await;
    select(&m.h, "gamma");
    let read = crate::mcp::handlers::mcp_resources::handle_mcp_read(
        &m.h.orch,
        &McpCallbackRequest::default(),
        Some("probe-mcp".to_string()),
        None,
    )
    .await;
    assert!(
        read.contains("# MCP server: probe-mcp (machine service kit/probe)"),
        "{read}"
    );
    assert!(read.contains("No machine serves it now"), "{read}");
    assert!(read.contains("## gamma"), "{read}");
    assert!(
        read.contains("- State: not running: machine absent"),
        "{read}"
    );

    let out = run_batch(
        &m.h,
        json!({"commands": [{"target": "cairn://mcp/probe-mcp/look"}]}),
    )
    .await;
    assert!(
        out.contains(
            "`probe-mcp` is served by no machine now: gamma (not running: machine absent)."
        ),
        "{out}"
    );
    assert!(gateway.calls().is_empty());
}

/// Option A of cairn/5991: a machine-service call declared longer than the
/// synchronous window parks its batch, keeps running on the resident while the
/// turn is parked, and the parked call resumes with its result. It is never
/// cut at the window, and the resident is handed the declared bound.
#[tokio::test(flavor = "multi_thread")]
async fn a_long_machine_service_call_parks_and_resumes_with_its_result() {
    use crate::mcp::handlers::run::host_batch_tests as batches;
    let (m, _resources, gateway) = served_harness(&["alpha"], &["alpha"]).await;
    *gateway.delay.lock().unwrap() = std::time::Duration::from_millis(800);
    batches::seed_parkable_run(&m.h.orch).await;

    let text = batches::run_parkable_item(
        &m.h.orch,
        "cairn://mcp/probe-mcp/run on alpha",
        batches::machine_service_spec("probe-mcp", "run", Some(5_000), probe_key(), "alpha"),
        std::time::Duration::from_millis(100),
    )
    .await;
    assert!(
        text.contains(crate::mcp::handlers::run::RUN_BATCH_SUSPENDED_MARKER),
        "{text}"
    );
    let (state, condition) = batches::wait_row(&m.h.orch).await;
    assert_eq!(state, "pending");
    assert!(condition.contains("\"host\":true"), "{condition}");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let result = loop {
        if let Some(result) =
            batches::tool_result_containing(&m.h.orch, "run answered on kit/probe@alpha").await
        {
            break result;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the parked call resumes with the resident's result"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    let resumed: Value = serde_json::from_str(&result).unwrap();
    assert_eq!(resumed["toolUseId"], "tool-1");
    assert_eq!(resumed["toolResult"], "run answered on kit/probe@alpha");
    assert_eq!(resumed["isError"], false);
    assert!(
        !gateway.abandoned.load(std::sync::atomic::Ordering::SeqCst),
        "parking did not cancel the call"
    );
    assert_eq!(
        gateway.calls(),
        [(
            "kit/probe@alpha".to_string(),
            "run".to_string(),
            Some(5_000)
        )]
    );
}

/// A durable continuation resumes on a pooled gateway session, which a placed
/// facade does not have, so a tool that asks for input fails saying so.
#[tokio::test]
async fn a_machine_service_tool_that_asks_for_input_fails_saying_it_cannot_suspend() {
    let (m, _resources, gateway) = served_harness(&["alpha"], &["alpha"]).await;
    gateway
        .asks_for_input
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let out = run_batch(
        &m.h,
        json!({"commands": [{"target": "cairn://mcp/probe-mcp/look"}]}),
    )
    .await;
    assert!(
        out.contains(
            "MCP call 'probe-mcp/look' asked for follow-up input, and a machine-service tool cannot suspend for follow-up input"
        ),
        "{out}"
    );
}

/// A machine waiting for its operator has its resident up, so a call that
/// names it reaches it (Axon's `permit`, which asks for the grant). It never
/// serves a call that does not name it.
#[tokio::test]
async fn a_machine_waiting_for_its_operator_takes_a_call_that_names_it() {
    let (m, _resources, gateway) = served_harness(&["alpha"], &[]).await;
    *m.h.host.probe_answer.lock().unwrap() = Some(Ok(
        r#"{"ready":false,"reason":"permission-denied"}"#.to_string(),
    ));
    select(&m.h, "alpha");
    machine::apply_machine_services(&m.h.orch, "test").await;
    assert!(
        matches!(
            instance_state(&m.h, "alpha"),
            machine::InstanceState::WaitingForOperator { .. }
        ),
        "{:?}",
        instance_state(&m.h, "alpha")
    );

    let unnamed = run_batch(
        &m.h,
        json!({"commands": [{"target": "cairn://mcp/probe-mcp/look"}]}),
    )
    .await;
    assert!(unnamed.contains("served by no machine now"), "{unnamed}");
    assert!(
        unnamed.contains("still takes a call that names it"),
        "{unnamed}"
    );
    assert!(gateway.calls().is_empty());

    let named = run_batch(
        &m.h,
        json!({
            "executor": {"name": "alpha"},
            "commands": [{"target": "cairn://mcp/probe-mcp/permit"}],
        }),
    )
    .await;
    assert!(
        named.contains("permit answered on kit/probe@alpha"),
        "{named}"
    );
}

/// An `on_demand` service with no call for its idle stop is stopped, not
/// stickily, and the next call starts it again; a call in flight holds it.
#[tokio::test]
async fn an_idle_on_demand_service_stops_and_the_next_call_starts_it() {
    let h = harness().await;
    *h.source.idle.lock().unwrap() = Some(Duration::from_millis(150));
    call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    assert_eq!(
        status(&h, "echo").idle_stop_after,
        Some(Duration::from_millis(150))
    );

    // A call in flight past the idle stop keeps the process.
    let orch = h.orch.clone();
    let slow = tokio::spawn(async move { call(&orch, request("echo", "slow", agent())).await });
    eventually("the slow call", || h.host.frames_of("call").len() == 2).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(status(&h, "echo").state, ServiceState::Running);
    h.host.release_slow.notify_waiters();
    assert!(slow.await.unwrap().is_ok());

    eventually("the idle stop", || {
        status(&h, "echo").state == ServiceState::Stopped
    })
    .await;
    assert!(
        !status(&h, "echo").sticky_stop,
        "an idle stop is not sticky"
    );
    eventually("the drain", || h.host.frames_of("drain").len() == 1).await;
    let log = log_texts(&h, "echo").await;
    assert!(
        log.iter().any(|text| text.starts_with("no call for 150ms")),
        "{log:?}"
    );

    let value = call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    assert_eq!(value["generation"], 2, "the next call boots it again");
    assert_eq!(h.host.launches.load(Ordering::Acquire), 2);
}

/// A deadline that passes while the call waits for a place in the service's
/// concurrency gate sends nothing (no call, so nothing to cancel) and still
/// ends the delegated run minted for it, bearer and all.
#[tokio::test]
async fn a_deadline_before_dispatch_sends_nothing_and_ends_its_delegation() {
    let h = harness().await;
    h.source.put(
        "narrow",
        "name: Narrow\nlimits:\n  concurrency: 1\n  callTimeoutSeconds: 5\n",
        "serve({})",
    );
    let orch = h.orch.clone();
    let holder = tokio::spawn(async move { call(&orch, request("narrow", "slow", agent())).await });
    let holder_frame = eventually_frame(&h, "slow").await;
    let (holder_run, _) = authority_of(&holder_frame);
    let incarnation = h
        .orch
        .project_services
        .supervisor
        .cell("p", "cairn", &ServiceOwner::Project, "narrow")
        .serving()
        .unwrap();

    let timeout = Duration::from_millis(200);
    let mut hang = request("narrow", "hang", agent());
    hang.timeout = Some(timeout);
    let orch = h.orch.clone();
    let caller = tokio::spawn(async move { call(&orch, hang).await });
    eventually("the call to queue behind the held place", || {
        incarnation.waiting.load(Ordering::Acquire) == 1
    })
    .await;
    // The deadline was fixed before the call queued, so it has certainly
    // passed once `timeout` has elapsed from here, however slow the host.
    let queued = Instant::now();
    while queued.elapsed() <= timeout {
        tokio::time::sleep(timeout.saturating_sub(queued.elapsed()) + Duration::from_millis(1))
            .await;
    }
    assert!(
        !caller.is_finished(),
        "the call is still queued for the gate"
    );

    // Delegation precedes admission: the queued call holds a live run.
    let standing = status(&h, "narrow").run_id.unwrap();
    let job = crate::project_services::rows::ensure_service_job(
        &h.orch.db.local,
        "p",
        &ServiceOwner::Project,
        "narrow",
    )
    .await
    .unwrap();
    let delegated_where = format!(
        "FROM runs WHERE job_id = '{job}' AND status = 'live' AND id NOT IN ('{standing}', '{holder_run}')"
    );
    assert_eq!(
        count(&h, &format!("SELECT COUNT(*) {delegated_where}")).await,
        1
    );
    let delegated = h
        .orch
        .db
        .local
        .query_opt_text(format!("SELECT id {delegated_where}"), ())
        .await
        .unwrap()
        .unwrap();
    assert!(h.orch.project_services.runs.get(&delegated).is_some());
    assert!(!h.orch.run_bearers.run_is_revoked(&delegated));

    h.host.release_slow.notify_waiters();
    assert!(holder.await.unwrap().is_ok());
    let error = caller.await.unwrap().unwrap_err();
    assert_eq!(error.code, "deadline", "{error:?}");
    assert_eq!(
        error.message, DEADLINE_BEFORE_SEND,
        "a call admitted past its deadline is refused before sending"
    );

    flush(&h, "narrow").await;
    assert!(
        calls_of(&h, "hang").is_empty(),
        "{:?}",
        calls_of(&h, "hang")
    );
    assert!(h.host.frames_of("cancel").is_empty());
    assert!(h.orch.run_bearers.run_is_revoked(&delegated));
    assert!(h.orch.project_services.runs.get(&delegated).is_none());
    assert_eq!(
        count(
            &h,
            &format!("SELECT COUNT(*) FROM runs WHERE id = '{delegated}' AND status = 'exited'")
        )
        .await,
        1
    );
}

#[tokio::test]
async fn calls_past_concurrency_and_queue_are_busy() {
    let h = harness().await;
    h.source.put(
        "narrow",
        "name: Narrow\nlimits:\n  concurrency: 1\n  callTimeoutSeconds: 5\n",
        "serve({})",
    );
    let mut held = Vec::new();
    // One in flight, then the queue (twice the concurrency).
    for expected in 1..=3 {
        let orch = h.orch.clone();
        held.push(tokio::spawn(async move {
            call(&orch, request("narrow", "slow", agent())).await
        }));
        if expected == 1 {
            eventually("the first call", || h.host.frames_of("call").len() == 1).await;
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let busy = call(&h.orch, request("narrow", "echo", agent()))
        .await
        .unwrap_err();
    assert_eq!(busy.code, "busy");
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.host.release_slow.notify_waiters();
    }
    for call in held {
        assert!(call.await.unwrap().is_ok());
    }
}

#[tokio::test]
async fn a_stop_is_sticky_until_a_start() {
    let h = harness().await;
    call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap();
    stop(
        &h.orch,
        &ServiceRef::project("p", "cairn", "echo"),
        "cairn://p/cairn/12/1/builder",
    )
    .await
    .unwrap();
    assert_eq!(status(&h, "echo").state, ServiceState::Stopped);
    let error = call(&h.orch, request("echo", "echo", agent()))
        .await
        .unwrap_err();
    assert_eq!(error.code, "service_stopped");
    // A source change does not undo an explicit stop.
    h.source.put("echo", ECHO, "serve({ v: 3 })");
    apply_source(&h.orch, "p", &ServiceOwner::Project, "base advance", None).await;
    assert_eq!(
        call(&h.orch, request("echo", "echo", agent()))
            .await
            .unwrap_err()
            .code,
        "service_stopped"
    );
    start(
        &h.orch,
        &ServiceRef::project("p", "cairn", "echo"),
        "operator",
    )
    .await
    .unwrap();
    assert!(call(&h.orch, request("echo", "echo", agent()))
        .await
        .is_ok());
    let log = log_texts(&h, "echo").await;
    assert!(log.iter().any(|text| text == "stopped"), "{log:?}");
}

async fn eventually_frame(h: &Harness, method: &str) -> Value {
    eventually("the call frame", || !calls_of(h, method).is_empty()).await;
    calls_of(h, method).remove(0)
}
