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
    async fn launch(&self, _orch: &Orchestrator, spec: &LaunchSpec) -> Result<HostProcess, String> {
        self.launches.fetch_add(1, Ordering::AcqRel);
        self.envs.lock().unwrap().push(spec.env.clone());
        let boot = self.next_boot.lock().unwrap().take().unwrap_or(Boot::Ready);
        let generation = spec.generation;
        let (lines, mut input) = mpsc::unbounded_channel::<Option<Vec<u8>>>();
        let (events, receiver) = mpsc::unbounded_channel();
        let host = self.clone();
        tokio::spawn(async move {
            let mut slow: Vec<String> = Vec::new();
            let mut draining = false;
            let finish = |events: &mpsc::UnboundedSender<HostEvent>, code| {
                let _ = events.send(HostEvent::Exited {
                    code,
                    detail: "the process exited".into(),
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
        match owner {
            ServiceOwner::Project => Ok(self.0.clone()),
            // A thread's services come from the real store, so a test writes
            // them the way a thread does.
            ServiceOwner::Thread { .. } => OwnerSources.open(orch, project_id, owner).await,
        }
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
            .run_id,
        standing
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

#[tokio::test]
async fn a_deadline_cancels_the_call_and_revokes_its_bearer() {
    let h = harness().await;
    let mut hang = request("echo", "hang", agent());
    hang.timeout = Some(Duration::from_millis(200));
    let error = call(&h.orch, hang).await.unwrap_err();
    assert_eq!(error.code, "deadline");
    let call_frame = &h.host.frames_of("call")[0];
    eventually("the cancel frame", || {
        !h.host.frames_of("cancel").is_empty()
    })
    .await;
    assert_eq!(h.host.frames_of("cancel")[0]["id"], call_frame["id"]);
    let (_, token) = authority_of(call_frame);
    assert!(matches!(
        h.orch.run_bearers.verify(&token, &local()),
        Err(crate::mcp::run_bearer::BearerRefusal::Revoked { .. })
    ));
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
            scope: DelegatedScope::ThreadDb { .. },
            ..
        }
    ));
    // What `judge` would do with a page's submission: a run, refused.
    let attempt = McpCallbackRequest {
        tool: "run".into(),
        payload: json!({"commands": [{"code": "print(1)", "interpreter": "python"}]}),
        run_id: Some(run_id),
        ..Default::default()
    };
    let refusal = authority::refuse(&h.orch, &attempt).await.unwrap();
    assert!(refusal.contains("cairn/5564"), "{refusal}");
    h.host.release_slow.notify_waiters();
    assert!(in_flight.await.unwrap().is_ok());
    let log = log_texts(&h, "echo").await;
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

/// A patch of `main.ts` is generation 2, and no call fails while the
/// generations switch: calls before, during, and after it are all answered,
/// and a call generation 1 accepted finishes on generation 1.
#[tokio::test]
async fn a_patch_is_the_next_generation_with_no_failed_call_during_the_switch() {
    let h = thread_harness().await;
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
    eventually("the slow call", || h.host.frames_of("call").len() == 1).await;

    let stop = Arc::new(AtomicBool::new(false));
    let (orch, flag) = (h.orch.clone(), stop.clone());
    let callers = tokio::spawn(async move {
        let mut answers = Vec::new();
        while !flag.load(Ordering::Acquire) {
            answers.push(
                call(
                    &orch,
                    tutor_request("echo", thread_agent("cairn://p/cairn/spanish/task/drill")),
                )
                .await,
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        answers
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let status = write_tutor(
        &h,
        "spanish-session",
        json!({"files": {"main.ts": "serve({ v: 2 })"}}),
        true,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(status.state, ServiceState::Running);
    assert_eq!(status.generation, Some(2));
    tokio::time::sleep(Duration::from_millis(20)).await;
    stop.store(true, Ordering::Release);
    let answers = callers.await.unwrap();
    assert!(!answers.is_empty());
    for answer in &answers {
        assert!(
            answer.is_ok(),
            "a call failed during the switch: {answer:?}"
        );
    }
    let after = call(
        &h.orch,
        tutor_request("echo", thread_agent("cairn://p/cairn/spanish")),
    )
    .await
    .unwrap();
    assert_eq!(after["generation"], 2);
    h.host.release_slow.notify_waiters();
    assert_eq!(
        in_flight.await.unwrap().unwrap(),
        json!({"slow": true, "generation": 1})
    );
    // The patch merged files per path: the title survived.
    let stored =
        crate::project_services::thread_store::load(&h.orch.db.local, "t-1", "tutor", None)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(stored.version, 2);
    assert_eq!(stored.data["title"], "Spanish tutor");
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
/// database, and running code for a page is still refused (cairn/5564).
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
            scope: DelegatedScope::ThreadDb { thread },
            ..
        } if thread == "spanish"
    ));
    let attempt = |tool: &str, payload: Value| McpCallbackRequest {
        tool: tool.into(),
        payload,
        run_id: Some(run_id.clone()),
        ..Default::default()
    };
    let run = authority::refuse(
        &h.orch,
        &attempt("run", json!({"commands": [{"code": "print(1)"}]})),
    )
    .await
    .unwrap();
    assert!(run.contains("cairn/5564"), "{run}");
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
        .query_text(&format!("SELECT CAST(({sql}) AS TEXT)"), ())
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
    // own call runs `code` items and its tool targets (an MCP tool, an
    // executor verb), each logged; a workflow is still refused.
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
        "cairn://executors/local/look",
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
        "cairn://executors/local/look",
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
/// an MCP tool or an executor verb reaches its dispatch instead of being
/// refused for that branch; a branch the service names is still refused.
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
        executor.contains(at_dispatch),
        "the executor verb reached its dispatch: {executor}"
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
