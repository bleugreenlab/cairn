use super::*;
use crate::db::DbState;
use crate::operator_requests as requests;
use crate::orchestrator::OrchestratorBuilder;
use crate::services::testing::TestServicesBuilder;
use crate::storage::{LocalDb, SearchIndex};
use cairn_common::identity::{
    Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, CredentialRef,
    VerificationMethod, VerificationRecord, VerificationStatus, VerificationStrength,
};
use cairn_common::protocol::CallbackRequest;
use cairn_common::requests::{FileOperatorRequest, OperatorRequest};
use std::sync::Mutex;

fn commit(letter: char) -> String {
    letter.to_string().repeat(40)
}

#[derive(Clone)]
enum Bootout {
    Removes,
    Fails(String),
    LeavesLoaded,
}

struct FakeLaunchd {
    job: Mutex<ActorJob>,
    bootstraps: Mutex<Vec<PathBuf>>,
    bootouts: Mutex<usize>,
    bootout: Mutex<Bootout>,
}

impl FakeLaunchd {
    fn new(state: ActorState) -> Arc<Self> {
        Arc::new(Self {
            job: Mutex::new(state.into()),
            bootstraps: Mutex::new(vec![]),
            bootouts: Mutex::new(0),
            bootout: Mutex::new(Bootout::Removes),
        })
    }
    fn set(&self, state: ActorState) {
        *self.job.lock().unwrap() = state.into();
    }
    /// An exited definition whose program is `program`, as launchctl prints it.
    fn exited(&self, program: &Path) {
        self.ended(program, "last exit code = 0");
    }
    /// A definition whose process the kernel killed: no exit code is printed.
    fn killed(&self, program: &Path) {
        self.ended(
            program,
            "last exit reason = JETSAM_REASON_MEMORY_PERPROCESSLIMIT",
        );
    }
    fn ended(&self, program: &Path, last_exit: &str) {
        *self.job.lock().unwrap() = ActorJob {
            state: ActorState::Exited,
            pid: None,
            program: Some(program.display().to_string()),
            last_exit: Some(last_exit.into()),
        };
    }
    fn answer_bootout(&self, answer: Bootout) {
        *self.bootout.lock().unwrap() = answer;
    }
    fn bootstraps(&self) -> usize {
        self.bootstraps.lock().unwrap().len()
    }
    fn bootouts(&self) -> usize {
        *self.bootouts.lock().unwrap()
    }
    fn state_now(&self) -> ActorState {
        self.job.lock().unwrap().state
    }
}

#[async_trait]
impl Launchd for FakeLaunchd {
    async fn state(&self) -> ActorJob {
        self.job.lock().unwrap().clone()
    }
    async fn bootstrap(&self, plist: &Path) -> Result<(), String> {
        self.bootstraps.lock().unwrap().push(plist.to_path_buf());
        self.set(ActorState::Running);
        Ok(())
    }
    async fn bootout(&self) -> Result<(), String> {
        *self.bootouts.lock().unwrap() += 1;
        let answer = self.bootout.lock().unwrap().clone();
        match answer {
            Bootout::Removes => {
                self.set(ActorState::Absent);
                Ok(())
            }
            Bootout::Fails(text) => Err(text),
            Bootout::LeavesLoaded => Ok(()),
        }
    }
}

/// A linear history a <- b <- c, with main at b.
struct FixedSource;

#[async_trait]
impl SourceStore for FixedSource {
    async fn resolve(&self, _: &Orchestrator) -> Result<Source, String> {
        Ok(Source {
            project_id: "project".into(),
            project_key: "project".into(),
            checkout: PathBuf::from("/nonexistent/checkout"),
            main_commit: commit('b'),
        })
    }
    async fn contains(
        &self,
        _: &Orchestrator,
        _: &Source,
        ancestor: &str,
        descendant: &str,
    ) -> Result<bool, String> {
        let history = [commit('a'), commit('b'), commit('c')];
        let position = |c: &str| history.iter().position(|h| h == c);
        match (position(ancestor), position(descendant)) {
            (Some(a), Some(d)) => Ok(a <= d),
            _ => Err("unknown commit".into()),
        }
    }
    async fn commits_since(
        &self,
        _: &Orchestrator,
        _: &Source,
        _: &str,
    ) -> Result<CommitsSince, String> {
        Ok(CommitsSince::default())
    }
    async fn signing_identity(
        &self,
        _: &Orchestrator,
        _: &Source,
        _: &str,
    ) -> Result<String, String> {
        Ok("Developer ID Application: Test".into())
    }
    async fn object_repository(&self, _: &Orchestrator, _: &Source) -> Result<PathBuf, String> {
        Ok(PathBuf::from("/nonexistent/store"))
    }
}

struct Fixture {
    orch: Orchestrator,
    root: PathBuf,
    launchd: Arc<FakeLaunchd>,
    consumer: Arc<RebuildConsumer>,
}

impl Fixture {
    fn host(&self) -> &RebuildHost {
        &self.consumer.host
    }
    fn journal(&self, id: &str) -> PathBuf {
        self.host().rebuilds.join(format!("{id}.jsonl"))
    }
    fn append(&self, id: &str, body: Value) {
        let mut event = json!({"event": uuid::Uuid::new_v4().to_string(), "rebuild": id, "at": "2026-11-08T15:00:00Z"});
        event
            .as_object_mut()
            .unwrap()
            .extend(body.as_object().unwrap().clone());
        let mut file = OpenOptions::new()
            .append(true)
            .open(self.journal(id))
            .unwrap();
        writeln!(file, "{event}").unwrap();
    }
}

fn host(root: &Path) -> RebuildHost {
    let app = root.join("Cairn.app");
    fs::create_dir_all(app.join("Contents/MacOS")).unwrap();
    fs::create_dir_all(app.join("Contents/Resources/runtime/rebuild")).unwrap();
    fs::write(app.join("Contents/MacOS/bun"), "bun").unwrap();
    fs::write(
        app.join("Contents/Resources/runtime/rebuild/rebuild.js"),
        "actor",
    )
    .unwrap();
    RebuildHost {
        home: root.join("home"),
        rebuilds: root.join("home/rebuilds"),
        launch_agents: root.join("LaunchAgents"),
        app: Some(app),
    }
}

async fn orchestrator(root: &Path) -> Orchestrator {
    let local = LocalDb::open_migrated_for_test(root.join("requests.db"))
        .await
        .unwrap();
    orchestrator_over(root, Arc::new(local)).await
}

async fn orchestrator_over(root: &Path, local: Arc<LocalDb>) -> Orchestrator {
    // One search index per simulated process: the index holds a writer lock.
    let search = Arc::new(
        SearchIndex::open_or_create(root.join(format!("search-{}", uuid::Uuid::new_v4()))).unwrap(),
    );
    OrchestratorBuilder::new(
        Arc::new(DbState::new(local, search)),
        Arc::new(TestServicesBuilder::new().build()),
        root.join("home"),
    )
    .build()
}

async fn seed(orch: &Orchestrator) {
    for statement in [
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('workspace','W',1,1)",
        "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('project','workspace','P','project','/tmp/requests',1,1)",
        "INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES('thread','project','devops',1,1)",
        "INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at) VALUES('issue','project',1,'Request','active','thread',1,1)",
        "INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('execution','recipe','issue','project','running',1,1)",
        "INSERT INTO jobs(id,execution_id,issue_id,project_id,node_name,status,created_at,updated_at,uri_segment) VALUES('job','execution','issue','project','Builder','running',1,1,'builder')",
        "INSERT INTO runs(id,project_id,issue_id,job_id,status,created_at,updated_at,start_mode) VALUES('run','project','issue','job','live',1,1,'resume')",
    ] {
        orch.db.local.execute(statement, ()).await.unwrap();
    }
}

/// `gate_open` is the production construction; closed is the held path a
/// reviewed `MACOS_FLOOR_PROVEN = false` would give.
async fn fixture_with(state: ActorState, gate_open: bool) -> Fixture {
    let root = tempfile::tempdir().unwrap().keep();
    let orch = orchestrator(&root).await;
    seed(&orch).await;
    let launchd = FakeLaunchd::new(state);
    let consumer = RebuildConsumer::new(host(&root), launchd.clone(), Arc::new(FixedSource));
    let consumer = if gate_open {
        consumer
    } else {
        consumer.with_gate_closed_for_test()
    };
    let consumer = Arc::new(consumer);
    orch.operator_requests.register(consumer.clone()).unwrap();
    Fixture {
        orch,
        root,
        launchd,
        consumer,
    }
}

async fn fixture() -> Fixture {
    fixture_with(ActorState::Absent, true).await
}

fn operator() -> (PrincipalRef, AppearanceSnapshot) {
    let principal = PrincipalRef::Machine {
        device_id: "test-device".into(),
    };
    let verification = VerificationRecord::new(
        VerificationMethod::DesktopCredential,
        VerificationStatus::Verified,
        None,
        None,
        None,
        Some(CredentialRef::new("desktop_operator_credential").unwrap()),
        VerificationStrength::new("local_shared_secret").unwrap(),
        1,
    )
    .unwrap();
    let evidence = AppearanceEvidence::new(
        AppearanceTransport::AuthenticatedDesktop,
        Address::Desktop {
            device_id: "test-device".into(),
        },
        verification,
        1,
        None,
    )
    .unwrap();
    let appearance = AppearanceSnapshot::new(principal.clone(), evidence, vec![], None).unwrap();
    (principal, appearance)
}

async fn file(orch: &Orchestrator) -> OperatorRequest {
    requests::file(
        orch,
        &CallbackRequest {
            run_id: Some("run".into()),
            ..Default::default()
        },
        FileOperatorRequest {
            kind: RequestKind::Rebuild,
            target: None,
            rationale: "verify the landed fix".into(),
            waiting: vec![],
            requirements: Default::default(),
        },
    )
    .await
    .unwrap()
    .request
}

async fn accept(orch: &Orchestrator, request: &OperatorRequest) -> Result<OperatorRequest, String> {
    let (principal, appearance) = operator();
    requests::accept(
        orch,
        &request.id,
        request.revision,
        request.facts.as_ref().unwrap().version,
        &principal,
        &appearance,
    )
    .await
}

async fn get(orch: &Orchestrator, id: &str) -> OperatorRequest {
    request_store::get_request(&orch.db.local, id)
        .await
        .unwrap()
        .unwrap()
}

/// File, accept and deliver the dispatch intent: the one way in.
async fn launched(f: &Fixture) -> (OperatorRequest, String) {
    let request = file(&f.orch).await;
    let accepted = accept(&f.orch, &request).await.unwrap();
    crate::operator_requests::delivery::drain_once(&f.orch)
        .await
        .unwrap();
    let attempt = &accepted.attempts.last().unwrap().acceptance.attempt_id;
    (get(&f.orch, &request.id).await, rebuild_id(attempt))
}

// Holding request dispatch again is a reviewed change to this line and the
// constant: the proof is cairn/6384 comment #13, at main 0c29467f.
const _: () = assert!(
    MACOS_FLOOR_PROVEN,
    "the cairn/6384 floor proof is posted; rebuild dispatch is open on macOS"
);

#[tokio::test]
async fn the_production_construction_dispatches_and_the_kind_is_available() {
    let f = fixture().await;
    assert!(f.consumer.dispatch_enabled);
    f.consumer.availability(&f.orch).await.unwrap();
    let filed = file(&f.orch).await;
    let request = requests::get(&f.orch, &filed.id).await.unwrap().unwrap();
    assert!(request.available, "{:?}", request.unavailable_reason);
}

#[tokio::test]
async fn a_held_consumer_refuses_filing_acceptance_and_launch() {
    for name in [
        "rebuild_floor_refuses_child_writes_even_after_an_approved_crossing_live",
        "rebuild_floor_protects_relocated_roots_and_their_ancestors_live",
        "cairn/6384",
    ] {
        assert!(FLOOR_REFUSAL.contains(name), "{name}");
    }
    let f = fixture_with(ActorState::Absent, false).await;
    // Availability: the kind is UNAVAILABLE, so a requesting thread is refused
    // at once by name instead of leaving a card that can never be accepted.
    let refused = requests::file(
        &f.orch,
        &CallbackRequest {
            run_id: Some("run".into()),
            ..Default::default()
        },
        FileOperatorRequest {
            kind: RequestKind::Rebuild,
            target: None,
            rationale: "verify the landed fix".into(),
            waiting: vec![],
            requirements: Default::default(),
        },
    )
    .await
    .unwrap_err();
    assert!(refused.contains("no live proof yet"), "{refused}");
    assert!(refused.contains("cairn/6384"), "{refused}");

    // validate_snapshot: an acceptance shaped exactly like a real one refuses.
    let open = fixture().await;
    let request = file(&open.orch).await;
    let accepted = accept(&open.orch, &request).await.unwrap();
    let acceptance = accepted.attempts.last().unwrap().acceptance.clone();
    let error = f
        .consumer
        .validate_snapshot(&f.orch, &acceptance)
        .await
        .unwrap_err();
    assert_eq!(error, FLOOR_REFUSAL);

    // launch: a claimed context handed to the gated consumer stages nothing.
    crate::operator_requests::delivery::drain_once(&open.orch)
        .await
        .unwrap();
    let attempt = get(&open.orch, &request.id)
        .await
        .attempts
        .last()
        .unwrap()
        .clone();
    let context = recover_claimed_request(
        &open.orch,
        &attempt.authorization_event_id,
        &attempt.acceptance,
        open.orch.operator_requests.consumer(KIND).unwrap(),
    )
    .await
    .unwrap();
    let id = rebuild_id(&attempt.acceptance.attempt_id);
    let gated = RebuildConsumer::new(
        open.consumer.host.clone(),
        f.launchd.clone(),
        Arc::new(FixedSource),
    )
    .with_gate_closed_for_test();
    assert_eq!(
        gated.launch(&open.orch, &context, &id).await.unwrap_err(),
        FLOOR_REFUSAL
    );
    assert_eq!(f.launchd.bootstraps(), 0);
    assert!(!f.host().launch_agents.exists());
}

#[tokio::test]
async fn an_accepted_request_launches_the_actor_once_from_its_claimed_attempt() {
    let f = fixture().await;
    let (request, id) = launched(&f).await;
    assert_eq!(request.state, RequestState::Acting);
    assert_eq!(f.launchd.bootstraps(), 1);
    let attempt = request.attempts.last().unwrap();
    let row = ledger::get(&f.orch.db.local, &id).await.unwrap().unwrap();
    let launch = row.launch.unwrap();
    assert_eq!(launch.attempt_id, attempt.acceptance.attempt_id);
    assert_eq!(
        launch.authorization_event_id,
        attempt.authorization_event_id
    );
    assert_eq!(launch.commit, commit('b'));
    assert!(launch.bootstrap_requested_at.is_some());
    assert_eq!(row.attempt_id.as_deref(), Some(launch.attempt_id.as_str()));

    let spec: Value = serde_json::from_str(
        &fs::read_to_string(f.host().rebuilds.join(&id).join("spec.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(spec["commit"], commit('b'));
    // The store the acceptance was checked against, never a network remote.
    assert_eq!(spec["origin"], "/nonexistent/store");
    assert_eq!(spec["request"]["attemptId"], attempt.acceptance.attempt_id);
    assert_eq!(
        spec["request"]["authorizationEventId"],
        attempt.authorization_event_id
    );
    assert_eq!(spec["request"]["rebuildId"], id);
    assert_eq!(
        spec["request"]["factsVersion"],
        attempt.acceptance.facts_version
    );
    // A reference, never a bearer.
    let text = spec.to_string();
    for secret in ["binding", "appearance", "MCP_SECRET", "claim"] {
        assert!(!text.contains(secret), "{secret}");
    }
    let plist =
        fs::read_to_string(f.host().launch_agents.join(format!("{ACTOR_LABEL}.plist"))).unwrap();
    assert!(plist.contains(&format!("<string>{ACTOR_LABEL}</string>")));
    assert!(plist.contains("--actor"));
    assert!(!plist.contains("CAIRN_MCP_SECRET") && !plist.contains("CAIRN_RUN_ID"));
    let mode = fs::metadata(f.journal(&id)).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);

    // Redelivery and reconciliation never start a second actor.
    crate::operator_requests::delivery::drain_once(&f.orch)
        .await
        .unwrap();
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert_eq!(f.launchd.bootstraps(), 1);
    assert_eq!(get(&f.orch, &request.id).await.state, RequestState::Acting);
}

#[tokio::test]
async fn the_launch_refuses_while_the_label_is_loaded_by_anyone() {
    for state in [ActorState::Running, ActorState::Unknown] {
        let f = fixture_with(state, true).await;
        let (request, id) = launched(&f).await;
        assert_eq!(f.launchd.bootstraps(), 0, "{state:?}");
        // A running or unreadable label is never booted out.
        assert_eq!(f.launchd.bootouts(), 0, "{state:?}");
        // Nothing was launched, so the refusal settles and the request reopens.
        assert_eq!(request.state, RequestState::Open, "{state:?}");
        let attempt = request.attempts.last().unwrap();
        assert_eq!(attempt.outcome, Some(RequestAttemptOutcome::Failed));
        let report = attempt.report.as_ref().unwrap().to_string();
        assert!(report.contains(ACTOR_LABEL), "{report}");
        let row = ledger::get(&f.orch.db.local, &id).await.unwrap().unwrap();
        assert!(row.launch.unwrap().bootstrap_requested_at.is_none());
        assert!(row.consumer_report.unwrap().acknowledged_at.is_some());
    }
}

/// The staged runtime a launched attempt's definition runs.
fn staged_runtime(f: &Fixture, id: &str) -> PathBuf {
    f.host()
        .rebuilds
        .join(id)
        .join(crate::rebuilds::ACTOR_RUNTIME)
}

#[tokio::test]
async fn an_exited_actor_settles_its_attempt_and_is_booted_out() {
    let f = fixture().await;
    let (request, id) = launched(&f).await;
    f.append(&id, json!({"type":"fact","facts":{"commit": commit('b')}}));
    f.append(
        &id,
        json!({"type":"outcome","outcome":"done","summary":"installed"}),
    );
    // launchd keeps a finished one-shot loaded and not running.
    f.launchd.exited(&staged_runtime(&f, &id));
    assert_eq!(f.consumer.reconcile(&f.orch).await.unwrap(), 1);
    assert_eq!(get(&f.orch, &request.id).await.state, RequestState::Done);
    assert_eq!(f.launchd.bootouts(), 1);
    assert_eq!(f.launchd.state_now(), ActorState::Absent);
    let row = ledger::get(&f.orch.db.local, &id).await.unwrap().unwrap();
    let unload = &row.actor_unloads[..];
    assert_eq!(unload.len(), 1);
    assert_eq!(unload[0].reason, "settled");
    assert_eq!(unload[0].owner.as_deref(), Some(id.as_str()));
    assert!(unload[0].removed);
    assert_eq!(unload[0].last_exit.as_deref(), Some("last exit code = 0"));
    // The report froze from the ledger outcome before the bootout.
    let frozen = row.consumer_report.unwrap();
    assert_eq!(frozen.outcome, RequestAttemptOutcome::Done);
    assert!(frozen.document.get("removedDefinitions").is_none());
    assert_eq!(f.consumer.reconcile(&f.orch).await.unwrap(), 0);
    assert_eq!(f.launchd.bootouts(), 1);
}

#[tokio::test]
async fn a_bootout_the_reconciler_could_not_finish_is_tried_once() {
    let f = fixture().await;
    let (_, id) = launched(&f).await;
    f.append(
        &id,
        json!({"type":"outcome","outcome":"failed","summary":"build failed"}),
    );
    f.launchd.exited(&staged_runtime(&f, &id));
    f.launchd.answer_bootout(Bootout::Fails(
        "Boot-out failed: 5: Input/output error".into(),
    ));
    f.consumer.reconcile(&f.orch).await.unwrap();
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert_eq!(f.launchd.bootouts(), 1);
    let row = ledger::get(&f.orch.db.local, &id).await.unwrap().unwrap();
    assert!(!row.actor_unloads[0].removed);
    assert!(row.actor_unloads[0]
        .detail
        .as_deref()
        .unwrap()
        .contains("Input/output error"));
    assert_eq!(
        row.consumer_report.unwrap().outcome,
        RequestAttemptOutcome::Failed
    );
}

#[tokio::test]
async fn the_fence_boots_out_a_definition_no_request_launched_and_launches() {
    let f = fixture().await;
    // A hand-run rebuild's leftover: a journalled row with no launch intent.
    let stale = "20261108T193554-1890af";
    create_private_dir(&f.host().rebuilds).unwrap();
    write_private(&f.journal(stale), b"", 0o600).unwrap();
    f.append(
        stale,
        json!({"type":"outcome","outcome":"done","summary":"installed"}),
    );
    f.launchd.exited(&staged_runtime(&f, stale));
    // The reconciler never adopts or unloads a definition it did not launch.
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert_eq!(f.launchd.bootouts(), 0);

    let (request, id) = launched(&f).await;
    assert_eq!(f.launchd.bootouts(), 1);
    assert_eq!(f.launchd.bootstraps(), 1);
    assert_eq!(request.state, RequestState::Acting);
    let row = ledger::get(&f.orch.db.local, &id).await.unwrap().unwrap();
    assert_eq!(row.actor_unloads.len(), 1);
    let unload = &row.actor_unloads[0];
    assert_eq!(unload.reason, "fence");
    assert_eq!(unload.owner, None);
    assert!(unload.removed);
    assert_eq!(
        unload.program.as_deref(),
        Some(staged_runtime(&f, stale).display().to_string().as_str())
    );

    // The new actor's own outcome is its report, naming what was removed.
    f.append(&id, json!({"type":"fact","facts":{"commit": commit('b')}}));
    f.append(
        &id,
        json!({"type":"outcome","outcome":"done","summary":"installed"}),
    );
    f.launchd.set(ActorState::Absent);
    assert_eq!(f.consumer.reconcile(&f.orch).await.unwrap(), 1);
    let after = get(&f.orch, &request.id).await;
    assert_eq!(after.state, RequestState::Done);
    let report = after
        .attempts
        .last()
        .unwrap()
        .report
        .as_ref()
        .unwrap()
        .to_string();
    assert!(report.contains("removedDefinitions"), "{report}");
    assert!(report.contains("no request launched"), "{report}");
    assert!(report.contains(stale), "{report}");
}

#[tokio::test]
async fn the_fence_settles_an_exited_launched_actor_before_booting_it_out() {
    let f = fixture().await;
    let (earlier, earlier_id) = launched(&f).await;
    f.append(
        &earlier_id,
        json!({"type":"outcome","outcome":"failed","summary":"build failed"}),
    );
    f.launchd.exited(&staged_runtime(&f, &earlier_id));
    // A later attempt's fence reaches the label before any reconciler pass.
    let attempt = earlier.attempts.last().unwrap();
    let next = "req-next";
    ledger::record_launch_intent(
        &f.orch.db.local,
        next,
        LaunchIntent {
            attempt_id: "next".into(),
            ..intent(&attempt.acceptance, &attempt.authorization_event_id)
        },
    )
    .await
    .unwrap();
    f.consumer.fence(&f.orch, next).await.unwrap();
    assert_eq!(f.launchd.bootouts(), 1);
    let settled = get(&f.orch, &earlier.id).await;
    assert_eq!(settled.state, RequestState::Open);
    assert_eq!(
        settled.attempts.last().unwrap().outcome,
        Some(RequestAttemptOutcome::Failed)
    );
    let row = ledger::get(&f.orch.db.local, next).await.unwrap().unwrap();
    assert_eq!(
        row.actor_unloads[0].owner.as_deref(),
        Some(earlier_id.as_str())
    );
    assert!(row.actor_unloads[0].removed);
}

#[tokio::test]
async fn a_bootout_that_fails_or_leaves_the_job_refuses_the_launch() {
    for (answer, expected) in [
        (
            Bootout::Fails(
                "launchctl bootout exited Some(5): Boot-out failed: 5: Input/output error".into(),
            ),
            "Input/output error",
        ),
        (Bootout::LeavesLoaded, "still shows"),
    ] {
        let f = fixture().await;
        f.launchd.exited(Path::new("/elsewhere/bun"));
        f.launchd.answer_bootout(answer);
        let (request, _) = launched(&f).await;
        assert_eq!(f.launchd.bootstraps(), 0, "{expected}");
        assert_eq!(request.state, RequestState::Open, "{expected}");
        let report = request
            .attempts
            .last()
            .unwrap()
            .report
            .as_ref()
            .unwrap()
            .to_string();
        assert!(report.contains(expected), "{report}");
        assert!(report.contains("/elsewhere/bun"), "{report}");
    }
}

#[tokio::test]
async fn the_launch_refuses_without_an_acting_claimed_attempt() {
    let f = fixture_with(ActorState::Running, true).await;
    let (request, _) = launched(&f).await;
    assert_eq!(request.state, RequestState::Open);
    // A context recovered for the settled attempt cannot launch anything.
    let attempt = request.attempts.last().unwrap();
    let context = recover_claimed_request(
        &f.orch,
        &attempt.authorization_event_id,
        &attempt.acceptance,
        f.orch.operator_requests.consumer(KIND).unwrap(),
    )
    .await
    .unwrap();
    f.launchd.set(ActorState::Absent);
    assert!(f.consumer.dispatch(&f.orch, &context).await.is_err());
    assert_eq!(f.launchd.bootstraps(), 0);
    assert!(context.recheck(&f.orch).await.is_err());
}

#[tokio::test]
async fn each_actor_outcome_maps_onto_the_request() {
    for (outcome, expected) in [
        ("done", RequestAttemptOutcome::Done),
        ("failed", RequestAttemptOutcome::Failed),
        ("rolled_back", RequestAttemptOutcome::RolledBack),
        ("inconclusive", RequestAttemptOutcome::Inconclusive),
        ("cancelled", RequestAttemptOutcome::Cancelled),
    ] {
        let f = fixture().await;
        let (request, id) = launched(&f).await;
        f.append(
            &id,
            json!({"type":"phase","name":"build","state":"started"}),
        );
        f.append(&id, json!({"type":"fact","facts":{"commit": commit('b')}}));
        f.append(
            &id,
            json!({"type":"outcome","outcome":outcome,"summary":outcome}),
        );
        // Still loaded: the outcome waits for launchd to show the actor gone.
        f.consumer.reconcile(&f.orch).await.unwrap();
        assert_eq!(get(&f.orch, &request.id).await.state, RequestState::Acting);
        f.launchd.set(ActorState::Absent);
        assert_eq!(f.consumer.reconcile(&f.orch).await.unwrap(), 1);
        let after = get(&f.orch, &request.id).await;
        let attempt = after
            .attempts
            .iter()
            .find(|a| rebuild_id(&a.acceptance.attempt_id) == id)
            .unwrap();
        assert_eq!(attempt.outcome.as_ref(), Some(&expected), "{outcome}");
        if expected == RequestAttemptOutcome::Done {
            assert_eq!(after.state, RequestState::Done);
        } else {
            // The attempt stays as it was; the request reopens for a new
            // acceptance. Cancelled is not declined.
            assert_eq!(after.state, RequestState::Open, "{outcome}");
            assert_eq!(attempt.state, RequestState::Failed, "{outcome}");
        }
        let report = attempt.report.as_ref().unwrap().to_string();
        assert!(
            report.contains(&format!("cairn://rebuilds/{id}")),
            "{report}"
        );
        // Settled once: later passes report nothing new.
        assert_eq!(f.consumer.reconcile(&f.orch).await.unwrap(), 0);
    }
}

#[tokio::test]
async fn a_done_that_built_another_commit_is_not_success() {
    let f = fixture().await;
    let (request, id) = launched(&f).await;
    f.append(&id, json!({"type":"fact","facts":{"commit": commit('c')}}));
    f.append(
        &id,
        json!({"type":"outcome","outcome":"done","summary":"done"}),
    );
    f.launchd.set(ActorState::Absent);
    f.consumer.reconcile(&f.orch).await.unwrap();
    let after = get(&f.orch, &request.id).await;
    assert_eq!(after.state, RequestState::Open);
    assert_eq!(
        after.attempts.last().unwrap().outcome,
        Some(RequestAttemptOutcome::Inconclusive)
    );
}

#[tokio::test]
async fn an_actor_that_exits_without_an_outcome_fails_naming_its_phase() {
    let f = fixture().await;
    let (request, id) = launched(&f).await;
    f.append(
        &id,
        json!({"type":"phase","name":"install","state":"started"}),
    );
    f.launchd.set(ActorState::Unknown);
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert_eq!(get(&f.orch, &request.id).await.state, RequestState::Acting);
    // Not running is death evidence, exactly as absent is.
    f.launchd.exited(&staged_runtime(&f, &id));
    f.consumer.reconcile(&f.orch).await.unwrap();
    let after = get(&f.orch, &request.id).await;
    assert_eq!(after.state, RequestState::Open);
    let report = after
        .attempts
        .last()
        .unwrap()
        .report
        .as_ref()
        .unwrap()
        .to_string();
    assert!(report.contains("install"), "{report}");
    assert_eq!(f.launchd.bootouts(), 1);
}

#[tokio::test]
async fn a_killed_actor_settles_as_failed_and_is_booted_out() {
    let f = fixture().await;
    let (request, id) = launched(&f).await;
    f.append(
        &id,
        json!({"type":"phase","name":"build","state":"started"}),
    );
    // Jetsam or SIGKILL leaves no exit code, only launchd's reason for the end.
    f.launchd.killed(&staged_runtime(&f, &id));
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert_eq!(get(&f.orch, &request.id).await.state, RequestState::Open);
    let row = ledger::get(&f.orch.db.local, &id).await.unwrap().unwrap();
    assert_eq!(row.outcome.as_deref(), Some("failed"));
    assert_eq!(row.summary.as_deref(), Some("actor died during build"));
    assert_eq!(
        row.consumer_report.unwrap().outcome,
        RequestAttemptOutcome::Failed
    );
    assert_eq!(f.launchd.bootouts(), 1);
    assert_eq!(
        row.actor_unloads[0].last_exit.as_deref(),
        Some("last exit reason = JETSAM_REASON_MEMORY_PERPROCESSLIMIT")
    );
}

#[tokio::test]
async fn restart_recovery_neither_duplicates_nor_relaunches() {
    let f = fixture().await;
    let (request, id) = launched(&f).await;
    assert_eq!(f.launchd.bootstraps(), 1);

    // A new runner process over the same database and home.
    let restarted = orchestrator_over(&f.root, f.orch.db.local.clone()).await;
    let launchd = FakeLaunchd::new(ActorState::Running);
    let consumer = Arc::new(RebuildConsumer::new(
        f.consumer.host.clone(),
        launchd.clone(),
        Arc::new(FixedSource),
    ));
    restarted
        .operator_requests
        .register(consumer.clone())
        .unwrap();
    crate::operator_requests::delivery::drain_once(&restarted)
        .await
        .unwrap();
    consumer.reconcile(&restarted).await.unwrap();
    launchd.set(ActorState::Unknown);
    consumer.reconcile(&restarted).await.unwrap();
    assert_eq!(launchd.bootstraps(), 0);
    assert_eq!(
        get(&restarted, &request.id).await.state,
        RequestState::Acting
    );

    f.append(&id, json!({"type":"fact","facts":{"commit": commit('b')}}));
    f.append(
        &id,
        json!({"type":"outcome","outcome":"done","summary":"installed"}),
    );
    launchd.set(ActorState::Absent);
    assert_eq!(consumer.reconcile(&restarted).await.unwrap(), 1);
    assert_eq!(get(&restarted, &request.id).await.state, RequestState::Done);
    assert_eq!(launchd.bootstraps(), 0);
}

#[tokio::test]
async fn a_crash_after_report_and_before_acknowledgement_replays_the_same_receipt() {
    let f = fixture().await;
    let (request, id) = launched(&f).await;
    f.append(
        &id,
        json!({"type":"outcome","outcome":"failed","summary":"build failed"}),
    );
    f.launchd.set(ActorState::Absent);
    // Do what the reconciler does, then "crash" before acknowledging.
    crate::rebuilds::ingest_once(&f.orch.db.local, &f.host().rebuilds, ActorState::Absent)
        .await
        .unwrap();
    let row = ledger::get(&f.orch.db.local, &id).await.unwrap().unwrap();
    let frozen = ledger::freeze_report(
        &f.orch.db.local,
        &id,
        decide_report(&row, row.launch.as_ref().unwrap(), ActorState::Absent, 1).unwrap(),
    )
    .await
    .unwrap();
    let attempt = get(&f.orch, &request.id)
        .await
        .attempts
        .last()
        .unwrap()
        .clone();
    let context = recover_claimed_request(
        &f.orch,
        &attempt.authorization_event_id,
        &attempt.acceptance,
        f.orch.operator_requests.consumer(KIND).unwrap(),
    )
    .await
    .unwrap();
    context
        .report(
            &f.orch,
            &frozen.receipt_id,
            ConsumerReport {
                outcome: frozen.outcome.clone(),
                document: frozen.document.clone(),
                settled: true,
                built_commit: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(get(&f.orch, &request.id).await.state, RequestState::Open);

    // The restart's pass replays the receipt as its acknowledgement.
    let restarted = orchestrator_over(&f.root, f.orch.db.local.clone()).await;
    let launchd = FakeLaunchd::new(ActorState::Absent);
    let consumer = Arc::new(RebuildConsumer::new(
        f.consumer.host.clone(),
        launchd.clone(),
        Arc::new(FixedSource),
    ));
    restarted
        .operator_requests
        .register(consumer.clone())
        .unwrap();
    assert_eq!(consumer.reconcile(&restarted).await.unwrap(), 1);
    let row = ledger::get(&restarted.db.local, &id)
        .await
        .unwrap()
        .unwrap();
    assert!(row.consumer_report.unwrap().acknowledged_at.is_some());
    let receipts = restarted
        .db
        .local
        .query_opt_i64(
            "SELECT COUNT(*) FROM request_report_receipts WHERE attempt_id=?1",
            (attempt.acceptance.attempt_id.clone(),),
        )
        .await
        .unwrap();
    assert_eq!(receipts, Some(1));
    assert_eq!(consumer.reconcile(&restarted).await.unwrap(), 0);
    assert_eq!(launchd.bootstraps(), 0);
}

#[tokio::test]
async fn a_claim_an_earlier_runner_never_launched_settles_without_a_launch() {
    let f = fixture().await;
    let request = file(&f.orch).await;
    let accepted = accept(&f.orch, &request).await.unwrap();
    let attempt = accepted.attempts.last().unwrap();
    // An earlier process claimed, then died before recording a launch.
    request_store::claim_request(
        &f.orch.db.local,
        &attempt.authorization_event_id,
        &attempt.acceptance,
        "earlier-claim",
        1,
    )
    .await
    .unwrap();
    f.orch
        .db
        .local
        .execute("UPDATE request_attempts SET updated_at=1", ())
        .await
        .unwrap();
    assert_eq!(f.consumer.reconcile(&f.orch).await.unwrap(), 1);
    let after = get(&f.orch, &request.id).await;
    assert_eq!(after.state, RequestState::Open);
    let report = after
        .attempts
        .last()
        .unwrap()
        .report
        .as_ref()
        .unwrap()
        .to_string();
    assert!(report.contains("before launching"), "{report}");
    assert_eq!(f.launchd.bootstraps(), 0);
}

#[tokio::test]
async fn cancel_writes_the_marker_and_is_refused_past_stop_runner() {
    let f = fixture().await;
    let (request, id) = launched(&f).await;
    let (principal, appearance) = operator();
    f.append(
        &id,
        json!({"type":"phase","name":"build","state":"started"}),
    );
    requests::cancel(&f.orch, &request.id, &principal, &appearance)
        .await
        .unwrap();
    let marker = f.host().rebuilds.join(&id).join("cancel");
    assert!(marker.is_file());
    fs::remove_file(&marker).unwrap();

    f.append(
        &id,
        json!({"type":"phase","name":"build","state":"ended","outcome":"ok"}),
    );
    f.append(
        &id,
        json!({"type":"phase","name":"stop-runner","state":"started"}),
    );
    let refused = requests::cancel(&f.orch, &request.id, &principal, &appearance)
        .await
        .unwrap_err();
    assert!(refused.contains("stop-runner"), "{refused}");
    assert!(!marker.exists());
    // Cancelling never settles the attempt itself.
    assert_eq!(get(&f.orch, &request.id).await.state, RequestState::Acting);
}

#[test]
fn facts_carry_their_age_and_report_checks_without_gating() {
    let observed = 1_800_000_000;
    let facts = assemble_facts(
        FactsInputs {
            source: Source {
                project_id: "project".into(),
                project_key: "cairn".into(),
                checkout: PathBuf::from("/checkout"),
                main_commit: commit('b'),
            },
            running_version: "1.2.3".into(),
            running_build_id: Some("build".into()),
            running_source_commit: Some(commit('a')),
            running_booted_at: Some(observed - 100),
            commits_since: Ok(CommitsSince {
                count: 1,
                capped: false,
                recent: vec![(commit('b'), "land the fix".into())],
            }),
            checks: vec![
                CheckResult {
                    check: "rust-tests".into(),
                    platform: Some("macos".into()),
                    verdict: "fail".into(),
                    commit: commit('b'),
                    ran_at_ms: (observed - 3_600) * 1000,
                },
                CheckResult {
                    check: COMPILE_CHECK.into(),
                    platform: None,
                    verdict: "pass".into(),
                    commit: commit('a'),
                    ran_at_ms: (observed - 60) * 1000,
                },
            ],
            build_seconds: vec![600, 900, 1200],
            signing_identity: Ok("Developer ID Application: Test".into()),
        },
        observed,
    );
    let doc = &facts.document;
    assert_eq!(doc["observedAt"], observed);
    assert_eq!(doc["checks"][0]["ageSeconds"], 3_600);
    assert_eq!(doc["checks"][1]["ranAt"], observed - 60);
    assert_eq!(doc["compile"][0]["verdict"], "pass");
    assert!(doc["readiness"].as_str().unwrap().contains("never gated"));
    assert_eq!(doc["expectedDuration"]["medianBuildSeconds"], 900);
    assert_eq!(doc["commitsSinceRunning"]["count"], 1);
    assert_eq!(doc["running"]["sourceCommit"], commit('a'));
    assert_eq!(doc["signingIdentity"], "Developer ID Application: Test");
    // A failing check is reported, and the candidate is still offered.
    assert_eq!(facts.candidate_commit, Some(commit('b')));
    assert_eq!(facts.policy_project_id.as_deref(), Some("project"));

    let unknown = assemble_facts(
        FactsInputs {
            running_source_commit: None,
            checks: vec![],
            build_seconds: vec![],
            ..FactsInputs {
                source: Source {
                    project_id: "project".into(),
                    project_key: "cairn".into(),
                    checkout: PathBuf::from("/checkout"),
                    main_commit: commit('b'),
                },
                running_version: "1.2.3".into(),
                running_build_id: None,
                running_source_commit: None,
                running_booted_at: None,
                commits_since: Err("unstamped build".into()),
                checks: vec![],
                build_seconds: vec![],
                signing_identity: Err("no identity".into()),
            }
        },
        observed,
    );
    let doc = &unknown.document;
    assert!(doc["commitsSinceRunning"]["unknown"].is_string());
    assert!(doc["expectedDuration"]["unknown"].is_string());
    assert!(doc["compile"]["unknown"].is_string());
    assert!(doc["signingIdentity"]["unknown"].is_string());
}

#[test]
fn the_runner_bundle_and_plist_are_read_exactly() {
    assert_eq!(
        installed_bundle(Path::new(
            "/Applications/Cairn.app/Contents/MacOS/cairn-orch"
        )),
        Some(PathBuf::from("/Applications/Cairn.app"))
    );
    assert_eq!(
        installed_bundle(Path::new("/repo/src-tauri/target/debug/cairn-orch")),
        None
    );
    let plist = actor_plist(
        &["/a&b/bun".into()],
        &[("K".into(), "<v>".into())],
        Path::new("/log"),
    );
    assert!(plist.contains("<string>/a&amp;b/bun</string>"));
    // Verification plus five minutes for cutover.
    assert!(plist.contains("<key>ExitTimeOut</key><integer>900</integer>"));
    assert!(plist.contains("<string>&lt;v&gt;</string>"));
    assert_eq!(
        signing_identity_of(
            r#"{"bundle":{"macOS":{"signingIdentity":"Developer ID Application: X"}}}"#
        )
        .unwrap(),
        "Developer ID Application: X"
    );
    assert!(signing_identity_of(r#"{"bundle":{"macOS":{"signingIdentity":"-"}}}"#).is_err());
}

fn git_in(dir: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .current_dir(dir)
        .args([
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

/// A non-bare repository whose one commit holds `files`; returns the commit.
fn working_tree_with(dir: &Path, files: &[(&str, &str)]) -> String {
    git_in(dir, &["init", "-q", "-b", "main"]);
    for (path, content) in files {
        let path = dir.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }
    git_in(dir, &["add", "-A"]);
    git_in(dir, &["commit", "-q", "--allow-empty", "-m", "init"]);
    git_in(dir, &["rev-parse", "HEAD"])
}

#[test]
fn the_signing_identity_is_read_at_a_commit_of_a_working_tree() {
    let dir = tempfile::tempdir().unwrap();
    let head = working_tree_with(
        dir.path(),
        &[(
            "src-tauri/tauri.conf.json",
            r#"{"bundle":{"macOS":{"signingIdentity":"Developer ID Application: X (TEAM)"}}}"#,
        )],
    );
    // The working tree itself, whose git dir is `<path>/.git`, is what the
    // project resolution hands over as the object repository.
    assert_eq!(
        signing_identity_at(dir.path(), &head).unwrap(),
        "Developer ID Application: X (TEAM)"
    );
}

#[test]
fn an_absent_tauri_conf_names_the_commit() {
    let dir = tempfile::tempdir().unwrap();
    let head = working_tree_with(dir.path(), &[("README.md", "hi")]);
    let error = signing_identity_at(dir.path(), &head).unwrap_err();
    assert!(
        error.contains("is absent at") && error.contains(&head),
        "{error}"
    );
}

const REPO_TAURI_CONF: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tauri.conf.json"
));

#[test]
fn the_bundle_identifier_constant_matches_the_repo_tauri_conf() {
    let value: Value = serde_json::from_str(REPO_TAURI_CONF).unwrap();
    assert_eq!(value["identifier"].as_str(), Some(CAIRN_BUNDLE_IDENTIFIER));
}

#[test]
fn the_repo_tauri_conf_is_recognised_as_building_cairn() {
    assert!(builds_cairn(REPO_TAURI_CONF));

    let mut other: Value = serde_json::from_str(REPO_TAURI_CONF).unwrap();
    other["identifier"] = json!("com.example.other");
    assert!(!builds_cairn(&other.to_string()));

    assert!(!builds_cairn("not json {"));
}

#[tokio::test]
async fn a_runner_outside_an_installed_bundle_cannot_offer_rebuilds() {
    let f = fixture().await;
    let mut host = f.consumer.host.clone();
    host.app = None;
    let consumer = RebuildConsumer::new(host, f.launchd.clone(), Arc::new(FixedSource));
    let error = consumer.availability(&f.orch).await.unwrap_err();
    assert!(error.contains("installed Cairn.app"), "{error}");
}

/// A colocated executor that answers every adoption and release, and records
/// what it was told and what the release taught. It reports a key adopted
/// only on the link that adopted it: a new generation is an executor that
/// holds nothing yet.
#[derive(Default)]
struct FakeAdopter {
    generation: Mutex<Option<u64>>,
    changes: Mutex<Vec<(u64, ProcessTreeAdoptionChange)>>,
    observed: Mutex<Vec<(String, bool)>>,
    adopted: Mutex<Vec<(u64, String)>>,
}

impl FakeAdopter {
    fn on_link(generation: u64) -> Arc<Self> {
        let adopter = Self::default();
        *adopter.generation.lock().unwrap() = Some(generation);
        Arc::new(adopter)
    }
    fn told(&self) -> Vec<String> {
        self.changes
            .lock()
            .unwrap()
            .iter()
            .map(|(generation, change)| match change {
                ProcessTreeAdoptionChange::Adopt { adoption } => {
                    format!("adopt {} on {generation}", adoption.key)
                }
                ProcessTreeAdoptionChange::Release { key } => {
                    format!("release {key} on {generation}")
                }
            })
            .collect()
    }
}

#[async_trait]
impl TreeAdopter for FakeAdopter {
    fn generation(&self, _: &Orchestrator) -> Option<u64> {
        *self.generation.lock().unwrap()
    }
    fn adopted_keys(&self, _: &Orchestrator) -> Vec<String> {
        let generation = *self.generation.lock().unwrap();
        self.adopted
            .lock()
            .unwrap()
            .iter()
            .filter(|(on, _)| Some(*on) == generation)
            .map(|(_, key)| key.clone())
            .collect()
    }
    async fn reservation(
        &self,
        _: &Orchestrator,
        _: &CommandResourceIdentity,
    ) -> Option<ResourceReservation> {
        Some(ResourceReservation {
            memory_bytes: 14 << 30,
            concurrency_units: 0,
            ..ResourceReservation::default()
        })
    }
    async fn change(
        &self,
        _: &Orchestrator,
        generation: u64,
        change: ProcessTreeAdoptionChange,
    ) -> Result<ProcessTreeAdoptionOutcome, String> {
        {
            let mut adopted = self.adopted.lock().unwrap();
            match &change {
                ProcessTreeAdoptionChange::Adopt { adoption } => {
                    adopted.push((generation, adoption.key.clone()))
                }
                ProcessTreeAdoptionChange::Release { key } => {
                    adopted.retain(|(_, held)| held != key)
                }
            }
        }
        let outcome = match &change {
            ProcessTreeAdoptionChange::Adopt { .. } => ProcessTreeAdoptionOutcome::Adopted {
                leader_start: "1:0".into(),
            },
            ProcessTreeAdoptionChange::Release { .. } => ProcessTreeAdoptionOutcome::Released {
                measured: Some(AdoptedTreeMeasurement {
                    started_at_unix_ms: 1,
                    finished_at_unix_ms: 2,
                    duration_ms: 1,
                    peak_rss_bytes: Some(12 << 30),
                    peak_process: None,
                    measurement_quality:
                        cairn_common::executor_protocol::ExecutionMeasurementQuality {
                            duration:
                                cairn_common::executor_protocol::MeasurementQuality::Approximate,
                            memory: cairn_common::executor_protocol::MeasurementQuality::Sampled,
                            disk: cairn_common::executor_protocol::MeasurementQuality::Unavailable,
                            memory_platform: None,
                            disk_boundary: "unavailable-outside-a-cell".into(),
                        },
                }),
            },
        };
        self.changes.lock().unwrap().push((generation, change));
        Ok(outcome)
    }
    async fn observe(
        &self,
        _: &Orchestrator,
        identity: &CommandResourceIdentity,
        _: &AdoptedTreeMeasurement,
        succeeded: bool,
    ) {
        self.observed
            .lock()
            .unwrap()
            .push((identity.key.clone(), succeeded));
    }
}

fn running_actor(pid: u32) -> ActorJob {
    ActorJob {
        state: ActorState::Running,
        pid: Some(pid),
        program: None,
        last_exit: None,
    }
}

/// A launched attempt's ledger row whose phases are `phases`, each
/// `(name, ended)`.
fn row_in(phases: &[(&str, bool)], outcome: Option<&str>) -> RebuildRecord {
    serde_json::from_value(json!({
        "id": "req-attempt",
        "status": outcome.unwrap_or("acting"),
        "outcome": outcome,
        "requests": [], "requesters": [], "waiting": [], "facts": {}, "events": [], "faults": [],
        "phases": phases.iter().map(|(name, ended)| json!({
            "name": name,
            "started": "2026-11-11T20:00:00Z",
            "ended": ended.then_some("2026-11-11T20:10:00Z"),
            "outcome": ended.then_some("ok"),
        })).collect::<Vec<_>>(),
        "launch": {
            "request_ids": ["request"], "attempt_id": "attempt", "authorization_event_id": "event",
            "kind": "rebuild", "target": null, "commit": commit('b'), "facts_version": 1,
            "recorded_at": 1, "bootstrap_requested_at": 1,
        },
    }))
    .unwrap()
}

/// The build is counted from its first rustc to its last: only while launchd
/// runs the label with a pid and the attempt's open phase is one of cargo's.
#[test]
fn the_build_is_adopted_only_while_a_running_actor_has_a_cargo_phase_open() {
    let adopted = |job: &ActorJob, row: RebuildRecord| desired_adoption(job, &[row]);
    let build = adopted(
        &running_actor(7),
        row_in(&[("fetch", true), ("build", false)], None),
    )
    .expect("the app build is compile work");
    assert_eq!(build.key, "rebuild:attempt:build");
    assert_eq!(build.leader_pid, 7);
    assert_eq!(
        adopted(
            &running_actor(7),
            row_in(&[("build", true), ("voice-engine", false)], None)
        )
        .map(|desired| desired.phase),
        Some("voice-engine")
    );
    for phases in [
        &[("fetch", false)][..],
        &[("build", true), ("install", false)][..],
        &[("build", true)][..],
    ] {
        assert_eq!(
            adopted(&running_actor(7), row_in(phases, None)),
            None,
            "{phases:?}"
        );
    }
    assert_eq!(
        adopted(
            &running_actor(7),
            row_in(&[("build", false)], Some("failed"))
        ),
        None,
        "an attempt with an outcome is over"
    );
    for state in [ActorState::Exited, ActorState::Unknown, ActorState::Absent] {
        let job = ActorJob {
            state,
            ..running_actor(7)
        };
        assert_eq!(adopted(&job, row_in(&[("build", false)], None)), None);
    }
    let pidless = ActorJob {
        pid: None,
        ..running_actor(7)
    };
    assert_eq!(adopted(&pidless, row_in(&[("build", false)], None)), None);
}

async fn adopting_fixture(adopter: Arc<FakeAdopter>) -> Fixture {
    let root = tempfile::tempdir().unwrap().keep();
    let orch = orchestrator(&root).await;
    seed(&orch).await;
    let launchd = FakeLaunchd::new(ActorState::Absent);
    let consumer = Arc::new(
        RebuildConsumer::new(host(&root), launchd.clone(), Arc::new(FixedSource))
            .with_adopter(adopter, ADOPT_RETRY_MS),
    );
    orch.operator_requests.register(consumer.clone()).unwrap();
    Fixture {
        orch,
        root,
        launchd,
        consumer,
    }
}

/// The reconciler tells the colocated executor once per cargo phase, again on
/// a new link, and releases as a phase or its actor ends; each release teaches
/// the release-build profile, as a success only when the phase reported ok.
#[tokio::test]
async fn the_reconciler_adopts_each_cargo_phase_once_and_releases_it_with_its_outcome() {
    let adopter = FakeAdopter::on_link(1);
    let f = adopting_fixture(adopter.clone()).await;
    let (_, id) = launched(&f).await;
    let attempt = id.strip_prefix("req-").unwrap().to_string();
    let key = |phase: &str| format!("rebuild:{attempt}:{phase}");
    *f.launchd.job.lock().unwrap() = running_actor(4242);

    f.append(
        &id,
        json!({"type":"phase","name":"fetch","state":"started"}),
    );
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert!(adopter.told().is_empty(), "fetching compiles nothing");

    f.append(
        &id,
        json!({"type":"phase","name":"fetch","state":"ended","outcome":"ok"}),
    );
    f.append(
        &id,
        json!({"type":"phase","name":"build","state":"started"}),
    );
    f.consumer.reconcile(&f.orch).await.unwrap();
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert_eq!(adopter.told(), vec![format!("adopt {} on 1", key("build"))]);
    {
        let changes = adopter.changes.lock().unwrap();
        let ProcessTreeAdoptionChange::Adopt { adoption } = &changes[0].1 else {
            panic!("an adoption");
        };
        assert_eq!(adoption.leader_pid, 4242);
        assert_eq!(adoption.label, ACTOR_LABEL);
        assert_eq!(adoption.command_class, CellCommandClass::ReleaseBuild);
        assert_eq!(adoption.resource_reservation.concurrency_units, 0);
    }

    // The colocated executor reattached: the new link is told again.
    *adopter.generation.lock().unwrap() = Some(2);
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert_eq!(adopter.told().len(), 2);
    assert_eq!(adopter.told()[1], format!("adopt {} on 2", key("build")));

    f.append(
        &id,
        json!({"type":"phase","name":"build","state":"ended","outcome":"ok"}),
    );
    f.append(
        &id,
        json!({"type":"phase","name":"voice-engine","state":"started"}),
    );
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert_eq!(
        adopter.told()[2..],
        [
            format!("release {} on 2", key("build")),
            format!("adopt {} on 2", key("voice-engine")),
        ]
    );

    // The actor dies with the voice engine's phase still open.
    f.launchd.exited(&staged_runtime(&f, &id));
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert_eq!(
        adopter.told().last().unwrap(),
        &format!("release {} on 2", key("voice-engine"))
    );
    let identity = format!("{ACTOR_LABEL}:release-build");
    assert_eq!(
        *adopter.observed.lock().unwrap(),
        vec![(identity.clone(), true), (identity, false)],
        "the ok build teaches as a success, the phase its actor died in as a failure"
    );
    let told = adopter.told().len();
    f.consumer.reconcile(&f.orch).await.unwrap();
    assert_eq!(adopter.told().len(), told, "nothing more to say");
}
