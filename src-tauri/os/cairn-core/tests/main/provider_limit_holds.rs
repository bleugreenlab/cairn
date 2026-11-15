//! Provider refusals are durable unfinished work, not successful turn endings.

use crate::common;
use cairn_core::credentials::{ClaudeAuth, CredentialStore};
use cairn_core::identity::{CredentialStoreExt, UserIdentity};
use cairn_core::internal::agent_process::process::AgentProcessState;
use cairn_core::internal::backends::claude::{claude_runtime, dispatch_claude_result};
use cairn_core::internal::db::DbState;
use cairn_core::internal::execution::advancement::recompute_job_status_conn;
use cairn_core::internal::execution::jobs::{continue_job_impl, promote_private_lanes_for_test};
use cairn_core::internal::orchestrator::lifecycle::{finalize_run, stop_job};
use cairn_core::internal::orchestrator::wakes::{
    load_active_provider_limit_holds, release_on_supply_change, resolve_provider_limit_hold,
    supersede_orphaned_provider_limit_holds, ProviderLimitHold, SUPPLY_RECHECK_SECS,
};
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::{
    MockGitClient, RecordingProcessSpawner, TestServicesBuilder,
};
use cairn_core::internal::services::{
    ChildProcess, CommandOutput, GitOutput, ProcessSpawner, SpawnConfig,
};
use cairn_core::internal::storage::{LocalDb, SearchIndex};
use cairn_core::models::{JobStatus, RunStatus};
use cairn_db::turso::params;
use cairn_models::subscription::testing::TempHomes;
use serde_json::json;
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::{mpsc, Arc, Mutex, Weak};
use tempfile::TempDir;

const OBSERVED_AT: i64 = 1788545269;
const RESET_AT: i64 = 1788547200;
const LIMIT: &str = "You've hit your session limit · resets 2:40pm (America/New_York)";

// The stock recorder ends stdout immediately. Holding EOF until the fixture
// releases it prevents a fake process exit racing the Result being asserted.
struct RecordedProcesses {
    recorder: RecordingProcessSpawner,
    stdout_guards: Arc<Mutex<Vec<mpsc::Sender<()>>>>,
}

impl RecordedProcesses {
    fn spawn_count(&self) -> usize {
        self.recorder.spawn_count()
    }
}

struct LiveRecordingSpawner {
    fail_spawn: bool,
    recorder: RecordingProcessSpawner,
    stdout_guards: Weak<Mutex<Vec<mpsc::Sender<()>>>>,
}

struct HeldStdout(mpsc::Receiver<()>);

impl Read for HeldStdout {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        let _ = self.0.recv();
        Ok(0)
    }
}

struct HeldChild {
    child: Box<dyn ChildProcess>,
    stdout: Option<mpsc::Receiver<()>>,
}

impl ChildProcess for HeldChild {
    fn id(&self) -> u32 {
        self.child.id()
    }
    fn take_stdout(&mut self) -> Option<Box<dyn BufRead + Send>> {
        self.stdout
            .take()
            .map(|rx| Box::new(BufReader::new(HeldStdout(rx))) as Box<dyn BufRead + Send>)
    }
    fn take_stderr(&mut self) -> Option<Box<dyn BufRead + Send>> {
        self.child.take_stderr()
    }
    fn take_stdin(&mut self) -> Option<Box<dyn Write + Send>> {
        self.child.take_stdin()
    }
    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }
    fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill()
    }
}

impl ProcessSpawner for LiveRecordingSpawner {
    fn spawn(&self, config: SpawnConfig) -> Result<Box<dyn ChildProcess>, String> {
        let child = self.recorder.spawn(config)?;
        if self.fail_spawn {
            return Err("fixture cold spawn refused".into());
        }
        let (tx, rx) = mpsc::channel();
        self.stdout_guards
            .upgrade()
            .ok_or("fixture process guard dropped")?
            .lock()
            .unwrap()
            .push(tx);
        Ok(Box::new(HeldChild {
            child,
            stdout: Some(rx),
        }))
    }
    fn run(&self, config: SpawnConfig) -> Result<CommandOutput, String> {
        self.recorder.run(config)
    }
}

fn orchestrator(temp: &TempDir, db: Arc<LocalDb>) -> (Orchestrator, RecordedProcesses) {
    orchestrator_with_credential(temp, db, Some(ClaudeAuth::ApiKey("test-key".to_string())))
}

/// [`orchestrator`], with the account made optional so one test can exercise a
/// launch refused after the hold claims its successor but before spawning.
fn orchestrator_with_credential(
    temp: &TempDir,
    db: Arc<LocalDb>,
    claude_auth: Option<ClaudeAuth>,
) -> (Orchestrator, RecordedProcesses) {
    orchestrator_with_spawn_failure(temp, db, claude_auth, false)
}

fn orchestrator_with_spawn_failure(
    temp: &TempDir,
    db: Arc<LocalDb>,
    claude_auth: Option<ClaudeAuth>,
    fail_spawn: bool,
) -> (Orchestrator, RecordedProcesses) {
    build_orchestrator(temp, db, claude_auth, fail_spawn, None)
}

fn build_orchestrator(
    temp: &TempDir,
    db: Arc<LocalDb>,
    claude_auth: Option<ClaudeAuth>,
    fail_spawn: bool,
    homes: Option<TempHomes>,
) -> (Orchestrator, RecordedProcesses) {
    let search_path = tempfile::Builder::new()
        .prefix("search-")
        .tempdir_in(temp.path())
        .unwrap()
        .keep();
    let search_index = Arc::new(SearchIndex::open_or_create(search_path).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let recorder = RecordedProcesses {
        recorder: RecordingProcessSpawner::new(),
        stdout_guards: Arc::new(Mutex::new(Vec::new())),
    };
    let mut git = MockGitClient::new();
    git.expect_rev_parse().returning(|repo, args| {
        cairn_platform::services::git::GitClient::rev_parse(
            &cairn_platform::services::git::RealGitClient,
            repo,
            args,
        )
    });
    git.expect_run().times(0..).returning(|_, _| {
        Ok(GitOutput {
            success: true,
            stdout: String::new(),
            stderr: String::new(),
        })
    });
    let services = Arc::new(
        TestServicesBuilder::new()
            .with_process(LiveRecordingSpawner {
                fail_spawn,
                recorder: recorder.recorder.clone(),
                stdout_guards: Arc::downgrade(&recorder.stdout_guards),
            })
            .with_git(git)
            .build(),
    );
    let config_dir = temp.path().join("config");
    std::fs::create_dir_all(config_dir.join("agents")).unwrap();
    std::fs::create_dir_all(config_dir.join("recipes")).unwrap();

    // A planted managed release, so the launch never waits on an install. The
    // recording spawner never executes it.
    let process_state = Arc::new(AgentProcessState::default());
    let managed =
        cairn_models::managed::testing::tools_with_claude(&temp.path().join("managed"), "exit 0");

    let credential_store = claude_auth.map(|claude_auth| {
        CredentialStore::from_user_identity(&UserIdentity {
            user_id: "test-user".to_string(),
            email: "test@example.com".to_string(),
            name: "Test User".to_string(),
            claude_auth: Some(claude_auth),
            codex_auth: None,
            github_token: None,
        })
    });

    let mut builder = Orchestrator::builder(db_state, services, config_dir)
        .boot_at(0)
        .process_state(process_state)
        .managed_tools(managed)
        .credential_store(credential_store);
    if let Some(homes) = homes {
        builder = builder.subscriptions(Arc::new(homes));
    }
    (builder.build(), recorder)
}

async fn fixture() -> (TempDir, Arc<LocalDb>, Orchestrator, RecordedProcesses) {
    let (temp, db) = seeded().await;
    let (orch, recorder) = orchestrator(&temp, db.clone());
    (temp, db, orch, recorder)
}

/// [`fixture`], with subscription support, so a signed-in managed Claude
/// account can launch rather than being refused as a test build refuses it.
async fn subscribed_fixture() -> (
    TempDir,
    Arc<LocalDb>,
    Orchestrator,
    RecordedProcesses,
    TempHomes,
) {
    let (temp, db) = seeded().await;
    let homes = TempHomes::new(temp.path().join("claude-profiles"));
    let (orch, recorder) = build_orchestrator(
        &temp,
        db.clone(),
        Some(ClaudeAuth::ApiKey("test-key".to_string())),
        false,
        Some(homes.clone()),
    );
    (temp, db, orch, recorder, homes)
}

async fn seeded() -> (TempDir, Arc<LocalDb>) {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let root = temp.path().to_string_lossy().replace('\'', "''");
    let snapshot = json!({
        "recipe": {
            "id": "hold", "name": "Hold", "trigger": "manual",
            "nodes": [
                {"id":"trigger","name":"Trigger","nodeType":"trigger","position":{"x":0,"y":0}},
                {"id":"a","name":"A","nodeType":"agent","position":{"x":0,"y":1}},
                {"id":"output","name":"Output","nodeType":"artifact","position":{"x":1,"y":1},
                 "artifactConfig":{"name":"result","schema":{"type":"object"},"confirmPolicy":"auto"}},
                {"id":"b","name":"B","nodeType":"agent","position":{"x":0,"y":2}}
            ],
            "edges": [
                {"id":"start","edgeType":"control","sourceNodeId":"trigger","sourceHandle":"control-out","targetNodeId":"a","targetHandle":"control-in"},
                {"id":"next","edgeType":"control","sourceNodeId":"a","sourceHandle":"control-out","targetNodeId":"b","targetHandle":"control-in"},
                {"id":"out","edgeType":"context","sourceNodeId":"a","sourceHandle":"context-out","targetNodeId":"output","targetHandle":"context-in"}
            ]
        },
        "agents":{}, "skills":{}, "tools":{},
        "triggerContext":{"projectId":"project","issueId":"issue","triggerType":"manual"},
        "createdAt":0
    }).to_string().replace('\'', "''");
    db.execute_script(format!("
        INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
          VALUES('project','default','Hold','hold','{root}',1,1);
        INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
          VALUES('issue','project',1,'Held work','active',1,1);
        INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
          VALUES('exec','hold','issue','project','running',1,1,'{snapshot}');
        INSERT INTO jobs(id, execution_id, recipe_node_id, project_id, issue_id, status,
                         uri_segment, node_name, created_at, updated_at)
          VALUES('a','exec','a','project','issue','running','a','A',1,1),
                ('b','exec','b','project','issue','pending','b','B',1,1);
        INSERT INTO sessions(id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
          VALUES('session','a','claude','claude-session','open',1,2000000000,2000000000);
        INSERT INTO runs(id, project_id, issue_id, job_id, session_id, status, created_at, updated_at, start_mode)
          VALUES('run','project','issue','a','session','live',1,1,'resume');
        INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
          VALUES('turn','session','run','a',1,'running','initial',1,1);
        UPDATE jobs SET current_session_id='session', current_turn_id='turn' WHERE id='a';
    ")).await.unwrap();
    (temp, db)
}

fn result(orch: &Orchestrator, db: &Arc<LocalDb>, run: &str, text: &str, is_error: bool, now: i64) {
    let runtime = claude_runtime(
        orch,
        &orch.services.emitter,
        run,
        Some("session".into()),
        db.clone(),
    );
    let payload = if is_error {
        json!({"type":"result","subtype":"success","is_error":true,
               "api_error_status":429,"terminal_reason":"api_error","result":text})
    } else {
        json!({"type":"result","subtype":"success","is_error":false,"result":text})
    };
    dispatch_claude_result(
        runtime.host.as_ref(),
        is_error,
        &payload,
        Default::default(),
        now,
    );
}

async fn hold(orch: &Orchestrator, db: &Arc<LocalDb>, text: &str) -> ProviderLimitHold {
    result(orch, db, "run", text, true, OBSERVED_AT);
    let holds = load_active_provider_limit_holds(db).await.unwrap();
    assert_eq!(
        holds.len(),
        1,
        "the actual terminal Result must arm one durable hold"
    );
    holds.into_iter().next().unwrap()
}

// The yield and its hold are one transaction. With head ownership broken, the
// hold refuses after the yield statement already ran inside that transaction, so
// a non-atomic implementation would leave a provider-yielded head with no hold.
#[tokio::test]
async fn a_failed_hold_write_never_leaves_an_unowned_provider_yield() {
    let (_temp, db, orch, _recorder) = fixture().await;
    db.execute("UPDATE jobs SET current_turn_id=NULL WHERE id='a'", ())
        .await
        .unwrap();
    result(&orch, &db, "run", LIMIT, true, OBSERVED_AT);

    assert!(load_active_provider_limit_holds(&db)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        common::scalar_i64_by_id(
            &db,
            "SELECT COUNT(*) FROM turns t WHERE t.job_id=?1 AND t.state='yielded'
               AND t.yield_reason='provider_limit'
               AND NOT EXISTS (SELECT 1 FROM provider_limit_holds h
                               WHERE h.turn_id=t.id AND h.state='active')",
            "a",
        )
        .await,
        0,
        "no provider-yielded head may exist without an active hold"
    );
    // The refused turn is unfinished and resumable, not a completion.
    assert_eq!(
        text(&db, "SELECT state FROM turns WHERE id=?1", "turn").await,
        "interrupted"
    );
    assert_eq!(
        text(&db, "SELECT end_reason FROM turns WHERE id=?1", "turn").await,
        "provider_depleted"
    );
}

async fn text(db: &LocalDb, sql: &'static str, id: &str) -> String {
    common::scalar_text_by_id(db, sql, id).await.unwrap()
}

async fn turns(db: &LocalDb) -> i64 {
    common::scalar_i64_by_id(db, "SELECT COUNT(*) FROM turns WHERE job_id=?1", "a").await
}

// Enqueue and promotion are separate production boundaries. Observe the durable
// admission result, rather than racing the detached promoter or sleeping for an
// assumed launch duration. The deadline only bounds a broken fixture.
async fn await_admission(orch: &Orchestrator, job: &str, expected: &str) {
    promote_private_lanes_for_test(orch);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let state = common::scalar_text_by_id(
                &orch.db.local,
                "SELECT r.state FROM runtime_admission_requests r
                 JOIN prepared_ordinary_launches p USING(request_id)
                 WHERE p.job_id=?1 ORDER BY r.enqueue_seq DESC LIMIT 1",
                job,
            )
            .await;
            if state.as_deref() == Some(expected) {
                break;
            }
            if state.as_deref() == Some("failed") && expected != "failed" {
                let detail = common::scalar_text_by_id(
                    &orch.db.local,
                    "SELECT r.failure_detail FROM runtime_admission_requests r
                     JOIN prepared_ordinary_launches p USING(request_id)
                     WHERE p.job_id=?1 ORDER BY r.enqueue_seq DESC LIMIT 1",
                    job,
                )
                .await;
                panic!("admission failed for {job}: {detail:?}");
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("admission for {job} did not reach {expected}"));
}

async fn assert_held(db: &LocalDb) {
    assert_eq!(
        text(db, "SELECT state FROM turns WHERE id=?1", "turn").await,
        "yielded"
    );
    assert_eq!(
        text(db, "SELECT yield_reason FROM turns WHERE id=?1", "turn").await,
        "provider_limit"
    );
    assert_eq!(
        text(db, "SELECT current_turn_id FROM jobs WHERE id=?1", "a").await,
        "turn"
    );
    assert_eq!(
        text(db, "SELECT status FROM jobs WHERE id=?1", "a").await,
        "running"
    );
    let status = db
        .read(|conn| Box::pin(async move { recompute_job_status_conn(conn, "a").await }))
        .await
        .unwrap();
    assert_eq!(status, JobStatus::Running);
    assert!(!cairn_workflow::advancement::issue_settled(db, "issue")
        .await
        .unwrap());
}

#[tokio::test]
async fn resolution_claims_and_launches_exactly_one_successor() {
    let (_temp, db, orch, recorder) = fixture().await;
    let hold = hold(&orch, &db, LIMIT).await;
    assert_eq!(hold.retry_not_before, Some(RESET_AT));
    assert_eq!(hold.boundary_source, "result_text");
    assert_held(&db).await;
    assert_eq!(hold.due_at(RESET_AT - 1), None);
    let due = hold.next_due_at().unwrap();
    assert!(!resolve_provider_limit_hold(&orch, &hold, due - 1)
        .await
        .unwrap());
    assert_eq!(turns(&db).await, 1);
    assert_eq!(recorder.spawn_count(), 0);
    assert_eq!(
        supersede_orphaned_provider_limit_holds(&db, RESET_AT)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        text(
            &db,
            "SELECT state FROM provider_limit_holds WHERE id=?1",
            &hold.id
        )
        .await,
        "active"
    );
    assert!(resolve_provider_limit_hold(&orch, &hold, due)
        .await
        .unwrap());
    assert_eq!(turns(&db).await, 2);
    assert_eq!(common::scalar_i64_by_id(&db, "SELECT COUNT(*) FROM turns WHERE job_id=?1 AND start_reason='wait_resolved' AND predecessor_id='turn'", "a").await, 1);
    assert_eq!(
        text(
            &db,
            "SELECT state FROM provider_limit_holds WHERE id=?1",
            &hold.id
        )
        .await,
        "resolved"
    );
    assert!(!resolve_provider_limit_hold(&orch, &hold, due + 1)
        .await
        .unwrap());
    assert_eq!(turns(&db).await, 2);
    await_admission(&orch, "a", "active").await;
    assert_eq!(recorder.spawn_count(), 1);
}

/// Add a managed Claude account through the operator's path.
fn managed_account(orch: &Orchestrator, label: &str) -> String {
    orch.add_account(
        cairn_core::credentials::ApiProvider::Anthropic,
        label.to_string(),
        cairn_core::credentials::ProviderAuth::ClaudeProfile { oauth: None },
        None,
    )
    .unwrap()
    .id
}

async fn pin_session(db: &LocalDb, account: &str) {
    db.execute(
        "UPDATE sessions SET account_id=?1 WHERE id='session'",
        params![account],
    )
    .await
    .unwrap();
}

/// Two managed Claude accounts, with the session pinned to the first.
async fn two_accounts(orch: &Orchestrator, db: &LocalDb) -> (String, String) {
    let [exhausted, replacement] =
        ["Work Max", "Personal Max"].map(|label| managed_account(orch, label));
    pin_session(db, &exhausted).await;
    (exhausted, replacement)
}

/// Stream a recorded Claude stdout through the production reader and host.
fn read_stream(orch: &Orchestrator, db: &Arc<LocalDb>, stdout: &'static str) {
    let runtime = claude_runtime(
        orch,
        &orch.services.emitter,
        "run",
        Some("session".into()),
        db.clone(),
    );
    let db = db.clone();
    std::thread::spawn(move || {
        cairn_session::backends::claude::ClaudeRuntime::reader_thread(
            &runtime,
            "run",
            Some("session".into()),
            None,
            false,
            Box::new(std::io::Cursor::new(stdout.as_bytes())),
            None,
            db,
        )
    })
    .join()
    .expect("reader thread");
}

const TOOL_IN_FLIGHT: &str =
    include_str!("../../../cairn-session/tests/fixtures/claude_limit_tool_in_flight.jsonl");
const TOOL_ANSWERED: &str =
    include_str!("../../../cairn-session/tests/fixtures/claude_limit_tool_answered.jsonl");

async fn only_hold(db: &LocalDb) -> ProviderLimitHold {
    let holds = load_active_provider_limit_holds(db).await.unwrap();
    assert_eq!(holds.len(), 1, "the refusal must arm one durable hold");
    holds.into_iter().next().unwrap()
}

// Recorded 2026-09-03: a tool_use whose result never reached the stream before
// the 429. The account still fails over, but the work waits for an operator,
// because the CLI may already have run the tool.
#[tokio::test]
async fn a_tool_in_flight_holds_without_a_timer() {
    let (_temp, db, orch, recorder) = fixture().await;
    let (exhausted, replacement) = two_accounts(&orch, &db).await;
    read_stream(&orch, &db, TOOL_IN_FLIGHT);

    let hold = only_hold(&db).await;
    assert_eq!(hold.resume_gate.as_deref(), Some("tool_in_flight"));
    assert_eq!(hold.boundary_source, "account_switch");
    assert_eq!(hold.account_id.as_deref(), Some(exhausted.as_str()));
    assert_eq!(
        text(
            &db,
            "SELECT account_id FROM sessions WHERE id=?1",
            "session"
        )
        .await,
        replacement,
        "failover bookkeeping still runs"
    );
    assert_held(&db).await;
    assert_eq!(hold.next_due_at(), None);
    for now in [OBSERVED_AT, RESET_AT, i64::MAX] {
        assert!(!resolve_provider_limit_hold(&orch, &hold, now)
            .await
            .unwrap());
    }
    assert_eq!(turns(&db).await, 1);
    assert_eq!(recorder.spawn_count(), 0);
    let note = text(
        &db,
        "SELECT data FROM events WHERE run_id=?1 AND data LIKE '%provider_limit_hold%'",
        "run",
    )
    .await;
    assert!(note.contains("did not resume automatically"), "{note}");
    assert!(note.contains("tool_in_flight"), "{note}");

    // One explicit continue is authoritative over the gated hold, which then has
    // no head left to own.
    continue_job_impl(
        cairn_core::models::WakeCause::Addressed,
        &orch,
        "a",
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(turns(&db).await, 2);
    assert!(!resolve_provider_limit_hold(&orch, &hold, i64::MAX)
        .await
        .unwrap());
    assert_eq!(
        supersede_orphaned_provider_limit_holds(&db, RESET_AT)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        text(
            &db,
            "SELECT state FROM provider_limit_holds WHERE id=?1",
            &hold.id
        )
        .await,
        "superseded"
    );
}

/// A live process mid-turn on `account`, so stdout EOF reads as the process
/// exiting under the turn rather than as a host-initiated stop.
fn register_live_process(orch: &Orchestrator, account: &str) {
    let mut handle = cairn_core::internal::agent_process::process::RunHandle::test_handle(
        Some("session"),
        Some("a"),
    );
    handle.account_id = Some(account.to_string());
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run".to_string(), handle);
    orch.process_state.begin_turn("run", "turn");
}

// Acceptance 3. The CLI reports a blocking limit and exits before any terminal
// result. That is the same refusal a terminal 429 is: the account fails over and
// the work resumes on the replacement through the same hold, with no crash and
// no operator input.
#[tokio::test]
async fn a_blocking_rate_limit_exit_without_a_result_holds_and_fails_over() {
    let (_temp, db, orch, _recorder) = fixture().await;
    let (exhausted, replacement) = two_accounts(&orch, &db).await;
    register_live_process(&orch, &exhausted);
    read_stream(
        &orch,
        &db,
        r#"{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetsAt":4102444800}}"#,
    );

    let hold = only_hold(&db).await;
    assert_eq!(hold.boundary_source, "account_switch");
    assert_eq!(hold.resume_gate, None);
    assert_eq!(hold.account_id.as_deref(), Some(exhausted.as_str()));
    assert_held(&db).await;
    assert_eq!(
        text(
            &db,
            "SELECT account_id FROM sessions WHERE id=?1",
            "session"
        )
        .await,
        replacement
    );
    assert_eq!(
        text(&db, "SELECT status FROM runs WHERE id=?1", "run").await,
        "exited",
        "a held exit is not a crash"
    );
    assert_eq!(
        common::scalar_i64_by_id(
            &db,
            "SELECT COUNT(*) FROM runs WHERE id=?1 AND exit_reason IS NOT NULL",
            "run"
        )
        .await,
        0
    );
    let now = chrono::Utc::now().timestamp();
    assert!(resolve_provider_limit_hold(&orch, &hold, now)
        .await
        .unwrap());
    assert_eq!(common::scalar_i64_by_id(&db, "SELECT COUNT(*) FROM turns WHERE job_id=?1 AND start_reason='wait_resolved' AND predecessor_id='turn'", "a").await, 1);
}

/// A blocking limit whose reset is far off: 2100-01-01.
const FAR_RESET: i64 = 4102444800;
const BLOCKING_LIMIT: &str =
    r#"{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetsAt":4102444800}}"#;

// cairn/5378, the 10-21 shape: the only account hits its limit, so nothing can
// take the work and it holds for the declared reset. The operator then adds an
// account. That change, not the reset and not a message, releases the hold, and
// the work continues once on the account just added.
#[tokio::test]
async fn an_added_account_releases_a_limit_held_session_before_its_reset() {
    let (_temp, db, orch, recorder, homes) = subscribed_fixture().await;
    let exhausted = managed_account(&orch, "Work Max");
    homes.sign_in(&exhausted);
    pin_session(&db, &exhausted).await;
    register_live_process(&orch, &exhausted);
    read_stream(&orch, &db, BLOCKING_LIMIT);

    let hold = only_hold(&db).await;
    assert_eq!(
        hold.boundary_source, "rate_limit_event",
        "with one account there is nothing to fail over to"
    );
    assert_eq!(hold.resume_gate, None);
    let due = hold.next_due_at().expect("a timed hold");
    assert!((FAR_RESET..=FAR_RESET + 120).contains(&due), "{due}");
    let now = chrono::Utc::now().timestamp();
    assert!(
        !resolve_provider_limit_hold(&orch, &hold, now)
            .await
            .unwrap(),
        "its clock is far from due"
    );
    assert_held(&db).await;
    assert_eq!(recorder.spawn_count(), 0);

    let added = managed_account(&orch, "Personal Max");
    homes.sign_in(&added);
    assert!(
        release_on_supply_change(&orch, &hold, now).await.unwrap(),
        "an account that can serve the work releases the hold"
    );
    assert_eq!(
        text(
            &db,
            "SELECT state FROM provider_limit_holds WHERE id=?1",
            &hold.id
        )
        .await,
        "resolved"
    );
    assert_eq!(turns(&db).await, 2);
    assert_eq!(common::scalar_i64_by_id(&db, "SELECT COUNT(*) FROM turns WHERE job_id=?1 AND start_reason='wait_resolved' AND predecessor_id='turn'", "a").await, 1);
    assert_eq!(
        text(
            &db,
            "SELECT s.account_id FROM jobs j JOIN sessions s ON s.id=j.current_session_id WHERE j.id=?1",
            "a"
        )
        .await,
        added,
        "the session leaves {exhausted} for the account just added"
    );
    assert_eq!(
        common::scalar_i64_by_id(
            &db,
            "SELECT COUNT(*) FROM events WHERE run_id=?1
               AND data LIKE '%\"kind\":\"provider_limit_resolved\"%'
               AND data LIKE '%\"trigger\":\"supply_changed\"%'",
            "run"
        )
        .await,
        1,
        "the transcript says the accounts, not the reset, released it"
    );

    await_admission(&orch, "a", "active").await;
    let spawned = recorder.recorder.spawned();
    assert_eq!(spawned.len(), 1, "the work continues exactly once");
    let home = spawned[0].env.get("CLAUDE_CONFIG_DIR");
    assert!(
        home.is_some_and(|home| home.contains(added.as_str())),
        "it launches in the added account's home: {home:?}"
    );
}

// A hold gated on the operator is not a supply question, so a new account does
// not release it: resuming could run the unanswered tool call a second time.
#[tokio::test]
async fn an_added_account_does_not_release_a_tool_in_flight_hold() {
    let (_temp, db, orch, recorder, homes) = subscribed_fixture().await;
    let exhausted = managed_account(&orch, "Work Max");
    homes.sign_in(&exhausted);
    pin_session(&db, &exhausted).await;
    read_stream(&orch, &db, TOOL_IN_FLIGHT);

    let hold = only_hold(&db).await;
    assert_eq!(hold.resume_gate.as_deref(), Some("tool_in_flight"));

    let added = managed_account(&orch, "Personal Max");
    homes.sign_in(&added);
    let now = chrono::Utc::now().timestamp();
    assert!(!release_on_supply_change(&orch, &hold, now).await.unwrap());
    assert_held(&db).await;
    assert_eq!(turns(&db).await, 1);
    assert_eq!(recorder.spawn_count(), 0);
    assert_eq!(
        text(
            &db,
            "SELECT state FROM provider_limit_holds WHERE id=?1",
            &hold.id
        )
        .await,
        "active"
    );
}

// The credits shape: the CLI's synthetic credits message, then a generic error
// result with no 429 status. It fails over through the hold like any limit.
#[tokio::test]
async fn a_credits_message_before_a_generic_error_holds() {
    let (_temp, db, orch, _recorder) = fixture().await;
    let (exhausted, replacement) = two_accounts(&orch, &db).await;
    register_live_process(&orch, &exhausted);
    read_stream(
        &orch,
        &db,
        concat!(
            r#"{"type":"assistant","uuid":"a1","session_id":"s1","is_api_error_message":true,"api_error_status":429,"error_details":"credits_required","message":{"role":"assistant","model":"<synthetic>","content":[{"type":"text","text":"Manage credits with /usage-credits"}]}}"#,
            "\n",
            r#"{"type":"result","subtype":"success","session_id":"s1","is_error":true,"result":"You're out of usage credits."}"#,
        ),
    );

    let hold = only_hold(&db).await;
    assert_eq!(hold.boundary_source, "account_switch");
    assert_held(&db).await;
    assert_eq!(
        text(
            &db,
            "SELECT account_id FROM sessions WHERE id=?1",
            "session"
        )
        .await,
        replacement
    );
    let note = text(
        &db,
        "SELECT data FROM events WHERE run_id=?1 AND data LIKE '%provider_limit_hold%'",
        "run",
    )
    .await;
    assert!(
        note.replace('\\', "")
            .contains(r#""depletionKind":"credits""#),
        "{note}"
    );
    assert!(note.contains(exhausted.as_str()), "{note}");
}

#[tokio::test]
async fn an_answered_tool_call_fails_over_and_resumes_on_its_own() {
    let (_temp, db, orch, _recorder) = fixture().await;
    two_accounts(&orch, &db).await;
    read_stream(&orch, &db, TOOL_ANSWERED);

    let hold = only_hold(&db).await;
    assert_eq!(hold.resume_gate, None);
    assert_eq!(hold.boundary_source, "account_switch");
    assert!(hold.next_due_at().is_some());
    assert_held(&db).await;
}

// A limit on one account is a reason to wait only when no other account can take
// the work. With an eligible replacement, the session is repinned and the hold is
// due immediately rather than at the exhausted account's reset.
#[tokio::test]
async fn a_limit_with_an_eligible_replacement_switches_accounts_and_resumes_now() {
    let (_temp, db, orch, _recorder) = fixture().await;
    let (exhausted, replacement) = two_accounts(&orch, &db).await;

    let hold = hold(&orch, &db, LIMIT).await;
    assert_eq!(hold.boundary_source, "account_switch");
    assert_eq!(hold.retry_not_before, Some(OBSERVED_AT));
    assert_eq!(
        hold.next_due_at(),
        Some(OBSERVED_AT),
        "a switch is not jittered"
    );
    assert_eq!(hold.account_id.as_deref(), Some(exhausted.as_str()));
    assert_held(&db).await;
    let pinned = text(
        &db,
        "SELECT account_id FROM sessions WHERE id=?1",
        "session",
    )
    .await;
    assert_ne!(
        pinned, exhausted,
        "the session must leave the exhausted account"
    );
    assert_eq!(pinned, replacement);
    let note = text(
        &db,
        "SELECT data FROM events WHERE run_id=?1 AND data LIKE '%provider_limit_hold%'",
        "run",
    )
    .await;
    assert!(note.contains("account_switch"), "{note}");
    assert!(note.contains(replacement.as_str()), "{note}");

    assert!(resolve_provider_limit_hold(&orch, &hold, OBSERVED_AT)
        .await
        .unwrap());
    assert_eq!(common::scalar_i64_by_id(&db, "SELECT COUNT(*) FROM turns WHERE job_id=?1 AND start_reason='wait_resolved' AND predecessor_id='turn'", "a").await, 1);
    assert!(!resolve_provider_limit_hold(&orch, &hold, OBSERVED_AT + 1)
        .await
        .unwrap());
    // Test builds carry no subscription support, so admission refuses to launch a
    // managed profile; launching the claimed turn is covered by the other holds.
}

#[tokio::test]
async fn restart_reloads_the_yielded_hold_and_resumes_once() {
    let (temp, db, orch, recorder) = fixture().await;
    let original = hold(&orch, &db, LIMIT).await;
    finalize_run(&orch, "run", RunStatus::Exited);
    assert_held(&db).await;
    drop(orch);
    assert_eq!(recorder.spawn_count(), 0);
    let (restarted, recorder) = orchestrator(&temp, db.clone());
    assert_held(&db).await;
    let holds = load_active_provider_limit_holds(&db).await.unwrap();
    assert_eq!(holds.len(), 1);
    assert_eq!(holds[0].id, original.id);
    let due = holds[0].next_due_at().unwrap();
    assert!(resolve_provider_limit_hold(&restarted, &holds[0], due)
        .await
        .unwrap());
    assert!(!resolve_provider_limit_hold(&restarted, &holds[0], due + 1)
        .await
        .unwrap());
    assert_eq!(turns(&db).await, 2);
    await_admission(&restarted, "a", "active").await;
    assert_eq!(recorder.spawn_count(), 1);
}

#[tokio::test]
async fn operator_stop_cancels_hold_but_explicit_resume_still_runs() {
    let (_temp, db, orch, recorder) = fixture().await;
    let hold = hold(&orch, &db, LIMIT).await;
    stop_job(&orch, "a").unwrap();
    assert_eq!(
        text(&db, "SELECT state FROM turns WHERE id=?1", "turn").await,
        "cancelled"
    );
    assert_eq!(
        text(
            &db,
            "SELECT state FROM provider_limit_holds WHERE id=?1",
            &hold.id
        )
        .await,
        "cancelled"
    );
    assert!(
        !resolve_provider_limit_hold(&orch, &hold, hold.next_due_at().unwrap())
            .await
            .unwrap()
    );
    assert_eq!(turns(&db).await, 1);
    assert_eq!(recorder.spawn_count(), 0);
    continue_job_impl(
        cairn_core::models::WakeCause::Addressed,
        &orch,
        "a",
        None,
        None,
        None,
    )
    .unwrap();
    assert_eq!(turns(&db).await, 2);
    await_admission(&orch, "a", "active").await;
    assert_eq!(recorder.spawn_count(), 1);
}

#[tokio::test]
async fn duplicate_results_coalesce_and_keep_the_later_reset() {
    let (_temp, db, orch, recorder) = fixture().await;
    let first = hold(&orch, &db, LIMIT).await;
    let second = hold(
        &orch,
        &db,
        "You've hit your session limit · resets 3:40pm (America/New_York)",
    )
    .await;
    assert_eq!(first.id, second.id);
    assert_eq!(second.observed_count, 2);
    assert_eq!(second.retry_not_before, Some(RESET_AT + 3600));
    assert_eq!(
        common::scalar_i64_by_id(
            &db,
            "SELECT COUNT(*) FROM provider_limit_holds WHERE job_id=?1",
            "a"
        )
        .await,
        1
    );
    assert!(
        !resolve_provider_limit_hold(&orch, &first, first.next_due_at().unwrap())
            .await
            .unwrap()
    );
    assert!(
        resolve_provider_limit_hold(&orch, &second, second.next_due_at().unwrap())
            .await
            .unwrap()
    );
    assert_eq!(turns(&db).await, 2);
    await_admission(&orch, "a", "active").await;
    assert_eq!(recorder.spawn_count(), 1);
}

#[tokio::test]
async fn unknown_reset_guesses_no_reset_and_rechecks_on_a_bounded_clock() {
    let (_temp, db, orch, recorder) = fixture().await;
    let hold = hold(&orch, &db, "Usage limit reached").await;
    assert_eq!(hold.retry_not_before, None, "no reset is ever guessed");
    assert_eq!(hold.boundary_source, "unknown");
    // cairn/5378: it is not left waiting forever on an event that may never
    // come; it asks again a bounded interval after it last learned anything.
    let recheck = hold.updated_at + SUPPLY_RECHECK_SECS;
    assert_eq!(hold.next_due_at(), Some(recheck));
    assert_held(&db).await;
    for now in [hold.updated_at, recheck - 1] {
        assert_eq!(hold.due_at(now), None);
        assert!(!resolve_provider_limit_hold(&orch, &hold, now)
            .await
            .unwrap());
    }
    assert_eq!(hold.due_at(recheck), Some(recheck));
    assert_eq!(turns(&db).await, 1);
    assert_eq!(recorder.spawn_count(), 0);
}

#[tokio::test]
async fn cold_process_spawn_refusal_restores_the_held_head() {
    let (temp, db, orch, _recorder) = fixture().await;
    let hold = hold(&orch, &db, LIMIT).await;
    finalize_run(&orch, "run", RunStatus::Exited);
    assert_held(&db).await;
    drop(orch);
    let (orch, recorder) = orchestrator_with_spawn_failure(
        &temp,
        db.clone(),
        Some(ClaudeAuth::ApiKey("test-key".into())),
        true,
    );
    let due = hold.next_due_at().unwrap();
    let before = chrono::Utc::now().timestamp();
    assert!(resolve_provider_limit_hold(&orch, &hold, due)
        .await
        .unwrap());
    await_admission(&orch, "a", "failed").await;
    let after = chrono::Utc::now().timestamp();
    assert_eq!(
        recorder.spawn_count(),
        1,
        "failure must reach the cold spawn boundary"
    );
    assert_held(&db).await;
    let holds = load_active_provider_limit_holds(&db).await.unwrap();
    assert_eq!(holds.len(), 1);
    assert_eq!(holds[0].id, hold.id);
    assert_eq!(holds[0].launch_attempts, 1);
    assert!((before + 300..=after + 300).contains(&holds[0].retry_not_before.unwrap()));
    assert_eq!(
        text(&db, "SELECT status FROM jobs WHERE id=?1", "b").await,
        "pending"
    );
    assert_eq!(holds[0].resolved_at, None);
}

/// One parked child beside one running child at focus=1. No notifier or
/// background gate loop is installed: every recovery below gets no hint.
async fn focus_fixture() -> (TempDir, Arc<LocalDb>, Orchestrator, RecordedProcesses) {
    let (temp, db, orch, recorder) = fixture().await;
    orch.update_settings(cairn_core::models::UpdateSettings {
        max_open_children_per_thread: Some(1),
        max_concurrent_executions: Some(10),
        ..Default::default()
    })
    .unwrap();
    db.execute_script("INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES('focus','project','focus',1,1);
        UPDATE issues SET parent_thread_id='focus' WHERE id='issue';
        UPDATE jobs SET status='blocked' WHERE id='b';
        UPDATE turns SET state='complete',ended_at=2 WHERE id='turn';
        UPDATE runs SET status='exited' WHERE id='run';
        INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at) VALUES('busy','project',2,'Busy','active','focus',1,1);
        INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('busy-exec','hold','busy','project','running',1,1);
        INSERT INTO jobs(id,execution_id,project_id,issue_id,status,node_name,created_at,updated_at) VALUES('busy-job','busy-exec','project','busy','running','Busy',1,1);
        INSERT INTO sessions(id,job_id,backend,status,created_at,updated_at) VALUES('busy-session','busy-job','claude','open',1,1);
        INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('busy-turn','busy-session','busy-job',1,'running','initial',1,1);
        UPDATE jobs SET current_turn_id='busy-turn',current_session_id='busy-session' WHERE id='busy-job';").await.unwrap();
    (temp, db, orch, recorder)
}

#[tokio::test]
async fn focus_resume_survives_boot_and_periodic_pass_without_a_hint_and_delivers_once() {
    use cairn_core::internal::execution::jobs::WakeCause;
    let (temp, db, orch, _recorder) = focus_fixture().await;
    let error = continue_job_impl(
        WakeCause::System,
        &orch,
        "a",
        Some("focus-system-marker"),
        None,
        None,
    )
    .unwrap_err();
    assert!(error.contains("focus"), "{error}");
    let held = only_hold(&db).await;
    assert_eq!(
        held.kind,
        cairn_core::internal::orchestrator::wakes::HoldKind::Resume
    );
    assert_eq!(held.boundary_source, "thread_focus");
    assert_eq!(held.next_due_at(), None);
    assert_eq!(
        common::scalar_i64_by_id(&db, "SELECT COUNT(*) FROM turns WHERE job_id=?1", "a").await,
        1
    );
    drop(orch);
    let (orch, recorder) = orchestrator(&temp, db.clone());
    // Boot pass: still full. It must neither launch nor lose the durable input.
    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    assert_eq!(only_hold(&db).await.id, held.id);
    // Silent SQL changes model a missed hint. Job remains running while its
    // head publishes the artifact and ends, so only activity frees the slot.
    db.execute(
        "UPDATE turns SET state='complete',ended_at=3 WHERE id='busy-turn'",
        (),
    )
    .await
    .unwrap();
    let (pass, timer) = tokio::join!(
        cairn_core::internal::execution::gates::sweep::sweep(&orch),
        resolve_provider_limit_hold(&orch, &held, i64::MAX)
    );
    pass.unwrap();
    assert!(!timer.unwrap(), "thread holds have no timer");
    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    assert_eq!(
        common::scalar_i64_by_id(&db, "SELECT COUNT(*) FROM turns WHERE job_id=?1", "a").await,
        2
    );
    assert_eq!(common::scalar_i64_by_id(&db,"SELECT COUNT(*) FROM events e JOIN turns t ON t.id=e.turn_id WHERE t.job_id=?1 AND e.data LIKE '%focus-system-marker%'","a").await,1);
    await_admission(&orch, "a", "active").await;
    assert_eq!(recorder.spawn_count(), 1);
}

#[tokio::test]
async fn addressed_continue_and_start_now_bypass_full_focus_without_duplicate_delivery() {
    use cairn_core::internal::execution::jobs::WakeCause;
    for mode in ["continue", "start_now", "operator", "agent"] {
        let (_temp, db, orch, recorder) = focus_fixture().await;
        if mode == "start_now" {
            continue_job_impl(
                WakeCause::System,
                &orch,
                "a",
                Some("focus-once-marker"),
                None,
                None,
            )
            .unwrap_err();
            let held = only_hold(&db).await;
            assert!(
                cairn_core::internal::orchestrator::wakes::start_now_thread_hold(
                    &orch,
                    &held.id,
                    json!({"operator":true})
                )
                .await
                .unwrap()
            );
            assert!(
                !cairn_core::internal::orchestrator::wakes::start_now_thread_hold(
                    &orch,
                    &held.id,
                    json!({"operator":true})
                )
                .await
                .unwrap()
            );
        } else if matches!(mode, "operator" | "agent") {
            use cairn_core::internal::mcp::types::McpCallbackRequest;
            db.execute_script("INSERT INTO runs(id,job_id,project_id,issue_id,session_id,status,start_mode,created_at,updated_at) VALUES('busy-run','busy-job','project','busy','busy-session','live','resume',1,1);
                UPDATE turns SET run_id='busy-run' WHERE id='busy-turn';").await.unwrap();
            let request = McpCallbackRequest {
                answer_bound_ms: None,
                thread_id: None,
                cwd: "/tmp".into(),
                run_id: (mode == "agent").then(|| "busy-run".into()),
                tool: "write".into(),
                payload: json!({}),
                tool_use_id: None,
                origin: None,
            };
            let sent = cairn_core::internal::mcp::handlers::messages::append_direct_message(
                &orch,
                &request,
                "hold",
                1,
                1,
                "a",
                None,
                "focus-once-marker",
                false,
            )
            .await
            .unwrap();
            assert!(sent.starts_with("Sent direct message"), "{mode}: {sent}");
        } else {
            continue_job_impl(
                WakeCause::Addressed,
                &orch,
                "a",
                Some("focus-once-marker"),
                None,
                None,
            )
            .unwrap();
        }
        await_admission(&orch, "a", "active").await;
        assert_eq!(recorder.spawn_count(), 1);
        assert_eq!(
            common::scalar_i64_by_id(&db, "SELECT COUNT(*) FROM turns WHERE job_id=?1", "a").await,
            2
        );
        db.execute_script("INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at) VALUES('ingress-fresh','project',9002,'Fresh','active','focus',1,1);
            INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('ingress-fresh','hold','ingress-fresh','project','running',1,1);
            INSERT INTO jobs(id,execution_id,issue_id,project_id,status,node_name,created_at,updated_at) VALUES('ingress-fresh','ingress-fresh','ingress-fresh','project','pending','Fresh',1,1);
            UPDATE turns SET state='complete',ended_at=2 WHERE id='busy-turn';
            UPDATE jobs SET status='completed' WHERE id='busy-job';").await.unwrap();
        assert_ingress_counts_and_holds_fresh(&orch, &db, "a").await;
    }
}

#[tokio::test]
async fn ending_a_focus_held_child_retires_the_hold_without_claiming_its_input() {
    use cairn_core::internal::orchestrator::attention_push::{push, Boundary, Wake};
    use cairn_core::messages::delivery::nudge_job_for_urgency;
    use cairn_core::messages::queued::Delivery;
    // Merge, close, and the operator's withdrawal (Stop) each end the held
    // child. The held wake is a real queued check-settled push, so the
    // assertion is on queue ownership: its delivery stamp stays NULL.
    for exit in ["merged_at", "closed_at", "stop"] {
        let (_temp, db, orch, recorder) = focus_fixture().await;
        let checks = "cairn://p/hold/1/1/a/checks";
        let (push_id, _) = push(
            &db,
            "a",
            checks,
            Wake::Wake,
            Boundary::Event,
            &format!("turn-checks:{checks}"),
        )
        .await
        .unwrap();
        let _ = nudge_job_for_urgency(&orch, "a", Delivery::Queue);
        let held = only_hold(&db).await;
        assert_eq!(held.boundary_source, "thread_focus", "{exit}");
        assert_eq!(push_delivery(&db, &push_id).await, None, "{exit}");

        if exit == "stop" {
            stop_job(&orch, "a").unwrap();
        } else {
            db.execute(format!("UPDATE issues SET {exit}=3 WHERE id='issue'"), ())
                .await
                .unwrap();
        }
        // A freed slot must not revive the ended child's wake.
        db.execute(
            "UPDATE turns SET state='complete',ended_at=3 WHERE id='busy-turn'",
            (),
        )
        .await
        .unwrap();
        for _ in 0..2 {
            cairn_core::internal::execution::gates::sweep::sweep(&orch)
                .await
                .unwrap();
        }
        assert!(
            load_active_provider_limit_holds(&db)
                .await
                .unwrap()
                .is_empty(),
            "{exit}"
        );
        assert_eq!(recorder.spawn_count(), 0, "{exit}");
        assert_eq!(
            common::scalar_i64_by_id(&db, "SELECT COUNT(*) FROM turns WHERE job_id=?1", "a").await,
            1,
            "{exit}: no successor"
        );
        assert_eq!(
            push_delivery(&db, &push_id).await,
            None,
            "{exit}: the wake is neither delivered nor stamped"
        );
    }
}

/// A second parked child `second` (job `c`) in the focus thread, shaped like
/// `issue`'s job `a`: running, its head turn complete, its run exited.
async fn second_parked_child(db: &LocalDb) {
    db.execute_script("INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at) VALUES('second','project',3,'Second','active','focus',1,1);
        INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq,snapshot) SELECT 'second-exec','hold','second','project','running',1,1,snapshot FROM executions WHERE id='exec';
        INSERT INTO jobs(id,execution_id,recipe_node_id,project_id,issue_id,status,uri_segment,node_name,created_at,updated_at) VALUES('c','second-exec','a','project','second','running','a','A',1,1);
        INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at) VALUES('session-c','c','claude','claude-session-c','open',1,2000000000,2000000000);
        INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,created_at,updated_at,start_mode) VALUES('run-c','project','second','c','session-c','exited',1,1,'resume');
        INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,ended_at,updated_at) VALUES('turn-c','session-c','run-c','c',1,'complete','initial',1,2,2);
        UPDATE jobs SET current_session_id='session-c',current_turn_id='turn-c' WHERE id='c';").await.unwrap();
}

async fn marker_deliveries(db: &LocalDb, job: &str, marker: &str) -> i64 {
    db.query_opt_i64(
        "SELECT COUNT(*) FROM events e JOIN turns t ON t.id=e.turn_id WHERE t.job_id=?1 AND e.data LIKE ?2",
        (job.to_owned(), format!("%{marker}%")),
    )
    .await
    .unwrap()
    .unwrap_or(0)
}

/// Plan verification 4 (Acceptance C): two system re-entries at a full focus
/// cap release in the order they arose, one per freed slot, each delivering
/// its wake exactly once. `c` re-enters first though its hold id sorts after
/// `a`'s, so only arrival order can put it ahead. No hint is installed.
#[tokio::test]
async fn two_focus_reentries_release_in_the_order_they_arose() {
    use cairn_core::internal::execution::jobs::WakeCause;
    let (_temp, db, orch, recorder) = focus_fixture().await;
    second_parked_child(&db).await;
    for (job, marker) in [
        ("c", "first-reentry-marker"),
        ("a", "second-reentry-marker"),
    ] {
        let error =
            continue_job_impl(WakeCause::System, &orch, job, Some(marker), None, None).unwrap_err();
        assert!(error.contains("focus"), "{job}: {error}");
    }
    let holds = load_active_provider_limit_holds(&db).await.unwrap();
    assert_eq!(holds.len(), 2, "{holds:?}");
    let first = holds.iter().find(|h| h.job_id == "c").unwrap().clone();
    let second = holds.iter().find(|h| h.job_id == "a").unwrap().clone();
    assert!(first.queue_order() < second.queue_order());
    for hold in [&first, &second] {
        assert_eq!(hold.boundary_source, "thread_focus");
        assert_eq!(hold.next_due_at(), None);
    }

    // One slot frees silently. One pass releases only the earlier re-entry;
    // its resumed turn makes `second` active, which fills the cap again.
    db.execute(
        "UPDATE turns SET state='complete',ended_at=3 WHERE id='busy-turn'",
        (),
    )
    .await
    .unwrap();
    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    let state = |id: String| {
        let db = db.clone();
        async move {
            text(
                &db,
                "SELECT state FROM provider_limit_holds WHERE id=?1",
                &id,
            )
            .await
        }
    };
    assert_eq!(state(first.id.clone()).await, "resolved");
    assert_eq!(state(second.id.clone()).await, "active");
    assert_eq!(
        common::scalar_i64_by_id(&db, "SELECT COUNT(*) FROM turns WHERE job_id=?1", "a").await,
        1,
        "the later re-entry still waits"
    );
    await_admission(&orch, "c", "active").await;
    assert_eq!(recorder.spawn_count(), 1);

    // The resumed turn ends; the next pass releases the later re-entry.
    db.execute(
        "UPDATE turns SET state='complete',ended_at=4 WHERE job_id='c' AND state IN ('pending','running')",
        (),
    )
    .await
    .unwrap();
    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    assert_eq!(state(second.id.clone()).await, "resolved");
    await_admission(&orch, "a", "active").await;
    assert_eq!(recorder.spawn_count(), 2);
    assert_eq!(marker_deliveries(&db, "c", "first-reentry-marker").await, 1);
    assert_eq!(
        marker_deliveries(&db, "a", "second-reentry-marker").await,
        1
    );
}

/// Waits until the admission worker's failed start has restored hold `id` for
/// its `attempts`-th time.
async fn await_restored(db: &LocalDb, id: &str, attempts: i64) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let restored = db
                .query_opt_i64(
                    "SELECT launch_attempts FROM provider_limit_holds WHERE id=?1 AND state='active'",
                    (id.to_owned(),),
                )
                .await
                .unwrap();
            if restored == Some(attempts) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("hold {id} was not restored for attempt {attempts}"));
}

/// A held re-entry whose launch really fails three times is gated
/// `launch_failed`: only the operator releases it, so it leaves the band. A
/// later re-entry and then a fresh filing on the same thread both still
/// release when a slot frees, rather than queueing behind it forever.
#[tokio::test]
async fn a_launch_failed_reentry_leaves_the_band_and_blocks_no_later_work() {
    use cairn_core::internal::execution::jobs::WakeCause;
    use cairn_core::internal::orchestrator::wakes::RESUME_GATE_LAUNCH_FAILED;
    let (temp, db, orch, _recorder) = focus_fixture().await;
    continue_job_impl(
        WakeCause::System,
        &orch,
        "a",
        Some("gated-marker"),
        None,
        None,
    )
    .unwrap_err();
    let gated = only_hold(&db).await;
    assert_eq!(gated.launch_attempts, 0);
    drop(orch);
    let (orch, failing) = orchestrator_with_spawn_failure(
        &temp,
        db.clone(),
        Some(ClaudeAuth::ApiKey("test-key".into())),
        true,
    );
    db.execute(
        "UPDATE turns SET state='complete',ended_at=3 WHERE id='busy-turn'",
        (),
    )
    .await
    .unwrap();
    for attempt in 1..=3 {
        cairn_core::internal::execution::gates::sweep::sweep(&orch)
            .await
            .unwrap();
        promote_private_lanes_for_test(&orch);
        await_restored(&db, &gated.id, attempt).await;
        assert_eq!(failing.spawn_count(), attempt as usize);
    }
    let restored = only_hold(&db).await;
    assert_eq!(
        restored.resume_gate.as_deref(),
        Some(RESUME_GATE_LAUNCH_FAILED)
    );
    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    assert_eq!(failing.spawn_count(), 3, "a gated hold is not retried");
    drop(orch);

    // Healthy launches from here. The cap fills again and a second child
    // re-enters behind it.
    let (orch, recorder) = orchestrator(&temp, db.clone());
    second_parked_child(&db).await;
    db.execute(
        "UPDATE turns SET state='running',ended_at=NULL WHERE id='busy-turn'",
        (),
    )
    .await
    .unwrap();
    let error = continue_job_impl(
        WakeCause::System,
        &orch,
        "c",
        Some("later-reentry-marker"),
        None,
        None,
    )
    .unwrap_err();
    assert!(error.contains("focus"), "{error}");
    let later = load_active_provider_limit_holds(&db)
        .await
        .unwrap()
        .into_iter()
        .find(|h| h.job_id == "c")
        .unwrap();
    assert!(gated.queue_order() < later.queue_order());

    // The slot frees: the later re-entry releases despite the older gated hold.
    db.execute(
        "UPDATE turns SET state='complete',ended_at=4 WHERE id='busy-turn'",
        (),
    )
    .await
    .unwrap();
    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    await_admission(&orch, "c", "active").await;
    assert_eq!(recorder.spawn_count(), 1);
    assert_eq!(marker_deliveries(&db, "c", "later-reentry-marker").await, 1);

    // Its turn ends: a fresh filing on the thread starts at once.
    db.execute_script("UPDATE turns SET state='complete',ended_at=5 WHERE job_id='c' AND state IN ('pending','running');
        INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at) VALUES('fresh','project',4,'Fresh','active','focus',1,1);
        INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq) VALUES('fresh-exec','hold','fresh','project','running',1,1);
        INSERT INTO jobs(id,execution_id,issue_id,project_id,status,node_name,created_at,updated_at) VALUES('fresh','fresh-exec','fresh','project','pending','Fresh',1,1);").await.unwrap();
    assert!(
        cairn_core::internal::orchestrator::wakes::admit_thread_launch(
            &orch,
            "fresh",
            WakeCause::System,
            false
        )
        .await
        .unwrap()
        .is_some(),
        "a fresh start is not held behind a gated hold"
    );
    let still = load_active_provider_limit_holds(&db).await.unwrap();
    assert_eq!(still.len(), 1, "{still:?}");
    assert_eq!(
        still[0].id, gated.id,
        "the gated hold waits for the operator"
    );
    assert_eq!(
        common::scalar_i64_by_id(
            &db,
            "SELECT COUNT(*) FROM turns WHERE job_id=?1 AND state IN ('pending','running')",
            "a"
        )
        .await,
        0,
        "the gated child has no live successor"
    );
}

/// Plan verification 5 (harness condition 2): an Addressed Continue on a child
/// that ALREADY holds a focus-held System wake runs at once at a full cap,
/// retires the hold, and delivers both inputs exactly once. A later pass finds
/// nothing to redeliver.
#[tokio::test]
async fn an_addressed_continue_supersedes_a_held_system_wake_and_delivers_it_once() {
    use cairn_core::internal::execution::jobs::WakeCause;
    let (_temp, db, orch, recorder) = focus_fixture().await;
    continue_job_impl(
        WakeCause::System,
        &orch,
        "a",
        Some("held-system-marker"),
        None,
        None,
    )
    .unwrap_err();
    let held = only_hold(&db).await;
    assert_eq!(held.boundary_source, "thread_focus");

    continue_job_impl(
        WakeCause::Addressed,
        &orch,
        "a",
        Some("addressed-marker"),
        None,
        None,
    )
    .unwrap();
    await_admission(&orch, "a", "active").await;
    assert_eq!(recorder.spawn_count(), 1);
    assert_eq!(turns(&db).await, 2);
    assert!(
        load_active_provider_limit_holds(&db)
            .await
            .unwrap()
            .is_empty(),
        "the addressed turn retires the held continuation"
    );
    assert_eq!(marker_deliveries(&db, "a", "addressed-marker").await, 1);
    assert_eq!(
        marker_deliveries(&db, "a", "held-system-marker").await,
        1,
        "the held wake is neither dropped nor duplicated"
    );

    // The busy sibling ends and a pass runs: nothing is redelivered.
    db.execute(
        "UPDATE turns SET state='complete',ended_at=3 WHERE id='busy-turn'",
        (),
    )
    .await
    .unwrap();
    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    assert_eq!(turns(&db).await, 2);
    assert_eq!(recorder.spawn_count(), 1);
    assert_eq!(marker_deliveries(&db, "a", "held-system-marker").await, 1);
}

async fn push_delivery(db: &LocalDb, push: &str) -> Option<String> {
    common::scalar_text_by_id(
        db,
        "SELECT delivered_event_id FROM attention_pushes WHERE id=?1",
        push,
    )
    .await
}

/// Plan verification 5 with a real check-settled wake rather than a message
/// marker: the push is stored in the shape the checks router stores
/// (`turn-checks:<checks uri>`, wake, event boundary) and delivered by the
/// production nudge, whose cause comes from the push key. At a full cap the
/// wake is focus-held with the push undelivered; the no-hint pass releases
/// it, the push is stamped by one briefing event in the resumed turn, and a
/// second pass neither re-stamps nor relaunches.
#[tokio::test]
async fn a_check_settled_push_is_held_then_delivered_exactly_once() {
    use cairn_core::internal::orchestrator::attention_push::{push, Boundary, Wake};
    use cairn_core::messages::delivery::nudge_job_for_urgency;
    use cairn_core::messages::queued::Delivery;
    let (_temp, db, orch, recorder) = focus_fixture().await;
    let checks = "cairn://p/hold/1/1/a/checks";
    let (push_id, wake) = push(
        &db,
        "a",
        checks,
        Wake::Wake,
        Boundary::Event,
        &format!("turn-checks:{checks}"),
    )
    .await
    .unwrap();
    assert_eq!(wake, Wake::Wake);

    // The nudge's resume is refused by focus, so the outcome is not asserted.
    let _ = nudge_job_for_urgency(&orch, "a", Delivery::Queue);
    let held = only_hold(&db).await;
    assert_eq!(held.boundary_source, "thread_focus");
    assert_eq!(
        held.kind,
        cairn_core::internal::orchestrator::wakes::HoldKind::Resume
    );
    assert_eq!(turns(&db).await, 1);
    assert_eq!(
        push_delivery(&db, &push_id).await,
        None,
        "held, not delivered"
    );
    assert_eq!(recorder.spawn_count(), 0);

    db.execute(
        "UPDATE turns SET state='complete',ended_at=3 WHERE id='busy-turn'",
        (),
    )
    .await
    .unwrap();
    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    await_admission(&orch, "a", "active").await;
    assert_eq!(turns(&db).await, 2);
    let event = push_delivery(&db, &push_id)
        .await
        .expect("the released turn delivers the push");
    assert_eq!(
        common::scalar_i64_by_id(
            &db,
            "SELECT COUNT(*) FROM events e JOIN turns t ON t.id=e.turn_id
             WHERE e.id=?1 AND t.job_id='a' AND t.predecessor_id IS NOT NULL",
            &event,
        )
        .await,
        1,
        "stamped by an event in the resumed turn"
    );
    assert_eq!(recorder.spawn_count(), 1);

    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    assert_eq!(
        push_delivery(&db, &push_id).await.as_deref(),
        Some(event.as_str())
    );
    assert_eq!(turns(&db).await, 2);
    assert_eq!(recorder.spawn_count(), 1);
}

/// A synced focus hold whose execution another runner owns is never claimed
/// here, even with a free slot: the pass leaves it active and launches
/// nothing. Once this runner owns the execution, the next pass releases it
/// once and delivers its input once.
#[tokio::test]
async fn a_foreign_owned_focus_hold_is_not_claimed_until_this_runner_owns_it() {
    use cairn_core::internal::execution::jobs::WakeCause;
    let (_temp, db, orch, recorder) = focus_fixture().await;
    continue_job_impl(
        WakeCause::System,
        &orch,
        "a",
        Some("foreign-owner-marker"),
        None,
        None,
    )
    .unwrap_err();
    let held = only_hold(&db).await;
    db.execute_script(
        "UPDATE executions SET runner_device_id='a-peer-device' WHERE id='exec';
         UPDATE turns SET state='complete',ended_at=3 WHERE id='busy-turn';",
    )
    .await
    .unwrap();
    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    assert!(!release_on_supply_change(&orch, &held, i64::MAX)
        .await
        .unwrap());
    assert_eq!(
        text(
            &db,
            "SELECT state FROM provider_limit_holds WHERE id=?1",
            &held.id
        )
        .await,
        "active"
    );
    assert_eq!(turns(&db).await, 1);
    assert_eq!(recorder.spawn_count(), 0);

    db.execute(
        "UPDATE executions SET runner_device_id=NULL WHERE id='exec'",
        (),
    )
    .await
    .unwrap();
    cairn_core::internal::execution::gates::sweep::sweep(&orch)
        .await
        .unwrap();
    await_admission(&orch, "a", "active").await;
    assert_eq!(turns(&db).await, 2);
    assert_eq!(recorder.spawn_count(), 1);
    assert_eq!(marker_deliveries(&db, "a", "foreign-owner-marker").await, 1);
}

/// Crash recovery for a focus-held continuation whose claim committed but
/// whose launch never happened: (a) the hold is `resolved` and nothing else
/// moved; (b) a pending direct successor of the held head was also written,
/// with no run and no prepared launch, and became the job's head. A new
/// runner's no-hint pass restores the claim with its FIFO place, it waits
/// while the cap is full, then releases once and delivers the original input
/// once.
#[tokio::test]
async fn a_crash_between_focus_claim_and_launch_is_recovered_with_its_place() {
    use cairn_core::internal::execution::jobs::WakeCause;
    for orphan_successor in [false, true] {
        let (temp, db, orch, _recorder) = focus_fixture().await;
        continue_job_impl(
            WakeCause::System,
            &orch,
            "a",
            Some("crash-recovery-marker"),
            None,
            None,
        )
        .unwrap_err();
        let held = only_hold(&db).await;
        drop(orch);
        db.execute(
            "UPDATE provider_limit_holds SET state='resolved',resolved_at=3,updated_at=3 WHERE id=?1",
            params![held.id.as_str()],
        )
        .await
        .unwrap();
        if orphan_successor {
            db.execute_script(
                "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,predecessor_id,created_at,updated_at)
                   VALUES('crash-successor','session',NULL,'a',2,'pending','wait_resolved','turn',3,3);
                 UPDATE jobs SET current_turn_id='crash-successor' WHERE id='a';",
            )
            .await
            .unwrap();
        }

        let (orch, recorder) = orchestrator(&temp, db.clone());
        cairn_core::internal::execution::gates::sweep::sweep(&orch)
            .await
            .unwrap();
        let restored = only_hold(&db).await;
        assert_eq!(restored.id, held.id, "orphan={orphan_successor}");
        assert_eq!(restored.boundary_source, "thread_focus");
        assert_eq!(
            restored.queue_order(),
            held.queue_order(),
            "its re-entry place is kept"
        );
        assert_eq!(recorder.spawn_count(), 0, "the cap is still full");
        assert_eq!(
            marker_deliveries(&db, "a", "crash-recovery-marker").await,
            0
        );

        db.execute(
            "UPDATE turns SET state='complete',ended_at=4 WHERE id='busy-turn'",
            (),
        )
        .await
        .unwrap();
        cairn_core::internal::execution::gates::sweep::sweep(&orch)
            .await
            .unwrap();
        await_admission(&orch, "a", "active").await;
        assert_eq!(recorder.spawn_count(), 1, "orphan={orphan_successor}");
        assert_eq!(
            marker_deliveries(&db, "a", "crash-recovery-marker").await,
            1,
            "orphan={orphan_successor}"
        );
        assert_eq!(
            common::scalar_i64_by_id(
                &db,
                "SELECT COUNT(*) FROM turns WHERE job_id=?1 AND state IN ('pending','running')",
                "a"
            )
            .await,
            1,
            "one live successor"
        );
        cairn_core::internal::execution::gates::sweep::sweep(&orch)
            .await
            .unwrap();
        assert_eq!(recorder.spawn_count(), 1);
    }
}

/// Saturate a parked ingress recipient's thread without another migrated store.
pub(super) async fn saturate_ingress_focus(
    orch: &Orchestrator,
    db: &LocalDb,
    issue: &str,
    execution: &str,
) {
    orch.update_settings(cairn_core::models::UpdateSettings {
        max_open_children_per_thread: Some(1),
        max_concurrent_executions: Some(10),
        ..Default::default()
    })
    .unwrap();
    db.execute("INSERT INTO threads(id,project_id,name,created_at,updated_at) SELECT 'ingress-focus',project_id,'ingress-focus',1,1 FROM issues WHERE id=?1", (issue.to_owned(),)).await.unwrap();
    db.execute(
        "UPDATE issues SET parent_thread_id='ingress-focus' WHERE id=?1",
        (issue.to_owned(),),
    )
    .await
    .unwrap();
    for (id, number) in [("ingress-busy", 9001), ("ingress-fresh", 9002)] {
        db.execute("INSERT INTO issues(id,project_id,number,title,status,parent_thread_id,created_at,updated_at) SELECT ?1,project_id,?2,?1,'active','ingress-focus',1,1 FROM issues WHERE id=?3", (id.to_owned(), number, issue.to_owned())).await.unwrap();
        db.execute("INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq,snapshot) SELECT ?1,recipe_id,?1,project_id,'running',1,1,snapshot FROM executions WHERE id=?2", (id.to_owned(),execution.to_owned())).await.unwrap();
        db.execute("INSERT INTO jobs(id,execution_id,issue_id,project_id,status,node_name,created_at,updated_at) SELECT ?1,?1,?1,project_id,'pending',?1,1,1 FROM issues WHERE id=?1", (id.to_owned(),)).await.unwrap();
    }
    db.execute_script("INSERT INTO sessions(id,job_id,backend,status,created_at,updated_at) VALUES('ingress-busy','ingress-busy','claude','open',1,1);
      INSERT INTO turns(id,session_id,job_id,sequence,state,start_reason,created_at,updated_at) VALUES('ingress-busy','ingress-busy','ingress-busy',1,'running','initial',1,1);
      UPDATE jobs SET status='running',current_session_id='ingress-busy',current_turn_id='ingress-busy' WHERE id='ingress-busy';").await.unwrap();
}

/// Remove the original occupant: only the addressed recipient can now fill focus.
pub(super) async fn assert_ingress_counts_and_holds_fresh(
    orch: &Orchestrator,
    db: &LocalDb,
    recipient: &str,
) {
    assert!(load_active_provider_limit_holds(db)
        .await
        .unwrap()
        .iter()
        .all(|h| h.job_id != recipient));
    db.execute_script(
        "UPDATE turns SET state='complete',ended_at=2 WHERE id='ingress-busy';
      UPDATE jobs SET status='completed' WHERE id='ingress-busy';",
    )
    .await
    .unwrap();
    assert!(
        cairn_core::internal::orchestrator::wakes::admit_thread_launch(
            orch,
            "ingress-fresh",
            cairn_core::models::WakeCause::System,
            false
        )
        .await
        .unwrap()
        .is_none()
    );
    let holds = load_active_provider_limit_holds(db).await.unwrap();
    assert!(holds
        .iter()
        .any(|h| h.job_id == "ingress-fresh" && h.boundary_source == "thread_focus"));
}

/// An operator answer, authenticated as a desktop operator.
fn operator_allow() -> cairn_core::internal::mcp::handlers::permission::PermissionAnswer {
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, CredentialRef,
        PrincipalRef, VerificationMethod, VerificationRecord, VerificationStatus,
        VerificationStrength,
    };
    use cairn_core::internal::mcp::handlers::permission::{
        OperatorApproval, PermissionAnswer, PermissionDecision,
    };
    let actor = PrincipalRef::Machine {
        device_id: "test-device".to_string(),
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
            device_id: "test-device".to_string(),
        },
        verification,
        1,
        None,
    )
    .unwrap();
    let appearance = AppearanceSnapshot::new(actor.clone(), evidence, vec![], None).unwrap();
    PermissionAnswer::from_operator(
        PermissionDecision::Allow,
        OperatorApproval::authenticated(actor, appearance).unwrap(),
    )
}

/// Plan verification 6, the permission path: at a full focus cap, answering a
/// child's parked permission resumes it at once (an Addressed turn, never
/// held). It then counts, so once the busy sibling ends a second child's
/// fresh launch is still held behind it.
#[tokio::test]
async fn a_permission_answer_resumes_at_a_full_cap_and_then_counts() {
    use cairn_core::internal::execution::jobs::WakeCause;
    use cairn_core::internal::mcp::handlers::permission::resolve_permission_request;
    let (_temp, db, orch, recorder) = focus_fixture().await;
    second_parked_child(&db).await;
    db.execute_script(
        "UPDATE turns SET state='yielded',yield_reason='permission',ended_at=NULL WHERE id='turn';
         INSERT INTO permission_requests(id,run_id,job_id,tool_use_id,tool_name,tool_input,status,created_at,turn_id,uri_segment)
           VALUES('perm-a','run','a','toolu-perm','Bash','{}','pending',1,'turn','perm-1');",
    )
    .await
    .unwrap();

    resolve_permission_request(&orch, "perm-a", operator_allow())
        .await
        .expect("the answer applies");
    assert!(
        load_active_provider_limit_holds(&db)
            .await
            .unwrap()
            .is_empty(),
        "an answer is never focus-held"
    );
    await_admission(&orch, "a", "active").await;
    assert_eq!(turns(&db).await, 2);
    assert_eq!(recorder.spawn_count(), 1);

    // The busy sibling ends; the answered child still holds the one slot.
    db.execute(
        "UPDATE turns SET state='complete',ended_at=3 WHERE id='busy-turn'",
        (),
    )
    .await
    .unwrap();
    let admitted = cairn_core::internal::orchestrator::wakes::admit_thread_launch(
        &orch,
        "c",
        WakeCause::System,
        false,
    )
    .await
    .unwrap();
    assert!(admitted.is_none(), "the next fresh launch is held");
    let held = only_hold(&db).await;
    assert_eq!(held.job_id, "c");
    assert_eq!(
        held.kind,
        cairn_core::internal::orchestrator::wakes::HoldKind::Launch
    );
    assert_eq!(held.boundary_source, "thread_focus");
    assert_eq!(recorder.spawn_count(), 1);
}
