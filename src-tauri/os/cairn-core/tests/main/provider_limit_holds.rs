//! Provider refusals are durable unfinished work, not successful turn endings.

use crate::common;
use cairn_core::credentials::{ClaudeAuth, CredentialStore};
use cairn_core::identity::{CredentialStoreExt, UserIdentity};
use cairn_core::internal::agent_process::process::AgentProcessState;
use cairn_core::internal::backends::claude::{claude_runtime, dispatch_claude_result};
use cairn_core::internal::db::DbState;
use cairn_core::internal::execution::advancement::{
    advance_execution_with_actions, is_action_node_ready, recompute_job_status_conn,
};
use cairn_core::internal::execution::jobs::{
    continue_job_impl, prepare_job, promote_private_lanes_for_test, start_prepared_job_session,
};
use cairn_core::internal::orchestrator::lifecycle::{finalize_run, stop_job};
use cairn_core::internal::orchestrator::wakes::{
    load_active_provider_limit_holds, resolve_provider_limit_hold, ProviderLimitHold,
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

    // Pre-resolved so the spawn never shells out to `which claude`. The recording
    // spawner never executes it, so the path only has to be a path.
    let process_state = Arc::new(AgentProcessState::default());
    *process_state.cli_binary_path.lock().unwrap() = Some("/cairn-test/claude".to_string());

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

    (
        Orchestrator::builder(db_state, services, config_dir)
            .boot_at(0)
            .process_state(process_state)
            .credential_store(credential_store)
            .build(),
        recorder,
    )
}

async fn fixture() -> (TempDir, Arc<LocalDb>, Orchestrator, RecordedProcesses) {
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
    db.execute_script(&format!("
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
    let (orch, recorder) = orchestrator(&temp, db.clone());
    (temp, db, orch, recorder)
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
async fn terminal_result_stores_boundary_and_holds_the_current_turn() {
    let (_temp, db, orch, recorder) = fixture().await;
    let hold = hold(&orch, &db, LIMIT).await;
    assert_eq!(hold.retry_not_before, Some(RESET_AT));
    assert_eq!(hold.boundary_source, "result_text");
    assert_held(&db).await;
    assert_eq!(hold.due_at(RESET_AT - 1), None);
    assert!(
        !resolve_provider_limit_hold(&orch, &hold, hold.next_due_at().unwrap() - 1)
            .await
            .unwrap()
    );
    assert_eq!(turns(&db).await, 1);
    assert_eq!(recorder.spawn_count(), 0);
}

#[tokio::test]
async fn held_recipe_node_dispatches_dependents_only_after_resumed_work_completes() {
    let (temp, db, mut orch, recorder) = fixture().await;
    orch.jj_binary_path =
        common::jj_bin().expect("provision the pinned jj binary before running integration tests");
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    for args in [
        vec!["init", "-q", "-b", "main"],
        vec![
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-qm",
            "fixture",
        ],
    ] {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(&repo)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    db.execute(
        "UPDATE projects SET repo_path=?1 WHERE id='project'",
        params![repo.to_string_lossy().as_ref()],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE jobs SET base_branch=?1 WHERE id='b'",
        params![common::head_sha(&repo)],
    )
    .await
    .unwrap();
    let hold = hold(&orch, &db, LIMIT).await;
    assert_held(&db).await;
    assert!(!cairn_workflow::advancement::issue_settled(&db, "issue")
        .await
        .unwrap());
    assert!(!is_action_node_ready(db.clone(), "exec", "b", "hold").unwrap());
    assert!(advance_execution_with_actions(&orch, "exec")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        text(&db, "SELECT status FROM jobs WHERE id=?1", "b").await,
        "pending"
    );
    assert_eq!(recorder.spawn_count(), 0);

    assert!(
        resolve_provider_limit_hold(&orch, &hold, hold.next_due_at().unwrap())
            .await
            .unwrap()
    );
    await_admission(&orch, "a", "active").await;
    assert_eq!(recorder.spawn_count(), 1);
    assert!(!is_action_node_ready(db.clone(), "exec", "b", "hold").unwrap());
    assert!(advance_execution_with_actions(&orch, "exec")
        .await
        .unwrap()
        .is_empty());
    let run = text(
        &db,
        "SELECT run_id FROM turns WHERE id=(SELECT current_turn_id FROM jobs WHERE id=?1)",
        "a",
    )
    .await;
    db.execute("INSERT INTO artifacts(id, job_id, artifact_type, confirmed, data, version, output_name, created_at, updated_at)
                VALUES('output','a','result',1,'{}',1,'result',?1,?1)", params![RESET_AT]).await.unwrap();
    result(&orch, &db, &run, "Finished work", false, RESET_AT + 200);
    assert_eq!(
        text(&db, "SELECT status FROM jobs WHERE id=?1", "a").await,
        "complete"
    );
    assert!(is_action_node_ready(db.clone(), "exec", "b", "hold").unwrap());
    let dispatched = advance_execution_with_actions(&orch, "exec").await.unwrap();
    assert_eq!(
        dispatched
            .iter()
            .map(|job| job.id.as_str())
            .collect::<Vec<_>>(),
        vec!["b"]
    );
    assert_eq!(
        text(&db, "SELECT status FROM jobs WHERE id=?1", "b").await,
        "running"
    );
    let prepared = prepare_job(&orch, "b").unwrap();
    start_prepared_job_session(&orch, prepared, None).unwrap();
    await_admission(&orch, "b", "active").await;
    assert_eq!(recorder.spawn_count(), 2);
}

#[tokio::test]
async fn resolution_claims_and_launches_exactly_one_successor() {
    let (_temp, db, orch, recorder) = fixture().await;
    let hold = hold(&orch, &db, LIMIT).await;
    let due = hold.next_due_at().unwrap();
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

// A limit on one account is a reason to wait only when no other account can take
// the work. With an eligible replacement, the session is repinned and the hold is
// due immediately rather than at the exhausted account's reset.
#[tokio::test]
async fn a_limit_with_an_eligible_replacement_switches_accounts_and_resumes_now() {
    let (_temp, db, orch, _recorder) = fixture().await;
    let [exhausted, replacement] = ["Work Max", "Personal Max"].map(|label| {
        orch.add_account(
            cairn_core::credentials::ApiProvider::Anthropic,
            label.to_string(),
            cairn_core::credentials::ProviderAuth::ClaudeProfile { oauth: None },
            None,
        )
        .unwrap()
        .id
    });
    db.execute(
        "UPDATE sessions SET account_id=?1 WHERE id='session'",
        params![exhausted.as_str()],
    )
    .await
    .unwrap();

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
    continue_job_impl(&orch, "a", None, None, None).unwrap();
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
async fn unknown_reset_is_unfinished_work_without_a_guessed_timer() {
    let (_temp, db, orch, recorder) = fixture().await;
    let hold = hold(&orch, &db, "Usage limit reached").await;
    assert_eq!(hold.retry_not_before, None);
    assert_eq!(hold.boundary_source, "unknown");
    assert_eq!(hold.next_due_at(), None);
    assert_held(&db).await;
    for now in [OBSERVED_AT, RESET_AT, RESET_AT + 604800, i64::MAX] {
        assert_eq!(hold.due_at(now), None);
        assert!(!resolve_provider_limit_hold(&orch, &hold, now)
            .await
            .unwrap());
    }
    assert_eq!(turns(&db).await, 1);
    assert_eq!(recorder.spawn_count(), 0);
}

#[tokio::test]
async fn operator_follow_up_supersedes_the_old_hold_without_an_extra_resume() {
    let (_temp, db, orch, recorder) = fixture().await;
    let hold = hold(&orch, &db, LIMIT).await;
    continue_job_impl(&orch, "a", None, None, None).unwrap();
    let head = text(&db, "SELECT current_turn_id FROM jobs WHERE id=?1", "a").await;
    assert_ne!(head, "turn");
    let before = turns(&db).await;
    assert!(
        !resolve_provider_limit_hold(&orch, &hold, hold.next_due_at().unwrap())
            .await
            .unwrap()
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
    assert_eq!(turns(&db).await, before);
    assert_eq!(
        text(&db, "SELECT current_turn_id FROM jobs WHERE id=?1", "a").await,
        head
    );
    await_admission(&orch, "a", "active").await;
    assert_eq!(recorder.spawn_count(), 1);
}

#[tokio::test]
async fn failed_launches_restore_the_held_head_then_stop_retrying_after_three_attempts() {
    let (temp, db, orch, _) = fixture().await;
    let original = hold(&orch, &db, LIMIT).await;
    finalize_run(&orch, "run", RunStatus::Exited);
    assert_held(&db).await;
    drop(orch);
    let (orch, recorder) = orchestrator_with_spawn_failure(
        &temp,
        db.clone(),
        Some(ClaudeAuth::ApiKey("test-key".into())),
        true,
    );
    let mut current = original;
    for attempt in 1..=3 {
        let due = current.next_due_at().unwrap();
        let before = chrono::Utc::now().timestamp();
        assert!(resolve_provider_limit_hold(&orch, &current, due)
            .await
            .unwrap());
        await_admission(&orch, "a", "failed").await;
        let after = chrono::Utc::now().timestamp();
        assert_eq!(recorder.spawn_count(), attempt as usize);
        assert_held(&db).await;
        let holds = load_active_provider_limit_holds(&db).await.unwrap();
        assert_eq!(holds.len(), 1);
        current = holds.into_iter().next().unwrap();
        assert_eq!(current.launch_attempts, attempt);
        assert_eq!(current.state, "active");
        assert_eq!(current.resolved_at, None);
        assert_eq!(
            common::scalar_i64_by_id(
                &db,
                "SELECT COUNT(*) FROM turns WHERE job_id=?1 AND state='pending'",
                "a"
            )
            .await,
            0,
        );
        if attempt < 3 {
            assert!((before + 300..=after + 300).contains(&current.retry_not_before.unwrap()));
        } else {
            assert_eq!(current.retry_not_before, None);
            assert_eq!(current.next_due_at(), None);
        }
    }
    assert!(!resolve_provider_limit_hold(&orch, &current, i64::MAX)
        .await
        .unwrap());
    assert_eq!(recorder.spawn_count(), 3);
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
