use super::sweep_process_lifetimes;
use super::warm_completion_tests::test_orchestrator;
use crate::agent_process::process::RunHandle;
use crate::orchestrator::Orchestrator;
use crate::storage::{migrated_test_db, RowExt};
use std::time::{Duration, Instant};

async fn fixture(
    name: &str,
    backend: &str,
    session_status: &str,
    job_status: &str,
) -> Orchestrator {
    let db = migrated_test_db(name).await;
    db.execute_script(format!(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO jobs (id, project_id, status, current_session_id, created_at, updated_at)
           VALUES ('job', 'project', '{job_status}', 'session', 1, 1);
         INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
           VALUES ('session', 'job', '{backend}', '{session_status}', 1, 1, 1);
         INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
           VALUES ('run', 'job', 'session', 'live', 1, 1);
         PRAGMA foreign_keys = ON;"
    ))
    .await
    .unwrap();
    test_orchestrator(db)
}

fn register(orch: &Orchestrator, idle_secs: u64) {
    let mut handle = RunHandle::test_handle(Some("session"), Some("job"));
    handle.transition_to_warm();
    handle.last_activity = Instant::now() - Duration::from_secs(idle_secs);
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run".into(), handle);
}

async fn run_state(orch: &Orchestrator) -> (String, Option<String>) {
    orch.db
        .local
        .query_opt(
            "SELECT status, exit_reason FROM runs WHERE id='run'",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_closed_and_cancelled_record_deliberate_exits() {
    for (session, job, reason) in [
        ("closed", "running", "session_closed"),
        ("open", "cancelled", "job_cancelled"),
    ] {
        let orch = fixture("lifetime-closed-cancelled", "other", session, job).await;
        register(&orch, 0);
        sweep_process_lifetimes(&orch);
        assert!(!orch.process_state.is_warm("run"), "{reason}");
        assert_eq!(
            run_state(&orch).await,
            ("exited".into(), Some(reason.into()))
        );
    }
}

/// A yielded head turn, as a park leaves it, and the owner rows that may stand
/// behind it. Each owner is seeded beside the same yielded turn so the turn
/// itself is never what decides.
const YIELDED_TURN: &str = "INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, yield_reason, start_reason, created_at, updated_at)
     VALUES ('turn', 'job', 'session', 'run', 1, 'yielded', 'wait', 'initial', 1, 1);";

fn owner_rows(owner: &str) -> &'static str {
    match owner {
        "parked_wait" => "INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id, condition_json, state, created_at)
             VALUES ('wait', 'job', 'run', 'session', 'turn', 'tool', '{}', 'pending', 1);",
        "resolving_wait" => "INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id, condition_json, state, created_at)
             VALUES ('wait', 'job', 'run', 'session', 'turn', 'tool', '{}', 'resolving', 1);",
        "inline_hold" => "INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id, condition_json, state, created_at, hold)
             VALUES ('wait', 'job', 'run', 'session', 'turn', 'tool', '{}', 'pending', 1, 'inline');",
        "permission" => "INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status, created_at, turn_id, job_id)
             VALUES ('permission', 'run', 'tool', 'Bash', '{}', 'pending', 1, 'turn', 'job');",
        "question" => "INSERT INTO prompts (id, run_id, questions, created_at, turn_id, job_id)
             VALUES ('prompt', 'run', '[]', 1, 'turn', 'job');",
        "child" => "INSERT INTO jobs (id, project_id, status, parent_job_id, created_at, updated_at)
             VALUES ('child', 'project', 'running', 'job', 1, 1);",
        "detached_batch" => "INSERT INTO run_batch_detached (request_id, job_id, run_id, state, reason, created_ms)
             VALUES ('batch', 'job', 'run', 'running', 'test', 1);",
        "parked_batch" => "INSERT INTO run_batch_detached (request_id, job_id, run_id, state, reason, created_ms, holds_branch)
             VALUES ('batch', 'job', 'run', 'parked', 'test', 1, 0);",
        // Settled rows own nothing: they are the history every live job carries.
        "settled" => "INSERT INTO agent_waits (id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id, condition_json, state, created_at)
             VALUES ('wait', 'job', 'run', 'session', 'turn', 'tool', '{}', 'resolved', 1);
           INSERT INTO turns (id, job_id, session_id, run_id, sequence, predecessor_id, state, start_reason, created_at, updated_at)
             VALUES ('successor', 'job', 'session', 'run', 2, 'turn', 'complete', 'wait_resolved', 2, 2);
           INSERT INTO permission_requests (id, run_id, tool_use_id, tool_name, tool_input, status, created_at, turn_id, job_id)
             VALUES ('permission', 'run', 'tool', 'Bash', '{}', 'denied', 1, 'turn', 'job');
           INSERT INTO prompts (id, run_id, questions, created_at, turn_id, job_id, withdrawn_at)
             VALUES ('prompt', 'run', '[]', 1, 'turn', 'job', 2);
           INSERT INTO jobs (id, project_id, status, parent_job_id, created_at, updated_at)
             VALUES ('child', 'project', 'complete', 'job', 1, 1);
           INSERT INTO run_batch_detached (request_id, job_id, run_id, state, reason, created_ms)
             VALUES ('batch', 'job', 'run', 'settled', 'test', 1);",
        _ => "",
    }
}

async fn seed_yield(orch: &Orchestrator, owner: &str) {
    orch.db
        .local
        .execute_script(format!(
            "PRAGMA foreign_keys = OFF; {YIELDED_TURN} {} PRAGMA foreign_keys = ON;",
            owner_rows(owner)
        ))
        .await
        .unwrap();
}

async fn swept(orch: &Orchestrator) -> Option<String> {
    sweep_process_lifetimes(orch);
    if orch.process_state.is_warm("run") {
        assert_eq!(run_state(orch).await, ("live".into(), None));
        return None;
    }
    let (status, reason) = run_state(orch).await;
    assert_eq!(status, "exited");
    reason
}

/// Work held in the process, or about to enter it, keeps the process past the
/// horizon and against closure and cancellation: a pending turn (a delivery
/// awaiting launch), a running turn, and a `run` call held open on its socket.
#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_in_process_owners_outrank_closure_and_the_horizon() {
    for (session, job) in [("closed", "cancelled"), ("open", "running")] {
        for state in ["pending", "running"] {
            let orch = fixture("lifetime-live-turn", "claude", session, job).await;
            orch.db.local.execute(
                "INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, start_reason, created_at, updated_at)
                 VALUES ('turn', 'job', 'session', 'run', 1, ?1, 'initial', 1, 1)", (state,)
            ).await.unwrap();
            register(&orch, 7200);
            assert_eq!(swept(&orch).await, None, "{state} turn: {session} session");
        }
        let orch = fixture("lifetime-inline-hold", "claude", session, job).await;
        seed_yield(&orch, "inline_hold").await;
        register(&orch, 7200);
        assert_eq!(swept(&orch).await, None, "inline hold: {session} session");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_launch_lock_protects_closed_session() {
    let orch = fixture("lifetime-locked", "claude", "closed", "running").await;
    register(&orch, 0);
    let lock = orch.job_launch_lock("job");
    let guard = lock.lock().unwrap();
    sweep_process_lifetimes(&orch);
    assert!(orch.process_state.is_warm("run"));
    drop(guard);
    sweep_process_lifetimes(&orch);
    assert_eq!(
        run_state(&orch).await,
        ("exited".into(), Some("session_closed".into()))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_idle_fallback_and_unknown_backend() {
    for (backend, idle, stopped) in [
        ("claude", 3500, false),
        ("claude", 3600, true),
        ("codex", 1700, false),
        ("codex", 1800, true),
        ("other", 7200, false),
    ] {
        let orch = fixture("lifetime-idle", backend, "open", "running").await;
        register(&orch, idle);
        sweep_process_lifetimes(&orch);
        assert_eq!(
            !orch.process_state.is_warm("run"),
            stopped,
            "{backend} {idle}"
        );
        if stopped {
            assert_eq!(
                run_state(&orch).await,
                ("exited".into(), Some("cache_horizon_idle".into()))
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_missing_facts_preserve_process() {
    let orch = fixture("lifetime-missing-facts", "claude", "closed", "running").await;
    register(&orch, 7200);
    orch.db
        .local
        .execute("DELETE FROM sessions WHERE id='session'", ())
        .await
        .unwrap();
    sweep_process_lifetimes(&orch);
    assert!(orch.process_state.is_warm("run"));
    assert_eq!(run_state(&orch).await.0, "live");
}

#[test]
fn process_lifetime_periodic_wiring_precedes_warm_trim() {
    let source = include_str!("../mod.rs");
    let start = source.find("pub fn spawn_process_sweep").unwrap();
    let source = &source[start..];
    let watchdog = source.find("sweep_stranded_watchdog_jobs(&orch)").unwrap();
    let sweep = source.find("sweep_process_lifetimes(&orch)").unwrap();
    let trim = source.find("orch.collect_warm(").unwrap();
    assert!(watchdog < sweep && sweep < trim);
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_active_occupancies_are_never_candidates() {
    for occupancy in ["busy", "serving", "awaiting"] {
        let orch = fixture("lifetime-occupancy", "claude", "closed", "cancelled").await;
        register(&orch, 7200);
        match occupancy {
            "busy" => {
                assert!(orch.process_state.transition_to_active("run"));
            }
            "serving" => {
                assert!(orch.process_state.begin_turn("run", "turn"));
            }
            _ => {
                assert!(orch.process_state.begin_turn("run", "turn"));
                assert!(orch.process_state.yield_for_host("run", "turn"));
            }
        }
        let before = orch.process_state.get_occupancy("run");
        sweep_process_lifetimes(&orch);
        assert_eq!(
            orch.process_state.get_occupancy("run"),
            before,
            "{occupancy}"
        );
        assert_eq!(run_state(&orch).await.0, "live");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_provider_request_predicate_and_horizon() {
    for (backend, age, stops) in [
        ("claude", 3599, false),
        ("claude", 3600, true),
        ("codex", 1799, false),
        ("codex", 1800, true),
        ("other", 7200, false),
    ] {
        let orch = fixture("lifetime-request", backend, "open", "running").await;
        // Clock is injected by test_orchestrator and returns zero. Newer events
        // with no usage or nested tool usage must not refresh the provider cache.
        orch.db.local.execute_script(format!(
            "INSERT INTO events(id,run_id,session_id,sequence,timestamp,event_type,data,created_at,input_tokens)
               VALUES('provider','run','session',1,-{age},'assistant','{{}}',-{age},0);
             INSERT INTO events(id,run_id,session_id,sequence,timestamp,event_type,data,created_at)
               VALUES('no-usage','run','session',2,-1,'assistant','{{}}',-1);
             INSERT INTO events(id,run_id,session_id,sequence,timestamp,event_type,data,created_at,output_tokens,parent_tool_use_id)
               VALUES('nested','run','session',3,-1,'assistant','{{}}',-1,1,'tool');"
        )).await.unwrap();
        register(&orch, 0);
        sweep_process_lifetimes(&orch);
        assert_eq!(
            !orch.process_state.is_warm("run"),
            stops,
            "{backend} age {age}"
        );
        if stops {
            assert_eq!(
                run_state(&orch).await,
                ("exited".into(), Some("cache_horizon_idle".into()))
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_rechecks_facts_after_candidate_snapshot() {
    for change in ["turn", "request"] {
        let mut orch = fixture(
            "lifetime-recheck",
            "codex",
            if change == "turn" { "closed" } else { "open" },
            "running",
        )
        .await;
        if change == "request" {
            orch.db.local.execute_script(
                "INSERT INTO events(id,run_id,session_id,sequence,timestamp,event_type,data,created_at,input_tokens)
                 VALUES('old','run','session',1,1,'assistant','{}',1,1);"
            ).await.unwrap();
        }
        register(&orch, 7200);
        let db = orch.db.local.clone();
        let mut clock = crate::services::testing::MockClock::new();
        clock.expect_now().times(1).returning(move || {
            let db = db.clone();
            crate::storage::run_db_blocking(move || async move {
                let sql = if change == "turn" {
                    "INSERT INTO turns(id,job_id,session_id,run_id,sequence,state,start_reason,created_at,updated_at)
                     VALUES('turn','job','session','run',1,'pending','initial',1,1)"
                } else {
                    "INSERT INTO events(id,run_id,session_id,sequence,timestamp,event_type,data,created_at,input_tokens)
                     VALUES('fresh','run','session',2,10000,'assistant','{}',10000,1)"
                };
                db.execute(sql, ()).await.map_err(|e| e.to_string())?;
                Ok::<_, String>(())
            }).unwrap();
            10000
        });
        orch.services = std::sync::Arc::new(
            crate::services::testing::TestServicesBuilder::new()
                .with_clock(clock)
                .build(),
        );
        sweep_process_lifetimes(&orch);
        assert!(
            orch.process_state.is_warm("run"),
            "{change} arrived after the initial read"
        );
        assert_eq!(run_state(&orch).await.0, "live");
    }
}

struct ObservedChild(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl crate::services::ChildProcess for ObservedChild {
    fn id(&self) -> u32 {
        i32::MAX as u32
    }
    fn take_stdout(&mut self) -> Option<Box<dyn std::io::BufRead + Send>> {
        None
    }
    fn take_stderr(&mut self) -> Option<Box<dyn std::io::BufRead + Send>> {
        None
    }
    fn take_stdin(&mut self) -> Option<Box<dyn std::io::Write + Send>> {
        None
    }
    fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        Ok(None)
    }
    fn kill(&mut self) -> std::io::Result<()> {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_teardown_releases_admission_and_keeps_terminal() {
    use crate::agent_process::process::ProcessAdmissionClaim;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    let orch = fixture("lifetime-teardown", "codex", "closed", "running").await;
    orch.db.local.execute_script(
        r#"INSERT INTO job_terminals(id,job_id,session_id,command,status,created_at,slug)
             VALUES('terminal','job','terminal-session','sleep 100','running',1,'dev');
           INSERT INTO runtime_admission_requests(request_id,workspace_key,resource_lane,launch_key,claim_json,priority,enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at)
             VALUES('request','private-runtime','codex','ordinary:run','{"residentProcessUnits":1,"logicalStreamUnits":1}',0,1,'active','owner',7,9999999999,1,1);"#
    ).await.unwrap();
    let killed = Arc::new(AtomicBool::new(false));
    let child = Arc::new(Mutex::new(Some(
        Box::new(ObservedChild(killed.clone())) as Box<dyn crate::services::ChildProcess>
    )));
    let mut handle = RunHandle::new(
        child.clone(),
        Arc::new(Mutex::new(None)),
        Some("session".into()),
        Some("job".into()),
    );
    handle.transition_to_warm();
    orch.process_state.prepare_admission_claim(
        "run".into(),
        ProcessAdmissionClaim {
            request_id: "request".into(),
            workspace_key: "private-runtime".into(),
            launch_key: "ordinary:run".into(),
            owner: "owner".into(),
            generation: 7,
            team_workspace_id: None,
        },
    );
    orch.process_state
        .register_process("run".into(), handle)
        .unwrap();

    sweep_process_lifetimes(&orch);

    assert!(killed.load(Ordering::SeqCst));
    assert!(child.lock().unwrap().is_none());
    assert!(orch
        .process_state
        .processes
        .lock()
        .unwrap()
        .get("run")
        .is_none());
    assert_eq!(orch.process_state.find_process_by_session("session"), None);
    assert_eq!(
        run_state(&orch).await,
        ("exited".into(), Some("session_closed".into()))
    );
    let admission = orch
        .db
        .local
        .query_opt_text(
            "SELECT state FROM runtime_admission_requests WHERE request_id='request'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(admission.as_deref(), Some("released"));
    let terminal = orch
        .db
        .local
        .query_opt_text("SELECT status FROM job_terminals WHERE id='terminal'", ())
        .await
        .unwrap();
    assert_eq!(terminal.as_deref(), Some("running"));
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_failed_fact_query_preserves_process() {
    let orch = fixture("lifetime-query-failure", "claude", "closed", "running").await;
    register(&orch, 7200);
    orch.db
        .local
        .execute("DROP TABLE events", ())
        .await
        .unwrap();
    sweep_process_lifetimes(&orch);
    assert!(orch.process_state.is_warm("run"));
    assert_eq!(run_state(&orch).await.0, "live");
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_routes_team_runs_and_fails_closed_without_replica() {
    use std::sync::Arc;
    const TEAM_RUN: &str = "teamA~00000000-0000-4000-8000-000000000001";
    const TEAM_JOB: &str = "teamA~00000000-0000-4000-8000-000000000002";
    let orch = fixture("lifetime-private-route", "other", "open", "running").await;
    register(&orch, 0);
    let mut handle = RunHandle::test_handle(Some("team-session"), Some(TEAM_JOB));
    handle.transition_to_warm();
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register(TEAM_RUN.into(), handle);
    sweep_process_lifetimes(&orch);
    assert!(
        orch.process_state.is_warm(TEAM_RUN),
        "closed replica must not route to private"
    );

    let team = Arc::new(migrated_test_db("lifetime-team-route").await);
    team.execute_script(format!(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO jobs(id,project_id,status,current_session_id,created_at,updated_at)
          VALUES('{TEAM_JOB}','project','running','team-session',1,1);
         INSERT INTO sessions(id,job_id,backend,status,sequence,created_at,updated_at)
          VALUES('team-session','{TEAM_JOB}','other','closed',1,1,1);
         INSERT INTO runs(id,job_id,session_id,status,created_at,updated_at)
          VALUES('{TEAM_RUN}','{TEAM_JOB}','team-session','live',1,1);"
    ))
    .await
    .unwrap();
    orch.db
        .register_team_db_for_test("teamA".into(), team.clone())
        .await;
    sweep_process_lifetimes(&orch);
    assert!(!orch.process_state.is_warm(TEAM_RUN));
    assert!(
        orch.process_state.is_warm("run"),
        "private candidate remains open"
    );
    let status = team
        .query_opt(
            "SELECT status,exit_reason FROM runs WHERE id=?1",
            (TEAM_RUN,),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, ("exited".into(), Some("session_closed".into())));
}

#[tokio::test(flavor = "multi_thread")]
async fn process_lifetime_exit_reason_write_failure_still_finalizes() {
    let orch = fixture("lifetime-reason-failure", "other", "closed", "running").await;
    register(&orch, 0);
    orch.db
        .local
        .execute_script(
            "CREATE TRIGGER reject_exit_reason BEFORE UPDATE OF exit_reason ON runs
         BEGIN SELECT RAISE(FAIL, 'injected reason failure'); END;",
        )
        .await
        .unwrap();
    sweep_process_lifetimes(&orch);
    assert!(orch
        .process_state
        .processes
        .lock()
        .unwrap()
        .get("run")
        .is_none());
    assert_eq!(run_state(&orch).await, ("exited".into(), None));
}

/// A managed-CLI swap retires an idle provider process and only it: the
/// job's running terminal survives, and the run is recorded as retired by the
/// swap.
#[tokio::test(flavor = "multi_thread")]
async fn managed_swap_retires_an_idle_provider_and_keeps_its_terminal() {
    let orch = fixture("swap-retire", "claude", "open", "running").await;
    orch.db
        .local
        .execute_script(
            r#"INSERT INTO job_terminals(id,job_id,session_id,command,status,created_at,slug)
                 VALUES('terminal','job','terminal-session','sleep 100','running',1,'dev');"#,
        )
        .await
        .unwrap();
    register(&orch, 0);

    assert!(super::retire_idle_provider(
        &orch,
        "run",
        "managed_cli_swap"
    ));

    assert!(!orch.process_state.is_warm("run"));
    assert_eq!(
        run_state(&orch).await,
        ("exited".into(), Some("managed_cli_swap".into()))
    );
    let terminal = orch
        .db
        .local
        .query_opt_text("SELECT status FROM job_terminals WHERE id='terminal'", ())
        .await
        .unwrap();
    assert_eq!(
        terminal.as_deref(),
        Some("running"),
        "the dev server keeps running"
    );
}

/// The swap never interrupts a process a message is bringing back to work:
/// one whose job a launch or resume holds is left alone, and so is one that
/// is no longer idle by the time the swap reaches it.
#[tokio::test(flavor = "multi_thread")]
async fn managed_swap_leaves_a_process_being_resumed_alone() {
    let orch = fixture("swap-held", "claude", "open", "running").await;
    register(&orch, 0);
    let lock = orch.job_launch_lock("job");
    let guard = lock.lock().unwrap();
    assert!(!super::retire_idle_provider(
        &orch,
        "run",
        "managed_cli_swap"
    ));
    assert!(orch.process_state.is_warm("run"), "a held launch keeps it");
    drop(guard);

    // A resume took it out of warm between the swap's snapshot and its turn.
    let orch = fixture("swap-busy", "claude", "open", "running").await;
    let handle = RunHandle::test_handle(Some("session"), Some("job"));
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run".into(), handle);
    assert!(!super::retire_idle_provider(
        &orch,
        "run",
        "managed_cli_swap"
    ));
    assert_eq!(run_state(&orch).await, ("live".into(), None));
    assert!(orch
        .process_state
        .processes
        .lock()
        .unwrap()
        .get("run")
        .is_some());
}
