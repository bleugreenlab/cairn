use crate::common;

use cairn_core::internal::orchestrator::lifecycle;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;

async fn insert_project_job_run_turn(db: &LocalDb, turn_state: &str) {
    let project_id = common::create_project(db, "stop").await;
    let turn_state = turn_state.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let turn_state = turn_state.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
                 VALUES ('job-1', ?1, 'running', 'session-1', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-1', ?1, 'job-1', NULL, 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 1, ?1, 1, 1)",
                params![turn_state.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stop_session_preserves_settled_resource_write_report_once() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;
    let report = serde_json::json!({
        "applied": [{
            "index": 0,
            "target": "cairn://p/cairn/4572/messages",
            "mode": "append",
            "kind": "resource",
            "summary": "Message appended"
        }]
    })
    .to_string();
    insert_pending_write_tool_event(&orch.db.local, Some(&report)).await;

    lifecycle::stop_session(&orch, "run-1").unwrap();
    lifecycle::stop_session(&orch, "run-1").unwrap();

    assert_eq!(
        common::scalar_i64_by_id(
            &orch.db.local,
            "SELECT COUNT(*) FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
            "run-1",
        )
        .await,
        1,
        "the settled resource effect must be reported exactly once"
    );
    let data = common::scalar_text_by_id(
        &orch.db.local,
        "SELECT data FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
        "run-1",
    )
    .await
    .unwrap();
    let event: serde_json::Value = serde_json::from_str(&data).unwrap();
    let result: serde_json::Value =
        serde_json::from_str(event["toolResult"].as_str().unwrap()).unwrap();
    assert_eq!(result["applied"][0]["kind"], "resource");
    assert!(result.get("commit").is_none());
    assert!(result["interrupted_after_effect"]
        .as_str()
        .unwrap()
        .contains("Do not retry"));
}

#[tokio::test]
async fn stop_session_preserves_settled_write_report() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;
    let report = serde_json::json!({
        "applied": [{"index": 0, "target": "file:notes.txt", "mode": "create"}],
        "commit": {"status": "committed", "sha": "3df112fac40918911eb39e49cc9b574f1ebcf6ad"}
    })
    .to_string();
    insert_pending_write_tool_event(&orch.db.local, Some(&report)).await;

    lifecycle::stop_session(&orch, "run-1").unwrap();

    let data = common::scalar_text_by_id(
        &orch.db.local,
        "SELECT data FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
        "run-1",
    )
    .await
    .expect("settled write must receive its authoritative result before interruption");
    let event: serde_json::Value = serde_json::from_str(&data).unwrap();
    assert_eq!(event["toolUseId"], "tool-write-1");
    assert_eq!(event["isError"], false);
    let result: serde_json::Value =
        serde_json::from_str(event["toolResult"].as_str().unwrap()).unwrap();
    assert_eq!(
        result["commit"]["sha"],
        "3df112fac40918911eb39e49cc9b574f1ebcf6ad"
    );
    assert!(result["interrupted_after_effect"]
        .as_str()
        .unwrap()
        .contains("Do not retry"));
}

#[tokio::test]
async fn stop_session_does_not_invent_a_pre_effect_write_result() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;
    insert_pending_write_tool_event(&orch.db.local, None).await;

    lifecycle::stop_session(&orch, "run-1").unwrap();

    assert_eq!(
        common::scalar_i64_by_id(
            &orch.db.local,
            "SELECT COUNT(*) FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
            "run-1",
        )
        .await,
        0,
        "a stop before apply must leave ordinary provider rejection semantics intact"
    );
}

async fn insert_pending_write_tool_event(db: &LocalDb, settled_report: Option<&str>) {
    let data = serde_json::json!({
        "eventType": "assistant",
        "sessionId": "session-1",
        "toolUses": [{
            "id": "tool-write-1",
            "name": "mcp__cairn__write",
            "input": { "changes": [{ "target": "file:notes.txt", "mode": "create",
                         "payload": { "content": "written" } }], "commit_msg": "write notes" }
        }],
        "isError": false
    })
    .to_string();
    db.write(|conn| {
        let data = data.clone();
        let settled_report = settled_report.map(str::to_string);
        Box::pin(async move {
            conn.execute(
                "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, turn_id)
                 VALUES ('event-write-1', 'run-1', 'session-1', 1, 1, 'assistant', ?1, 1, 'turn-1')",
                params![data],
            ).await?;
            if let Some(report) = settled_report {
                conn.execute(
                    "INSERT INTO write_replay_ledger(run_id, tool_use_id, report, created_at)
                     VALUES ('run-1', 'tool-write-1', ?1, 1)",
                    params![report],
                ).await?;
            }
            Ok(())
        })
    }).await.unwrap();
}

async fn insert_pending_run_tool_event(db: &LocalDb) {
    let data = serde_json::json!({
        "eventType": "assistant",
        "sessionId": "session-1",
        "parentToolUseId": null,
        "content": null,
        "thinking": null,
        "toolName": null,
        "toolInput": null,
        "toolUses": [
            {
                "id": "tool-run-1",
                "name": "mcp__cairn__run",
                "input": { "commands": [{ "command": "sleep 60" }] }
            },
            {
                "id": "tool-read-1",
                "name": "mcp__cairn__read",
                "input": { "paths": ["file:README.md"] }
            }
        ],
        "toolUseId": null,
        "toolResult": null,
        "isError": false,
        "raw": null
    })
    .to_string();
    db.write(|conn| {
        let data = data.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data, created_at, turn_id)
                 VALUES ('event-assistant-1', 'run-1', 'session-1', 1, 1, 'assistant', ?1, 1, 'turn-1')",
                params![data.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn insert_issue_job_run_turn(db: &LocalDb, project_id: &str) {
    let project_id = project_id.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Title', 'desc', 'active', 'active', 'none', 0, 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
                 VALUES ('job-1', ?1, 'issue-1', 'running', 'session-1', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'open', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, job_id, chat_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-1', ?1, 'job-1', NULL, 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 1, 'running', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// A user marking an issue terminal must not be refused while an agent runs:
/// the issue reaches its terminal state and the live run's turn is interrupted
/// so teardown can clean up without stranding the agent.
#[tokio::test]
async fn marking_issue_closed_stops_active_runs() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "close").await;
    insert_issue_job_run_turn(&orch.db.local, &project_id).await;

    cairn_core::issues::status::update_status(
        &orch,
        "issue-1",
        "closed",
        cairn_core::issues::status::ResolutionActor::User,
        cairn_core::issues::status::Confirmation::Given,
    )
    .await
    .unwrap();

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT state FROM turns WHERE id = ?1",
            "turn-1"
        )
        .await,
        Some("interrupted".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM issues WHERE id = ?1",
            "issue-1"
        )
        .await,
        Some("closed".to_string())
    );
}

async fn insert_issue_with_job(db: &LocalDb, project_id: &str, job_status: &str) {
    let project_id = project_id.to_string();
    let job_status = job_status.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let job_status = job_status.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Title', 'desc', 'active', 'active', 'none', 0, 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
                 VALUES ('job-1', ?1, 'issue-1', ?2, 1, 1)",
                params![project_id.as_str(), job_status.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// A recipe/coordinator terminal resolution must see a running job on the issue
/// as live work so it refuses rather than resolving out from under it.
#[tokio::test]
async fn live_work_includes_a_running_job() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "gate").await;
    insert_issue_with_job(&orch.db.local, &project_id, "running").await;

    let live_work = cairn_core::issues::status::live_work_for_issue(&orch, "issue-1")
        .await
        .unwrap();

    assert_eq!(
        live_work.len(),
        1,
        "a running job is live work: {live_work:?}"
    );
    assert!(live_work[0].is_started());
}

/// Once the issue's jobs are terminal, nothing is live and the resolution may
/// proceed with no confirmation.
#[tokio::test]
async fn live_work_is_empty_when_jobs_are_complete() {
    let (_temp, orch) = common::test_orchestrator().await;
    let project_id = common::create_project(&orch.db.local, "gate").await;
    insert_issue_with_job(&orch.db.local, &project_id, "complete").await;

    let live_work = cairn_core::issues::status::live_work_for_issue(&orch, "issue-1")
        .await
        .unwrap();

    assert!(
        live_work.is_empty(),
        "a complete job is not live work, got {live_work:?}"
    );
}

#[tokio::test]
async fn stop_session_reconciles_live_run_missing_from_process_map() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;

    lifecycle::stop_session(&orch, "run-1").unwrap();

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT state FROM turns WHERE id = ?1",
            "turn-1"
        )
        .await,
        Some("interrupted".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM runs WHERE id = ?1",
            "run-1"
        )
        .await,
        Some("exited".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-1"
        )
        .await,
        Some("user_stop".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM jobs WHERE id = ?1",
            "job-1"
        )
        .await,
        Some("running".to_string())
    );
}

#[tokio::test]
async fn stop_session_cancels_pending_turn_missing_from_process_map() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "pending").await;

    lifecycle::stop_session(&orch, "run-1").unwrap();

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT state FROM turns WHERE id = ?1",
            "turn-1"
        )
        .await,
        Some("cancelled".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM runs WHERE id = ?1",
            "run-1"
        )
        .await,
        Some("exited".to_string())
    );
}

/// Recovery terminalization is a durable boundary, not merely process cleanup.
/// Even if an unrelated panic poisoned the in-memory process registry, the
/// predecessor must not remain live/open and block its recovery successor.
#[tokio::test]
async fn kill_session_finalizes_after_process_registry_poisoning() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;

    std::thread::scope(|scope| {
        let processes = &orch.process_state.processes;
        let poisoner = scope.spawn(move || {
            let _guard = processes.lock().unwrap();
            panic!("poison process registry for regression coverage");
        });
        assert!(poisoner.join().is_err());
    });

    lifecycle::kill_session_with_reason(
        &orch,
        "run-1",
        "provider_silence_recovery",
        lifecycle::TerminalPolicy::Keep,
    )
    .unwrap();

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM runs WHERE id = ?1",
            "run-1"
        )
        .await
        .as_deref(),
        Some("crashed")
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-1"
        )
        .await
        .as_deref(),
        Some("provider_silence_recovery")
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT state FROM turns WHERE id = ?1",
            "turn-1"
        )
        .await
        .as_deref(),
        Some("interrupted")
    );

    let successor = cairn_core::internal::agent_process::process::RunHandle::new(
        std::sync::Arc::new(std::sync::Mutex::new(None)),
        std::sync::Arc::new(std::sync::Mutex::new(None)),
        Some("session-1".to_string()),
        Some("job-1".to_string()),
    );
    orch.process_state
        .processes
        .lock()
        .expect("terminalization must clear registry poison for the successor")
        .register("run-2".to_string(), successor);
    assert_eq!(orch.process_state.run_ids(), vec!["run-2".to_string()]);
}

#[tokio::test]
async fn watchdog_arm_failure_crashes_run_and_does_not_complete_turn() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;

    lifecycle::kill_session_with_reason(
        &orch,
        "run-1",
        "watchdog_arm_failed",
        lifecycle::TerminalPolicy::Keep,
    )
    .unwrap();

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM runs WHERE id = ?1",
            "run-1"
        )
        .await
        .as_deref(),
        Some("crashed")
    );
    assert_ne!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT state FROM turns WHERE id = ?1",
            "turn-1"
        )
        .await
        .as_deref(),
        Some("complete"),
        "an unowned provider turn must never be published as successful"
    );
}

#[tokio::test]
async fn stop_session_fails_pending_run_tool_result() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;
    insert_pending_run_tool_event(&orch.db.local).await;

    lifecycle::stop_session(&orch, "run-1").unwrap();

    let data = common::scalar_text_by_id(
        &orch.db.local,
        "SELECT data FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
        "run-1",
    )
    .await
    .expect("stop should synthesize a tool_result for the pending run call");
    let parsed: serde_json::Value = serde_json::from_str(&data).unwrap();
    assert_eq!(
        parsed.get("toolUseId").and_then(|value| value.as_str()),
        Some("tool-run-1")
    );
    assert_eq!(
        parsed.get("isError").and_then(|value| value.as_bool()),
        Some(true)
    );
    assert_eq!(
        parsed.get("toolResult").and_then(|value| value.as_str()),
        Some("Run interrupted by user stop.")
    );
    assert_eq!(
        common::scalar_i64_by_id(
            &orch.db.local,
            "SELECT COUNT(*) FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
            "run-1",
        )
        .await,
        1,
        "non-run pending tools should not get synthetic run failure results"
    );
}

/// A real OS process behind the `ChildProcess` seam, so a shutdown test observes
/// an actual process being stopped instead of a mock recording that it was asked
/// to be. The whole defect this covers is that dropping a handle does NOT stop
/// the process, which only a real one can demonstrate.
///
/// Unix-only, and not because of a porting gap: the stand-in process is `sleep`
/// and liveness is signal 0, neither of which Windows has. Compiling it there
/// anyway failed the whole `main` test target on `nix::sys`, which took every
/// Rust check placed on a Windows executor down with it (CAIRN-3448).
#[cfg(unix)]
struct RealChild(std::process::Child);

#[cfg(unix)]
impl cairn_core::internal::services::ChildProcess for RealChild {
    fn id(&self) -> u32 {
        self.0.id()
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
        self.0.try_wait()
    }

    fn kill(&mut self) -> std::io::Result<()> {
        self.0.kill()
    }
}

/// Whether `pid` still exists, via signal 0. A stopped-and-reaped child is gone;
/// the process this asks about was spawned by this test, so there is no window in
/// which another process could claim its id.
#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok()
}

/// CAIRN-3287: a runner must not exit while its agents keep running. The teardown
/// stops the real process, empties the registry, and records that WE stopped it
/// rather than leaving `crash` for a later startup sweep to invent.
#[cfg(unix)]
#[tokio::test]
async fn host_shutdown_stops_the_agent_process_it_spawned() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;

    let child = std::process::Command::new("sleep")
        .arg("120")
        .spawn()
        .expect("spawn a stand-in agent process");
    let pid = child.id();
    let handle = cairn_core::internal::agent_process::process::RunHandle::new(
        std::sync::Arc::new(std::sync::Mutex::new(Some(Box::new(RealChild(child))))),
        std::sync::Arc::new(std::sync::Mutex::new(None)),
        Some("session-1".to_string()),
        Some("job-1".to_string()),
    );
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run-1".to_string(), handle);
    assert!(process_alive(pid), "the stand-in agent should be running");

    let stops =
        lifecycle::stop_agents_for_host_shutdown(&orch, std::time::Duration::from_secs(10)).await;

    assert_eq!(
        stops,
        lifecycle::HostShutdownStops {
            stopped: 1,
            failed: 0,
            timed_out: 0
        }
    );
    assert!(
        !process_alive(pid),
        "the agent process must be gone before the runner exits, not reparented to launchd"
    );
    assert!(
        orch.process_state.run_ids().is_empty(),
        "a stopped agent must leave the registry"
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-1"
        )
        .await
        .as_deref(),
        Some(lifecycle::RUNNER_SHUTDOWN_EXIT_REASON),
        "the row must say we stopped it, not that it crashed"
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM runs WHERE id = ?1",
            "run-1"
        )
        .await
        .as_deref(),
        Some("exited")
    );
}

/// A stand-in agent that ignores SIGTERM, so `graceful_stop` spends its whole
/// grace period on it before falling back to SIGKILL. The ignored disposition
/// survives `exec`. Returns only once the trap is installed: a TERM that raced
/// ahead of it would kill the shell at once and leave no window to observe.
#[cfg(unix)]
fn spawn_term_ignoring_child() -> std::process::Child {
    use std::io::BufRead;

    let mut child = std::process::Command::new("sh")
        .args(["-c", "trap '' TERM; echo ready; exec sleep 30"])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn a TERM-ignoring stand-in agent");
    let mut line = String::new();
    std::io::BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    assert_eq!(line.trim(), "ready");
    child
}

#[cfg(unix)]
fn register_real_child(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    run_id: &str,
    child: std::process::Child,
) {
    let handle = cairn_core::internal::agent_process::process::RunHandle::new(
        std::sync::Arc::new(std::sync::Mutex::new(Some(Box::new(RealChild(child))))),
        std::sync::Arc::new(std::sync::Mutex::new(None)),
        Some("session-1".to_string()),
        Some("job-1".to_string()),
    );
    orch.process_state
        .register_process(run_id.to_string(), handle)
        .unwrap();
}

/// Starts killing `run_id` on the blocking pool and returns once its handle has
/// left the registry while the kill is still waiting on the child. Each lookup
/// takes the registry lock, so reaching the handle's absence quickly is itself
/// the proof that the kill is not holding that lock across the stop.
#[cfg(unix)]
async fn kill_until_inside_graceful_stop(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    run_id: &str,
) -> tokio::task::JoinHandle<Result<(), String>> {
    let kill = {
        let orch = orch.clone();
        let run_id = run_id.to_string();
        tokio::task::spawn_blocking(move || {
            lifecycle::kill_session_with_reason(
                &orch,
                &run_id,
                "warm_evict",
                lifecycle::TerminalPolicy::Finalize,
            )
        })
    };
    let started = std::time::Instant::now();
    while orch.process_state.has_process(run_id) != Some(false) {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "the killed run never left the registry"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "the registry was unreadable for {elapsed:?}: the kill held its lock while stopping the child"
    );
    assert!(
        !kill.is_finished(),
        "the kill should still be waiting out the child's ignored SIGTERM"
    );
    kill
}

/// Reaps `pid`, a child of this test process, and returns its wait status, or
/// `None` if it is still running after `within`. Signal 0 cannot answer this:
/// a SIGKILLed child nobody has reaped is a zombie that still accepts it.
#[cfg(unix)]
fn reap_within(pid: u32, within: std::time::Duration) -> Option<libc::c_int> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let mut status = 0;
        // SAFETY: `status` is a valid out-pointer for the duration of the call.
        let reaped = unsafe { libc::waitpid(pid as libc::pid_t, &mut status, libc::WNOHANG) };
        if reaped == pid as libc::pid_t {
            return Some(status);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(unix)]
fn assert_killed_by_sigkill(pid: u32) {
    let status = reap_within(pid, std::time::Duration::from_secs(2))
        .expect("the kill must not return while its child is still running");
    assert!(
        libc::WIFSIGNALED(status) && libc::WTERMSIG(status) == libc::SIGKILL,
        "the TERM-ignoring child should have been stopped by the SIGKILL fallback, got wait status {status}"
    );
}

/// cairn/5789: every session's reader takes the process registry lock per
/// stdout line, so a kill that holds it across `graceful_stop` (up to 3 s for a
/// child that ignores SIGTERM) stalls every session on the host. Another
/// session's reader must get through while a run is being killed, and the kill
/// must still return only once its child is dead.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_session_reader_can_take_the_registry_while_another_run_is_being_killed() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;

    let child = spawn_term_ignoring_child();
    let pid = child.id();
    register_real_child(&orch, "run-1", child);
    orch.process_state
        .register_process(
            "run-2".to_string(),
            cairn_core::internal::agent_process::process::RunHandle::test_handle(
                Some("session-2"),
                Some("job-2"),
            ),
        )
        .unwrap();
    assert!(orch.process_state.begin_turn("run-2", "turn-2"));

    let kill = kill_until_inside_graceful_stop(&orch, "run-1").await;

    let started = std::time::Instant::now();
    let turn = orch.process_state.get_current_turn_id("run-2");
    let waited = started.elapsed();
    assert_eq!(turn.as_deref(), Some("turn-2"));
    assert!(
        waited < std::time::Duration::from_millis(250),
        "another session's reader waited {waited:?} on the registry during a kill"
    );
    assert!(
        !kill.is_finished(),
        "the reader's lookup must have overlapped the stop"
    );

    kill.await.unwrap().unwrap();
    assert_killed_by_sigkill(pid);
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT status FROM runs WHERE id = ?1",
            "run-1"
        )
        .await
        .as_deref(),
        Some("exited")
    );
}

/// A kill owns only the handle it removed. A process registered under the same
/// run id after that removal, while the old child is still being stopped, is a
/// successor the old kill must leave running and registered.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_kill_never_stops_a_process_registered_after_its_removal() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local, "running").await;

    let predecessor = spawn_term_ignoring_child();
    let predecessor_pid = predecessor.id();
    register_real_child(&orch, "run-1", predecessor);

    let kill = kill_until_inside_graceful_stop(&orch, "run-1").await;

    let successor = std::process::Command::new("sleep")
        .arg("120")
        .spawn()
        .expect("spawn a successor agent process");
    let successor_pid = successor.id();
    register_real_child(&orch, "run-1", successor);

    kill.await.unwrap().unwrap();
    assert_killed_by_sigkill(predecessor_pid);
    assert_eq!(orch.process_state.has_process("run-1"), Some(true));
    assert!(
        process_alive(successor_pid),
        "the old kill must not stop the process registered after it removed its handle"
    );

    assert!(orch.process_state.stop_and_remove("run-1"));
    assert!(!process_alive(successor_pid));
}

/// The common case — an idle runner restarting — must cost nothing and report
/// nothing, so the shutdown log stays quiet unless agents were actually stopped.
#[tokio::test]
async fn host_shutdown_with_no_agents_is_a_silent_no_op() {
    let (_temp, orch) = common::test_orchestrator().await;

    let stops =
        lifecycle::stop_agents_for_host_shutdown(&orch, std::time::Duration::from_secs(1)).await;

    assert_eq!(stops, lifecycle::HostShutdownStops::default());
}
