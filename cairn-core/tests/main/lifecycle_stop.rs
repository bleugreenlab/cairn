use crate::common;

use cairn_core::internal::orchestrator::lifecycle;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;

async fn insert_project_job_run_turn(db: &LocalDb, turn_state: &str) {
    let project_id = common::create_project(db, "STOP").await;
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
    let project_id = common::create_project(&orch.db.local, "CLOSE").await;
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
    let project_id = common::create_project(&orch.db.local, "GATE").await;
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
    let project_id = common::create_project(&orch.db.local, "GATE").await;
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

/// The common case — an idle runner restarting — must cost nothing and report
/// nothing, so the shutdown log stays quiet unless agents were actually stopped.
#[tokio::test]
async fn host_shutdown_with_no_agents_is_a_silent_no_op() {
    let (_temp, orch) = common::test_orchestrator().await;

    let stops =
        lifecycle::stop_agents_for_host_shutdown(&orch, std::time::Duration::from_secs(1)).await;

    assert_eq!(stops, lifecycle::HostShutdownStops::default());
}
