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

/// cairn/4634: a deliberate kill never records the turn it cut off as a
/// completion, and never as a crash. Each exit reason settles a turn still
/// running by who killed it.
#[tokio::test]
async fn a_kill_settles_the_turn_it_cut_off_by_its_exit_reason() {
    for (exit_reason, expected) in [
        ("provider_silence_recovery", "interrupted/watchdog_recovery"),
        ("user_stop", "interrupted/user_stop"),
        ("provider_change", "interrupted/user_stop"),
        ("watchdog_arm_failed", "interrupted/crash"),
    ] {
        let (_temp, orch) = common::test_orchestrator().await;
        insert_project_job_run_turn(&orch.db.local, "running").await;
        lifecycle::kill_session_with_reason(
            &orch,
            "run-1",
            exit_reason,
            lifecycle::TerminalPolicy::Keep,
        )
        .unwrap();
        assert_eq!(
            common::scalar_text_by_id(
                &orch.db.local,
                "SELECT state || '/' || COALESCE(end_reason, '') FROM turns WHERE id = ?1",
                "turn-1"
            )
            .await
            .as_deref(),
            Some(expected),
            "{exit_reason}"
        );
    }
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

/// The common case — an idle runner restarting — must cost nothing and report
/// nothing, so the shutdown log stays quiet unless agents were actually stopped.
#[tokio::test]
async fn host_shutdown_with_no_agents_is_a_silent_no_op() {
    let (_temp, orch) = common::test_orchestrator().await;

    let stops =
        lifecycle::stop_agents_for_host_shutdown(&orch, std::time::Duration::from_secs(1)).await;

    assert_eq!(stops, lifecycle::HostShutdownStops::default());
}
