use super::warm_completion_tests::{register_warm_process, test_orchestrator};
use super::watchdog_recovery::{
    current_recovery_target, finish_settled_recovery, has_pending_resume_trigger,
    recover_provider_watchdog, still_owns_recovery_target, ProviderWatchdogRecovery,
    ALREADY_TERMINAL_RECONCILED_REASON, TARGET_TURN_COMPLETED_REASON,
};
use crate::agent_process::process::RunHandle;
use crate::orchestrator::Orchestrator;
use crate::runs::watchdog_ledger::{
    append_watchdog_lifecycle, arm_watchdog, claim_watchdog_recovery, get_watchdog_lease,
    list_watchdog_lifecycle, NewWatchdogLease, NewWatchdogLifecycle, WatchdogIdentity,
    WatchdogLeaseState, WatchdogLifecycleKind, WatchdogPhase,
};
use crate::storage::{migrated_test_db, RowExt};
use std::sync::{Arc, Mutex};

#[tokio::test(flavor = "multi_thread")]
async fn post_tool_hang_interrupts_only_the_lease_and_rearms_the_goal() {
    let db = migrated_test_db("post-tool-goal-continuation").await;
    db.execute_script(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO jobs (id, project_id, status, current_session_id, current_turn_id, created_at, updated_at)
           VALUES ('job', 'project', 'running', 'session', 'turn', 1, 1);
         INSERT INTO sessions (id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
           VALUES ('session', 'job', 'codex', 'native-thread', 'open', 1, 1, 1);
         INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
           VALUES ('run', 'job', 'session', 'live', 1, 1);
         INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, start_reason, created_at, updated_at)
           VALUES ('turn', 'job', 'session', 'run', 1, 'running', 'initial', 1, 1);
         PRAGMA foreign_keys = ON;",
    ).await.unwrap();
    let identity = WatchdogIdentity {
        run_id: "run".into(),
        session_id: "session".into(),
        provider_turn_id: "provider-turn".into(),
        generation: "generation".into(),
    };
    arm_watchdog(
        &db,
        NewWatchdogLease {
            identity: identity.clone(),
            runner_boot_id: "boot".into(),
            phase: WatchdogPhase::PostToolContinuation,
            phase_deadline_at: 150,
            now: 100,
        },
        "post-tool-silence".into(),
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);
    let (stdin, requests, provider_turn) =
        crate::backends::codex::scripted_active_stdin_for_test("native-thread", "provider-turn");
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(stdin))),
        Some("session".into()),
        Some("job".into()),
    );
    handle.backend = Some("codex".into());
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run".into(), handle);
    orch.process_state.set_current_turn_id("run", Some("turn"));

    *provider_turn.lock().unwrap() = Some("newer-provider-turn".into());
    assert_eq!(
        recover_provider_watchdog(&orch, &identity, 300).unwrap(),
        ProviderWatchdogRecovery::Recovered {
            reason: "post_tool_interrupt_target_advanced".into(),
            successor: None,
        }
    );
    assert!(requests.lock().unwrap().is_empty());

    let retry = WatchdogIdentity {
        generation: "generation-retry".into(),
        ..identity.clone()
    };
    arm_watchdog(
        &orch.db.local,
        NewWatchdogLease {
            identity: retry.clone(),
            runner_boot_id: "boot".into(),
            phase: WatchdogPhase::PostToolContinuation,
            phase_deadline_at: 350,
            now: 310,
        },
        "post-tool-silence-retry".into(),
    )
    .await
    .unwrap();
    let (process_stdin, process_turn, _fixture) =
        crate::backends::codex::process_active_stdin_for_test(
            "native-thread",
            "provider-turn",
            Some("aborted"),
        );
    let mut process_handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(process_stdin))),
        Some("session".into()),
        Some("job".into()),
    );
    process_handle.backend = Some("codex".into());
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run".into(), process_handle);
    let recovery = recover_provider_watchdog(&orch, &retry, 400);
    let lifecycle = list_watchdog_lifecycle(&orch.db.local, &retry)
        .await
        .unwrap();
    assert_eq!(
        recovery,
        Ok(ProviderWatchdogRecovery::Recovered {
            reason: "post_tool_goal_continuation_rearmed".into(),
            successor: None,
        }),
        "requests: {:?}; lifecycle: {:?}",
        requests.lock().unwrap(),
        lifecycle
    );

    assert_eq!(
        orch.db
            .local
            .query_opt(
                "SELECT status, backend_id FROM sessions WHERE id = 'session'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap(),
        Some(("open".into(), Some("native-thread".into())))
    );
    assert_eq!(*process_turn.lock().unwrap(), None);
    assert_eq!(
        list_watchdog_lifecycle(&orch.db.local, &retry)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == WatchdogLifecycleKind::SuccessorReserved)
            .count(),
        0,
        "provider-goal recovery must not spend synthetic-successor budget"
    );

    let completion_race = WatchdogIdentity {
        generation: "generation-completion-race".into(),
        ..identity
    };
    arm_watchdog(
        &orch.db.local,
        NewWatchdogLease {
            identity: completion_race.clone(),
            runner_boot_id: "boot".into(),
            phase: WatchdogPhase::PostToolContinuation,
            phase_deadline_at: 450,
            now: 410,
        },
        "post-tool-completion-race".into(),
    )
    .await
    .unwrap();
    let (completion_stdin, completion_turn, _completion_fixture) =
        crate::backends::codex::process_active_stdin_for_test(
            "native-thread",
            "provider-turn",
            Some("completed"),
        );
    let mut completion_handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(completion_stdin))),
        Some("session".into()),
        Some("job".into()),
    );
    completion_handle.backend = Some("codex".into());
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run".into(), completion_handle);
    assert_eq!(
        recover_provider_watchdog(&orch, &completion_race, 500).unwrap(),
        ProviderWatchdogRecovery::Recovered {
            reason: "post_tool_goal_continuation_rearmed".into(),
            successor: None,
        }
    );
    assert_eq!(*completion_turn.lock().unwrap(), None);
    assert_eq!(session_status(&orch).await, "open");
    assert_eq!(
        list_watchdog_lifecycle(&orch.db.local, &completion_race)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == WatchdogLifecycleKind::SuccessorReserved)
            .count(),
        0
    );
}

/// The interrupt is acknowledged, so the only unknown left is the goal's
/// status. A `thread/goal/get` that cannot answer is uncertainty about that
/// status, not evidence the provider thread is dead: recovery must preserve the
/// live process and session and spend no automatic recovery budget.
#[tokio::test(flavor = "multi_thread")]
async fn failed_goal_read_after_acknowledged_interrupt_preserves_the_session() {
    let db = migrated_test_db("post-tool-goal-read-failure").await;
    db.execute_script(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO jobs (id, project_id, status, current_session_id, current_turn_id, created_at, updated_at)
           VALUES ('job', 'project', 'running', 'session', 'turn', 1, 1);
         INSERT INTO sessions (id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
           VALUES ('session', 'job', 'codex', 'native-thread', 'open', 1, 1, 1);
         INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
           VALUES ('run', 'job', 'session', 'live', 1, 1);
         INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, start_reason, created_at, updated_at)
           VALUES ('turn', 'job', 'session', 'run', 1, 'running', 'initial', 1, 1);
         PRAGMA foreign_keys = ON;",
    )
    .await
    .unwrap();
    let identity = WatchdogIdentity {
        run_id: "run".into(),
        session_id: "session".into(),
        provider_turn_id: "provider-turn".into(),
        generation: "generation".into(),
    };
    arm_watchdog(
        &db,
        NewWatchdogLease {
            identity: identity.clone(),
            runner_boot_id: "boot".into(),
            phase: WatchdogPhase::PostToolContinuation,
            phase_deadline_at: 150,
            now: 100,
        },
        "post-tool-silence".into(),
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);

    // The provider acknowledges the interrupt by ending the exact turn, then
    // answers the goal read with a JSON-RPC error.
    let provider_turn = Arc::new(Mutex::new(Some("provider-turn".to_string())));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed_turn = provider_turn.clone();
    let client = Arc::new(
        cairn_models::codex::app_server::AppServerClient::for_test_responding(
            move |request| match request["method"].as_str() {
                Some("turn/interrupt") => {
                    *observed_turn.lock().unwrap() = None;
                    Ok(serde_json::json!({ "result": {} }))
                }
                method => Ok(serde_json::json!({ "error": {
                    "code": -32601,
                    "message": format!("fixture has no reply for {}", method.unwrap_or("?")),
                } })),
            },
            requests.clone(),
        ),
    );
    let control = cairn_models::session::codex::CodexControl::new(
        client,
        "native-thread".to_string(),
        "/tmp/project".to_string(),
        None,
        None,
        None,
        provider_turn.clone(),
    );
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(Box::new(control)))),
        Some("session".into()),
        Some("job".into()),
    );
    handle.backend = Some("codex".into());
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run".into(), handle);
    orch.process_state.set_current_turn_id("run", Some("turn"));

    let recovery = recover_provider_watchdog(&orch, &identity, 300);
    let lifecycle = list_watchdog_lifecycle(&orch.db.local, &identity)
        .await
        .unwrap();
    assert_eq!(
        recovery,
        Ok(ProviderWatchdogRecovery::PreservedUncertain {
            reason: "post_tool_goal_reactivation_failed".into(),
        }),
        "requests: {:?}; lifecycle: {:?}",
        requests.lock().unwrap(),
        lifecycle
    );
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request["method"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["turn/interrupt", "thread/goal/get"]
    );
    assert_eq!(*provider_turn.lock().unwrap(), None);

    let reasons = lifecycle
        .iter()
        .map(|event| (event.kind, event.reason.as_deref()))
        .collect::<Vec<_>>();
    assert!(
        reasons.contains(&(
            WatchdogLifecycleKind::TerminalizationSucceeded,
            Some("post_tool_interrupt_acknowledged")
        )),
        "{lifecycle:?}"
    );
    let failure = lifecycle
        .iter()
        .find(|event| event.reason.as_deref() == Some("post_tool_goal_reactivation_failed"))
        .expect("goal read failure is recorded");
    assert_eq!(failure.kind, WatchdogLifecycleKind::TerminalizationFailed);
    assert!(
        failure
            .details_json
            .as_deref()
            .and_then(|details| serde_json::from_str::<serde_json::Value>(details).ok())
            .and_then(|details| details["error"].as_str().map(str::to_owned))
            .is_some_and(|error| error.contains("thread/goal/get")),
        "the provider error stays on the record: {failure:?}"
    );

    // The automatic recovery budget is the job's count of reserved successors.
    let reservations = orch
        .db
        .local
        .query_opt(
            "SELECT COUNT(*) FROM codex_watchdog_lifecycle lifecycle
             JOIN runs ON runs.id = lifecycle.run_id
             WHERE runs.job_id = 'job' AND lifecycle.kind = 'successor_reserved'",
            (),
            |row| Ok(row.get::<i64>(0)?),
        )
        .await
        .unwrap();
    assert_eq!(reservations, Some(0), "{lifecycle:?}");
    assert!(orch.process_state.has_process("run") == Some(true));
    assert_eq!(session_status(&orch).await, "open");
    assert_eq!(
        orch.db
            .local
            .query_opt("SELECT status FROM runs WHERE id = 'run'", (), |row| row
                .text(0))
            .await
            .unwrap(),
        Some("live".into())
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn repeated_process_interrupt_timeouts_preserve_one_native_session() {
    let db = migrated_test_db("post-tool-interrupt-timeout-preserves-thread").await;
    db.execute_script(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO jobs (id, project_id, status, current_session_id, current_turn_id, created_at, updated_at)
           VALUES ('job', 'project', 'running', 'session', 'turn', 1, 1);
         INSERT INTO sessions (id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
           VALUES ('session', 'job', 'codex', 'native-thread', 'open', 1, 1, 1);
         INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
           VALUES ('run', 'job', 'session', 'live', 1, 1);
         INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, start_reason, created_at, updated_at)
           VALUES ('turn', 'job', 'session', 'run', 1, 'running', 'initial', 1, 1);
         PRAGMA foreign_keys = ON;",
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);
    let (stdin, provider_turn, _fixture) = crate::backends::codex::process_active_stdin_for_test(
        "native-thread",
        "provider-turn",
        None,
    );
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(stdin))),
        Some("session".into()),
        Some("job".into()),
    );
    handle.backend = Some("codex".into());
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run".into(), handle);
    orch.process_state.set_current_turn_id("run", Some("turn"));

    for attempt in 0..3 {
        let identity = WatchdogIdentity {
            run_id: "run".into(),
            session_id: "session".into(),
            provider_turn_id: "provider-turn".into(),
            generation: format!("generation-{attempt}"),
        };
        arm_watchdog(
            &orch.db.local,
            NewWatchdogLease {
                identity: identity.clone(),
                runner_boot_id: "boot".into(),
                phase: WatchdogPhase::PostToolContinuation,
                phase_deadline_at: 150 + attempt,
                now: 100 + attempt,
            },
            format!("post-tool-silence-{attempt}"),
        )
        .await
        .unwrap();
        assert_eq!(
            recover_provider_watchdog(&orch, &identity, 300 + attempt).unwrap(),
            ProviderWatchdogRecovery::PreservedUncertain {
                reason: "post_tool_interrupt_ack_timeout".into(),
            }
        );
        assert_eq!(*provider_turn.lock().unwrap(), Some("provider-turn".into()));
        assert!(orch.process_state.has_process("run") == Some(true));
    }

    assert_eq!(
        orch.db
            .local
            .query_opt(
                "SELECT COUNT(*), MIN(status), MIN(backend_id), MAX(backend_id) FROM sessions WHERE job_id = 'job'",
                (),
                |row| Ok((row.get::<i64>(0)?, row.text(1)?, row.opt_text(2)?, row.opt_text(3)?)),
            )
            .await
            .unwrap(),
        Some((1, "open".into(), Some("native-thread".into()), Some("native-thread".into())))
    );
    let reservations = orch
        .db
        .local
        .query_opt(
            "SELECT COUNT(*) FROM codex_watchdog_lifecycle WHERE kind = 'successor_reserved'",
            (),
            |row| Ok(row.get::<i64>(0)?),
        )
        .await
        .unwrap();
    assert_eq!(reservations, Some(0));

    let delivery_failure = WatchdogIdentity {
        run_id: "run".into(),
        session_id: "session".into(),
        provider_turn_id: "provider-turn".into(),
        generation: "generation-delivery-failure".into(),
    };
    arm_watchdog(
        &orch.db.local,
        NewWatchdogLease {
            identity: delivery_failure.clone(),
            runner_boot_id: "boot".into(),
            phase: WatchdogPhase::PostToolContinuation,
            phase_deadline_at: 500,
            now: 450,
        },
        "post-tool-delivery-failure".into(),
    )
    .await
    .unwrap();
    let failing_stdin =
        crate::backends::codex::failing_interrupt_stdin_for_test("native-thread", "provider-turn");
    let mut failing_handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(failing_stdin))),
        Some("session".into()),
        Some("job".into()),
    );
    failing_handle.backend = Some("codex".into());
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run".into(), failing_handle);
    assert_eq!(
        recover_provider_watchdog(&orch, &delivery_failure, 600).unwrap(),
        ProviderWatchdogRecovery::PreservedUncertain {
            reason: "post_tool_interrupt_delivery_failed".into(),
        }
    );
    assert_eq!(session_status(&orch).await, "open");
    assert!(orch.process_state.has_process("run") == Some(true));
}

async fn active_watchdog_fixture(name: &str) -> (Orchestrator, WatchdogIdentity) {
    let db = migrated_test_db(name).await;
    db.execute_script(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO jobs (id, project_id, status, current_session_id, current_turn_id, created_at, updated_at)
           VALUES ('job', 'project', 'running', 'session', 'turn', 1, 1);
         INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
           VALUES ('session', 'job', 'codex', 'open', 1, 1, 1);
         INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
           VALUES ('run', 'job', 'session', 'live', 1, 1);
         INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, start_reason, created_at, updated_at)
           VALUES ('turn', 'job', 'session', 'run', 1, 'running', 'initial', 1, 1);
         PRAGMA foreign_keys = ON;",
    )
    .await
    .unwrap();
    let identity = WatchdogIdentity {
        run_id: "run".into(),
        session_id: "session".into(),
        provider_turn_id: "provider-turn".into(),
        generation: "generation".into(),
    };
    arm_watchdog(
        &db,
        NewWatchdogLease {
            identity: identity.clone(),
            runner_boot_id: "boot".into(),
            phase: WatchdogPhase::ProviderProgress,
            phase_deadline_at: 150,
            now: 100,
        },
        "armed".into(),
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);
    register_warm_process(&orch, "run", Some("job"));
    orch.process_state.set_current_turn_id("run", Some("turn"));
    (orch, identity)
}

#[tokio::test(flavor = "multi_thread")]
async fn completed_job_reconciles_without_reserving_even_when_budget_is_exhausted() {
    let (orch, identity) = active_watchdog_fixture("watchdog-complete-no-reservation").await;
    orch.db
        .local
        .execute_script(
            "UPDATE turns SET state = 'complete', ended_at = 250 WHERE id = 'turn';
             UPDATE jobs SET status = 'complete' WHERE id = 'job';",
        )
        .await
        .unwrap();
    for index in 0..3 {
        append_watchdog_lifecycle(
            &orch.db.local,
            NewWatchdogLifecycle {
                id: format!("spent-reservation-{index}"),
                identity: identity.clone(),
                kind: WatchdogLifecycleKind::SuccessorReserved,
                phase: None,
                reason: None,
                details: None,
                successor_run_id: None,
                successor_turn_id: None,
                created_at: 200 + index,
            },
        )
        .await
        .unwrap();
    }

    // The turn completed on a process this host still holds, so recovery stands
    // down without touching it.
    let recovery = recover_provider_watchdog(&orch, &identity, 300).unwrap();
    assert_eq!(
        recovery,
        ProviderWatchdogRecovery::Recovered {
            reason: TARGET_TURN_COMPLETED_REASON.to_string(),
            successor: None,
        }
    );
    assert_eq!(orch.process_state.has_process("run"), Some(true));
    assert_eq!(
        list_watchdog_lifecycle(&orch.db.local, &identity)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == WatchdogLifecycleKind::SuccessorReserved)
            .count(),
        3
    );

    // CAIRN-4618, through the whole of `recover_provider_watchdog`: a turn that
    // had already reached its terminal state, noticed late, costs this node
    // nothing. The session it was holding is still open and still the job's
    // current one. This path used to close it as
    // `provider_silence_already_terminal_reconciled` and flag the job to start a
    // replacement conversation on its next turn — 199 of the 1,181 native-thread
    // replacements on the reference install.
    assert_eq!(session_status(&orch).await, "open");
    assert_eq!(
        orch.db
            .local
            .query_opt(
                "SELECT COUNT(*), MIN(current_session_id) FROM jobs WHERE id = 'job'",
                (),
                |row| Ok((row.get::<i64>(0)?, row.text(1)?)),
            )
            .await
            .unwrap(),
        Some((1, "session".into()))
    );
}

async fn session_status(orch: &Orchestrator) -> String {
    orch.db
        .local
        .query_opt(
            "SELECT status FROM sessions WHERE id = 'session'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_reservation_precedes_terminalization_and_preserves_resumability() {
    let (orch, identity) = active_watchdog_fixture("watchdog-budget-before-kill").await;
    for index in 0..3 {
        append_watchdog_lifecycle(
            &orch.db.local,
            NewWatchdogLifecycle {
                id: format!("reservation-{index}"),
                identity: identity.clone(),
                kind: WatchdogLifecycleKind::SuccessorReserved,
                phase: None,
                reason: None,
                details: None,
                successor_run_id: None,
                successor_turn_id: None,
                created_at: 200 + index,
            },
        )
        .await
        .unwrap();
    }

    let error = recover_provider_watchdog(&orch, &identity, 300).unwrap_err();
    assert!(error.contains("budget exhausted"));
    assert_eq!(session_status(&orch).await, "open");
    assert!(orch.process_state.has_process("run") == Some(true));
}

#[tokio::test(flavor = "multi_thread")]
async fn undetermined_reservation_is_visible_and_preserves_the_session() {
    let (orch, identity) = active_watchdog_fixture("watchdog-reservation-error").await;
    orch.db
        .local
        .execute_batch(
            "CREATE TRIGGER reject_watchdog_reservation
             BEFORE INSERT ON codex_watchdog_lifecycle
             WHEN NEW.kind = 'successor_reserved'
             BEGIN SELECT RAISE(ABORT, 'injected reservation failure'); END;",
        )
        .await
        .unwrap();

    let error = recover_provider_watchdog(&orch, &identity, 300).unwrap_err();
    assert!(error.contains("failed to reserve watchdog successor"));
    assert_eq!(session_status(&orch).await, "open");
    assert!(orch.process_state.has_process("run") == Some(true));
    let events = list_watchdog_lifecycle(&orch.db.local, &identity)
        .await
        .unwrap();
    assert!(events.iter().any(|event| {
        event.kind == WatchdogLifecycleKind::SuccessorFailed
            && event.reason.as_deref() == Some("provider_silence_recovery_reservation_failed")
    }));
    assert!(!events.iter().any(|event| {
        event.reason.as_deref() == Some("provider_silence_recovery_budget_exhausted")
    }));
}

#[tokio::test(flavor = "multi_thread")]
async fn complete_turn_with_open_session_and_blocked_job_reconciles_once() {
    let db = migrated_test_db("terminal-watchdog-ownership").await;
    db.execute_script(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO jobs (id, project_id, status, current_session_id, current_turn_id, created_at, updated_at)
           VALUES ('job', 'project', 'blocked', 'session', 'turn', 1, 1);
         INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
           VALUES ('session', 'job', 'codex', 'open', 1, 1, 1);
         INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
           VALUES ('run', 'job', 'session', 'live', 1, 1);
         INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, start_reason, created_at, ended_at, updated_at)
           VALUES ('turn', 'job', 'session', 'run', 1, 'complete', 'initial', 1, 2, 2);
         PRAGMA foreign_keys = ON;",
    )
    .await
    .unwrap();
    let identity = WatchdogIdentity {
        run_id: "run".into(),
        session_id: "session".into(),
        provider_turn_id: "provider-turn".into(),
        generation: "generation".into(),
    };
    arm_watchdog(
        &db,
        NewWatchdogLease {
            identity: identity.clone(),
            runner_boot_id: "boot".into(),
            phase: WatchdogPhase::PostToolContinuation,
            phase_deadline_at: 150,
            now: 100,
        },
        "silent-provider".into(),
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);
    assert!(!has_pending_resume_trigger(orch.db.local.clone(), "job").unwrap());
    orch.db
        .local
        .execute(
            "INSERT INTO queued_messages (id, job_id, content, delivery, created_at) VALUES ('queued', 'job', 'resume me', 'queue', 1)",
            (),
        )
        .await
        .unwrap();
    assert!(has_pending_resume_trigger(orch.db.local.clone(), "job").unwrap());
    orch.db
        .local
        .execute("DELETE FROM queued_messages WHERE id = 'queued'", ())
        .await
        .unwrap();
    register_warm_process(&orch, "run", Some("job"));
    orch.process_state.set_current_turn_id("run", Some("turn"));
    let db = orch.db.local.clone();
    assert!(claim_watchdog_recovery(&db, &identity, 300).await.unwrap());

    let target = current_recovery_target(db.clone(), &identity, 300)
        .unwrap()
        .expect("the terminal turn still owns the open session and blocked job");
    assert_eq!(target.turn_state, "complete");
    assert_eq!(target.job_status, "blocked");

    // Model an ordinary continuation winning the launch lock after target
    // validation. Neither settled closure nor successor fencing may touch the
    // newly advanced owner.
    db.execute_script(
        "INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, start_reason, created_at, updated_at)
           VALUES ('other-turn', 'job', 'session', 'run', 2, 'running', 'follow_up', 3, 3);
         UPDATE jobs SET current_turn_id = 'other-turn' WHERE id = 'job';",
    )
    .await
    .unwrap();
    assert!(!finish_settled_recovery(db.clone(), &identity, &target, 300, 301).unwrap());
    assert!(!still_owns_recovery_target(db.clone(), &identity, &target, 300).unwrap());
    assert_eq!(
        db.query_opt(
            "SELECT status FROM sessions WHERE id = 'session'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .as_deref(),
        Some("open")
    );
    db.execute(
        "UPDATE jobs SET current_turn_id = 'turn' WHERE id = 'job'",
        (),
    )
    .await
    .unwrap();

    // With the target back under this recovery's ownership, the nonterminal path
    // may proceed — and it proceeds onto the SAME session. Ownership is all this
    // predicate decides now; it used to also set `needs_fresh_session`, which
    // fenced the successor onto a replacement conversation on nothing more than
    // provider silence (CAIRN-4618).
    assert!(still_owns_recovery_target(db.clone(), &identity, &target, 300).unwrap());

    // Process finalization/recompute can settle the job after the recovery claim.
    db.execute("UPDATE jobs SET status = 'complete' WHERE id = 'job'", ())
        .await
        .unwrap();
    assert!(finish_settled_recovery(db.clone(), &identity, &target, 300, 301).unwrap());

    let lease = get_watchdog_lease(&db, "run", "session", "provider-turn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.state, WatchdogLeaseState::Terminalized);
    assert_eq!(
        lease.terminal_reason.as_deref(),
        Some(ALREADY_TERMINAL_RECONCILED_REASON)
    );
    // The reconciliation publishes the terminal turn and leaves the conversation
    // alone. A turn that finished normally, noticed late, is not a reason to
    // discard a resumable provider thread.
    assert_eq!(
        db.query_opt(
            "SELECT status FROM sessions WHERE id = 'session'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .as_deref(),
        Some("open")
    );
    assert!(!claim_watchdog_recovery(&db, &identity, 700).await.unwrap());
}

/// A Codex run parked warm, whose control's provider-turn slot holds
/// `serving`. `None` is exactly how the reader leaves it on `turn/completed`.
fn register_warm_codex_process(
    orch: &Orchestrator,
    serving: Option<&str>,
) -> Arc<Mutex<Vec<serde_json::Value>>> {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(
        cairn_models::codex::app_server::AppServerClient::for_test_responding(
            |request| {
                Ok(serde_json::json!({ "error": {
                    "code": -32601,
                    "message": format!("fixture has no reply for {}", request["method"]),
                } }))
            },
            requests.clone(),
        ),
    );
    let control = cairn_models::session::codex::CodexControl::new(
        client,
        "native-thread".to_string(),
        "/tmp/project".to_string(),
        None,
        None,
        None,
        Arc::new(Mutex::new(serving.map(str::to_string))),
    );
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(Box::new(control)))),
        Some("session".into()),
        Some("job".into()),
    );
    handle.backend = Some("codex".into());
    handle.transition_to_warm();
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register("run".into(), handle);
    requests
}

/// The 11-07 specimen (cairn/6410), in its production row shape. A lease whose
/// worker had stopped tending it passed its deadline while the turn was still
/// working; the turn then completed, the run went warm and the job carried on,
/// and only then did the periodic reconciler claim the lease. Recovery must
/// read the turn's state at the moment it would act and stand down: nothing
/// killed, no `crashed` row, no successor budget spent, and the lease closed
/// with a typed no-op reason.
#[tokio::test(flavor = "multi_thread")]
async fn lease_claimed_after_its_turn_completed_kills_nothing() {
    use crate::runs::watchdog_reconciler::{
        reconcile_watchdog_leases, ReconcileScope, DEFAULT_HEARTBEAT_GRACE_SECS,
    };

    let db = migrated_test_db("watchdog-claim-after-completion").await;
    db.execute_script(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO jobs (id, project_id, status, current_session_id, current_turn_id, created_at, updated_at)
           VALUES ('job', 'project', 'running', 'session', 'turn', 1, 1);
         INSERT INTO sessions (id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
           VALUES ('session', 'job', 'codex', 'native-thread', 'open', 1, 1, 1);
         INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
           VALUES ('run', 'job', 'session', 'live', 1, 1);
         INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, start_reason, created_at, ended_at, updated_at)
           VALUES ('turn', 'job', 'session', 'run', 1, 'complete', 'initial', 100, 459, 459);
         PRAGMA foreign_keys = ON;",
    )
    .await
    .unwrap();
    let identity = WatchdogIdentity {
        run_id: "run".into(),
        session_id: "session".into(),
        provider_turn_id: "provider-turn".into(),
        generation: "generation".into(),
    };
    // Armed at 100 in `provider_progress` with the five-minute deadline, and
    // never refreshed or heartbeaten again: the specimen's lease row.
    arm_watchdog(
        &db,
        NewWatchdogLease {
            identity: identity.clone(),
            runner_boot_id: "boot".into(),
            phase: WatchdogPhase::ProviderProgress,
            phase_deadline_at: 400,
            now: 100,
        },
        "armed".into(),
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);
    let requests = register_warm_codex_process(&orch, None);

    let report = reconcile_watchdog_leases(
        &orch.db.local,
        ReconcileScope::Periodic,
        460,
        DEFAULT_HEARTBEAT_GRACE_SECS,
        &orch,
    )
    .await
    .unwrap();
    assert_eq!(report.claimed, 1, "{report:?}");
    assert_eq!(report.recovered, 1, "{report:?}");

    assert_eq!(orch.process_state.has_process("run"), Some(true));
    assert!(orch.process_state.is_warm("run"));
    assert!(
        requests.lock().unwrap().is_empty(),
        "nothing is sent to a provider whose turn already ended: {:?}",
        requests.lock().unwrap()
    );
    assert_eq!(
        orch.db
            .local
            .query_opt(
                "SELECT status, exit_reason FROM runs WHERE id = 'run'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap(),
        Some(("live".into(), None))
    );
    assert_eq!(
        orch.db
            .local
            .query_opt(
                "SELECT COUNT(*) FROM runs WHERE status = 'crashed'",
                (),
                |row| Ok(row.get::<i64>(0)?),
            )
            .await
            .unwrap(),
        Some(0)
    );
    assert_eq!(session_status(&orch).await, "open");

    let lease = get_watchdog_lease(&orch.db.local, "run", "session", "provider-turn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.state, WatchdogLeaseState::Terminalized);
    assert_eq!(
        lease.terminal_reason.as_deref(),
        Some("target_turn_completed")
    );
    let lifecycle = list_watchdog_lifecycle(&orch.db.local, &identity)
        .await
        .unwrap();
    assert!(
        !lifecycle
            .iter()
            .any(|event| event.kind == WatchdogLifecycleKind::SuccessorReserved),
        "a stale claim spends no recovery budget: {lifecycle:?}"
    );
}

async fn insert_running_terminal(orch: &Orchestrator) {
    orch.db
        .local
        .execute_script(
            r#"INSERT INTO job_terminals(id,job_id,session_id,command,status,created_at,slug)
                 VALUES('terminal','job','terminal-session','cargo build','running',1,'build');"#,
        )
        .await
        .unwrap();
}

async fn terminal_row(orch: &Orchestrator) -> (String, Option<String>) {
    orch.db
        .local
        .query_opt(
            "SELECT status, exit_reason FROM job_terminals WHERE id = 'terminal'",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?)),
        )
        .await
        .unwrap()
        .unwrap()
}

/// A recovery that does kill (a turn genuinely silent, its successor reserved)
/// ends only the silent provider process. The job continues, and its background
/// terminal is the job's work: it stays running, untouched, for the successor.
/// The specimen's casualty was a dependency compile three minutes in.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_kill_keeps_the_jobs_background_terminals() {
    let (orch, identity) = active_watchdog_fixture("watchdog-kill-keeps-terminals").await;
    insert_running_terminal(&orch).await;

    // The successor's launch is the job's business; whatever it reports, the
    // kill has already happened by the time it runs.
    let _ = recover_provider_watchdog(&orch, &identity, 300);

    assert_eq!(orch.process_state.has_process("run"), Some(false));
    assert_eq!(
        orch.db
            .local
            .query_opt(
                "SELECT status, exit_reason FROM runs WHERE id = 'run'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap(),
        Some(("crashed".into(), Some("provider_silence_recovery".into())))
    );
    assert!(
        list_watchdog_lifecycle(&orch.db.local, &identity)
            .await
            .unwrap()
            .iter()
            .any(|event| event.kind == WatchdogLifecycleKind::SuccessorReserved),
        "this is the killing path, with a successor reserved"
    );
    // No row for the kept terminal at all, so nothing here can carry the NULL
    // exit reason cairn/6225 tracks.
    assert_eq!(terminal_row(&orch).await, ("running".into(), None));
}

/// The provider's word outranks Cairn's rows. Cairn settled the turn and parked
/// the run warm, but the provider still serves the leased turn: it is wedged
/// mid-turn under a settled row, and every later delivery would queue behind a
/// turn that never ends. Recovery kills it rather than standing down.
#[tokio::test(flavor = "multi_thread")]
async fn a_provider_still_serving_the_turn_is_killed_under_a_settled_cairn_turn() {
    let db = migrated_test_db("watchdog-provider-serves-settled-turn").await;
    db.execute_script(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO jobs (id, project_id, status, current_session_id, current_turn_id, created_at, updated_at)
           VALUES ('job', 'project', 'running', 'session', 'turn', 1, 1);
         INSERT INTO sessions (id, job_id, backend, backend_id, status, sequence, created_at, updated_at)
           VALUES ('session', 'job', 'codex', 'native-thread', 'open', 1, 1, 1);
         INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
           VALUES ('run', 'job', 'session', 'live', 1, 1);
         INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, start_reason, created_at, ended_at, updated_at)
           VALUES ('turn', 'job', 'session', 'run', 1, 'complete', 'initial', 100, 200, 200);
         PRAGMA foreign_keys = ON;",
    )
    .await
    .unwrap();
    let identity = WatchdogIdentity {
        run_id: "run".into(),
        session_id: "session".into(),
        provider_turn_id: "provider-turn".into(),
        generation: "generation".into(),
    };
    arm_watchdog(
        &db,
        NewWatchdogLease {
            identity: identity.clone(),
            runner_boot_id: "boot".into(),
            phase: WatchdogPhase::ProviderProgress,
            phase_deadline_at: 150,
            now: 100,
        },
        "armed".into(),
    )
    .await
    .unwrap();
    let orch = test_orchestrator(db);
    register_warm_codex_process(&orch, Some("provider-turn"));
    assert!(orch.process_state.is_warm("run"));

    let recovery = recover_provider_watchdog(&orch, &identity, 300);
    assert_ne!(
        recovery.as_ref().ok(),
        Some(&ProviderWatchdogRecovery::Recovered {
            reason: TARGET_TURN_COMPLETED_REASON.to_string(),
            successor: None,
        }),
        "a provider that still serves the turn is not a completed turn"
    );
    assert_eq!(orch.process_state.has_process("run"), Some(false));
    assert_eq!(
        orch.db
            .local
            .query_opt(
                "SELECT status, exit_reason FROM runs WHERE id = 'run'",
                (),
                |row| Ok((row.text(0)?, row.opt_text(1)?)),
            )
            .await
            .unwrap(),
        Some(("crashed".into(), Some("provider_silence_recovery".into())))
    );
    assert!(
        !list_watchdog_lifecycle(&orch.db.local, &identity)
            .await
            .unwrap()
            .iter()
            .any(|event| event.reason.as_deref() == Some(TARGET_TURN_COMPLETED_REASON)),
        "recovery did not stand down"
    );
}
