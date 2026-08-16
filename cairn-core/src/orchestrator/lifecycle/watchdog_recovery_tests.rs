use super::warm_completion_tests::{register_warm_process, test_orchestrator};
use super::watchdog_recovery::{
    current_recovery_target, finish_settled_recovery, recover_provider_watchdog,
    require_fresh_successor_session, ProviderWatchdogRecovery, ALREADY_TERMINAL_RECONCILED_REASON,
};
use crate::execution::jobs::continue_job_launch_locked_for_watchdog;
use crate::runs::watchdog_ledger::{
    arm_watchdog, claim_watchdog_recovery, get_watchdog_lease, NewWatchdogLease, WatchdogIdentity,
    WatchdogLeaseState, WatchdogPhase,
};
use crate::storage::{migrated_test_db, RowExt};

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
    assert!(!require_fresh_successor_session(db.clone(), &identity, &target, 300).unwrap());
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

    // A nonterminal ownership result fences its fail-forward successor onto a
    // fresh provider session. A settled result below closes instead of resuming.
    assert!(require_fresh_successor_session(db.clone(), &identity, &target, 300).unwrap());
    assert_eq!(
        db.query_opt(
            "SELECT needs_fresh_session FROM jobs WHERE id = 'job'",
            (),
            |row| Ok(row.get::<i64>(0)?),
        )
        .await
        .unwrap(),
        Some(1)
    );

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
    assert_eq!(
        db.query_opt(
            "SELECT status FROM sessions WHERE id = 'session'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .as_deref(),
        Some("closed")
    );
    assert!(!claim_watchdog_recovery(&db, &identity, 700).await.unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn expired_terminal_turn_is_reconciled_through_watchdog_orchestration() {
    let db = migrated_test_db("terminal-watchdog-integration").await;
    db.execute_script(
        "PRAGMA foreign_keys = OFF;
         INSERT INTO workspaces (id, name, created_at, updated_at)
           VALUES ('workspace-integration', 'Workspace', 1, 1);
         INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('project-integration', 'workspace-integration', 'Project', 'prj', '/tmp/prj', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, attention, created_at, updated_at)
           VALUES ('issue-integration', 'project-integration', 1, 'Issue', 'active', 'none', 1, 1);
         INSERT INTO jobs (id, issue_id, project_id, status, node_name, current_session_id, current_turn_id, created_at, updated_at)
           VALUES ('job-integration', 'issue-integration', 'project-integration', 'blocked', 'builder', 'session-integration', 'turn-integration', 1, 1);
         INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
           VALUES ('session-integration', 'job-integration', 'codex', 'open', 1, 1, 1);
         INSERT INTO runs (id, job_id, session_id, status, created_at, updated_at)
           VALUES ('run-integration', 'job-integration', 'session-integration', 'live', 1, 1);
         INSERT INTO turns (id, job_id, session_id, run_id, sequence, state, start_reason, created_at, ended_at, updated_at)
           VALUES ('turn-integration', 'job-integration', 'session-integration', 'run-integration', 1, 'complete', 'initial', 1, 2, 2);
         PRAGMA foreign_keys = ON;",
    )
    .await
    .unwrap();
    let identity = WatchdogIdentity {
        run_id: "run-integration".into(),
        session_id: "session-integration".into(),
        provider_turn_id: "provider-turn-integration".into(),
        generation: "generation-integration".into(),
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
    register_warm_process(&orch, "run-integration", Some("job-integration"));
    orch.process_state
        .set_current_turn_id("run-integration", Some("turn-integration"));

    assert_eq!(
        recover_provider_watchdog(&orch, &identity, 300).unwrap(),
        ProviderWatchdogRecovery::Recovered {
            reason: ALREADY_TERMINAL_RECONCILED_REASON.to_string(),
            successor: None,
        }
    );
    let db = orch.db.local.clone();
    let lease = get_watchdog_lease(
        &db,
        "run-integration",
        "session-integration",
        "provider-turn-integration",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(lease.state, WatchdogLeaseState::Terminalized);
    assert_eq!(
        lease.terminal_reason.as_deref(),
        Some(ALREADY_TERMINAL_RECONCILED_REASON)
    );
    assert_eq!(
        db.query_opt(
            "SELECT status FROM sessions WHERE id = 'session-integration'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .as_deref(),
        Some("closed")
    );
    assert_eq!(
        db.query_opt(
            "SELECT status FROM jobs WHERE id = 'job-integration'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .as_deref(),
        Some("blocked")
    );
    assert_eq!(
        db.query_opt(
            "SELECT needs_fresh_session FROM jobs WHERE id = 'job-integration'",
            (),
            |row| Ok(row.get::<i64>(0)?),
        )
        .await
        .unwrap(),
        Some(1)
    );
    assert_eq!(
        db.query_opt(
            "SELECT state FROM turns WHERE id = 'turn-integration'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .as_deref(),
        Some("complete")
    );
    assert!(!claim_watchdog_recovery(&db, &identity, 700).await.unwrap());

    let closed_session = crate::sessions::queries::get(&db, "session-integration")
        .await
        .unwrap();
    db.execute(
        "UPDATE jobs SET current_session_id = 'raced-session' WHERE id = 'job-integration'",
        (),
    )
    .await
    .unwrap();
    assert!(
        crate::sessions::queries::rotate_watchdog_reconciled_job_session(
            &db,
            &closed_session,
            "job-integration",
            orch.services.emitter.as_ref(),
        )
        .await
        .is_err()
    );
    assert_eq!(
        db.query_opt(
            "SELECT needs_fresh_session FROM jobs WHERE id = 'job-integration'",
            (),
            |row| Ok(row.get::<i64>(0)?),
        )
        .await
        .unwrap(),
        Some(1),
        "failed rotation must preserve retry authorization"
    );
    db.execute(
        "UPDATE jobs SET current_session_id = 'session-integration' WHERE id = 'job-integration'",
        (),
    )
    .await
    .unwrap();

    let _ = continue_job_launch_locked_for_watchdog(&orch, "job-integration", None);
    let successor_session_id = db
        .query_opt(
            "SELECT current_session_id FROM jobs WHERE id = 'job-integration'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .expect("normal continuation must rotate onto a successor session");
    assert_ne!(successor_session_id, "session-integration");
    assert_eq!(
        db.query_opt(
            "SELECT status FROM sessions WHERE id = ?1",
            (successor_session_id,),
            |row| row.text(0),
        )
        .await
        .unwrap()
        .as_deref(),
        Some("open")
    );
    assert_eq!(
        db.query_opt(
            "SELECT needs_fresh_session FROM jobs WHERE id = 'job-integration'",
            (),
            |row| Ok(row.get::<i64>(0)?),
        )
        .await
        .unwrap(),
        Some(0)
    );
}
