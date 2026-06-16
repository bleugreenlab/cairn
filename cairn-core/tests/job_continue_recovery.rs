mod common;

use cairn_core::internal::execution::jobs::reconcile_stale_active_turn_for_continue_for_test;
use cairn_core::internal::storage::LocalDb;
use turso::params;

async fn insert_job_session_run_turn(db: &LocalDb, turn_state: &str, run_id: Option<&str>) {
    let project_id = common::create_project(db, "CONT").await;
    let turn_state = turn_state.to_string();
    let run_id = run_id.map(str::to_string);
    db.write(|conn| {
        let project_id = project_id.clone();
        let turn_state = turn_state.clone();
        let run_id = run_id.clone();
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
            if let Some(run_id) = run_id.as_deref() {
                conn.execute(
                    "INSERT INTO runs(id, project_id, job_id, chat_id, status, session_id, backend, created_at, updated_at, start_mode)
                     VALUES (?1, ?2, 'job-1', NULL, 'live', 'session-1', 'codex', 1, 1, 'resume')",
                    params![run_id, project_id.as_str()],
                )
                .await?;
            }
            conn.execute(
                "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', ?1, 'job-1', 1, ?2, 1, 1)",
                params![run_id.as_deref(), turn_state.as_str()],
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
async fn continue_recovery_interrupts_stale_running_turn_and_crashes_run() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_job_session_run_turn(&orch.db.local, "running", Some("run-1")).await;

    let recovered =
        reconcile_stale_active_turn_for_continue_for_test(&orch, "job-1", "session-1").unwrap();

    assert!(recovered);
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
        Some("crashed".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT exit_reason FROM runs WHERE id = ?1",
            "run-1"
        )
        .await,
        Some("stale_continue_recovery".to_string())
    );
    assert_eq!(
        common::scalar_i64_by_id(
            &orch.db.local,
            "SELECT COUNT(*) FROM turns WHERE job_id = ?1 AND state IN ('pending', 'running')",
            "job-1"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn continue_recovery_cancels_stale_pending_turn_without_run() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_job_session_run_turn(&orch.db.local, "pending", None).await;

    let recovered =
        reconcile_stale_active_turn_for_continue_for_test(&orch, "job-1", "session-1").unwrap();

    assert!(recovered);
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
        common::scalar_i64_by_id(
            &orch.db.local,
            "SELECT COUNT(*) FROM turns WHERE job_id = ?1 AND state IN ('pending', 'running')",
            "job-1"
        )
        .await,
        0
    );
}
