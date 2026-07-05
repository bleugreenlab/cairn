use crate::common;

use cairn_core::internal::execution::jobs::store_tool_result_event_with_turn;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;

async fn insert_project_job_run_turn(db: &LocalDb) {
    let project_id = common::create_project(db, "PRESUME").await;
    db.write(|conn| {
        let project_id = project_id.clone();
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
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 1, 'yielded', 1, 1)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// The slow-path prompt resume writes a synthetic `tool_result` event attached
/// to the originating Question call by `tool_use_id` and the question's turn, so
/// it renders in place (identical to the fast path) rather than as a separate
/// user block.
#[tokio::test]
async fn synthetic_tool_result_attaches_to_question_call_and_turn() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local).await;

    let answer = r#"{"Which approach?":"Option A"}"#;
    store_tool_result_event_with_turn(
        &orch,
        "run-1",
        "session-1",
        "toolu_question",
        answer,
        false,
        42,
        Some("turn-1"),
    )
    .unwrap();

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT event_type FROM events WHERE run_id = ?1",
            "run-1",
        )
        .await,
        Some("tool_result".to_string())
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT turn_id FROM events WHERE run_id = ?1",
            "run-1",
        )
        .await,
        Some("turn-1".to_string())
    );

    let data = common::scalar_text_by_id(
        &orch.db.local,
        "SELECT data FROM events WHERE run_id = ?1",
        "run-1",
    )
    .await
    .unwrap();
    // TranscriptEvent serializes camelCase; the synthetic event carries both the
    // originating tool_use id and the answer payload.
    assert!(
        data.contains("toolu_question"),
        "data missing tool_use id: {data}"
    );
    assert!(data.contains("Option A"), "data missing answer: {data}");
    assert!(
        data.contains("\"isError\":false"),
        "data should not be an error: {data}"
    );
}
