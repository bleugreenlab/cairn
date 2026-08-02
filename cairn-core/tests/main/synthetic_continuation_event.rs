//! Substrate text that occupies the user slot must not reach the transcript as a
//! user message: a resume Cairn synthesizes for itself (CAIRN-3175), and the
//! prompt a job is launched on (CAIRN-3408). These pin the storage seam — the
//! stored row's `event_type` is what every downstream projection dispatches on,
//! so it is the representation that carries authorship end to end.

use crate::common;

use cairn_core::internal::execution::jobs::{
    store_continuation_event_with_turn, store_launch_event_with_turn,
    store_tool_result_event_with_turn,
};
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;

async fn insert_project_job_run_turn(db: &LocalDb) {
    let project_id = common::create_project(db, "CONTIN").await;
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

/// The synthesized continuation lands under its own namespaced event type, so
/// no projection that renders `user` as the operator can ever reach it — while
/// the prompt text itself stays addressable in the raw stream.
#[tokio::test]
async fn synthesized_continuation_is_not_stored_as_a_user_event() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local).await;

    store_continuation_event_with_turn(
        &orch,
        "run-1",
        "session-1",
        "Automatic resume: Cairn restarted this turn after the previous one ended without completing.",
        7,
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
        Some("user:continuation".to_string()),
        "a Cairn-synthesized resume must never be stored as a plain `user` event"
    );

    let data = common::scalar_text_by_id(
        &orch.db.local,
        "SELECT data FROM events WHERE run_id = ?1",
        "run-1",
    )
    .await
    .unwrap();
    assert!(
        data.contains("Automatic resume"),
        "the prompt the agent actually received must stay recoverable: {data}"
    );
}

/// A job's launch prompt lands under its own namespaced type too (CAIRN-3408).
///
/// Nobody types a launch prompt — Cairn composes it from the issue's resolved
/// inputs, so under delegation its author is the coordinator or thread that
/// filed the child. Stored as a plain `user` event it came back to that parent
/// as `**User:** <its own issue description>` in the child's catch-up digest.
#[tokio::test]
async fn a_launch_prompt_is_not_stored_as_a_user_event() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local).await;

    let task =
        "# Fix the panic in the CLI logger\n\nEvery fenced batch shell dies at logging init.";
    store_launch_event_with_turn(&orch, "run-1", "session-1", task, 7, Some("turn-1")).unwrap();

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT event_type FROM events WHERE run_id = ?1",
            "run-1",
        )
        .await,
        Some("user:launch".to_string()),
        "a launch prompt must never be stored as a plain `user` event"
    );

    let data = common::scalar_text_by_id(
        &orch.db.local,
        "SELECT data FROM events WHERE run_id = ?1",
        "run-1",
    )
    .await
    .unwrap();
    assert!(
        data.contains("Fix the panic in the CLI logger"),
        "the task the agent was given must stay recoverable: {data}"
    );
}

/// The sibling storage paths are untouched: a genuine event keeps its own type,
/// so namespacing the continuation did not blur any other attribution.
#[tokio::test]
async fn other_stored_events_keep_their_own_type() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local).await;

    store_tool_result_event_with_turn(
        &orch,
        "run-1",
        "session-1",
        "toolu_x",
        "done",
        false,
        7,
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
}
