use crate::common;

use cairn_core::internal::execution::jobs::store_tool_result_event_with_turn;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;

async fn insert_project_job_run_turn(db: &LocalDb) {
    let project_id = common::create_project(db, "presume").await;
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

#[tokio::test]
async fn answered_write_question_preserves_report_and_opaque_answer() {
    use cairn_common::identity::AppearanceTransport;
    use cairn_core::internal::mcp::handlers::planning::answer_prompt_id;
    use cairn_core::turns::queries::ResolutionProvenance;
    use serde_json::{json, Value};

    let (_temp, orch) = common::test_orchestrator().await;
    insert_project_job_run_turn(&orch.db.local).await;

    let original = json!({
        "applied": [{"index": 0, "target": "file:note.md", "mode": "patch",
                     "kind": "file", "summary": "Updated note.md"}],
        "failures": [{"index": 1, "target": "file:missing.md", "mode": "patch",
                      "kind": "file", "error": "not found"}],
        "commit": {"status": "committed", "sha": "parent-commit",
                   "pr_number": null, "message": null},
        "branch": {"number": 1, "git_ref": "question-branch",
                   "uri": "cairn://p/presume/branches/1", "stage": "active"},
        "partial_success": true,
        "blocking": {"text": "Question suspended", "artifact_uri": "cairn://stale"}
    });
    let suffix = "\n\nChecks: ✗ lint";
    let checkpoint = format!("{original}{suffix}");
    let call = json!({
        "type": "tool_use", "sessionId": "session-1",
        "toolName": "write", "toolUseId": "toolu_question",
        "toolInput": {"changes": [{"target": "cairn:~/questions", "mode": "append",
            "payload": {"questions": [{"question": "Which approach?"}]}}]}
    });
    orch.db.local.write(|conn| {
        let checkpoint = checkpoint.clone();
        let call = call.to_string();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO prompts(id, run_id, job_id, turn_id, questions, created_at, tool_use_id)
                 VALUES ('prompt-1', 'run-1', 'job-1', 'turn-1',
                         '[{\"question\":\"Which approach?\"}]', 1, 'toolu_question')",
                (),
            ).await?;
            conn.execute(
                "INSERT INTO events(id, run_id, session_id, turn_id, sequence, timestamp, event_type, data, created_at)
                 VALUES ('call-1', 'run-1', 'session-1', 'turn-1', 1, 1, 'tool_use', ?1, 1)",
                params![call],
            ).await?;
            conn.execute(
                "INSERT INTO write_replay_ledger(run_id, tool_use_id, report, created_at)
                 VALUES ('run-1', 'toolu_question', ?1, 1)",
                params![checkpoint],
            ).await?;
            Ok(())
        })
    }).await.unwrap();

    let answer = r#"{"applied":[],"blocking":{"text":"This is answer data, not another host report","artifact_uri":"cairn://child"}}"#;
    let provenance = ResolutionProvenance {
        id: "answer-resolution".to_string(),
        surface: AppearanceTransport::ChannelReply,
        provider: Some("test-provider".to_string()),
        conversation: Some("test-conversation".to_string()),
        actor: Some("test-actor".to_string()),
    };
    let outcome = answer_prompt_id(&orch, "prompt-1", answer.to_string(), provenance).await;
    // This fixture has no launchable execution. Dispatch may fail only after the
    // real answer producer has durably written its synthetic transcript event.
    if let Err(error) = outcome {
        assert!(
            error.starts_with("Failed to resume prompt response:"),
            "{error}"
        );
    }

    let data = common::scalar_text_by_id(
        &orch.db.local,
        "SELECT data FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
        "run-1",
    )
    .await
    .expect("answer producer must store its synthetic result");
    let event: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(event["toolUseId"], "toolu_question");
    assert_eq!(event["sessionId"], "session-1");
    assert_eq!(event["isError"], false);
    let result = event["toolResult"].as_str().unwrap();
    let mut stream = serde_json::Deserializer::from_str(result).into_iter::<Value>();
    let report = stream.next().unwrap().unwrap();
    assert_eq!(&result[stream.byte_offset()..], suffix);
    let mut expected = original;
    expected["blocking"] = json!({"text": answer});
    assert_eq!(
        report, expected,
        "only the host blocking payload may change"
    );
    assert_eq!(report["blocking"]["text"], answer);

    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT turn_id FROM events WHERE run_id = ?1 AND event_type = 'tool_result'",
            "run-1",
        )
        .await
        .as_deref(),
        Some("turn-1"),
    );
    assert_eq!(
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT response FROM prompts WHERE id = ?1",
            "prompt-1",
        )
        .await
        .as_deref(),
        Some(answer),
    );
    let receipt: cairn_db::models::ResolutionReceipt =
        serde_json::from_value(event["raw"]["resolution"].clone()).unwrap();
    assert_eq!(receipt.surface, "channel_reply");
    assert_eq!(receipt.provider.as_deref(), Some("test-provider"));
    assert_eq!(receipt.conversation.as_deref(), Some("test-conversation"));
    assert_eq!(receipt.actor.as_deref(), Some("test-actor"));
    assert!(receipt.resolved_at > 0);
    assert_eq!(
        receipt.id,
        common::scalar_text_by_id(
            &orch.db.local,
            "SELECT resolution_id FROM prompts WHERE id = ?1",
            "prompt-1",
        )
        .await,
    );
}
