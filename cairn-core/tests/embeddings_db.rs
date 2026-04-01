//! Integration tests for embedding database queries.
//!
//! Tests the CRUD operations in `cairn_core::internal::embeddings::queries`
//! against an in-memory SQLite database with migrations applied.

mod common;

use cairn_core::internal::embeddings::queries;

/// Helper: create a test run and return its ID.
fn create_test_run(conn: &mut diesel::SqliteConnection) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    diesel::sql_query(
        "INSERT INTO runs (id, status, created_at, updated_at) VALUES (?, 'running', ?, ?)",
    )
    .bind::<diesel::sql_types::Text, _>(&id)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Integer, _>(now)
    .execute(conn)
    .expect("Failed to create test run");

    id
}

/// Helper: create a test event and return its ID.
fn create_test_event(
    conn: &mut diesel::SqliteConnection,
    run_id: &str,
    event_type: &str,
    sequence: i32,
    data: &str,
) -> String {
    create_test_event_with_session(conn, run_id, event_type, sequence, data, None)
}

/// Helper: create a test event with optional session_id and return its ID.
fn create_test_event_with_session(
    conn: &mut diesel::SqliteConnection,
    run_id: &str,
    event_type: &str,
    sequence: i32,
    data: &str,
    session_id: Option<&str>,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp() as i32;

    diesel::sql_query(
        "INSERT INTO events (id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind::<diesel::sql_types::Text, _>(&id)
    .bind::<diesel::sql_types::Text, _>(run_id)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(session_id)
    .bind::<diesel::sql_types::Integer, _>(sequence)
    .bind::<diesel::sql_types::Integer, _>(now)
    .bind::<diesel::sql_types::Text, _>(event_type)
    .bind::<diesel::sql_types::Text, _>(data)
    .bind::<diesel::sql_types::Integer, _>(now)
    .execute(conn)
    .expect("Failed to create test event");

    id
}

use diesel::prelude::*;

#[test]
fn upsert_and_get_embedding() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);
    let event_id = create_test_event(
        &mut conn,
        &run_id,
        "assistant",
        1,
        r#"{"content": "hello"}"#,
    );

    let embedding_data = vec![1.0_f32, 2.0, 3.0];
    let bytes = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&embedding_data);

    // Insert
    queries::upsert_embedding(&mut conn, &event_id, &bytes, "test-model", 3).unwrap();

    // Retrieve
    let result = queries::get_embedding(&mut conn, &event_id).unwrap();
    assert!(result.is_some());
    let emb = result.unwrap();
    assert_eq!(emb.event_id, event_id);
    assert_eq!(emb.model_name, "test-model");
    assert_eq!(emb.dimensions, 3);

    // Verify the bytes roundtrip
    let recovered = cairn_core::internal::embeddings::EmbeddingEngine::from_bytes(&emb.embedding);
    assert_eq!(recovered, embedding_data);
}

#[test]
fn upsert_replaces_existing_embedding() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);
    let event_id = create_test_event(
        &mut conn,
        &run_id,
        "assistant",
        1,
        r#"{"content": "hello"}"#,
    );

    let bytes_v1 = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&[1.0, 2.0, 3.0]);
    let bytes_v2 = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&[4.0, 5.0, 6.0]);

    queries::upsert_embedding(&mut conn, &event_id, &bytes_v1, "model-v1", 3).unwrap();
    queries::upsert_embedding(&mut conn, &event_id, &bytes_v2, "model-v2", 3).unwrap();

    let result = queries::get_embedding(&mut conn, &event_id)
        .unwrap()
        .unwrap();
    assert_eq!(result.model_name, "model-v2");
    let recovered =
        cairn_core::internal::embeddings::EmbeddingEngine::from_bytes(&result.embedding);
    assert_eq!(recovered, vec![4.0, 5.0, 6.0]);

    // Should still be only one row
    let count = queries::count_embeddings(&mut conn).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn get_embedding_returns_none_for_missing() {
    let mut conn = common::test_conn();
    let result = queries::get_embedding(&mut conn, "nonexistent").unwrap();
    assert!(result.is_none());
}

#[test]
fn get_embeddings_for_events_batch() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);

    let id1 = create_test_event(&mut conn, &run_id, "assistant", 1, r#"{"content": "a"}"#);
    let id2 = create_test_event(&mut conn, &run_id, "assistant", 2, r#"{"content": "b"}"#);
    let id3 = create_test_event(&mut conn, &run_id, "assistant", 3, r#"{"content": "c"}"#);

    let bytes = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&[1.0, 2.0]);
    queries::upsert_embedding(&mut conn, &id1, &bytes, "test", 2).unwrap();
    queries::upsert_embedding(&mut conn, &id2, &bytes, "test", 2).unwrap();
    // id3 intentionally not embedded

    let results =
        queries::get_embeddings_for_events(&mut conn, &[id1.clone(), id2.clone(), id3.clone()])
            .unwrap();
    assert_eq!(results.len(), 2);

    let ids: Vec<&str> = results.iter().map(|e| e.event_id.as_str()).collect();
    assert!(ids.contains(&id1.as_str()));
    assert!(ids.contains(&id2.as_str()));
}

#[test]
fn get_embeddings_for_run_orders_by_sequence() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);

    // Insert events out of order
    let id2 = create_test_event(
        &mut conn,
        &run_id,
        "assistant",
        2,
        r#"{"content": "second"}"#,
    );
    let id1 = create_test_event(
        &mut conn,
        &run_id,
        "assistant",
        1,
        r#"{"content": "first"}"#,
    );
    let id3 = create_test_event(
        &mut conn,
        &run_id,
        "assistant",
        3,
        r#"{"content": "third"}"#,
    );

    let bytes = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&[1.0]);
    queries::upsert_embedding(&mut conn, &id1, &bytes, "test", 1).unwrap();
    queries::upsert_embedding(&mut conn, &id2, &bytes, "test", 1).unwrap();
    queries::upsert_embedding(&mut conn, &id3, &bytes, "test", 1).unwrap();

    let results = queries::get_embeddings_for_run(&mut conn, &run_id).unwrap();
    assert_eq!(results.len(), 3);

    // Verify ordering by sequence
    let sequences: Vec<i32> = results.iter().map(|(_, seq)| *seq).collect();
    assert_eq!(sequences, vec![1, 2, 3]);

    // Verify event IDs match sequence order
    assert_eq!(results[0].0.event_id, id1);
    assert_eq!(results[1].0.event_id, id2);
    assert_eq!(results[2].0.event_id, id3);
}

#[test]
fn find_unembedded_events_only_returns_assistant_events_without_embeddings() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);

    // assistant event with embedding
    let id1 = create_test_event(&mut conn, &run_id, "assistant", 1, r#"{"content": "a"}"#);
    let bytes = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&[1.0]);
    queries::upsert_embedding(&mut conn, &id1, &bytes, "test", 1).unwrap();

    // assistant event without embedding (should be found)
    let id2 = create_test_event(&mut conn, &run_id, "assistant", 2, r#"{"content": "b"}"#);

    // tool_use event without embedding (should NOT be found — not assistant)
    let _id3 = create_test_event(
        &mut conn,
        &run_id,
        "tool_use",
        3,
        r#"{"tool": "read", "input": {}}"#,
    );

    // system event without embedding (should NOT be found)
    let _id4 = create_test_event(
        &mut conn,
        &run_id,
        "system",
        4,
        r#"{"content": "system msg"}"#,
    );

    let unembedded = queries::find_unembedded_events(&mut conn, 100).unwrap();
    assert_eq!(unembedded.len(), 1);
    assert_eq!(unembedded[0].0, id2);
}

#[test]
fn find_unembedded_events_respects_limit() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);

    for i in 0..5 {
        create_test_event(
            &mut conn,
            &run_id,
            "assistant",
            i,
            &format!(r#"{{"content": "msg {}"}}"#, i),
        );
    }

    let unembedded = queries::find_unembedded_events(&mut conn, 3).unwrap();
    assert_eq!(unembedded.len(), 3);
}

#[test]
fn count_embeddings_and_embeddable_events() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);

    // 3 assistant events, 1 tool_use event
    let id1 = create_test_event(&mut conn, &run_id, "assistant", 1, r#"{"content": "a"}"#);
    let _id2 = create_test_event(&mut conn, &run_id, "assistant", 2, r#"{"content": "b"}"#);
    let _id3 = create_test_event(&mut conn, &run_id, "assistant", 3, r#"{"content": "c"}"#);
    let _id4 = create_test_event(&mut conn, &run_id, "tool_use", 4, r#"{"tool": "read"}"#);

    // Only 1 embedded so far
    let bytes = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&[1.0]);
    queries::upsert_embedding(&mut conn, &id1, &bytes, "test", 1).unwrap();

    assert_eq!(queries::count_embeddings(&mut conn).unwrap(), 1);
    assert_eq!(queries::count_embeddable_events(&mut conn).unwrap(), 3);
}

#[test]
fn embedding_deleted_on_event_cascade() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);
    let event_id = create_test_event(
        &mut conn,
        &run_id,
        "assistant",
        1,
        r#"{"content": "hello"}"#,
    );

    let bytes = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&[1.0, 2.0]);
    queries::upsert_embedding(&mut conn, &event_id, &bytes, "test", 2).unwrap();

    // Verify embedding exists
    assert!(queries::get_embedding(&mut conn, &event_id)
        .unwrap()
        .is_some());

    // Delete the event
    diesel::sql_query("DELETE FROM events WHERE id = ?")
        .bind::<diesel::sql_types::Text, _>(&event_id)
        .execute(&mut conn)
        .unwrap();

    // Embedding should be cascade-deleted
    assert!(queries::get_embedding(&mut conn, &event_id)
        .unwrap()
        .is_none());
}

#[test]
fn get_embeddings_for_session_filters_by_session_id() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);

    let session_a = "session-aaa";
    let session_b = "session-bbb";

    // Events in session A
    let id1 = create_test_event_with_session(
        &mut conn,
        &run_id,
        "assistant",
        1,
        r#"{"content": "a1"}"#,
        Some(session_a),
    );
    let id2 = create_test_event_with_session(
        &mut conn,
        &run_id,
        "assistant",
        2,
        r#"{"content": "a2"}"#,
        Some(session_a),
    );

    // Event in session B
    let id3 = create_test_event_with_session(
        &mut conn,
        &run_id,
        "assistant",
        3,
        r#"{"content": "b1"}"#,
        Some(session_b),
    );

    // Event with no session
    let id4 = create_test_event(&mut conn, &run_id, "assistant", 4, r#"{"content": "none"}"#);

    // Embed all four
    let bytes = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&[1.0, 2.0]);
    queries::upsert_embedding(&mut conn, &id1, &bytes, "test", 2).unwrap();
    queries::upsert_embedding(&mut conn, &id2, &bytes, "test", 2).unwrap();
    queries::upsert_embedding(&mut conn, &id3, &bytes, "test", 2).unwrap();
    queries::upsert_embedding(&mut conn, &id4, &bytes, "test", 2).unwrap();

    // Query session A — should only get id1 and id2
    let results = queries::get_embeddings_for_session(&mut conn, session_a).unwrap();
    assert_eq!(results.len(), 2);
    let ids: Vec<&str> = results.iter().map(|e| e.event_id.as_str()).collect();
    assert!(ids.contains(&id1.as_str()));
    assert!(ids.contains(&id2.as_str()));

    // Query session B — should only get id3
    let results = queries::get_embeddings_for_session(&mut conn, session_b).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].event_id, id3);
}

#[test]
fn get_embeddings_for_session_orders_by_created_at_and_sequence() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);
    let session = "session-ordered";

    // Insert events out of order (sequence 3, 1, 2)
    let id3 = create_test_event_with_session(
        &mut conn,
        &run_id,
        "assistant",
        3,
        r#"{"content": "third"}"#,
        Some(session),
    );
    let id1 = create_test_event_with_session(
        &mut conn,
        &run_id,
        "assistant",
        1,
        r#"{"content": "first"}"#,
        Some(session),
    );
    let id2 = create_test_event_with_session(
        &mut conn,
        &run_id,
        "assistant",
        2,
        r#"{"content": "second"}"#,
        Some(session),
    );

    let bytes = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&[1.0]);
    queries::upsert_embedding(&mut conn, &id1, &bytes, "test", 1).unwrap();
    queries::upsert_embedding(&mut conn, &id2, &bytes, "test", 1).unwrap();
    queries::upsert_embedding(&mut conn, &id3, &bytes, "test", 1).unwrap();

    let results = queries::get_embeddings_for_session(&mut conn, session).unwrap();
    assert_eq!(results.len(), 3);

    // Should be ordered by (created_at, sequence) — since all have same created_at
    // (same timestamp resolution), sequence should break the tie
    // The events were inserted as seq 3, 1, 2 but should come back as 1, 2, 3
    assert_eq!(results[0].event_id, id1);
    assert_eq!(results[1].event_id, id2);
    assert_eq!(results[2].event_id, id3);
}

#[test]
fn get_embeddings_for_session_returns_empty_for_unknown_session() {
    let mut conn = common::test_conn();
    let results = queries::get_embeddings_for_session(&mut conn, "nonexistent").unwrap();
    assert!(results.is_empty());
}

#[test]
fn get_embeddings_for_session_excludes_events_without_embeddings() {
    let mut conn = common::test_conn();
    let run_id = create_test_run(&mut conn);
    let session = "session-partial";

    let id1 = create_test_event_with_session(
        &mut conn,
        &run_id,
        "assistant",
        1,
        r#"{"content": "embedded"}"#,
        Some(session),
    );
    let _id2 = create_test_event_with_session(
        &mut conn,
        &run_id,
        "assistant",
        2,
        r#"{"content": "not embedded"}"#,
        Some(session),
    );

    // Only embed id1
    let bytes = cairn_core::internal::embeddings::EmbeddingEngine::to_bytes(&[1.0]);
    queries::upsert_embedding(&mut conn, &id1, &bytes, "test", 1).unwrap();

    let results = queries::get_embeddings_for_session(&mut conn, session).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].event_id, id1);
}
