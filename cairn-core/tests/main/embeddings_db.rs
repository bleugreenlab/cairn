//! Integration tests for the Cohere embedding storage model
//! (resource_embeddings + event_vibes).

use crate::common;

use cairn_core::internal::embeddings::{queries, vector, EmbedJob};
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;

async fn create_test_run(db: &LocalDb) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "INSERT INTO runs (id, status, created_at, updated_at)
         VALUES (?1, 'running', ?2, ?3)",
        params![id.as_str(), now, now],
    )
    .await
    .unwrap();
    id
}

async fn create_test_event_with_session(
    db: &LocalDb,
    run_id: &str,
    event_type: &str,
    sequence: i64,
    data: &str,
    session_id: Option<&str>,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let run_id = run_id.to_string();
    let event_type = event_type.to_string();
    let data = data.to_string();
    let session_id = session_id.map(str::to_string);
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let id = id.clone();
        let run_id = run_id.clone();
        let event_type = event_type.clone();
        let data = data.clone();
        let session_id = session_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO events (
                    id, run_id, session_id, sequence, timestamp, event_type, data, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    id.as_str(),
                    run_id.as_str(),
                    session_id.as_deref(),
                    sequence,
                    now,
                    event_type.as_str(),
                    data.as_str(),
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    id
}

async fn create_test_event(
    db: &LocalDb,
    run_id: &str,
    event_type: &str,
    sequence: i64,
    data: &str,
) -> String {
    create_test_event_with_session(db, run_id, event_type, sequence, data, None).await
}

// ===== resource_embeddings =====

#[tokio::test]
async fn upsert_and_get_resource_embedding() {
    let (_temp, db) = common::migrated_db().await;
    let bytes = vector::to_bytes(&[1.0_f32, 2.0, 3.0]);
    let uri = "cairn://p/PROJ/1";
    queries::upsert_resource_embedding_async(&db, uri, &bytes, "cohere-embed-v4", 3)
        .await
        .unwrap();

    let got = queries::get_resource_embedding_async(&db, uri)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.uri, uri);
    assert_eq!(got.model, "cohere-embed-v4");
    assert_eq!(got.dims, 3);
    assert_eq!(vector::from_bytes(&got.embedding), vec![1.0, 2.0, 3.0]);
}

#[tokio::test]
async fn upsert_replaces_resource_embedding() {
    let (_temp, db) = common::migrated_db().await;
    let uri = "cairn://skills/abc";
    queries::upsert_resource_embedding_async(&db, uri, &vector::to_bytes(&[1.0, 2.0]), "m1", 2)
        .await
        .unwrap();
    queries::upsert_resource_embedding_async(&db, uri, &vector::to_bytes(&[4.0, 5.0]), "m2", 2)
        .await
        .unwrap();

    let got = queries::get_resource_embedding_async(&db, uri)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.model, "m2");
    assert_eq!(vector::from_bytes(&got.embedding), vec![4.0, 5.0]);
    assert_eq!(
        queries::count_resource_embeddings_async(&db).await.unwrap(),
        1
    );
}

#[tokio::test]
async fn get_resource_embedding_returns_none_for_missing() {
    let (_temp, db) = common::migrated_db().await;
    assert!(queries::get_resource_embedding_async(&db, "cairn://nope")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn delete_resource_embedding_removes_row() {
    let (_temp, db) = common::migrated_db().await;
    let uri = "cairn://memory/x";
    queries::upsert_resource_embedding_async(&db, uri, &vector::to_bytes(&[1.0]), "m", 1)
        .await
        .unwrap();
    queries::delete_resource_embedding_async(&db, uri)
        .await
        .unwrap();
    assert!(queries::get_resource_embedding_async(&db, uri)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        queries::count_resource_embeddings_async(&db).await.unwrap(),
        0
    );
}

/// The worker routes a blank-text resource job to a delete. Simulate the path:
/// seed a row, then apply the job `EmbedJob::resource` produces for empty text
/// and confirm the row is gone. This is what an issue updated to a blank
/// description does end-to-end (minus the network embed call).
#[tokio::test]
async fn blank_resource_text_deletes_existing_row() {
    let (_temp, db) = common::migrated_db().await;
    let uri = "cairn://p/PROJ/7";
    queries::upsert_resource_embedding_async(&db, uri, &vector::to_bytes(&[1.0, 2.0]), "m", 2)
        .await
        .unwrap();

    match EmbedJob::resource(uri, "   ".to_string()) {
        EmbedJob::ResourceDelete { uri } => {
            queries::delete_resource_embedding_async(&db, &uri)
                .await
                .unwrap();
        }
        other => panic!("expected ResourceDelete, got {other:?}"),
    }

    assert!(queries::get_resource_embedding_async(&db, uri)
        .await
        .unwrap()
        .is_none());
}

/// A non-empty resource job upserts the vector under its URI.
#[tokio::test]
async fn nonempty_resource_text_upserts_row() {
    let (_temp, db) = common::migrated_db().await;
    let uri = "cairn://skills/demo";
    match EmbedJob::resource(uri, "a real description".to_string()) {
        EmbedJob::Resource { uri, text } => {
            assert_eq!(text, "a real description");
            // The worker would embed `text`; here we persist a stand-in vector.
            queries::upsert_resource_embedding_async(&db, &uri, &vector::to_bytes(&[0.5]), "m", 1)
                .await
                .unwrap();
        }
        other => panic!("expected Resource, got {other:?}"),
    }
    assert!(queries::get_resource_embedding_async(&db, uri)
        .await
        .unwrap()
        .is_some());
}

// ===== event_vibes =====

#[tokio::test]
async fn upsert_and_read_event_vibe() {
    let (_temp, db) = common::migrated_db().await;
    let run_id = create_test_run(&db).await;
    let session = "sess-1";
    let event_id = create_test_event_with_session(
        &db,
        &run_id,
        "assistant",
        1,
        r#"{"content":"hi"}"#,
        Some(session),
    )
    .await;

    queries::upsert_event_vibe_async(
        &db,
        &event_id,
        Some(session),
        "oklch(0.65 0.250 250)",
        0.42,
        0.83,
        "cohere-embed-v4",
    )
    .await
    .unwrap();

    let vibes = queries::get_event_vibes_for_session_async(&db, session)
        .await
        .unwrap();
    assert_eq!(vibes.len(), 1);
    assert_eq!(vibes[0].event_id, event_id);
    assert_eq!(vibes[0].css_color, "oklch(0.65 0.250 250)");
    assert!((vibes[0].phase - 0.42).abs() < 1e-4);
    assert!((vibes[0].friction - 0.83).abs() < 1e-4);
}

#[tokio::test]
async fn event_vibe_deleted_on_event_cascade() {
    let (_temp, db) = common::migrated_db().await;
    let run_id = create_test_run(&db).await;
    let session = "sess-cascade";
    let event_id = create_test_event_with_session(
        &db,
        &run_id,
        "assistant",
        1,
        r#"{"content":"x"}"#,
        Some(session),
    )
    .await;
    queries::upsert_event_vibe_async(&db, &event_id, Some(session), "oklch(...)", 0.3, 0.7, "m")
        .await
        .unwrap();

    db.write(|conn| {
        let event_id = event_id.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM events WHERE id = ?1",
                params![event_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    assert!(queries::get_event_vibes_for_session_async(&db, session)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn get_event_vibes_for_session_filters_and_orders() {
    let (_temp, db) = common::migrated_db().await;
    let run_id = create_test_run(&db).await;
    let session_a = "sa";
    let session_b = "sb";

    let e2 = create_test_event_with_session(
        &db,
        &run_id,
        "assistant",
        2,
        r#"{"content":"a2"}"#,
        Some(session_a),
    )
    .await;
    let e1 = create_test_event_with_session(
        &db,
        &run_id,
        "assistant",
        1,
        r#"{"content":"a1"}"#,
        Some(session_a),
    )
    .await;
    let eb = create_test_event_with_session(
        &db,
        &run_id,
        "assistant",
        1,
        r#"{"content":"b1"}"#,
        Some(session_b),
    )
    .await;

    // Pin the primary ordering key so this test actually exercises the
    // documented sequence tie-breaker. Millisecond wall-clock timestamps may
    // differ under the full parallel suite even though isolated inserts often
    // land in the same tick.
    db.execute(
        "UPDATE events SET created_at = 1 WHERE id IN (?1, ?2)",
        cairn_db::turso::params![e1.as_str(), e2.as_str()],
    )
    .await
    .unwrap();

    for id in [&e1, &e2, &eb] {
        queries::upsert_event_vibe_async(&db, id, None, "oklch()", 0.5, 0.6, "m")
            .await
            .unwrap();
    }

    let a = queries::get_event_vibes_for_session_async(&db, session_a)
        .await
        .unwrap();
    assert_eq!(a.len(), 2);
    // ordered by created_at then sequence; e1 (seq 1) before e2 (seq 2)
    assert_eq!(a[0].event_id, e1);
    assert_eq!(a[1].event_id, e2);

    let b = queries::get_event_vibes_for_session_async(&db, session_b)
        .await
        .unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].event_id, eb);
}

#[tokio::test]
async fn get_event_vibes_for_events_returns_only_requested_ids() {
    let (_temp, db) = common::migrated_db().await;
    let run_id = create_test_run(&db).await;
    let session = "event-list";

    let e1 = create_test_event_with_session(
        &db,
        &run_id,
        "assistant",
        1,
        r#"{"content":"a1"}"#,
        Some(session),
    )
    .await;
    let e2 = create_test_event_with_session(
        &db,
        &run_id,
        "assistant",
        2,
        r#"{"content":"a2"}"#,
        Some(session),
    )
    .await;
    let e3 = create_test_event_with_session(
        &db,
        &run_id,
        "assistant",
        3,
        r#"{"content":"a3"}"#,
        Some(session),
    )
    .await;

    for id in [&e1, &e2, &e3] {
        queries::upsert_event_vibe_async(&db, id, None, "oklch()", 0.5, 0.6, "m")
            .await
            .unwrap();
    }

    let requested = vec![e2.clone(), "missing-event".to_string(), e1.clone()];
    let vibes = queries::get_event_vibes_for_events_async(&db, &requested)
        .await
        .unwrap();

    assert_eq!(vibes.len(), 2);
    assert_eq!(vibes[0].event_id, e2);
    assert_eq!(vibes[1].event_id, e1);
}

// Silence unused-helper warning for the no-session constructor under some configs.
#[tokio::test]
async fn create_event_without_session_compiles() {
    let (_temp, db) = common::migrated_db().await;
    let run_id = create_test_run(&db).await;
    let _ = create_test_event(&db, &run_id, "assistant", 1, r#"{"content":"x"}"#).await;
}

/// Verify the recommender's query path: `vector_distance_cos` over plain f32
/// BLOBs, with the query vector built by `vector32`, executes on the live Turso
/// engine and ranks the nearest vector first.
#[tokio::test]
async fn vector_distance_cos_executes_and_ranks() {
    use cairn_core::internal::storage::{DbError, RowExt};

    let (_temp, db) = common::migrated_db().await;
    queries::upsert_resource_embedding_async(
        &db,
        "cairn://a",
        &vector::to_bytes(&[1.0, 0.0, 0.0]),
        "cohere-embed-v4",
        3,
    )
    .await
    .unwrap();
    queries::upsert_resource_embedding_async(
        &db,
        "cairn://b",
        &vector::to_bytes(&[0.0, 1.0, 0.0]),
        "cohere-embed-v4",
        3,
    )
    .await
    .unwrap();

    // Query vector closest to "cairn://a".
    let q = vector::to_bytes(&[0.9, 0.1, 0.0]);
    let nearest = db
        .read(|conn| {
            let q = q.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT uri, vector_distance_cos(embedding, vector32(?1)) AS d \
                         FROM resource_embeddings ORDER BY d ASC LIMIT 1",
                        params![q],
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("no rows".to_string()))?;
                row.text(0)
            })
        })
        .await
        .expect("vector_distance_cos query should execute");
    assert_eq!(nearest, "cairn://a");
}
