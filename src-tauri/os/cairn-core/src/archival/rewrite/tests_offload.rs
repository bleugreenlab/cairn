use super::testsupport::*;
use super::*;
use crate::storage::{InMemoryContentStore, TeamReplicaContext};
use std::sync::Arc;

fn attach_team_store(db: &mut LocalDb, store: Arc<dyn ContentStore>) {
    db.set_team_context(
        TeamReplicaContext {
            team_id: "team-x".to_string(),
            private_db: None,
        },
        store,
    );
}

fn large_system_prompt() -> (String, String) {
    let backend_base = "CLAUDE-BASE ".repeat(800);
    let cairn = format!("\n\n{}", "CAIRN-PROMPT ".repeat(700));
    let workspace = "\n\n## Workspace Instructions\n\nworkspace doctrine".to_string();
    let agent = "\n\n<agent_role>\nbuilder role body".to_string();
    system_prompt(&[
        ("backend_base", backend_base.as_str()),
        ("cairn", cairn.as_str()),
        ("workspace", workspace.as_str()),
        ("agent", agent.as_str()),
        (
            "dynamic",
            "\n\n## Orientation\n\ncwd=/work/run-1\n</agent_role>",
        ),
    ])
}

/// A team run offloads system-prompt segment blobs to the shared store by hash,
/// writes NO `archival_blobs` rows to the synced replica, and reconstructs
/// byte-identically by fetching the segment bytes back from the store.
#[tokio::test]
async fn team_run_offloads_system_blobs_and_reconstructs_from_store() {
    let mut db = migrated_test_db("archival-team-blobs.db").await;
    seed_chain(&db, "/nonexistent", false).await;
    let store = InMemoryContentStore::new();
    attach_team_store(&mut db, Arc::new(store.clone()));

    let (data, content) = large_system_prompt();
    insert_event(&db, "sp1", "run", 1, 1, "system:prompt", &data).await;

    let summary = archive(&db, None).await.unwrap();
    assert_eq!(summary.system_prompt, 1);
    assert_eq!(blob_count(&db).await, 0, "no archival_blobs on the replica");
    assert_eq!(
        store.len().await,
        4,
        "the four static segments are offloaded"
    );

    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let value: Value = serde_json::from_str(&recon[0].data).unwrap();
    assert_eq!(value["content"].as_str().unwrap(), content);
}

/// A team run offloads the range pack as one framed store object, publishes it
/// to the pack catalog under the execution, and keeps only a `pack_hash`
/// pointer. After the branch is deleted and collected, a gitcoord read
/// reconstructs only through the store-fetched pack.
#[tokio::test]
async fn team_run_offloads_pack_and_reconstructs_gitcoord_from_store() {
    let fx = build_fixture();
    let mut db = migrated_test_db("archival-team-pack.db").await;
    seed_chain(&db, fx.repo.to_str().unwrap(), false).await;
    let store = InMemoryContentStore::new();
    attach_team_store(&mut db, Arc::new(store.clone()));

    let stored_w1 = rendered(&[("file:a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n")]);
    let w1_short = short(&fx.repo, &fx.w1);
    insert_event(
        &db,
        "a-w1",
        "run",
        1,
        1,
        "assistant",
        &assistant_write("w1"),
    )
    .await;
    insert_event(
        &db,
        "e-w1",
        "run",
        2,
        2,
        "tool_result",
        &write_result("w1", &w1_short),
    )
    .await;
    insert_event(
        &db,
        "a-r2",
        "run",
        3,
        3,
        "assistant",
        &assistant_read("r2", &["file:a.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e-r2",
        "run",
        4,
        4,
        "tool_result",
        &read_result("r2", &stored_w1),
    )
    .await;

    let summary = archive(&db, Some(fx.range())).await.unwrap();
    assert_eq!(summary.gitcoord_read, 1);

    let (pack_null, pack_hash) = db
        .query_one(
            "SELECT pack IS NULL AND pack_idx IS NULL, pack_hash FROM execution_history
             WHERE execution_id = 'exec'",
            (),
            |row| Ok((row.i64(0)? == 1, row.opt_text(1)?)),
        )
        .await
        .unwrap();
    assert!(pack_null, "pack/pack_idx are NULL on the team replica");
    let pack_hash = pack_hash.expect("pack_hash pointer written");
    assert!(store.contains(&pack_hash).await);
    let referenced = db
        .query_one(
            "SELECT COUNT(*) FROM pack_catalog c
             JOIN pack_catalog_references r ON r.content_hash = c.content_hash
             WHERE c.content_hash = ?1 AND c.kind = 'execution_range'
               AND r.owner_kind = 'execution_history' AND r.owner_id = 'exec'",
            (pack_hash.clone(),),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(referenced, 1, "the pack is cataloged under its execution");

    git(&fx.repo, &["checkout", "-q", "main"]);
    git(&fx.repo, &["branch", "-D", "-q", "work"]);
    git(&fx.repo, &["reflog", "expire", "--expire=now", "--all"]);
    git(&fx.repo, &["gc", "-q", "--prune=now"]);

    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let r2 = recon.iter().find(|e| e.id == "e-r2").unwrap();
    assert_eq!(
        tool_result_of(r2),
        stored_w1,
        "restored through the store-fetched pack"
    );

    attach_team_store(&mut db, Arc::new(InMemoryContentStore::new()));
    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let r2 = recon.iter().find(|e| e.id == "e-r2").unwrap();
    assert!(
        tool_result_of(r2).contains(STUB_PREFIX),
        "a store without the pack degrades to a labeled stub"
    );
}

/// Fail-soft: a store `put` failure aborts archival before any replica write,
/// so the rows stay `full` for a later pass.
#[tokio::test]
async fn team_run_store_put_failure_leaves_rows_full() {
    struct FailingStore;
    #[async_trait::async_trait]
    impl ContentStore for FailingStore {
        async fn put(&self, _: &str, _: &[u8]) -> Result<(), String> {
            Err("store unavailable".to_string())
        }
        async fn get(&self, _: &str) -> Result<Option<Vec<u8>>, String> {
            Ok(None)
        }
    }

    let mut db = migrated_test_db("archival-team-failsoft.db").await;
    seed_chain(&db, "/nonexistent", false).await;
    attach_team_store(&mut db, Arc::new(FailingStore));
    let (data, _) = large_system_prompt();
    insert_event(&db, "sp1", "run", 1, 1, "system:prompt", &data).await;

    assert!(archive(&db, None).await.is_err());
    assert_eq!(blob_count(&db).await, 0);
    assert_eq!(storage_mode(&db, "sp1").await.as_deref(), Some("full"));
}
