use super::testsupport::*;
use super::*;

/// Read the `execution_history` pointer: whether `pack`/`pack_idx` are both
/// NULL (the offloaded shape) and the `pack_hash` pointer.
async fn pack_pointer(db: &LocalDb) -> (bool, Option<String>) {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT pack, pack_idx, pack_hash FROM execution_history
                         WHERE execution_id = 'exec'",
                    (),
                )
                .await?;
            let row = rows.next().await?.expect("execution_history row present");
            let pack_null = row.opt_blob(0)?.is_none() && row.opt_blob(1)?.is_none();
            DbResult::Ok((pack_null, row.opt_text(2)?))
        })
    })
    .await
    .unwrap()
}

async fn event_storage_mode(db: &LocalDb, id: &'static str) -> Option<String> {
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT storage_mode FROM events WHERE id = ?1", (id,))
                .await?;
            let row = rows.next().await?.expect("event present");
            row.opt_text(0)
        })
    })
    .await
    .unwrap()
}

/// A team run (the handle carries a content store) offloads system-prompt
/// segment blobs to the shared store by hash, writes NO `archival_blobs` rows
/// to the synced replica, and reconstructs byte-identically by fetching the
/// segment bytes back from the store.
#[tokio::test]
async fn team_run_offloads_system_blobs_and_reconstructs_from_store() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"x\n");
    commit_all(repo, "base");

    let mut db = migrated_test_db("archival-team-blobs.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        None,
        None,
        false,
    )
    .await;

    let store = crate::storage::InMemoryContentStore::new();
    db.set_team_context(crate::storage::TeamReplicaContext {
        team_id: "team-x".to_string(),
        store: std::sync::Arc::new(store.clone()),
        private_db: None,
    });

    let backend_base = "CLAUDE-BASE ".repeat(800);
    let cairn = format!("\n\n{}", "CAIRN-PROMPT ".repeat(700));
    let workspace = "\n\n## Workspace Instructions\n\nworkspace doctrine".to_string();
    let agent = "\n\n<agent_role>\nbuilder role body".to_string();
    let dyn1 = "\n\n## Orientation\n\ncwd=/work/run-1\n</agent_role>".to_string();
    let statics: [(&str, &str); 4] = [
        ("backend_base", &backend_base),
        ("cairn", &cairn),
        ("workspace", &workspace),
        ("agent", &agent),
    ];
    let (data1, content1) = system_prompt(
        &statics
            .iter()
            .copied()
            .chain([("dynamic", dyn1.as_str())])
            .collect::<Vec<_>>(),
    );
    insert_event(&db, "sp1", "run", 1, 1, "system:prompt", &data1).await;

    let summary = archive_target(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(summary.system_prompt, 1);
    assert_eq!(
        blob_count(&db).await,
        0,
        "team run writes NO archival_blobs to the replica"
    );
    assert_eq!(
        store.len().await,
        4,
        "the four static segments are offloaded to the shared store"
    );

    let events = load_events(&db).await;
    let recon = reconstruct_events(&db, events).await;
    let sp = recon.iter().find(|e| e.id == "sp1").unwrap();
    let value: Value = serde_json::from_str(&sp.data).unwrap();
    assert_eq!(
        value["content"].as_str().unwrap(),
        content1,
        "segments fetched from the store reconstruct byte-identically"
    );
}

/// A team run offloads the range pack to the store as a single framed object
/// and writes only a `pack_hash` pointer (pack/pack_idx NULL) into the synced
/// replica. A gitcoord read reconstructs byte-identically by fetching the pack
/// from the store; a store that lacks the object degrades to a labeled stub.
#[tokio::test]
async fn team_run_offloads_pack_and_reconstructs_gitcoord_from_store() {
    let fx = build_fixture();
    let mut db = migrated_test_db("archival-team-pack.db").await;
    seed_chain(
        &db,
        fx.origin.to_str().unwrap(),
        fx.clone.to_str().unwrap(),
        Some(&fx.anchor),
        Some(&fx.anchor),
        false,
    )
    .await;
    db.execute(
        "UPDATE projects SET repository_id = 'repo-coordinate' WHERE id = 'proj'",
        (),
    )
    .await
    .unwrap();

    let store = crate::storage::InMemoryContentStore::new();
    db.set_team_context(crate::storage::TeamReplicaContext {
        team_id: "team-x".to_string(),
        store: std::sync::Arc::new(store.clone()),
        private_db: None,
    });

    crate::mcp::vcs::publish_sealed_commit_pack(&db, "proj", &fx.clone, &fx.w1)
        .await
        .unwrap();
    assert_eq!(
        db.query_opt_i64(
            "SELECT COUNT(*) FROM pack_catalog c
             JOIN pack_catalog_references r
               ON r.content_hash = c.content_hash
              AND r.project_id = c.project_id
              AND r.repository_id = c.repository_id
              AND r.object_format = c.object_format
             WHERE c.repository_id = 'repo-coordinate'
               AND c.kind = 'reachable' AND c.tip_commit = ?1
               AND r.owner_kind = 'sealed_commit' AND r.owner_id = ?1",
            (fx.w1.clone(),),
        )
        .await
        .unwrap(),
        Some(1),
        "the first cloud-visible sealed root gets self-contained reachable coverage"
    );
    git(&fx.clone, &["reset", "--hard", &fx.w1]);
    write_file(&fx.clone, "sealed-next.txt", b"next sealed root\n");
    let sealed_next = commit_all(&fx.clone, "next sealed root");
    crate::mcp::vcs::publish_sealed_commit_pack(&db, "proj", &fx.clone, &sealed_next)
        .await
        .unwrap();
    assert_eq!(
        db.query_opt_i64(
            "SELECT COUNT(*) FROM pack_catalog
             WHERE repository_id = 'repo-coordinate'
               AND kind = 'execution_range' AND base_commit = ?1 AND tip_commit = ?2",
            (fx.w1.clone(), sealed_next),
        )
        .await
        .unwrap(),
        Some(1),
        "a later sealed root chains a complete range from its covered parent"
    );

    let v2: &[u8] = b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n";
    let w1_short = short(&fx.clone, &fx.w1);
    let targets: Vec<(&str, &[u8])> = vec![("file:a.txt", v2)];
    let stored_w1 = rendered(&targets);

    // Write advances the replay tracker to W1; the post-write read of
    // a.txt=v2 resolves only from the range pack (W1's blob is absent from the
    // origin ODB), so it is the pack-dependent gitcoord read.
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

    let summary = archive_target(
        &db,
        fx.clone.to_str().unwrap(),
        fx.origin.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        summary.gitcoord_read, 1,
        "the post-write read is git-addressed"
    );

    let (pack_null, pack_hash) = pack_pointer(&db).await;
    assert!(pack_null, "pack/pack_idx are NULL on the team replica");
    let pack_hash = pack_hash.expect("pack_hash pointer written");
    assert!(
        store.contains(&pack_hash).await,
        "the framed pack is offloaded to the store under pack_hash"
    );
    let catalog_count = db
        .query_opt_i64(
            "SELECT COUNT(*) FROM pack_catalog c
             JOIN pack_catalog_references r ON r.content_hash = c.content_hash
             WHERE c.content_hash = ?1 AND c.kind = 'execution_range'
               AND c.publication_state = 'published'
               AND r.owner_kind = 'execution_history' AND r.owner_id = 'exec'",
            (pack_hash.clone(),),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        catalog_count, 1,
        "the pointer and catalog reference publish together"
    );
    assert_eq!(
        db.query_opt_i64(
            "SELECT COUNT(*) FROM pack_catalog
             WHERE content_hash = ?1 AND repository_id = 'repo-coordinate'",
            (pack_hash.clone(),),
        )
        .await
        .unwrap(),
        Some(1),
        "archival publishes under the durable repository coordinate, not project_id"
    );

    db.execute(
        "DELETE FROM pack_catalog WHERE content_hash = ?1",
        (pack_hash.clone(),),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO executions(id, recipe_id, status, started_at)
         VALUES ('aaa-missing', 'r', 'complete', 1)",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs(id, execution_id, project_id, status, created_at, updated_at)
         VALUES ('aaa-missing-job', 'aaa-missing', 'proj', 'complete', 1, 1)",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO execution_history
         (execution_id, base_sha, tip_sha, pack_hash, repository_id)
         VALUES ('aaa-missing', ?1, ?2, ?3, 'repo-coordinate')",
        (
            fx.anchor.as_str(),
            fx.w1.as_str(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        crate::storage::pack_catalog::backfill_execution_pack_catalog(&db, 1)
            .await
            .unwrap(),
        0,
        "the first bounded pass records the early missing pointer"
    );
    assert_eq!(
        crate::storage::pack_catalog::backfill_execution_pack_catalog(&db, 1)
            .await
            .unwrap(),
        1,
        "a recorded missing pointer cannot starve the later valid pointer"
    );

    let fake_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let framed = store.get(&pack_hash).await.unwrap().unwrap();
    let (pack, _) = cairn_codec::transfer::unframe_pack(&framed).unwrap();
    let validated =
        cairn_codec::transfer::validate_pack(&pack, cairn_codec::transfer::PackLimits::default())
            .unwrap();
    crate::storage::pack_catalog::publish_pack(
        &db,
        crate::storage::pack_catalog::PackCatalogPublication {
            content_hash: fake_hash.into(),
            project_id: "proj".into(),
            repository_id: "repo-coordinate".into(),
            object_format: "sha1".into(),
            byte_count: framed.len() as u64,
            pack_checksum: validated.manifest.pack_checksum,
            object_count: validated.manifest.object_count,
            kind: crate::storage::pack_catalog::PackKind::ExecutionRange,
            base_commit: Some(fx.anchor.clone()),
            tip_commit: fx.w1.clone(),
            owner_kind: "execution_history".into(),
            owner_id: "exec".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        db.query_opt_i64(
            "SELECT COUNT(*) FROM pack_catalog_references
             WHERE owner_kind = 'execution_history' AND owner_id = 'exec'",
            (),
        )
        .await
        .unwrap(),
        Some(1),
        "an owner replacement leaves exactly one reference in the GC mark set"
    );
    assert_eq!(
        db.query_opt_i64(
            "SELECT COUNT(*) FROM pack_catalog_references
             WHERE owner_kind = 'execution_history' AND owner_id = 'exec'
               AND content_hash = ?1",
            (fake_hash,),
        )
        .await
        .unwrap(),
        Some(1),
        "the owner now marks only the replacement pack"
    );
    assert_eq!(
        crate::storage::pack_catalog::backfill_execution_pack_catalog(&db, 1)
            .await
            .unwrap(),
        1,
        "a stale owner reference does not suppress repair of the live pack_hash"
    );
    assert_eq!(
        db.query_opt_i64(
            "SELECT COUNT(*) FROM pack_catalog_references
             WHERE owner_kind = 'execution_history' AND owner_id = 'exec'
               AND content_hash = ?1",
            (pack_hash.clone(),),
        )
        .await
        .unwrap(),
        Some(1),
        "repair restores the sole mark to the canonical execution pointer"
    );

    let events = load_events(&db).await;
    let recon = reconstruct_events(&db, events).await;
    let r2 = recon.iter().find(|e| e.id == "e-r2").unwrap();
    assert_eq!(
        tool_result_of(r2),
        stored_w1,
        "gitcoord read reconstructs byte-identically via the store-fetched pack"
    );

    // A store missing the object (a teammate without it, or a dropped object)
    // degrades to a labeled stub rather than erroring.
    let empty = crate::storage::InMemoryContentStore::new();
    db.set_team_context(crate::storage::TeamReplicaContext {
        team_id: "team-x".to_string(),
        store: std::sync::Arc::new(empty),
        private_db: None,
    });
    let events = load_events(&db).await;
    let recon = reconstruct_events(&db, events).await;
    let r2 = recon.iter().find(|e| e.id == "e-r2").unwrap();
    assert!(
        tool_result_of(r2).contains(STUB_PREFIX),
        "a missing pack object degrades to a labeled stub"
    );

    // Integrity: a store object whose bytes do NOT hash to pack_hash (corrupt
    // object, broker/backend bug, tampering) is rejected and treated as
    // absent, so a teammate never reconstructs from bytes that are not the
    // object the pointer names.
    let corrupt = crate::storage::InMemoryContentStore::new();
    corrupt
        .put(&pack_hash, b"these bytes do not hash to pack_hash")
        .await
        .unwrap();
    db.set_team_context(crate::storage::TeamReplicaContext {
        team_id: "team-x".to_string(),
        store: std::sync::Arc::new(corrupt),
        private_db: None,
    });
    let events = load_events(&db).await;
    let recon = reconstruct_events(&db, events).await;
    let r2 = recon.iter().find(|e| e.id == "e-r2").unwrap();
    assert!(
        tool_result_of(r2).contains(STUB_PREFIX),
        "a corrupt pack object (wrong bytes under pack_hash) degrades to a stub"
    );
}

/// Fail-soft: a store `put` failure aborts archival before any DB write
/// (put-before-commit), so the event rows stay `full` and no `archival_blobs`
/// row is written. The teardown caller logs the error and proceeds.
#[tokio::test]
async fn team_run_store_put_failure_leaves_rows_full() {
    struct FailingStore;
    #[async_trait::async_trait]
    impl crate::storage::ContentStore for FailingStore {
        async fn put(&self, _: &str, _: &[u8]) -> Result<(), String> {
            Err("store unavailable".to_string())
        }
        async fn get(&self, _: &str) -> Result<Option<Vec<u8>>, String> {
            Ok(None)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"x\n");
    commit_all(repo, "base");
    let mut db = migrated_test_db("archival-team-failsoft.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        None,
        None,
        false,
    )
    .await;
    db.set_team_context(crate::storage::TeamReplicaContext {
        team_id: "team-x".to_string(),
        store: std::sync::Arc::new(FailingStore),
        private_db: None,
    });

    let backend_base = "CLAUDE-BASE ".repeat(800);
    let cairn = format!("\n\n{}", "CAIRN-PROMPT ".repeat(700));
    let agent = "\n\n<agent_role>\nbuilder role body".to_string();
    let (data1, _content1) = system_prompt(&[
        ("backend_base", backend_base.as_str()),
        ("cairn", cairn.as_str()),
        ("agent", agent.as_str()),
        ("dynamic", "\n\n## Orientation\n\ntail\n</agent_role>"),
    ]);
    insert_event(&db, "sp1", "run", 1, 1, "system:prompt", &data1).await;

    let result = archive_target(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await;
    assert!(
        result.is_err(),
        "a store put failure fails archival so teardown leaves the rows full"
    );
    assert_eq!(blob_count(&db).await, 0, "no archival_blobs written");
    assert_eq!(
        event_storage_mode(&db, "sp1").await.as_deref(),
        Some("full"),
        "the event row is left full when the offload fails"
    );
}
