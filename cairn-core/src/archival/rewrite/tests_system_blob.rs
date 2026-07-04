use super::testsupport::*;
use super::*;
use serde_json::json;

async fn event_data_of(db: &LocalDb, id: &'static str) -> String {
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT data FROM events WHERE id = ?1", (id,))
                .await?;
            rows.next().await?.unwrap().text(0)
        })
    })
    .await
    .unwrap()
}

/// Set up a git repo + seeded chain so `archive_target` can run, returning the
/// db and the repo path string. System-init archival is independent of git,
/// but `archive_target` still needs the chain and a present worktree.
async fn init_db_for(name: &'static str) -> (LocalDb, tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().to_str().unwrap().to_string();
    init_repo(dir.path());
    write_file(dir.path(), "a.txt", b"x\n");
    commit_all(dir.path(), "base");
    let db = migrated_test_db(name).await;
    seed_chain(&db, &repo, &repo, None, None, false).await;
    (db, dir, repo)
}

/// A realistic assembled prompt round-trips byte-identical through segment
/// archival; identical static segments across two runs collapse to shared blob
/// rows (only refs added the second time); a re-archival pass is a no-op.
#[tokio::test]
async fn system_prompt_segments_round_trip_and_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"x\n");
    commit_all(repo, "base");

    // No git anchors: system-prompt segmentation is independent of git, so it
    // must still fire on an otherwise zstd-only execution.
    let db = migrated_test_db("archival-sysprompt-roundtrip.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        None,
        None,
        false,
    )
    .await;

    let backend_base = "CLAUDE-BASE ".repeat(800);
    let cairn = format!("\n\n{}", "CAIRN-PROMPT ".repeat(700));
    let workspace = "\n\n## Workspace Instructions\n\nworkspace doctrine".to_string();
    let agent = "\n\n<agent_role>\nbuilder role body".to_string();
    let dyn1 = "\n\n## Orientation\n\ncwd=/work/run-1\n</agent_role>".to_string();
    let dyn2 = "\n\n## Orientation\n\ncwd=/work/run-2\n</agent_role>".to_string();

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
    let (data2, content2) = system_prompt(
        &statics
            .iter()
            .copied()
            .chain([("dynamic", dyn2.as_str())])
            .collect::<Vec<_>>(),
    );
    insert_event(&db, "sp1", "run", 1, 1, "system:prompt", &data1).await;
    insert_event(&db, "sp2", "run", 2, 2, "system:prompt", &data2).await;

    let summary = archive_target(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(summary.system_prompt, 2);
    // Four distinct static segments shared by both events — not eight rows.
    assert_eq!(blob_count(&db).await, 4, "identical static segments dedup");

    let stored1 = event_data_of(&db, "sp1").await;
    assert!(
        !stored1.contains("CLAUDE-BASE"),
        "static bytes moved to archival_blobs"
    );
    assert!(
        stored1.contains("cwd=/work/run-1"),
        "dynamic tail stays inline"
    );
    // The redundant `raw.segments` byte map and `raw.hash` are stripped from
    // the archived stub (the archived top-level `segments` list supersedes
    // them; byte offsets are meaningless once `content` is gone).
    let stored1_value: Value = serde_json::from_str(&stored1).unwrap();
    assert!(
        stored1_value["raw"].get("segments").is_none(),
        "redundant raw.segments stripped from the archived stub"
    );
    assert!(
        stored1_value["raw"].get("hash").is_none(),
        "redundant raw.hash stripped from the archived stub"
    );
    assert!(
        stored1_value["segments"].is_array(),
        "archived segments list is preserved"
    );
    eprintln!(
        "SYS_PROMPT_BYTES full={} archived_stub={} (blobs one-time, 4 rows)",
        content1.len(),
        stored1.len()
    );

    let events = load_events(&db).await;
    let recon = reconstruct_events(&db, events).await;
    let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
    let recon1: Value = serde_json::from_str(&by_id["sp1"].data).unwrap();
    let recon2: Value = serde_json::from_str(&by_id["sp2"].data).unwrap();
    assert_eq!(recon1["content"].as_str().unwrap(), content1);
    assert_eq!(recon2["content"].as_str().unwrap(), content2);

    // Re-archival is a no-op: rows are already gitcoord, no blobs added.
    let again = archive_target(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(again.system_prompt, 0);
    assert_eq!(blob_count(&db).await, 4, "second pass adds zero blob rows");
}

/// Corrupted boundary metadata (spans that no longer tile the content) fails
/// the byte-exact verify and the event keeps its whole bytes as zstd.
#[tokio::test]
async fn system_prompt_corrupt_boundary_falls_back_to_zstd() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"x\n");
    commit_all(repo, "base");
    let db = migrated_test_db("archival-sysprompt-corrupt.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        None,
        None,
        false,
    )
    .await;

    let (data, content) = system_prompt(&[
        ("backend_base", "AAAA"),
        ("cairn", "BBBB"),
        ("dynamic", "CCCC"),
    ]);
    // Shorten the first recorded span so the spans drop a byte.
    let mut value: Value = serde_json::from_str(&data).unwrap();
    value["raw"]["segments"][0]["byteLen"] = json!(3);
    let data = value.to_string();
    insert_event(&db, "sp1", "run", 1, 1, "system:prompt", &data).await;

    let summary = archive_target(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(summary.system_prompt, 0, "verify fails, no segmentation");
    assert_eq!(summary.zstd, 1, "falls back to whole-event zstd");
    assert_eq!(blob_count(&db).await, 0);

    let events = load_events(&db).await;
    let recon = reconstruct_events(&db, events).await;
    let sp = recon.iter().find(|e| e.id == "sp1").unwrap();
    let value: Value = serde_json::from_str(&sp.data).unwrap();
    assert_eq!(value["content"].as_str().unwrap(), content);
}

/// A missing blob row (e.g. a dropped segment) degrades to a labeled stub in
/// place; the resolvable segments and the inline tail still reconstruct.
#[tokio::test]
async fn system_prompt_missing_blob_degrades_to_stub() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"x\n");
    commit_all(repo, "base");
    let db = migrated_test_db("archival-sysprompt-missing.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        None,
        None,
        false,
    )
    .await;

    let (data, _content) = system_prompt(&[
        ("backend_base", "BACKENDBASEDATA"),
        ("cairn", "CAIRNDATA"),
        ("dynamic", "DYNTAIL"),
    ]);
    insert_event(&db, "sp1", "run", 1, 1, "system:prompt", &data).await;
    archive_target(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(blob_count(&db).await, 2);

    // Drop the cairn segment's blob.
    let cairn_hash = sha256_hex(b"CAIRNDATA");
    db.write(move |conn| {
        let cairn_hash = cairn_hash.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM archival_blobs WHERE hash = ?1",
                (cairn_hash.as_str(),),
            )
            .await?;
            DbResult::Ok(())
        })
    })
    .await
    .unwrap();

    let events = load_events(&db).await;
    let recon = reconstruct_events(&db, events).await;
    let sp = recon.iter().find(|e| e.id == "sp1").unwrap();
    let value: Value = serde_json::from_str(&sp.data).unwrap();
    let content = value["content"].as_str().unwrap();
    assert!(
        content.contains("BACKENDBASEDATA"),
        "resolvable segment intact"
    );
    assert!(
        content.contains(STUB_PREFIX),
        "missing segment becomes a labeled stub"
    );
    assert!(content.contains("DYNTAIL"), "inline dynamic tail intact");
}

/// Two Claude `system:init` events from different runs of the same machine —
/// differing only in session ids, cwd, and the CLI's per-run tool shuffle —
/// round-trip byte-exact and collapse to one shared skeleton blob plus one
/// shared (sorted) tool-set blob. A re-archival pass adds nothing.
#[tokio::test]
async fn system_init_round_trips_and_dedup() {
    let (db, _dir, repo) = init_db_for("archival-sysinit-roundtrip.db").await;

    // Same tool SET, shuffled order; different session/uuid/cwd.
    let tools_a = ["mcp__cairn__read", "Glob", "Grep", "mcp__cairn__write"];
    let tools_b = ["Grep", "mcp__cairn__write", "mcp__cairn__read", "Glob"];
    let init1 = system_init_claude("sess-1", "uuid-1", "/work/run-1", &tools_a);
    let init2 = system_init_claude("sess-2", "uuid-2", "/work/run-2", &tools_b);
    insert_event(&db, "si1", "run", 1, 1, "system:init", &init1).await;
    insert_event(&db, "si2", "run", 2, 2, "system:init", &init2).await;

    let summary = archive_target(&db, &repo, &repo, &["job".to_string()], None)
        .await
        .unwrap();
    assert_eq!(summary.system_init, 2);
    assert_eq!(summary.zstd, 0, "both inits content-addressed, none zstd");
    // One skeleton (identical config) + one tool-set (same sorted set) — not
    // four rows.
    assert_eq!(
        blob_count(&db).await,
        2,
        "skeleton + tool set dedup across runs"
    );

    let stored1 = event_data_of(&db, "si1").await;
    assert!(
        !stored1.contains("claude-sonnet"),
        "constant inventory moved to the skeleton blob"
    );
    assert!(
        !stored1.contains("Glob"),
        "tool names live in the shared tool-set blob, not the per-row stub"
    );
    assert!(stored1.contains("/work/run-1"), "cwd inlined in the stub");
    assert!(stored1.contains("sess-1"), "session id inlined in the stub");

    // Byte-exact reconstruction, including each run's original tool order.
    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
    assert_eq!(by_id["si1"].data, init1, "run 1 reconstructs byte-exact");
    assert_eq!(by_id["si2"].data, init2, "run 2 reconstructs byte-exact");

    // Re-archival is a no-op: rows are already blobbed, no blobs added.
    let again = archive_target(&db, &repo, &repo, &["job".to_string()], None)
        .await
        .unwrap();
    assert_eq!(again.system_init, 0);
    assert_eq!(blob_count(&db).await, 2, "second pass adds zero blob rows");
}

/// Codex's fixed init (only `sessionId` varies, no tools) is the degenerate
/// case: two runs collapse to one shared skeleton blob and reconstruct exactly.
#[tokio::test]
async fn system_init_codex_round_trips_and_dedups() {
    let (db, _dir, repo) = init_db_for("archival-sysinit-codex.db").await;
    let init1 = system_init_codex("codex-sess-1");
    let init2 = system_init_codex("codex-sess-2");
    insert_event(&db, "ci1", "run", 1, 1, "system:init", &init1).await;
    insert_event(&db, "ci2", "run", 2, 2, "system:init", &init2).await;

    let summary = archive_target(&db, &repo, &repo, &["job".to_string()], None)
        .await
        .unwrap();
    assert_eq!(summary.system_init, 2);
    assert_eq!(blob_count(&db).await, 1, "one shared skeleton, no tool set");

    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
    assert_eq!(by_id["ci1"].data, init1);
    assert_eq!(by_id["ci2"].data, init2);
}

/// Version-agnostic: an init recorded by a different app version (here, an
/// extra `usage` field the current struct lacks) still round-trips byte-exact,
/// because the skeleton is the literal recorded bytes with the varying spans
/// substituted — never a re-serialization of the current struct. This is what
/// lets the backfill reclaim the historical backlog.
#[tokio::test]
async fn system_init_foreign_version_round_trips() {
    let (db, _dir, repo) = init_db_for("archival-sysinit-foreign.db").await;
    let base = system_init_claude("sess-x", "uuid-x", "/work/x", &["Glob", "Grep"]);
    // Simulate an older recorder that emitted a `usage` field.
    let init = base.replace("\"isError\":false,", "\"isError\":false,\"usage\":null,");
    assert!(init.contains("\"usage\":null"));
    insert_event(&db, "sf1", "run", 1, 1, "system:init", &init).await;

    let summary = archive_target(&db, &repo, &repo, &["job".to_string()], None)
        .await
        .unwrap();
    assert_eq!(
        summary.system_init, 1,
        "foreign-version init still archives"
    );

    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let si = recon.iter().find(|e| e.id == "sf1").unwrap();
    assert_eq!(
        si.data, init,
        "foreign-version init reconstructs byte-exact"
    );
}

/// A `system:init` whose `data` is not the expected object falls back to whole-
/// event zstd and round-trips verbatim — the safety net for any shape the
/// builder cannot prove byte-exact.
#[tokio::test]
async fn system_init_unexpected_shape_falls_back_to_zstd() {
    let (db, _dir, repo) = init_db_for("archival-sysinit-fallback.db").await;
    let data = "\"a bare json string, not an init object\"";
    insert_event(&db, "sb1", "run", 1, 1, "system:init", data).await;

    let summary = archive_target(&db, &repo, &repo, &["job".to_string()], None)
        .await
        .unwrap();
    assert_eq!(summary.system_init, 0);
    assert_eq!(summary.zstd, 1, "falls back to whole-event zstd");
    assert_eq!(blob_count(&db).await, 0);

    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let sb = recon.iter().find(|e| e.id == "sb1").unwrap();
    assert_eq!(sb.data, data, "verbatim round-trip");
}

/// A dropped skeleton blob degrades to a labeled stub object (still valid JSON),
/// rather than corrupting the transcript.
#[tokio::test]
async fn system_init_missing_skeleton_degrades_to_stub() {
    let (db, _dir, repo) = init_db_for("archival-sysinit-missing.db").await;
    let init = system_init_claude("sess-m", "uuid-m", "/work/m", &["Glob", "Grep"]);
    insert_event(&db, "sm1", "run", 1, 1, "system:init", &init).await;
    archive_target(&db, &repo, &repo, &["job".to_string()], None)
        .await
        .unwrap();
    assert_eq!(blob_count(&db).await, 2);

    // Drop every blob so the skeleton can't resolve.
    db.write(|conn| {
        Box::pin(async move {
            conn.execute("DELETE FROM archival_blobs", ()).await?;
            DbResult::Ok(())
        })
    })
    .await
    .unwrap();

    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let sm = recon.iter().find(|e| e.id == "sm1").unwrap();
    let value: Value = serde_json::from_str(&sm.data).expect("degraded stub is valid json");
    assert_eq!(value["eventType"], "system:init");
    assert!(
        value["content"].as_str().unwrap().contains(STUB_PREFIX),
        "missing skeleton becomes a labeled stub"
    );
}
