use super::testsupport::*;
use super::*;
use serde_json::json;

/// Regression for CAIRN-1538: real sessions record MCP-prefixed tool names
/// (`mcp__cairn__read`, `mcp__cairn__write`, `mcp__cairn__run`), not the bare
/// verbs the classifier matched on. Without prefix normalization every result
/// falls to zstd and the gitcoord counts stay zero. Both backends emit the
/// `__`-joined form; the dotted `mcp__cairn.read` variant is exercised too so
/// the normalizer's delimiter tolerance is covered.
#[tokio::test]
async fn mcp_prefixed_names_classify_to_gitcoord() {
    let fx = build_fixture();
    let db = migrated_test_db("archival-mcp-prefixed.db").await;
    seed_chain(
        &db,
        fx.origin.to_str().unwrap(),
        fx.clone.to_str().unwrap(),
        Some(&fx.anchor),
        Some(&fx.anchor),
        false,
    )
    .await;

    let b_txt = b"unchanged-keep\n";
    let stored_b = rendered(&[("file:dir/b.txt", b_txt)]);
    let w1_short = short(&fx.clone, &fx.w1);

    // Claude-form read, then the dotted Codex-style variant, both at the
    // anchor (current == base), then a prefixed write that committed W1.
    insert_event(
        &db,
        "a1",
        "run",
        1,
        1,
        "assistant",
        &assistant_tool(
            "r1",
            "mcp__cairn__read",
            json!({ "paths": ["file:dir/b.txt"] }),
        ),
    )
    .await;
    insert_event(
        &db,
        "e1",
        "run",
        2,
        2,
        "tool_result",
        &read_result("r1", &stored_b),
    )
    .await;
    insert_event(
        &db,
        "a2",
        "run",
        3,
        3,
        "assistant",
        &assistant_tool(
            "r2",
            "mcp__cairn.read",
            json!({ "paths": ["file:dir/b.txt"] }),
        ),
    )
    .await;
    insert_event(
        &db,
        "e2",
        "run",
        4,
        4,
        "tool_result",
        &read_result("r2", &stored_b),
    )
    .await;
    insert_event(
        &db,
        "a3",
        "run",
        5,
        5,
        "assistant",
        &assistant_tool(
            "w1",
            "mcp__cairn__write",
            json!({
                "changes": [{ "target": "file:a.txt", "mode": "patch",
                    "payload": { "diff": "@@ heavy diff payload @@" } }], "commit_msg": "w" }),
        ),
    )
    .await;
    insert_event(
        &db,
        "e3",
        "run",
        6,
        6,
        "tool_result",
        &write_result("w1", &w1_short),
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
        summary.gitcoord_read, 2,
        "both prefix forms normalize to a read"
    );
    assert_eq!(
        summary.gitcoord_write, 1,
        "prefixed write normalizes and classifies"
    );
    assert_eq!(summary.mismatch_fallback, 0);

    // Reconstruct: the short result is verbatim, the assistant payload regen'd.
    let events = load_events(&db).await;
    let reconstructed = reconstruct_events(&db, events).await;
    let by_id: HashMap<&str, &Event> = reconstructed.iter().map(|e| (e.id.as_str(), e)).collect();
    assert_eq!(by_id["e3"].data, write_result("w1", &w1_short));
    let recon: Value = serde_json::from_str(&by_id["a3"].data).unwrap();
    let diff = recon["toolUses"][0]["input"]["changes"][0]["payload"]["diff"]
        .as_str()
        .unwrap();
    assert!(!diff.contains("heavy diff payload"), "heavy payload gone");
    assert!(
        diff.contains("diff --git a/a.txt b/a.txt"),
        "committed diff re-injected, got: {diff}"
    );
}

/// A single-tool-use write carries its input twice (`toolUses[0].input` and
/// the backwards-compat top-level `toolInput`). The archived `data_blob` must
/// retain neither heavy payload copy, while reconstruction still regenerates
/// the committed diff from the coordinate.
#[tokio::test]
async fn duplicate_tool_input_archives_without_either_payload() {
    let fx = build_fixture();
    let db = migrated_test_db("archival-dup-tool-input.db").await;
    seed_chain(
        &db,
        fx.origin.to_str().unwrap(),
        fx.clone.to_str().unwrap(),
        Some(&fx.anchor),
        Some(&fx.anchor),
        false,
    )
    .await;

    let w1_short = short(&fx.clone, &fx.w1);
    insert_event(
        &db,
        "a-w1",
        "run",
        1,
        1,
        "assistant",
        &assistant_write_dup_tool_input("w1"),
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

    let summary = archive_target(
        &db,
        fx.clone.to_str().unwrap(),
        fx.origin.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(summary.gitcoord_write, 1);

    // The stored remainder (zstd in data_blob) carries neither payload copy.
    let (blob, codec) = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT data_blob, codec FROM events WHERE id = 'a-w1'", ())
                    .await?;
                let row = rows.next().await?.unwrap();
                DbResult::Ok((row.opt_blob(0)?.unwrap(), row.text(1)?))
            })
        })
        .await
        .unwrap();
    let remainder = String::from_utf8(crate::storage::decompress(&codec, &blob).unwrap()).unwrap();
    assert!(
        !remainder.contains("HEAVYPAYLOAD"),
        "archived remainder drops both payload copies"
    );
    let stored: Value = serde_json::from_str(&remainder).unwrap();
    assert!(
        stored["toolUses"][0]["input"]["changes"][0]
            .get("payload")
            .is_none(),
        "toolUses payload stripped"
    );
    assert!(
        stored["toolInput"]["changes"][0].get("payload").is_none(),
        "duplicate toolInput payload stripped"
    );

    // Reconstruction regenerates the committed diff into the toolUses changes.
    let events = load_events(&db).await;
    let reconstructed = reconstruct_events(&db, events).await;
    let recon = reconstructed.iter().find(|e| e.id == "a-w1").unwrap();
    let value: Value = serde_json::from_str(&recon.data).unwrap();
    let combined: String = value["toolUses"][0]["input"]["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["payload"]["diff"].as_str().unwrap().to_string())
        .collect();
    let expected = git(&fx.clone, &["show", "--format=", "--no-color", &fx.w1]);
    assert_eq!(combined, expected.trim_start_matches('\n'));
}

#[test]
fn normalize_tool_name_strips_prefixes() {
    assert_eq!(normalize_tool_name("mcp__cairn__read"), "read");
    assert_eq!(normalize_tool_name("mcp__cairn__write"), "write");
    assert_eq!(normalize_tool_name("mcp__cairn__run"), "run");
    // Dotted server/tool join still resolves to the bare tool.
    assert_eq!(normalize_tool_name("mcp__cairn.write"), "write");
    // A non-MCP name passes through untouched.
    assert_eq!(normalize_tool_name("read"), "read");
    assert_eq!(normalize_tool_name("bash"), "bash");
}

/// An execution whose range is empty (tip == anchor == default branch)
/// archives reads with a NULL pack and still reconstructs from the repo ODB.
#[tokio::test]
async fn empty_range_archives_with_null_pack() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"alpha\nbeta\n");
    let anchor = commit_all(repo, "base");

    let db = migrated_test_db("archival-empty-range.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        Some(&anchor),
        Some(&anchor),
        false,
    )
    .await;

    let stored = rendered(&[("file:a.txt", b"alpha\nbeta\n")]);
    insert_event(
        &db,
        "a1",
        "run",
        1,
        1,
        "assistant",
        &assistant_read("r1", &["file:a.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e1",
        "run",
        2,
        2,
        "tool_result",
        &read_result("r1", &stored),
    )
    .await;

    let summary = archive_target(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(summary.gitcoord_read, 1);

    // The execution_history row exists with a NULL pack.
    let pack_is_null = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT pack FROM execution_history WHERE execution_id = 'exec'",
                        (),
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                DbResult::Ok(row.opt_blob(0)?.is_none())
            })
        })
        .await
        .unwrap();
    assert!(pack_is_null, "empty range stores a NULL pack");

    let events = load_events(&db).await;
    let reconstructed = reconstruct_events(&db, events).await;
    let read = reconstructed.iter().find(|e| e.id == "e1").unwrap();
    assert_eq!(tool_result_of(read), stored);
}

/// No base_commit recorded → the whole execution is zstd-only, no pack, no
/// execution_history; every event still round-trips.
#[tokio::test]
async fn missing_anchor_is_zstd_only() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"alpha\n");
    commit_all(repo, "base");

    let db = migrated_test_db("archival-zstd-only.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        None,
        None,
        false,
    )
    .await;

    let stored = rendered(&[("file:a.txt", b"alpha\n")]);
    insert_event(
        &db,
        "a1",
        "run",
        1,
        1,
        "assistant",
        &assistant_read("r1", &["file:a.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e1",
        "run",
        2,
        2,
        "tool_result",
        &read_result("r1", &stored),
    )
    .await;

    let summary = archive_target(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(summary.gitcoord_read, 0);
    assert_eq!(summary.zstd, 2);

    let has_history = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT 1 FROM execution_history WHERE execution_id = 'exec'",
                        (),
                    )
                    .await?;
                DbResult::Ok(rows.next().await?.is_some())
            })
        })
        .await
        .unwrap();
    assert!(
        !has_history,
        "zstd-only execution writes no execution_history row"
    );

    let events = load_events(&db).await;
    let reconstructed = reconstruct_events(&db, events).await;
    let read = reconstructed.iter().find(|e| e.id == "e1").unwrap();
    assert_eq!(tool_result_of(read), stored);
}

/// Full-text search still finds an archived session's text: the writer
/// rewrites a `user` event to a zstd stub (its needle living in `data_blob`),
/// and the index rebuild reconstructs it before indexing.
#[tokio::test]
async fn archived_session_text_is_still_searchable() {
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"x\n");
    let anchor = commit_all(repo, "base");

    let db = migrated_test_db("archival-fts.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        Some(&anchor),
        Some(&anchor),
        false,
    )
    .await;
    insert_event(
        &db,
        "u1",
        "run",
        1,
        1,
        "user",
        &user_text("zephyr archival needle"),
    )
    .await;

    archive_target(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();

    // The user event is now a zstd stub; its text survives only in data_blob.
    let stub_data = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT data FROM events WHERE id = 'u1'", ())
                    .await?;
                rows.next().await?.unwrap().text(0)
            })
        })
        .await
        .unwrap();
    assert!(
        !stub_data.contains("zephyr"),
        "stub no longer carries the needle inline"
    );

    let index_dir = tempfile::tempdir().unwrap();
    let index = crate::storage::SearchIndex::open_or_create(index_dir.path()).unwrap();
    index.rebuild(&db).await.unwrap();
    assert_eq!(
        index.search("zephyr", None).unwrap().len(),
        1,
        "archived session text is reconstructed and indexed"
    );
}

#[test]
fn run_commit_sha_extracts_barrier_sha() {
    assert_eq!(
        run_commit_sha("output\n\n\u{2713} Committed changes (abc1234) updated PR#5").as_deref(),
        Some("abc1234")
    );
    assert_eq!(run_commit_sha("no commit here"), None);
}

#[test]
fn read_stub_pins_paths_and_drops_result() {
    let data = read_result("t1", "heavy rendered bytes");
    let stub = read_stub(&data, &["file:a.txt".to_string()]);
    let value: Value = serde_json::from_str(&stub).unwrap();
    assert_eq!(value["toolInput"]["paths"][0], "file:a.txt");
    assert!(value["toolResult"].is_null());
    // The Claude `raw.tool_use_result` duplicate of the rendered read is
    // dropped too — nulling `toolResult` alone would leave it behind.
    assert!(value["raw"]["tool_use_result"].is_null());
    assert!(!stub.contains("heavy rendered bytes"));
    assert!(!stub.contains(STUB_PREFIX));
}

/// The heavy bytes live on the *assistant* event's `toolUses[].input` (real
/// events shape — the paired tool_result carries no `toolInput`). Stripping
/// drops every change payload while keeping the call skeleton.
#[test]
fn strip_change_payloads_drops_payload_keeps_skeleton() {
    let stripped = strip_change_payloads(&assistant_write("w1"));
    assert!(
        !stripped.contains("HEAVYPAYLOAD"),
        "heavy payloads stripped"
    );
    let value: Value = serde_json::from_str(&stripped).unwrap();
    let input = &value["toolUses"][0]["input"];
    assert_eq!(input["changes"][0]["target"], "file:a.txt");
    assert_eq!(input["changes"][0]["mode"], "patch");
    assert!(
        input["changes"][0].get("payload").is_none(),
        "payload dropped"
    );
    assert!(
        input["changes"][1].get("payload").is_none(),
        "payload dropped"
    );
    assert_eq!(input["commit_msg"], "w", "call skeleton kept");
}
