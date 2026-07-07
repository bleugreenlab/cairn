use super::testsupport::*;
use super::*;
use serde_json::json;

/// The heart of the PR: a realistic event sequence is archived, then
/// reconstructed, and every classification is asserted end to end.
#[tokio::test]
async fn keystone_roundtrip() {
    let fx = build_fixture();
    let db = migrated_test_db("archival-keystone.db").await;
    seed_chain(
        &db,
        fx.origin.to_str().unwrap(),
        fx.clone.to_str().unwrap(),
        Some(&fx.anchor),
        Some(&fx.anchor),
        true,
    )
    .await;

    let v1 = b"alpha\nbeta\ngamma\ndelta\n";
    let v2 = b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n";
    let v3 = b"DRIFT\nbeta\ngamma\ndelta\nepsilon\n";
    let b_txt = b"unchanged-keep\n";
    let w1_short = short(&fx.clone, &fx.w1);

    // Anchor read: full file + single-file grep + an unchanged file (repo
    // layer). current == base == anchor.
    let anchor_targets: Vec<(&str, &[u8])> = vec![
        ("file:a.txt", v1),
        ("file:a.txt?grep=beta", v1),
        ("file:dir/b.txt", b_txt),
    ];
    let stored_anchor = rendered(&anchor_targets);
    // Post-write read at W1.
    let stored_w1 = rendered(&[("file:a.txt", v2)]);
    // Drift read: stored bytes are the real (drifted) HEAD; the tracker is
    // stale at W1, so render-and-compare must fail.
    let stored_drift = rendered(&[("file:a.txt", v3)]);
    // Dirty read: bytes match no committed state at W1.
    let dirty = b"ALPHA\nbeta\ngamma\ndelta\nepsilon\nuncommitted\n";
    let stored_dirty = rendered(&[("file:a.txt", dirty)]);
    // Task read of the unchanged file at W1 (multi-run; repo-layer blob).
    let stored_task = rendered(&[("file:dir/b.txt", b_txt)]);

    // run "run": chronological created_at order.
    insert_event(
        &db,
        "a-r1",
        "run",
        1,
        1,
        "assistant",
        &assistant_read(
            "r1",
            &["file:a.txt", "file:a.txt?grep=beta", "file:dir/b.txt"],
        ),
    )
    .await;
    insert_event(
        &db,
        "e-r1",
        "run",
        2,
        2,
        "tool_result",
        &read_result("r1", &stored_anchor),
    )
    .await;
    insert_event(
        &db,
        "a-w1",
        "run",
        3,
        3,
        "assistant",
        &assistant_write("w1"),
    )
    .await;
    insert_event(
        &db,
        "e-w1",
        "run",
        4,
        4,
        "tool_result",
        &write_result("w1", &w1_short),
    )
    .await;
    // Task run interleaves here (current == W1).
    insert_event(
        &db,
        "a-t1",
        "taskrun",
        1,
        5,
        "assistant",
        &assistant_read("t1", &["file:dir/b.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e-t1",
        "taskrun",
        2,
        6,
        "tool_result",
        &read_result("t1", &stored_task),
    )
    .await;
    insert_event(
        &db,
        "a-r2",
        "run",
        5,
        7,
        "assistant",
        &assistant_read("r2", &["file:a.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e-r2",
        "run",
        6,
        8,
        "tool_result",
        &read_result("r2", &stored_w1),
    )
    .await;
    // Drift run: no "Committed changes" marker; the tracker stays at W1.
    insert_event(
        &db,
        "a-run",
        "run",
        7,
        9,
        "assistant",
        &assistant_run("run1"),
    )
    .await;
    insert_event(
        &db,
        "e-run",
        "run",
        8,
        10,
        "tool_result",
        &read_result("run1", "ran a command"),
    )
    .await;
    insert_event(
        &db,
        "a-r3",
        "run",
        9,
        11,
        "assistant",
        &assistant_read("r3", &["file:a.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e-r3",
        "run",
        10,
        12,
        "tool_result",
        &read_result("r3", &stored_drift),
    )
    .await;
    insert_event(
        &db,
        "a-r4",
        "run",
        11,
        13,
        "assistant",
        &assistant_read("r4", &["file:a.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e-r4",
        "run",
        12,
        14,
        "tool_result",
        &read_result("r4", &stored_dirty),
    )
    .await;
    insert_event(
        &db,
        "a-r5",
        "run",
        13,
        15,
        "assistant",
        &assistant_read("r5", &["file:outside.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e-r5",
        "run",
        14,
        16,
        "tool_result",
        &read_result("r5", "out of repo content"),
    )
    .await;
    insert_event(
        &db,
        "a-text",
        "run",
        15,
        17,
        "assistant",
        &assistant_text("thinking out loud"),
    )
    .await;
    // Extended-thinking and backend system events fall to plain zstd and must
    // round-trip verbatim.
    insert_event(
        &db,
        "a-think",
        "run",
        16,
        18,
        "assistant",
        &assistant_thinking("planning the next step"),
    )
    .await;
    insert_event(
        &db,
        "sys",
        "run",
        17,
        19,
        "system:init",
        &system_event("init"),
    )
    .await;

    let summary = archive_target(
        &db,
        fx.clone.to_str().unwrap(),
        fx.origin.to_str().unwrap(),
        &["job".to_string(), "taskjob".to_string()],
        None,
    )
    .await
    .unwrap();

    assert_eq!(summary.gitcoord_read, 3, "anchor + W1 + task reads");
    assert_eq!(summary.gitcoord_write, 1);
    assert_eq!(summary.mismatch_fallback, 2, "drift + dirty reads");
    assert!(summary.bytes_before > 0 && summary.bytes_after > 0);

    // The heavy rendered payload is dropped from a gitcoord-read stub (the
    // storage win; net byte totals only shrink at real payload sizes, where
    // zstd's per-frame overhead on many small events no longer dominates).
    let stub_r2 = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT data FROM events WHERE id = 'e-r2'", ())
                    .await?;
                rows.next().await?.unwrap().text(0)
            })
        })
        .await
        .unwrap();
    assert!(
        stub_r2.len() < read_result("r2", &stored_w1).len(),
        "gitcoord stub drops the rendered bytes"
    );
    assert!(
        !stub_r2.contains("epsilon"),
        "the gitcoord stub retains no read content bytes — not even Claude's \
             raw.tool_use_result duplicate of the rendered read"
    );

    // Reconstruct everything and index by id.
    let events = load_events(&db).await;
    let reconstructed = reconstruct_events(&db, events).await;
    let by_id: HashMap<&str, &Event> = reconstructed.iter().map(|e| (e.id.as_str(), e)).collect();

    // gitcoord reads are byte-identical with matching render shas.
    assert_eq!(tool_result_of(by_id["e-r1"]), stored_anchor);
    assert_eq!(tool_result_of(by_id["e-r2"]), stored_w1);
    assert_eq!(tool_result_of(by_id["e-t1"]), stored_task);

    // The short write result (e-w1) round-trips byte-for-byte: it IS the
    // change summary, never replaced by a regenerated diff.
    assert_eq!(by_id["e-w1"].data, write_result("w1", &w1_short));

    // The assistant event (a-w1) had its heavy change payloads stripped: the
    // remainder stored in data_blob no longer carries them.
    let remainder_a_w1 = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT data_blob, codec FROM events WHERE id = 'a-w1'", ())
                    .await?;
                let row = rows.next().await?.unwrap();
                let blob = row.opt_blob(0)?.unwrap();
                let codec: String = row.text(1)?;
                DbResult::Ok((blob, codec))
            })
        })
        .await
        .unwrap();
    let decompressed = String::from_utf8(
        crate::storage::decompress(&remainder_a_w1.1, &remainder_a_w1.0).unwrap(),
    )
    .unwrap();
    assert!(
        !decompressed.contains("HEAVYPAYLOAD"),
        "archived assistant remainder drops the heavy payloads"
    );

    // Reconstruction re-injects the committed per-change diff where each
    // payload was; concatenated in change order they reproduce `git show W1`.
    let recon_w1: Value = serde_json::from_str(&by_id["a-w1"].data).unwrap();
    let changes = recon_w1["toolUses"][0]["input"]["changes"]
        .as_array()
        .unwrap();
    let combined: String = changes
        .iter()
        .map(|c| c["payload"]["diff"].as_str().unwrap().to_string())
        .collect();
    let expected_diff = git(&fx.clone, &["show", "--format=", "--no-color", &fx.w1]);
    let expected_diff = expected_diff.trim_start_matches('\n');
    assert_eq!(combined, expected_diff);

    // drift + dirty reads landed zstd and round-trip their stored bytes.
    assert_eq!(tool_result_of(by_id["e-r3"]), stored_drift);
    assert_eq!(tool_result_of(by_id["e-r4"]), stored_dirty);
    // out-of-repo read fell to plain zstd (not a mismatch) and round-trips.
    assert_eq!(tool_result_of(by_id["e-r5"]), "out of repo content");
    // run output round-trips.
    assert_eq!(tool_result_of(by_id["e-run"]), "ran a command");
    // model text, thinking, and system events round-trip exactly.
    assert_eq!(by_id["a-text"].data, assistant_text("thinking out loud"));
    assert_eq!(
        by_id["a-think"].data,
        assistant_thinking("planning the next step")
    );
    assert_eq!(by_id["sys"].data, system_event("init"));

    // The render-sha tripwire was captured for a gitcoord read.
    let render_sha = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT content_render_sha FROM events WHERE id = 'e-r2'",
                        (),
                    )
                    .await?;
                DbResult::Ok(
                    rows.next()
                        .await?
                        .and_then(|r| r.opt_text(0).ok().flatten()),
                )
            })
        })
        .await
        .unwrap();
    assert_eq!(render_sha, Some(sha256_hex(stored_w1.as_bytes())));

    // A second archival pass is a no-op (rows already archived).
    let again = archive_target(
        &db,
        fx.clone.to_str().unwrap(),
        fx.origin.to_str().unwrap(),
        &["job".to_string(), "taskjob".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(again.gitcoord_read, 0);
    assert_eq!(again.zstd, 0);
}

/// The genuine de-circularization: the stored `toolResult` is produced by the
/// REAL [`handle_read_batch`](crate::mcp::handlers::read::handle_read_batch) over
/// real worktree files — not the `render_targets` fixture — so this proves the
/// live producer + shared `assemble` and the archival reconstruction agree
/// byte-for-byte across the rewrite/replay round trip. Covers a full small
/// file, a windowed read with a continue footer, a single-file grep, an empty
/// file, and a multi-target batch in one event — plus a file edited after the
/// read, which must fall to zstd rather than a false gitcoord match.
#[tokio::test]
async fn live_read_batch_round_trips_through_archival() {
    use crate::db::DbState;
    use crate::mcp::handlers::read::handle_read_batch;
    use crate::mcp::types::McpCallbackRequest;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use cairn_common::read::ReadBatchEnvelope;
    use std::collections::HashMap as Map;
    use std::sync::{Arc, Mutex};

    // A real repo at one clean commit; the live worktree is a fresh clone
    // checked out at that commit, so every live file read resolves to the
    // committed blob and archival can address it at `anchor`.
    let origin_dir = tempfile::tempdir().unwrap();
    let origin = origin_dir.path().to_path_buf();
    init_repo(&origin);
    let big: String = (1..=40)
        .map(|i| format!("line {i}"))
        .collect::<Vec<_>>()
        .join("\n");
    write_file(&origin, "big.rs", big.as_bytes());
    write_file(&origin, "small.rs", b"hello\nworld\n");
    write_file(
        &origin,
        "needle.rs",
        b"alpha\nNEEDLE\ngamma\nNEEDLE again\n",
    );
    write_file(&origin, "empty.rs", b"");
    let anchor = commit_all(&origin, "base");

    let clone_dir = tempfile::tempdir().unwrap();
    let clone = clone_dir.path().to_path_buf();
    git(
        &origin,
        &[
            "clone",
            "-q",
            origin.to_str().unwrap(),
            clone.to_str().unwrap(),
        ],
    );

    // Build a throwaway orchestrator whose only job is to run the live read
    // over the worktree files (file reads never touch the DB).
    let live_db = migrated_test_db("archival-live-orch.db").await;
    let search =
        Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
    let db_state = Arc::new(DbState::new(Arc::new(live_db), search));
    let orch = OrchestratorBuilder::new(
        db_state,
        Arc::new(TestServicesBuilder::new().build()),
        tempfile::tempdir().unwrap().keep(),
    )
    .build();
    let cursors = Mutex::new(Map::new());
    let live_text = |paths: serde_json::Value| {
        let orch = &orch;
        let cursors = &cursors;
        let clone = clone.clone();
        async move {
            let request = McpCallbackRequest {
                thread_id: None,
                cwd: clone.display().to_string(),
                run_id: None,
                tool: "read_batch".to_string(),
                payload: json!({ "paths": paths }),
                tool_use_id: None,
            };
            let raw = handle_read_batch(orch, &request, cursors).await;
            serde_json::from_str::<ReadBatchEnvelope>(&raw)
                .unwrap()
                .text
        }
    };

    let batch_paths = [
        "file:small.rs",
        "file:big.rs?offset=2&limit=3",
        "file:needle.rs?grep=NEEDLE",
        "file:empty.rs",
    ];
    let clean_text = live_text(json!(batch_paths)).await;
    // Sanity: the live envelope carries the enriched suffixes/footers we expect.
    assert!(clean_text.contains("=== file:small.rs [lines 1\u{2013}2 of 2] ==="));
    assert!(clean_text.contains("=== file:big.rs?offset=2&limit=3 [lines 3\u{2013}5 of 40] ==="));
    assert!(clean_text.contains("continue: file:big.rs?offset=5"));
    assert!(clean_text.contains("=== file:needle.rs?grep=NEEDLE [2 matches] ==="));
    assert!(clean_text.contains("=== file:empty.rs ==="));

    // A read whose bytes drift from the committed blob (here: an uncommitted
    // edit on disk) cannot be addressed by the coordinate and must fall to
    // zstd, round-tripping its stored bytes verbatim — never a false match.
    std::fs::write(clone.join("small.rs"), b"hello\nDIRTY\nworld\n").unwrap();
    let dirty_text = live_text(json!(["file:small.rs"])).await;
    assert_ne!(clean_text, dirty_text);

    // Archive the reads against the committed worktree state.
    let db = migrated_test_db("archival-live-roundtrip.db").await;
    seed_chain(
        &db,
        origin.to_str().unwrap(),
        clone.to_str().unwrap(),
        Some(&anchor),
        Some(&anchor),
        false,
    )
    .await;
    insert_event(
        &db,
        "a-r1",
        "run",
        1,
        1,
        "assistant",
        &assistant_read("r1", &batch_paths),
    )
    .await;
    insert_event(
        &db,
        "e-r1",
        "run",
        2,
        2,
        "tool_result",
        &read_result("r1", &clean_text),
    )
    .await;
    insert_event(
        &db,
        "a-r2",
        "run",
        3,
        3,
        "assistant",
        &assistant_read("r2", &["file:small.rs"]),
    )
    .await;
    insert_event(
        &db,
        "e-r2",
        "run",
        4,
        4,
        "tool_result",
        &read_result("r2", &dirty_text),
    )
    .await;

    let summary = archive_target(
        &db,
        clone.to_str().unwrap(),
        origin.to_str().unwrap(),
        &["job".to_string()],
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        summary.gitcoord_read, 1,
        "the clean multi-target live read archives to a git coordinate, not mismatch_fallback"
    );
    assert_eq!(
        summary.mismatch_fallback, 1,
        "the post-edit read cannot match its coordinate and falls to zstd"
    );

    // Reconstruct and assert byte equality with the live reads.
    let events = load_events(&db).await;
    let reconstructed = reconstruct_events(&db, events).await;
    let by_id: HashMap<&str, &Event> = reconstructed.iter().map(|e| (e.id.as_str(), e)).collect();
    assert_eq!(
        tool_result_of(by_id["e-r1"]),
        clean_text,
        "reconstruction is byte-identical to the live multi-target read"
    );
    assert_eq!(
        tool_result_of(by_id["e-r2"]),
        dirty_text,
        "the zstd-backstopped read round-trips its stored bytes verbatim"
    );
}

/// A mixed read batch — a reproducible in-repo file plus a verbatim resource
/// section — archives per-section: the file section is git-addressed, the
/// resource stays verbatim in the skeleton, and reconstruction is byte-exact.
#[tokio::test]
async fn mixed_read_batch_archives_hybrid() {
    use crate::storage::event_fixture::{mixed_render_targets, MixedSection};
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"alpha\nbeta\ngamma\n");
    let anchor = commit_all(repo, "base");

    let db = migrated_test_db("archival-hybrid-mixed.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        Some(&anchor),
        Some(&anchor),
        false,
    )
    .await;

    let stored = mixed_render_targets(&[
        MixedSection::File("file:a.txt", b"alpha\nbeta\ngamma\n"),
        MixedSection::Resource("cairn://p/P/1", "Issue overview\nrelevant context"),
    ]);
    insert_event(
        &db,
        "a1",
        "run",
        1,
        1,
        "assistant",
        &assistant_read("r1", &["file:a.txt", "cairn://p/P/1"]),
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
    assert_eq!(
        summary.hybrid_read, 1,
        "the mixed batch coordinatizes its file section"
    );
    assert_eq!(summary.gitcoord_read, 0);

    let events = load_events(&db).await;
    let row = events.iter().find(|e| e.id == "e1").unwrap();
    assert_eq!(row.storage_mode.as_deref(), Some("gitcoord"));
    assert!(row.content_commit.is_some());
    assert!(
        row.content_render_sha.is_some(),
        "the drift sha over the full original bytes"
    );
    assert!(row.data_blob.is_some(), "the skeleton lives in data_blob");
    assert!(
        row.data_blob.as_ref().unwrap().len() < stored.len(),
        "the compressed skeleton is smaller than the original composed result"
    );

    let reconstructed = reconstruct_events(&db, events).await;
    let read = reconstructed.iter().find(|e| e.id == "e1").unwrap();
    assert_eq!(
        tool_result_of(read),
        stored,
        "hybrid reconstruction is byte-identical to the live mixed read"
    );
}

/// Per-section degradation: a batch with one committed file and one file-shaped
/// target absent from the commit coordinatizes only the resolvable section and
/// leaves the other verbatim, recording exactly that section index.
#[tokio::test]
async fn hybrid_read_degrades_unresolvable_file_sections_verbatim() {
    use crate::storage::event_fixture::{mixed_render_targets, MixedSection};
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"alpha\nbeta\n");
    let anchor = commit_all(repo, "base");

    let db = migrated_test_db("archival-hybrid-degrade.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        Some(&anchor),
        Some(&anchor),
        false,
    )
    .await;

    // ghost.txt is absent from the commit, so its section cannot be regenerated
    // and must stay verbatim in the skeleton.
    let stored = mixed_render_targets(&[
        MixedSection::File("file:a.txt", b"alpha\nbeta\n"),
        MixedSection::File("file:ghost.txt", b"phantom\nrows\n"),
    ]);
    insert_event(
        &db,
        "a1",
        "run",
        1,
        1,
        "assistant",
        &assistant_read("r1", &["file:a.txt", "file:ghost.txt"]),
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
    assert_eq!(summary.hybrid_read, 1);

    let events = load_events(&db).await;
    let row = events.iter().find(|e| e.id == "e1").unwrap();
    let data: Value = serde_json::from_str(&row.data).unwrap();
    let sections: Vec<u64> = data["sections"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert_eq!(
        sections,
        vec![0],
        "only the committed file section is coordinatized"
    );

    let reconstructed = reconstruct_events(&db, events).await;
    let read = reconstructed.iter().find(|e| e.id == "e1").unwrap();
    assert_eq!(
        tool_result_of(read),
        stored,
        "byte-identical despite the verbatim degraded section"
    );
}

/// A pure-resource batch (no reproducible file section) still falls to plain
/// zstd, never hybrid, and round-trips its stored bytes verbatim.
#[tokio::test]
async fn pure_resource_batch_falls_to_zstd() {
    use crate::storage::event_fixture::{mixed_render_targets, MixedSection};
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path();
    init_repo(repo);
    write_file(repo, "a.txt", b"alpha\n");
    let anchor = commit_all(repo, "base");

    let db = migrated_test_db("archival-pure-resource.db").await;
    seed_chain(
        &db,
        repo.to_str().unwrap(),
        repo.to_str().unwrap(),
        Some(&anchor),
        Some(&anchor),
        false,
    )
    .await;

    let stored = mixed_render_targets(&[
        MixedSection::Resource("cairn://p/P/1", "issue one"),
        MixedSection::Resource("cairn://p/P/2", "issue two"),
    ]);
    insert_event(
        &db,
        "a1",
        "run",
        1,
        1,
        "assistant",
        &assistant_read("r1", &["cairn://p/P/1", "cairn://p/P/2"]),
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
    assert_eq!(summary.hybrid_read, 0);
    assert_eq!(summary.gitcoord_read, 0);

    let events = load_events(&db).await;
    let row = events.iter().find(|e| e.id == "e1").unwrap();
    assert_eq!(
        row.storage_mode.as_deref(),
        Some("zstd"),
        "the pure-resource result falls to plain zstd"
    );

    let reconstructed = reconstruct_events(&db, events).await;
    let read = reconstructed.iter().find(|e| e.id == "e1").unwrap();
    assert_eq!(tool_result_of(read), stored);
}
