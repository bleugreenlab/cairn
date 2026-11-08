use std::path::Path;

use cairn_db::turso::Value as SqlValue;

use super::*;
use crate::archival::rewrite::testsupport::*;
use crate::models::Event;
use crate::storage::events::{load_execution_pack, segment_hashes};
use crate::storage::{compress, reconstruct_events_reporting, Reconstruction};

/// A transcript with a committed write, two coordinate reads, and an assembled
/// system prompt: everything archival packs or blobs.
async fn seed_transcript(db: &LocalDb, fx: &Fixture) {
    let (prompt, _) = system_prompt(&[
        ("backend_base", &"BACKEND-BASE ".repeat(400)),
        ("cairn", &"CAIRN-PROMPT ".repeat(300)),
        ("dynamic", "\n\ncwd=/work/run-1"),
    ]);
    insert_event(db, "sp", "run", 0, 0, "system:prompt", &prompt).await;
    insert_event(db, "a-w1", "run", 1, 1, "assistant", &assistant_write("w1")).await;
    let w1_short = short(&fx.repo, &fx.w1);
    insert_event(
        db,
        "e-w1",
        "run",
        2,
        2,
        "tool_result",
        &write_result("w1", &w1_short),
    )
    .await;
    let stored_a = rendered(&[("file:a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n")]);
    let read_a = assistant_read("r1", &["file:a.txt"]);
    insert_event(db, "a-r1", "run", 3, 3, "assistant", &read_a).await;
    insert_event(
        db,
        "e-r1",
        "run",
        4,
        4,
        "tool_result",
        &read_result("r1", &stored_a),
    )
    .await;
    let stored_c = rendered(&[("file:c.txt", b"new file\n")]);
    let read_c = assistant_read("r2", &["file:c.txt"]);
    insert_event(db, "a-r2", "run", 5, 5, "assistant", &read_c).await;
    insert_event(
        db,
        "e-r2",
        "run",
        6,
        6,
        "tool_result",
        &read_result("r2", &stored_c),
    )
    .await;
}

/// Delete the work branch and collect its objects, so archived coordinates
/// resolve only through the execution's pack.
fn collect_branch(fx: &Fixture) {
    git(&fx.repo, &["checkout", "-q", "main"]);
    git(&fx.repo, &["branch", "-D", "-q", "work"]);
    git(&fx.repo, &["reflog", "expire", "--expire=now", "--all"]);
    git(&fx.repo, &["gc", "-q", "--prune=now"]);
}

/// What every reader sees: each event's reconstructed `data` and outcome.
async fn snapshot(db: &LocalDb) -> Vec<(String, String, Reconstruction)> {
    reconstruct_events_reporting(db, load_events(db).await)
        .await
        .into_iter()
        .map(|(event, outcome)| (event.id, event.data, outcome))
        .collect()
}

async fn node_diff(db: &LocalDb) -> String {
    let diff = crate::diff::node_base_tip_diff(db, "job", "", Path::new("/nonexistent"))
        .await
        .unwrap()
        .expect("the archived history renders the node diff");
    format!("{diff:?}")
}

async fn inline_pack_rows(db: &LocalDb) -> i64 {
    db.query_one(
        "SELECT COUNT(*) FROM execution_history WHERE pack IS NOT NULL",
        (),
        |row| row.i64(0),
    )
    .await
    .unwrap()
}

async fn seed_history_rows(db: &LocalDb, count: usize, inline_every: Option<usize>) {
    db.write(move |conn| {
        Box::pin(async move {
            for n in 0..count {
                let id = format!("history-{n:05}");
                conn.execute(
                    "INSERT INTO executions(id, recipe_id, status, started_at)
                     VALUES (?1, 'r', 'complete', 1)",
                    (id.clone(),),
                )
                .await?;
                let pack = if inline_every.is_some_and(|every| n % every == 0) {
                    // Only the pending probe reads these placeholder bytes.
                    SqlValue::Blob(vec![1])
                } else {
                    SqlValue::Null
                };
                conn.execute(
                    "INSERT INTO execution_history(execution_id, base_sha, tip_sha, pack)
                     VALUES (?1, 'base', 'tip', ?2)",
                    (id, pack),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn remaining_pack_probe_counts_only_pending_rows_and_caps_at_1001() {
    let db = migrated_test_db("offload-pending-probe.db").await;
    let sql = "SELECT execution_id FROM execution_history WHERE pack IS NOT NULL LIMIT ?1";
    assert_eq!(probe(&db, sql).await, Some(0));

    // Three NULL histories between candidates, including beyond the probe cap.
    seed_history_rows(&db, 4020, Some(4)).await;
    assert_eq!(inline_pack_rows(&db).await, 1005);
    assert_eq!(probe(&db, sql).await, Some(1001));

    db.execute(
        "UPDATE execution_history SET pack = NULL WHERE execution_id < 'history-04008'",
        (),
    )
    .await
    .unwrap();
    assert_eq!(inline_pack_rows(&db).await, 3);
    assert_eq!(probe(&db, sql).await, Some(3));

    for (id, remaining) in [
        ("history-04008", 2),
        ("history-04012", 1),
        ("history-04016", 0),
    ] {
        db.execute(
            "UPDATE execution_history SET pack = NULL WHERE execution_id = ?1",
            (id,),
        )
        .await
        .unwrap();
        assert_eq!(probe(&db, sql).await, Some(remaining));
    }
    assert_eq!(inline_pack_rows(&db).await, 0);
    assert_eq!(
        db.query_one("SELECT COUNT(*) FROM execution_history", (), |row| row
            .i64(0))
            .await
            .unwrap(),
        4020,
        "completed histories remain after the pending set drains"
    );
}

async fn pack_hash(db: &LocalDb) -> Option<String> {
    db.query_one(
        "SELECT pack_hash FROM execution_history WHERE execution_id = 'exec'",
        (),
        |row| row.opt_text(0),
    )
    .await
    .unwrap()
}

/// Rewrite an archived execution into the shape the private database held
/// before it had a store: its pack inline in `execution_history` with no
/// `pack_hash` and no catalog entry, and its segment blobs zstd-compressed in
/// `archival_blobs` and absent from the store.
async fn to_legacy_inline(db: &LocalDb) {
    let store = db.content_store().unwrap().clone();
    let hash = pack_hash(db).await.expect("an archived pack");
    let (pack, idx) = load_execution_pack(db, "exec").await.unwrap().unwrap();
    db.execute(
        "UPDATE execution_history SET pack = ?1, pack_idx = ?2, pack_hash = NULL
         WHERE execution_id = 'exec'",
        (SqlValue::Blob(pack), SqlValue::Blob(idx)),
    )
    .await
    .unwrap();
    db.execute_script(
        "DELETE FROM pack_catalog_references WHERE owner_id = 'exec';
         DELETE FROM pack_catalog;",
    )
    .await
    .unwrap();
    drop_store_object(db, &hash);

    let mut hashes: Vec<String> = load_events(db)
        .await
        .iter()
        .filter(|event| event.storage_mode.as_deref() == Some("gitcoord"))
        .flat_map(|event: &Event| segment_hashes(&event.data))
        .collect();
    hashes.sort();
    hashes.dedup();
    assert!(!hashes.is_empty(), "the fixture blobs a system prompt");
    for hash in hashes {
        let bytes = store.get(&hash).await.unwrap().unwrap();
        db.execute(
            "INSERT INTO archival_blobs(hash, content, created_at) VALUES (?1, ?2, 1)",
            (hash.clone(), SqlValue::Blob(compress(&bytes).unwrap())),
        )
        .await
        .unwrap();
        drop_store_object(db, &hash);
    }
    assert_eq!(blob_count(db).await, 0, "nothing left in the store");
}

async fn archived_fixture(name: &str) -> (Fixture, LocalDb) {
    let fx = build_fixture();
    let db = migrated_test_db(name).await;
    seed_chain(&db, fx.repo.to_str().unwrap(), false).await;
    seed_transcript(&db, &fx).await;
    let summary = archive(&db, Some(fx.range())).await.unwrap();
    assert_eq!(summary.gitcoord_read, 2);
    assert_eq!(summary.system_prompt, 1);
    collect_branch(&fx);
    (fx, db)
}

#[tokio::test]
async fn offload_moves_inline_packs_and_blobs_and_every_event_reads_the_same() {
    let (_fx, db) = archived_fixture("offload-roundtrip.db").await;
    seed_history_rows(&db, 128, None).await;
    let expected = snapshot(&db).await;
    assert!(
        expected
            .iter()
            .all(|(_, _, outcome)| !outcome.is_unresolved()),
        "{expected:?}"
    );
    let expected_diff = node_diff(&db).await;

    to_legacy_inline(&db).await;
    assert_eq!(inline_pack_rows(&db).await, 1);
    let legacy_blobs = blob_row_count(&db).await;
    assert!(legacy_blobs > 0);
    assert_eq!(
        snapshot(&db).await,
        expected,
        "the inline shape reads the same"
    );
    assert_eq!(node_diff(&db).await, expected_diff);

    let report = run_offload_pass(&db, OffloadBudget::PRODUCTION).await;
    assert!(report.error.is_none(), "{report}");
    assert_eq!(report.packs_moved, 1, "{report}");
    assert_eq!(report.remaining_packs, Some(0), "{report}");
    assert_eq!(report.blobs_moved as i64, legacy_blobs, "{report}");
    assert_eq!(report.packs_uncatalogued, 0, "{report}");
    assert!(report.drained(), "{report}");

    assert_eq!(inline_pack_rows(&db).await, 0);
    assert_eq!(blob_row_count(&db).await, 0);
    assert_eq!(
        blob_count(&db).await,
        legacy_blobs + 1,
        "segments and the pack"
    );
    let hash = pack_hash(&db).await.expect("repointed at the stored pack");
    let catalogued = db
        .query_one(
            "SELECT COUNT(*) FROM pack_catalog_references
             WHERE owner_kind = 'execution_history' AND owner_id = 'exec' AND content_hash = ?1",
            (hash,),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(catalogued, 1);
    assert_eq!(
        snapshot(&db).await,
        expected,
        "the offloaded shape reads the same"
    );
    assert_eq!(node_diff(&db).await, expected_diff);

    let again = run_offload_pass(&db, OffloadBudget::PRODUCTION).await;
    assert_eq!((again.packs_moved, again.blobs_moved), (0, 0), "{again}");
    assert!(again.drained() && again.error.is_none(), "{again}");
}

/// A crash after the puts and before the repoint and the delete leaves both
/// copies; the next pass converges on the stored shape, and every event reads
/// the same throughout.
#[tokio::test]
async fn a_pass_interrupted_after_its_puts_resumes_and_converges() {
    let (_fx, db) = archived_fixture("offload-interrupted.db").await;
    let expected = snapshot(&db).await;
    let stored_pack = pack_hash(&db).await.unwrap();
    let store = db.content_store().unwrap().clone();
    let framed = store.get(&stored_pack).await.unwrap().unwrap();
    to_legacy_inline(&db).await;

    // The puts of an interrupted pass, without its repoint or deletes.
    store.put(&stored_pack, &framed).await.unwrap();
    let blobs: Vec<(String, Vec<u8>)> = db
        .query_all("SELECT hash, content FROM archival_blobs", (), |row| {
            Ok((row.text(0)?, row.opt_blob(1)?.unwrap()))
        })
        .await
        .unwrap();
    for (hash, stored) in &blobs {
        let bytes = decompress(CODEC_ZSTD_V1, stored).unwrap();
        store.put(hash, &bytes).await.unwrap();
    }
    assert_eq!(snapshot(&db).await, expected, "both copies read the same");

    let report = run_offload_pass(&db, OffloadBudget::PRODUCTION).await;
    assert!(report.error.is_none() && report.drained(), "{report}");
    assert_eq!(pack_hash(&db).await, Some(stored_pack));
    assert_eq!(blob_count(&db).await, blobs.len() as i64 + 1);
    assert_eq!(snapshot(&db).await, expected);
}

/// An offline archive ends the pass with nothing moved and nothing lost; once
/// the archive is back, the next pass finishes the job.
#[tokio::test]
async fn an_offline_archive_halts_the_pass_and_a_later_pass_resumes() {
    let (_fx, db) = archived_fixture("offload-offline.db").await;
    let expected = snapshot(&db).await;
    to_legacy_inline(&db).await;

    let root = archive_root(&db);
    let away = root.with_extension("away");
    std::fs::rename(&root, &away).unwrap();
    let halted = run_offload_pass(&db, OffloadBudget::PRODUCTION).await;
    let error = halted.error.clone().expect("the pass stops");
    assert!(error.contains("archive offline"), "{error}");
    assert_eq!(halted.packs_moved, 0);
    assert_eq!(inline_pack_rows(&db).await, 1);
    assert!(blob_row_count(&db).await > 0);
    assert_eq!(snapshot(&db).await, expected, "inline rows still serve");

    std::fs::rename(&away, &root).unwrap();
    let resumed = run_offload_pass(&db, OffloadBudget::PRODUCTION).await;
    assert!(resumed.error.is_none() && resumed.drained(), "{resumed}");
    assert_eq!(snapshot(&db).await, expected);
}

async fn seed_blob_rows(db: &LocalDb, count: usize) -> Vec<String> {
    let mut hashes = Vec::new();
    for n in 0..count {
        let bytes = format!("segment {n} ").repeat(40).into_bytes();
        let hash = content_hash(&bytes);
        db.execute(
            "INSERT INTO archival_blobs(hash, content, created_at) VALUES (?1, ?2, 1)",
            (hash.clone(), SqlValue::Blob(compress(&bytes).unwrap())),
        )
        .await
        .unwrap();
        hashes.push(hash);
    }
    hashes
}

/// No page carries more rows than the budget allows, or more stored bytes
/// beyond its first row.
#[tokio::test]
async fn no_page_exceeds_its_bounds() {
    let db = migrated_test_db("offload-bounds.db").await;
    seed_blob_rows(&db, 250).await;
    let report = run_offload_pass(&db, OffloadBudget::PRODUCTION).await;
    assert!(report.error.is_none() && report.drained(), "{report}");
    assert_eq!(report.blobs_moved, 250);
    assert_eq!(report.page_rows_max, 100, "{report}");
    assert_eq!(report.pages, 3, "{report}");

    let db = migrated_test_db("offload-byte-bounds.db").await;
    seed_blob_rows(&db, 7).await;
    let tiny = OffloadBudget {
        blob_page_bytes: 1,
        ..OffloadBudget::PRODUCTION
    };
    let report = run_offload_pass(&db, tiny).await;
    assert!(report.drained(), "{report}");
    assert_eq!((report.pages, report.page_rows_max), (7, 1), "{report}");
    assert_eq!(blob_count(&db).await, 7);
}

/// A row whose bytes do not hash to its key is left in place, and the pass
/// carries on past it.
#[tokio::test]
async fn a_corrupt_blob_row_is_left_and_passed_over() {
    let db = migrated_test_db("offload-corrupt.db").await;
    let good = seed_blob_rows(&db, 3).await;
    let bad = "0".repeat(64);
    db.execute(
        "INSERT INTO archival_blobs(hash, content, created_at) VALUES (?1, ?2, 1)",
        (
            bad.clone(),
            SqlValue::Blob(compress(b"not these bytes").unwrap()),
        ),
    )
    .await
    .unwrap();
    let report = run_offload_pass(&db, OffloadBudget::PRODUCTION).await;
    assert!(report.error.is_none(), "{report}");
    assert_eq!(
        (report.blobs_moved, report.blobs_skipped),
        (3, 1),
        "{report}"
    );
    assert_eq!(report.remaining_blobs, Some(1));
    let left: Vec<String> = db
        .query_all("SELECT hash FROM archival_blobs", (), |row| row.text(0))
        .await
        .unwrap();
    assert_eq!(left, vec![bad]);
    for hash in good {
        assert!(db
            .content_store()
            .unwrap()
            .get(&hash)
            .await
            .unwrap()
            .is_some());
    }
    // The phase reached its end, so the next pass starts over and retries it.
    let saved = progress::load(&db, progress::OFFLOAD_BLOBS).await.unwrap();
    assert_eq!(saved.cursor, None);
}
