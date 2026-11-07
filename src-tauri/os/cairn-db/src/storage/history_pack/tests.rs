use std::collections::BTreeMap;

use serde_json::{json, Map, Value as Json};

use super::testing::{publish_for_test, seed_fixture_execution};
use super::write::{pack_execution, PackWriter};
use super::*;
use crate::storage::content_store::content_hash;
use crate::storage::events::compress;
use crate::storage::{ContentStore, FileContentStore};

fn columns(names: &[&str]) -> Vec<ColumnDecl> {
    names
        .iter()
        .map(|name| ColumnDecl {
            name: name.to_string(),
            decl_type: "TEXT".to_string(),
        })
        .collect()
}

fn manifest() -> Manifest {
    Manifest {
        format: FORMAT,
        migration_high_water: None,
        scope: Scope {
            kind: "execution".to_string(),
            id: "e".to_string(),
        },
        project_id: None,
        issue_id: None,
        execution_id: "e".to_string(),
        jobs: Vec::new(),
        runs: Vec::new(),
        sessions: Vec::new(),
        turns: Vec::new(),
        sections: Vec::new(),
        token_rollup: TOKEN_ROLLUP_NOTE.to_string(),
    }
}

fn writer() -> PackWriter {
    PackWriter::new(BTreeMap::from([(
        Section::ToolInvocations,
        columns(&["id"]),
    )]))
}

fn row(value: &str) -> Map<String, Json> {
    let Json::Object(map) = json!({ "id": value }) else {
        unreachable!()
    };
    map
}

fn built_from(writer: PackWriter) -> Vec<u8> {
    writer.finish(manifest()).unwrap().0
}

/// A pack with the rows `a`, `b`, `c` under one key.
fn small_pack() -> Vec<u8> {
    let mut writer = writer();
    for (rowid, value) in ["a", "b", "c"].iter().enumerate() {
        writer
            .push(Section::ToolInvocations, "run", rowid as i64, row(value))
            .unwrap();
    }
    built_from(writer)
}

/// The pack with its manifest and index rewritten, keeping the frames.
fn rewrite(
    bytes: &[u8],
    edit_manifest: impl FnOnce(&mut Manifest),
    edit_index: impl FnOnce(&mut Vec<IndexEntry>),
) -> Vec<u8> {
    let pack = HistoryPack::open(bytes.to_vec()).unwrap();
    let mut manifest = pack.manifest().clone();
    let mut index = pack.index().to_vec();
    edit_manifest(&mut manifest);
    edit_index(&mut index);
    let footer = &bytes[bytes.len() - FOOTER_LEN..];
    let manifest_off = u64::from_le_bytes(footer[..8].try_into().unwrap()) as usize;
    let mut out = bytes[..manifest_off].to_vec();
    let manifest_frame = compress(&serde_json::to_vec(&manifest).unwrap()).unwrap();
    let index_frame = compress(&serde_json::to_vec(&index).unwrap()).unwrap();
    let new_manifest_off = out.len() as u64;
    out.extend_from_slice(&manifest_frame);
    let new_index_off = out.len() as u64;
    out.extend_from_slice(&index_frame);
    for word in [
        new_manifest_off,
        manifest_frame.len() as u64,
        new_index_off,
        index_frame.len() as u64,
    ] {
        out.extend_from_slice(&word.to_le_bytes());
    }
    out.extend_from_slice(FOOTER_MAGIC);
    out
}

#[test]
fn frames_cut_at_their_row_bound_and_never_span_keys() {
    let mut writer = writer();
    for rowid in 0..(FRAME_ROWS + 10) {
        writer
            .push(Section::ToolInvocations, "run-a", rowid as i64, row("x"))
            .unwrap();
    }
    writer
        .push(Section::ToolInvocations, "run-b", 5_000, row("y"))
        .unwrap();
    let pack = HistoryPack::open(built_from(writer)).unwrap();
    let shape: Vec<(&str, u32, i64)> = pack
        .index()
        .iter()
        .map(|entry| (entry.key.as_str(), entry.rows, entry.first_rowid))
        .collect();
    assert_eq!(
        shape,
        [
            ("run-a", FRAME_ROWS as u32, 0),
            ("run-a", 10, FRAME_ROWS as i64),
            ("run-b", 1, 5_000),
        ]
    );
    pack.verify().unwrap();
}

#[test]
fn frames_cut_at_their_byte_bound() {
    let big = "x".repeat(FRAME_RAW_BYTES / 2 + 1);
    let mut writer = writer();
    for rowid in 0..3 {
        writer
            .push(Section::ToolInvocations, "run", rowid, row(&big))
            .unwrap();
    }
    let pack = HistoryPack::open(built_from(writer)).unwrap();
    assert_eq!(
        pack.index()
            .iter()
            .map(|entry| entry.rows)
            .collect::<Vec<_>>(),
        [1, 1, 1]
    );
    assert!(pack
        .index()
        .iter()
        .all(|entry| entry.raw_len <= FRAME_RAW_BYTES as u64));
    pack.verify().unwrap();
}

#[test]
fn a_row_at_the_byte_bound_fits_but_one_more_byte_is_rejected() {
    let mut empty = row("");
    empty.insert("rowid".to_string(), Json::from(7));
    let overhead = serde_json::to_vec(&empty).unwrap().len() + 1;
    let fits = "x".repeat(FRAME_RAW_BYTES - overhead);
    for has_open_frame in [false, true] {
        let mut writer = writer();
        if has_open_frame {
            writer
                .push(Section::ToolInvocations, "run", 1, row("before"))
                .unwrap();
        }
        let error = writer
            .push(Section::ToolInvocations, "run", 7, row(&format!("{fits}x")))
            .unwrap_err();
        assert_eq!(
            error,
            PackError::Corrupt(format!(
                "tool_invocations key \"run\" rowid 7 encodes to {} JSONL bytes, exceeds FRAME_RAW_BYTES ({FRAME_RAW_BYTES})",
                FRAME_RAW_BYTES + 1
            ))
        );
        writer
            .push(Section::ToolInvocations, "run", 7, row(&fits))
            .unwrap();
        let pack = HistoryPack::open(built_from(writer)).unwrap();
        pack.verify().unwrap();
        assert_eq!(pack.index().last().unwrap().raw_len, FRAME_RAW_BYTES as u64);
        let rows = pack
            .rows(Section::ToolInvocations, "run")
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows.len(), if has_open_frame { 2 } else { 1 });
        assert_eq!(rows.last().unwrap()["id"], fits, "no truncation");
    }
}

#[tokio::test]
async fn unresolved_blob_base64_expansion_cannot_exceed_the_frame_bound() {
    let db = crate::storage::migrated_test_db("pack-oversized-blob.db").await;
    let fixture = seed_fixture_execution(&db, "oversized").await;
    let baseline = pack_execution(&db, "exec-1").await.unwrap();
    let pack = HistoryPack::open(baseline.bytes).unwrap();
    let mut unresolved = pack
        .rows(Section::Events, "run-1")
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .find(|row| row["id"] == fixture.undecodable)
        .unwrap();
    assert_eq!(unresolved["reconstruction"], "undecodable");
    let rowid = unresolved["rowid"].as_i64().unwrap();
    // The stored blob fits the input bound, but its lossless base64 form does not.
    let blob = vec![0u8; FRAME_RAW_BYTES * 3 / 4 + 1];
    assert!(blob.len() < FRAME_RAW_BYTES);
    unresolved["stored"]["data_blob"] = encode::json_value(&turso::Value::Blob(blob.clone()));
    let encoded_bytes = serde_json::to_vec(&unresolved).unwrap().len() + 1;
    assert!(encoded_bytes > FRAME_RAW_BYTES);
    db.execute(
        "UPDATE events SET data_blob = ?1 WHERE id = ?2",
        turso::params![blob.clone(), fixture.undecodable.as_str()],
    )
    .await
    .unwrap();
    let error = pack_execution(&db, "exec-1").await.unwrap_err();
    let super::write::PackBuildError::Pack(error) = error else {
        panic!("expected a typed pack error, got {error}");
    };
    assert_eq!(
        error,
        PackError::Corrupt(format!(
            "events key \"run-1\" rowid {rowid} encodes to {encoded_bytes} JSONL bytes, exceeds FRAME_RAW_BYTES ({FRAME_RAW_BYTES})"
        ))
    );
    assert_eq!(
        db.query_opt_i64(
            "SELECT length(data_blob) FROM events WHERE id = ?1",
            (fixture.undecodable,),
        )
        .await
        .unwrap(),
        Some(blob.len() as i64),
        "the source blob is never truncated"
    );
}

#[test]
fn rows_read_back_under_their_key_in_order() {
    let pack = HistoryPack::open(small_pack()).unwrap();
    let rows: Vec<Map<String, Json>> = pack
        .rows(Section::ToolInvocations, "run")
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1]["id"], "b");
    assert_eq!(rows[1]["rowid"], 1);
    assert_eq!(pack.rows(Section::ToolInvocations, "other").count(), 0);
}

#[test]
fn rows_after_skips_frames_wholly_below_the_cursor() {
    let mut writer = writer();
    for rowid in 0..(FRAME_ROWS as i64 * 2) {
        writer
            .push(Section::ToolInvocations, "run", rowid, row("x"))
            .unwrap();
    }
    let pack = HistoryPack::open(built_from(writer)).unwrap();
    let after_first_frame = FRAME_ROWS as i64 + 3;
    let rows: Vec<Map<String, Json>> = pack
        .rows_after(Section::ToolInvocations, "run", after_first_frame)
        .collect::<Result<_, _>>()
        .unwrap();
    // Only the second frame inflates, and its rows at or below the cursor are
    // the caller's to drop.
    assert_eq!(rows.len(), FRAME_ROWS);
    assert_eq!(rows[0]["rowid"], FRAME_ROWS as i64);
}

#[test]
fn verify_catches_a_flipped_byte() {
    let mut bytes = small_pack();
    bytes[MAGIC.len() + 6] ^= 0xFF;
    let pack = HistoryPack::open(bytes).unwrap();
    assert!(matches!(pack.verify(), Err(PackError::Corrupt(_))));
}

#[test]
fn verify_catches_a_missing_frame() {
    let bytes = rewrite(&small_pack(), |_| {}, Vec::clear);
    let pack = HistoryPack::open(bytes).unwrap();
    assert!(matches!(pack.verify(), Err(PackError::Corrupt(_))));
}

#[test]
fn verify_catches_a_wrong_count() {
    let bytes = rewrite(
        &small_pack(),
        |manifest| {
            let section = manifest
                .sections
                .iter_mut()
                .find(|entry| entry.packed)
                .unwrap();
            section.rows = Some(99);
        },
        |_| {},
    );
    let pack = HistoryPack::open(bytes).unwrap();
    assert!(matches!(pack.verify(), Err(PackError::Corrupt(_))));
}

#[test]
fn verify_catches_a_wrong_hash() {
    let bytes = rewrite(
        &small_pack(),
        |manifest| {
            let section = manifest
                .sections
                .iter_mut()
                .find(|entry| entry.packed)
                .unwrap();
            section.sha256 = Some("0".repeat(64));
        },
        |_| {},
    );
    let pack = HistoryPack::open(bytes).unwrap();
    assert!(matches!(pack.verify(), Err(PackError::Corrupt(_))));
}

#[test]
fn open_names_what_is_wrong_with_the_bytes() {
    assert_eq!(
        HistoryPack::open(b"short".to_vec()).unwrap_err(),
        PackError::Truncated
    );
    let mut bad_magic = small_pack();
    bad_magic[0] = b'X';
    assert_eq!(
        HistoryPack::open(bad_magic).unwrap_err(),
        PackError::BadMagic
    );
    let mut newer = small_pack();
    newer[MAGIC.len() - 1] = 2;
    assert_eq!(
        HistoryPack::open(newer).unwrap_err(),
        PackError::UnsupportedFormat(2)
    );
    let newer_manifest = rewrite(&small_pack(), |manifest| manifest.format = 9, |_| {});
    assert_eq!(
        HistoryPack::open(newer_manifest).unwrap_err(),
        PackError::UnsupportedFormat(9)
    );
    let mut cut = small_pack();
    cut.truncate(cut.len() - 1);
    assert_eq!(HistoryPack::open(cut).unwrap_err(), PackError::Truncated);
}

#[test]
fn an_offline_store_error_keeps_its_path() {
    let error = PackError::from(crate::storage::ContentStoreError::Offline {
        path: "/gone/archive".into(),
    });
    assert_eq!(error.to_string(), "archive offline: /gone/archive");
}

#[tokio::test]
async fn the_same_history_packs_to_the_same_bytes() {
    let db = crate::storage::migrated_test_db("pack-determinism.db").await;
    seed_fixture_execution(&db, "determinism").await;
    let first = pack_execution(&db, "exec-1").await.unwrap();
    let second = pack_execution(&db, "exec-1").await.unwrap();
    assert_eq!(first.content_hash, second.content_hash);
    assert_eq!(first.bytes, second.bytes);
    assert_eq!(first.content_hash, content_hash(&first.bytes));
}

#[tokio::test]
async fn a_pack_states_every_section_and_counts_what_it_read() {
    let db = crate::storage::migrated_test_db("pack-manifest.db").await;
    seed_fixture_execution(&db, "manifest").await;
    let built = pack_execution(&db, "exec-1").await.unwrap();
    let pack = HistoryPack::open(built.bytes.clone()).unwrap();
    pack.verify().unwrap();
    let manifest = pack.manifest();
    assert_eq!(manifest.scope.kind, "execution");
    assert_eq!(manifest.execution_id, "exec-1");
    assert_eq!(manifest.project_id.as_deref(), Some("p"));
    assert!(manifest.migration_high_water.is_some());

    let counts: BTreeMap<&str, Option<u64>> = manifest
        .sections
        .iter()
        .map(|entry| (entry.table.as_str(), entry.rows))
        .collect();
    assert_eq!(counts["events"], Some(8));
    assert_eq!(counts["event_vibes"], Some(1));
    assert_eq!(counts["event_read_tokens"], Some(1));
    assert_eq!(counts["tool_invocations"], Some(1));
    assert_eq!(counts["turn_embeddings"], Some(1));
    for (table, _) in NOT_PACKED {
        let entry = manifest.sections.iter().find(|e| e.table == table).unwrap();
        assert!(!entry.packed && !entry.reason.is_empty(), "{table}");
    }
    assert_eq!(built.stats.event_rows(), 8);
    assert!(built.stats.pages >= 5);

    let run = manifest.runs.iter().find(|run| run.id == "run-1").unwrap();
    assert_eq!(run.session_ids, ["sess-1"]);
    assert_eq!(run.turn_ids, ["turn-1"]);
    assert_eq!(run.event_rows, 6);
    let session = manifest
        .sessions
        .iter()
        .find(|session| session.id == "sess-2")
        .unwrap();
    assert_eq!(session.event_rows, 2);
    assert!(session.max_rowid.is_some());
    let events_section = manifest.section(Section::Events).unwrap();
    let columns = events_section.columns.as_ref().unwrap();
    let live: Vec<&str> = columns.iter().map(|column| column.name.as_str()).collect();
    for column in [
        "reported_model",
        "selected_model",
        "change_preview_status",
        "change_applied_at",
        "account_id",
    ] {
        assert!(live.contains(&column), "{column} is packed");
    }
}

/// The stored form of an event round-trips through the pack: blob columns of a
/// sidecar row decode back to bytes by their declared type.
#[tokio::test]
async fn a_blob_column_decodes_back_from_its_declared_type() {
    let db = crate::storage::migrated_test_db("pack-blob-column.db").await;
    seed_fixture_execution(&db, "blob").await;
    let built = pack_execution(&db, "exec-1").await.unwrap();
    let pack = HistoryPack::open(built.bytes).unwrap();
    let section = pack.manifest().section(Section::TurnEmbeddings).unwrap();
    let decl = section
        .columns
        .as_ref()
        .unwrap()
        .iter()
        .find(|column| column.name == "embedding")
        .unwrap();
    let row = pack
        .rows(Section::TurnEmbeddings, "job-1")
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(
        encode::stored_value(&row["embedding"], &decl.decl_type),
        turso::Value::Blob(vec![0, 0, 128, 63])
    );
}

#[tokio::test]
async fn a_pack_decodes_from_the_store_directory_alone() {
    let (root, hash, expected_rows) = {
        let db = crate::storage::migrated_test_db("pack-decode-alone.db").await;
        seed_fixture_execution(&db, "alone").await;
        let built = publish_for_test(&db, "exec-1").await;
        (
            db.path().with_extension("archive"),
            built.content_hash,
            built.manifest.runs[0].event_rows,
        )
    };

    // Nothing from here on has a database: the directory and the hash suffice.
    let store = FileContentStore::open_existing(&root);
    let bytes = store.get(&hash).await.unwrap().unwrap();
    let pack = HistoryPack::open(bytes).unwrap();
    pack.verify().unwrap();
    let run_id = pack.manifest().runs[0].id.clone();
    let events = pack.run_events(&run_id).unwrap();
    assert_eq!(events.len() as u64, expected_rows);
    assert_eq!(events[0].event.id, "alone-plain");
    let restored = events
        .iter()
        .find(|row| row.event.id == "alone-restored")
        .unwrap();
    assert!(restored.event.data.contains("the restored body"));
    assert!(restored.event.data_blob.is_none());
    assert_eq!(
        restored.extras.reported_model.as_deref(),
        Some("claude-opus-5-5")
    );
    assert_eq!(restored.extras.selected_model.as_deref(), Some("opus"));

    let mut legacy = pack.rows(Section::Events, &run_id).next().unwrap().unwrap();
    legacy.remove("selected_model");
    assert!(decode_event_row(&legacy)
        .unwrap()
        .extras
        .selected_model
        .is_none());
}

#[tokio::test]
async fn publish_refuses_a_store_that_does_not_hold_what_it_took() {
    use async_trait::async_trait;

    /// Takes anything and hands back something else.
    struct Lying;
    #[async_trait]
    impl ContentStore for Lying {
        async fn put(&self, _: &str, _: &[u8]) -> Result<(), crate::storage::ContentStoreError> {
            Ok(())
        }
        async fn get(&self, _: &str) -> Result<Option<Vec<u8>>, crate::storage::ContentStoreError> {
            Ok(Some(small_pack()))
        }
    }

    let db = crate::storage::migrated_test_db("pack-publish-mismatch.db").await;
    seed_fixture_execution(&db, "mismatch").await;
    let built = pack_execution(&db, "exec-1").await.unwrap();
    let error = catalog::publish_to(
        &db,
        &Lying,
        &built,
        catalog::PublishFacts {
            hot_bytes_estimate: 0,
            resolved_at: 1,
            packed_at: 2,
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, catalog::PublishError::Mismatch(_)),
        "{error}"
    );
    assert!(catalog::tier_entry(&db, "exec-1").await.unwrap().is_none());
    let run_rows = db
        .query_opt_i64("SELECT COUNT(*) FROM history_tier_runs", ())
        .await
        .unwrap();
    assert_eq!(run_rows, Some(0), "nothing is catalogued on a mismatch");
}

#[tokio::test]
async fn publish_refuses_valid_pack_bytes_under_the_wrong_content_hash() {
    use async_trait::async_trait;

    struct WrongKey(Vec<u8>);
    #[async_trait]
    impl ContentStore for WrongKey {
        async fn put(&self, _: &str, _: &[u8]) -> Result<(), crate::storage::ContentStoreError> {
            Ok(())
        }
        async fn get(&self, _: &str) -> Result<Option<Vec<u8>>, crate::storage::ContentStoreError> {
            Ok(Some(self.0.clone()))
        }
    }

    let db = crate::storage::migrated_test_db("pack-publish-wrong-key.db").await;
    seed_fixture_execution(&db, "wrong-key").await;
    let mut built = pack_execution(&db, "exec-1").await.unwrap();
    let store = WrongKey(built.bytes.clone());
    let pack = HistoryPack::open(store.0.clone()).unwrap();
    pack.verify().unwrap();
    assert_eq!(pack.manifest(), &built.manifest);
    built.content_hash = "0".repeat(64);
    assert_ne!(content_hash(&store.0), built.content_hash);

    let error = catalog::publish_to(
        &db,
        &store,
        &built,
        catalog::PublishFacts {
            hot_bytes_estimate: 0,
            resolved_at: 1,
            packed_at: 2,
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, catalog::PublishError::Mismatch(ref detail) if detail.contains("content hash")),
        "{error}"
    );
    assert!(catalog::tier_entry(&db, "exec-1").await.unwrap().is_none());
    assert_eq!(
        db.query_opt_i64("SELECT COUNT(*) FROM history_tier_runs", ())
            .await
            .unwrap(),
        Some(0),
        "no catalog rows are written for bytes under the wrong key"
    );
}

#[tokio::test]
async fn publish_writes_the_catalog_and_changes_no_hot_row() {
    let db = crate::storage::migrated_test_db("pack-publish.db").await;
    seed_fixture_execution(&db, "publish").await;
    let count = "SELECT COUNT(*) FROM events";
    let before = db.query_opt_i64(count, ()).await.unwrap();
    let built = publish_for_test(&db, "exec-1").await;
    assert_eq!(db.query_opt_i64(count, ()).await.unwrap(), before);

    let entry = catalog::tier_entry(&db, "exec-1").await.unwrap().unwrap();
    assert_eq!(entry.content_hash, built.content_hash);
    assert_eq!(entry.format, 1);
    assert_eq!(entry.bytes, built.bytes.len() as i64);
    assert_eq!(entry.row_counts["events"], 8);
    assert_eq!(entry.evicted_at, None);
    assert_eq!(
        db.query_opt_i64("SELECT COUNT(*) FROM history_tier_runs", ())
            .await
            .unwrap(),
        Some(2)
    );

    // Packing again replaces the catalog rows rather than adding to them.
    let again = publish_for_test(&db, "exec-1").await;
    assert_eq!(again.content_hash, built.content_hash);
    assert_eq!(
        db.query_opt_i64("SELECT COUNT(*) FROM history_tier", ())
            .await
            .unwrap(),
        Some(1)
    );
}

async fn assert_evicted_republication(evict_during_publish: bool) {
    use crate::storage::events::history::{EventFilter, HistoryReader, Provenance};
    use crate::storage::history_pack::testing::{evict_all, fingerprint};
    use crate::storage::RowExt;

    let db = crate::storage::migrated_test_db("pack-publish-evicted.db").await;
    seed_fixture_execution(&db, "republish").await;
    let original = publish_for_test(&db, "exec-1").await;
    let mut expected_entry = catalog::tier_entry(&db, "exec-1").await.unwrap().unwrap();
    expected_entry.evicted_at = Some(5);
    let mappings_sql = "SELECT run_id, execution_id FROM history_tier_runs ORDER BY run_id";
    let mappings = db
        .query_all(mappings_sql, (), |row| Ok((row.text(0)?, row.text(1)?)))
        .await
        .unwrap();
    let mut expected_history = Vec::new();
    for run in ["run-1", "run-2"] {
        let page = HistoryReader::new(&db)
            .run_events(run, EventFilter::default())
            .await
            .unwrap();
        assert!(!page.rows.is_empty());
        expected_history.push((run, fingerprint(&page.rows)));
    }

    if evict_during_publish {
        db.execute("DELETE FROM events", ()).await.unwrap();
        // Evict after the early check and run mappings, before the final
        // catalog write. The SQL commit guard must refuse the replacement.
        db.execute_script(
            "CREATE TRIGGER evict_during_publish AFTER INSERT ON history_tier_runs
             WHEN NEW.run_id = 'run-1'
             BEGIN
                 UPDATE history_tier SET evicted_at = 5 WHERE execution_id = 'exec-1';
             END;",
        )
        .await
        .unwrap();
    } else {
        evict_all(&db).await;
    }
    let empty = pack_execution(&db, "exec-1").await.unwrap();
    assert_eq!(empty.stats.event_rows(), 0);
    assert_ne!(empty.content_hash, original.content_hash);

    // Both an empty rebuild and an otherwise idempotent re-publication must
    // leave the authoritative catalog untouched once eviction is marked.
    for built in [&empty, &original] {
        let error = catalog::publish(
            &db,
            built,
            catalog::PublishFacts {
                hot_bytes_estimate: 0,
                resolved_at: 10,
                packed_at: 20,
            },
        )
        .await
        .unwrap_err();
        if evict_during_publish {
            db.mark_history_evicted();
        }
        assert!(
            matches!(error, catalog::PublishError::Evicted { ref execution_id } if execution_id == "exec-1"),
            "{error}"
        );
        assert_eq!(
            catalog::tier_entry(&db, "exec-1").await.unwrap().unwrap(),
            expected_entry,
            "the original hash, eviction marker, and all catalog facts survive"
        );
        let actual_mappings = db
            .query_all(mappings_sql, (), |row| Ok((row.text(0)?, row.text(1)?)))
            .await
            .unwrap();
        assert_eq!(actual_mappings, mappings);
        for (run, expected) in &expected_history {
            let page = HistoryReader::new(&db)
                .run_events(run, EventFilter::default())
                .await
                .unwrap();
            assert_eq!(
                page.provenance,
                Provenance::Pack {
                    execution_id: "exec-1".to_string(),
                    content_hash: original.content_hash.clone(),
                }
            );
            assert_eq!(&fingerprint(&page.rows), expected);
        }
    }
    assert_eq!(
        db.query_opt_i64("SELECT COUNT(*) FROM events", ())
            .await
            .unwrap(),
        Some(0),
        "history was served without any hot events"
    );
}

#[tokio::test]
async fn publish_refuses_replacing_an_evicted_pack_with_an_empty_rebuild() {
    assert_evicted_republication(false).await;
}

#[tokio::test]
async fn publish_final_write_refuses_an_eviction_after_the_early_check() {
    assert_evicted_republication(true).await;
}

#[tokio::test]
async fn publish_run_mapping_write_refuses_an_eviction_during_store_read_back() {
    use crate::storage::events::history::{EventFilter, HistoryReader, Provenance};
    use crate::storage::history_pack::testing::{evict_all, fingerprint};
    use crate::storage::{ContentStoreError, LocalDb, RowExt};

    struct EvictOnReadBack<'a> {
        db: &'a LocalDb,
        inner: &'a dyn ContentStore,
    }

    #[async_trait::async_trait]
    impl ContentStore for EvictOnReadBack<'_> {
        async fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), ContentStoreError> {
            self.inner.put(hash, bytes).await
        }

        async fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, ContentStoreError> {
            let bytes = self.inner.get(hash).await?;
            // Publication has passed its early guard, but has not written any
            // mappings. Committing eviction here exercises the mapping SQL guard.
            evict_all(self.db).await;
            Ok(bytes)
        }
    }

    let db = crate::storage::migrated_test_db("pack-publish-mapping-evicted.db").await;
    seed_fixture_execution(&db, "mapping-evicted").await;
    let original = publish_for_test(&db, "exec-1").await;
    let mut expected_entry = catalog::tier_entry(&db, "exec-1").await.unwrap().unwrap();
    expected_entry.evicted_at = Some(5);
    let mappings_sql = "SELECT run_id, execution_id FROM history_tier_runs ORDER BY run_id";
    let expected_mappings = db
        .query_all(mappings_sql, (), |row| Ok((row.text(0)?, row.text(1)?)))
        .await
        .unwrap();
    let mut expected_history = Vec::new();
    for run in ["run-1", "run-2"] {
        let page = HistoryReader::new(&db)
            .run_events(run, EventFilter::default())
            .await
            .unwrap();
        assert!(!page.rows.is_empty());
        expected_history.push((run, fingerprint(&page.rows)));
    }

    db.execute(
        "INSERT INTO runs(id, project_id, job_id, session_id, status, created_at, updated_at)
         VALUES('new-run', 'p', 'job-1', 'sess-2', 'exited', 30, 30)",
        (),
    )
    .await
    .unwrap();
    let replacement = pack_execution(&db, "exec-1").await.unwrap();
    assert!(!original.manifest.runs.iter().any(|run| run.id == "new-run"));
    assert!(replacement
        .manifest
        .runs
        .iter()
        .any(|run| run.id == "new-run"));
    assert_ne!(replacement.content_hash, original.content_hash);
    let error = catalog::publish_to(
        &db,
        &EvictOnReadBack {
            db: &db,
            inner: db.content_store().unwrap().as_ref(),
        },
        &replacement,
        catalog::PublishFacts {
            hot_bytes_estimate: 0,
            resolved_at: 10,
            packed_at: 20,
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, catalog::PublishError::Evicted { ref execution_id } if execution_id == "exec-1"),
        "{error}"
    );
    assert_eq!(
        db.query_opt_i64(
            "SELECT COUNT(*) FROM history_tier_runs WHERE run_id = 'new-run'",
            ()
        )
        .await
        .unwrap(),
        Some(0),
        "the mapping guard must not leak a run absent from the authoritative pack"
    );
    assert_eq!(
        db.query_all(mappings_sql, (), |row| Ok((row.text(0)?, row.text(1)?)))
            .await
            .unwrap(),
        expected_mappings
    );
    assert_eq!(
        catalog::tier_entry(&db, "exec-1").await.unwrap().unwrap(),
        expected_entry
    );
    for (run, expected) in expected_history {
        let page = HistoryReader::new(&db)
            .run_events(run, EventFilter::default())
            .await
            .unwrap();
        assert_eq!(
            page.provenance,
            Provenance::Pack {
                execution_id: "exec-1".to_string(),
                content_hash: original.content_hash.clone(),
            }
        );
        assert_eq!(fingerprint(&page.rows), expected);
    }
    assert_eq!(
        db.query_opt_i64("SELECT COUNT(*) FROM events", ())
            .await
            .unwrap(),
        Some(0)
    );
}
