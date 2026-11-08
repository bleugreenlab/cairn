//! Each seam call, hot against pack-sourced, over a fixture execution holding
//! every outcome and same-second ties.

use super::*;
use crate::storage::history_pack::catalog;
use crate::storage::history_pack::testing::{
    fingerprint, publish_for_test, read_both, seed_fixture_execution, FixtureEvents,
};
use crate::storage::history_pack::ScopeKind;
use crate::storage::quarantine::{
    quarantine, QuarantineSet, HISTORY_PACK_SINK, TRANSCRIPT_EVENT_SINK, WITHHELD_PREFIX,
};
use crate::storage::query_plan;

async fn packed_db(name: &str) -> (LocalDb, FixtureEvents) {
    let db = crate::storage::migrated_test_db(name).await;
    let events = seed_fixture_execution(&db, name).await;
    publish_for_test(&db, "exec-1").await;
    (db, events)
}

fn assert_pack(page: &Page) {
    assert!(
        matches!(page.provenance, Provenance::Pack { .. }),
        "expected a pack read, got {:?}",
        page.provenance
    );
}

/// Quarantine `event_id` under `sink` for the length of a test.
struct Quarantined(std::path::PathBuf);

impl Quarantined {
    fn new(source: &str, sink: &str, event_id: &str) -> Self {
        let source = std::path::PathBuf::from(source);
        quarantine().install_for(
            &source,
            QuarantineSet::from_entries([(
                sink.to_string(),
                event_id.to_string(),
                "incident-1".to_string(),
            )]),
        );
        Quarantined(source)
    }
}

impl Drop for Quarantined {
    fn drop(&mut self) {
        quarantine().install_for(&self.0, QuarantineSet::default());
    }
}

#[tokio::test]
async fn run_events_page_matches_across_sources() {
    let (db, _) = packed_db("seam-run-page").await;
    for run in ["run-1", "run-2"] {
        let (hot, pack) = read_both(&db, || async {
            let mut reader = HistoryReader::new(&db);
            let mut pages = Vec::new();
            let mut after = i64::MIN;
            loop {
                let page = reader
                    .run_events_page(run, after, 2, EventFilter::default())
                    .await
                    .unwrap();
                let exhausted = page.fetched < 2;
                after = page.last_rowid.unwrap_or(after);
                pages.push(page);
                if exhausted {
                    break;
                }
            }
            pages
        })
        .await;
        assert_eq!(hot.len(), pack.len(), "{run} pages");
        for (hot, pack) in hot.iter().zip(&pack) {
            assert_eq!(hot.provenance, Provenance::Hot);
            assert_pack(pack);
            assert_eq!(fingerprint(&hot.rows), fingerprint(&pack.rows));
        }
    }
}

#[tokio::test]
async fn run_events_keep_chat_order_with_same_second_ties() {
    let (db, events) = packed_db("seam-run-events").await;
    let (hot, pack) = read_both(&db, || async {
        HistoryReader::new(&db)
            .run_events("run-1", EventFilter::default())
            .await
            .unwrap()
    })
    .await;
    assert_eq!(fingerprint(&hot.rows), fingerprint(&pack.rows));
    assert_pack(&pack);
    // Three rows share a second: insertion order breaks the tie, not sequence.
    let order: Vec<&str> = hot.rows.iter().map(|row| row.event.id.as_str()).collect();
    assert_eq!(
        order[..3],
        [
            events.plain.as_str(),
            events.tied.as_str(),
            events.restored.as_str()
        ]
    );
}

#[tokio::test]
async fn every_outcome_and_label_survives_the_pack() {
    let (db, events) = packed_db("seam-outcomes").await;
    db.execute_script("UPDATE jobs SET model = 'sonnet' WHERE id = 'job-1';")
        .await
        .unwrap();
    let (hot, pack) = read_both(&db, || async {
        HistoryReader::new(&db)
            .run_events("run-1", EventFilter::default())
            .await
            .unwrap()
    })
    .await;
    for page in [&hot, &pack] {
        let label = |id: &str| {
            page.rows
                .iter()
                .find(|row| row.event.id == id)
                .unwrap()
                .reconstruction
        };
        assert_eq!(label(&events.plain), Reconstruction::Stored);
        assert_eq!(label(&events.restored), Reconstruction::Restored);
        assert_eq!(label(&events.degraded), Reconstruction::Degraded);
        assert_eq!(label(&events.undecodable), Reconstruction::Undecodable);

        let restored = page
            .rows
            .iter()
            .find(|row| row.event.id == events.restored)
            .unwrap();
        assert!(restored.event.data.contains("the restored body"));
        assert!(
            restored.event.data_blob.is_none(),
            "a resolved row has no blob"
        );
        assert_eq!(restored.event.input_tokens, Some(7));
        assert_eq!(restored.event.cost_usd, Some(0.125));
        assert_eq!(
            restored.extras,
            EventExtras {
                reported_model: Some("claude-opus-5-5".to_string()),
                selected_model: Some("opus".to_string()),
                change_preview_status: Some("applied".to_string()),
                change_applied_at: Some(99),
                account_id: Some("acct-a".to_string()),
            }
        );

        // An unresolved row keeps its stored blob either way.
        let undecodable = page
            .rows
            .iter()
            .find(|row| row.event.id == events.undecodable)
            .unwrap();
        assert_eq!(
            undecodable.event.data_blob.as_deref(),
            Some(&b"not a zstd frame"[..])
        );
    }
}

#[tokio::test]
async fn a_type_filter_matches_across_sources() {
    let (db, _) = packed_db("seam-types").await;
    let (hot, pack) = read_both(&db, || async {
        HistoryReader::new(&db)
            .run_events("run-1", EventFilter::types(&["assistant"]))
            .await
            .unwrap()
    })
    .await;
    assert_eq!(fingerprint(&hot.rows), fingerprint(&pack.rows));
    assert_eq!(hot.rows.len(), 2);
    assert!(hot
        .rows
        .iter()
        .all(|row| row.event.event_type == "assistant"));
}

fn is_status_or_seed(event_type: &str) -> bool {
    matches!(event_type, "system:status" | "user:seed")
}

fn withhold_envelope(event: &mut Event) {
    event.data = format!("{{\"bodyWithheld\":\"{}\"}}", event.event_type);
    event.storage_mode = None;
    event.data_blob = None;
    event.codec = None;
}

#[tokio::test]
async fn a_body_projection_matches_across_sources() {
    let (db, events) = packed_db("seam-bodies").await;
    let bodies = BodyProjection::new(
        "event_type IN ('system:status', 'user:seed')",
        is_status_or_seed,
        withhold_envelope,
    );
    let filter = EventFilter::projecting(&bodies);
    let (hot, pack) = read_both(&db, || async {
        let mut reader = HistoryReader::new(&db);
        let mut rows = reader.run_events("run-1", filter).await.unwrap().rows;
        rows.extend(reader.run_events("run-2", filter).await.unwrap().rows);
        rows
    })
    .await;
    assert_eq!(fingerprint(&hot), fingerprint(&pack));
    let seed = hot
        .iter()
        .find(|row| row.event.id == events.second_session)
        .unwrap();
    assert_eq!(seed.event.data, "{\"bodyWithheld\":\"user:seed\"}");
    assert_eq!(seed.reconstruction, Reconstruction::Stored);
}

#[tokio::test]
async fn the_window_matches_across_sources() {
    let (db, _) = packed_db("seam-window").await;
    for (below, floor, limit) in [
        (i64::MAX, i64::MIN, 500),
        (5, 2, 500),
        (i64::MAX, i64::MIN, 2),
    ] {
        let (hot, pack) = read_both(&db, || async {
            HistoryReader::new(&db)
                .run_events_window("run-1", below, floor, limit, EventFilter::default())
                .await
                .unwrap()
        })
        .await;
        assert_eq!(
            fingerprint(&hot.rows),
            fingerprint(&pack.rows),
            "{below} {floor} {limit}"
        );
        assert_pack(&pack);
        let sequences: Vec<i32> = hot.rows.iter().map(|row| row.event.sequence).collect();
        assert!(
            sequences.windows(2).all(|pair| pair[0] > pair[1]),
            "newest first"
        );
    }
}

#[tokio::test]
async fn single_event_lookups_match_across_sources() {
    let (db, events) = packed_db("seam-single").await;
    let (hot, pack) = read_both(&db, || async {
        let mut reader = HistoryReader::new(&db);
        (
            reader.run_event_by_sequence("run-1", 2).await.unwrap(),
            reader.run_event_by_sequence("run-1", 99).await.unwrap(),
            reader
                .run_events_by_id(
                    "run-1",
                    &[
                        events.tied.clone(),
                        events.restored.clone(),
                        "absent".to_string(),
                    ],
                )
                .await
                .unwrap(),
        )
    })
    .await;
    assert_eq!(
        fingerprint(&[hot.0.clone().unwrap()]),
        fingerprint(&[pack.0.unwrap()])
    );
    assert_eq!(hot.0.unwrap().event.id, events.restored);
    assert!(hot.1.is_none() && pack.1.is_none());
    assert_eq!(fingerprint(&hot.2), fingerprint(&pack.2));
    assert_eq!(hot.2.len(), 2);
}

#[tokio::test]
async fn locating_an_event_matches_across_sources() {
    let (db, events) = packed_db("seam-locate").await;
    let runs = vec!["run-1".to_string(), "run-2".to_string()];
    let (hot, pack) = read_both(&db, || async {
        let mut reader = HistoryReader::new(&db);
        (
            reader
                .locate_event(&runs, &events.second_run)
                .await
                .unwrap(),
            reader.locate_event(&runs, "absent").await.unwrap(),
            reader
                .locate_event(&["run-2".to_string()], &events.plain)
                .await
                .unwrap(),
        )
    })
    .await;
    assert_eq!(hot, pack);
    assert_eq!(
        hot.0,
        Some(EventLocation {
            run_id: "run-2".to_string(),
            sequence: 1
        })
    );
    assert_eq!(hot.1, None);
    assert_eq!(hot.2, None, "an event outside the named runs is not found");
}

#[tokio::test]
async fn turn_reads_match_across_sources() {
    let (db, events) = packed_db("seam-turn").await;
    let (hot, pack) = read_both(&db, || async {
        let mut reader = HistoryReader::new(&db);
        (
            reader.turn_events("turn-1").await.unwrap(),
            reader.turn_start_sequence("turn-1", "run-1").await.unwrap(),
        )
    })
    .await;
    assert_eq!(fingerprint(&hot.0.rows), fingerprint(&pack.0.rows));
    assert_pack(&pack.0);
    assert_eq!(hot.1, pack.1);
    assert_eq!(hot.1, Some(1));
    // Sequence order, not insertion order.
    let order: Vec<&str> = hot.0.rows.iter().map(|row| row.event.id.as_str()).collect();
    assert_eq!(
        order,
        [
            events.plain.as_str(),
            events.restored.as_str(),
            events.tied.as_str(),
            events.read_result.as_str()
        ]
    );
}

#[tokio::test]
async fn session_reads_match_across_sources() {
    let (db, events) = packed_db("seam-session").await;
    let (hot, pack) = read_both(&db, || async {
        let mut reader = HistoryReader::new(&db);
        let all = reader
            .session_events("sess-1", None, EventFilter::default())
            .await
            .unwrap();
        let filtered = reader
            .session_events("sess-1", None, EventFilter::types(&["user", "assistant"]))
            .await
            .unwrap();
        let except = reader
            .session_events("sess-1", Some("run-1"), EventFilter::default())
            .await
            .unwrap();
        let after = reader
            .session_events_after("sess-2", None, i64::MIN, EventFilter::default())
            .await
            .unwrap();
        let page = reader
            .session_events_page("sess-1", None, i64::MIN, 2, EventFilter::default())
            .await
            .unwrap();
        let tail = reader.session_tail("sess-1", 2).await.unwrap();
        let max = reader.session_max_rowid("sess-2").await.unwrap();
        (all, filtered, except, after, page, tail, max)
    })
    .await;
    assert_eq!(fingerprint(&hot.0.rows), fingerprint(&pack.0.rows));
    assert_eq!(hot.0.rows.len(), 6);
    assert_pack(&pack.0);
    assert_eq!(fingerprint(&hot.1.rows), fingerprint(&pack.1.rows));
    assert_eq!(hot.1.rows.len(), 3);
    assert!(
        hot.2.rows.is_empty() && pack.2.rows.is_empty(),
        "the except run is left out"
    );
    assert_eq!(fingerprint(&hot.3.rows), fingerprint(&pack.3.rows));
    assert_eq!(fingerprint(&hot.4.rows), fingerprint(&pack.4.rows));
    assert_eq!(hot.4.rows[0].event.id, events.plain);
    assert_eq!(fingerprint(&hot.5.rows), fingerprint(&pack.5.rows));
    assert_eq!(hot.5.rows.len(), 2);
    assert!(
        hot.5.rows[0].rowid < hot.5.rows[1].rowid,
        "oldest of the tail first"
    );
    assert_eq!(hot.6, pack.6);
    assert!(hot.6.is_some());
}

#[tokio::test]
async fn read_segments_match_across_sources() {
    let (db, events) = packed_db("seam-segments").await;
    let wanted = vec![
        ("run-1".to_string(), events.read_result.clone()),
        ("run-1".to_string(), events.plain.clone()),
    ];
    let (hot, pack) = read_both(&db, || async {
        HistoryReader::new(&db)
            .read_segments(&wanted)
            .await
            .unwrap()
    })
    .await;
    assert_eq!(hot.json, pack.json);
    assert_eq!(hot.json.len(), 1);
    assert!(hot.pack_runs.is_empty());
    assert!(pack.pack_runs.contains("run-1"));
}

#[tokio::test]
async fn a_row_quarantined_after_packing_is_withheld_from_the_pack() {
    let (db, events) = packed_db("seam-quarantine-after").await;
    let guard = Quarantined::new(
        "seam-quarantine-after-source",
        TRANSCRIPT_EVENT_SINK,
        &events.restored,
    );
    let (hot, pack) = read_both(&db, || async {
        HistoryReader::new(&db)
            .run_events_by_id("run-1", std::slice::from_ref(&events.restored))
            .await
            .unwrap()
    })
    .await;
    drop(guard);
    assert_eq!(fingerprint(&hot), fingerprint(&pack));
    assert_eq!(pack[0].reconstruction, Reconstruction::Withheld);
    assert!(pack[0].event.data.contains(WITHHELD_PREFIX));
    assert!(!pack[0].event.data.contains("the restored body"));
}

#[tokio::test]
async fn a_pack_keyed_quarantine_withholds_only_the_pack_read() {
    let (db, events) = packed_db("seam-quarantine-pack").await;
    let guard = Quarantined::new(
        "seam-quarantine-pack-source",
        HISTORY_PACK_SINK,
        &events.restored,
    );
    let (hot, pack) = read_both(&db, || async {
        HistoryReader::new(&db)
            .run_events_by_id("run-1", std::slice::from_ref(&events.restored))
            .await
            .unwrap()
    })
    .await;
    drop(guard);
    assert_eq!(pack[0].reconstruction, Reconstruction::Withheld);
    assert!(pack[0].event.data.contains(WITHHELD_PREFIX));
    // The hot copy is a different record, quarantined separately.
    assert_eq!(hot[0].reconstruction, Reconstruction::Restored);
}

#[tokio::test]
async fn a_row_withheld_at_pack_time_is_stored_as_its_notice_and_regates_to_itself() {
    let db = crate::storage::migrated_test_db("seam-quarantine-before").await;
    let events = seed_fixture_execution(&db, "seam-quarantine-before").await;
    let guard = Quarantined::new(
        "seam-quarantine-before-source",
        TRANSCRIPT_EVENT_SINK,
        &events.restored,
    );
    let built = publish_for_test(&db, "exec-1").await;

    // Nothing of the body or the stored blob reached the pack.
    let pack = crate::storage::history_pack::HistoryPack::open(built.bytes).unwrap();
    let stored = pack
        .rows(Section::Events, "run-1")
        .map(Result::unwrap)
        .find(|row| row["id"] == events.restored.as_str())
        .unwrap();
    let text = serde_json::to_string(&stored).unwrap();
    assert!(!text.contains("the restored body"));
    assert_eq!(stored["reconstruction"], "withheld");
    assert!(
        stored.get("stored").is_none(),
        "no stored form of a withheld row"
    );

    let (first, again) = read_both(&db, || async {
        db.set_history_source_override(Some(HistorySource::Pack));
        HistoryReader::new(&db)
            .run_events_by_id("run-1", std::slice::from_ref(&events.restored))
            .await
            .unwrap()
    })
    .await;
    assert_eq!(fingerprint(&first), fingerprint(&again));
    drop(guard);

    // Released later, the pack still holds only the notice: its row reads as
    // what was stored, a withheld notice, not as the original content.
    db.set_history_source_override(Some(HistorySource::Pack));
    let released = HistoryReader::new(&db)
        .run_events_by_id("run-1", std::slice::from_ref(&events.restored))
        .await
        .unwrap();
    db.set_history_source_override(None);
    assert_eq!(released[0].reconstruction, Reconstruction::Withheld);
    assert!(released[0].event.data.contains(WITHHELD_PREFIX));
}

/// Set `evicted_at` as C2 will, to prove the production rule without the
/// override.
async fn mark_evicted(db: &LocalDb) {
    db.execute("UPDATE history_tier SET evicted_at = 5", ())
        .await
        .unwrap();
    db.mark_history_evicted();
}

#[tokio::test]
async fn a_run_is_hot_until_its_execution_is_evicted() {
    let (db, events) = packed_db("seam-source-rule").await;
    let hot = HistoryReader::new(&db)
        .run_events("run-1", EventFilter::default())
        .await
        .unwrap();
    assert_eq!(
        hot.provenance,
        Provenance::Hot,
        "a catalogued run reads hot"
    );
    let turn = HistoryReader::new(&db).turn_events("turn-1").await.unwrap();
    assert_eq!(turn.provenance, Provenance::Hot);
    let session = HistoryReader::new(&db)
        .session_events("sess-1", None, EventFilter::default())
        .await
        .unwrap();
    assert_eq!(session.provenance, Provenance::Hot);

    mark_evicted(&db).await;
    let evicted = HistoryReader::new(&db)
        .run_events("run-1", EventFilter::default())
        .await
        .unwrap();
    assert_pack(&evicted);
    assert_eq!(fingerprint(&hot.rows), fingerprint(&evicted.rows));
    assert_pack(&HistoryReader::new(&db).turn_events("turn-1").await.unwrap());
    assert_pack(
        &HistoryReader::new(&db)
            .session_events("sess-1", None, EventFilter::default())
            .await
            .unwrap(),
    );

    // Eviction reads the pack even once the hot rows are gone.
    db.execute("DELETE FROM events WHERE run_id = 'run-1'", ())
        .await
        .unwrap();
    let after_delete = HistoryReader::new(&db)
        .run_events("run-1", EventFilter::default())
        .await
        .unwrap();
    assert_eq!(fingerprint(&hot.rows), fingerprint(&after_delete.rows));
    assert!(after_delete
        .rows
        .iter()
        .any(|row| row.event.id == events.restored));
}

#[tokio::test]
async fn orphan_run_rows_are_ignored() {
    let db = crate::storage::migrated_test_db("seam-orphans").await;
    seed_fixture_execution(&db, "seam-orphans").await;
    // A crash between the run rows and the commit point leaves these.
    db.execute(
        "INSERT INTO history_tier_runs(run_id, scope_id) VALUES ('run-1', 'exec-1')",
        (),
    )
    .await
    .unwrap();
    let page = HistoryReader::new(&db)
        .run_events("run-1", EventFilter::default())
        .await
        .unwrap();
    assert_eq!(page.provenance, Provenance::Hot);
    assert_eq!(page.rows.len(), 6);
    assert!(catalog::tier_entry(&db, "exec-1").await.unwrap().is_none());
}

#[tokio::test]
async fn an_offline_archive_is_an_error_naming_it_never_a_stub() {
    let db = crate::storage::migrated_test_db("seam-offline").await;
    seed_fixture_execution(&db, "seam-offline").await;
    publish_for_test(&db, "exec-1").await;
    mark_evicted(&db).await;

    let root = db.path().with_extension("archive");
    std::fs::rename(&root, root.with_extension("unplugged")).unwrap();
    let error = HistoryReader::new(&db)
        .run_events("run-1", EventFilter::default())
        .await
        .unwrap_err();
    assert!(matches!(error, HistoryError::Offline { .. }), "{error}");
    let message = error.to_string();
    assert_eq!(message, format!("archive offline: {}", root.display()));
    // The message survives the conversion callers use.
    assert!(DbError::from(error).to_string().contains("archive offline"));
}

#[tokio::test]
async fn a_pack_the_store_lost_is_an_error_not_a_stub() {
    let db = crate::storage::migrated_test_db("seam-missing").await;
    seed_fixture_execution(&db, "seam-missing").await;
    publish_for_test(&db, "exec-1").await;
    db.execute(
        "UPDATE history_tier SET content_hash = '0000000000000000000000000000000000000000000000000000000000000000', evicted_at = 5",
        (),
    )
    .await
    .unwrap();
    db.mark_history_evicted();
    let error = HistoryReader::new(&db)
        .run_events("run-1", EventFilter::default())
        .await
        .unwrap_err();
    assert!(matches!(error, HistoryError::Offline { .. }), "{error}");
    assert_eq!(
        error.to_string(),
        format!(
            "archive offline: {}",
            db.path().with_extension("archive").display()
        )
    );
}

#[tokio::test]
async fn the_hot_statements_stay_on_their_indexes() {
    let db = crate::storage::migrated_test_db("seam-plans").await;
    let text = |value: &str| Value::Text(value.to_string());
    let int = Value::Integer;
    let plan_uses = |plan: &str, index: &str| {
        assert!(plan.contains(index), "expected {index}, got {plan}");
        assert!(!plan.contains("SCAN"), "a scan: {plan}");
        assert!(!plan.contains("SORTER"), "a sort: {plan}");
    };
    let filter = EventFilter::default();

    plan_uses(
        &query_plan(
            &db,
            &statements::run_page(&filter),
            vec![text("r"), int(0), int(512)],
        )
        .await,
        "idx_events_run_id",
    );
    plan_uses(
        &query_plan(
            &db,
            &statements::run_window(&filter),
            vec![text("r"), int(i64::MAX), int(i64::MIN), int(500)],
        )
        .await,
        "idx_events_run_sequence",
    );
    plan_uses(
        &query_plan(&db, &statements::run_sequence(), vec![text("r"), int(1)]).await,
        "idx_events_run_sequence",
    );
    plan_uses(
        &query_plan(
            &db,
            &statements::turn_page(),
            vec![text("t"), int(0), int(512)],
        )
        .await,
        "idx_events_turn_id",
    );
    plan_uses(
        &query_plan(
            &db,
            &statements::session_page(&filter, false),
            vec![text("s"), int(0), int(512)],
        )
        .await,
        "idx_events_session_id",
    );
    plan_uses(
        &query_plan(&db, &statements::session_tail(), vec![text("s"), int(64)]).await,
        "idx_events_session_id",
    );
    plan_uses(
        &query_plan(&db, statements::SESSION_MAX_ROWID, vec![text("s")]).await,
        "idx_events_session_id",
    );
    plan_uses(
        &query_plan(&db, statements::TURN_START, vec![text("t"), text("r")]).await,
        "idx_events_turn_id",
    );

    plan_uses(
        &query_plan(
            &db,
            &statements::run_slice(),
            vec![text("r"), int(512), int(10)],
        )
        .await,
        "idx_events_run_sequence",
    );
    plan_uses(
        &query_plan(
            &db,
            statements::SESSION_KEYS,
            vec![text("s"), int(0), int(512)],
        )
        .await,
        "idx_events_session_id",
    );

    // The filtered session replay keeps its keyset order on the session index.
    let replay = EventFilter::types(&["user", "assistant", "tool_result", "user:launch"]);
    let replay_plan = query_plan(
        &db,
        &statements::session_page(&replay, true),
        vec![
            text("s"),
            int(0),
            int(512),
            text("r"),
            text("user"),
            text("assistant"),
            text("tool_result"),
            text("user:launch"),
        ],
    )
    .await;
    plan_uses(&replay_plan, "idx_events_session_id");
    plan_uses(
        &query_plan(
            &db,
            &statements::run_page(&EventFilter::types(&["assistant"])),
            vec![text("r"), int(0), int(512), text("assistant")],
        )
        .await,
        "idx_events_run_id",
    );
    let by_id = query_plan(
        &db,
        &statements::run_ids(2),
        vec![text("r"), text("a"), text("b")],
    )
    .await;
    assert!(
        !by_id.contains("SCAN") && !by_id.contains("idx_events_run"),
        "an id lookup seeks the primary key, not the whole run: {by_id}"
    );

    for count in [256, 512] {
        let runs: Vec<_> = (0..count).map(|id| text(&format!("run-{id}"))).collect();
        let sources = query_plan(&db, &statements::run_sources(count), runs.clone()).await;
        plan_uses(&sources, "history_tier_runs");
        plan_uses(&sources, "history_tier");
        let mut ids = vec![text("r")];
        ids.extend((0..count).map(|id| text(&format!("event-{id}"))));
        let by_id = query_plan(&db, &statements::run_ids(count), ids).await;
        assert!(
            by_id.contains("SEARCH")
                && !by_id.contains("SCAN")
                && !by_id.contains("idx_events_run")
                && !by_id.contains("SORTER"),
            "{by_id}"
        );
        let mut scopes = runs;
        scopes.extend((0..count).map(|id| text(&format!("session-{id}"))));
        let scopes = query_plan(&db, &statements::scope_sources(count, count), scopes).await;
        plan_uses(&scopes, "history_tier_runs");
        plan_uses(&scopes, "history_tier_sessions");
        plan_uses(&scopes, "history_tier");
    }
    let locate = query_plan(&db, statements::LOCATE_EVENT, vec![text("event")]).await;
    assert!(
        locate.contains("SEARCH") && !locate.contains("SCAN") && !locate.contains("SORTER"),
        "{locate}"
    );

    let turn = query_plan(&db, statements::SOURCE_OF_TURN, vec![text("t")]).await;
    plan_uses(&turn, "history_tier_runs");
    plan_uses(&turn, "history_tier_sessions");
    assert!(!turn.contains("jobs"), "no execution join: {turn}");
    let session = query_plan(&db, statements::SOURCE_OF_SESSION, vec![text("s")]).await;
    plan_uses(&session, "history_tier_sessions");
    assert!(!session.contains("jobs"), "no execution join: {session}");
}

const RANGE_SCOPE: &str = "job-1/1-2";

/// The fixture's two sessions as one thread range, with `run-1` straddling
/// into `sess-2`, and a hot third session beside it.
async fn thread_range_db(name: &str) -> (LocalDb, crate::storage::history_pack::write::BuiltPack) {
    use crate::storage::history_pack::testing::{
        insert_event, publish_unit_for_test, seed_hot_tail,
    };
    use crate::storage::history_pack::write::{PackUnit, ThreadRange};

    let db = crate::storage::migrated_test_db(name).await;
    seed_fixture_execution(&db, name).await;
    // A turn of sess-2 whose run belongs to sess-1: the straddle a rotation
    // can leave. It joins the two sessions into one range.
    db.execute_script(
        "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
         VALUES('turn-straddle','sess-2','run-1','job-1',2,'complete',21,21);",
    )
    .await
    .unwrap();
    insert_event(
        &db,
        &format!("{name}-straddle"),
        "run-1",
        "sess-2",
        Some("turn-straddle"),
        7,
        21,
    )
    .await;
    seed_hot_tail(&db, name).await;
    let range = ThreadRange {
        owner_job_id: "job-1".into(),
        first_sequence: 1,
        last_sequence: 2,
        through_generation: 4,
        jobs: vec!["job-1".into()],
        sessions: vec!["sess-1".into(), "sess-2".into()],
        runs: vec!["run-1".into(), "run-2".into()],
    };
    assert_eq!(range.scope_id(), RANGE_SCOPE);
    let built = publish_unit_for_test(&db, &PackUnit::ThreadRange(range)).await;
    (db, built)
}

/// Every read a transcript makes of one job, in a fixed order: each run, each
/// turn, and each session's whole read, tail and after-cursor read, then the
/// lineage path (keys, then bodies by id) and each session's newest rowid.
async fn every_job_read(db: &LocalDb) -> (Vec<Page>, Vec<String>, Vec<Option<i64>>) {
    let mut reader = HistoryReader::new(db);
    let mut pages = Vec::new();
    for run in ["run-1", "run-2", "run-3"] {
        pages.push(
            reader
                .run_events(run, EventFilter::default())
                .await
                .unwrap(),
        );
    }
    for turn in [
        "turn-1",
        "turn-2",
        "turn-straddle",
        "turn-3",
        "turn-runless",
    ] {
        pages.push(reader.turn_events(turn).await.unwrap());
    }
    let mut lineage = Vec::new();
    let mut max_rowids = Vec::new();
    for session in ["sess-1", "sess-2", "sess-3"] {
        pages.push(
            reader
                .session_events(session, None, EventFilter::default())
                .await
                .unwrap(),
        );
        pages.push(reader.session_tail(session, 2).await.unwrap());
        pages.push(
            reader
                .session_events_after(session, None, i64::MIN, EventFilter::default())
                .await
                .unwrap(),
        );
        let mut by_run: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for key in reader.session_event_keys(session).await.unwrap() {
            by_run.entry(key.run_id).or_default().push(key.id);
        }
        for (run, ids) in by_run {
            lineage.extend(reader.run_events_by_id(&run, &ids).await.unwrap());
        }
        max_rowids.push(reader.session_max_rowid(session).await.unwrap());
    }
    (pages, fingerprint(&lineage), max_rowids)
}

#[tokio::test]
async fn a_thread_range_serves_its_sessions_and_leaves_the_rest_of_the_job_hot() {
    let (db, built) = thread_range_db("seam-thread-range").await;
    let manifest = &built.manifest;
    assert_eq!(
        (manifest.scope.kind, manifest.scope.id.as_str()),
        (ScopeKind::ThreadRange, RANGE_SCOPE)
    );
    assert_eq!(manifest.execution_id.as_deref(), Some("exec-1"));
    let sessions: Vec<&str> = manifest.sessions.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(sessions, ["sess-1", "sess-2"]);
    for run in &manifest.runs {
        for session in &run.session_ids {
            assert!(sessions.contains(&session.as_str()), "closed: {run:?}");
        }
    }
    let turns: Vec<&str> = manifest.turns.iter().map(|t| t.id.as_str()).collect();
    assert_eq!(turns, ["turn-1", "turn-2", "turn-straddle"]);
    let entry = catalog::tier_entry(&db, RANGE_SCOPE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            entry.kind,
            entry.owner_id.as_str(),
            entry.through_generation
        ),
        (ScopeKind::ThreadRange, "job-1", Some(4))
    );
    assert!(
        catalog::tier_entry(&db, "exec-1").await.unwrap().is_none(),
        "the range is catalogued under its own scope, not the thread's execution"
    );

    let (hot, pack) = read_both(&db, || every_job_read(&db)).await;
    let packed = Provenance::Pack(PackRef {
        scope_id: RANGE_SCOPE.to_string(),
        kind: ScopeKind::ThreadRange,
        content_hash: built.content_hash.clone(),
    });
    // Three runs, five turns, then three reads for each of three sessions.
    let mut expected = vec![packed.clone(), packed.clone(), Provenance::Hot];
    expected.extend([packed.clone(), packed.clone(), packed.clone()]);
    expected.extend([Provenance::Hot, Provenance::Hot]);
    expected.extend(std::iter::repeat_n(packed.clone(), 6));
    expected.extend(std::iter::repeat_n(Provenance::Hot, 3));
    let provenance: Vec<_> = pack.0.iter().map(|page| page.provenance.clone()).collect();
    assert_eq!(provenance, expected);
    assert!(hot.0.iter().all(|page| page.provenance == Provenance::Hot));
    for (index, (hot, pack)) in hot.0.iter().zip(&pack.0).enumerate() {
        assert!(!hot.rows.is_empty(), "read {index} is empty");
        assert_eq!(
            fingerprint(&hot.rows),
            fingerprint(&pack.rows),
            "read {index}"
        );
    }
    assert_eq!(hot.1, pack.1);
    assert_eq!(hot.2, pack.2);
    let sess_2 = &hot.0[11];
    assert!(
        sess_2
            .rows
            .iter()
            .any(|row| row.event.run_id == "run-1"
                && row.event.session_id.as_deref() == Some("sess-2")),
        "sess-2 holds the straddling run's event"
    );

    // Evict the range as C2 will and delete its hot rows: the same reads still
    // answer, from the pack, and the tail from hot.
    db.execute_script(
        "UPDATE history_tier SET evicted_at = 5;
         DELETE FROM events WHERE run_id IN ('run-1', 'run-2');",
    )
    .await
    .unwrap();
    db.mark_history_evicted();
    let evicted = every_job_read(&db).await;
    for (index, (hot, evicted)) in hot.0.iter().zip(&evicted.0).enumerate() {
        assert_eq!(
            fingerprint(&hot.rows),
            fingerprint(&evicted.rows),
            "read {index}"
        );
    }
    assert_eq!(hot.1, evicted.1);
    assert_eq!(hot.2, evicted.2);
}

#[tokio::test]
async fn a_turn_or_session_no_pack_maps_reads_hot_whatever_its_execution_catalog_says() {
    use crate::storage::history_pack::testing::{insert_event, seed_hot_tail};

    let db = crate::storage::migrated_test_db("seam-unmapped").await;
    seed_fixture_execution(&db, "seam-unmapped").await;
    // A run-less turn of a packed session resolves through the session.
    db.execute_script(
        "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
         VALUES('turn-runless-packed','sess-1',NULL,'job-1',2,'complete',14,14);",
    )
    .await
    .unwrap();
    insert_event(
        &db,
        "seam-unmapped-runless",
        "run-1",
        "sess-1",
        Some("turn-runless-packed"),
        7,
        14,
    )
    .await;
    let built = publish_for_test(&db, "exec-1").await;
    // History its execution gained after the pack: exec-1 has a catalog row,
    // and nothing maps any of this.
    seed_hot_tail(&db, "seam-unmapped").await;

    db.set_history_source_override(Some(HistorySource::Pack));
    let packed = Source::Pack(PackRef {
        scope_id: "exec-1".to_string(),
        kind: ScopeKind::Execution,
        content_hash: built.content_hash.clone(),
    });
    let mut reader = HistoryReader::new(&db);
    assert_eq!(
        reader.source_of_session("sess-3").await.unwrap(),
        Source::Hot
    );
    assert_eq!(reader.source_of_turn("turn-3").await.unwrap(), Source::Hot);
    assert_eq!(
        reader.source_of_turn("turn-runless").await.unwrap(),
        Source::Hot
    );
    assert_eq!(reader.source_of_run("run-3").await.unwrap(), Source::Hot);
    assert_eq!(reader.source_of_session("sess-1").await.unwrap(), packed);
    assert_eq!(reader.source_of_turn("turn-1").await.unwrap(), packed);
    assert_eq!(
        reader.source_of_turn("turn-runless-packed").await.unwrap(),
        packed
    );
    let mut primed = HistoryReader::new(&db);
    primed
        .prime_sessions_and_runs(&["sess-3".into(), "sess-1".into()], &["run-3".into()])
        .await
        .unwrap();
    assert_eq!(primed.sessions["sess-3"], Source::Hot);
    assert_eq!(primed.runs["run-3"], Source::Hot);
    assert_eq!(primed.sessions["sess-1"], packed);

    let tail = reader
        .session_events("sess-3", None, EventFilter::default())
        .await
        .unwrap();
    assert_eq!(tail.provenance, Provenance::Hot);
    assert_eq!(
        tail.rows.len(),
        2,
        "the unmapped session is read whole, hot"
    );
    let runless = reader.turn_events("turn-runless-packed").await.unwrap();
    assert_pack(&runless);
    db.set_history_source_override(Some(HistorySource::Hot));
    let hot = HistoryReader::new(&db)
        .turn_events("turn-runless-packed")
        .await
        .unwrap();
    db.set_history_source_override(None);
    assert_eq!(hot.rows.len(), 1);
    assert_eq!(fingerprint(&hot.rows), fingerprint(&runless.rows));
}

async fn all_event_reads(db: &LocalDb, ids: &[String]) -> Vec<Vec<String>> {
    let mut reader = HistoryReader::new(db);
    vec![
        fingerprint(
            &reader
                .run_events_page("run-1", i64::MIN, 512, EventFilter::default())
                .await
                .unwrap()
                .rows,
        ),
        fingerprint(
            &reader
                .run_events("run-1", EventFilter::default())
                .await
                .unwrap()
                .rows,
        ),
        fingerprint(
            &reader
                .run_events_window("run-1", i64::MAX, i64::MIN, 512, EventFilter::default())
                .await
                .unwrap()
                .rows,
        ),
        fingerprint(&[reader
            .run_event_by_sequence("run-1", 7)
            .await
            .unwrap()
            .unwrap()]),
        fingerprint(&reader.run_events_by_id("run-1", ids).await.unwrap()),
        fingerprint(&reader.turn_events("turn-1").await.unwrap().rows),
        fingerprint(
            &reader
                .session_events_page("sess-1", None, i64::MIN, 512, EventFilter::default())
                .await
                .unwrap()
                .rows,
        ),
        fingerprint(
            &reader
                .session_events("sess-1", None, EventFilter::default())
                .await
                .unwrap()
                .rows,
        ),
    ]
}

#[tokio::test]
async fn every_event_read_preserves_a_restored_gitcoord_and_a_withheld_row() {
    use crate::storage::content_store::content_hash;
    use crate::storage::events::compress;
    use turso::params;

    let db = crate::storage::migrated_test_db("seam-mixed-outcomes").await;
    let events = seed_fixture_execution(&db, "seam-mixed-outcomes").await;
    let body = b"the restored prompt segment";
    let hash = content_hash(body);
    db.execute(
        "INSERT INTO archival_blobs(hash, content, created_at) VALUES (?1, ?2, 1)",
        params![hash.as_str(), compress(body).unwrap()],
    )
    .await
    .unwrap();
    let data = serde_json::json!({
        "eventType": "system:prompt", "archived": "system_prompt",
        "segments": [{"hash": hash, "kind": "base"}, {"inline": " and tail"}],
    })
    .to_string();
    let gitcoord = "seam-mixed-outcomes-gitcoord";
    db.execute(
        "INSERT INTO events(id, run_id, session_id, turn_id, sequence, timestamp,
                            event_type, data, created_at, storage_mode)
         VALUES (?1, 'run-1', 'sess-1', 'turn-1', 7, 14, 'system:prompt', ?2, 14, 'gitcoord')",
        params![gitcoord, data],
    )
    .await
    .unwrap();
    let _guard = Quarantined::new(
        "seam-mixed-outcomes-source",
        TRANSCRIPT_EVENT_SINK,
        &events.restored,
    );
    publish_for_test(&db, "exec-1").await;
    let ids = vec![gitcoord.to_string(), events.restored.clone()];
    let (hot, pack) = read_both(&db, || all_event_reads(&db, &ids)).await;
    assert_eq!(hot, pack);
    for rows in &hot {
        assert!(
            rows.iter()
                .any(|row| row.contains("the restored prompt segment and tail")
                    && row.contains("reconstruction: Restored")),
            "{rows:?}"
        );
    }
    // All but the single-coordinate lookup also carry the quarantined row.
    for index in [0, 1, 2, 4, 5, 6, 7] {
        assert!(
            hot[index].iter().any(
                |row| row.contains("reconstruction: Withheld") && row.contains(WITHHELD_PREFIX)
            ),
            "{:?}",
            hot[index]
        );
        assert!(!hot[index]
            .iter()
            .any(|row| row.contains("the restored body")));
    }
    // Once packed, no coordinate reconstruction is needed, even without its blob.
    db.execute("DELETE FROM archival_blobs", ()).await.unwrap();
    db.set_history_source_override(Some(HistorySource::Pack));
    assert_eq!(pack, all_event_reads(&db, &ids).await);
    db.set_history_source_override(None);
}

/// Three frames per run, with interleaved rowids, two sessions in each run,
/// and chat timestamps deliberately disagreeing with rowid order.
async fn multi_frame_session_db(
    name: &str,
) -> (LocalDb, crate::storage::history_pack::write::BuiltPack) {
    let db = crate::storage::migrated_test_db(name).await;
    seed_fixture_execution(&db, name).await;
    db.execute_script(format!(
        "DELETE FROM tool_invocations;
         DELETE FROM event_read_tokens;
         DELETE FROM event_vibes;
         DELETE FROM events;
         INSERT INTO events(rowid, id, run_id, session_id, sequence, timestamp,
                            event_type, data, created_at)
         WITH RECURSIVE numbers(n) AS (
             VALUES(1) UNION ALL SELECT n + 1 FROM numbers WHERE n < {}
         )
         SELECT n, 'multi-frame-' || n,
                CASE WHEN n % 2 = 1 THEN 'run-1' ELSE 'run-2' END,
                CASE WHEN n % 5 = 0 THEN 'sess-2' ELSE 'sess-1' END,
                n, n,
                CASE WHEN n % 6 = 3 THEN 'assistant' ELSE 'user' END,
                '{{}}', n % 3
         FROM numbers;",
        crate::storage::history_pack::FRAME_ROWS * 6
    ))
    .await
    .unwrap();
    let built = publish_for_test(&db, "exec-1").await;
    let pack = HistoryPack::open(built.bytes.clone()).unwrap();
    for run in ["run-1", "run-2"] {
        assert_eq!(
            pack.index()
                .iter()
                .filter(|entry| entry.section == Section::Events && entry.key == run)
                .count(),
            3
        );
    }
    (db, built)
}

/// Inject damage after publication, directly into the reader's opened-pack
/// cache. Corruption is a decode tripwire, not a content-store integrity test.
fn reader_with_damaged_session_frames<'a>(
    db: &'a LocalDb,
    built: &crate::storage::history_pack::write::BuiltPack,
    damage: impl Fn(&crate::storage::history_pack::IndexEntry) -> bool,
) -> HistoryReader<'a> {
    let pack = HistoryPack::open(built.bytes.clone()).unwrap();
    let mut bytes = built.bytes.clone();
    let mut damaged = 0;
    for entry in pack.index() {
        if entry.section == Section::Events && damage(entry) {
            bytes[entry.offset as usize..(entry.offset + entry.len) as usize].fill(0);
            damaged += 1;
        }
    }
    assert!(damaged > 0);
    let damaged = HistoryPack::open(bytes).unwrap();
    assert!(damaged.verify().is_err(), "the decode tripwire must fail");
    let mut reader = HistoryReader::new(db);
    reader
        .packs
        .insert(built.content_hash.clone(), Arc::new(damaged));
    reader
}

#[tokio::test]
async fn session_pages_do_not_decode_later_or_excluded_run_frames() {
    let (db, built) = multi_frame_session_db("seam-session-bounded").await;
    let frame_end = (crate::storage::history_pack::FRAME_ROWS * 2) as i64;
    // Only first frames are readable. In particular, seeking a filtered head
    // eagerly through each run would trip over a later damaged frame.
    for (except, filter, limit) in [
        (None, EventFilter::default(), 1),
        (None, EventFilter::types(&["assistant"]), 64),
        (Some("run-2"), EventFilter::default(), 410),
    ] {
        db.set_history_source_override(Some(HistorySource::Hot));
        let expected = HistoryReader::new(&db)
            .session_events_page("sess-1", except, i64::MIN, limit, filter)
            .await
            .unwrap();
        db.set_history_source_override(Some(HistorySource::Pack));
        let mut reader = reader_with_damaged_session_frames(&db, &built, |entry| {
            entry.first_rowid > frame_end || (except == Some("run-2") && entry.key == "run-2")
        });
        let actual = reader
            .session_events_page("sess-1", except, i64::MIN, limit, filter)
            .await
            .unwrap();
        assert_pack(&actual);
        assert_eq!(fingerprint(&actual.rows), fingerprint(&expected.rows));
        assert_eq!(actual.fetched, limit);
        assert_eq!(actual.last_rowid, expected.last_rowid);
        if except.is_some() {
            // The 410th session row is the last row of run-1's first frame.
            assert_eq!(actual.last_rowid, Some(frame_end - 1));
            assert!(
                reader
                    .session_events_page("sess-1", except, actual.last_rowid.unwrap(), 1, filter,)
                    .await
                    .is_err(),
                "the next requested page must reach the damaged frame"
            );
        }
    }
    db.set_history_source_override(None);
}

#[tokio::test]
async fn session_page_cursor_skips_earlier_frames_without_decoding_them() {
    let (db, built) = multi_frame_session_db("seam-session-seek-bounded").await;
    let frame_span = (crate::storage::history_pack::FRAME_ROWS * 2) as i64;
    let after = frame_span + 2;
    let filter = EventFilter::types(&["assistant"]);
    let expected = HistoryReader::new(&db)
        .session_events_page("sess-1", None, after, 64, filter)
        .await
        .unwrap();
    db.set_history_source_override(Some(HistorySource::Pack));
    // Only middle frames are readable: neither the prefix nor suffix may be
    // decoded to answer this page.
    let mut reader = reader_with_damaged_session_frames(&db, &built, |entry| {
        entry.first_rowid <= frame_span || entry.first_rowid > frame_span * 2
    });
    let actual = reader
        .session_events_page("sess-1", None, after, 64, filter)
        .await
        .unwrap();
    assert_eq!(fingerprint(&actual.rows), fingerprint(&expected.rows));
    assert_eq!(actual.fetched, 64);
    assert_eq!(actual.last_rowid, expected.last_rowid);
    assert!(actual.rows.iter().all(|row| row.rowid > after));
    assert!(
        reader
            .session_events_page("sess-1", None, after, frame_span as usize, filter)
            .await
            .is_err(),
        "requesting beyond the middle frames must trip the damaged suffix"
    );
    db.set_history_source_override(None);
}

#[tokio::test]
async fn multi_frame_session_reads_keep_paging_chat_and_tail_order() {
    let (db, _) = multi_frame_session_db("seam-session-multi-order").await;
    let (hot, pack) = read_both(&db, || async {
        let mut reader = HistoryReader::new(&db);
        let rows = reader
            .session_events_after("sess-1", None, i64::MIN, EventFilter::default())
            .await
            .unwrap();
        let chat = reader
            .session_events("sess-1", None, EventFilter::default())
            .await
            .unwrap();
        let tail = reader.session_tail("sess-1", 9).await.unwrap();
        let empty = reader
            .session_events_page("sess-1", None, i64::MAX, 1, EventFilter::default())
            .await
            .unwrap();
        (rows, chat, tail, empty)
    })
    .await;
    for (hot, pack) in [
        (&hot.0, &pack.0),
        (&hot.1, &pack.1),
        (&hot.2, &pack.2),
        (&hot.3, &pack.3),
    ] {
        assert_eq!(fingerprint(&hot.rows), fingerprint(&pack.rows));
        assert_eq!(hot.fetched, pack.fetched);
        assert_eq!(hot.last_rowid, pack.last_rowid);
    }
    assert!(pack
        .0
        .rows
        .windows(2)
        .all(|pair| pair[0].rowid < pair[1].rowid));
    assert!(pack.1.rows.windows(2).all(|pair| {
        (pair[0].event.created_at, pair[0].rowid) < (pair[1].event.created_at, pair[1].rowid)
    }));
    let newest = &pack.0.rows[pack.0.rows.len() - 9..];
    assert_eq!(fingerprint(&pack.2.rows), fingerprint(newest));
    assert!(pack.3.rows.is_empty());
}

#[tokio::test]
async fn filtered_window_applies_types_before_limit_in_sequence_order() {
    let (db, events) = packed_db("seam-filtered-window").await;
    for (below, floor, limit, types, expected) in [
        (
            i64::MAX,
            i64::MIN,
            2,
            vec!["assistant"],
            vec![events.undecodable.as_str(), events.restored.as_str()],
        ),
        (6, 1, 1, vec!["assistant"], vec![events.restored.as_str()]),
        (
            6,
            1,
            2,
            vec!["assistant", "user"],
            vec![events.restored.as_str(), events.plain.as_str()],
        ),
        (i64::MAX, i64::MIN, 2, vec![], vec![]),
        (i64::MAX, i64::MIN, 0, vec!["assistant"], vec![]),
    ] {
        let (hot, pack) = read_both(&db, || async {
            HistoryReader::new(&db)
                .run_events_window("run-1", below, floor, limit, EventFilter::types(&types))
                .await
                .unwrap()
        })
        .await;
        assert_pack(&pack);
        assert_eq!(fingerprint(&hot.rows), fingerprint(&pack.rows));
        for page in [&hot, &pack] {
            assert_eq!(
                page.rows
                    .iter()
                    .map(|row| row.event.id.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(page.fetched, expected.len());
        }
    }
}

#[tokio::test]
async fn filtered_window_keeps_the_run_sequence_plan() {
    let db = crate::storage::migrated_test_db("seam-filtered-window-plan").await;
    let text = |s: &str| turso::Value::Text(s.to_string());
    let int = turso::Value::Integer;
    let plan_uses = |plan: &str, index: &str| {
        assert!(plan.contains(index), "expected {index}, got {plan}");
        assert!(!plan.contains("SCAN"), "a scan: {plan}");
        assert!(!plan.contains("SORTER"), "a sort: {plan}");
    };
    plan_uses(
        &query_plan(
            &db,
            &statements::run_window(&EventFilter::types(&["assistant", "user"])),
            vec![
                text("r"),
                int(i64::MAX),
                int(i64::MIN),
                int(20),
                text("assistant"),
                text("user"),
            ],
        )
        .await,
        "idx_events_run_sequence",
    );
}

#[tokio::test]
async fn rowid_lookup_matches_sources_and_enforces_exact_run_scoping() {
    let (db, _) = packed_db("seam-rowid-parity").await;
    let mut expected = Vec::new();
    for run in ["run-1", "run-2"] {
        expected.extend(
            HistoryReader::new(&db)
                .run_events(run, EventFilter::default())
                .await
                .unwrap()
                .rows,
        );
    }
    let (hot, pack) = read_both(&db, || async {
        let mut reader = HistoryReader::new(&db);
        let mut found = Vec::new();
        for row in &expected {
            found.push(
                reader
                    .run_event_by_rowid(&row.event.run_id, row.rowid)
                    .await
                    .unwrap()
                    .unwrap(),
            );
            let other_run = if row.event.run_id == "run-1" {
                "run-2"
            } else {
                "run-1"
            };
            assert!(reader
                .run_event_by_rowid(other_run, row.rowid)
                .await
                .unwrap()
                .is_none());
        }
        for rowid in [i64::MIN, 0, i64::MAX] {
            assert!(reader
                .run_event_by_rowid("run-1", rowid)
                .await
                .unwrap()
                .is_none());
        }
        assert!(reader
            .run_event_by_rowid("absent-run", expected[0].rowid)
            .await
            .unwrap()
            .is_none());
        found
    })
    .await;
    assert_eq!(fingerprint(&hot), fingerprint(&expected));
    assert_eq!(fingerprint(&pack), fingerprint(&expected));
    crate::storage::history_pack::testing::evict_all(&db).await;
    for expected in expected {
        let actual = HistoryReader::new(&db)
            .run_event_by_rowid(&expected.event.run_id, expected.rowid)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fingerprint(&[actual]), fingerprint(&[expected]));
    }
}

#[tokio::test]
async fn rowid_lookup_gates_hot_and_post_pack_quarantine_last() {
    let (db, events) = packed_db("seam-rowid-quarantine").await;
    let rows = HistoryReader::new(&db)
        .run_events("run-1", EventFilter::default())
        .await
        .unwrap()
        .rows;
    let rowid = rows
        .iter()
        .find(|row| row.event.id == events.restored)
        .unwrap()
        .rowid;
    let _transcript = Quarantined::new(
        "seam-rowid-transcript-quarantine",
        TRANSCRIPT_EVENT_SINK,
        &events.restored,
    );
    let (hot, pack) = read_both(&db, || async {
        HistoryReader::new(&db)
            .run_event_by_rowid("run-1", rowid)
            .await
            .unwrap()
            .unwrap()
    })
    .await;
    assert_eq!(
        fingerprint(std::slice::from_ref(&hot)),
        fingerprint(std::slice::from_ref(&pack))
    );
    for row in [hot, pack] {
        assert_eq!(row.reconstruction, Reconstruction::Withheld);
        assert!(row.event.data.contains(WITHHELD_PREFIX));
        assert!(!row.event.data.contains("the restored body"));
    }

    let second = HistoryReader::new(&db)
        .run_events("run-2", EventFilter::default())
        .await
        .unwrap()
        .rows
        .into_iter()
        .find(|row| row.event.id == events.second_run)
        .unwrap();
    let _packed = Quarantined::new(
        "seam-rowid-pack-quarantine",
        HISTORY_PACK_SINK,
        &events.second_run,
    );
    db.set_history_source_override(Some(HistorySource::Pack));
    let withheld = HistoryReader::new(&db)
        .run_event_by_rowid("run-2", second.rowid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(withheld.reconstruction, Reconstruction::Withheld);
    assert!(withheld.event.data.contains(WITHHELD_PREFIX));
    assert!(!withheld.event.data.contains("second run"));
    db.set_history_source_override(None);
}

#[tokio::test]
async fn rowid_lookup_decodes_only_the_candidate_frame_even_on_a_miss() {
    let (db, built) = multi_frame_session_db("seam-rowid-bounded").await;
    let span = (crate::storage::history_pack::FRAME_ROWS * 2) as i64;
    let mut expected = Vec::new();
    for rowid in [span + 1, span + 3, span * 2 - 1] {
        expected.push(
            HistoryReader::new(&db)
                .run_event_by_rowid("run-1", rowid)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    db.set_history_source_override(Some(HistorySource::Pack));
    let mut reader = reader_with_damaged_session_frames(&db, &built, |entry| {
        entry.key != "run-1" || entry.first_rowid != span + 1
    });
    for row in expected {
        let actual = reader
            .run_event_by_rowid("run-1", row.rowid)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fingerprint(&[actual]), fingerprint(&[row]));
    }
    // The key lies after the candidate's last row, before the next frame.
    assert!(reader
        .run_event_by_rowid("run-1", span * 2)
        .await
        .unwrap()
        .is_none());
    // A key before the first indexed row must not inflate even that frame.
    assert!(reader
        .run_event_by_rowid("run-1", 0)
        .await
        .unwrap()
        .is_none());
    assert!(
        reader
            .run_event_by_rowid("run-1", span * 2 + 1)
            .await
            .is_err(),
        "asking for the damaged frame itself must trip the decode guard"
    );
    db.set_history_source_override(None);
}

#[tokio::test]
async fn rowid_lookup_seeks_the_hot_rowid_not_the_whole_run() {
    let db = crate::storage::migrated_test_db("seam-rowid-plan").await;
    let plan = query_plan(
        &db,
        &statements::run_rowid(),
        vec![Value::Text("run-1".into()), Value::Integer(7)],
    )
    .await;
    assert!(plan.contains("SEARCH") && plan.contains("rowid"), "{plan}");
    assert!(
        !plan.contains("SCAN") && !plan.contains("idx_events_run") && !plan.contains("SORTER"),
        "{plan}"
    );
}

#[tokio::test]
async fn rowid_lookup_propagates_offline_catalog_even_with_hot_rows() {
    let (db, events) = packed_db("seam-rowid-offline").await;
    let rowid = db
        .query_opt(
            "SELECT rowid FROM events WHERE id = ?1",
            (events.restored,),
            |row| row.i64(0),
        )
        .await
        .unwrap()
        .unwrap();
    mark_evicted(&db).await;
    let root = db.path().with_extension("archive");
    std::fs::rename(&root, root.with_extension("unplugged")).unwrap();
    let error = HistoryReader::new(&db)
        .run_event_by_rowid("run-1", rowid)
        .await
        .unwrap_err();
    assert!(matches!(error, HistoryError::Offline { .. }), "{error}");
    assert_eq!(
        error.to_string(),
        format!("archive offline: {}", root.display())
    );
}

#[tokio::test]
async fn storeless_team_replica_reads_hot_without_a_private_catalog() {
    use crate::storage::{MigrationRunner, TEAM_MIGRATIONS};

    let temp = tempfile::tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("team.db")).await.unwrap();
    MigrationRunner::new(TEAM_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    assert!(db.team_content_store().is_none());
    db.execute_script(
        "INSERT INTO teams (id, name, created_at, updated_at) VALUES ('team', 'Team', 1, 1);
         INSERT INTO projects (id, team_id, name, key, repo_path, created_at, updated_at)
         VALUES ('project', 'team', 'Team', 'TEAM', '/tmp/team', 1, 1);
         INSERT INTO jobs (id, project_id, status, current_session_id, created_at, updated_at)
         VALUES ('job', 'project', 'running', 'session', 1, 1);
         INSERT INTO runs (id, job_id, status, session_id, created_at, updated_at)
         VALUES ('run', 'job', 'running', 'session', 1, 1);
         INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
         VALUES ('session', 'job', 'claude', 'open', 1, 1, 1);
         INSERT INTO events (id, run_id, session_id, sequence, timestamp, event_type, data, created_at)
         VALUES ('storeless-team-event', 'run', 'session', 1, 1, 'assistant',
                 '{\"eventType\":\"assistant\",\"content\":\"team body\",\"isError\":false}', 1);"
    ).await.unwrap();

    // Even a forced pack read cannot use a host-local catalog on a team schema.
    for source in [None, Some(HistorySource::Pack)] {
        db.set_history_source_override(source);
        let mut reader = HistoryReader::new(&db);
        let before = read_cost(&db);
        assert_eq!(reader.source_of_run("run").await.unwrap(), Source::Hot);
        assert_eq!(
            reader.source_of_session("session").await.unwrap(),
            Source::Hot
        );
        assert_eq!(
            reader.source_of_turn("missing-turn").await.unwrap(),
            Source::Hot
        );
        assert_read_cost(&db, before, (0, 0, 0));
        let run = reader
            .run_events("run", EventFilter::default())
            .await
            .unwrap();
        assert_eq!(run.provenance, Provenance::Hot);
        assert_eq!(run.rows.len(), 1);
        assert_eq!(run.rows[0].event.id, "storeless-team-event");
        let session = reader
            .session_events("session", None, EventFilter::default())
            .await
            .unwrap();
        assert_eq!(fingerprint(&run.rows), fingerprint(&session.rows));
        assert_eq!(reader.session_event_keys("session").await.unwrap().len(), 1);
        assert!(reader
            .turn_events("missing-turn")
            .await
            .unwrap()
            .rows
            .is_empty());
        assert!(!db.has_history_catalog());
        assert!(!db.has_evicted_history());
        assert_read_cost(&db, before, (4, 4, 0));
    }
    let path = db.path().to_path_buf();
    drop(db);
    let db = LocalDb::open(path).await.unwrap();
    assert!(!db.has_history_catalog());
    assert!(!db.has_evicted_history());
    let before = read_cost(&db);
    assert!(HistoryReader::new(&db).is_hot_only());
    assert_read_cost(&db, before, (0, 0, 0));
    let _quarantine = Quarantined::new(
        "storeless-team-quarantine",
        TRANSCRIPT_EVENT_SINK,
        "storeless-team-event",
    );
    let rows = HistoryReader::new(&db)
        .run_events("run", EventFilter::default())
        .await
        .unwrap()
        .rows;
    assert_eq!(rows[0].reconstruction, Reconstruction::Withheld);
    assert!(!rows[0].event.data.contains("team body"));
}

#[tokio::test]
async fn an_empty_id_selection_does_not_resolve_a_source() {
    let db = crate::storage::migrated_test_db("seam-empty-ids").await;
    let before = (db.read_statement_count(), db.read_checkout_count());
    let mut reader = HistoryReader::new(&db);
    assert!(reader
        .run_events_by_id("missing", &[])
        .await
        .unwrap()
        .is_empty());
    assert!(reader.runs.is_empty());
    assert_eq!(db.read_statement_count(), before.0);
    assert_eq!(db.read_checkout_count(), before.1);
}

fn read_cost(db: &LocalDb) -> (usize, usize, usize) {
    (
        db.read_statement_count(),
        db.read_checkout_count(),
        db.read_transaction_count(),
    )
}

fn assert_read_cost(db: &LocalDb, before: (usize, usize, usize), expected: (usize, usize, usize)) {
    let after = read_cost(db);
    assert_eq!(
        (after.0 - before.0, after.1 - before.1, after.2 - before.2),
        expected
    );
}

#[tokio::test]
async fn source_selection_costs_zero_without_eviction_even_with_catalog_rows() {
    let db = crate::storage::migrated_test_db("seam-zero-source-cost").await;
    seed_fixture_execution(&db, "seam-zero-source-cost").await;
    for published in [false, true] {
        if published {
            publish_for_test(&db, "exec-1").await;
        }
        assert!(!db.has_evicted_history());
        let before = read_cost(&db);
        let mut reader = HistoryReader::new(&db);
        assert!(reader.is_hot_only());
        reader
            .prime_sessions_and_runs(
                &["sess-1".into(), "sess-2".into()],
                &["run-1".into(), "run-2".into()],
            )
            .await
            .unwrap();
        for _ in 0..3 {
            assert_eq!(reader.source_of_run("run-1").await.unwrap(), Source::Hot);
            assert_eq!(reader.source_of_turn("turn-1").await.unwrap(), Source::Hot);
            assert_eq!(
                reader.source_of_session("sess-1").await.unwrap(),
                Source::Hot
            );
        }
        // A second reader must not even re-probe the schema.
        assert_eq!(
            HistoryReader::new(&db)
                .source_of_run("run-2")
                .await
                .unwrap(),
            Source::Hot
        );
        assert_read_cost(&db, before, (0, 0, 0));
        let before = read_cost(&db);
        let page = HistoryReader::new(&db)
            .run_events_page("run-2", i64::MIN, 512, EventFilter::default())
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 2);
        assert_read_cost(&db, before, (1, 1, 0));
        let before = read_cost(&db);
        let event = HistoryReader::new(&db)
            .run_event_by_sequence("run-2", 1)
            .await
            .unwrap()
            .unwrap();
        assert!(event.event.data.contains("second run"));
        assert_read_cost(&db, before, (1, 1, 0));
    }
}

#[tokio::test]
async fn request_priming_batches_and_caches_hot_and_pack_sources() {
    let (db, _) = packed_db("seam-source-prime-cost").await;
    let sessions: Vec<String> = ["sess-1", "sess-2", "missing-session"]
        .map(str::to_string)
        .into();
    let runs: Vec<String> = ["run-1", "run-2", "missing-run"].map(str::to_string).into();
    // Also exercise negative results when some unrelated history has evicted.
    for evicted in [false, true] {
        db.mark_history_evicted();
        if evicted {
            mark_evicted(&db).await;
        }
        let before = read_cost(&db);
        let mut reader = HistoryReader::new(&db);
        reader
            .prime_sessions_and_runs(&sessions, &runs)
            .await
            .unwrap();
        assert_read_cost(&db, before, (1, 1, 0));
        let before = read_cost(&db);
        for _ in 0..3 {
            reader
                .prime_sessions_and_runs(&sessions, &runs)
                .await
                .unwrap();
            reader.prime_runs(&runs).await.unwrap();
            for run in &runs {
                reader.source_of_run(run).await.unwrap();
            }
            for session in &sessions {
                reader.source_of_session(session).await.unwrap();
            }
        }
        assert_read_cost(&db, before, (0, 0, 0));
        assert_eq!(
            reader.source_of_run("missing-run").await.unwrap(),
            Source::Hot
        );
        assert_eq!(
            reader.source_of_session("missing-session").await.unwrap(),
            Source::Hot
        );
        assert_eq!(
            matches!(
                reader.source_of_run("run-1").await.unwrap(),
                Source::Pack { .. }
            ),
            evicted
        );

        let before = read_cost(&db);
        for _ in 0..3 {
            reader.source_of_turn("turn-1").await.unwrap();
        }
        assert_read_cost(&db, before, (1, 1, 0));
        let before = read_cost(&db);
        for after in [i64::MIN, 1, 3] {
            reader
                .session_events_page("sess-2", None, after, 1, EventFilter::default())
                .await
                .unwrap();
        }
        assert_read_cost(&db, before, if evicted { (0, 0, 0) } else { (3, 3, 0) });
    }
}

#[tokio::test]
async fn capability_is_loaded_at_open_and_migration_not_first_read() {
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("history-open.db");
    let db = LocalDb::open(&path).await.unwrap();
    assert!(!db.has_history_catalog());
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    assert!(db.has_history_catalog());
    assert!(!db.has_evicted_history());
    seed_fixture_execution(&db, "seam-reopen").await;
    publish_for_test(&db, "exec-1").await;
    drop(db);

    let db = LocalDb::open(&path).await.unwrap();
    assert!(db.has_history_catalog());
    assert!(!db.has_evicted_history());
    let before = read_cost(&db);
    db.initialize_history_capability().await.unwrap();
    assert_eq!(
        HistoryReader::new(&db)
            .source_of_session("sess-1")
            .await
            .unwrap(),
        Source::Hot
    );
    assert_read_cost(&db, before, (0, 0, 0));
    mark_evicted(&db).await;
    drop(db);

    let db = LocalDb::open(&path).await.unwrap();
    assert!(
        db.has_evicted_history(),
        "durable eviction must be loaded before the first read"
    );
    assert!(db.content_store().is_none());
    let before = read_cost(&db);
    db.initialize_history_capability().await.unwrap();
    assert_read_cost(&db, before, (0, 0, 0));
    let error = HistoryReader::new(&db)
        .run_events("run-1", EventFilter::default())
        .await
        .unwrap_err();
    assert!(matches!(error, HistoryError::NoStore { .. }), "{error}");
}

#[tokio::test]
async fn forced_pack_bypasses_the_unevicted_fast_path() {
    let (db, _) = packed_db("seam-force-cost").await;
    assert!(!db.has_evicted_history());
    db.set_history_source_override(Some(HistorySource::Pack));
    let mut reader = HistoryReader::new(&db);
    assert!(!reader.is_hot_only());
    let before = read_cost(&db);
    reader
        .prime_sessions_and_runs(&["sess-1".into()], &["run-1".into()])
        .await
        .unwrap();
    assert_read_cost(&db, before, (1, 1, 0));
    let before = read_cost(&db);
    assert_pack(
        &reader
            .session_events_page("sess-1", None, i64::MIN, 2, EventFilter::default())
            .await
            .unwrap(),
    );
    assert_pack(
        &reader
            .run_events_page("run-1", i64::MIN, 2, EventFilter::default())
            .await
            .unwrap(),
    );
    assert_read_cost(&db, before, (0, 0, 0));
    db.set_history_source_override(None);
}

#[tokio::test]
async fn selected_hot_projection_offset_and_connection_gather_share_reconstruction() {
    let (db, events) = packed_db("seam-selected-hot").await;
    let _quarantine = Quarantined::new(
        "seam-selected-hot-gate",
        TRANSCRIPT_EVENT_SINK,
        &events.restored,
    );
    let baseline = read_cost(&db);
    let expected = HistoryReader::new(&db)
        .run_events("run-1", EventFilter::default())
        .await
        .unwrap();
    let after_expected = read_cost(&db);
    let reconstruction_cost = (
        after_expected.0 - baseline.0 - 1,
        after_expected.1 - baseline.1 - 1,
        after_expected.2 - baseline.2,
    );
    let rows = db
        .query_all(
            format!(
                "SELECT 42, 'prefix', {} FROM events e WHERE e.run_id = ?1 ORDER BY e.rowid",
                HotRow::columns(Some("e"))
            ),
            ("run-1",),
            |row| HotRow::from_row_at(row, 2),
        )
        .await
        .unwrap();
    let before = read_cost(&db);
    let rows = HistoryReader::new(&db)
        .hydrate_selected_hot_rows(rows, EventFilter::default())
        .await
        .unwrap();
    // Archived fixture rows can require reconstruction reads, but not source reads.
    assert_eq!(rows.len(), expected.rows.len());
    let mut rows = rows;
    sort_chat_order(&mut rows);
    assert_eq!(fingerprint(&rows), fingerprint(&expected.rows));
    assert_read_cost(&db, before, reconstruction_cost);

    let sql = statements::run_page(&EventFilter::default());
    let before = read_cost(&db);
    let raw = db
        .read(move |conn| {
            Box::pin(async move {
                gather_hot_page(
                    conn,
                    &sql,
                    vec![
                        Value::Text("run-2".into()),
                        Value::Integer(i64::MIN),
                        Value::Integer(512),
                    ],
                )
                .await
            })
        })
        .await
        .unwrap();
    assert_read_cost(&db, before, (1, 1, 1));
    let before = read_cost(&db);
    let rows = raw.reconstruct(&db, EventFilter::default()).await.rows;
    assert_eq!(rows.len(), 2);
    assert_read_cost(&db, before, (0, 0, 0));
}

#[tokio::test]
async fn chat_pages_match_sources_and_preserve_chronological_ties() {
    let (db, _) = packed_db("seam-chat-pages").await;
    // Row insertion order need not agree with created_at.
    db.execute(
        "UPDATE events SET created_at = 9 WHERE sequence = 6 AND run_id = 'run-1'",
        (),
    )
    .await
    .unwrap();
    publish_for_test(&db, "exec-1").await;
    let (hot, pack) = read_both(&db, || async {
        let mut reader = HistoryReader::new(&db);
        let mut after = None;
        let mut all = Vec::new();
        loop {
            let page = reader
                .run_events_chat_page("run-1", after, 2, EventFilter::default())
                .await
                .unwrap();
            after = page
                .rows
                .last()
                .map(|row| (row.event.created_at, row.rowid));
            let exhausted = page.fetched < 2;
            all.extend(page.rows);
            if exhausted {
                break;
            }
        }
        all
    })
    .await;
    assert_eq!(fingerprint(&hot), fingerprint(&pack));
    assert!(hot
        .windows(2)
        .all(|rows| (rows[0].event.created_at, rows[0].rowid)
            < (rows[1].event.created_at, rows[1].rowid)));
}

#[tokio::test]
async fn statement_counters_cover_raw_helpers_and_tracked_calls_once() {
    let db = crate::storage::migrated_test_db("seam-statement-counters").await;
    let before = (db.statement_count(), read_cost(&db));
    db.query_all("SELECT 1", (), |row| row.i64(0))
        .await
        .unwrap();
    db.query_opt("SELECT 2", (), |row| row.i64(0))
        .await
        .unwrap();
    db.query_one("SELECT 3", (), |row| row.i64(0))
        .await
        .unwrap();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query("SELECT 4", ()).await?;
            rows.next().await?;
            drop(rows);
            let _statement = conn.prepare("SELECT 5").await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    assert_eq!(db.statement_count() - before.0, 5);
    assert_read_cost(&db, before.1, (5, 4, 1));
    let before = (db.statement_count(), db.read_statement_count());
    db.execute("UPDATE jobs SET updated_at = updated_at WHERE 0", ())
        .await
        .unwrap();
    db.write(|conn| {
        Box::pin(async move {
            conn.execute("UPDATE jobs SET updated_at = updated_at WHERE 0", ())
                .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    assert_eq!(db.statement_count() - before.0, 2);
    assert_eq!(db.read_statement_count(), before.1);
}

#[tokio::test]
async fn full_hot_scopes_use_one_cursor_across_reconstruction_batches() {
    let (db, _) = multi_frame_session_db("seam-full-hot-cost").await;
    db.execute(
        "UPDATE events SET turn_id = 'turn-1' WHERE run_id = 'run-1'",
        (),
    )
    .await
    .unwrap();
    for unrelated_eviction in [false, true] {
        if unrelated_eviction {
            db.mark_history_evicted();
        }
        let mut reader = HistoryReader::new(&db);
        let before = read_cost(&db);
        reader
            .prime_sessions_and_runs(&["sess-1".into()], &["run-1".into()])
            .await
            .unwrap();
        assert_read_cost(
            &db,
            before,
            if unrelated_eviction {
                (1, 1, 0)
            } else {
                (0, 0, 0)
            },
        );
        reader.source_of_turn("turn-1").await.unwrap();

        let before = read_cost(&db);
        let run = reader
            .run_events("run-1", EventFilter::default())
            .await
            .unwrap();
        assert!(run.rows.len() > RECONSTRUCT_BATCH_ROWS);
        assert_read_cost(&db, before, (1, 1, 0));

        let before = read_cost(&db);
        let session = reader
            .session_events_after("sess-1", None, i64::MIN, EventFilter::default())
            .await
            .unwrap();
        assert!(session.rows.len() > RECONSTRUCT_BATCH_ROWS);
        assert_read_cost(&db, before, (1, 1, 0));

        let before = read_cost(&db);
        let turn = reader.turn_events("turn-1").await.unwrap();
        assert_eq!(turn.rows.len(), run.rows.len());
        assert_read_cost(&db, before, (1, 1, 0));
    }
}

#[tokio::test]
async fn chronological_cursor_advances_past_a_fully_malformed_page() {
    let db = crate::storage::migrated_test_db("seam-chat-malformed-cursor").await;
    let events = seed_fixture_execution(&db, "seam-chat-malformed-cursor").await;
    // SQLite's integer affinity permits this historic malformed row. The
    // chronological key remains valid while decoding the Event fails.
    db.execute(
        "UPDATE events SET sequence = 'malformed' WHERE id = ?1",
        (events.second_run,),
    )
    .await
    .unwrap();
    let mut reader = HistoryReader::new(&db);
    let before = read_cost(&db);
    let first = reader
        .run_events_chat_page("run-2", None, 1, EventFilter::default())
        .await
        .unwrap();
    assert_eq!(first.fetched, 1);
    assert!(first.rows.is_empty());
    assert!(first.last_chat_key.is_some());
    let second = reader
        .run_events_chat_page("run-2", first.last_chat_key, 1, EventFilter::default())
        .await
        .unwrap();
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].event.id, events.second_session);
    assert!(second.last_chat_key > first.last_chat_key);
    assert_read_cost(&db, before, (2, 2, 0));
}

#[tokio::test]
async fn matching_selector_stops_hot_early_and_scans_each_pack_row_once() {
    use std::cell::Cell;
    let (db, _) = multi_frame_session_db("seam-find-event").await;
    let expected = HistoryReader::new(&db)
        .run_events("run-1", EventFilter::default())
        .await
        .unwrap();
    let first = expected.rows.first().unwrap();
    let id = first.event.id.clone();
    let before = read_cost(&db);
    let found = HistoryReader::new(&db)
        .find_run_event("run-1", EventFilter::default(), |row| row.event.id == id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fingerprint(&[found]),
        fingerprint(std::slice::from_ref(first))
    );
    assert_read_cost(&db, before, (1, 1, 0));

    db.set_history_source_override(Some(HistorySource::Pack));
    let mut reader = HistoryReader::new(&db);
    let calls = Cell::new(0);
    assert!(reader
        .find_run_event("run-1", EventFilter::default(), |_| {
            calls.set(calls.get() + 1);
            false
        })
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        calls.get(),
        expected.rows.len(),
        "a no-match archive lookup visits each row once"
    );
    let found = reader
        .find_run_event("run-1", EventFilter::default(), |_| true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        fingerprint(&[found]),
        fingerprint(std::slice::from_ref(first))
    );
    db.set_history_source_override(None);
}

#[tokio::test]
async fn matching_selector_applies_quarantine_before_the_predicate() {
    let (db, events) = packed_db("seam-find-gate").await;
    let _quarantine = Quarantined::new("seam-find-gate", TRANSCRIPT_EVENT_SINK, &events.restored);
    let (hot, pack) = read_both(&db, || async {
        HistoryReader::new(&db)
            .find_run_event("run-1", EventFilter::default(), |row| {
                row.event.data.contains("the restored body")
            })
            .await
            .unwrap()
    })
    .await;
    assert!(hot.is_none());
    assert!(pack.is_none());
}

#[tokio::test]
async fn history_capability_initialization_fails_closed_on_a_broken_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("broken-catalog.db");
    let db = LocalDb::open(&path).await.unwrap();
    db.execute_batch("CREATE TABLE history_tier(execution_id TEXT PRIMARY KEY)")
        .await
        .unwrap();
    drop(db);
    let error = LocalDb::open(&path).await.unwrap_err();
    assert!(error.to_string().contains("evicted_at"), "{error}");
}

#[tokio::test]
async fn signed_run_slices_match_pack_and_use_one_hot_cursor() {
    let (db, _) = packed_db("seam-signed-single-cursor").await;
    for limit in [-2, -1, 0, 1, 3, 512, i64::MAX] {
        for offset in [-3, 0, 1, 2, 7, i64::MAX] {
            let (hot, pack) = read_both(&db, || async {
                HistoryReader::new(&db)
                    .run_events_slice("run-2", limit, offset)
                    .await
                    .unwrap()
            })
            .await;
            assert_eq!(
                fingerprint(&hot.rows),
                fingerprint(&pack.rows),
                "limit={limit}, offset={offset}"
            );
            let before = read_cost(&db);
            let actual = HistoryReader::new(&db)
                .run_events_slice("run-2", limit, offset)
                .await
                .unwrap();
            assert_eq!(fingerprint(&actual.rows), fingerprint(&hot.rows));
            assert_read_cost(&db, before, (1, 1, 0));
        }
    }
}

#[tokio::test]
async fn cached_prefix_is_selected_before_any_body_fetch_or_reconstruction() {
    let (db, _) = packed_db("seam-prefix-single-cursor").await;
    let sessions = vec!["sess-1".to_string(), "sess-2".to_string()];
    let mut expected = Vec::new();
    for run in ["run-1", "run-2"] {
        let mut rows = HistoryReader::new(&db)
            .run_events(run, EventFilter::default())
            .await
            .unwrap()
            .rows;
        rows.sort_by_key(|row| (row.event.sequence, row.rowid));
        expected.extend(rows);
    }
    for offset in [-1, 0, 1, 6, 8, i64::MAX] {
        let before = read_cost(&db);
        let page = HistoryReader::new(&db)
            .hot_sessions_after_count(&sessions, offset)
            .await
            .unwrap()
            .unwrap();
        let skip = usize::try_from(offset.max(0))
            .unwrap_or(usize::MAX)
            .min(expected.len());
        assert_eq!(
            fingerprint(&page.rows),
            fingerprint(&expected[skip..]),
            "offset={offset}"
        );
        if offset >= 6 {
            // The prefix contains a gitcoord body that would need metadata
            // queries. Its selection metadata must not fetch or reconstruct it.
            assert_read_cost(&db, before, (1, 1, 0));
        }
    }
    db.set_history_source_override(Some(HistorySource::Pack));
    let before = read_cost(&db);
    assert!(HistoryReader::new(&db)
        .hot_sessions_after_count(&sessions, 6)
        .await
        .unwrap()
        .is_none());
    assert_read_cost(&db, before, (0, 0, 0));
    db.set_history_source_override(None);
    db.mark_history_evicted();
    let before = read_cost(&db);
    assert!(HistoryReader::new(&db)
        .hot_sessions_after_count(&sessions, 6)
        .await
        .unwrap()
        .is_none());
    assert_read_cost(&db, before, (0, 0, 0));
}

#[tokio::test]
async fn cached_prefix_and_chronological_statements_keep_keyed_access() {
    let db = crate::storage::migrated_test_db("seam-prefix-plans").await;
    let chat = query_plan(
        &db,
        &statements::run_chat_page(&EventFilter::types(&["assistant"])),
        vec![
            Value::Text("run".into()),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MIN),
            Value::Integer(512),
            Value::Text("assistant".into()),
        ],
    )
    .await;
    assert!(
        chat.contains("SEARCH")
            && chat.contains("idx_events_run_id")
            && !chat.contains("SCAN")
            && !chat.contains("idx_events_type_run"),
        "{chat}"
    );
    for count in [1, 256, 512] {
        let sql = statements::sessions_after_count(count);
        let mut binds: Vec<_> = (0..count)
            .map(|i| Value::Text(format!("session-{i}")))
            .collect();
        binds.push(Value::Integer(1030));
        let plan = query_plan(&db, &sql, binds).await;
        assert!(
            plan.contains("idx_events_session_id")
                && plan.contains("SEARCH")
                && plan.contains("rowid")
                && !plan.contains("SCAN e"),
            "{plan}"
        );
        let (before_join, after_join) = sql.split_once(") pick").unwrap();
        let selected_keys = before_join.split_once("FROM (").unwrap().1;
        assert!(
            !selected_keys.contains(".data"),
            "sort keys must not carry bodies: {sql}"
        );
        assert!(
            !after_join.contains("ORDER BY"),
            "outer body fetch must not enter a sorter: {sql}"
        );
    }
}
