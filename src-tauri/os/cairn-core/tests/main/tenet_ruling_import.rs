//! The boot-time import of thread rulings into migrated tenets, against real
//! pre-tenet thread stores (thread schema 0001) and real arc artifacts.
//!
//! Project `imp` has five threads covering every source combination:
//!
//! - A: a store with three rulings, one of each status, all slug-distinct,
//!   one sharing its slug with B;
//! - B: a store with one accepted ruling under the shared slug;
//! - C: no store at all, two rulings embedded in its arc;
//! - D: a store carrying the pre-cutover arc-import marker and an arc with a
//!   ruling, which was already copied into the table, so the arc is skipped;
//! - E: a store already at 0002 (table gone) with no ledger row and an arc
//!   ruling, so the arc is the only record and is imported.

use crate::common;
use crate::common::orchestrator;

use cairn_common::identity::PrincipalRef;
use cairn_core::internal::db::DbState;
use cairn_core::internal::services::EventEmitter;
use cairn_core::internal::storage::{LocalDb, RowExt, SearchIndex};
use cairn_core::tenets::{AnswerSource, TenetOrigin, TenetStatus};
use cairn_core::threads::db_store;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

const NOW: i64 = 1_800_000_000;

#[derive(Default)]
struct Recorder(Mutex<Vec<Value>>);

impl EventEmitter for Recorder {
    fn emit(&self, _event: &str, payload: Value) -> Result<(), String> {
        self.0.lock().unwrap().push(payload);
        Ok(())
    }

    fn emit_empty(&self, _event: &str) -> Result<(), String> {
        Ok(())
    }
}

struct Fixture {
    temp: tempfile::TempDir,
    db: Arc<LocalDb>,
    state: DbState,
}

fn thread(letter: &str) -> String {
    format!("imp-{letter}")
}

fn store_path(temp: &tempfile::TempDir, letter: &str) -> std::path::PathBuf {
    temp.path()
        .join("threads")
        .join(format!("{}.db", thread(letter)))
}

async fn ruling_row(store: &LocalDb, slug: &str, text: &str, status: &str, at: i64) {
    store
        .execute_script(format!(
            "INSERT INTO _cairn_rulings(slug,text,status,rationale,provenance,created_at,updated_at)
             VALUES ('{slug}','{text}','{status}','Because {slug}','[\"cairn://p/imp/{at}\"]',{at},{})",
            at + 1
        ))
        .await
        .unwrap();
}

async fn arc(db: &LocalDb, letter: &str, at: i64, rulings: Value) {
    let thread_id = thread(letter);
    db.execute_script(format!(
        "INSERT INTO jobs (id, thread_id, project_id, status, node_name, uri_segment, created_at, updated_at)
         VALUES ('j-{thread_id}', '{thread_id}', 'p-imp', 'idle', 'thread', 'thread', 1, 1)"
    ))
    .await
    .unwrap();
    db.execute(
        "INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, created_at, updated_at, confirmed)
         VALUES (?1, ?2, 'arc', ?3, 1, 'arc', ?4, ?4, 1)",
        (
            format!("a-{thread_id}"),
            format!("j-{thread_id}"),
            json!({"current_intent": "x", "rulings": rulings}).to_string(),
            at,
        ),
    )
    .await
    .unwrap();
}

async fn fixture() -> Fixture {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    db.execute_script(
        "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1);
         INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p-imp','w-1','Imp','imp','/tmp/imp',1,1);",
    )
    .await
    .unwrap();
    for letter in ["a", "b", "c", "d", "e"] {
        let id = thread(letter);
        db.execute_script(format!(
            "INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
             VALUES ('{id}','p-imp','thread-{letter}','active',1,1)"
        ))
        .await
        .unwrap();
        db_store::use_test_path(&id, store_path(&temp, letter));
    }

    let a = db_store::stage_store_for_test(&store_path(&temp, "a"), "0001").await;
    ruling_row(&a, "shared-slug", "Leases decide placement", "accepted", 10).await;
    ruling_row(&a, "no-reassign", "Reassign on expiry", "rejected", 20).await;
    ruling_row(&a, "old-timeouts", "Fixed timeouts", "superseded", 30).await;
    a.execute_script("INSERT INTO _cairn_open_questions(question) VALUES ('Still open?')")
        .await
        .unwrap();
    drop(a);

    let b = db_store::stage_store_for_test(&store_path(&temp, "b"), "0001").await;
    ruling_row(&b, "shared-slug", "Packs resolve by name", "accepted", 15).await;
    drop(b);

    arc(
        &db,
        "c",
        25,
        json!([
            {"slug": "c-first", "text": "C first", "status": "accepted",
             "rationale": "why c", "provenance": "cairn://p/imp/9"},
            {"text": "C second has no slug"}
        ]),
    )
    .await;

    let d = db_store::stage_store_for_test(&store_path(&temp, "d"), "0001").await;
    d.execute_script("INSERT INTO _cairn_arc_imports(id) VALUES (1)")
        .await
        .unwrap();
    drop(d);
    arc(
        &db,
        "d",
        26,
        json!([{"slug": "d-arc", "text": "Already copied", "status": "accepted"}]),
    )
    .await;

    let e = db_store::stage_store_for_test(&store_path(&temp, "e"), "0002").await;
    e.execute_script("INSERT INTO _cairn_arc_imports(id) VALUES (1)")
        .await
        .unwrap();
    drop(e);
    arc(
        &db,
        "e",
        40,
        json!([{"slug": "e-arc", "text": "Only in the arc", "status": "accepted"}]),
    )
    .await;

    let search = Arc::new(SearchIndex::open_or_create(temp.path().join("import-search")).unwrap());
    let state = DbState::new(db.clone(), search);
    Fixture { temp, db, state }
}

struct Imported {
    number: i64,
    thread: String,
    slug: String,
    question: String,
    options: Vec<String>,
    status: String,
    verdict: Option<String>,
    selected: Option<i64>,
    answer_source: Option<String>,
    answered_at: Option<i64>,
    answered_by: Option<String>,
    origin: String,
    legacy: Value,
    proposer: PrincipalRef,
    created_at: i64,
    updated_at: i64,
}

async fn imported(db: &LocalDb) -> Vec<Imported> {
    db.query_all(
        "SELECT number, proposing_thread_id, slug, question, options_json, status, verdict,
                selected_option, answer_source, answered_at, answered_by_json, origin,
                legacy_json, proposer_principal_json, created_at, updated_at
         FROM tenets WHERE project_id = 'p-imp' ORDER BY number",
        (),
        |row| {
            Ok(Imported {
                number: row.i64(0)?,
                thread: row.text(1)?,
                slug: row.text(2)?,
                question: row.text(3)?,
                options: serde_json::from_str(&row.text(4)?).unwrap(),
                status: row.text(5)?,
                verdict: row.opt_text(6)?,
                selected: row.opt_i64(7)?,
                answer_source: row.opt_text(8)?,
                answered_at: row.opt_i64(9)?,
                answered_by: row.opt_text(10)?,
                origin: row.text(11)?,
                legacy: serde_json::from_str(&row.text(12)?).unwrap(),
                proposer: serde_json::from_str(&row.text(13)?).unwrap(),
                created_at: row.i64(14)?,
                updated_at: row.i64(15)?,
            })
        },
    )
    .await
    .unwrap()
}

async fn count(db: &LocalDb, sql: &str) -> i64 {
    db.query_one(sql.to_string(), (), |row| row.i64(0))
        .await
        .unwrap()
}

#[tokio::test]
async fn rulings_from_both_sources_become_numbered_migrated_tenets_exactly_once() {
    let fx = fixture().await;
    let emitter = Recorder::default();

    let report = cairn_core::tenets::import_thread_rulings(&fx.state, &emitter, NOW).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.inserted(), 7);
    assert_eq!(report.projects.len(), 1);
    assert_eq!(report.projects[0].threads, 5);
    assert_eq!(report.projects[0].slug_collisions, 1);
    assert_eq!(
        *emitter.0.lock().unwrap(),
        [json!({"table": "tenets", "action": "insert"})]
    );

    let rows = imported(&fx.db).await;
    // Contiguous numbers in the order the rulings were made; arc rows take
    // their artifact's time and sort by array index within it.
    let order: Vec<(i64, &str, &str)> = rows
        .iter()
        .map(|row| (row.number, row.thread.as_str(), row.slug.as_str()))
        .collect();
    assert_eq!(
        order,
        [
            (1, "imp-a", "shared-slug"),
            (2, "imp-b", "shared-slug-2"),
            (3, "imp-a", "no-reassign"),
            (4, "imp-c", "c-first"),
            (5, "imp-c", "c-second-has-no-slug"),
            (6, "imp-a", "old-timeouts"),
            (7, "imp-e", "e-arc"),
        ]
    );

    let accepted = &rows[0];
    assert_eq!(accepted.question, "Leases decide placement");
    assert_eq!(accepted.options, ["Leases decide placement"]);
    assert_eq!(accepted.status, "approved");
    assert_eq!(accepted.verdict.as_deref(), Some("option"));
    assert_eq!(accepted.selected, Some(0));
    assert_eq!(accepted.answer_source.as_deref(), Some("migrated"));
    assert_eq!(accepted.answered_by, None);
    assert_eq!(accepted.answered_at, Some(11));
    assert_eq!(accepted.origin, TenetOrigin::MigratedRuling.as_str());
    assert_eq!((accepted.created_at, accepted.updated_at), (10, 11));
    assert_eq!(
        accepted.legacy,
        json!({
            "rationale": "Because shared-slug",
            "provenance": ["cairn://p/imp/10"],
            "legacy_key": "imp-a:shared-slug",
            "original_slug": "shared-slug",
        })
    );
    assert_eq!(
        accepted.proposer,
        PrincipalRef::Agent {
            node: "cairn://p/imp/thread-a".into(),
            run_id: None
        }
    );

    let rejected = &rows[2];
    assert_eq!(rejected.status, TenetStatus::Declined.as_str());
    assert_eq!(rejected.verdict.as_deref(), Some("none"));
    assert_eq!(rejected.selected, None);

    let superseded = &rows[5];
    assert_eq!(superseded.status, TenetStatus::Superseded.as_str());
    assert_eq!(superseded.verdict.as_deref(), Some("option"));
    assert_eq!(superseded.selected, Some(0));
    assert_eq!(
        superseded.answer_source.as_deref(),
        Some(AnswerSource::Migrated.as_str())
    );

    // The renamed slug keeps its thread-local key; arc defaults fill in.
    assert_eq!(rows[1].legacy["legacy_key"], "imp-b:shared-slug");
    assert_eq!(rows[1].legacy["original_slug"], "shared-slug");
    assert_eq!(rows[3].legacy["provenance"], json!(["cairn://p/imp/9"]));
    assert_eq!(rows[4].legacy["rationale"], "No rationale recorded");
    assert_eq!((rows[4].created_at, rows[4].updated_at), (25, 25));

    // The import clock moves the project forward; it never moves it back.
    assert_eq!(
        count(&fx.db, "SELECT updated_at FROM projects WHERE id = 'p-imp'").await,
        NOW
    );
    assert_eq!(
        count(
            &fx.db,
            "SELECT next_tenet_number FROM projects WHERE id = 'p-imp'"
        )
        .await,
        8
    );
    assert_eq!(
        count(
            &fx.db,
            "SELECT COUNT(*) FROM tenet_ruling_imports WHERE project_id = 'p-imp'"
        )
        .await,
        5
    );

    // A second boot finds every ruling already present.
    let again = cairn_core::tenets::import_thread_rulings(&fx.state, &emitter, NOW + 1).await;
    assert!(again.errors.is_empty(), "{:?}", again.errors);
    assert_eq!(again.inserted(), 0);
    assert_eq!(imported(&fx.db).await.len(), 7);
    assert_eq!(emitter.0.lock().unwrap().len(), 1);

    // A's store is now imported, so its first open applies 0002 and drops the
    // rulings table while keeping its open questions.
    let a = db_store::open(&thread("a")).await.unwrap();
    assert_eq!(
        count(
            &a,
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = '_cairn_rulings'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &a,
            "SELECT COUNT(*) FROM cairn_schema_migrations WHERE version = '0002'"
        )
        .await,
        1
    );
    assert_eq!(
        count(&a, "SELECT COUNT(*) FROM _cairn_open_questions").await,
        1
    );

    // A third boot, with A's table gone and its ledger row present, takes the
    // fast path and still inserts nothing.
    let third = cairn_core::tenets::import_thread_rulings(&fx.state, &emitter, NOW + 2).await;
    assert_eq!(third.inserted(), 0);

    // The old slug still addresses the renamed tenet.
    let orch = orchestrator(&fx.temp, fx.db.clone());
    let entity = common::read_resource(&orch, "cairn://p/imp/tenets/shared-slug-2").await;
    assert!(entity.contains("# Tenet #2"), "{entity}");
    assert!(entity.contains("Packs resolve by name"), "{entity}");
    assert!(entity.contains("migrated ruling"), "{entity}");
}

/// A failed boot import leaves no trace, and the thread keeps running. Its arc
/// read copies the open questions (writing an arc-import marker) and its next
/// prose patch stores an arc without the `rulings` key. Neither may hide the
/// arc-only rulings from the next boot's retry.
#[tokio::test]
async fn arc_reads_and_writes_after_a_failed_import_do_not_hide_arc_rulings_from_the_retry() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    db.execute_script(
        "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1','W',1,1);
         INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('p-retry','w-1','Retry','retry','/tmp/retry',1,1);
         INSERT INTO threads (id, project_id, name, status, created_at, updated_at)
           VALUES ('retry-f','p-retry','thread-f','active',1,1);
         INSERT INTO jobs (id, thread_id, project_id, status, agent_config_id, node_name, uri_segment, created_at, updated_at)
           VALUES ('j-retry-f','retry-f','p-retry','idle','thread','Thread','thread',1,1);",
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, created_at, updated_at, confirmed)
         VALUES ('a-retry-f', 'j-retry-f', 'arc', ?1, 1, 'arc', 20, 20, 1)",
        (json!({
            "current_intent": "Carry on.",
            "rulings": [{"slug": "arc-only", "text": "Only in the arc", "status": "accepted"}],
            "open_questions": [{"question": "Still open?"}]
        })
        .to_string(),),
    )
    .await
    .unwrap();
    let path = temp.path().join("threads").join("retry-f.db");
    db_store::use_test_path("retry-f", path.clone());
    let staged = db_store::stage_store_for_test(&path, "0001").await;
    ruling_row(&staged, "table-only", "Only in the table", "accepted", 10).await;
    drop(staged);

    // No import has committed. The thread reads its arc, then patches its prose.
    let orch = orchestrator(&temp, db.clone());
    let read = common::read_resource(&orch, "cairn://p/retry/thread-f/arc").await;
    assert!(read.contains("Still open?"), "{read}");
    let patched = common::change_resource(
        &orch,
        json!([{"target": "cairn://p/retry/thread-f/arc", "mode": "patch",
                "payload": {"working": "moving on"}}]),
    )
    .await;
    assert!(patched.contains("version 2"), "{patched}");
    let latest = db
        .query_one(
            "SELECT data FROM artifacts WHERE job_id = 'j-retry-f' ORDER BY version DESC LIMIT 1",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert!(!latest.contains("rulings"), "{latest}");
    let store = db_store::open("retry-f").await.unwrap();
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM _cairn_rulings").await,
        1
    );
    assert_eq!(
        count(&store, "SELECT rulings_copied FROM _cairn_arc_imports").await,
        0,
        "a post-cutover marker never claims to have copied rulings"
    );

    // The retry imports the table ruling AND the arc-only one.
    let search = Arc::new(SearchIndex::open_or_create(temp.path().join("retry-search")).unwrap());
    let state = DbState::new(db.clone(), search);
    let report = cairn_core::tenets::import_thread_rulings(&state, &Recorder::default(), NOW).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let slugs = db
        .query_all(
            "SELECT slug FROM tenets WHERE project_id = 'p-retry' ORDER BY number",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(slugs, ["table-only", "arc-only"]);
}

/// A store whose rulings were never imported keeps them: the guard holds
/// thread migration 0002 rather than letting the open destroy rows nothing
/// has copied.
#[tokio::test]
async fn an_unimported_store_keeps_its_rulings_and_stays_at_0001() {
    let temp = tempfile::tempdir().unwrap();
    let id = "imp-unimported";
    let path = temp.path().join("unimported.db");
    db_store::use_test_path(id, path.clone());
    let staged = db_store::stage_store_for_test(&path, "0001").await;
    ruling_row(&staged, "kept", "Kept ruling", "accepted", 5).await;
    drop(staged);

    let store = db_store::open(id).await.unwrap();
    assert_eq!(
        count(
            &store,
            "SELECT COUNT(*) FROM cairn_schema_migrations WHERE version = '0002'"
        )
        .await,
        0
    );
    assert_eq!(
        count(&store, "SELECT COUNT(*) FROM _cairn_rulings").await,
        1
    );
}
