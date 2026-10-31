//! Thread database rows as resources: the row grammar end to end through the
//! read and write resources, on a thread's own store and on the application
//! database, with the `db-change` events every thread write emits.

use crate::common;
use cairn_core::internal::db::DbState;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::{CapturingEmitter, TestServicesBuilder};
use cairn_core::internal::services::EventEmitter;
use cairn_core::internal::storage::{LocalDb, SearchIndex};
use serde_json::{json, Value};
use std::sync::Arc;

const PROJECT: &str = "thread-rows";
const THREAD: &str = "row-owner";
const INTRUDER: &str = "row-intruder";

/// A run id no other test in this binary uses. A thread store's test path is
/// process-global and keyed by thread id, so fixtures that ran concurrently
/// under one id would read and write each other's stores.
fn unique_run(role: &str) -> String {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("thread-rows-{role}-{n}")
}

/// Hands the orchestrator an emitter while the test keeps a handle on what it
/// captured.
struct Shared(Arc<CapturingEmitter>);

impl EventEmitter for Shared {
    fn emit(&self, event: &str, payload: Value) -> Result<(), String> {
        self.0.emit(event, payload)
    }

    fn emit_empty(&self, event: &str) -> Result<(), String> {
        self.0.emit_empty(event)
    }
}

struct Fixture {
    _temp: tempfile::TempDir,
    _db: Arc<LocalDb>,
    orch: Orchestrator,
    events: Arc<CapturingEmitter>,
    run: String,
    intruder_run: String,
}

impl Fixture {
    fn db_changes(&self) -> Vec<Value> {
        self.events.events_named("db-change")
    }
}

async fn fixture() -> Fixture {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let project_id = common::insert_project_with_repo(&db, PROJECT, &repo).await;
    let run = unique_run("owner");
    let intruder_run = unique_run("intruder");
    for (thread, run) in [(THREAD, run.as_str()), (INTRUDER, intruder_run.as_str())] {
        common::seed_authenticated_thread_run(&db, &project_id, thread, run).await;
        cairn_core::threads::db_store::use_test_path(
            &format!("thread-{run}"),
            temp.path().join(format!("{run}.db")),
        );
    }
    let events = Arc::new(CapturingEmitter::new());
    let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let services = TestServicesBuilder::new()
        .with_emitter(Shared(events.clone()))
        .build();
    let orch = Orchestrator::builder(
        Arc::new(DbState::new(db.clone(), search)),
        Arc::new(services),
        temp.path().join("config"),
    )
    .build();
    Fixture {
        _temp: temp,
        _db: db,
        orch,
        events,
        run,
        intruder_run,
    }
}

fn root() -> String {
    format!("cairn://p/{PROJECT}/{THREAD}/db")
}

async fn write(fixture: &Fixture, target: &str, mode: &str, payload: Option<Value>) -> String {
    let mut change = json!({ "target": target, "mode": mode });
    if let Some(payload) = payload {
        change["payload"] = payload;
    }
    common::change_resource_as_run(&fixture.orch, json!([change]), Some(&fixture.run)).await
}

async fn sql(fixture: &Fixture, sql: &str) -> String {
    write(fixture, &root(), "patch", Some(json!({ "sql": sql }))).await
}

/// The one-line JSON document a `format=json` row read renders.
fn json_document(rendered: &str) -> Value {
    let line = rendered
        .lines()
        .find(|line| line.starts_with("{\""))
        .unwrap_or_else(|| panic!("no JSON document in: {rendered}"));
    serde_json::from_str(line).unwrap()
}

#[tokio::test]
async fn a_thread_creates_appends_reads_filters_patches_and_deletes_rows_without_sql() {
    let fixture = fixture().await;
    let attempts = format!("{}/attempts", root());

    let created = sql(
        &fixture,
        "CREATE TABLE attempts(id INTEGER PRIMARY KEY, problem TEXT NOT NULL, created_at INTEGER)",
    )
    .await;
    assert!(created.contains("Applied 1 statements"), "{created}");

    let inserted = write(
        &fixture,
        &attempts,
        "append",
        Some(json!({ "problem": "two-sum", "created_at": 10 })),
    )
    .await;
    let first = format!("{attempts}/1");
    assert!(
        inserted.contains(&format!("Inserted {first}")),
        "{inserted}"
    );

    let row = common::read_resource(&fixture.orch, &first).await;
    assert!(row.contains("| problem | two-sum |"), "{row}");
    assert!(row.contains("| created_at | 10 |"), "{row}");

    for (problem, created_at) in [("two-sum", 30), ("three-sum", 20)] {
        let inserted = write(
            &fixture,
            &attempts,
            "append",
            Some(json!({ "problem": problem, "created_at": created_at })),
        )
        .await;
        assert!(inserted.contains("Inserted "), "{inserted}");
    }

    let filtered = common::read_resource(
        &fixture.orch,
        format!("{attempts}?where=problem=two-sum&order=-created_at&limit=5&format=json"),
    )
    .await;
    let document = json_document(&filtered);
    assert_eq!(document["primaryKey"], "id");
    assert_eq!(document["total"], 2);
    let rows = document["rows"].as_array().unwrap();
    assert_eq!(
        rows[0],
        json!({ "id": 2, "problem": "two-sum", "created_at": 30 })
    );
    assert_eq!(
        rows[1],
        json!({ "id": 1, "problem": "two-sum", "created_at": 10 })
    );

    let patched = write(&fixture, &first, "patch", Some(json!({ "created_at": 40 }))).await;
    assert!(patched.contains(&format!("Updated {first}")), "{patched}");
    let row = common::read_resource(&fixture.orch, format!("{first}?format=json")).await;
    assert_eq!(json_document(&row)["row"]["created_at"], 40);

    let rekey = write(&fixture, &first, "patch", Some(json!({ "id": 9 }))).await;
    assert!(rekey.contains("row's address"), "{rekey}");

    let missing = format!("{attempts}/99");
    let patch_missing = write(
        &fixture,
        &missing,
        "patch",
        Some(json!({ "created_at": 1 })),
    )
    .await;
    assert!(
        patch_missing.contains(&format!("No row at {missing}")),
        "{patch_missing}"
    );
    let delete_missing = write(&fixture, &missing, "delete", None).await;
    assert!(
        delete_missing.contains(&format!("No row at {missing}")),
        "{delete_missing}"
    );

    let second = format!("{attempts}/2");
    let deleted = write(&fixture, &second, "delete", None).await;
    assert!(deleted.contains(&format!("Deleted {second}")), "{deleted}");
    let gone = common::read_resource(&fixture.orch, &second).await;
    assert!(gone.contains(&format!("No row at {second}")), "{gone}");

    let not_null = write(
        &fixture,
        &attempts,
        "append",
        Some(json!({ "created_at": 1 })),
    )
    .await;
    assert!(
        not_null.contains("NOT NULL constraint failed: attempts.problem"),
        "{not_null}"
    );
    let unknown = write(&fixture, &attempts, "append", Some(json!({ "nope": 1 }))).await;
    assert!(unknown.contains("no column `nope`"), "{unknown}");
    assert!(unknown.contains("id, problem, created_at"), "{unknown}");
}

/// A column with no declared type has no affinity: a stored `7` never equals
/// the text `'7'`. The URI an append returns must still read, patch, and
/// delete its row, and a filter on such a column must still match, while a
/// text key that merely looks numeric keeps matching as text.
#[tokio::test]
async fn rows_of_untyped_columns_are_addressable_by_the_uri_append_returns() {
    let fixture = fixture().await;
    sql(&fixture, "CREATE TABLE kv(k PRIMARY KEY, v, score)").await;
    let kv = format!("{}/kv", root());

    let inserted = write(
        &fixture,
        &kv,
        "append",
        Some(json!({ "k": 7, "v": "x", "score": 5 })),
    )
    .await;
    let row = format!("{kv}/7");
    assert!(inserted.contains(&format!("Inserted {row}")), "{inserted}");
    let read = common::read_resource(&fixture.orch, format!("{row}?format=json")).await;
    assert_eq!(json_document(&read)["row"]["v"], "x");

    let filtered =
        common::read_resource(&fixture.orch, format!("{kv}?where=score=5&format=json")).await;
    assert_eq!(json_document(&filtered)["total"], 1);

    let patched = write(&fixture, &row, "patch", Some(json!({ "v": "y" }))).await;
    assert!(patched.contains(&format!("Updated {row}")), "{patched}");

    let text_key = write(
        &fixture,
        &kv,
        "append",
        Some(json!({ "k": "007", "v": "text" })),
    )
    .await;
    let text_row = format!("{kv}/007");
    assert!(
        text_key.contains(&format!("Inserted {text_row}")),
        "{text_key}"
    );
    let read = common::read_resource(&fixture.orch, format!("{text_row}?format=json")).await;
    assert_eq!(json_document(&read)["row"]["v"], "text");

    let deleted = write(&fixture, &row, "delete", None).await;
    assert!(deleted.contains(&format!("Deleted {row}")), "{deleted}");
    let gone = common::read_resource(&fixture.orch, &row).await;
    assert!(gone.contains(&format!("No row at {row}")), "{gone}");
}

#[tokio::test]
async fn the_collection_affordance_lists_live_columns_and_the_primary_key() {
    let fixture = fixture().await;
    sql(
        &fixture,
        "CREATE TABLE attempts(id INTEGER PRIMARY KEY, problem TEXT NOT NULL, created_at INTEGER)",
    )
    .await;
    let collection = common::read_resource(&fixture.orch, format!("{}/attempts", root())).await;
    assert!(collection.starts_with("0 rows in "), "{collection}");
    assert!(
        collection.contains("`id(INTEGER, primary key, generated when omitted)`"),
        "{collection}"
    );
    assert!(
        collection.contains("`problem(TEXT, not null)`"),
        "{collection}"
    );
    assert!(
        collection.contains("required `problem(TEXT, not null)`"),
        "{collection}"
    );
    assert!(
        collection.contains(&format!("{}/attempts/{{pk}}", root())),
        "{collection}"
    );
}

#[tokio::test]
async fn keyless_rows_and_reserved_tables_are_refused_with_their_reasons() {
    let fixture = fixture().await;
    sql(
        &fixture,
        "CREATE TABLE pairs(a TEXT, b TEXT, PRIMARY KEY(a, b))",
    )
    .await;
    let row = common::read_resource(&fixture.orch, format!("{}/pairs/x", root())).await;
    assert!(row.contains("composite (or no) primary key"), "{row}");
    assert!(row.contains(&format!("{}?sql=", root())), "{row}");

    let schema_row = common::read_resource(&fixture.orch, format!("{}/schema/x", root())).await;
    assert!(schema_row.contains("reserved segment"), "{schema_row}");
}

#[tokio::test]
async fn only_the_owning_thread_writes_its_rows() {
    let fixture = fixture().await;
    sql(
        &fixture,
        "CREATE TABLE attempts(id INTEGER PRIMARY KEY, problem TEXT)",
    )
    .await;
    let intruded = common::change_resource_as_run(
        &fixture.orch,
        json!([{
            "target": format!("{}/attempts", root()),
            "mode": "append",
            "payload": { "problem": "stolen" }
        }]),
        Some(&fixture.intruder_run),
    )
    .await;
    assert!(
        intruded.contains("writable only by that thread"),
        "{intruded}"
    );
}

#[tokio::test]
async fn every_thread_write_emits_db_change_and_structured_writes_name_the_row() {
    let fixture = fixture().await;
    sql(
        &fixture,
        "CREATE TABLE attempts(id INTEGER PRIMARY KEY, problem TEXT); CREATE TABLE notes(body TEXT)",
    )
    .await;
    let ddl: Vec<Value> = fixture.db_changes();
    assert!(
        ddl.iter()
            .all(|event| event["table"] == "thread_db" && event["rowUri"].is_null()),
        "{ddl:?}"
    );
    assert!(
        ddl.iter().any(|event| event["dbTable"] == "attempts"),
        "{ddl:?}"
    );
    assert!(
        ddl.iter().any(|event| event["dbTable"] == "notes"),
        "{ddl:?}"
    );

    let before = fixture.db_changes().len();
    write(
        &fixture,
        &format!("{}/attempts", root()),
        "append",
        Some(json!({ "problem": "two-sum" })),
    )
    .await;
    let structured: Vec<Value> = fixture.db_changes().split_off(before);
    assert_eq!(structured.len(), 1, "{structured:?}");
    assert_eq!(
        structured[0],
        json!({
            "table": "thread_db",
            "action": "insert",
            "projectId": format!("project-{PROJECT}"),
            "threadId": format!("thread-{}", fixture.run),
            "name": THREAD,
            "dbTable": "attempts",
            "rowUri": format!("{}/attempts/1", root()),
        })
    );

    // A refused write changed nothing and announces nothing.
    let before = fixture.db_changes().len();
    write(&fixture, &format!("{}/attempts/99", root()), "delete", None).await;
    assert_eq!(fixture.db_changes().len(), before);

    sql(
        &fixture,
        "INSERT INTO attempts(problem) VALUES ('a'); DELETE FROM notes; SELECT 1",
    )
    .await;
    let attributed: Vec<Value> = fixture.db_changes().split_off(before);
    let pairs: Vec<(Value, Value)> = attributed
        .iter()
        .map(|event| (event["dbTable"].clone(), event["action"].clone()))
        .collect();
    assert_eq!(
        pairs,
        [
            (json!("attempts"), json!("insert")),
            (json!("notes"), json!("delete")),
        ]
    );
}

#[tokio::test]
async fn the_application_database_reads_rows_and_refuses_every_write() {
    let fixture = fixture().await;
    let project_row = format!("cairn://db/projects/project-{PROJECT}");
    let row = common::read_resource(&fixture.orch, &project_row).await;
    assert!(row.contains(&format!("| key | {PROJECT} |")), "{row}");

    let collection = common::read_resource(
        &fixture.orch,
        format!("cairn://db/projects?where=key={PROJECT}&format=json"),
    )
    .await;
    assert_eq!(json_document(&collection)["total"], 1);

    for target in ["cairn://db", "cairn://db/projects", project_row.as_str()] {
        for (mode, payload) in [
            ("create", Some(json!({ "key": "x" }))),
            ("append", Some(json!({ "key": "x" }))),
            ("patch", Some(json!({ "key": "x" }))),
            ("replace", Some(json!({ "key": "x" }))),
            ("delete", None),
        ] {
            let refused = write(&fixture, target, mode, payload).await;
            assert!(
                refused.contains("This resource is read-only"),
                "{mode} on {target}: {refused}"
            );
        }
    }
}
