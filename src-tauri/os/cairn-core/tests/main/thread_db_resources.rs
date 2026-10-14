use crate::common;
use serde_json::json;

const PROJECT: &str = "thread-db";
const THREAD: &str = "schema-owner";
const RUN: &str = "thread-db-run";
const CONDITION_THREAD: &str = "condition-owner";
const CONDITION_RUN: &str = "thread-db-condition-run";
const PROCEDURE_THREAD: &str = "procedure-owner";
const PROCEDURE_RUN: &str = "thread-db-procedure-run";
const INTRUDER_THREAD: &str = "intruder";
const INTRUDER_RUN: &str = "thread-db-intruder-run";

fn fixture_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    &LOCK
}

async fn fixture() -> (
    tempfile::TempDir,
    std::sync::Arc<cairn_core::internal::storage::LocalDb>,
    cairn_core::internal::orchestrator::Orchestrator,
) {
    let (temp, db, orch, _repo) = common::project_resource_fixture(PROJECT).await;
    common::seed_authenticated_thread_run(&db, &format!("project-{PROJECT}"), THREAD, RUN).await;
    cairn_core::threads::db_store::use_test_path(
        &format!("thread-{RUN}"),
        temp.path().join("thread-store.db"),
    );
    (temp, db, orch)
}

fn patch_with_params(sql: &str, params: serde_json::Value) -> serde_json::Value {
    json!([{
        "target": format!("cairn://p/{PROJECT}/{THREAD}/db"),
        "mode": "patch",
        "payload": { "sql": sql, "params": params }
    }])
}

#[tokio::test]
async fn parameterized_batches_are_atomic_and_single_statement_params_still_work() {
    let _fixture_guard = fixture_lock().lock().await;
    let (_temp, _db, orch) = fixture().await;
    let uri = format!("cairn://p/{PROJECT}/{THREAD}/db");

    let created = common::change_resource_as_run(
        &orch,
        patch_with_params(
            "CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT); INSERT INTO notes(body) VALUES (?); INSERT INTO notes(body) VALUES (?)",
            json!([[], ["first"], ["second"]]),
        ),
        Some(RUN),
    )
    .await;
    assert!(created.contains("Applied 3 statements"), "{created}");

    let flat = common::change_resource_as_run(
        &orch,
        patch_with_params("INSERT INTO notes(body) VALUES (?)", json!(["third"])),
        Some(RUN),
    )
    .await;
    assert!(flat.contains("Applied 1 statements"), "{flat}");

    let failed = common::change_resource_as_run(
        &orch,
        patch_with_params(
            "INSERT INTO notes(body) VALUES (?); INSERT INTO missing(body) VALUES (?)",
            json!([["rolled back"], ["failure"]]),
        ),
        Some(RUN),
    )
    .await;
    assert!(failed.contains("statement 2 failed"), "{failed}");

    let rows = common::read_resource(
        &orch,
        format!("{uri}?sql=SELECT body FROM notes ORDER BY id"),
    )
    .await;
    assert!(rows.contains("first"), "{rows}");
    assert!(rows.contains("second"), "{rows}");
    assert!(rows.contains("third"), "{rows}");
    assert!(!rows.contains("rolled back"), "{rows}");
}

#[tokio::test]
async fn parameterized_batches_refuse_mismatched_flat_and_mixed_shapes() {
    let _fixture_guard = fixture_lock().lock().await;
    let (_temp, _db, orch) = fixture().await;
    let sql = "CREATE TABLE notes(body TEXT); INSERT INTO notes(body) VALUES (?); INSERT INTO notes(body) VALUES (?)";

    let mismatch = common::change_resource_as_run(
        &orch,
        patch_with_params(sql, json!([[], ["only two arrays"]])),
        Some(RUN),
    )
    .await;
    assert!(
        mismatch.contains("2 statement parameter arrays"),
        "{mismatch}"
    );
    assert!(mismatch.contains("3 statements"), "{mismatch}");

    let flat = common::change_resource_as_run(
        &orch,
        patch_with_params(sql, json!(["first", "second"])),
        Some(RUN),
    )
    .await;
    assert!(flat.contains("flat array"), "{flat}");
    assert!(flat.contains("params:[[...],[...],[...]]"), "{flat}");

    let mixed = common::change_resource_as_run(
        &orch,
        patch_with_params(sql, json!([[], "second", ["third"]])),
        Some(RUN),
    )
    .await;
    assert!(mixed.contains("cannot mix"), "{mixed}");
    assert!(mixed.contains("params:[[...],[...],[...]]"), "{mixed}");
}

fn patch(sql: &str) -> serde_json::Value {
    json!([{
        "target": format!("cairn://p/{PROJECT}/{THREAD}/db"),
        "mode": "patch",
        "payload": { "sql": sql }
    }])
}

fn patch_thread(thread: &str, sql: &str) -> serde_json::Value {
    json!([{
        "target": format!("cairn://p/{PROJECT}/{thread}/db"),
        "mode": "patch",
        "payload": { "sql": sql }
    }])
}

struct ConditionFixture {
    _temp: tempfile::TempDir,
    db: std::sync::Arc<cairn_core::internal::storage::LocalDb>,
    orch: cairn_core::internal::orchestrator::Orchestrator,
}

/// One orchestrator, one main database, and exactly one installed evaluator,
/// shared by every condition test in this binary.
///
/// `install_evaluator` replaces a **process-global** sender, so a second
/// installation silently steals the first evaluator's notifications. Under
/// `cargo test` the tests in a binary share a process and run concurrently, so
/// two independent installations race: the loser's write is delivered to the
/// winner's evaluator, which reads a database holding none of the loser's
/// conditions, and the loser waits out its whole timeout for a wake that went
/// somewhere else. Distinct thread ids and store paths do not isolate that
/// global — nothing about it is per-fixture.
///
/// Installing once against one database that holds every condition removes the
/// race rather than scheduling around it. A new condition test seeds its thread
/// here and shares this orchestrator; it must not build a fixture of its own.
async fn condition_fixture() -> &'static ConditionFixture {
    static FIXTURE: tokio::sync::OnceCell<ConditionFixture> = tokio::sync::OnceCell::const_new();
    FIXTURE
        .get_or_init(|| async {
            let (temp, db, orch, _repo) = common::project_resource_fixture(PROJECT).await;
            for (thread, run) in [
                (CONDITION_THREAD, CONDITION_RUN),
                (PROCEDURE_THREAD, PROCEDURE_RUN),
            ] {
                common::seed_authenticated_thread_run(
                    &db,
                    &format!("project-{PROJECT}"),
                    thread,
                    run,
                )
                .await;
                cairn_core::threads::db_store::use_test_path(
                    &format!("thread-{run}"),
                    temp.path().join(format!("{run}.db")),
                );
            }
            cairn_core::internal::orchestrator::wakes::conditions::install_evaluator(orch.clone());
            ConditionFixture {
                _temp: temp,
                db,
                orch,
            }
        })
        .await
}

/// Wait for the single `condition:` push a fired condition leaves for `run`'s
/// job. Panics with `surface` named when none arrives.
async fn await_condition_push(
    db: &cairn_core::internal::storage::LocalDb,
    run: &str,
    surface: &str,
) {
    use cairn_db::storage::RowExt;

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let pushes = db
                .query_one(
                    "SELECT COUNT(*) FROM attention_pushes
                     WHERE recipient = ?1 AND key LIKE 'condition:%'",
                    cairn_db::turso::params![format!("job-{run}")],
                    |row| row.i64(0),
                )
                .await
                .unwrap();
            if pushes == 1 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("a {surface} write never reached the wake-condition evaluator"));
}

#[tokio::test]
async fn first_write_creates_thread_database_and_preserves_transaction_policy() {
    let _fixture_guard = fixture_lock().lock().await;
    let (_temp, _db, orch) = fixture().await;
    let uri = format!("cairn://p/{PROJECT}/{THREAD}/db");

    let empty = common::read_resource(&orch, &uri).await;
    assert!(empty.contains("thread database not created"), "{empty}");

    let created = common::change_resource_as_run(
        &orch,
        patch("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT); INSERT INTO notes(body) VALUES ('first');"),
        Some(RUN),
    )
    .await;
    assert!(created.contains("Applied 2 statements"), "{created}");

    let store = cairn_core::threads::db_store::open_existing(&format!("thread-{RUN}"))
        .await
        .unwrap()
        .expect("the CREATE write creates the store");
    assert_eq!(
        store.write_transaction_count(),
        0,
        "schema batches use the exclusive transaction primitive"
    );

    let schema = common::read_resource(&orch, &uri).await;
    assert!(schema.contains("notes"), "{schema}");
    let rows = common::read_resource(&orch, format!("{uri}?sql=SELECT id, body FROM notes")).await;
    assert!(rows.contains("first"), "{rows}");

    let rolled_back = common::change_resource_as_run(
        &orch,
        patch("CREATE TABLE doomed(id INTEGER); INSERT INTO missing_table VALUES (1);"),
        Some(RUN),
    )
    .await;
    assert!(rolled_back.contains("statement 2 failed"), "{rolled_back}");
    let schema_after_failure = common::read_resource(&orch, &uri).await;
    assert!(
        !schema_after_failure.contains("doomed"),
        "the first statement of a failed schema batch was not rolled back: {schema_after_failure}"
    );

    let inserted = common::change_resource_as_run(
        &orch,
        patch("INSERT INTO notes(body) VALUES ('second')"),
        Some(RUN),
    )
    .await;
    assert!(inserted.contains("Applied 1 statements"), "{inserted}");
    assert_eq!(
        store.write_transaction_count(),
        1,
        "ordinary DML retains the concurrent write path"
    );

    let transaction_control = common::change_resource_as_run(
        &orch,
        patch("BEGIN; INSERT INTO notes(body) VALUES ('forbidden'); COMMIT;"),
        Some(RUN),
    )
    .await;
    assert!(
        transaction_control.contains("Unsupported SQL statement 'BEGIN'"),
        "{transaction_control}"
    );
    let count = common::read_resource(
        &orch,
        format!("{uri}?sql=SELECT COUNT(*) AS count FROM notes"),
    )
    .await;
    assert!(
        count.contains("2"),
        "forbidden transaction SQL changed rows: {count}"
    );
}

/// The regression this guards: the resource dispatcher once grew its own copy
/// of the statement loop, which ran schema batches on the concurrent write path
/// and never notified the condition evaluator. Coverage that exercised
/// `db_store::apply_write` directly stayed green throughout, so this test drives
/// the write through the RESOURCE and asserts the wake actually lands.
#[tokio::test]
async fn a_write_through_the_resource_fires_an_armed_db_condition() {
    use cairn_core::internal::orchestrator::wakes::conditions;

    let ConditionFixture { db, orch, .. } = condition_fixture().await;
    let thread_id = format!("thread-{CONDITION_RUN}");

    conditions::create_wake_condition(
        db,
        &thread_id,
        "SELECT id FROM signals WHERE ready = 1",
        "signals ready",
        false,
        "agent",
    )
    .await
    .unwrap();

    // Two writes, so the schema path and the plain write path are each covered:
    // the DDL batch must reach the exclusive transaction primitive, and the DML
    // batch that actually satisfies the condition must reach the evaluator.
    let created = common::change_resource_as_run(
        orch,
        patch_thread(
            CONDITION_THREAD,
            "CREATE TABLE signals(id INTEGER PRIMARY KEY, ready INTEGER)",
        ),
        Some(CONDITION_RUN),
    )
    .await;
    assert!(created.contains("Applied 1 statements"), "{created}");

    let inserted = common::change_resource_as_run(
        orch,
        patch_thread(
            CONDITION_THREAD,
            "INSERT INTO signals(id, ready) VALUES (1, 1)",
        ),
        Some(CONDITION_RUN),
    )
    .await;
    assert!(inserted.contains("Applied 1 statements"), "{inserted}");

    await_condition_push(db, CONDITION_RUN, "cairn://.../db").await;

    let condition = conditions::list_wake_conditions(db, &thread_id)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(condition.last_outcome.as_deref(), Some("matched"));
}

/// The condition contract is "after every committed write to that store", and a
/// condition can read the app-contract tables the SQL surface never writes. A
/// procedure is stored through its own resource and its own writer, so this is
/// the check that the obligation follows committing rather than following
/// `cairn:~/db`.
#[tokio::test]
async fn a_procedures_write_also_fires_an_armed_db_condition() {
    use cairn_core::internal::orchestrator::wakes::conditions;

    let ConditionFixture { db, orch, .. } = condition_fixture().await;

    conditions::create_wake_condition(
        db,
        &format!("thread-{PROCEDURE_RUN}"),
        "SELECT name FROM _cairn_procedures WHERE name = 'collect'",
        "collect procedure stored",
        false,
        "agent",
    )
    .await
    .unwrap();

    let created = common::change_resource_as_run(
        orch,
        json!([{
            "target": format!("cairn://p/{PROJECT}/{PROCEDURE_THREAD}/procedures"),
            "mode": "create",
            "payload": {
                "name": "collect",
                "body": { "commands": [{ "command": "echo hi" }] }
            }
        }]),
        Some(PROCEDURE_RUN),
    )
    .await;
    assert!(created.contains("Created procedure"), "{created}");

    await_condition_push(db, PROCEDURE_RUN, "procedures").await;
}

/// `cairn:~/db` is the spelling a stored procedure actually writes, and the one
/// nothing here covered: every other test in this file names the canonical URI.
/// The ownership gate had no negative coverage at all, so "the gate refuses the
/// wrong caller" was an assumption rather than a fact.
///
/// This pins both halves against the same store: the owner writes through the
/// shorthand and the rows land, and two kinds of non-owner are refused in the
/// resource's own vocabulary without disturbing what the owner wrote.
#[tokio::test]
async fn the_home_shorthand_writes_and_a_non_owner_is_refused() {
    let _fixture_guard = fixture_lock().lock().await;
    let (temp, db, orch) = fixture().await;
    let canonical = format!("cairn://p/{PROJECT}/{THREAD}/db");

    common::seed_authenticated_thread_run(
        &db,
        &format!("project-{PROJECT}"),
        INTRUDER_THREAD,
        INTRUDER_RUN,
    )
    .await;
    cairn_core::threads::db_store::use_test_path(
        &format!("thread-{INTRUDER_RUN}"),
        temp.path().join("intruder-store.db"),
    );

    let home = json!([{
        "target": "cairn:~/db",
        "mode": "patch",
        "payload": { "sql": "CREATE TABLE ledger(id INTEGER PRIMARY KEY, note TEXT); INSERT INTO ledger(note) VALUES ('owned')" }
    }]);
    let owned = common::change_resource_as_run(&orch, home, Some(RUN)).await;
    assert!(
        owned.contains("Applied 2 statements"),
        "the owning thread must reach its own store through `cairn:~/db`: {owned}"
    );
    let rows =
        common::read_resource(&orch, format!("{canonical}?sql=SELECT note FROM ledger")).await;
    assert!(rows.contains("owned"), "{rows}");

    let intrusion = json!([{
        "target": canonical.clone(),
        "mode": "patch",
        "payload": { "sql": "INSERT INTO ledger(note) VALUES ('intruder')" }
    }]);

    // A run that belongs to a DIFFERENT thread: the gate's owner comparison.
    let refused =
        common::change_resource_as_run(&orch, intrusion.clone(), Some(INTRUDER_RUN)).await;
    assert!(
        refused.contains(&format!("is thread '{THREAD}'s own store")),
        "another thread's run must be refused in the resource's own words: {refused}"
    );

    // No authenticated run at all: the gate's lookup arm, which must refuse with
    // the same sentence rather than leaking a resolution error.
    let anonymous = common::change_resource_as_run(&orch, intrusion, None).await;
    assert!(
        anonymous.contains(&format!("is thread '{THREAD}'s own store")),
        "an unauthenticated write must be refused the same way: {anonymous}"
    );

    let after = common::read_resource(
        &orch,
        format!("{canonical}?sql=SELECT COUNT(*) AS count FROM ledger"),
    )
    .await;
    assert!(
        after.contains('1'),
        "a refused write must leave the store untouched: {after}"
    );
}
